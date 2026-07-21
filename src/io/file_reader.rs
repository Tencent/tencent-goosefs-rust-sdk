// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! High-level file reader that orchestrates the complete read pipeline.
//!
//! `GoosefsFileReader` ties together all low-level components into a single
//! easy-to-use API, analogous to Java's `GoosefsFileInStream`:
//!
//! ```text
//! GoosefsFileReader::open_with_context(ctx, path)
//!   → MasterClient.get_status()          — get file metadata + block IDs
//!   → BlockMapper.plan_read()            — split file range → block segments
//!   → for each block segment:
//!       → WorkerRouterView.select_worker()  — consistent hash routing
//!       → WorkerClient.connect()         — connect to target worker (pooled)
//!       → GrpcBlockReader.open()         — open streaming read
//!       → reader.read_all()              — read all chunk data
//!   → concatenate results
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use goosefs_sdk::io::GoosefsFileReader;
//! use goosefs_sdk::context::FileSystemContext;
//! use goosefs_sdk::config::GoosefsConfig;
//!
//! # async fn example() -> goosefs_sdk::error::Result<()> {
//! let ctx = FileSystemContext::connect(GoosefsConfig::new("127.0.0.1:9200")).await?;
//!
//! // Read entire file
//! let data = GoosefsFileReader::read_file_with_context(ctx.clone(), "/my-file.txt").await?;
//! println!("read {} bytes", data.len());
//!
//! // Range read (offset=100, length=500)
//! let data = GoosefsFileReader::read_range_with_context(ctx.clone(), "/my-file.txt", 100, 500).await?;
//!
//! // Or use the builder for streaming reads
//! let mut reader = GoosefsFileReader::open_with_context(ctx.clone(), "/my-file.txt").await?;
//! while let Some(chunk) = reader.read_next_block().await? {
//!     println!("got {} bytes from block", chunk.len());
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::block::mapper::{BlockMapper, BlockReadPlan};
use crate::block::router::WorkerRouterView;
use crate::block::short_circuit::{ShortCircuitError, ShortCircuitFactory};
use crate::cache::{
    page_cache_eligible, read_through_cache, CacheManager, ExternalRangeReader, FillMode,
};
use crate::client::worker::WorkerClientPool;
use crate::client::WorkerClient;
use crate::config::GoosefsConfig;
use crate::context::FileSystemContext;
use crate::error::{Error, Result};
use crate::io::reader::GrpcBlockReader;
use crate::proto::grpc::block::WorkerInfo;
use crate::proto::grpc::file::FileInfo;
use crate::proto::proto::dataserver::OpenUfsBlockOptions;

/// High-level file reader that orchestrates the full Goosefs read pipeline.
///
/// This struct encapsulates the complete read flow:
/// 1. `GetStatus` on Master to obtain file metadata (block IDs, block size, length)
/// 2. Discover workers and build consistent hash router
/// 3. Plan the read via `BlockMapper` (map file range → block segments)
/// 4. For each block: select worker → connect → stream-read via `GrpcBlockReader`
/// 5. Concatenate results
pub struct GoosefsFileReader {
    /// The Goosefs config.
    config: GoosefsConfig,
    /// The file path being read.
    path: String,
    /// File info from Master (contains block IDs, block size, length).
    ///
    /// **S3** (`docs/perf/2026-07-07-hotspot-optimizations/README.md`):
    /// wrapped in `Arc<FileInfo>` so a `FileInfoCache` hit on a
    /// repeated `open_with_context` / `open_range_with_context` call
    /// returns an `Arc` clone (one atomic inc) instead of a deep
    /// `FileInfo::clone` (which copies `block_ids: Vec<i64>`,
    /// `file_block_infos`, `ufs_path`, etc.). `Arc` implements `Deref`,
    /// so all `self.file_info.field` call sites work unchanged.
    file_info: Arc<FileInfo>,
    /// Worker router for block → worker mapping.
    /// Worker router view for block → worker mapping.
    ///
    /// P0-D Step 2 (`docs/perf/2026-07-07-hotspot-optimizations/README.md`
    /// §3.4): migrated from `WorkerRouter` (per-reader `ArcSwap`×3) to
    /// `WorkerRouterView` (per-reader `Arc`×2 + `Option<i64>` value).
    /// Byte-exact routing behaviour is guaranteed by
    /// `block::router::tests::test_view_select_worker_matches_shared_for_all_block_ids`.
    router: WorkerRouterView,
    /// Optional shared worker-connection pool.
    ///
    /// Non-`None` when constructed via `*_with_context`: worker connections
    /// are acquired from the shared `WorkerClientPool` (zero TCP+SASL
    /// handshake on cache hit). `None` falls back to per-call
    /// `WorkerClient::connect` (legacy path).
    worker_pool: Option<Arc<WorkerClientPool>>,
    /// Optional shared context (non-`None` when created via `*_with_context`).
    /// Kept alive to prevent context GC while the reader is in use.
    _context: Option<Arc<FileSystemContext>>,
    /// Block-level read plans (populated on open).
    plans: Vec<BlockReadPlan>,
    /// Index of the next block to read.
    current_plan_index: usize,
    /// Total bytes read so far.
    total_bytes_read: u64,
    /// Requested read offset in the file.
    offset: u64,
    /// Requested read length.
    length: u64,

    // ── Local page cache plumbing (only populated by `*_with_context`) ───────
    /// Shared local page cache; `None` disables caching (falls back to the
    /// original worker-direct read, byte-for-byte unchanged). Mirrors the
    /// plumbing on [`crate::io::GoosefsFileInStream`] so both readers share
    /// the same on-disk pages.
    cache: Option<Arc<dyn CacheManager>>,
    /// Stable per-file cache namespace; derived identically to
    /// `GoosefsFileInStream` (`file_id.to_string()`, `unwrap_or(0)`).
    cache_file_id: Arc<str>,
    /// Effective page size (= `config.client_cache_page_size`).
    cache_page_size: u64,
    /// Whether missed pages are written back (opendal never `NoCache`, so this
    /// equals `cache.is_some()`).
    cache_fill: bool,
    /// Whether back-fill uses the bounded async write-back pool.
    cache_async_write: bool,

    // ── Short-circuit (local mmap) read path ─────────────────────────────────
    /// Shared short-circuit factory when SC is enabled and the reader was built
    /// in context mode. `None` disables SC (legacy path / kill switch off).
    /// When `Some`, the per-block read entry point (`read_segment`) first attempts a
    /// local mmap read and transparently falls back to gRPC on any recoverable
    /// failure.
    short_circuit: Option<Arc<ShortCircuitFactory>>,

    // ── S4: pre-built UFS read options ───────────────────────────────────────
    /// Pre-built `OpenUfsBlockOptions` template for the UFS path, built once
    /// in [`Self::build`]. Cloned per segment with only `offset_in_file`
    /// updated — avoids re-cloning `ufs_path: String` + re-deriving
    /// `mount_id` / `no_cache` / `block_size` on every `read_segment` call.
    ///
    /// `None` when the file has no UFS path (cache-only data).
    ufs_read_options: Option<OpenUfsBlockOptions>,
}

// ── F1: on_file_open dedup cache ─────────────────────────────────────────
//
// `attach_cache` is called on every `open_range_with_context`. When the same
// file is read repeatedly (e.g. Lance vector search hitting the same .lance
// file), `on_file_open` is invoked every time even though the (file_id, length,
// mtime) triple is identical. The `versions` RwLock read inside `on_file_open`
// is cheap when uncontended, but under 20+ concurrent queries the lock
// contention alone wastes ~5.5ms per call (result.log data).
//
// This process-level dedup cache records the last `on_file_open` check time
// per `(file_id, length, mtime)` key. If the same identity was checked within
// the TTL window, `on_file_open` is skipped entirely — zero lock acquisition.

/// TTL for the on_file_open dedup cache. Within this window, repeated opens of
/// the same file (same file_id + length + mtime) skip the `on_file_open` call
/// entirely. Set to 60s to match the `ConfigRefresher` cadence — if a file is
/// overwritten while the process is running, the next config refresh + open
/// will catch it. For safety, a server-detected mtime change always
/// re-triggers `on_file_open` (different key → miss).
const ON_FILE_OPEN_DEDUP_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Key: (file_id as i64, length as i64, mtime as i64).
/// Value: Instant of the last `on_file_open` check.
type OnFileOpenDedupMap = std::collections::HashMap<(i64, i64, i64), std::time::Instant>;

static ON_FILE_OPEN_CACHE: std::sync::LazyLock<RwLock<OnFileOpenDedupMap>> =
    std::sync::LazyLock::new(|| RwLock::new(std::collections::HashMap::new()));

impl GoosefsFileReader {
    /// Open a file for reading using a shared [`FileSystemContext`].
    ///
    /// Reuses the Master client, worker-list snapshot and worker-connection
    /// pool cached inside the context — **no additional TCP+SASL handshake**
    /// to Master or Worker Manager is performed. This is the recommended
    /// constructor for long-running clients (OpenDAL, Lance, etc.).
    pub async fn open_with_context(ctx: Arc<FileSystemContext>, path: &str) -> Result<Self> {
        let (file_info, router) = Self::init_with_context(&ctx, path).await?;
        let file_length = file_info.length.unwrap_or(0) as u64;
        let config = ctx.config().clone();
        let pool = Some(ctx.acquire_worker_pool());
        let mut reader = Self::build(
            &config,
            path,
            file_info,
            router,
            pool,
            Some(ctx.clone()),
            0,
            file_length,
        )?;
        reader.attach_cache(&ctx).await;
        Ok(reader)
    }

    /// Open a file for range reading using a shared [`FileSystemContext`].
    ///
    /// Same benefits as [`open_with_context`](Self::open_with_context) plus
    /// explicit `(offset, length)` control.
    pub async fn open_range_with_context(
        ctx: Arc<FileSystemContext>,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Self> {
        let (file_info, router) = Self::init_with_context(&ctx, path).await?;
        let config = ctx.config().clone();
        let pool = Some(ctx.acquire_worker_pool());
        let mut reader = Self::build(
            &config,
            path,
            file_info,
            router,
            pool,
            Some(ctx.clone()),
            offset,
            length,
        )?;
        reader.attach_cache(&ctx).await;
        Ok(reader)
    }

    /// Internal: fetch file info via the shared Master, snapshot workers from
    /// the shared router — **no new RPC connections**.
    ///
    /// This is the context-aware analogue of [`Self::init`]. It mirrors the
    /// pattern used by `GoosefsFileWriter::create_with_context`: a local
    /// `WorkerRouterView` is created and seeded from the shared router's current
    /// snapshot, so per-read failure marking stays local and does not pollute
    /// the long-lived context-level routing state.
    ///
    /// **A1** (`docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md`): the local view is
    /// built via [`WorkerRouterView::from_shared`], which clones the shared
    /// router's `workers` + `hash_ring` `Arc`s wait-free (two `Arc::clone`s
    /// plus a value copy of `local_worker_id`) with **no `ArcSwap`
    /// allocation** — replacing the previous
    /// `WorkerRouter::snapshot_from` that also allocated three per-reader
    /// `ArcSwap` fields. `init_with_context`'s hot-path CPU drops from
    /// ~10.4 % to ~0 % of on-CPU, and per-reader Drop cost drops from
    /// ~19 % (`arc_swap::debt::list::LocalNode::with`) to ~0 %.
    /// Failure isolation is preserved: the view has its own
    /// `failed_workers` DashMap.
    async fn init_with_context(
        ctx: &Arc<FileSystemContext>,
        path: &str,
    ) -> Result<(Arc<FileInfo>, WorkerRouterView)> {
        // 1. Reuse the shared Master client (zero handshake).
        //
        // **A3** (`docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md`): consult the opt-in
        // FileInfo metadata cache first. On hit, skip the RPC entirely; on
        // miss, populate the cache after a successful `get_status`. The
        // cache is `None` only when the caller has explicitly opted out
        // (`file_info_cache_ttl == 0`). By default the TTL is 30 s, so this
        // branch consults the live cache.
        //
        // **S3**: `get` returns `Arc<FileInfo>` — a cache hit is now a
        // single `Arc::clone` (atomic inc) instead of a deep
        // `FileInfo::clone`. On miss, the fetched `FileInfo` is wrapped
        // in `Arc` once, inserted into the cache, and returned directly
        // (zero clone on the insert path too — the old code did
        // `fetched.clone()` for the cache + moved `fetched` out).
        let file_info_cache = ctx.acquire_file_info_cache();
        let file_info = if let Some(cached) = file_info_cache.as_ref().and_then(|c| c.get(path)) {
            debug!(path = %path, "FileInfo cache hit (§A3 + S3 — Arc clone, zero deep copy)");
            cached
        } else {
            let master = ctx.acquire_master();
            let fetched = master.get_status(path).await?;
            // Wrap once in Arc; the cache stores an Arc clone (atomic
            // inc) and we keep the original Arc — zero deep copy on
            // both the insert and the return path (S3).
            let arc_fetched = Arc::new(fetched);
            if let Some(cache) = &file_info_cache {
                cache.insert_arc(path, Arc::clone(&arc_fetched));
            }
            arc_fetched
        };

        let file_length = file_info.length.unwrap_or(0);
        if file_length == 0 {
            debug!(path = %path, "file is empty");
        }

        debug!(
            path = %path,
            file_length = file_length,
            block_count = file_info.block_ids.len(),
            block_size = ?file_info.block_size_bytes,
            "fetched file metadata (via context)"
        );

        // 2. Snapshot from the shared router — clones two `Arc`s (workers +
        //    hash_ring) wait-free; does NOT rebuild the ring. The shared
        //    router is kept fresh by the context's background worker-refresh
        //    task, so no extra RPC is issued here.
        let shared_router = ctx.acquire_router();
        // Cheap non-empty guard (H4: use `workers_is_empty()` instead of
        // `get_workers().await.len()` — avoids an `Arc::clone` + `Drop`
        // just for a non-empty check).
        if shared_router.workers_is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers available for reading".to_string(),
            });
        }
        let router = WorkerRouterView::from_shared(&shared_router);
        debug!("reusing worker snapshot from context (A1)");

        Ok((file_info, router))
    }

    /// Internal: build the reader from file info and router.
    #[allow(clippy::too_many_arguments)]
    fn build(
        config: &GoosefsConfig,
        path: &str,
        file_info: Arc<FileInfo>,
        router: WorkerRouterView,
        worker_pool: Option<Arc<WorkerClientPool>>,
        context: Option<Arc<FileSystemContext>>,
        offset: u64,
        length: u64,
    ) -> Result<Self> {
        // Plan the read
        let plans = BlockMapper::plan_read(&file_info, offset, length);
        debug!(
            path = %path,
            offset = offset,
            length = length,
            block_segments = plans.len(),
            "read plan created"
        );

        // S4: pre-build the UFS read options template once. Per-segment
        // `build_ufs_read_options` now clones this and updates only
        // `offset_in_file`, avoiding a `ufs_path.clone()` + field
        // re-derivation on every `read_segment`.
        let ufs_read_options = {
            let ufs_path = file_info.ufs_path.as_ref();
            if ufs_path.map_or(true, |p| p.is_empty()) {
                None
            } else {
                let block_size = file_info.block_size_bytes.unwrap_or(64 * 1024 * 1024);
                Some(OpenUfsBlockOptions {
                    ufs_path: file_info.ufs_path.clone(),
                    // offset_in_file is per-segment; set to 0 as placeholder.
                    offset_in_file: Some(0),
                    block_size: Some(block_size),
                    max_ufs_read_concurrency: None,
                    mount_id: file_info.mount_id,
                    no_cache: Some(!file_info.cacheable.unwrap_or(true)),
                    user: None,
                    caller_type: None,
                })
            }
        };

        Ok(Self {
            config: config.clone(),
            path: path.to_string(),
            file_info,
            router,
            worker_pool,
            _context: context,
            plans,
            current_plan_index: 0,
            total_bytes_read: 0,
            offset,
            length,
            // Cache/SC are disabled by default here; `attach_cache()` enables
            // them in the async opener when a shared context is present.
            cache: None,
            cache_file_id: Arc::from(""),
            cache_page_size: config.client_cache_page_size,
            cache_fill: false,
            cache_async_write: false,
            short_circuit: None,
            ufs_read_options,
        })
    }

    /// Inject the shared local page cache and short-circuit factory from a
    /// shared [`FileSystemContext`] (best-effort).
    ///
    /// `build()` is synchronous, but `on_file_open` is async, so cache
    /// activation is deferred to this async opener helper. The `file_id`,
    /// `length` and `mtime` derivations are kept **byte-for-byte aligned** with
    /// `URIStatus::from_proto` (all `unwrap_or(0)`), so this reader hits exactly
    /// the same on-disk pages as `GoosefsFileInStream`.
    async fn attach_cache(&mut self, ctx: &Arc<FileSystemContext>) {
        // Short-circuit is independent of the page cache: it can accelerate the
        // worker read even when the page cache is disabled.
        self.short_circuit = ctx.acquire_short_circuit();

        // HR-1 (design §9.2): `file_id <= 0` means no stable inode identity.
        // See [`page_cache_eligible`]. This guard MUST run before
        // `acquire_cache_manager()` / `on_file_open()` so a "0"-keyed open never
        // pollutes the version table.
        let file_id = self.file_info.file_id.unwrap_or(0);
        if !page_cache_eligible(file_id) {
            self.cache = None;
            self.cache_fill = false;
            return;
        }

        let cache = ctx.acquire_cache_manager();
        // Key on `file_id` (> 0), aligned with `URIStatus.file_id`. Do NOT fall
        // back to `path` — that would key differently from the InStream's "0"
        // and would go stale on same-name recreate.
        let cache_file_id: Arc<str> = Arc::from(file_id.to_string());

        // F1: Skip `on_file_open` if this exact (file_id, length, mtime) was
        // already checked within the dedup TTL window. The dedup cache is keyed
        // by the server-reported identity, so a genuine overwrite (different
        // length or mtime) always produces a different key → cache miss →
        // `on_file_open` runs and detects the change.
        if let Some(cache) = &cache {
            let length = self.file_info.length.unwrap_or(0);
            let mtime = self.file_info.last_modification_time_ms.unwrap_or(0);
            let dedup_key = (file_id, length, mtime);

            let skip = {
                let guard = ON_FILE_OPEN_CACHE.read().await;
                guard
                    .get(&dedup_key)
                    .is_some_and(|last| last.elapsed() < ON_FILE_OPEN_DEDUP_TTL)
            };
            if !skip {
                cache.on_file_open(&cache_file_id, length, mtime).await;
                let mut guard = ON_FILE_OPEN_CACHE.write().await;
                // Bounded: evict expired entries when the cache grows beyond a
                // reasonable size to prevent unbounded growth from many files.
                if guard.len() > 4096 {
                    guard.retain(|_, last| last.elapsed() < ON_FILE_OPEN_DEDUP_TTL);
                }
                guard.insert(dedup_key, std::time::Instant::now());
            }
        }

        let cfg = ctx.config();
        self.cache_fill = cache.is_some(); // opendal read path is never NoCache
        self.cache_async_write = cfg.client_cache_async_write_enabled;
        self.cache_page_size = cfg.client_cache_page_size;
        self.cache_file_id = cache_file_id;
        self.cache = cache;
    }

    /// Read the next block segment and return its data.
    ///
    /// Returns `None` when all block segments have been read.
    /// This is useful for streaming reads where you want to process
    /// data block-by-block.
    ///
    /// # Authentication failure recovery
    ///
    /// When a cached worker connection's SASL stream has expired, block reads
    /// fail with `AuthenticationFailed`.  This method detects such failures
    /// and automatically:
    /// 1. Invalidates the stale connection in the pool
    /// 2. Reconnects with fresh authentication
    /// 3. Retries the block read **once**
    ///
    /// This handles the common case of SASL expiry after process fork
    /// (Python `multiprocessing.spawn`) or long idle periods.
    pub async fn read_next_block(&mut self) -> Result<Option<Bytes>> {
        // 1) Skip invalid blocks and compute this segment's absolute file range.
        //    (Same invalid-block-skip semantics as the pre-cache implementation.)
        let (abs_offset, abs_end) = loop {
            if self.current_plan_index >= self.plans.len() {
                return Ok(None);
            }
            let plan = self.plans[self.current_plan_index].clone();
            let block_id = self.resolve_block_id(&plan);
            if block_id <= 0 {
                warn!(
                    block_index = plan.block_index,
                    plan_block_id = plan.block_id,
                    "invalid block ID, skipping block"
                );
                self.current_plan_index += 1;
                continue;
            }
            let block_size = self.file_info.block_size_bytes.unwrap_or(64 * 1024 * 1024) as u64;
            let abs_offset = (plan.block_index * block_size + plan.offset_in_block) as i64;
            break (abs_offset, abs_offset + plan.length as i64);
        };

        // 2) Fetch the bytes. With a cache, route through `read_through_cache`
        //    (this reader acts as the miss source via `ExternalRangeReader`);
        //    otherwise read directly, which is byte-for-byte equivalent to the
        //    pre-cache implementation (same plan → same worker verb → same
        //    bytes).
        let data = match self.cache.clone() {
            Some(cache) => {
                let file_id = self.cache_file_id.clone();
                let page_size = self.cache_page_size;
                let file_length = self.file_length() as i64;
                let fill_mode = if !self.cache_fill {
                    FillMode::None
                } else if self.cache_async_write {
                    FillMode::Async
                } else {
                    FillMode::Sync
                };
                // The scalar plan values are already copied out above, so there
                // is no live borrow of `self` — we can pass `&mut self` as the
                // miss source to `read_through_cache`.
                read_through_cache(
                    &cache,
                    self,
                    &file_id,
                    page_size,
                    file_length,
                    abs_offset,
                    abs_end,
                    fill_mode,
                )
                .await?
            }
            None => self.read_file_range(abs_offset, abs_end).await?,
        };

        // 3) Advance the iterator state (identical to the old implementation).
        let bytes_read = data.len() as u64;
        self.total_bytes_read += bytes_read;
        self.current_plan_index += 1;

        debug!(
            abs_offset = abs_offset,
            abs_end = abs_end,
            bytes_read = bytes_read,
            total_read = self.total_bytes_read,
            cache_enabled = self.cache.is_some(),
            "block segment read complete"
        );

        Ok(Some(data))
    }

    /// Stateless: read a single block `plan`'s bytes (`[offset_in_block, +length)`).
    ///
    /// Preserves the full **two-layer failover** of the original
    /// `read_next_block`:
    ///   ① connect failure → auth failure reconnects; non-auth marks the worker
    ///      failed and retries once on a different worker;
    ///   ② auth failure during the RPC → single-flight reconnect + re-read
    ///      (same `open + read_all` verb, byte-equivalent — HR-3).
    /// It also attempts the short-circuit (local mmap) path first, falling back
    /// transparently to gRPC on any recoverable failure (§8.2).
    ///
    /// Uses only `&self` — it never touches iterator state, so it is safe to be
    /// re-entered as the cache-miss source.
    async fn read_segment(&self, block_id: i64, plan: &BlockReadPlan) -> Result<Bytes> {
        // Try the short-circuit (local mmap) path first; a recoverable failure
        // returns `None` and we fall through to the gRPC path below (§8.2).
        if let Some(sc_result) = self.try_short_circuit_read(block_id, plan).await {
            return sc_result;
        }

        let ufs_options = self.build_ufs_read_options(plan);

        // ① Select worker + connection failover (mirrors the old read_next_block).
        let worker_info = self.router.select_worker(block_id).await?;
        let worker_addr = Self::worker_addr(&worker_info)?;
        let worker = match self.acquire_worker(&worker_addr).await {
            Ok(w) => w,
            Err(e) if e.is_authentication_failed() => {
                debug!(
                    worker = %worker_addr,
                    error = %e,
                    "authentication failed on connect, reconnecting"
                );
                self.reconnect_worker(&worker_addr, None).await?
            }
            Err(e) => {
                // Non-auth connect failure: mark the worker failed and retry
                // once on a different worker.
                if let Some(a) = worker_info.address.as_ref() {
                    self.router.mark_failed(a);
                }
                warn!(
                    worker = %worker_addr,
                    error = %e,
                    "worker connection failed, trying another worker"
                );
                let retry = self.router.select_worker(block_id).await.map_err(|_| e)?;
                let retry_addr = Self::worker_addr(&retry)?;
                self.acquire_worker(&retry_addr).await?
            }
        };

        // ② Read the segment + single-flight auth reconnect on RPC failure.
        let worker_generation = worker.generation();
        match self
            .try_read_block(&worker, block_id, plan, ufs_options.clone())
            .await
        {
            Ok(d) => Ok(d),
            Err(e) if e.is_authentication_failed() => {
                debug!(
                    block_id = block_id,
                    worker = %worker_addr,
                    stale_generation = worker_generation,
                    error = %e,
                    "auth failed during block read, requesting single-flight reconnect"
                );
                let fresh = self
                    .reconnect_worker(&worker_addr, Some(worker_generation))
                    .await?;
                self.try_read_block(&fresh, block_id, plan, ufs_options)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    /// Stateless: read the absolute file range `[abs_offset, abs_end)`.
    ///
    /// Never touches `self.plans` / `self.current_plan_index` / `self.offset` /
    /// `self.length`, so it is safe to be re-entered as the cache-miss source
    /// (page-level back-fill) without corrupting the outer `read_next_block`
    /// iteration.
    async fn read_file_range(&self, abs_offset: i64, abs_end: i64) -> Result<Bytes> {
        if abs_end <= abs_offset {
            return Ok(Bytes::new());
        }
        // Local re-planning: a page (≤ 1 MiB) usually lands inside a single
        // block (≥ 64 MiB), so this is typically one segment.
        let plans = BlockMapper::plan_read(
            &self.file_info,
            abs_offset as u64,
            (abs_end - abs_offset) as u64,
        );

        let mut buf = BytesMut::with_capacity((abs_end - abs_offset) as usize);
        for plan in &plans {
            let block_id = self.resolve_block_id(plan);
            // Same invalid-block-skip semantics as `read_next_block` (HR-1):
            // a non-positive `block_id` means the master returned no valid
            // block for this segment, so we skip it silently rather than
            // fail the whole range read. Any real hole in the file surfaces
            // via the `data.is_empty()` check below on a *valid* block.
            if block_id <= 0 {
                continue;
            }
            let data = self.read_segment(block_id, plan).await?;
            if data.is_empty() {
                return Err(Error::Internal {
                    message: format!("read_file_range: 0 bytes for block {block_id}"),
                    source: None,
                });
            }
            buf.extend_from_slice(&data);
        }
        Ok(buf.freeze())
    }

    /// Attempt a short-circuit (local mmap) read of a single block segment.
    ///
    /// Returns:
    /// - `Some(Ok(bytes))` — SC served the read (zero-copy).
    /// - `Some(Err(e))`   — a **semantic** error (`OutOfRange`) that must be
    ///   surfaced unchanged; the caller must NOT fall back.
    /// - `None`           — SC was not used, or hit a recoverable failure; the
    ///   caller transparently falls back to gRPC.
    async fn try_short_circuit_read(
        &self,
        block_id: i64,
        plan: &BlockReadPlan,
    ) -> Option<Result<Bytes>> {
        // §B1: `self.short_circuit == None` means the SC factory was never
        // built (SC disabled by config, or the reader was constructed
        // without a `FileSystemContext`). Count it as SKIPPED so
        //     hit_rate = HIT / (HIT + SKIPPED + FALLBACK_OPEN + FALLBACK_READ)
        // has a stable denominator across `short_circuit_enabled` toggles.
        let factory = match self.short_circuit.as_ref() {
            Some(f) => f,
            None => {
                crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_SKIPPED).inc(1);
                return None;
            }
        };
        let block_size = self.block_logical_size(plan.block_index);

        if !factory.should_use(block_id, block_size).await {
            // §B1: SC disabled / pre-filter rejected the block. This is the
            // hit-rate denominator's "skipped" bucket (see registry docs).
            crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_SKIPPED).inc(1);
            return None;
        }

        let reader = match factory.get_or_open(block_id, block_size).await {
            Ok(r) => r,
            Err(e) => {
                debug!(
                    block_id = block_id,
                    error = %e,
                    "short-circuit open failed, falling back to gRPC"
                );
                // §B1: SC attempted but the open step failed. The specific
                // cause is exposed via `ShortCircuitOpenLocalFail` /
                // `ShortCircuitFileOpenFail` / `ShortCircuitMmapFail`.
                crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_FALLBACK_OPEN)
                    .inc(1);
                return None;
            }
        };

        match reader.read_bytes(plan.offset_in_block as usize, plan.length as usize) {
            Ok(bytes) => {
                // §B1: SC actually served this read.
                crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_HIT).inc(1);
                Some(Ok(bytes))
            }
            Err(ShortCircuitError::OutOfRange {
                off,
                len,
                file_size,
            }) => {
                // §B1: semantic error — propagates rather than falls back.
                crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_SEMANTIC_ERROR)
                    .inc(1);
                Some(Err(Error::InvalidArgument {
                    message: format!(
                        "short-circuit read out of range on block {block_id}: \
                         off={off} len={len} block_size={file_size}"
                    ),
                }))
            }
            Err(e) => {
                debug!(
                    block_id = block_id,
                    error = %e,
                    "short-circuit read failed, falling back to gRPC"
                );
                // §B1: SC opened OK but read failed recoverably; falling back.
                crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_FALLBACK_READ)
                    .inc(1);
                factory.invalidate(block_id).await;
                None
            }
        }
    }

    /// Logical (on-disk) byte size of the block at `block_index`.
    ///
    /// Full blocks are `block_size_bytes`; the trailing block is the file
    /// remainder. This is the value the short-circuit factory expects (matching
    /// the Worker's `OpenLocalBlock` response `block_size`).
    fn block_logical_size(&self, block_index: u64) -> i64 {
        let bs = self.file_info.block_size_bytes.unwrap_or(64 * 1024 * 1024);
        let file_length = self.file_length() as i64;
        if bs <= 0 {
            return file_length.max(0);
        }
        let start = block_index as i64 * bs;
        (file_length - start).clamp(0, bs)
    }

    /// Format a `WorkerInfo`'s address as `host:rpc_port`.
    ///
    /// Shared by the primary read and the failover retry so both build the
    /// address identically (inlines the old manual formatting + the
    /// `worker has no address` error branch).
    ///
    /// P0-F.2: delegates to the shared [`rpc_endpoint`] helper (uses
    /// `itoa::write` for the port) so every `host:port` construction in
    /// the crate goes through the same allocation-free path.
    fn worker_addr(worker_info: &WorkerInfo) -> Result<String> {
        let addr = worker_info
            .address
            .as_ref()
            .ok_or_else(|| Error::Internal {
                message: "worker has no address".to_string(),
                source: None,
            })?;
        Ok(crate::block::router::rpc_endpoint(addr))
    }

    /// Read all remaining data and return it as a single `Bytes`.
    ///
    /// This reads all block segments sequentially and concatenates the results.
    pub async fn read_all(&mut self) -> Result<Bytes> {
        let expected_len = self.plans.iter().map(|p| p.length).sum::<u64>();
        let mut buf = BytesMut::with_capacity(expected_len as usize);

        while let Some(chunk) = self.read_next_block().await? {
            buf.extend_from_slice(&chunk);
        }

        Ok(buf.freeze())
    }

    /// Acquire a worker client for the given address.
    ///
    /// When a shared [`WorkerClientPool`] is available (context path), the
    /// connection is pulled from the pool — which caches authenticated gRPC
    /// channels per-address — so repeated reads to the same worker pay
    /// **zero** handshake cost.
    ///
    /// Without a pool (legacy config-only path), a fresh `WorkerClient` is
    /// established per call, matching the pre-context behaviour.
    async fn acquire_worker(&self, addr: &str) -> Result<WorkerClient> {
        if let Some(pool) = &self.worker_pool {
            pool.acquire(addr).await
        } else {
            WorkerClient::connect(addr, &self.config).await
        }
    }

    /// Invalidate the stale connection and reconnect with fresh authentication.
    ///
    /// Used when a block read or connect fails with `AuthenticationFailed`.
    ///
    /// When `stale_generation` is `Some(gen)`, the pool performs a
    /// **single-flight reconnect**: if another concurrent reader has already
    /// replaced the channel with a newer generation the existing fresh
    /// connection is returned without triggering a redundant TCP+SASL
    /// handshake.  When `None` (e.g. the failure happened during `connect`
    /// so no `WorkerClient` was ever produced) the pool performs an
    /// unconditional reconnect (still serialised per-address so concurrent
    /// callers share one handshake).
    async fn reconnect_worker(
        &self,
        addr: &str,
        stale_generation: Option<u64>,
    ) -> Result<WorkerClient> {
        if let Some(pool) = &self.worker_pool {
            match stale_generation {
                Some(gen) => pool.reconnect_if_stale(addr, gen).await,
                None => pool.reconnect(addr).await,
            }
        } else {
            WorkerClient::connect(addr, &self.config).await
        }
    }

    /// Attempt to open a block reader and read all data from it.
    ///
    /// Factored out from `read_next_block` so the auth-failure retry path
    /// can reuse the same logic with a fresh worker.
    async fn try_read_block(
        &self,
        worker: &WorkerClient,
        block_id: i64,
        plan: &BlockReadPlan,
        ufs_options: Option<OpenUfsBlockOptions>,
    ) -> Result<Bytes> {
        let mut block_reader = GrpcBlockReader::open(
            worker,
            block_id,
            plan.offset_in_block as i64,
            plan.length as i64,
            self.config.chunk_size as i64,
            ufs_options,
        )
        .await?;

        block_reader.read_all().await
    }

    /// Resolve the best block ID for a read plan.
    ///
    /// Prefers the block ID from `file_block_infos` (which contains the actual
    /// assigned block ID from the server) over the ID computed from `block_ids`.
    fn resolve_block_id(&self, plan: &BlockReadPlan) -> i64 {
        if let Some(fbi) = self
            .file_info
            .file_block_infos
            .get(plan.block_index as usize)
        {
            if let Some(bi) = &fbi.block_info {
                if let Some(id) = bi.block_id {
                    if id > 0 {
                        return id;
                    }
                }
            }
        }
        // Fall back to block_ids list
        plan.block_id
    }

    /// Build `OpenUfsBlockOptions` for a block that may reside in UFS.
    ///
    /// When data was written with `THROUGH` mode, the block only exists in the
    /// underlying file system (UFS). The Worker needs `OpenUfsBlockOptions` to
    /// know the UFS path, mount ID, and block geometry so it can read the data
    /// from UFS on behalf of the client.
    ///
    /// Returns `None` if the file has no UFS path (i.e. data is cache-only).
    ///
    /// **S4** (`docs/perf/2026-07-07-hotspot-optimizations/README.md`):
    /// clones the pre-built `self.ufs_read_options` template and updates only
    /// `offset_in_file`. The old path re-cloned `ufs_path: String` +
    /// re-derived `mount_id` / `no_cache` / `block_size` on every
    /// `read_segment` call. Now the per-segment cost is one `OpenUfsBlockOptions::clone`
    /// (which clones `ufs_path: Option<String>` — still a String clone, but
    /// only one field instead of re-deriving all fields) plus one `offset_in_file`
    /// assignment. The `ufs_path` clone is unavoidable because the proto
    /// type owns its string.
    fn build_ufs_read_options(&self, plan: &BlockReadPlan) -> Option<OpenUfsBlockOptions> {
        let template = self.ufs_read_options.as_ref()?;
        let block_size = template.block_size.unwrap_or(64 * 1024 * 1024);
        let offset_in_file = plan.block_index as i64 * block_size;

        let mut opts = template.clone();
        opts.offset_in_file = Some(offset_in_file);
        Some(opts)
    }

    /// One-shot convenience: read an entire file using a shared context.
    ///
    /// Reuses Master and Worker connections from the supplied
    /// [`FileSystemContext`]. This is the recommended entry point for
    /// long-running clients that want to avoid per-call handshakes.
    ///
    /// ```rust,no_run
    /// # async fn example() -> goosefs_sdk::error::Result<()> {
    /// use std::sync::Arc;
    /// use goosefs_sdk::io::GoosefsFileReader;
    /// use goosefs_sdk::config::GoosefsConfig;
    /// use goosefs_sdk::context::FileSystemContext;
    ///
    /// let config = GoosefsConfig::new("127.0.0.1:9200");
    /// let ctx = FileSystemContext::connect(config).await?;
    /// let data = GoosefsFileReader::read_file_with_context(ctx, "/my-file.txt").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_file_with_context(ctx: Arc<FileSystemContext>, path: &str) -> Result<Bytes> {
        let mut reader = Self::open_with_context(ctx, path).await?;
        reader.read_all().await
    }

    /// One-shot convenience: read a byte range from a file using a shared context.
    pub async fn read_range_with_context(
        ctx: Arc<FileSystemContext>,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let mut reader = Self::open_range_with_context(ctx, path, offset, length).await?;
        reader.read_all().await
    }

    /// Read multiple `(offset, length)` ranges from `path` in one call
    /// (FLAMEGRAPH_OPTIMIZATION_PLAN §B2).
    ///
    /// # Behaviour
    ///
    /// The returned `Vec<Bytes>` has **exactly the same length and order**
    /// as `ranges`, and `result[i]` is byte-identical to what
    /// [`Self::read_range_with_context`] would return for
    /// `ranges[i]` — regardless of whether coalescing is enabled.
    ///
    /// - `ctx.config().range_coalesce_enabled == false` (**default**): each
    ///   input range is served by an independent
    ///   `read_range_with_context` call. Behaviour is exactly what a
    ///   caller-side loop would produce, so this path is a drop-in
    ///   replacement.
    /// - `ctx.config().range_coalesce_enabled == true`: adjacent input
    ///   ranges (gap ≤ `range_coalesce_gap_bytes`) are merged into one
    ///   fetch, capped at `range_coalesce_max_bytes` per merged fetch,
    ///   and the payload is spliced back so each `result[i]` recovers
    ///   the exact bytes of `ranges[i]`. This trades over-read of
    ///   `≤ Σ gap_i` bytes for a large drop in H2 stream count, which
    ///   is what the flame graph identifies as the dominant cost on
    ///   Lance / DuckDB scan patterns.
    ///
    /// Empty input ranges (`len == 0`) produce empty `Bytes` in the
    /// output without triggering any I/O.
    ///
    /// # Errors
    ///
    /// If any underlying `read_range` call fails, that error is returned
    /// immediately. In the enabled path a merged fetch failure fails
    /// **all** its constituent input ranges (they share transport), which
    /// matches the failure model the H2 layer would produce anyway.
    pub async fn read_ranges_with_context(
        ctx: Arc<FileSystemContext>,
        path: &str,
        ranges: &[(u64, u64)],
    ) -> Result<Vec<Bytes>> {
        let cfg = ctx.config();
        // Fast path: feature off → verbatim per-range reads. This keeps
        // behaviour bit-identical to the pre-B2 baseline whenever the
        // opt-in flag is not set.
        if !cfg.range_coalesce_enabled {
            let mut out: Vec<Bytes> = Vec::with_capacity(ranges.len());
            for &(off, len) in ranges {
                if len == 0 {
                    out.push(Bytes::new());
                    continue;
                }
                let bytes = Self::read_range_with_context(ctx.clone(), path, off, len).await?;
                out.push(bytes);
            }
            return Ok(out);
        }

        // Enabled path: plan → issue merged fetches → splice back.
        let plan = crate::io::range_coalesce::plan(
            ranges,
            cfg.range_coalesce_gap_bytes,
            cfg.range_coalesce_max_bytes,
        );
        debug!(
            path = %path,
            input_count = ranges.len(),
            output_count = plan.fetches.len(),
            input_bytes = plan.total_input_bytes,
            fetch_bytes = plan.total_fetch_bytes,
            wasted_bytes = plan.wasted_bytes(),
            "range coalesce plan (§B2)"
        );

        // Issue one `read_range` per merged fetch. We do them sequentially
        // to preserve error-order semantics (first failing fetch wins);
        // downstream callers already treat the whole batch as atomic.
        let mut fetch_bufs: Vec<Bytes> = Vec::with_capacity(plan.fetches.len());
        for f in &plan.fetches {
            let bytes = Self::read_range_with_context(ctx.clone(), path, f.offset, f.len).await?;
            // Defensive: the reader should return exactly `f.len` bytes.
            // If a short read slips through, fail loudly — the byte-
            // equivalence contract of `read_ranges` cannot be honoured.
            if bytes.len() as u64 != f.len {
                return Err(Error::BlockIoError {
                    message: format!(
                        "read_ranges: merged fetch (offset={}, len={}) returned {} bytes",
                        f.offset,
                        f.len,
                        bytes.len()
                    ),
                });
            }
            fetch_bufs.push(bytes);
        }

        // Splice each caller-visible slice out of its assigned fetch.
        // `Bytes::slice` is O(1) — it just clones the ref-count and
        // narrows the view — so this loop allocates nothing.
        Ok(Self::splice_from_plan(&plan, &fetch_bufs))
    }

    /// Pure splice layer for [`Self::read_ranges_with_context`]:
    /// given the coalesce plan and the concrete `Bytes` returned for
    /// each merged fetch, produce one `Bytes` per input range in the
    /// caller's original order.
    ///
    /// Extracted so this behaviour is trivially unit-testable offline
    /// (no `FileSystemContext` / master required). Every splice is an
    /// `O(1)` `Bytes::slice` — no allocation, no copy.
    ///
    /// # Preconditions
    ///
    /// - `fetch_bufs.len() == plan.fetches.len()`
    /// - `fetch_bufs[i].len() as u64 == plan.fetches[i].len` for all `i`
    ///
    /// The caller (`read_ranges_with_context`) enforces both by
    /// construction; violations panic here (they would indicate a
    /// transport-layer contract violation).
    fn splice_from_plan(
        plan: &crate::io::range_coalesce::CoalescePlan,
        fetch_bufs: &[Bytes],
    ) -> Vec<Bytes> {
        debug_assert_eq!(fetch_bufs.len(), plan.fetches.len());
        let mut out: Vec<Bytes> = Vec::with_capacity(plan.slices.len());
        for s in &plan.slices {
            if s.fetch_index == crate::io::range_coalesce::NO_FETCH {
                out.push(Bytes::new());
                continue;
            }
            let src = &fetch_bufs[s.fetch_index];
            let end = s.offset_in_fetch + s.len;
            out.push(src.slice(s.offset_in_fetch..end));
        }
        out
    }

    // ── Accessors ────────────────────────────────────────────────

    /// Get the file path being read.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the file info.
    pub fn file_info(&self) -> &FileInfo {
        &self.file_info
    }

    /// Get the total file length (from metadata).
    pub fn file_length(&self) -> u64 {
        self.file_info.length.unwrap_or(0) as u64
    }

    /// Get the total bytes read so far.
    pub fn bytes_read(&self) -> u64 {
        self.total_bytes_read
    }

    /// Get the number of block segments in the read plan.
    pub fn block_count(&self) -> usize {
        self.plans.len()
    }

    /// Get the index of the next block to be read.
    pub fn current_block_index(&self) -> usize {
        self.current_plan_index
    }

    /// Whether all blocks have been read.
    pub fn is_complete(&self) -> bool {
        self.current_plan_index >= self.plans.len()
    }

    /// Get the requested read offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the requested read length.
    pub fn length(&self) -> u64 {
        self.length
    }
}

/// Bridges the reader's stateless worker/UFS range read to the cache layer's
/// miss source, so [`crate::cache::read_through_cache`] can drive page fills
/// from within [`GoosefsFileReader::read_next_block`].
///
/// `read_file_range` is stateless (`&self`), so being re-entered here does not
/// perturb the outer `read_next_block` iteration state (`plans` /
/// `current_plan_index` / `offset` / `length`).
#[async_trait::async_trait]
impl ExternalRangeReader for GoosefsFileReader {
    async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes> {
        self.read_file_range(offset, end).await
    }
}

// ── Unit tests (pure logic — no I/O) ─────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::grpc::WorkerNetAddress;

    /// Build a cache-disabled reader over a synthetic file for pure-logic tests.
    fn make_reader(length: i64, block_size: i64) -> GoosefsFileReader {
        let num_blocks = if block_size > 0 {
            (length + block_size - 1) / block_size
        } else {
            0
        };
        let block_ids: Vec<i64> = (1001..(1001 + num_blocks.max(0))).collect();
        let file_info = FileInfo {
            length: Some(length),
            block_size_bytes: Some(block_size),
            block_ids,
            completed: Some(true),
            folder: Some(false),
            ufs_path: Some(String::new()),
            ..Default::default()
        };
        let config = GoosefsConfig::new("127.0.0.1:9200");
        GoosefsFileReader::build(
            &config,
            "/synthetic",
            Arc::new(file_info),
            WorkerRouterView::empty(),
            None,
            None,
            0,
            length as u64,
        )
        .expect("build reader")
    }

    /// `block_logical_size` returns the full block size for interior blocks, the
    /// remainder for the trailing block, and clamps to `0` past EOF.
    #[test]
    fn test_block_logical_size() {
        let bs = 64 * 1024 * 1024i64;
        // 2.5 blocks: 0 and 1 are full; block 2 is a half-block remainder.
        let len = 2 * bs + bs / 2;
        let reader = make_reader(len, bs);

        assert_eq!(reader.block_logical_size(0), bs);
        assert_eq!(reader.block_logical_size(1), bs);
        assert_eq!(reader.block_logical_size(2), bs / 2);
        // Past EOF clamps to 0 (never negative).
        assert_eq!(reader.block_logical_size(99), 0);
    }

    /// A freshly-built reader (no context) has caching and short-circuit off,
    /// so its read path is byte-for-byte the legacy worker-direct path.
    #[test]
    fn test_build_defaults_disable_cache_and_sc() {
        let reader = make_reader(1024, 1024);
        assert!(reader.cache.is_none(), "cache must default to disabled");
        assert!(!reader.cache_fill, "fill must default to false");
        assert!(
            reader.short_circuit.is_none(),
            "short-circuit must default to disabled"
        );
    }

    /// HR-1: missing / non-positive `file_id` must keep the page cache off.
    /// (`attach_cache` uses [`page_cache_eligible`]; synthetic readers start
    /// with `file_id = None` → treated as 0.)
    #[test]
    fn hr1_non_positive_file_id_is_not_page_cache_eligible() {
        use crate::cache::page_cache_eligible;

        let reader = make_reader(1024, 1024);
        // Synthetic FileInfo defaults leave file_id unset → treated as 0.
        assert_eq!(reader.file_info.file_id, None);
        assert!(!page_cache_eligible(reader.file_info.file_id.unwrap_or(0)));
        assert!(reader.cache.is_none());

        assert!(!page_cache_eligible(0));
        assert!(!page_cache_eligible(-7));
        assert!(page_cache_eligible(42));
    }

    /// `worker_addr` formats `host:rpc_port` and defaults sensibly.
    #[test]
    fn test_worker_addr_formatting() {
        let wi = WorkerInfo {
            address: Some(WorkerNetAddress {
                host: Some("10.0.0.5".to_string()),
                rpc_port: Some(9207),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            GoosefsFileReader::worker_addr(&wi).unwrap(),
            "10.0.0.5:9207"
        );

        // Missing host/port fall back to the documented defaults.
        let wi_defaults = WorkerInfo {
            address: Some(WorkerNetAddress::default()),
            ..Default::default()
        };
        assert_eq!(
            GoosefsFileReader::worker_addr(&wi_defaults).unwrap(),
            "127.0.0.1:9203"
        );
    }

    /// A `WorkerInfo` with no address surfaces an internal error rather than
    /// panicking.
    #[test]
    fn test_worker_addr_missing_address_errors() {
        let wi = WorkerInfo {
            address: None,
            ..Default::default()
        };
        assert!(GoosefsFileReader::worker_addr(&wi).is_err());
    }

    // ── B2: read_ranges splice layer ──────────────────────────────

    /// The splice layer must return one `Bytes` per input range, in the
    /// caller's original order, byte-identical to a hypothetical
    /// standalone `read_range` for that input.
    #[test]
    fn splice_from_plan_reconstructs_caller_order_and_bytes() {
        use crate::io::range_coalesce::plan;

        // Inputs deliberately out of order to exercise sort + reorder.
        let inputs = &[(200u64, 10u64), (0, 20), (10, 5), (100, 4)];
        // gap = 100 → merges (0,20) and (10,5) into [0,25); (100,4) sits
        // alone (gap to nearest = 75 ≤ 100 → also merges into [0, 104]);
        // (200,10) is 96 bytes past the end → also merges. Use a small
        // cap to force a split so we cover both single- and
        // multi-fetch splice paths.
        let p = plan(inputs, 100, 60);
        assert!(!p.fetches.is_empty());

        // Simulate storage: byte(off) = (off & 0xff) as u8.
        let synth = |off: u64, len: u64| -> Bytes {
            let v: Vec<u8> = (0..len).map(|i| ((off + i) & 0xff) as u8).collect();
            Bytes::from(v)
        };
        let fetch_bufs: Vec<Bytes> = p.fetches.iter().map(|f| synth(f.offset, f.len)).collect();

        let out = GoosefsFileReader::splice_from_plan(&p, &fetch_bufs);
        assert_eq!(out.len(), inputs.len(), "output length must match input");
        for (i, &(off, len)) in inputs.iter().enumerate() {
            let expected = synth(off, len);
            assert_eq!(
                out[i].as_ref(),
                expected.as_ref(),
                "output[{i}] byte mismatch for input ({off},{len})"
            );
        }
    }

    /// Empty input ranges must produce empty `Bytes` without triggering
    /// a fetch, and must NOT shift the fetch indices of the real ranges.
    #[test]
    fn splice_from_plan_preserves_empty_ranges_at_original_position() {
        use crate::io::range_coalesce::plan;

        // (100, 0) is empty, (0, 10) is real, (7, 0) is empty, (50, 5) real.
        let inputs = &[(100u64, 0u64), (0, 10), (7, 0), (50, 5)];
        let p = plan(inputs, 0, 4096);
        // Two real ranges → two disjoint fetches (gap=0 disallows merge).
        assert_eq!(p.fetches.len(), 2);

        // Synth: byte(off) = (off & 0x7f)
        let synth = |off: u64, len: u64| -> Bytes {
            Bytes::from(
                (0..len)
                    .map(|i| ((off + i) & 0x7f) as u8)
                    .collect::<Vec<_>>(),
            )
        };
        let fetch_bufs: Vec<Bytes> = p.fetches.iter().map(|f| synth(f.offset, f.len)).collect();

        let out = GoosefsFileReader::splice_from_plan(&p, &fetch_bufs);
        assert_eq!(out.len(), 4);
        assert!(out[0].is_empty(), "empty input must yield empty Bytes");
        assert_eq!(out[1].len(), 10);
        assert!(out[2].is_empty(), "empty input must yield empty Bytes");
        assert_eq!(out[3].len(), 5);
    }

    /// Config default must keep `range_coalesce_enabled = false`, i.e.
    /// the `read_ranges` fast path serves each input verbatim
    /// (behaviour identical to a caller-side loop). This test guards
    /// against an accidental default flip.
    #[test]
    fn range_coalesce_disabled_by_default() {
        let cfg = GoosefsConfig::default();
        assert!(
            !cfg.range_coalesce_enabled,
            "range_coalesce_enabled must default to false (opt-in per §B2)"
        );
        assert_eq!(cfg.range_coalesce_gap_bytes, 64 * 1024);
        assert_eq!(cfg.range_coalesce_max_bytes, 4 * 1024 * 1024);
    }
}
