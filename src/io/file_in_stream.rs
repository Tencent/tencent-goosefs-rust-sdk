//! Dual-path seekable file input stream.
//!
//! [`GoosefsFileInStream`] provides both sequential reads and random reads
//! (`read_at` / `seek + read`) over a Goosefs file.  It mirrors the Java
//! client's `GoosefsFileInStream` and Go SDK's `GoosefsFileInStream`.
//!
//! # Two read paths
//!
//! ```text
//! Sequential read (read / next-block)
//!   → block_in_stream   (GrpcBlockReader, streaming, prefetch)
//!
//! Random read  (read_at / large seek)
//!   → positioned_read   (GrpcBlockReader::positioned_read, position_short=true)
//!     cached per block in cached_positioned_block_id
//! ```
//!
//! The stream switches automatically between paths based on
//! `TRANSFER_POSITIONED_READ_THRESHOLD` (8 KiB):
//!
//! | Condition                                    | Path used          |
//! |----------------------------------------------|--------------------|
//! | Sequential / small forward seek (< 8 KiB)   | `block_in_stream`  |
//! | Large seek or backward seek (≥ 8 KiB)        | `positioned_read`  |
//! | `read_at()` call                             | `positioned_read`  |
//!
//! # Java authority
//!
//! Ported from `alluxio.client.file.GoosefsFileInStream` (Java) and verified
//! against `client/fs/file_in_stream.go` (Go SDK).
//!
//! Key constants match Go SDK:
//! - `TRANSFER_POSITIONED_READ_THRESHOLD = 8 * 1024` bytes
//! - `MAX_PREFETCH_WINDOW = 8` chunks
//!
//! # Concurrency
//!
//! `GoosefsFileInStream` is NOT `Sync` — it requires exclusive (`&mut self`)
//! access for all reads and seeks.  Random reads via `read_at` also use
//! `&mut self` to allow updating the per-block cache.
//!
//! This matches the Java client's single-threaded contract.  Callers that
//! need concurrent random reads should create multiple streams.

use std::io::SeekFrom;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tracing::{debug, warn};

use crate::block::router::{rpc_endpoint, WorkerRouterView};
use crate::block::short_circuit::{ShortCircuitError, ShortCircuitFactory};
use crate::cache::{page_cache_eligible, CacheManager, ExternalRangeReader};
use crate::client::{WorkerClient, WorkerClientPool, WorkerManagerClient};
use crate::config::GoosefsConfig;
use crate::context::FileSystemContext;
use crate::error::{Error, Result};
use crate::fs::options::InStreamOptions;
use crate::fs::uri_status::URIStatus;
use crate::io::reader::{GrpcBlockReader, ReadTuning};
use crate::proto::proto::dataserver::OpenUfsBlockOptions;

/// Threshold in bytes above which a seek switches from the sequential
/// `block_in_stream` path to the `positioned_read` path.
///
/// Value `8 * 1024` matches the Go SDK's `transferPositionedReadThreshold`.
pub const TRANSFER_POSITIONED_READ_THRESHOLD: i64 = 8 * 1024;

/// Maximum adaptive-prefetch window (in chunks).
#[allow(dead_code)]
const MAX_PREFETCH_WINDOW: i32 = 8;

/// Seekable, dual-path file input stream for a Goosefs file.
///
/// # Usage
///
/// ```rust,no_run
/// use goosefs_sdk::io::GoosefsFileInStream;
/// use goosefs_sdk::config::GoosefsConfig;
/// use goosefs_sdk::fs::options::OpenFileOptions;
///
/// # async fn example() -> goosefs_sdk::error::Result<()> {
/// let config = GoosefsConfig::new("127.0.0.1:9200");
/// let opts = OpenFileOptions::default();
/// let mut stream = GoosefsFileInStream::open(&config, "/data/file.parquet", opts).await?;
///
/// // Sequential read
/// let mut buf = vec![0u8; 4096];
/// let n = stream.read(&mut buf).await?;
///
/// // Random read (positioned)
/// let chunk = stream.read_at(1024 * 1024, 4096).await?;
/// # Ok(())
/// # }
/// ```
pub struct GoosefsFileInStream {
    // ── File metadata ────────────────────────────────────────────────────────
    /// Immutable file status (block map, length, etc.).
    status: URIStatus,
    /// Goosefs config (chunk_size, etc.).
    config: GoosefsConfig,
    /// Read options for this stream.
    options: InStreamOptions,

    // ── Position tracking ─────────────────────────────────────────────────────
    /// Internal absolute byte position — points at the byte that the SDK's
    /// chunk reader will deliver next. May be ahead of the user-visible
    /// position when bytes have been pulled from the worker but not yet
    /// consumed by the caller (parked in `carry_over`).
    pos: i64,
    /// Total file length (cached from `status.length`).
    file_length: i64,

    // ── Carry-over buffer (chunk → small read adapter) ───────────────────────
    /// Bytes already pulled from the SDK chunk reader but not yet copied
    /// into the caller's `read()` buffer. Drained from index 0 on the
    /// next `read()` call; cleared on every `seek` / `seek_from`.
    ///
    /// Workers deliver one variable-size chunk per gRPC frame (up to
    /// `chunk_size`, default 1 MiB). When the caller's buffer is smaller
    /// than the chunk, the excess lives here so subsequent reads continue
    /// where the previous one left off — matching the
    /// `std::io::Read` / `tokio::io::AsyncRead` contract that short reads
    /// must never lose bytes.
    carry_over: BytesMut,

    // ── Sequential read path ─────────────────────────────────────────────────
    /// Active sequential block stream, if any.
    ///
    /// `Some` when reading sequentially; `None` between blocks.
    block_in_stream: Option<GrpcBlockReader>,
    /// Block ID of the currently-open sequential stream.
    block_in_stream_block_id: i64,

    // ── Positioned (random) read path ─────────────────────────────────────────
    /// Block ID of the currently-cached positioned-read stream.
    ///
    /// `-1` = no cached stream.  Reserved for future per-block cache.
    #[allow(dead_code)]
    cached_positioned_block_id: i64,

    // ── Worker routing ────────────────────────────────────────────────────────
    /// Worker router for block → worker selection.
    /// Worker router view for block → worker mapping.
    ///
    /// P0-D Step 2 (`docs/perf/2026-07-07-hotspot-optimizations/README.md`
    /// §3.4): migrated from `WorkerRouter` (per-stream `ArcSwap`×3) to
    /// `WorkerRouterView` (per-stream `Arc`×2 + `Option<i64>` value).
    /// The legacy `open()` path builds the view via
    /// [`WorkerRouterView::from_workers`] (in-line ring build,
    /// `local_worker_id = None`); the context path uses
    /// [`WorkerRouterView::from_shared`] (wait-free `Arc::clone`).
    router: WorkerRouterView,

    // ── Shared connection pool (optional) ─────────────────────────────────────
    /// Worker connection pool shared across all streams in the same context.
    ///
    /// `Some` when constructed via `open_with_context()`, `None` in legacy mode
    /// (`open()`).  When `Some`, `connect_worker` reuses pooled connections
    /// instead of creating a new one per block.
    worker_pool: Option<Arc<WorkerClientPool>>,

    // ── Client local page cache (random-read path) ───────────────────────────
    /// Shared local page cache, when enabled. `None` disables caching for this
    /// stream (legacy `open()` always `None`).
    cache: Option<Arc<dyn CacheManager>>,
    /// Page size in bytes used to split random reads into cache pages.
    cache_page_size: u64,
    /// Stable file identifier (server inode id rendered as text) used as the
    /// cache key namespace. Same file → same id → cross-stream hits.
    cache_file_id: Arc<str>,
    /// Whether to write missed pages back into the cache.
    cache_fill: bool,
    /// Whether back-fills use the bounded async write-back pool (`true`) or
    /// block inline until cached (`false`).
    cache_async_write: bool,
    /// Whether **sequential** reads (`read`) are routed through the page cache.
    /// Random reads (`read_at`) always use the cache when present; sequential
    /// reads default to the native streaming path to avoid read amplification.
    cache_sequential_read: bool,

    // ── Short-circuit (local mmap) read path ─────────────────────────────────
    /// Short-circuit factory, when SC is enabled **and** the stream was built
    /// in context mode (it needs the shared `WorkerClientPool` + `WorkerRouterView`).
    ///
    /// `None` disables SC for this stream (legacy `open()`, or SC kill switch
    /// off). When `Some`, both the positioned-read path (`read_external_range`)
    /// and the sequential `read()` path first attempt a local mmap read and
    /// transparently fall back to gRPC on any recoverable failure (INV-S1).
    /// See `docs/SHORT_CIRCUIT_DESIGN.md` §4.3.
    short_circuit: Option<Arc<ShortCircuitFactory>>,

    /// Block id currently being served sequentially via the short-circuit
    /// (local mmap) path, or `-1`. Lets consecutive `read()` calls within the
    /// same block reuse the SC decision (and the cached reader) without
    /// re-running `should_use` each chunk. Reset to `-1` when the block falls
    /// back to gRPC.
    sc_seq_block: i64,
}

impl GoosefsFileInStream {
    // ── Construction ────────────────────────────────────────────────────────

    /// Open a `GoosefsFileInStream` for the file at `path`.
    ///
    /// # Errors
    ///
    /// - [`Error::FileIncomplete`] if the file is in `INCOMPLETE` state
    ///   (another writer has not yet called `close()`).
    /// - [`Error::OpenDirectory`] if `path` refers to a directory.
    /// - [`Error::NotFound`] if the path does not exist.
    pub async fn open(
        config: &GoosefsConfig,
        path: &str,
        options: crate::fs::options::OpenFileOptions,
    ) -> Result<Self> {
        use crate::client::MasterClient;

        config
            .validate()
            .map_err(|e| Error::ConfigError { message: e })?;

        let master = MasterClient::connect(config).await?;
        let file_info = master.get_status(path).await?;

        let status = URIStatus::from_proto(file_info);

        // Reject INCOMPLETE non-folder files
        if status.is_folder() {
            return Err(Error::OpenDirectory {
                path: path.to_string(),
            });
        }
        if !status.is_completed() {
            return Err(Error::FileIncomplete {
                message: format!("{path} is incomplete"),
            });
        }

        // Discover workers
        let inquire_client = master.inquire_client().clone();
        let wm = WorkerManagerClient::connect_with_inquire(config, inquire_client).await?;
        let workers = wm.get_worker_info_list().await?;
        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers available for reading".to_string(),
            });
        }

        // Legacy path: build a view directly from the worker list. This is
        // O(N · virtual_nodes) once here, matching the pre-Step-2 cost of
        // `WorkerRouter::new() + update_workers(workers).await`. The view
        // deliberately captures `local_worker_id = None` on this path
        // (see `WorkerRouterView::from_workers` doc): the legacy `open()`
        // has no probed shared router to inherit from, and running
        // `detect_local_worker` here would drag the `hostname::get()`
        // syscall onto the caller. Local-first is a context-path
        // optimisation only — pinned by
        // `test_view_from_workers_no_local_first_when_not_probed`.
        let router =
            WorkerRouterView::from_workers(workers, WorkerRouterView::default_failure_ttl());

        let file_length = status.length;

        debug!(
            path = %path,
            file_length = file_length,
            block_count = status.block_ids.len(),
            "GoosefsFileInStream opened"
        );

        Ok(Self {
            file_length,
            status,
            config: config.clone(),
            options: options.in_stream_options,
            pos: 0,
            carry_over: BytesMut::new(),
            block_in_stream: None,
            block_in_stream_block_id: -1,
            cached_positioned_block_id: -1,
            router,
            worker_pool: None, // legacy mode: no shared pool
            cache: None,       // legacy mode: no page cache
            cache_page_size: config.client_cache_page_size,
            cache_file_id: Arc::from(String::new()),
            cache_fill: false,
            cache_async_write: false,
            cache_sequential_read: false,
            // SC needs the shared pool + router from a FileSystemContext; the
            // legacy `open()` path has neither, so SC is disabled here.
            short_circuit: None,
            sc_seq_block: -1,
        })
    }

    /// Open a `GoosefsFileInStream` using a shared [`FileSystemContext`].
    ///
    /// # Connection sharing
    ///
    /// This is the recommended constructor in production.  It:
    /// - Reuses the context's persistent `MasterClient` (zero extra TCP)
    /// - Reuses the context's `WorkerRouterView` (via `from_shared`)
    /// - Uses the context's `WorkerClientPool` so block reads reuse connections
    ///
    /// # Errors
    ///
    /// Same as [`GoosefsFileInStream::open`].
    pub async fn open_with_context(
        ctx: Arc<FileSystemContext>,
        path: &str,
        options: crate::fs::options::OpenFileOptions,
    ) -> Result<Self> {
        let config = ctx.config().clone();
        config
            .validate()
            .map_err(|e| Error::ConfigError { message: e })?;

        // Reuse persistent Master connection — no network I/O.
        //
        // **A3** (`docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md`): consult the opt-in
        // FileInfo metadata cache first. On hit, skip the RPC entirely; on
        // miss, populate the cache after a successful `get_status`. Cache is
        // `None` unless the caller has opted in via `with_file_info_cache_ttl`.
        let file_info_cache = ctx.acquire_file_info_cache();
        let file_info = if let Some(cached) = file_info_cache.as_ref().and_then(|c| c.get(path)) {
            debug!(path = %path, "FileInfo cache hit (§A3 + S3)");
            // S3: `cached` is `Arc<FileInfo>`; `from_proto` needs owned
            // `FileInfo`, so one clone is unavoidable here (it moves
            // the fields into URIStatus). The win is that the cache
            // itself no longer deep-copies on every hit — only the
            // `from_proto` conversion does.
            (*cached).clone()
        } else {
            let master = ctx.acquire_master();
            let fetched = master.get_status(path).await?;
            // S3: insert_arc shares the Arc with the cache (atomic inc),
            // avoiding the old `fetched.clone()` for the cache. The
            // caller still needs owned `FileInfo` for `from_proto`, so
            // we clone once here — but that's one clone instead of two.
            let arc_fetched = Arc::new(fetched);
            if let Some(cache) = &file_info_cache {
                cache.insert_arc(path, Arc::clone(&arc_fetched));
            }
            // Clone out of the Arc for `from_proto` (which moves).
            (*arc_fetched).clone()
        };
        let status = URIStatus::from_proto(file_info);

        // Reject INCOMPLETE non-folder files
        if status.is_folder() {
            return Err(Error::OpenDirectory {
                path: path.to_string(),
            });
        }
        if !status.is_completed() {
            return Err(Error::FileIncomplete {
                message: format!("{path} is incomplete"),
            });
        }

        // Reuse shared router — already populated and TTL-refreshed.
        // A1 (`docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md`): clone the workers +
        // hash_ring `Arc`s wait-free instead of rebuilding the ring. Failure
        // isolation is preserved via the new router's own `failed_workers`
        // DashMap.
        let shared_router = ctx.acquire_router();
        let router = WorkerRouterView::from_shared(&shared_router);

        let file_length = status.length;
        let worker_pool = ctx.acquire_worker_pool();

        // Reuse the context-shared short-circuit factory (P8): all streams from
        // this context share one hot-block reader LRU, so a hot local block is
        // opened/mmap'd once and reused across streams. `None` when the SC kill
        // switch is off.
        let short_circuit = ctx.acquire_short_circuit();

        // Inject the shared page cache (best-effort; `None` when disabled).
        //
        // HR-1 (design §9.2): see [`page_cache_eligible`]. This guard MUST run
        // before `on_file_open` below, otherwise a "0"-keyed open would pollute
        // the version table for the next id-less file.
        let cache = if page_cache_eligible(status.file_id) {
            ctx.acquire_cache_manager()
        } else {
            None
        };
        let cache_file_id: Arc<str> = Arc::from(status.file_id.to_string());
        // Only back-fill the local page cache when the read allows caching.
        // A `NoCache` read still serves cache *hits* (free speedup) but must
        // not pollute the cache with one-off / large-scan pages.
        let cache_fill = cache.is_some()
            && options.in_stream_options.read_type != crate::fs::options::ReadType::NoCache;

        // Notify the cache that this file was (re)opened. If the same file_id
        // is now backing different content (overwrite — length or mtime
        // changed), the cache invalidates its stale pages so this stream never
        // serves stale data. Best-effort and cheap when nothing changed.
        if let Some(cache) = &cache {
            cache
                .on_file_open(
                    &cache_file_id,
                    status.length,
                    status.last_modification_time_ms,
                )
                .await;
        }

        debug!(
            path = %path,
            file_length = file_length,
            block_count = status.block_ids.len(),
            cache_enabled = cache.is_some(),
            "GoosefsFileInStream opened (context mode)"
        );

        Ok(Self {
            file_length,
            status,
            config: config.clone(),
            options: options.in_stream_options,
            pos: 0,
            carry_over: BytesMut::new(),
            block_in_stream: None,
            block_in_stream_block_id: -1,
            cached_positioned_block_id: -1,
            router,
            worker_pool: Some(worker_pool),
            cache,
            cache_page_size: config.client_cache_page_size,
            cache_file_id,
            cache_fill,
            cache_async_write: config.client_cache_async_write_enabled,
            cache_sequential_read: config.client_cache_sequential_read_enabled,
            short_circuit,
            sc_seq_block: -1,
        })
    }

    // ── Position ─────────────────────────────────────────────────────────────

    /// Current user-visible byte position within the file.
    ///
    /// Equal to `internal_pos - carry_over.len()`, where `internal_pos`
    /// is the next byte the SDK chunk reader would deliver. The two
    /// only diverge between a `read()` that pulled an oversized chunk
    /// and the subsequent `read()` that drains the leftover.
    pub fn pos(&self) -> i64 {
        self.pos - self.carry_over.len() as i64
    }

    /// File length in bytes.
    pub fn len(&self) -> i64 {
        self.file_length
    }

    /// `true` if the file has zero bytes.
    pub fn is_empty(&self) -> bool {
        self.file_length == 0
    }

    /// `true` if the stream is at or past the end of the file from the
    /// caller's point of view.
    pub fn is_eof(&self) -> bool {
        self.carry_over.is_empty() && self.pos >= self.file_length
    }

    /// Returns the number of bytes remaining from the current user
    /// position to EOF (includes any bytes parked in `carry_over`).
    pub fn remaining(&self) -> i64 {
        let raw = (self.file_length - self.pos).max(0);
        raw + self.carry_over.len() as i64
    }

    // ── Seek ──────────────────────────────────────────────────────────────────

    /// Seek to an absolute byte position.
    ///
    /// # Design
    ///
    /// - If the seek distance from the current position is ≥
    ///   `TRANSFER_POSITIONED_READ_THRESHOLD`, the sequential `block_in_stream`
    ///   is dropped — the next `read()` will use the positioned-read path.
    /// - Small forward seeks (< threshold, same block) fast-path through the
    ///   existing sequential stream by discarding bytes up to the target.
    /// - Any non-zero seek invalidates `carry_over` because the chunk
    ///   reader is repositioned (or its leftover no longer matches the
    ///   new offset).
    ///
    /// Seeking past EOF clamps to `file_length`.
    pub async fn seek(&mut self, pos: i64) -> Result<i64> {
        let target = pos.clamp(0, self.file_length);
        let user_pos = self.pos();

        if target == user_pos {
            return Ok(user_pos);
        }

        // Drop any bytes parked in the carry-over buffer; the chunk
        // reader is about to be repositioned (or replaced).
        self.carry_over.clear();

        let seek_dist = (target - self.pos).abs();
        let same_block = self.block_index_for_pos(target) == self.block_index_for_pos(self.pos);

        if seek_dist < TRANSFER_POSITIONED_READ_THRESHOLD && same_block {
            // Small forward seek within the same block — skip bytes in the
            // existing sequential stream
            if self.block_in_stream.is_some() {
                if target > self.pos {
                    let skip = (target - self.pos) as usize;
                    self.skip_bytes(skip).await?;
                    // `skip_bytes` may have over-pulled a chunk and parked
                    // the trailing bytes (those beyond `target`) into
                    // `self.carry_over` so they will be delivered on the
                    // next read. Internal `self.pos` therefore advances by
                    // `skip + carry_over.len()` to preserve the invariant
                    //   `pos() == self.pos - self.carry_over.len() == target`.
                    self.pos = target + self.carry_over.len() as i64;
                    return Ok(target);
                } else {
                    // Backward seek within threshold but same block — still
                    // need to close and re-open (can't seek backwards in gRPC stream)
                    self.block_in_stream = None;
                    self.block_in_stream_block_id = -1;
                }
            }
        } else {
            // Large seek or cross-block seek — switch to positioned-read path
            self.block_in_stream = None;
            self.block_in_stream_block_id = -1;
        }

        self.pos = target;
        Ok(self.pos)
    }

    /// Seek using `std::io::SeekFrom` semantics.
    ///
    /// `SeekFrom::Current(n)` is resolved against the **user-visible**
    /// position (i.e. [`Self::pos`]), not the internal chunk-reader
    /// position, so that a `Current(0)` is always a no-op regardless of
    /// whether `carry_over` holds parked bytes.
    pub async fn seek_from(&mut self, seek_from: SeekFrom) -> Result<i64> {
        let target = match seek_from {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => self.file_length + n,
            SeekFrom::Current(n) => self.pos() + n,
        };
        self.seek(target).await
    }

    // ── Sequential read ───────────────────────────────────────────────────────

    /// Read up to `buf.len()` bytes from the current position into `buf`.
    ///
    /// Returns the number of bytes read.  Returns `0` at EOF.
    ///
    /// # Design
    ///
    /// If the sequential `block_in_stream` is available for the current block,
    /// reads from it.  Otherwise opens a new sequential stream.
    ///
    /// Falls back to positioned read when the stream would need to skip more
    /// than `TRANSFER_POSITIONED_READ_THRESHOLD` bytes (handled in `seek`).
    ///
    /// # Authentication failure recovery
    ///
    /// If opening a block reader fails with `AuthenticationFailed`, the stale
    /// connection is invalidated and a fresh authenticated connection is used
    /// for one retry.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // 1) Drain any bytes parked from a previous oversized chunk first.
        if !self.carry_over.is_empty() {
            let take = self.carry_over.len().min(buf.len());
            buf[..take].copy_from_slice(&self.carry_over[..take]);
            let _ = self.carry_over.split_to(take);
            return Ok(take);
        }

        // 2) Already past EOF on the chunk-reader side and no leftover —
        //    nothing more to read.
        if self.pos >= self.file_length {
            return Ok(0);
        }

        // 2.5) Cache-enabled path: optionally serve sequential reads through
        //      the page cache too (mirrors Java `LocalCacheFileInStream`, which
        //      routes all reads through the cache).
        //
        //      Gated behind `client_cache_sequential_read_enabled` (default
        //      off): routing a large sequential scan through fixed-size pages
        //      turns one streamed request into many per-page positioned reads
        //      (read amplification), and a `NoCache` sequential read would
        //      re-fetch a whole page for every small buffer with no caching
        //      benefit. When disabled, sequential reads use the native
        //      streaming path below while random `read_at` still uses the cache.
        //
        //      `carry_over` is guaranteed empty here: step 1 above returns
        //      early whenever it holds parked bytes, so this branch always
        //      copies exactly what the caller asked for and never bypasses
        //      buffered data.
        if self.cache.is_some() && self.cache_sequential_read {
            debug_assert!(
                self.carry_over.is_empty(),
                "carry_over must be drained before the sequential cache path"
            );
            let end = (self.pos + buf.len() as i64).min(self.file_length);
            let data = self.read_at_cached(self.pos, end).await?;
            let n = data.len().min(buf.len());
            buf[..n].copy_from_slice(&data[..n]);
            self.pos += n as i64;
            return Ok(n);
        }

        let block_idx = self.block_index_for_pos(self.pos);
        let block_id = self.block_id_at(block_idx)?;

        // Short-circuit sequential fast path: serve the read directly from the
        // local block mmap (zero-copy slice), bypassing the gRPC stream. SC is
        // stateless/positioned so no carry-over / stream state is involved.
        //
        // We attempt SC when: (a) we are already SC-serving this block
        // (`sc_seq_block == block_id`, reuse the decision + cached reader), or
        // (b) we are at a fresh block boundary (`block_in_stream_block_id !=
        // block_id`) and `should_use` approves. We do NOT switch a block that
        // is mid-gRPC-stream. Any recoverable failure transparently falls back
        // to gRPC (INV-S1).
        if self.short_circuit.is_some() {
            let use_sc = self.sc_seq_block == block_id
                || (self.block_in_stream_block_id != block_id
                    && self.sc_should_use_seq(block_id, block_idx).await);
            if use_sc {
                if let Some(res) = self.sc_sequential_read(buf, block_idx, block_id).await {
                    return res;
                }
                // SC declined/failed for this block — fall through to gRPC.
            }
        }

        // Does the sequential stream match the current block?
        if self.block_in_stream_block_id != block_id {
            // Open a new sequential stream for this block
            let offset_in_block = self.offset_in_block(self.pos);
            let remaining_in_block = self.remaining_in_block(self.pos);

            let worker = self.connect_worker(block_id).await?;
            let worker_generation = worker.generation();
            let ufs_opts = self.build_ufs_opts(block_idx);
            let tuning = ReadTuning::from_config(&self.config);
            let reader_result = GrpcBlockReader::open_sequential(
                &worker,
                block_id,
                offset_in_block,
                remaining_in_block,
                self.config.chunk_size as i64,
                ufs_opts.clone(),
                tuning,
            )
            .await;

            let reader = match reader_result {
                Ok(r) => r,
                Err(e) if e.is_authentication_failed() => {
                    // SASL expired between connect_worker and open.  Use the
                    // single-flight reconnect path so concurrent readers
                    // hitting the same stale channel produce exactly one
                    // TCP+SASL handshake, not N.
                    //
                    // `debug!` (not `warn!`) because collapsed-herd events
                    // are expected and bounded.
                    debug!(
                        block_id = block_id,
                        stale_generation = worker_generation,
                        error = %e,
                        "auth failed on block reader open, requesting single-flight reconnect"
                    );
                    let fresh = self
                        .reconnect_worker_for_block(block_id, Some(worker_generation))
                        .await?;
                    GrpcBlockReader::open_sequential(
                        &fresh,
                        block_id,
                        offset_in_block,
                        remaining_in_block,
                        self.config.chunk_size as i64,
                        ufs_opts,
                        tuning,
                    )
                    .await?
                }
                Err(e) => return Err(e),
            };

            self.block_in_stream = Some(reader);
            self.block_in_stream_block_id = block_id;
        }

        // Read from the sequential stream. The helper itself advances
        // `self.pos` by the *chunk* size it pulled from the worker (not by
        // `n`) so that `self.pos` always reflects bytes consumed from the
        // chunk reader — including any overflow parked in `carry_over`.
        let n = self.read_from_sequential_stream(buf).await?;
        Ok(n)
    }

    /// Read exactly `n` bytes starting at `offset` without changing `self.pos`.
    ///
    /// # Positioned read path
    ///
    /// Always uses `GrpcBlockReader::positioned_read` with `position_short=true`.
    /// Each call opens a fresh gRPC stream for the target block range and
    /// closes it on return.
    ///
    /// Reads that span multiple blocks are handled by issuing one positioned
    /// read per block and concatenating the results.
    ///
    /// # Performance — drive concurrent random reads
    ///
    /// Each call opens (and closes) a fresh positioned-read stream, so a
    /// **single-task tight loop** of sequential `read_at(...).await` calls
    /// leaves only one op in flight and is bottlenecked by the per-op
    /// round-trip (measured ~2x slower than the steady-state floor; see
    /// `docs/RUST_PYTHON_SDK_OPTIMIZATION.md` Part V §V.5 "B1 verification result").
    ///
    /// For throughput-oriented random reads, issue reads **concurrently**
    /// (one future per `read_at`, e.g. `stream::iter(..).buffer_unordered(8..16)`)
    /// so multiple ops overlap and hide each round-trip. Raising
    /// [`GoosefsConfig::worker_connection_pool_size`](crate::config::GoosefsConfig)
    /// further lifts the single-connection ceiling. See Part IV of that doc
    /// for caller-side concurrency patterns.
    ///
    /// # Authentication failure recovery
    ///
    /// If a positioned read fails with `AuthenticationFailed`, the stale
    /// connection is invalidated and a fresh one is used for one retry.
    pub async fn read_at(&mut self, offset: i64, n: usize) -> Result<Bytes> {
        if offset >= self.file_length || n == 0 {
            return Ok(Bytes::new());
        }

        let end = (offset + n as i64).min(self.file_length);

        // Route through the local page cache when enabled; otherwise read
        // straight from the worker/UFS.
        if self.cache.is_some() {
            return self.read_at_cached(offset, end).await;
        }
        self.read_external_range(offset, end).await
    }

    /// Read the byte range `[offset, end)` directly from the worker/UFS,
    /// bypassing the local page cache.
    ///
    /// This is the original (cache-less) `read_at` implementation, factored
    /// out so the cached path can reuse it as the miss/back-fill source.
    async fn read_external_range(&mut self, offset: i64, end: i64) -> Result<Bytes> {
        // ── R2 fast path: the whole request lives inside a single block ──
        // Random reads (PR) almost always fall here (256 KiB / 1 MiB ≪ 64 MiB
        // block). Returning the `positioned_read` `Bytes` directly avoids the
        // extra `BytesMut::extend_from_slice` copy the multi-block merge path
        // below performs. The H2 short-read guard and auth single-flight
        // reconnect are preserved inside `positioned_read_with_retry`, so the
        // C2/C3 consistency invariants still hold.
        let first_block_idx = self.block_index_for_pos(offset);
        let first_block_end = self.block_start(first_block_idx) + self.status.block_size_bytes;
        if end <= first_block_end {
            let block_id = self.block_id_at(first_block_idx)?;
            let offset_in_block = self.offset_in_block(offset);
            let length = end - offset;

            // Try the short-circuit (local mmap) path first; it falls back
            // transparently to gRPC on any recoverable failure (INV-S1).
            if let Some(sc_result) = self
                .try_short_circuit_read(block_id, first_block_idx, offset_in_block, length)
                .await
            {
                return sc_result;
            }

            let data = self
                .positioned_read_with_retry(first_block_idx, block_id, offset_in_block, length)
                .await?;
            // Defensive: surface a zero-byte delivery for a non-zero request as
            // an error rather than a silent truncation (mirrors the slow path).
            if data.is_empty() {
                return Err(Error::Internal {
                    message: format!(
                        "read_at: positioned_read returned 0 bytes for block {} \
                         offset_in_block {} length {}",
                        block_id, offset_in_block, length
                    ),
                    source: None,
                });
            }
            return Ok(data);
        }

        // ── Slow path: the request spans multiple blocks — merge per-block
        //    positioned reads (kept identical to the pre-R2 behaviour). ──
        let mut result = BytesMut::with_capacity((end - offset) as usize);
        let mut cur = offset;

        while cur < end {
            let block_idx = self.block_index_for_pos(cur);
            let block_id = self.block_id_at(block_idx)?;
            let offset_in_block = self.offset_in_block(cur);
            let block_end = self.block_start(block_idx) + self.status.block_size_bytes;
            let read_end = end.min(block_end);
            let length = read_end - cur;

            // Short-circuit per block, with transparent gRPC fallback.
            let data = match self
                .try_short_circuit_read(block_id, block_idx, offset_in_block, length)
                .await
            {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Err(e),
                None => {
                    self.positioned_read_with_retry(block_idx, block_id, offset_in_block, length)
                        .await?
                }
            };

            // H2 fix: advance the cursor by the *actual* number of bytes
            // delivered, not by the requested `length`. `positioned_read` /
            // `read_all` may return fewer bytes if the server half-closes
            // mid-stream (`ChunkAction::Eof` arm); advancing by `length`
            // would silently skip the missing range and return mis-aligned
            // data to the caller. With `cur += data.len()`, the outer
            // `while cur < end` loop will naturally re-issue another
            // positioned read with the adjusted (offset, length) pair.
            //
            // Defensive: if the worker delivered zero bytes for a non-zero
            // request, treat it as a real EOF / corruption and surface an
            // error instead of looping forever.
            let advanced = data.len() as i64;
            result.extend_from_slice(&data);
            if advanced == 0 {
                return Err(Error::Internal {
                    message: format!(
                        "read_at: positioned_read returned 0 bytes for block {} \
                         offset_in_block {} length {} (cur={}, end={})",
                        block_id, offset_in_block, length, cur, end
                    ),
                    source: None,
                });
            }
            cur += advanced;
        }

        Ok(result.freeze())
    }

    /// Cached random-read path: serve `[offset, end)` page-by-page from the
    /// local cache, reading whole pages from the worker/UFS on a miss and
    /// (optionally) writing them back.
    ///
    /// The cache is **best-effort**: any cache failure degrades to an external
    /// read of the same range, so correctness never depends on the cache.
    /// The page-split loop itself lives in
    /// [`crate::cache::read_through_cache`] so it can be unit-tested offline.
    async fn read_at_cached(&mut self, offset: i64, end: i64) -> Result<Bytes> {
        // Clone the `Arc<dyn CacheManager>` so cache calls don't borrow `self`
        // (the external read below needs `&mut self`).
        let cache = match self.cache.clone() {
            Some(c) => c,
            None => return self.read_external_range(offset, end).await,
        };
        let page_size = self.cache_page_size;
        let file_id = self.cache_file_id.clone();
        let file_length = self.file_length;
        let fill_mode = self.cache_fill_mode();

        crate::cache::read_through_cache(
            &cache,
            self,
            &file_id,
            page_size,
            file_length,
            offset,
            end,
            fill_mode,
        )
        .await
    }

    /// Effective back-fill mode for this stream's cache.
    fn cache_fill_mode(&self) -> crate::cache::FillMode {
        if !self.cache_fill {
            crate::cache::FillMode::None
        } else if self.cache_async_write {
            crate::cache::FillMode::Async
        } else {
            crate::cache::FillMode::Sync
        }
    }

    /// Issue a single positioned read for `(block_id, offset_in_block, length)`
    /// with the auth single-flight reconnect retry.
    ///
    /// Shared by the R2 fast path and the multi-block slow path so both keep
    /// identical H2 short-read and auth-recovery semantics (C2/C3).
    async fn positioned_read_with_retry(
        &mut self,
        block_idx: usize,
        block_id: i64,
        offset_in_block: i64,
        length: i64,
    ) -> Result<Bytes> {
        let worker = self.connect_worker(block_id).await?;
        let worker_generation = worker.generation();
        let ufs_opts = self.build_ufs_opts(block_idx);

        let read_result = GrpcBlockReader::positioned_read(
            &worker,
            block_id,
            offset_in_block,
            length,
            self.config.chunk_size as i64,
            ufs_opts.clone(),
        )
        .await;

        match read_result {
            Ok(d) => Ok(d),
            Err(e) if e.is_authentication_failed() => {
                debug!(
                    block_id = block_id,
                    stale_generation = worker_generation,
                    error = %e,
                    "auth failed on positioned read, requesting single-flight reconnect"
                );
                let fresh = self
                    .reconnect_worker_for_block(block_id, Some(worker_generation))
                    .await?;
                GrpcBlockReader::positioned_read(
                    &fresh,
                    block_id,
                    offset_in_block,
                    length,
                    self.config.chunk_size as i64,
                    ufs_opts,
                )
                .await
            }
            Err(e) => Err(e),
        }
    }

    /// Logical (on-disk) byte size of the block at `block_idx`.
    ///
    /// Full blocks are `block_size_bytes`; the trailing block is the file
    /// remainder. This is what the Worker reports as the `OpenLocalBlock`
    /// response `block_size`, so it is the value passed to the short-circuit
    /// factory (request `block_size` + the SC decision's size threshold).
    fn block_logical_size(&self, block_idx: usize) -> i64 {
        let bs = self.status.block_size_bytes;
        if bs <= 0 {
            return self.file_length.max(0);
        }
        let start = self.block_start(block_idx);
        (self.file_length - start).clamp(0, bs)
    }

    /// Attempt a short-circuit (local mmap) read of `[offset_in_block,
    /// offset_in_block+length)` within `block_id`.
    ///
    /// Returns:
    /// - `Some(Ok(bytes))` — SC served the read (zero-copy).
    /// - `Some(Err(e))`   — a **semantic** error (e.g. `OutOfRange`) that must
    ///   be surfaced unchanged (INV-S4); the caller must NOT fall back.
    /// - `None`           — SC was not used, or hit a recoverable failure;
    ///   the caller transparently falls back to the gRPC path (INV-S1).
    ///
    /// capability is `None` here (design §3.1 P3 item — the read path has no
    /// capability fetcher yet). On capability-enabled clusters the
    /// `OpenLocalBlock` RPC is rejected and this returns `None`, so the read
    /// still completes over gRPC with identical bytes.
    async fn try_short_circuit_read(
        &self,
        block_id: i64,
        block_idx: usize,
        offset_in_block: i64,
        length: i64,
    ) -> Option<Result<Bytes>> {
        // §B1: SC decision histogram — count SKIPPED when the factory itself
        // is absent so hit_rate = HIT / (HIT + SKIPPED + FALLBACK_*) has a
        // stable denominator across `short_circuit_enabled` toggles.
        let factory = match self.short_circuit.as_ref() {
            Some(f) => f,
            None => {
                crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_SKIPPED).inc(1);
                return None;
            }
        };
        let block_size = self.block_logical_size(block_idx);

        if !factory.should_use(block_id, block_size).await {
            crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_SKIPPED).inc(1);
            return None;
        }

        let reader = match factory.get_or_open(block_id, block_size).await {
            Ok(r) => r,
            Err(e) => {
                // `get_or_open` already negative-caches recoverable failures.
                // A semantic error cannot arise from open; treat everything as
                // a transparent fallback to gRPC.
                debug!(
                    block_id = block_id,
                    error = %e,
                    "short-circuit open failed, falling back to gRPC"
                );
                crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_FALLBACK_OPEN)
                    .inc(1);
                return None;
            }
        };

        match reader.read_bytes(offset_in_block as usize, length as usize) {
            Ok(bytes) => {
                crate::metrics::counter(crate::metrics::name::CLIENT_SC_DECISION_HIT).inc(1);
                Some(Ok(bytes))
            }
            Err(ShortCircuitError::OutOfRange {
                off,
                len,
                file_size,
            }) => {
                // Semantic error — propagate (INV-S4). With ranges already
                // clamped to file_length this should not occur; if it does it
                // signals a real metadata/block-size inconsistency worth
                // surfacing rather than masking behind a fallback.
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
                // Any other (recoverable) read error: drop the cached reader
                // and fall back to gRPC for this read.
                debug!(
                    block_id = block_id,
                    error = %e,
                    "short-circuit read failed, falling back to gRPC"
                );
                factory.invalidate(block_id).await;
                None
            }
        }
    }

    // ── Convenience ───────────────────────────────────────────────────────────

    /// Whether the sequential read at the current block should use SC.
    async fn sc_should_use_seq(&self, block_id: i64, block_idx: usize) -> bool {
        match &self.short_circuit {
            Some(f) => {
                f.should_use(block_id, self.block_logical_size(block_idx))
                    .await
            }
            None => false,
        }
    }

    /// Serve a sequential read of the current block directly from the local
    /// mmap (short-circuit). Copies `min(buf.len(), bytes-left-in-block)` bytes
    /// from `offset_in_block`, advances `self.pos`, and returns the count.
    ///
    /// Returns:
    /// - `Some(Ok(n))` — SC served `n` bytes (a short read at the block
    ///   boundary is normal; the caller's loop continues into the next block).
    /// - `Some(Err(e))` — a semantic error (`OutOfRange`) to surface (INV-S4).
    /// - `None` — recoverable failure; the caller falls back to the gRPC
    ///   streaming path for this block (INV-S1).
    async fn sc_sequential_read(
        &mut self,
        buf: &mut [u8],
        block_idx: usize,
        block_id: i64,
    ) -> Option<Result<usize>> {
        // Clone the Arc so we can mutate `self` after the async open without a
        // borrow conflict.
        let factory = self.short_circuit.clone()?;
        let block_size = self.block_logical_size(block_idx);

        let reader = match factory.get_or_open(block_id, block_size).await {
            Ok(r) => r,
            Err(e) => {
                debug!(
                    block_id = block_id,
                    error = %e,
                    "short-circuit sequential open failed, falling back to gRPC"
                );
                self.sc_seq_block = -1;
                return None;
            }
        };

        let off = self.offset_in_block(self.pos) as usize;
        let remaining = self.remaining_in_block(self.pos).max(0) as usize;
        let n = remaining.min(buf.len());
        if n == 0 {
            self.sc_seq_block = -1;
            return Some(Ok(0));
        }

        match reader.read(off, n) {
            Ok(slice) => {
                buf[..n].copy_from_slice(slice);
                // SC is now the source for this block — drop any stale gRPC
                // stream and remember the decision for subsequent chunks.
                self.block_in_stream = None;
                self.block_in_stream_block_id = -1;
                self.sc_seq_block = block_id;
                self.pos += n as i64;
                Some(Ok(n))
            }
            Err(ShortCircuitError::OutOfRange {
                off,
                len,
                file_size,
            }) => Some(Err(Error::InvalidArgument {
                message: format!(
                    "short-circuit sequential read out of range on block {block_id}: \
                     off={off} len={len} block_size={file_size}"
                ),
            })),
            Err(e) => {
                debug!(
                    block_id = block_id,
                    error = %e,
                    "short-circuit sequential read failed, falling back to gRPC"
                );
                factory.invalidate(block_id).await;
                self.sc_seq_block = -1;
                None
            }
        }
    }

    /// Read all remaining bytes from the current position to EOF.
    ///
    /// Now that [`Self::read`] is loss-less (any chunk overflow is parked
    /// in `carry_over`), this is a straightforward read-loop with a
    /// chunk-sized scratch buffer.
    pub async fn read_all(&mut self) -> Result<Bytes> {
        let remaining = self.remaining() as usize;
        let mut buf = BytesMut::with_capacity(remaining);
        // Use a generous scratch buffer so each loop turn typically pulls
        // a whole worker chunk — but cap it at 64 KiB to keep stack /
        // peak-memory bounded.
        let scratch_cap = (self.config.chunk_size as usize).clamp(8 * 1024, 64 * 1024);
        let mut tmp = vec![0u8; scratch_cap];

        loop {
            let n = self.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
        }

        Ok(buf.freeze())
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// 0-based block index for an absolute byte offset.
    fn block_index_for_pos(&self, offset: i64) -> usize {
        if self.status.block_size_bytes <= 0 {
            return 0;
        }
        (offset / self.status.block_size_bytes) as usize
    }

    /// Byte offset of the start of block `idx`.
    fn block_start(&self, idx: usize) -> i64 {
        idx as i64 * self.status.block_size_bytes
    }

    /// Byte offset within a block for an absolute file offset.
    fn offset_in_block(&self, offset: i64) -> i64 {
        if self.status.block_size_bytes <= 0 {
            return offset;
        }
        offset % self.status.block_size_bytes
    }

    /// Number of bytes remaining from `offset` to the end of its block.
    fn remaining_in_block(&self, offset: i64) -> i64 {
        let block_idx = self.block_index_for_pos(offset);
        let block_end = self.block_start(block_idx) + self.status.block_size_bytes;
        block_end.min(self.file_length) - offset
    }

    /// Resolve the block ID at `block_idx`.
    fn block_id_at(&self, block_idx: usize) -> Result<i64> {
        // Prefer block_ids list (authoritative order)
        if let Some(&id) = self.status.block_ids.get(block_idx) {
            if id > 0 {
                return Ok(id);
            }
        }
        // Fallback: block_infos by index
        let id = self
            .status
            .block_infos()
            .values()
            .find(|fbi| {
                fbi.offset
                    .is_some_and(|off| off == self.block_start(block_idx))
            })
            .and_then(|fbi| fbi.block_info.as_ref())
            .and_then(|bi| bi.block_id)
            .unwrap_or(-1);

        if id > 0 {
            Ok(id)
        } else {
            Err(Error::Internal {
                message: format!("no valid block_id at index {block_idx}"),
                source: None,
            })
        }
    }

    /// Connect to the worker responsible for `block_id`, with one retry on failure.
    ///
    /// # Connection pooling
    ///
    /// If a `WorkerClientPool` is available (context mode), connections are
    /// reused from the pool instead of establishing a new TCP connection per block.
    ///
    /// # Authentication failure recovery
    ///
    /// When a cached connection's SASL stream has expired (e.g. after process
    /// fork or long idle), the pool returns a stale client whose RPCs will fail
    /// with `AuthenticationFailed`.  This method detects such failures during
    /// gRPC calls and triggers a `reconnect()` to drop the stale channel and
    /// establish a fresh authenticated connection.
    async fn connect_worker(&mut self, block_id: i64) -> Result<WorkerClient> {
        let worker_info = self.router.select_worker(block_id).await?;
        let addr = worker_info
            .address
            .as_ref()
            .ok_or_else(|| Error::Internal {
                message: "worker has no address".to_string(),
                source: None,
            })?;

        let worker_addr = rpc_endpoint(addr);

        // Use pool when available (context mode)
        let result = if let Some(pool) = &self.worker_pool {
            pool.acquire(&worker_addr).await
        } else {
            WorkerClient::connect(&worker_addr, &self.config).await
        };

        match result {
            Ok(w) => Ok(w),
            Err(e) => {
                if matches!(e, Error::AuthenticationFailed { .. }) {
                    // Authentication failure on `acquire` — no WorkerClient
                    // was produced so we have no generation to coalesce
                    // against.  Fall back to the unconditional reconnect,
                    // which still funnels through the per-address mutex
                    // inside the pool to dedupe concurrent callers.
                    debug!(
                        worker = %worker_addr,
                        error = %e,
                        "authentication failed on acquire, reconnecting with fresh credentials"
                    );
                    if let Some(pool) = &self.worker_pool {
                        return pool.reconnect(&worker_addr).await;
                    }
                    // No pool — just create a fresh connection
                    return WorkerClient::connect(&worker_addr, &self.config).await;
                }

                // Non-auth error: mark worker as failed and try another
                self.router.mark_failed(addr);
                if let Some(pool) = &self.worker_pool {
                    pool.invalidate(&worker_addr).await;
                }
                warn!(worker = %worker_addr, error = %e, "worker connect failed, retrying");

                // Retry with a different worker
                let retry_info = self.router.select_worker(block_id).await?;
                let retry_addr_info =
                    retry_info.address.as_ref().ok_or_else(|| Error::Internal {
                        message: "retry worker has no address".to_string(),
                        source: None,
                    })?;
                let retry_addr = rpc_endpoint(retry_addr_info);
                if let Some(pool) = &self.worker_pool {
                    pool.acquire(&retry_addr).await
                } else {
                    WorkerClient::connect(&retry_addr, &self.config).await
                }
            }
        }
    }

    /// Reconnect to the worker for `block_id` with fresh authentication.
    ///
    /// Used as a retry path when a block read fails with `AuthenticationFailed`.
    ///
    /// When `stale_generation` is `Some(gen)`, the pool performs a
    /// **single-flight reconnect**: concurrent callers that observed the same
    /// stale generation on this address share exactly one TCP+SASL handshake
    /// — all but the first receive the already-replaced client without doing
    /// any network I/O.  When `None`, an unconditional reconnect is issued
    /// (still per-address serialised).
    async fn reconnect_worker_for_block(
        &mut self,
        block_id: i64,
        stale_generation: Option<u64>,
    ) -> Result<WorkerClient> {
        let worker_info = self.router.select_worker(block_id).await?;
        let addr = worker_info
            .address
            .as_ref()
            .ok_or_else(|| Error::Internal {
                message: "worker has no address".to_string(),
                source: None,
            })?;

        let worker_addr = rpc_endpoint(addr);

        if let Some(pool) = &self.worker_pool {
            match stale_generation {
                Some(gen) => pool.reconnect_if_stale(&worker_addr, gen).await,
                None => pool.reconnect(&worker_addr).await,
            }
        } else {
            WorkerClient::connect(&worker_addr, &self.config).await
        }
    }

    /// Build `OpenUfsBlockOptions` for block at `block_idx`.
    fn build_ufs_opts(&self, block_idx: usize) -> Option<OpenUfsBlockOptions> {
        let ufs_path = self.status.ufs_path.as_str();
        if ufs_path.is_empty() {
            return None;
        }
        let block_size = self.status.block_size_bytes;
        let offset_in_file = block_idx as i64 * block_size;

        Some(OpenUfsBlockOptions {
            ufs_path: Some(ufs_path.to_string()),
            offset_in_file: Some(offset_in_file),
            block_size: Some(block_size),
            max_ufs_read_concurrency: Some(self.options.max_ufs_read_concurrency),
            mount_id: Some(self.status.mount_id),
            no_cache: Some(!self.status.cacheable),
            user: None,
            caller_type: None,
        })
    }

    /// Drain `skip` bytes from the current sequential stream.
    ///
    /// IMPORTANT: chunks come from the worker in fixed sizes (typically 1 MiB).
    /// When the next chunk is **larger than the remaining `skip`**, the bytes
    /// past `skip` MUST NOT be discarded — the stream's internal position has
    /// already advanced past them, and dropping them would cause the next
    /// `read()` to return data from a position **after** the user's seek
    /// target (silent data corruption / loss).
    ///
    /// We park those trailing bytes into `self.carry_over` so that a
    /// subsequent `read()` delivers them first, and the caller-visible
    /// position (`pos()`) stays anchored at the seek target.
    async fn skip_bytes(&mut self, mut skip: usize) -> Result<()> {
        let stream = match self.block_in_stream.as_mut() {
            Some(s) => s,
            None => return Ok(()),
        };
        while skip > 0 {
            match stream.read_chunk().await? {
                Some(data) => {
                    if data.len() > skip {
                        // Park the bytes beyond the skip target. Caller of
                        // `seek` clears `carry_over` first and bails into
                        // the slow path on backward seeks, so `carry_over`
                        // is empty when we get here.
                        self.carry_over.extend_from_slice(&data[skip..]);
                        skip = 0;
                    } else {
                        skip -= data.len();
                    }
                }
                None => break,
            }
        }
        Ok(())
    }

    /// Read one chunk from the existing sequential block stream and copy
    /// at most `buf.len()` bytes into `buf`. Any chunk overflow is parked
    /// in `self.carry_over` so it is delivered on the next `read()` call.
    async fn read_from_sequential_stream(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Pre-condition: caller guarantees `carry_over` is empty (drained
        // by `read()` before delegating here).
        debug_assert!(
            self.carry_over.is_empty(),
            "carry_over must be drained before pulling a fresh chunk"
        );

        let stream = match self.block_in_stream.as_mut() {
            Some(s) => s,
            None => return Ok(0),
        };

        match stream.read_chunk().await? {
            Some(data) => {
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);

                // Park any chunk overflow so the next `read()` can deliver
                // it without losing bytes — this is what gives us the
                // `std::io::Read`-style "short reads never lose data"
                // contract that callers expect.
                if data.len() > n {
                    self.carry_over.extend_from_slice(&data[n..]);
                    debug!(
                        chunk_len = data.len(),
                        copied = n,
                        carried = self.carry_over.len(),
                        "chunk larger than caller buffer — overflow parked in carry_over"
                    );
                }

                // Advance `self.pos` by the *full* chunk length — every byte
                // delivered by the chunk reader has been consumed from the
                // worker's perspective, even those parked in `carry_over`.
                // The caller-visible position (`pos()`) compensates by
                // subtracting `carry_over.len()`.
                self.pos += data.len() as i64;

                // If stream is complete after this chunk, drop it.
                if stream.is_complete() {
                    self.block_in_stream = None;
                    self.block_in_stream_block_id = -1;
                }
                Ok(n)
            }
            None => {
                // Block stream exhausted — move to next block on next call.
                self.block_in_stream = None;
                self.block_in_stream_block_id = -1;
                Ok(0)
            }
        }
    }
}

/// Bridges the stream's worker/UFS positioned-read path to the cache layer's
/// miss source, so [`crate::cache::read_through_cache`] can drive cache fills.
#[async_trait::async_trait]
impl ExternalRangeReader for GoosefsFileInStream {
    async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes> {
        self.read_external_range(offset, end).await
    }
}

// ── Unit tests (pure logic — no I/O) ─────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::uri_status::URIStatus;
    use crate::proto::grpc::file::FileInfo;

    fn make_status(length: i64, block_size: i64) -> URIStatus {
        let num_blocks = (length + block_size - 1) / block_size;
        let block_ids: Vec<i64> = (1001..(1001 + num_blocks)).collect();
        let fi = FileInfo {
            length: Some(length),
            block_size_bytes: Some(block_size),
            block_ids: block_ids.clone(),
            completed: Some(true),
            folder: Some(false),
            ufs_path: Some(String::new()), // no UFS
            ..Default::default()
        };
        URIStatus::from_proto(fi)
    }

    fn make_stream(status: URIStatus) -> GoosefsFileInStream {
        let config = crate::config::GoosefsConfig::new("127.0.0.1:9200");
        let file_length = status.length;
        GoosefsFileInStream {
            file_length,
            status,
            config,
            options: InStreamOptions::default(),
            pos: 0,
            carry_over: BytesMut::new(),
            block_in_stream: None,
            block_in_stream_block_id: -1,
            cached_positioned_block_id: -1,
            router: WorkerRouterView::empty(),
            worker_pool: None,
            cache: None,
            cache_page_size: 1024 * 1024,
            cache_file_id: Arc::from(String::new()),
            cache_fill: false,
            cache_async_write: false,
            cache_sequential_read: false,
            short_circuit: None,
            sc_seq_block: -1,
        }
    }

    #[test]
    fn test_block_index_calculation() {
        // Use a dummy struct to test the pure arithmetic
        // 3 blocks of 64 MiB each
        let bs = 64 * 1024 * 1024i64;
        let len = 3 * bs;
        let status = make_status(len, bs);

        let stream = make_stream(status);

        assert_eq!(stream.block_index_for_pos(0), 0);
        assert_eq!(stream.block_index_for_pos(bs - 1), 0);
        assert_eq!(stream.block_index_for_pos(bs), 1);
        assert_eq!(stream.block_index_for_pos(2 * bs), 2);
    }

    /// HR-1: `URIStatus.file_id <= 0` must not attach a page cache.
    #[test]
    fn hr1_non_positive_file_id_disables_page_cache_eligibility() {
        use crate::cache::page_cache_eligible;

        let mut status = make_status(1024, 1024);
        status.file_id = 0;
        assert!(!page_cache_eligible(status.file_id));
        let stream = make_stream(status);
        assert!(
            stream.cache.is_none(),
            "synthetic stream without open must not enable cache"
        );

        let mut status_neg = make_status(1024, 1024);
        status_neg.file_id = -1;
        assert!(!page_cache_eligible(status_neg.file_id));

        let mut status_ok = make_status(1024, 1024);
        status_ok.file_id = 1001;
        assert!(page_cache_eligible(status_ok.file_id));
    }

    #[test]
    fn test_offset_in_block() {
        let bs = 64 * 1024 * 1024i64;
        let status = make_status(2 * bs, bs);
        let stream = make_stream(status);

        assert_eq!(stream.offset_in_block(0), 0);
        assert_eq!(stream.offset_in_block(100), 100);
        assert_eq!(stream.offset_in_block(bs), 0);
        assert_eq!(stream.offset_in_block(bs + 42), 42);
    }

    #[test]
    fn test_remaining_in_block() {
        let bs = 64 * 1024 * 1024i64;
        let status = make_status(2 * bs, bs);
        let stream = make_stream(status);

        assert_eq!(stream.remaining_in_block(0), bs);
        assert_eq!(stream.remaining_in_block(bs - 100), 100);
        assert_eq!(stream.remaining_in_block(bs), bs);
    }

    #[test]
    fn test_block_id_at() {
        let bs = 64 * 1024 * 1024i64;
        let status = make_status(2 * bs, bs);
        let stream = make_stream(status);

        assert_eq!(stream.block_id_at(0).unwrap(), 1001);
        assert_eq!(stream.block_id_at(1).unwrap(), 1002);
        assert!(stream.block_id_at(99).is_err()); // out of range
    }

    #[test]
    fn test_is_eof() {
        let bs = 1024i64;
        let status = make_status(bs, bs);
        let mut stream = make_stream(status);

        assert!(!stream.is_eof());
        stream.pos = bs;
        assert!(stream.is_eof());
    }

    #[test]
    fn test_remaining() {
        let bs = 1024i64;
        let status = make_status(bs, bs);
        let mut stream = make_stream(status);

        assert_eq!(stream.remaining(), bs);
        stream.pos = 100;
        assert_eq!(stream.remaining(), bs - 100);
        stream.pos = bs;
        assert_eq!(stream.remaining(), 0);
    }

    /// `pos()` reports the user-visible position. With bytes parked in
    /// `carry_over`, the internal `pos` is ahead of the user view.
    #[test]
    fn test_pos_accounts_for_carry_over() {
        let bs = 1024i64;
        let status = make_status(bs, bs);
        let mut stream = make_stream(status);

        stream.pos = 200;
        stream.carry_over.extend_from_slice(&[0u8; 50]);
        // SDK has consumed 200 bytes from the worker, but only 150 have
        // been delivered to the caller — the other 50 sit in carry_over.
        assert_eq!(stream.pos(), 150);
        assert_eq!(stream.remaining(), bs - 150);
        assert!(!stream.is_eof());

        // Drain the carry-over and we're at the SDK position.
        stream.carry_over.clear();
        assert_eq!(stream.pos(), 200);
        assert_eq!(stream.remaining(), bs - 200);
    }

    /// EOF is only true when both the chunk reader is exhausted *and* the
    /// carry-over buffer is empty.
    #[test]
    fn test_is_eof_with_carry_over() {
        let bs = 1024i64;
        let status = make_status(bs, bs);
        let mut stream = make_stream(status);

        // SDK has read everything but caller hasn't consumed the tail yet.
        stream.pos = bs;
        stream.carry_over.extend_from_slice(&[7u8; 20]);
        assert!(!stream.is_eof(), "carry_over still has bytes — not EOF");

        stream.carry_over.clear();
        assert!(
            stream.is_eof(),
            "chunk reader done and carry_over drained — EOF"
        );
    }

    /// Verify that legacy mode sets worker_pool to None.
    #[test]
    fn test_legacy_mode_no_pool() {
        let bs = 1024i64;
        let status = make_status(bs, bs);
        let stream = make_stream(status);
        assert!(
            stream.worker_pool.is_none(),
            "legacy mode should have no pool"
        );
        assert!(
            stream.short_circuit.is_none(),
            "legacy mode should not enable short-circuit"
        );
    }

    /// `block_logical_size` returns the full block size for interior blocks and
    /// the file remainder for the trailing (partial) block — matching the
    /// Worker's `OpenLocalBlock` response `block_size`.
    #[test]
    fn test_block_logical_size() {
        let bs = 64 * 1024 * 1024i64;
        // 2.5 blocks: blocks 0,1 are full; block 2 is a half-block remainder.
        let len = 2 * bs + bs / 2;
        let status = make_status(len, bs);
        let stream = make_stream(status);

        assert_eq!(stream.block_logical_size(0), bs);
        assert_eq!(stream.block_logical_size(1), bs);
        assert_eq!(stream.block_logical_size(2), bs / 2);
        // Past EOF clamps to 0 (never negative).
        assert_eq!(stream.block_logical_size(99), 0);
    }
}
