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
//!       → WorkerRouter.select_worker()   — consistent hash routing
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
use tracing::{debug, warn};

use crate::block::mapper::{BlockMapper, BlockReadPlan};
use crate::block::router::WorkerRouter;
use crate::client::worker::WorkerClientPool;
use crate::client::WorkerClient;
use crate::config::GoosefsConfig;
use crate::context::FileSystemContext;
use crate::error::{Error, Result};
use crate::io::reader::GrpcBlockReader;
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
    file_info: FileInfo,
    /// Worker router for block → worker mapping.
    router: WorkerRouter,
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
}

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
        Self::build(
            &config,
            path,
            file_info,
            router,
            pool,
            Some(ctx),
            0,
            file_length,
        )
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
        Self::build(
            &config,
            path,
            file_info,
            router,
            pool,
            Some(ctx),
            offset,
            length,
        )
    }

    /// Internal: fetch file info via the shared Master, snapshot workers from
    /// the shared router — **no new RPC connections**.
    ///
    /// This is the context-aware analogue of [`Self::init`]. It mirrors the
    /// pattern used by `GoosefsFileWriter::create_with_context`: a local
    /// `WorkerRouter` is created and seeded from the shared router's current
    /// snapshot, so per-read failure marking stays local and does not pollute
    /// the long-lived context-level routing state.
    async fn init_with_context(
        ctx: &Arc<FileSystemContext>,
        path: &str,
    ) -> Result<(FileInfo, WorkerRouter)> {
        // 1. Reuse the shared Master client (zero handshake).
        let master = ctx.acquire_master();
        let file_info = master.get_status(path).await?;

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

        // 2. Snapshot workers from the shared router (kept fresh by the
        //    context's background worker-refresh task — no extra RPC here).
        let shared_router = ctx.acquire_router();
        let workers = (*shared_router.get_workers().await).clone();
        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers available for reading".to_string(),
            });
        }
        debug!(
            worker_count = workers.len(),
            "reusing worker list from context"
        );

        let router = WorkerRouter::new();
        router.update_workers(workers).await;

        Ok((file_info, router))
    }

    /// Internal: build the reader from file info and router.
    #[allow(clippy::too_many_arguments)]
    fn build(
        config: &GoosefsConfig,
        path: &str,
        file_info: FileInfo,
        router: WorkerRouter,
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
        })
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
        loop {
            if self.current_plan_index >= self.plans.len() {
                return Ok(None);
            }

            let plan = &self.plans[self.current_plan_index];

            // Resolve the block ID — prefer FileBlockInfo if available
            let block_id = self.resolve_block_id(plan);
            if block_id <= 0 {
                warn!(
                    block_index = plan.block_index,
                    plan_block_id = plan.block_id,
                    "invalid block ID, skipping block"
                );
                self.current_plan_index += 1;
                continue; // loop instead of recursion
            }

            // Select worker for this block
            let worker_info = self.router.select_worker(block_id).await?;
            let addr = worker_info
                .address
                .as_ref()
                .ok_or_else(|| Error::Internal {
                    message: "worker has no address".to_string(),
                    source: None,
                })?;

            let worker_addr = format!(
                "{}:{}",
                addr.host.as_deref().unwrap_or("127.0.0.1"),
                addr.rpc_port.unwrap_or(9203)
            );

            debug!(
                block_id = block_id,
                block_index = plan.block_index,
                offset_in_block = plan.offset_in_block,
                length = plan.length,
                worker = %worker_addr,
                "reading block"
            );

            // Connect to the worker (with one retry on a different worker).
            let worker = match self.acquire_worker(&worker_addr).await {
                Ok(w) => w,
                Err(e) => {
                    if e.is_authentication_failed() {
                        // Auth failure on connect: reconnect with fresh credentials.
                        // `acquire` returned a fresh-but-stale (gen=0) client; use
                        // the unconditional `reconnect()` path.
                        debug!(
                            worker = %worker_addr,
                            error = %e,
                            "authentication failed on connect, reconnecting"
                        );
                        self.reconnect_worker(&worker_addr, None).await?
                    } else {
                        // Mark worker as failed for future routing
                        if let Some(w_addr) = worker_info.address.as_ref() {
                            self.router.mark_failed(w_addr);
                        }
                        warn!(
                            worker = %worker_addr,
                            error = %e,
                            "worker connection failed, trying another worker"
                        );

                        // Retry: select a different worker and try once more
                        match self.router.select_worker(block_id).await {
                            Ok(retry_worker_info) => {
                                let retry_addr_info = retry_worker_info
                                    .address
                                    .as_ref()
                                    .ok_or_else(|| Error::Internal {
                                        message: "retry worker has no address".to_string(),
                                        source: None,
                                    })?;
                                let retry_worker_addr = format!(
                                    "{}:{}",
                                    retry_addr_info.host.as_deref().unwrap_or("127.0.0.1"),
                                    retry_addr_info.rpc_port.unwrap_or(9203)
                                );
                                debug!(retry_worker = %retry_worker_addr, "retrying with different worker");
                                self.acquire_worker(&retry_worker_addr).await?
                            }
                            Err(_) => return Err(e),
                        }
                    }
                }
            };

            // Build OpenUfsBlockOptions when the block may reside in UFS.
            let ufs_options = self.build_ufs_read_options(plan);

            // Remember the generation of the client we are about to use so we
            // can request a **single-flight** reconnect if this particular
            // connection turns out to be stale.  Any concurrent reader that
            // observes the same failure will pass the same generation and
            // the pool will collapse them into one handshake.
            let worker_generation = worker.generation();

            // Open block reader — with auth-failure retry
            match self
                .try_read_block(&worker, block_id, plan, ufs_options.clone())
                .await
            {
                Ok(data) => {
                    let bytes_read = data.len() as u64;
                    self.total_bytes_read += bytes_read;
                    self.current_plan_index += 1;

                    debug!(
                        block_id = block_id,
                        bytes_read = bytes_read,
                        total_read = self.total_bytes_read,
                        "block read complete"
                    );

                    return Ok(Some(data));
                }
                Err(e) if e.is_authentication_failed() => {
                    // The RPC itself hit auth failure (SASL stream expired
                    // between connect and read_block).  Single-flight
                    // reconnect against the *observed* generation: if another
                    // concurrent reader already refreshed this channel, the
                    // pool returns the new client without a second handshake.
                    //
                    // Intentionally logged at `debug` level — under the
                    // single-flight policy these events are expected and
                    // strictly bounded (≤1 real reconnect per channel
                    // generation).  Elevating to `warn` here used to produce
                    // hundreds of duplicate lines per SASL expiry.
                    debug!(
                        block_id = block_id,
                        worker = %worker_addr,
                        stale_generation = worker_generation,
                        error = %e,
                        "auth failed during block read, requesting single-flight reconnect"
                    );
                    let fresh_worker = self
                        .reconnect_worker(&worker_addr, Some(worker_generation))
                        .await?;

                    let mut block_reader = GrpcBlockReader::open(
                        &fresh_worker,
                        block_id,
                        plan.offset_in_block as i64,
                        plan.length as i64,
                        self.config.chunk_size as i64,
                        ufs_options,
                    )
                    .await?;

                    let data = block_reader.read_all().await?;
                    let bytes_read = data.len() as u64;
                    self.total_bytes_read += bytes_read;
                    self.current_plan_index += 1;

                    debug!(
                        block_id = block_id,
                        bytes_read = bytes_read,
                        total_read = self.total_bytes_read,
                        "block read complete (after auth reconnect)"
                    );

                    return Ok(Some(data));
                }
                Err(e) => return Err(e),
            }
        }
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
    fn build_ufs_read_options(&self, plan: &BlockReadPlan) -> Option<OpenUfsBlockOptions> {
        let ufs_path = self.file_info.ufs_path.as_ref()?;
        if ufs_path.is_empty() {
            return None;
        }

        let block_size = self.file_info.block_size_bytes.unwrap_or(64 * 1024 * 1024);

        // The offset in the file where this block starts
        let offset_in_file = plan.block_index as i64 * block_size;

        Some(OpenUfsBlockOptions {
            ufs_path: Some(ufs_path.clone()),
            offset_in_file: Some(offset_in_file),
            block_size: Some(block_size),
            max_ufs_read_concurrency: None,
            mount_id: self.file_info.mount_id,
            no_cache: Some(!self.file_info.cacheable.unwrap_or(true)),
            user: None,
            caller_type: None,
        })
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
