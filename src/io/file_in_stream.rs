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

use crate::block::router::WorkerRouter;
use crate::client::{WorkerClient, WorkerClientPool, WorkerManagerClient};
use crate::config::GoosefsConfig;
use crate::context::FileSystemContext;
use crate::error::{Error, Result};
use crate::fs::options::InStreamOptions;
use crate::fs::uri_status::URIStatus;
use crate::io::reader::GrpcBlockReader;
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
    router: WorkerRouter,

    // ── Shared connection pool (optional) ─────────────────────────────────────
    /// Worker connection pool shared across all streams in the same context.
    ///
    /// `Some` when constructed via `open_with_context()`, `None` in legacy mode
    /// (`open()`).  When `Some`, `connect_worker` reuses pooled connections
    /// instead of creating a new one per block.
    worker_pool: Option<Arc<WorkerClientPool>>,
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

        let router = WorkerRouter::new();
        router.update_workers(workers).await;

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
        })
    }

    /// Open a `GoosefsFileInStream` using a shared [`FileSystemContext`].
    ///
    /// # Connection sharing
    ///
    /// This is the recommended constructor in production.  It:
    /// - Reuses the context's persistent `MasterClient` (zero extra TCP)
    /// - Reuses the context's `WorkerRouter` (already populated)
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
        let master = ctx.acquire_master();
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

        // Reuse shared router — already populated and TTL-refreshed.
        let shared_router = ctx.acquire_router();
        let router = WorkerRouter::with_ttls(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(30),
        );
        // Sync initial worker snapshot from the shared router.
        let workers = shared_router.get_workers().await;
        router.update_workers((*workers).clone()).await;

        let file_length = status.length;
        let worker_pool = ctx.acquire_worker_pool();

        debug!(
            path = %path,
            file_length = file_length,
            block_count = status.block_ids.len(),
            "GoosefsFileInStream opened (context mode)"
        );

        Ok(Self {
            file_length,
            status,
            config,
            options: options.in_stream_options,
            pos: 0,
            carry_over: BytesMut::new(),
            block_in_stream: None,
            block_in_stream_block_id: -1,
            cached_positioned_block_id: -1,
            router,
            worker_pool: Some(worker_pool),
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
            if let Some(ref mut stream) = self.block_in_stream {
                if target > self.pos {
                    let skip = (target - self.pos) as usize;
                    Self::skip_bytes(stream, skip).await?;
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

        let block_idx = self.block_index_for_pos(self.pos);
        let block_id = self.block_id_at(block_idx)?;

        // Does the sequential stream match the current block?
        if self.block_in_stream_block_id != block_id {
            // Open a new sequential stream for this block
            let offset_in_block = self.offset_in_block(self.pos);
            let remaining_in_block = self.remaining_in_block(self.pos);

            let worker = self.connect_worker(block_id).await?;
            let worker_generation = worker.generation();
            let ufs_opts = self.build_ufs_opts(block_idx);
            let reader_result = GrpcBlockReader::open(
                &worker,
                block_id,
                offset_in_block,
                remaining_in_block,
                self.config.chunk_size as i64,
                ufs_opts.clone(),
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
                    GrpcBlockReader::open(
                        &fresh,
                        block_id,
                        offset_in_block,
                        remaining_in_block,
                        self.config.chunk_size as i64,
                        ufs_opts,
                    )
                    .await?
                }
                Err(e) => return Err(e),
            };

            self.block_in_stream = Some(reader);
            self.block_in_stream_block_id = block_id;
        }

        // Read from the sequential stream
        let n = self.read_from_sequential_stream(buf).await?;
        if n > 0 {
            self.pos += n as i64;
        }
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
    /// # Authentication failure recovery
    ///
    /// If a positioned read fails with `AuthenticationFailed`, the stale
    /// connection is invalidated and a fresh one is used for one retry.
    pub async fn read_at(&mut self, offset: i64, n: usize) -> Result<Bytes> {
        if offset >= self.file_length || n == 0 {
            return Ok(Bytes::new());
        }

        let end = (offset + n as i64).min(self.file_length);
        let mut result = BytesMut::with_capacity((end - offset) as usize);
        let mut cur = offset;

        while cur < end {
            let block_idx = self.block_index_for_pos(cur);
            let block_id = self.block_id_at(block_idx)?;
            let offset_in_block = self.offset_in_block(cur);
            let block_end = self.block_start(block_idx) + self.status.block_size_bytes;
            let read_end = end.min(block_end);
            let length = read_end - cur;

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

            let data = match read_result {
                Ok(d) => d,
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
                    .await?
                }
                Err(e) => return Err(e),
            };

            result.extend_from_slice(&data);
            cur += length;
        }

        Ok(result.freeze())
    }

    // ── Convenience ───────────────────────────────────────────────────────────

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

        let worker_addr = format!(
            "{}:{}",
            addr.host.as_deref().unwrap_or("127.0.0.1"),
            addr.rpc_port.unwrap_or(9203)
        );

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
                let retry_addr = format!(
                    "{}:{}",
                    retry_addr_info.host.as_deref().unwrap_or("127.0.0.1"),
                    retry_addr_info.rpc_port.unwrap_or(9203)
                );
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

        let worker_addr = format!(
            "{}:{}",
            addr.host.as_deref().unwrap_or("127.0.0.1"),
            addr.rpc_port.unwrap_or(9203)
        );

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
    async fn skip_bytes(stream: &mut GrpcBlockReader, mut skip: usize) -> Result<()> {
        while skip > 0 {
            let chunk = stream.read_chunk().await?;
            match chunk {
                Some(data) => {
                    let consumed = data.len().min(skip);
                    skip -= consumed;
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
            router: WorkerRouter::new(),
            worker_pool: None,
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
        assert!(stream.is_eof(), "chunk reader done and carry_over drained — EOF");
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
    }
}
