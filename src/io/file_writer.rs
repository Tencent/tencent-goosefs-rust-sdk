//! High-level file writer that orchestrates the complete write pipeline.
//!
//! `GooseFsFileWriter` ties together all low-level components into a single
//! easy-to-use API, analogous to Java's `GooseFSFileOutStream`:
//!
//! ```text
//! GooseFsFileWriter::create(path, data)
//!   → MasterClient.create_file()
//!   → BlockMapper.plan_write()
//!   → for each block:
//!       → WorkerRouter.select_worker()
//!       → WorkerClient.connect()
//!       → GrpcBlockWriter.open() → write_all() → flush() → close()
//!   → MasterClient.complete_file()
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use goosefs_sdk::io::GooseFsFileWriter;
//! use goosefs_sdk::config::GooseFsConfig;
//!
//! # async fn example() -> goosefs_sdk::error::Result<()> {
//! let config = GooseFsConfig::new("127.0.0.1:9200");
//! let data = b"Hello, GooseFS!";
//!
//! // One-shot write
//! GooseFsFileWriter::write_file(&config, "/my-file.txt", data).await?;
//!
//! // Or use the builder for more control
//! let mut writer = GooseFsFileWriter::create(&config, "/my-file.txt").await?;
//! writer.write(data).await?;
//! writer.close().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::block::router::WorkerRouter;
use crate::client::master::default_file_mode;
use crate::client::worker::{WorkerClientPool, WriteBlockOptions};
use crate::client::{MasterClient, WorkerManagerClient};
use crate::config::GooseFsConfig;
use crate::error::{Error, Result};
use crate::io::writer::GrpcBlockWriter;
use crate::proto::grpc::block::RequestType;
use crate::proto::grpc::file::{CreateFilePOptions, FileInfo};
use crate::proto::proto::dataserver::CreateUfsFileOptions;

/// Write strategy derived from the effective `WritePType`.
///
/// Unlike the old single-stream design, CACHE_THROUGH must drive **two
/// independent streams in parallel** (matching Java `GooseFSFileOutStream`):
/// - a cache stream, sliced by block boundaries (`RequestType::GoosefsBlock`);
/// - a UFS stream, a single long-lived stream for the entire file
///   (`RequestType::UfsFile`, `block_id = -1`, `length = i64::MAX`).
///
/// Using `RequestType::UfsFile` with per-block RPCs (the old buggy behavior)
/// makes the Worker call `ufs.createNonexistingFile(path)` for every new block,
/// which truncates-and-rewrites the UFS file so only the last block survives.
#[derive(Clone, Debug)]
struct WriteStrategy {
    /// Open a per-block cache stream (`RequestType::GoosefsBlock`).
    cache_stream: bool,
    /// Open a single long-lived UFS stream (`RequestType::UfsFile`,
    /// `block_id = -1`, `length = i64::MAX`).
    ufs_stream: bool,
    /// UFS file creation options — used on the UFS stream's initial command.
    create_ufs_file_options: Option<CreateUfsFileOptions>,
    /// Whether `close()` should call `schedule_async_persistence` (ASYNC_THROUGH).
    need_async_persist: bool,
}

/// Derive the write strategy from `write_type` (i32 enum value) and the
/// `FileInfo` returned by `CreateFile`.
///
/// | write_type             | cache_stream | ufs_stream | async_persist |
/// |------------------------|:------------:|:----------:|:-------------:|
/// | MUST_CACHE (1)         | yes          | no         | no            |
/// | TRY_CACHE  (2)         | yes          | no         | no            |
/// | **CACHE_THROUGH (3)**  | **yes**      | **yes**    | **no**        |
/// | THROUGH (4)            | no           | yes        | no            |
/// | ASYNC_THROUGH (5)      | yes          | no         | yes           |
/// | NONE / unset           | yes          | no         | no            |
fn resolve_write_strategy(write_type: Option<i32>, file_info: &FileInfo) -> WriteStrategy {
    let build_ufs_opts = || CreateUfsFileOptions {
        ufs_path: file_info.ufs_path.clone(),
        owner: file_info.owner.clone(),
        group: file_info.group.clone(),
        mode: file_info.mode,
        mount_id: file_info.mount_id,
        acl: None,
    };
    match write_type {
        // CACHE_THROUGH: dual stream (cache blocks + single UFS stream)
        Some(3) => WriteStrategy {
            cache_stream: true,
            ufs_stream: true,
            create_ufs_file_options: Some(build_ufs_opts()),
            need_async_persist: false,
        },
        // THROUGH: UFS only
        Some(4) => WriteStrategy {
            cache_stream: false,
            ufs_stream: true,
            create_ufs_file_options: Some(build_ufs_opts()),
            need_async_persist: false,
        },
        // ASYNC_THROUGH: cache only, schedule async persist after close
        Some(5) => WriteStrategy {
            cache_stream: true,
            ufs_stream: false,
            create_ufs_file_options: None,
            need_async_persist: true,
        },
        // MUST_CACHE (1), TRY_CACHE (2), NONE (6), unset: cache only
        _ => WriteStrategy {
            cache_stream: true,
            ufs_stream: false,
            create_ufs_file_options: None,
            need_async_persist: false,
        },
    }
}

/// High-level file writer that orchestrates the full GooseFS write pipeline.
///
/// This struct encapsulates the complete write flow:
/// 1. `CreateFile` on Master to register the new file
/// 2. Discover workers and set up routing
/// 3. Split data into blocks via `BlockMapper`
/// 4. Write each block to a worker via `GrpcBlockWriter`
/// 5. `CompleteFile` on Master to finalize
pub struct GooseFsFileWriter {
    /// The GooseFS config.
    config: GooseFsConfig,
    /// The file path being written.
    path: String,
    /// Master client for metadata operations.
    master: MasterClient,
    /// Worker router for block → worker mapping (with failed-worker exclusion).
    router: WorkerRouter,
    /// Connection pool for reusing authenticated worker gRPC channels.
    /// Matches Java's `FileSystemContext.acquireBlockWorkerClient()`.
    worker_pool: Arc<WorkerClientPool>,
    /// File info returned by CreateFile.
    file_info: FileInfo,
    /// Total bytes written so far across all blocks (committed only).
    total_bytes_written: u64,
    /// Whether the file has been completed (closed) or cancelled.
    completed: bool,
    /// Whether the file write has been cancelled.
    cancelled: bool,
    /// Write strategy derived from config.write_type + FileInfo.
    write_strategy: WriteStrategy,
    /// Block IDs that have been successfully committed to workers.
    /// Used for cancel/rollback — matches Java's `mPreviousCommittedBlockIds`.
    committed_block_ids: Vec<i64>,
    /// Current in-progress block writer (chunk-level streaming).
    /// Data is streamed chunk-by-chunk as it arrives, matching Java's
    /// `BlockOutStream` + `DataWriter.writeChunk()` pattern.
    current_block_writer: Option<ActiveBlockWriter>,
    /// Single long-lived UFS stream used by `CACHE_THROUGH` / `THROUGH` modes.
    ///
    /// Matches Java `UnderFileSystemFileOutStream`: the entire file is written
    /// to the UFS as **one** continuous `WriteBlock(UFS_FILE)` stream with
    /// `block_id = -1` and `space_to_reserve = i64::MAX`. The Worker calls
    /// `createNonexistingFile` exactly once (on the first chunk) and then
    /// appends every subsequent chunk to the same `OutputStream`.
    ///
    /// Opened lazily on the first `write()` that needs UFS persistence.
    ufs_stream: Option<GrpcBlockWriter>,
    /// Worker address hosting the UFS stream (for failure tracking).
    ufs_worker_addr: Option<String>,
}

impl GooseFsFileWriter {
    /// Create a new file and prepare for writing.
    ///
    /// This calls `CreateFile` on the Master and discovers available workers.
    /// After creation, call `write()` to send data, then `close()` to finalize.
    pub async fn create(config: &GooseFsConfig, path: &str) -> Result<Self> {
        Self::create_with_options(config, path, None).await
    }

    /// Create a new file with custom options.
    ///
    /// # Arguments
    /// - `config` — GooseFS client configuration
    /// - `path` — File path in GooseFS namespace
    /// - `options` — Optional `CreateFilePOptions` (block size, write type, etc.)
    pub async fn create_with_options(
        config: &GooseFsConfig,
        path: &str,
        options: Option<CreateFilePOptions>,
    ) -> Result<Self> {
        config
            .validate()
            .map_err(|e| Error::ConfigError { message: e })?;

        // 1. Connect to Master (uses MasterInquireClient for HA support)
        let master = MasterClient::connect(config).await?;
        debug!(path = %path, "connected to Master for file creation");

        // 2. Create the file
        let create_options = options.unwrap_or_else(|| {
            let mut opts = CreateFilePOptions {
                block_size_bytes: Some(config.block_size as i64),
                // Default file mode: 0644 (rw-r--r--)
                mode: Some(default_file_mode()),
                // Automatically create parent directories (e.g. for Lance Dataset sub-dirs)
                recursive: Some(true),
                ..Default::default()
            };
            // Apply config-level write_type if set
            if config.write_type.is_some() {
                opts.write_type = config.write_type;
            }
            opts
        });

        // Ensure recursive is set so parent directories are created automatically
        let mut create_options = create_options;
        if create_options.recursive.is_none() {
            create_options.recursive = Some(true);
        }

        let file_info = master.create_file(path, create_options.clone()).await?;
        debug!(
            path = %path,
            file_id = ?file_info.file_id,
            block_size = ?file_info.block_size_bytes,
            "file created on Master"
        );

        // Derive the write strategy from the effective write_type + file info.
        // Priority: CreateFilePOptions.write_type > config.write_type > default (MUST_CACHE).
        let effective_write_type = create_options.write_type.or(config.write_type);
        let write_strategy = resolve_write_strategy(effective_write_type, &file_info);
        debug!(
            write_type = ?effective_write_type,
            cache_stream = write_strategy.cache_stream,
            ufs_stream = write_strategy.ufs_stream,
            need_async_persist = write_strategy.need_async_persist,
            "resolved write strategy"
        );

        // 3. Discover workers (shares inquire client via MasterClient)
        let inquire_client = master.inquire_client().clone();
        let wm = WorkerManagerClient::connect_with_inquire(config, inquire_client).await?;
        let workers = wm.get_worker_info_list().await?;
        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers available for writing".to_string(),
            });
        }
        debug!(worker_count = workers.len(), "discovered workers");

        let router = WorkerRouter::new();
        router.update_workers(workers).await;

        // Create connection pool for worker client reuse
        let worker_pool = WorkerClientPool::new_shared(config.clone());

        Ok(Self {
            config: config.clone(),
            path: path.to_string(),
            master,
            router,
            worker_pool,
            file_info,
            total_bytes_written: 0,
            completed: false,
            cancelled: false,
            write_strategy,
            committed_block_ids: Vec::new(),
            current_block_writer: None,
            ufs_stream: None,
            ufs_worker_addr: None,
        })
    }

    /// Write data to the file.
    ///
    /// Depending on the resolved [`WriteStrategy`], data is fanned out to one or
    /// both of the following streams — matching Java `GooseFSFileOutStream.writeInternal`:
    ///
    /// - **cache stream** (`cache_stream = true`): chunk-level streaming, sliced
    ///   by block boundaries. Matches Java's `BlockOutStream.write()` →
    ///   `updateCurrentChunk()` → `DataWriter.writeChunk()`.
    /// - **UFS stream** (`ufs_stream = true`): a single long-lived stream for
    ///   the entire file (`block_id = -1`, `length = i64::MAX`). Every chunk is
    ///   appended to the same `OutputStream` on the Worker. Opened lazily on
    ///   the first write that needs UFS persistence.
    ///
    /// Can be called multiple times for streaming writes.
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.completed || self.cancelled {
            return Err(Error::BlockIoError {
                message: "cannot write to a completed or cancelled file".to_string(),
            });
        }

        if data.is_empty() {
            return Ok(());
        }

        // 1) Feed the cache stream (sliced by block boundaries).
        if self.write_strategy.cache_stream {
            self.write_to_cache_stream(data).await?;
        }

        // 2) Feed the UFS stream (single long stream, no block boundaries —
        //    only sliced by chunk_size).
        if self.write_strategy.ufs_stream {
            self.write_to_ufs_stream(data).await?;
        }

        Ok(())
    }

    /// Append data to the per-block cache stream, slicing at block boundaries.
    async fn write_to_cache_stream(&mut self, data: &[u8]) -> Result<()> {
        let block_size = self
            .file_info
            .block_size_bytes
            .unwrap_or(self.config.block_size as i64) as u64;
        let chunk_size = self.config.chunk_size as usize;

        let mut offset = 0usize;
        while offset < data.len() {
            // Ensure we have an active block writer
            if self.current_block_writer.is_none()
                || self.current_block_writer.as_ref().unwrap().remaining() == 0
            {
                self.open_next_block(block_size).await?;
            }

            let writer = self.current_block_writer.as_mut().unwrap();
            let remaining_in_block = writer.remaining() as usize;
            let remaining_data = data.len() - offset;
            let to_write = std::cmp::min(remaining_in_block, remaining_data);

            // Stream data chunk-by-chunk (matching Java's chunk-level granularity)
            let end = offset + to_write;
            let mut chunk_offset = offset;
            while chunk_offset < end {
                let chunk_end = std::cmp::min(chunk_offset + chunk_size, end);
                let chunk = Bytes::copy_from_slice(&data[chunk_offset..chunk_end]);
                let chunk_len = chunk.len() as u64;

                match writer.writer.write_chunk(chunk).await {
                    Ok(()) => {
                        writer.bytes_written += chunk_len;
                    }
                    Err(e) => {
                        return self.handle_cache_write_exception(e).await;
                    }
                }
                chunk_offset = chunk_end;
            }

            offset = end;

            // If block is full, flush and close it
            if writer.remaining() == 0 {
                self.close_current_block().await?;
            }
        }

        Ok(())
    }

    /// Append data to the single long-lived UFS stream (`RequestType::UfsFile`,
    /// `block_id = -1`, `length = i64::MAX`). Opens the stream lazily on the
    /// first call.
    async fn write_to_ufs_stream(&mut self, data: &[u8]) -> Result<()> {
        if self.ufs_stream.is_none() {
            self.open_ufs_stream().await?;
        }
        let chunk_size = self.config.chunk_size as usize;
        let ufs = self
            .ufs_stream
            .as_mut()
            .expect("ufs_stream just opened above");

        let total = data.len();
        match ufs.write_all(data, chunk_size).await {
            Ok(()) => {
                // Track total UFS bytes written (for completeFile's ufsLength).
                self.total_bytes_written += total as u64;
                Ok(())
            }
            Err(e) => self.handle_ufs_write_exception(e).await,
        }
    }

    /// Open the next **cache** block writer.
    ///
    /// Matches Java's `GooseFSFileOutStream.getNextBlock()`:
    /// - Close the current block if any
    /// - Compute the next block ID
    /// - Select a worker via consistent hashing (excluding failed workers)
    /// - Open a new `GrpcBlockWriter` with `RequestType::GoosefsBlock`
    ///
    /// UFS persistence is handled by a separate long-lived stream
    /// (`open_ufs_stream`), not by this per-block RPC.
    async fn open_next_block(&mut self, block_size: u64) -> Result<()> {
        // Close current block if it exists
        if self.current_block_writer.is_some() {
            self.close_current_block().await?;
        }

        let file_id = self.file_info.file_id.unwrap_or(0);
        let block_index = self.committed_block_ids.len() as u64;
        let block_id = compute_block_id(file_id, block_index);

        // Select a worker for this block (failed workers are automatically excluded
        // by WorkerRouter's consistent hashing with failure tracking)
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
            block_index = block_index,
            worker = %worker_addr,
            "opening new cache block writer"
        );

        // Acquire worker client from connection pool (reuses existing channel)
        let worker = match self.worker_pool.acquire(&worker_addr).await {
            Ok(w) => w,
            Err(e) => {
                // Mark worker as failed for future exclusion
                self.router.mark_failed(addr);
                self.worker_pool.invalidate(&worker_addr).await;
                return Err(e);
            }
        };

        // Cache blocks always use GoosefsBlock — UFS persistence is on a
        // separate long-lived stream opened by `open_ufs_stream()`.
        let write_opts = WriteBlockOptions {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
        };

        // Open block writer with space reservation = block size
        let block_writer =
            match GrpcBlockWriter::open(&worker, block_id, block_size as i64, write_opts).await {
                Ok(w) => w,
                Err(e) => {
                    // Mark worker as failed on open failure
                    self.router.mark_failed(addr);
                    self.worker_pool.invalidate(&worker_addr).await;
                    return Err(e);
                }
            };

        self.current_block_writer = Some(ActiveBlockWriter {
            writer: block_writer,
            block_id,
            block_size,
            bytes_written: 0,
            worker_addr,
        });

        Ok(())
    }

    /// Close the current block writer: flush, close, and record the committed block ID.
    ///
    /// Matches Java's block close in `getNextBlock()` and `close()`.
    async fn close_current_block(&mut self) -> Result<()> {
        if let Some(active) = self.current_block_writer.take() {
            let block_id = active.block_id;
            let bytes_written = active.bytes_written;
            let mut writer = active.writer;

            if bytes_written > 0 {
                // Flush to ensure data is persisted on the worker
                let ack_offset = writer.flush().await?;
                debug!(
                    block_id = block_id,
                    ack_offset = ack_offset,
                    bytes_written = bytes_written,
                    "cache block flushed"
                );

                // Close the writer (triggers server-side commitBlock)
                writer.close().await?;

                // Track committed block for cancel/rollback
                self.committed_block_ids.push(block_id);
                // Only accumulate here when there is no UFS stream; otherwise the UFS
                // stream is the authoritative byte counter (see `write_to_ufs_stream`).
                if !self.write_strategy.ufs_stream {
                    self.total_bytes_written += bytes_written;
                }
            } else {
                // No data written, just cancel the empty block
                writer.cancel().await;
            }
        }
        Ok(())
    }

    /// Open the single long-lived UFS stream used by CACHE_THROUGH / THROUGH.
    ///
    /// Matches Java `UnderFileSystemFileOutStream`:
    /// - picks a worker at random (independent of cache routing);
    /// - opens one `WriteBlock` RPC with `block_id = -1`, `length = i64::MAX`,
    ///   `RequestType::UfsFile`, and the resolved `CreateUfsFileOptions`;
    /// - the Worker calls `createNonexistingFile` exactly once and appends every
    ///   subsequent chunk to the same `OutputStream`.
    async fn open_ufs_stream(&mut self) -> Result<()> {
        const UFS_BLOCK_ID: i64 = -1; // ID_UNUSED in Java
        const UFS_STREAM_LENGTH: i64 = i64::MAX; // Long.MAX_VALUE in Java

        let worker_info = self.router.pick_any_worker().await?;
        let addr = worker_info
            .address
            .as_ref()
            .ok_or_else(|| Error::Internal {
                message: "ufs-stream worker has no address".to_string(),
                source: None,
            })?;

        let worker_addr = format!(
            "{}:{}",
            addr.host.as_deref().unwrap_or("127.0.0.1"),
            addr.rpc_port.unwrap_or(9203)
        );

        debug!(
            worker = %worker_addr,
            path = %self.path,
            "opening UFS stream for CACHE_THROUGH/THROUGH"
        );

        let worker = match self.worker_pool.acquire(&worker_addr).await {
            Ok(w) => w,
            Err(e) => {
                self.router.mark_failed(addr);
                self.worker_pool.invalidate(&worker_addr).await;
                return Err(e);
            }
        };

        let write_opts = WriteBlockOptions {
            request_type: RequestType::UfsFile,
            create_ufs_file_options: self.write_strategy.create_ufs_file_options.clone(),
        };

        let writer =
            match GrpcBlockWriter::open(&worker, UFS_BLOCK_ID, UFS_STREAM_LENGTH, write_opts).await
            {
                Ok(w) => w,
                Err(e) => {
                    self.router.mark_failed(addr);
                    self.worker_pool.invalidate(&worker_addr).await;
                    return Err(e);
                }
            };

        self.ufs_stream = Some(writer);
        self.ufs_worker_addr = Some(worker_addr);
        Ok(())
    }

    /// Handle a cache write exception.
    ///
    /// Matches Java's `GooseFSFileOutStream.handleCacheWriteException()`:
    /// - Cancel the current block stream
    /// - Mark the worker as failed
    /// - Return the error (caller decides whether to retry or propagate)
    async fn handle_cache_write_exception(&mut self, err: Error) -> Result<()> {
        warn!(
            path = %self.path,
            error = %err,
            "failed to write to GooseFS cache, cancelling block"
        );

        // Cancel the current block writer
        if let Some(active) = self.current_block_writer.take() {
            // Mark the worker as failed for future exclusion
            self.router
                .mark_failed(&crate::proto::grpc::WorkerNetAddress {
                    host: Some(
                        active
                            .worker_addr
                            .split(':')
                            .next()
                            .unwrap_or("unknown")
                            .to_string(),
                    ),
                    rpc_port: active
                        .worker_addr
                        .split(':')
                        .nth(1)
                        .and_then(|p| p.parse().ok()),
                    ..Default::default()
                });
            self.worker_pool.invalidate(&active.worker_addr).await;
            active.writer.cancel().await;
        }

        Err(err)
    }

    /// Handle a UFS-stream write exception.
    ///
    /// Unlike the cache stream (which can be sliced into fresh blocks on the
    /// next write), the UFS stream is a single long-lived connection for the
    /// whole file — if it fails mid-write, the file cannot be recovered on the
    /// UFS side. We tear it down, mark the worker failed, and surface the error.
    async fn handle_ufs_write_exception(&mut self, err: Error) -> Result<()> {
        warn!(
            path = %self.path,
            error = %err,
            "failed to write to UFS stream"
        );

        if let Some(writer) = self.ufs_stream.take() {
            writer.cancel().await;
        }
        if let Some(worker_addr) = self.ufs_worker_addr.take() {
            let host = worker_addr
                .split(':')
                .next()
                .unwrap_or("unknown")
                .to_string();
            let port = worker_addr.split(':').nth(1).and_then(|p| p.parse().ok());
            self.router
                .mark_failed(&crate::proto::grpc::WorkerNetAddress {
                    host: Some(host),
                    rpc_port: port,
                    ..Default::default()
                });
            self.worker_pool.invalidate(&worker_addr).await;
        }

        Err(err)
    }

    /// Cancel the file write, cleaning up all committed blocks.
    ///
    /// Matches Java's `GooseFSFileOutStream.cancel()`:
    /// 1. Cancel the current in-progress block stream
    /// 2. Request Master to remove all previously committed blocks
    /// 3. Mark the file as cancelled
    ///
    /// After cancellation, the incomplete file should be deleted by the caller.
    pub async fn cancel(&mut self) -> Result<()> {
        if self.completed || self.cancelled {
            return Ok(());
        }

        self.cancelled = true;

        // 1. Cancel the UFS stream first (no commit, Worker will clean up temp file).
        if let Some(writer) = self.ufs_stream.take() {
            writer.cancel().await;
        }
        self.ufs_worker_addr = None;

        // 2. Cancel the current cache block writer if any
        if let Some(active) = self.current_block_writer.take() {
            active.writer.cancel().await;
        }

        // 3. Request Master to remove committed blocks
        // Note: Java uses `fileSystemMasterClient.removeBlocks(mPreviousCommittedBlockIds)`
        // Since our MasterClient doesn't have removeBlocks yet, we delete the incomplete file
        // which will trigger block cleanup on the Master side.
        if !self.committed_block_ids.is_empty() || self.write_strategy.ufs_stream {
            warn!(
                path = %self.path,
                committed_blocks = self.committed_block_ids.len(),
                "cancelling file write, requesting cleanup of committed blocks"
            );
            // Delete the incomplete file — Master will clean up associated blocks
            if let Err(e) = self.master.delete(&self.path, false).await {
                warn!(
                    path = %self.path,
                    error = %e,
                    "failed to delete incomplete file during cancel — blocks may need manual cleanup"
                );
            }
        }

        info!(
            path = %self.path,
            committed_blocks = self.committed_block_ids.len(),
            "file write cancelled"
        );

        Ok(())
    }

    /// Close the file writer, finalizing the file on the Master.
    ///
    /// This flushes both streams (if any), then calls `CompleteFile` to mark
    /// the file as fully written. After calling `close()`, the writer cannot
    /// be used again.
    ///
    /// Matches Java's `GooseFSFileOutStream.close()` — note the order:
    /// 1. close UFS stream (flush + close, triggers Worker-side `OutputStream.close()`);
    /// 2. close current cache block (flush + commitBlock);
    /// 3. `completeFile(ufsLength)` on Master;
    /// 4. ASYNC_THROUGH → `scheduleAsyncPersistence`.
    pub async fn close(&mut self) -> Result<()> {
        if self.completed {
            warn!(path = %self.path, "close() called on already-completed file");
            return Ok(());
        }

        if self.cancelled {
            return Ok(());
        }

        // 1) Close the single long-lived UFS stream first.
        //    Dropping the request channel signals Worker-side onCompleted,
        //    which in turn flushes and closes the UFS OutputStream.
        if let Some(mut ufs) = self.ufs_stream.take() {
            if let Err(e) = ufs.flush().await {
                warn!(
                    path = %self.path,
                    error = %e,
                    "failed to flush UFS stream during close, cancelling"
                );
                ufs.cancel().await;
                self.cancel().await?;
                return Err(e);
            }
            if let Err(e) = ufs.close().await {
                warn!(
                    path = %self.path,
                    error = %e,
                    "failed to close UFS stream during close, cancelling"
                );
                self.cancel().await?;
                return Err(e);
            }
            self.ufs_worker_addr = None;
        }

        // 2) Close the current in-progress cache block (flush + commitBlock)
        if let Err(e) = self.close_current_block().await {
            warn!(
                path = %self.path,
                error = %e,
                "failed to close current block during file close, cancelling"
            );
            self.cancel().await?;
            return Err(e);
        }

        // 3) Complete the file. Always pass ufs_length for CACHE_THROUGH/THROUGH so
        //    Master knows exactly how many bytes ended up in UFS.
        let ufs_length = if self.write_strategy.ufs_stream {
            Some(self.total_bytes_written as i64)
        } else if self.total_bytes_written > 0 {
            Some(self.total_bytes_written as i64)
        } else {
            None
        };

        self.master.complete_file(&self.path, ufs_length).await?;
        self.completed = true;

        // 4) ASYNC_THROUGH: schedule asynchronous persistence to UFS after file is complete.
        if self.write_strategy.need_async_persist {
            debug!(path = %self.path, "scheduling async persistence for ASYNC_THROUGH");
            if let Err(e) = self
                .master
                .schedule_async_persistence(&self.path, None)
                .await
            {
                warn!(
                    path = %self.path,
                    error = %e,
                    "failed to schedule async persistence — file is complete but may not persist to UFS"
                );
            }
        }

        info!(
            path = %self.path,
            total_bytes = self.total_bytes_written,
            cache_blocks = self.committed_block_ids.len(),
            ufs_stream = self.write_strategy.ufs_stream,
            "file write completed"
        );

        Ok(())
    }

    /// One-shot convenience method: create file, write all data, and close.
    ///
    /// This is the simplest way to write a file to GooseFS:
    ///
    /// ```rust,no_run
    /// # async fn example() -> goosefs_sdk::error::Result<()> {
    /// use goosefs_sdk::io::GooseFsFileWriter;
    /// use goosefs_sdk::config::GooseFsConfig;
    ///
    /// let config = GooseFsConfig::new("127.0.0.1:9200");
    /// GooseFsFileWriter::write_file(&config, "/my-file.txt", b"Hello!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_file(config: &GooseFsConfig, path: &str, data: &[u8]) -> Result<u64> {
        let mut writer = Self::create(config, path).await?;
        writer.write(data).await?;
        writer.close().await?;
        Ok(writer.total_bytes_written)
    }

    /// One-shot convenience method with custom create options.
    pub async fn write_file_with_options(
        config: &GooseFsConfig,
        path: &str,
        data: &[u8],
        options: CreateFilePOptions,
    ) -> Result<u64> {
        let mut writer = Self::create_with_options(config, path, Some(options)).await?;
        writer.write(data).await?;
        writer.close().await?;
        Ok(writer.total_bytes_written)
    }

    /// Get the total bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.total_bytes_written
    }

    /// Get the file path being written.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Whether the file has been completed.
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Get a reference to the file info.
    pub fn file_info(&self) -> &FileInfo {
        &self.file_info
    }
}

/// Compute a deterministic block ID from file ID (inode ID) and block index.
///
/// GooseFS uses a scheme where block IDs are derived from the file's inode ID:
///
/// ```text
/// Block ID layout (64 bits):
///   [container ID: 40 bits][sequence number: 24 bits]
///
/// container ID = inode_id >> 24   (extract upper 40 bits)
/// block ID     = (container_id << 24) | block_index
/// ```
///
/// This matches the Java implementation in `com.qcloud.cos.goosefs.master.block.BlockId`:
///   - `CONTAINER_ID_BITS = 40`
///   - `SEQUENCE_NUMBER_BITS = 24`
///   - `getContainerId(inodeId) = (inodeId >> 24) & CONTAINER_ID_MASK`
///   - `createBlockId(containerId, seq) = (containerId << 24) | seq`
fn compute_block_id(file_id: i64, block_index: u64) -> i64 {
    const CONTAINER_ID_BITS: u32 = 40;
    const SEQUENCE_NUMBER_BITS: u32 = 64 - CONTAINER_ID_BITS; // 24
    const CONTAINER_ID_MASK: i64 = (1i64 << CONTAINER_ID_BITS) - 1;
    const SEQUENCE_NUMBER_MASK: u64 = (1u64 << SEQUENCE_NUMBER_BITS) - 1;

    // Extract container ID from the inode ID (file_id)
    let container_id = (file_id >> SEQUENCE_NUMBER_BITS) & CONTAINER_ID_MASK;
    let seq = (block_index & SEQUENCE_NUMBER_MASK) as i64;
    (container_id << SEQUENCE_NUMBER_BITS) | seq
}

/// State for the currently active block being written.
///
/// Holds the `GrpcBlockWriter` and tracks how many bytes have been
/// streamed to it. This enables chunk-level streaming (matching Java's
/// `BlockOutStream` pattern) instead of whole-block buffering.
struct ActiveBlockWriter {
    /// The underlying gRPC streaming writer.
    writer: GrpcBlockWriter,
    /// Block ID being written.
    block_id: i64,
    /// Total block capacity.
    block_size: u64,
    /// Bytes written to this block so far.
    bytes_written: u64,
    /// Worker address (for failure tracking).
    worker_addr: String,
}

impl ActiveBlockWriter {
    /// Remaining bytes that can be written to this block.
    fn remaining(&self) -> u64 {
        self.block_size - self.bytes_written
    }
}

impl Drop for GooseFsFileWriter {
    fn drop(&mut self) {
        if !self.completed && !self.cancelled && self.total_bytes_written > 0 {
            warn!(
                path = %self.path,
                bytes_written = self.total_bytes_written,
                committed_blocks = self.committed_block_ids.len(),
                "GooseFsFileWriter dropped without calling close() or cancel() — file may be incomplete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_block_id() {
        // GooseFS inode IDs have the container ID in the upper 40 bits.
        // For inode_id = 33554431 (0x1FFFFFF), container_id = 33554431 >> 24 = 1
        // block_id = (1 << 24) | 0 = 16777216
        let inode_id = 33554431i64; // typical GooseFS inode ID
        assert_eq!(compute_block_id(inode_id, 0), 1 << 24);
        assert_eq!(compute_block_id(inode_id, 1), (1 << 24) | 1);

        // For inode_id with container_id = 2: inode_id = 2 << 24 | anything
        let inode_id_2 = 2i64 << 24;
        assert_eq!(compute_block_id(inode_id_2, 0), 2 << 24);
    }

    #[test]
    fn test_compute_block_id_container_extraction() {
        // Verify container ID extraction matches Java's BlockId.getContainerId()
        const SEQUENCE_NUMBER_BITS: u32 = 24;
        const CONTAINER_ID_MASK: i64 = (1i64 << 40) - 1;

        let file_id = 33554431i64;
        let block_id = compute_block_id(file_id, 3);
        // Extract container ID from block_id
        let container_id = (block_id >> SEQUENCE_NUMBER_BITS) & CONTAINER_ID_MASK;
        assert_eq!(container_id, 1);
        // Extract sequence number from block_id
        assert_eq!(block_id & ((1 << SEQUENCE_NUMBER_BITS) - 1), 3);
    }

    /// Helper to build a minimal FileInfo for strategy tests.
    fn make_test_file_info() -> FileInfo {
        FileInfo {
            file_id: Some(1),
            ufs_path: Some("/ufs/data/test.txt".to_string()),
            owner: Some("hadoop".to_string()),
            group: Some("supergroup".to_string()),
            mode: Some(0o644),
            mount_id: Some(42),
            ..Default::default()
        }
    }

    #[test]
    fn test_strategy_must_cache() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(1), &fi); // MUST_CACHE
        assert!(s.cache_stream);
        assert!(!s.ufs_stream);
        assert!(s.create_ufs_file_options.is_none());
        assert!(!s.need_async_persist);
    }

    #[test]
    fn test_strategy_cache_through() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(3), &fi); // CACHE_THROUGH
                                                      // CRITICAL: CACHE_THROUGH must drive BOTH streams in parallel.
        assert!(s.cache_stream, "CACHE_THROUGH must enable cache stream");
        assert!(s.ufs_stream, "CACHE_THROUGH must enable UFS stream");
        assert!(s.create_ufs_file_options.is_some());
        assert!(!s.need_async_persist);
    }

    #[test]
    fn test_strategy_through() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(4), &fi); // THROUGH
        assert!(!s.cache_stream, "THROUGH must NOT enable cache stream");
        assert!(s.ufs_stream);
        let ufs_opts = s.create_ufs_file_options.as_ref().unwrap();
        assert_eq!(ufs_opts.ufs_path, Some("/ufs/data/test.txt".to_string()));
        assert_eq!(ufs_opts.owner, Some("hadoop".to_string()));
        assert_eq!(ufs_opts.group, Some("supergroup".to_string()));
        assert_eq!(ufs_opts.mode, Some(0o644));
        assert_eq!(ufs_opts.mount_id, Some(42));
        assert!(!s.need_async_persist);
    }

    #[test]
    fn test_strategy_async_through() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(5), &fi); // ASYNC_THROUGH
        assert!(s.cache_stream);
        assert!(!s.ufs_stream);
        assert!(s.create_ufs_file_options.is_none());
        assert!(s.need_async_persist);
    }

    #[test]
    fn test_strategy_default_unset() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(None, &fi);
        assert!(s.cache_stream);
        assert!(!s.ufs_stream);
        assert!(s.create_ufs_file_options.is_none());
        assert!(!s.need_async_persist);
    }

    #[test]
    fn test_strategy_try_cache() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(2), &fi); // TRY_CACHE
        assert!(s.cache_stream);
        assert!(!s.ufs_stream);
        assert!(s.create_ufs_file_options.is_none());
        assert!(!s.need_async_persist);
    }
}
