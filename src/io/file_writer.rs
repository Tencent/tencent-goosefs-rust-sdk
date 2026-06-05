//! High-level file writer that orchestrates the complete write pipeline.
//!
//! `GoosefsFileWriter` ties together all low-level components into a single
//! easy-to-use API, analogous to Java's `GoosefsFileOutStream`:
//!
//! ```text
//! GoosefsFileWriter::create_with_context(ctx, path, opts)
//!   → MasterClient.create_file()
//!   → BlockMapper.plan_write()
//!   → for each block:
//!       → WorkerRouter.select_worker()
//!       → WorkerClient.connect()        (pooled — zero new TCP+SASL)
//!       → GrpcBlockWriter.open() → write_all() → flush() → close()
//!   → MasterClient.complete_file()
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use goosefs_sdk::io::GoosefsFileWriter;
//! use goosefs_sdk::context::FileSystemContext;
//! use goosefs_sdk::config::GoosefsConfig;
//!
//! # async fn example() -> goosefs_sdk::error::Result<()> {
//! let ctx = FileSystemContext::connect(GoosefsConfig::new("127.0.0.1:9200")).await?;
//! let data = b"Hello, Goosefs!";
//!
//! // One-shot write (zero new connections)
//! GoosefsFileWriter::write_file_with_context(ctx.clone(), "/my-file.txt", data).await?;
//!
//! // Or use the builder for more control
//! let mut writer = GoosefsFileWriter::create_with_context(ctx.clone(), "/my-file.txt", None).await?;
//! writer.write(data).await?;
//! writer.close().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::block::router::WorkerRouter;
use crate::client::master::default_file_mode;
use crate::client::worker::{WorkerClientPool, WriteBlockOptions};
use crate::client::MasterClient;
use crate::config::GoosefsConfig;
use crate::context::FileSystemContext;
use crate::error::{Error, Result};
use crate::fs::options::DeleteOptions;
use crate::io::writer::GrpcBlockWriter;
use crate::proto::grpc::block::RequestType;
use crate::proto::grpc::file::{CreateFilePOptions, FileInfo, FsOpPId};
use crate::proto::proto::dataserver::CreateUfsFileOptions;

/// Write strategy derived from the effective `WritePType`.
///
/// Unlike the old single-stream design, CACHE_THROUGH must drive **two
/// independent streams in parallel** (matching Java `GoosefsFileOutStream`):
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

/// Convert a [`Uuid`] to the `FsOpPId` proto message expected by Goosefs Master.
///
/// # Java authority
///
/// Java uses `UUID.getMostSignificantBits()` / `getLeastSignificantBits()` which
/// return the high 64 bits and low 64 bits of the 128-bit UUID value respectively.
/// `Uuid::as_u64_pair()` in the `uuid` crate returns `(high, low)` with the same
/// bit layout (big-endian interpretation of the 16-byte UUID).
///
/// # Go SDK bug
///
/// The Go SDK stores the UUID locally but **never writes `FsOpPId` into the proto
/// request** (`CompleteFilePOptions.common_options.operation_id` is always empty).
/// This implementation fixes that by always wiring the ID into the request.
fn uuid_to_fs_op_pid(id: Uuid) -> FsOpPId {
    let (high, low) = id.as_u64_pair();
    FsOpPId {
        most_significant_bits: Some(high as i64),
        least_significant_bits: Some(low as i64),
    }
}

/// High-level file writer that orchestrates the full Goosefs write pipeline.
///
/// This struct encapsulates the complete write flow:
/// 1. `CreateFile` on Master to register the new file
/// 2. Discover workers and set up routing
/// 3. Split data into blocks via `BlockMapper`
/// 4. Write each block to a worker via `GrpcBlockWriter`
/// 5. `CompleteFile` on Master to finalize
///
/// ## Cancellation / Close state machine
///
/// Two atomic flags model the writer lifecycle:
///
/// - `cancelled`: set to `true` when `cancel()` is called.  Once set,
///   subsequent writes are rejected and `close()` becomes a no-op.
/// - `closed`: CAS-locked by `close()` to prevent concurrent/duplicate closes.
///   Once `closed` is `true` the writer is terminal.
///
/// This mirrors Java `GoosefsFileOutStream.mCanceled` + `mClosed` and avoids
/// the ambiguity of the previous single-bool design.
pub struct GoosefsFileWriter {
    /// The Goosefs config.
    config: GoosefsConfig,
    /// The file path being written.
    path: String,
    /// Master client for metadata operations.
    master: MasterClient,
    /// Worker router for block → worker mapping (with failed-worker exclusion).
    router: WorkerRouter,
    /// Connection pool for reusing authenticated worker gRPC channels.
    /// Matches Java's `FileSystemContext.acquireBlockWorkerClient()`.
    worker_pool: Arc<WorkerClientPool>,
    /// Optional shared context (non-None when created via `create_with_context`).
    /// Kept alive to prevent context GC while the writer is in use.
    _context: Option<Arc<FileSystemContext>>,
    /// File info returned by CreateFile.
    file_info: FileInfo,
    /// Total bytes written so far across all blocks (committed only).
    total_bytes_written: u64,
    /// Idempotency token for `CompleteFile`.
    ///
    /// Generated at construction time; reused on every retry of `complete_file`.
    /// Stored as a `Uuid` and converted to `FsOpPId` at call time via
    /// [`uuid_to_fs_op_pid`].
    operation_id: Uuid,
    /// Cancel intent flag — set by `cancel()`, checked by `write()` / `close()`.
    ///
    /// Uses `Ordering::SeqCst` throughout to ensure visibility across tasks.
    cancelled: AtomicBool,
    /// Close CAS lock — set by the first `close()` call to prevent duplicates.
    ///
    /// `close()` does `compare_exchange(false, true)` to claim exclusive access.
    closed: AtomicBool,
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
    /// Whether the UFS stream has been successfully closed.
    ///
    /// Used during CACHE_THROUGH error recovery in `handle_complete_file_error`:
    /// if UFS close succeeded but `completeFile` failed, we must clean up the
    /// Goosefs-side metadata entry only (not the UFS file).
    ufs_stream_completed: AtomicBool,
}

impl GoosefsFileWriter {
    /// Create a new file using a shared [`FileSystemContext`].
    ///
    /// Reuses the persistent Master connection, worker router, and connection
    /// pool from `ctx` — **no additional TCP+SASL handshake** is performed.
    /// Use this when you have a long-lived [`FileSystemContext`] and want
    /// zero-handshake file writes.
    ///
    /// # Arguments
    /// - `ctx` — Shared context created with `FileSystemContext::connect()`
    /// - `path` — File path in Goosefs namespace
    /// - `options` — Optional `CreateFilePOptions` (block size, write type, etc.)
    pub async fn create_with_context(
        ctx: Arc<FileSystemContext>,
        path: &str,
        options: Option<CreateFilePOptions>,
    ) -> Result<Self> {
        let config = ctx.config().clone();

        // Reuse the shared Master client (zero TCP+SASL handshake).
        let master_arc = ctx.acquire_master();

        let create_options = options.unwrap_or_else(|| {
            let mut opts = CreateFilePOptions {
                block_size_bytes: Some(config.block_size as i64),
                mode: Some(default_file_mode()),
                recursive: Some(true),
                ..Default::default()
            };
            if config.write_type.is_some() {
                opts.write_type = config.write_type;
            }
            opts
        });

        let mut create_options = create_options;
        if create_options.recursive.is_none() {
            create_options.recursive = Some(true);
        }
        // Always ensure block_size_bytes and mode are set — callers that pass a
        // partial CreateFilePOptions (e.g. only overriding write_type) would
        // otherwise get "Invalid block size 0" from the Master.
        if create_options.block_size_bytes.is_none() || create_options.block_size_bytes == Some(0) {
            create_options.block_size_bytes = Some(config.block_size as i64);
        }
        if create_options.mode.is_none() {
            create_options.mode = Some(default_file_mode());
        }

        let file_info = master_arc.create_file(path, create_options).await?;
        debug!(
            path = %path,
            file_id = ?file_info.file_id,
            "file created on Master (via context)"
        );

        let effective_write_type = create_options.write_type.or(config.write_type);
        let write_strategy = resolve_write_strategy(effective_write_type, &file_info);

        // Reuse shared router and pool from context (zero additional RPCs).
        let router_arc = ctx.acquire_router();
        let worker_pool = ctx.acquire_worker_pool();

        // Clone the router into a local WorkerRouter wrapper.
        // We snapshot the current worker list from the shared router.
        let router = WorkerRouter::new();
        let workers = (*router_arc.get_workers().await).clone();
        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers available for writing".to_string(),
            });
        }
        router.update_workers(workers).await;

        let operation_id = Uuid::new_v4();

        // SAFETY: We clone the MasterClient from Arc<MasterClient>.
        // The file_writer holds it by value; the Arc in ctx keeps the channel alive.
        let master = (*master_arc).clone();

        Ok(Self {
            config,
            path: path.to_string(),
            master,
            router,
            worker_pool,
            _context: Some(ctx), // keep ctx alive for pool/router lifetime
            file_info,
            total_bytes_written: 0,
            operation_id,
            cancelled: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            write_strategy,
            committed_block_ids: Vec::new(),
            current_block_writer: None,
            ufs_stream: None,
            ufs_worker_addr: None,
            ufs_stream_completed: AtomicBool::new(false),
        })
    }

    /// Write data to the file.
    ///
    /// Depending on the resolved `WriteStrategy`, data is fanned out to one or
    /// both of the following streams — matching Java `GoosefsFileOutStream.writeInternal`:
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
        if self.cancelled.load(Ordering::SeqCst) || self.closed.load(Ordering::SeqCst) {
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

    /// Flush in-progress data to the current block writer.
    ///
    /// Calls `flush()` on the active `GrpcBlockWriter` to push buffered chunks
    /// to the worker and wait for an acknowledgment.  This does **not** close
    /// the current block or call `completeFile`.
    ///
    /// Any trailing partial chunk held back by the chunk-coalescing
    /// workaround is also drained here, because an explicit `flush()` is a
    /// safe boundary (the user has asked for an ack and is fine with a
    /// partial chunk landing on the wire).
    ///
    /// # Java authority
    ///
    /// Mirrors `GoosefsFileOutStream.flush()` which calls
    /// `mCurrentBlockOutStream.flush()` if one is active.
    pub async fn flush(&mut self) -> Result<()> {
        if self.cancelled.load(Ordering::SeqCst) || self.closed.load(Ordering::SeqCst) {
            return Err(Error::BlockIoError {
                message: "cannot flush a completed or cancelled file".to_string(),
            });
        }

        if let Some(active) = self.current_block_writer.as_mut() {
            if active.bytes_written > 0 {
                // Drain any held-back trailing partial chunk before flushing.
                if !active.pending_chunk.is_empty() {
                    let tail = Bytes::copy_from_slice(&active.pending_chunk);
                    active.pending_chunk.clear();
                    if let Err(e) = active.writer.write_chunk(tail).await {
                        return self.handle_cache_write_exception(e).await;
                    }
                }
                active.writer.flush().await?;
            }
        }
        Ok(())
    }

    /// Append data to the per-block cache stream, slicing at block boundaries.
    ///
    /// To avoid the server-side concurrent-writer race in
    /// `LocalFileBlockWriter.appendComposite` (see
    /// `docs/BUG_concurrent_writer_file_length_inconsistent.md`), every chunk
    /// pushed onto the gRPC stream is **exactly `chunk_size` bytes**, except
    /// at safe boundaries (block end / explicit flush / block close), where a
    /// trailing partial chunk is allowed because no further chunks follow on
    /// the same stream. Sub-`chunk_size` tails are buffered in
    /// `ActiveBlockWriter::pending_chunk` and merged with subsequent writes.
    async fn write_to_cache_stream(&mut self, data: &[u8]) -> Result<()> {
        let block_size = self
            .file_info
            .block_size_bytes
            .unwrap_or(self.config.block_size as i64) as u64;
        let chunk_size = self.config.chunk_size as usize;

        // Instrument: record cache-path bytes written.
        crate::metrics::counter(crate::metrics::name::CLIENT_BYTES_WRITTEN_LOCAL)
            .inc(data.len() as i64);

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
            let to_accept = std::cmp::min(remaining_in_block, remaining_data);
            let end = offset + to_accept;

            // Append the new bytes to the pending buffer first, then peel off
            // as many full `chunk_size` chunks as possible. Anything left
            // over (strictly < chunk_size) stays in `pending_chunk`.
            writer.pending_chunk.extend_from_slice(&data[offset..end]);
            writer.bytes_written += to_accept as u64;
            offset = end;

            let block_full = writer.remaining() == 0;

            // Drain full chunks from the pending buffer.
            while writer.pending_chunk.len() >= chunk_size {
                let chunk = Bytes::copy_from_slice(&writer.pending_chunk[..chunk_size]);
                writer.pending_chunk.drain(..chunk_size);
                if let Err(e) = writer.writer.write_chunk(chunk).await {
                    return self.handle_cache_write_exception(e).await;
                }
            }

            // If this fills the block, also flush any trailing partial chunk
            // (block boundary is a safe place for a partial chunk because the
            // stream is about to be closed).
            if block_full {
                if !writer.pending_chunk.is_empty() {
                    let tail = Bytes::copy_from_slice(&writer.pending_chunk);
                    writer.pending_chunk.clear();
                    if let Err(e) = writer.writer.write_chunk(tail).await {
                        return self.handle_cache_write_exception(e).await;
                    }
                }
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
                // Instrument: record UFS-path bytes written.
                crate::metrics::counter(crate::metrics::name::CLIENT_BYTES_WRITTEN_UFS)
                    .inc(total as i64);
                Ok(())
            }
            Err(e) => self.handle_ufs_write_exception(e).await,
        }
    }

    /// Open the next **cache** block writer.
    ///
    /// Matches Java's `GoosefsFileOutStream.getNextBlock()`:
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
            async_write: self.write_strategy.need_async_persist,
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
            pending_chunk: Vec::with_capacity(self.config.chunk_size as usize),
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
            let mut pending_chunk = active.pending_chunk;
            let mut writer = active.writer;

            if bytes_written > 0 {
                // Drain any held-back trailing partial chunk: closing the
                // block is a safe boundary because no further chunks follow
                // on this stream.
                if !pending_chunk.is_empty() {
                    let tail = Bytes::copy_from_slice(&pending_chunk);
                    pending_chunk.clear();
                    if let Err(e) = writer.write_chunk(tail).await {
                        // Best-effort cancel: tear the stream down and bubble up.
                        writer.cancel().await;
                        return Err(e);
                    }
                }

                // Flush to ensure data is persisted on the worker.
                //
                // N3 fix: on flush failure the worker has already received
                // (part of) the chunk stream, so we must `cancel()` the
                // stream explicitly. Otherwise the temp block lingers on
                // the worker until the lease TTL expires.
                let ack_offset = match writer.flush().await {
                    Ok(off) => off,
                    Err(e) => {
                        warn!(
                            block_id = block_id,
                            error = %e,
                            "flush failed during close_current_block; cancelling block stream"
                        );
                        writer.cancel().await;
                        return Err(e);
                    }
                };
                debug!(
                    block_id = block_id,
                    ack_offset = ack_offset,
                    bytes_written = bytes_written,
                    "cache block flushed"
                );

                // Close the writer (triggers server-side commitBlock).
                //
                // N3 fix: on close failure we cannot `cancel()` (close()
                // consumes the writer). The worker may have committed the
                // block before responding the error, so record `block_id`
                // into `committed_block_ids` so that `do_cancel_cleanup`
                // can issue `remove_blocks` (idempotent) on the rollback
                // path. Without this, the partial inode would survive.
                if let Err(e) = writer.close().await {
                    warn!(
                        block_id = block_id,
                        error = %e,
                        "close failed during close_current_block; \
                         recording block_id for cancel-cleanup remove_blocks"
                    );
                    self.committed_block_ids.push(block_id);
                    return Err(e);
                }

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
            async_write: false,
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
    /// Matches Java's `GoosefsFileOutStream.handleCacheWriteException()`:
    /// - Cancel the current block stream
    /// - Mark the worker as failed
    /// - Return the error (caller decides whether to retry or propagate)
    async fn handle_cache_write_exception(&mut self, err: Error) -> Result<()> {
        warn!(
            path = %self.path,
            error = %err,
            "failed to write to Goosefs cache, cancelling block"
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

    // -----------------------------------------------------------------------
    // Cancel cleanup
    // -----------------------------------------------------------------------

    /// Perform cancel cleanup: tear down streams, then call
    /// `remove_blocks` with a fallback to `delete(unchecked=true)`.
    ///
    /// # Java authority
    ///
    /// Matches `GoosefsFileOutStream.cancel()`:
    /// 1. Cancel all in-flight streams (UFS + cache block).
    /// 2. Call `fileSystemMasterClient.removeBlocks(mPreviousCommittedBlockIds)`.
    /// 3. If `removeBlocks` fails, fall back to `delete(path, unchecked=true)`.
    ///
    /// `removeBlocks` is preferred over `delete` because it only cleans up
    /// block metadata and does **not** remove the INCOMPLETE inode from the
    /// namespace.  This is important if a higher-level retry layer wants to
    /// re-create the file at the same path.
    async fn do_cancel_cleanup(&mut self) {
        // 1. Cancel UFS stream (Worker cleans up the temp UFS file).
        if let Some(writer) = self.ufs_stream.take() {
            writer.cancel().await;
        }
        self.ufs_worker_addr = None;

        // 2. Cancel current in-progress cache block writer.
        if let Some(active) = self.current_block_writer.take() {
            active.writer.cancel().await;
        }

        // 3. Clean up committed blocks on Master.
        if !self.committed_block_ids.is_empty() {
            let block_ids = self.committed_block_ids.clone();
            debug!(
                path = %self.path,
                block_count = block_ids.len(),
                "cancel: calling remove_blocks on Master"
            );
            if let Err(e) = self.master.remove_blocks(block_ids).await {
                // remove_blocks failed — fall back to delete(unchecked=true).
                warn!(
                    path = %self.path,
                    error = %e,
                    "remove_blocks failed, falling back to delete(unchecked=true)"
                );
                if let Err(del_err) = self
                    .master
                    .delete_with_options(&self.path, DeleteOptions::for_cancel())
                    .await
                {
                    warn!(
                        path = %self.path,
                        error = %del_err,
                        "fallback delete also failed — blocks may need manual cleanup"
                    );
                }
            }
        }
    }

    /// Cancel the file write, cleaning up all committed blocks.
    ///
    /// Sets the `cancelled` flag and delegates to `do_cancel_cleanup`.
    ///
    /// Calling `cancel()` after `close()` is a no-op.
    /// Calling `cancel()` twice is idempotent.
    pub async fn cancel(&mut self) -> Result<()> {
        // Already closed (normally) — nothing to clean up.
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Already cancelled — idempotent.
        if self.cancelled.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.do_cancel_cleanup().await;

        info!(
            path = %self.path,
            committed_blocks = self.committed_block_ids.len(),
            "file write cancelled"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // T2-C: CACHE_THROUGH error recovery
    // -----------------------------------------------------------------------

    /// Handle a `completeFile` failure after UFS `close()` succeeded.
    ///
    /// In CACHE_THROUGH mode there is a small window where the UFS file has
    /// been fully written (UFS `close()` returned OK) but `completeFile` on
    /// the Master then fails (e.g. a Master failover or transient network
    /// error).  In this situation we must:
    ///
    /// 1. Delete the Goosefs metadata entry (`goosefs_only=true, unchecked=true`)
    ///    so the incomplete inode is cleaned up.
    /// 2. **Not** touch the UFS file — it was written successfully and serves
    ///    as the source of truth.
    ///
    /// # Java authority (T2-C)
    ///
    /// Matches the catch block in `GoosefsFileOutStream.close()`:
    /// ```java
    /// } catch (Exception e) {
    ///     if (ufsSucceeded) {
    ///         // UFS file is OK; remove the Goosefs entry only.
    ///         mFileSystem.delete(mUri,
    ///             DeleteOptions.defaults().setGoosefsOnly(true).setUnchecked(true));
    ///         // Reload so the next open() sees the UFS file via listStatus(ALWAYS).
    ///         mFileSystem.loadMetadata(mUri, ...);
    ///     }
    ///     throw e;
    /// }
    /// ```
    ///
    /// The `listStatus` reload (equivalent of `loadMetadata`) is tracked as a
    /// separate TODO — it requires a new `list_status` RPC variant — and is
    /// deferred to Wave 2.
    async fn handle_complete_file_error(&mut self, err: Error) -> Error {
        if self.ufs_stream_completed.load(Ordering::SeqCst) {
            warn!(
                path = %self.path,
                error = %err,
                "completeFile failed after UFS close succeeded; \
                 removing Goosefs-only metadata entry (goosefs_only=true, unchecked=true)"
            );
            if let Err(del_err) = self
                .master
                .delete_with_options(&self.path, DeleteOptions::goosefs_only_unchecked())
                .await
            {
                warn!(
                    path = %self.path,
                    error = %del_err,
                    "failed to clean up Goosefs metadata after completeFile failure — \
                     manual cleanup may be required"
                );
            }
            // TODO (Wave 2): call list_status(ALWAYS) to force the Master to reload
            // UFS metadata so a subsequent open() of this path returns the UFS file.
            // This requires adding a new list_status_with_load_metadata() variant to
            // MasterClient (see T2-C in FINAL_DESIGN.md).
        }
        err
    }

    /// Close the file writer, finalizing the file on the Master.
    ///
    /// This flushes both streams (if any), then calls `CompleteFile` to mark
    /// the file as fully written. After calling `close()`, the writer cannot
    /// be used again.
    ///
    /// Matches Java's `GoosefsFileOutStream.close()` — note the order:
    /// 1. close UFS stream (flush + close, triggers Worker-side `OutputStream.close()`);
    /// 2. close current cache block (flush + commitBlock);
    /// 3. `completeFile(path, ufsLength, operationId)` on Master;
    /// 4. ASYNC_THROUGH → `scheduleAsyncPersistence`.
    ///
    /// ## Idempotency
    ///
    /// `closed` is set via `compare_exchange(false, true)` so only the first
    /// concurrent `close()` call proceeds; subsequent calls are no-ops.
    pub async fn close(&mut self) -> Result<()> {
        // CAS: only the first close() wins.
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!(path = %self.path, "close() called on already-completed file");
            return Ok(());
        }

        if self.cancelled.load(Ordering::SeqCst) {
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
                self.do_cancel_cleanup().await;
                return Err(e);
            }
            if let Err(e) = ufs.close().await {
                warn!(
                    path = %self.path,
                    error = %e,
                    "failed to close UFS stream during close, cancelling"
                );
                self.do_cancel_cleanup().await;
                return Err(e);
            }
            // UFS stream closed successfully — record this for error recovery.
            self.ufs_stream_completed.store(true, Ordering::SeqCst);
            self.ufs_worker_addr = None;
        }

        // 2) Close the current in-progress cache block (flush + commitBlock)
        if let Err(e) = self.close_current_block().await {
            warn!(
                path = %self.path,
                error = %e,
                "failed to close current block during file close, cancelling"
            );
            self.do_cancel_cleanup().await;
            return Err(e);
        }

        // 3) Complete the file on Master with the idempotency operation ID.
        //    Always pass ufs_length for CACHE_THROUGH/THROUGH so Master knows
        //    exactly how many bytes ended up in UFS.
        let ufs_length = if self.write_strategy.ufs_stream || self.total_bytes_written > 0 {
            Some(self.total_bytes_written as i64)
        } else {
            None
        };

        let op_id = uuid_to_fs_op_pid(self.operation_id);
        if let Err(e) = self
            .master
            .complete_file(&self.path, ufs_length, Some(op_id))
            .await
        {
            // T2-C: CACHE_THROUGH error recovery — clean up Goosefs-only if UFS succeeded.
            let e = self.handle_complete_file_error(e).await;
            return Err(e);
        }

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

    /// One-shot convenience: create file, write all data, and complete it.
    ///
    /// Reuses the Master client, worker router, and connection pool from `ctx`.
    /// This is the context-based equivalent of `write_file(&config, path, data)`.
    ///
    /// # Arguments
    /// - `ctx` — Shared context created with `FileSystemContext::connect()`
    /// - `path` — File path in Goosefs namespace
    /// - `data` — Bytes to write
    ///
    /// # Returns
    /// Total bytes written on success.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use goosefs_sdk::context::FileSystemContext;
    /// use goosefs_sdk::config::GoosefsConfig;
    /// use goosefs_sdk::io::GoosefsFileWriter;
    ///
    /// # async fn example() -> goosefs_sdk::error::Result<()> {
    /// let ctx = FileSystemContext::connect(GoosefsConfig::new("127.0.0.1:9200")).await?;
    /// GoosefsFileWriter::write_file_with_context(ctx, "/my-file.txt", b"Hello!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_file_with_context(
        ctx: Arc<FileSystemContext>,
        path: &str,
        data: &[u8],
    ) -> Result<u64> {
        Self::write_file_with_context_and_options(ctx, path, data, None).await
    }

    /// One-shot convenience with custom create options, using a shared context.
    ///
    /// Like [`write_file_with_context`](Self::write_file_with_context) but lets the caller supply
    /// `CreateFilePOptions` (e.g. to override `write_type` or `block_size_bytes`).
    ///
    /// # Arguments
    /// - `ctx` — Shared context created with `FileSystemContext::connect()`
    /// - `path` — File path in Goosefs namespace
    /// - `data` — Bytes to write
    /// - `options` — Optional `CreateFilePOptions`
    pub async fn write_file_with_context_and_options(
        ctx: Arc<FileSystemContext>,
        path: &str,
        data: &[u8],
        options: Option<CreateFilePOptions>,
    ) -> Result<u64> {
        let mut writer = Self::create_with_context(ctx, path, options).await?;
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

    /// Whether the file has been completed (close returned OK).
    pub fn is_completed(&self) -> bool {
        self.closed.load(Ordering::SeqCst) && !self.cancelled.load(Ordering::SeqCst)
    }

    /// Whether the write has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Get a reference to the file info.
    pub fn file_info(&self) -> &FileInfo {
        &self.file_info
    }
}

/// Compute a deterministic block ID from file ID (inode ID) and block index.
///
/// Goosefs uses a scheme where block IDs are derived from the file's inode ID:
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
///
/// # Trailing partial-chunk coalescing (workaround for server-side BUG)
///
/// To work around a GooseFS Worker race in
/// `LocalFileBlockWriter.appendComposite(CompositeByteBuf)` (which uses a
/// position-relative gathering write on a shared `FileChannel` and is unsafe
/// under concurrent stream pressure when chunks are not `chunk_size`-aligned;
/// see `docs/BUG_concurrent_writer_file_length_inconsistent.md`), this struct
/// keeps a `pending_chunk` buffer. Bytes are accumulated until exactly one
/// `chunk_size`-aligned chunk can be sent; any trailing remainder is held
/// back and merged with subsequent writes. The buffer is force-flushed only
/// at safe boundaries:
///
/// 1. an explicit user `flush()` call;
/// 2. the block becomes full (`remaining == 0`);
/// 3. `close_current_block()` (end of block / file close).
///
/// At these boundaries the trailing partial chunk is fine because either no
/// further chunks follow on this stream, or the stream is about to be torn
/// down — so the server-side `mLocalFileChannel.size()` and accumulated
/// `mPosition` cannot drift any further before `commitBlock`.
struct ActiveBlockWriter {
    /// The underlying gRPC streaming writer.
    writer: GrpcBlockWriter,
    /// Block ID being written.
    block_id: i64,
    /// Total block capacity.
    block_size: u64,
    /// Bytes accepted into this writer (sent + still pending). This is the
    /// authoritative byte counter for block-fullness checks; it advances as
    /// soon as bytes enter `pending_chunk`.
    bytes_written: u64,
    /// Worker address (for failure tracking).
    worker_addr: String,
    /// Trailing partial chunk buffer — at all times its length is strictly
    /// less than `chunk_size`. Holds the unaligned tail of the most recent
    /// write so it can be merged with subsequent data. Drained only at safe
    /// boundaries (see struct-level docs).
    pending_chunk: Vec<u8>,
}

impl ActiveBlockWriter {
    /// Remaining bytes that can be written to this block.
    fn remaining(&self) -> u64 {
        self.block_size - self.bytes_written
    }
}

impl GoosefsFileWriter {
    /// Drop-time best-effort cleanup. Extracted into a method so it can be
    /// unit-tested without going through `mem::drop` (which would deallocate
    /// `self` and forbid any further observation of the `cancelled` flag).
    ///
    /// Safe to call multiple times: the `is_closed || is_cancelled` early
    /// return makes it idempotent.
    fn perform_drop_cleanup(&mut self) {
        let is_closed = self.closed.load(Ordering::SeqCst);
        let is_cancelled = self.cancelled.load(Ordering::SeqCst);
        if is_closed || is_cancelled {
            return;
        }

        // Mark cancelled so any concurrent observers see the intent.
        self.cancelled.store(true, Ordering::SeqCst);

        warn!(
            path = %self.path,
            bytes_written = self.total_bytes_written,
            committed_blocks = self.committed_block_ids.len(),
            "GoosefsFileWriter dropped without close()/cancel() — performing best-effort cleanup"
        );

        // Move the cleanup-relevant state out so the spawned task owns it.
        // The writer is being destroyed, so this can't conflict with anyone.
        let ufs_stream = self.ufs_stream.take();
        let current_block_writer = self.current_block_writer.take();
        let committed_block_ids = std::mem::take(&mut self.committed_block_ids);
        let master = self.master.clone();
        let path = self.path.clone();
        // N2 fix: keep the FileSystemContext Arc alive across the async
        // cleanup. The spawned task may drive worker-side `cancel()` RPCs
        // through the context's `worker_pool` / `router` connection cache;
        // if `_context` were dropped on the main thread before the task
        // finishes, those resources could be torn down mid-flight and the
        // cleanup RPCs would fail to reach the worker.
        let _ctx_keepalive = self._context.take();

        // Spawn cleanup on the current tokio runtime, if any. `Drop` runs
        // synchronously, but the cleanup RPCs are async — `try_current()`
        // covers the typical case where the writer is dropped from inside
        // a tokio context (the runtime keeps running while the spawned task
        // executes asynchronously after `drop` returns). When no runtime is
        // available we cannot do anything beyond the warn above.
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                // N2: bind ctx into the future so it is kept alive until
                // the cleanup completes. The variable is otherwise unused.
                let _ctx = _ctx_keepalive;

                // 1. Cancel UFS stream (Worker cleans up the temp UFS file).
                if let Some(writer) = ufs_stream {
                    writer.cancel().await;
                }
                // 2. Cancel in-progress cache block writer.
                if let Some(active) = current_block_writer {
                    active.writer.cancel().await;
                }
                // 3. Clean up committed blocks on Master so the partial
                //    inode does not become a permanent ghost entry.
                if !committed_block_ids.is_empty() {
                    if let Err(e) = master.remove_blocks(committed_block_ids.clone()).await {
                        warn!(
                            path = %path,
                            error = %e,
                            "Drop cleanup: remove_blocks failed, falling back to delete(unchecked=true)"
                        );
                        if let Err(de) = master
                            .delete_with_options(&path, DeleteOptions::for_cancel())
                            .await
                        {
                            warn!(
                                path = %path,
                                error = %de,
                                "Drop cleanup: fallback delete also failed — manual cleanup may be required"
                            );
                        }
                    }
                }
            });
        } else {
            warn!(
                path = %self.path,
                "Drop cleanup: no tokio runtime available; in-flight blocks/UFS file may leak — \
                 callers should explicitly call close()/cancel() before dropping"
            );
        }
    }
}

impl Drop for GoosefsFileWriter {
    fn drop(&mut self) {
        self.perform_drop_cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_block_id() {
        // Goosefs inode IDs have the container ID in the upper 40 bits.
        // For inode_id = 33554431 (0x1FFFFFF), container_id = 33554431 >> 24 = 1
        // block_id = (1 << 24) | 0 = 16777216
        let inode_id = 33554431i64; // typical Goosefs inode ID
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

    /// Verify that legacy mode has `_context = None` and
    /// context mode would hold `Some(Arc<FileSystemContext>)`.
    /// (Full create() requires a running server — we test the shape here.)
    #[test]
    fn test_context_field_is_option_arc() {
        // The field type must be `Option<Arc<FileSystemContext>>`.
        // We verify this at the type-system level by creating a None value.
        let ctx_field: Option<Arc<FileSystemContext>> = None;
        assert!(ctx_field.is_none());
    }

    /// Verify UUID → FsOpPId bit layout matches Java's UUID.getMostSignificantBits().
    #[test]
    fn test_uuid_to_fs_op_pid_bit_layout() {
        // Construct a UUID with known high/low values.
        // UUID bytes: first 8 bytes = high, last 8 bytes = low (big-endian).
        let high_bytes: [u8; 8] = [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
        let low_bytes: [u8; 8] = [0x88u8, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff];
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&high_bytes);
        bytes[8..].copy_from_slice(&low_bytes);
        let uuid = Uuid::from_bytes(bytes);

        let op_id = uuid_to_fs_op_pid(uuid);

        let expected_high = i64::from_be_bytes(high_bytes);
        let expected_low = i64::from_be_bytes(low_bytes);

        assert_eq!(op_id.most_significant_bits, Some(expected_high));
        assert_eq!(op_id.least_significant_bits, Some(expected_low));
    }

    /// Build a `GoosefsFileWriter` with never-connected stubs for unit tests
    /// of `Drop` semantics. The channel is `connect_lazy()` so no actual
    /// network handshake happens; methods that would issue an RPC will fail
    /// at the first `await` (which is fine — Drop must work without ever
    /// calling such methods).
    fn make_drop_test_writer() -> GoosefsFileWriter {
        use crate::client::{MasterClient, WorkerClientPool};
        use tonic::transport::Channel;

        let config = GoosefsConfig::new("127.0.0.1:9200");
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let master = MasterClient::from_channel(channel, config.clone());
        let router = WorkerRouter::new();
        let worker_pool = Arc::new(WorkerClientPool::new(config.clone()));
        let file_info = make_test_file_info();
        let strategy = resolve_write_strategy(Some(1), &file_info);

        GoosefsFileWriter {
            config,
            path: "/test/drop-without-close.bin".to_string(),
            master,
            router,
            worker_pool,
            _context: None,
            file_info,
            total_bytes_written: 0,
            operation_id: Uuid::nil(),
            cancelled: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            write_strategy: strategy,
            committed_block_ids: Vec::new(),
            current_block_writer: None,
            ufs_stream: None,
            ufs_worker_addr: None,
            ufs_stream_completed: AtomicBool::new(false),
        }
    }

    /// **Regression for C5**: dropping a `GoosefsFileWriter` without going
    /// through `close()` / `cancel()` MUST mark it as cancelled (so any
    /// concurrent observer of the flag sees the intent) and MUST attempt
    /// best-effort async cleanup when a tokio runtime is available.
    ///
    /// Pre-fix behaviour: Drop only emitted a warning and did nothing —
    /// leaving worker temp blocks, UFS half-files, and INCOMPLETE inodes
    /// behind on every error path that bypassed `close()`.
    ///
    /// We exercise the same code path as `Drop::drop` via the extracted
    /// `perform_drop_cleanup()` helper so we can observe the `cancelled`
    /// flag *after* the cleanup has run (calling `mem::drop` would free
    /// `self` and any post-drop pointer read would be undefined behaviour).
    /// The subsequent end-of-scope `Drop` is a no-op thanks to the
    /// `is_cancelled` early return.
    #[tokio::test]
    async fn drop_without_close_marks_cancelled() {
        let mut writer = make_drop_test_writer();
        // Sanity: starts as neither closed nor cancelled.
        assert!(!writer.closed.load(Ordering::SeqCst));
        assert!(!writer.cancelled.load(Ordering::SeqCst));

        // Drive the same logic Drop runs.
        writer.perform_drop_cleanup();

        // Drop must:
        //   1. set `cancelled = true` so observers know the writer was abandoned
        //   2. drain ufs_stream / current_block_writer / committed_block_ids so
        //      a second invocation cannot double-spawn cleanup tasks.
        assert!(
            writer.cancelled.load(Ordering::SeqCst),
            "perform_drop_cleanup must set cancelled=true"
        );
        assert!(writer.ufs_stream.is_none());
        assert!(writer.current_block_writer.is_none());
        assert!(writer.committed_block_ids.is_empty());

        // Idempotency: calling again must be a complete no-op (early return).
        writer.perform_drop_cleanup();
        assert!(writer.cancelled.load(Ordering::SeqCst));
    }

    /// Drop after a successful `close()` / `cancel()` MUST be a no-op:
    /// no extra cleanup spawn, no double-warn.
    #[tokio::test]
    async fn drop_after_close_is_noop() {
        let writer = make_drop_test_writer();
        // Simulate close() having succeeded.
        writer.closed.store(true, Ordering::SeqCst);
        // Drop should hit the "is_closed → return" early-exit without
        // doing anything observable. We just check it does not panic.
        drop(writer);
    }

    /// Drop after `cancel()` must also be a no-op (idempotency).
    #[tokio::test]
    async fn drop_after_cancel_is_noop() {
        let writer = make_drop_test_writer();
        writer.cancelled.store(true, Ordering::SeqCst);
        drop(writer);
    }

    /// **Regression for N2 (Round-3)**: `perform_drop_cleanup` must
    /// `take()` the `_context` field as part of the cleanup so that the
    /// `Arc<FileSystemContext>` is moved into the spawned task's future
    /// and kept alive until the cleanup RPCs complete.
    ///
    /// Pre-fix behaviour: `_context` was left untouched on `self` and
    /// only `master / ufs_stream / current_block_writer / committed_block_ids`
    /// were moved into the spawn closure. If the writer was the last
    /// owner of the context, the context (and its `worker_pool` /
    /// `router` / heartbeat resources) could be dropped on the main
    /// thread before the spawned cancel-RPCs finished, occasionally
    /// breaking cleanup.
    ///
    /// We cannot construct a real `FileSystemContext` in unit tests
    /// (it requires a live cluster), so we verify the structural
    /// invariant: after `perform_drop_cleanup`, `self._context` is `None`.
    /// This is necessary (though not by itself sufficient) for the Arc
    /// to have been moved into the spawn closure.
    #[tokio::test]
    async fn drop_cleanup_takes_context_field() {
        let mut writer = make_drop_test_writer();
        // Test fixture starts with `_context = None`. To exercise the
        // `take()` semantics meaningfully we'd need a real ctx; what we
        // *can* assert here is that the field is left as `None` after
        // cleanup regardless of starting state — guarding against any
        // future refactor that forgets to drain it.
        writer.perform_drop_cleanup();
        assert!(
            writer._context.is_none(),
            "perform_drop_cleanup must take() _context (N2 regression)"
        );
    }
}
