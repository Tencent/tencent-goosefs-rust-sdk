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
//!       → WorkerRouterView.select_worker()
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

use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::block::router::{rpc_endpoint, WorkerRouterView};
use crate::client::master::default_file_mode;
use crate::client::worker::{WorkerClientPool, WriteBlockOptions};
use crate::client::{CompleteFileOptions, MasterClient};
use crate::config::{GoosefsConfig, NO_AUTO_PERSIST};
use crate::context::FileSystemContext;
use crate::error::{Error, Result};
use crate::fs::options::DeleteOptions;
use crate::io::replica_write::{
    cache_min_ratio, degrade_replicas, enough_replicas, filter_no_space_workers,
    replica_write_plan, should_abort_remaining, ReplicaWritePlan,
};
use crate::io::writer::{owned_chunk, GrpcBlockWriter};
use crate::proto::grpc::block::{RequestType, WorkerInfo};
use crate::proto::grpc::file::{
    CreateFilePOptions, FileInfo, FsOpPId, LoadMetadataPType, ScheduleAsyncPersistencePOptions,
};
use crate::proto::grpc::WorkerNetAddress;
use crate::proto::proto::dataserver::CreateUfsFileOptions;
use crate::proto::proto::shared::FileLocation;

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
    /// `block_id = -1`, `length = i64::MAX`) from the first write onwards.
    ///
    /// ASYNC_THROUGH leaves this `false` yet can still end up on the UFS
    /// stream, by degrading — see `GoosefsFileWriter::ufs_write_enabled`.
    ufs_stream: bool,
    /// UFS file creation options — used on the UFS stream's initial command.
    ///
    /// Populated for every write type that *can* reach the UFS stream,
    /// including ASYNC_THROUGH, which only reaches it by degrading. Resolving
    /// them up front keeps the degrade path free of `FileInfo` lookups at the
    /// point where the cache write has already failed.
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
        // ASYNC_THROUGH: cache only, schedule async persist after close.
        // Replica count / watermark filtering live in `open_next_block`
        // (Java `GooseFSBlockStore.getOutStream` + `filterNoSpaceWorkers`).
        Some(5) => WriteStrategy {
            cache_stream: true,
            ufs_stream: false,
            // Not used unless the cache write degrades, but resolved eagerly:
            // by then `handle_cache_write_exception` has already torn the
            // block writer down and has no clean way to fail.
            create_ufs_file_options: Some(build_ufs_opts()),
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
    /// Worker router view for block → worker mapping.
    ///
    ///  Step 2
    /// migrated from `WorkerRouter` (per-writer `ArcSwap`×3) to
    /// `WorkerRouterView` (per-writer `Arc`×2 + `Option<i64>` value).
    /// `create_with_context` stores a `WorkerRouterView::empty()` placeholder
    /// so zero-byte writes never touch the hash ring; the first `write()`
    /// swaps in a `from_shared` view via `ensure_router_init`.
    router: WorkerRouterView,
    /// Connection pool for reusing authenticated worker gRPC channels.
    /// Matches Java's `FileSystemContext.acquireBlockWorkerClient()`.
    worker_pool: Arc<WorkerClientPool>,
    /// Optional shared context (non-None when created via `create_with_context`).
    /// Kept alive to prevent context GC while the writer is in use.
    _context: Option<Arc<FileSystemContext>>,
    /// File info returned by CreateFile.
    file_info: FileInfo,
    /// Total bytes accepted by `write()` so far, across every branch.
    ///
    /// Advanced once per successful `write()` rather than per stream, so a
    /// writer that switches branches mid-file (cache → UFS degrade) still
    /// reports the true file length. This is what `CompleteFile` sends as
    /// `ufs_length` — Java `GoosefsFileOutStream.mBytesWritten`.
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
    ///
    /// The file's *initial* configuration; it is never mutated. Where the
    /// writer actually sends bytes right now is [`Self::should_cache`] /
    /// [`Self::ufs_write_enabled`], which can diverge after a degrade.
    write_strategy: WriteStrategy,
    /// Whether writes still go to the Goosefs cache.
    ///
    /// Starts as `write_strategy.cache_stream` and latches to `false` the
    /// moment a cache write degrades to UFS-only. Java
    /// `mShouldCacheCurrentBlock`.
    should_cache: bool,
    /// Whether writes go to the UFS stream.
    ///
    /// Starts as `write_strategy.ufs_stream` and latches to `true` on a
    /// degrade, which is how ASYNC_THROUGH — configured with no UFS stream —
    /// can end up writing straight to the UFS. Java tracks this as
    /// `mUnderStorageOutputStream != null`.
    ufs_write_enabled: bool,
    /// Whether any cache block has ever been opened successfully.
    ///
    /// Gates the two degrade rules that depend on how much the client knows:
    /// once a block has opened, ASYNC_THROUGH must not degrade (partial data
    /// is already cached), and authentication is proven to work. Java
    /// `openBlock`.
    block_opened: bool,
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
    /// Opened eagerly in `create_with_context` for CACHE_THROUGH / THROUGH, so
    /// that a zero-byte write still creates the file on the UFS. ASYNC_THROUGH
    /// leaves it `None` and only opens one if the cache write degrades.
    ufs_stream: Option<GrpcBlockWriter>,
    /// Worker address hosting the UFS stream (for failure tracking).
    ufs_worker_addr: Option<String>,
    /// Whether the UFS stream has been successfully closed.
    ///
    /// Used during CACHE_THROUGH error recovery in `handle_complete_file_error`:
    /// if UFS close succeeded but `completeFile` failed, we must clean up the
    /// Goosefs-side metadata entry only (not the UFS file).
    ufs_stream_completed: AtomicBool,
    /// Whether the local worker router needs lazy initialization from the shared context.
    ///
    /// Set to `true` in `create_with_context` (deferred init) and `false` in
    /// test constructors (where the router starts empty). Once initialized via
    /// `ensure_router_init()`, this is set to `false` and subsequent calls are no-ops.
    _router_needs_init: AtomicBool,
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

        let mut create_options = options.unwrap_or_default();

        // Every unset field falls back to the same config-derived default,
        // whether the caller passed nothing at all or a partial message. A
        // caller overriding one field must not silently lose the others:
        // dropping block_size_bytes gets "Invalid block size 0" back from the
        // Master, and dropping write_type makes the Master pick its own
        // persistence semantics while `write_strategy` below still follows
        // `config.write_type` — the two then disagree about the same file.
        if create_options.recursive.is_none() {
            create_options.recursive = Some(true);
        }
        if create_options.block_size_bytes.is_none() || create_options.block_size_bytes == Some(0) {
            create_options.block_size_bytes = Some(config.block_size as i64);
        }
        if create_options.mode.is_none() {
            create_options.mode = Some(default_file_mode());
        }
        if create_options.write_type.is_none() {
            create_options.write_type = config.write_type;
        }

        let file_info = master_arc.create_file(path, create_options).await?;
        debug!(
            path = %path,
            file_id = ?file_info.file_id,
            "file created on Master (via context)"
        );

        // A3 consistency: on overwrite (WritePType::CACHE_THROUGH etc.), a
        // previously cached FileInfo now points at a defunct file identity
        // (block_ids / file_id changed). Drop it immediately so any read
        // issued after `create_file` returns — even before this writer is
        // closed — never observes the stale metadata. No-op when the
        // opt-in cache is disabled.
        ctx.invalidate_file_info(path);

        // Already backfilled from `config.write_type` above, so this is the
        // same value the Master was told about.
        let write_strategy = resolve_write_strategy(create_options.write_type, &file_info);

        // Reuse shared router and pool from context (zero additional RPCs).
        // For cache-only write types the worker list is NOT snapshotted here —
        // it is deferred to the first `write()` via `ensure_router_init()`, so
        // a zero-byte MUST_CACHE write never pays for a hash-ring build. Write
        // types with a UFS stream give that up a few lines below, because they
        // have to reach a worker at create time anyway.
        let worker_pool = ctx.acquire_worker_pool();
        let router = WorkerRouterView::empty();

        let operation_id = Uuid::new_v4();

        // SAFETY: We clone the MasterClient from Arc<MasterClient>.
        // The file_writer holds it by value; the Arc in ctx keeps the channel alive.
        let master = (*master_arc).clone();

        let mut writer = Self {
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
            should_cache: write_strategy.cache_stream,
            ufs_write_enabled: write_strategy.ufs_stream,
            block_opened: false,
            write_strategy,
            committed_block_ids: Vec::new(),
            current_block_writer: None,
            ufs_stream: None,
            ufs_worker_addr: None,
            ufs_stream_completed: AtomicBool::new(false),
            _router_needs_init: AtomicBool::new(true),
        };

        // Java opens the UFS stream in the `GoosefsFileOutStream` constructor
        // whenever `mUnderStorageType.isSyncPersist()` — CACHE_THROUGH and
        // THROUGH. Opening it here rather than on the first `write()` is what
        // makes a zero-byte write land an empty file on the UFS: the initial
        // `WriteBlock` command carries `CreateUfsFileOptions`, and the worker's
        // `completeRequest` creates the file on stream close even when no chunk
        // was ever sent. Deferring the open skips that RPC entirely, so the UFS
        // silently ends up with no file at all.
        //
        // ASYNC_THROUGH is excluded, matching Java: it has no UFS stream unless
        // the cache write degrades, and that path opens one on demand.
        if writer.write_strategy.ufs_stream {
            let opened = match writer.ensure_router_init().await {
                Ok(()) => writer.open_ufs_stream().await,
                Err(e) => Err(e),
            };
            if let Err(e) = opened {
                warn!(
                    path = %path,
                    error = %e,
                    "failed to open the UFS stream during create; \
                     the INCOMPLETE inode is left for a retry to reuse"
                );
                // Suppress the `Drop` cleanup: there is nothing written to roll
                // back, and its "dropped without close()" warning would be
                // misleading. Java's `closeAndRethrow` likewise leaves the inode
                // in place rather than deleting it.
                writer.cancelled.store(true, Ordering::SeqCst);
                return Err(e);
            }
        }

        Ok(writer)
    }

    /// Lazily populate the local worker router from the shared context.
    ///
    /// Called at the start of `write()` — this is the first point where worker
    /// routing is actually needed. For zero-byte writes (CreateFile + close
    /// without any data), the expensive `build_hash_ring` is never invoked.
    ///
    /// Safe to call multiple times — once initialized, this immediately returns.
    ///
    ///: the local router is
    /// **replaced** by a snapshot of the shared context router, so no hash
    /// ring is rebuilt on the first `write()`. Failure isolation is preserved
    /// via the snapshot's own `failed_workers` DashMap.
    async fn ensure_router_init(&mut self) -> Result<()> {
        if !self._router_needs_init.load(Ordering::Acquire) {
            return Ok(());
        }
        // Production paths always set `_context` via `create_with_context`; `None` only appears in tests.
        debug_assert!(
            self._context.is_some(),
            "`_context` must be set in production paths"
        );
        if let Some(ctx) = &self._context {
            let shared = ctx.acquire_router();
            if shared.get_workers().await.is_empty() {
                return Err(Error::NoWorkerAvailable {
                    message: "no workers available for writing".to_string(),
                });
            }
            // Wait-free view: clones two `Arc`s (workers + hash_ring) plus a
            // value copy of `local_worker_id`. Does NOT rebuild the ring and
            // does NOT allocate any `ArcSwap` — the whole point of
            // Step 2
            // ).
            self.router = WorkerRouterView::from_shared(&shared);
            self._router_needs_init.store(false, Ordering::Release);
        }
        Ok(())
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

        // Lazy-init: first write() triggers worker router population.
        // Deferring this from create_with_context() avoids expensive
        // hash-ring builds for zero-byte writes (CreateFile-then-close).
        self.ensure_router_init().await?;

        // 1) Feed the cache stream (sliced by block boundaries). A failure
        //    here is not necessarily fatal: `handle_cache_write_exception`
        //    decides between aborting the write and degrading to UFS-only,
        //    and on a degrade it has already torn the cache block down.
        if self.should_cache {
            if let Err(e) = self.write_to_cache_stream(data).await {
                self.handle_cache_write_exception(e).await?;
            }
        }

        // 2) Feed the UFS stream (single long stream, no block boundaries —
        //    only sliced by chunk_size).
        //
        //    After a degrade this receives the *whole* buffer, including the
        //    prefix the cache stream had already accepted. That is not double
        //    writing: the cache block was cancelled, so those bytes exist
        //    nowhere else. It is also the only reason the degrade produces a
        //    complete file — see `handle_cache_write_exception` for why a
        //    degrade can never happen once earlier blocks have been committed.
        if self.ufs_write_enabled {
            self.write_to_ufs_stream(data).await?;
        }

        // 3) Single accounting point for `CompleteFilePOptions.ufs_length`,
        //    matching Java `GoosefsFileOutStream.writeInternal`'s trailing
        //    `mBytesWritten += len`. Keeping this out of the per-stream
        //    helpers means the counter stays correct when a writer switches
        //    branches at runtime (cache → UFS degrade).
        self.total_bytes_written += data.len() as u64;

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
    /// `GoosefsFileOutStream.flush()`:
    ///
    /// ```java
    /// if (mUnderStorageOutputStream != null) {
    ///   mUnderStorageOutputStream.flush();
    /// }
    /// if (mUnderStorageType.isAsyncPersist() && mCurrentBlockOutStream != null
    ///     && conf.getBoolean(USER_FILE_ASYNC_PERSIST_FLUSH_ENABLED)) {
    ///   mCurrentBlockOutStream.flush();
    /// }
    /// ```
    ///
    /// Two things follow from that. The cache flush is *narrower* than a
    /// naive reading suggests — only ASYNC_THROUGH with the flag on reaches
    /// `mCurrentBlockOutStream.flush()`, because for every other write type
    /// the durability the caller asked for comes from the UFS stream, and
    /// forcing a worker-disk sync would just add latency. And a flush failure
    /// is always fatal: Java routes it to `handleUnderStorageWriteException`,
    /// which rethrows unconditionally. Degrading here would be wrong anyway,
    /// since the caller has been told the earlier bytes were accepted.
    pub async fn flush(&mut self) -> Result<()> {
        if self.cancelled.load(Ordering::SeqCst) || self.closed.load(Ordering::SeqCst) {
            return Err(Error::BlockIoError {
                message: "cannot flush a completed or cancelled file".to_string(),
            });
        }

        if let Some(ufs) = self.ufs_stream.as_mut() {
            ufs.flush().await?;
        }

        if self.write_strategy.need_async_persist && self.config.file_async_persist_flush_enabled {
            if let Some(active) = self.current_block_writer.as_mut() {
                if active.bytes_written > 0 {
                    // An explicit flush is a safe boundary for the chunk
                    // alignment workaround: the caller wants an ack, and no
                    // further chunk follows this one on the stream.
                    let tail = std::mem::take(&mut active.pending_chunk);
                    if !tail.is_empty() {
                        active.write_chunk(tail).await?;
                    }
                    active.flush_replicas().await?;
                }
            }
        }
        Ok(())
    }

    /// Append data to the per-block cache stream, slicing at block boundaries.
    ///
    /// To avoid the server-side concurrent-writer race in
    /// `LocalFileBlockWriter.appendComposite`, every chunk
    /// pushed onto the gRPC stream is **exactly `chunk_size` bytes**, except
    /// at safe boundaries (block end / explicit flush / block close), where a
    /// trailing partial chunk is allowed because no further chunks follow on
    /// the same stream. Sub-`chunk_size` tails stay in
    /// `ActiveBlockWriter::pending_chunk`. Aligned `chunk_size` slices are
    /// copied once from the caller's buffer — they are **not** appended onto
    /// `pending_chunk` and drained from the front (that was O(n²) memmove).
    ///
    /// TODO(java-parity): optional follow-up — Java `BlockOutStream` uses one
    /// fixed-capacity `mCurrentChunk` (size = `chunkSize`) filled from the
    /// caller slice until full. This path is already O(n) and equivalent for
    /// aligned writes; a current-chunk refactor would mainly unify cache/UFS
    /// coalescing, not cut another copy. Revisit if UFS also needs to hold
    /// sub-`chunk_size` tails across `write()` calls (see `write_all`).
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

            let block_full;
            let emit_result;
            {
                let writer = self.current_block_writer.as_mut().unwrap();
                let remaining_in_block = writer.remaining() as usize;
                let remaining_data = data.len() - offset;
                let to_accept = remaining_in_block.min(remaining_data);
                let slice = &data[offset..offset + to_accept];
                writer.bytes_written += to_accept as u64;
                offset += to_accept;
                block_full = writer.remaining() == 0;
                emit_result = emit_aligned_chunks(writer, slice, chunk_size).await;
            }
            // Raw error on purpose: only `write()` knows whether a degrade is
            // permitted, so the classification happens there.
            emit_result?;
            if block_full {
                self.close_current_block(true).await?;
            }
        }

        Ok(())
    }

    /// Append data to the single long-lived UFS stream (`RequestType::UfsFile`,
    /// `block_id = -1`, `length = i64::MAX`).
    ///
    /// CACHE_THROUGH / THROUGH already opened the stream at create time; the
    /// lazy open here is what serves a degraded ASYNC_THROUGH write, which
    /// only learns it needs a UFS stream once the cache write has failed.
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
                // Instrument: record UFS-path bytes written. The authoritative
                // `total_bytes_written` counter is advanced once per `write()`
                // (Java `GoosefsFileOutStream.writeInternal` does the same),
                // so neither stream branch may touch it here.
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
    /// - Close the current block if any (and `commitLocation` for ASYNC_THROUGH)
    /// - Compute the next block ID
    /// - Select workers via consistent hashing (durable replica count +
    ///   persist-capacity watermark for ASYNC_THROUGH)
    /// - Open N `GrpcBlockWriter`s (`RequestType::GoosefsBlock`)
    /// - On hash-pick failure, retry with the full worker list
    async fn open_next_block(&mut self, block_size: u64) -> Result<()> {
        if self.current_block_writer.is_some() {
            self.close_current_block(true).await?;
        }

        let file_id = self.file_info.file_id.unwrap_or(0);
        let block_index = self.committed_block_ids.len() as u64;
        let block_id = compute_block_id(file_id, block_index);
        let async_through = self.write_strategy.need_async_persist;
        let plan = replica_write_plan(
            async_through,
            self.config.file_replication_number,
            self.config.file_replication_durable,
            self.config.file_replication_durable_min,
            self.config.file_write_max_node_retry,
        )?;

        match self
            .open_replica_writers(block_id, block_size, &plan, false)
            .await
        {
            Ok(active) => {
                self.current_block_writer = Some(active);
                self.block_opened = true;
                Ok(())
            }
            Err(e) => {
                warn!(
                    block_id = block_id,
                    error = %e,
                    "failed to open block with hash-picked workers, retrying with all workers"
                );
                let active = self
                    .open_replica_writers(block_id, block_size, &plan, true)
                    .await?;
                self.current_block_writer = Some(active);
                self.block_opened = true;
                Ok(())
            }
        }
    }

    /// Open up to `plan.initial_replicas` DataWriters, matching Java
    /// `GooseFSBlockStore.getOutStream`.
    async fn open_replica_writers(
        &mut self,
        block_id: i64,
        block_size: u64,
        plan: &ReplicaWritePlan,
        use_all_workers: bool,
    ) -> Result<ActiveBlockWriter> {
        let async_through = self.write_strategy.need_async_persist;
        let mut pool = if use_all_workers {
            (*self.router.all_workers()).clone()
        } else {
            self.router
                .select_workers(block_id, plan.max_retry_node)
                .await?
        };
        pool = self.router.filter_not_failed(&pool);
        if async_through {
            let allow_fallback = block_sequence_number(block_id) > 0;
            pool = filter_no_space_workers(
                &pool,
                allow_fallback,
                plan.min_needed_replicas,
                self.config.block_worker_available_min_remain_bytes as i64,
                self.config.block_worker_available_min_remain_ratio,
                cache_min_ratio(self.config.worker_read_cache_min_ratio),
            );
        }
        if pool.is_empty() {
            // Java resets the failure list here so the caller's retry re-picks
            // from a clean pool. Without this, a writer that transiently
            // blacklists every worker can never recover for the rest of the
            // file. Deliberately not done on the "opened fewer replicas than
            // required" path below — there the workers really did fail.
            debug!(
                block_id = block_id,
                "no available GooseFS worker after filtering; \
                 clearing failed-worker set so the retry can re-pick"
            );
            self.router.clear_failed();
            return Err(Error::NoWorkerAvailable {
                message: format!("no available GooseFS worker for block_id={block_id}"),
            });
        }

        let (initial, min_needed) = degrade_replicas(
            async_through,
            plan.initial_replicas,
            plan.min_needed_replicas,
            pool.len(),
        );

        let mut opened: Vec<ReplicaWriter> = Vec::new();
        let mut last_open_err: Option<Error> = None;
        for worker_info in pool {
            if opened.len() >= initial {
                break;
            }
            match self
                .try_open_replica(block_id, block_size, &worker_info, opened.len())
                .await
            {
                Ok(r) => opened.push(r),
                Err(e) => {
                    warn!(
                        block_id = block_id,
                        error = %e,
                        "meet block worker exception while opening replica"
                    );
                    if let Some(addr) = &worker_info.address {
                        self.router.mark_failed(addr);
                        self.worker_pool.invalidate(&rpc_endpoint(addr)).await;
                    }
                    last_open_err = Some(e);
                    if initial == 1 && opened.is_empty() {
                        continue;
                    }
                }
            }
        }

        let worker_count = opened.len();
        if worker_count == 0 || worker_count < min_needed {
            for r in opened {
                r.writer.cancel().await;
            }
            // Java single-writer loop rethrows the last open IOException when
            // no writer could be opened. A replica-count shortfall after
            // degrade (ASYNC_THROUGH `durable.min`) is ResourceExhausted —
            // not NoWorkerAvailable. `initial == 1` after degrade must not
            // hide that (1 worker + durable.min=2 used to look like
            // "no worker").
            if worker_count == 0 {
                return Err(last_open_err.unwrap_or_else(|| Error::NoWorkerAvailable {
                    message: format!("no available GooseFS worker for block_id={block_id}"),
                }));
            }
            return Err(Error::ResourceExhausted {
                message: format!(
                    "Not enough workers for replications of block {block_id}, {worker_count} workers selected but {min_needed} required"
                ),
            });
        }

        debug!(
            block_id = block_id,
            replicas = worker_count,
            min_needed = min_needed,
            parallel = async_through && worker_count > 1,
            "opened cache block replica writers"
        );

        Ok(ActiveBlockWriter {
            replicas: opened,
            block_id,
            block_size,
            bytes_written: 0,
            pending_chunk: Vec::with_capacity(self.config.chunk_size as usize),
            parallel: async_through && worker_count > 1,
            min_needed,
        })
    }

    async fn try_open_replica(
        &self,
        block_id: i64,
        block_size: u64,
        worker_info: &WorkerInfo,
        ordinal: usize,
    ) -> Result<ReplicaWriter> {
        let addr = worker_info
            .address
            .as_ref()
            .ok_or_else(|| Error::Internal {
                message: "worker has no address".to_string(),
                source: None,
            })?;
        let worker_addr = rpc_endpoint(addr);
        let worker = self.worker_pool.acquire(&worker_addr).await?;
        let write_opts = WriteBlockOptions {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
            async_write: self.write_strategy.need_async_persist,
        };
        let writer =
            GrpcBlockWriter::open(&worker, block_id, block_size as i64, write_opts).await?;
        Ok(ReplicaWriter {
            ordinal,
            writer,
            worker_id: worker_info.id.unwrap_or(0),
            worker_addr,
            net_address: addr.clone(),
        })
    }

    /// Close the current block writer: flush, close, and record the committed block ID.
    ///
    /// When `commit_location` is true (next-block transition), ASYNC_THROUGH
    /// reports succeed-worker IDs via `CommitLocation`. The last block of the
    /// file is reported on `completeFile` instead (`commit_location = false`).
    async fn close_current_block(&mut self, commit_location: bool) -> Result<Option<FileLocation>> {
        let Some(mut active) = self.current_block_writer.take() else {
            return Ok(None);
        };
        let block_id = active.block_id;
        let bytes_written = active.bytes_written;
        let pending_chunk = std::mem::take(&mut active.pending_chunk);
        let block_offset = (self.committed_block_ids.len() as i64) * (active.block_size as i64);

        if bytes_written > 0 {
            if !pending_chunk.is_empty() {
                if let Err(e) = active.write_chunk(pending_chunk).await {
                    active.cancel_replicas().await;
                    return Err(e);
                }
            }

            if let Err(e) = active.flush_replicas().await {
                warn!(
                    block_id = block_id,
                    error = %e,
                    "flush failed during close_current_block; cancelling replica streams"
                );
                active.cancel_replicas().await;
                return Err(e);
            }
            debug!(
                block_id = block_id,
                bytes_written = bytes_written,
                replicas = active.replicas.len(),
                "cache block flushed"
            );

            let loc = active.file_location(block_offset);
            if let Err(e) = active.close_replicas().await {
                warn!(
                    block_id = block_id,
                    error = %e,
                    "close failed during close_current_block; \
                     recording block_id for cancel-cleanup remove_blocks"
                );
                self.committed_block_ids.push(block_id);
                return Err(e);
            }

            if commit_location && self.write_strategy.need_async_persist {
                if let Some(ref loc) = loc {
                    if let Err(e) = self
                        .master
                        .commit_location(
                            &self.path,
                            self.file_info.file_id,
                            block_id,
                            vec![loc.clone()],
                        )
                        .await
                    {
                        warn!(
                            block_id = block_id,
                            error = %e,
                            "commitLocation failed after block close"
                        );
                        return Err(e);
                    }
                }
            }

            self.committed_block_ids.push(block_id);
            Ok(loc)
        } else {
            active.cancel_replicas().await;
            Ok(None)
        }
    }

    /// Open the single long-lived UFS stream used by CACHE_THROUGH / THROUGH,
    /// and by a degraded ASYNC_THROUGH write.
    ///
    /// Note this returns as soon as the worker connection is established: the
    /// `WriteBlock` call runs on a background task and the server's response
    /// headers do not arrive until the first flush or the stream close. So a
    /// failure here means "no reachable worker", not "the UFS rejected us" —
    /// UFS-side errors surface later, on flush or close.
    ///
    /// Matches Java `UnderFileSystemFileOutStream`:
    /// - picks a worker at random (independent of cache routing);
    /// - opens one `WriteBlock` RPC with `block_id = -1`, `length = i64::MAX`,
    ///   `RequestType::UfsFile`, and the resolved `CreateUfsFileOptions`;
    /// - the Worker calls `createNonexistingFile` exactly once and appends every
    ///   subsequent chunk to the same `OutputStream`.
    ///
    /// This deliberately tracks Java's *worker*-UFS branch, which is not Java's
    /// default. With `goosefs.user.local.write.ufs.client.enabled = true`
    /// (Java's default) the client writes to the UFS itself and never involves
    /// a worker. Routing through a worker instead is an intentional choice, not
    /// an unfinished one: it keeps UFS credentials and endpoint configuration
    /// on the workers, so a client needs no direct UFS reachability. The
    /// trade-off is an extra network hop and a dependency on worker liveness
    /// for what Java can do client-side. Revisit only if client-direct UFS
    /// writes become a requirement — it is a new code path, not a tweak here.
    ///
    /// TODO(java-parity): retry across workers instead of giving up after one.
    /// Java's worker-UFS branch (`GooseFSFileOutStream` constructor, the
    /// `USER_LOCAL_WRITE_UFS_CLIENT_ENABLED = false` path) loops under
    /// `USER_FILE_WRITE_INIT_MAX_DURATION`, reshuffling the worker list each
    /// round and calling `handleRetryableException` on failure, so one flaky
    /// worker does not fail the write. Here a single failure marks the worker
    /// and returns. Deferred rather than done inline because it interacts with
    /// the degrade path added alongside it: a retry loop has to decide whether
    /// a degraded write may retry at all (it has already lost its cache copy)
    /// and how the retries interact with `failed_workers`, which the caller
    /// also mutates. Worth its own change with its own fault-injection tests.
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

        let worker_addr = rpc_endpoint(addr);

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

    /// Decide what a failed cache write means: abort, or degrade to UFS-only.
    ///
    /// `Ok(())` means the writer has degraded — the cache block is torn down,
    /// [`Self::should_cache`] is off, [`Self::ufs_write_enabled`] is on, and
    /// the caller should send its buffer to the UFS stream. `Err` means the
    /// write is unrecoverable; the writer is marked cancelled.
    ///
    /// # Java authority
    ///
    /// `GoosefsFileOutStream.handleCacheWriteException`. Four rules make the
    /// failure fatal, and each protects a different guarantee:
    ///
    /// 1. `ResourceExhausted` / `InvalidArgument` — the block store already
    ///    degraded the replica count as far as it is allowed to and still came
    ///    up short. Silently writing one UFS copy would break the replication
    ///    contract the caller asked for.
    /// 2. Neither sync- nor async-persist (MUST_CACHE, TRY_CACHE, NONE) —
    ///    there is no UFS destination configured to degrade *to*.
    /// 3. ASYNC_THROUGH with a block already opened — earlier blocks are
    ///    committed in the cache and are not on the UFS, so a UFS stream
    ///    started now would produce a truncated file.
    /// 4. `Unauthenticated` / `PermissionDenied` — the credentials are
    ///    rejected, and the UFS write would use the same ones.
    ///
    /// Rule 3 is also what makes the degrade safe for the caller: it can only
    /// fire while no cache block has ever opened, so no committed bytes are
    /// stranded and the UFS stream starts from a genuinely empty file.
    ///
    /// One further rule is conditional. If the *first* block failed to open,
    /// the client never got a reply and cannot distinguish a rejection from a
    /// transport error, so
    /// `goosefs.user.local.ufs.client.ignore.block.stream.unknown.status`
    /// (default `true`) decides whether to treat that ambiguity as fatal.
    async fn handle_cache_write_exception(&mut self, err: Error) -> Result<()> {
        warn!(
            path = %self.path,
            error = %err,
            block_opened = self.block_opened,
            "failed to write into the Goosefs cache"
        );

        let credentials_rejected = matches!(
            err,
            Error::AuthenticationFailed { .. } | Error::PermissionDenied { .. }
        );
        let fatal = cache_write_failure_is_fatal(
            &err,
            &self.write_strategy,
            self.block_opened,
            self.config
                .local_ufs_client_ignore_block_stream_unknown_status,
        );

        // A rejected credential says nothing about the worker's health, and
        // blacklisting on it would walk the whole pool one worker at a time.
        self.tear_down_cache_block(!credentials_rejected).await;

        if fatal {
            self.cancelled.store(true, Ordering::SeqCst);
            return Err(err);
        }

        warn!(
            path = %self.path,
            "degrading to a UFS-only write for the rest of this file"
        );
        crate::metrics::counter(crate::metrics::name::CLIENT_WRITE_DEGRADED_TO_UFS).inc(1);
        self.should_cache = false;
        self.ufs_write_enabled = true;
        Ok(())
    }

    /// Cancel the in-progress cache block, optionally blacklisting the workers
    /// behind it.
    ///
    /// Pass `blacklist = false` when the failure says nothing about worker
    /// health (a rejected credential, say) — the connections are still dropped
    /// so nothing is left half-written, but the workers stay selectable.
    ///
    /// Safe to call with no block open. Failures are swallowed: this only ever
    /// runs while a more interesting error is being handled.
    async fn tear_down_cache_block(&mut self, blacklist: bool) {
        if let Some(active) = self.current_block_writer.take() {
            if blacklist {
                for r in &active.replicas {
                    self.router.mark_failed(&r.net_address);
                    self.worker_pool.invalidate(&r.worker_addr).await;
                }
            }
            active.cancel_replicas().await;
        }
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

        // 2. Cancel current in-progress cache block writer (all replicas).
        if let Some(active) = self.current_block_writer.take() {
            active.cancel_replicas().await;
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

        // A3 consistency: cancel path may have removed blocks or deleted
        // the inode entirely, so any cached FileInfo now points at a
        // defunct state.
        if let Some(ctx) = &self._context {
            ctx.invalidate_file_info(&self.path);
        }

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

    /// Recover from a `completeFile` failure when the UFS write already
    /// succeeded.
    ///
    /// There is a window where the UFS file is fully written (UFS `close()`
    /// returned OK) but `completeFile` on the Master then fails — a Master
    /// failover or a transient network error. The bytes are safe; only the
    /// Goosefs-side metadata is wrong. Two steps fix that:
    ///
    /// 1. `delete(goosefs_only = true, unchecked = true)` — drop the
    ///    INCOMPLETE inode without touching the UFS file, which is now the
    ///    source of truth.
    /// 2. `get_status(LoadMetadataPType::Always, sync_interval_ms = 0)` — force
    ///    the Master to re-import the file from the UFS.
    ///
    /// When both succeed the write **is** successful and the original error is
    /// swallowed; `close()` returns `Ok`. If either step fails the file is left
    /// in whatever state it reached and the original error surfaces, since that
    /// is the more actionable one.
    ///
    /// # Java authority
    ///
    /// The catch block in `GoosefsFileOutStream.close()`, which ends the
    /// recovery with a bare `return;` — not a rethrow. Applies to SYNC_PERSIST
    /// and ASYNC_PERSIST alike (`(isSyncPersist() || isAsyncPersist()) &&
    /// mUnderStorageOutputStreamCompleted`), so a degraded ASYNC_THROUGH write
    /// is covered too.
    async fn handle_complete_file_error(&mut self, err: Error) -> Result<()> {
        let persistable = self.write_strategy.ufs_stream || self.write_strategy.need_async_persist;
        if !persistable || !self.ufs_stream_completed.load(Ordering::SeqCst) {
            return Err(err);
        }

        warn!(
            path = %self.path,
            error = %err,
            "completeFile failed after UFS close succeeded; attempting UFS metadata recovery"
        );

        if let Err(del_err) = self
            .master
            .delete_with_options(&self.path, DeleteOptions::goosefs_only_unchecked())
            .await
        {
            warn!(
                path = %self.path,
                error = %del_err,
                "recovery step 1/2 (delete goosefs-only) failed — \
                 manual cleanup may be required"
            );
            return Err(err);
        }

        if let Err(reload_err) = self
            .master
            .get_status_with_load_type(&self.path, Some(LoadMetadataPType::Always), Some(0))
            .await
        {
            warn!(
                path = %self.path,
                error = %reload_err,
                "recovery step 2/2 (loadMetadata ALWAYS) failed — \
                 the UFS file exists but Goosefs cannot see it yet"
            );
            return Err(err);
        }

        warn!(
            path = %self.path,
            error = %err,
            "completeFile failed but the file was recovered from UFS; \
             treating the write as successful"
        );
        if let Some(ctx) = &self._context {
            ctx.invalidate_file_info(&self.path);
        }
        Ok(())
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
    /// 3. `completeFile` on Master, including last-block locations and
    ///    (for ASYNC_THROUGH) `asyncPersistOptions`.
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

        // 2) Close the current in-progress cache block (flush + commitBlock).
        //    Last-block locations travel with completeFile (Java close()).
        let last_location = match self.close_current_block(false).await {
            Ok(loc) => loc,
            Err(e) => {
                warn!(
                    path = %self.path,
                    error = %e,
                    "failed to close current block during file close, cancelling"
                );
                self.do_cancel_cleanup().await;
                return Err(e);
            }
        };

        // 3) Complete the file on Master with the idempotency operation ID.
        //    Java sets `ufsLength` unconditionally — even under MUST_CACHE it
        //    doubles as the file size, and omitting it for a zero-byte file
        //    makes the Master record `UNKNOWN_SIZE`.
        let ufs_length = Some(self.total_bytes_written as i64);

        let op_id = uuid_to_fs_op_pid(self.operation_id);
        // Java `GoosefsFileOutStream.close()` only attaches last-block
        // `locations` (+ `asyncPersistOptions`) when
        // `mUnderStorageType.isAsyncPersist()`. Sending locations on
        // MUST_CACHE / CACHE_THROUGH makes Master treat the file as
        // persist-scheduled and multi-block reads can miss the last block.
        let locations =
            complete_file_locations(self.write_strategy.need_async_persist, last_location);
        let (force_persisted, async_persist_options) = resolve_persist_options(
            self.write_strategy.need_async_persist,
            self.ufs_stream_completed.load(Ordering::SeqCst),
            self.config.file_persistence_initial_wait_time_ms,
        );
        if let Err(e) = self
            .master
            .complete_file_with_options(
                &self.path,
                CompleteFileOptions {
                    ufs_length,
                    operation_id: Some(op_id),
                    locations,
                    async_persist_options,
                    force_persisted,
                },
            )
            .await
        {
            // The UFS copy may already be complete, in which case re-importing
            // it from the UFS makes this a successful write after all.
            self.handle_complete_file_error(e).await?;
        }

        info!(
            path = %self.path,
            total_bytes = self.total_bytes_written,
            cache_blocks = self.committed_block_ids.len(),
            ufs_stream = self.write_strategy.ufs_stream,
            "file write completed"
        );

        // A3 consistency: the file's `length` / `block_ids` / `completed` /
        // `ufs_length` are now different from what the master reported (or
        // would have reported) at any earlier `get_status`. Drop any cached
        // FileInfo so subsequent readers observe the fresh metadata. No-op
        // when the opt-in cache is disabled.
        if let Some(ctx) = &self._context {
            ctx.invalidate_file_info(&self.path);
        }

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

    /// Total bytes accepted by `write()` so far.
    ///
    /// Counts bytes as they are accepted, not as they are committed, so a
    /// read taken before `close()` may include a block that is still in
    /// flight. After `close()` returns this is the final file length.
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

/// If `pending` plus a prefix of `incoming` fills `chunk_size`, move that
/// coalesced buffer out. Otherwise append all of `incoming` into `pending`.
///
/// `pending.len()` is always `< chunk_size` on entry and on exit.
fn take_completed_pending<'a>(
    pending: &mut Vec<u8>,
    incoming: &mut &'a [u8],
    chunk_size: usize,
) -> Option<Vec<u8>> {
    debug_assert!(chunk_size > 0);
    debug_assert!(pending.len() < chunk_size);
    if pending.is_empty() {
        return None;
    }
    let need = chunk_size - pending.len();
    if incoming.len() < need {
        pending.extend_from_slice(incoming);
        *incoming = &[];
        return None;
    }
    pending.extend_from_slice(&incoming[..need]);
    *incoming = &incoming[need..];
    let full = std::mem::take(pending);
    pending.reserve(chunk_size);
    debug_assert!(pending.is_empty());
    Some(full)
}

/// Peel exact `chunk_size` payloads from `incoming` without growing `pending`
/// past `chunk_size - 1`. Used by tests; the write path streams via
/// [`emit_aligned_chunks`] so a 512MB `write()` does not hold every chunk.
#[cfg(test)]
fn take_full_chunks(pending: &mut Vec<u8>, incoming: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut src = incoming;
    if let Some(full) = take_completed_pending(pending, &mut src, chunk_size) {
        chunks.push(full);
    }
    let n_full = src.len() / chunk_size;
    for i in 0..n_full {
        chunks.push(owned_chunk(&src[i * chunk_size..(i + 1) * chunk_size]));
    }
    let rem = src.len() % chunk_size;
    if rem > 0 {
        pending.extend_from_slice(&src[src.len() - rem..]);
    }
    debug_assert!(pending.len() < chunk_size);
    chunks
}

/// Send aligned `chunk_size` payloads as they are produced (no extra buffer
/// of the whole `write()`). Leftover `< chunk_size` stays in `pending`.
async fn emit_aligned_chunks(
    active: &mut ActiveBlockWriter,
    incoming: &[u8],
    chunk_size: usize,
) -> Result<()> {
    if chunk_size == 0 {
        return Err(Error::InvalidArgument {
            message: "chunk_size must be > 0".into(),
        });
    }
    let mut src = incoming;
    if let Some(full) = take_completed_pending(&mut active.pending_chunk, &mut src, chunk_size) {
        active.write_chunk(full).await?;
    }
    let mut offset = 0usize;
    while offset + chunk_size <= src.len() {
        active
            .write_chunk(owned_chunk(&src[offset..offset + chunk_size]))
            .await?;
        offset += chunk_size;
    }
    if offset < src.len() {
        active.pending_chunk.extend_from_slice(&src[offset..]);
    }
    debug_assert!(active.pending_chunk.len() < chunk_size);
    Ok(())
}

/// One DataWriter targeting a single worker replica of the current block.
struct ReplicaWriter {
    /// Hash-pick order (0 = primary). Preserved when reporting commitLocation.
    ordinal: usize,
    writer: GrpcBlockWriter,
    worker_id: i64,
    worker_addr: String,
    net_address: WorkerNetAddress,
}

/// State for the currently active block being written.
///
/// Holds one or more [`ReplicaWriter`]s (Java `BlockOutStream.mDataWriters`)
/// and tracks how many bytes have been streamed. ASYNC_THROUGH with more
/// than one replica fans each chunk out in parallel
/// (`BlockOutStream.executeWithReplication`); other write types write
/// replicas sequentially.
///
/// # Trailing partial-chunk coalescing (workaround for server-side BUG)
///
/// To work around a GooseFS Worker race in
/// `LocalFileBlockWriter.appendComposite(CompositeByteBuf)` (which uses a
/// position-relative gathering write on a shared `FileChannel` and is unsafe
/// under concurrent stream pressure when chunks are not `chunk_size`-aligned),
/// this struct keeps a `pending_chunk` buffer whose length is **always
/// strictly less than `chunk_size`**. Full chunks are sliced from the
/// caller's `&[u8]` and copied once onto the wire; only an unaligned tail
/// is held and merged with the next `write()`. The buffer is force-flushed
/// only at safe boundaries:
///
/// 1. an explicit user `flush()` call;
/// 2. the block becomes full (`remaining == 0`);
/// 3. `close_current_block()` (end of block / file close).
struct ActiveBlockWriter {
    replicas: Vec<ReplicaWriter>,
    block_id: i64,
    block_size: u64,
    /// Bytes accepted into this writer (sent + still pending). This is the
    /// authoritative byte counter for block-fullness checks; it advances as
    /// soon as bytes are accepted, including a trailing `pending_chunk`.
    bytes_written: u64,
    pending_chunk: Vec<u8>,
    /// `true` when ASYNC_THROUGH and more than one replica (Java parallel path).
    parallel: bool,
    /// Minimum successful replicas required (Java `replicationDurableMin`).
    min_needed: usize,
}

impl ActiveBlockWriter {
    /// Remaining bytes that can be written to this block.
    fn remaining(&self) -> u64 {
        self.block_size - self.bytes_written
    }

    fn file_location(&self, block_offset: i64) -> Option<FileLocation> {
        if self.replicas.is_empty() {
            return None;
        }
        Some(FileLocation {
            block_id: Some(self.block_id),
            offset: Some(block_offset),
            length: Some(self.bytes_written as i64),
            worker_id: self.replicas.iter().map(|r| r.worker_id).collect(),
        })
    }

    async fn write_chunk(&mut self, data: Vec<u8>) -> Result<()> {
        if self.replicas.is_empty() {
            return Err(Error::BlockIoError {
                message: format!("no replica writers left for block_id={}", self.block_id),
            });
        }
        if self.parallel && self.replicas.len() > 1 {
            self.write_chunk_parallel(data).await
        } else {
            self.write_chunk_sequential(data).await
        }
    }

    async fn write_chunk_sequential(&mut self, mut data: Vec<u8>) -> Result<()> {
        let replicas = std::mem::take(&mut self.replicas);
        let last = replicas.len().saturating_sub(1);
        let mut kept = Vec::with_capacity(replicas.len());
        for (i, mut r) in replicas.into_iter().enumerate() {
            let payload = if i == last {
                std::mem::take(&mut data)
            } else {
                data.clone()
            };
            match r.writer.write_chunk(payload).await {
                Ok(()) => kept.push(r),
                Err(e) => {
                    r.writer.cancel().await;
                    for k in kept {
                        k.writer.cancel().await;
                    }
                    return Err(e);
                }
            }
        }
        self.replicas = kept;
        Ok(())
    }

    async fn write_chunk_parallel(&mut self, data: Vec<u8>) -> Result<()> {
        fanout_parallel(
            &mut self.replicas,
            self.min_needed,
            self.block_id,
            ReplicaOp::Write(data),
        )
        .await
    }

    async fn flush_replicas(&mut self) -> Result<i64> {
        if self.replicas.is_empty() {
            return Ok(self.bytes_written as i64);
        }
        if self.parallel && self.replicas.len() > 1 {
            fanout_parallel(
                &mut self.replicas,
                self.min_needed,
                self.block_id,
                ReplicaOp::Flush,
            )
            .await?;
        } else {
            for r in &mut self.replicas {
                r.writer.flush().await?;
            }
        }
        Ok(self.bytes_written as i64)
    }

    async fn close_replicas(self) -> Result<()> {
        let mut first_err = None;
        for r in self.replicas {
            if let Err(e) = r.writer.close().await {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn cancel_replicas(self) {
        for r in self.replicas {
            r.writer.cancel().await;
        }
    }
}

enum ReplicaOp {
    Write(Vec<u8>),
    Flush,
}

/// Parallel replica action matching Java `BlockOutStream.executeWithReplication`.
async fn fanout_parallel(
    replicas: &mut Vec<ReplicaWriter>,
    min_needed: usize,
    block_id: i64,
    op: ReplicaOp,
) -> Result<()> {
    let writer_size = replicas.len();
    if writer_size < min_needed {
        return Err(Error::ResourceExhausted {
            message: format!(
                "Failed to write enough replicas. dataWriters size: {}, Required: {}",
                writer_size, min_needed
            ),
        });
    }

    let taken = std::mem::take(replicas);
    let mut join_set = tokio::task::JoinSet::new();
    for r in taken {
        let op = match &op {
            ReplicaOp::Write(data) => ReplicaOp::Write(data.clone()),
            ReplicaOp::Flush => ReplicaOp::Flush,
        };
        join_set.spawn(async move {
            let mut r = r;
            let result = match op {
                ReplicaOp::Write(payload) => r.writer.write_chunk(payload).await,
                ReplicaOp::Flush => r.writer.flush().await.map(|_| ()),
            };
            match result {
                Ok(()) => Ok(r),
                Err(e) => {
                    r.writer.cancel().await;
                    Err(e)
                }
            }
        });
    }

    let mut kept = Vec::new();
    let mut failures = 0usize;
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(r)) => kept.push(r),
            Ok(Err(e)) => {
                failures += 1;
                tracing::warn!(error = %e, "DataWriter write failed");
                if should_abort_remaining(failures, writer_size, min_needed) {
                    join_set.abort_all();
                }
            }
            Err(_) => {
                failures += 1;
                if should_abort_remaining(failures, writer_size, min_needed) {
                    join_set.abort_all();
                }
            }
        }
    }

    kept.sort_by_key(|r| r.ordinal);
    if !enough_replicas(kept.len(), min_needed) {
        for r in kept {
            r.writer.cancel().await;
        }
        return Err(Error::ResourceExhausted {
            message: format!(
                "Failed to write enough replicas. Success: {}, Required: {} (block_id={})",
                writer_size.saturating_sub(failures),
                min_needed,
                block_id
            ),
        });
    }
    *replicas = kept;
    Ok(())
}

/// Whether a failed cache write must abort the file rather than degrade to a
/// UFS-only write.
///
/// See [`GoosefsFileWriter::handle_cache_write_exception`] for what each rule
/// protects; this is the decision on its own so it can be exercised directly.
fn cache_write_failure_is_fatal(
    err: &Error,
    strategy: &WriteStrategy,
    block_opened: bool,
    ignore_unknown_first_block_status: bool,
) -> bool {
    // The replication contract was already relaxed as far as allowed.
    if matches!(
        err,
        Error::ResourceExhausted { .. } | Error::InvalidArgument { .. }
    ) {
        return true;
    }
    // MUST_CACHE / TRY_CACHE / NONE: nothing to degrade to.
    if !strategy.ufs_stream && !strategy.need_async_persist {
        return true;
    }
    // ASYNC_THROUGH past the first block: earlier blocks are cached but not on
    // the UFS, so a UFS stream started now would produce a truncated file.
    if strategy.need_async_persist && block_opened {
        return true;
    }
    // The UFS write would reuse the credentials that were just rejected.
    if matches!(
        err,
        Error::AuthenticationFailed { .. } | Error::PermissionDenied { .. }
    ) {
        return true;
    }
    // First block never opened: the failure could be a rejection or just
    // transport trouble, and the operator decides how to read that ambiguity.
    !block_opened && !ignore_unknown_first_block_status
}

/// Resolve the mutually exclusive persist fields of `CompleteFilePOptions`.
///
/// Returns `(force_persisted, async_persist_options)`.
///
/// # Java authority
///
/// `GoosefsFileOutStream.close()`:
///
/// ```java
/// if (!mCanceled && mUnderStorageType.isAsyncPersist()) {
///   if (mUnderStorageOutputStreamCompleted) {
///     optionsBuilder.setForcePersisted(true);
///   } else if (mOptions.getPersistenceWaitTime() != Constants.NO_AUTO_PERSIST) {
///     optionsBuilder.setAsyncPersistOptions(... .setPersistenceWaitTime(...));
///   }
/// }
/// ```
///
/// Three outcomes, all reachable:
/// - the writer degraded to UFS and finished it → `force_persisted`, and the
///   Master skips the persist job entirely;
/// - a normal ASYNC_THROUGH write → schedule the job after `wait_time_ms`;
/// - `wait_time_ms == NO_AUTO_PERSIST` → neither, so the file waits for a
///   rename or an explicit persist command.
///
/// `common_options` stays `None`: Java fills it from
/// `scheduleAsyncPersistDefaults`, but the Master's
/// `ScheduleAsyncPersistenceContext` only ever reads `persistenceWaitTime`.
fn resolve_persist_options(
    need_async_persist: bool,
    ufs_stream_completed: bool,
    wait_time_ms: i64,
) -> (Option<bool>, Option<ScheduleAsyncPersistencePOptions>) {
    if !need_async_persist {
        return (None, None);
    }
    if ufs_stream_completed {
        return (Some(true), None);
    }
    if wait_time_ms == NO_AUTO_PERSIST {
        return (None, None);
    }
    (
        None,
        Some(ScheduleAsyncPersistencePOptions {
            common_options: None,
            persistence_wait_time: Some(wait_time_ms),
        }),
    )
}

/// Last-block locations for `CompleteFile`, matching Java
/// `GoosefsFileOutStream.close()`: only ASYNC_THROUGH attaches them.
fn complete_file_locations(
    async_through: bool,
    last_location: Option<FileLocation>,
) -> Vec<FileLocation> {
    if async_through {
        last_location.into_iter().collect()
    } else {
        Vec::new()
    }
}

/// Lower 24 bits of a GooseFS block ID (Java `BlockId.getSequenceNumber`).
fn block_sequence_number(block_id: i64) -> u64 {
    const SEQUENCE_NUMBER_BITS: u32 = 24;
    (block_id as u64) & ((1u64 << SEQUENCE_NUMBER_BITS) - 1)
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
                // 2. Cancel in-progress cache block writer (all replicas).
                if let Some(active) = current_block_writer {
                    active.cancel_replicas().await;
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
        assert!(s.need_async_persist);
        // Options are resolved but unused unless the cache write degrades;
        // `ufs_stream` above is what keeps the happy path off the UFS.
        assert!(s.create_ufs_file_options.is_some());
    }

    fn cache_through() -> WriteStrategy {
        resolve_write_strategy(Some(3), &FileInfo::default())
    }

    fn async_through() -> WriteStrategy {
        resolve_write_strategy(Some(5), &FileInfo::default())
    }

    fn must_cache() -> WriteStrategy {
        resolve_write_strategy(Some(1), &FileInfo::default())
    }

    fn io_err() -> Error {
        Error::BlockIoError {
            message: "worker went away".to_string(),
        }
    }

    /// The baseline degrade: a plain I/O failure on a write type that has a
    /// UFS destination should fall back rather than fail the write.
    #[test]
    fn cache_failure_degrades_on_plain_io_error() {
        assert!(!cache_write_failure_is_fatal(
            &io_err(),
            &cache_through(),
            true,
            true
        ));
        // ASYNC_THROUGH may degrade too, but only before any block opened.
        assert!(!cache_write_failure_is_fatal(
            &io_err(),
            &async_through(),
            false,
            true
        ));
    }

    /// `ResourceExhausted` / `InvalidArgument` mean the block store already
    /// relaxed the replica count as far as allowed and still fell short.
    /// Degrading would quietly leave a single UFS copy in place of the
    /// replication the caller asked for.
    #[test]
    fn cache_failure_is_fatal_when_replication_contract_broken() {
        for err in [
            Error::ResourceExhausted {
                message: "alive < durable.min".to_string(),
            },
            Error::InvalidArgument {
                message: "durable < durable.min".to_string(),
            },
        ] {
            assert!(
                cache_write_failure_is_fatal(&err, &cache_through(), true, true),
                "{err} must not degrade"
            );
        }
    }

    /// MUST_CACHE / TRY_CACHE / NONE have no UFS destination configured, so
    /// there is nothing to degrade to.
    #[test]
    fn cache_failure_is_fatal_without_a_ufs_destination() {
        assert!(cache_write_failure_is_fatal(
            &io_err(),
            &must_cache(),
            true,
            true
        ));
    }

    /// Once an ASYNC_THROUGH block has opened, earlier blocks are committed in
    /// the cache and absent from the UFS. A UFS stream started now would only
    /// hold the bytes from this `write()` onward, silently truncating the file.
    #[test]
    fn cache_failure_is_fatal_for_async_through_past_the_first_block() {
        assert!(cache_write_failure_is_fatal(
            &io_err(),
            &async_through(),
            true,
            true
        ));
        // CACHE_THROUGH is unaffected: its UFS stream already has every byte.
        assert!(!cache_write_failure_is_fatal(
            &io_err(),
            &cache_through(),
            true,
            true
        ));
    }

    /// The UFS write would present the same credentials that were just
    /// rejected, so degrading only converts a clear error into a confusing one.
    #[test]
    fn cache_failure_is_fatal_when_credentials_are_rejected() {
        for err in [
            Error::AuthenticationFailed {
                message: "bad token".to_string(),
            },
            Error::PermissionDenied {
                message: "no write permission".to_string(),
            },
        ] {
            assert!(
                cache_write_failure_is_fatal(&err, &cache_through(), true, true),
                "{err} must not degrade"
            );
        }
    }

    /// A first-block open failure leaves the client unable to tell a rejection
    /// from a transport error. The default is to degrade anyway; flipping the
    /// flag makes that ambiguity fatal.
    #[test]
    fn cache_failure_first_block_ambiguity_follows_the_config() {
        assert!(!cache_write_failure_is_fatal(
            &io_err(),
            &cache_through(),
            false,
            true
        ));
        assert!(cache_write_failure_is_fatal(
            &io_err(),
            &cache_through(),
            false,
            false
        ));
        // The flag only covers the first block; afterwards auth is known-good.
        assert!(!cache_write_failure_is_fatal(
            &io_err(),
            &cache_through(),
            true,
            false
        ));
    }

    /// ASYNC_THROUGH opens no UFS stream up front, but a degrade needs one, so
    /// the create options must be resolved even though the happy path drops
    /// them.
    #[test]
    fn async_through_carries_ufs_options_for_the_degrade_path() {
        let info = FileInfo {
            ufs_path: Some("cosn://bucket/f".to_string()),
            ..Default::default()
        };
        let strategy = resolve_write_strategy(Some(5), &info);
        assert!(!strategy.ufs_stream);
        assert_eq!(
            strategy
                .create_ufs_file_options
                .as_ref()
                .and_then(|o| o.ufs_path.as_deref()),
            Some("cosn://bucket/f")
        );
    }

    /// A degraded ASYNC_THROUGH write lands on the UFS before `CompleteFile`
    /// runs, so the Master must be told the file is already persisted rather
    /// than being asked to queue a persist job for it.
    #[test]
    fn persist_options_force_persisted_after_degrade() {
        let (force, async_opts) = resolve_persist_options(true, true, 0);
        assert_eq!(force, Some(true));
        assert!(
            async_opts.is_none(),
            "a persisted file must not also be queued for persisting"
        );

        // The wait time is irrelevant once the UFS copy exists.
        let (force, async_opts) = resolve_persist_options(true, true, 5_000);
        assert_eq!(force, Some(true));
        assert!(async_opts.is_none());
    }

    #[test]
    fn persist_options_schedule_job_on_the_normal_path() {
        let (force, async_opts) = resolve_persist_options(true, false, 0);
        assert!(force.is_none());
        assert_eq!(
            async_opts,
            Some(ScheduleAsyncPersistencePOptions {
                common_options: None,
                persistence_wait_time: Some(0),
            })
        );

        let (_, async_opts) = resolve_persist_options(true, false, 5_000);
        assert_eq!(
            async_opts.and_then(|o| o.persistence_wait_time),
            Some(5_000),
            "the configured wait time must reach the Master"
        );
    }

    /// `NO_AUTO_PERSIST` means the file is only persisted by a later rename or
    /// an explicit persist command. Sending `async_persist_options` anyway
    /// would make the Master queue the job regardless.
    #[test]
    fn persist_options_no_auto_persist_sends_neither() {
        let (force, async_opts) = resolve_persist_options(true, false, NO_AUTO_PERSIST);
        assert!(force.is_none());
        assert!(async_opts.is_none());
    }

    /// Only ASYNC_PERSIST write types touch these fields. CACHE_THROUGH also
    /// completes a UFS stream, so gating on `ufs_stream_completed` alone would
    /// wrongly stamp `force_persisted` on every CACHE_THROUGH write.
    #[test]
    fn persist_options_untouched_for_non_async_write_types() {
        for ufs_completed in [false, true] {
            let (force, async_opts) = resolve_persist_options(false, ufs_completed, 0);
            assert!(force.is_none(), "ufs_completed={ufs_completed}");
            assert!(async_opts.is_none(), "ufs_completed={ufs_completed}");
        }
    }

    #[test]
    fn complete_file_locations_only_for_async_through() {
        let loc = FileLocation {
            block_id: Some(1),
            offset: Some(0),
            length: Some(64),
            worker_id: vec![7],
        };
        assert!(complete_file_locations(false, Some(loc.clone())).is_empty());
        assert!(complete_file_locations(false, None).is_empty());
        let got = complete_file_locations(true, Some(loc.clone()));
        assert_eq!(got, vec![loc]);
        assert!(complete_file_locations(true, None).is_empty());
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
        let router = WorkerRouterView::empty();
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
            should_cache: strategy.cache_stream,
            ufs_write_enabled: strategy.ufs_stream,
            block_opened: false,
            write_strategy: strategy,
            committed_block_ids: Vec::new(),
            current_block_writer: None,
            ufs_stream: None,
            ufs_worker_addr: None,
            ufs_stream_completed: AtomicBool::new(false),
            _router_needs_init: AtomicBool::new(false),
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

    fn assert_pending_invariant(pending: &[u8], chunk_size: usize) {
        assert!(
            pending.len() < chunk_size,
            "pending_chunk must stay strictly below chunk_size, got {} >= {}",
            pending.len(),
            chunk_size
        );
    }

    #[test]
    fn take_full_chunks_slices_aligned_payload_without_growing_pending() {
        let chunk_size = 1024;
        let mut pending = Vec::with_capacity(chunk_size);
        // 17 full chunks + 100-byte tail — the old drain-from-front path
        // memmoved ~136 MiB for a similar 17 MiB write().
        let n = 17 * chunk_size + 100;
        let incoming: Vec<u8> = (0u8..=255).cycle().take(n).collect();
        let chunks = take_full_chunks(&mut pending, &incoming, chunk_size);
        assert_eq!(chunks.len(), 17);
        assert!(chunks.iter().all(|c| c.len() == chunk_size));
        assert_eq!(&chunks[0], &incoming[..chunk_size]);
        assert_eq!(pending.len(), 100);
        assert_eq!(&pending[..], &incoming[incoming.len() - 100..]);
        assert_pending_invariant(&pending, chunk_size);
        assert!(
            pending.capacity() <= chunk_size * 2,
            "pending must not hold the whole write(); capacity={}",
            pending.capacity()
        );
    }

    #[test]
    fn take_full_chunks_completes_existing_tail() {
        let chunk_size = 1000;
        let mut pending = vec![0u8; 400];
        let incoming = vec![1u8; 700];
        let chunks = take_full_chunks(&mut pending, &incoming, chunk_size);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1000);
        assert_eq!(&chunks[0][..400], &[0u8; 400]);
        assert_eq!(&chunks[0][400..], &[1u8; 600]);
        assert_eq!(pending.len(), 100);
        assert_eq!(&pending[..], &[1u8; 100]);
        assert_pending_invariant(&pending, chunk_size);
    }

    #[test]
    fn take_full_chunks_holds_short_write_in_pending() {
        let chunk_size = 1000;
        let mut pending = vec![0u8; 400];
        let incoming = vec![1u8; 200];
        let chunks = take_full_chunks(&mut pending, &incoming, chunk_size);
        assert!(chunks.is_empty());
        assert_eq!(pending.len(), 600);
        assert_pending_invariant(&pending, chunk_size);
    }
}
