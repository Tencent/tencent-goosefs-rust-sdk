//! Internal helper that owns the shared `FileSystemContext` + `BaseFileSystem`
//! pair driving every Python-facing call.
//!
//! This module is not exposed to Python — it exists so that P2's
//! `AsyncGoosefs`, P3's sync `Goosefs`, and P5's `FileReader` /
//! `FileWriter` all share the same lifecycle hooks (`close`, drop) and
//! Arc-based connection sharing.
//!
//! ```text
//! AsyncGoosefs ─┐
//!               ├──> Arc<PyFsHandle> ──> Arc<FileSystemContext>
//! Goosefs    ─┘                          │
//!                                        └─> Arc<BaseFileSystem>
//! ```

use std::sync::Arc;

use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::BaseFileSystem;

/// Maximum number of in-flight RPCs per `batch_*` call.
///
/// `batch_get_status` / `batch_exists` (sync + async) drive their futures
/// through `stream::iter(..).buffered(BATCH_CONCURRENCY_LIMIT)`, so a caller
/// passing `paths.len() == 10_000` will *not* open ten thousand simultaneous
/// gRPC streams to the master.
///
/// Re-exported from [`crate::runtime::RUNTIME_MAX_BLOCKING_THREADS`] so that
/// the batch fan-out cannot drift away from the runtime's
/// `max_blocking_threads` budget — both values are tuned together by
/// editing the single source of truth in `runtime.rs`.
pub const BATCH_CONCURRENCY_LIMIT: usize = crate::runtime::RUNTIME_MAX_BLOCKING_THREADS;

/// Maximum number of in-flight RPCs for resource-holding `batch_*` calls
/// (e.g. `batch_open_file` which holds open file handles and worker
/// connections for each returned reader).
///
/// Uses the same limit as [`BATCH_CONCURRENCY_LIMIT`] since resource-holding
/// batch operations should have the same concurrency constraints.
pub const RESOURCE_BATCH_CONCURRENCY_LIMIT: usize = crate::runtime::RUNTIME_MAX_BLOCKING_THREADS;

/// Bundles a Goosefs context with the high-level filesystem façade.
///
/// Cheap to clone — both fields are `Arc<…>` internally.
#[derive(Clone)]
pub struct PyFsHandle {
    pub ctx: Arc<FileSystemContext>,
    pub fs: Arc<BaseFileSystem>,
}

impl PyFsHandle {
    pub fn new(ctx: Arc<FileSystemContext>) -> Self {
        let fs = BaseFileSystem::from_context(ctx.clone());
        Self { ctx, fs }
    }
}
