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
