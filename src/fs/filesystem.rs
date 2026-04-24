//! `FileSystem` trait — the high-level GooseFS file-system interface.
//!
//! [`FileSystem`] defines the contract for all GooseFS client implementations.
//! The primary implementation is [`crate::fs::base_filesystem::BaseFileSystem`].
//!
//! # Design decisions
//!
//! ## `async_trait` + `Box<dyn FileInStream>`
//!
//! Returning `impl Future` from trait methods currently requires nightly Rust
//! or `async_trait`.  We use `async_trait` (stable) and return
//! `Box<dyn GooseFsFileInStream>` from `open_file` so the trait is object-safe
//! and usable as `dyn FileSystem`.
//!
//! ## `Send + Sync + 'static`
//!
//! All types implementing `FileSystem` must be `Send + Sync + 'static` so they
//! can be shared across tokio tasks via `Arc<dyn FileSystem>`.

use async_trait::async_trait;

use crate::error::Result;
use crate::fs::options::{CreateFileOptions, DeleteOptions, OpenFileOptions};
use crate::fs::uri_status::URIStatus;
use crate::io::GooseFsFileInStream;

/// High-level GooseFS file-system interface.
///
/// All operations correspond directly to GooseFS Master RPCs.
///
/// # Thread safety
///
/// Implementations must be `Send + Sync + 'static` so they can be wrapped in
/// `Arc<dyn FileSystem>` and shared across async tasks.
#[async_trait]
pub trait FileSystem: Send + Sync + 'static {
    // ── Status / listing ─────────────────────────────────────────────────────

    /// Retrieve metadata for a file or directory.
    ///
    /// # Errors
    ///
    /// - [`crate::error::Error::NotFound`] if the path does not exist.
    async fn get_status(&self, path: &str) -> Result<URIStatus>;

    /// List the direct children of a directory.
    ///
    /// # Arguments
    ///
    /// - `path`      — the directory to list.
    /// - `recursive` — if `true`, list all descendants.
    ///
    /// # Errors
    ///
    /// - [`crate::error::Error::NotFound`] if `path` does not exist.
    /// - [`crate::error::Error::OpenDirectory`] if `path` is a file.
    async fn list_status(&self, path: &str, recursive: bool) -> Result<Vec<URIStatus>>;

    /// Return `true` if a path exists in GooseFS.
    ///
    /// # Java authority
    ///
    /// Based on `DefaultFileSystem.exists()`:
    /// - `NotFound` error → `false`
    /// - Any other error → propagated
    /// - `INCOMPLETE` non-folder file → `false`
    ///   (the file exists as an inode but is not usable)
    /// - `COMPLETE` file or directory → `true`
    ///
    /// **Note**: This differs from the Go SDK, which returns `true` for all
    /// existing inodes regardless of completion state.  Rust follows the Java
    /// server semantics.
    async fn exists(&self, path: &str) -> Result<bool>;

    // ── File read ────────────────────────────────────────────────────────────

    /// Open a file for reading.
    ///
    /// Returns a [`GooseFsFileInStream`] positioned at the beginning of the file.
    ///
    /// # Errors
    ///
    /// - [`crate::error::Error::FileIncomplete`] if the file is being written.
    /// - [`crate::error::Error::OpenDirectory`] if `path` is a directory.
    /// - [`crate::error::Error::NotFound`] if the path does not exist.
    async fn open_file(&self, path: &str, options: OpenFileOptions) -> Result<GooseFsFileInStream>;

    // ── File write ───────────────────────────────────────────────────────────

    /// Create a new file and return an open writer for it.
    ///
    /// The file is not visible to readers until the returned writer is closed.
    ///
    /// # WriteType inheritance
    ///
    /// If `options.write_type` is [`crate::fs::write_type::WriteTypeXAttr::Inherit`],
    /// the implementation must:
    /// 1. Call `get_status(parent(path))` to get the parent directory.
    /// 2. Call [`crate::fs::write_type::get_write_type_from_xattr`] on the
    ///    parent's `xattr` map.
    /// 3. Use the resolved `WriteType`, or fall back to the config default.
    async fn create_file(
        &self,
        path: &str,
        options: CreateFileOptions,
    ) -> Result<crate::io::GooseFsFileWriter>;

    // ── Directory operations ─────────────────────────────────────────────────

    /// Create a directory (and any missing parent directories).
    async fn mkdir(&self, path: &str, recursive: bool) -> Result<()>;

    // ── Delete ───────────────────────────────────────────────────────────────

    /// Delete a file or directory.
    ///
    /// Use [`DeleteOptions::recursive()`] to delete non-empty directories.
    async fn delete(&self, path: &str, options: DeleteOptions) -> Result<()>;

    // ── Rename ───────────────────────────────────────────────────────────────

    /// Rename / move a file or directory.
    async fn rename(&self, src: &str, dst: &str) -> Result<()>;
}
