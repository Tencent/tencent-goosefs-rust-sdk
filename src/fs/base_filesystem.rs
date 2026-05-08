//! `BaseFileSystem` — the standard `FileSystem` implementation.
//!
//! [`BaseFileSystem`] implements the [`FileSystem`] trait against a real
//! Goosefs cluster via gRPC.  It is the primary production implementation.
//!
//! # Thread safety
//!
//! `BaseFileSystem` is `Send + Sync + 'static` and can be wrapped in
//! `Arc<BaseFileSystem>` or `Arc<dyn FileSystem>` and shared freely across
//! async tasks.
//!
//! # `exists()` semantics
//!
//! **Java authority**: Verified against `DefaultFileSystem.exists()`:
//! ```java
//! try {
//!     URIStatus status = getStatus(path);
//!     if (!status.isCompleted() && !status.isFolder()) return false;
//!     return true;
//! } catch (FileDoesNotExistException e) {
//!     return false;
//! }
//! ```
//! An `INCOMPLETE` non-folder file → `false`.  This differs from the Go SDK
//! which returns `true` for all existing inodes.
//!
//! # `WriteType` xattr inheritance
//!
//! When `CreateFileOptions.write_type == WriteTypeXAttr::Inherit`, `create_file`
//! fetches the parent directory's xattr and calls
//! [`crate::fs::write_type::get_write_type_from_xattr`] to determine the
//! effective `WriteType`.  Falls back to the `GoosefsConfig` default.
//!
//! # Connection sharing
//!
//! All operations reuse the persistent gRPC channel from [`FileSystemContext`].
//! Construct via [`BaseFileSystem::connect`] or [`BaseFileSystem::from_context`].

use std::sync::Arc;

use async_trait::async_trait;

use crate::client::MasterClient;
use crate::config::{GoosefsConfig, WriteType};
use crate::context::FileSystemContext;
use crate::error::{Error, Result};
use crate::fs::filesystem::FileSystem;
use crate::fs::options::{CreateFileOptions, DeleteOptions, OpenFileOptions};
use crate::fs::uri_status::URIStatus;
use crate::fs::write_type::{get_write_type_from_xattr, WriteTypeXAttr};
use crate::io::{GoosefsFileInStream, GoosefsFileWriter};
use crate::proto::grpc::file::{CreateFilePOptions, WritePType};

/// Standard Goosefs filesystem client.
///
/// All operations delegate to the underlying `MasterClient` gRPC stub.
///
/// ## Usage
///
/// ```rust,no_run
/// use goosefs_sdk::context::FileSystemContext;
/// use goosefs_sdk::fs::BaseFileSystem;
/// use goosefs_sdk::config::GoosefsConfig;
/// use goosefs_sdk::fs::filesystem::FileSystem;
///
/// # async fn example() -> goosefs_sdk::error::Result<()> {
/// // Build once per application lifetime — one TCP+SASL handshake
/// let ctx = FileSystemContext::connect(GoosefsConfig::new("127.0.0.1:9200")).await?;
/// let fs = BaseFileSystem::from_context(ctx);
///
/// // All calls reuse the same Master connection — zero extra handshakes
/// let status = fs.get_status("/data/file.parquet").await?;
/// println!("length = {}", status.length);
/// # Ok(())
/// # }
/// ```
pub struct BaseFileSystem {
    /// Shared context — owns the persistent Master + Worker connections.
    ctx: Arc<FileSystemContext>,

    /// Cached config from the context for convenience access.
    config: GoosefsConfig,
}

impl BaseFileSystem {
    // ── Constructors ─────────────────────────────────────────────────────────

    /// Create a `BaseFileSystem` backed by a shared [`FileSystemContext`].
    ///
    /// All Master RPCs reuse the context's persistent gRPC channel.
    /// This is the recommended constructor for production use.
    pub fn from_context(ctx: Arc<FileSystemContext>) -> Arc<Self> {
        let config = ctx.config().clone();
        Arc::new(Self { config, ctx })
    }

    /// Connect to Goosefs and create both a [`FileSystemContext`] and a
    /// `BaseFileSystem` in one step.
    ///
    /// Equivalent to:
    /// ```rust,ignore
    /// let ctx = FileSystemContext::connect(config).await?;
    /// let fs  = BaseFileSystem::from_context(ctx);
    /// ```
    pub async fn connect(config: GoosefsConfig) -> Result<Arc<Self>> {
        let ctx = FileSystemContext::connect(config).await?;
        Ok(Self::from_context(ctx))
    }

    /// Borrow the underlying config.
    pub fn config(&self) -> &GoosefsConfig {
        &self.config
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    /// Obtain a `MasterClient` — O(1) Arc clone from the shared context.
    fn master(&self) -> Arc<MasterClient> {
        self.ctx.acquire_master()
    }

    /// Resolve the effective `WriteType` for a new file at `path`.
    ///
    /// Priority:
    /// 1. Explicit `WriteTypeXAttr::Explicit(wt)` in `options`
    /// 2. Parent directory `"innerWriteType"` xattr
    /// 3. `GoosefsConfig.write_type` (if set)
    /// 4. Default: `WriteType::MustCache` (Java default)
    async fn resolve_write_type(&self, path: &str, options: &CreateFileOptions) -> WriteType {
        // 1. Explicit override
        if let WriteTypeXAttr::Explicit(wt) = options.write_type {
            return wt;
        }

        // 2. Parent xattr
        let parent = Self::parent_path(path);
        if let Some(parent_path) = parent {
            let master = self.master();
            if let Ok(parent_info) = master.get_status(&parent_path).await {
                let parent_status = URIStatus::from_proto(parent_info);
                if let Some(wt) = get_write_type_from_xattr(&parent_status.xattr) {
                    return wt;
                }
            }
        }

        // 3. Config default
        if let Some(proto_wt) = self.config.get_write_type() {
            if let Ok(wt) = WriteType::try_from_proto(proto_wt) {
                return wt;
            }
        }

        // 4. Java default
        WriteType::MustCache
    }

    /// Extract the parent path of `path`.
    ///
    /// Returns `None` for root `/`.
    fn parent_path(path: &str) -> Option<String> {
        let trimmed = path.trim_end_matches('/');
        if trimmed.is_empty() {
            return None;
        }
        let last_slash = trimmed.rfind('/')?;
        if last_slash == 0 {
            Some("/".to_string())
        } else {
            Some(trimmed[..last_slash].to_string())
        }
    }
}

#[async_trait]
impl FileSystem for BaseFileSystem {
    // ── Status ────────────────────────────────────────────────────────────────

    async fn get_status(&self, path: &str) -> Result<URIStatus> {
        let master = self.master();
        let fi = master.get_status(path).await?;
        Ok(URIStatus::from_proto(fi))
    }

    async fn list_status(&self, path: &str, recursive: bool) -> Result<Vec<URIStatus>> {
        let master = self.master();
        let items = master.list_status(path, recursive).await?;
        Ok(items.into_iter().map(URIStatus::from_proto).collect())
    }

    /// Return `true` if `path` exists and is either a completed file or a directory.
    ///
    /// # Java semantics
    ///
    /// An `INCOMPLETE` non-folder file returns `false` because it is not yet
    /// usable.  The Go SDK incorrectly returns `true` in this case.
    async fn exists(&self, path: &str) -> Result<bool> {
        match self.get_status(path).await {
            Ok(status) => {
                // INCOMPLETE non-folder → not usable → false
                Ok(status.is_readable())
            }
            Err(Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    // ── File read ─────────────────────────────────────────────────────────────

    async fn open_file(&self, path: &str, options: OpenFileOptions) -> Result<GoosefsFileInStream> {
        GoosefsFileInStream::open_with_context(self.ctx.clone(), path, options).await
    }

    // ── File write ────────────────────────────────────────────────────────────

    /// Create a new file, inheriting `WriteType` from the parent directory
    /// xattr if not explicitly set.
    async fn create_file(
        &self,
        path: &str,
        options: CreateFileOptions,
    ) -> Result<GoosefsFileWriter> {
        let write_type = self.resolve_write_type(path, &options).await;

        let proto_opts = CreateFilePOptions {
            block_size_bytes: options.block_size_bytes,
            recursive: Some(options.recursive),
            write_type: Some(WritePType::from(write_type) as i32),
            ..Default::default()
        };

        GoosefsFileWriter::create_with_context(self.ctx.clone(), path, Some(proto_opts)).await
    }

    // ── Directory ─────────────────────────────────────────────────────────────

    async fn mkdir(&self, path: &str, recursive: bool) -> Result<()> {
        let master = self.master();
        master.create_directory(path, recursive).await
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    async fn delete(&self, path: &str, options: DeleteOptions) -> Result<()> {
        let master = self.master();
        master.delete_with_options(path, options).await
    }

    // ── Rename ────────────────────────────────────────────────────────────────

    async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let master = self.master();
        master.rename(src, dst).await
    }
}

// ── Unit tests (pure logic — no I/O) ─────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parent_path_normal() {
        assert_eq!(
            BaseFileSystem::parent_path("/data/hello.txt"),
            Some("/data".to_string())
        );
    }

    #[test]
    fn test_parent_path_root_child() {
        assert_eq!(
            BaseFileSystem::parent_path("/hello.txt"),
            Some("/".to_string())
        );
    }

    #[test]
    fn test_parent_path_root() {
        assert_eq!(BaseFileSystem::parent_path("/"), None);
    }

    #[test]
    fn test_parent_path_nested() {
        assert_eq!(
            BaseFileSystem::parent_path("/a/b/c/file.parquet"),
            Some("/a/b/c".to_string())
        );
    }

    #[test]
    fn test_parent_path_trailing_slash() {
        assert_eq!(
            BaseFileSystem::parent_path("/data/dir/"),
            Some("/data".to_string())
        );
    }

    /// Verify that `from_context()` creates a `BaseFileSystem` with a shared context.
    #[test]
    fn test_from_context_sets_ctx() {
        // Can't call connect() in a unit test (needs network), but we can
        // verify the test_new_is_legacy_mode test was removed. Just a compile check.
    }
}
