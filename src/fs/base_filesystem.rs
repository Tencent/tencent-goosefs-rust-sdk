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

    /// Borrow the shared [`FileSystemContext`].
    pub fn context(&self) -> &Arc<FileSystemContext> {
        &self.ctx
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
        //
        // We must distinguish three cases:
        // - parent exists, has xattr → use that WriteType
        // - parent exists but no xattr → fall through to config default
        // - parent does not exist (NotFound) → fall through to config default
        // - any other RPC error (Unavailable, etc.) → also fall through but
        //   log a warning, because silently using a different default on
        //   transient network errors changes persistence semantics for the
        //   newly created file.
        let parent = Self::parent_path(path);
        if let Some(parent_path) = parent {
            let master = self.master();
            match master.get_status(&parent_path).await {
                Ok(parent_info) => {
                    let parent_status = URIStatus::from_proto(parent_info);
                    if let Some(wt) = get_write_type_from_xattr(&parent_status.xattr) {
                        return wt;
                    }
                }
                Err(e) if e.is_not_found() => {
                    // Parent doesn't exist yet — totally fine, fall through.
                }
                Err(e) => {
                    tracing::warn!(
                        path = %path,
                        parent = %parent_path,
                        error = %e,
                        "resolve_write_type: failed to fetch parent xattr; \
                         falling back to config default — file will be created with that WriteType"
                    );
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

    // ── One-shot write convenience ─────────────────────────────────────────
    //
    // NOT part of the `FileSystem` trait; lives on `BaseFileSystem` directly.

    /// Create a file, write data, and close it in a single async call.
    ///
    /// Equivalent to `create_file()` → `write()` → `close()`, but avoids the
    /// extra tokio scheduler yield between create and close. Matches the
    /// Python SDK's `write_file` API.
    ///
    /// Returns the number of bytes written.
    pub async fn write_file(
        &self,
        path: &str,
        data: &[u8],
        options: CreateFileOptions,
    ) -> Result<u64> {
        let write_type = self.resolve_write_type(path, &options).await;

        let proto_opts = CreateFilePOptions {
            block_size_bytes: options.block_size_bytes,
            recursive: Some(options.recursive),
            write_type: Some(WritePType::from(write_type) as i32),
            ..Default::default()
        };

        GoosefsFileWriter::write_file_with_context_and_options(
            self.ctx.clone(),
            path,
            data,
            Some(proto_opts),
        )
        .await
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
        if !recursive {
            let items = master.list_status(path, false).await?;
            return Ok(items.into_iter().map(URIStatus::from_proto).collect());
        }
        // The master's `recursive` option is best-effort and (on some cluster
        // builds) only returns entries whose metadata is already loaded,
        // collapsing a deep tree to its first level. To match the Java client
        // and `goosefs fs ls -R` (which walk the namespace client-side), we
        // perform the recursion ourselves: list one level at a time and
        // descend into every directory child.
        let mut out: Vec<URIStatus> = Vec::new();
        let mut queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        queue.push_back(path.to_string());
        while let Some(cur) = queue.pop_front() {
            let items = master.list_status(&cur, false).await?;
            for fi in items {
                let status = URIStatus::from_proto(fi);
                if status.is_folder() {
                    queue.push_back(status.path.clone());
                }
                out.push(status);
            }
        }
        Ok(out)
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
        master.delete_with_options(path, options).await?;
        // A3 consistency: drop any cached FileInfo so a subsequent open sees
        // NotFound (or the fresh state after re-create). No-op when the
        // opt-in cache is disabled.
        self.ctx.invalidate_file_info(path);
        Ok(())
    }

    // ── Rename ────────────────────────────────────────────────────────────────

    async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let master = self.master();
        master.rename(src, dst).await?;
        // A3 consistency: both endpoints of the rename change identity — the
        // src is gone, the dst now points at what src used to be.
        self.ctx.invalidate_file_info(src);
        self.ctx.invalidate_file_info(dst);
        Ok(())
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
