//! Page storage backends.
//!
//! A [`PageStore`] is the raw IO layer: it persists page bytes and reads them
//! back, with **no** thread-safety, accounting, or eviction concerns (those
//! live in the cache manager). Mirrors Java `PageStore`.
//!
//! Two backends are available:
//! - [`LocalPageStore`] — tokio::fs backend (default, all platforms)
//! - [`UringPageStore`] — io_uring backend (Linux 5.1+, see
//!   `docs/CLIENT_PAGE_CACHE_DESIGN.md`)

mod local;
mod uring;

pub use local::LocalPageStore;
#[cfg(target_os = "linux")]
pub use uring::UringPageStore;
pub use uring::{init_uring_config, is_uring_available};

use bytes::Bytes;

use crate::cache::page_id::PageId;
use crate::error::Result;
use std::path::Path;

/// Raw page storage backend.
///
/// Implementations are pure IO and are expected to be called under the cache
/// manager's locking discipline.
///
/// In addition to the core `put` / `get` / `delete` operations, the trait
/// includes identity-sidecar management methods (`write_identity` /
/// `read_identity` / `delete_identity`) and `root_dir` for restore scanning.
/// These are **not** on the cache-hit hot path and may use `tokio::fs`
/// internally even in the io_uring backend.
#[async_trait::async_trait]
pub trait PageStore: Send + Sync {
    /// Persist the full bytes of a page.
    ///
    /// Implementations should make the write atomic (e.g. temp file + rename)
    /// so a partially written page is never observable by [`PageStore::get`].
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()>;

    /// Read up to `dst.len()` bytes starting at `offset` within the page into
    /// `dst`. Returns the number of bytes read (may be `< dst.len()` at the
    /// page tail).
    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize>;

    /// Read bytes from a page and return them directly.
    ///
    /// Backends that naturally allocate their own read buffer (notably
    /// io_uring) should override this to avoid copying into a temporary caller
    /// buffer before returning to the cache layer.
    async fn get_bytes(&self, page_id: &PageId, offset: usize, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let mut dst = vec![0u8; len];
        let n = self.get(page_id, offset, &mut dst).await?;
        if n == 0 {
            Ok(Bytes::new())
        } else {
            dst.truncate(n);
            Ok(Bytes::from(dst))
        }
    }

    /// Remove a page. Removing a non-existent page is **not** an error.
    async fn delete(&self, page_id: &PageId) -> Result<()>;

    /// Root directory of this store (`<dir>/<page_size>`).
    ///
    /// Used by `LocalCacheManager::restore` to scan persisted pages on startup.
    fn root_dir(&self) -> &Path;

    /// Persist `file_id`'s `(length, mtime)` identity sidecar (best-effort,
    /// atomic). Written so overwrite detection survives a process restart.
    async fn write_identity(&self, file_id: &str, length: i64, mtime: i64) -> Result<()>;

    /// Read and parse `file_id`'s persisted identity. Returns `None` when the
    /// sidecar is absent or malformed (restore treats it as "identity unknown").
    async fn read_identity(&self, file_id: &str) -> Option<(i64, i64)>;

    /// Remove `file_id`'s identity sidecar (best-effort; missing is OK).
    async fn delete_identity(&self, file_id: &str) -> Result<()>;
}
