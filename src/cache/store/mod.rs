//! Page storage backends.
//!
//! A [`PageStore`] is the raw IO layer: it persists page bytes and reads them
//! back, with **no** thread-safety, accounting, or eviction concerns (those
//! live in the cache manager). Mirrors Java `PageStore`.

mod local;

pub use local::LocalPageStore;

use crate::cache::page_id::PageId;
use crate::error::Result;

/// Raw page storage backend.
///
/// Implementations are pure IO and are expected to be called under the cache
/// manager's locking discipline.
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

    /// Remove a page. Removing a non-existent page is **not** an error.
    async fn delete(&self, page_id: &PageId) -> Result<()>;
}
