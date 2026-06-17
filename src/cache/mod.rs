//! Client-side local page cache.
//!
//! This module implements a local, page-based read cache for the Goosefs Rust
//! SDK, mirroring the Java client's
//! `com.qcloud.cos.goosefs.client.file.cache.*` design.
//!
//! # Status
//!
//! Implemented: the public abstractions ([`CacheManager`], [`PageId`],
//! [`PageInfo`], [`CacheManagerOptions`]), the disabled, always-miss
//! [`DisabledCacheManager`], and the disk-backed [`LocalCacheManager`]
//! (multi-dir [`store::LocalPageStore`] + [`evictor`] + bounded async
//! write-back + striped page locks). The page-split read loop lives in
//! [`caching_reader::read_through_cache`]. See
//! `docs/CLIENT_PAGE_CACHE_DESIGN.md` for the full design.
//!
//! The cache is **disabled by default** ([`crate::config::GoosefsConfig::client_cache_enabled`]
//! defaults to `false`), so existing behaviour is unchanged unless explicitly
//! opted in.
//!
//! # Architecture (target)
//!
//! ```text
//! GoosefsFileInStream::read_at
//!   → CachingPositionReader (page split + hit/miss + fill)
//!        ├── cache.get()                    → hit  (copy from local disk)
//!        └── external read (GrpcBlockReader) → miss (read + async fill)
//!              │
//!              ▼
//!        CacheManager (trait) → LocalCacheManager
//!              ├── PageMetaStore (index + accounting)
//!              ├── PageStore (LocalPageStore: disk IO)
//!              ├── CacheEvictor (LRU / LFU)
//!              └── Allocator (multi-dir)
//! ```
//!
//! # Best-effort contract
//!
//! The cache is **best-effort**: a miss or any internal error must never
//! affect read correctness — callers always fall back to reading from the
//! worker/UFS. Errors are swallowed internally and surfaced only as
//! `Client.Cache*Errors` metrics (mirrors Java `NoExceptionCacheManager`).

mod metrics;
mod options;
mod page_id;

pub mod allocator;
pub mod caching_reader;
pub mod evictor;
pub mod manager;
pub mod store;

pub use allocator::{Allocator, HashAllocator};
pub use caching_reader::{read_through_cache, ExternalRangeReader, FillMode};
pub use manager::LocalCacheManager;
pub use metrics::name as metric_name;
pub use options::CacheManagerOptions;
pub use page_id::{CacheScope, PageId, PageInfo};

use bytes::Bytes;
use std::sync::Arc;

/// Operational state of a [`CacheManager`].
///
/// Mirrors Java `CacheManager.State`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheState {
    /// Cache is not usable (e.g. failed to initialize). All gets miss.
    NotInUse,
    /// Cache can serve reads but rejects writes (e.g. recovering / read-only).
    ReadOnly,
    /// Cache is fully operational.
    ReadWrite,
}

impl CacheState {
    /// Numeric encoding for the `Client.CacheState` gauge.
    ///
    /// Matches Java's ordinal-style encoding: `NOT_IN_USE = 0`,
    /// `READ_ONLY = 1`, `READ_WRITE = 2`.
    pub fn as_i64(self) -> i64 {
        match self {
            CacheState::NotInUse => 0,
            CacheState::ReadOnly => 1,
            CacheState::ReadWrite => 2,
        }
    }
}

/// Local page cache abstraction.
///
/// Implementations coordinate the metadata store, disk store, evictor and
/// locking to serve cached pages. See the module docs for the best-effort
/// contract.
///
/// All methods are intentionally infallible (`bool` / `usize` rather than
/// `Result`): cache failures must never propagate as read errors.
#[async_trait::async_trait]
pub trait CacheManager: Send + Sync {
    /// Store (fill) a whole page.
    ///
    /// `page` should be the full page bytes (≤ page size). Returns `true` if
    /// the page was cached, `false` otherwise (e.g. cache full, racing write,
    /// or cache not in `ReadWrite` state).
    async fn put(&self, page_id: &PageId, page: Bytes) -> bool;

    /// Schedule a best-effort cache fill that does **not** block the caller.
    ///
    /// The default implementation spawns a detached task that calls
    /// [`CacheManager::put`]. Implementations with bounded async write-back
    /// override this to apply back-pressure (rejecting fills when the
    /// write-back pool is saturated, recording
    /// `Client.CachePutAsyncRejectionErrors`).
    fn schedule_fill(self: Arc<Self>, page_id: PageId, page: Bytes)
    where
        Self: 'static,
    {
        tokio::spawn(async move {
            let _ = self.put(&page_id, page).await;
        });
    }

    /// Read `dst.len()` bytes from page `page_id` starting at `page_offset`
    /// into `dst`.
    ///
    /// Returns the number of bytes actually read. `0` means a cache miss (or
    /// any internal error): the caller must read from the worker/UFS instead.
    async fn get(&self, page_id: &PageId, page_offset: usize, dst: &mut [u8]) -> usize;

    /// Delete a single page. Returns `true` if a page was removed.
    async fn delete(&self, page_id: &PageId) -> bool;

    /// Invalidate all cached pages belonging to `file_id`.
    ///
    /// Used when a file is overwritten or deleted so stale pages are not
    /// served. Implementations should treat this as best-effort.
    async fn invalidate(&self, file_id: &str);

    /// Notify the cache that a file was (re)opened with the given identity.
    ///
    /// Implementations compare `(length, last_modification_time_ms)` against
    /// the version recorded for `file_id`; if they differ (the file was
    /// overwritten while reusing the same id), all cached pages for that file
    /// are invalidated so stale data is never served. The default
    /// implementation is a no-op.
    ///
    /// **Consistency caveat (best-effort):** overwrite detection relies on the
    /// modification-time granularity reported by the backing UFS. On a UFS that
    /// only exposes second-level `mtime`, two writes of equal length within the
    /// same second (and any same-`(length, mtime)` in-place overwrite) are
    /// indistinguishable and may serve stale pages until the entry is evicted
    /// or its TTL elapses. Use a short `client_cache_ttl` — or extend the
    /// identity with an etag/version — where the UFS cannot guarantee
    /// millisecond `mtime` precision.
    async fn on_file_open(&self, _file_id: &str, _length: i64, _last_modification_time_ms: i64) {}

    /// Current operational state.
    fn state(&self) -> CacheState;
}

/// A [`CacheManager`] that caches nothing.
///
/// Every [`CacheManager::get`] returns `0` (miss) and every
/// [`CacheManager::put`] returns `false`. Used as the implementation when the
/// cache is disabled, and as a safe fallback when initialization fails.
#[derive(Debug, Default, Clone)]
pub struct DisabledCacheManager;

#[async_trait::async_trait]
impl CacheManager for DisabledCacheManager {
    async fn put(&self, _page_id: &PageId, _page: Bytes) -> bool {
        false
    }

    fn schedule_fill(self: Arc<Self>, _page_id: PageId, _page: Bytes) {
        // No-op: nothing to cache.
    }

    async fn get(&self, _page_id: &PageId, _page_offset: usize, _dst: &mut [u8]) -> usize {
        0
    }

    async fn delete(&self, _page_id: &PageId) -> bool {
        false
    }

    async fn invalidate(&self, _file_id: &str) {}

    fn state(&self) -> CacheState {
        CacheState::NotInUse
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_state_encoding() {
        assert_eq!(CacheState::NotInUse.as_i64(), 0);
        assert_eq!(CacheState::ReadOnly.as_i64(), 1);
        assert_eq!(CacheState::ReadWrite.as_i64(), 2);
    }

    #[tokio::test]
    async fn disabled_manager_always_misses() {
        let mgr = DisabledCacheManager;
        let id = PageId::new("file-1", 0);

        assert!(!mgr.put(&id, Bytes::from_static(b"hello")).await);

        let mut dst = [0u8; 8];
        assert_eq!(mgr.get(&id, 0, &mut dst).await, 0);
        assert_eq!(dst, [0u8; 8]);

        assert!(!mgr.delete(&id).await);
        mgr.invalidate("file-1").await; // no panic
        assert_eq!(mgr.state(), CacheState::NotInUse);
    }
}
