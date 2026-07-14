//! Cache eviction policies.
//!
//! A [`CacheEvictor`] decides which page to drop when the cache is full.
//! Mirrors Java `CacheEvictor`. The evictor only tracks page *identity* and
//! access order — byte accounting and the actual page removal live in the
//! cache manager.
//!
//! Both LRU and LFU are backed by [`MokaCacheEvictor`] using
//! `moka::sync::Cache` with per-segment write locks (~64 segments), replacing
//! the previous global `Mutex` implementations that caused 38x contention
//! under 32 concurrent reads. See
//! `docs/perf/2026-07-09-oncpu6-concurrent-uring-analysis/MOKA_LRU_OPTIMIZATION.md`.

mod moka_evictor;

pub use moka_evictor::MokaCacheEvictor;

use crate::config::CacheEvictorType;

/// Eviction policy abstraction.
///
/// Implementations must be internally synchronized (the manager calls these
/// from async contexts, potentially concurrently).
pub trait CacheEvictor: Send + Sync {
    /// Record that a new page was added.
    fn on_add(&self, id: &crate::cache::page_id::PageId);
    /// Record that a page was accessed (read hit).
    fn on_access(&self, id: &crate::cache::page_id::PageId);
    /// Record that a page was removed (evicted or invalidated).
    fn on_remove(&self, id: &crate::cache::page_id::PageId);
    /// Return the next page that should be evicted, if any.
    fn evict_candidate(&self) -> Option<crate::cache::page_id::PageId>;
    /// Number of pages currently tracked.
    fn len(&self) -> usize;
    /// `true` if no pages are tracked.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Build an evictor for the configured policy.
///
/// Both policies are backed by `MokaCacheEvictor`:
/// - `Lru` → moka with `EvictionPolicy::lru()` + tick-based recency
/// - `Lfu` → moka with `EvictionPolicy::tiny_lfu()` + frequency counting
pub fn build_evictor(policy: CacheEvictorType) -> Box<dyn CacheEvictor> {
    match policy {
        CacheEvictorType::Lru => Box::new(MokaCacheEvictor::new_lru()),
        CacheEvictorType::Lfu => Box::new(MokaCacheEvictor::new_lfu()),
    }
}
