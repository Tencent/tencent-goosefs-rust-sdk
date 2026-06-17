//! Cache eviction policies.
//!
//! A [`CacheEvictor`] decides which page to drop when the cache is full.
//! Mirrors Java `CacheEvictor`. The evictor only tracks page *identity* and
//! access order — byte accounting and the actual page removal live in the
//! cache manager.

mod lfu;
mod lru;

pub use lfu::LfuCacheEvictor;
pub use lru::LruCacheEvictor;

use crate::cache::page_id::PageId;
use crate::config::CacheEvictorType;

/// Eviction policy abstraction.
///
/// Implementations must be internally synchronized (the manager calls these
/// from async contexts, potentially concurrently).
pub trait CacheEvictor: Send + Sync {
    /// Record that a new page was added.
    fn on_add(&self, id: &PageId);
    /// Record that a page was accessed (read hit).
    fn on_access(&self, id: &PageId);
    /// Record that a page was removed (evicted or invalidated).
    fn on_remove(&self, id: &PageId);
    /// Return the next page that should be evicted, if any.
    fn evict_candidate(&self) -> Option<PageId>;
    /// Number of pages currently tracked.
    fn len(&self) -> usize;
    /// `true` if no pages are tracked.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Build an evictor for the configured policy.
pub fn build_evictor(policy: CacheEvictorType) -> Box<dyn CacheEvictor> {
    match policy {
        CacheEvictorType::Lru => Box::new(LruCacheEvictor::new()),
        CacheEvictorType::Lfu => Box::new(LfuCacheEvictor::new()),
    }
}
