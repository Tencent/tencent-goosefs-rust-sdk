//! Least-Recently-Used eviction policy.
//!
//! Implemented with two synchronized maps:
//! - `ticks: HashMap<PageId, u64>` — page → last-access logical tick,
//! - `order: BTreeMap<u64, PageId>` — tick → page (ordered by recency).
//!
//! All operations are `O(log n)`. The LRU victim is the smallest tick in
//! `order`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use crate::cache::evictor::CacheEvictor;
use crate::cache::page_id::PageId;

#[derive(Default)]
struct LruState {
    ticks: HashMap<PageId, u64>,
    order: BTreeMap<u64, PageId>,
    counter: u64,
}

impl LruState {
    fn touch(&mut self, id: &PageId) {
        // Remove the old tick (if present) from the ordering index.
        if let Some(old) = self.ticks.get(id).copied() {
            self.order.remove(&old);
        }
        self.counter += 1;
        let tick = self.counter;
        self.ticks.insert(id.clone(), tick);
        self.order.insert(tick, id.clone());
    }

    fn remove(&mut self, id: &PageId) {
        if let Some(old) = self.ticks.remove(id) {
            self.order.remove(&old);
        }
    }
}

/// LRU evictor.
#[derive(Default)]
pub struct LruCacheEvictor {
    state: Mutex<LruState>,
}

impl LruCacheEvictor {
    /// Create an empty LRU evictor.
    pub fn new() -> Self {
        Self::default()
    }
}

impl CacheEvictor for LruCacheEvictor {
    fn on_add(&self, id: &PageId) {
        self.state.lock().unwrap().touch(id);
    }

    fn on_access(&self, id: &PageId) {
        // Only re-rank pages already tracked; a hit on an unknown page (racy
        // delete) should not resurrect it.
        let mut s = self.state.lock().unwrap();
        if s.ticks.contains_key(id) {
            s.touch(id);
        }
    }

    fn on_remove(&self, id: &PageId) {
        self.state.lock().unwrap().remove(id);
    }

    fn evict_candidate(&self) -> Option<PageId> {
        let s = self.state.lock().unwrap();
        s.order.values().next().cloned()
    }

    fn len(&self) -> usize {
        self.state.lock().unwrap().ticks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(i: u64) -> PageId {
        PageId::new("f", i)
    }

    #[test]
    fn evicts_least_recently_used() {
        let e = LruCacheEvictor::new();
        e.on_add(&pid(0));
        e.on_add(&pid(1));
        e.on_add(&pid(2));

        // Access page 0 → page 1 becomes the LRU victim.
        e.on_access(&pid(0));
        assert_eq!(e.evict_candidate(), Some(pid(1)));

        e.on_remove(&pid(1));
        // Now page 2 is the next victim (page 0 was just accessed).
        assert_eq!(e.evict_candidate(), Some(pid(2)));
    }

    #[test]
    fn empty_has_no_candidate() {
        let e = LruCacheEvictor::new();
        assert!(e.is_empty());
        assert_eq!(e.evict_candidate(), None);
    }

    #[test]
    fn remove_updates_len() {
        let e = LruCacheEvictor::new();
        e.on_add(&pid(0));
        e.on_add(&pid(1));
        assert_eq!(e.len(), 2);
        e.on_remove(&pid(0));
        assert_eq!(e.len(), 1);
        assert_eq!(e.evict_candidate(), Some(pid(1)));
    }

    #[test]
    fn access_unknown_page_is_noop() {
        let e = LruCacheEvictor::new();
        e.on_add(&pid(0));
        e.on_access(&pid(99)); // unknown
        assert_eq!(e.len(), 1);
        assert_eq!(e.evict_candidate(), Some(pid(0)));
    }
}
