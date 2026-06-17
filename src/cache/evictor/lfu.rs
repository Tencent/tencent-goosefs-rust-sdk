//! Least-Frequently-Used eviction policy.
//!
//! Tracks an access frequency per page. The victim is the page with the
//! smallest frequency; ties are broken by insertion order (oldest first) via a
//! monotonic sequence number so eviction is deterministic.
//!
//! `evict_candidate` is `O(n)` (a scan for the minimum). LFU is opt-in and
//! caches are moderate in size, so this is acceptable for P2.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::cache::evictor::CacheEvictor;
use crate::cache::page_id::PageId;

#[derive(Default)]
struct LfuState {
    /// `page → (frequency, insertion-sequence)`.
    entries: HashMap<PageId, (u64, u64)>,
    seq: u64,
}

/// LFU evictor.
#[derive(Default)]
pub struct LfuCacheEvictor {
    state: Mutex<LfuState>,
}

impl LfuCacheEvictor {
    /// Create an empty LFU evictor.
    pub fn new() -> Self {
        Self::default()
    }
}

impl CacheEvictor for LfuCacheEvictor {
    fn on_add(&self, id: &PageId) {
        let mut s = self.state.lock().unwrap();
        s.seq += 1;
        let seq = s.seq;
        s.entries.insert(id.clone(), (1, seq));
    }

    fn on_access(&self, id: &PageId) {
        let mut s = self.state.lock().unwrap();
        if let Some(entry) = s.entries.get_mut(id) {
            entry.0 = entry.0.saturating_add(1);
        }
    }

    fn on_remove(&self, id: &PageId) {
        self.state.lock().unwrap().entries.remove(id);
    }

    fn evict_candidate(&self) -> Option<PageId> {
        let s = self.state.lock().unwrap();
        s.entries
            .iter()
            // Minimum by (frequency, sequence): least frequent, then oldest.
            .min_by_key(|(_, (freq, seq))| (*freq, *seq))
            .map(|(id, _)| id.clone())
    }

    fn len(&self) -> usize {
        self.state.lock().unwrap().entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(i: u64) -> PageId {
        PageId::new("f", i)
    }

    #[test]
    fn evicts_least_frequently_used() {
        let e = LfuCacheEvictor::new();
        e.on_add(&pid(0)); // freq 1
        e.on_add(&pid(1)); // freq 1
        e.on_add(&pid(2)); // freq 1

        // Bump page 0 and 2; page 1 stays least frequent.
        e.on_access(&pid(0));
        e.on_access(&pid(2));
        assert_eq!(e.evict_candidate(), Some(pid(1)));
    }

    #[test]
    fn tie_breaks_by_insertion_order() {
        let e = LfuCacheEvictor::new();
        e.on_add(&pid(5));
        e.on_add(&pid(6));
        // Equal frequency → oldest (page 5) is the victim.
        assert_eq!(e.evict_candidate(), Some(pid(5)));
    }

    #[test]
    fn remove_and_len() {
        let e = LfuCacheEvictor::new();
        e.on_add(&pid(0));
        e.on_add(&pid(1));
        assert_eq!(e.len(), 2);
        e.on_remove(&pid(0));
        assert_eq!(e.len(), 1);
        assert_eq!(e.evict_candidate(), Some(pid(1)));
    }

    #[test]
    fn empty_has_no_candidate() {
        let e = LfuCacheEvictor::new();
        assert!(e.is_empty());
        assert_eq!(e.evict_candidate(), None);
    }
}
