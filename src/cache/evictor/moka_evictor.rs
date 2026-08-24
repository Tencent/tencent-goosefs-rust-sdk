// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Moka-based cache evictor — **A/B baseline only, not for production**.
//!
//! Retained behind [`CacheEvictorBackend::Moka`](crate::config::CacheEvictorBackend)
//! so `benchmarks/cache_evictor_bench.rs` can measure the foyer replacement
//! against the implementation it replaced, under an identical manager flow.
//! [`FoyerCacheEvictor`](super::FoyerCacheEvictor) is the default.
//!
//! ## Why it is not the default
//!
//! `moka::sync::Cache` is used here as a concurrent map, not as an eviction
//! engine: `max_capacity` is `u64::MAX` so moka's own O(1) eviction never runs,
//! and the victim is instead found with `iter().min_by_key()`. That scan is
//! O(page count) — about 1-2 ms for the 100 GB / 100k-page working point, paid
//! on every fill once the directory is full. Because page *data* lives on SSD
//! and only page *identity* is held here, moka's capacity/eviction machinery
//! could not be used directly to manage the on-disk lifecycle. See
//! `docs/FOYER_SSD_CACHE_MIGRATION.md` §1.3.
//!
//! ## Design
//!
//! | Mode | Value semantics | Victim |
//! |------|-----------------|--------|
//! | LRU  | monotonic access tick | min tick = least recently used |
//! | LFU  | access frequency count | min count = least frequently used |
//!
//! The admit-then-evict contract of [`CacheEvictor`] is emulated: `on_add`
//! accounts the page and, while over quota, runs the scan to pick and queue
//! victims for `drain_victims`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use moka::sync::Cache as MokaCache;

use crate::cache::evictor::CacheEvictor;
use crate::cache::page_id::PageId;
use crate::config::CacheEvictorType;

/// Per-page eviction state.
#[derive(Clone, Copy, Debug)]
struct EvictMeta {
    /// Access tick (LRU) or access count (LFU). Smallest value is the victim.
    order: u64,
    /// Page size in bytes, for quota accounting.
    size: u64,
}

/// Moka-backed evictor supporting both the LRU and LFU policies.
///
/// All operations are O(1) except victim selection, which is O(page count).
pub struct MokaCacheEvictor {
    /// `PageId → EvictMeta`. `max_capacity = u64::MAX` disables moka's own
    /// eviction; the quota below is enforced by hand.
    cache: MokaCache<PageId, EvictMeta>,
    /// Monotonic counter for LRU ticks. `Relaxed` is sufficient — ticks only
    /// need to be unique, not strictly ordered across threads.
    counter: AtomicU64,
    mode: CacheEvictorType,
    /// Directory byte quota.
    capacity: u64,
    /// Bytes currently admitted.
    used: AtomicU64,
    /// Pages evicted since the last `drain_victims`.
    victims: Mutex<Vec<PageId>>,
}

impl MokaCacheEvictor {
    /// Build an evictor for `policy` holding at most `capacity` bytes.
    pub fn new(policy: CacheEvictorType, capacity: u64) -> Self {
        Self {
            cache: MokaCache::builder().max_capacity(u64::MAX).build(),
            counter: AtomicU64::new(0),
            mode: policy,
            capacity: capacity.max(1),
            used: AtomicU64::new(0),
            victims: Mutex::new(Vec::new()),
        }
    }

    /// Atomically get the next tick (LRU mode).
    #[inline]
    fn next_tick(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The O(page count) scan this backend exists to demonstrate.
    ///
    /// `run_pending_tasks` first so the iterator does not yield entries that
    /// have already been invalidated.
    fn scan_for_victim(&self) -> Option<(PageId, u64)> {
        self.cache.run_pending_tasks();
        self.cache
            .iter()
            .min_by_key(|(_, meta)| meta.order)
            .map(|(k, meta)| (k.as_ref().clone(), meta.size))
    }
}

impl CacheEvictor for MokaCacheEvictor {
    fn on_add(&self, id: &PageId, size: u64) {
        let order = match self.mode {
            CacheEvictorType::Lru => self.next_tick(),
            CacheEvictorType::Lfu => 1, // new page starts with frequency 1
        };
        let size = size.max(1);
        self.cache.insert(id.clone(), EvictMeta { order, size });
        self.used.fetch_add(size, Ordering::Relaxed);

        // Admit-then-evict: scan until the quota is satisfied again.
        while self.used.load(Ordering::Relaxed) > self.capacity {
            let Some((victim, victim_size)) = self.scan_for_victim() else {
                break;
            };
            self.cache.invalidate(&victim);
            self.used.fetch_sub(victim_size, Ordering::Relaxed);
            self.victims.lock().unwrap().push(victim);
        }
    }

    fn on_access(&self, id: &PageId) {
        match self.mode {
            CacheEvictorType::Lru => {
                // Refresh the access tick. Per-segment write lock, O(1).
                if let Some(meta) = self.cache.get(id) {
                    let order = self.next_tick();
                    self.cache.insert(id.clone(), EvictMeta { order, ..meta });
                }
            }
            CacheEvictorType::Lfu => {
                // Increment the frequency count. Read-modify-write — not
                // atomic, but a racy undercount is harmless for eviction
                // quality.
                if let Some(meta) = self.cache.get(id) {
                    self.cache.insert(
                        id.clone(),
                        EvictMeta {
                            order: meta.order.saturating_add(1),
                            ..meta
                        },
                    );
                }
            }
        }
    }

    fn on_remove(&self, id: &PageId) {
        if let Some(meta) = self.cache.get(id) {
            self.cache.invalidate(id);
            self.used.fetch_sub(meta.size, Ordering::Relaxed);
        }
    }

    fn drain_victims(&self) -> Vec<PageId> {
        std::mem::take(&mut *self.victims.lock().unwrap())
    }

    fn len(&self) -> usize {
        self.cache.run_pending_tasks();
        self.cache.entry_count() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const PAGE: u64 = 1024;

    fn pid(i: u64) -> PageId {
        PageId::new("f", i)
    }

    fn evictor(policy: CacheEvictorType, pages: u64) -> MokaCacheEvictor {
        MokaCacheEvictor::new(policy, PAGE * pages)
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let e = evictor(CacheEvictorType::Lru, 3);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);
        e.on_add(&pid(2), PAGE);
        assert!(e.drain_victims().is_empty());

        e.on_access(&pid(0));
        e.on_add(&pid(3), PAGE);
        assert_eq!(e.drain_victims(), vec![pid(1)]);
    }

    #[test]
    fn lfu_evicts_least_frequently_used() {
        let e = evictor(CacheEvictorType::Lfu, 3);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);
        e.on_add(&pid(2), PAGE);
        let _ = e.drain_victims();

        e.on_access(&pid(0));
        e.on_access(&pid(0));
        e.on_access(&pid(2));

        e.on_add(&pid(3), PAGE);
        let victims = e.drain_victims();

        // Pages enter at frequency 1, so the fresh page ties with the untouched
        // one and either may be picked; the accessed pages must not be.
        assert_eq!(victims.len(), 1);
        assert!(
            victims[0] == pid(1) || victims[0] == pid(3),
            "expected a frequency-1 page, got {victims:?}"
        );
    }

    #[test]
    fn under_quota_never_evicts() {
        let e = evictor(CacheEvictorType::Lru, 10);
        for i in 0..10 {
            e.on_add(&pid(i), PAGE);
        }
        assert!(e.drain_victims().is_empty());
    }

    #[test]
    fn on_remove_is_not_reported_as_a_victim() {
        let e = evictor(CacheEvictorType::Lru, 4);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);
        e.on_remove(&pid(0));
        assert!(e.drain_victims().is_empty());
    }

    #[test]
    fn remove_frees_quota() {
        let e = evictor(CacheEvictorType::Lru, 2);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);
        e.on_remove(&pid(0));

        e.on_add(&pid(2), PAGE);
        assert!(e.drain_victims().is_empty());
    }

    #[test]
    fn quota_is_bytes_not_pages() {
        let e = MokaCacheEvictor::new(CacheEvictorType::Lru, PAGE * 4);
        for i in 0..8 {
            e.on_add(&pid(i), PAGE / 2);
        }
        assert!(e.drain_victims().is_empty());

        e.on_add(&pid(8), PAGE / 2);
        assert_eq!(e.drain_victims().len(), 1);
    }

    #[test]
    fn concurrent_on_access_no_deadlock() {
        use std::thread;

        for policy in [CacheEvictorType::Lru, CacheEvictorType::Lfu] {
            let e = Arc::new(evictor(policy, 100));
            for i in 0..100u64 {
                e.on_add(&pid(i), PAGE);
            }
            let _ = e.drain_victims();

            let mut handles = Vec::new();
            for t in 0..8u64 {
                let e = Arc::clone(&e);
                handles.push(thread::spawn(move || {
                    for i in 0..1000u64 {
                        e.on_access(&pid((t * 1000 + i) % 100));
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            assert!(e.drain_victims().is_empty(), "reads must not evict");
        }
    }
}
