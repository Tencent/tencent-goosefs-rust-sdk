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

//! Moka-based concurrent cache evictor — backs both LRU and LFU policies.
//!
//! Replaces the old `Mutex<LruState>` / `Mutex<LfuState>` with
//! `moka::sync::Cache<PageId, u64>`, using moka's per-segment write locks
//! (~64 segments) instead of a single global mutex.
//!
//! ## Why
//!
//! Under 32 concurrent reads, the global `Mutex` in the old evictors caused
//! 38x latency degradation (20µs → 772µs) and extreme tail latency
//! (P95/P50 = 204x). See
//! the Moka LRU optimisation analysis.
//!
//! ## Design
//!
//! A single struct [`MokaCacheEvictor`] handles both policies via a
//! [`EvictMode`] flag:
//!
//! | Mode | moka `EvictionPolicy` | Value semantics | `evict_candidate` |
//! |------|-----------------------|-----------------|-------------------|
//! | LRU  | `EvictionPolicy::lru()` | monotonic access tick | min tick = oldest |
//! | LFU  | `EvictionPolicy::tiny_lfu()` | access frequency count | min count = least frequent |
//!
//! - **`max_capacity`**: `u64::MAX` — no auto-eviction; eviction is driven
//!   manually by [`LocalCacheManager::pop_victim`](crate::cache::manager::LocalCacheManager).
//! - **`on_access`** (LRU): `insert(id, next_tick())` — O(1), per-segment lock.
//! - **`on_access`** (LFU): `get(id)` → increment → `insert(id, count+1)` —
//!   read-modify-write, not atomic but acceptable for best-effort eviction
//!   (same race tolerance as the old `LfuCacheEvictor`).
//! - **`evict_candidate`**: `iter().min_by_key(value)` — O(n) scan, cold path
//!   (only on `put` when cache is full).
//!
//! ## Reference
//!
//! Lance uses `moka::future::Cache` as its entire cache backend
//! (`lance-core/src/cache/moka.rs`), replacing hand-written LRU entirely.

use std::sync::atomic::{AtomicU64, Ordering};

use moka::policy::EvictionPolicy;
use moka::sync::Cache as MokaCache;

use crate::cache::evictor::CacheEvictor;
use crate::cache::page_id::PageId;

/// Which eviction semantics to use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EvictMode {
    /// Track access recency via a monotonic tick; evict the smallest tick.
    Lru,
    /// Track access frequency via a counter; evict the smallest count.
    Lfu,
}

/// Moka-backed evictor supporting both LRU and LFU (W-TinyLFU) policies.
///
/// Uses `moka::sync::Cache<PageId, u64>` for per-segment concurrent access.
/// All operations are O(1) except `evict_candidate` which is O(n) (cold path).
pub struct MokaCacheEvictor {
    /// `PageId → access_tick` (LRU) or `PageId → frequency_count` (LFU).
    /// `max_capacity = u64::MAX` disables moka's auto-eviction; we evict
    /// manually via `evict_candidate`.
    cache: MokaCache<PageId, u64>,
    /// Monotonic counter for LRU ticks. `Relaxed` ordering is sufficient —
    /// ticks only need to be unique, not strictly ordered across threads.
    counter: AtomicU64,
    /// LRU (tick-based) or LFU (frequency-based) semantics.
    mode: EvictMode,
}

impl MokaCacheEvictor {
    /// Create an LRU evictor backed by moka with `EvictionPolicy::lru()`.
    pub fn new_lru() -> Self {
        Self {
            cache: MokaCache::builder()
                .max_capacity(u64::MAX)
                .eviction_policy(EvictionPolicy::lru())
                .build(),
            counter: AtomicU64::new(0),
            mode: EvictMode::Lru,
        }
    }

    /// Create an LFU evictor backed by moka with `EvictionPolicy::tiny_lfu()`.
    ///
    /// moka's TinyLFU combines LRU eviction with an LFU admission filter
    /// (FrequencySketch), providing better scan-resistance than pure LFU.
    pub fn new_lfu() -> Self {
        Self {
            cache: MokaCache::builder()
                .max_capacity(u64::MAX)
                .eviction_policy(EvictionPolicy::tiny_lfu())
                .build(),
            counter: AtomicU64::new(0),
            mode: EvictMode::Lfu,
        }
    }

    /// Atomically get the next tick (LRU mode).
    #[inline]
    fn next_tick(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

impl CacheEvictor for MokaCacheEvictor {
    fn on_add(&self, id: &PageId) {
        let value = match self.mode {
            EvictMode::Lru => self.next_tick(),
            EvictMode::Lfu => 1, // new page starts with frequency 1
        };
        self.cache.insert(id.clone(), value);
    }

    fn on_access(&self, id: &PageId) {
        match self.mode {
            EvictMode::Lru => {
                // LRU: update the access tick. Per-segment write lock, O(1).
                let tick = self.next_tick();
                self.cache.insert(id.clone(), tick);
            }
            EvictMode::Lfu => {
                // LFU: increment the frequency count. Read-modify-write —
                // not atomic, but a racy undercount is harmless for eviction
                // quality (same tolerance as the old LfuCacheEvictor).
                let current = self.cache.get(id).unwrap_or(0);
                self.cache.insert(id.clone(), current.saturating_add(1));
            }
        }
    }

    fn on_remove(&self, id: &PageId) {
        self.cache.invalidate(id);
    }

    fn evict_candidate(&self) -> Option<PageId> {
        // O(n) scan for the minimum value. This is the cold path — only called
        // by `pop_victim` when the cache is full and a `put` needs to make
        // room. For a 10k-entry cache this takes ~100-200µs, far less than the
        // disk IO that follows.
        //
        // `run_pending_tasks` ensures pending invalidations are applied so the
        // iterator does not yield already-removed entries.
        self.cache.run_pending_tasks();
        self.cache
            .iter()
            .min_by_key(|(_, v)| *v)
            .map(|(k, _)| k.as_ref().clone())
    }

    fn len(&self) -> usize {
        self.cache.entry_count() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(i: u64) -> PageId {
        PageId::new("f", i)
    }

    // ── LRU tests ──────────────────────────────────────────────

    #[test]
    fn lru_evicts_least_recently_used() {
        let e = MokaCacheEvictor::new_lru();
        e.on_add(&pid(0));
        e.on_add(&pid(1));
        e.on_add(&pid(2));

        // Access page 0 → page 1 becomes the LRU victim.
        e.on_access(&pid(0));
        assert_eq!(e.evict_candidate(), Some(pid(1)));

        e.on_remove(&pid(1));
        assert_eq!(e.evict_candidate(), Some(pid(2)));
    }

    #[test]
    fn lru_empty_has_no_candidate() {
        let e = MokaCacheEvictor::new_lru();
        assert!(e.is_empty());
        assert_eq!(e.evict_candidate(), None);
    }

    #[test]
    fn lru_access_updates_recency() {
        let e = MokaCacheEvictor::new_lru();
        e.on_add(&pid(0));
        e.on_add(&pid(1));

        // Without accessing p0, p0 should be the victim (older tick).
        assert_eq!(e.evict_candidate(), Some(pid(0)));

        // Access p0 → p1 becomes the victim.
        e.on_access(&pid(0));
        assert_eq!(e.evict_candidate(), Some(pid(1)));
    }

    // ── LFU tests ──────────────────────────────────────────────

    #[test]
    fn lfu_evicts_least_frequently_used() {
        let e = MokaCacheEvictor::new_lfu();
        e.on_add(&pid(0)); // freq=1
        e.on_add(&pid(1)); // freq=1
        e.on_add(&pid(2)); // freq=1

        // Access p0 twice → p0 freq=3, p1/p2 freq=1.
        e.on_access(&pid(0));
        e.on_access(&pid(0));

        // Victim should be p1 or p2 (both freq=1), not p0 (freq=3).
        let victim = e.evict_candidate().unwrap();
        assert!(
            victim == pid(1) || victim == pid(2),
            "victim should be p1 or p2, got {victim:?}"
        );
        assert_ne!(victim, pid(0), "p0 (frequent) should not be evicted");
    }

    #[test]
    fn lfu_empty_has_no_candidate() {
        let e = MokaCacheEvictor::new_lfu();
        assert!(e.is_empty());
        assert_eq!(e.evict_candidate(), None);
    }

    #[test]
    fn lfu_frequency_increments_on_access() {
        let e = MokaCacheEvictor::new_lfu();
        e.on_add(&pid(0)); // freq=1
        e.on_add(&pid(1)); // freq=1

        // Access p0 three times → p0 freq=4, p1 freq=1.
        e.on_access(&pid(0));
        e.on_access(&pid(0));
        e.on_access(&pid(0));

        // p1 (freq=1) should be evicted before p0 (freq=4).
        assert_eq!(e.evict_candidate(), Some(pid(1)));
    }

    // ── Shared tests ───────────────────────────────────────────

    #[test]
    fn lru_remove_updates_len() {
        let e = MokaCacheEvictor::new_lru();
        e.on_add(&pid(0));
        e.on_add(&pid(1));
        e.cache.run_pending_tasks();
        assert_eq!(e.len(), 2);
        e.on_remove(&pid(0));
        e.cache.run_pending_tasks();
        assert_eq!(e.len(), 1);
        assert_eq!(e.evict_candidate(), Some(pid(1)));
    }

    #[test]
    fn lfu_remove_updates_len() {
        let e = MokaCacheEvictor::new_lfu();
        e.on_add(&pid(0));
        e.on_add(&pid(1));
        e.cache.run_pending_tasks();
        assert_eq!(e.len(), 2);
        e.on_remove(&pid(0));
        e.cache.run_pending_tasks();
        assert_eq!(e.len(), 1);
        assert_eq!(e.evict_candidate(), Some(pid(1)));
    }

    #[test]
    fn concurrent_on_access_no_deadlock() {
        // Verify that concurrent on_access calls don't deadlock — the primary
        // motivation for replacing Mutex<LruState> with moka.
        use std::sync::Arc;
        use std::thread;

        for evictor in [MokaCacheEvictor::new_lru(), MokaCacheEvictor::new_lfu()] {
            let e = Arc::new(evictor);
            for i in 0..100u64 {
                e.on_add(&pid(i));
            }

            let mut handles = Vec::new();
            for t in 0..8 {
                let e = Arc::clone(&e);
                handles.push(thread::spawn(move || {
                    for i in 0..1000u64 {
                        let id = pid((t * 1000 + i) % 100);
                        e.on_access(&id);
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }

            e.cache.run_pending_tasks();
            assert_eq!(e.len(), 100);
        }
    }
}
