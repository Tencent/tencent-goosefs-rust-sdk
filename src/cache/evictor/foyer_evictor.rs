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

//! foyer-backed cache evictor — backs both the LRU and LFU policies.
//!
//! Replaces the `moka`-backed evictor's `iter().min_by_key()` victim scan with
//! `foyer-memory`'s sharded intrusive eviction containers, where the victim is
//! the head of a linked list.
//!
//! ## Why
//!
//! The page cache keeps page *data* on SSD and only page *identity* in memory,
//! so the previous evictor disabled moka's own eviction (`max_capacity =
//! u64::MAX`) and picked victims by scanning every tracked page. That is
//! O(page count): about 1-2 ms per eviction for a 100 GB directory of 1 MiB
//! pages (~100k pages), paid on every fill once the directory is full — even at
//! a 99% hit rate, because the hit rate only controls how often the scan runs,
//! not what it costs. See `docs/FOYER_SSD_CACHE_MIGRATION.md` §1.4.
//!
//! ## Design
//!
//! The foyer cache holds `PageId → page size` and is given the directory's byte
//! quota as its capacity, with a weighter that reports each page's size. It
//! therefore evicts at exactly the point the directory would have overflowed.
//! Page *bytes* are never stored here — the value is just the weight.
//!
//! | Policy | foyer config | Eviction order |
//! |---|---|---|
//! | [`CacheEvictorType::Lru`] | [`LruConfig`] with no high-priority pool | Plain LRU |
//! | [`CacheEvictorType::Lfu`] | [`LfuConfig`] (w-TinyLFU) | Window / probation by frequency |
//!
//! Evictions are observed through foyer's [`EventListener`], which pushes the
//! evicted keys onto a queue drained by
//! [`CacheEvictor::drain_victims`]. foyer invokes the listener *outside* its
//! shard lock, but the listener still does nothing except push onto that queue:
//! it must never take a manager lock, touch the disk, or re-enter the cache.
//!
//! ## Sharding and effective capacity
//!
//! foyer divides the capacity evenly across shards and evicts per shard, so a
//! shard can overflow while the directory as a whole is still under quota. The
//! directory therefore fills to roughly the point where its *busiest* shard is
//! full, which is slightly below the configured quota. The manager handles this
//! by reclaiming whatever `drain_victims` reports rather than assuming eviction
//! only happens at the global limit.
//!
//! The shortfall is the hash imbalance across shards, on the order of
//! `1/sqrt(pages per shard)`. [`MIN_PAGES_PER_SHARD`] keeps that within a few
//! percent by giving small directories fewer shards: a directory holding fewer
//! than [`MIN_PAGES_PER_SHARD`] pages gets a single shard and fills exactly to
//! quota. The cap of [`MAX_SHARDS`] bounds it from the other side — at the
//! 100k-page working point each shard holds ~1.5k pages, for ~2-3% shortfall.
//!
//! Trading a few percent of capacity for shard concurrency is the same bargain
//! the previous moka-backed evictor made with its segments; what is new is only
//! that foyer also enforces the quota, so the effect is now observable.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use foyer_common::event::{Event, EventListener};
use foyer_memory::{Cache, CacheBuilder, LfuConfig, LruConfig};

use crate::cache::evictor::CacheEvictor;
use crate::cache::page_id::PageId;
use crate::config::CacheEvictorType;

/// Upper bound on shards. Matches the order of magnitude of moka's segment
/// count, which the previous evictor relied on for concurrency.
const MAX_SHARDS: usize = 64;

/// Minimum pages a shard should be able to hold.
///
/// Sharding more finely than this costs capacity: pages hash across shards
/// unevenly, and the directory stops filling once its busiest shard is full, so
/// the shortfall grows as `1/sqrt(pages per shard)`. At 256 pages per shard that
/// is a few percent; at 16 it would be ~25%. Below this many pages a directory
/// gets a single shard and fills exactly to quota.
const MIN_PAGES_PER_SHARD: u64 = 256;

/// Collects keys foyer evicts so the manager can reclaim the page files.
///
/// Only [`Event::Evict`] is recorded. [`Event::Remove`] is the manager removing
/// a page it already knows about (TTL expiry, invalidation, explicit delete)
/// and [`Event::Replace`] / [`Event::Clear`] likewise originate from the
/// caller, so reporting them would ask the manager to reclaim a page twice.
#[derive(Debug, Default)]
struct VictimCollector {
    victims: Mutex<Vec<PageId>>,
}

impl EventListener for VictimCollector {
    type Key = PageId;
    type Value = u64;

    fn on_leave(&self, reason: Event, key: &Self::Key, _value: &Self::Value) {
        if reason == Event::Evict {
            // Must stay allocation-cheap and lock-light: foyer calls this while
            // completing an insert. Never take a manager lock here.
            self.victims.lock().unwrap().push(key.clone());
        }
    }
}

/// foyer-backed evictor supporting both the LRU and LFU (w-TinyLFU) policies.
///
/// All operations are O(1); victim selection is the head of an intrusive list
/// rather than a scan over tracked pages.
pub struct FoyerCacheEvictor {
    /// `PageId → page size in bytes`. The value doubles as the entry weight, so
    /// the cache's capacity is the directory's byte quota.
    cache: Cache<PageId, u64>,
    /// Receives evicted keys from foyer.
    collector: Arc<VictimCollector>,
    /// Pages currently admitted. Tracked separately because `Cache::entries()`
    /// is not a cheap exact count on every backend, and `len()` is used by
    /// tests and by the occupancy gauges.
    tracked: AtomicU64,
}

impl FoyerCacheEvictor {
    /// Build an evictor for `policy` holding at most `capacity` bytes.
    ///
    /// `page_size` only sizes the shard count; pages shorter than `page_size`
    /// (file tails) are accounted at their real size.
    pub fn new(policy: CacheEvictorType, capacity: u64, page_size: u64) -> Self {
        let collector = Arc::new(VictimCollector::default());

        // foyer's capacity is in weight units, which here are bytes.
        let capacity_bytes = usize::try_from(capacity.max(1)).unwrap_or(usize::MAX);

        let builder = CacheBuilder::new(capacity_bytes)
            .with_name("goosefs-page-cache-evictor")
            .with_shards(shards_for(capacity, page_size))
            // The weight is the page's own size, so the cache overflows exactly
            // when the directory's byte quota would.
            .with_weighter(|_id: &PageId, size: &u64| usize::try_from(*size).unwrap_or(usize::MAX))
            .with_event_listener(collector.clone());

        let builder = match policy {
            // `high_priority_pool_ratio: 0.0` disables the priority pool so all
            // pages share one recency list — plain LRU. The page cache has no
            // notion of page priority to feed the pool.
            CacheEvictorType::Lru => builder.with_eviction_config(LruConfig {
                high_priority_pool_ratio: 0.0,
            }),
            // w-TinyLFU: an LFU admission filter in front of LRU eviction. Same
            // family as the TinyLFU the moka-backed evictor used, so the
            // `LFU` policy keeps its scan-resistant behaviour.
            CacheEvictorType::Lfu => builder.with_eviction_config(LfuConfig::default()),
        };

        Self {
            cache: builder.build(),
            collector,
            tracked: AtomicU64::new(0),
        }
    }

    /// Pages the policy currently holds, for assertions and gauges.
    #[cfg(test)]
    fn victim_queue_len(&self) -> usize {
        self.collector.victims.lock().unwrap().len()
    }
}

/// Pick a shard count that leaves each shard enough pages for its eviction
/// order to be meaningful.
///
/// foyer splits the capacity evenly across shards, so over-sharding a small
/// directory gives each shard a quota below one page and every insertion
/// immediately evicts itself.
fn shards_for(capacity: u64, page_size: u64) -> usize {
    let pages = capacity / page_size.max(1);
    let shards = pages / MIN_PAGES_PER_SHARD;
    shards.clamp(1, MAX_SHARDS as u64) as usize
}

impl CacheEvictor for FoyerCacheEvictor {
    fn on_add(&self, id: &PageId, size: u64) {
        // Dropping the returned entry immediately is required: while a
        // `CacheEntry` is alive foyer pins the record and will not evict it.
        drop(self.cache.insert(id.clone(), size.max(1)));
        self.tracked.fetch_add(1, Ordering::Relaxed);
    }

    fn on_access(&self, id: &PageId) {
        // `touch` updates the eviction order without handing out a `CacheEntry`
        // (which would pin the record until dropped). O(1), shard-local.
        self.cache.touch(id);
    }

    fn on_remove(&self, id: &PageId) {
        if self.cache.remove(id).is_some() {
            self.tracked.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn drain_victims(&self) -> Vec<PageId> {
        let mut queue = self.collector.victims.lock().unwrap();
        if queue.is_empty() {
            return Vec::new();
        }
        let victims = std::mem::take(&mut *queue);
        drop(queue);
        self.tracked
            .fetch_sub(victims.len() as u64, Ordering::Relaxed);
        victims
    }

    fn len(&self) -> usize {
        self.tracked.load(Ordering::Relaxed) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: u64 = 1024;

    fn pid(i: u64) -> PageId {
        PageId::new("f", i)
    }

    /// Capacity for exactly `pages` full pages.
    fn evictor(policy: CacheEvictorType, pages: u64) -> FoyerCacheEvictor {
        FoyerCacheEvictor::new(policy, PAGE * pages, PAGE)
    }

    // ── Shard sizing ───────────────────────────────────────────

    #[test]
    fn small_caches_get_a_single_shard() {
        // A 3-page directory sharded 64 ways would give every shard a quota
        // below one page, so each insert would evict itself.
        assert_eq!(shards_for(PAGE * 3, PAGE), 1);
        assert_eq!(shards_for(PAGE * MIN_PAGES_PER_SHARD, PAGE), 1);
        assert_eq!(shards_for(PAGE * MIN_PAGES_PER_SHARD * 2, PAGE), 2);
    }

    #[test]
    fn large_caches_are_capped_at_max_shards() {
        // 100 GB of 1 MiB pages ≈ 100k pages.
        let cap = 100 * 1024 * 1024 * 1024_u64;
        assert_eq!(shards_for(cap, 1024 * 1024), MAX_SHARDS);
    }

    // ── LRU ────────────────────────────────────────────────────

    #[test]
    fn lru_evicts_least_recently_used() {
        let e = evictor(CacheEvictorType::Lru, 3);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);
        e.on_add(&pid(2), PAGE);
        assert!(e.drain_victims().is_empty(), "3 pages fit in 3 page slots");

        // Touch page 0 so page 1 becomes the least recently used.
        e.on_access(&pid(0));

        // Admitting a 4th page evicts exactly one.
        e.on_add(&pid(3), PAGE);
        assert_eq!(e.drain_victims(), vec![pid(1)]);
    }

    #[test]
    fn lru_untouched_page_is_evicted_first() {
        let e = evictor(CacheEvictorType::Lru, 2);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);
        e.on_access(&pid(0));

        e.on_add(&pid(2), PAGE);
        assert_eq!(e.drain_victims(), vec![pid(1)]);
    }

    #[test]
    fn lru_under_quota_never_evicts() {
        let e = evictor(CacheEvictorType::Lru, 10);
        for i in 0..10 {
            e.on_add(&pid(i), PAGE);
        }
        assert!(e.drain_victims().is_empty());
        assert_eq!(e.len(), 10);
    }

    // ── LFU ────────────────────────────────────────────────────

    #[test]
    fn lfu_keeps_the_frequently_accessed_page() {
        let e = evictor(CacheEvictorType::Lfu, 8);
        for i in 0..8 {
            e.on_add(&pid(i), PAGE);
        }
        // Make page 0 clearly the hottest.
        for _ in 0..32 {
            e.on_access(&pid(0));
        }
        let _ = e.drain_victims();

        for i in 8..16 {
            e.on_add(&pid(i), PAGE);
        }
        let victims = e.drain_victims();
        assert!(!victims.is_empty(), "admitting past quota must evict");
        assert!(
            !victims.contains(&pid(0)),
            "the hottest page must not be evicted first, got {victims:?}"
        );
    }

    #[test]
    fn lfu_under_quota_never_evicts() {
        let e = evictor(CacheEvictorType::Lfu, 32);
        for i in 0..32 {
            e.on_add(&pid(i), PAGE);
        }
        assert!(e.drain_victims().is_empty());
    }

    // ── Shared behaviour ───────────────────────────────────────

    #[test]
    fn quota_is_bytes_not_pages() {
        // Capacity of 4 full pages, filled with half-sized pages: twice as many
        // pages fit before anything is evicted.
        let e = FoyerCacheEvictor::new(CacheEvictorType::Lru, PAGE * 4, PAGE);
        for i in 0..8 {
            e.on_add(&pid(i), PAGE / 2);
        }
        assert!(
            e.drain_victims().is_empty(),
            "8 half pages fit in a 4 page quota"
        );

        e.on_add(&pid(8), PAGE / 2);
        assert_eq!(e.drain_victims().len(), 1);
    }

    /// A sharded cache fills to its busiest shard, not to the nominal quota, so
    /// the last few percent of a large directory go unused. Documented rather
    /// than fixed — see the module docs on effective capacity.
    #[test]
    fn sharded_directories_fill_close_to_but_below_quota() {
        let pages = 4_000u64;
        let e = evictor(CacheEvictorType::Lru, pages);
        assert!(shards_for(PAGE * pages, PAGE) > 1, "test needs >1 shard");

        for i in 0..pages {
            e.on_add(&pid(i), PAGE);
        }
        let evicted = e.drain_victims().len() as u64;
        let resident = e.len() as u64;

        assert_eq!(resident + evicted, pages, "every page is accounted for");
        assert!(resident <= pages, "the quota is never exceeded");
        assert!(
            resident * 100 >= pages * 90,
            "shard imbalance should cost a few percent, not 10%: {resident}/{pages} resident"
        );
    }

    #[test]
    fn on_remove_is_not_reported_as_a_victim() {
        // The manager already reclaimed these pages; reporting them again would
        // double-count the eviction metrics and re-delete the page files.
        let e = evictor(CacheEvictorType::Lru, 4);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);
        e.on_remove(&pid(0));

        assert!(e.drain_victims().is_empty());
        assert_eq!(e.len(), 1);
    }

    #[test]
    fn remove_frees_quota() {
        let e = evictor(CacheEvictorType::Lru, 2);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);
        e.on_remove(&pid(0));

        // The freed slot means the next admission fits without eviction.
        e.on_add(&pid(2), PAGE);
        assert!(e.drain_victims().is_empty());
    }

    #[test]
    fn drain_victims_is_idempotent() {
        let e = evictor(CacheEvictorType::Lru, 1);
        e.on_add(&pid(0), PAGE);
        e.on_add(&pid(1), PAGE);

        assert_eq!(e.drain_victims(), vec![pid(0)]);
        assert!(
            e.drain_victims().is_empty(),
            "victims are taken, not copied"
        );
        assert_eq!(e.victim_queue_len(), 0);
    }

    #[test]
    fn len_tracks_admissions_and_evictions() {
        let e = evictor(CacheEvictorType::Lru, 4);
        assert!(e.is_empty());
        for i in 0..4 {
            e.on_add(&pid(i), PAGE);
        }
        assert_eq!(e.len(), 4);

        e.on_add(&pid(4), PAGE);
        let victims = e.drain_victims();
        assert_eq!(victims.len(), 1);
        assert_eq!(e.len(), 4, "one admitted, one evicted");
    }

    #[test]
    fn concurrent_on_access_no_deadlock() {
        // The moka evictor replaced a global `Mutex` that degraded 38x under 32
        // concurrent reads; the hot path must stay contention-free here too.
        use std::thread;

        for policy in [CacheEvictorType::Lru, CacheEvictorType::Lfu] {
            let e = Arc::new(evictor(policy, 100));
            for i in 0..100u64 {
                e.on_add(&pid(i), PAGE);
            }

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

            assert!(
                e.drain_victims().is_empty(),
                "reads must not evict ({policy:?})"
            );
            assert_eq!(e.len(), 100);
        }
    }

    #[test]
    fn concurrent_admissions_hold_the_quota() {
        use std::thread;

        // 8 threads admitting 500 pages each into a 100 page quota.
        let e = Arc::new(evictor(CacheEvictorType::Lru, 100));
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let e = Arc::clone(&e);
            handles.push(thread::spawn(move || {
                let mut evicted = 0usize;
                for i in 0..500u64 {
                    e.on_add(&PageId::new("f", t * 500 + i), PAGE);
                    evicted += e.drain_victims().len();
                }
                evicted
            }));
        }
        let evicted: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let evicted = evicted + e.drain_victims().len();

        // Everything admitted beyond the quota must have been reported, or the
        // manager would leak page files and its byte accounting would drift.
        assert_eq!(
            evicted + e.len(),
            8 * 500,
            "every admitted page is either resident or reported as a victim"
        );
        assert!(
            e.len() <= 100,
            "resident pages must respect the quota, got {}",
            e.len()
        );
    }

    /// The whole point of the migration: victim selection must not scale with
    /// the number of tracked pages.
    ///
    /// The moka-backed evictor scanned every page (`iter().min_by_key()`), so
    /// this ratio grew roughly 20x from 1e3 to 1e5 pages (see
    /// `benchmarks/cache_evictor_bench.rs`).
    #[test]
    fn eviction_cost_does_not_scale_with_page_count() {
        use std::time::Instant;

        const BATCH: u64 = 500;
        const ROUNDS: usize = 5;

        /// Best-case nanoseconds per evicting admission against a full cache.
        ///
        /// The minimum over several batches rather than the mean: this runs
        /// alongside the rest of the suite, and scheduler preemption can only
        /// ever add time, so the minimum is the stable statistic. The mean is
        /// not — it made this test flaky under a loaded machine.
        fn steady_state_evict_nanos(pages: u64) -> u128 {
            let e = evictor(CacheEvictorType::Lru, pages);
            for i in 0..pages {
                e.on_add(&pid(i), PAGE);
            }
            let _ = e.drain_victims();

            let mut next = pages;
            (0..ROUNDS)
                .map(|_| {
                    let start = Instant::now();
                    for _ in 0..BATCH {
                        e.on_add(&pid(next), PAGE);
                        let _ = e.drain_victims();
                        next += 1;
                    }
                    start.elapsed().as_nanos() / BATCH as u128
                })
                .min()
                .unwrap()
                .max(1)
        }

        let small = steady_state_evict_nanos(10_000);
        let large = steady_state_evict_nanos(100_000);

        // O(1) would be ~1x. Allow headroom for allocator and cache effects at
        // 10x the working set; a linear scan would be ~10x and still fail.
        assert!(
            large < small * 5,
            "eviction looks superlinear: {small} ns at 10k pages vs {large} ns at 100k pages"
        );

        // Absolute budget at the 100k-page working point. Generous next to the
        // sub-microsecond cost of a list-head pop, but the moka evictor's scan
        // was ~23 ms here, so this catches any regression back to a scan even
        // if the ratio above happened to look flat.
        const BUDGET_NS: u128 = 50_000; // 50 µs
        assert!(
            large < BUDGET_NS,
            "eviction at 100k pages took {large} ns, over the {BUDGET_NS} ns budget"
        );
    }
}
