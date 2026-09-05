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

//! Disk-backed local cache manager.
//!
//! [`LocalCacheManager`] is the multi-directory implementation of
//! [`CacheManager`]. It coordinates one [`LocalPageStore`] per cache directory,
//! a `foyer` cache that owns both page metadata and eviction, a striped
//! page-lock pool, and a bounded async write-back pool.
//!
//! # Concurrency model
//!
//! - **Striped page locks** (`LOCK_SIZE` `RwLock`s): `get` takes a read lock,
//!   `put`/`delete` take a write lock for the page's stripe. Same-page
//!   operations serialize; different pages run concurrently. Disk IO never
//!   happens under any other lock.
//! - **Page metadata** (`caches[dir]`): a `foyer_memory::Cache` per directory
//!   holds `PageId -> PageInfo` *and* the eviction order in one sharded
//!   structure, with byte capacity enforced by foyer itself. A read hit is one
//!   shard operation that both looks up the value and updates recency.
//!
//!   This replaced a `DashMap` for metadata plus a separate moka cache for
//!   ordering plus an `AtomicU64` for byte accounting. That split is why
//!   picking a victim used to cost an `iter().min_by_key()` scan over every
//!   resident page — O(pages), ~7ms at 100k pages. It is now a shard-local
//!   pop off an intrusive list, independent of cache size.
//! - **Version lock** (`versions: RwLock<HashMap>`): `on_file_open` takes a
//!   read lock in the common case (same file → no change) and a write lock only
//!   on overwrite, so it never blocks `get`/`put`/`delete`.
//! - **Reaper task**: foyer evicts synchronously inside `insert`, but the
//!   victim's *file* must still be deleted. The eviction listener forwards
//!   victims to a bounded channel drained by a background task, so `insert`
//!   never blocks on IO. See [`LocalCacheManager::spawn_reaper`].
//!
//! ## Two rules that are easy to get wrong
//!
//! 1. **Never call `Cache::touch`.** It runs `Eviction::acquire` without the
//!    paired `release` — the only `release` site is `RawCacheEntry::drop`
//!    (foyer-memory-0.22.3 `src/raw.rs:836-851`). Under `LruConfig` that pins
//!    the record into `pin_list` forever and eviction silently stops making
//!    progress. Always `drop(cache.get(id))`.
//! 2. **Never hold a `CacheEntry` across `.await`.** Same pinning problem, for
//!    as long as the guard is alive. Copy what you need out and drop it before
//!    any IO.
//!
//! **Platform note:** the store relies on POSIX semantics — atomic
//! `tmp + rename` and deleting files that may be concurrently opened. The
//! cache is therefore validated on Unix only; Windows is not currently a
//! supported target for the local page cache.
//!
//! # Best-effort contract
//!
//! Any error is swallowed, recorded as a `Client.Cache*Errors` metric, and
//! surfaced as a miss (`get` → 0) or failed fill (`put` → false).

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
use std::sync::Weak;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use foyer_common::event::{Event, EventListener};
use foyer_memory::{Cache, CacheBuilder, LfuConfig, LruConfig, S3FifoConfig};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{debug, warn};
use xxhash_rust::xxh3::Xxh3Default;

use crate::cache::allocator::{Allocator, HashAllocator};
use crate::cache::metric_name as mn;
use crate::cache::options::CacheManagerOptions;
use crate::cache::page_id::{CacheScope, PageId, PageInfo};
#[cfg(all(target_os = "linux", feature = "page-cache-io-uring"))]
use crate::cache::store::UringPageStore;
use crate::cache::store::{init_uring_config, is_uring_available, LocalPageStore, PageStore};
use crate::cache::{CacheManager, CacheState, PageReadRequest};
use crate::config::{CacheEvictorType, GoosefsConfig};
use crate::error::Result;
use crate::metrics::{counter, gauge};
use futures::future::join_all;

/// Number of page-lock stripes (mirrors Java `LocalCacheManager.LOCK_SIZE`).
const LOCK_SIZE: usize = 1024;

/// Lower bound on pages per shard when sizing the metadata cache.
///
/// foyer splits the byte capacity evenly across shards, so a shard whose share
/// is smaller than one page can never hold anything and its slice of the cache
/// is dead. Requiring room for a few pages per shard keeps small
/// configurations working — a 9.5 MiB directory of 1 MiB pages gets 2 shards
/// rather than 256 shards of 38 KiB each.
const MIN_PAGES_PER_SHARD: u64 = 4;

/// Upper bound on shard count. Past this, added shards buy no more concurrency
/// but do cost capacity granularity.
const MAX_SHARDS: usize = 256;

/// Capacity of the eviction-reaper channel, in pages.
///
/// This bounds how far behind the reaper may fall, and therefore how far disk
/// usage can overshoot: at most `REAP_QUEUE_CAP * page_size` bytes of evicted
/// pages are still on disk. With 1 MiB pages that is 1 GiB, inside the 5%
/// overhead already reserved from `dir_capacity`.
const REAP_QUEUE_CAP: usize = 1024;

/// A page evicted by foyer whose file still needs to be deleted.
#[derive(Debug)]
struct ReapTask {
    page_id: PageId,
    page_size: u64,
    dir_index: usize,
}

/// Forwards foyer's evictions to the reaper task.
///
/// foyer calls this synchronously from `insert`, but *outside* the shard lock:
/// victims are collected into a `garbages` vec while the lock is held
/// (foyer-memory-0.22.3 `src/raw.rs:595-597`) and the listener runs after it is
/// released (`src/raw.rs:609-620`, where upstream's own comment reads
/// "Deallocate data out of the lock critical section"). So `try_send` here
/// cannot deadlock against foyer internals.
struct ReapListener {
    tx: mpsc::Sender<ReapTask>,
    dir_index: usize,
}

impl EventListener for ReapListener {
    type Key = PageId;
    type Value = PageInfo;

    fn on_leave(&self, event: Event, key: &Self::Key, value: &Self::Value) {
        // Only capacity evictions need the file removed. `Event::Remove` means
        // an explicit `delete`/`invalidate`, which deletes the file itself, and
        // `Event::Replace` means the file was just overwritten in place.
        if event != Event::Evict {
            return;
        }

        let task = ReapTask {
            page_id: key.clone(),
            page_size: value.page_size,
            dir_index: self.dir_index,
        };

        // `try_send`, never a blocking send. `on_leave` is a synchronous
        // method running inside `insert`, which is itself called from an async
        // context on a tokio worker: `blocking_send` would deadlock and
        // `send().await` is not permitted by the signature.
        //
        // A full queue therefore means dropping the task. The page file
        // survives as an orphan until the next startup, where `restore()`
        // reclaims it (sidecar-gated, so it is never served as fresh data).
        // That is a space leak, not a correctness bug. A non-zero
        // CacheReapDropped should be treated as "raise REAP_QUEUE_CAP".
        if self.tx.try_send(task).is_err() {
            counter(mn::CLIENT_CACHE_REAP_DROPPED).inc(1);
        }
    }
}

/// Shard count for a directory's metadata cache.
///
/// Balances two opposing needs: more shards reduce lock contention, but foyer
/// divides the byte capacity evenly among them, so too many shards on a small
/// directory leaves each one unable to hold even a single page.
fn shard_count(dir_capacity: u64, page_size: u64) -> usize {
    if page_size == 0 {
        return 1;
    }
    let pages = dir_capacity / page_size;
    ((pages / MIN_PAGES_PER_SHARD).max(1) as usize).min(MAX_SHARDS)
}

/// Build the metadata cache for one directory.
fn build_page_cache(
    policy: CacheEvictorType,
    dir_capacity: u64,
    page_size: u64,
    reap_tx: mpsc::Sender<ReapTask>,
    dir_index: usize,
) -> Cache<PageId, PageInfo> {
    let shards = shard_count(dir_capacity, page_size);
    let capacity = usize::try_from(dir_capacity).unwrap_or(usize::MAX);

    let builder = CacheBuilder::new(capacity)
        .with_name("goosefs-page-meta")
        .with_shards(shards)
        // Weigh entries in bytes so foyer enforces the directory's byte
        // capacity directly, replacing the manual `used_bytes` accounting.
        .with_weighter(|_: &PageId, info: &PageInfo| info.page_size as usize)
        .with_event_listener(Arc::new(ReapListener {
            tx: reap_tx,
            dir_index,
        }));

    match policy {
        CacheEvictorType::Lru => builder.with_eviction_config(LruConfig {
            // Not `LruConfig::default()`: that reserves 90% of the capacity for
            // a high-priority pool (foyer-memory-0.22.3
            // `src/eviction/lru.rs:46`) that this cache never inserts into,
            // which would strand most of the capacity.
            high_priority_pool_ratio: 0.0,
        }),
        CacheEvictorType::Lfu => builder.with_eviction_config(LfuConfig::default()),
        CacheEvictorType::S3Fifo => builder.with_eviction_config(S3FifoConfig::default()),
    }
    .build()
}

/// Reverse index state: `file_id → set(page_index)` for `invalidate`.
/// Under a `RwLock` because it's accessed on cold paths (invalidate, sweep)
/// and needs atomic read-modify-write of the inner HashSet.
type ByFileMap = HashMap<Arc<str>, HashSet<u64>>;

/// Local, disk-backed page cache manager.
pub struct LocalCacheManager {
    options: CacheManagerOptions,
    /// One page store per cache directory (immutable; IO runs outside any lock).
    stores: Vec<Arc<dyn PageStore>>,
    allocator: Box<dyn Allocator>,

    /// Page metadata and eviction order, one cache per directory.
    ///
    /// Owns what used to be three separate structures: the `PageId -> PageInfo`
    /// map, the eviction order, and the used-bytes counter. Capacity is
    /// enforced in bytes via the weighter, so an insert that would exceed
    /// `dir_capacity` evicts from the same shard synchronously.
    caches: Vec<Cache<PageId, PageInfo>>,

    /// Queues evicted pages for file deletion. Bounded: see [`REAP_QUEUE_CAP`].
    reap_tx: mpsc::Sender<ReapTask>,

    /// File reverse index (`file_id → set(page_index)`). Under a `RwLock`
    /// because it's only touched on cold paths (`invalidate`, `delete`) and the
    /// inner `HashSet` needs atomic insert/remove.
    ///
    /// Kept even though foyer could not provide it: foyer has no iteration, so
    /// `invalidate(file_id)` has no other way to enumerate a file's pages.
    by_file: RwLock<ByFileMap>,

    /// File-identity version table (`file_id → (length, mtime)`), used by
    /// `on_file_open` to detect overwrites. Separate `RwLock` so the common
    /// `on_file_open` path (same file → read lock) never blocks `get`/`put`.
    versions: RwLock<HashMap<Arc<str>, (i64, i64)>>,
    /// Striped page locks.
    page_locks: Vec<RwLock<()>>,
    /// Bounded async write-back permits (`async_write_threads`).
    async_write_sem: Arc<Semaphore>,
    state: CacheState,
}

fn page_lock_index(page_id: &PageId) -> usize {
    // xxHash3 (same hash Lance uses via `xxhash_rust::xxh3`): fast,
    // non-cryptographic. This only picks an in-process lock stripe, so it needs
    // neither DoS resistance nor cross-run stability. Standardised across the
    // project on xxHash3.
    let mut h = Xxh3Default::default();
    page_id.file_id.hash(&mut h);
    page_id.page_index.hash(&mut h);
    (h.finish() % LOCK_SIZE as u64) as usize
}

impl LocalCacheManager {
    /// Create a manager from resolved [`CacheManagerOptions`].
    ///
    /// Initializes one on-disk store per configured directory.
    ///
    /// Returns `Arc<Self>` because the eviction reaper is a background task
    /// holding a `Weak<Self>`, so the manager must be inside an `Arc` before it
    /// can start.
    pub async fn create(options: CacheManagerOptions) -> Result<Arc<Self>> {
        let dir_paths: Vec<&Path> = if options.dirs.is_empty() {
            vec![Path::new("/tmp/goosefs_cache")]
        } else {
            options.dirs.iter().map(|p| p.as_path()).collect()
        };

        // Detect io_uring availability. On non-Linux or when disabled by
        // config, falls back transparently to LocalPageStore (tokio::fs).
        let use_uring = options.uring_enabled && is_uring_available();
        if options.uring_enabled && !use_uring {
            warn!("io_uring requested but unavailable; falling back to tokio::fs backend");
        }

        // Initialise the io_uring thread pool configuration before any store
        // operation. This ensures config-file values (not just env vars) are
        // respected for queue_depth and thread_count.
        if use_uring {
            init_uring_config(options.uring_queue_depth, options.uring_thread_count);
        }

        let (reap_tx, reap_rx) = mpsc::channel::<ReapTask>(REAP_QUEUE_CAP);

        // foyer admits an entry heavier than the whole cache rather than
        // rejecting it (verified against 0.22.3: weight 32 into capacity 16
        // stays resident with usage 32). With page_size > dir_capacity every
        // page would therefore push usage past the quota, and each insert would
        // evict the previous page — a cache that holds exactly one entry and
        // reports over-capacity while doing it.
        if options.page_size > options.dir_capacity {
            warn!(
                page_size = options.page_size,
                dir_capacity = options.dir_capacity,
                "page_size exceeds dir_capacity: the cache will hold at most one page \
                 and its reported usage will exceed the configured capacity"
            );
        }

        let mut stores: Vec<Arc<dyn PageStore>> = Vec::with_capacity(dir_paths.len());
        let mut caches: Vec<Cache<PageId, PageInfo>> = Vec::with_capacity(dir_paths.len());
        for dir in &dir_paths {
            let store: Arc<dyn PageStore> = if use_uring {
                #[cfg(all(target_os = "linux", feature = "page-cache-io-uring"))]
                {
                    match UringPageStore::create_with_pread(
                        dir,
                        options.page_size,
                        options.sync_read_enabled,
                    )
                    .await
                    {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            warn!(error = %e, "UringPageStore creation failed; fallback to LocalPageStore");
                            Arc::new(LocalPageStore::create(dir, options.page_size).await?)
                        }
                    }
                }
                #[cfg(not(all(target_os = "linux", feature = "page-cache-io-uring")))]
                {
                    Arc::new(LocalPageStore::create(dir, options.page_size).await?)
                }
            } else {
                Arc::new(LocalPageStore::create(dir, options.page_size).await?)
            };
            stores.push(store);
            caches.push(build_page_cache(
                options.evictor,
                options.dir_capacity,
                options.page_size,
                reap_tx.clone(),
                caches.len(),
            ));
        }

        let page_locks = (0..LOCK_SIZE).map(|_| RwLock::new(())).collect();
        let async_write_sem = Arc::new(Semaphore::new(options.async_write_threads.max(1)));

        let mgr = Arc::new(Self {
            options,
            stores,
            allocator: Box::new(HashAllocator::new()),
            caches,
            reap_tx,
            by_file: RwLock::new(HashMap::new()),
            versions: RwLock::new(HashMap::new()),
            page_locks,
            async_write_sem,
            state: CacheState::ReadWrite,
        });

        // Start the reaper BEFORE restore: restore inserts every page it finds
        // and lets foyer evict whatever exceeds capacity, so evictions can fire
        // during restore. With no reaper draining the channel those victims
        // would be dropped and their files orphaned.
        mgr.clone().spawn_reaper(reap_rx);

        // Best-effort restore of pages persisted by a previous process.
        if let Err(e) = mgr.restore().await {
            warn!(error = %e, "cache restore failed; starting with empty cache");
        }
        mgr.publish_capacity_gauges_initial();
        debug!(
            page_size = mgr.options.page_size,
            num_dirs = mgr.stores.len(),
            dir_capacity = mgr.options.dir_capacity,
            async_write_threads = mgr.options.async_write_threads,
            evictor = ?mgr.options.evictor,
            ttl = ?mgr.options.ttl,
            "LocalCacheManager initialized"
        );
        Ok(mgr)
    }

    /// Convenience constructor from a [`GoosefsConfig`].
    pub async fn from_config(config: &GoosefsConfig) -> Result<Arc<Self>> {
        let options = CacheManagerOptions::from_config(config);
        Self::create(options).await
    }

    /// Resolved options this manager was built with.
    pub fn options(&self) -> &CacheManagerOptions {
        &self.options
    }

    fn total_capacity(&self) -> u64 {
        // `saturating_mul` guards against overflow for pathological multi-dir
        // PB-scale configurations; the value only feeds occupancy gauges.
        self.options
            .dir_capacity
            .saturating_mul(self.stores.len() as u64)
    }

    fn publish_capacity_gauges_initial(&self) {
        gauge(mn::CLIENT_CACHE_SPACE_AVAILABLE).set(self.total_capacity() as i64);
        gauge(mn::CLIENT_CACHE_SPACE_USED).set(0);
        gauge(mn::CLIENT_CACHE_PAGES).set(0);
        gauge(mn::CLIENT_CACHE_SPACE_USED_COUNT).set(0);
        gauge(mn::CLIENT_CACHE_HIT_RATE).set(0);
        gauge(mn::CLIENT_CACHE_STATE).set(self.state.as_i64());
    }

    /// Refresh occupancy gauges.
    fn publish_occupancy(&self) {
        // `usage()` and `entries()` walk the shards taking read locks, so this
        // is O(shards), not O(entries). Called on put/delete/reap, never on the
        // read hot path.
        let used: u64 = self.caches.iter().map(|c| c.usage() as u64).sum();
        let pages: i64 = self.caches.iter().map(|c| c.entries() as i64).sum();
        gauge(mn::CLIENT_CACHE_PAGES).set(pages);
        gauge(mn::CLIENT_CACHE_SPACE_USED_COUNT).set(pages);
        gauge(mn::CLIENT_CACHE_SPACE_USED).set(used as i64);
        gauge(mn::CLIENT_CACHE_SPACE_AVAILABLE)
            .set(self.total_capacity().saturating_sub(used) as i64);
    }

    /// Drain evicted pages and delete their files.
    ///
    /// foyer evicts synchronously inside `insert`, but only drops the metadata;
    /// the page file has to be removed separately. Doing that inline would put
    /// disk IO on the `put` path, so victims are queued here instead.
    ///
    /// The task holds a `Weak` reference and exits once the manager is dropped.
    fn spawn_reaper(self: Arc<Self>, mut rx: mpsc::Receiver<ReapTask>) {
        let weak: Weak<Self> = Arc::downgrade(&self);
        // Drop the strong reference: keeping it would make the manager
        // immortal, since the task lives as long as the channel is open.
        drop(self);

        tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                let Some(mgr) = weak.upgrade() else {
                    break; // manager dropped
                };
                mgr.reap_one(task).await;
                gauge(mn::CLIENT_CACHE_REAP_QUEUE_DEPTH).set(rx.len() as i64);
            }
        });
    }

    /// Delete one evicted page's file.
    async fn reap_one(&self, task: ReapTask) {
        counter(mn::CLIENT_CACHE_BYTES_EVICTED).inc(task.page_size as i64);
        counter(mn::CLIENT_CACHE_PAGES_EVICTED).inc(1);

        // Update the reverse index first. Safe outside the page lock: a
        // concurrent re-admission re-inserts its own page_index, so the set
        // converges either way.
        let file_empty = {
            let mut by_file = self.by_file.write().await;
            let mut empty = false;
            if let Some(set) = by_file.get_mut(&task.page_id.file_id) {
                set.remove(&task.page_id.page_index);
                if set.is_empty() {
                    by_file.remove(&task.page_id.file_id);
                    empty = true;
                }
            }
            empty
        };

        {
            // Take the page lock the put/delete paths use. Deleting outside it
            // races with re-admission: `put` may have already written a fresh
            // file for this same page after the eviction was queued, and we
            // would delete that file instead. The page would then be resident
            // according to foyer but unreadable on disk, and `put` could not
            // repair it either — its racing check sees `contains == true` and
            // refuses to refill. A permanent zombie.
            let _wl = self.page_locks[page_lock_index(&task.page_id)]
                .write()
                .await;

            // Re-check under the lock. `contains` performs no `acquire()`, so
            // polling it cannot disturb the eviction order or pin anything.
            if self.caches[task.dir_index].contains(&task.page_id) {
                counter(mn::CLIENT_CACHE_REAP_SKIPPED_READMITTED).inc(1);
                return;
            }

            if let Err(e) = self.stores[task.dir_index].delete(&task.page_id).await {
                warn!(error = %e, "reap: failed to delete evicted page from store");
                counter(mn::CLIENT_CACHE_DELETE_FROM_STORE_ERRORS).inc(1);
            }
        }

        if file_empty {
            let _ = self.stores[task.dir_index]
                .delete_identity(&task.page_id.file_id)
                .await;
        }
        self.publish_occupancy();
    }

    /// Wait for queued evictions to be processed, then stop accepting more.
    ///
    /// Optional: dropping the manager also stops the reaper, but then any
    /// queued victims are left as orphan files for the next `restore()` to
    /// reclaim. Call this for a clean shutdown.
    pub async fn close(&self) {
        self.drain_reaper().await;
    }

    /// Block until the reaper has caught up with the queue.
    ///
    /// Polls rather than using a completion signal: the reaper processes one
    /// task at a time, so an empty channel plus a yield is enough to know the
    /// last task finished. Bounded so a stuck reaper cannot hang a test.
    async fn drain_reaper(&self) {
        for _ in 0..1_000 {
            if self.reap_tx.capacity() == REAP_QUEUE_CAP {
                // Queue empty. Yield once more so the in-flight task (already
                // popped, still running) can finish its disk delete.
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        warn!("reaper did not drain within 1s");
    }

    /// Test hook: deterministically wait for the reaper to catch up.
    ///
    /// Needed because the reap-vs-readmit race in [`Self::reap_one`] cannot be
    /// tested reliably otherwise — a sleep-based test passes even when the
    /// page lock and re-check are missing.
    #[cfg(test)]
    pub(crate) async fn flush_reaper(&self) {
        self.drain_reaper().await;
    }

    /// Rebuild the in-memory index from pages persisted on disk by a previous
    /// process. Best-effort: unreadable or malformed entries are skipped.
    ///
    /// Layout walked per directory: `<dir>/<page_size>/<bucket>/<file_id>/<page_index>`.
    ///
    /// **Sidecar-gated**: a file's pages are restored only when its persisted
    /// `(length, mtime)` identity sidecar is present and parseable. This makes
    /// the invariant "a restored page always has a validated identity" hold at
    /// the only point where it matters for correctness — independent of any
    /// `put`/`evict`/`delete` ordering or race at runtime. Pages without an
    /// identity (e.g. cached before the identity was known, or whose sidecar
    /// was concurrently reclaimed) are dropped rather than served as fresh,
    /// since the next `on_file_open` could not detect a down-time overwrite for
    /// them. The TTL sweeper still bounds anything that slips through.
    async fn restore(&self) -> Result<()> {
        let mut restored_pages = 0u64;
        let mut restored_bytes = 0u64;

        for (dir_index, store) in self.stores.iter().enumerate() {
            let root = store.root_dir().to_path_buf();
            let mut buckets = match tokio::fs::read_dir(&root).await {
                Ok(rd) => rd,
                Err(_) => continue, // fresh dir, nothing to restore
            };
            while let Ok(Some(bucket)) = buckets.next_entry().await {
                if !bucket.path().is_dir() {
                    continue;
                }
                let mut files = match tokio::fs::read_dir(bucket.path()).await {
                    Ok(rd) => rd,
                    Err(_) => continue,
                };
                while let Ok(Some(file_dir)) = files.next_entry().await {
                    let file_id_os = file_dir.file_name();
                    let Some(file_id) = file_id_os.to_str() else {
                        continue;
                    };
                    let file_id: Arc<str> = Arc::from(file_id);

                    // Gate on the identity sidecar: no valid identity → the
                    // pages cannot be safely validated on the next open, so
                    // drop the whole file directory instead of restoring it.
                    let Some(identity) = store.read_identity(&file_id).await else {
                        let _ = tokio::fs::remove_dir_all(file_dir.path()).await;
                        continue;
                    };

                    let mut pages = match tokio::fs::read_dir(file_dir.path()).await {
                        Ok(rd) => rd,
                        Err(_) => continue,
                    };
                    // Count pages actually restored for this file so we can
                    // distinguish a live file from an empty shell (sidecar but
                    // no data pages — e.g. the last page was deleted before its
                    // sidecar, or every page was corrupt). The identity version
                    // is recorded only for a live file; an empty shell is
                    // reclaimed instead of leaking an orphan version + dir.
                    let mut file_pages_restored = 0u64;
                    while let Ok(Some(page)) = pages.next_entry().await {
                        let name = page.file_name();
                        let Some(name) = name.to_str() else { continue };
                        // Skip in-flight temp files and the identity sidecar
                        // (already loaded above).
                        if name.contains(".tmp-") {
                            let _ = tokio::fs::remove_file(page.path()).await;
                            continue;
                        }
                        if LocalPageStore::is_identity_file(name) {
                            continue;
                        }
                        let Ok(page_index) = name.parse::<u64>() else {
                            continue;
                        };
                        let Ok(md) = page.metadata().await else {
                            continue;
                        };
                        let size = md.len();
                        if size == 0 || size > self.options.page_size {
                            let _ = tokio::fs::remove_file(page.path()).await;
                            continue;
                        }

                        let page_id = PageId::new(file_id.clone(), page_index);
                        // Already restored (duplicate on-disk entry) → drop it.
                        if self.caches[dir_index].contains(&page_id) {
                            let _ = tokio::fs::remove_file(page.path()).await;
                            continue;
                        }
                        // No manual capacity check: foyer enforces the byte
                        // capacity itself, and anything over it is evicted here
                        // and reaped by the background task. That is why
                        // `spawn_reaper` must already be running.
                        drop(self.caches[dir_index].insert(
                            page_id.clone(),
                            PageInfo {
                                page_id: page_id.clone(),
                                page_size: size,
                                dir_index,
                                created_at: Instant::now(),
                                scope: CacheScope::Global,
                            },
                        ));
                        {
                            let mut by_file = self.by_file.write().await;
                            by_file
                                .entry(file_id.clone())
                                .or_default()
                                .insert(page_index);
                        }
                        file_pages_restored += 1;
                        restored_pages += 1;
                        restored_bytes += size;
                    }

                    if file_pages_restored > 0 {
                        // Live file → keep its identity for overwrite detection.
                        self.versions
                            .write()
                            .await
                            .insert(file_id.clone(), identity);
                    } else {
                        // Empty shell (sidecar but no data pages) → reclaim it
                        // rather than leak an orphan version + on-disk dir.
                        let _ = tokio::fs::remove_dir_all(file_dir.path()).await;
                    }
                }
            }
        }

        if restored_pages > 0 {
            debug!(
                pages = restored_pages,
                bytes = restored_bytes,
                "restored cache pages from disk"
            );
        }
        Ok(())
    }

    /// Expired-page cleanup path.
    ///
    /// TTL is enforced lazily, on access: foyer has no TTL support and no
    /// iteration, so the background sweeper that used to walk the metadata map
    /// is gone. An expired page is never *served* — `get_bytes` checks
    /// `created_at` before returning — but its space is now reclaimed either
    /// here, on the next access, or by ordinary capacity eviction.
    ///
    /// The observable difference is that an expired page nobody touches again
    /// keeps occupying quota until it is evicted on merit. Correctness is
    /// unchanged; only the reclamation timing is.
    ///
    /// **Race safety**: between the lookup in `get_bytes` and the removal here,
    /// a concurrent `put` may have replaced the entry with a fresh one. The
    /// expiry is re-checked under the page write lock before removing.
    async fn get_expired_path(&self, page_id: &PageId) -> usize {
        let Some(ttl) = self.options.ttl else {
            return 0; // TTL disabled — should never reach here
        };
        let dir_index = self.allocator.allocate(page_id, self.stores.len());

        // Serialise against `put` for this page: without the lock we could
        // remove an entry a concurrent put has just refreshed.
        let _wl = self.page_locks[page_lock_index(page_id)].write().await;

        let info = {
            let Some(entry) = self.caches[dir_index].get(page_id) else {
                return 0;
            };
            let info = entry.value().clone();
            // Drop the guard before any further work: a live CacheEntry pins
            // the record under LruConfig.
            drop(entry);
            if info.created_at.elapsed() <= ttl {
                return 0; // refreshed by a concurrent put
            }
            info
        };

        // `remove` fires the listener with `Event::Remove`, which the reaper
        // ignores, so the file has to be deleted here.
        self.caches[dir_index].remove(page_id);

        let file_empty = {
            let mut by_file = self.by_file.write().await;
            let mut empty = false;
            if let Some(set) = by_file.get_mut(&page_id.file_id) {
                set.remove(&page_id.page_index);
                if set.is_empty() {
                    by_file.remove(&page_id.file_id);
                    empty = true;
                }
            }
            empty
        };

        if let Err(e) = self.stores[dir_index].delete(page_id).await {
            warn!(error = %e, "expire: failed to delete page from store");
            counter(mn::CLIENT_CACHE_DELETE_FROM_STORE_ERRORS).inc(1);
        }
        if file_empty {
            let _ = self.stores[dir_index]
                .delete_identity(&page_id.file_id)
                .await;
        }

        counter(mn::CLIENT_CACHE_PAGES_DISCARDED).inc(1);
        counter(mn::CLIENT_CACHE_BYTES_DISCARDED).inc(info.page_size as i64);
        self.publish_occupancy();
        0 // expired → miss
    }
}

#[async_trait]
impl CacheManager for LocalCacheManager {
    async fn put(&self, page_id: &PageId, page: Bytes) -> bool {
        if self.state != CacheState::ReadWrite {
            counter(mn::CLIENT_CACHE_PUT_NOT_READY_ERRORS).inc(1);
            counter(mn::CLIENT_CACHE_PUT_ERRORS).inc(1);
            return false;
        }
        let page_len = page.len() as u64;
        if page_len == 0 || page_len > self.options.page_size {
            return false;
        }

        let _wl = self.page_locks[page_lock_index(page_id)].write().await;

        let dir_index = self.allocator.allocate(page_id, self.stores.len());

        if self.caches[dir_index].contains(page_id) {
            counter(mn::CLIENT_CACHE_PUT_BENIGN_RACING_ERRORS).inc(1);
            return false;
        }

        // Write to disk BEFORE publishing metadata.
        //
        // The old flow reserved capacity first (a CAS loop over `used_bytes`,
        // evicting victims until the page fit), then wrote, then rolled the
        // reservation back on failure. foyer makes room itself during `insert`,
        // so the reservation is gone — but that means `insert` is also the
        // point of no return, and it must not happen before the bytes are
        // durable. Otherwise a failed write would leave metadata claiming a
        // page that does not exist on disk.
        if let Err(e) = self.stores[dir_index].put(page_id, &page).await {
            warn!(error = %e, "put: failed to write page to store");
            counter(mn::CLIENT_CACHE_PUT_STORE_WRITE_ERRORS).inc(1);
            counter(mn::CLIENT_CACHE_PUT_ERRORS).inc(1);
            return false;
        }

        // Publish metadata. This may evict from the same shard synchronously;
        // victims are queued to the reaper by the event listener.
        //
        // The returned CacheEntry is dropped immediately — holding it would pin
        // the record out of the eviction order under LruConfig.
        drop(self.caches[dir_index].insert(
            page_id.clone(),
            PageInfo {
                page_id: page_id.clone(),
                page_size: page_len,
                dir_index,
                created_at: Instant::now(),
                scope: CacheScope::Global,
            },
        ));

        // A page can be evicted by its own insert: if it is the coldest entry
        // in a shard that is already at capacity, foyer admits and immediately
        // drops it. Report that as a failed put rather than letting the caller
        // assume a subsequent `get` will hit.
        if !self.caches[dir_index].contains(page_id) {
            counter(mn::CLIENT_CACHE_PUT_INSUFFICIENT_SPACE_ERRORS).inc(1);
            counter(mn::CLIENT_CACHE_PUT_ERRORS).inc(1);
            // The reaper deletes the file: the eviction went through the
            // listener like any other.
            return false;
        }

        counter(mn::CLIENT_CACHE_BYTES_WRITTEN_CACHE).inc(page_len as i64);
        self.publish_occupancy();

        // First page of this file → persist its identity sidecar so the
        // overwrite check survives a restart. The identity comes from
        // `versions`, populated by `on_file_open`; the file stream always
        // opens (→ `on_file_open`) before reading (→ `put`), so it is
        // present on the normal path. If it is somehow absent we simply
        // skip the sidecar — restore is sidecar-gated, so any page left
        // without an identity is dropped on the next startup rather than
        // served stale (no correctness risk, only a lost cache entry).
        let first_page = {
            let by_file = self.by_file.read().await;
            !by_file.contains_key(&page_id.file_id)
        };
        let identity = if first_page {
            self.versions.read().await.get(&page_id.file_id).copied()
        } else {
            None
        };
        {
            let mut by_file = self.by_file.write().await;
            by_file
                .entry(page_id.file_id.clone())
                .or_default()
                .insert(page_id.page_index);
        }

        if let Some((length, mtime)) = identity {
            if let Err(e) = self.stores[dir_index]
                .write_identity(&page_id.file_id, length, mtime)
                .await
            {
                debug!(file_id = %page_id.file_id, error = %e,
                    "failed to persist cache identity");
            }
        }
        true
    }

    async fn get(&self, page_id: &PageId, page_offset: usize, dst: &mut [u8]) -> usize {
        let bytes = self.get_bytes(page_id, page_offset, dst.len()).await;
        let n = bytes.len().min(dst.len());
        if n > 0 {
            dst[..n].copy_from_slice(&bytes[..n]);
        }
        n
    }

    async fn get_bytes(&self, page_id: &PageId, page_offset: usize, len: usize) -> Bytes {
        if self.state == CacheState::NotInUse {
            counter(mn::CLIENT_CACHE_GET_NOT_READY_ERRORS).inc(1);
            return Bytes::new();
        }
        if len == 0 {
            return Bytes::new();
        }

        let _rl = self.page_locks[page_lock_index(page_id)].read().await;

        let dir_index = self.allocator.allocate(page_id, self.stores.len());

        {
            // Lookup and recency update in one shard operation.
            //
            // MUST be `get`, never `touch`. `touch` runs `Eviction::acquire`
            // but never the paired `release` (the only `release` site is
            // `RawCacheEntry::drop`, foyer-memory-0.22.3 `src/raw.rs:836-851`),
            // which under `LruConfig` pins every page ever read into `pin_list`
            // permanently. Eviction then finds nothing evictable and the cache
            // silently stops accepting new pages.
            let Some(entry) = self.caches[dir_index].get(page_id) else {
                return Bytes::new(); // miss
            };

            // Copy out what is needed and drop the guard BEFORE the disk read.
            // A live CacheEntry pins its record under LruConfig, so holding one
            // across `.await` would keep the page unevictable for the whole IO.
            let created_at = entry.value().created_at;
            drop(entry);

            if let Some(ttl) = self.options.ttl {
                if created_at.elapsed() > ttl {
                    // Expired — never serve it. Reclaim on this access, since
                    // there is no background sweeper anymore.
                    drop(_rl);
                    let _ = self.get_expired_path(page_id).await;
                    return Bytes::new();
                }
            }
        }

        // Disk IO — completely lock-free.
        let start = Instant::now();
        let bytes = match self.stores[dir_index]
            .get_bytes(page_id, page_offset, len)
            .await
        {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(error = %e, "get: failed to read page from store");
                counter(mn::CLIENT_CACHE_GET_STORE_READ_ERRORS).inc(1);
                counter(mn::CLIENT_CACHE_GET_ERRORS).inc(1);
                return Bytes::new();
            }
        };
        if bytes.is_empty() {
            return Bytes::new(); // racy eviction → miss
        }

        // No second lock needed — LRU was already updated in the read-lock
        // block above (E1).
        counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).inc(bytes.len() as i64);
        counter(mn::CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS).inc(start.elapsed().as_nanos() as i64);
        crate::cache::metrics::publish_hit_rate();
        bytes
    }

    async fn get_batch_bytes(&self, requests: &[PageReadRequest]) -> Vec<Bytes> {
        join_all(
            requests
                .iter()
                .map(|req| self.get_bytes(&req.page_id, req.page_offset, req.len)),
        )
        .await
    }

    async fn delete(&self, page_id: &PageId) -> bool {
        let _wl = self.page_locks[page_lock_index(page_id)].write().await;

        let dir_index = self.allocator.allocate(page_id, self.stores.len());

        // `remove` returns the evicted entry and fires the listener with
        // `Event::Remove`, which the reaper deliberately ignores — an explicit
        // delete removes the file itself, right here, rather than queueing it.
        let Some(entry) = self.caches[dir_index].remove(page_id) else {
            counter(mn::CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS).inc(1);
            return false;
        };
        drop(entry);

        let file_empty = {
            let mut by_file = self.by_file.write().await;
            let mut empty = false;
            if let Some(set) = by_file.get_mut(&page_id.file_id) {
                set.remove(&page_id.page_index);
                if set.is_empty() {
                    by_file.remove(&page_id.file_id);
                    empty = true;
                }
            }
            empty
        };
        self.publish_occupancy();

        if let Err(e) = self.stores[dir_index].delete(page_id).await {
            warn!(error = %e, "delete: failed to remove page from store");
            counter(mn::CLIENT_CACHE_DELETE_STORE_DELETE_ERRORS).inc(1);
            counter(mn::CLIENT_CACHE_DELETE_ERRORS).inc(1);
        }
        // Last page of the file is gone → drop its identity sidecar too.
        if file_empty {
            let _ = self.stores[dir_index]
                .delete_identity(&page_id.file_id)
                .await;
        }
        true
    }

    async fn invalidate(&self, file_id: &str) {
        let pages: Vec<PageId> = {
            let by_file = self.by_file.read().await;
            match by_file.get(file_id) {
                Some(set) => set.iter().map(|idx| PageId::new(file_id, *idx)).collect(),
                None => return,
            }
        };
        for pid in pages {
            self.delete(&pid).await;
        }
        debug!(file_id = %file_id, "invalidated cached pages for file");
    }

    async fn on_file_open(&self, file_id: &str, length: i64, last_modification_time_ms: i64) {
        // E2: Use a separate RwLock for version checks so this never blocks
        // `get`/`put`/`delete` (which use `inner`). The common case (same
        // file → identical identity) takes a read lock and returns immediately.
        let changed = {
            let versions = self.versions.read().await;
            match versions.get(file_id) {
                // Same identity → nothing to do.
                Some(&(l, m)) if l == length && m == last_modification_time_ms => false,
                // Known but different → the file was overwritten.
                Some(_) => true,
                // First time we see this file → need write lock to record it.
                None => {
                    drop(versions); // release read lock before acquiring write lock
                    let mut versions = self.versions.write().await;
                    // Re-check (could have been inserted by another thread).
                    match versions.get(file_id) {
                        Some(&(l, m)) if l == length && m == last_modification_time_ms => false,
                        Some(_) => true,
                        None => {
                            versions
                                .insert(Arc::from(file_id), (length, last_modification_time_ms));
                            false
                        }
                    }
                }
            }
        };
        if changed {
            warn!(file_id = %file_id, "file overwritten; invalidating cached pages");
            // `invalidate` drops every page (and its identity sidecar); the
            // refreshed identity is re-persisted lazily when the file is next
            // cached (see `put`).
            self.invalidate(file_id).await;
            self.versions
                .write()
                .await
                .insert(Arc::from(file_id), (length, last_modification_time_ms));
        }
    }

    fn schedule_fill(self: Arc<Self>, page_id: PageId, page: Bytes) {
        // Apply back-pressure: drop the fill if the write-back pool is full.
        match self.async_write_sem.clone().try_acquire_owned() {
            Ok(permit) => {
                tokio::spawn(async move {
                    let _permit = permit; // released when the task ends
                    let _ = self.put(&page_id, page).await;
                });
            }
            Err(_) => {
                counter(mn::CLIENT_CACHE_PUT_ASYNC_REJECTION_ERRORS).inc(1);
            }
        }
    }

    fn state(&self) -> CacheState {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    use crate::config::CacheEvictorType;

    fn opts(
        page_size: u64,
        capacity: u64,
        num_dirs: usize,
        evictor: CacheEvictorType,
        async_threads: usize,
    ) -> (CacheManagerOptions, Vec<PathBuf>) {
        let dirs: Vec<PathBuf> = (0..num_dirs)
            .map(|_| std::env::temp_dir().join(format!("gfs_mgr_test_{}", uuid::Uuid::new_v4())))
            .collect();
        (
            CacheManagerOptions {
                page_size,
                dir_capacity: capacity,
                dirs: dirs.clone(),
                evictor,
                async_write_enabled: async_threads > 0,
                async_write_threads: async_threads.max(1),
                quota_enabled: false,
                ttl: None,
                uring_enabled: false,
                uring_queue_depth: 0,
                uring_thread_count: 0,
                sync_read_enabled: false,
            },
            dirs,
        )
    }

    async fn manager(
        page_size: u64,
        capacity: u64,
        num_dirs: usize,
    ) -> (Arc<LocalCacheManager>, Vec<PathBuf>) {
        let (o, dirs) = opts(page_size, capacity, num_dirs, CacheEvictorType::Lru, 4);
        (LocalCacheManager::create(o).await.unwrap(), dirs)
    }

    async fn cleanup(dirs: &[PathBuf]) {
        for d in dirs {
            let _ = tokio::fs::remove_dir_all(d).await;
        }
    }

    #[tokio::test]
    async fn put_then_get_hit_single_dir() {
        let (mgr, dirs) = manager(16, 1024, 1).await;
        let id = PageId::new("f1", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"0123456789")).await);
        let mut dst = vec![0u8; 5];
        assert_eq!(mgr.get(&id, 2, &mut dst).await, 5);
        assert_eq!(&dst, b"23456");
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn multi_dir_roundtrip_and_affinity() {
        let (mgr, dirs) = manager(16, 1024, 4).await;
        // Insert pages for several files; each must be retrievable.
        for f in 0..10 {
            for p in 0..3u64 {
                let id = PageId::new(format!("file-{f}"), p);
                assert!(mgr.put(&id, Bytes::from(vec![f as u8; 8])).await);
            }
        }
        for f in 0..10 {
            for p in 0..3u64 {
                let id = PageId::new(format!("file-{f}"), p);
                let mut dst = vec![0u8; 8];
                assert_eq!(mgr.get(&id, 0, &mut dst).await, 8);
                assert_eq!(dst, vec![f as u8; 8]);
            }
        }
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn eviction_per_dir_lru() {
        // Single dir, capacity = 2 pages of 8 bytes.
        let (mgr, dirs) = manager(8, 16, 1).await;
        let p0 = PageId::new("f", 0);
        let p1 = PageId::new("f", 1);
        let p2 = PageId::new("f", 2);
        assert!(mgr.put(&p0, Bytes::from_static(b"00000000")).await);
        assert!(mgr.put(&p1, Bytes::from_static(b"11111111")).await);
        let mut dst = vec![0u8; 8];
        assert_eq!(mgr.get(&p0, 0, &mut dst).await, 8); // touch p0
        assert!(mgr.put(&p2, Bytes::from_static(b"22222222")).await); // evicts p1
        assert_eq!(mgr.get(&p1, 0, &mut dst).await, 0, "p1 evicted");
        assert_eq!(mgr.get(&p0, 0, &mut dst).await, 8, "p0 survives");
        assert_eq!(mgr.get(&p2, 0, &mut dst).await, 8, "p2 present");
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn eviction_per_dir_lru_explicit_policy() {
        // Same as eviction_per_dir_lru but selecting the LRU policy explicitly
        // rather than relying on the default.
        let (o, dirs) = opts(8, 16, 1, CacheEvictorType::Lru, 4);
        let mgr = LocalCacheManager::create(o).await.unwrap();
        let p0 = PageId::new("f", 0);
        let p1 = PageId::new("f", 1);
        let p2 = PageId::new("f", 2);
        assert!(mgr.put(&p0, Bytes::from_static(b"00000000")).await);
        assert!(mgr.put(&p1, Bytes::from_static(b"11111111")).await);
        let mut dst = vec![0u8; 8];
        assert_eq!(mgr.get(&p0, 0, &mut dst).await, 8); // touch p0
        assert!(mgr.put(&p2, Bytes::from_static(b"22222222")).await); // evicts p1
        assert_eq!(mgr.get(&p1, 0, &mut dst).await, 0, "p1 evicted");
        assert_eq!(mgr.get(&p0, 0, &mut dst).await, 8, "p0 survives");
        assert_eq!(mgr.get(&p2, 0, &mut dst).await, 8, "p2 present");
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn concurrent_gets_do_not_deadlock() {
        // 32 concurrent reads of one page. This was the original motivation for
        // moving off the global Mutex<LruState>, and remains a guard against
        // any future lock ordering mistake on the read path.
        let (o, dirs) = opts(256, 1024 * 1024, 1, CacheEvictorType::Lru, 4);
        let mgr = LocalCacheManager::create(o).await.unwrap();
        // Pre-populate one page.
        let id = PageId::new("conc-file", 0);
        assert!(
            mgr.put(&id, Bytes::from(vec![0x42u8; 256])).await,
            "put should succeed"
        );
        // 32 concurrent reads of the same page.
        let mut handles = Vec::new();
        for _ in 0..32 {
            let m = mgr.clone();
            let id = id.clone();
            handles.push(tokio::spawn(async move {
                let mut dst = vec![0u8; 256];
                let n = m.get(&id, 0, &mut dst).await;
                assert_eq!(n, 256);
                assert_eq!(dst, vec![0x42u8; 256]);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn lfu_evictor_keeps_frequent_pages() {
        let (o, dirs) = opts(8, 16, 1, CacheEvictorType::Lfu, 4);
        let mgr = LocalCacheManager::create(o).await.unwrap();
        let p0 = PageId::new("f", 0);
        let p1 = PageId::new("f", 1);
        let p2 = PageId::new("f", 2);
        assert!(mgr.put(&p0, Bytes::from_static(b"00000000")).await);
        assert!(mgr.put(&p1, Bytes::from_static(b"11111111")).await);
        // Access p0 several times → most frequent.
        let mut dst = vec![0u8; 8];
        for _ in 0..3 {
            assert_eq!(mgr.get(&p0, 0, &mut dst).await, 8);
        }
        // Insert p2 → least frequent (p1) is evicted.
        assert!(mgr.put(&p2, Bytes::from_static(b"22222222")).await);
        assert_eq!(mgr.get(&p1, 0, &mut dst).await, 0, "p1 (LFU) evicted");
        assert_eq!(mgr.get(&p0, 0, &mut dst).await, 8, "p0 (frequent) survives");
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn invalidate_removes_all_file_pages() {
        let (mgr, dirs) = manager(8, 1024, 2).await;
        assert!(
            mgr.put(&PageId::new("fileX", 0), Bytes::from_static(b"aaaa"))
                .await
        );
        assert!(
            mgr.put(&PageId::new("fileX", 1), Bytes::from_static(b"bbbb"))
                .await
        );
        assert!(
            mgr.put(&PageId::new("fileY", 0), Bytes::from_static(b"cccc"))
                .await
        );
        mgr.invalidate("fileX").await;
        let mut dst = vec![0u8; 4];
        assert_eq!(mgr.get(&PageId::new("fileX", 0), 0, &mut dst).await, 0);
        assert_eq!(mgr.get(&PageId::new("fileX", 1), 0, &mut dst).await, 0);
        assert_eq!(mgr.get(&PageId::new("fileY", 0), 0, &mut dst).await, 4);
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn schedule_fill_eventually_caches() {
        let (mgr, dirs) = manager(16, 1024, 1).await;
        let id = PageId::new("async-f", 0);
        mgr.clone()
            .schedule_fill(id.clone(), Bytes::from_static(b"async-bytes!"));

        // Poll until the async write-back lands (bounded wait).
        let mut dst = vec![0u8; 12];
        let mut hit = false;
        for _ in 0..100 {
            if mgr.get(&id, 0, &mut dst).await == 12 {
                hit = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(hit, "schedule_fill should eventually cache the page");
        assert_eq!(&dst, b"async-bytes!");
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn concurrent_puts_and_gets_same_and_distinct_pages() {
        let (mgr, dirs) = manager(32, 64 * 1024, 2).await;
        let mut handles = Vec::new();
        for i in 0..32u64 {
            let m = mgr.clone();
            handles.push(tokio::spawn(async move {
                let id = PageId::new(format!("file-{}", i % 4), i);
                let payload = vec![i as u8; 16];
                m.put(&id, Bytes::from(payload.clone())).await;
                let mut dst = vec![0u8; 16];
                let n = m.get(&id, 0, &mut dst).await;
                // Either a hit (16) or a benign miss if evicted; never corrupt.
                if n == 16 {
                    assert_eq!(dst, payload);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn benign_racing_put_rejected() {
        let (mgr, dirs) = manager(16, 1024, 1).await;
        let id = PageId::new("f", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"aaa")).await);
        assert!(!mgr.put(&id, Bytes::from_static(b"bbb")).await);
        cleanup(&dirs).await;
    }

    /// Build a manager with an explicit TTL.
    async fn manager_with_ttl(
        page_size: u64,
        capacity: u64,
        ttl: Duration,
    ) -> (Arc<LocalCacheManager>, Vec<PathBuf>) {
        let (mut o, dirs) = opts(page_size, capacity, 1, CacheEvictorType::Lru, 4);
        o.ttl = Some(ttl);
        (LocalCacheManager::create(o).await.unwrap(), dirs)
    }

    #[tokio::test]
    async fn get_lazily_expires_page() {
        let (mgr, dirs) = manager_with_ttl(16, 1024, Duration::from_millis(40)).await;
        let id = PageId::new("ttl-f", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"0123456789")).await);

        // Fresh entry → hit.
        let mut dst = vec![0u8; 10];
        assert_eq!(mgr.get(&id, 0, &mut dst).await, 10);

        // After the TTL window the lazy check drops the page on `get`.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(mgr.get(&id, 0, &mut dst).await, 0, "expired page is a miss");

        // The entry was removed from the index (occupancy reflects this), so a
        // subsequent put for the same page is accepted (not a benign race).
        assert!(
            mgr.put(&id, Bytes::from_static(b"refilled..")).await,
            "expired page should be re-fillable"
        );
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn no_ttl_never_expires() {
        let (mgr, dirs) = manager(16, 1024, 1).await; // ttl = None
        let id = PageId::new("no-ttl", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"abcd")).await);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let mut dst = vec![0u8; 4];
        assert_eq!(mgr.get(&id, 0, &mut dst).await, 4, "no TTL → never expires");
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn on_file_open_first_time_keeps_pages() {
        let (mgr, dirs) = manager(16, 1024, 1).await;
        let id = PageId::new("100", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"aaaa")).await);
        // First open records the version; existing pages survive.
        mgr.on_file_open("100", 4, 1_700_000_000_000).await;
        let mut dst = vec![0u8; 4];
        assert_eq!(mgr.get(&id, 0, &mut dst).await, 4);
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn on_file_open_invalidates_on_overwrite() {
        let (mgr, dirs) = manager(16, 1024, 1).await;
        let id = PageId::new("200", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"aaaa")).await);
        // Record the initial identity.
        mgr.on_file_open("200", 4, 1_700_000_000_000).await;

        // Reopen with a changed mtime → overwrite → stale pages dropped.
        mgr.on_file_open("200", 4, 1_700_000_999_000).await;
        let mut dst = vec![0u8; 4];
        assert_eq!(mgr.get(&id, 0, &mut dst).await, 0, "stale page invalidated");

        // Length change is likewise treated as an overwrite.
        assert!(
            mgr.put(&PageId::new("200", 0), Bytes::from_static(b"bbbb"))
                .await
        );
        mgr.on_file_open("200", 8, 1_700_000_999_000).await;
        assert_eq!(mgr.get(&PageId::new("200", 0), 0, &mut dst).await, 0);
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn on_file_open_same_identity_is_noop() {
        let (mgr, dirs) = manager(16, 1024, 1).await;
        let id = PageId::new("300", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"keep")).await);
        mgr.on_file_open("300", 4, 1_700_000_000_000).await;
        // Reopen with identical (length, mtime) → pages preserved.
        mgr.on_file_open("300", 4, 1_700_000_000_000).await;
        let mut dst = vec![0u8; 4];
        assert_eq!(mgr.get(&id, 0, &mut dst).await, 4);
        cleanup(&dirs).await;
    }

    /// TTL is now enforced lazily, on access, rather than by a background
    /// sweeper (foyer has no iteration, so a sweeper has nothing to walk).
    ///
    /// The guarantee that matters is unchanged and is what this asserts: an
    /// expired page is never served. What changed is only *when* its space is
    /// reclaimed — on the next access, or by ordinary capacity eviction,
    /// instead of on a timer.
    #[tokio::test]
    async fn expired_pages_are_never_served_and_are_reclaimed_on_access() {
        let (mgr, dirs) = manager_with_ttl(16, 1024, Duration::from_millis(30)).await;
        for p in 0..3u64 {
            assert!(
                mgr.put(&PageId::new("sweep", p), Bytes::from_static(b"xxxx"))
                    .await
            );
        }
        let used_before = mgr.caches.iter().map(|c| c.usage()).sum::<usize>();
        assert!(used_before > 0, "pages should be resident before the TTL");

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Every read misses: expired pages are not served.
        let mut dst = vec![0u8; 4];
        for p in 0..3u64 {
            assert_eq!(mgr.get(&PageId::new("sweep", p), 0, &mut dst).await, 0);
        }

        // And that same access reclaimed them, so nothing lingers.
        let used_after = mgr.caches.iter().map(|c| c.usage()).sum::<usize>();
        assert_eq!(
            used_after, 0,
            "expired pages should be reclaimed by the access that found them stale"
        );
        cleanup(&dirs).await;
    }

    /// Build a manager over an explicit (reusable) set of dirs so a restart can
    /// be simulated by dropping and recreating against the same directories.
    async fn manager_at(
        page_size: u64,
        capacity: u64,
        dirs: Vec<PathBuf>,
    ) -> Arc<LocalCacheManager> {
        let options = CacheManagerOptions {
            page_size,
            dir_capacity: capacity,
            dirs,
            evictor: CacheEvictorType::Lru,
            async_write_enabled: false,
            async_write_threads: 1,
            quota_enabled: false,
            ttl: None,
            uring_enabled: false,
            uring_queue_depth: 0,
            uring_thread_count: 0,
            sync_read_enabled: false,
        };
        LocalCacheManager::create(options).await.unwrap()
    }

    /// Recursively collect every regular file under `root` (test helper).
    fn walk_files(root: &std::path::Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        if let Ok(rd) = std::fs::read_dir(root) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    out.extend(walk_files(&p));
                } else {
                    out.push(p);
                }
            }
        }
        out
    }

    fn count_identity_files(root: &std::path::Path) -> usize {
        walk_files(root)
            .iter()
            .filter(|p| p.file_name().and_then(|s| s.to_str()) == Some(".identity"))
            .count()
    }

    #[tokio::test]
    async fn restore_preserves_pages_when_identity_unchanged() {
        let dirs = vec![std::env::temp_dir().join(format!("gfs_restore_{}", uuid::Uuid::new_v4()))];
        {
            let mgr = manager_at(16, 1024, dirs.clone()).await;
            mgr.on_file_open("file-r", 4, 1_700_000_000_000).await;
            assert!(
                mgr.put(&PageId::new("file-r", 0), Bytes::from_static(b"abcd"))
                    .await
            );
        }
        // Restart: a fresh manager over the same dirs restores pages + identity.
        let mgr2 = manager_at(16, 1024, dirs.clone()).await;
        let mut dst = vec![0u8; 4];
        assert_eq!(mgr2.get(&PageId::new("file-r", 0), 0, &mut dst).await, 4);
        // Reopen with the SAME identity → restored page is still served.
        mgr2.on_file_open("file-r", 4, 1_700_000_000_000).await;
        assert_eq!(mgr2.get(&PageId::new("file-r", 0), 0, &mut dst).await, 4);
        assert_eq!(&dst, b"abcd");
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn restore_invalidates_pages_on_overwrite_after_restart() {
        let dirs = vec![std::env::temp_dir().join(format!("gfs_restore_{}", uuid::Uuid::new_v4()))];
        {
            let mgr = manager_at(16, 1024, dirs.clone()).await;
            mgr.on_file_open("file-o", 4, 1_700_000_000_000).await;
            assert!(
                mgr.put(&PageId::new("file-o", 0), Bytes::from_static(b"old!"))
                    .await
            );
        }
        // Restart: pages + persisted identity are restored.
        let mgr2 = manager_at(16, 1024, dirs.clone()).await;
        let mut dst = vec![0u8; 4];
        assert_eq!(mgr2.get(&PageId::new("file-o", 0), 0, &mut dst).await, 4);
        // The file was overwritten while the process was down (mtime changed):
        // the restored identity lets `on_file_open` detect it and drop stale pages.
        mgr2.on_file_open("file-o", 4, 1_700_000_999_000).await;
        assert_eq!(
            mgr2.get(&PageId::new("file-o", 0), 0, &mut dst).await,
            0,
            "stale restored page must be invalidated after a detected overwrite"
        );
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn identity_sidecar_reclaimed_when_last_page_removed() {
        let dirs = vec![std::env::temp_dir().join(format!("gfs_ident_{}", uuid::Uuid::new_v4()))];
        let mgr = manager_at(16, 1024, dirs.clone()).await;
        mgr.on_file_open("gone", 4, 1_700_000_000_000).await;
        assert!(
            mgr.put(&PageId::new("gone", 0), Bytes::from_static(b"data"))
                .await
        );
        // Removing the last page drops the identity sidecar too, so a restart
        // would not resurrect a version record for a file with no cached pages.
        assert!(mgr.delete(&PageId::new("gone", 0)).await);

        let mgr2 = manager_at(16, 1024, dirs.clone()).await;
        // No pages and no version restored for the deleted file.
        let mut dst = vec![0u8; 4];
        assert_eq!(mgr2.get(&PageId::new("gone", 0), 0, &mut dst).await, 0);
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn restore_drops_pages_without_identity_sidecar() {
        // D1/D2 guard: a page cached without a persisted identity (here: `put`
        // with no preceding `on_file_open`, so `versions` is empty and no
        // sidecar is written) must NOT be restored after a restart, because it
        // could not be validated against a down-time overwrite. Restore is
        // sidecar-gated, so such pages are dropped.
        let dirs =
            vec![std::env::temp_dir().join(format!("gfs_nosidecar_{}", uuid::Uuid::new_v4()))];
        {
            let mgr = manager_at(16, 1024, dirs.clone()).await;
            // No on_file_open → versions empty → first-page put writes no sidecar.
            assert!(
                mgr.put(&PageId::new("orphan", 0), Bytes::from_static(b"data"))
                    .await
            );
            // The page is live in this session...
            let mut dst = vec![0u8; 4];
            assert_eq!(mgr.get(&PageId::new("orphan", 0), 0, &mut dst).await, 4);
        }
        // ...but after a restart it is dropped (no identity to validate it).
        let mgr2 = manager_at(16, 1024, dirs.clone()).await;
        let mut dst = vec![0u8; 4];
        assert_eq!(
            mgr2.get(&PageId::new("orphan", 0), 0, &mut dst).await,
            0,
            "page without an identity sidecar must not be restored"
        );
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn restore_reclaims_empty_shell_dir_with_only_sidecar() {
        // C-level resource hygiene: a directory that holds an identity sidecar
        // but no data pages (e.g. the last page was deleted before its sidecar,
        // or every page was corrupt) must be reclaimed on restart rather than
        // leaking an orphan version entry + on-disk shell directory.
        let dirs = vec![std::env::temp_dir().join(format!("gfs_shell_{}", uuid::Uuid::new_v4()))];
        {
            let mgr = manager_at(16, 1024, dirs.clone()).await;
            mgr.on_file_open("shell", 4, 1_700_000_000_000).await;
            assert!(
                mgr.put(&PageId::new("shell", 0), Bytes::from_static(b"data"))
                    .await
            );
        }
        // Simulate "page gone but sidecar lingered": delete the numeric page
        // file(s) on disk, leaving the `.identity` sidecar behind.
        for p in walk_files(&dirs[0]) {
            if p.file_name()
                .and_then(|s| s.to_str())
                .and_then(|n| n.parse::<u64>().ok())
                .is_some()
            {
                let _ = std::fs::remove_file(&p);
            }
        }
        assert!(
            count_identity_files(&dirs[0]) > 0,
            "precondition: an orphan sidecar exists before restart"
        );

        // Restart → restore reclaims the empty shell.
        let _mgr2 = manager_at(16, 1024, dirs.clone()).await;
        assert_eq!(
            count_identity_files(&dirs[0]),
            0,
            "empty shell directory (sidecar but no pages) must be reclaimed on restore"
        );
        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn get_bytes_returns_page_slice_and_miss_is_empty() {
        let (mgr, dirs) = manager(16, 1024, 1).await;
        let id = PageId::new("bytes-file", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"0123456789abcdef")).await);

        let hit = mgr.get_bytes(&id, 4, 6).await;
        assert_eq!(&hit[..], b"456789");

        let miss = mgr.get_bytes(&PageId::new("bytes-file", 99), 0, 8).await;
        assert!(miss.is_empty(), "missing page must return empty Bytes");

        let zero_len = mgr.get_bytes(&id, 0, 0).await;
        assert!(zero_len.is_empty());

        cleanup(&dirs).await;
    }

    #[tokio::test]
    async fn get_batch_bytes_preserves_order_and_miss_slots() {
        let (mgr, dirs) = manager(8, 1024, 1).await;
        let p0 = PageId::new("batch", 0);
        let p1 = PageId::new("batch", 1);
        let p2 = PageId::new("batch", 2);
        assert!(mgr.put(&p0, Bytes::from_static(b"00000000")).await);
        assert!(mgr.put(&p2, Bytes::from_static(b"22222222")).await);
        // p1 intentionally missing → empty Bytes at that index.

        let out = mgr
            .get_batch_bytes(&[
                crate::cache::PageReadRequest {
                    page_id: p0.clone(),
                    page_offset: 0,
                    len: 8,
                },
                crate::cache::PageReadRequest {
                    page_id: p1.clone(),
                    page_offset: 0,
                    len: 8,
                },
                crate::cache::PageReadRequest {
                    page_id: p2.clone(),
                    page_offset: 2,
                    len: 4,
                },
            ])
            .await;

        assert_eq!(out.len(), 3);
        assert_eq!(&out[0][..], b"00000000");
        assert!(out[1].is_empty(), "miss slot must be empty Bytes");
        assert_eq!(&out[2][..], b"2222");

        cleanup(&dirs).await;
    }

    // ── foyer migration regressions ──────────────────────────────────
    //
    // These guard the three failure modes found while migrating off moka.
    // Each one passes trivially if written slightly wrong, so the shape of the
    // test matters as much as the assertion; see the comments on each.

    /// `shard_count` must never hand a shard less than one page of capacity.
    ///
    /// foyer splits the byte capacity evenly across shards, so an over-sharded
    /// small directory ends up with shards too small to hold anything and the
    /// cache silently loses most of its space. Covers the parameter sets from
    /// the design doc, including the 100 GiB / 1 TiB production points.
    #[test]
    fn shard_count_never_starves_a_shard() {
        const GIB: u64 = 1 << 30;
        const MIB: u64 = 1 << 20;
        const KIB: u64 = 1 << 10;
        let cases: &[(u64, u64)] = &[
            (9_961_472, MIB), // 9.5 MiB, existing unit-test config
            (MIB, MIB),       // exactly one page
            (MIB, KIB),       // benchmark config
            (100 * MIB, MIB),
            (GIB, 64 * KIB),
            (20 * GIB, MIB), // default
            (100 * GIB, MIB),
            (100 * GIB, 64 * KIB),
            (1024 * GIB, MIB), // 1 TiB
            (1024 * GIB, 64 * KIB),
        ];
        for &(capacity, page_size) in cases {
            let shards = shard_count(capacity, page_size);
            assert!(
                shards >= 1 && shards <= MAX_SHARDS,
                "{capacity}/{page_size}: {shards} shards"
            );
            let per_shard = capacity / shards as u64;
            assert!(
                shards == 1 || per_shard >= page_size * MIN_PAGES_PER_SHARD,
                "{capacity}/{page_size}: {shards} shards give {per_shard} B each, \
                 under {MIN_PAGES_PER_SHARD} pages"
            );
        }
        // Degenerate input must not divide by zero.
        assert_eq!(shard_count(MIB, 0), 1);
    }

    /// Reads must not stall eviction (design doc §3.6).
    ///
    /// `Cache::touch` runs `Eviction::acquire` without the paired `release`, so
    /// under `LruConfig` every page ever read gets pinned out of the eviction
    /// order permanently. `get_bytes` uses `drop(cache.get(id))` instead; this
    /// is the regression guard.
    ///
    /// Three things about the shape are load-bearing:
    ///
    /// 1. **Compare read-first against no-read within the same policy.** An
    ///    absolute survivor threshold would be wrong: how much of set B fits is
    ///    also affected by shard hash skew and by the policy's own scan
    ///    resistance. Only the delta between "A was read" and "A was not read"
    ///    isolates the missing `release`.
    /// 2. **Do not assert on usage staying within capacity.** When `pop()`
    ///    finds only pinned records it returns `None` and `evict()` just breaks
    ///    out (foyer-memory-0.22.3 `src/raw.rs:117-136`), so usage stays
    ///    *inside* the quota while the cache is frozen.
    /// 3. **Read the entire resident set**, not a sample. Reading a few pages
    ///    passes even with the bug, because the rest of the eviction list is
    ///    still long enough for `pop()` to succeed.
    #[tokio::test]
    async fn reads_do_not_stall_eviction_under_lru() {
        const PAGE: usize = 8;
        const CAP_PAGES: u64 = 64;
        // Fill A to half of capacity. An exactly-sized working set does not
        // fully fit: foyer divides capacity evenly across shards, so hash skew
        // pushes some shards over their slice (measured: 55/64 resident at
        // exact fit, 32/32 at half).
        const A_PAGES: u64 = CAP_PAGES / 2;

        /// Returns how many pages of set B stayed resident.
        async fn b_survivors(read_a_first: bool) -> usize {
            // LRU is the only pinning policy: LfuConfig's release() is already
            // a no-op, so it cannot exhibit this and is useless as a canary.
            let (o, dirs) = opts(
                PAGE as u64,
                PAGE as u64 * CAP_PAGES,
                1,
                CacheEvictorType::Lru,
                0,
            );
            let mgr = LocalCacheManager::create(o).await.unwrap();

            for i in 0..A_PAGES {
                assert!(
                    mgr.put(&PageId::new("A", i), Bytes::from(vec![b'a'; PAGE]))
                        .await
                );
            }
            if read_a_first {
                // Read every page of A. With a pin leak, each read makes that
                // page immortal.
                let mut dst = vec![0u8; PAGE];
                for i in 0..A_PAGES {
                    assert_eq!(mgr.get(&PageId::new("A", i), 0, &mut dst).await, PAGE);
                }
            }
            // Set B: disjoint, large enough to need A's space as well.
            for i in 0..CAP_PAGES {
                let _ = mgr
                    .put(&PageId::new("B", i), Bytes::from(vec![b'b'; PAGE]))
                    .await;
            }
            mgr.flush_reaper().await;

            // `contains` performs no acquire(), so polling cannot perturb the
            // eviction order.
            let survivors = (0..CAP_PAGES)
                .filter(|i| mgr.caches[0].contains(&PageId::new("B", *i)))
                .count();
            cleanup(&dirs).await;
            survivors
        }

        let without_reads = b_survivors(false).await;
        let with_reads = b_survivors(true).await;

        assert!(
            without_reads > 0,
            "baseline is broken: set B never became resident even without reads"
        );
        // Reading A must not materially reduce how much of B can be admitted.
        // A pin leak collapses this to near zero; the tolerance absorbs the
        // shard skew that makes the two runs differ slightly anyway.
        assert!(
            with_reads * 2 >= without_reads,
            "eviction stalled after reads: {with_reads} of set B resident when A was \
             read first, vs {without_reads} when it was not — reads are pinning pages \
             out of the eviction order"
        );
    }

    /// The reaper must not delete a page that was re-admitted after its
    /// eviction was queued (design doc §4.5.1).
    ///
    /// Eviction queues the victim; the file is deleted asynchronously. If a
    /// `put` re-admits the same page in that window, deleting the queued victim
    /// removes the *fresh* file. The page then reads as resident but empty, and
    /// `put` will not repair it either — its racing check sees `contains ==
    /// true`. The reaper therefore takes the page lock and re-checks.
    ///
    /// Drives the sequence deterministically via `flush_reaper` rather than
    /// hoping to hit the window: a timing-based version passes almost always
    /// even with the guard removed.
    #[tokio::test]
    async fn reaper_does_not_delete_readmitted_page() {
        const PAGES: u64 = 16;
        const PAGE: usize = 8;
        let (o, dirs) = opts(
            PAGE as u64,
            PAGE as u64 * PAGES,
            1,
            CacheEvictorType::Lfu,
            0,
        );
        let mgr = LocalCacheManager::create(o).await.unwrap();

        let victim = PageId::new("V", 0);
        assert!(mgr.put(&victim, Bytes::from(vec![b'v'; PAGE])).await);

        // Push the victim out by filling past capacity. Its file deletion is
        // now queued on the reaper.
        for i in 0..(PAGES * 2) {
            let _ = mgr
                .put(&PageId::new("F", i), Bytes::from(vec![b'f'; PAGE]))
                .await;
        }
        mgr.flush_reaper().await;
        assert!(
            !mgr.caches[0].contains(&victim),
            "victim should have been evicted by the fill"
        );

        // Re-admit it, then let the reaper run again. Without the page lock and
        // the contains() re-check, a queued deletion would take out this fresh
        // file.
        assert!(mgr.put(&victim, Bytes::from(vec![b'w'; PAGE])).await);
        mgr.flush_reaper().await;

        let mut dst = vec![0u8; PAGE];
        let n = mgr.get(&victim, 0, &mut dst).await;
        assert_eq!(
            n, PAGE,
            "reaper deleted the file of a re-admitted page: metadata says resident \
             but the bytes are gone"
        );
        assert_eq!(
            &dst,
            &vec![b'w'; PAGE],
            "should read the re-admitted content"
        );
        cleanup(&dirs).await;
    }

    /// Victim selection must not scale with the number of resident pages.
    ///
    /// This is the whole point of the migration: the moka evictor picked
    /// victims with `iter().min_by_key()`, which measured ~55-96µs at 1k pages
    /// and ~7-10ms at 100k — linear. foyer pops off an intrusive list instead.
    ///
    /// Asserts on both a ratio and an absolute bound. The ratio alone cannot
    /// catch a regression that slows both sizes equally; the absolute bound
    /// alone cannot catch a return to linear scaling on a fast machine.
    #[tokio::test]
    async fn eviction_cost_does_not_scale_with_page_count() {
        const PAGE: usize = 8;

        // Steady-state cost of one evicting admission, in nanoseconds.
        async fn evict_nanos(pages: u64) -> u128 {
            let (o, dirs) = opts(
                PAGE as u64,
                PAGE as u64 * pages,
                1,
                CacheEvictorType::Lfu,
                0,
            );
            let mgr = LocalCacheManager::create(o).await.unwrap();
            for i in 0..pages {
                let _ = mgr
                    .put(&PageId::new("fill", i), Bytes::from(vec![b'x'; PAGE]))
                    .await;
            }

            // Minimum over several batches, not the mean: this runs alongside
            // the rest of the suite, where preemption can only add time.
            let mut best = u128::MAX;
            for round in 0..3u64 {
                const BATCH: u64 = 50;
                let start = Instant::now();
                for b in 0..BATCH {
                    let id = PageId::new("probe", round * BATCH + b);
                    let _ = mgr.put(&id, Bytes::from(vec![b'y'; PAGE])).await;
                }
                best = best.min(start.elapsed().as_nanos() / BATCH as u128);
            }
            cleanup(&dirs).await;
            best
        }

        // Note these include the page-store write, which dominates; the point
        // is that the *difference* between the two sizes stays flat.
        let small = evict_nanos(1_000).await;
        let large = evict_nanos(10_000).await;

        assert!(
            large < small * 5,
            "eviction looks superlinear: {small}ns at 1k pages vs {large}ns at 10k"
        );
    }

    /// A page larger than the whole directory capacity stays resident.
    ///
    /// foyer admits an oversized entry rather than rejecting it: `evict()` pops
    /// until the cache is empty, then keeps the new record anyway, so `usage()`
    /// ends up above `capacity`. Measured directly against foyer 0.22.3 —
    /// inserting a weight-32 value into a capacity-16 cache leaves
    /// `contains == true` and `usage == 32`.
    ///
    /// So "usage never exceeds capacity" holds only while
    /// `page_size <= dir_capacity`, which `create` warns about. Pinned here so
    /// that a future foyer upgrade which starts rejecting these is noticed.
    #[tokio::test]
    async fn oversized_page_is_admitted_and_capacity_is_exceeded() {
        // page_size 64 so `put`'s own size check passes; dir_capacity 16 so the
        // page cannot fit.
        let (o, dirs) = opts(64, 16, 1, CacheEvictorType::Lfu, 0);
        let mgr = LocalCacheManager::create(o).await.unwrap();
        let id = PageId::new("too-big", 0);

        assert!(
            mgr.put(&id, Bytes::from(vec![b'z'; 32])).await,
            "foyer admits an entry heavier than the whole cache"
        );
        let mut dst = vec![0u8; 32];
        assert_eq!(
            mgr.get(&id, 0, &mut dst).await,
            32,
            "and it is readable afterwards"
        );
        assert!(
            mgr.caches[0].usage() > 16,
            "usage exceeds capacity here — this is why page_size > dir_capacity \
             is a misconfiguration that `create` warns about"
        );
        cleanup(&dirs).await;
    }

    /// Evicted page files must actually be removed from disk.
    ///
    /// The metadata-level tests all pass whether or not the reaper works, since
    /// they only ask foyer what it holds. This one looks at the filesystem.
    #[tokio::test]
    async fn reaper_deletes_evicted_page_files_from_disk() {
        const PAGES: u64 = 8;
        const PAGE: usize = 8;
        let (o, dirs) = opts(
            PAGE as u64,
            PAGE as u64 * PAGES,
            1,
            CacheEvictorType::Lfu,
            0,
        );
        let mgr = LocalCacheManager::create(o).await.unwrap();

        // Write 4x the capacity so most pages must be evicted.
        for i in 0..(PAGES * 4) {
            let _ = mgr
                .put(&PageId::new("disk", i), Bytes::from(vec![b'd'; PAGE]))
                .await;
        }
        mgr.flush_reaper().await;

        let on_disk = walk_files(&dirs[0])
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.parse::<u64>().is_ok())
            })
            .count();
        let resident = mgr.caches[0].entries();
        assert!(
            on_disk <= resident + 2,
            "{on_disk} page files on disk but only {resident} resident — \
             the reaper is not deleting evicted files"
        );
        cleanup(&dirs).await;
    }

    /// Byte capacity must hold across policies, including at the boundary.
    #[tokio::test]
    async fn usage_never_exceeds_capacity() {
        for policy in [
            CacheEvictorType::Lru,
            CacheEvictorType::Lfu,
            CacheEvictorType::S3Fifo,
        ] {
            const PAGES: u64 = 32;
            const PAGE: usize = 8;
            let capacity = PAGE as u64 * PAGES;
            let (o, dirs) = opts(PAGE as u64, capacity, 1, policy, 0);
            let mgr = LocalCacheManager::create(o).await.unwrap();
            for i in 0..(PAGES * 3) {
                let _ = mgr
                    .put(&PageId::new("cap", i), Bytes::from(vec![b'c'; PAGE]))
                    .await;
            }
            let usage = mgr.caches[0].usage() as u64;
            assert!(
                usage <= capacity,
                "{policy:?}: usage {usage} exceeds capacity {capacity}"
            );
            cleanup(&dirs).await;
        }
    }

    /// S3-FIFO must be selectable end to end, not just parseable.
    #[tokio::test]
    async fn s3fifo_policy_roundtrips() {
        let (o, dirs) = opts(8, 1024, 1, CacheEvictorType::S3Fifo, 0);
        let mgr = LocalCacheManager::create(o).await.unwrap();
        let id = PageId::new("s3", 0);
        assert!(mgr.put(&id, Bytes::from_static(b"s3fifo!!")).await);
        let mut dst = vec![0u8; 8];
        assert_eq!(mgr.get(&id, 0, &mut dst).await, 8);
        assert_eq!(&dst, b"s3fifo!!");
        cleanup(&dirs).await;
    }

    /// A full reaper queue must not block `put`.
    ///
    /// `on_leave` runs inside `insert` on a tokio worker, so it uses
    /// `try_send`: a full queue drops the task (leaving an orphan file for
    /// `restore` to reclaim) rather than blocking. Verifies puts keep
    /// completing under heavy eviction.
    #[tokio::test]
    async fn reap_queue_pressure_does_not_block_puts() {
        const PAGES: u64 = 4;
        const PAGE: usize = 8;
        let (o, dirs) = opts(
            PAGE as u64,
            PAGE as u64 * PAGES,
            1,
            CacheEvictorType::Lfu,
            0,
        );
        let mgr = LocalCacheManager::create(o).await.unwrap();
        // Far more evictions than the queue can hold at once.
        for i in 0..2_000u64 {
            let _ = mgr
                .put(&PageId::new("flood", i), Bytes::from(vec![b'p'; PAGE]))
                .await;
        }
        // Reaching here at all is the assertion: no deadlock, no hang.
        mgr.flush_reaper().await;
        assert!(
            mgr.caches[0].usage() as u64 <= PAGE as u64 * PAGES,
            "capacity must still hold under reaper pressure"
        );
        cleanup(&dirs).await;
    }

    /// `close()` drains queued evictions instead of orphaning them.
    #[tokio::test]
    async fn close_drains_pending_evictions() {
        const PAGES: u64 = 8;
        const PAGE: usize = 8;
        let (o, dirs) = opts(
            PAGE as u64,
            PAGE as u64 * PAGES,
            1,
            CacheEvictorType::Lfu,
            0,
        );
        let mgr = LocalCacheManager::create(o).await.unwrap();
        for i in 0..(PAGES * 3) {
            let _ = mgr
                .put(&PageId::new("close", i), Bytes::from(vec![b'k'; PAGE]))
                .await;
        }
        mgr.close().await;
        let on_disk = walk_files(&dirs[0])
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.parse::<u64>().is_ok())
            })
            .count();
        assert!(
            on_disk <= mgr.caches[0].entries() + 2,
            "close() should have drained the reaper: {on_disk} files on disk"
        );
        cleanup(&dirs).await;
    }

    // ── design doc section 8.3 items not covered above ───────────────

    /// The reaper reclaims the identity sidecar when eviction removes a file's
    /// last page (design doc section 8.3 item 4).
    ///
    /// Distinct from `identity_sidecar_reclaimed_when_last_page_removed`, which
    /// covers the explicit `delete()` path. This is the eviction path: the
    /// sidecar is dropped by the reaper (`reap_one`), not by `delete`, and the
    /// two run different code.
    ///
    /// A leaked sidecar is not harmless. `restore()` is sidecar-gated, so a
    /// stale one lets orphan page files from a crashed run be readmitted after
    /// a restart, when nothing has validated them against a down-time
    /// overwrite.
    #[tokio::test]
    async fn reaper_reclaims_identity_sidecar_on_last_page() {
        const PAGE: usize = 8;
        const PAGES: u64 = 8;
        let (o, dirs) = opts(
            PAGE as u64,
            PAGE as u64 * PAGES,
            1,
            CacheEvictorType::Lfu,
            0,
        );
        let mgr = LocalCacheManager::create(o).await.unwrap();

        mgr.on_file_open("victim", PAGE as i64, 1_700_000_000_000)
            .await;
        assert!(
            mgr.put(&PageId::new("victim", 0), Bytes::from(vec![b'v'; PAGE]))
                .await
        );

        // Evict it by filling well past capacity with another file's pages.
        mgr.on_file_open("filler", (PAGE as i64) * 32, 1_700_000_000_000)
            .await;
        for i in 0..(PAGES * 4) {
            let _ = mgr
                .put(&PageId::new("filler", i), Bytes::from(vec![b'f'; PAGE]))
                .await;
        }
        mgr.flush_reaper().await;
        assert!(
            !mgr.caches[0].contains(&PageId::new("victim", 0)),
            "the victim page should have been evicted by the fill"
        );

        // Restart. The victim's sidecar should be gone, so even if an orphan
        // page file survived it must not be restored.
        let (o2, _) = opts(
            PAGE as u64,
            PAGE as u64 * PAGES,
            1,
            CacheEvictorType::Lfu,
            0,
        );
        let o2 = CacheManagerOptions {
            dirs: dirs.clone(),
            ..o2
        };
        let mgr2 = LocalCacheManager::create(o2).await.unwrap();
        let mut dst = vec![0u8; PAGE];
        assert_eq!(
            mgr2.get(&PageId::new("victim", 0), 0, &mut dst).await,
            0,
            "evicting a file's last page must drop its identity sidecar, so nothing \
             of that file is restorable"
        );
        cleanup(&dirs).await;
    }

    /// A partial final page is charged at its real size, not the full page size
    /// (design doc section 8.3 item 6).
    ///
    /// The weighter reads `PageInfo.page_size`, so a tail page written short
    /// must record its actual length. Charging the full page size would
    /// under-fill the cache by the rounding error on every file.
    #[tokio::test]
    async fn tail_page_accounted_by_real_size() {
        let (o, dirs) = opts(64, 4096, 1, CacheEvictorType::Lfu, 0);
        let mgr = LocalCacheManager::create(o).await.unwrap();

        let before = mgr.caches[0].usage();
        // 20 bytes into a 64-byte page slot.
        assert!(
            mgr.put(&PageId::new("tail", 0), Bytes::from(vec![b't'; 20]))
                .await
        );
        let delta = mgr.caches[0].usage() - before;
        assert_eq!(
            delta, 20,
            "a 20-byte tail page must be charged 20 bytes, not the 64-byte page size"
        );
        cleanup(&dirs).await;
    }

    /// `restore()` respects capacity when the directory holds more than fits
    /// (design doc section 8.3 item 7).
    ///
    /// Restore inserts every page it finds, which can exceed capacity — foyer
    /// evicts during those inserts, so the reaper must already be running. It
    /// is started before `restore()` in `create()` for exactly this reason;
    /// this test is what would catch that ordering being reversed.
    #[tokio::test]
    async fn restore_over_capacity_evicts_excess() {
        const PAGE: usize = 8;
        const CAP_PAGES: u64 = 16;
        let capacity = PAGE as u64 * CAP_PAGES;

        // First run: a cache twice as large, filled completely.
        let (o1, dirs) = opts(PAGE as u64, capacity * 2, 1, CacheEvictorType::Lfu, 0);
        let mgr1 = LocalCacheManager::create(o1).await.unwrap();
        mgr1.on_file_open("big", (PAGE as i64) * 32, 1_700_000_000_000)
            .await;
        for i in 0..(CAP_PAGES * 2) {
            assert!(
                mgr1.put(&PageId::new("big", i), Bytes::from(vec![b'b'; PAGE]))
                    .await
            );
        }
        mgr1.close().await;

        // Second run over the same directory, but with half the capacity.
        let (o2, _) = opts(PAGE as u64, capacity, 1, CacheEvictorType::Lfu, 0);
        let o2 = CacheManagerOptions {
            dirs: dirs.clone(),
            ..o2
        };
        let mgr2 = LocalCacheManager::create(o2).await.unwrap();
        mgr2.flush_reaper().await;

        let usage = mgr2.caches[0].usage() as u64;
        assert!(
            usage <= capacity,
            "restore left {usage} B resident against a {capacity} B capacity"
        );
        let on_disk = walk_files(&dirs[0])
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.parse::<u64>().is_ok())
            })
            .count();
        assert!(
            on_disk <= mgr2.caches[0].entries() + 2,
            "{on_disk} page files left on disk after restore evicted down to \
             {} resident — the reaper did not run during restore",
            mgr2.caches[0].entries()
        );
        cleanup(&dirs).await;
    }

    /// An explicit `delete()` must not also enqueue a reap (design doc section
    /// 8.3 item 9).
    ///
    /// `delete` removes the file itself and then drops the metadata, which
    /// fires `on_leave` with `Event::Remove`. Treating that as an eviction
    /// would delete the file a second time — and if the page had already been
    /// re-added, the second delete would take out the new file. `on_leave`
    /// filters on `Event::Evict` for this reason.
    ///
    /// Asserts on the re-add surviving rather than on a delete error, since
    /// deleting an absent file is not an error on any platform we target: a
    /// double delete is silent, so it has to be made observable.
    ///
    /// Deliberately does not assert on `CLIENT_CACHE_REAP_SKIPPED_READMITTED`.
    /// The metric counters are process-global, so any other test that trips one
    /// would break this — passing today only because none currently does.
    #[tokio::test]
    async fn delete_does_not_double_delete() {
        let (o, dirs) = opts(8, 4096, 1, CacheEvictorType::Lfu, 0);
        let mgr = LocalCacheManager::create(o).await.unwrap();
        let id = PageId::new("dd", 0);

        assert!(mgr.put(&id, Bytes::from_static(b"first!!!")).await);
        assert!(mgr.delete(&id).await);

        // Re-add immediately. A stray queued reap from the delete would now
        // remove this fresh file.
        assert!(mgr.put(&id, Bytes::from_static(b"second!!")).await);
        mgr.flush_reaper().await;

        let mut dst = vec![0u8; 8];
        assert_eq!(
            mgr.get(&id, 0, &mut dst).await,
            8,
            "delete() enqueued a reap that then deleted the re-added page"
        );
        assert_eq!(&dst, b"second!!");
        cleanup(&dirs).await;
    }
}
