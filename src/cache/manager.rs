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
//! an in-memory metadata index, per-directory byte accounting and eviction, a
//! striped page-lock pool, and a bounded async write-back pool.
//!
//! # Concurrency model (E1+E2+E3)
//!
//! - **Striped page locks** (`LOCK_SIZE` `RwLock`s): `get` takes a read lock,
//!   `put`/`delete` take a write lock for the page's stripe. Same-page
//!   operations serialize; different pages run concurrently.
//! - **Metadata lock** (`inner: RwLock<Inner>`): `get` takes a **read** lock
//!   (concurrent meta lookup + LRU update via the evictor's own interior
//!   mutability); `put`/`delete` take a **write** lock. The lock is held only
//!   for short in-memory critical sections — **never across page-store disk
//!   IO**. This eliminates the previous `Mutex` bottleneck where 20 concurrent
//!   `get` calls serialized through a single exclusive lock (E3).
//! - **Version lock** (`versions: RwLock<HashMap>`): `on_file_open` takes a
//!   read lock for the common case (same file → no change) and a write lock
//!   only when the file was overwritten. This lock is separate from `inner`
//!   so `on_file_open` never blocks `get`/`put`/`delete` (E2).
//! - **E1 (get lock merge)**: `get` previously locked `inner` twice (meta
//!   lookup, then LRU update after IO). Now it takes a single read lock,
//!   updates the LRU immediately (before IO), and releases. Even if the IO
//!   later fails, a spurious LRU touch is harmless (no correctness risk).
//! - Eviction removes victims' metadata under `inner.write()`, then deletes
//!   their files outside the lock.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, warn};
use xxhash_rust::xxh3::Xxh3Default;

use crate::cache::allocator::{Allocator, HashAllocator};
use crate::cache::evictor::{build_evictor, CacheEvictor};
use crate::cache::metric_name as mn;
use crate::cache::options::CacheManagerOptions;
use crate::cache::page_id::{CacheScope, PageId, PageInfo};
#[cfg(target_os = "linux")]
use crate::cache::store::UringPageStore;
use crate::cache::store::{init_uring_config, is_uring_available, LocalPageStore, PageStore};
use crate::cache::{CacheManager, CacheState, PageReadRequest};
use crate::config::GoosefsConfig;
use crate::error::Result;
use crate::metrics::{counter, gauge};
use futures::future::join_all;

/// Number of page-lock stripes (mirrors Java `LocalCacheManager.LOCK_SIZE`).
const LOCK_SIZE: usize = 1024;

/// Per-directory evictor + byte accounting.
///
/// The evictor uses interior mutability for reads (`on_access` takes `&self`).
/// For writes (`evict_candidate`, `on_add`, `on_remove`), the per-dir
/// `dir_locks[i]` `StdMutex` serialises the operation — but only for the
/// same directory. Different directories can mutate concurrently.
struct DirState {
    evictor: Box<dyn CacheEvictor>,
    /// Used bytes in this directory. `AtomicU64` for lock-free reads on the
    /// `get`/`put` hot path; updated via `fetch_add` / `fetch_sub`.
    used_bytes: AtomicU64,
    capacity: u64,
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

    // ── Phase C: lock-free metadata indices ───────────────────────────
    /// `PageId → PageInfo` primary index. `DashMap` provides lock-free reads
    /// (per-shard read guard) — the `get` hot path takes zero global locks.
    meta: DashMap<PageId, PageInfo>,

    /// Per-directory state. `dirs[i]` is accessed without locks for reads
    /// (evictor.on_access is interior-mutable). Writes are serialised by
    /// `dir_locks[i]`.
    dirs: Vec<DirState>,
    /// Per-directory `StdMutex` for serialising evictor write operations
    /// (`evict_candidate`, `on_add`, `on_remove`) and `used_bytes` updates.
    /// Each dir has its own mutex — different dirs evict concurrently.
    dir_locks: Vec<StdMutex<()>>,

    /// File reverse index (`file_id → set(page_index)`). Under a `RwLock`
    /// because it's only touched on cold paths (`invalidate`, `sweep`,
    /// `delete`) and the inner `HashSet` needs atomic insert/remove.
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
    pub async fn create(options: CacheManagerOptions) -> Result<Self> {
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

        let mut stores: Vec<Arc<dyn PageStore>> = Vec::with_capacity(dir_paths.len());
        let mut dirs = Vec::with_capacity(dir_paths.len());
        for dir in &dir_paths {
            let store: Arc<dyn PageStore> = if use_uring {
                #[cfg(target_os = "linux")]
                {
                    match UringPageStore::create(dir, options.page_size).await {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            warn!(error = %e, "UringPageStore creation failed; fallback to LocalPageStore");
                            Arc::new(LocalPageStore::create(dir, options.page_size).await?)
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    Arc::new(LocalPageStore::create(dir, options.page_size).await?)
                }
            } else {
                Arc::new(LocalPageStore::create(dir, options.page_size).await?)
            };
            stores.push(store);
            dirs.push(DirState {
                evictor: build_evictor(options.evictor),
                used_bytes: AtomicU64::new(0),
                capacity: options.dir_capacity,
            });
        }

        let page_locks = (0..LOCK_SIZE).map(|_| RwLock::new(())).collect();
        let async_write_sem = Arc::new(Semaphore::new(options.async_write_threads.max(1)));
        let dir_locks: Vec<StdMutex<()>> = (0..dirs.len()).map(|_| StdMutex::new(())).collect();

        let mgr = Self {
            options,
            stores,
            allocator: Box::new(HashAllocator::new()),
            meta: DashMap::new(),
            dirs,
            dir_locks,
            by_file: RwLock::new(HashMap::new()),
            versions: RwLock::new(HashMap::new()),
            page_locks,
            async_write_sem,
            state: CacheState::ReadWrite,
        };

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
    ///
    /// Also spawns the background TTL sweeper when a TTL is configured.
    pub async fn from_config(config: &GoosefsConfig) -> Result<Arc<Self>> {
        let options = CacheManagerOptions::from_config(config);
        let mgr = Arc::new(Self::create(options).await?);
        mgr.clone().maybe_spawn_ttl_sweeper();
        Ok(mgr)
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

    /// Refresh occupancy gauges. Lock-free (reads from `meta` and `dirs`).
    fn publish_occupancy(&self) {
        let used: u64 = self
            .dirs
            .iter()
            .map(|d| d.used_bytes.load(Ordering::Relaxed))
            .sum();
        let pages = self.meta.len() as i64;
        gauge(mn::CLIENT_CACHE_PAGES).set(pages);
        gauge(mn::CLIENT_CACHE_SPACE_USED_COUNT).set(pages);
        gauge(mn::CLIENT_CACHE_SPACE_USED).set(used as i64);
        gauge(mn::CLIENT_CACHE_SPACE_AVAILABLE)
            .set(self.total_capacity().saturating_sub(used) as i64);
    }

    /// Pop one eviction victim from directory `dir_index`'s evictor, updating
    /// the index and accounting. Returns the victim id, its size, and whether
    /// the victim's file now has no remaining cached pages (so the caller can
    /// reclaim its identity sidecar). File deletion is the caller's
    /// responsibility, performed outside the lock.
    ///
    /// Takes the per-dir `StdMutex` to serialise evictor write operations.
    /// The Mutex is released before the `by_file.write().await` to keep the
    /// guard `Send` across the await point.
    async fn pop_victim(&self, dir_index: usize) -> Option<(PageId, u64, bool)> {
        // Phase 1: under per-dir Mutex — evictor + meta + used_bytes.
        let (victim, size) = {
            let _guard = self.dir_locks[dir_index].lock().unwrap();
            let victim = self.dirs[dir_index].evictor.evict_candidate()?;
            let size = self
                .meta
                .remove(&victim)
                .map(|(_, v)| v.page_size)
                .unwrap_or(0);
            self.dirs[dir_index].evictor.on_remove(&victim);
            self.dirs[dir_index]
                .used_bytes
                .fetch_sub(size, Ordering::Relaxed);
            (victim, size)
        };
        // Phase 2: by_file update (async, no Mutex held).
        let mut file_empty = false;
        {
            let mut by_file = self.by_file.write().await;
            if let Some(set) = by_file.get_mut(&victim.file_id) {
                set.remove(&victim.page_index);
                if set.is_empty() {
                    by_file.remove(&victim.file_id);
                    file_empty = true;
                }
            }
        }
        Some((victim, size, file_empty))
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
                        // Respect per-dir capacity; drop the file if it would overflow.
                        // Phase C: `used_bytes` is atomic → no Mutex for the check.
                        // Mutex is only taken briefly for evictor.on_add, then released
                        // before the by_file write.
                        if self.dirs[dir_index].used_bytes.load(Ordering::Relaxed) + size
                            > self.dirs[dir_index].capacity
                            || self.meta.contains_key(&page_id)
                        {
                            let _ = tokio::fs::remove_file(page.path()).await;
                            continue;
                        }
                        self.meta.insert(
                            page_id.clone(),
                            PageInfo {
                                page_id: page_id.clone(),
                                page_size: size,
                                dir_index,
                                created_at: Instant::now(),
                                scope: CacheScope::Global,
                            },
                        );
                        {
                            let _dir_guard = self.dir_locks[dir_index].lock().unwrap();
                            self.dirs[dir_index].evictor.on_add(&page_id);
                            drop(_dir_guard);
                        }
                        self.dirs[dir_index]
                            .used_bytes
                            .fetch_add(size, Ordering::Relaxed);
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

    /// Spawn the background TTL sweeper if a TTL is configured.
    ///
    /// The task holds a `Weak` reference so it exits automatically once the
    /// manager is dropped.
    fn maybe_spawn_ttl_sweeper(self: Arc<Self>) {
        let Some(ttl) = self.options.ttl else {
            return;
        };
        // Sweep at most once per TTL window, capped at 60s for responsiveness.
        let interval = ttl.min(Duration::from_secs(60)).max(Duration::from_secs(1));
        let weak: Weak<Self> = Arc::downgrade(&self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(mgr) = weak.upgrade() else {
                    break; // manager dropped
                };
                mgr.sweep_expired().await;
            }
        });
    }

    /// Remove all pages whose TTL has elapsed. No-op when TTL is disabled.
    pub async fn sweep_expired(&self) {
        let Some(ttl) = self.options.ttl else {
            return;
        };
        // Lock-free read of `meta` (DashMap). Collect expired page ids, then
        // call `delete` for each (which takes its own per-dir lock).
        let expired: Vec<PageId> = self
            .meta
            .iter()
            .filter(|entry| entry.value().created_at.elapsed() > ttl)
            .map(|entry| entry.key().clone())
            .collect();
        for pid in expired {
            self.delete(&pid).await;
        }
    }

    /// Expired-page cleanup path: takes the per-dir Mutex to remove the stale
    /// entry from the index, reverse index, and evictor.
    ///
    /// **Race safety**: between the DashMap read in `get()` and the per-dir
    /// lock here, a concurrent `put()` may have replaced the entry with a fresh
    /// one (new `created_at`). We re-check `created_at.elapsed() > ttl` under
    /// the per-dir lock to avoid deleting a freshly cached page.
    async fn get_expired_path(&self, page_id: &PageId) -> usize {
        let Some(ttl) = self.options.ttl else {
            return 0; // TTL disabled — should never reach here
        };
        // First find the dir_index via the lock-free DashMap read.
        let dir_index = match self.meta.get(page_id) {
            Some(info) => info.dir_index,
            None => return 0,
        };
        // Phase 1 under per-dir Mutex: re-check expiry + meta.remove + evictor.on_remove.
        // Mutex is released before the by_file write.
        let info = {
            let _guard = self.dir_locks[dir_index].lock().unwrap();
            // Re-check under per-dir lock: a concurrent put may have refreshed the entry.
            let is_expired = self
                .meta
                .get(page_id)
                .is_some_and(|info| info.created_at.elapsed() > ttl);
            if !is_expired {
                return 0;
            }
            self.meta.remove(page_id).map(|(_, v)| v)
        };
        if let Some(info) = info {
            let di = info.dir_index;
            self.dirs[di].evictor.on_remove(page_id);
            self.dirs[di]
                .used_bytes
                .fetch_sub(info.page_size, Ordering::Relaxed);
            {
                let mut by_file = self.by_file.write().await;
                if let Some(set) = by_file.get_mut(&page_id.file_id) {
                    set.remove(&page_id.page_index);
                    if set.is_empty() {
                        by_file.remove(&page_id.file_id);
                    }
                }
            }
            counter(mn::CLIENT_CACHE_PAGES_DISCARDED).inc(1);
            counter(mn::CLIENT_CACHE_BYTES_DISCARDED).inc(info.page_size as i64);
            self.publish_occupancy();
        }
        0 // expired → miss (or concurrently refreshed → caller treats as miss, next get hits)
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

        // Reserve capacity (evicting as needed), collecting victims to delete
        // outside the lock. Each victim carries whether its file became empty
        // so the caller can also reclaim the file's identity sidecar.
        //
        // Phase C: `used_bytes` is an `AtomicU64`, so capacity checks are
        // lock-free. The per-dir `StdMutex` is only acquired briefly inside
        // `pop_victim` for evictor write operations, and is NEVER held across
        // an `.await` point.
        if self.meta.contains_key(page_id) {
            counter(mn::CLIENT_CACHE_PUT_BENIGN_RACING_ERRORS).inc(1);
            return false;
        }
        let mut victims: Vec<(PageId, bool)> = Vec::new();
        loop {
            let current = self.dirs[dir_index].used_bytes.load(Ordering::Relaxed);
            if current + page_len <= self.dirs[dir_index].capacity {
                // Try to reserve (CAS loop — concurrent puts may have consumed
                // some of the freed space, so retry on contention).
                if self.dirs[dir_index]
                    .used_bytes
                    .compare_exchange(
                        current,
                        current + page_len,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break;
                }
                // CAS failed → another put reserved; re-check capacity.
                continue;
            }
            // Over capacity → evict one victim (pop_victim acquires/releases
            // its own per-dir Mutex internally).
            match self.pop_victim(dir_index).await {
                Some((victim, size, file_empty)) => {
                    counter(mn::CLIENT_CACHE_BYTES_EVICTED).inc(size as i64);
                    counter(mn::CLIENT_CACHE_PAGES_EVICTED).inc(1);
                    victims.push((victim, file_empty));
                }
                None => {
                    counter(mn::CLIENT_CACHE_PUT_INSUFFICIENT_SPACE_ERRORS).inc(1);
                    counter(mn::CLIENT_CACHE_PUT_ERRORS).inc(1);
                    // Roll back any successful evictions? The evictions
                    // already removed the page files; nothing to undo.
                    return false;
                }
            }
        }

        // Delete evicted files outside the lock (best-effort).
        for (victim, file_empty) in &victims {
            if let Err(e) = self.stores[dir_index].delete(victim).await {
                warn!(error = %e, "evict: failed to delete page from store");
                counter(mn::CLIENT_CACHE_DELETE_FROM_STORE_ERRORS).inc(1);
            }
            if *file_empty {
                let _ = self.stores[dir_index]
                    .delete_identity(&victim.file_id)
                    .await;
            }
        }

        // Roll back the reservation on store write failure.
        if let Err(e) = self.stores[dir_index].put(page_id, &page).await {
            warn!(error = %e, "put: failed to write page to store");
            self.dirs[dir_index]
                .used_bytes
                .fetch_sub(page_len, Ordering::Relaxed);
            self.publish_occupancy();
            counter(mn::CLIENT_CACHE_PUT_STORE_WRITE_ERRORS).inc(1);
            counter(mn::CLIENT_CACHE_PUT_ERRORS).inc(1);
            return false;
        }

        // Commit metadata.
        {
            // Per-dir Mutex: serialise evictor.on_add with any concurrent
            // pop_victim for the same dir. Released before any await.
            let _dir_guard = self.dir_locks[dir_index].lock().unwrap();
            let info = PageInfo {
                page_id: page_id.clone(),
                page_size: page_len,
                dir_index,
                created_at: Instant::now(),
                scope: CacheScope::Global,
            };
            self.meta.insert(page_id.clone(), info);
            self.dirs[dir_index].evictor.on_add(page_id);
            counter(mn::CLIENT_CACHE_BYTES_WRITTEN_CACHE).inc(page_len as i64);
            drop(_dir_guard);
        }
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

        // Phase C: `get` is now fully lock-free on the hot path.
        // - `meta` is a `DashMap` → `get` takes a per-shard read guard (no global lock).
        // - `dirs[i].evictor.on_access()` uses interior mutability (`&self`).
        // - The page-lock stripe is the only contention point, and it shards
        //   1024-way, so concurrent gets on different pages never block each other.
        //
        // If the page is expired (rare), we fall through to `get_expired_path`
        // which takes the per-dir Mutex for cleanup.
        let dir_index = match self.meta.get(page_id) {
            Some(info) => {
                // Check TTL (no-op when TTL is None).
                if let Some(ttl) = self.options.ttl {
                    if info.created_at.elapsed() > ttl {
                        // Expired — fall through to the cleanup path.
                        drop(info);
                        let _ = self.get_expired_path(page_id).await;
                        return Bytes::new();
                    }
                }
                let di = info.dir_index;
                // E1: Update LRU now (before IO), via evictor's `&self`.
                self.dirs[di].evictor.on_access(page_id);
                di
            }
            None => return Bytes::new(), // miss
        };

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

        // Phase C: lock-free DashMap read for dir_index, then per-dir Mutex
        // for the multi-field atomic update. The Mutex is released before the
        // by_file async write.
        let dir_index = match self.meta.get(page_id) {
            Some(info) => info.dir_index,
            None => {
                counter(mn::CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS).inc(1);
                return false;
            }
        };
        // Phase 1 under per-dir Mutex: meta.remove + evictor.on_remove + used_bytes.
        let _info = {
            let _dir_guard = self.dir_locks[dir_index].lock().unwrap();
            let Some((_, info)) = self.meta.remove(page_id) else {
                counter(mn::CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS).inc(1);
                return false;
            };
            self.dirs[info.dir_index].evictor.on_remove(page_id);
            self.dirs[info.dir_index]
                .used_bytes
                .fetch_sub(info.page_size, Ordering::Relaxed);
            info
        };
        // Phase 2: by_file update + file_empty detection (async, no Mutex).
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
        (Arc::new(LocalCacheManager::create(o).await.unwrap()), dirs)
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
    async fn eviction_per_dir_moka() {
        // Same as eviction_per_dir_lru but explicitly using the moka-backed LRU evictor.
        let (o, dirs) = opts(8, 16, 1, CacheEvictorType::Lru, 4);
        let mgr = Arc::new(LocalCacheManager::create(o).await.unwrap());
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
    async fn moka_evictor_concurrent_gets_no_deadlock() {
        // Verify that 32 concurrent gets on the same file don't deadlock
        // with the moka-backed evictor (the primary motivation for the replacement).
        let (o, dirs) = opts(256, 1024 * 1024, 1, CacheEvictorType::Lru, 4);
        let mgr = Arc::new(LocalCacheManager::create(o).await.unwrap());
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
        let mgr = Arc::new(LocalCacheManager::create(o).await.unwrap());
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
        (Arc::new(LocalCacheManager::create(o).await.unwrap()), dirs)
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

    #[tokio::test]
    async fn sweep_expired_removes_all_stale_pages() {
        let (mgr, dirs) = manager_with_ttl(16, 1024, Duration::from_millis(30)).await;
        for p in 0..3u64 {
            assert!(
                mgr.put(&PageId::new("sweep", p), Bytes::from_static(b"xxxx"))
                    .await
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        mgr.sweep_expired().await;
        let mut dst = vec![0u8; 4];
        for p in 0..3u64 {
            assert_eq!(mgr.get(&PageId::new("sweep", p), 0, &mut dst).await, 0);
        }
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
        };
        Arc::new(LocalCacheManager::create(options).await.unwrap())
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
}
