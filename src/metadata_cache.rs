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

//! Client-side metadata cache aligned with Java `MetadataCache`.
//!
//! One LRU key (normalized path) may hold a status slot, a directory listing,
//! or both. The whole [`CachedItem`] shares a write-time TTL, matching Java
//! Guava `expireAfterWrite`.
//!
//! The cache is constructed only when `goosefs.user.metadata.cache.enabled`
//! is true **and** the expiration is `> 0`. Callers store
//! `Option<Arc<MetadataCache>>` and skip all lookup/insert when it is `None`.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lru::LruCache;
use tracing::debug;

use crate::error::{Error, Result};
use crate::metrics;
use crate::proto::grpc::file::{FileBlockInfo, FileInfo, LoadMetadataPType};

/// Result of a status-slot lookup.
#[derive(Clone, Debug)]
pub enum StatusLookup {
    /// No live status slot (miss, expired, or listing-only entry).
    Miss,
    /// Master `get_status` snapshot. Incomplete files must **not** be served
    /// as hits by callers (INV-MC-S3); use [`status_is_completed`].
    Present(Arc<FileInfo>),
    /// Negative cache. Callers must convert this to [`Error::NotFound`].
    NotFound,
}

enum StatusSlot {
    Present(Arc<FileInfo>),
    NotFound,
}

/// One LRU entry: status and listing share `inserted_at` (Java `expireAfterWrite`).
struct CachedItem {
    status: Option<StatusSlot>,
    dir_listing: Option<Arc<Vec<FileInfo>>>,
    inserted_at: Instant,
}

/// Snapshot of cache counters (tests / debugging).
#[derive(Debug, Default, Clone, Copy)]
pub struct MetadataCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub expired: u64,
    pub invalidations: u64,
    pub negative_hits: u64,
}

/// TTL-bounded LRU cache for path metadata (status + listing + negative).
pub struct MetadataCache {
    inner: Mutex<Inner>,
    ttl: Duration,
}

struct Inner {
    lru: LruCache<Arc<str>, CachedItem>,
    hits: u64,
    misses: u64,
    expired: u64,
    invalidations: u64,
    negative_hits: u64,
}

impl MetadataCache {
    /// Build a cache. Returns `None` when `ttl` is zero so callers can store
    /// `Option<Arc<MetadataCache>>` and pay nothing on the disabled path.
    ///
    /// `capacity` is clamped to at least `1` (LRU cannot be empty).
    pub fn maybe_new(ttl: Duration, capacity: usize) -> Option<Arc<Self>> {
        if ttl.is_zero() {
            return None;
        }
        let cap = NonZeroUsize::new(capacity.max(1)).expect("capacity clamped to >=1");
        Some(Arc::new(Self {
            inner: Mutex::new(Inner {
                lru: LruCache::new(cap),
                hits: 0,
                misses: 0,
                expired: 0,
                invalidations: 0,
                negative_hits: 0,
            }),
            ttl,
        }))
    }

    /// Look up the status slot for `path`.
    ///
    /// Expired entries are evicted lazily. A listing-only item counts as a
    /// status miss without dropping the listing.
    pub fn lookup_status(&self, path: &str) -> StatusLookup {
        let key = normalize_path(path);
        let mut inner = self.lock();
        if !self.retain_fresh(&mut inner, &key) {
            inner.misses += 1;
            self.bump_miss_metric();
            self.sync_size_gauge(&inner);
            return StatusLookup::Miss;
        }
        let slot = match inner.lru.get(&key).and_then(|e| e.status.as_ref()) {
            Some(StatusSlot::Present(info)) => Some(StatusLookup::Present(Arc::clone(info))),
            Some(StatusSlot::NotFound) => Some(StatusLookup::NotFound),
            None => None,
        };
        match slot {
            Some(StatusLookup::Present(info)) => {
                inner.hits += 1;
                self.bump_hit_metric();
                StatusLookup::Present(info)
            }
            Some(StatusLookup::NotFound) => {
                inner.hits += 1;
                inner.negative_hits += 1;
                self.bump_hit_metric();
                metrics::counter(metrics::name::CLIENT_METADATA_CACHE_NEGATIVE_HITS).inc(1);
                StatusLookup::NotFound
            }
            None | Some(StatusLookup::Miss) => {
                inner.misses += 1;
                self.bump_miss_metric();
                StatusLookup::Miss
            }
        }
    }

    /// Look up a cached non-recursive directory listing.
    pub fn get_listing(&self, path: &str) -> Option<Arc<Vec<FileInfo>>> {
        let key = normalize_path(path);
        let mut inner = self.lock();
        if !self.retain_fresh(&mut inner, &key) {
            inner.misses += 1;
            self.bump_miss_metric();
            self.sync_size_gauge(&inner);
            return None;
        }
        match inner.lru.get(&key).and_then(|e| e.dir_listing.clone()) {
            Some(list) => {
                inner.hits += 1;
                self.bump_hit_metric();
                Some(list)
            }
            None => {
                inner.misses += 1;
                self.bump_miss_metric();
                None
            }
        }
    }

    /// Insert or refresh the status slot. Does **not** drop an existing listing.
    /// Refreshes `inserted_at` for the whole item (Java `expireAfterWrite`).
    pub fn insert_arc(&self, path: &str, info: Arc<FileInfo>) {
        self.put_status(path, StatusSlot::Present(info));
    }

    /// Insert an owned `FileInfo` by wrapping it in `Arc`.
    pub fn insert(&self, path: &str, info: FileInfo) {
        self.insert_arc(path, Arc::new(info));
    }

    /// Record a negative-cache entry for `path`.
    pub fn insert_not_found(&self, path: &str) {
        self.put_status(path, StatusSlot::NotFound);
    }

    /// Cache a directory listing. Does **not** split children into status
    /// slots (INV-MC-S6 / Java `MetadataCache.java:132-137`).
    pub fn insert_listing(&self, path: &str, listing: Arc<Vec<FileInfo>>) {
        let key = normalize_path(path);
        let now = Instant::now();
        let mut inner = self.lock();
        if let Some(item) = inner.lru.get_mut(&key) {
            item.dir_listing = Some(listing);
            item.inserted_at = now;
        } else {
            inner.lru.put(
                key,
                CachedItem {
                    status: None,
                    dir_listing: Some(listing),
                    inserted_at: now,
                },
            );
        }
        self.sync_size_gauge(&inner);
    }

    /// Drop the cached entry for `path`. Idempotent.
    pub fn invalidate(&self, path: &str) {
        let key = normalize_path(path);
        let mut inner = self.lock();
        if inner.lru.pop(&key).is_some() {
            inner.invalidations += 1;
            metrics::counter(metrics::name::CLIENT_METADATA_CACHE_INVALIDATIONS).inc(1);
            debug!(path = %key, "MetadataCache: invalidated entry");
            self.sync_size_gauge(&inner);
        }
    }

    /// Drop `path` and its parent (Java write-path invalidate range).
    pub fn invalidate_with_parent(&self, path: &str) {
        if let Some(parent) = parent_path(path) {
            self.invalidate(&parent);
        }
        self.invalidate(path);
    }

    /// Clear every entry. Used by tests.
    pub fn clear(&self) {
        let mut inner = self.lock();
        let n = inner.lru.len() as u64;
        inner.lru.clear();
        inner.invalidations += n;
        if n > 0 {
            metrics::counter(metrics::name::CLIENT_METADATA_CACHE_INVALIDATIONS).inc(n as i64);
        }
        self.sync_size_gauge(&inner);
    }

    /// Snapshot of the internal counters (test / metric use).
    pub fn stats(&self) -> MetadataCacheStats {
        let inner = self.lock();
        MetadataCacheStats {
            hits: inner.hits,
            misses: inner.misses,
            expired: inner.expired,
            invalidations: inner.invalidations,
            negative_hits: inner.negative_hits,
        }
    }

    /// Number of live entries. Test-only view.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.lock().lru.len()
    }

    /// Rewind `inserted_at` so TTL tests do not depend on `thread::sleep`.
    /// Subtracts from the stored timestamp (does not snap to `now - age`).
    #[cfg(test)]
    fn rewind_inserted_at(&self, path: &str, age: Duration) {
        let key = normalize_path(path);
        let mut inner = self.lock();
        if let Some(item) = inner.lru.get_mut(&key) {
            item.inserted_at = item
                .inserted_at
                .checked_sub(age)
                .expect("age too large for Instant");
        }
    }

    /// Return the configured TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    fn put_status(&self, path: &str, slot: StatusSlot) {
        let key = normalize_path(path);
        let now = Instant::now();
        let mut inner = self.lock();
        if let Some(item) = inner.lru.get_mut(&key) {
            item.status = Some(slot);
            item.inserted_at = now;
        } else {
            inner.lru.put(
                key,
                CachedItem {
                    status: Some(slot),
                    dir_listing: None,
                    inserted_at: now,
                },
            );
        }
        self.sync_size_gauge(&inner);
    }

    /// Return `true` if `key` is present and unexpired. On expiry, pop it.
    fn retain_fresh(&self, inner: &mut Inner, key: &str) -> bool {
        let expired = match inner.lru.peek(key) {
            Some(entry) => entry.inserted_at.elapsed() >= self.ttl,
            None => return false,
        };
        if expired {
            inner.lru.pop(key);
            inner.expired += 1;
            metrics::counter(metrics::name::CLIENT_METADATA_CACHE_EXPIRATIONS).inc(1);
            false
        } else {
            true
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("MetadataCache mutex poisoned")
    }

    fn bump_hit_metric(&self) {
        metrics::counter(metrics::name::CLIENT_METADATA_CACHE_HITS).inc(1);
    }

    fn bump_miss_metric(&self) {
        metrics::counter(metrics::name::CLIENT_METADATA_CACHE_MISSES).inc(1);
    }

    fn sync_size_gauge(&self, inner: &Inner) {
        metrics::gauge(metrics::name::CLIENT_METADATA_CACHE_SIZE).set(inner.lru.len() as i64);
    }
}

/// `true` when the cached `FileInfo` is a completed inode (Java `isCompleted()`).
#[inline]
pub fn status_is_completed(info: &FileInfo) -> bool {
    info.completed.unwrap_or(false)
}

/// Normalize a path to Java `GooseFSURI.getPath()` form: leading `/`, no
/// trailing slash except for the root.
pub fn normalize_path(path: &str) -> Arc<str> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return Arc::from("/");
    }
    let stripped = trimmed.trim_end_matches('/');
    if stripped.is_empty() {
        return Arc::from("/");
    }
    if stripped.starts_with('/') {
        Arc::from(stripped)
    } else {
        Arc::from(format!("/{stripped}"))
    }
}

/// Parent of `path`. `None` for root `/`.
pub fn parent_path(path: &str) -> Option<String> {
    let normalized = normalize_path(path);
    if &*normalized == "/" {
        return None;
    }
    let last_slash = normalized.rfind('/')?;
    if last_slash == 0 {
        Some("/".to_string())
    } else {
        Some(normalized[..last_slash].to_string())
    }
}

/// Listing cache is skipped for recursive / ALWAYS / loadMetadataOnly /
/// `sync_interval_ms == 0` (INV-MC-S4/S5).
#[inline]
pub fn should_skip_listing_cache(
    recursive: bool,
    load_type: LoadMetadataPType,
    load_metadata_only: bool,
    sync_interval_ms: i64,
) -> bool {
    recursive
        || load_type == LoadMetadataPType::Always
        || load_metadata_only
        || sync_interval_ms == 0
}

/// Resolve a getStatus/open from the cache, or fetch from Master.
///
/// When `sync_interval_ms == 0` the cache is not consulted, but a successful
/// (or NotFound) RPC is still written back. Incomplete cached entries fall
/// through to RPC (INV-MC-S3). CheckBlocks enrichment must happen on a
/// **clone** of the returned `FileInfo` (INV-MC-D1).
pub async fn get_status_through_cache<F, Fut>(
    cache: Option<&MetadataCache>,
    path: &str,
    sync_interval_ms: i64,
    fetch: F,
) -> Result<FileInfo>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<FileInfo>>,
{
    if let Some(cache) = cache {
        if sync_interval_ms != 0 {
            match cache.lookup_status(path) {
                StatusLookup::Present(info) if status_is_completed(&info) => {
                    return Ok((*info).clone());
                }
                StatusLookup::NotFound => {
                    return Err(Error::NotFound {
                        path: path.to_string(),
                    });
                }
                StatusLookup::Present(_) | StatusLookup::Miss => {}
            }
        }
    }

    match fetch().await {
        Ok(info) => {
            if let Some(cache) = cache {
                cache.insert_arc(path, Arc::new(info.clone()));
            }
            Ok(info)
        }
        Err(e) if e.is_not_found() => {
            if let Some(cache) = cache {
                cache.insert_not_found(path);
            }
            Err(e)
        }
        Err(e) => Err(e),
    }
}

/// Resolve a non-recursive listing from the cache, or fetch from Master.
///
/// Callers must pass `skip = should_skip_listing_cache(...)`. Recursive
/// walks must not use this helper (INV-MC-S5).
pub async fn list_status_through_cache<F, Fut>(
    cache: Option<&MetadataCache>,
    path: &str,
    skip: bool,
    fetch: F,
) -> Result<Vec<FileInfo>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<FileInfo>>>,
{
    if let Some(cache) = cache {
        if !skip {
            if let Some(list) = cache.get_listing(path) {
                return Ok((*list).clone());
            }
        }
    }

    let list = fetch().await?;
    if let Some(cache) = cache {
        if !skip {
            cache.insert_listing(path, Arc::new(list.clone()));
        }
    }
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn info_of_length(len: i64) -> FileInfo {
        FileInfo {
            length: Some(len),
            completed: Some(true),
            ..Default::default()
        }
    }

    fn incomplete_info(len: i64) -> FileInfo {
        FileInfo {
            length: Some(len),
            completed: Some(false),
            ..Default::default()
        }
    }

    fn enabled_cache() -> Arc<MetadataCache> {
        MetadataCache::maybe_new(Duration::from_secs(60), 128).unwrap()
    }

    #[test]
    fn maybe_new_none_when_ttl_zero() {
        assert!(MetadataCache::maybe_new(Duration::ZERO, 1024).is_none());
    }

    #[test]
    fn maybe_new_none_when_enabled_false() {
        // Construction gate is `enabled && maybe_new(ttl, cap)`. ttl=0 is the
        // data-plane stand-in for "do not construct".
        assert!(MetadataCache::maybe_new(Duration::ZERO, 100_000).is_none());
    }

    #[test]
    fn maybe_new_clamps_capacity_to_one() {
        let cache = MetadataCache::maybe_new(Duration::from_secs(1), 0)
            .expect("cache should be enabled with non-zero ttl");
        cache.insert("/a", info_of_length(1));
        cache.insert("/b", info_of_length(2));
        assert!(
            matches!(cache.lookup_status("/a"), StatusLookup::Miss),
            "LRU cap = 1 must evict older"
        );
        assert!(matches!(
            cache.lookup_status("/b"),
            StatusLookup::Present(_)
        ));
    }

    #[test]
    fn hit_and_miss_counters_and_lookup_work() {
        let cache = enabled_cache();
        assert!(matches!(cache.lookup_status("/x"), StatusLookup::Miss));
        cache.insert("/x", info_of_length(42));
        match cache.lookup_status("/x") {
            StatusLookup::Present(got) => assert_eq!(got.length, Some(42)),
            StatusLookup::Miss => panic!("expected Present, got Miss"),
            StatusLookup::NotFound => panic!("expected Present, got NotFound"),
        }
        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.expired, 0);
    }

    #[test]
    fn not_found_slot_roundtrip() {
        let cache = enabled_cache();
        cache.insert_not_found("/missing");
        assert!(matches!(
            cache.lookup_status("/missing"),
            StatusLookup::NotFound
        ));
        assert_eq!(cache.stats().negative_hits, 1);
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn listing_not_split_into_status() {
        let cache = enabled_cache();
        let child = FileInfo {
            path: Some("/dir/child".into()),
            length: Some(7),
            completed: Some(true),
            ..Default::default()
        };
        cache.insert_listing("/dir", Arc::new(vec![child]));
        assert!(
            matches!(cache.lookup_status("/dir/child"), StatusLookup::Miss),
            "INV-MC-S6: listing children must not become status entries"
        );
        assert!(
            matches!(cache.lookup_status("/dir"), StatusLookup::Miss),
            "listing insert must not invent a status slot for the directory"
        );
        assert_eq!(cache.get_listing("/dir").unwrap().len(), 1);
    }

    #[test]
    fn invalidate_with_parent_drops_both() {
        let cache = enabled_cache();
        cache.insert("/data/file", info_of_length(1));
        cache.insert_listing("/data", Arc::new(vec![info_of_length(1)]));
        cache.invalidate_with_parent("/data/file");
        assert!(matches!(
            cache.lookup_status("/data/file"),
            StatusLookup::Miss
        ));
        assert!(cache.get_listing("/data").is_none());
    }

    #[test]
    fn incomplete_status_not_served_as_hit() {
        let cache = enabled_cache();
        cache.insert("/wip", incomplete_info(10));
        match cache.lookup_status("/wip") {
            StatusLookup::Present(info) => {
                assert!(
                    !status_is_completed(&info),
                    "incomplete must not be served as a completed hit"
                );
            }
            StatusLookup::Miss => panic!("expected Present(incomplete), got Miss"),
            StatusLookup::NotFound => panic!("expected Present(incomplete), got NotFound"),
        }
    }

    #[test]
    fn ttl_expires_both_status_and_listing() {
        let cache = MetadataCache::maybe_new(Duration::from_millis(1), 128).unwrap();
        cache.insert("/e", info_of_length(7));
        cache.insert_listing("/e", Arc::new(vec![info_of_length(1)]));
        std::thread::sleep(Duration::from_millis(5));
        assert!(matches!(cache.lookup_status("/e"), StatusLookup::Miss));
        assert!(cache.get_listing("/e").is_none());
        assert!(cache.stats().expired >= 1);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn insert_listing_refreshes_inserted_at() {
        let ttl = Duration::from_secs(10);
        let cache = MetadataCache::maybe_new(ttl, 128).unwrap();
        cache.insert("/d", info_of_length(1));
        cache.rewind_inserted_at("/d", Duration::from_secs(6));
        cache.insert_listing("/d", Arc::new(vec![info_of_length(2)]));
        cache.rewind_inserted_at("/d", Duration::from_secs(6));
        // 6s + 6s > 10s TTL unless insert_listing reset inserted_at.
        assert!(matches!(
            cache.lookup_status("/d"),
            StatusLookup::Present(_)
        ));
        assert!(cache.get_listing("/d").is_some());
    }

    #[test]
    fn ttl_expiry_evicts_and_counts() {
        let cache = MetadataCache::maybe_new(Duration::from_millis(1), 128).unwrap();
        cache.insert("/e", info_of_length(7));
        std::thread::sleep(Duration::from_millis(5));
        assert!(matches!(cache.lookup_status("/e"), StatusLookup::Miss));
        let s = cache.stats();
        assert_eq!(s.expired, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn invalidate_removes_entry_and_counts() {
        let cache = enabled_cache();
        cache.insert("/i", info_of_length(1));
        assert_eq!(cache.len(), 1);
        cache.invalidate("/i");
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.stats().invalidations, 1);
        cache.invalidate("/i");
        assert_eq!(cache.stats().invalidations, 1);
    }

    #[test]
    fn clear_drops_all_and_counts_as_invalidations() {
        let cache = enabled_cache();
        for i in 0..5 {
            cache.insert(&format!("/k{}", i), info_of_length(i as i64));
        }
        assert_eq!(cache.len(), 5);
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.stats().invalidations, 5);
    }

    #[test]
    fn get_updates_lru_recency() {
        let cache = MetadataCache::maybe_new(Duration::from_secs(60), 2).unwrap();
        cache.insert("/a", info_of_length(1));
        cache.insert("/b", info_of_length(2));
        assert!(matches!(
            cache.lookup_status("/a"),
            StatusLookup::Present(_)
        ));
        cache.insert("/c", info_of_length(3));
        assert!(matches!(
            cache.lookup_status("/a"),
            StatusLookup::Present(_)
        ));
        assert!(matches!(cache.lookup_status("/b"), StatusLookup::Miss));
        assert!(matches!(
            cache.lookup_status("/c"),
            StatusLookup::Present(_)
        ));
    }

    #[test]
    fn insert_arc_shares_arc_identity_with_get() {
        let cache = enabled_cache();
        let original = Arc::new(info_of_length(99));
        cache.insert_arc("/shared", Arc::clone(&original));
        match cache.lookup_status("/shared") {
            StatusLookup::Present(got) => {
                assert!(
                    Arc::ptr_eq(&original, &got),
                    "lookup must return the same Arc inserted via insert_arc"
                );
            }
            _ => panic!("expected Present"),
        }
    }

    #[test]
    fn parent_path_helpers() {
        assert_eq!(parent_path("/data/hello.txt"), Some("/data".to_string()));
        assert_eq!(parent_path("/hello.txt"), Some("/".to_string()));
        assert_eq!(parent_path("/"), None);
        assert_eq!(
            parent_path("/a/b/c/file.parquet"),
            Some("/a/b/c".to_string())
        );
        assert_eq!(parent_path("/data/dir/"), Some("/data".to_string()));
    }

    #[test]
    fn normalize_path_strips_trailing_slash() {
        assert_eq!(&*normalize_path("/foo/"), "/foo");
        assert_eq!(&*normalize_path("/"), "/");
        assert_eq!(&*normalize_path("foo"), "/foo");
    }

    #[tokio::test]
    async fn get_status_through_cache_second_call_is_hit() {
        let cache = enabled_cache();
        let rpcs = AtomicUsize::new(0);
        let fetch = || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(info_of_length(1))
        };
        get_status_through_cache(Some(&cache), "/f", -1, fetch)
            .await
            .unwrap();
        get_status_through_cache(Some(&cache), "/f", -1, || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(info_of_length(1))
        })
        .await
        .unwrap();
        assert_eq!(rpcs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn get_status_through_cache_disabled_always_rpcs() {
        let rpcs = AtomicUsize::new(0);
        for _ in 0..2 {
            get_status_through_cache(None, "/f", -1, || async {
                rpcs.fetch_add(1, Ordering::SeqCst);
                Ok(info_of_length(1))
            })
            .await
            .unwrap();
        }
        assert_eq!(rpcs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn get_status_not_found_cached() {
        let cache = enabled_cache();
        let rpcs = AtomicUsize::new(0);
        let missing = || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Err(Error::NotFound {
                path: "/gone".into(),
            })
        };
        assert!(get_status_through_cache(Some(&cache), "/gone", -1, missing)
            .await
            .unwrap_err()
            .is_not_found());
        assert!(get_status_through_cache(Some(&cache), "/gone", -1, missing)
            .await
            .unwrap_err()
            .is_not_found());
        assert_eq!(rpcs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn sync_interval_zero_skips_status_cache() {
        let cache = enabled_cache();
        let rpcs = AtomicUsize::new(0);
        for _ in 0..2 {
            get_status_through_cache(Some(&cache), "/f", 0, || async {
                rpcs.fetch_add(1, Ordering::SeqCst);
                Ok(info_of_length(1))
            })
            .await
            .unwrap();
        }
        assert_eq!(rpcs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn create_invalidates_negative_cache() {
        let cache = enabled_cache();
        cache.insert_not_found("/new");
        cache.invalidate_with_parent("/new");
        let rpcs = AtomicUsize::new(0);
        get_status_through_cache(Some(&cache), "/new", -1, || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(info_of_length(1))
        })
        .await
        .unwrap();
        assert_eq!(rpcs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn list_status_through_cache_second_call_is_hit() {
        let cache = enabled_cache();
        let rpcs = AtomicUsize::new(0);
        let listing = vec![info_of_length(1)];
        list_status_through_cache(Some(&cache), "/d", false, || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(listing.clone())
        })
        .await
        .unwrap();
        list_status_through_cache(Some(&cache), "/d", false, || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(listing.clone())
        })
        .await
        .unwrap();
        assert_eq!(rpcs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn list_status_skip_always_rpcs() {
        let cache = enabled_cache();
        let rpcs = AtomicUsize::new(0);
        let skip = should_skip_listing_cache(false, LoadMetadataPType::Always, false, -1);
        assert!(skip);
        for _ in 0..2 {
            list_status_through_cache(Some(&cache), "/d", skip, || async {
                rpcs.fetch_add(1, Ordering::SeqCst);
                Ok(vec![info_of_length(1)])
            })
            .await
            .unwrap();
        }
        assert_eq!(rpcs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn incomplete_falls_through_to_rpc() {
        let cache = enabled_cache();
        cache.insert("/wip", incomplete_info(3));
        let rpcs = AtomicUsize::new(0);
        let got = get_status_through_cache(Some(&cache), "/wip", -1, || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(info_of_length(3))
        })
        .await
        .unwrap();
        assert_eq!(rpcs.load(Ordering::SeqCst), 1);
        assert!(status_is_completed(&got));
    }

    /// INV-MC-S1: mkdir / create / delete / writer close|cancel all call
    /// `invalidate_metadata(path, true)`. Stale status (including old
    /// length/block_ids) must not be served afterwards.
    #[tokio::test]
    async fn write_invalidate_drops_stale_status() {
        let cache = enabled_cache();
        cache.insert("/data/file", info_of_length(10));
        let hit = get_status_through_cache(Some(&cache), "/data/file", -1, || async {
            panic!("must be a cache hit before invalidate");
        })
        .await
        .unwrap();
        assert_eq!(hit.length, Some(10));

        cache.invalidate_with_parent("/data/file");

        let rpcs = AtomicUsize::new(0);
        let got = get_status_through_cache(Some(&cache), "/data/file", -1, || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(info_of_length(99))
        })
        .await
        .unwrap();
        assert_eq!(
            rpcs.load(Ordering::SeqCst),
            1,
            "INV-MC-S1: must RPC after write"
        );
        assert_eq!(got.length, Some(99), "must not return pre-write length");
    }

    /// INV-MC-S1: mkdir of a previously negatively-cached path must RPC.
    #[tokio::test]
    async fn mkdir_invalidates_negative_cache_then_get_rpcs() {
        let cache = enabled_cache();
        cache.insert_not_found("/data/dir");
        cache.invalidate_with_parent("/data/dir");
        let rpcs = AtomicUsize::new(0);
        let got = get_status_through_cache(Some(&cache), "/data/dir", -1, || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(FileInfo {
                folder: Some(true),
                completed: Some(true),
                ..Default::default()
            })
        })
        .await
        .unwrap();
        assert_eq!(rpcs.load(Ordering::SeqCst), 1);
        assert_eq!(got.folder, Some(true));
    }

    /// INV-MC-S2: create/mkdir must drop the parent listing so the next
    /// `list_status(parent)` RPCs and sees the new child.
    #[tokio::test]
    async fn write_invalidate_drops_parent_listing() {
        let cache = enabled_cache();
        cache.insert_listing("/data", Arc::new(vec![info_of_length(1)]));
        let skip = should_skip_listing_cache(false, LoadMetadataPType::Once, false, -1);
        let first = list_status_through_cache(Some(&cache), "/data", skip, || async {
            panic!("parent listing must hit before invalidate");
        })
        .await
        .unwrap();
        assert_eq!(first.len(), 1);

        cache.invalidate_with_parent("/data/child");

        let rpcs = AtomicUsize::new(0);
        let after = list_status_through_cache(Some(&cache), "/data", skip, || async {
            rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![info_of_length(1), info_of_length(2)])
        })
        .await
        .unwrap();
        assert_eq!(
            rpcs.load(Ordering::SeqCst),
            1,
            "INV-MC-S2: parent listing must miss"
        );
        assert_eq!(after.len(), 2);
    }

    /// INV-MC-S1 + S2: rename invalidates src, dst, and both parents
    /// (`BaseFileSystem::rename`).
    #[tokio::test]
    async fn rename_invalidates_src_dst_and_parents() {
        let cache = enabled_cache();
        cache.insert("/a/src", info_of_length(1));
        cache.insert("/b/dst", info_of_length(2));
        cache.insert_listing("/a", Arc::new(vec![info_of_length(1)]));
        cache.insert_listing("/b", Arc::new(vec![info_of_length(2)]));

        cache.invalidate_with_parent("/a/src");
        cache.invalidate_with_parent("/b/dst");

        let status_rpcs = AtomicUsize::new(0);
        get_status_through_cache(Some(&cache), "/a/src", -1, || async {
            status_rpcs.fetch_add(1, Ordering::SeqCst);
            Err(Error::NotFound {
                path: "/a/src".into(),
            })
        })
        .await
        .unwrap_err();
        get_status_through_cache(Some(&cache), "/b/dst", -1, || async {
            status_rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(info_of_length(1))
        })
        .await
        .unwrap();
        assert_eq!(
            status_rpcs.load(Ordering::SeqCst),
            2,
            "INV-MC-S1: src and dst must RPC"
        );

        let skip = should_skip_listing_cache(false, LoadMetadataPType::Once, false, -1);
        let list_rpcs = AtomicUsize::new(0);
        list_status_through_cache(Some(&cache), "/a", skip, || async {
            list_rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        })
        .await
        .unwrap();
        list_status_through_cache(Some(&cache), "/b", skip, || async {
            list_rpcs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![info_of_length(1)])
        })
        .await
        .unwrap();
        assert_eq!(
            list_rpcs.load(Ordering::SeqCst),
            2,
            "INV-MC-S2: both parent listings must miss"
        );
    }

    /// INV-MC-S5: `load_metadata_only=true` must not read or write listing cache.
    #[tokio::test]
    async fn load_metadata_only_skips_listing_cache() {
        let cache = enabled_cache();
        cache.insert_listing("/d", Arc::new(vec![info_of_length(1)]));
        let skip = should_skip_listing_cache(false, LoadMetadataPType::Once, true, -1);
        assert!(skip);
        let rpcs = AtomicUsize::new(0);
        for _ in 0..2 {
            list_status_through_cache(Some(&cache), "/d", skip, || async {
                rpcs.fetch_add(1, Ordering::SeqCst);
                Ok(vec![info_of_length(9)])
            })
            .await
            .unwrap();
        }
        assert_eq!(rpcs.load(Ordering::SeqCst), 2);
        assert_eq!(
            cache.get_listing("/d").unwrap().len(),
            1,
            "loadMetadataOnly must not overwrite the cached listing"
        );
    }

    /// INV-MC-S5: recursive listing never consults the listing cache.
    #[tokio::test]
    async fn recursive_skips_listing_cache() {
        let cache = enabled_cache();
        cache.insert_listing("/d", Arc::new(vec![info_of_length(1)]));
        let skip = should_skip_listing_cache(true, LoadMetadataPType::Once, false, -1);
        assert!(skip);
        let rpcs = AtomicUsize::new(0);
        for _ in 0..2 {
            list_status_through_cache(Some(&cache), "/d", skip, || async {
                rpcs.fetch_add(1, Ordering::SeqCst);
                Ok(vec![info_of_length(1), info_of_length(2)])
            })
            .await
            .unwrap();
        }
        assert_eq!(rpcs.load(Ordering::SeqCst), 2);
    }

    fn info_with_empty_locations(block_id: i64) -> FileInfo {
        FileInfo {
            length: Some(100),
            completed: Some(true),
            block_ids: vec![block_id],
            file_block_infos: vec![FileBlockInfo {
                offset: Some(0),
                block_info: Some(crate::proto::grpc::BlockInfo {
                    block_id: Some(block_id),
                    length: Some(100),
                    locations: vec![],
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// INV-MC-D1: CheckBlocks enrichment mutates a local clone; cached
    /// `FileInfo.locations` must stay as inserted (empty Master snapshot).
    #[tokio::test]
    async fn checkblocks_locations_not_written_back_to_cache() {
        let cache = enabled_cache();
        let original = Arc::new(info_with_empty_locations(42));
        cache.insert_arc("/f", Arc::clone(&original));

        let mut fetched = get_status_through_cache(Some(&cache), "/f", -1, || async {
            panic!("must be a cache hit");
        })
        .await
        .unwrap();
        fetched.file_block_infos[0]
            .block_info
            .as_mut()
            .unwrap()
            .locations
            .push(crate::proto::grpc::BlockLocation {
                worker_id: Some(7),
                worker_address: None,
            });
        assert_eq!(
            fetched.file_block_infos[0]
                .block_info
                .as_ref()
                .unwrap()
                .locations
                .len(),
            1,
            "local clone is what open/get_status enrich"
        );

        match cache.lookup_status("/f") {
            StatusLookup::Present(cached) => {
                assert!(
                    Arc::ptr_eq(&original, &cached),
                    "INV-MC-D2: cache still holds the inserted Arc"
                );
                assert!(
                    cached.file_block_infos[0]
                        .block_info
                        .as_ref()
                        .unwrap()
                        .locations
                        .is_empty(),
                    "INV-MC-D1: CheckBlocks locations must not be written back"
                );
            }
            other => panic!("expected Present, got {other:?}"),
        }
    }
}
