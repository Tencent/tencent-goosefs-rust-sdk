//! Opt-in short-TTL cache for `FileInfo` metadata
//! (FLAMEGRAPH_OPTIMIZATION_PLAN §A3).
//!
//! # Rationale
//!
//! `MasterClient::get_status` shows up as ~2.8 % on-CPU on the profiling
//! workload because every open pays the round-trip. When the same file is
//! opened multiple times inside one query (typical Lance / DuckDB scan
//! pattern) all but the first call are redundant.
//!
//! # Design
//!
//! - **Data structure**: `LruCache<Arc<str>, CachedFileInfo>` guarded by a
//!   `std::sync::Mutex` (short critical sections, no async), bounded by
//!   `file_info_cache_capacity`. Uses the pre-existing `lru = "0.12"`
//!   dependency — no new crates.
//! - **TTL**: monotonic `Instant` per entry; `get` returns `None` on
//!   expiry and lazily evicts.
//! - **Kill switch**: `Duration::ZERO` disables the cache entirely; the
//!   [`FileInfoCache::maybe_new`] constructor returns `None` in that case
//!   so the read path can `Option::and_then` cheaply (a `None` cache is
//!   never consulted, never populated, and never invalidated).
//! - **Consistency guarantee**: the SDK **explicitly invalidates** entries
//!   on every write / delete / rename issued through this client (see
//!   call sites in `fs::base::BaseFileSystem` and
//!   `io::file_writer::GoosefsFileWriter`). The staleness window of `ttl`
//!   therefore only affects out-of-band mutations by *other* writers,
//!   which is the documented contract of the opt-in flag.
//!
//! # Non-goals
//!
//! - Not a negative cache (misses do not populate).
//! - Not a persistent cache (per-process only).
//! - Not shared across `FileSystemContext`s.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lru::LruCache;
use tracing::debug;

use crate::proto::grpc::file::FileInfo;

/// One cached `FileInfo` entry with its capture time.
#[derive(Clone)]
struct CachedFileInfo {
    /// The `FileInfo` snapshot returned by `MasterClient::get_status`
    /// at capture time.
    ///
    /// **S3** (`docs/perf/2026-07-07-hotspot-optimizations/README.md`):
    /// wrapped in `Arc<FileInfo>` so `get` returns an `Arc` clone (one
    /// atomic inc) instead of a deep `FileInfo::clone` (which copies
    /// `block_ids: Vec<i64>`, `file_block_infos: Vec<FileBlockInfo>`,
    /// `ufs_path: Option<String>`, etc.). On a typical Lance scan the
    /// same file is opened many times — the old path cloned the entire
    /// `FileInfo` struct per range read.
    info: Arc<FileInfo>,
    /// Instant at which the entry was inserted / refreshed.
    inserted_at: Instant,
}

/// Metrics counters for observability. Cheap `AtomicU64`s — the read path
/// increments them on every open, so we keep the type layout compact.
///
/// Exposed via `stats()` for tests and (future) Prometheus scrape.
#[derive(Debug, Default)]
pub struct FileInfoCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub expired: u64,
    pub invalidations: u64,
}

/// Opt-in TTL-bounded LRU cache for `FileInfo` metadata.
///
/// Use [`FileInfoCache::maybe_new`] — it returns `None` when the cache is
/// disabled (`ttl == 0`), so cost is zero on the disabled path.
pub struct FileInfoCache {
    inner: Mutex<Inner>,
    ttl: Duration,
}

struct Inner {
    lru: LruCache<Arc<str>, CachedFileInfo>,
    hits: u64,
    misses: u64,
    expired: u64,
    invalidations: u64,
}

impl FileInfoCache {
    /// Build a cache with the given `ttl` and `capacity`. Returns `None`
    /// when the cache is disabled (`ttl == 0`), so callers can store an
    /// `Option<Arc<FileInfoCache>>` and get zero overhead in the default
    /// (disabled) configuration.
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
            }),
            ttl,
        }))
    }

    /// Look up a fresh `FileInfo` for `path`. Returns `None` on miss or
    /// when the cached entry is older than `ttl` (in which case it is
    /// evicted lazily — no separate sweep task).
    ///
    /// **S3**: returns `Arc<FileInfo>` instead of `FileInfo` — the clone
    /// is a single atomic increment rather than a deep copy of the
    /// `block_ids` / `file_block_infos` / `ufs_path` fields.
    pub fn get(&self, path: &str) -> Option<Arc<FileInfo>> {
        let mut inner = self.inner.lock().expect("FileInfoCache mutex poisoned");
        // We use `peek` first to inspect age without touching the LRU
        // recency, then `pop` on expiry so the next miss re-populates
        // rather than serving stale bytes.
        let expired = match inner.lru.peek(path) {
            Some(entry) => entry.inserted_at.elapsed() >= self.ttl,
            None => {
                inner.misses += 1;
                return None;
            }
        };
        if expired {
            inner.lru.pop(path);
            inner.expired += 1;
            inner.misses += 1;
            return None;
        }
        // Fresh — bump LRU recency and clone the Arc<FileInfo> out.
        // (`get` on `LruCache` is `&mut self` because it moves the entry
        // to the front.)
        let info = inner.lru.get(path).map(|e| Arc::clone(&e.info));
        if info.is_some() {
            inner.hits += 1;
        }
        info
    }

    /// Insert or refresh the cached entry for `path`.
    ///
    /// **S3**: wraps `info` in `Arc<FileInfo>` on insert so `get` can
    /// return an `Arc` clone (one atomic inc) instead of a deep copy.
    pub fn insert(&self, path: &str, info: FileInfo) {
        self.insert_arc(path, Arc::new(info));
    }

    /// Insert a pre-wrapped `Arc<FileInfo>` — lets a cache-miss path
    /// share the same `Arc` with the caller's return value, avoiding
    /// both the `FileInfo::clone` for the cache and the `Arc::new` on
    /// the return path.
    ///
    /// **S3**: added so `init_with_context` can do `cache.insert_arc(path,
    /// arc.clone())` on a miss — the cache gets an `Arc` clone (atomic
    /// inc) and the caller keeps the original `Arc` (no deep copy at
    /// all on the miss path either).
    pub fn insert_arc(&self, path: &str, info: Arc<FileInfo>) {
        let key: Arc<str> = Arc::from(path);
        let entry = CachedFileInfo {
            info,
            inserted_at: Instant::now(),
        };
        let mut inner = self.inner.lock().expect("FileInfoCache mutex poisoned");
        inner.lru.put(key, entry);
    }

    /// Explicitly drop the cached entry for `path`. Idempotent.
    ///
    /// The write path (create / delete / rename) MUST call this after a
    /// successful mutation so subsequent reads observe fresh metadata.
    pub fn invalidate(&self, path: &str) {
        let mut inner = self.inner.lock().expect("FileInfoCache mutex poisoned");
        if inner.lru.pop(path).is_some() {
            inner.invalidations += 1;
            debug!(path = %path, "FileInfoCache: invalidated entry after write/delete/rename");
        }
    }

    /// Clear every entry. Used by tests and explicit `drop_cache` requests.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().expect("FileInfoCache mutex poisoned");
        let n = inner.lru.len() as u64;
        inner.lru.clear();
        inner.invalidations += n;
    }

    /// Snapshot of the internal counters (test / metric use).
    pub fn stats(&self) -> FileInfoCacheStats {
        let inner = self.inner.lock().expect("FileInfoCache mutex poisoned");
        FileInfoCacheStats {
            hits: inner.hits,
            misses: inner.misses,
            expired: inner.expired,
            invalidations: inner.invalidations,
        }
    }

    /// Number of live entries. Test-only view.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("FileInfoCache mutex poisoned")
            .lru
            .len()
    }

    /// Return the configured TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info_of_length(len: i64) -> FileInfo {
        FileInfo {
            length: Some(len),
            ..Default::default()
        }
    }

    #[test]
    fn maybe_new_returns_none_when_disabled() {
        assert!(FileInfoCache::maybe_new(Duration::ZERO, 1024).is_none());
    }

    #[test]
    fn maybe_new_clamps_capacity_to_one() {
        // Cap = 0 must not panic — clamp to 1.
        let cache = FileInfoCache::maybe_new(Duration::from_secs(1), 0)
            .expect("cache should be enabled with non-zero ttl");
        cache.insert("/a", info_of_length(1));
        cache.insert("/b", info_of_length(2)); // evicts /a (LRU cap = 1).
        assert!(cache.get("/a").is_none(), "LRU cap = 1 must evict older");
        assert!(cache.get("/b").is_some());
    }

    #[test]
    fn hit_and_miss_counters_and_lookup_work() {
        let cache = FileInfoCache::maybe_new(Duration::from_secs(60), 128).unwrap();
        assert!(cache.get("/x").is_none()); // miss
        cache.insert("/x", info_of_length(42));
        let got = cache.get("/x").unwrap(); // hit
        assert_eq!(got.length, Some(42));

        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.expired, 0);
    }

    #[test]
    fn ttl_expiry_evicts_and_counts() {
        // 1 ms TTL; sleep past it and confirm the entry is treated as a
        // miss (with `expired` ticked) rather than served stale.
        let cache = FileInfoCache::maybe_new(Duration::from_millis(1), 128).unwrap();
        cache.insert("/e", info_of_length(7));
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            cache.get("/e").is_none(),
            "expired entry must not be served"
        );
        let s = cache.stats();
        assert_eq!(s.expired, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(cache.len(), 0, "expired entry must be evicted lazily");
    }

    #[test]
    fn invalidate_removes_entry_and_counts() {
        let cache = FileInfoCache::maybe_new(Duration::from_secs(60), 128).unwrap();
        cache.insert("/i", info_of_length(1));
        assert_eq!(cache.len(), 1);
        cache.invalidate("/i");
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.stats().invalidations, 1);

        // Idempotent — second invalidate on missing key does not tick.
        cache.invalidate("/i");
        assert_eq!(cache.stats().invalidations, 1);
    }

    #[test]
    fn clear_drops_all_and_counts_as_invalidations() {
        let cache = FileInfoCache::maybe_new(Duration::from_secs(60), 128).unwrap();
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
        // With capacity=2 and access order (a, b, a), inserting c must
        // evict *b* (least-recently used), not a.
        let cache = FileInfoCache::maybe_new(Duration::from_secs(60), 2).unwrap();
        cache.insert("/a", info_of_length(1));
        cache.insert("/b", info_of_length(2));
        assert!(cache.get("/a").is_some(), "warm /a to bump recency");
        cache.insert("/c", info_of_length(3));
        assert!(cache.get("/a").is_some(), "/a must survive as MRU");
        assert!(cache.get("/b").is_none(), "/b must be evicted as LRU");
        assert!(cache.get("/c").is_some());
    }

    /// S3: `insert_arc` must share the same `Arc` with subsequent `get`
    /// callers (atomic refcount bump only — no deep `FileInfo` clone).
    #[test]
    fn insert_arc_shares_arc_identity_with_get() {
        let cache = FileInfoCache::maybe_new(Duration::from_secs(60), 128).unwrap();
        let original = Arc::new(info_of_length(99));
        cache.insert_arc("/shared", Arc::clone(&original));

        let got = cache.get("/shared").expect("hit");
        assert!(
            Arc::ptr_eq(&original, &got),
            "get must return the same Arc inserted via insert_arc"
        );
        assert_eq!(got.length, Some(99));
    }
}
