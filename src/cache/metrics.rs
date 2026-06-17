//! Page-cache metric name constants.
//!
//! These mirror Java `MetricKey.Client.Cache*` and are registered through the
//! existing [`crate::metrics`] registry (so they flow through the same
//! heartbeat / pushgateway reporting path — no new transport needed).
//!
//! The instrumentation is wired up in [`crate::cache::manager::LocalCacheManager`]
//! and [`crate::cache::caching_reader`]; [`publish_hit_rate`] recomputes the
//! hit-rate gauge from the cache-hit vs. external-read byte counters on the
//! read hot paths.

/// Metric name constants for the client local page cache.
pub mod name {
    // ── Bytes served / requested ─────────────────────────────
    /// Bytes served directly from the local cache (hits).
    pub const CLIENT_CACHE_BYTES_READ_CACHE: &str = "Client.CacheBytesReadCache";
    /// Bytes served from the in-stream buffer.
    pub const CLIENT_CACHE_BYTES_READ_IN_STREAM_BUFFER: &str =
        "Client.CacheBytesReadInStreamBuffer";
    /// Bytes read from the external source (worker/UFS) on a miss.
    pub const CLIENT_CACHE_BYTES_READ_EXTERNAL: &str = "Client.CacheBytesReadExternal";
    /// Bytes requested from the external source (may exceed bytes actually used).
    pub const CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL: &str = "Client.CacheBytesRequestedExternal";
    /// Bytes written into the cache (fills).
    pub const CLIENT_CACHE_BYTES_WRITTEN_CACHE: &str = "Client.CacheBytesWrittenCache";

    // ── Latency ──────────────────────────────────────────────
    /// Cumulative time spent reading pages from the cache (nanos).
    pub const CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS: &str = "Client.CachePageReadCacheTimeNanos";
    /// Cumulative time spent reading pages from the external source (nanos).
    pub const CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS: &str =
        "Client.CachePageReadExternalTimeNanos";

    // ── Capacity / occupancy gauges ──────────────────────────
    /// Number of pages currently cached.
    pub const CLIENT_CACHE_PAGES: &str = "Client.CachePages";
    /// Available cache space in bytes.
    pub const CLIENT_CACHE_SPACE_AVAILABLE: &str = "Client.CacheSpaceAvailable";
    /// Used cache space in bytes.
    pub const CLIENT_CACHE_SPACE_USED: &str = "Client.CacheSpaceUsed";
    /// Used cache space count (entries).
    pub const CLIENT_CACHE_SPACE_USED_COUNT: &str = "Client.CacheSpaceUsedCount";
    /// Cache hit rate (0..=100, computed periodically).
    pub const CLIENT_CACHE_HIT_RATE: &str = "Client.CacheHitRate";

    // ── Eviction / discard ───────────────────────────────────
    /// Bytes evicted by the replacement policy.
    pub const CLIENT_CACHE_BYTES_EVICTED: &str = "Client.CacheBytesEvicted";
    /// Pages evicted by the replacement policy.
    pub const CLIENT_CACHE_PAGES_EVICTED: &str = "Client.CachePagesEvicted";
    /// Bytes discarded (e.g. failed fill).
    pub const CLIENT_CACHE_BYTES_DISCARDED: &str = "Client.CacheBytesDiscarded";
    /// Pages discarded.
    pub const CLIENT_CACHE_PAGES_DISCARDED: &str = "Client.CachePagesDiscarded";

    // ── State ────────────────────────────────────────────────
    /// Cache state (see [`crate::cache::CacheState::as_i64`]).
    pub const CLIENT_CACHE_STATE: &str = "Client.CacheState";

    // ── Error counters ───────────────────────────────────────
    /// General cleanup errors.
    pub const CLIENT_CACHE_CLEAN_ERRORS: &str = "Client.CacheCleanErrors";
    /// Cleanup-get errors.
    pub const CLIENT_CACHE_CLEANUP_GET_ERRORS: &str = "Client.CacheCleanupGetErrors";
    /// Cleanup-put errors.
    pub const CLIENT_CACHE_CLEANUP_PUT_ERRORS: &str = "Client.CacheCleanupPutErrors";
    /// Page-store create errors.
    pub const CLIENT_CACHE_CREATE_ERRORS: &str = "Client.CacheCreateErrors";
    /// Delete errors (aggregate).
    pub const CLIENT_CACHE_DELETE_ERRORS: &str = "Client.CacheDeleteErrors";
    /// Delete of a non-existing page.
    pub const CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS: &str =
        "Client.CacheDeleteNonExistingPageErrors";
    /// Delete attempted while not ready.
    pub const CLIENT_CACHE_DELETE_NOT_READY_ERRORS: &str = "Client.CacheDeleteNotReadyErrors";
    /// Delete-from-store errors.
    pub const CLIENT_CACHE_DELETE_FROM_STORE_ERRORS: &str = "Client.CacheDeleteFromStoreErrors";
    /// Store-delete errors during delete.
    pub const CLIENT_CACHE_DELETE_STORE_DELETE_ERRORS: &str = "Client.CacheDeleteStoreDeleteErrors";
    /// Get errors (aggregate).
    pub const CLIENT_CACHE_GET_ERRORS: &str = "Client.CacheGetErrors";
    /// Get attempted while not ready.
    pub const CLIENT_CACHE_GET_NOT_READY_ERRORS: &str = "Client.CacheGetNotReadyErrors";
    /// Store-read errors during get.
    pub const CLIENT_CACHE_GET_STORE_READ_ERRORS: &str = "Client.CacheGetStoreReadErrors";
    /// Put errors (aggregate).
    pub const CLIENT_CACHE_PUT_ERRORS: &str = "Client.CachePutErrors";
    /// Async put rejected (queue full).
    pub const CLIENT_CACHE_PUT_ASYNC_REJECTION_ERRORS: &str = "Client.CachePutAsyncRejectionErrors";
    /// Put failed during eviction.
    pub const CLIENT_CACHE_PUT_EVICTION_ERRORS: &str = "Client.CachePutEvictionErrors";
    /// Benign racing put (page already present).
    pub const CLIENT_CACHE_PUT_BENIGN_RACING_ERRORS: &str = "Client.CachePutBenignRacingErrors";
    /// Put failed due to insufficient space.
    pub const CLIENT_CACHE_PUT_INSUFFICIENT_SPACE_ERRORS: &str =
        "Client.CachePutInsufficientSpaceErrors";
    /// Put attempted while not ready.
    pub const CLIENT_CACHE_PUT_NOT_READY_ERRORS: &str = "Client.CachePutNotReadyErrors";
    /// Store-delete errors during put.
    pub const CLIENT_CACHE_PUT_STORE_DELETE_ERRORS: &str = "Client.CachePutStoreDeleteErrors";
    /// Store-write errors during put.
    pub const CLIENT_CACHE_PUT_STORE_WRITE_ERRORS: &str = "Client.CachePutStoreWriteErrors";
    /// Store-write no-space errors during put.
    pub const CLIENT_CACHE_PUT_STORE_WRITE_NO_SPACE_ERRORS: &str =
        "Client.CachePutStoreWriteNoSpaceErrors";

    // ── Store timeouts / rejections ──────────────────────────
    /// Store delete timed out.
    pub const CLIENT_CACHE_STORE_DELETE_TIMEOUT: &str = "Client.CacheStoreDeleteTimeout";
    /// Store get timed out.
    pub const CLIENT_CACHE_STORE_GET_TIMEOUT: &str = "Client.CacheStoreGetTimeout";
    /// Store put timed out.
    pub const CLIENT_CACHE_STORE_PUT_TIMEOUT: &str = "Client.CacheStorePutTimeout";
    /// Store worker threads rejected.
    pub const CLIENT_CACHE_STORE_THREADS_REJECTED: &str = "Client.CacheStoreThreadsRejected";
}

use crate::metrics::{counter, gauge};

/// Recompute and publish the `Client.CacheHitRate` gauge (0..=100) from the
/// cumulative cache-hit vs. external-read byte counters.
///
/// Cheap (two atomic loads + a divide); called from the read hot paths so the
/// gauge stays fresh without a dedicated background task.
pub fn publish_hit_rate() {
    let hit = counter(name::CLIENT_CACHE_BYTES_READ_CACHE).get();
    let ext = counter(name::CLIENT_CACHE_BYTES_READ_EXTERNAL).get();
    let total = hit + ext;
    let rate = if total > 0 {
        hit.saturating_mul(100) / total
    } else {
        0
    };
    gauge(name::CLIENT_CACHE_HIT_RATE).set(rate);
}
