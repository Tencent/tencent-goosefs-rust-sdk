//! io_uring page-store backend.
//!
//! This module provides [`UringPageStore`] — an io_uring-backed implementation
//! of [`crate::cache::store::PageStore`] that eliminates the `spawn_blocking`
//! overhead of `LocalPageStore` (tokio::fs) on the cache-hit hot path.
//!
//! # Platform support
//!
//! io_uring is Linux-only (kernel ≥ 5.1). On other platforms the module
//! exposes only [`is_uring_available`] (which returns `false`), and
//! `LocalCacheManager` transparently falls back to `LocalPageStore`.
//!
//! # Disk layout
//!
//! Identical to `LocalPageStore`:
//! `<dir>/<page_size>/<bucket>/<file_id>/<page_index>`
//!
//! so both backends can read each other's files (cross-backend compatibility).
//!
//! See `docs/CLIENT_PAGE_CACHE_DESIGN.md`.

#[cfg(target_os = "linux")]
mod driver;
#[cfg(target_os = "linux")]
mod future;
#[cfg(target_os = "linux")]
mod requests;
#[cfg(target_os = "linux")]
mod store;
mod sys;

#[cfg(target_os = "linux")]
pub use store::UringPageStore;
pub use sys::is_uring_available;

/// Initialise the io_uring thread pool configuration from `CacheManagerOptions`.
///
/// On non-Linux platforms this is a no-op (the io_uring backend is unavailable).
#[cfg(target_os = "linux")]
pub fn init_uring_config(queue_depth: usize, thread_count: usize) {
    driver::init_uring_config(queue_depth, thread_count);
}

#[cfg(not(target_os = "linux"))]
#[allow(unused_variables)]
pub fn init_uring_config(queue_depth: usize, thread_count: usize) {}

use crate::error::Error;

/// Number of hash buckets — must match `LocalPageStore` for cross-backend
/// compatibility.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const NUM_BUCKETS: u64 = 1000;

/// xxHash3 64-bit hash of `file_id` for bucket selection.
///
/// **Must stay byte-for-byte identical to `LocalPageStore::hash_file_id`** —
/// the on-disk bucket directory is determined by this hash, so a mismatch
/// would orphan the entire cache when switching backends.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn hash_file_id(file_id: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(file_id.as_bytes())
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn io_error(message: impl Into<String>, e: std::io::Error) -> Error {
    Error::Internal {
        message: message.into(),
        source: Some(Box::new(e)),
    }
}
