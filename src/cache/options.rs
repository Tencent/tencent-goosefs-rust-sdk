//! Resolved page-cache options.
//!
//! [`CacheManagerOptions`] is the validated, ready-to-use view of the
//! `client_cache_*` fields on [`crate::config::GoosefsConfig`]. It mirrors
//! Java `CacheManagerOptions`, which is likewise derived from configuration.

use std::path::PathBuf;
use std::time::Duration;

use crate::config::{CacheEvictorType, GoosefsConfig};

/// Fraction of a directory's raw capacity reserved for filesystem/metadata
/// overhead, matching Java `PageStoreType.LOCAL` (5%).
const LOCAL_STORE_OVERHEAD: f64 = 0.05;

/// Validated options for a local cache manager.
#[derive(Debug, Clone)]
pub struct CacheManagerOptions {
    /// Page size in bytes (always > 0).
    pub page_size: u64,
    /// Usable per-directory capacity in bytes (after overhead reservation).
    pub dir_capacity: u64,
    /// Cache directories.
    pub dirs: Vec<PathBuf>,
    /// Eviction policy.
    pub evictor: CacheEvictorType,
    /// Whether async write-back is enabled.
    pub async_write_enabled: bool,
    /// Async write-back concurrency (always ≥ 1).
    pub async_write_threads: usize,
    /// Whether per-scope quota is enabled.
    pub quota_enabled: bool,
    /// Page TTL; `None` means no expiry.
    pub ttl: Option<Duration>,
    /// Whether to use the io_uring page-store backend (Linux 5.1+).
    /// When `false` or unavailable, falls back to `LocalPageStore` (tokio::fs).
    pub uring_enabled: bool,
    /// io_uring SQ/CQ queue depth (0 = use driver default of 16384).
    pub uring_queue_depth: usize,
    /// io_uring background thread count (0 = use driver default of 2).
    pub uring_thread_count: usize,
}

impl CacheManagerOptions {
    /// Build options from a [`GoosefsConfig`].
    ///
    /// Sanitizes user input:
    /// - `page_size` falls back to 1 MiB if `0`,
    /// - `async_write_threads` is clamped to at least `1`,
    /// - the usable per-directory capacity reserves [`LOCAL_STORE_OVERHEAD`],
    /// - `ttl_secs == 0` maps to `None` (no expiry).
    pub fn from_config(config: &GoosefsConfig) -> Self {
        let page_size = if config.client_cache_page_size == 0 {
            1024 * 1024
        } else {
            config.client_cache_page_size
        };

        let dir_capacity = (config.client_cache_size as f64 * (1.0 - LOCAL_STORE_OVERHEAD)) as u64;

        let dirs = config.client_cache_dirs.iter().map(PathBuf::from).collect();

        let ttl = if config.client_cache_ttl_secs == 0 {
            None
        } else {
            Some(Duration::from_secs(config.client_cache_ttl_secs))
        };

        Self {
            page_size,
            dir_capacity,
            dirs,
            evictor: config.client_cache_evictor,
            async_write_enabled: config.client_cache_async_write_enabled,
            async_write_threads: config.client_cache_async_write_threads.max(1),
            quota_enabled: config.client_cache_quota_enabled,
            ttl,
            uring_enabled: config.client_cache_uring_enabled,
            uring_queue_depth: config.client_cache_uring_queue_depth,
            uring_thread_count: config.client_cache_uring_thread_count,
        }
    }

    /// Maximum number of full pages that fit in a single directory.
    pub fn pages_per_dir(&self) -> u64 {
        if self.page_size == 0 {
            0
        } else {
            self.dir_capacity / self.page_size
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_defaults() {
        let cfg = GoosefsConfig::default();
        let opts = CacheManagerOptions::from_config(&cfg);

        assert_eq!(opts.page_size, 1024 * 1024);
        // 1 GiB * 0.95
        assert_eq!(opts.dir_capacity, (1024.0 * 1024.0 * 1024.0 * 0.95) as u64);
        assert_eq!(opts.dirs.len(), 1);
        assert_eq!(opts.evictor, CacheEvictorType::Lfu);
        assert!(opts.async_write_enabled);
        assert_eq!(opts.async_write_threads, 16);
        assert!(!opts.quota_enabled);
        assert!(opts.ttl.is_none());
    }

    #[test]
    fn sanitizes_zero_page_size_and_threads() {
        let mut cfg = GoosefsConfig::default();
        cfg.client_cache_page_size = 0;
        cfg.client_cache_async_write_threads = 0;
        let opts = CacheManagerOptions::from_config(&cfg);
        assert_eq!(opts.page_size, 1024 * 1024);
        assert_eq!(opts.async_write_threads, 1);
    }

    #[test]
    fn ttl_zero_is_none() {
        let mut cfg = GoosefsConfig::default();
        cfg.client_cache_ttl_secs = 0;
        assert!(CacheManagerOptions::from_config(&cfg).ttl.is_none());
        cfg.client_cache_ttl_secs = 30;
        assert_eq!(
            CacheManagerOptions::from_config(&cfg).ttl,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn pages_per_dir_computation() {
        let mut cfg = GoosefsConfig::default();
        cfg.client_cache_page_size = 1024 * 1024;
        cfg.client_cache_size = 10 * 1024 * 1024; // 10 MiB raw
        let opts = CacheManagerOptions::from_config(&cfg);
        // usable = 10 MiB * 0.95 = 9.5 MiB → 9 full 1 MiB pages
        assert_eq!(opts.pages_per_dir(), 9);
    }
}
