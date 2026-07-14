//! Platform detection and io_uring availability probe.
//!
//! On non-Linux platforms `is_uring_available` always returns `false`,
//! causing `LocalCacheManager` to transparently fall back to
//! `LocalPageStore` (tokio::fs backend).
//!
//! References: Lance `uring.rs:32-35` — "only available on Linux and requires
//! kernel 5.1".
//!
//! See `docs/CLIENT_PAGE_CACHE_IO_URING_DESIGN.md` §3.5.

/// Detect whether io_uring is available.
///
/// 1. `target_os == "linux"` (compile-time).
/// 2. Runtime probe: try to create a minimal io_uring instance.
/// 3. On failure returns `false` → caller falls back to `LocalPageStore`.
pub fn is_uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        match io_uring::IoUring::new(4) {
            Ok(_) => {
                tracing::info!("io_uring is available on this platform");
                true
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "io_uring not available; falling back to tokio::fs backend"
                );
                false
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}
