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

//! Platform detection and io_uring availability probe.
//!
//! On non-Linux platforms `is_uring_available` always returns `false`,
//! causing `LocalCacheManager` to transparently fall back to
//! `LocalPageStore` (tokio::fs backend).
//!
//! References: Lance `uring.rs:32-35` — "only available on Linux and requires
//! kernel 5.1".
//!
//! See `docs/CLIENT_PAGE_CACHE_DESIGN.md` .

/// Detect whether io_uring is usable for the page-store backend.
///
/// Checks:
/// 1. `target_os == "linux"` (compile-time).
/// 2. `IoUring::new` succeeds (kernel ≥ 5.1, sysctl not fully disabled).
/// 3. A real create-mode `IORING_OP_OPENAT` (`O_WRONLY|O_CREAT|O_TRUNC`)
///    succeeds on a temp file. GitHub Actions runners often allow ring
///    setup and even read-only open of `/dev/null`, but deny create-mode
///    openat with `EPERM` — which is exactly what `UringPageStore::put`
///    needs. Without this probe the cache manager would select the uring
///    backend and every put would fail.
///
/// Result is cached for the process lifetime.
pub fn is_uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::sync::OnceLock;
        static AVAILABLE: OnceLock<bool> = OnceLock::new();
        *AVAILABLE.get_or_init(probe_uring_create_openat)
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Synchronous probe matching the `put` hot path: create a temp file via
/// io_uring `OPENAT` with `O_WRONLY | O_CREAT | O_TRUNC`.
#[cfg(target_os = "linux")]
fn probe_uring_create_openat() -> bool {
    use io_uring::{opcode, types, IoUring};
    use std::ffi::CString;
    use std::path::PathBuf;

    let mut ring = match IoUring::new(8) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "io_uring not available; falling back to tokio::fs backend"
            );
            return false;
        }
    };

    // Unique path under the process temp dir — same area `UringPageStore`
    // tests / production cache dirs use.
    let probe_path: PathBuf = std::env::temp_dir().join(format!(
        "gfs_uring_probe_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let path_cstr = match CString::new(probe_path.to_string_lossy().as_bytes()) {
        Ok(p) => p,
        Err(_) => return false,
    };

    let open_e = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_cstr.as_ptr())
        .flags(libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_CLOEXEC)
        .mode(0o644)
        .build()
        .user_data(1);

    // SAFETY: path_cstr lives until submit_and_wait returns; SQE is pushed once.
    unsafe {
        if ring.submission().push(&open_e).is_err() {
            tracing::warn!("io_uring OPENAT(create) probe: submission queue full");
            return false;
        }
    }

    if let Err(e) = ring.submit_and_wait(1) {
        tracing::warn!(
            error = %e,
            "io_uring OPENAT(create) probe: submit_and_wait failed; falling back"
        );
        let _ = std::fs::remove_file(&probe_path);
        return false;
    }

    let result = {
        let mut cq = ring.completion();
        match cq.next() {
            Some(cqe) => cqe.result(),
            None => {
                tracing::warn!("io_uring OPENAT(create) probe: no CQE");
                let _ = std::fs::remove_file(&probe_path);
                return false;
            }
        }
    };

    if result < 0 {
        let err = std::io::Error::from_raw_os_error(-result);
        tracing::warn!(
            error = %err,
            "io_uring OPENAT(create) probe failed (EPERM is common on GHA); \
             falling back to tokio::fs backend"
        );
        let _ = std::fs::remove_file(&probe_path);
        return false;
    }

    unsafe {
        libc::close(result);
    }
    let _ = std::fs::remove_file(&probe_path);

    tracing::info!("io_uring is available on this platform (OPENAT create probe ok)");
    true
}
