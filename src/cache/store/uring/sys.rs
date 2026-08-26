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
/// Checks, in order:
/// 1. `target_os == "linux"` (compile-time).
/// 2. `IoUring::new` succeeds (kernel ≥ 5.1, sysctl not fully disabled).
/// 3. Every opcode the page store actually issues works on a real temp file:
///    create-mode `OPENAT` (`O_RDWR|O_CREAT|O_TRUNC`), `WRITE`, `READ` and
///    `CLOSE`.
///
/// Step 3 covers all four because sandboxes filter io_uring **per opcode**, not
/// wholesale. Probing only `OPENAT` is not enough: GitHub Actions runners permit
/// ring setup and create-mode openat but deny `READ`, so a probe that stopped at
/// openat reported the backend as available and then every `get` failed with
/// `internal error: uring read`. The page cache is best-effort, so those
/// failures degrade silently to misses — the cache appears to do nothing, with
/// no error pointing at io_uring.
///
/// The same applies to any sandbox with a partial io_uring allowlist (container
/// seccomp profiles, gVisor, some Kubernetes runtimes), which is why this probes
/// the real operations rather than trusting a kernel version.
///
/// Result is cached for the process lifetime.
pub fn is_uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::sync::OnceLock;
        static AVAILABLE: OnceLock<bool> = OnceLock::new();
        *AVAILABLE.get_or_init(probe_uring_page_store_ops)
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Submit one SQE and wait for its CQE, returning the raw result.
///
/// `None` means the submission or completion machinery itself failed, which is
/// reported separately from an operation that completed with an error.
#[cfg(target_os = "linux")]
fn probe_submit_one(
    ring: &mut io_uring::IoUring,
    entry: &io_uring::squeue::Entry,
    what: &str,
) -> Option<i32> {
    // SAFETY: the caller keeps any buffer or path referenced by `entry` alive
    // until this function returns, and each entry is pushed exactly once.
    unsafe {
        if ring.submission().push(entry).is_err() {
            tracing::warn!(op = what, "io_uring probe: submission queue full");
            return None;
        }
    }
    if let Err(e) = ring.submit_and_wait(1) {
        tracing::warn!(op = what, error = %e, "io_uring probe: submit_and_wait failed");
        return None;
    }
    let mut cq = ring.completion();
    match cq.next() {
        Some(cqe) => Some(cqe.result()),
        None => {
            tracing::warn!(op = what, "io_uring probe: no CQE");
            None
        }
    }
}

/// Synchronous probe exercising the page store's full op set on a temp file:
/// `OPENAT` (create) → `WRITE` → `READ` → `CLOSE`.
#[cfg(target_os = "linux")]
fn probe_uring_page_store_ops() -> bool {
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

    // Cleans up on every exit path below.
    struct ProbeFile(PathBuf);
    impl Drop for ProbeFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = ProbeFile(probe_path.clone());

    // 1) OPENAT, create mode. O_RDWR rather than O_WRONLY so the same fd can
    //    serve the READ probe below.
    let open_e = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_cstr.as_ptr())
        .flags(libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_CLOEXEC)
        .mode(0o644)
        .build()
        .user_data(1);
    let fd = match probe_submit_one(&mut ring, &open_e, "openat") {
        Some(r) if r >= 0 => r,
        Some(r) => {
            let err = std::io::Error::from_raw_os_error(-r);
            tracing::warn!(
                error = %err,
                "io_uring OPENAT(create) probe failed (EPERM is common in sandboxes); \
                 falling back to tokio::fs backend"
            );
            return false;
        }
        None => return false,
    };

    // From here on the fd must be closed on every path. `libc::close` is used
    // for the failure paths because a denied CLOSE opcode would leak it.
    let payload = b"gfs-uring-probe";
    let mut readback = [0u8; 15];
    debug_assert_eq!(payload.len(), readback.len());

    // 2) WRITE — the `put` hot path.
    let write_e = opcode::Write::new(types::Fd(fd), payload.as_ptr(), payload.len() as u32)
        .offset(0)
        .build()
        .user_data(2);
    match probe_submit_one(&mut ring, &write_e, "write") {
        Some(r) if r == payload.len() as i32 => {}
        Some(r) => {
            if r < 0 {
                let err = std::io::Error::from_raw_os_error(-r);
                tracing::warn!(error = %err, "io_uring WRITE probe failed; falling back");
            } else {
                tracing::warn!(wrote = r, "io_uring WRITE probe short write; falling back");
            }
            unsafe { libc::close(fd) };
            return false;
        }
        None => {
            unsafe { libc::close(fd) };
            return false;
        }
    }

    // 3) READ — the `get` hot path, and the one GitHub Actions denies.
    let read_e = opcode::Read::new(types::Fd(fd), readback.as_mut_ptr(), readback.len() as u32)
        .offset(0)
        .build()
        .user_data(3);
    match probe_submit_one(&mut ring, &read_e, "read") {
        Some(r) if r == payload.len() as i32 => {}
        Some(r) => {
            if r < 0 {
                let err = std::io::Error::from_raw_os_error(-r);
                tracing::warn!(
                    error = %err,
                    "io_uring READ probe failed while OPENAT succeeded — this environment \
                     allows io_uring per-opcode (GitHub Actions does exactly this); \
                     falling back to tokio::fs backend"
                );
            } else {
                tracing::warn!(read = r, "io_uring READ probe short read; falling back");
            }
            unsafe { libc::close(fd) };
            return false;
        }
        None => {
            unsafe { libc::close(fd) };
            return false;
        }
    }

    if readback != *payload {
        tracing::warn!("io_uring READ probe returned wrong bytes; falling back");
        unsafe { libc::close(fd) };
        return false;
    }

    // 4) CLOSE. A failure here is not disqualifying on its own — the store's
    //    close path is fire-and-forget with a `libc::close` fallback — but it
    //    signals a partial allowlist, so report it and close by hand.
    let close_e = opcode::Close::new(types::Fd(fd)).build().user_data(4);
    match probe_submit_one(&mut ring, &close_e, "close") {
        Some(0) => {}
        Some(r) => {
            let err = std::io::Error::from_raw_os_error(-r);
            tracing::warn!(
                error = %err,
                "io_uring CLOSE probe failed; the store falls back to libc::close, so \
                 continuing with the uring backend"
            );
            unsafe { libc::close(fd) };
        }
        None => {
            unsafe { libc::close(fd) };
        }
    }

    tracing::info!("io_uring is available (OPENAT / WRITE / READ / CLOSE probe ok)");
    true
}
