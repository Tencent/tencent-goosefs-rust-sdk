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

//! Process-global SIGBUS diagnostic handler for the short-circuit mmap path.
//!
//! A SIGBUS while touching a block mapping means the underlying file was
//! truncated / replaced **while the Worker was supposed to be holding the block
//! lock** — i.e. INV-D1 (the immutability protocol, design §1.3 / §8.1) was
//! violated. There is no safe way to *recover* inside a signal handler: SIGBUS
//! fires at an arbitrary faulting instruction (e.g. deep in `memcpy`), and
//! unwinding from a signal handler through libc frames is UB. `catch_unwind`
//! cannot catch a signal either.
//!
//! Therefore the only correct response is to **fail loudly**: emit an
//! async-signal-safe diagnostic line to stderr and `abort()`. This turns a
//! protocol violation into an immediate, observable crash rather than silently
//! returning torn / stale bytes (which would break data consistency, §0a).
//!
//! Deployments that need to tolerate untrusted/mutable backing files should use
//! the `pread` data plane instead of mmap (design §11.4) rather than relying on
//! signal recovery.
//!
//! The handler is installed once per process (idempotent) and only on unix.

#[cfg(unix)]
mod imp {
    use std::sync::Once;

    static INSTALL: Once = Once::new();

    /// SA_SIGINFO handler: write a fixed diagnostic + the faulting address to
    /// stderr using only async-signal-safe calls, then `abort()`.
    ///
    /// # Safety
    /// Registered via `sigaction` for `SIGBUS`. Only async-signal-safe
    /// functions are used: `write(2)` and `abort(3)`. No allocation, no
    /// locking, no Rust formatting machinery.
    extern "C" fn handle_sigbus(
        _sig: libc::c_int,
        info: *mut libc::siginfo_t,
        _ctx: *mut libc::c_void,
    ) {
        const MSG: &[u8] =
            b"\n[goosefs-sc] FATAL: SIGBUS on a short-circuit block mmap. The backing block \
file was truncated/replaced while locked (INV-D1 violated). Aborting to avoid returning \
torn/stale data. Consider io.mode=pread for untrusted filesystems. fault_addr=0x";

        // SAFETY: async-signal-safe write to stderr (fd 2).
        unsafe {
            libc::write(2, MSG.as_ptr() as *const libc::c_void, MSG.len());

            // Best-effort: print the faulting address (si_addr) as hex. Reading
            // si_addr from siginfo is safe within the handler.
            if !info.is_null() {
                let addr = (*info).si_addr() as usize;
                let mut buf = [0u8; 16];
                let mut n = addr;
                // Render hex into buf from the right.
                let mut i = buf.len();
                if n == 0 {
                    i -= 1;
                    buf[i] = b'0';
                } else {
                    while n != 0 && i > 0 {
                        i -= 1;
                        let d = (n & 0xf) as u8;
                        buf[i] = if d < 10 { b'0' + d } else { b'a' + (d - 10) };
                        n >>= 4;
                    }
                }
                libc::write(2, buf[i..].as_ptr() as *const libc::c_void, buf.len() - i);
            }
            libc::write(2, b"\n".as_ptr() as *const libc::c_void, 1);

            // `abort()` is async-signal-safe and produces a core dump.
            libc::abort();
        }
    }

    /// Install the SIGBUS handler once per process. Idempotent.
    ///
    /// If `sigaction(2)` itself fails (extremely unlikely for SIGBUS on a
    /// well-formed `struct sigaction`), we emit a single warning line to
    /// stderr via `write(2)` and continue. The process keeps the kernel's
    /// default SIGBUS disposition (terminate + core), which is still safe
    /// w.r.t. data correctness — we just lose the targeted INV-D1 diagnostic
    /// message. We deliberately do NOT panic / abort here: failing to install
    /// a *diagnostic* aid must not itself take down the process.
    pub fn install() {
        INSTALL.call_once(|| {
            // SAFETY: standard one-time `sigaction` registration. We use
            // SA_SIGINFO so the handler receives `siginfo_t` (faulting addr).
            unsafe {
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = handle_sigbus as usize;
                action.sa_flags = libc::SA_SIGINFO;
                libc::sigemptyset(&mut action.sa_mask);
                let rc = libc::sigaction(libc::SIGBUS, &action, std::ptr::null_mut());
                if rc != 0 {
                    // Read errno *immediately* before any other libc call can
                    // clobber it. We don't format it (keeps the warning path
                    // allocation-free and trivially auditable); operators can
                    // correlate with strace if needed.
                    const WARN: &[u8] =
                        b"[goosefs-sc] WARN: sigaction(SIGBUS) registration failed; \
short-circuit mmap will rely on the kernel's default SIGBUS disposition \
(terminate). Data correctness is unaffected; only the targeted INV-D1 \
diagnostic message is lost.\n";
                    libc::write(2, WARN.as_ptr() as *const libc::c_void, WARN.len());
                }
            }
        });
    }
}

#[cfg(not(unix))]
mod imp {
    pub fn install() {}
}

/// Install the process-global SIGBUS diagnostic handler if `enabled`.
///
/// Idempotent and cheap to call repeatedly (e.g. once per
/// [`crate::block::short_circuit::ShortCircuitFactory`] construction). No-op on
/// non-unix targets or when `enabled` is `false`.
pub fn install_if_enabled(enabled: bool) {
    if enabled {
        imp::install();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `install_if_enabled(false)` must be a no-op and never panic; calling it
    /// repeatedly is safe. (We avoid asserting on the actual handler to not
    /// globally replace the process SIGBUS disposition during the test run.)
    #[test]
    fn install_disabled_is_noop() {
        install_if_enabled(false);
        install_if_enabled(false);
    }
}
