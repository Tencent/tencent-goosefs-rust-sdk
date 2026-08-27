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

//! Background io_uring thread pool + main loop.
//!
//! References: Lance `thread.rs:30-396`. The design is preserved (N dedicated
//! OS threads, each owning an `IoUring` instance, round-robin selection,
//! batched submit, short-read/short-write retry) but extended to handle write,
//! open, close, and unlink opcodes in addition to read.
//!
//! See `docs/CLIENT_PAGE_CACHE_DESIGN.md` .

use super::requests::{IoRequest, UringOpType};
use io_uring::{opcode, types, IoUring};
use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

/// Process-wide io_uring configuration, set once from `CacheManagerOptions`
/// before the thread pool initialises. Falls back to env vars if never set
/// (e.g. when the driver is used outside the cache manager).
static URING_CONFIG: OnceLock<UringConfig> = OnceLock::new();

struct UringConfig {
    queue_depth: usize,
    thread_count: usize,
}

/// Initialise the io_uring thread pool configuration from `CacheManagerOptions`.
///
/// Must be called before the first `submit_request` (i.e. before any store
/// operation). Subsequent calls are no-ops — the thread pool is process-global.
///
/// `0` means "not configured" and falls back to the env var / built-in default,
/// exactly as if this function had never been called.
///
/// It must not be clamped to 1 instead. `queue_depth` sizes both the
/// `sync_channel` feeding each uring thread and the io_uring SQ itself, and
/// submission uses `try_send`, which fails rather than blocks when the channel
/// is full. A depth of 1 therefore turns any concurrency — even a single `put`,
/// which issues three ops (openat, write, close) — into spurious `WouldBlock`
/// errors surfacing as failed cache writes.
pub fn init_uring_config(queue_depth: usize, thread_count: usize) {
    let _ = URING_CONFIG.set(UringConfig {
        queue_depth: resolve_or_default(queue_depth, default_queue_depth()),
        thread_count: resolve_or_default(thread_count, default_thread_count()),
    });
}

/// Handle to a background io_uring thread — holds the channel sender for
/// submitting requests.
struct UringThreadHandle {
    request_tx: SyncSender<Arc<IoRequest>>,
}

/// Global io_uring thread pool — process-level singleton, lazily initialised
/// on first access.
///
/// References: Lance `thread.rs:30-54`.
/// Not `pub`: the handle type is private, and every user (`submit_request`,
/// `try_submit_request`) lives in this module.
static URING_THREADS: LazyLock<Vec<UringThreadHandle>> = LazyLock::new(|| {
    let queue_depth = get_queue_depth();
    let thread_count = get_thread_count();

    let mut threads = Vec::with_capacity(thread_count);
    for i in 0..thread_count {
        let (tx, rx) = std::sync::mpsc::sync_channel(queue_depth);
        std::thread::Builder::new()
            .name(format!("gfs-uring-{i}"))
            .spawn(move || run_uring_thread(rx, queue_depth as u32, i))
            .expect("Failed to spawn io_uring thread");
        threads.push(UringThreadHandle { request_tx: tx });
    }
    tracing::info!(
        thread_count,
        queue_depth,
        "io_uring thread pool initialised for page cache"
    );
    threads
});

/// Round-robin thread selection counter.
static THREAD_SELECTOR: AtomicU64 = AtomicU64::new(0);

/// user_data generator — each SQE gets a unique ID for CQE matching.
static USER_DATA_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Default batch size for submission.
const DEFAULT_SUBMIT_BATCH_SIZE: usize = 128;

/// Default poll timeout when the channel is empty and no ops are in flight.
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_millis(1);

/// Try to submit a request without blocking. Returns `false` if the
/// channel is full or disconnected (the request is NOT marked failed —
/// the caller handles the fallback).
///
/// Uses `try_send` instead of `send` to avoid blocking tokio workers
/// when the channel is full (H1 fix).
pub fn try_submit_request(request: Arc<IoRequest>) -> bool {
    let thread_idx =
        (THREAD_SELECTOR.fetch_add(1, Ordering::Relaxed) as usize) % URING_THREADS.len();
    URING_THREADS[thread_idx]
        .request_tx
        .try_send(request)
        .is_ok()
}

/// Submit a request, marking it failed if the channel is full or
/// disconnected. The caller should await the [`UringOpFuture`] to
/// observe the error.
///
/// Uses `try_send` (non-blocking) instead of `send` (blocking) so that
/// a full channel degrades gracefully (returns miss) instead of hanging
/// the tokio worker (H1 fix).
///
/// References: Lance `reader.rs:183-238` `submit_read()`.
pub fn submit_request(request: Arc<IoRequest>) {
    let thread_idx =
        (THREAD_SELECTOR.fetch_add(1, Ordering::Relaxed) as usize) % URING_THREADS.len();
    match URING_THREADS[thread_idx]
        .request_tx
        .try_send(Arc::clone(&request))
    {
        Ok(()) => {}
        Err(std::sync::mpsc::TrySendError::Full(_)) => {
            request.fail(io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring submission channel full",
            ));
        }
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
            request.fail(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring thread died",
            ));
        }
    }
}

/// Background thread main loop.
///
/// References: Lance `thread.rs:117-250` `run_uring_thread()`.
///
/// # Loop design (Lance-style spin + CPU-aware yield)
///
/// The original loop called `ring.submit_and_wait(1)` whenever there were
/// in-flight ops. This blocks the worker thread until at least 1 CQE arrives
/// (~10 µs for NVMe), during which new channel requests queue up but are
/// NOT processed. Under high concurrency (128 threads), this creates a
/// serialization point: the effective concurrency per io_uring thread is 1,
/// not the SQ depth, causing P50 to double from 6.5ms to 12.9ms.
///
/// B1's first attempt removed `submit_and_wait(1)` entirely, replacing it
/// with a `continue` (pure busy-spin). Under high load this caused P99 to
/// balloon 5x because 8 uring threads busy-spinning consumed CPU cores that
/// tokio workers needed for query processing.
///
/// The current design mirrors Lance's approach (no `submit_and_wait(1)` at
/// all) but adds **CPU-aware yielding** to prevent starving tokio workers:
/// 1. **Non-blocking reap**: try to reap any available CQEs (no wait).
/// 2. **Non-blocking submit**: try to push pending channel requests as SQEs
///    and `ring.submit()` (no wait).
/// 3. **Only when idle**: if both channel AND in-flight set are empty,
///    fall back to `recv_timeout` (blocks up to 1ms for new requests).
/// 4. **Spin + yield (Lance-style)**: if only in-flight ops exist (no new
///    channel requests to process), use `spin_loop()` for low-latency CQE
///    reaping, with a periodic `yield_now()` every 32 iterations to let
///    tokio workers run. This prevents both the serialization of
///    `submit_and_wait(1)` and the CPU starvation of pure busy-spin.
///
/// Net effect: the worker thread continuously drains both the channel and
/// the CQE ring, maximising throughput under concurrent load. CQEs are
/// reaped immediately upon arrival (no blocking syscall), and tokio workers
/// get CPU time via periodic yields.
fn run_uring_thread(request_rx: Receiver<Arc<IoRequest>>, queue_depth: u32, thread_id: usize) {
    let mut ring = match IoUring::builder().build(queue_depth) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, thread_id, "failed to create io_uring; thread exiting");
            return;
        }
    };

    // user_data → IoRequest map for CQE matching.
    let mut pending: HashMap<u64, Arc<IoRequest>> = HashMap::with_capacity(queue_depth as usize);
    let submit_batch_size = DEFAULT_SUBMIT_BATCH_SIZE;

    // Per-thread spin counter for CPU-aware yielding. After 32 spin_loop
    // iterations (~100ns-1µs on modern x86), we yield_now() to let tokio
    // workers run. This prevents 8 uring threads from starving the tokio
    // runtime under high concurrency (128+ threads).
    //
    // The 32:1 spin:yield ratio was chosen because:
    // - 32 × spin_loop ≈ 100ns-1µs, shorter than NVMe IO latency (~10µs),
    //   so most CQEs arrive during the spin phase (zero-latency reap).
    // - yield_now() costs ~100-500ns (OS scheduler overhead), amortised
    //   over 32 spins → ~3-15ns per iteration overhead (negligible).
    // - With 8 uring threads, aggregate yield rate = 8/32 = 0.25 cores
    //   of yield overhead, leaving ~7.75 cores for productive IO spinning.
    thread_local! {
        static SPIN_COUNT: Cell<u32> = Cell::new(0);
    }

    loop {
        // ── Step 1: Reap ALL available CQEs (non-blocking) ──────────────
        // This is fast and bounded by SQ depth — at most queue_depth CQEs
        // can be reaped per iteration. We always do this first to free
        // up SQ slots before pushing new requests.
        let retries = process_completions(&mut ring, &mut pending);

        // Re-arm every short read/write. `process_completions` has already
        // removed these from `pending` and advanced `bytes_transferred`, so
        // without this loop the request is never driven again: no further CQE
        // can arrive for it and its waker is never called, leaving the caller
        // blocked until `URING_OP_TIMEOUT` fires. Any read whose page holds
        // fewer bytes than requested takes this path, which is the common case
        // at a file tail.
        let mut needs_submit = false;
        // NOTE: this loop has no unit test — it needs a live ring and a running
        // driver thread. Its regression guard is
        // `store::tests::uring_get_short_read_at_tail`, which is `#[ignore]`d
        // and therefore skipped by CI (GitHub Actions denies io_uring OPENAT
        // with EPERM); it only runs on a host with io_uring available. Dropping
        // `retries` here does not fail loudly — the request is already out of
        // `pending`, so no CQE can arrive and no waker fires, and the caller
        // simply blocks until `URING_OP_TIMEOUT`.
        for request in retries {
            match push_to_sq(&mut ring, &mut pending, request) {
                Ok(()) => needs_submit = true,
                // `push_to_sq` has already failed the request, so the caller
                // observes an error instead of hanging.
                Err(e) => {
                    tracing::error!(error = %e, "failed to resubmit short io_uring op")
                }
            }
        }

        // Reset spin counter — we just did useful work (reaped CQEs or
        // processed retries), so the next idle spin starts fresh.
        if !pending.is_empty() {
            // After reaping, pending may still have in-flight ops. Reset the
            // counter only when a retry was actually resubmitted above — that
            // means IO is flowing, so the next idle spin starts fresh.
            if needs_submit {
                SPIN_COUNT.with(|c| c.set(0));
            }
        }

        // ── Step 2: Try to receive from channel and push SQEs ──────────
        // Non-blocking `try_recv` while we have pending work; block briefly
        // only when both channel and in-flight set are empty.
        let mut batch_count = 0usize;
        let mut should_exit = false;
        loop {
            let request = if pending.is_empty() && batch_count == 0 {
                // Nothing in flight and nothing in batch → first recv can block.
                // But we cap the block at 1ms so we periodically check for CQEs
                // that might have completed on another thread (defensive).
                match request_rx.recv_timeout(DEFAULT_POLL_TIMEOUT) {
                    Ok(req) => Some(req),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        should_exit = true;
                        None
                    }
                }
            } else {
                // Either we have in-flight ops or we're building a batch —
                // never block the recv, just try.
                match request_rx.try_recv() {
                    Ok(req) => Some(req),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        should_exit = true;
                        None
                    }
                }
            };

            match request {
                Some(request) => {
                    if let Err(e) = push_to_sq(&mut ring, &mut pending, request) {
                        tracing::error!(error = %e, "failed to push to io_uring SQ");
                    } else {
                        batch_count += 1;
                    }
                    if batch_count >= submit_batch_size {
                        break;
                    }
                }
                None => break,
            }
        }

        if should_exit {
            if batch_count > 0 {
                let _ = ring.submit();
            }
            tracing::info!(thread_id, "io_uring thread shutting down");
            return;
        }

        // ── Step 3: Submit the batch (non-blocking) ─────────────────────
        if batch_count > 0 || needs_submit {
            if let Err(e) = ring.submit() {
                tracing::error!(error = %e, batch_count, "failed to submit io_uring batch");
            }
        }

        // ── Step 4: Spin + yield (Lance-style, replaces submit_and_wait(1))
        // When there are in-flight ops but no new channel requests, we
        // must wait for CQEs to arrive. Instead of blocking on
        // `submit_and_wait(1)` (which serialises the thread behind a
        // single CQE and prevents batching), we busy-spin with periodic
        // `yield_now()` to let tokio workers run.
        //
        // This mirrors Lance's `thread.rs` design: no `submit_and_wait(1)`
        // at all. The spin ensures CQEs are reaped with minimum latency
        // (~100ns vs ~10µs for submit_and_wait). The periodic yield
        // prevents the 8 uring threads from starving tokio workers
        // (which caused P99 to balloon 5x in the pure-spin attempt).
        //
        // See the concurrent uring analysis for the detailed rationale.
        if !pending.is_empty() && batch_count == 0 {
            let should_yield = SPIN_COUNT.with(|c| {
                let n = c.get().saturating_add(1);
                c.set(n);
                n % 32 == 0
            });
            if should_yield {
                // Every 32 spins (~100ns-1µs), yield to let tokio workers
                // run. This costs ~100-500ns but prevents CPU starvation.
                std::thread::yield_now();
            } else {
                // spin_loop hint: tells the CPU we're in a short-duration
                // spin loop (maps to PAUSE on x86, YIELD on ARM). Reduces
                // power consumption and improves hyper-threading efficiency
                // without giving up the core.
                std::hint::spin_loop();
            }
            continue;
        }

        // Either we have new work to process (batch_count > 0) or we just
        // reaped CQEs. Continue the loop to push more work and reap more
        // completions.
    }
}

/// Whether a read/write SQE may still point into `RequestState::buffer`.
///
/// A resubmitted short transfer must still own the buffer it was created with.
/// Once the result has been consumed, `Future::poll` moved the buffer out with
/// `mem::take`, leaving a zero-length `BytesMut`; `as_ptr().add(transferred)`
/// would then hand the kernel a pointer past the end of that empty allocation,
/// and the kernel — not us — performs the out-of-bounds write.
///
/// Split out from `push_to_sq` so it can be tested without an `IoUring`: the
/// dangerous path only opens up when a timed-out request is retried, which is
/// hard to provoke end-to-end but trivial to state as a predicate.
///
/// `transferred >= total` also covers the nonsensical "retry something already
/// complete" case, where `total - transferred` would be zero or would wrap.
fn buffer_usable_for_transfer(transferred: usize, total: usize, buffer_len: usize) -> bool {
    transferred < total && buffer_len >= total
}

/// Construct an SQE for the request and push it to the submission queue
/// (without calling `submit`).
///
/// Handles all operation types:
/// - `Read`   → `opcode::Read` (pread)
/// - `Write`  → `opcode::Write` (pwrite)
/// - `OpenAt` → `opcode::OpenAt`
/// - `Close`  → `opcode::Close`
/// - `UnlinkAt` → `opcode::UnlinkAt`
///
/// Short read/write retries adjust `offset + bytes_transferred`.
///
/// References: Lance `thread.rs:256-309` (Lance only handles Read).
fn push_to_sq(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, Arc<IoRequest>>,
    request: Arc<IoRequest>,
) -> io::Result<()> {
    let user_data = USER_DATA_COUNTER.fetch_add(1, Ordering::Relaxed);

    let sqe = match request.op_type {
        UringOpType::Read => {
            let (buf_ptr, read_offset, read_len) = {
                let state = request.state.lock().unwrap();
                let br = state.bytes_transferred;
                if !buffer_usable_for_transfer(br, request.length, state.buffer.len()) {
                    drop(state);
                    let msg = "io_uring read buffer no longer valid for retry";
                    request.fail(io::Error::other(msg));
                    return Err(io::Error::other(msg));
                }
                (
                    unsafe { state.buffer.as_ptr().add(br) as *mut u8 },
                    request.offset + br as u64,
                    (request.length - br) as u32,
                )
            };
            opcode::Read::new(types::Fd(request.fd), buf_ptr, read_len)
                .offset(read_offset)
                .build()
        }
        UringOpType::Write => {
            let (buf_ptr, write_offset, write_len) = {
                let state = request.state.lock().unwrap();
                let bt = state.bytes_transferred;
                if !buffer_usable_for_transfer(bt, request.length, state.buffer.len()) {
                    drop(state);
                    let msg = "io_uring write buffer no longer valid for retry";
                    request.fail(io::Error::other(msg));
                    return Err(io::Error::other(msg));
                }
                (
                    unsafe { state.buffer.as_ptr().add(bt) as *const u8 },
                    request.offset + bt as u64,
                    (request.length - bt) as u32,
                )
            };
            opcode::Write::new(types::Fd(request.fd), buf_ptr, write_len)
                .offset(write_offset)
                .build()
        }
        UringOpType::OpenAt => {
            let state = request.state.lock().unwrap();
            let path_ptr = state.buffer.as_ptr() as *const libc::c_char;
            opcode::OpenAt::new(types::Fd(request.fd), path_ptr)
                .flags(request.open_flags | libc::O_CLOEXEC)
                .mode(0o644)
                .build()
        }
        UringOpType::Close => opcode::Close::new(types::Fd(request.fd)).build(),
        UringOpType::UnlinkAt => {
            let state = request.state.lock().unwrap();
            let path_ptr = state.buffer.as_ptr() as *const libc::c_char;
            opcode::UnlinkAt::new(types::Fd(request.fd), path_ptr).build()
        }
    }
    .user_data(user_data);

    let mut sq = ring.submission();
    if sq.is_full() {
        drop(sq);
        request.fail(io::Error::new(
            io::ErrorKind::WouldBlock,
            "io_uring submission queue full",
        ));
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "io_uring submission queue full",
        ));
    }

    unsafe {
        if sq.push(&sqe).is_err() {
            drop(sq);
            request.fail(io::Error::other("Failed to push to SQ"));
            return Err(io::Error::other("Failed to push to SQ"));
        }
    }
    drop(sq);

    pending.insert(user_data, request);
    Ok(())
}

/// Reap all available CQEs, update `RequestState`, and wake futures.
///
/// Short reads/writes are collected into the returned `Vec` for resubmission
/// (the caller resubmits and then calls `ring.submit()`).
///
/// EOF on a read (result == 0) is treated as completion, not an error — this
/// matches `LocalPageStore::get` where a short read at the page tail returns
/// the bytes actually read.
///
/// References: Lance `thread.rs:324-396` `process_completions()`.
fn process_completions(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, Arc<IoRequest>>,
) -> Vec<Arc<IoRequest>> {
    let mut retries = Vec::new();

    for cqe in ring.completion() {
        let user_data = cqe.user_data();
        let result = cqe.result();

        let Some(request) = pending.remove(&user_data) else {
            tracing::warn!(user_data, "CQE for unknown user_data");
            continue;
        };

        let mut state = request.state.lock().unwrap();

        if result < 0 {
            // Kernel error.
            state.err = Some(io::Error::from_raw_os_error(-result));
            state.completed = true;
        } else {
            match request.op_type {
                UringOpType::Read => {
                    let n = result as usize;
                    if n == 0 {
                        // EOF — partial read complete (or 0-byte read).
                        let bytes_transferred = state.bytes_transferred;
                        state.buffer.truncate(bytes_transferred);
                        state.result_code = bytes_transferred as i32;
                        state.completed = true;
                    } else {
                        state.bytes_transferred += n;
                        if state.bytes_transferred >= request.length {
                            // Full read complete.
                            let bytes_transferred = state.bytes_transferred;
                            state.buffer.truncate(bytes_transferred);
                            state.result_code = bytes_transferred as i32;
                            state.completed = true;
                        } else {
                            // Short read — retry.
                            drop(state);
                            retries.push(request);
                            continue;
                        }
                    }
                }
                UringOpType::Write => {
                    let n = result as usize;
                    state.bytes_transferred += n;
                    if state.bytes_transferred >= request.length {
                        // Full write complete.
                        state.result_code = 0;
                        state.completed = true;
                    } else {
                        // Short write — retry.
                        drop(state);
                        retries.push(request);
                        continue;
                    }
                }
                UringOpType::OpenAt => {
                    // result is the fd.
                    state.result_code = result;
                    state.completed = true;
                }
                UringOpType::Close | UringOpType::UnlinkAt => {
                    state.result_code = 0;
                    state.completed = true;
                }
            }
        }

        // Wake the waiting future.
        if let Some(waker) = state.waker.take() {
            drop(state);
            waker.wake();
        }
    }

    retries
}

// ── Configuration ───────────────────────────────────────────

/// Built-in default io_uring SQ / channel depth, used when neither
/// `CacheManagerOptions` nor the env var specifies one.
fn default_queue_depth() -> usize {
    std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&d: &usize| d > 0)
        .unwrap_or(16384)
}

/// Built-in default uring thread count.
///
/// 8 threads (was 2) to match NVMe multi-queue parallelism: 2 threads gave only
/// 2-4 effective concurrency and left most cores idle, while 8 allows up to 8
/// concurrent in-flight SQE batches, saturating a typical NVMe (queue depth
/// 32-64) without head-of-line blocking.
fn default_thread_count() -> usize {
    std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&t: &usize| t > 0)
        .unwrap_or(8)
}

fn get_queue_depth() -> usize {
    if let Some(config) = URING_CONFIG.get() {
        return config.queue_depth;
    }
    default_queue_depth()
}

fn get_thread_count() -> usize {
    if let Some(config) = URING_CONFIG.get() {
        return config.thread_count;
    }
    default_thread_count()
}

/// Resolve a configured value against the built-in default.
///
/// Extracted so the `0 means unset` rule can be tested: `URING_CONFIG` is a
/// process-global `OnceLock`, so a test that actually called
/// `init_uring_config` would fix the value for every other test in the binary.
fn resolve_or_default(configured: usize, default: usize) -> usize {
    if configured == 0 {
        default
    } else {
        configured
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `0` must mean "unset", not "one".
    ///
    /// Regression test. `init_uring_config` used `queue_depth.max(1)` while its
    /// doc comment promised a fallback to the default, so `CacheManagerOptions`
    /// with `uring_queue_depth: 0` — which is what "leave it alone" looks like
    /// to a caller — silently produced a depth of 1.
    ///
    /// That is not a mild misconfiguration. `queue_depth` sizes both the
    /// `sync_channel` feeding each uring thread and the io_uring SQ, and
    /// `submit_request` uses `try_send`, which fails instead of blocking when
    /// the channel is full. At depth 1 a single `put` — three ops: openat,
    /// write, close — is enough to overflow it, so writes fail with
    /// `WouldBlock` and surface as failed cache puts.
    #[test]
    fn zero_means_unset_not_one() {
        assert_eq!(resolve_or_default(0, 16384), 16384, "0 must fall back");
        assert_eq!(resolve_or_default(0, 8), 8);
        // An explicit value is respected, including a small one.
        assert_eq!(resolve_or_default(1, 16384), 1);
        assert_eq!(resolve_or_default(64, 16384), 64);
        assert_eq!(resolve_or_default(16384, 16384), 16384);
    }

    /// The built-in defaults must be usable, i.e. deep enough that one `put`
    /// cannot fill the queue.
    ///
    /// Asserts a floor rather than the exact number so tuning the default does
    /// not break the test, while still catching a change to something
    /// degenerate.
    #[test]
    fn defaults_are_deep_enough_for_a_multi_op_put() {
        // A single `put` issues openat + write + close.
        const OPS_PER_PUT: usize = 3;
        let depth = default_queue_depth();
        assert!(
            depth >= OPS_PER_PUT * 16,
            "default queue depth {depth} leaves no headroom over the {OPS_PER_PUT} ops \
             a single put issues"
        );
        let threads = default_thread_count();
        assert!(threads >= 1, "default thread count must be at least 1");
    }

    /// An env var set to `0` must not defeat the default either.
    ///
    /// `parse().ok()` alone would accept `"0"` and hand back a depth of zero,
    /// which `IoUring::builder().build(0)` rejects outright — the uring threads
    /// would fail to start and every operation would fall back to a miss.
    #[test]
    fn env_var_zero_is_ignored() {
        // Exercises the same `.filter(|&d| d > 0)` guard the env path uses,
        // without mutating process-wide environment state mid-suite.
        let parsed: Option<usize> = "0".parse().ok();
        assert_eq!(parsed.filter(|&d: &usize| d > 0), None);
        let parsed: Option<usize> = "64".parse().ok();
        assert_eq!(parsed.filter(|&d: &usize| d > 0), Some(64));
    }

    // ── Short-transfer retry safety ──────────────────────────────────────
    //
    // `push_to_sq` needs a live `IoUring`, so these exercise the predicate it
    // consults rather than the function itself. That predicate is the whole of
    // the decision: everything after it is pointer arithmetic on a buffer the
    // predicate has just certified.

    /// A partially-filled buffer is exactly the case retries exist for, so it
    /// must be accepted — otherwise short reads fail instead of resuming.
    #[test]
    fn partially_transferred_buffer_is_still_usable() {
        assert!(buffer_usable_for_transfer(0, 4096, 4096), "fresh request");
        assert!(buffer_usable_for_transfer(1, 4096, 4096), "1 byte in");
        assert!(buffer_usable_for_transfer(4095, 4096, 4096), "1 byte left");
    }

    /// A buffer moved out by `Future::poll` (timeout, then a late CQE triggers
    /// a retry) is left zero-length. Retrying against it would make the kernel
    /// write past the end of an empty allocation, so it must be rejected.
    #[test]
    fn taken_buffer_is_rejected() {
        // `mem::take` leaves len 0 while `length` still says 4096.
        assert!(
            !buffer_usable_for_transfer(0, 4096, 0),
            "empty buffer must never be handed to the kernel"
        );
        // Partially transferred *and* taken — the timeout-then-retry shape.
        assert!(!buffer_usable_for_transfer(2048, 4096, 0));
        // Any buffer shorter than the declared length is equally unsafe.
        assert!(!buffer_usable_for_transfer(0, 4096, 4095));
    }

    /// Retrying an already-complete transfer would compute a zero (or
    /// wrapping) length, so it is rejected too.
    #[test]
    fn fully_transferred_request_is_not_retried() {
        assert!(
            !buffer_usable_for_transfer(4096, 4096, 4096),
            "exactly done"
        );
        assert!(
            !buffer_usable_for_transfer(5000, 4096, 4096),
            "over-transferred would wrap `length - transferred`"
        );
    }

    /// The guard must not fire on a rejected request before it has a chance to
    /// run: `fail` marks the request complete and wakes the waiter, which is
    /// how the caller learns of the error instead of blocking until
    /// `URING_OP_TIMEOUT`.
    #[test]
    fn failing_a_request_completes_it_for_the_waiter() {
        let request = Arc::new(IoRequest {
            fd: -1,
            offset: 0,
            length: 4096,
            op_type: UringOpType::Read,
            open_flags: 0,
            state: std::sync::Mutex::new(crate::cache::store::uring::requests::RequestState {
                completed: false,
                consumed: false,
                waker: None,
                err: None,
                buffer: bytes::BytesMut::new(),
                bytes_transferred: 0,
                result_code: 0,
            }),
        });

        request.fail(io::Error::other("buffer no longer valid"));

        let state = request.state.lock().unwrap();
        assert!(state.completed, "a failed request must be observable");
        assert!(state.err.is_some(), "the error must reach the caller");
    }
}
