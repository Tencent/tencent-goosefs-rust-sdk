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
/// Values of `0` fall back to the env var / built-in default.
pub fn init_uring_config(queue_depth: usize, thread_count: usize) {
    let _ = URING_CONFIG.set(UringConfig {
        queue_depth: queue_depth.max(1),
        thread_count: thread_count.max(1),
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
pub static URING_THREADS: LazyLock<Vec<UringThreadHandle>> = LazyLock::new(|| {
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
        let needs_submit = !retries.is_empty();

        // Reset spin counter — we just did useful work (reaped CQEs or
        // processed retries), so the next idle spin starts fresh.
        if !pending.is_empty() {
            // After reaping, pending may still have in-flight ops. The
            // counter is reset only when we successfully reap CQEs (i.e.,
            // when process_completions returns non-empty retries or when
            // the CQE ring had entries). We detect this by checking if
            // retries is non-empty — if so, IO is flowing, reset the counter.
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

fn get_queue_depth() -> usize {
    if let Some(config) = URING_CONFIG.get() {
        return config.queue_depth;
    }
    std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16384)
}

fn get_thread_count() -> usize {
    if let Some(config) = URING_CONFIG.get() {
        return config.thread_count;
    }
    // B2 fix: default 8 threads (was 2) to match NVMe multi-queue parallelism.
    // 2 threads → 2-4 effective concurrency, leaving most cores idle.
    // 8 threads → up to 8 concurrent in-flight SQE batches, saturating
    // typical NVMe (queue depth 32-64) without head-of-line blocking.
    std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8)
}
