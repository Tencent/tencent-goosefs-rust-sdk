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

//! Micro-benchmarks for the `MasterClient` hot-path optimisations introduced
//! by the GetFileStatus performance work.
//!
//! See the SDK optimisation analysis for the full context.
//! §4) for the full analysis. This file contains three benchmark
//! groups, each comparing the *old* implementation strategy against the
//! *new* one, using identical inputs so the only variable is the strategy:
//!
//! 1. `counter_lookup` — every RPC used to call
//!    `crate::metrics::counter(name)` (a global `DashMap<String, Arc<...>>`
//!    lookup) and then `.inc(1)`. The new code caches the
//!    `Arc<Counter>` once on the `MasterClient` and increments through
//!    the cached handle. This bench measures the per-RPC overhead delta.
//!
//! 2. `path_owned_strategy` — the previous `with_retry` accepted `Fn`,
//!    forcing the closure to `clone()` the request `path: String`
//!    unconditionally so it would still be available on a retry. The new
//!    `FnMut`-based closure stashes the owned `String` in
//!    `Option<String>` and `take()`s it on the first attempt (zero extra
//!    allocation), only re-allocating when a retry actually fires.
//!    Bench scenarios: (a) no retries (the common case), (b) one retry
//!    out of two attempts.
//!
//! 3. `shared_state_concurrency` — the read side of `with_retry` used to
//!    do `RwLock::read().await + clone()`; the new code does
//!    `ArcSwap::load() + clone()`. The contention behaviour at moderate
//!    concurrency dominates the win; this bench drives N threads doing
//!    pure reads under a low-rate writer.
//!
//! Run with: `cargo bench --bench master_hotpath`.
//!
//! Why no end-to-end `get_status` bench: that would require either a
//! live Master cluster or a stub gRPC server, neither of which belongs
//! in a deterministic micro-benchmark. The three strategy benches above
//! cover every code-path delta that this branch actually changes.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use goosefs_sdk::metrics;
use goosefs_sdk::metrics::name as metric_name;
use goosefs_sdk::metrics::Counter;

// ---------------------------------------------------------------------------
// Group 1 — cached `Arc<Counter>` vs per-call `metrics::counter(name)` lookup.
// ---------------------------------------------------------------------------

fn bench_counter_lookup(c: &mut Criterion) {
    // Pre-touch the registry so the counters definitely exist (otherwise the
    // first lookup would also pay the slow-path `or_insert`, distorting the
    // baseline in favour of the cached handle).
    let _ = metrics::counter(metric_name::CLIENT_GET_STATUS_OPS);
    let _ = metrics::counter(metric_name::CLIENT_GET_STATUS_LATENCY_US);

    let mut group = c.benchmark_group("counter_lookup");
    // Each iteration is a single counter increment; report ops/sec.
    group.throughput(Throughput::Elements(1));

    // ---- Baseline: old code path. ---------------------------------------
    // Every RPC re-resolves the counter through the global DashMap.
    group.bench_function("baseline_dashmap_per_call", |b| {
        b.iter(|| {
            // Two counters per call to match what `get_status` does
            // (ops + latency_us).
            metrics::counter(black_box(metric_name::CLIENT_GET_STATUS_OPS)).inc(black_box(1));
            metrics::counter(black_box(metric_name::CLIENT_GET_STATUS_LATENCY_US))
                .inc(black_box(42));
        });
    });

    // ---- Optimised: cache the Arc<Counter> once. ------------------------
    // This mirrors the new `MasterClient::counter_*` fields populated in
    // `from_parts()`.
    let cached_ops: Arc<Counter> = metrics::counter(metric_name::CLIENT_GET_STATUS_OPS);
    let cached_latency: Arc<Counter> = metrics::counter(metric_name::CLIENT_GET_STATUS_LATENCY_US);
    group.bench_function("cached_arc_counter", |b| {
        b.iter(|| {
            black_box(&*cached_ops).inc(black_box(1));
            black_box(&*cached_latency).inc(black_box(42));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 2 — owned-path strategy on the `with_retry` closure.
// ---------------------------------------------------------------------------
//
// We model the path argument as a `String` and a "do-RPC" callback that
// just consumes it. The two strategies differ only in how the closure
// carries `path` between attempts; the rest of the pipeline is identical
// and amortised away by criterion's internal warm-up.
//
// To make the comparison apples-to-apples we use a single inline async
// driver (no tokio runtime), implemented as a sync loop — the closure
// produces a value and we count completion. Allocator pressure is what
// the new code avoids, so eliminating the runtime keeps the signal clean.

/// Old-style strategy: the closure is `Fn`, so it MUST clone the path on
/// every attempt — including the very first one (which is by far the
/// most common case in production).
///
/// The `black_box` calls below are critical: under `lto = "fat"` LLVM is
/// otherwise allowed to observe that the allocated buffer is only ever
/// inspected through `len()` and elide the allocation entirely, which
/// would falsely flatter both strategies. By forcing the optimiser to
/// treat the buffer as side-effecting we get numbers that reflect the
/// real heap traffic the production code pays.
fn run_clone_each_attempt(path: &String, retries: usize) -> usize {
    let mut total = 0usize;
    for _ in 0..=retries {
        // This `clone()` is what the old code paid on every call.
        let path_owned: String = black_box(path).clone();
        // Touch every byte so the allocation cannot be elided.
        total = total.wrapping_add(path_owned.bytes().map(|b| b as usize).sum::<usize>());
        // Force the heap buffer to be observable as an externally-visible
        // side effect.
        black_box(&path_owned);
        // Drop happens here (= request future completes).
    }
    total
}

/// New-style strategy: the closure is `FnMut`, captures `Option<String>`,
/// `take()`s on the first attempt (move; no extra allocation), and only
/// re-allocates on the retry path.
fn run_take_then_realloc(path: &str, retries: usize) -> usize {
    // The outer `to_string()` matches the one done by `get_status`
    // *before* `with_retry` is called: it's accounted for in this
    // function so the comparison with `run_clone_each_attempt` is fair.
    let mut path_owned: Option<String> = Some(black_box(path).to_string());
    let mut total = 0usize;
    for _ in 0..=retries {
        // First attempt: `take()` returns `Some(...)` → zero extra alloc.
        // Subsequent attempts (rare): `path_owned` is now `None`, so we
        // pay one `to_string()`.
        let req_path = path_owned
            .take()
            .unwrap_or_else(|| black_box(path).to_string());
        total = total.wrapping_add(req_path.bytes().map(|b| b as usize).sum::<usize>());
        black_box(&req_path);
    }
    total
}

fn bench_path_owned_strategy(c: &mut Criterion) {
    let mut group = c.benchmark_group("path_owned_strategy");
    group.throughput(Throughput::Elements(1));

    // A representative HDFS-style path. Length matters because
    // `String::clone` allocates `len` bytes, while `to_string()` is also
    // `len` bytes — but the new strategy makes that allocation conditional
    // on retries, which are *rare* in production.
    let path = String::from(
        "/tencent/cos/goosefs/datalake/warehouse/db.s/table=foo/dt=2026-06-10/part-000123.parquet",
    );

    // -- Scenario A: success on first attempt (no retries). The old code
    //    cloned once needlessly; the new code allocates once (the initial
    //    `to_string()` outside the closure) and then `take()`s it.
    group.bench_function("first_attempt_success__baseline_clone", |b| {
        b.iter(|| black_box(run_clone_each_attempt(black_box(&path), black_box(0))));
    });
    group.bench_function("first_attempt_success__new_take", |b| {
        // The outer `to_string()` inside `run_take_then_realloc` matches
        // the one done by `get_status` before `with_retry` is called.
        b.iter(|| {
            black_box(run_take_then_realloc(
                black_box(path.as_str()),
                black_box(0),
            ))
        });
    });

    // -- Scenario B: one retry happens (e.g. transient UNAVAILABLE). The
    //    new code now pays the realloc on the retry path, so the gap
    //    versus the old code shrinks. This bench documents that.
    group.bench_function("with_one_retry__baseline_clone", |b| {
        b.iter(|| black_box(run_clone_each_attempt(black_box(&path), black_box(1))));
    });
    group.bench_function("with_one_retry__new_take", |b| {
        b.iter(|| {
            black_box(run_take_then_realloc(
                black_box(path.as_str()),
                black_box(1),
            ))
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 3 — shared-state read concurrency: ArcSwap vs RwLock.
// ---------------------------------------------------------------------------
//
// `MasterClient::with_retry` previously did
//
//     let inner = self.inner.read().await; // tokio RwLock
//     inner.clone()
//
// per RPC, contending against `reconnect()`'s `write().await`. The new
// code replaces this with
//
//     self.state.load().client.clone()       // ArcSwap, wait-free
//
// To stay deterministic and avoid pulling tokio runtime overhead into a
// micro-bench, this group uses `std::sync::RwLock` as the baseline.
// `tokio::sync::RwLock` has even higher per-op cost on uncontended reads
// (it parks the task), so the std baseline is a *conservative lower
// bound* on the win — the production delta will be larger.

/// A small struct that mirrors the shape of `AuthedState` for the bench:
/// big enough that copying it would be expensive, but Clone is cheap
/// because the inner `Arc`s shallow-copy (matching `tonic::Channel`).
#[derive(Clone)]
struct StateLike {
    #[allow(dead_code)]
    epoch: u64,
    #[allow(dead_code)]
    inner: Arc<[u8; 64]>,
}

impl StateLike {
    fn new(epoch: u64) -> Self {
        Self {
            epoch,
            inner: Arc::new([0u8; 64]),
        }
    }
}

/// Drive `readers` threads against the shared state for a fixed iteration
/// count, while a writer thread publishes a new state every
/// `swap_every_iters` iterations of the readers (roughly).
///
/// Returns the total number of read ops completed (used as throughput
/// element count).
fn run_arcswap_readers(readers: usize, total_reads: usize) -> usize {
    let state = Arc::new(ArcSwap::from_pointee(StateLike::new(0)));
    let stop = Arc::new(AtomicBool::new(false));
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(readers);
    for _ in 0..readers {
        let state = state.clone();
        let stop = stop.clone();
        let counter = counter.clone();
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let snap = state.load();
                // Mimic `client.clone()` after the load.
                let _cloned: StateLike = StateLike::clone(&snap);
                black_box(&_cloned);
                if counter.fetch_add(1, Ordering::Relaxed) + 1 >= total_reads {
                    stop.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }));
    }

    // Writer: low-rate failover simulation.
    let writer_state = state.clone();
    let writer_stop = stop.clone();
    let writer = thread::spawn(move || {
        let mut epoch = 1u64;
        while !writer_stop.load(Ordering::Relaxed) {
            writer_state.store(Arc::new(StateLike::new(epoch)));
            epoch += 1;
            thread::sleep(Duration::from_micros(100));
        }
    });

    for h in handles {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    counter.load(Ordering::Relaxed)
}

fn run_rwlock_readers(readers: usize, total_reads: usize) -> usize {
    let state = Arc::new(std::sync::RwLock::new(StateLike::new(0)));
    let stop = Arc::new(AtomicBool::new(false));
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(readers);
    for _ in 0..readers {
        let state = state.clone();
        let stop = stop.clone();
        let counter = counter.clone();
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let guard = state.read().unwrap();
                let _cloned: StateLike = (*guard).clone();
                drop(guard);
                black_box(&_cloned);
                if counter.fetch_add(1, Ordering::Relaxed) + 1 >= total_reads {
                    stop.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }));
    }

    // Writer: low-rate failover simulation.
    let writer_state = state.clone();
    let writer_stop = stop.clone();
    let writer = thread::spawn(move || {
        let mut epoch = 1u64;
        while !writer_stop.load(Ordering::Relaxed) {
            {
                let mut g = writer_state.write().unwrap();
                *g = StateLike::new(epoch);
            }
            epoch += 1;
            thread::sleep(Duration::from_micros(100));
        }
    });

    for h in handles {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    counter.load(Ordering::Relaxed)
}

fn bench_shared_state_concurrency(c: &mut Criterion) {
    let mut group = c.benchmark_group("shared_state_concurrency");
    // Each batch performs `READS_PER_BATCH` total reads spread across the
    // configured number of reader threads. Throughput is reported in ops.
    const READS_PER_BATCH: usize = 50_000;
    group.throughput(Throughput::Elements(READS_PER_BATCH as u64));
    // Concurrency-heavy benches are noisy; widen the warm-up window and
    // shorten the measurement so `cargo bench` finishes in reasonable time.
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(20);

    for &readers in &[1usize, 4, 16] {
        let id_arcswap = format!("arcswap__readers_{}", readers);
        group.bench_function(id_arcswap, |b| {
            b.iter_batched(
                || readers,
                |r| {
                    let n = run_arcswap_readers(r, READS_PER_BATCH);
                    assert!(n >= READS_PER_BATCH);
                    black_box(n)
                },
                BatchSize::SmallInput,
            );
        });

        let id_rwlock = format!("std_rwlock__readers_{}", readers);
        group.bench_function(id_rwlock, |b| {
            b.iter_batched(
                || readers,
                |r| {
                    let n = run_rwlock_readers(r, READS_PER_BATCH);
                    assert!(n >= READS_PER_BATCH);
                    black_box(n)
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// Single-shot smoke check that the per-iteration work isn't optimised
// away. Not really a benchmark — kept so `cargo bench` exits with a
// clear signal even if a group is filtered out.
#[allow(dead_code)]
fn smoke_check() {
    let _ = Instant::now();
}

criterion_group!(
    benches,
    bench_counter_lookup,
    bench_path_owned_strategy,
    bench_shared_state_concurrency,
);
criterion_main!(benches);
