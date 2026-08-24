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

//! Cache evictor A/B benchmark, **local-only** (no GooseFS cluster required).
//!
//! Two phases, both driven through the real `LocalCacheManager` so the numbers
//! include the manager's locking and PageStore IO rather than a microbenchmark
//! of the eviction container alone:
//!
//! 1. **Hit path** — concurrent `get()` (metadata lookup + evictor `on_access` +
//!    PageStore read) with capacity headroom so nothing is evicted. Measures
//!    what the evictor costs a cache hit.
//! 2. **Eviction path** — `put()` against a directory already at quota, so every
//!    put evicts, swept across page counts. This is the phase that validates the
//!    foyer migration: the moka backend picks victims with an
//!    `iter().min_by_key()` scan, so its cost grows with the page count, while
//!    foyer's intrusive lists make it flat. See
//!    `docs/FOYER_SSD_CACHE_MIGRATION.md`.
//!
//! Both phases run against each evictor backend
//! ([`CacheEvictorBackend`]) and each policy (LRU / LFU).
//!
//! ## Usage
//! ```bash
//! cargo run --release --example cache_evictor_bench
//! ```
//!
//! ## Env knobs
//! - `BENCH_PAGE_SIZE` — page size in bytes (default 1024)
//! - `BENCH_NUM_PAGES` — number of pre-populated pages for phase 1 (default 1000)
//! - `BENCH_CONCURRENCY` — comma-separated concurrency levels (default "1,8,16,32")
//! - `BENCH_ITERS_PER_TASK` — iterations per concurrent task (default 10_000)
//! - `BENCH_EVICT_SCALE` — comma-separated page counts for phase 2
//!   (default "1000,10000,100000")
//! - `BENCH_EVICT_OPS` — evicting puts measured per page count (default 300)
//! - `BENCH_USE_URING` — use io_uring backend on Linux (default "1")
//!
//! ## Measured results (macOS, tokio::fs, 1 KiB pages)
//!
//! Phase 2, average cost of an evicting put:
//!
//! | Evictor | 1e3 pages | 1e4 pages | 1e5 pages | growth |
//! |---|---|---|---|---|
//! | foyer/lru | 937µs | 956µs | 838µs | 0.9x |
//! | foyer/lfu | 878µs | 775µs | 856µs | 1.0x |
//! | moka/lru | 1.01ms | 2.03ms | 22.87ms | 22.7x |
//! | moka/lfu | 1.23ms | 2.06ms | 22.30ms | 18.1x |
//!
//! The absolute numbers include writing the new page and deleting the victim,
//! which both backends pay; the difference is victim selection alone.
//!
//! Phase 1 is roughly equal across backends — `on_access` was already O(1) in
//! the moka evictor, so the migration was never expected to move the hit path.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use goosefs_sdk::cache::{CacheManager, CacheManagerOptions, LocalCacheManager, PageId};
use goosefs_sdk::config::{CacheEvictorBackend, CacheEvictorType};

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct BenchResult {
    label: String,
    concurrency: usize,
    ops_per_sec: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    avg_ns: u64,
}

async fn create_manager(
    backend: CacheEvictorBackend,
    evictor: CacheEvictorType,
    page_size: u64,
    capacity_pages: u64,
    use_uring: bool,
) -> (Arc<LocalCacheManager>, std::path::PathBuf) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let label = format!("{backend:?}_{evictor:?}").to_lowercase();
    let dir = std::env::temp_dir().join(format!("gfs_evictor_bench_{label}_{ts}"));

    let options = CacheManagerOptions {
        page_size,
        dir_capacity: page_size * capacity_pages,
        dirs: vec![dir.clone()],
        evictor,
        evictor_backend: backend,
        async_write_enabled: false,
        async_write_threads: 1,
        quota_enabled: false,
        ttl: None,
        uring_enabled: use_uring,
        uring_queue_depth: 0,
        uring_thread_count: 0,
        sync_read_enabled: false,
    };

    let mgr = Arc::new(LocalCacheManager::create(options).await.unwrap());
    (mgr, dir)
}

/// Pre-populate `num_pages` pages under a single file.
///
/// The caller is expected to leave capacity headroom; a put may still be
/// refused if the policy evicts the page it just admitted, so failures are
/// counted rather than asserted.
async fn populate(mgr: &LocalCacheManager, file_id: &str, num_pages: u64, page_size: usize) -> u64 {
    let data = vec![0x42u8; page_size];
    let mut admitted = 0;
    for i in 0..num_pages {
        let id = PageId::new(file_id, i);
        if mgr.put(&id, Bytes::from(data.clone())).await {
            admitted += 1;
        }
    }
    admitted
}

/// Measure the cost of a `put()` into a directory that is already at quota, so
/// each one forces the policy to pick a victim.
///
/// `capacity_pages` is the working-set size being swept: the cost of victim
/// selection is what should — or should not — scale with it.
async fn bench_steady_state_evictions(
    backend: CacheEvictorBackend,
    evictor: CacheEvictorType,
    page_size: usize,
    capacity_pages: u64,
    ops: usize,
    use_uring: bool,
) -> BenchResult {
    let (mgr, dir) = create_manager(
        backend,
        evictor,
        page_size as u64,
        capacity_pages,
        use_uring,
    )
    .await;

    // Fill to quota so every measured put below has to evict.
    populate(&mgr, "evict-file", capacity_pages, page_size).await;

    let data = vec![0x42u8; page_size];
    let mut latencies: Vec<u64> = Vec::with_capacity(ops);
    let start = Instant::now();
    for i in 0..ops as u64 {
        let id = PageId::new("evict-file", capacity_pages + i);
        let op_start = Instant::now();
        let _ = mgr.put(&id, Bytes::from(data.clone())).await;
        latencies.push(op_start.elapsed().as_nanos() as u64);
    }
    let total = start.elapsed();

    drop(mgr);
    let _ = tokio::fs::remove_dir_all(&dir).await;

    latencies.sort_unstable();
    let n = latencies.len();
    BenchResult {
        label: format!("{backend:?}/{evictor:?}").to_lowercase(),
        concurrency: capacity_pages as usize,
        ops_per_sec: ops as f64 / total.as_secs_f64().max(1e-9),
        p50_ns: latencies[n / 2],
        p95_ns: latencies[n * 95 / 100],
        p99_ns: latencies[n * 99 / 100],
        avg_ns: latencies.iter().sum::<u64>() / n as u64,
    }
}

/// Run concurrent `get()` calls (all cache hits) and measure latency.
async fn bench_concurrent_gets(
    mgr: Arc<LocalCacheManager>,
    file_id: &str,
    num_pages: u64,
    page_size: usize,
    concurrency: usize,
    iters_per_task: usize,
    label: &str,
) -> BenchResult {
    // Warm-up: one read per page to fill any fd caches.
    let mut warmup_dst = vec![0u8; page_size];
    for i in 0..num_pages.min(32) {
        let _ = mgr.get(&PageId::new(file_id, i), 0, &mut warmup_dst).await;
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let mgr = Arc::clone(&mgr);
        let file_id = file_id.to_string();
        handles.push(tokio::spawn(async move {
            let mut dst = vec![0u8; page_size];
            let mut latencies: Vec<u64> = Vec::with_capacity(iters_per_task);
            for i in 0..iters_per_task {
                // Round-robin across pages — all cache hits.
                let page_idx = ((i + task_id) as u64) % num_pages;
                let id = PageId::new(file_id.as_str(), page_idx);
                let op_start = Instant::now();
                let n = mgr.get(&id, 0, &mut dst).await;
                debug_assert_eq!(n, page_size, "expected cache hit at page {page_idx}");
                latencies.push(op_start.elapsed().as_nanos() as u64);
            }
            latencies
        }));
    }

    let mut all_latencies: Vec<u64> = Vec::with_capacity(concurrency * iters_per_task);
    for h in handles {
        all_latencies.extend(h.await.unwrap());
    }
    let total = start.elapsed();

    all_latencies.sort_unstable();
    let n = all_latencies.len();
    let p50 = all_latencies[n / 2];
    let p95 = all_latencies[n * 95 / 100];
    let p99 = all_latencies[n * 99 / 100];
    let avg = all_latencies.iter().sum::<u64>() / n as u64;
    let total_ops = concurrency * iters_per_task;
    let ops_per_sec = total_ops as f64 / total.as_secs_f64().max(1e-9);

    BenchResult {
        label: label.to_string(),
        concurrency,
        ops_per_sec,
        p50_ns: p50,
        p95_ns: p95,
        p99_ns: p99,
        avg_ns: avg,
    }
}

/// Deterministic xorshift, so a run is reproducible without pulling in a RNG.
#[inline]
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn percentiles(mut lat: Vec<u64>) -> (u64, u64, u64, u64) {
    if lat.is_empty() {
        return (0, 0, 0, 0);
    }
    lat.sort_unstable();
    let n = lat.len();
    (
        lat[n / 2],
        lat[n * 95 / 100],
        lat[n * 99 / 100],
        lat.iter().sum::<u64>() / n as u64,
    )
}

/// Outcome of the mixed read/fill workload against a directory at quota.
struct RefillResult {
    label: String,
    concurrency: usize,
    /// Fraction of reads that hit, measured rather than assumed.
    hit_rate: f64,
    hit_p99_ns: u64,
    hit_avg_ns: u64,
    /// Fills are the misses: each one writes a page and evicts another.
    fill_p50_ns: u64,
    fill_p99_ns: u64,
    fill_avg_ns: u64,
    fills_per_sec: f64,
}

/// **The primary acceptance scenario.** A directory already at quota serving a
/// mostly-hit workload, where each miss fills a page and therefore evicts one.
///
/// This is where the moka evictor's victim scan showed up in production: a high
/// hit rate does not help, because it only controls how *often* the scan runs,
/// not what it costs.
#[allow(clippy::too_many_arguments)]
async fn bench_steady_state_refill(
    backend: CacheEvictorBackend,
    evictor: CacheEvictorType,
    page_size: usize,
    capacity_pages: u64,
    concurrency: usize,
    iters_per_task: usize,
    hit_percent: u64,
    use_uring: bool,
) -> RefillResult {
    let (mgr, dir) = create_manager(
        backend,
        evictor,
        page_size as u64,
        capacity_pages,
        use_uring,
    )
    .await;
    populate(&mgr, "refill-file", capacity_pages, page_size).await;

    // Reads must target pages that are actually resident, otherwise the
    // measured hit rate is set by the shard-level shortfall (§5.8) rather than
    // by the workload we asked for. Sweep once to learn the resident set; the
    // run evicts a small fraction of it, which shows up in `hit_rate`.
    let resident: Arc<Vec<u64>> = {
        let mut dst = vec![0u8; page_size];
        let mut ids = Vec::with_capacity(capacity_pages as usize);
        for i in 0..capacity_pages {
            if mgr.get(&PageId::new("refill-file", i), 0, &mut dst).await > 0 {
                ids.push(i);
            }
        }
        Arc::new(ids)
    };
    assert!(!resident.is_empty(), "refill bench populated nothing");

    // Hand each task a disjoint range of fresh page indices so fills never
    // collide (a colliding put is rejected as a benign race, not a fill).
    let per_task_fills = iters_per_task; // upper bound
    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let mgr = Arc::clone(&mgr);
        let resident = Arc::clone(&resident);
        let fill_base = capacity_pages + (task_id * per_task_fills) as u64;
        handles.push(tokio::spawn(async move {
            let mut dst = vec![0u8; page_size];
            let data = vec![0x42u8; page_size];
            let mut rng = 0x9E3779B97F4A7C15 ^ (task_id as u64 + 1);
            let mut hit_lat: Vec<u64> = Vec::new();
            let mut fill_lat: Vec<u64> = Vec::new();
            let (mut hits, mut reads) = (0u64, 0u64);
            let mut next_fill = fill_base;

            for _ in 0..iters_per_task {
                if next_rand(&mut rng) % 100 < hit_percent {
                    let idx = resident[(next_rand(&mut rng) % resident.len() as u64) as usize];
                    let op = Instant::now();
                    let n = mgr.get(&PageId::new("refill-file", idx), 0, &mut dst).await;
                    let elapsed = op.elapsed().as_nanos() as u64;
                    reads += 1;
                    if n > 0 {
                        hits += 1;
                        hit_lat.push(elapsed);
                    }
                } else {
                    let op = Instant::now();
                    let _ = mgr
                        .put(
                            &PageId::new("refill-file", next_fill),
                            Bytes::from(data.clone()),
                        )
                        .await;
                    fill_lat.push(op.elapsed().as_nanos() as u64);
                    next_fill += 1;
                }
            }
            (hit_lat, fill_lat, hits, reads)
        }));
    }

    let mut hit_lat = Vec::new();
    let mut fill_lat = Vec::new();
    let (mut hits, mut reads) = (0u64, 0u64);
    for h in handles {
        let (hl, fl, hi, rd) = h.await.unwrap();
        hit_lat.extend(hl);
        fill_lat.extend(fl);
        hits += hi;
        reads += rd;
    }
    let total = start.elapsed();

    drop(mgr);
    let _ = tokio::fs::remove_dir_all(&dir).await;

    let fills = fill_lat.len();
    let (_, _, hit_p99, hit_avg) = percentiles(hit_lat);
    let (fill_p50, _, fill_p99, fill_avg) = percentiles(fill_lat);

    RefillResult {
        label: format!("{backend:?}/{evictor:?}").to_lowercase(),
        concurrency,
        hit_rate: if reads == 0 {
            0.0
        } else {
            hits as f64 / reads as f64
        },
        hit_p99_ns: hit_p99,
        hit_avg_ns: hit_avg,
        fill_p50_ns: fill_p50,
        fill_p99_ns: fill_p99,
        fill_avg_ns: fill_avg,
        fills_per_sec: fills as f64 / total.as_secs_f64().max(1e-9),
    }
}

/// Cold start: fill an empty directory from `concurrency` tasks at once.
///
/// Checks that admission does not serialise — the failure mode the original
/// global-Mutex evictor had, where throughput collapsed as concurrency rose.
async fn bench_cold_fill(
    backend: CacheEvictorBackend,
    evictor: CacheEvictorType,
    page_size: usize,
    capacity_pages: u64,
    concurrency: usize,
    use_uring: bool,
) -> BenchResult {
    let (mgr, dir) = create_manager(
        backend,
        evictor,
        page_size as u64,
        capacity_pages,
        use_uring,
    )
    .await;

    let per_task = capacity_pages / concurrency as u64;
    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let mgr = Arc::clone(&mgr);
        let base = task_id as u64 * per_task;
        handles.push(tokio::spawn(async move {
            let data = vec![0x42u8; page_size];
            let mut lat = Vec::with_capacity(per_task as usize);
            for i in 0..per_task {
                let op = Instant::now();
                let _ = mgr
                    .put(
                        &PageId::new("cold-file", base + i),
                        Bytes::from(data.clone()),
                    )
                    .await;
                lat.push(op.elapsed().as_nanos() as u64);
            }
            lat
        }));
    }
    let mut lat = Vec::new();
    for h in handles {
        lat.extend(h.await.unwrap());
    }
    let total = start.elapsed();

    drop(mgr);
    let _ = tokio::fs::remove_dir_all(&dir).await;

    let ops = lat.len();
    let (p50, p95, p99, avg) = percentiles(lat);
    BenchResult {
        label: format!("{backend:?}/{evictor:?}").to_lowercase(),
        concurrency,
        ops_per_sec: ops as f64 / total.as_secs_f64().max(1e-9),
        p50_ns: p50,
        p95_ns: p95,
        p99_ns: p99,
        avg_ns: avg,
    }
}

fn fmt_us(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}

fn print_result(r: &BenchResult) {
    println!(
        "  {:<8} conc={:<3}  {:>10.0} ops/s  avg={:>8}  p50={:>8}  p95={:>8}  p99={:>8}",
        r.label,
        r.concurrency,
        r.ops_per_sec,
        fmt_us(r.avg_ns),
        fmt_us(r.p50_ns),
        fmt_us(r.p95_ns),
        fmt_us(r.p99_ns),
    );
}

#[tokio::main]
async fn main() {
    let page_size: usize = env_or("BENCH_PAGE_SIZE", 1024);
    let num_pages: u64 = env_or("BENCH_NUM_PAGES", 1000);
    let concurrency_str =
        std::env::var("BENCH_CONCURRENCY").unwrap_or_else(|_| "1,8,16,32".to_string());
    let concurrency_levels: Vec<usize> = concurrency_str
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let iters_per_task: usize = env_or("BENCH_ITERS_PER_TASK", 10_000);
    #[cfg(target_os = "linux")]
    let use_uring_str = std::env::var("BENCH_USE_URING").unwrap_or_else(|_| "1".to_string());
    #[cfg(target_os = "linux")]
    let use_uring = use_uring_str == "1" || use_uring_str == "true";
    #[cfg(not(target_os = "linux"))]
    let use_uring = false;

    let evict_scale_str =
        std::env::var("BENCH_EVICT_SCALE").unwrap_or_else(|_| "1000,10000,100000".to_string());
    let evict_scale: Vec<u64> = evict_scale_str
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let evict_ops: usize = env_or("BENCH_EVICT_OPS", 300);

    // Phases are selectable because a full N=1e5 sweep populates 100k pages per
    // (backend, policy) combination and takes tens of minutes.
    let phases = std::env::var("BENCH_PHASES").unwrap_or_else(|_| "1,2,3,4".to_string());
    let run_phase = |n: u32| phases.split(',').any(|p| p.trim() == n.to_string());

    let refill_conc_str =
        std::env::var("BENCH_REFILL_CONCURRENCY").unwrap_or_else(|_| "8,32".to_string());
    let refill_conc: Vec<usize> = refill_conc_str
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let refill_pages: u64 = env_or("BENCH_REFILL_PAGES", 100_000);
    let refill_iters: usize = env_or("BENCH_REFILL_ITERS", 5_000);
    let hit_percent: u64 = env_or("BENCH_HIT_PERCENT", 99);
    let cold_conc: usize = env_or("BENCH_COLD_CONCURRENCY", 8);
    let cold_pages: u64 = env_or("BENCH_COLD_PAGES", 100_000);

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Cache Evictor Benchmark: foyer vs moka, LRU vs LFU          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("  page_size={page_size}B  num_pages={num_pages}  iters/task={iters_per_task}");
    println!("  concurrency levels: {concurrency_str}");
    println!("  eviction scale: {evict_scale_str}  ops/point={evict_ops}");
    println!("  io_uring backend: {use_uring}");
    println!();

    let evictors = [CacheEvictorType::Lfu, CacheEvictorType::Lru];
    let evictor_backends = [CacheEvictorBackend::Foyer, CacheEvictorBackend::Moka];

    // Backends to test: tokio::fs always; io_uring only when requested
    // (and on Linux). Each backend gets its own sub-table in the summary.
    let backends: Vec<(&'static str, bool)> = {
        #[cfg(target_os = "linux")]
        {
            if use_uring {
                vec![("tokio::fs", false), ("io_uring", true)]
            } else {
                vec![("tokio::fs", false)]
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            vec![("tokio::fs", false)]
        }
    };

    // Results keyed by (backend, evictor, concurrency) for the summary.
    let mut all_results: Vec<(String, String, BenchResult)> = Vec::new();

    for (backend_name, backend_uring) in backends.iter().filter(|_| run_phase(1)) {
        let dashes = "─".repeat(50);
        println!("\n══ phase 1 (hit path) backend={backend_name} {dashes}");

        for &concurrency in &concurrency_levels {
            let dashes2 = "─".repeat(40);
            println!("\n── concurrency={concurrency} {dashes2}");

            for evictor_backend in &evictor_backends {
                for evictor in &evictors {
                    let evictor_label = format!("{evictor_backend:?}/{evictor:?}").to_lowercase();
                    let label = format!("{backend_name} / {evictor_label}");
                    // Double the quota so shard imbalance cannot evict during
                    // the hit-path measurement.
                    let (mgr, dir) = create_manager(
                        *evictor_backend,
                        *evictor,
                        page_size as u64,
                        num_pages * 2,
                        *backend_uring,
                    )
                    .await;
                    populate(&mgr, "bench-file", num_pages, page_size).await;

                    let result = bench_concurrent_gets(
                        mgr.clone(),
                        "bench-file",
                        num_pages,
                        page_size,
                        concurrency,
                        iters_per_task,
                        &label,
                    )
                    .await;
                    print_result(&result);
                    all_results.push((backend_name.to_string(), evictor_label, result));

                    drop(mgr);
                    let _ = tokio::fs::remove_dir_all(&dir).await;
                }
            }
        }
    }

    // ── Summary comparison tables ────────────────────────────
    let evictor_labels: Vec<String> = evictor_backends
        .iter()
        .flat_map(|eb| {
            evictors
                .iter()
                .map(move |e| format!("{eb:?}/{e:?}").to_lowercase())
        })
        .collect();

    if run_phase(1) {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("  Phase 1 summary — hit-path avg latency");
        println!("═══════════════════════════════════════════════════════════════");
    }

    for (backend_name, _) in backends.iter().filter(|_| run_phase(1)) {
        println!("\n── backend: {backend_name} ──");
        print!("  {:<12}", "Evictor");
        for &c in &concurrency_levels {
            print!(" {:>10}", format!("conc={c}"));
        }
        println!();
        println!("───────────────────────────────────────────────────────────────");

        for evictor_label in &evictor_labels {
            print!("  {evictor_label:<12}");
            for &c in &concurrency_levels {
                let result = all_results.iter().find(|(b, e, r)| {
                    b == backend_name && e == evictor_label && r.concurrency == c
                });
                match result {
                    Some((_, _, r)) => print!(" {:>10}", fmt_us(r.avg_ns)),
                    None => print!(" {:>10}", "—"),
                }
            }
            println!();
        }

        // foyer vs moka on the hit path, per policy. Both should be close:
        // `on_access` was already O(1) in the moka evictor, so the migration is
        // not expected to move these numbers much.
        println!();
        println!("  foyer vs moka on the hit path ({backend_name}):");
        for evictor in &evictors {
            let foyer_label = format!("Foyer/{evictor:?}").to_lowercase();
            let moka_label = format!("Moka/{evictor:?}").to_lowercase();
            for &c in &concurrency_levels {
                let foyer = all_results
                    .iter()
                    .find(|(b, e, r)| b == backend_name && e == &foyer_label && r.concurrency == c);
                let moka = all_results
                    .iter()
                    .find(|(b, e, r)| b == backend_name && e == &moka_label && r.concurrency == c);
                if let (Some((_, _, f)), Some((_, _, m))) = (foyer, moka) {
                    let ratio = m.avg_ns as f64 / f.avg_ns as f64;
                    println!(
                        "    {evictor:?} conc={c:<3}  moka={} → foyer={}  {ratio:.2}×",
                        fmt_us(m.avg_ns),
                        fmt_us(f.avg_ns),
                    );
                }
            }
        }
    }

    // ── Cross-backend comparison (when io_uring is available) ──
    if backends.len() > 1 && run_phase(1) {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("  io_uring vs tokio::fs speedup (foyer/lfu evictor):");
        println!("───────────────────────────────────────────────────────────────");
        for &c in &concurrency_levels {
            let tokio = all_results
                .iter()
                .find(|(b, e, r)| b == "tokio::fs" && e == "foyer/lfu" && r.concurrency == c);
            let uring = all_results
                .iter()
                .find(|(b, e, r)| b == "io_uring" && e == "foyer/lfu" && r.concurrency == c);
            if let (Some((_, _, t)), Some((_, _, u))) = (tokio, uring) {
                let speedup = t.avg_ns as f64 / u.avg_ns as f64;
                println!(
                    "    conc={c:<3}  tokio::fs={} → io_uring={}  speedup={speedup:.2}×",
                    fmt_us(t.avg_ns),
                    fmt_us(u.avg_ns),
                );
            }
        }
    }

    // ── Phase 2: eviction cost vs page count ─────────────────
    if run_phase(2) {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("  Phase 2 — cost of an evicting put vs resident page count");
        println!("═══════════════════════════════════════════════════════════════");
        println!("  Every put below is against a full directory, so each one evicts.");
        println!("  foyer should be flat; moka scans every page and should not be.");
        println!();

        let mut evict_results: Vec<(String, u64, BenchResult)> = Vec::new();
        for evictor_backend in &evictor_backends {
            for evictor in &evictors {
                let label = format!("{evictor_backend:?}/{evictor:?}").to_lowercase();
                for &pages in &evict_scale {
                    let r = bench_steady_state_evictions(
                        *evictor_backend,
                        *evictor,
                        page_size,
                        pages,
                        evict_ops,
                        false,
                    )
                    .await;
                    println!(
                        "  {label:<12} pages={pages:<7}  avg={:>9}  p50={:>9}  p99={:>9}",
                        fmt_us(r.avg_ns),
                        fmt_us(r.p50_ns),
                        fmt_us(r.p99_ns),
                    );
                    evict_results.push((label.clone(), pages, r));
                }
            }
        }

        println!();
        println!("  Scaling from the smallest to the largest page count:");
        println!("───────────────────────────────────────────────────────────────");
        if let (Some(&first), Some(&last)) = (evict_scale.first(), evict_scale.last()) {
            for label in &evictor_labels {
                let lo = evict_results
                    .iter()
                    .find(|(l, p, _)| l == label && *p == first);
                let hi = evict_results
                    .iter()
                    .find(|(l, p, _)| l == label && *p == last);
                if let (Some((_, _, lo)), Some((_, _, hi))) = (lo, hi) {
                    let growth = hi.avg_ns as f64 / lo.avg_ns.max(1) as f64;
                    let verdict = if growth < 2.0 { "flat" } else { "scales" };
                    println!(
                        "    {label:<12} {first} pages={} → {last} pages={}  {growth:.1}× ({verdict})",
                        fmt_us(lo.avg_ns),
                        fmt_us(hi.avg_ns),
                    );
                }
            }
        }
    }

    // ── Phase 3: steady-state refill (the primary acceptance) ─
    if run_phase(3) {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("  Phase 3 — full directory, {hit_percent}% hit, concurrent refill");
        println!("═══════════════════════════════════════════════════════════════");
        println!("  pages={refill_pages}  iters/task={refill_iters}");
        println!("  A high hit rate does not hide a slow evictor: it only changes");
        println!("  how often victim selection runs, not what it costs.");
        println!();

        let mut refill_results: Vec<RefillResult> = Vec::new();
        for &conc in &refill_conc {
            println!("── concurrency={conc} ──");
            for evictor_backend in &evictor_backends {
                for evictor in &evictors {
                    let r = bench_steady_state_refill(
                        *evictor_backend,
                        *evictor,
                        page_size,
                        refill_pages,
                        conc,
                        refill_iters,
                        hit_percent,
                        use_uring,
                    )
                    .await;
                    println!(
                        "  {:<12} hit={:.1}%  hit_avg={:>9} hit_p99={:>9} │ fill_avg={:>9} fill_p50={:>9} fill_p99={:>9}  {:>8.0} fills/s",
                        r.label,
                        r.hit_rate * 100.0,
                        fmt_us(r.hit_avg_ns),
                        fmt_us(r.hit_p99_ns),
                        fmt_us(r.fill_avg_ns),
                        fmt_us(r.fill_p50_ns),
                        fmt_us(r.fill_p99_ns),
                        r.fills_per_sec,
                    );
                    refill_results.push(r);
                }
            }
        }

        println!();
        println!("  Acceptance — foyer vs moka under the refill workload:");
        println!("───────────────────────────────────────────────────────────────");
        for evictor in &evictors {
            let f_label = format!("Foyer/{evictor:?}").to_lowercase();
            let m_label = format!("Moka/{evictor:?}").to_lowercase();
            for &conc in &refill_conc {
                let f = refill_results
                    .iter()
                    .find(|r| r.label == f_label && r.concurrency == conc);
                let m = refill_results
                    .iter()
                    .find(|r| r.label == m_label && r.concurrency == conc);
                if let (Some(f), Some(m)) = (f, m) {
                    let fill_gain = m.fill_p99_ns as f64 / f.fill_p99_ns.max(1) as f64;
                    // Positive = foyer is slower on the hit path. The acceptance
                    // bar is a regression of no more than 10%.
                    let hit_delta =
                        (f.hit_avg_ns as f64 - m.hit_avg_ns as f64) / m.hit_avg_ns.max(1) as f64;
                    let verdict = if hit_delta <= 0.10 { "PASS" } else { "FAIL" };
                    println!(
                        "    {evictor:?} conc={conc:<3}  fill_p99 {} → {} ({fill_gain:.1}× better)  │  hit_avg {} → {} ({:+.1}%, {verdict})",
                        fmt_us(m.fill_p99_ns),
                        fmt_us(f.fill_p99_ns),
                        fmt_us(m.hit_avg_ns),
                        fmt_us(f.hit_avg_ns),
                        hit_delta * 100.0,
                    );
                }
            }
        }
    }

    // ── Phase 4: cold-start fill ─────────────────────────────
    if run_phase(4) {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("  Phase 4 — cold start, {cold_pages} pages, concurrency={cold_conc}");
        println!("═══════════════════════════════════════════════════════════════");
        println!("  Checks admission does not serialise as concurrency rises.");
        println!();

        for evictor_backend in &evictor_backends {
            for evictor in &evictors {
                let r = bench_cold_fill(
                    *evictor_backend,
                    *evictor,
                    page_size,
                    cold_pages,
                    cold_conc,
                    use_uring,
                )
                .await;
                print_result(&r);
            }
        }
    }

    println!("\n═══════════════════════════════════════════════════════════════");
}
