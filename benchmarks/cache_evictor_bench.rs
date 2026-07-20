//! Cache evictor A/B benchmark: LRU vs LFU (both moka-backed), **local-only**
//! (no GooseFS cluster required).
//!
//! Exercises the full `LocalCacheManager::get()` path (metadata lookup +
//! evictor `on_access` + PageStore IO) to measure the evictor's impact on
//! concurrent cache-hit latency. This is the benchmark that validates the
//! moka replacement documented in
//! `docs/perf/2026-07-09-oncpu6-concurrent-uring-analysis/MOKA_LRU_OPTIMIZATION.md`.
//!
//! ## Usage
//! ```bash
//! cargo run --release --example cache_evictor_bench
//! ```
//!
//! ## Env knobs
//! - `BENCH_PAGE_SIZE` — page size in bytes (default 1024)
//! - `BENCH_NUM_PAGES` — number of pre-populated pages (default 1000)
//! - `BENCH_CONCURRENCY` — comma-separated concurrency levels (default "1,8,16,32")
//! - `BENCH_ITERS_PER_TASK` — iterations per concurrent task (default 10_000)
//! - `BENCH_USE_URING` — use io_uring backend on Linux (default "1")
//!
//! ## Expected results
//!
//! Under 32 concurrent reads of the same file (single-dir workload):
//! - **LRU (moka)**: ~300-450µs avg (per-segment locks, ~3x improvement vs old Mutex)
//! - **LFU (moka TinyLFU)**: ~300-450µs avg (similar per-segment locks)

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use goosefs_sdk::cache::{CacheManager, CacheManagerOptions, LocalCacheManager, PageId};
use goosefs_sdk::config::CacheEvictorType;

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
    evictor: CacheEvictorType,
    page_size: u64,
    num_pages: u64,
    use_uring: bool,
) -> (Arc<LocalCacheManager>, std::path::PathBuf) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let label = format!("{evictor:?}").to_lowercase();
    let dir = std::env::temp_dir().join(format!("gfs_evictor_bench_{label}_{ts}"));

    let capacity = page_size * num_pages + page_size; // slight headroom
    let options = CacheManagerOptions {
        page_size,
        dir_capacity: capacity,
        dirs: vec![dir.clone()],
        evictor,
        async_write_enabled: false,
        async_write_threads: 1,
        quota_enabled: false,
        ttl: None,
        uring_enabled: use_uring,
        uring_queue_depth: 0,
        uring_thread_count: 0,
    };

    let mgr = Arc::new(LocalCacheManager::create(options).await.unwrap());
    (mgr, dir)
}

/// Pre-populate `num_pages` pages under a single file.
async fn populate(mgr: &LocalCacheManager, file_id: &str, num_pages: u64, page_size: usize) {
    let data = vec![0x42u8; page_size];
    for i in 0..num_pages {
        let id = PageId::new(file_id, i);
        assert!(
            mgr.put(&id, Bytes::from(data.clone())).await,
            "put failed for page {i}"
        );
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

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Cache Evictor Benchmark: LRU vs LFU (moka-backed)          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("  page_size={page_size}B  num_pages={num_pages}  iters/task={iters_per_task}");
    println!("  concurrency levels: {concurrency_str}");
    println!("  io_uring backend: {use_uring}");
    println!();

    let evictors = [CacheEvictorType::Lfu, CacheEvictorType::Lru];

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

    for (backend_name, backend_uring) in &backends {
        let dashes = "─".repeat(50);
        println!("\n══ backend={backend_name} {dashes}");

        for &concurrency in &concurrency_levels {
            let dashes2 = "─".repeat(40);
            println!("\n── concurrency={concurrency} {dashes2}");

            for evictor in &evictors {
                let evictor_label = format!("{evictor:?}");
                let label = format!("{backend_name} / {evictor_label}");
                let (mgr, dir) =
                    create_manager(*evictor, page_size as u64, num_pages, *backend_uring).await;
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

    // ── Summary comparison tables ────────────────────────────
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  Summary — avg latency by backend × evictor × concurrency");
    println!("═══════════════════════════════════════════════════════════════");

    for (backend_name, _) in &backends {
        println!("\n── backend: {backend_name} ──");
        print!("  {:<10}", "Evictor");
        for &c in &concurrency_levels {
            print!(" {:>10}", format!("conc={c}"));
        }
        println!();
        println!("───────────────────────────────────────────────────────────────");

        for evictor in &evictors {
            let evictor_label = format!("{evictor:?}");
            print!("  {:<10}", evictor_label);
            for &c in &concurrency_levels {
                let result = all_results.iter().find(|(b, e, r)| {
                    b == backend_name && e == &evictor_label && r.concurrency == c
                });
                match result {
                    Some((_, _, r)) => print!(" {:>10}", fmt_us(r.avg_ns)),
                    None => print!(" {:>10}", "—"),
                }
            }
            println!();
        }

        // Per-backend speedup table.
        println!();
        println!("  LFU vs LRU speedup (avg latency, {backend_name}):");
        for &c in &concurrency_levels {
            let lru = all_results
                .iter()
                .find(|(b, e, r)| b == backend_name && e == "Lru" && r.concurrency == c);
            let lfu = all_results
                .iter()
                .find(|(b, e, r)| b == backend_name && e == "Lfu" && r.concurrency == c);
            if let (Some((_, _, lru)), Some((_, _, lfu))) = (lru, lfu) {
                let speedup = lru.avg_ns as f64 / lfu.avg_ns as f64;
                println!(
                    "    conc={c:<3}  LRU={} → LFU={}  speedup={speedup:.2}×",
                    fmt_us(lru.avg_ns),
                    fmt_us(lfu.avg_ns),
                );
            }
        }
    }

    // ── Cross-backend comparison (when io_uring is available) ──
    if backends.len() > 1 {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("  io_uring vs tokio::fs speedup (LFU evictor):");
        println!("───────────────────────────────────────────────────────────────");
        for &c in &concurrency_levels {
            let tokio = all_results
                .iter()
                .find(|(b, e, r)| b == "tokio::fs" && e == "Lfu" && r.concurrency == c);
            let uring = all_results
                .iter()
                .find(|(b, e, r)| b == "io_uring" && e == "Lfu" && r.concurrency == c);
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

    println!("\n═══════════════════════════════════════════════════════════════");
}
