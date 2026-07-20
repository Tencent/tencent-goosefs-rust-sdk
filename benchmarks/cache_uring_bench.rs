//! Page-cache backend A/B benchmark: io_uring vs tokio::fs, **local-only**
//! (no GooseFS cluster required).
//!
//! Directly exercises the `PageStore` trait with both backends to isolate the
//! IO-layer overhead. Each iteration is one `get()` call — a cache hit — so
//! ops/s is the primary metric.
//!
//! ## Usage (Linux 5.1+)
//! ```bash
//! cargo run --release --example cache_uring_bench
//! ```
//! On non-Linux platforms only the `LocalPageStore` (tokio::fs) path is
//! benchmarked; `UringPageStore` is `#[cfg(target_os = "linux")]`-gated.
//!
//! ## Env knobs
//! - `BENCH_ITERATIONS` — single-threaded iterations per backend (default 100_000)
//! - `BENCH_CONCURRENCY` — concurrent task count (default 32)
//! - `BENCH_CONCURRENT_ITERATIONS` — iterations per concurrent task (default 10_000)
//! - `BENCH_PAGE_SIZE` — page size in bytes (default 1024)
//!
//! See `docs/CLIENT_PAGE_CACHE_IO_URING_DESIGN.md` §10.3 for expected results.

use std::sync::Arc;
use std::time::Instant;

#[cfg(target_os = "linux")]
use goosefs_sdk::cache::store::UringPageStore;
use goosefs_sdk::cache::store::{LocalPageStore, PageStore};
use goosefs_sdk::cache::PageId;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct BenchResult {
    label: &'static str,
    ops_per_sec: f64,
    p50_ns: u64,
    p99_ns: u64,
    total_ns: u64,
}

/// Run `iterations` single-threaded `get()` calls against `store` on `page_id`,
/// measuring per-op latency.
///
/// A warm-up read is performed first (to fill the fd cache on the io_uring
/// backend) so all measured iterations are cache-hit + fd-cache-hit.
async fn bench_single_threaded(
    store: &Arc<dyn PageStore>,
    page_id: &PageId,
    page_size: usize,
    iterations: usize,
    label: &'static str,
) -> BenchResult {
    let mut dst = vec![0u8; page_size];

    // Warm-up (fd cache fill for UringPageStore). Timeout after 5s to avoid
    // hanging the whole benchmark if the io_uring backend is broken.
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.get(page_id, 0, &mut dst),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => panic!("{label} warm-up failed: {e}"),
        Err(_) => panic!(
            "{label} warm-up TIMED OUT after 5s — io_uring backend may be broken; \
             check RUST_LOG=trace output and `dmesg | tail` for kernel errors"
        ),
    }

    let mut latencies_ns: Vec<u64> = Vec::with_capacity(iterations);
    let start = Instant::now();
    for _ in 0..iterations {
        let op_start = Instant::now();
        let n = store.get(page_id, 0, &mut dst).await.expect("get failed");
        debug_assert_eq!(n, page_size, "short read in benchmark");
        latencies_ns.push(op_start.elapsed().as_nanos() as u64);
    }
    let total = start.elapsed();

    latencies_ns.sort_unstable();
    let p50 = latencies_ns[latencies_ns.len() / 2];
    let p99 = latencies_ns[latencies_ns.len() * 99 / 100];
    let ops_per_sec = iterations as f64 / total.as_secs_f64().max(1e-9);

    BenchResult {
        label,
        ops_per_sec,
        p50_ns: p50,
        p99_ns: p99,
        total_ns: total.as_nanos() as u64,
    }
}

/// Run `concurrency` tasks, each doing `iterations_per_task` `get()` calls.
async fn bench_concurrent(
    store: Arc<dyn PageStore>,
    page_id: PageId,
    page_size: usize,
    concurrency: usize,
    iterations_per_task: usize,
    label: &'static str,
) -> BenchResult {
    // Warm-up.
    {
        let mut dst = vec![0u8; page_size];
        let _ = store.get(&page_id, 0, &mut dst).await;
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let store = Arc::clone(&store);
        let pid = page_id.clone();
        handles.push(tokio::spawn(async move {
            let mut dst = vec![0u8; page_size];
            let mut latencies: Vec<u64> = Vec::with_capacity(iterations_per_task);
            for _ in 0..iterations_per_task {
                let op_start = Instant::now();
                let n = store.get(&pid, 0, &mut dst).await.expect("get failed");
                debug_assert_eq!(n, page_size);
                latencies.push(op_start.elapsed().as_nanos() as u64);
            }
            latencies
        }));
    }

    let mut all_latencies: Vec<u64> = Vec::with_capacity(concurrency * iterations_per_task);
    for h in handles {
        all_latencies.extend(h.await.unwrap());
    }
    let total = start.elapsed();

    all_latencies.sort_unstable();
    let p50 = all_latencies[all_latencies.len() / 2];
    let p99 = all_latencies[all_latencies.len() * 99 / 100];
    let total_ops = concurrency * iterations_per_task;
    let ops_per_sec = total_ops as f64 / total.as_secs_f64().max(1e-9);

    BenchResult {
        label,
        ops_per_sec,
        p50_ns: p50,
        p99_ns: p99,
        total_ns: total.as_nanos() as u64,
    }
}

fn print_result(r: &BenchResult) {
    println!(
        "  {:<16} {:>10.0} ops/s   p50={:>6}µs   p99={:>6}µs   total={:.2}s",
        r.label,
        r.ops_per_sec,
        r.p50_ns / 1000,
        r.p99_ns / 1000,
        r.total_ns as f64 / 1e9,
    );
}

fn print_header(title: &str) {
    println!("\n── {title} ────────────────────────────────────────");
}

/// Like `bench_concurrent` but each task reads from a different file
/// (round-robin). This exercises the dir fd cache's main benefit
/// (eliminating VFS lock contention on concurrent `open()`) and the
/// multi-file scaling of the `LocalCacheManager` metadata layer.
async fn bench_concurrent_multi_file(
    store: Arc<dyn PageStore>,
    page_ids: Vec<PageId>,
    page_size: usize,
    concurrency: usize,
    iterations_per_task: usize,
    label: &'static str,
) -> BenchResult {
    // Warm-up: read each file once.
    for id in &page_ids {
        let mut dst = vec![0u8; page_size];
        let _ = store.get(id, 0, &mut dst).await;
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let store = Arc::clone(&store);
        let page_ids = page_ids.clone();
        handles.push(tokio::spawn(async move {
            let mut dst = vec![0u8; page_size];
            let mut latencies: Vec<u64> = Vec::with_capacity(iterations_per_task);
            for i in 0..iterations_per_task {
                // Round-robin: each task reads a different file each iteration.
                let id = &page_ids[(i + task_id) % page_ids.len()];
                let op_start = Instant::now();
                let n = store.get(id, 0, &mut dst).await.expect("get failed");
                debug_assert_eq!(n, page_size);
                latencies.push(op_start.elapsed().as_nanos() as u64);
            }
            latencies
        }));
    }

    let mut all_latencies: Vec<u64> = Vec::with_capacity(concurrency * iterations_per_task);
    for h in handles {
        all_latencies.extend(h.await.unwrap());
    }
    let total = start.elapsed();

    all_latencies.sort_unstable();
    let p50 = all_latencies[all_latencies.len() / 2];
    let p99 = all_latencies[all_latencies.len() * 99 / 100];
    let total_ops = concurrency * iterations_per_task;
    let ops_per_sec = total_ops as f64 / total.as_secs_f64().max(1e-9);

    BenchResult {
        label,
        ops_per_sec,
        p50_ns: p50,
        p99_ns: p99,
        total_ns: total.as_nanos() as u64,
    }
}

#[tokio::main]
async fn main() {
    let iterations: usize = env_or("BENCH_ITERATIONS", 100_000);
    let concurrency: usize = env_or("BENCH_CONCURRENCY", 32);
    let concurrent_iterations: usize = env_or("BENCH_CONCURRENT_ITERATIONS", 10_000);
    let page_size: usize = env_or("BENCH_PAGE_SIZE", 1024);

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let base_dir = std::env::temp_dir().join(format!("gfs_uring_bench_{ts}"));

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Page-Cache Backend Benchmark: io_uring vs tokio::fs        ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("  page_size={page_size}B  iterations={iterations}  concurrency={concurrency}×{concurrent_iterations}");
    println!("  cache_dir={}", base_dir.display());

    #[cfg(target_os = "linux")]
    {
        // Probe io_uring availability early — if it fails the user should
        // see it here rather than waiting for the warm-up timeout below.
        match goosefs_sdk::cache::store::is_uring_available() {
            true => println!("  io_uring: available"),
            false => panic!(
                "io_uring is NOT available on this platform. \
                 Set GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED=false to skip the io_uring benchmark."
            ),
        }
    }

    // ── Create stores ──────────────────────────────────────────
    let local_dir = base_dir.join("tokio_fs");
    let local_store: Arc<dyn PageStore> = Arc::new(
        LocalPageStore::create(&local_dir, page_size as u64)
            .await
            .expect("LocalPageStore create"),
    );

    #[cfg(target_os = "linux")]
    let uring_store: Arc<dyn PageStore> = {
        // Use the new default of 8 threads (B2 fix). The old bench used 2,
        // which masked the concurrency benefit of the non-blocking driver
        // loop (B1 fix). Set to 0 to use the default (8).
        goosefs_sdk::cache::store::init_uring_config(16384, 0);
        // The background thread pool is lazily initialised on the first
        // submit_request() call (inside the first get()). The warm-up
        // timeout above will surface any hang.
        let uring_dir = base_dir.join("uring");
        Arc::new(
            UringPageStore::create(&uring_dir, page_size as u64)
                .await
                .expect("UringPageStore create"),
        )
    };

    let page_id = PageId::new("bench-file", 0);
    let page_data = vec![0x42u8; page_size];

    // ── Write the page to both stores ──────────────────────────
    local_store
        .put(&page_id, &page_data)
        .await
        .expect("local put");
    #[cfg(target_os = "linux")]
    uring_store
        .put(&page_id, &page_data)
        .await
        .expect("uring put");

    // ── Single-threaded benchmark ──────────────────────────────
    print_header("Single-threaded cache-hit throughput");

    let r_local =
        bench_single_threaded(&local_store, &page_id, page_size, iterations, "tokio::fs").await;
    print_result(&r_local);

    #[cfg(target_os = "linux")]
    let r_uring = Some(
        bench_single_threaded(&uring_store, &page_id, page_size, iterations, "io_uring").await,
    );
    #[cfg(not(target_os = "linux"))]
    let r_uring: Option<BenchResult> = None;

    if let Some(r) = &r_uring {
        print_result(r);
        let speedup = r.ops_per_sec / r_local.ops_per_sec.max(1.0);
        println!("  → io_uring speedup: {speedup:.2}×");
    }
    #[cfg(not(target_os = "linux"))]
    {
        println!("  (io_uring backend not available on this platform)");
    }

    // ── Concurrent benchmark ───────────────────────────────────
    print_header(&format!(
        "Concurrent cache-hit throughput ({concurrency} tasks)"
    ));

    let rc_local = bench_concurrent(
        Arc::clone(&local_store),
        page_id.clone(),
        page_size,
        concurrency,
        concurrent_iterations,
        "tokio::fs",
    )
    .await;
    print_result(&rc_local);

    #[cfg(target_os = "linux")]
    let rc_uring = Some(
        bench_concurrent(
            Arc::clone(&uring_store),
            page_id.clone(),
            page_size,
            concurrency,
            concurrent_iterations,
            "io_uring",
        )
        .await,
    );
    #[cfg(not(target_os = "linux"))]
    let rc_uring: Option<BenchResult> = None;

    if let Some(rc) = &rc_uring {
        print_result(rc);
        let speedup = rc.ops_per_sec / rc_local.ops_per_sec.max(1.0);
        println!("  → io_uring speedup: {speedup:.2}×");
    }
    #[cfg(not(target_os = "linux"))]
    {
        println!("  (io_uring backend not available on this platform)");
    }

    // ── Multi-file benchmark (exercises dir fd cache) ───────────
    // The single-file bench above doesn't trigger the dir fd cache's main
    // benefit (4-level → 1-level path resolution is a constant saving
    // regardless of file count, but VFS lock contention is only visible
    // with multiple files). This benchmark uses N files, each task
    // accesses its own file — this is the workload that exposes the
    // VFS lock contention that the dir fd cache is designed to fix.
    let n_files = env_or("BENCH_MULTI_FILE_COUNT", 64);
    print_header(&format!(
        "Multi-file cache-hit throughput ({n_files} files, {concurrency} concurrent tasks)"
    ));

    // Pre-populate N files in both stores.
    let multi_file_ids: Vec<PageId> = (0..n_files)
        .map(|i| PageId::new(format!("bench-file-{i}"), 0))
        .collect();
    for id in &multi_file_ids {
        local_store
            .put(id, &page_data)
            .await
            .expect("local put multi");
        #[cfg(target_os = "linux")]
        uring_store
            .put(id, &page_data)
            .await
            .expect("uring put multi");
    }

    // Run concurrent reads: each task picks a file in round-robin.
    // This pattern matches real workloads where many Lance queries read
    // pages from different blocks/files concurrently.
    let rc_local_multi = bench_concurrent_multi_file(
        Arc::clone(&local_store),
        multi_file_ids.clone(),
        page_size,
        concurrency,
        concurrent_iterations,
        "tokio::fs",
    )
    .await;
    print_result(&rc_local_multi);

    #[cfg(target_os = "linux")]
    let rc_uring_multi = Some(
        bench_concurrent_multi_file(
            Arc::clone(&uring_store),
            multi_file_ids.clone(),
            page_size,
            concurrency,
            concurrent_iterations,
            "io_uring",
        )
        .await,
    );
    #[cfg(not(target_os = "linux"))]
    let rc_uring_multi: Option<BenchResult> = None;

    if let Some(rc) = &rc_uring_multi {
        print_result(rc);
        let speedup = rc.ops_per_sec / rc_local_multi.ops_per_sec.max(1.0);
        println!("  → io_uring speedup: {speedup:.2}×");
    }

    // ── Summary table ──────────────────────────────────────────
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  Summary (page_size={page_size}B)");
    println!("───────────────────────────────────────────────────────────────");
    println!(
        "  {:<16} {:>14} {:>14} {:>10} {:>10}",
        "Backend", "Single ops/s", "Conc ops/s", "p99(1T)", "p99(32T)"
    );
    println!("───────────────────────────────────────────────────────────────");
    println!(
        "  {:<16} {:>14.0} {:>14.0} {:>8}µs {:>8}µs",
        "tokio::fs",
        r_local.ops_per_sec,
        rc_local.ops_per_sec,
        r_local.p99_ns / 1000,
        rc_local.p99_ns / 1000,
    );
    if let (Some(r), Some(rc)) = (&r_uring, &rc_uring) {
        println!(
            "  {:<16} {:>14.0} {:>14.0} {:>8}µs {:>8}µs",
            "io_uring",
            r.ops_per_sec,
            rc.ops_per_sec,
            r.p99_ns / 1000,
            rc.p99_ns / 1000,
        );
    }
    println!("───────────────────────────────────────────────────────────────");

    // ── Cleanup ────────────────────────────────────────────────
    let _ = tokio::fs::remove_dir_all(&base_dir).await;
}
