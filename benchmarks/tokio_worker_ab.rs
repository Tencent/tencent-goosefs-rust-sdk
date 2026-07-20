//! B4 knee-finder A/B: sweep tokio `worker_threads` over the *same* PR
//! `read_at` workload used by `pr_runtime_ab` (mode 6 pattern: bounded
//! concurrency, one persistent stream per reader), so operators can pick a
//! `worker_threads` value on **their** hardware / workload before flipping
//! the shared runtime default (see `docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md`
//! §B4 — "cap tokio `worker_threads`").
//!
//! ## Why this bench exists
//!
//! §B4 recommends `worker_threads = min(available_cores, 8)` to reduce the
//! ~40 % `tokio worker::run` self time observed in the on-CPU flame graph
//! (`docs/perf/2026-07-06-oncpu-goose-vs-local/`). But under-sizing hurts
//! throughput for IO-heavy workloads, so the plan explicitly gates the
//! default flip on "rerun the workload with 4 / 8 / 16 workers, pick the
//! knee".
//!
//! The Python binding (`bindings/python/src/runtime.rs`) already exposes
//! `GOOSEFS_TOKIO_WORKER_THREADS` as an opt-in override; this harness
//! provides the missing "collect the numbers on your workload" step so
//! sites do not have to hand-roll a comparison.
//!
//! ## What it measures
//!
//! For each `worker_threads` in `{4, 8, cpus, cpus.max(16)}` (deduped),
//! build a fresh multi-thread Tokio runtime and run:
//!
//!   * `CONC` concurrent reader tasks
//!   * each with **one persistent** `GoosefsFileInStream`
//!   * each doing `per_reader = READS / CONC` random `read_at` ops
//!   * over the *same* file / IO size / offset PRNG seed across all rows
//!
//! Reports aggregate MiB/s + per-op p50/p99, so the knee is directly
//! visible in the output table.
//!
//! ## Consistency guarantees
//!
//! * Byte-for-byte verification (matches `pr_runtime_ab`'s `verify_slice`).
//! * Identical PRNG seed per slot across worker-thread rows, so every row
//!   visits the exact same offsets.
//! * Fresh runtime + fresh `FileSystemContext` per row — no cross-row
//!   state (channels, caches) leaks that would bias later rows.
//!
//! ## Usage
//!
//! ```bash
//! GFS_SIZE_MB=256 GFS_IO_KB=1024 GFS_CONC=16 GFS_READS=2000 \
//!   cargo run --release --example tokio_worker_ab
//!
//! # Custom sweep (comma-separated, positive integers only):
//! GFS_WORKERS=1,2,4,8,16 cargo run --release --example tokio_worker_ab
//! ```

use std::sync::Arc;
use std::time::Instant;

use futures::stream::{self, StreamExt};
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
use tokio::runtime::Builder;

const TEST_DIR: &str = "/partv-bench";

/// Resolve the test file path from `GFS_TAG`, mirroring `pr_runtime_ab` so
/// the same on-disk file can be reused across the two harnesses.
fn test_file() -> String {
    let tag = std::env::var("GFS_TAG")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "twab".into());
    format!("{TEST_DIR}/data-{tag}.bin")
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[inline]
fn payload_byte(pos: u64) -> u8 {
    (pos % 251) as u8
}

fn verify_slice(slice: &[u8], offset: u64) -> bool {
    slice
        .iter()
        .enumerate()
        .all(|(i, &b)| b == payload_byte(offset + i as u64))
}

/// Same PRNG shape as `pr_runtime_ab` so both harnesses walk the same
/// offset sequence for a given seed.
struct XorShift(u64);
impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

fn mib_per_s(bytes: u64, secs: f64) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / secs.max(1e-9)
}

/// Fresh context per row (pool=1 / wpool=1 to isolate the runtime effect
/// from Master/Worker channel pool tuning covered by B3 elsewhere).
async fn connect(addr: &str) -> Result<Arc<FileSystemContext>> {
    let config = GoosefsConfig::new(addr)
        .with_master_connection_pool_size(1)
        .with_worker_connection_pool_size(1);
    FileSystemContext::connect(config).await
}

/// Parse `GFS_WORKERS` (comma-separated positive integers). Empty / missing
/// / all-invalid falls back to the default sweep.
fn parse_sweep(default: &[usize]) -> Vec<usize> {
    let raw = std::env::var("GFS_WORKERS").unwrap_or_default();
    if raw.trim().is_empty() {
        return default.to_vec();
    }
    let mut out: Vec<usize> = raw
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .collect();
    if out.is_empty() {
        return default.to_vec();
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn default_sweep() -> Vec<usize> {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(16);
    // §B4 candidate (`min(cores, 8)`), the "small cap" candidate, and the
    // current shared-runtime default (`cpus.max(16)` — matches
    // `bindings/python/src/runtime.rs`). Also throw in `cpus` on its own so
    // small-machine cases (cpus < 8) still show a middle row.
    let mut v = vec![4usize, 8, cpus, cpus.max(16)];
    v.sort_unstable();
    v.dedup();
    v
}

/// Row output — kept as a struct so the summary table stays aligned even
/// when rows arrive in different orders (they don't today, but future
/// parallel sweeps might).
struct Row {
    workers: usize,
    bytes: u64,
    secs: f64,
    p50_us: u64,
    p99_us: u64,
    mism: u64,
}

/// Run one row: build a fresh multi-thread runtime with `workers`
/// `worker_threads`, drive `CONC` bounded-concurrency reader tasks over
/// one persistent stream each, return aggregate metrics.
fn run_row(
    workers: usize,
    addr: &str,
    file: &str,
    file_size: u64,
    io_size: usize,
    reads: usize,
    conc: usize,
) -> Result<Row> {
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let per_reader = reads.div_ceil(conc);
    let addr = addr.to_string();
    let file = file.to_string();

    let (bytes, secs, mut lat_us, mism) = rt.block_on(async move {
        let ctx = connect(&addr).await?;
        let max_off = file_size.saturating_sub(io_size as u64).max(1);
        let start = Instant::now();
        let results: Vec<Result<(u64, Vec<u64>, u64)>> = stream::iter(0..conc)
            .map(|t| {
                let ctx = ctx.clone();
                let file = file.clone();
                async move {
                    let mut stream = GoosefsFileInStream::open_with_context(
                        ctx,
                        &file,
                        OpenFileOptions::default(),
                    )
                    .await?;
                    // Same seed shape as `pr_runtime_ab` so cross-harness
                    // comparisons visit the same offsets when `GFS_TAG` is
                    // shared.
                    let mut rng = XorShift::new(0x9E3779B97F4A7C15 ^ (t as u64 + 1));
                    for _ in 0..4 {
                        let _ = stream.read_at(0, io_size).await?; // warm
                    }
                    let mut lat = Vec::with_capacity(per_reader);
                    let mut bytes = 0u64;
                    let mut mism = 0u64;
                    for _ in 0..per_reader {
                        let off = rng.next() % max_off;
                        let op = Instant::now();
                        let data = stream.read_at(off as i64, io_size).await?;
                        lat.push(op.elapsed().as_micros() as u64);
                        if !verify_slice(&data, off) {
                            mism += 1;
                        }
                        bytes += data.len() as u64;
                    }
                    Ok::<(u64, Vec<u64>, u64), goosefs_sdk::error::Error>((bytes, lat, mism))
                }
            })
            .buffer_unordered(conc)
            .collect()
            .await;
        let secs = start.elapsed().as_secs_f64();
        ctx.close().await?;
        let mut total_bytes = 0u64;
        let mut all_lat: Vec<u64> = Vec::with_capacity(reads);
        let mut total_mism = 0u64;
        for r in results {
            let (b, mut l, m) = r?;
            total_bytes += b;
            all_lat.append(&mut l);
            total_mism += m;
        }
        Ok::<(u64, f64, Vec<u64>, u64), goosefs_sdk::error::Error>((
            total_bytes,
            secs,
            all_lat,
            total_mism,
        ))
    })?;

    // Explicitly drop the runtime so its worker threads unwind before the
    // next row builds a new pool — otherwise thread counts stack up and
    // later rows race against phantom idle workers.
    drop(rt);

    lat_us.sort_unstable();
    let p = |q: f64| lat_us[((lat_us.len() as f64 * q) as usize).min(lat_us.len() - 1)];
    Ok(Row {
        workers,
        bytes,
        secs,
        p50_us: p(0.50),
        p99_us: p(0.99),
        mism,
    })
}

/// Ensure the deterministic test file exists at the requested size. Runs
/// on a *separate* setup runtime that is dropped before the sweep starts,
/// so the sweep's fresh-runtime discipline is not polluted.
fn ensure_file(addr: &str, file: &str, file_size: u64) -> Result<()> {
    let setup_rt = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build setup runtime");
    let addr = addr.to_string();
    let file = file.to_string();
    setup_rt.block_on(async move {
        let ctx = connect(&addr).await?;
        let master = ctx.acquire_master();
        let need_write = match master.get_status(&file).await {
            Ok(st) => st.length.unwrap_or(-1) != file_size as i64,
            Err(_) => true,
        };
        if need_write {
            let _ = master.delete(&file, false).await;
            let _ = master.create_directory(TEST_DIR, true).await;
            println!(
                "[setup] writing {} MiB -> {} ...",
                file_size / (1024 * 1024),
                file
            );
            let mut writer =
                GoosefsFileWriter::create_with_context(ctx.clone(), &file, None).await?;
            const CHUNK: usize = 8 * 1024 * 1024;
            let mut written: u64 = 0;
            let mut buf = vec![0u8; CHUNK];
            while written < file_size {
                let this = CHUNK.min((file_size - written) as usize);
                for (i, b) in buf[..this].iter_mut().enumerate() {
                    *b = payload_byte(written + i as u64);
                }
                writer.write(&buf[..this]).await?;
                written += this as u64;
            }
            writer.close().await?;
        } else {
            println!(
                "[setup] reusing existing {} MiB file",
                file_size / (1024 * 1024)
            );
        }
        ctx.close().await?;
        Ok::<(), goosefs_sdk::error::Error>(())
    })?;
    drop(setup_rt);
    Ok(())
}

fn main() -> Result<()> {
    let addr = std::env::var("GFS_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    let size_mb: u64 = env_or("GFS_SIZE_MB", 256);
    let io_kb: usize = env_or("GFS_IO_KB", 1024);
    let conc: usize = env_or("GFS_CONC", 16);
    let reads: usize = env_or("GFS_READS", 2000);
    let file_size = size_mb * 1024 * 1024;
    let io_size = io_kb * 1024;
    let file = test_file();
    let sweep = parse_sweep(&default_sweep());

    println!("B4 tokio worker_threads knee-finder");
    println!("===================================");
    println!("  master  = {addr}");
    println!("  file    = {file}");
    println!("  size    = {size_mb} MiB");
    println!("  io      = {io_kb} KiB");
    println!("  conc    = {conc}  (bounded, one persistent stream per reader)");
    println!(
        "  reads   = {reads} (per row; per-reader = {})",
        reads.div_ceil(conc)
    );
    println!("  workers = {sweep:?}");
    println!("  (pool=1 / wpool=1 — isolate runtime effect from B3 pool tuning)\n");

    ensure_file(&addr, &file, file_size)?;

    let mut rows: Vec<Row> = Vec::with_capacity(sweep.len());
    for w in &sweep {
        println!("[run ] workers = {w}");
        let row = run_row(*w, &addr, &file, file_size, io_size, reads, conc)?;
        if row.mism != 0 {
            eprintln!("      WARNING: {} reads MISMATCHED payload", row.mism);
        }
        println!(
            "      {:.1} MiB in {:.3}s -> {:>6.0} MiB/s   (p50={}us p99={}us)",
            row.bytes as f64 / (1024.0 * 1024.0),
            row.secs,
            mib_per_s(row.bytes, row.secs),
            row.p50_us,
            row.p99_us,
        );
        rows.push(row);
    }

    println!("\nSummary");
    println!("-------");
    println!(
        "  {:>8}   {:>10}   {:>8}   {:>8}   {:>10}",
        "workers", "MiB/s", "p50 us", "p99 us", "mismatch"
    );
    for r in &rows {
        println!(
            "  {:>8}   {:>10.0}   {:>8}   {:>8}   {:>10}",
            r.workers,
            mib_per_s(r.bytes, r.secs),
            r.p50_us,
            r.p99_us,
            r.mism,
        );
    }

    // Knee heuristic: highlight the smallest `workers` whose MiB/s is
    // within 3 % of the best row. That is the value operators should
    // pick for §B4's `GOOSEFS_TOKIO_WORKER_THREADS` override — it caps
    // the pool without measurable throughput loss.
    if let Some(best) = rows
        .iter()
        .max_by(|a, b| mib_per_s(a.bytes, a.secs).total_cmp(&mib_per_s(b.bytes, b.secs)))
    {
        let best_mib = mib_per_s(best.bytes, best.secs);
        let threshold = best_mib * 0.97;
        let knee = rows
            .iter()
            .find(|r| mib_per_s(r.bytes, r.secs) >= threshold)
            .unwrap_or(best);
        println!(
            "\nKnee (smallest workers within 3% of best {best_mib:.0} MiB/s): {} workers -> {:.0} MiB/s",
            knee.workers,
            mib_per_s(knee.bytes, knee.secs),
        );
        println!(
            "  export GOOSEFS_TOKIO_WORKER_THREADS={}   # opt-in cap per FLAMEGRAPH_OPTIMIZATION_PLAN §B4",
            knee.workers,
        );
    }

    println!("\nNotes");
    println!("-----");
    println!("  * The Python binding reads GOOSEFS_TOKIO_WORKER_THREADS at module init");
    println!("    (see bindings/python/src/runtime.rs). Set it before importing the wheel.");
    println!("  * The Rust SDK does not build a runtime itself; embedders control the");
    println!("    tokio::Builder they hand to it. Apply the picked value in your own");
    println!("    Builder::new_multi_thread().worker_threads(N).");
    println!("  * The file is left in place for reuse; delete manually via");
    println!("    `goosefs fs rm {file}` when done.");
    Ok(())
}
