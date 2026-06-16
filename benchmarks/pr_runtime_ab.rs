//! B1 root-cause confirmation: **runtime / task-driving model** A/B.
//!
//! Runs the *exact same* single-reader random-read (PR) workload under three
//! drive modes that differ **only** in how the `read_at` future is driven —
//! the SDK code path (`GoosefsFileInStream::read_at` → `positioned_read`) is
//! identical in all three:
//!
//!   1. `multi_thread + spawn`   — mirrors `partv_perf_verify.rs` CONC=1
//!                                 (`JoinSet::spawn` onto a multi-thread pool)
//!   2. `multi_thread + block_on`— same multi-thread runtime, but the read loop
//!                                 is driven inline by `block_on` (no spawn)
//!   3. `current_thread+ block_on`— a current-thread runtime, driven inline
//!
//! Hypothesis (docs/RUST_PYTHON_SDK_OPTIMIZATION.md §V.5 / B1): the Python sync
//! reader is faster at CONC=1 only because it drives the identical future via
//! `Runtime::block_on` on a dedicated thread, whereas the Rust example `spawn`s
//! a single in-flight task onto a multi-thread runtime and pays a park/unpark
//! (futex) wakeup on every `stream.message().await` (each woken by the separate
//! h2 connection task). If true, modes 2 and 3 should jump from ~580 MiB/s to
//! the Python-like ~1300 MiB/s, while mode 1 reproduces ~580.
//!
//! ## Usage
//! ```bash
//! GFS_SIZE_MB=128 GFS_IO_KB=1024 GFS_READS=1000 \
//!   cargo run --release --example pr_runtime_ab
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

/// Resolve the test file path from `GFS_TAG` so this A/B can share the exact
/// same file with the Python harness (`bench_pr_concurrency.py` uses the same
/// `/partv-bench/data-<tag>.bin` naming). Default tag keeps it self-contained.
fn test_file() -> String {
    let tag = std::env::var("GFS_TAG")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "rtab".into());
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

/// Identical PRNG to `partv_perf_verify.rs` and the Python harness, so all
/// three modes visit the same offset sequence.
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

/// Connect a fresh context (pool=1 / wpool=1, matching the published Python
/// wheel's capabilities for a fair comparison).
async fn connect(addr: &str) -> Result<Arc<FileSystemContext>> {
    let config = GoosefsConfig::new(addr)
        .with_master_connection_pool_size(1)
        .with_worker_connection_pool_size(1);
    FileSystemContext::connect(config).await
}

/// The shared read loop: `reads` random positioned reads of `io_size`,
/// verified byte-for-byte. Returns total bytes read.
async fn read_loop(
    ctx: Arc<FileSystemContext>,
    file: String,
    file_size: u64,
    io_size: usize,
    reads: usize,
) -> Result<u64> {
    let mut stream =
        GoosefsFileInStream::open_with_context(ctx, &file, OpenFileOptions::default()).await?;
    let max_off = file_size.saturating_sub(io_size as u64).max(1);
    let mut rng = XorShift::new(0x9E3779B97F4A7C15 ^ 1);
    let mut total = 0u64;
    let mut mism = 0u64;
    let mut lat_us: Vec<u64> = Vec::with_capacity(reads);
    // Warm phase: hammer the SAME offset to prime worker-location resolution
    // and the cached worker channel, so the timed loop measures steady state.
    for _ in 0..16 {
        let _ = stream.read_at(0, io_size).await?;
    }
    for _ in 0..reads {
        let off = rng.next() % max_off;
        let op = std::time::Instant::now();
        let data = stream.read_at(off as i64, io_size).await?;
        lat_us.push(op.elapsed().as_micros() as u64);
        if !verify_slice(&data, off) {
            mism += 1;
        }
        total += data.len() as u64;
    }
    if mism != 0 {
        eprintln!("    ❌ {mism} reads MISMATCHED");
    }
    lat_us.sort_unstable();
    let p = |q: f64| lat_us[((lat_us.len() as f64 * q) as usize).min(lat_us.len() - 1)];
    eprintln!(
        "      per-op latency: min={}us p50={}us p99={}us max={}us",
        lat_us[0],
        p(0.50),
        p(0.99),
        lat_us[lat_us.len() - 1]
    );
    Ok(total)
}

fn report(label: &str, bytes: u64, secs: f64, reads: usize) {
    println!(
        "  {label:<28} {:.1} MiB in {:.3}s → {:>6.0} MiB/s  ({:.3} ms/op)",
        bytes as f64 / (1024.0 * 1024.0),
        secs,
        mib_per_s(bytes, secs),
        secs * 1000.0 / reads as f64,
    );
}

fn main() -> Result<()> {
    let addr = std::env::var("GFS_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    let size_mb: u64 = env_or("GFS_SIZE_MB", 128);
    let io_kb: usize = env_or("GFS_IO_KB", 1024);
    let reads: usize = env_or("GFS_READS", 1000);
    let file_size = size_mb * 1024 * 1024;
    let io_size = io_kb * 1024;
    let file = test_file();

    println!("B1 runtime/task-driving A/B (single reader, CONC=1)");
    println!("===================================================");
    println!("  master = {addr}  file = {file}");
    println!("  size = {size_mb} MiB  io = {io_kb} KiB  reads = {reads}");
    println!("  (pool=1 / wpool=1; same SDK read_at, same offset sequence)\n");

    // ── Setup: write the deterministic test file once (reused if present). ──
    let setup_rt = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");
    setup_rt.block_on(async {
        let ctx = connect(&addr).await?;
        let master = ctx.acquire_master();
        let need_write = match master.get_status(&file).await {
            Ok(st) => st.length.unwrap_or(-1) != file_size as i64,
            Err(_) => true,
        };
        if need_write {
            let _ = master.delete(&file, false).await;
            let _ = master.create_directory(TEST_DIR, true).await;
            println!("[setup] writing {size_mb} MiB → {file} ...");
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
            println!("[setup] reusing existing {size_mb} MiB file");
        }
        ctx.close().await?;
        Ok::<(), goosefs_sdk::error::Error>(())
    })?;
    drop(setup_rt);

    // ── Mode 1: multi_thread + spawn (mirrors partv_perf_verify CONC=1) ──────
    {
        let rt = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt");
        let file_m1 = file.clone();
        let (bytes, secs) = rt.block_on(async {
            let ctx = connect(&addr).await?;
            let start = Instant::now();
            // Spawn the read loop as a single task on the multi-thread pool,
            // exactly like the JoinSet::spawn in partv_perf_verify.
            let handle = tokio::spawn(read_loop(ctx.clone(), file_m1, file_size, io_size, reads));
            let bytes = handle.await.expect("task panicked")?;
            let secs = start.elapsed().as_secs_f64();
            ctx.close().await?;
            Ok::<(u64, f64), goosefs_sdk::error::Error>((bytes, secs))
        })?;
        report("multi_thread + spawn", bytes, secs, reads);
        drop(rt);
    }

    // ── Mode 2: multi_thread + block_on (drive inline, no spawn) ─────────────
    {
        let rt = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt");
        let file_m2 = file.clone();
        let (bytes, secs) = rt.block_on(async {
            let ctx = connect(&addr).await?;
            let start = Instant::now();
            let bytes = read_loop(ctx.clone(), file_m2, file_size, io_size, reads).await?;
            let secs = start.elapsed().as_secs_f64();
            ctx.close().await?;
            Ok::<(u64, f64), goosefs_sdk::error::Error>((bytes, secs))
        })?;
        report("multi_thread + block_on", bytes, secs, reads);
        drop(rt);
    }

    // ── Mode 3: current_thread + block_on (drive inline on one thread) ───────
    {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let file_m3 = file.clone();
        let (bytes, secs) = rt.block_on(async {
            let ctx = connect(&addr).await?;
            let start = Instant::now();
            let bytes = read_loop(ctx.clone(), file_m3, file_size, io_size, reads).await?;
            let secs = start.elapsed().as_secs_f64();
            ctx.close().await?;
            Ok::<(u64, f64), goosefs_sdk::error::Error>((bytes, secs))
        })?;
        report("current_thread + block_on", bytes, secs, reads);
        drop(rt);
    }

    // ── Mode 4: runtime configured IDENTICALLY to the Python binding ─────────
    //    (worker_threads = max(cpu,16), max_blocking_threads = 64) — to test
    //    whether the gap is a runtime *configuration* difference.
    {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(16);
        let rt = Builder::new_multi_thread()
            .worker_threads(cpus.max(16))
            .max_blocking_threads(64)
            .enable_all()
            .build()
            .expect("rt");
        let file_m4 = file.clone();
        let (bytes, secs) = rt.block_on(async {
            let ctx = connect(&addr).await?;
            let start = Instant::now();
            let bytes = read_loop(ctx.clone(), file_m4, file_size, io_size, reads).await?;
            let secs = start.elapsed().as_secs_f64();
            ctx.close().await?;
            Ok::<(u64, f64), goosefs_sdk::error::Error>((bytes, secs))
        })?;
        report("py-style mt + block_on", bytes, secs, reads);
        drop(rt);
    }

    // ── Mode 5: per-call block_on on a persistent stream ────────────────────
    //    EXACTLY mirrors the Python sync model: one `block_on` per `read_at`,
    //    the stream lives between calls. If this matches Python (~580us) while
    //    modes 1-4 (single block_on / spawn driving the whole loop) sit at
    //    ~1200us, the gap is the *inter-call* driving model, not the SDK.
    {
        let rt = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt");
        let file_m5 = file.clone();
        let ctx = rt.block_on(connect(&addr))?;
        let mut stream = rt.block_on(GoosefsFileInStream::open_with_context(
            ctx.clone(),
            &file_m5,
            OpenFileOptions::default(),
        ))?;
        let max_off = file_size.saturating_sub(io_size as u64).max(1);
        let mut rng = XorShift::new(0x9E3779B97F4A7C15 ^ 1);
        for _ in 0..16 {
            let _ = rt.block_on(stream.read_at(0, io_size))?; // warm
        }
        let mut lat_us: Vec<u64> = Vec::with_capacity(reads);
        let start = Instant::now();
        let mut total = 0u64;
        for _ in 0..reads {
            let off = (rng.next() % max_off) as i64;
            let op = Instant::now();
            let data = rt.block_on(stream.read_at(off, io_size))?; // one block_on per read
            lat_us.push(op.elapsed().as_micros() as u64);
            total += data.len() as u64;
        }
        let secs = start.elapsed().as_secs_f64();
        lat_us.sort_unstable();
        let p = |q: f64| lat_us[((lat_us.len() as f64 * q) as usize).min(lat_us.len() - 1)];
        eprintln!(
            "      per-op latency: min={}us p50={}us p99={}us max={}us",
            lat_us[0],
            p(0.50),
            p(0.99),
            lat_us[lat_us.len() - 1]
        );
        report("per-call block_on", total, secs, reads);
        rt.block_on(ctx.close())?;
        drop(rt);
    }

    // ── Mode 6: RECOMMENDED caller pattern — bounded concurrency ────────────
    //    `buffer_unordered(CONC)` over CONC reader-tasks, each owning ONE
    //    persistent stream and looping its share of the reads (NO per-read
    //    open). This is the Part IV guidance: spread independent random reads
    //    across a bounded pool of concurrent streams so per-op round-trips
    //    overlap. Expected to reach the worker-channel ceiling, NOT the slow
    //    single-task floor of modes 1-4.
    {
        const CONC: usize = 16;
        let rt = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt");
        let file_m6 = file.clone();
        let per_reader = reads.div_ceil(CONC); // reads handled by each stream
        let (bytes, secs, lat_us) = rt.block_on(async {
            let ctx = connect(&addr).await?;
            let max_off = file_size.saturating_sub(io_size as u64).max(1);
            let start = Instant::now();
            let results: Vec<Result<(u64, Vec<u64>)>> = stream::iter(0..CONC)
                .map(|t| {
                    let ctx = ctx.clone();
                    let file = file_m6.clone();
                    async move {
                        // One persistent stream per concurrent slot (reused
                        // across this slot's reads — no re-open per read).
                        let mut stream = GoosefsFileInStream::open_with_context(
                            ctx,
                            &file,
                            OpenFileOptions::default(),
                        )
                        .await?;
                        let mut rng = XorShift::new(0x9E3779B97F4A7C15 ^ (t as u64 + 1));
                        for _ in 0..4 {
                            let _ = stream.read_at(0, io_size).await?; // warm
                        }
                        let mut lat = Vec::with_capacity(per_reader);
                        let mut bytes = 0u64;
                        for _ in 0..per_reader {
                            let off = (rng.next() % max_off) as i64;
                            let op = Instant::now();
                            let data = stream.read_at(off, io_size).await?;
                            lat.push(op.elapsed().as_micros() as u64);
                            bytes += data.len() as u64;
                        }
                        Ok::<(u64, Vec<u64>), goosefs_sdk::error::Error>((bytes, lat))
                    }
                })
                .buffer_unordered(CONC)
                .collect()
                .await;
            let secs = start.elapsed().as_secs_f64();
            ctx.close().await?;
            let mut total = 0u64;
            let mut all_lat: Vec<u64> = Vec::with_capacity(reads);
            for r in results {
                let (b, mut l) = r?;
                total += b;
                all_lat.append(&mut l);
            }
            Ok::<(u64, f64, Vec<u64>), goosefs_sdk::error::Error>((total, secs, all_lat))
        })?;
        let mut lat_us = lat_us;
        lat_us.sort_unstable();
        let p = |q: f64| lat_us[((lat_us.len() as f64 * q) as usize).min(lat_us.len() - 1)];
        // NB: under N-way concurrency each op's *in-flight* latency inflates
        // (N ops queue on the single WPOOL=1 worker channel); the meaningful
        // metric is the AGGREGATE MiB/s on the next line, not this per-op p50.
        eprintln!(
            "      per-op latency (in-flight under {CONC}x conc): min={}us p50={}us p99={}us max={}us",
            lat_us[0], p(0.50), p(0.99), lat_us[lat_us.len() - 1]
        );
        report(
            &format!("buffer_unordered({CONC}) [REC]"),
            bytes,
            secs,
            lat_us.len(),
        );
        drop(rt);
    }

    // Leave the test file in place so the Python harness can read the *exact
    // same* file (run `bench_pr_concurrency.py` with the same GFS_TAG). Delete
    // manually when done: `goosefs fs rm /partv-bench/data-<tag>.bin`.

    println!("\n===================================================");
    println!("If modes 2/3 ≫ mode 1, the CONC=1 gap is a runtime/task-driving");
    println!("artifact (multi-thread single-task scheduling), NOT the SDK path.");
    Ok(())
}
