//! Short-circuit positioned-read A/B benchmark: SC (local mmap) vs gRPC.
//!
//! Writes one file, then issues many random `read_at` ops under two configs
//! that differ only in `short_circuit_enabled`. With SC on (and a local
//! worker), reads are served from a zero-copy local `mmap` slice; with SC off
//! they go over the gRPC data plane. Reports throughput and p50/p99/p999
//! latency for each, plus the speedup — the design predicts a large win for
//! the positioned-read (random) pattern (SHORT_CIRCUIT_DESIGN §5.2).
//!
//! ## Usage
//! ```bash
//! # NOSASL dev cluster with a LOCAL worker:
//! GOOSEFS_AUTH_TYPE=nosasl GFS_SIZE_MB=64 GFS_IO_KB=64 GFS_READS=20000 \
//!   cargo run --release --example sc_pr_ab
//! ```
//!
//! Env knobs: `GOOSEFS_MASTER_ADDR`, `GOOSEFS_AUTH_TYPE`, `GFS_SIZE_MB`
//! (file size), `GFS_IO_KB` (per-read size), `GFS_READS` (iterations).

use std::sync::Arc;
use std::time::{Duration, Instant};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
use goosefs_sdk::metrics::{counter, name};

const TEST_PATH: &str = "/sc-bench/data.bin";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn master_addr() -> String {
    std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
}

fn auth_type() -> AuthType {
    std::env::var("GOOSEFS_AUTH_TYPE")
        .ok()
        .and_then(|s| s.parse::<AuthType>().ok())
        .unwrap_or(AuthType::NoSasl)
}

fn base_config(sc_enabled: bool) -> GoosefsConfig {
    let mut c = GoosefsConfig::new(master_addr());
    c.auth_type = auth_type();
    c.short_circuit_enabled = sc_enabled;
    // Keep the page cache out of the comparison: we want SC-vs-gRPC, not cache.
    c.client_cache_enabled = false;
    c
}

/// Simple xorshift RNG (deterministic, no external dep churn).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

struct Stats {
    elapsed: Duration,
    bytes: u64,
    reads: usize,
    p50: u128,
    p99: u128,
    p999: u128,
}

fn percentile(sorted: &[u128], q: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Run `reads` random positioned reads of `io` bytes over `[0, size)`.
async fn run_pr(ctx: &Arc<FileSystemContext>, size: u64, io: usize, reads: usize) -> Result<Stats> {
    let mut s =
        GoosefsFileInStream::open_with_context(ctx.clone(), TEST_PATH, OpenFileOptions::default())
            .await?;
    let max_off = size.saturating_sub(io as u64).max(1);
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let mut lat = Vec::with_capacity(reads);
    let mut bytes = 0u64;

    let start = Instant::now();
    for _ in 0..reads {
        let off = (rng.next() % max_off) as i64;
        let t = Instant::now();
        let data = s.read_at(off, io).await?;
        lat.push(t.elapsed().as_micros());
        bytes += data.len() as u64;
    }
    let elapsed = start.elapsed();

    lat.sort_unstable();
    Ok(Stats {
        elapsed,
        bytes,
        reads,
        p50: percentile(&lat, 0.50),
        p99: percentile(&lat, 0.99),
        p999: percentile(&lat, 0.999),
    })
}

fn report(label: &str, s: &Stats) {
    let secs = s.elapsed.as_secs_f64();
    let mbps = (s.bytes as f64 / (1024.0 * 1024.0)) / secs;
    let iops = s.reads as f64 / secs;
    println!(
        "  {label:<10} {:>8.1} MiB/s | {:>9.0} ops/s | p50={:>5}us p99={:>6}us p999={:>7}us | {:.2}s",
        mbps, iops, s.p50, s.p99, s.p999, secs
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let size_mb: u64 = env_or("GFS_SIZE_MB", 64);
    let io_kb: usize = env_or("GFS_IO_KB", 64);
    let reads: usize = env_or("GFS_READS", 20_000);
    let size = size_mb * 1024 * 1024;
    let io = io_kb * 1024;

    println!("Short-Circuit Positioned-Read A/B Benchmark");
    println!("===========================================");
    println!("file={size_mb} MiB  io={io_kb} KiB  reads={reads}  (random offsets, single task)");

    // ── Write the test file once (SC config irrelevant for the write). ──
    let write_ctx = FileSystemContext::connect(base_config(true)).await?;
    {
        let master = write_ctx.acquire_master();
        let _ = master.create_directory("/sc-bench", true).await;
        let _ = master.delete(TEST_PATH, false).await;
        let payload: Vec<u8> = (0..size as usize).map(|i| (i % 251) as u8).collect();
        let mut w =
            GoosefsFileWriter::create_with_context(write_ctx.clone(), TEST_PATH, None).await?;
        w.write(&payload).await?;
        w.close().await?;
        println!("\nwrote {size_mb} MiB to {TEST_PATH}");
    }

    // ── B: gRPC only (SC off). ──
    let grpc_ctx = FileSystemContext::connect(base_config(false)).await?;
    // warm up the worker connection / page cache symmetrically
    let _ = run_pr(&grpc_ctx, size, io, reads.min(2000)).await?;
    let grpc = run_pr(&grpc_ctx, size, io, reads).await?;

    // ── A: short-circuit (SC on). ──
    let sc_ctx = FileSystemContext::connect(base_config(true)).await?;
    let sc_open_before = counter(name::CLIENT_SC_OPEN_SUCCESS).get();
    let sc_bytes_before = counter(name::CLIENT_SC_READ_BYTES).get();
    let _ = run_pr(&sc_ctx, size, io, reads.min(2000)).await?;
    let sc = run_pr(&sc_ctx, size, io, reads).await?;
    let sc_fired = counter(name::CLIENT_SC_OPEN_SUCCESS).get() > sc_open_before
        && counter(name::CLIENT_SC_READ_BYTES).get() > sc_bytes_before;

    println!("\nresults:");
    report("gRPC", &grpc);
    report("SC", &sc);

    let tput_speedup = (sc.bytes as f64 / sc.elapsed.as_secs_f64())
        / (grpc.bytes as f64 / grpc.elapsed.as_secs_f64()).max(1.0);
    let p99_improve = grpc.p99 as f64 / (sc.p99.max(1)) as f64;
    println!("\nspeedup: throughput x{tput_speedup:.2},  p99 x{p99_improve:.1} better");
    if sc_fired {
        println!("short-circuit: ACTIVE (served from local mmap)");
    } else {
        println!("short-circuit: NOT active (no local worker?) — 'SC' row reflects gRPC fallback");
    }

    // ── cleanup ──
    write_ctx
        .acquire_master()
        .delete(TEST_PATH, false)
        .await
        .ok();
    write_ctx.close().await?;
    grpc_ctx.close().await?;
    sc_ctx.close().await?;
    Ok(())
}
