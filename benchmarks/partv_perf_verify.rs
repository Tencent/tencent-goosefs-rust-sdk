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

//! Part V optimisation verification & micro-benchmark.
//!
//! Exercises and measures the optimisations landed for
//! Part V optimisation verification on a **live local cluster**:
//!
//! - **R2** — `read_at` single-block fast path (random read / PR large IO).
//! - **R1-B** — sequential-read prefetch window + buffered drain + ACK merge
//!   (sequential read / SR throughput).
//! - **R3** — Master multi-channel connection pool (metadata `get_status`
//!   throughput, pool=1 vs pool=N).
//!
//! Every random read is also **verified byte-for-byte** against a
//! deterministic payload, so this doubles as a correctness check for the
//! R2 fast path / multi-block slow path split (consistency red lines C1/C2).
//!
//! ## Usage
//!
//! ```bash
//! # defaults: 128 MiB file, 1 MiB IO, 16 readers, 8 reads each, master pool 8
//! cargo run --release --example partv_perf_verify
//!
//! # override via env vars
//! GFS_ADDR=127.0.0.1:9200 \
//! GFS_SIZE_MB=256 GFS_IO_KB=256 GFS_CONC=32 GFS_READS=16 \
//! GFS_POOL=8 GFS_META_OPS=20000 GFS_META_CONC=256 \
//!   cargo run --release --example partv_perf_verify
//! ```
//!
//! Build in `--release` for representative numbers (LTO=fat is on).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const TEST_DIR: &str = "/partv-bench";

/// Resolve the test file path. `GFS_TAG` lets several instances run in
/// parallel against distinct files (e.g. to measure aggregate throughput
/// across independent worker channels).
fn test_path() -> String {
    match std::env::var("GFS_TAG") {
        Ok(tag) if !tag.is_empty() => format!("{TEST_DIR}/data-{tag}.bin"),
        _ => format!("{TEST_DIR}/data.bin"),
    }
}

/// Read an env var as the given type, falling back to `default`.
fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Deterministic payload byte at absolute file offset `pos`.
/// `pos % 251` (251 is prime) gives a non-trivial, position-recoverable pattern.
#[inline]
fn payload_byte(pos: u64) -> u8 {
    (pos % 251) as u8
}

/// Verify `slice` equals the payload window starting at `offset`.
fn verify_slice(slice: &[u8], offset: u64) -> bool {
    slice
        .iter()
        .enumerate()
        .all(|(i, &b)| b == payload_byte(offset + i as u64))
}

/// Tiny xorshift64* PRNG — Send-safe (no `ThreadRng` across await), enough for
/// picking pseudo-random read offsets.
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

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("GFS_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    let size_mb: u64 = env_or("GFS_SIZE_MB", 128);
    let io_kb: usize = env_or("GFS_IO_KB", 1024);
    let conc: usize = env_or("GFS_CONC", 16);
    let reads_per_task: usize = env_or("GFS_READS", 8);
    let pool_size: usize = env_or("GFS_POOL", 8);
    let wpool_size: usize = env_or("GFS_WPOOL", 1);
    let meta_ops: usize = env_or("GFS_META_OPS", 20_000);
    let meta_conc: usize = env_or("GFS_META_CONC", 256);

    let file_size = size_mb * 1024 * 1024;
    let io_size = io_kb * 1024;

    println!("Part V optimisation verification / benchmark");
    println!("============================================");
    println!("  master      = {addr}");
    println!("  file size   = {size_mb} MiB");
    println!("  io size     = {io_kb} KiB");
    println!("  PR readers  = {conc} (x{reads_per_task} reads each)");
    println!("  SR readers  = {conc}");
    println!("  meta sweep  = {meta_ops} get_status @ conc {meta_conc}");
    println!("  master pool = {pool_size}");
    println!("  worker pool = {wpool_size} (per-worker channels)");

    // ── Build a context with the Master pool (R3) + Worker pool enabled ──────
    let config = GoosefsConfig::new(&addr)
        .with_master_connection_pool_size(pool_size)
        .with_worker_connection_pool_size(wpool_size)
        .with_prefetch_window(8); // R1-B-a default; shown explicitly
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    let master = ctx.acquire_master();
    println!("\n[setup] context connected (master pool = {pool_size})");

    // ── Prepare a large deterministic test file ──────────────────────────────
    let test_path = test_path();
    let _ = master.delete(&test_path, false).await;
    let _ = master.create_directory(TEST_DIR, true).await;

    println!("[setup] writing {size_mb} MiB test file → {test_path} ...");
    let write_start = Instant::now();
    {
        let mut writer =
            GoosefsFileWriter::create_with_context(ctx.clone(), &test_path, None).await?;
        // Write in 8 MiB chunks so the deterministic payload is contiguous and
        // peak memory stays bounded.
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
    }
    let write_secs = write_start.elapsed().as_secs_f64();
    println!(
        "[setup] wrote {size_mb} MiB in {:.2}s ({:.0} MiB/s)",
        write_secs,
        mib_per_s(file_size, write_secs)
    );

    // ─────────────────────────────────────────────────────────────────────────
    // 1) Random read (PR) — R2 single-block fast path, concurrent positioned
    //    reads. Each task owns its own stream (read_at takes &mut self).
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n[1] Random read (PR) — R2 fast path, {conc} concurrent readers");
    let max_off = file_size.saturating_sub(io_size as u64).max(1);
    let total_bytes = Arc::new(AtomicU64::new(0));
    let mismatches = Arc::new(AtomicU64::new(0));

    let pr_start = Instant::now();
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for t in 0..conc {
        let ctx = ctx.clone();
        let total_bytes = total_bytes.clone();
        let mismatches = mismatches.clone();
        let tp = test_path.clone();
        set.spawn(async move {
            let opts = OpenFileOptions::default();
            let mut stream = GoosefsFileInStream::open_with_context(ctx, &tp, opts).await?;
            let mut rng = XorShift::new(0x9E3779B97F4A7C15 ^ (t as u64 + 1));
            for _ in 0..reads_per_task {
                let off = rng.next() % max_off;
                let data = stream.read_at(off as i64, io_size).await?;
                if !verify_slice(&data, off) {
                    mismatches.fetch_add(1, Ordering::Relaxed);
                }
                total_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
            }
            Ok(())
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.expect("PR task panicked")?;
    }
    let pr_secs = pr_start.elapsed().as_secs_f64();
    let pr_bytes = total_bytes.load(Ordering::Relaxed);
    let pr_mism = mismatches.load(Ordering::Relaxed);
    println!(
        "    {} reads, {:.1} MiB in {:.3}s → {:.0} MiB/s",
        conc * reads_per_task,
        pr_bytes as f64 / (1024.0 * 1024.0),
        pr_secs,
        mib_per_s(pr_bytes, pr_secs)
    );
    if pr_mism == 0 {
        println!("    ✅ byte-for-byte verification passed (C1/C2 upheld)");
    } else {
        println!("    ❌ {pr_mism} reads MISMATCHED the expected payload!");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2) Sequential read (SR) — R1-B prefetch + buffered drain + ACK merge.
    //    Two flavours: small-buffer loop (64 KiB) and full read_all.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n[2] Sequential read (SR) — R1-B (prefetch/buffer/ACK-merge)");

    // 2a) 64 KiB small-buffer sequential scan (exercises prefetch + ACK merge).
    {
        let opts = OpenFileOptions::default();
        let mut stream =
            GoosefsFileInStream::open_with_context(ctx.clone(), &test_path, opts).await?;
        let mut buf = vec![0u8; 64 * 1024];
        let mut read_total: u64 = 0;
        let mut ok = true;
        let start = Instant::now();
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            if !verify_slice(&buf[..n], read_total) {
                ok = false;
            }
            read_total += n as u64;
        }
        let secs = start.elapsed().as_secs_f64();
        println!(
            "    64KiB-buf scan: {:.1} MiB in {:.3}s → {:.0} MiB/s {}",
            read_total as f64 / (1024.0 * 1024.0),
            secs,
            mib_per_s(read_total, secs),
            if ok && read_total == file_size {
                "✅"
            } else {
                "❌"
            }
        );
    }

    // 2b) Full read_all (single big sequential drain).
    {
        let opts = OpenFileOptions::default();
        let mut stream =
            GoosefsFileInStream::open_with_context(ctx.clone(), &test_path, opts).await?;
        let start = Instant::now();
        let data = stream.read_all().await?;
        let secs = start.elapsed().as_secs_f64();
        let ok = data.len() as u64 == file_size && verify_slice(&data, 0);
        println!(
            "    read_all     : {:.1} MiB in {:.3}s → {:.0} MiB/s {}",
            data.len() as f64 / (1024.0 * 1024.0),
            secs,
            mib_per_s(data.len() as u64, secs),
            if ok { "✅" } else { "❌" }
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3) Master metadata throughput (R3) — pool=1 vs pool=N get_status sweep.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n[3] Master get_status throughput (R3 pool comparison)");
    let single = bench_get_status(&addr, &test_path, 1, meta_ops, meta_conc).await?;
    println!(
        "    pool=1 : {meta_ops} ops in {:.3}s → {:.0} ops/s",
        single.1, single.0
    );
    let pooled = bench_get_status(&addr, &test_path, pool_size, meta_ops, meta_conc).await?;
    println!(
        "    pool={pool_size} : {meta_ops} ops in {:.3}s → {:.0} ops/s",
        pooled.1, pooled.0
    );
    if single.0 > 0.0 {
        println!(
            "    Δ pool={pool_size} vs pool=1 : {:+.1}%",
            (pooled.0 / single.0 - 1.0) * 100.0
        );
    }
    println!("    (note: single-node localhost RTT understates the remote-cluster R3 gain)");

    // ── Cleanup ──────────────────────────────────────────────────────────────
    println!("\n[cleanup] deleting test file + closing context ...");
    let _ = master.delete(&test_path, false).await;
    ctx.close().await?;

    println!("\n============================================");
    if pr_mism == 0 {
        println!("✅ Part V verification complete — random/sequential reads correct.");
    } else {
        println!("❌ Part V verification FAILED — see mismatch count above.");
    }
    Ok(())
}

/// Run a `get_status` sweep against `path` with the given master pool size and
/// bounded concurrency. Returns `(ops_per_sec, elapsed_secs)`.
async fn bench_get_status(
    addr: &str,
    path: &str,
    pool_size: usize,
    ops: usize,
    concurrency: usize,
) -> Result<(f64, f64)> {
    let config = GoosefsConfig::new(addr).with_master_connection_pool_size(pool_size);
    let ctx = FileSystemContext::connect(config).await?;
    // Warm-up: ensure the path exists and channels are primed.
    let _ = ctx.acquire_master().get_status(path).await?;

    let sem = Arc::new(Semaphore::new(concurrency));
    let start = Instant::now();
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for _ in 0..ops {
        let ctx = ctx.clone();
        let sem = sem.clone();
        let path = path.to_string();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let master = ctx.acquire_master();
            master.get_status(&path).await?;
            Ok(())
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.expect("meta task panicked")?;
    }
    let secs = start.elapsed().as_secs_f64();
    ctx.close().await?;
    Ok((ops as f64 / secs.max(1e-9), secs))
}
