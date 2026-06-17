//! Client page cache A/B benchmark: repeated-read throughput, cache on vs off.
//!
//! Writes one file, then reads the **same range repeatedly** under two configs
//! that differ only in `client_cache_enabled`. With the cache on, every read
//! after the first should be served from local disk (no worker round-trip),
//! so aggregate throughput rises and `CacheBytesReadExternal` stops growing.
//!
//! ## Usage
//! ```bash
//! # NOSASL dev cluster:
//! GOOSEFS_AUTH_TYPE=nosasl GFS_SIZE_MB=64 GFS_IO_KB=256 GFS_READS=200 \
//!   cargo run --release --example page_cache_ab
//! ```
//!
//! Env knobs: `GOOSEFS_MASTER_ADDR`, `GOOSEFS_AUTH_TYPE`, `GFS_SIZE_MB`,
//! `GFS_IO_KB` (per-read size), `GFS_READS` (iterations).

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::cache::metric_name as mn;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
use goosefs_sdk::metrics::counter;

const TEST_PATH: &str = "/page-cache-bench/data.bin";

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

fn base_config(cache_enabled: bool, cache_dir: &str) -> GoosefsConfig {
    let mut c = GoosefsConfig::new(&master_addr());
    c.auth_type = auth_type();
    c.client_cache_enabled = cache_enabled;
    c.client_cache_page_size = 1024 * 1024;
    c.client_cache_dirs = vec![cache_dir.to_string()];
    c.client_cache_async_write_enabled = false; // ensure first read fills before reuse
    c
}

/// Read `[0, io)` `reads` times via fresh streams; return (elapsed, bytes).
async fn repeated_read(
    ctx: &Arc<FileSystemContext>,
    io: usize,
    reads: usize,
) -> Result<(std::time::Duration, u64)> {
    let start = Instant::now();
    let mut total = 0u64;
    for _ in 0..reads {
        let mut s = GoosefsFileInStream::open_with_context(
            ctx.clone(),
            TEST_PATH,
            OpenFileOptions::default(),
        )
        .await?;
        let data = s.read_at(0, io).await?;
        total += data.len() as u64;
    }
    Ok((start.elapsed(), total))
}

fn mib_s(bytes: u64, dur: std::time::Duration) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / dur.as_secs_f64().max(1e-9)
}

#[tokio::main]
async fn main() -> Result<()> {
    let size_mb: usize = env_or("GFS_SIZE_MB", 64);
    let io_kb: usize = env_or("GFS_IO_KB", 256);
    let reads: usize = env_or("GFS_READS", 200);
    let io = io_kb * 1024;

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let cache_dir = std::env::temp_dir().join(format!("gfs_cache_ab_{ts}"));
    let cache_dir = cache_dir.to_string_lossy().into_owned();

    println!("Page Cache A/B  size={size_mb}MiB io={io_kb}KiB reads={reads}");
    println!("master={} auth={}", master_addr(), auth_type());

    // ── Prepare the test file (cache off context) ───────────────
    let ctx_off = FileSystemContext::connect(base_config(false, &cache_dir)).await?;
    let master = ctx_off.acquire_master();
    let _ = master.delete(TEST_PATH, false).await;
    let _ = master.create_directory("/page-cache-bench", true).await;
    let payload: Vec<u8> = (0..size_mb * 1024 * 1024)
        .map(|i| (i % 251) as u8)
        .collect();
    let mut w = GoosefsFileWriter::create_with_context(ctx_off.clone(), TEST_PATH, None).await?;
    w.write(&payload).await?;
    w.close().await?;

    // ── A: cache OFF ────────────────────────────────────────────
    let (dur_off, bytes_off) = repeated_read(&ctx_off, io, reads).await?;
    println!(
        "\n[cache OFF] {reads} reads in {:?} → {:.1} MiB/s",
        dur_off,
        mib_s(bytes_off, dur_off)
    );
    ctx_off.close().await?;

    // ── B: cache ON ─────────────────────────────────────────────
    let ctx_on = FileSystemContext::connect(base_config(true, &cache_dir)).await?;
    let ext_before = counter(mn::CLIENT_CACHE_BYTES_READ_EXTERNAL).get();
    let cache_before = counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).get();
    let (dur_on, bytes_on) = repeated_read(&ctx_on, io, reads).await?;
    let ext_delta = counter(mn::CLIENT_CACHE_BYTES_READ_EXTERNAL).get() - ext_before;
    let cache_delta = counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).get() - cache_before;
    println!(
        "[cache ON ] {reads} reads in {:?} → {:.1} MiB/s",
        dur_on,
        mib_s(bytes_on, dur_on)
    );
    println!(
        "           Δexternal={} B (≈1 page after warm-up), Δcache={} B, hitRate={}%",
        ext_delta,
        cache_delta,
        goosefs_sdk::metrics::gauge(mn::CLIENT_CACHE_HIT_RATE).get()
    );

    let speedup = mib_s(bytes_on, dur_on) / mib_s(bytes_off, dur_off).max(1e-9);
    println!("\n→ cache speedup ≈ {:.2}×", speedup);

    // ── Cleanup ─────────────────────────────────────────────────
    let _ = master.delete(TEST_PATH, false).await;
    ctx_on.close().await?;
    let _ = tokio::fs::remove_dir_all(&cache_dir).await;
    Ok(())
}
