//! `GoosefsFileReader` + client **page cache** end-to-end demo.
//!
//! This is the reader-side counterpart of `examples/page_cache_demo.rs` (which
//! uses `GoosefsFileInStream`). It demonstrates the §7 integration: the
//! streaming `GoosefsFileReader::read_next_block` path — the exact path that
//! OpenDAL / Lance drive — is now routed through the local page cache.
//!
//! Unlike `GoosefsFileInStream::read_all` (whose sequential fast path only
//! consults the cache when `client_cache_sequential_read_enabled` is set),
//! **every** `GoosefsFileReader` read goes through `read_through_cache` when the
//! cache is enabled. So a plain whole-file read shows:
//!
//! - cold read → `CacheBytesReadExternal` grows, `CacheBytesReadCache` flat
//! - warm read (fresh reader) → `CacheBytesReadCache` grows, external flat
//!
//! Usage:
//!   cargo run --example reader_page_cache_demo
//!   GOOSEFS_AUTH_TYPE=simple cargo run --example reader_page_cache_demo
//!
//! Requires a Goosefs master reachable at `127.0.0.1:9200` (override with
//! `GOOSEFS_MASTER_ADDR`).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::cache::metric_name as mn;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::metrics::counter;

/// Path of the test file inside Goosefs.
const TEST_PATH: &str = "/reader-page-cache-demo/data.bin";
/// Cache page size for this demo (64 KiB).
const PAGE_SIZE: u64 = 64 * 1024;
/// Payload size: 512 KiB → 8 cache pages.
const PAYLOAD_SIZE: usize = 512 * 1024;

/// Deterministic payload so reads can be verified byte-for-byte.
fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Snapshot of the cache metrics we assert on.
#[derive(Clone, Copy)]
struct CacheStats {
    read_cache: i64,
    read_external: i64,
    written_cache: i64,
}

fn snapshot() -> CacheStats {
    CacheStats {
        read_cache: counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).get(),
        read_external: counter(mn::CLIENT_CACHE_BYTES_READ_EXTERNAL).get(),
        written_cache: counter(mn::CLIENT_CACHE_BYTES_WRITTEN_CACHE).get(),
    }
}

fn print_delta(label: &str, before: CacheStats, after: CacheStats) {
    println!(
        "  [{label}] ΔreadCache=+{} ΔreadExternal=+{} ΔwrittenCache=+{}",
        after.read_cache - before.read_cache,
        after.read_external - before.read_external,
        after.written_cache - before.written_cache,
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Goosefs GoosefsFileReader Page Cache Demo");
    println!("=========================================");

    // ── Step 0: unique temp cache dir ────────────────────────────
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let cache_dir =
        std::env::temp_dir().join(format!("gfs_reader_pc_demo_{}_{ts}", std::process::id()));
    println!("\n0. Cache directory: {}", cache_dir.display());

    // ── Step 1: build a cache-enabled config ─────────────────────
    let addr =
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    let mut config = GoosefsConfig::new(&addr);
    // Default to SIMPLE (the GooseFS default) with the OS username; override
    // via GOOSEFS_AUTH_TYPE ("nosasl" / "simple").
    if let Ok(s) = std::env::var("GOOSEFS_AUTH_TYPE") {
        if let Ok(at) = s.parse::<AuthType>() {
            config.auth_type = at;
        }
    }
    config.client_cache_enabled = true;
    config.client_cache_page_size = PAGE_SIZE;
    config.client_cache_dirs = vec![cache_dir.to_string_lossy().into_owned()];
    // Synchronous fill → deterministic warm-read assertions.
    config.client_cache_async_write_enabled = false;
    println!(
        "   master={addr} auth={} cache enabled (page_size={} KiB, sync fill)",
        config.auth_type,
        PAGE_SIZE / 1024
    );

    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    let master = ctx.acquire_master();
    println!("  Context connected, page cache initialized");

    // ── Step 2: prepare the test file ────────────────────────────
    println!("\n2. Writing {PAYLOAD_SIZE}-byte payload to {TEST_PATH}...");
    let _ = master.delete(TEST_PATH, false).await;
    let _ = master
        .create_directory("/reader-page-cache-demo", true)
        .await;

    let payload = make_payload(PAYLOAD_SIZE);
    let mut writer = GoosefsFileWriter::create_with_context(ctx.clone(), TEST_PATH, None).await?;
    writer.write(&payload).await?;
    writer.close().await?;
    println!("  Wrote {} bytes", writer.bytes_written());

    // ── Step 3: COLD whole-file read via GoosefsFileReader ───────
    // `read_file_with_context` loops `read_next_block`, which now flows through
    // the page cache — so a cold read fetches externally and back-fills.
    println!("\n3. COLD GoosefsFileReader::read_file_with_context() — expect external bytes...");
    let cold_before = snapshot();
    let cold = GoosefsFileReader::read_file_with_context(ctx.clone(), TEST_PATH).await?;
    let cold_after = snapshot();
    print_delta("cold", cold_before, cold_after);
    assert_eq!(cold.len(), PAYLOAD_SIZE, "cold read length");
    assert_eq!(&cold[..], &payload[..], "cold read content mismatch");
    assert!(
        cold_after.read_external > cold_before.read_external,
        "cold read should fetch from the external source"
    );
    assert!(
        cold_after.written_cache > cold_before.written_cache,
        "cold read should back-fill the cache (sync)"
    );
    println!("  Cold read fetched externally and back-filled the cache");

    // ── Step 4: WARM whole-file read on a FRESH reader → cache hit ──
    println!("\n4. WARM read on a FRESH GoosefsFileReader — expect cache hits, no external...");
    let warm_before = snapshot();
    let warm = GoosefsFileReader::read_file_with_context(ctx.clone(), TEST_PATH).await?;
    let warm_after = snapshot();
    print_delta("warm", warm_before, warm_after);
    assert_eq!(warm, cold, "warm read must match cold read");
    assert!(
        warm_after.read_cache > warm_before.read_cache,
        "warm read should be served from the cache"
    );
    assert_eq!(
        warm_after.read_external, warm_before.read_external,
        "warm read must NOT touch the external source"
    );
    println!("  Warm read served entirely from local cache (no external bytes)");

    // ── Step 5: range read also flows through the cache ──────────
    println!("\n5. WARM range read via open_range_with_context() — expect cache hits...");
    let range_off: u64 = (PAGE_SIZE + 1234) as u64;
    let range_len: u64 = (PAGE_SIZE * 2 + 4096) as u64;
    let range_before = snapshot();
    let range_data = {
        let mut r = GoosefsFileReader::open_range_with_context(
            ctx.clone(),
            TEST_PATH,
            range_off,
            range_len,
        )
        .await?;
        r.read_all().await?
    };
    let range_after = snapshot();
    print_delta("range", range_before, range_after);
    assert_eq!(
        range_data.as_ref(),
        &payload[range_off as usize..(range_off + range_len) as usize],
        "range read content mismatch"
    );
    assert!(
        range_after.read_cache > range_before.read_cache,
        "warm range read should be served from the cache"
    );
    println!("  Range read served from local cache");

    // ── Cleanup ──────────────────────────────────────────────────
    println!("\n6. Cleanup...");
    let _ = master.delete(TEST_PATH, false).await;
    ctx.close().await?;
    let _ = tokio::fs::remove_dir_all(&cache_dir).await;
    println!("  Removed test file, closed context, deleted cache dir");

    println!("\n=========================================");
    println!(
        "GoosefsFileReader page cache demo complete — cold miss → back-fill → warm hit verified."
    );
    println!("This is the OpenDAL/Lance streaming read path benefiting from the 0.1.6 cache.");
    Ok(())
}
