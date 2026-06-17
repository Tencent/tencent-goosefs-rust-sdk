//! Client-side local **page cache** end-to-end demo.
//!
//! Exercises the local page cache (`src/cache/`) against a live Goosefs
//! service, proving the core value proposition: the first (cold) read of a
//! range goes to the worker/UFS and back-fills the local cache; the second
//! (warm) read is served entirely from local disk — no external bytes.
//!
//! It verifies behavior using the `Client.Cache*` metrics:
//!
//! - cold read  → `CacheBytesReadExternal` grows, `CacheBytesReadCache` flat
//! - warm read  → `CacheBytesReadCache` grows, `CacheBytesReadExternal` flat
//! - cross-stream hit → reopening the same path reuses the cached pages
//!   (the cache key is the server inode `file_id`, stable across streams)
//!
//! The cache is configured with:
//! - a unique temp directory (so the demo is self-contained / cleanable),
//! - a small 64 KiB page size (so a modest payload spans several pages),
//! - **synchronous** write-back (`async.write.enabled = false`) so a page is
//!   guaranteed cached the moment the cold read returns — making the warm-read
//!   assertions deterministic.
//!
//! Usage:
//!   cargo run --example page_cache_demo
//!
//! Requires a Goosefs master reachable at `127.0.0.1:9200`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::cache::metric_name as mn;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
use goosefs_sdk::metrics::counter;

/// Path of the test file inside Goosefs.
const TEST_PATH: &str = "/page-cache-test/data.bin";
/// Cache page size for this demo (64 KiB).
const PAGE_SIZE: u64 = 64 * 1024;
/// Payload size: 512 KiB → 8 cache pages.
const PAYLOAD_SIZE: usize = 512 * 1024;

/// Deterministic payload so reads can be verified byte-for-byte.
fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Verify `slice` matches the payload window starting at `offset`.
fn verify_slice(slice: &[u8], offset: usize) -> bool {
    slice
        .iter()
        .enumerate()
        .all(|(i, &b)| b == ((offset + i) % 251) as u8)
}

/// Snapshot of the cache metrics we assert on.
#[derive(Clone, Copy)]
struct CacheStats {
    read_cache: i64,
    read_external: i64,
    written_cache: i64,
    pages: i64,
}

fn snapshot() -> CacheStats {
    CacheStats {
        read_cache: counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).get(),
        read_external: counter(mn::CLIENT_CACHE_BYTES_READ_EXTERNAL).get(),
        written_cache: counter(mn::CLIENT_CACHE_BYTES_WRITTEN_CACHE).get(),
        // CachePages is a gauge, but the registry stores both as i64 cells;
        // we only read it for display, not assertions.
        pages: goosefs_sdk::metrics::gauge(mn::CLIENT_CACHE_PAGES).get(),
    }
}

fn print_delta(label: &str, before: CacheStats, after: CacheStats) {
    println!(
        "  [{label}] ΔreadCache=+{} ΔreadExternal=+{} ΔwrittenCache=+{} pages={}",
        after.read_cache - before.read_cache,
        after.read_external - before.read_external,
        after.written_cache - before.written_cache,
        after.pages,
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Goosefs Client Page Cache Demo");
    println!("==============================");

    // ── Step 0: unique temp cache dir ────────────────────────────
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let cache_dir =
        std::env::temp_dir().join(format!("gfs_page_cache_demo_{}_{ts}", std::process::id()));
    println!("\n0. Cache directory: {}", cache_dir.display());

    // ── Step 1: build a cache-enabled config ─────────────────────
    let addr =
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    let mut config = GoosefsConfig::new(&addr);
    // The local dev server runs without SASL; default to NOSASL but allow an
    // override via GOOSEFS_AUTH_TYPE ("nosasl" / "simple").
    config.auth_type = match std::env::var("GOOSEFS_AUTH_TYPE") {
        Ok(s) => s.parse::<AuthType>().unwrap_or(AuthType::NoSasl),
        Err(_) => AuthType::NoSasl,
    };
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
    println!("  ✅ Context connected, page cache initialized");

    // ── Step 2: prepare the test file ────────────────────────────
    println!("\n2. Writing {PAYLOAD_SIZE}-byte payload to {TEST_PATH}...");
    let _ = master.delete(TEST_PATH, false).await;
    let _ = master.create_directory("/page-cache-test", true).await;

    let payload = make_payload(PAYLOAD_SIZE);
    let mut writer = GoosefsFileWriter::create_with_context(ctx.clone(), TEST_PATH, None).await?;
    writer.write(&payload).await?;
    writer.close().await?;
    println!("  ✅ Wrote {} bytes", writer.bytes_written());

    // The range we'll probe: pages 1..=3 (a partial, multi-page window).
    let probe_off: i64 = (PAGE_SIZE + 1234) as i64;
    let probe_len: usize = (PAGE_SIZE as usize) * 2 + 4096;

    // ── Step 3: COLD read (miss → external + back-fill) ──────────
    println!("\n3. COLD read_at(off={probe_off}, len={probe_len}) — expect external bytes...");
    let cold_before = snapshot();
    let cold_data = {
        let mut s = GoosefsFileInStream::open_with_context(
            ctx.clone(),
            TEST_PATH,
            OpenFileOptions::default(),
        )
        .await?;
        s.read_at(probe_off, probe_len).await?
    };
    let cold_after = snapshot();
    print_delta("cold", cold_before, cold_after);
    assert_eq!(cold_data.len(), probe_len, "cold read length");
    assert!(
        verify_slice(&cold_data, probe_off as usize),
        "cold read content mismatch"
    );
    assert!(
        cold_after.read_external > cold_before.read_external,
        "cold read should fetch from the external source"
    );
    assert!(
        cold_after.written_cache > cold_before.written_cache,
        "cold read should back-fill the cache (sync)"
    );
    println!("  ✅ Cold read fetched externally and back-filled the cache");

    // ── Step 4: WARM read (same range, new stream → cache hit) ───
    println!("\n4. WARM read_at(same range) on a FRESH stream — expect cache hits...");
    let warm_before = snapshot();
    let warm_data = {
        let mut s = GoosefsFileInStream::open_with_context(
            ctx.clone(),
            TEST_PATH,
            OpenFileOptions::default(),
        )
        .await?;
        s.read_at(probe_off, probe_len).await?
    };
    let warm_after = snapshot();
    print_delta("warm", warm_before, warm_after);
    assert_eq!(warm_data, cold_data, "warm read must match cold read");
    assert!(
        warm_after.read_cache > warm_before.read_cache,
        "warm read should be served from the cache"
    );
    assert_eq!(
        warm_after.read_external, warm_before.read_external,
        "warm read must NOT touch the external source"
    );
    println!("  ✅ Warm read served entirely from local cache (no external bytes)");

    // ── Step 5: full-file sequential read also benefits ──────────
    println!("\n5. Full-file read_all() (sequential path also routes through cache)...");
    let full_before = snapshot();
    let full = {
        let mut s = GoosefsFileInStream::open_with_context(
            ctx.clone(),
            TEST_PATH,
            OpenFileOptions::default(),
        )
        .await?;
        s.read_all().await?
    };
    let full_after = snapshot();
    print_delta("full", full_before, full_after);
    assert_eq!(full.len(), PAYLOAD_SIZE, "full read length");
    assert_eq!(&full[..], &payload[..], "full-file content mismatch");
    // The previously cached pages (1..=3) are served from cache; the rest are
    // fetched externally and cached. So cache-served bytes must have grown.
    assert!(
        full_after.read_cache > full_before.read_cache,
        "full read should hit the already-cached pages"
    );
    println!("  ✅ Full read mixed cache hits with external fetches for new pages");

    // ── Step 6: show on-disk cached pages ────────────────────────
    println!("\n6. On-disk cache layout under {} :", cache_dir.display());
    let mut page_files = 0u64;
    if let Ok(rd) = walk_count(&cache_dir).await {
        page_files = rd;
    }
    println!("  ✅ {page_files} page file(s) persisted on local disk");

    // ── Cleanup ──────────────────────────────────────────────────
    println!("\n7. Cleanup...");
    let _ = master.delete(TEST_PATH, false).await;
    ctx.close().await?;
    let _ = tokio::fs::remove_dir_all(&cache_dir).await;
    println!("  ✅ Removed test file, closed context, deleted cache dir");

    println!("\n==============================");
    println!("✅ Page cache demo complete — cold miss → back-fill → warm hit verified.");
    Ok(())
}

/// Recursively count regular files under `root` (the persisted page files).
async fn walk_count(root: &std::path::Path) -> std::io::Result<u64> {
    let mut count = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        while let Some(entry) = rd.next_entry().await? {
            let ft = entry.file_type().await?;
            if ft.is_dir() {
                stack.push(entry.path());
            } else if ft.is_file() {
                count += 1;
            }
        }
    }
    Ok(count)
}
