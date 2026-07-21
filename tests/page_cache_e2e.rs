//! Integration tests for the client-side local page cache.
//!
//! These tests require a running GooseFS cluster (default `127.0.0.1:9200`)
//! and are **ignored by default**. Run them explicitly:
//!
//! ```bash
//! # NOSASL dev cluster:
//! GOOSEFS_AUTH_TYPE=nosasl cargo test --test page_cache_e2e -- --ignored --nocapture
//! ```
//!
//! Override the master address with `GOOSEFS_MASTER_ADDR`.

#[cfg(test)]
mod e2e {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use goosefs_sdk::auth::AuthType;
    use goosefs_sdk::cache::metric_name as mn;
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;
    use goosefs_sdk::error::Result;
    use goosefs_sdk::fs::options::OpenFileOptions;
    use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
    use goosefs_sdk::metrics::{counter, gauge};

    fn master_addr() -> String {
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
    }

    fn auth_type() -> AuthType {
        match std::env::var("GOOSEFS_AUTH_TYPE") {
            Ok(s) => s.parse::<AuthType>().unwrap_or(AuthType::NoSasl),
            Err(_) => AuthType::NoSasl,
        }
    }

    /// Unique temp cache directory for one test run.
    fn unique_cache_dir(tag: &str) -> std::path::PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("gfs_e2e_cache_{tag}_{}_{ts}", std::process::id()))
    }

    fn make_payload(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 251) as u8).collect()
    }

    fn read_cache_bytes() -> i64 {
        counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).get()
    }
    fn read_external_bytes() -> i64 {
        counter(mn::CLIENT_CACHE_BYTES_READ_EXTERNAL).get()
    }

    /// Open a stream, read a range, return the bytes (stream dropped after).
    async fn read_range_once(
        ctx: &Arc<FileSystemContext>,
        path: &str,
        off: i64,
        len: usize,
    ) -> Result<bytes::Bytes> {
        let mut s =
            GoosefsFileInStream::open_with_context(ctx.clone(), path, OpenFileOptions::default())
                .await?;
        s.read_at(off, len).await
    }

    /// Cold miss → back-fill → warm hit, asserted via `Client.Cache*` metrics.
    #[tokio::test]
    #[ignore]
    async fn cold_miss_then_warm_hit() -> Result<()> {
        let dir = unique_cache_dir("warm");
        let mut config = GoosefsConfig::new(&master_addr());
        config.auth_type = auth_type();
        config.client_cache_enabled = true;
        config.client_cache_page_size = 64 * 1024;
        config.client_cache_dirs = vec![dir.to_string_lossy().into_owned()];
        config.client_cache_async_write_enabled = false; // deterministic fill

        let ctx = FileSystemContext::connect(config).await?;
        let master = ctx.acquire_master();
        let path = "/page-cache-e2e/warm.bin";
        let _ = master.delete(path, false).await;
        let _ = master.create_directory("/page-cache-e2e", true).await;

        let payload = make_payload(512 * 1024);
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(&payload).await?;
        w.close().await?;

        let (off, len) = (64 * 1024 + 100, 128 * 1024);

        // Cold: external grows, fill happens.
        let ext0 = read_external_bytes();
        let cold = read_range_once(&ctx, path, off, len).await?;
        assert_eq!(&cold[..], &payload[off as usize..off as usize + len]);
        assert!(read_external_bytes() > ext0, "cold read hits external");

        // Warm (fresh stream): cache grows, external flat.
        let cache0 = read_cache_bytes();
        let ext1 = read_external_bytes();
        let warm = read_range_once(&ctx, path, off, len).await?;
        assert_eq!(warm, cold, "warm matches cold");
        assert!(read_cache_bytes() > cache0, "warm read served from cache");
        assert_eq!(read_external_bytes(), ext1, "warm read hits no external");

        // Hit-rate gauge is published.
        assert!(gauge(mn::CLIENT_CACHE_HIT_RATE).get() >= 0);

        let _ = master.delete(path, false).await;
        ctx.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    /// Filling past a tiny capacity triggers eviction (`CachePagesEvicted`).
    #[tokio::test]
    #[ignore]
    async fn capacity_full_triggers_eviction() -> Result<()> {
        let dir = unique_cache_dir("evict");
        let mut config = GoosefsConfig::new(&master_addr());
        config.auth_type = auth_type();
        config.client_cache_enabled = true;
        config.client_cache_page_size = 64 * 1024;
        // Capacity for ~2 pages only.
        config.client_cache_size = 140 * 1024;
        config.client_cache_dirs = vec![dir.to_string_lossy().into_owned()];
        config.client_cache_async_write_enabled = false;
        // `read_all()` below drives the sequential fast path, which by default
        // bypasses the local page cache. Route it through the cache so that
        // filling 8 pages into a ~2-page cache actually exercises eviction.
        config.client_cache_sequential_read_enabled = true;

        let ctx = FileSystemContext::connect(config).await?;
        let master = ctx.acquire_master();
        let path = "/page-cache-e2e/evict.bin";
        let _ = master.delete(path, false).await;
        let _ = master.create_directory("/page-cache-e2e", true).await;

        let payload = make_payload(512 * 1024); // 8 pages
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(&payload).await?;
        w.close().await?;

        let evicted0 = counter(mn::CLIENT_CACHE_PAGES_EVICTED).get();
        // Read the whole file → fills 8 pages into a ~2-page cache → evictions.
        let mut s =
            GoosefsFileInStream::open_with_context(ctx.clone(), path, OpenFileOptions::default())
                .await?;
        let all = s.read_all().await?;
        assert_eq!(all.len(), payload.len());
        assert_eq!(&all[..], &payload[..], "content correct despite eviction");
        assert!(
            counter(mn::CLIENT_CACHE_PAGES_EVICTED).get() > evicted0,
            "small capacity must trigger eviction"
        );

        let _ = master.delete(path, false).await;
        ctx.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    /// Overwriting a file must not serve stale cached pages on reopen.
    #[tokio::test]
    #[ignore]
    async fn overwrite_invalidates_stale_pages() -> Result<()> {
        let dir = unique_cache_dir("overwrite");
        let mut config = GoosefsConfig::new(&master_addr());
        config.auth_type = auth_type();
        config.client_cache_enabled = true;
        config.client_cache_page_size = 64 * 1024;
        config.client_cache_dirs = vec![dir.to_string_lossy().into_owned()];
        config.client_cache_async_write_enabled = false;

        let ctx = FileSystemContext::connect(config).await?;
        let master = ctx.acquire_master();
        let path = "/page-cache-e2e/overwrite.bin";
        let _ = master.create_directory("/page-cache-e2e", true).await;

        // v1: all 0xAA.
        let _ = master.delete(path, false).await;
        let v1 = vec![0xAAu8; 128 * 1024];
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(&v1).await?;
        w.close().await?;
        let r1 = read_range_once(&ctx, path, 0, 64 * 1024).await?;
        assert!(r1.iter().all(|&b| b == 0xAA), "v1 reads 0xAA");

        // Overwrite v2: all 0xBB, different length so (length,mtime) changes.
        let _ = master.delete(path, false).await;
        let v2 = vec![0xBBu8; 200 * 1024];
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(&v2).await?;
        w.close().await?;

        // Reopen → on_file_open detects the change → stale pages invalidated.
        let r2 = read_range_once(&ctx, path, 0, 64 * 1024).await?;
        assert!(
            r2.iter().all(|&b| b == 0xBB),
            "v2 must not serve stale 0xAA pages from cache"
        );

        let _ = master.delete(path, false).await;
        ctx.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    /// An unwritable cache directory must degrade to external reads, not error.
    #[tokio::test]
    #[ignore]
    async fn unwritable_cache_dir_falls_back() -> Result<()> {
        let mut config = GoosefsConfig::new(&master_addr());
        config.auth_type = auth_type();
        config.client_cache_enabled = true;
        config.client_cache_page_size = 64 * 1024;
        // A path that cannot be created/written (best-effort cache init or
        // per-page writes fail → reads still succeed from the worker).
        config.client_cache_dirs = vec!["/proc/goosefs_cannot_write_here".to_string()];
        config.client_cache_async_write_enabled = false;

        // connect() must succeed even if cache init fails (degrades to no-cache).
        let ctx = FileSystemContext::connect(config).await?;
        let master = ctx.acquire_master();
        let path = "/page-cache-e2e/fallback.bin";
        let _ = master.delete(path, false).await;
        let _ = master.create_directory("/page-cache-e2e", true).await;

        let payload = make_payload(128 * 1024);
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(&payload).await?;
        w.close().await?;

        // Reads must return correct data regardless of cache health.
        let got = read_range_once(&ctx, path, 0, 64 * 1024).await?;
        assert_eq!(&got[..], &payload[0..64 * 1024], "fallback read correct");

        let _ = master.delete(path, false).await;
        ctx.close().await?;
        Ok(())
    }
}
