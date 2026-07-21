//! Gating-grade client page-cache **consistency** regression suite.
//!
//! This is the test file referenced by `docs/CLIENT_PAGE_CACHE_DESIGN.md`
//! §12.5. Unlike `tests/page_cache_e2e.rs` (which is mostly about metric
//! counter movement), every case here is a hard byte-level invariant
//! check derived from §1.4:
//!
//! | Case                                            | Invariant   |
//! |-------------------------------------------------|-------------|
//! | `inv_pc_d1_cache_vs_direct_byte_diff`           | INV-PC-D1   |
//! | `inv_pc_d2_read_apis_are_equivalent`            | INV-PC-D2   |
//! | `inv_pc_s1_failed_fill_does_not_poison_cache`   | INV-PC-S1   |
//! | `inv_pc_s2_restart_byte_parity`                 | INV-PC-S2   |
//!
//! All cases require a running GooseFS cluster (default `127.0.0.1:9200`)
//! and are `#[ignore]`d so plain `cargo test` stays hermetic. Run them
//! explicitly:
//!
//! ```bash
//! GOOSEFS_AUTH_TYPE=nosasl \
//!   cargo test --test page_cache_consistency -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `--test-threads=1` keeps the cache byte counters (used as light-weight
//! sanity probes) clean across cases; byte-equality assertions hold
//! regardless.

#[cfg(test)]
mod consistency {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;

    use goosefs_sdk::auth::AuthType;
    use goosefs_sdk::cache::metric_name as mn;
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;
    use goosefs_sdk::error::Result;
    use goosefs_sdk::fs::options::OpenFileOptions;
    use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
    use goosefs_sdk::metrics::counter;

    // ── Test harness ─────────────────────────────────────────────────────────

    fn master_addr() -> String {
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
    }

    fn auth_type() -> AuthType {
        std::env::var("GOOSEFS_AUTH_TYPE")
            .ok()
            .and_then(|s| s.parse::<AuthType>().ok())
            .unwrap_or(AuthType::NoSasl)
    }

    /// Unique on-disk cache directory for one test invocation.
    fn unique_cache_dir(tag: &str) -> std::path::PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "gfs_pc_consistency_{tag}_{}_{ts}",
            std::process::id()
        ))
    }

    fn unique_path(tag: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!(
            "/page-cache-consistency/{tag}_{}_{ts}.bin",
            std::process::id()
        )
    }

    /// Position-dependent payload — any wrong offset / length surfaces as a
    /// byte mismatch rather than `0 == 0` luck. Same generator the SC
    /// consistency suite uses, for parity.
    fn make_payload(size: usize) -> Vec<u8> {
        (0..size)
            .map(|i| ((i.wrapping_mul(2654435761) >> 13) ^ i) as u8)
            .collect()
    }

    /// Base config: cache **on**, deterministic fills, modest block size so
    /// a 10 MiB payload crosses ≥ 2 block boundaries on a dev cluster.
    fn cache_on_config(dir: &std::path::Path) -> GoosefsConfig {
        let mut c = GoosefsConfig::new(master_addr());
        c.auth_type = auth_type();
        c.client_cache_enabled = true;
        c.client_cache_page_size = 64 * 1024; // 64 KiB pages
        c.client_cache_dirs = vec![dir.to_string_lossy().into_owned()];
        c.client_cache_async_write_enabled = false; // deterministic fill
        c.block_size = 4 * 1024 * 1024; // 4 MiB blocks
        c
    }

    /// Sibling config: cache **off**, all other knobs identical so the
    /// comparison isolates the cache layer.
    fn cache_off_config() -> GoosefsConfig {
        let mut c = GoosefsConfig::new(master_addr());
        c.auth_type = auth_type();
        c.client_cache_enabled = false;
        c.block_size = 4 * 1024 * 1024;
        c
    }

    async fn write_blob(ctx: &Arc<FileSystemContext>, path: &str, payload: &[u8]) -> Result<()> {
        let master = ctx.acquire_master();
        let _ = master
            .create_directory("/page-cache-consistency", true)
            .await;
        let _ = master.delete(path, false).await;
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(payload).await?;
        w.close().await?;
        Ok(())
    }

    async fn open_stream(ctx: &Arc<FileSystemContext>, path: &str) -> Result<GoosefsFileInStream> {
        GoosefsFileInStream::open_with_context(ctx.clone(), path, OpenFileOptions::default()).await
    }

    fn cache_bytes_read() -> i64 {
        counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).get()
    }

    /// Curated (offset, len) set hitting every consistency-relevant
    /// boundary for `size` bytes laid out in `block`-byte blocks and
    /// `page`-byte cache pages. Kept under a couple of MiB total reads.
    fn boundary_cases(size: usize, block: usize, page: usize) -> Vec<(i64, usize)> {
        let last = size as i64;
        let p = page as i64;
        let b = block as i64;
        vec![
            // ── Trivial ───────────────────────────────────────────
            (0, 1),
            (0, 4096),
            (0, page),
            // ── Cache-page boundary ──────────────────────────────
            (p - 1, 1),
            (p - 1, 2),
            (p, page),
            (p - 1, page + 2),
            // ── Sub-chunk straddle (1 MiB) ───────────────────────
            ((1 << 20) - 7, 14),
            ((1 << 20) - 1, 1 << 20),
            // ── Block boundary (`block` bytes) ───────────────────
            (b - 1, 2),
            (b - 1, (b + 1) as usize),
            (b, 4096),
            // ── Large random spread ──────────────────────────────
            (777, 33_000),
            (3 * b / 2, 200_000),
            // ── Tail ─────────────────────────────────────────────
            (last - 1, 1),
            (last - 4096, 4096),
        ]
    }

    // ── INV-PC-D1 ────────────────────────────────────────────────────────────

    /// **INV-PC-D1** — for the same blob, the cache-on path and the
    /// cache-off path return byte-for-byte identical data on every
    /// interesting boundary (page, chunk, block, tail), on both cold-miss
    /// and warm-hit passes. A divergence here is a data-correctness bug,
    /// not a performance regression.
    #[tokio::test]
    #[ignore]
    async fn inv_pc_d1_cache_vs_direct_byte_diff() -> Result<()> {
        // 10 MiB across ~3 × 4 MiB blocks — exercises cross-block reads.
        let payload = make_payload(10 * 1024 * 1024);
        let block = 4 * 1024 * 1024;
        let page = 64 * 1024;

        let dir = unique_cache_dir("d1");
        let ctx_cache = FileSystemContext::connect(cache_on_config(&dir)).await?;
        let ctx_direct = FileSystemContext::connect(cache_off_config()).await?;

        let path = unique_path("d1");
        write_blob(&ctx_cache, &path, &payload).await?;

        let cases = boundary_cases(payload.len(), block, page);

        // ── Pass 1: cold miss on the cache side ──────────────────────────
        {
            let mut s_cache = open_stream(&ctx_cache, &path).await?;
            let mut s_direct = open_stream(&ctx_direct, &path).await?;
            for (off, len) in &cases {
                let a: Bytes = s_cache.read_at(*off, *len).await?;
                let b: Bytes = s_direct.read_at(*off, *len).await?;
                let expected = &payload[*off as usize..*off as usize + *len];
                assert_eq!(
                    a.as_ref(),
                    expected,
                    "INV-PC-D1 (cold): cache bytes drift from source at off={off} len={len}"
                );
                assert_eq!(
                    b.as_ref(),
                    expected,
                    "INV-PC-D1 (cold): direct bytes drift from source at off={off} len={len}"
                );
                assert_eq!(
                    a, b,
                    "INV-PC-D1 (cold): cache vs direct mismatch at off={off} len={len}"
                );
            }
        }

        // ── Pass 2: warm hit on the cache side ───────────────────────────
        // Re-open both sides (a fresh stream is required for the cache to
        // observe `on_file_open` and serve from cached pages cleanly).
        {
            let cache_before = cache_bytes_read();
            let mut s_cache = open_stream(&ctx_cache, &path).await?;
            let mut s_direct = open_stream(&ctx_direct, &path).await?;
            for (off, len) in &cases {
                let a: Bytes = s_cache.read_at(*off, *len).await?;
                let b: Bytes = s_direct.read_at(*off, *len).await?;
                let expected = &payload[*off as usize..*off as usize + *len];
                assert_eq!(
                    a.as_ref(),
                    expected,
                    "INV-PC-D1 (warm): cache bytes drift from source at off={off} len={len}"
                );
                assert_eq!(
                    b.as_ref(),
                    expected,
                    "INV-PC-D1 (warm): direct bytes drift from source at off={off} len={len}"
                );
                assert_eq!(
                    a, b,
                    "INV-PC-D1 (warm): cache vs direct mismatch at off={off} len={len}"
                );
            }
            // Cheap sanity probe: warm pass should have served at least
            // *some* bytes from cache. Not strictly required for the
            // byte-equality claim, but a useful canary that we are in
            // fact on the cache path during this test.
            assert!(
                cache_bytes_read() > cache_before,
                "INV-PC-D1: warm pass did not serve any bytes from cache — \
                 is the cache layer actually engaged?"
            );
        }

        ctx_cache.acquire_master().delete(&path, false).await.ok();
        ctx_cache.close().await?;
        ctx_direct.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    // ── INV-PC-D2 ────────────────────────────────────────────────────────────

    /// **INV-PC-D2** — under cache-on, the three public read APIs on
    /// `GoosefsFileInStream` (`read` sequential, `read_at` positioned,
    /// `read_all` whole-file) return identical bytes for the same logical
    /// input. Each uses a fresh stream so cached pages are exercised
    /// rather than the in-stream prefetch buffer.
    #[tokio::test]
    #[ignore]
    async fn inv_pc_d2_read_apis_are_equivalent() -> Result<()> {
        let payload = make_payload(3 * 1024 * 1024 + 7); // odd tail
        let dir = unique_cache_dir("d2");
        let ctx = FileSystemContext::connect(cache_on_config(&dir)).await?;

        let path = unique_path("d2");
        write_blob(&ctx, &path, &payload).await?;

        // ── (1) read_all — also primes the on-disk cache. ───────────────
        let mut s_all = open_stream(&ctx, &path).await?;
        let all = s_all.read_all().await?;
        assert_eq!(all.len(), payload.len(), "INV-PC-D2: read_all length");
        assert_eq!(
            all.as_ref(),
            payload.as_slice(),
            "INV-PC-D2: read_all bytes drift from source"
        );

        // ── (2) read (sequential) ───────────────────────────────────────
        // Heterogeneous, intentionally-misaligned chunk sizes so any
        // chunk-boundary handling bug surfaces.
        let mut s_seq = open_stream(&ctx, &path).await?;
        let mut seq_buf = Vec::with_capacity(payload.len());
        let chunks: [usize; 5] = [37, 4096, 33_333, 1 << 20, 65_521];
        let mut ci = 0usize;
        let mut tmp = vec![0u8; *chunks.iter().max().unwrap()];
        loop {
            let want = chunks[ci % chunks.len()].min(tmp.len());
            ci += 1;
            let n = s_seq.read(&mut tmp[..want]).await?;
            if n == 0 {
                break;
            }
            seq_buf.extend_from_slice(&tmp[..n]);
        }
        assert_eq!(
            seq_buf.len(),
            payload.len(),
            "INV-PC-D2: sequential read drained length"
        );
        assert_eq!(
            seq_buf.as_slice(),
            payload.as_slice(),
            "INV-PC-D2: sequential read bytes drift from source"
        );
        assert_eq!(
            seq_buf.as_slice(),
            all.as_ref(),
            "INV-PC-D2: read != read_all"
        );

        // ── (3) read_at (positioned) ────────────────────────────────────
        // Reconstruct the file via positioned reads only, with a step
        // size that is prime-ish and straddles every cache-page (64 KiB)
        // and chunk (1 MiB) boundary.
        let mut s_pr = open_stream(&ctx, &path).await?;
        let mut pr_buf = Vec::with_capacity(payload.len());
        let mut off = 0i64;
        let step: usize = 257 * 1024;
        while (off as usize) < payload.len() {
            let want = step.min(payload.len() - off as usize);
            let got = s_pr.read_at(off, want).await?;
            assert_eq!(
                got.len(),
                want,
                "INV-PC-D2: read_at short read at off={off} want={want}"
            );
            pr_buf.extend_from_slice(got.as_ref());
            off += want as i64;
        }
        assert_eq!(
            pr_buf.as_slice(),
            payload.as_slice(),
            "INV-PC-D2: read_at bytes drift from source"
        );
        assert_eq!(
            pr_buf.as_slice(),
            all.as_ref(),
            "INV-PC-D2: read_at != read_all"
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    // ── INV-PC-S1 ────────────────────────────────────────────────────────────

    /// **INV-PC-S1** — when the cache layer can't fill (here: the cache
    /// directory is unwritable, so every `put` fails through
    /// `CachePutStoreWriteErrors`), the reader must still see bytes equal
    /// to the source on every range, and the cache must not appear to
    /// serve any bytes (a hit on a never-filled page would mean torn or
    /// fabricated data).
    ///
    /// `connect()` on an unwritable cache path either degrades to
    /// no-cache (init failure) or keeps the cache wired but every fill
    /// fails per-page; in both shapes the byte-equality contract must
    /// hold and `Client.CacheBytesReadCache` must stay flat.
    #[tokio::test]
    #[ignore]
    async fn inv_pc_s1_failed_fill_does_not_poison_cache() -> Result<()> {
        // Match D1's payload size so the shared `boundary_cases(size, 4 MiB, 64 KiB)`
        // generator stays in-bounds (it includes a `3*block/2 + 200_000 B`
        // case which requires payload >= ~6.2 MiB).
        let payload = make_payload(10 * 1024 * 1024);

        let mut config = GoosefsConfig::new(master_addr());
        config.auth_type = auth_type();
        config.client_cache_enabled = true;
        config.client_cache_page_size = 64 * 1024;
        // Path that cannot be created/written on a typical Linux/macOS
        // dev box — either init fails (degrade to no-cache) or every
        // per-page write fails. Either way, reads must still be correct.
        config.client_cache_dirs = vec!["/proc/goosefs_pc_cannot_write_here".to_string()];
        config.client_cache_async_write_enabled = false;
        config.block_size = 4 * 1024 * 1024;

        let ctx = FileSystemContext::connect(config).await?;
        let path = unique_path("s1");
        write_blob(&ctx, &path, &payload).await?;

        // Whole-file read must be byte-equal to source.
        let mut s_all = open_stream(&ctx, &path).await?;
        let all = s_all.read_all().await?;
        assert_eq!(
            all.as_ref(),
            payload.as_slice(),
            "INV-PC-S1: whole-file read drifted from source despite failed fills"
        );

        // Snapshot the cache-hit byte counter; any subsequent read that
        // somehow hits the cache would mean a poisoned page made it in.
        let cache_hits_before = cache_bytes_read();

        // Range reads on a fresh stream — across page / chunk / block
        // boundaries — must also match source bytes.
        let cases = boundary_cases(payload.len(), 4 * 1024 * 1024, 64 * 1024);
        let mut s_range = open_stream(&ctx, &path).await?;
        for (off, len) in cases {
            let got = s_range.read_at(off, len).await?;
            let expected = &payload[off as usize..off as usize + len];
            assert_eq!(
                got.as_ref(),
                expected,
                "INV-PC-S1: range read drift at off={off} len={len} \
                 (cache failure must fall through to external bytes)"
            );
        }

        let cache_hits_after = cache_bytes_read();
        assert_eq!(
            cache_hits_after,
            cache_hits_before,
            "INV-PC-S1: cache served bytes ({}) despite every fill having failed — \
             a poisoned page would be a correctness bug",
            cache_hits_after - cache_hits_before
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }

    // ── INV-PC-S2 ────────────────────────────────────────────────────────────

    /// **INV-PC-S2** — cached pages survive process restart only when the
    /// underlying file's identity `(file_id, length, mtime)` is unchanged.
    ///
    /// Phase A: a cache-on context writes payload v1 and reads it warm
    /// (priming the on-disk cache + sidecar). The context is then
    /// dropped, simulating a process exit.
    ///
    /// Phase B: a fresh context — pointing at the **same** on-disk cache
    /// directory — opens the same path. `restore` rebuilds the in-memory
    /// index from disk; the read must return v1 byte-for-byte (the
    /// cached pages and the source agree).
    ///
    /// Phase C: the file is overwritten as v2 (different length, so
    /// `(length, mtime)` shifts). A third cache-on context opens it and
    /// the very first read must observe v2 — `on_file_open` must
    /// invalidate the stale v1 pages before the read serves anything.
    /// Reading stale v1 from disk-cache would be a correctness bug.
    #[tokio::test]
    #[ignore]
    async fn inv_pc_s2_restart_byte_parity() -> Result<()> {
        let v1 = make_payload(1_500_000); // odd, > 20 cache pages
                                          // Different length so (length, mtime) is guaranteed to shift.
        let v2 = {
            let mut p = make_payload(1_700_000);
            // Bias every byte so a stale-cache read can't accidentally
            // alias v1 even on the prefix.
            for b in &mut p {
                *b = b.wrapping_add(0x5A);
            }
            p
        };

        let dir = unique_cache_dir("s2");
        let path = unique_path("s2");

        // ── Phase A: prime on-disk cache with v1, then drop ─────────────
        {
            let ctx_a = FileSystemContext::connect(cache_on_config(&dir)).await?;
            write_blob(&ctx_a, &path, &v1).await?;
            let mut s = open_stream(&ctx_a, &path).await?;
            let warm = s.read_all().await?;
            assert_eq!(
                warm.as_ref(),
                v1.as_slice(),
                "INV-PC-S2 (phase A): initial warm read drifted from v1"
            );
            ctx_a.close().await?;
        }

        // ── Phase B: fresh context, same cache dir, identity unchanged ──
        {
            let ctx_b = FileSystemContext::connect(cache_on_config(&dir)).await?;
            let mut s_all = open_stream(&ctx_b, &path).await?;
            let restored = s_all.read_all().await?;
            assert_eq!(
                restored.as_ref(),
                v1.as_slice(),
                "INV-PC-S2 (phase B): post-restart read drifted from v1 — \
                 either restore lost pages or served stale bytes"
            );

            // Also a positioned read across a block boundary, to make
            // sure restore's per-page reconstruction is intact.
            let mut s_pr = open_stream(&ctx_b, &path).await?;
            let off = (v1.len() / 2) as i64;
            let len = 200_000usize.min(v1.len() - off as usize);
            let got = s_pr.read_at(off, len).await?;
            assert_eq!(
                got.as_ref(),
                &v1[off as usize..off as usize + len],
                "INV-PC-S2 (phase B): positioned read drifted from v1"
            );
            ctx_b.close().await?;
        }

        // ── Phase C: overwrite as v2; a fresh context must see v2. ─────
        // Use a no-cache writer so the writer doesn't itself touch the
        // disk-cache directory (closer to the real "another client
        // overwrote the file" scenario).
        {
            let ctx_writer = FileSystemContext::connect(cache_off_config()).await?;
            let master = ctx_writer.acquire_master();
            let _ = master.delete(&path, false).await;
            let mut w =
                GoosefsFileWriter::create_with_context(ctx_writer.clone(), &path, None).await?;
            w.write(&v2).await?;
            w.close().await?;
            ctx_writer.close().await?;
        }

        {
            let ctx_c = FileSystemContext::connect(cache_on_config(&dir)).await?;
            let mut s = open_stream(&ctx_c, &path).await?;
            let observed = s.read_all().await?;
            assert_eq!(
                observed.len(),
                v2.len(),
                "INV-PC-S2 (phase C): length still matches stale v1 — \
                 on_file_open did not invalidate the cached pages"
            );
            assert_eq!(
                observed.as_ref(),
                v2.as_slice(),
                "INV-PC-S2 (phase C): served stale v1 bytes from disk-cache after overwrite"
            );

            ctx_c.acquire_master().delete(&path, false).await.ok();
            ctx_c.close().await?;
        }

        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// GoosefsFileReader — streaming `read_next_block` path (OpenDAL / Lance).
// ═══════════════════════════════════════════════════════════════════════════
//
// The suite above validates `GoosefsFileInStream`. The one below validates the
// same page-cache invariants on `GoosefsFileReader`, whose streaming
// `read_next_block` loop is the path OpenDAL / Lance actually drive (design §7).
// Merge gate from the upgrade design §9.4 (HR-3) / §9.5 (HR-4):
//
// | Case                                       | Invariant                  |
// |--------------------------------------------|----------------------------|
// | `inv_reader_disabled_cold_warm_parity`     | `disabled == cold == warm` |
// | `inv_reader_read_all_equals_source`        | whole-file read == source  |
// | `inv_reader_range_equals_source`           | every range read == source |
// | `inv_reader_concurrent_cold_read_same_page`| HR-4 concurrent fill safe  |
//
// Same cluster requirement + `#[ignore]` policy as the module above.

#[cfg(test)]
mod reader_consistency {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;

    use goosefs_sdk::auth::AuthType;
    use goosefs_sdk::cache::metric_name as mn;
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;
    use goosefs_sdk::error::Result;
    use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
    use goosefs_sdk::metrics::counter;

    // ── Test harness ─────────────────────────────────────────────────────────

    fn master_addr() -> String {
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
    }

    fn auth_type() -> AuthType {
        std::env::var("GOOSEFS_AUTH_TYPE")
            .ok()
            .and_then(|s| s.parse::<AuthType>().ok())
            .unwrap_or(AuthType::NoSasl)
    }

    fn unique_cache_dir(tag: &str) -> std::path::PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("gfs_reader_pc_{tag}_{}_{ts}", std::process::id()))
    }

    fn unique_path(tag: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/reader-page-cache/{tag}_{}_{ts}.bin", std::process::id())
    }

    /// Position-dependent payload — any wrong offset / length surfaces as a
    /// byte mismatch rather than `0 == 0` luck (same generator as the
    /// `GoosefsFileInStream` consistency suite, for parity).
    fn make_payload(size: usize) -> Vec<u8> {
        (0..size)
            .map(|i| ((i.wrapping_mul(2654435761) >> 13) ^ i) as u8)
            .collect()
    }

    /// Cache **on**, deterministic sync fills, small pages + modest block size
    /// so a multi-MiB payload crosses several page and block boundaries.
    fn cache_on_config(dir: &std::path::Path) -> GoosefsConfig {
        let mut c = GoosefsConfig::new(master_addr());
        c.auth_type = auth_type();
        c.client_cache_enabled = true;
        c.client_cache_page_size = 64 * 1024; // 64 KiB pages
        c.client_cache_dirs = vec![dir.to_string_lossy().into_owned()];
        c.client_cache_async_write_enabled = false; // deterministic fill
        c.block_size = 4 * 1024 * 1024; // 4 MiB blocks
        c
    }

    /// Sibling config: cache **off**, all other knobs identical so the
    /// comparison isolates the cache layer.
    fn cache_off_config() -> GoosefsConfig {
        let mut c = GoosefsConfig::new(master_addr());
        c.auth_type = auth_type();
        c.client_cache_enabled = false;
        c.block_size = 4 * 1024 * 1024;
        c
    }

    async fn write_blob(ctx: &Arc<FileSystemContext>, path: &str, payload: &[u8]) -> Result<()> {
        let master = ctx.acquire_master();
        let _ = master.create_directory("/reader-page-cache", true).await;
        let _ = master.delete(path, false).await;
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(payload).await?;
        w.close().await?;
        Ok(())
    }

    /// Read `[offset, offset+len)` through the **streaming** reader path
    /// (`open_range_with_context` → `read_next_block` loop) — exactly the path
    /// OpenDAL / Lance drive. This is what the §7 change routes through the
    /// page cache.
    async fn read_range_via_reader(
        ctx: &Arc<FileSystemContext>,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Bytes> {
        let mut reader =
            GoosefsFileReader::open_range_with_context(ctx.clone(), path, offset, len).await?;
        reader.read_all().await
    }

    fn cache_bytes_read() -> i64 {
        counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).get()
    }

    /// Curated (offset, len) set hitting every consistency-relevant boundary
    /// for `size` bytes laid out in `block`-byte blocks and `page`-byte pages.
    fn boundary_cases(size: usize, block: usize, page: usize) -> Vec<(u64, u64)> {
        let last = size as u64;
        let p = page as u64;
        let b = block as u64;
        vec![
            // ── Trivial ───────────────────────────────────────────
            (0, 1),
            (0, 4096),
            (0, p),
            // ── Cache-page boundary ──────────────────────────────
            (p - 1, 1),
            (p - 1, 2),
            (p, p),
            (p - 1, p + 2),
            // ── Sub-chunk straddle (1 MiB) ───────────────────────
            ((1 << 20) - 7, 14),
            ((1 << 20) - 1, 1 << 20),
            // ── Block boundary (`block` bytes) ───────────────────
            (b - 1, 2),
            (b - 1, b + 1),
            (b, 4096),
            // ── Large random spread ──────────────────────────────
            (777, 33_000),
            (3 * b / 2, 200_000),
            // ── Tail ─────────────────────────────────────────────
            (last - 1, 1),
            (last - 4096, 4096),
        ]
    }

    // ── HR-3: disabled == cold == warm ───────────────────────────────────────

    /// **HR-3** — for the same blob, `GoosefsFileReader` returns byte-for-byte
    /// identical data whether the cache is **disabled**, enabled on a **cold**
    /// (miss) pass, or enabled on a **warm** (hit) pass, on every interesting
    /// boundary. A divergence here is a data-correctness bug — the hardest gate
    /// for wiring the cache into the reader's hot path.
    #[tokio::test]
    #[ignore]
    async fn inv_reader_disabled_cold_warm_parity() -> Result<()> {
        // 10 MiB across ~3 × 4 MiB blocks — exercises cross-block reads.
        let payload = make_payload(10 * 1024 * 1024);
        let block = 4 * 1024 * 1024;
        let page = 64 * 1024;

        let dir = unique_cache_dir("hr3");
        let ctx_cache = FileSystemContext::connect(cache_on_config(&dir)).await?;
        let ctx_direct = FileSystemContext::connect(cache_off_config()).await?;

        let path = unique_path("hr3");
        write_blob(&ctx_cache, &path, &payload).await?;

        let cases = boundary_cases(payload.len(), block, page);

        // ── Pass 1: cold miss on the cache side vs disabled ──────────────
        for (off, len) in &cases {
            let cold = read_range_via_reader(&ctx_cache, &path, *off, *len).await?;
            let disabled = read_range_via_reader(&ctx_direct, &path, *off, *len).await?;
            let expected = &payload[*off as usize..(*off + *len) as usize];
            assert_eq!(
                disabled.as_ref(),
                expected,
                "HR-3 (disabled): reader bytes drift from source at off={off} len={len}"
            );
            assert_eq!(
                cold.as_ref(),
                expected,
                "HR-3 (cold): cached reader bytes drift from source at off={off} len={len}"
            );
            assert_eq!(
                cold, disabled,
                "HR-3: disabled != cold at off={off} len={len}"
            );
        }

        // ── Pass 2: warm hit on the cache side ───────────────────────────
        let cache_before = cache_bytes_read();
        for (off, len) in &cases {
            let warm = read_range_via_reader(&ctx_cache, &path, *off, *len).await?;
            let expected = &payload[*off as usize..(*off + *len) as usize];
            assert_eq!(
                warm.as_ref(),
                expected,
                "HR-3 (warm): cached reader bytes drift from source at off={off} len={len}"
            );
        }
        // Canary: the warm pass must have served *some* bytes from cache,
        // proving the reader is actually on the cache path (not silently
        // bypassing it). Byte-equality above holds regardless.
        assert!(
            cache_bytes_read() > cache_before,
            "HR-3: warm pass served zero bytes from cache — is GoosefsFileReader \
             actually routed through read_through_cache?"
        );

        ctx_cache.acquire_master().delete(&path, false).await.ok();
        ctx_cache.close().await?;
        ctx_direct.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    // ── Whole-file + full range coverage ─────────────────────────────────────

    /// The one-shot whole-file reader (`read_file_with_context`) returns the
    /// exact source bytes under cache-on, on both cold and warm passes.
    #[tokio::test]
    #[ignore]
    async fn inv_reader_read_all_equals_source() -> Result<()> {
        let payload = make_payload(3 * 1024 * 1024 + 7); // odd tail
        let dir = unique_cache_dir("all");
        let ctx = FileSystemContext::connect(cache_on_config(&dir)).await?;

        let path = unique_path("all");
        write_blob(&ctx, &path, &payload).await?;

        // Cold: primes the cache.
        let cold = GoosefsFileReader::read_file_with_context(ctx.clone(), &path).await?;
        assert_eq!(cold.len(), payload.len(), "read_all length (cold)");
        assert_eq!(cold.as_ref(), payload.as_slice(), "read_all bytes (cold)");

        // Warm: served (partly) from cache — must still match byte-for-byte.
        let warm = GoosefsFileReader::read_file_with_context(ctx.clone(), &path).await?;
        assert_eq!(warm.as_ref(), payload.as_slice(), "read_all bytes (warm)");
        assert_eq!(warm, cold, "read_all cold vs warm mismatch");

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    /// Reading the whole file as a single range and as a sequence of adjacent
    /// misaligned ranges both reconstruct the source exactly (cache-on).
    #[tokio::test]
    #[ignore]
    async fn inv_reader_range_equals_source() -> Result<()> {
        let payload = make_payload(2 * 1024 * 1024 + 123);
        let dir = unique_cache_dir("range");
        let ctx = FileSystemContext::connect(cache_on_config(&dir)).await?;

        let path = unique_path("range");
        write_blob(&ctx, &path, &payload).await?;

        // Single full range.
        let whole = read_range_via_reader(&ctx, &path, 0, payload.len() as u64).await?;
        assert_eq!(whole.as_ref(), payload.as_slice(), "full range bytes");

        // Reassemble from adjacent, intentionally-misaligned ranges.
        let mut buf = Vec::with_capacity(payload.len());
        let steps: [u64; 5] = [37, 65_521, 33_333, 1 << 20, 4096];
        let mut off = 0u64;
        let mut si = 0usize;
        while off < payload.len() as u64 {
            let len = steps[si % steps.len()].min(payload.len() as u64 - off);
            si += 1;
            let chunk = read_range_via_reader(&ctx, &path, off, len).await?;
            assert_eq!(
                chunk.as_ref(),
                &payload[off as usize..(off + len) as usize],
                "range chunk bytes at off={off} len={len}"
            );
            buf.extend_from_slice(&chunk);
            off += len;
        }
        assert_eq!(
            buf.as_slice(),
            payload.as_slice(),
            "reassembled range bytes"
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    // ── HR-4: concurrent cold-read fill safety ───────────────────────────────

    /// **HR-4** — many tasks concurrently issue the *same* cold range read via
    /// independent `GoosefsFileReader`s sharing one cache-on context. They race
    /// to fill the same pages; the cache's striped per-page locks + atomic
    /// `tmp + rename` page writes guarantee no reader ever observes a
    /// half-written page. Assert every task returns the exact source bytes and
    /// none panics. The visibility window only affects *hit/miss* (performance),
    /// never the bytes returned (correctness) — this test locks that in.
    #[tokio::test]
    #[ignore]
    async fn inv_reader_concurrent_cold_read_same_page() -> Result<()> {
        let payload = make_payload(2 * 1024 * 1024 + 4096);
        let dir = unique_cache_dir("hr4");
        let ctx = FileSystemContext::connect(cache_on_config(&dir)).await?;

        let path = unique_path("hr4");
        write_blob(&ctx, &path, &payload).await?;

        // A window spanning many 64 KiB pages, read concurrently from a cold
        // cache so several tasks race to fill the *same* pages.
        let off: u64 = 777;
        let len: u64 = 512 * 1024 + 33;
        let expected = payload[off as usize..(off + len) as usize].to_vec();

        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let ctx = ctx.clone();
                let path = path.clone();
                tokio::spawn(async move { read_range_via_reader(&ctx, &path, off, len).await })
            })
            .collect();

        for t in tasks {
            let bytes = t.await.expect("HR-4: reader task panicked")?;
            assert_eq!(
                bytes.as_ref(),
                expected.as_slice(),
                "HR-4: concurrent cold read returned wrong bytes (half-written page?)"
            );
        }

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }

    // ── HR-3 matrix edges: empty file + exact page-multiple length ───────────

    /// Closes the remaining §9.4 boundary dimensions for the reader: an
    /// **empty file** (0 bytes) and a file whose length is an exact multiple of
    /// the cache page size (final page full, no EOF remainder). Both must be
    /// byte-identical across cache-off, cold, and warm.
    #[tokio::test]
    #[ignore]
    async fn inv_reader_empty_and_page_multiple() -> Result<()> {
        let page = 64 * 1024u64;

        // (a) Empty file — read_all must return empty on every path.
        {
            let dir = unique_cache_dir("empty");
            let ctx = FileSystemContext::connect(cache_on_config(&dir)).await?;
            let ctx_direct = FileSystemContext::connect(cache_off_config()).await?;
            let path = unique_path("empty");
            write_blob(&ctx, &path, &[]).await?;

            let disabled =
                GoosefsFileReader::read_file_with_context(ctx_direct.clone(), &path).await?;
            let cold = GoosefsFileReader::read_file_with_context(ctx.clone(), &path).await?;
            let warm = GoosefsFileReader::read_file_with_context(ctx.clone(), &path).await?;
            assert!(disabled.is_empty(), "empty file: disabled read not empty");
            assert!(cold.is_empty(), "empty file: cold read not empty");
            assert!(warm.is_empty(), "empty file: warm read not empty");

            ctx.acquire_master().delete(&path, false).await.ok();
            ctx.close().await?;
            ctx_direct.close().await?;
            let _ = tokio::fs::remove_dir_all(&dir).await;
        }

        // (b) Length is an exact multiple of the page size (4 full pages) —
        //     page-aligned ranges + whole file, cold==warm==disabled==source.
        {
            let payload = make_payload((page * 4) as usize);
            let dir = unique_cache_dir("pmul");
            let ctx = FileSystemContext::connect(cache_on_config(&dir)).await?;
            let ctx_direct = FileSystemContext::connect(cache_off_config()).await?;
            let path = unique_path("pmul");
            write_blob(&ctx, &path, &payload).await?;

            let cases: &[(u64, u64)] = &[
                (0, page),        // first full page
                (page, page),     // second full page
                (page * 3, page), // last full page (no remainder)
                (0, page * 4),    // whole file, exact multiple
            ];
            for &(off, len) in cases {
                let expected = &payload[off as usize..(off + len) as usize];
                let disabled = read_range_via_reader(&ctx_direct, &path, off, len).await?;
                let cold = read_range_via_reader(&ctx, &path, off, len).await?;
                let warm = read_range_via_reader(&ctx, &path, off, len).await?;
                assert_eq!(
                    disabled.as_ref(),
                    expected,
                    "page-multiple disabled off={off} len={len}"
                );
                assert_eq!(
                    cold.as_ref(),
                    expected,
                    "page-multiple cold off={off} len={len}"
                );
                assert_eq!(
                    warm.as_ref(),
                    expected,
                    "page-multiple warm off={off} len={len}"
                );
            }

            ctx.acquire_master().delete(&path, false).await.ok();
            ctx.close().await?;
            ctx_direct.close().await?;
            let _ = tokio::fs::remove_dir_all(&dir).await;
        }

        Ok(())
    }
}
