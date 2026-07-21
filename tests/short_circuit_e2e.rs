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

//! Integration tests for the short-circuit (local mmap) read path.
//!
//! These require a running GooseFS cluster with a **local** worker (so that
//! `source_is_local` holds and the short-circuit path actually engages) and are
//! **ignored by default**. Run them explicitly:
//!
//! ```bash
//! # NOSASL dev cluster (default 127.0.0.1:9200):
//! GOOSEFS_AUTH_TYPE=nosasl cargo test --test short_circuit_e2e -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `--test-threads=1` is recommended: the SC counters asserted below are
//! process-global, so running the cases serially keeps the metric deltas
//! clean. Byte-equality assertions hold regardless of threading.
//!
//! Override the master address with `GOOSEFS_MASTER_ADDR`.
//!
//! What they lock in:
//! - **INV-D2 / INV-S1**: SC reads are byte-for-byte equal to the source and to
//!   the gRPC path.
//! - SC actually fires on a local worker (via `Client.ShortCircuit*` metrics).
//! - Per-block reader reuse (one `OpenLocalBlock` + LRU hits).

#[cfg(test)]
mod e2e {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use goosefs_sdk::auth::AuthType;
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;
    use goosefs_sdk::error::Result;
    use goosefs_sdk::fs::options::OpenFileOptions;
    use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
    use goosefs_sdk::metrics::{counter, name};

    fn master_addr() -> String {
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
    }

    fn auth_type() -> AuthType {
        match std::env::var("GOOSEFS_AUTH_TYPE") {
            Ok(s) => s.parse::<AuthType>().unwrap_or(AuthType::NoSasl),
            Err(_) => AuthType::NoSasl,
        }
    }

    fn base_config() -> GoosefsConfig {
        let mut config = GoosefsConfig::new(master_addr());
        config.auth_type = auth_type();
        // These tests specifically exercise the short-circuit read path. The
        // SDK default for `short_circuit_enabled` was flipped to `false` in
        // the 2026-07-07 hotspot pass (see docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md
        // §C6), so opt back in explicitly here — otherwise all SC counters
        // stay at zero and every assertion below trips.
        // The one gRPC-baseline callsite (`short_circuit_matches_grpc`) still
        // overrides this to `false` locally, so byte-parity is preserved.
        config.short_circuit_enabled = true;
        config
    }

    /// Deterministic, position-dependent payload so a wrong offset is caught.
    fn make_payload(size: usize) -> Vec<u8> {
        (0..size)
            .map(|i| ((i.wrapping_mul(2654435761) >> 13) ^ i) as u8)
            .collect()
    }

    fn unique_path(tag: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/sc-e2e/{tag}_{}_{ts}.bin", std::process::id())
    }

    async fn write_blob(ctx: &Arc<FileSystemContext>, path: &str, payload: &[u8]) -> Result<()> {
        let master = ctx.acquire_master();
        let _ = master.create_directory("/sc-e2e", true).await;
        let _ = master.delete(path, false).await;
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(payload).await?;
        w.close().await?;
        Ok(())
    }

    fn sc_open_success() -> i64 {
        counter(name::CLIENT_SC_OPEN_SUCCESS).get()
    }
    fn sc_read_bytes() -> i64 {
        counter(name::CLIENT_SC_READ_BYTES).get()
    }
    fn sc_cache_hits() -> i64 {
        counter(name::CLIENT_SC_CACHE_HITS).get()
    }

    /// SC engages on a local worker and returns the exact written bytes.
    #[tokio::test]
    #[ignore]
    async fn short_circuit_serves_local_reads() -> Result<()> {
        let ctx = FileSystemContext::connect(base_config()).await?;
        let path = unique_path("local");
        let payload = make_payload(4 * 1024 * 1024);
        write_blob(&ctx, &path, &payload).await?;

        let open_before = sc_open_success();
        let bytes_before = sc_read_bytes();

        let mut s =
            GoosefsFileInStream::open_with_context(ctx.clone(), &path, OpenFileOptions::default())
                .await?;

        // Several positioned reads, including a page-crossing and a tail read.
        let cases: &[(i64, usize)] = &[
            (0, 4096),
            (4095, 4098),
            (1_000_003, 65536),
            ((payload.len() - 100) as i64, 100),
        ];
        let mut expected_bytes = 0i64;
        for &(off, len) in cases {
            let got = s.read_at(off, len).await?;
            assert_eq!(
                got.as_ref(),
                &payload[off as usize..off as usize + len],
                "byte mismatch at off={off} len={len} (INV-D2/S1)"
            );
            expected_bytes += len as i64;
        }

        // SC must have fired (local worker) — at least one OpenLocalBlock and
        // the SC byte counter advanced by at least the bytes we asked for.
        // (Lower bounds keep the assertion robust if other SC tests run
        // concurrently and bump the same process-global counters.)
        assert!(
            sc_open_success() > open_before,
            "short-circuit did not engage — is a LOCAL worker registered? \
             (sc_open_success did not advance)"
        );
        assert!(
            sc_read_bytes() - bytes_before >= expected_bytes,
            "SC byte counter should advance by at least the requested bytes"
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }

    /// INV-S1: the SC path and the gRPC path (SC kill switch off) return
    /// identical bytes for the same ranges.
    #[tokio::test]
    #[ignore]
    async fn short_circuit_matches_grpc() -> Result<()> {
        let payload = make_payload(2 * 1024 * 1024);

        // Write once with SC enabled.
        let ctx_sc = FileSystemContext::connect(base_config()).await?;
        let path = unique_path("eq");
        write_blob(&ctx_sc, &path, &payload).await?;

        // Read via SC.
        let mut s_sc = GoosefsFileInStream::open_with_context(
            ctx_sc.clone(),
            &path,
            OpenFileOptions::default(),
        )
        .await?;

        // Read via gRPC only (SC disabled).
        let mut grpc_cfg = base_config();
        grpc_cfg.short_circuit_enabled = false;
        let ctx_grpc = FileSystemContext::connect(grpc_cfg).await?;
        let mut s_grpc = GoosefsFileInStream::open_with_context(
            ctx_grpc.clone(),
            &path,
            OpenFileOptions::default(),
        )
        .await?;

        for &(off, len) in &[(0i64, 8192usize), (777, 33_000), (1_500_000, 200_000)] {
            let a = s_sc.read_at(off, len).await?;
            let b = s_grpc.read_at(off, len).await?;
            assert_eq!(a, b, "SC vs gRPC mismatch at off={off} len={len} (INV-S1)");
            assert_eq!(a.as_ref(), &payload[off as usize..off as usize + len]);
        }

        ctx_sc.acquire_master().delete(&path, false).await.ok();
        ctx_sc.close().await?;
        ctx_grpc.close().await?;
        Ok(())
    }

    /// Sequential `read_all()` is served by the short-circuit path and matches
    /// the source byte-for-byte.
    #[tokio::test]
    #[ignore]
    async fn short_circuit_sequential_read_all() -> Result<()> {
        let ctx = FileSystemContext::connect(base_config()).await?;
        let path = unique_path("seq");
        let payload = make_payload(3 * 1024 * 1024);
        write_blob(&ctx, &path, &payload).await?;

        let open_before = sc_open_success();
        let bytes_before = sc_read_bytes();

        let mut s =
            GoosefsFileInStream::open_with_context(ctx.clone(), &path, OpenFileOptions::default())
                .await?;
        let all = s.read_all().await?;

        assert_eq!(all.len(), payload.len(), "read_all length mismatch");
        assert_eq!(
            all.as_ref(),
            payload.as_slice(),
            "read_all bytes mismatch (INV-S1)"
        );

        // The sequential path is now served by SC: at least one open and the
        // SC byte counter advanced by at least the full file size.
        assert!(
            sc_open_success() > open_before,
            "short-circuit did not engage on the sequential path"
        );
        assert!(
            sc_read_bytes() - bytes_before >= payload.len() as i64,
            "SC byte counter should cover the whole sequential read"
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }

    /// P8: two streams built from the **same context** share the SC reader
    /// LRU, so a hot block is `OpenLocalBlock`+mmap'd once and the second
    /// stream's read is a cache hit (not a fresh open).
    #[tokio::test]
    #[ignore]
    async fn short_circuit_reader_shared_across_streams() -> Result<()> {
        let ctx = FileSystemContext::connect(base_config()).await?;
        let path = unique_path("shared");
        let payload = make_payload(2 * 1024 * 1024);
        write_blob(&ctx, &path, &payload).await?;

        let open_before = sc_open_success();
        let hits_before = sc_cache_hits();

        // Stream A reads block 0 → opens the reader.
        let mut a =
            GoosefsFileInStream::open_with_context(ctx.clone(), &path, OpenFileOptions::default())
                .await?;
        let ra = a.read_at(0, 4096).await?;
        assert_eq!(ra.as_ref(), &payload[..4096]);

        // Stream B (same context) reads the same block → shared-LRU hit, no
        // additional OpenLocalBlock.
        let mut b =
            GoosefsFileInStream::open_with_context(ctx.clone(), &path, OpenFileOptions::default())
                .await?;
        let rb = b.read_at(2048, 4096).await?;
        assert_eq!(rb.as_ref(), &payload[2048..2048 + 4096]);

        let opens = sc_open_success() - open_before;
        let hits = sc_cache_hits() - hits_before;
        // One open shared by both streams; B's read is a hit. Lower bounds keep
        // the assertion robust under concurrent SC tests.
        assert!(opens >= 1, "expected at least one OpenLocalBlock");
        assert!(
            hits >= 1,
            "second stream must reuse the shared reader (>=1 cache hit), got {hits}"
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }

    /// Per-block reader reuse: many reads of the same block trigger exactly one
    /// `OpenLocalBlock`; the rest are LRU cache hits.
    #[tokio::test]
    #[ignore]
    async fn short_circuit_reader_is_reused() -> Result<()> {
        let ctx = FileSystemContext::connect(base_config()).await?;
        let path = unique_path("reuse");
        let payload = make_payload(2 * 1024 * 1024);
        write_blob(&ctx, &path, &payload).await?;

        let open_before = sc_open_success();
        let hits_before = sc_cache_hits();

        let mut s =
            GoosefsFileInStream::open_with_context(ctx.clone(), &path, OpenFileOptions::default())
                .await?;

        let n_reads = 8;
        for k in 0..n_reads {
            let off = (k * 4096) as i64;
            let got = s.read_at(off, 4096).await?;
            assert_eq!(got.as_ref(), &payload[off as usize..off as usize + 4096]);
        }

        let opens = sc_open_success() - open_before;
        let hits = sc_cache_hits() - hits_before;
        // One open for the single block; the remaining reads are cache hits.
        // `hits >= n_reads-1` proves reuse (fresh opens would give 0 hits);
        // bounds tolerate concurrent SC tests sharing the global counters.
        assert!(opens >= 1, "expected at least one OpenLocalBlock");
        assert!(
            hits >= (n_reads - 1) as i64,
            "expected >= {} LRU reader-cache hits (reader reuse), got {hits}",
            n_reads - 1
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// GoosefsFileReader — streaming `read_next_block` path (OpenDAL / Lance).
// ═══════════════════════════════════════════════════════════════════════════
//
// The suite above validates short-circuit on `GoosefsFileInStream`. The one
// below validates that short-circuit is also wired into `GoosefsFileReader`'s
// per-block collection point `read_segment` (design §8.2), the path OpenDAL /
// Lance drive. The page cache is disabled here so every read flows straight
// through `read_file_range → read_segment → try_short_circuit_read`, isolating
// the SC path. Same local-worker requirement + `#[ignore]` policy as above.

#[cfg(test)]
mod reader_sc {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use goosefs_sdk::auth::AuthType;
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;
    use goosefs_sdk::error::Result;
    use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
    use goosefs_sdk::metrics::{counter, name};

    fn master_addr() -> String {
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
    }

    fn auth_type() -> AuthType {
        std::env::var("GOOSEFS_AUTH_TYPE")
            .ok()
            .and_then(|s| s.parse::<AuthType>().ok())
            .unwrap_or(AuthType::NoSasl)
    }

    /// SC **on** (default), page cache **off** — isolates the short-circuit path.
    fn sc_on_config() -> GoosefsConfig {
        let mut c = GoosefsConfig::new(master_addr());
        c.auth_type = auth_type();
        c.short_circuit_enabled = true;
        c.client_cache_enabled = false;
        c
    }

    /// SC **off** — forces the gRPC read path (reference for byte-equality).
    fn sc_off_config() -> GoosefsConfig {
        let mut c = GoosefsConfig::new(master_addr());
        c.auth_type = auth_type();
        c.short_circuit_enabled = false;
        c.client_cache_enabled = false;
        c
    }

    fn make_payload(size: usize) -> Vec<u8> {
        (0..size)
            .map(|i| ((i.wrapping_mul(2654435761) >> 13) ^ i) as u8)
            .collect()
    }

    fn unique_path(tag: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/reader-sc-e2e/{tag}_{}_{ts}.bin", std::process::id())
    }

    async fn write_blob(ctx: &Arc<FileSystemContext>, path: &str, payload: &[u8]) -> Result<()> {
        let master = ctx.acquire_master();
        let _ = master.create_directory("/reader-sc-e2e", true).await;
        let _ = master.delete(path, false).await;
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(payload).await?;
        w.close().await?;
        Ok(())
    }

    /// Read `[offset, offset+len)` through the streaming reader path.
    async fn read_range_via_reader(
        ctx: &Arc<FileSystemContext>,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<bytes::Bytes> {
        let mut r =
            GoosefsFileReader::open_range_with_context(ctx.clone(), path, offset, len).await?;
        r.read_all().await
    }

    fn sc_open_success() -> i64 {
        counter(name::CLIENT_SC_OPEN_SUCCESS).get()
    }
    fn sc_read_bytes() -> i64 {
        counter(name::CLIENT_SC_READ_BYTES).get()
    }

    /// §8.2 — short-circuit engages on a local worker **through the
    /// GoosefsFileReader read path** and returns the exact written bytes.
    ///
    /// Proves the reader's per-block collection point (`read_segment`) attempts
    /// SC and that a local block is served from mmap (SC counters advance).
    #[tokio::test]
    #[ignore]
    async fn reader_short_circuit_serves_local_reads() -> Result<()> {
        let ctx = FileSystemContext::connect(sc_on_config()).await?;
        let path = unique_path("local");
        let payload = make_payload(4 * 1024 * 1024);
        write_blob(&ctx, &path, &payload).await?;

        let open_before = sc_open_success();
        let bytes_before = sc_read_bytes();

        // Whole-file read via the streaming reader (loops read_next_block →
        // read_file_range → read_segment → short-circuit).
        let whole = GoosefsFileReader::read_file_with_context(ctx.clone(), &path).await?;
        assert_eq!(whole.len(), payload.len(), "whole read length");
        assert_eq!(
            whole.as_ref(),
            payload.as_slice(),
            "whole read bytes (§8.2)"
        );

        // A few range reads too, including a page-crossing and a tail read.
        let cases: &[(u64, u64)] = &[
            (0, 4096),
            (4095, 4098),
            (1_000_003, 65536),
            ((payload.len() - 100) as u64, 100),
        ];
        for &(off, len) in cases {
            let got = read_range_via_reader(&ctx, &path, off, len).await?;
            assert_eq!(
                got.as_ref(),
                &payload[off as usize..(off + len) as usize],
                "reader SC byte mismatch at off={off} len={len}"
            );
        }

        // SC must have fired via the reader path (local worker). Lower bounds
        // keep the assertion robust if other SC tests bump the same
        // process-global counters concurrently.
        assert!(
            sc_open_success() > open_before,
            "short-circuit did not engage via GoosefsFileReader — is a LOCAL worker \
             registered? (sc_open_success did not advance)"
        );
        assert!(
            sc_read_bytes() >= bytes_before + payload.len() as i64,
            "short-circuit byte counter did not advance by at least the whole-file \
             read via GoosefsFileReader"
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }

    /// The reader returns byte-for-byte identical data whether short-circuit is
    /// **on** (local mmap) or **off** (gRPC), on every interesting boundary
    /// (INV-S1 analogue for `GoosefsFileReader`). Cache is off on both sides so
    /// the only variable is the SC path.
    #[tokio::test]
    #[ignore]
    async fn reader_sc_vs_grpc_byte_diff() -> Result<()> {
        let payload = make_payload(10 * 1024 * 1024); // cross several blocks
        let ctx_sc = FileSystemContext::connect(sc_on_config()).await?;
        let ctx_grpc = FileSystemContext::connect(sc_off_config()).await?;

        let path = unique_path("d2");
        write_blob(&ctx_sc, &path, &payload).await?;

        let last = payload.len() as u64;
        let cases: &[(u64, u64)] = &[
            (0, 1),
            (0, 4096),
            (4095, 4098),
            ((1 << 20) - 7, 14),
            ((1 << 20) - 1, 1 << 20),
            (777, 33_000),
            (6 * 1024 * 1024, 200_000), // spans a 4 MiB block boundary
            (last - 1, 1),
            (last - 4096, 4096),
        ];

        for &(off, len) in cases {
            let sc = read_range_via_reader(&ctx_sc, &path, off, len).await?;
            let grpc = read_range_via_reader(&ctx_grpc, &path, off, len).await?;
            let expected = &payload[off as usize..(off + len) as usize];
            assert_eq!(
                grpc.as_ref(),
                expected,
                "gRPC reader bytes drift from source at off={off} len={len}"
            );
            assert_eq!(
                sc.as_ref(),
                expected,
                "SC reader bytes drift from source at off={off} len={len}"
            );
            assert_eq!(
                sc, grpc,
                "SC vs gRPC reader mismatch at off={off} len={len}"
            );
        }

        ctx_sc.acquire_master().delete(&path, false).await.ok();
        ctx_sc.close().await?;
        ctx_grpc.close().await?;
        Ok(())
    }
}
