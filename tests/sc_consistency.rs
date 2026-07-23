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

//! Gating-grade short-circuit **consistency** regression suite.
//!
//! This is the test file referenced by `docs/SHORT_CIRCUIT_DESIGN.md`
//! item 5 (`cargo test --test sc_consistency`). Unlike the perf-oriented
//! `short_circuit_e2e.rs`, every case here is a hard byte-level invariant
//! check derived from :
//!
//! | Case                                | Invariant              |
//! |-------------------------------------|------------------------|
//! | `inv_d1_e2e_overwrite_visibility`   | INV-D1 (cross-reader)  |
//! | `inv_d2_sc_vs_grpc_byte_diff`       | INV-D2 (boundaries)    |
//! | `inv_s1_fallback_is_transparent`    | INV-S1 (fallback path) |
//! | `inv_s2_drop_releases_worker_lock`  | INV-S2 (RAII)          |
//! | `inv_s5_read_apis_are_equivalent`   | INV-S5 (3-API parity)  |
//!
//! Lower-level invariants (INV-D3/D4, INV-S4) are exercised by the
//! reader-layer unit tests in `src/block/short_circuit/reader.rs` (see
//! `read_bytes_outlives_reader`, `prefetch_does_not_change_bytes`,
//! `out_of_range_is_error`, ...). Within-reader INV-D1 (Worker holds
//! the OpenLocalBlock lock; block file content is immutable for the
//! lifetime of one reader) is a protocol invariant of GooseFS itself
//! and is asserted in the SDK indirectly via the SC-vs-gRPC byte diff
//! (INV-D2). The case here covers the *cross-reader* half of INV-D1:
//! a fresh stream opened after an overwrite must not reuse a stale
//! mmap / block-id view of the previous version.
//!
//! All cases require a running GooseFS cluster with a **local** worker
//! (so the SC path actually engages). They are `#[ignore]`d so plain
//! `cargo test` stays hermetic; run them explicitly:
//!
//! ```bash
//! GOOSEFS_AUTH_TYPE=nosasl \
//!   cargo test --test sc_consistency -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `--test-threads=1` keeps the SC counters (which we use as light-weight
//! sanity probes) clean across cases; byte-equality assertions hold
//! regardless.

#[cfg(test)]
mod consistency {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;

    use goosefs_sdk::auth::AuthType;
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;
    use goosefs_sdk::error::Result;
    use goosefs_sdk::fs::options::OpenFileOptions;
    use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
    use goosefs_sdk::metrics::{counter, name};

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

    /// Base config used by every consistency case.
    ///
    /// - SC enabled (each case may then build a sibling "SC off" config).
    /// - Page cache disabled — we want a clean SC↔gRPC comparison without
    ///   a third party serving bytes.
    /// - Small `block_size` so a moderately-sized payload exercises the
    ///   cross-block boundary on a developer cluster (default is 64 MiB
    ///   which would force >64 MiB payloads per case).
    fn sc_config() -> GoosefsConfig {
        let mut c = GoosefsConfig::new(master_addr());
        c.auth_type = auth_type();
        c.short_circuit_enabled = true;
        c.client_cache_enabled = false;
        c.block_size = 4 * 1024 * 1024; // 4 MiB blocks
        c
    }

    fn grpc_only_config() -> GoosefsConfig {
        let mut c = sc_config();
        c.short_circuit_enabled = false;
        c
    }

    /// Position-dependent payload — any wrong offset / length surfaces as a
    /// byte mismatch rather than `0 == 0` luck.
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
        format!("/sc-consistency/{tag}_{}_{ts}.bin", std::process::id())
    }

    async fn write_blob(ctx: &Arc<FileSystemContext>, path: &str, payload: &[u8]) -> Result<()> {
        let master = ctx.acquire_master();
        let _ = master.create_directory("/sc-consistency", true).await;
        let _ = master.delete(path, false).await;
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(payload).await?;
        w.close().await?;
        Ok(())
    }

    async fn open_stream(ctx: &Arc<FileSystemContext>, path: &str) -> Result<GoosefsFileInStream> {
        GoosefsFileInStream::open_with_context(ctx.clone(), path, OpenFileOptions::default()).await
    }

    fn sc_open_success() -> i64 {
        counter(name::CLIENT_SC_OPEN_SUCCESS).get()
    }

    /// Set of (offset, len) pairs that hit every consistency-relevant
    /// boundary for a payload of `size` bytes laid out in `block` -byte
    /// blocks. Keeps the case under a couple of MiB total reads.
    fn boundary_cases(size: usize, block: usize) -> Vec<(i64, usize)> {
        let last = size as i64;
        vec![
            // ── Trivial ────────────────────────────────────────────────
            (0, 1),
            (0, 4096),
            // ── Page boundary (4 KiB) ─────────────────────────────────
            (4095, 1),
            (4095, 2),
            (4096, 4096),
            (4095, 8194),
            // ── Sub-chunk straddle (chunk = 1 MiB) ────────────────────
            ((1 << 20) - 7, 14),
            ((1 << 20) - 1, 1 << 20),
            // ── Block boundary (`block` bytes) ────────────────────────
            ((block as i64) - 1, 2),
            ((block as i64) - 1, (block as i64 + 1) as usize),
            ((block as i64), 4096),
            // ── PR-style large random spread ──────────────────────────
            (777, 33_000),
            (3 * (block as i64) / 2, 200_000),
            // ── Tail ──────────────────────────────────────────────────
            (last - 1, 1),
            (last - 4096, 4096),
        ]
    }

    // ── INV-D2 ───────────────────────────────────────────────────────────────

    /// **INV-D2** — for the same physical block, the SC (mmap) path and the
    /// gRPC path return byte-for-byte identical data on every interesting
    /// boundary (page, chunk, block, tail). A divergence here is a
    /// data-correctness bug, not a performance regression.
    #[tokio::test]
    #[ignore]
    async fn inv_d2_sc_vs_grpc_byte_diff() -> Result<()> {
        // 10 MiB across ~3 × 4 MiB blocks → exercises cross-block reads.
        let payload = make_payload(10 * 1024 * 1024);
        let block = 4 * 1024 * 1024;

        let ctx_sc = FileSystemContext::connect(sc_config()).await?;
        let path = unique_path("d2");
        write_blob(&ctx_sc, &path, &payload).await?;

        let ctx_grpc = FileSystemContext::connect(grpc_only_config()).await?;

        let mut s_sc = open_stream(&ctx_sc, &path).await?;
        let mut s_grpc = open_stream(&ctx_grpc, &path).await?;

        for (off, len) in boundary_cases(payload.len(), block) {
            let a: Bytes = s_sc.read_at(off, len).await?;
            let b: Bytes = s_grpc.read_at(off, len).await?;
            let expected = &payload[off as usize..off as usize + len];

            assert_eq!(
                a.as_ref(),
                expected,
                "INV-D2: SC bytes drift from source at off={off} len={len}"
            );
            assert_eq!(
                b.as_ref(),
                expected,
                "INV-D2: gRPC bytes drift from source at off={off} len={len}"
            );
            assert_eq!(a, b, "INV-D2: SC vs gRPC mismatch at off={off} len={len}");
        }

        ctx_sc.acquire_master().delete(&path, false).await.ok();
        ctx_sc.close().await?;
        ctx_grpc.close().await?;
        Ok(())
    }

    // ── INV-S1 ───────────────────────────────────────────────────────────────

    /// **INV-S1** — when the SC path declines (here: by setting
    /// `short_circuit_min_block_size` larger than every block, which
    /// triggers the `should_use_short_circuit` size gate and forces the
    /// transparent fallback through the gRPC path), the bytes seen by the
    /// caller are exactly the same as both
    ///   (a) the source payload, and
    ///   (b) the SC-on path.
    ///
    /// Using the size gate (rather than killing SC outright) keeps the
    /// rest of the SC machinery alive and exercises the per-block
    /// fallback decision — which is the realistic shape of an in-flight
    /// recoverable failure.
    #[tokio::test]
    #[ignore]
    async fn inv_s1_fallback_is_transparent() -> Result<()> {
        // Match D2's payload size so the shared `boundary_cases(size, 4 MiB)`
        // generator stays in-bounds (it includes a `3*block/2 + 200_000 B`
        // case which requires payload >= ~6.2 MiB).
        let payload = make_payload(10 * 1024 * 1024);

        // Reference: SC enabled.
        let ctx_sc = FileSystemContext::connect(sc_config()).await?;
        let path = unique_path("s1");
        write_blob(&ctx_sc, &path, &payload).await?;

        // Fallback: SC compiled in but skipped per-block via the size gate.
        let mut fb_cfg = sc_config();
        fb_cfg.short_circuit_min_block_size = i64::MAX; // every block fails the gate
        let ctx_fb = FileSystemContext::connect(fb_cfg).await?;

        let mut s_sc = open_stream(&ctx_sc, &path).await?;
        let mut s_fb = open_stream(&ctx_fb, &path).await?;

        for (off, len) in boundary_cases(payload.len(), 4 * 1024 * 1024) {
            let a = s_sc.read_at(off, len).await?;
            let b = s_fb.read_at(off, len).await?;
            let expected = &payload[off as usize..off as usize + len];
            assert_eq!(
                a.as_ref(),
                expected,
                "INV-S1: SC drift at off={off} len={len}"
            );
            assert_eq!(
                b.as_ref(),
                expected,
                "INV-S1: fallback drift at off={off} len={len}"
            );
            assert_eq!(
                a, b,
                "INV-S1: SC and fallback returned different bytes at off={off} len={len}"
            );
        }

        ctx_sc.acquire_master().delete(&path, false).await.ok();
        ctx_sc.close().await?;
        ctx_fb.close().await?;
        Ok(())
    }

    // ── INV-S2 ───────────────────────────────────────────────────────────────

    /// **INV-S2** — dropping a stream releases the worker-side lock held by
    /// `OpenLocalBlock`. We can't introspect the worker's lock table from
    /// the client, but we can verify the *observable* contract: after a
    /// stream is dropped, opening a brand-new stream on the same file
    /// continues to (a) engage SC and (b) return identical bytes. If the
    /// previous reader had leaked its lock, the worker would either
    /// reject the new `OpenLocalBlock` or stall it; a passing run rules
    /// both out across many open/drop cycles.
    #[tokio::test]
    #[ignore]
    async fn inv_s2_drop_releases_worker_lock() -> Result<()> {
        let payload = make_payload(2 * 1024 * 1024);
        let ctx = FileSystemContext::connect(sc_config()).await?;
        let path = unique_path("s2");
        write_blob(&ctx, &path, &payload).await?;

        let opens_before = sc_open_success();

        for i in 0..8 {
            let mut s = open_stream(&ctx, &path).await?;
            let off = (i as i64 * 4096) % (payload.len() as i64 - 8192);
            let got = s.read_at(off, 8192).await?;
            assert_eq!(
                got.as_ref(),
                &payload[off as usize..off as usize + 8192],
                "INV-S2: byte mismatch on iteration {i}"
            );
            // Explicit drop — RAII guard must release the worker lock.
            drop(s);
        }

        // SC engaged at least once across the cycles. (Not strictly
        // required for the lock-release claim, but a cheap sanity probe
        // that we are in fact on the SC path during this test.)
        assert!(
            sc_open_success() > opens_before,
            "INV-S2: SC did not engage during the drop/reopen cycle — \
             is a LOCAL worker registered?"
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }

    // ── INV-S5 ───────────────────────────────────────────────────────────────

    /// **INV-S5** — the three public read APIs on `GoosefsFileInStream`
    /// (`read` sequential, `read_at` positioned, `read_all` whole-file)
    /// return identical bytes for the same logical input. The
    /// reader-layer counterpart (`read` / `read_bytes` / `read_to_slice`
    /// at the SC reader API surface) is covered by a unit test in
    /// `src/block/short_circuit/reader.rs`.
    #[tokio::test]
    #[ignore]
    async fn inv_s5_read_apis_are_equivalent() -> Result<()> {
        let payload = make_payload(3 * 1024 * 1024 + 7);
        let ctx = FileSystemContext::connect(sc_config()).await?;
        let path = unique_path("s5");
        write_blob(&ctx, &path, &payload).await?;

        // ── (1) read_all ─────────────────────────────────────────────
        let mut s_all = open_stream(&ctx, &path).await?;
        let all = s_all.read_all().await?;
        assert_eq!(all.len(), payload.len(), "INV-S5: read_all length");
        assert_eq!(
            all.as_ref(),
            payload.as_slice(),
            "INV-S5: read_all bytes drift from source"
        );

        // ── (2) read (sequential) ────────────────────────────────────
        // Drain in heterogeneous chunk sizes that intentionally don't
        // align with the SDK chunk size, so any chunk-boundary handling
        // bug surfaces.
        let mut s_seq = open_stream(&ctx, &path).await?;
        let mut seq_buf = Vec::with_capacity(payload.len());
        let chunks: [usize; 5] = [37, 4096, 33_333, 1 << 20, 65_521];
        let mut ci = 0usize;
        let mut tmp = vec![0u8; chunks.iter().copied().max().unwrap()];
        loop {
            let want = chunks[ci % chunks.len()].min(tmp.len());
            ci += 1;
            let n = s_seq.read(&mut tmp[..want]).await?;
            if n == 0 {
                break;
            }
            seq_buf.extend_from_slice(&tmp[..n]);
        }
        assert_eq!(seq_buf.len(), payload.len(), "INV-S5: read drained length");
        assert_eq!(
            seq_buf.as_slice(),
            payload.as_slice(),
            "INV-S5: sequential read bytes drift from source"
        );
        assert_eq!(seq_buf.as_slice(), all.as_ref(), "INV-S5: read != read_all");

        // ── (3) read_at (positioned) ────────────────────────────────
        // Reconstruct the file via positioned reads only and compare.
        let mut s_pr = open_stream(&ctx, &path).await?;
        let mut pr_buf = Vec::with_capacity(payload.len());
        let mut off = 0i64;
        let step: usize = 257 * 1024; // odd, prime-ish — straddles every boundary
        while (off as usize) < payload.len() {
            let want = step.min(payload.len() - off as usize);
            let got = s_pr.read_at(off, want).await?;
            assert_eq!(
                got.len(),
                want,
                "INV-S5: read_at short read at off={off} want={want}"
            );
            pr_buf.extend_from_slice(got.as_ref());
            off += want as i64;
        }
        assert_eq!(
            pr_buf.as_slice(),
            payload.as_slice(),
            "INV-S5: read_at bytes drift from source"
        );
        assert_eq!(
            pr_buf.as_slice(),
            all.as_ref(),
            "INV-S5: read_at != read_all"
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }

    // ── INV-D1 (cross-reader) ────────────────────────────────────────────────

    /// **INV-D1 (cross-reader half)** — within a single reader, GooseFS's
    /// OpenLocalBlock lock guarantees the underlying block file is
    /// immutable; that half is asserted indirectly through the SC vs
    /// gRPC byte diff (INV-D2). This case covers the complementary
    /// guarantee: when the file is **overwritten between streams**, a
    /// freshly opened SC stream must not reuse a stale mmap / block-id
    /// view of the previous version.
    ///
    /// Three phases per overwrite:
    ///   1. Write v(n), open a stream, read it warm, drop the stream.
    ///   2. Overwrite to v(n+1). The two payloads have **distinct
    ///      lengths and biased bytes** so that any stale-view leak
    ///      (whole or prefix) shows up as a mismatch.
    ///   3. Open a brand-new stream and assert
    ///        - new length is observed (no truncation to v(n).len()),
    ///        - all bytes match v(n+1) exactly,
    ///        - SC actually engaged on the post-overwrite read (a
    ///          fallback-only success would mask a real SC bug).
    ///
    /// The third assertion is a sanity probe via `CLIENT_SC_OPEN_SUCCESS`
    /// rather than a hard contract: if no LOCAL worker is registered,
    /// the test prints a hint and skips the counter check, but byte
    /// equality is still mandatory.
    #[tokio::test]
    #[ignore]
    async fn inv_d1_e2e_overwrite_visibility() -> Result<()> {
        let ctx = FileSystemContext::connect(sc_config()).await?;
        let path = unique_path("d1e2e");

        // Same length first (catches block-id reuse with identical layout),
        // then different length (catches stale length / cached file size).
        let v1 = make_payload(2 * 1024 * 1024 + 11);
        let v2 = {
            let mut p = make_payload(v1.len());
            for b in &mut p {
                *b = b.wrapping_add(0xA5);
            }
            p
        };
        let v3 = make_payload(3 * 1024 * 1024 + 777); // different length

        // ── Phase 1: write v1, read warm, drop. ──────────────────────────
        write_blob(&ctx, &path, &v1).await?;
        {
            let mut s = open_stream(&ctx, &path).await?;
            let warm = s.read_all().await?;
            assert_eq!(
                warm.as_ref(),
                v1.as_slice(),
                "INV-D1-E2E (phase 1): initial read drifted from v1"
            );
        }

        // ── Phase 2: overwrite to v2 (same length, bytes shifted). ───────
        write_blob(&ctx, &path, &v2).await?;
        let opens_before_v2 = sc_open_success();
        {
            let mut s = open_stream(&ctx, &path).await?;
            let observed = s.read_all().await?;
            assert_eq!(
                observed.len(),
                v2.len(),
                "INV-D1-E2E (phase 2): length mismatch after same-length overwrite"
            );
            assert_eq!(
                observed.as_ref(),
                v2.as_slice(),
                "INV-D1-E2E (phase 2): served stale v1 bytes after overwrite to v2 \
                 (same length — likely block-id reuse leaking a stale mmap view)"
            );

            // Cross-check via positioned read across a sub-chunk boundary.
            let mut s_pr = open_stream(&ctx, &path).await?;
            let off = ((1 << 20) - 7) as i64;
            let len = 2 * (1 << 20);
            let len = len.min(v2.len() - off as usize);
            let got = s_pr.read_at(off, len).await?;
            assert_eq!(
                got.as_ref(),
                &v2[off as usize..off as usize + len],
                "INV-D1-E2E (phase 2): read_at drifted from v2 across chunk boundary"
            );
        }
        let opens_after_v2 = sc_open_success();

        // ── Phase 3: overwrite to v3 (different length). ─────────────────
        write_blob(&ctx, &path, &v3).await?;
        {
            let mut s = open_stream(&ctx, &path).await?;
            let observed = s.read_all().await?;
            assert_eq!(
                observed.len(),
                v3.len(),
                "INV-D1-E2E (phase 3): length still matches v2 after overwrite \
                 to a longer v3 — file size is being cached across streams"
            );
            assert_eq!(
                observed.as_ref(),
                v3.as_slice(),
                "INV-D1-E2E (phase 3): served stale v2 bytes after overwrite to v3"
            );
        }

        // SC engagement sanity probe: across the post-overwrite reads
        // we expect at least one new successful SC open. If no LOCAL
        // worker is registered, every read fell back to gRPC — that's
        // a deployment shape, not a correctness bug, so we only print
        // a hint instead of failing.
        if opens_after_v2 <= opens_before_v2 {
            eprintln!(
                "INV-D1-E2E: no SC engagement observed across the overwrite \
                 reads — is a LOCAL worker registered? Byte-equality \
                 assertions still passed, so the data-plane contract holds; \
                 the SC-engagement probe is being skipped."
            );
        }

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }
}
