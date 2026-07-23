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

//! Gating-grade short-circuit **INV-S3** (capability authorization) regression
//! suite — design `docs/SHORT_CIRCUIT_DESIGN.md`  / .
//!
//! ## What INV-S3 actually claims
//!
//! Whatever happens at the capability-authorization boundary on the Worker
//! side, the SDK promises three observables to the caller:
//!
//! | Sub-claim | Statement                                                        |
//! |-----------|------------------------------------------------------------------|
//! | S3-a      | When the cluster does *not* enforce capability, SC engages and   |
//! |           | returns the source bytes.                                        |
//! | S3-b      | The bytes returned by the SC path equal the bytes returned by    |
//! |           | the pure-gRPC path, regardless of whether capability is enforced.|
//! | S3-c      | When the cluster *does* enforce capability and the SDK has no    |
//! |           | provider wired in, the open is *either* accepted (Worker chose   |
//! |           | not to enforce on this code path) *or* rejected with a transient |
//! |           | failure that triggers a transparent fallback (INV-S1). Under no  |
//! |           | circumstance does the caller see a hard error or wrong bytes.    |
//!
//! Sub-claim S3-c is deliberately *observational* — the "right answer" depends
//! on the live Worker's authentication-mode-specific policy, which the SDK
//! cannot pin down. The test therefore asserts the **SDK contract** (bytes
//! match + no hard error surfaces) and records the actual Worker behaviour
//! via the SC counters, so the operator running the suite can read the log
//! and update the design doc / runbook accordingly.
//!
//! ## Running
//!
//! All cases require a running GooseFS cluster with a **local** worker and
//! are `#[ignore]`d so plain `cargo test` stays hermetic.
//!
//! ```bash
//! # On a SIMPLE / capability-enabled cluster (the realistic dev setup):
//! GOOSEFS_AUTH_TYPE=simple \
//!   cargo test --test sc_inv_s3 -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `--test-threads=1` is required: the cases read SC counters as observation
//! probes and concurrent runs would interleave their deltas.

#[cfg(test)]
mod inv_s3 {
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

    // ── Test harness (mirrors tests/sc_consistency.rs) ──────────────────────

    fn master_addr() -> String {
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
    }

    /// Default to SIMPLE here (not NoSasl): INV-S3 is only meaningful on a
    /// cluster that has an authentication mode where capability-enforcement
    /// is at least *configurable*. Override via `GOOSEFS_AUTH_TYPE` if needed.
    fn auth_type() -> AuthType {
        std::env::var("GOOSEFS_AUTH_TYPE")
            .ok()
            .and_then(|s| s.parse::<AuthType>().ok())
            .unwrap_or(AuthType::Simple)
    }

    fn sc_config() -> GoosefsConfig {
        let mut c = GoosefsConfig::new(master_addr());
        c.auth_type = auth_type();
        c.short_circuit_enabled = true;
        c.client_cache_enabled = false;
        c.block_size = 4 * 1024 * 1024;
        c
    }

    fn grpc_only_config() -> GoosefsConfig {
        let mut c = sc_config();
        c.short_circuit_enabled = false;
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
        format!("/sc-inv-s3/{tag}_{}_{ts}.bin", std::process::id())
    }

    async fn write_blob(ctx: &Arc<FileSystemContext>, path: &str, payload: &[u8]) -> Result<()> {
        let master = ctx.acquire_master();
        let _ = master.create_directory("/sc-inv-s3", true).await;
        let _ = master.delete(path, false).await;
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
        w.write(payload).await?;
        w.close().await?;
        Ok(())
    }

    async fn open_stream(ctx: &Arc<FileSystemContext>, path: &str) -> Result<GoosefsFileInStream> {
        GoosefsFileInStream::open_with_context(ctx.clone(), path, OpenFileOptions::default()).await
    }

    /// Snapshot of every SC counter we observe in this suite.
    #[derive(Clone, Copy, Debug)]
    struct ScCounters {
        opens: i64,
        open_local_fail: i64,
        file_open_fail: i64,
        mmap_fail: i64,
        read_calls: i64,
    }

    impl ScCounters {
        fn snapshot() -> Self {
            Self {
                opens: counter(name::CLIENT_SC_OPEN_SUCCESS).get(),
                open_local_fail: counter(name::CLIENT_SC_OPENLOCAL_FAIL).get(),
                file_open_fail: counter(name::CLIENT_SC_FILE_OPEN_FAIL).get(),
                mmap_fail: counter(name::CLIENT_SC_MMAP_FAIL).get(),
                read_calls: counter(name::CLIENT_SC_READ_CALLS).get(),
            }
        }

        /// Per-field delta `self - other` (saturating; counters are
        /// monotonic non-negative so saturation only kicks in if a
        /// snapshot was taken across a counter reset).
        fn delta(self, base: Self) -> Self {
            Self {
                opens: (self.opens - base.opens).max(0),
                open_local_fail: (self.open_local_fail - base.open_local_fail).max(0),
                file_open_fail: (self.file_open_fail - base.file_open_fail).max(0),
                mmap_fail: (self.mmap_fail - base.mmap_fail).max(0),
                read_calls: (self.read_calls - base.read_calls).max(0),
            }
        }
    }

    /// Boundary cases small enough to keep S3 cases under 1 MiB of read I/O.
    fn small_boundaries(size: usize) -> Vec<(i64, usize)> {
        let last = size as i64;
        vec![
            (0, 1),
            (0, 4096),
            (4095, 2),
            (4096, 4096),
            ((1 << 20) - 7, 14),
            (777, 33_000),
            (last - 1, 1),
            (last - 4096, 4096),
        ]
    }

    // ── INV-S3-b ─────────────────────────────────────────────────────────────

    /// **INV-S3-b** — bytes returned by the SC path equal the source payload
    /// **and** the bytes returned by the gRPC-only path, *regardless* of how
    /// the live Worker chose to handle the (currently always-empty)
    /// capability field on `OpenLocalBlock`.
    ///
    /// This is the strictest of the S3 sub-claims: a divergence here is a
    /// data-correctness bug, not an authorization-policy disagreement.
    #[tokio::test]
    #[ignore]
    async fn inv_s3_b_sc_vs_grpc_under_current_auth() -> Result<()> {
        let payload = make_payload(2 * 1024 * 1024 + 17);

        let ctx_sc = FileSystemContext::connect(sc_config()).await?;
        let path = unique_path("s3b");
        write_blob(&ctx_sc, &path, &payload).await?;

        let ctx_grpc = FileSystemContext::connect(grpc_only_config()).await?;

        let mut s_sc = open_stream(&ctx_sc, &path).await?;
        let mut s_grpc = open_stream(&ctx_grpc, &path).await?;

        for (off, len) in small_boundaries(payload.len()) {
            let a: Bytes = s_sc.read_at(off, len).await?;
            let b: Bytes = s_grpc.read_at(off, len).await?;
            let expected = &payload[off as usize..off as usize + len];

            assert_eq!(
                a.as_ref(),
                expected,
                "INV-S3-b: SC bytes drift from source at off={off} len={len}"
            );
            assert_eq!(
                b.as_ref(),
                expected,
                "INV-S3-b: gRPC bytes drift from source at off={off} len={len}"
            );
            assert_eq!(a, b, "INV-S3-b: SC vs gRPC mismatch at off={off} len={len}");
        }

        ctx_sc.acquire_master().delete(&path, false).await.ok();
        ctx_sc.close().await?;
        ctx_grpc.close().await?;
        Ok(())
    }

    // ── INV-S3-c ─────────────────────────────────────────────────────────────

    /// **INV-S3-c** — *observational* probe of the Worker's capability
    /// enforcement policy under the current cluster auth mode.
    ///
    /// `FileSystemContext` does not yet wire a `CapabilityProvider` into its
    /// shared `ShortCircuitFactory` (design , P3 deliberately external),
    /// so every `OpenLocalBlock` produced via the public SDK API today carries
    /// `capability = None` on the wire. We exploit this to probe the live
    /// Worker:
    ///
    /// 1. **Read once via the SC-enabled context** and snapshot the SC
    ///    counters. The counter delta tells us, post-hoc, which path the
    ///    Worker picked:
    ///
    ///    | Observed delta                              | Conclusion           |
    ///    |---------------------------------------------|----------------------|
    ///    | `opens > 0`, `open_local_fail == 0`         | Worker accepted (no  |
    ///    |                                             | capability enforced  |
    ///    |                                             | on this code path).  |
    ///    | `open_local_fail > 0` or `file_open_fail`   | Worker rejected; the |
    ///    |                                             | SDK's INV-S1         |
    ///    |                                             | fallback ran.        |
    ///
    /// 2. **In both branches**, the bytes returned must still equal the source
    ///    payload — that is the SDK's hard contract (INV-S1 + INV-D2 jointly
    ///    cover INV-S3-c). A wrong-bytes result here is a P0 regression.
    ///
    /// The case prints the observed delta with `--nocapture` so the operator
    /// can record the live-cluster verdict in the design doc / runbook.
    #[tokio::test]
    #[ignore]
    async fn inv_s3_c_probe_capability_enforcement() -> Result<()> {
        let payload = make_payload(512 * 1024 + 123);

        let ctx = FileSystemContext::connect(sc_config()).await?;
        let path = unique_path("s3c");
        write_blob(&ctx, &path, &payload).await?;

        let before = ScCounters::snapshot();

        let mut s = open_stream(&ctx, &path).await?;
        // Two reads, one positioned (forces a fresh OpenLocalBlock attempt
        // for the relevant block) and one whole-file (drains every block) —
        // makes sure the probe touches at least one OpenLocalBlock decision.
        let probe = s.read_at(0, payload.len().min(64 * 1024)).await?;
        assert_eq!(
            probe.as_ref(),
            &payload[..probe.len()],
            "INV-S3-c: positioned read returned wrong bytes — SDK contract violation"
        );
        drop(s);

        let mut s2 = open_stream(&ctx, &path).await?;
        let all = s2.read_all().await?;
        assert_eq!(
            all.as_ref(),
            payload.as_slice(),
            "INV-S3-c: whole-file read returned wrong bytes — SDK contract violation"
        );
        drop(s2);

        let after = ScCounters::snapshot();
        let d = after.delta(before);

        // Every attempt must have terminated in either "accepted" or
        // "rejected-and-fell-back-cleanly"; the suite never tolerates
        // observable failures other than these two.
        eprintln!(
            "INV-S3-c probe (capability=None on wire, auth_type={:?}):\n\
             \tCLIENT_SC_OPEN_SUCCESS    +{}\n\
             \tCLIENT_SC_OPENLOCAL_FAIL  +{}\n\
             \tCLIENT_SC_FILE_OPEN_FAIL  +{}\n\
             \tCLIENT_SC_MMAP_FAIL       +{}\n\
             \tCLIENT_SC_READ_CALLS      +{}",
            auth_type(),
            d.opens,
            d.open_local_fail,
            d.file_open_fail,
            d.mmap_fail,
            d.read_calls,
        );

        // Diagnostic verdict — not asserted, *recorded*. Kept as a single
        // print so a CI run produces a stable line operators can grep for.
        let verdict = if d.opens > 0 && d.open_local_fail == 0 {
            "ACCEPTED  — Worker did not reject capability=None on this auth mode"
        } else if d.open_local_fail > 0 || d.file_open_fail > 0 || d.mmap_fail > 0 {
            "REJECTED  — Worker rejected SC; INV-S1 fallback handled it"
        } else {
            // Neither counter moved: the SC decision rejected the block
            // *before* attempting OpenLocalBlock (e.g. router said the
            // source isn't local on this node, or the size gate cut in).
            // Bytes still came through gRPC — INV-S3-b above guarantees
            // correctness.
            "BYPASSED  — SC decision skipped this block (non-local / gated)"
        };
        eprintln!("INV-S3-c verdict: {verdict}");

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }

    // ── INV-S3-a ─────────────────────────────────────────────────────────────

    /// **INV-S3-a** — sanity case: when the SDK kill-switch is the *only*
    /// thing in play (SC enabled, no provider, default boundaries), at least
    /// one block-open must have *engaged* SC during a fresh read of a fresh
    /// file on a local-worker cluster. Regression guard against accidentally
    /// pushing a default config that disables SC entirely (e.g. via a stale
    /// `min_block_size` clamp).
    ///
    /// On a capability-enforcing cluster where every `OpenLocalBlock` is
    /// rejected this case will **legitimately fail** — that is itself useful
    /// signal: it forces the operator to either (a) wire a real
    /// `CapabilityProvider`, or (b) acknowledge in the runbook that on this
    /// cluster the SC path is permanently dark and should be killed via the
    /// `short_circuit_enabled` switch.
    #[tokio::test]
    #[ignore]
    async fn inv_s3_a_sc_engages_when_capability_not_enforced() -> Result<()> {
        let payload = make_payload(2 * 1024 * 1024);

        let ctx = FileSystemContext::connect(sc_config()).await?;
        let path = unique_path("s3a");
        write_blob(&ctx, &path, &payload).await?;

        let before = ScCounters::snapshot();
        let mut s = open_stream(&ctx, &path).await?;
        let bytes = s.read_all().await?;
        assert_eq!(
            bytes.as_ref(),
            payload.as_slice(),
            "INV-S3-a: bytes drift from source — SDK contract violation"
        );
        drop(s);
        let after = ScCounters::snapshot();
        let d = after.delta(before);

        assert!(
            d.opens > 0,
            "INV-S3-a: SC did not engage on a fresh local-worker read \
             (auth_type={:?}, opens delta = 0). \
             Either (a) the cluster rejects capability=None on every block \
             — wire a CapabilityProvider — or (b) no LOCAL worker is \
             registered on this node.",
            auth_type()
        );

        ctx.acquire_master().delete(&path, false).await.ok();
        ctx.close().await?;
        Ok(())
    }
}
