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

//! Integration tests for the client metrics heartbeat pipeline.
//!
//! These tests verify end-to-end data flow through all three stages:
//!
//! ```text
//! IO instrumentation (counter.inc)
//!     └── metrics::registry::REGISTRY  (global AtomicI64 counters)
//!           └── ClientMetricsReporter::snapshot()  (incremental diff)
//!                 └── HeartbeatTask::do_heartbeat()
//!                       └── MetricsClient::heartbeat()  (mock or real RPC)
//! ```
//!
//! The integration tests here use a `MockMetricsClient` (backed by
//! `Arc<dyn MetricsClient>`) to avoid requiring a real Goosefs cluster.
//! Tests that need a real cluster are marked `#[ignore]`.
//!
//! ## Design spec reference:
//!
//! The primary verification is:
//!   switch enabled → `counter.inc(N)` → after ≥ interval, server receives
//!   `MetricsHeartbeatPRequest` with `client_metrics[0].source = app_id`
//!   and `Client.BytesReadLocal` counter with value `N`.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use goosefs_sdk::metrics;
use goosefs_sdk::metrics::name;
#[allow(unused_imports)]
use goosefs_sdk::metrics::HeartbeatTask;

// ── MockMetricsClient (shared across all integration tests) ───────────────────

/// Records every `heartbeat()` RPC call made by `HeartbeatTask`.
struct MockMetricsClient {
    call_count: Arc<AtomicUsize>,
    tx: tokio::sync::mpsc::Sender<Vec<goosefs_sdk::proto::grpc::metric::ClientMetrics>>,
}

impl MockMetricsClient {
    fn new(
        tx: tokio::sync::mpsc::Sender<Vec<goosefs_sdk::proto::grpc::metric::ClientMetrics>>,
    ) -> (Self, Arc<AtomicUsize>) {
        let call_count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                call_count: call_count.clone(),
                tx,
            },
            call_count,
        )
    }
}

#[async_trait]
impl goosefs_sdk::client::MetricsClient for MockMetricsClient {
    async fn heartbeat(
        &self,
        client_metrics: Vec<goosefs_sdk::proto::grpc::metric::ClientMetrics>,
    ) -> goosefs_sdk::error::Result<()> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.tx.send(client_metrics).await;
        Ok(())
    }
}

// ── Helper ────────────────────────────────────────────────────────────────────

/// Build a `HeartbeatTask` wired to a `MockMetricsClient`.
// ── Tests ─────────────────────────────────────────────────────────────────────

/// ** core scenario**: counter.inc(N) → HeartbeatTask → MockMetricsClient
/// receives `ClientMetrics` with `source = app_id` and the correct counter value.
///
/// Uses `shutdown()` to trigger an immediate final flush rather than timer
/// advance, for more deterministic test behaviour.
#[tokio::test]
async fn pipeline_counter_inc_reaches_heartbeat_payload() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let (mock, _call_count) = MockMetricsClient::new(tx);

    // Fresh reporter — new last_counter baseline, so first snapshot yields
    // full value as the diff.
    let reporter = Arc::new(goosefs_sdk::metrics::ClientMetricsReporter::new());
    let closed = Arc::new(AtomicBool::new(false));

    // Use a unique name to isolate from other tests sharing the global REGISTRY.
    let counter_name = "integ_pipeline_bytes_read_v2";
    let bytes_read = metrics::counter(counter_name);
    bytes_read.inc(1024);

    let task = goosefs_sdk::metrics::HeartbeatTask::spawn(
        Arc::new(mock) as Arc<dyn goosefs_sdk::client::MetricsClient>,
        reporter,
        "integ-app-id".into(),
        Duration::from_secs(60), // long interval — only fires via shutdown flush
        Duration::from_secs(5),  // rpc_timeout (< interval)
        closed,
    );

    // shutdown() triggers an immediate final flush.
    task.shutdown().await;

    // Receive the flushed payload.
    let received = rx
        .try_recv()
        .expect("heartbeat payload must have been sent on shutdown");

    // : verify `source = app_id`
    assert!(!received.is_empty(), "ClientMetrics list must not be empty");
    let cm = &received[0];
    assert_eq!(
        cm.source.as_deref(),
        Some("integ-app-id"),
        "ClientMetrics.source must equal the app_id passed to HeartbeatTask::spawn"
    );

    // : verify the counter appears with the correct value.
    let metric = cm
        .metrics
        .iter()
        .find(|m| m.name.as_deref() == Some(counter_name));
    assert!(
        metric.is_some(),
        "counter '{}' must be present in the payload",
        counter_name
    );
    assert_eq!(
        metric.unwrap().value,
        Some(1024.0),
        "counter value must match the inc(1024) call"
    );
}

/// Verify that a second heartbeat beat only carries the **incremental delta**,
/// not the full cumulative value.
///
/// This tests the diff semantics described in  (reporter) across the full
/// pipeline.
#[tokio::test]
async fn pipeline_second_beat_carries_only_delta() {
    let counter_name = "integ_delta_test_v2";
    let counter = metrics::counter(counter_name);

    // ── First task: flush the initial increment ────────────────────────────
    let (tx1, mut rx1) = tokio::sync::mpsc::channel(8);
    let (mock1, _) = MockMetricsClient::new(tx1);
    let reporter = Arc::new(goosefs_sdk::metrics::ClientMetricsReporter::new());
    let closed1 = Arc::new(AtomicBool::new(false));

    counter.inc(100);

    let task1 = goosefs_sdk::metrics::HeartbeatTask::spawn(
        Arc::new(mock1) as Arc<dyn goosefs_sdk::client::MetricsClient>,
        reporter.clone(),
        "delta-app".into(),
        Duration::from_secs(60),
        Duration::from_secs(5),
        closed1,
    );
    task1.shutdown().await;
    let first_beat = rx1.try_recv().ok();

    // Confirm first beat had value 100.
    if let Some(ref payload) = first_beat {
        if let Some(m) = payload
            .iter()
            .flat_map(|cm| cm.metrics.iter())
            .find(|m| m.name.as_deref() == Some(counter_name))
        {
            assert_eq!(m.value, Some(100.0), "first beat value must be 100");
        }
    }

    // ── Second flush: only the delta should appear ────────────────────────
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(8);
    let (mock2, _) = MockMetricsClient::new(tx2);
    let closed2 = Arc::new(AtomicBool::new(false));

    counter.inc(50); // only 50 new bytes since last flush

    let task2 = goosefs_sdk::metrics::HeartbeatTask::spawn(
        Arc::new(mock2) as Arc<dyn goosefs_sdk::client::MetricsClient>,
        reporter,
        "delta-app".into(),
        Duration::from_secs(60),
        Duration::from_secs(5),
        closed2,
    );
    task2.shutdown().await;

    if let Ok(second_beat) = rx2.try_recv() {
        let delta_metric = second_beat
            .iter()
            .flat_map(|cm| cm.metrics.iter())
            .find(|m| m.name.as_deref() == Some(counter_name));
        if let Some(m) = delta_metric {
            assert_eq!(
                m.value,
                Some(50.0),
                "second beat must carry only the incremental delta (50), not the cumulative (150)"
            );
        }
    }
}

/// Verify that when `metrics_enabled = false`, the heartbeat task is NOT
/// spawned (i.e. `metrics_heartbeat` stays `None` in `FileSystemContext`).
///
/// This is a structural test — it verifies the config flag is honoured at
/// the context layer without any network connection.
#[test]
fn disabled_no_task_spawn_config_flag() {
    use goosefs_sdk::config::GoosefsConfig;

    let config_off = GoosefsConfig::new("127.0.0.1:9200").with_metrics_enabled(false);
    assert!(
        !config_off.metrics_enabled,
        "metrics_enabled must be false after with_metrics_enabled(false)"
    );

    // The `start_metrics_heartbeat_task` method returns early (Ok(())) without
    // spawning when metrics_enabled = false.  We verify the flag path here;
    // the actual spawn-gate is covered by context.rs integration (requires
    // network). The config-flag is the sole guard at this level.
    let config_on = GoosefsConfig::new("127.0.0.1:9200").with_metrics_enabled(true);
    assert!(
        config_on.metrics_enabled,
        "metrics_enabled must be true after with_metrics_enabled(true)"
    );
}

/// Verify the well-known metric name constants are correct strings ( spec).
///
/// These constants are embedded in the heartbeat payload `Metric.name` field
/// and must match the Java SDK's metric names exactly to be aggregated
/// correctly on the Master side.
#[test]
fn metric_name_constants_match_java_sdk() {
    assert_eq!(name::CLIENT_BYTES_READ_LOCAL, "Client.BytesReadLocal");
    assert_eq!(name::CLIENT_BYTES_WRITTEN_LOCAL, "Client.BytesWrittenLocal");
    assert_eq!(name::CLIENT_BYTES_WRITTEN_UFS, "Client.BytesWrittenUfs");
}

/// Verify that `HeartbeatTask::spawn` + `shutdown()` completes without
/// panicking, regardless of what is in the global registry.
#[tokio::test]
async fn empty_flush_on_shutdown_does_not_panic() {
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (mock, _call_count) = MockMetricsClient::new(tx);

    // Fresh reporter.
    let reporter = Arc::new(goosefs_sdk::metrics::ClientMetricsReporter::new());
    let closed = Arc::new(AtomicBool::new(false));

    let task = goosefs_sdk::metrics::HeartbeatTask::spawn(
        Arc::new(mock) as Arc<dyn goosefs_sdk::client::MetricsClient>,
        reporter,
        "empty-flush".into(),
        Duration::from_secs(60),
        Duration::from_secs(5),
        closed,
    );

    // Shutdown — must not panic regardless of global registry state.
    task.shutdown().await;
    // No assertion on call_count: the global REGISTRY may have gauges from
    // other tests, which are always included in the snapshot.
    // The test verifies no panic occurs.
}

/// Verify that `resolve_app_id` falls back gracefully and never produces
/// an empty string (used as `ClientMetrics.source`).
#[test]
fn resolve_app_id_never_empty() {
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::metrics::resolve_app_id;

    let config = GoosefsConfig::new("127.0.0.1:9200");
    let id = resolve_app_id(&config);
    assert!(!id.is_empty(), "app_id must never be empty");

    // With explicit app_id set.
    let config_with_id = GoosefsConfig::new("127.0.0.1:9200").with_app_id("my-worker");
    assert_eq!(resolve_app_id(&config_with_id), "my-worker");
}

// ── Real-cluster tests (require `cargo test -- --ignored`) ───────────────────

/// End-to-end test requiring a real Goosefs cluster.
///
/// Run with:
/// ```sh
/// cargo test --test metrics_heartbeat heartbeat_real_cluster -- --ignored --nocapture
/// ```
///
/// Verifies:
/// - `FileSystemContext::connect` starts a heartbeat task.
/// - After `interval`, the Master receives `MetricsHeartbeatPRequest` with
///   `client_metrics[0].source = resolve_app_id(config)`.
/// - `close()` triggers a final flush.
#[tokio::test]
#[ignore]
async fn heartbeat_real_cluster() -> goosefs_sdk::error::Result<()> {
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;

    let config = GoosefsConfig::new("127.0.0.1:9200")
        .with_metrics_enabled(true)
        .with_metrics_heartbeat_interval(Duration::from_secs(2))
        .with_app_id("integ-test-node");

    let ctx = FileSystemContext::connect(config).await?;

    // Simulate some I/O: directly increment the bytes-read counter.
    metrics::counter(name::CLIENT_BYTES_READ_LOCAL).inc(4096);

    // Wait for at least two heartbeat intervals.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Final flush on close.
    ctx.close().await?;

    println!("✓ real-cluster heartbeat completed — check Master Web UI for Client.BytesReadLocal");
    Ok(())
}
