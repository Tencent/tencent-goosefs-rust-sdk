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

//! Client metrics reporter: snapshot + incremental diff → proto Metric list.
//!
//! Aligned with Java `MetricsSystem.reportMetrics()` (MetricsSystem.java:L636-L702)
//! and `MetricsSystem.LAST_REPORTED_METRICS` (MetricsSystem.java:L76).
//!
//! ## Diff semantics
//!
//! For each Counter in the global registry, the reporter tracks the last-reported
//! value and emits only the **delta** (`cur - prev`). On the very first snapshot
//! of a counter, the full current value is treated as the delta (i.e. `prev = 0`).
//!
//! Java reference (MetricsSystem.java:L663-L671):
//! ```text
//! Long prev = LAST_REPORTED_METRICS.replace(key, value);
//! if (prev == null) {
//!     LAST_REPORTED_METRICS.put(key, value);  // first time: record baseline
//! }
//! double diff = (prev != null) ? value - prev : value;
//! if (diff != 0) { rpcMetrics.add(...) }
//! ```
//!
//! Critical invariant: **the baseline is always written, even when diff == 0**.
//! This prevents the next snapshot from treating a zero-delta counter as if it
//! were brand new and double-counting.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::metrics::registry::get_registry;
use crate::proto::grpc::{Metric, MetricType};

/// Reports client metrics to the Master via periodic heartbeat.
///
/// Maintains a per-counter baseline (`last_counter`) so that each heartbeat
/// carries only the **incremental** bytes since the last report. Gauges are
/// always reported at their current value without diffing.
pub struct ClientMetricsReporter {
    /// Previous counter values, keyed by metric name.
    /// Protected by a `std::sync::Mutex` because snapshot() is called from a
    /// single background tokio task — there is no contention in practice, so a
    /// lightweight stdlib mutex is preferred over `tokio::sync::Mutex`.
    last_counter: Mutex<HashMap<String, i64>>,
}

impl Default for ClientMetricsReporter {
    fn default() -> Self {
        Self {
            last_counter: Mutex::new(HashMap::new()),
        }
    }
}

impl ClientMetricsReporter {
    pub fn new() -> Self {
        Self {
            last_counter: Mutex::new(HashMap::new()),
        }
    }

    /// Capture a snapshot of all registered metrics, computing incremental
    /// deltas for counters.
    ///
    /// Returns a `Vec<Metric>` ready to be placed inside a
    /// `MetricsHeartbeatPOptions.client_metrics` list. Returns an empty Vec
    /// when there is nothing to report (all deltas are zero and no gauges
    /// exist), which callers should use to skip the RPC entirely (aligned
    /// with Java `ClientMasterSync.heartbeat():L90-L93`).
    pub fn snapshot(&self) -> Vec<Metric> {
        let mut out = Vec::new();

        // --- Counters: report incremental diff ---
        //
        // We must hold last_counter for the entire counter sweep to guarantee
        // that the baseline we write corresponds exactly to the value we read
        // from the atomic — there is no window for another snapshot() call to
        // interleave (single background task).
        let mut last = self.last_counter.lock().unwrap();

        let registry = get_registry();
        for entry in registry.counters.iter() {
            let name = entry.key().clone();
            let cur = entry.value().get();

            let diff = match last.get(&name) {
                Some(&prev) => cur - prev, // subsequent snapshot: report delta
                None => cur,               // first snapshot: full value is the diff
            };

            // Always write the baseline, even when diff == 0.
            // This is the critical invariant from Java L667-L668: recording the
            // baseline ensures the *next* snapshot computes the correct delta
            // rather than treating this counter as brand new.
            last.insert(name.clone(), cur);

            if diff != 0 {
                out.push(Metric {
                    // `instance` must equal a Java `MetricsSystem.InstanceType`
                    // enum value (case-insensitive). For client-side reporting
                    // this is always "Client". The Master rejects the whole
                    // heartbeat with `IllegalArgumentException: No constant
                    // with text  found` if this is left empty.
                    instance: Some(INSTANCE_CLIENT.to_string()),
                    // `source` is filled in by the caller (HeartbeatTask) from
                    // the resolved `app_id` so that every Metric carries the
                    // same per-process identifier the Master uses as a key.
                    // `MetricsStore.putReportedMetrics` silently drops any
                    // Metric whose `source` is null, so this MUST be non-empty
                    // by the time the RPC is issued.
                    source: None,
                    // `name` must be the bare metric name (without the
                    // `Client.` prefix). Java's `MetricsStore` registers its
                    // ClusterCounterKey with `MetricKey.getMetricName()`
                    // which strips the instance prefix; matching that exactly
                    // is required for the master-side counter lookup to hit.
                    name: Some(strip_instance_prefix(&name).to_string()),
                    value: Some(diff as f64),
                    // metric_type is a required i32 field; use the enum discriminant.
                    metric_type: MetricType::Counter as i32,
                    tags: Default::default(),
                });
            }
        }

        // --- Gauges: report current value directly (no diff) ---
        for entry in registry.gauges.iter() {
            out.push(Metric {
                instance: Some(INSTANCE_CLIENT.to_string()),
                name: Some(strip_instance_prefix(entry.key()).to_string()),
                value: Some(entry.value().get() as f64),
                metric_type: MetricType::Gauge as i32,
                ..Default::default()
            });
        }

        out
    }
}

/// Java `MetricsSystem.InstanceType.CLIENT.toString()` value.
/// The Master matches this case-insensitively via `InstanceType.fromString`.
const INSTANCE_CLIENT: &str = "Client";

/// Strip the leading `<InstanceType>.` prefix from a fully-qualified metric
/// name, mirroring Java `MetricKey.getMetricName()`:
///
/// ```text
/// "Client.BytesReadLocal" -> "BytesReadLocal"
/// "Client.BytesWrittenLocal" -> "BytesWrittenLocal"
/// "BytesReadLocal" -> "BytesReadLocal"   (no prefix: returned as-is)
/// ```
///
/// This is required because the Master's `MetricsStore` indexes its cluster
/// counter table by the bare metric name (per `MetricsStore.initCounterKeys`
/// which uses `MetricKey.getMetricName()`).
fn strip_instance_prefix(full: &str) -> &str {
    match full.split_once('.') {
        Some((_instance, rest)) => rest,
        None => full,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::registry::{counter, gauge, name};

    // Each test uses unique metric names to avoid cross-test interference
    // with the global REGISTRY singleton.

    #[test]
    fn counter_first_snapshot_reports_full_value() {
        let reporter = ClientMetricsReporter::default();
        let c = counter("test_reporter_first_snap");
        c.inc(100);

        let snap = reporter.snapshot();
        let found = snap
            .iter()
            .find(|m| m.name.as_deref() == Some("test_reporter_first_snap"));
        assert!(found.is_some(), "counter should appear in first snapshot");
        assert_eq!(found.unwrap().value, Some(100.0));
        assert_eq!(found.unwrap().metric_type, MetricType::Counter as i32);
    }

    #[test]
    fn counter_diff_correct_between_snapshots() {
        let reporter = ClientMetricsReporter::default();
        let c = counter("test_reporter_diff");
        c.inc(50);

        // First snapshot: diff = 50 (full value)
        let snap1 = reporter.snapshot();
        let v1 = snap1
            .iter()
            .find(|m| m.name.as_deref() == Some("test_reporter_diff"))
            .and_then(|m| m.value);
        assert_eq!(v1, Some(50.0));

        // Increment again
        c.inc(30);

        // Second snapshot: diff = 30 (incremental only)
        let snap2 = reporter.snapshot();
        let v2 = snap2
            .iter()
            .find(|m| m.name.as_deref() == Some("test_reporter_diff"))
            .and_then(|m| m.value);
        assert_eq!(v2, Some(30.0));
    }

    #[test]
    fn counter_unchanged_not_in_snapshot() {
        let reporter = ClientMetricsReporter::default();
        let c = counter("test_reporter_unchanged");
        c.inc(10);

        // First snapshot: counter appears (diff = 10)
        let snap1 = reporter.snapshot();
        assert!(snap1
            .iter()
            .any(|m| m.name.as_deref() == Some("test_reporter_unchanged")));

        // No increment between snapshots
        // Second snapshot: diff = 0, counter must NOT appear
        let snap2 = reporter.snapshot();
        assert!(
            !snap2
                .iter()
                .any(|m| m.name.as_deref() == Some("test_reporter_unchanged")),
            "unchanged counter must not appear in snapshot"
        );
    }

    #[test]
    fn counter_zero_from_start_not_in_snapshot() {
        let reporter = ClientMetricsReporter::default();
        // Register counter but never increment it
        let _c = counter("test_reporter_zero_start");

        // snapshot: diff = 0 (cur=0, no prev → diff=0), must NOT appear
        let snap = reporter.snapshot();
        assert!(
            !snap
                .iter()
                .any(|m| m.name.as_deref() == Some("test_reporter_zero_start")),
            "zero-value counter must not appear in snapshot"
        );
    }

    #[test]
    fn counter_baseline_written_even_when_diff_zero() {
        // Validates the critical invariant: if a counter starts at 0 (diff=0 on
        // first snapshot), the baseline is still written so the next snapshot
        // produces the correct incremental diff rather than re-reporting the full
        // value as if it were brand new.
        let reporter = ClientMetricsReporter::default();
        let c = counter("test_reporter_baseline");

        // First snapshot: cur=0, diff=0 — not emitted, but baseline written
        let snap1 = reporter.snapshot();
        assert!(!snap1
            .iter()
            .any(|m| m.name.as_deref() == Some("test_reporter_baseline")));

        // Now increment
        c.inc(75);

        // Second snapshot: diff should be 75 (not a brand-new "cur=75" full value)
        let snap2 = reporter.snapshot();
        let v = snap2
            .iter()
            .find(|m| m.name.as_deref() == Some("test_reporter_baseline"))
            .and_then(|m| m.value);
        assert_eq!(
            v,
            Some(75.0),
            "diff after baseline write must be incremental"
        );
    }

    #[test]
    fn gauge_returns_current_value() {
        let reporter = ClientMetricsReporter::default();
        let g = gauge("test_reporter_gauge");
        g.set(42);

        let snap = reporter.snapshot();
        let found = snap
            .iter()
            .find(|m| m.name.as_deref() == Some("test_reporter_gauge"));
        assert!(found.is_some(), "gauge must appear in snapshot");
        assert_eq!(found.unwrap().value, Some(42.0));
        assert_eq!(found.unwrap().metric_type, MetricType::Gauge as i32);
    }

    #[test]
    fn gauge_no_diff_always_reported() {
        // Gauges always appear in snapshot regardless of whether the value changed.
        let reporter = ClientMetricsReporter::default();
        let g = gauge("test_reporter_gauge_stable");
        g.set(7);

        let snap1 = reporter.snapshot();
        let snap2 = reporter.snapshot();

        let in_snap1 = snap1
            .iter()
            .any(|m| m.name.as_deref() == Some("test_reporter_gauge_stable"));
        let in_snap2 = snap2
            .iter()
            .any(|m| m.name.as_deref() == Some("test_reporter_gauge_stable"));
        assert!(in_snap1, "gauge must appear in first snapshot");
        assert!(
            in_snap2,
            "gauge must appear in second snapshot even if unchanged"
        );
    }

    #[test]
    fn name_constants_round_trip() {
        // Smoke-test: inc via well-known name constants; verify the snapshot
        // emits each metric with `instance="Client"` and the bare metric name
        // (the `Client.` prefix stripped to match Java's
        // `MetricKey.getMetricName()`).
        let reporter = ClientMetricsReporter::default();

        counter(name::CLIENT_BYTES_READ_LOCAL).inc(1024);
        counter(name::CLIENT_BYTES_WRITTEN_LOCAL).inc(2048);
        counter(name::CLIENT_BYTES_WRITTEN_UFS).inc(512);

        let snap = reporter.snapshot();

        for bare in &["BytesReadLocal", "BytesWrittenLocal", "BytesWrittenUfs"] {
            let m = snap
                .iter()
                .find(|m| m.name.as_deref() == Some(*bare))
                .unwrap_or_else(|| panic!("metric {} missing from snapshot", bare));
            assert_eq!(
                m.instance.as_deref(),
                Some(INSTANCE_CLIENT),
                "metric {} must carry instance=\"Client\"",
                bare
            );
            assert_eq!(m.metric_type, MetricType::Counter as i32);
        }
    }

    #[test]
    fn strip_instance_prefix_behaviour() {
        // Java `MetricKey.getMetricName()` parity: keep the part after the
        // first dot, return the input unchanged when there is no dot.
        assert_eq!(
            strip_instance_prefix("Client.BytesReadLocal"),
            "BytesReadLocal"
        );
        assert_eq!(
            strip_instance_prefix("Worker.BytesReadAlluxio"),
            "BytesReadAlluxio"
        );
        // Multi-dot names: only the first segment is treated as the instance.
        assert_eq!(
            strip_instance_prefix("Client.BytesReadPerUfs.s3a"),
            "BytesReadPerUfs.s3a"
        );
        // Bare names round-trip unchanged.
        assert_eq!(strip_instance_prefix("BytesReadLocal"), "BytesReadLocal");
        assert_eq!(strip_instance_prefix(""), "");
    }
}
