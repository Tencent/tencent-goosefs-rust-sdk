//! Prometheus Pushgateway reporter.
//!
//! Periodically collects all metrics from the global [`Registry`](super::registry::Registry)
//! and pushes them to a Prometheus Pushgateway endpoint via HTTP POST using the
//! [Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/).
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────┐       HTTP POST        ┌──────────────┐
//! │  PushgatewayTask    │ ───────────────────────►│  Pushgateway │
//! │  (tokio background) │   /metrics/job/{job}    │  :9091       │
//! └─────────────────────┘                         └──────────────┘
//!         ▲
//!         │ reads
//! ┌───────┴─────────────┐
//! │  Global Registry    │
//! │  (counters, gauges) │
//! └─────────────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,no_run
//! use goosefs_sdk::metrics::pushgateway::{PushgatewayConfig, PushgatewayTask};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = PushgatewayConfig::new("http://127.0.0.1:9091", "goosefs_client")
//!         .with_instance("my-host")
//!         .with_push_interval(std::time::Duration::from_secs(10));
//!     let task = PushgatewayTask::spawn(config);
//!
//!     // ... application logic ...
//!
//!     task.shutdown().await;
//! }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::metrics::registry::get_registry;

// ── PushgatewayConfig ─────────────────────────────────────────────────────────

/// Configuration for the Pushgateway reporter.
#[derive(Debug, Clone)]
pub struct PushgatewayConfig {
    /// Pushgateway base URL (e.g. `"http://127.0.0.1:9091"`).
    pub endpoint: String,

    /// Job label for the pushed metrics (maps to `/metrics/job/{job}`).
    ///
    /// Typically `"goosefs_client"` or your service name.
    pub job: String,

    /// Optional instance label (adds `/instance/{instance}` to the push URL).
    ///
    /// When `None`, the Pushgateway auto-assigns based on the client IP.
    pub instance: Option<String>,

    /// Interval between metric pushes (default: 10 s).
    pub push_interval: Duration,

    /// Additional grouping labels appended to the push URL.
    ///
    /// Each `(key, value)` pair becomes `/{key}/{value}` in the URL path.
    /// Example: `[("namespace", "production")]` → `.../namespace/production`
    pub extra_labels: Vec<(String, String)>,
}

impl Default for PushgatewayConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:9091".to_string(),
            job: "goosefs_client".to_string(),
            instance: None,
            push_interval: Duration::from_secs(10),
            extra_labels: Vec::new(),
        }
    }
}

impl PushgatewayConfig {
    /// Create a new config with the given endpoint and job name.
    pub fn new(endpoint: impl Into<String>, job: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            job: job.into(),
            ..Default::default()
        }
    }

    /// Set the instance label.
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    /// Set the push interval.
    pub fn with_push_interval(mut self, interval: Duration) -> Self {
        self.push_interval = interval;
        self
    }

    /// Add an extra grouping label.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_labels.push((key.into(), value.into()));
        self
    }

    /// Build the full push URL for the Pushgateway.
    ///
    /// Format: `{endpoint}/metrics/job/{job}[/instance/{instance}][/{key}/{value}...]`
    pub fn push_url(&self) -> String {
        let mut url = format!(
            "{}/metrics/job/{}",
            self.endpoint.trim_end_matches('/'),
            url_encode(&self.job)
        );
        if let Some(ref inst) = self.instance {
            url.push_str(&format!("/instance/{}", url_encode(inst)));
        }
        for (k, v) in &self.extra_labels {
            url.push_str(&format!("/{}/{}", url_encode(k), url_encode(v)));
        }
        url
    }
}

// ── PushgatewayTask ───────────────────────────────────────────────────────────

/// Background task that periodically pushes metrics to a Prometheus Pushgateway.
///
/// ## Lifecycle
///
/// ```text
/// PushgatewayTask::spawn(config)
///   └── tokio::spawn  ← background loop (ticker + flush_rx select)
///
/// PushgatewayTask::shutdown().await
///   ├── closed.store(true)
///   ├── flush_tx.send(())   ← unblocks select immediately
///   └── JoinHandle::await   ← waits for final push
/// ```
pub struct PushgatewayTask {
    handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    closed: Arc<AtomicBool>,
    flush_tx: mpsc::Sender<()>,
}

impl PushgatewayTask {
    /// Spawn the pushgateway reporter background task.
    ///
    /// The task will perform its first push after one full `push_interval`,
    /// then repeat at that interval.
    pub fn spawn(config: PushgatewayConfig) -> Self {
        let (flush_tx, mut flush_rx) = mpsc::channel::<()>(1);
        let flush_tx_for_task = flush_tx.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let closed_for_task = closed.clone();

        let push_url = config.push_url();
        let interval = config.push_interval;

        info!(
            push_url = %push_url,
            interval_secs = interval.as_secs(),
            "pushgateway reporter starting"
        );

        let handle = tokio::spawn(async move {
            let closed = closed_for_task;
            let client = reqwest::Client::new();

            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip first immediate tick
            ticker.tick().await;

            loop {
                tokio::select! {
                    biased;
                    _ = flush_rx.recv() => {
                        debug!("pushgateway: flush signal received");
                    }
                    _ = ticker.tick() => {}
                }

                // Collect and push metrics
                let body = collect_metrics_text();
                if !body.is_empty() {
                    match client
                        .post(&push_url)
                        .header("Content-Type", "text/plain; version=0.0.4")
                        .body(body)
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            if resp.status().is_success() {
                                debug!(status = %resp.status(), "pushgateway: metrics pushed successfully");
                            } else {
                                let status = resp.status();
                                let text = resp.text().await.unwrap_or_default();
                                warn!(
                                    status = %status,
                                    body = %text,
                                    "pushgateway: push failed with non-2xx status"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "pushgateway: HTTP request failed");
                        }
                    }
                } else {
                    debug!("pushgateway: no metrics to push, skipping");
                }

                if closed.load(Ordering::SeqCst) {
                    break;
                }
            }

            drop(flush_tx_for_task);
            debug!("pushgateway reporter task exited");
        });

        Self {
            handle: tokio::sync::Mutex::new(Some(handle)),
            closed,
            flush_tx,
        }
    }

    /// Gracefully shut down the pushgateway task.
    ///
    /// Performs one final push before exiting.
    pub async fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let _ = self.flush_tx.send(()).await;

        if let Some(h) = self.handle.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
        }
    }
}

impl Drop for PushgatewayTask {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        let _ = self.flush_tx.try_send(());
    }
}

// ── Metrics collection ────────────────────────────────────────────────────────

/// Collect all metrics from the global registry and format them in the
/// Prometheus text exposition format.
///
/// Counter metrics are reported as `counter` type with their **current cumulative value**
/// (Prometheus expects monotonically increasing counters — the Pushgateway/Prometheus
/// handles rate/delta calculation).
///
/// Gauge metrics are reported as `gauge` type with their current value.
fn collect_metrics_text() -> String {
    let registry = get_registry();
    let mut lines = Vec::new();

    // Counters (reported as _total per Prometheus naming convention)
    for entry in registry.counters.iter() {
        let raw_name = entry.key();
        let value = entry.value().get();
        let prom_name = sanitize_metric_name(raw_name);

        // Add HELP and TYPE annotations
        lines.push(format!(
            "# HELP {} GooseFS client counter: {}",
            prom_name, raw_name
        ));
        lines.push(format!("# TYPE {} counter", prom_name));
        lines.push(format!("{} {}", prom_name, value));
    }

    // Gauges
    for entry in registry.gauges.iter() {
        let raw_name = entry.key();
        let value = entry.value().get();
        let prom_name = sanitize_metric_name(raw_name);

        lines.push(format!(
            "# HELP {} GooseFS client gauge: {}",
            prom_name, raw_name
        ));
        lines.push(format!("# TYPE {} gauge", prom_name));
        lines.push(format!("{} {}", prom_name, value));
    }

    if lines.is_empty() {
        return String::new();
    }

    // Prometheus text format requires a trailing newline
    lines.push(String::new());
    lines.join("\n")
}

/// Convert a GooseFS metric name to a valid Prometheus metric name.
///
/// Prometheus metric names must match `[a-zA-Z_:][a-zA-Z0-9_:]*`.
/// We convert `.` to `_`, add a `goosefs_` prefix, and convert to lowercase.
///
/// Examples:
/// - `"Client.BytesReadLocal"` → `"goosefs_client_bytes_read_local"`
/// - `"Client.BytesWrittenUfs"` → `"goosefs_client_bytes_written_ufs"`
fn sanitize_metric_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 8);
    result.push_str("goosefs_");

    let mut prev_was_upper = false;
    for (i, ch) in name.chars().enumerate() {
        if ch == '.' {
            result.push('_');
            prev_was_upper = false;
        } else if ch.is_uppercase() {
            // Insert underscore before uppercase letter if not at start of a word
            // and not following another uppercase or the start
            if i > 0 && !prev_was_upper && name.chars().nth(i - 1) != Some('.') {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap_or(ch));
            prev_was_upper = true;
        } else if ch.is_alphanumeric() || ch == '_' || ch == ':' {
            result.push(ch);
            prev_was_upper = false;
        } else {
            result.push('_');
            prev_was_upper = false;
        }
    }

    result
}

/// Simple percent-encoding for URL path segments.
///
/// Encodes characters that are not valid in URL path segments.
fn url_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => encoded.push(ch),
            _ => {
                for b in ch.to_string().as_bytes() {
                    encoded.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    encoded
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_metric_name_basic() {
        assert_eq!(
            sanitize_metric_name("Client.BytesReadLocal"),
            "goosefs_client_bytes_read_local"
        );
        assert_eq!(
            sanitize_metric_name("Client.BytesWrittenUfs"),
            "goosefs_client_bytes_written_ufs"
        );
        assert_eq!(
            sanitize_metric_name("Client.BytesWrittenLocal"),
            "goosefs_client_bytes_written_local"
        );
    }

    #[test]
    fn sanitize_metric_name_custom() {
        assert_eq!(
            sanitize_metric_name("Client.DemoOpsCount"),
            "goosefs_client_demo_ops_count"
        );
    }

    #[test]
    fn push_url_basic() {
        let config = PushgatewayConfig::new("http://localhost:9091", "my_job");
        assert_eq!(
            config.push_url(),
            "http://localhost:9091/metrics/job/my_job"
        );
    }

    #[test]
    fn push_url_with_instance() {
        let config =
            PushgatewayConfig::new("http://localhost:9091", "my_job").with_instance("host1");
        assert_eq!(
            config.push_url(),
            "http://localhost:9091/metrics/job/my_job/instance/host1"
        );
    }

    #[test]
    fn push_url_with_extra_labels() {
        let config = PushgatewayConfig::new("http://localhost:9091", "my_job")
            .with_instance("host1")
            .with_label("namespace", "prod")
            .with_label("cluster", "us-east");
        assert_eq!(
            config.push_url(),
            "http://localhost:9091/metrics/job/my_job/instance/host1/namespace/prod/cluster/us-east"
        );
    }

    #[test]
    fn push_url_trailing_slash_stripped() {
        let config = PushgatewayConfig::new("http://localhost:9091/", "my_job");
        assert_eq!(
            config.push_url(),
            "http://localhost:9091/metrics/job/my_job"
        );
    }

    #[test]
    fn collect_metrics_text_includes_counters() {
        // Register a counter and ensure it shows up in the text output
        let c = crate::metrics::counter("Client.PushgatewayTestCounter");
        c.inc(42);

        let text = collect_metrics_text();
        assert!(
            text.contains("goosefs_client_pushgateway_test_counter"),
            "expected metric name in output, got:\n{}",
            text
        );
        assert!(text.contains("42"), "expected value 42 in output");
        assert!(text.contains("# TYPE"), "expected TYPE annotation");
    }

    #[test]
    fn collect_metrics_text_includes_gauges() {
        let g = crate::metrics::gauge("Client.PushgatewayTestGauge");
        g.set(99);

        let text = collect_metrics_text();
        assert!(
            text.contains("goosefs_client_pushgateway_test_gauge"),
            "expected gauge metric name in output, got:\n{}",
            text
        );
        assert!(text.contains("99"), "expected gauge value 99 in output");
        assert!(text.contains("# TYPE"), "expected TYPE annotation");
    }

    #[test]
    fn url_encode_basic() {
        assert_eq!(url_encode("hello"), "hello");
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a/b"), "a%2Fb");
    }
}
