//! Metrics heartbeat background task.
//!
//! Periodically calls [`ClientMetricsReporter::snapshot`] and forwards the
//! incremental counter diffs to the Goosefs Master via
//! [`MetricsMasterClient::heartbeat`].
//!
//! ## Java alignment
//!
//! | Java construct | Rust equivalent |
//! |---|---|
//! | `scheduleWithFixedDelay(initialDelay=interval)` | `tokio::time::interval` + skip first tick |
//! | `MetricsHeartbeatContext.close()` flush | `flush_tx.send(())` triggers immediate loop iteration |
//! | `MetricsMasterSyncShutDownHook` | `Drop` only sends non-blocking signal via `try_send`; callers needing guaranteed flush must call `shutdown().await` explicitly |
//! | 30-second WARN sampling | [`LogSampler`] with `AtomicI64` epoch tracking |

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::client::metrics_master::MetricsClient;
use crate::config::GoosefsConfig;
use crate::metrics::reporter::ClientMetricsReporter;
use crate::proto::grpc::metric::ClientMetrics;

// ── LogSampler ────────────────────────────────────────────────────────────────

/// Rate-limits repeated WARN log lines to at most once per `window` seconds.
///
/// Aligned with Java's `SamplingLogger` pattern in `ClientMasterSync.java:L39-L41`
/// (30-second window for heartbeat failure warnings).
struct LogSampler {
    /// Unix-epoch seconds of the last emitted log line.
    last_emitted_secs: AtomicI64,
    /// Minimum gap between two emitted log lines (seconds).
    window_secs: i64,
}

impl LogSampler {
    const fn new(window: Duration) -> Self {
        Self {
            last_emitted_secs: AtomicI64::new(0),
            window_secs: window.as_secs() as i64,
        }
    }

    /// Returns `true` if enough time has elapsed since the last emission and
    /// resets the timer. Thread-safe (compare-and-swap).
    fn should_log(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let last = self.last_emitted_secs.load(Ordering::Relaxed);
        if now - last >= self.window_secs {
            // Try to claim this window. If another thread wins, they log.
            self.last_emitted_secs
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        } else {
            false
        }
    }
}

// ── resolve_app_id ────────────────────────────────────────────────────────────

/// Determine the `app_id` (= `ClientMetrics.source`) sent to the Master.
///
/// Priority order (aligned with Java's `MetricsHeartbeatContext` source resolution):
/// 1. `config.app_id` if explicitly set
/// 2. Hostname of the current machine
/// 3. Fallback: `goosefs-rust-<uuid_short>`
pub fn resolve_app_id(config: &GoosefsConfig) -> String {
    if let Some(ref id) = config.app_id {
        if !id.is_empty() {
            return id.clone();
        }
    }
    if let Ok(hostname) = hostname::get() {
        if let Ok(s) = hostname.into_string() {
            if !s.is_empty() {
                return s;
            }
        }
    }
    format!("goosefs-rust-{}", &uuid::Uuid::new_v4().to_string()[..8])
}

// ── HeartbeatTask ─────────────────────────────────────────────────────────────

/// Background task that periodically reports client metrics to the Goosefs Master.
///
/// ## Lifecycle
///
/// ```text
/// HeartbeatTask::spawn(...)
///   └── tokio::spawn  ← background loop (ticker + flush_rx select)
///
/// HeartbeatTask::shutdown().await
///   ├── closed.store(true)         ← signals loop to exit after current beat
///   ├── flush_tx.send(())          ← unblocks select immediately
///   └── JoinHandle::await (3s timeout) ← waits for final flush beat
/// ```
///
/// ## Drop behaviour
///
/// `Drop` sends a best-effort non-blocking signal (`try_send`) and sets `closed`.
/// It does **not** block on the join handle because `drop` is synchronous and
/// calling `block_on` inside a tokio runtime would panic.  If a guaranteed final
/// flush is required, call `shutdown().await` explicitly before dropping.
pub struct HeartbeatTask {
    handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    closed: Arc<AtomicBool>,
    flush_tx: mpsc::Sender<()>,
}

impl HeartbeatTask {
    /// Spawn the heartbeat background task and return a handle to it.
    ///
    /// The task will not fire immediately — it waits one full `interval` before
    /// the first beat, matching Java's `scheduleWithFixedDelay(initialDelay=interval)`.
    ///
    /// # Arguments
    ///
    /// - `client`   — shared `MetricsMasterClient` for the heartbeat RPC
    /// - `reporter` — shared `ClientMetricsReporter` that computes metric diffs
    /// - `app_id`   — source identifier written into `ClientMetrics.source`
    /// - `interval` — period between heartbeats (≥ 1s, enforced by config)
    /// - `closed`   — shared flag; set to `true` by the caller on shutdown
    pub fn spawn(
        client: Arc<dyn MetricsClient>,
        reporter: Arc<ClientMetricsReporter>,
        app_id: String,
        interval: Duration,
        closed: Arc<AtomicBool>,
    ) -> Self {
        let (flush_tx, mut flush_rx) = mpsc::channel::<()>(1);
        // Clone flush_tx so the task can detect channel closure (all senders dropped).
        let flush_tx_for_task = flush_tx.clone();

        // Clone closed for the spawned task; the original is kept in Self.
        let closed_for_task = closed.clone();

        let handle = tokio::spawn(async move {
            let closed = closed_for_task;
            // Keep a sampler for WARN-level heartbeat failures (30s window).
            let warn_sampler = LogSampler::new(Duration::from_secs(30));

            // Build the interval timer.
            // MissedTickBehavior::Delay: if a beat takes longer than `interval`,
            // the next tick fires `interval` after the previous one completes —
            // not immediately. This matches Java's `scheduleWithFixedDelay`.
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            // Skip the first immediate tick to match Java's initialDelay = interval.
            ticker.tick().await;

            loop {
                // Wait for either the periodic tick or a flush signal.
                tokio::select! {
                    biased; // flush signal has priority over the periodic tick
                    _ = flush_rx.recv() => {
                        debug!("metrics heartbeat: flush signal received");
                    }
                    _ = ticker.tick() => {}
                }

                // Execute the beat (even if closed — this is the final flush).
                Self::do_heartbeat(client.as_ref(), &reporter, &app_id, &warn_sampler).await;

                // Exit after the flush beat if closed.
                if closed.load(Ordering::SeqCst) {
                    break;
                }
            }

            // Suppress "unused variable" lint: flush_tx_for_task exists solely
            // to keep the channel open (not closed prematurely by the drop of
            // the sender held by the task).
            drop(flush_tx_for_task);

            debug!("metrics heartbeat task exited");
        });

        Self {
            handle: tokio::sync::Mutex::new(Some(handle)),
            closed,
            flush_tx,
        }
    }

    /// Execute a single heartbeat beat.
    ///
    /// If `snapshot()` returns an empty list (no changed counters, no gauges)
    /// the RPC is skipped entirely — aligned with Java
    /// `ClientMasterSync.heartbeat():L90-L93`.
    async fn do_heartbeat(
        client: &dyn MetricsClient,
        reporter: &ClientMetricsReporter,
        app_id: &str,
        warn_sampler: &LogSampler,
    ) {
        let metrics = reporter.snapshot();
        if metrics.is_empty() {
            debug!("metrics heartbeat: nothing to report, skipping RPC");
            return;
        }

        let payload = ClientMetrics {
            source: Some(app_id.to_string()),
            metrics,
        };

        if let Err(e) = client.heartbeat(vec![payload]).await {
            // Aligned with Java: heartbeat errors are WARN only and never
            // propagate to the caller. Rate-limit to one WARN per 30 seconds
            // to avoid log spam during sustained outages.
            if warn_sampler.should_log() {
                warn!(error = %e, "metrics heartbeat failed (further errors suppressed for 30s)");
            } else {
                debug!(error = %e, "metrics heartbeat failed (suppressed)");
            }
        }
    }

    /// Gracefully shut down the heartbeat task.
    ///
    /// 1. Sets `closed = true` so the task exits after the next beat.
    /// 2. Sends a flush signal so the task wakes immediately rather than
    ///    waiting for the next periodic tick.
    /// 3. Awaits the task with a 3-second timeout to allow the final beat
    ///    to complete (aligned with Java's `MetricsMasterSyncShutDownHook`
    ///    `join(500ms)` — we use 3s for safety margin).
    ///
    /// Safe to call multiple times (idempotent after the first call).
    pub async fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        // Signal the task to wake immediately.
        // Ignore send errors: if the channel is already closed the task has
        // already exited on its own.
        let _ = self.flush_tx.send(()).await;

        if let Some(h) = self.handle.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(3), h).await;
        }
    }
}

impl Drop for HeartbeatTask {
    fn drop(&mut self) {
        // `drop` is synchronous — we cannot await or call block_on here.
        // Signal the task with non-blocking primitives only.
        self.closed.store(true, Ordering::SeqCst);
        // try_send: if the channel buffer is full the task will still
        // observe `closed == true` on its next wake and exit.
        let _ = self.flush_tx.try_send(());
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GoosefsConfig;

    #[test]
    fn log_sampler_first_call_logs() {
        let sampler = LogSampler::new(Duration::from_secs(30));
        assert!(sampler.should_log(), "first call must always log");
    }

    #[test]
    fn log_sampler_second_call_suppressed() {
        let sampler = LogSampler::new(Duration::from_secs(30));
        sampler.should_log(); // consume first window
        assert!(
            !sampler.should_log(),
            "second call within window must be suppressed"
        );
    }

    #[test]
    fn log_sampler_zero_window_always_logs() {
        // A zero-second window means every call is in a new window.
        let sampler = LogSampler::new(Duration::from_secs(0));
        // First call logs.
        let first = sampler.should_log();
        // Second call: depends on whether system time advanced past 0s.
        // We just verify no panic and first call returns true.
        assert!(first);
        let _ = sampler.should_log();
    }

    #[test]
    fn resolve_app_id_uses_config_app_id() {
        let config = GoosefsConfig::new("127.0.0.1:9200").with_app_id("my-service");
        let id = resolve_app_id(&config);
        assert_eq!(id, "my-service");
    }

    #[test]
    fn resolve_app_id_fallback_non_empty() {
        // No app_id configured → falls back to hostname or uuid.
        let config = GoosefsConfig::new("127.0.0.1:9200");
        let id = resolve_app_id(&config);
        assert!(!id.is_empty(), "app_id fallback must never be empty");
    }

    #[test]
    fn resolve_app_id_empty_string_treated_as_unset() {
        // An app_id of "" is treated the same as None — falls back.
        let mut config = GoosefsConfig::new("127.0.0.1:9200");
        config.app_id = Some(String::new());
        let id = resolve_app_id(&config);
        assert!(!id.is_empty());
        assert_ne!(id, "", "empty app_id must not propagate");
    }

    #[test]
    fn heartbeat_task_is_send_sync()
    where
        HeartbeatTask: Send + Sync,
    {
    }

    // ── MockMetricsClient ─────────────────────────────────────────────────────

    /// In-memory mock that records every `heartbeat()` call.
    ///
    /// Used by the unit tests below to verify `HeartbeatTask` behaviour without
    /// a real gRPC connection.
    struct MockMetricsClient {
        /// Number of times `heartbeat()` was called.
        call_count: Arc<std::sync::atomic::AtomicUsize>,
        /// Channel to forward received metric payloads to the test.
        tx: tokio::sync::mpsc::Sender<Vec<crate::proto::grpc::metric::ClientMetrics>>,
    }

    impl MockMetricsClient {
        fn new(
            tx: tokio::sync::mpsc::Sender<Vec<crate::proto::grpc::metric::ClientMetrics>>,
        ) -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
            let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            (
                Self {
                    call_count: call_count.clone(),
                    tx,
                },
                call_count,
            )
        }
    }

    #[async_trait::async_trait]
    impl crate::client::metrics_master::MetricsClient for MockMetricsClient {
        async fn heartbeat(
            &self,
            client_metrics: Vec<crate::proto::grpc::metric::ClientMetrics>,
        ) -> crate::error::Result<()> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = self.tx.send(client_metrics).await;
            Ok(())
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Build a fresh [`ClientMetricsReporter`] backed by an isolated counter
    /// registered under the given unique `metric_name`.  Returns the reporter
    /// and the counter so the test can increment it.
    fn make_reporter(
        metric_name: &str,
    ) -> (
        Arc<crate::metrics::reporter::ClientMetricsReporter>,
        Arc<crate::metrics::registry::Counter>,
    ) {
        let c = crate::metrics::registry::counter(metric_name);
        let reporter = Arc::new(crate::metrics::reporter::ClientMetricsReporter::default());
        (reporter, c)
    }

    // ── Unit tests ────────────────────────────────────────────────────────────

    /// When the global registry has no non-zero counter deltas the heartbeat
    /// task must **not** call `MetricsClient::heartbeat()` at all.
    ///
    /// Aligned with Java `ClientMasterSync.heartbeat():L90-L93`:
    /// ```java
    /// if (rpcMetrics.isEmpty()) { return; }
    /// ```
    #[tokio::test]
    async fn skip_when_empty() {
        tokio::time::pause(); // deterministic timer control

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let (mock, call_count) = MockMetricsClient::new(tx);

        // Reporter with a counter that is never incremented (delta = 0).
        let counter_name = "test_hb_skip_when_empty";
        let (reporter, _counter) = make_reporter(counter_name);
        // Call snapshot once so the baseline is written (delta=0 from this point).
        // This mimics the state where the counter exists but nothing happened.
        let _ = reporter.snapshot();

        let closed = Arc::new(AtomicBool::new(false));
        let _task = HeartbeatTask::spawn(
            Arc::new(mock) as Arc<dyn crate::client::metrics_master::MetricsClient>,
            reporter,
            "test-app".into(),
            Duration::from_secs(5), // interval
            closed,
        );

        // Advance past two full intervals — task should fire twice but skip
        // the RPC both times because snapshot() returns empty.
        tokio::time::advance(Duration::from_secs(11)).await;
        // Yield so the spawned task has a chance to run.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // No RPC must have been issued.
        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "heartbeat RPC must be skipped when snapshot is empty"
        );
        // Channel must be empty too.
        assert!(
            rx.try_recv().is_err(),
            "no metrics must have been forwarded"
        );
    }

    /// When `shutdown()` is called the task must perform one final beat
    /// **immediately** (flushing any pending metrics) before exiting.
    #[tokio::test]
    async fn flush_on_shutdown() {
        tokio::time::pause(); // deterministic timer control

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let (mock, call_count) = MockMetricsClient::new(tx);

        // Register a counter with a unique name and increment it so the
        // first snapshot will have a non-zero delta.
        let counter_name = "test_hb_flush_on_shutdown";
        let (reporter, counter) = make_reporter(counter_name);
        counter.inc(42);

        let closed = Arc::new(AtomicBool::new(false));
        let task = HeartbeatTask::spawn(
            Arc::new(mock) as Arc<dyn crate::client::metrics_master::MetricsClient>,
            reporter,
            "flush-app".into(),
            Duration::from_secs(60), // very long interval — tick never fires
            closed,
        );

        // `shutdown()` must immediately signal the task to flush even though
        // the 60s timer hasn't elapsed.
        task.shutdown().await;

        // Exactly one RPC call must have been made (the final flush beat).
        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "shutdown must trigger exactly one final heartbeat flush"
        );

        // The flushed payload must contain the counter increment we registered.
        let received = rx
            .try_recv()
            .expect("final flush metrics must have been sent");
        assert!(
            !received.is_empty(),
            "flushed ClientMetrics must not be empty"
        );
        let cm = &received[0];
        assert_eq!(
            cm.source.as_deref(),
            Some("flush-app"),
            "ClientMetrics.source must equal the app_id"
        );
        let metric = cm
            .metrics
            .iter()
            .find(|m| m.name.as_deref() == Some(counter_name))
            .expect("counter must be present in the flushed payload");
        assert_eq!(metric.value, Some(42.0), "flushed counter value must be 42");
    }

    /// Verify that `do_heartbeat` sets the `source` field of `ClientMetrics`
    /// to the `app_id` passed in — verifying the payload construction path.
    ///
    /// This test focuses on the `app_id → ClientMetrics.source` mapping rather
    /// than the RPC-skip logic (which is covered by `skip_when_empty`).
    #[tokio::test]
    async fn do_heartbeat_sets_source_from_app_id() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let (mock, call_count) = MockMetricsClient::new(tx);

        // Ensure the global counter has a non-zero value so snapshot() is non-empty.
        let counter_name = "test_do_hb_source_app_id";
        let c = crate::metrics::registry::counter(counter_name);
        c.inc(77);

        let reporter = crate::metrics::reporter::ClientMetricsReporter::default();
        let sampler = LogSampler::new(Duration::from_secs(30));

        HeartbeatTask::do_heartbeat(&mock, &reporter, "my-node-id", &sampler).await;

        // RPC should have been called (snapshot is non-empty because of counter inc above).
        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "do_heartbeat must call RPC when snapshot is non-empty"
        );

        let received = rx.try_recv().expect("metrics must have been forwarded");
        assert_eq!(
            received.len(),
            1,
            "exactly one ClientMetrics entry expected"
        );
        let cm = &received[0];
        assert_eq!(
            cm.source.as_deref(),
            Some("my-node-id"),
            "ClientMetrics.source must equal the app_id argument"
        );
    }

    /// Verify that `do_heartbeat` correctly wraps all snapshot metrics into a
    /// single `ClientMetrics` envelope (not one per metric).
    #[tokio::test]
    async fn do_heartbeat_wraps_all_metrics_in_single_envelope() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let (mock, _call_count) = MockMetricsClient::new(tx);

        // Register two distinct counters with non-zero values.
        let c1 = crate::metrics::registry::counter("test_do_hb_envelope_c1");
        let c2 = crate::metrics::registry::counter("test_do_hb_envelope_c2");
        c1.inc(10);
        c2.inc(20);

        let reporter = crate::metrics::reporter::ClientMetricsReporter::default();
        let sampler = LogSampler::new(Duration::from_secs(30));

        HeartbeatTask::do_heartbeat(&mock, &reporter, "envelope-app", &sampler).await;

        let received = rx.try_recv().expect("metrics must have been forwarded");
        // All metrics must be packed into a single ClientMetrics struct.
        assert_eq!(
            received.len(),
            1,
            "all metrics must be packed into a single ClientMetrics envelope"
        );
        // The envelope must contain both counters (among potentially others from global REGISTRY).
        let metrics_names: Vec<&str> = received[0]
            .metrics
            .iter()
            .filter_map(|m| m.name.as_deref())
            .collect();
        assert!(
            metrics_names.contains(&"test_do_hb_envelope_c1"),
            "c1 must be in the payload"
        );
        assert!(
            metrics_names.contains(&"test_do_hb_envelope_c2"),
            "c2 must be in the payload"
        );
    }
}
