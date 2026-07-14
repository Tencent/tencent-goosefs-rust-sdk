//! Client metrics registry for tracking counters and gauges.
//!
//! Provides a global, thread-safe registry for application metrics.
//! All counter/gauge mutations are atomic operations.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;

use dashmap::DashMap;

/// A simple counter metric that tracks a cumulative value.
/// Increments are atomic and safe for concurrent access.
#[derive(Default)]
pub struct Counter {
    v: AtomicI64,
}

impl Counter {
    /// Increment the counter by `n` bytes/items.
    /// This operation is atomic and will never be reordered or lost
    /// even in the presence of concurrent calls.
    #[inline]
    pub fn inc(&self, n: i64) {
        self.v.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the current value of the counter.
    /// Note: This is a snapshot; the value may change immediately after.
    #[inline]
    pub fn get(&self) -> i64 {
        self.v.load(Ordering::Relaxed)
    }
}

/// A gauge metric that tracks a point-in-time value.
/// Can be set and read atomically.
#[derive(Default)]
pub struct Gauge {
    v: AtomicI64,
}

impl Gauge {
    /// Set the gauge to the given value.
    #[inline]
    pub fn set(&self, val: i64) {
        self.v.store(val, Ordering::Relaxed);
    }

    /// Get the current value of the gauge.
    #[inline]
    pub fn get(&self) -> i64 {
        self.v.load(Ordering::Relaxed)
    }
}

/// Global metrics registry.
/// Stores all named counters and gauges in thread-safe concurrent maps.
pub(crate) struct Registry {
    pub(crate) counters: DashMap<String, std::sync::Arc<Counter>>,
    pub(crate) gauges: DashMap<String, std::sync::Arc<Gauge>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            counters: DashMap::new(),
            gauges: DashMap::new(),
        }
    }
}

/// Global singleton registry, lazily initialized.
pub(crate) static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Initialize and return the global registry.
/// (Internal; used by counter/gauge factory functions and the reporter.)
pub(crate) fn get_registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::default)
}

/// Get or create a counter by name.
/// Returns an `Arc` that can be cheaply cloned and shared across threads.
pub fn counter(name: &str) -> std::sync::Arc<Counter> {
    let registry = get_registry();

    // Fast path: counter already exists
    if let Some(c) = registry.counters.get(name) {
        return c.value().clone();
    }

    // Slow path: create and insert
    let c = std::sync::Arc::new(Counter::default());
    // If another thread races and inserts first, we'll return theirs.
    registry
        .counters
        .entry(name.to_string())
        .or_insert(c.clone());

    // Get the final value (may be different due to race)
    registry
        .counters
        .get(name)
        .map(|entry| entry.value().clone())
        .unwrap_or(c)
}

/// Get or create a gauge by name.
/// Returns an `Arc` that can be cheaply cloned and shared across threads.
pub fn gauge(name: &str) -> std::sync::Arc<Gauge> {
    let registry = get_registry();

    // Fast path: gauge already exists
    if let Some(g) = registry.gauges.get(name) {
        return g.value().clone();
    }

    // Slow path: create and insert
    let g = std::sync::Arc::new(Gauge::default());
    // If another thread races and inserts first, we'll return theirs.
    registry.gauges.entry(name.to_string()).or_insert(g.clone());

    // Get the final value (may be different due to race)
    registry
        .gauges
        .get(name)
        .map(|entry| entry.value().clone())
        .unwrap_or(g)
}

/// Metric name constants, aligned with Java's MetricKey definitions.
/// Only metrics with `isClusterAggregated=true` should be reported in heartbeat.
pub mod name {
    // ── Throughput counters (cluster aggregated) ─────────────────────────────

    /// Local short-circuit read bytes (client reads from local Alluxio worker).
    /// Cluster aggregated: true
    pub const CLIENT_BYTES_READ_LOCAL: &str = "Client.BytesReadLocal";

    /// Local short-circuit write bytes (client writes to local Alluxio worker).
    /// Cluster aggregated: true
    pub const CLIENT_BYTES_WRITTEN_LOCAL: &str = "Client.BytesWrittenLocal";

    /// Client direct UFS write bytes (bypass Alluxio layer).
    /// Cluster aggregated: true
    pub const CLIENT_BYTES_WRITTEN_UFS: &str = "Client.BytesWrittenUfs";

    // ── RPC operation counters ───────────────────────────────────────────────

    /// Total number of file read operations (open + stream fully consumed or closed).
    pub const CLIENT_READ_OPS_TOTAL: &str = "Client.ReadOpsTotal";

    /// Total number of file write operations (create + complete).
    pub const CLIENT_WRITE_OPS_TOTAL: &str = "Client.WriteOpsTotal";

    /// Total number of getStatus RPCs to Master.
    pub const CLIENT_GET_STATUS_OPS: &str = "Client.GetStatusOps";

    /// Total number of listStatus RPCs to Master.
    pub const CLIENT_LIST_STATUS_OPS: &str = "Client.ListStatusOps";

    /// Total number of createFile RPCs to Master.
    pub const CLIENT_CREATE_FILE_OPS: &str = "Client.CreateFileOps";

    /// Total number of createDirectory RPCs to Master.
    pub const CLIENT_CREATE_DIR_OPS: &str = "Client.CreateDirOps";

    /// Total number of delete (remove) RPCs to Master.
    pub const CLIENT_DELETE_OPS: &str = "Client.DeleteOps";

    /// Total number of rename RPCs to Master.
    pub const CLIENT_RENAME_OPS: &str = "Client.RenameOps";

    // ── Error / failure counters ─────────────────────────────────────────────

    /// Total RPC failures (all types: network, timeout, server error).
    pub const CLIENT_RPC_ERRORS_TOTAL: &str = "Client.RpcErrorsTotal";

    /// RPC failures broken down by type — UNAUTHENTICATED errors.
    pub const CLIENT_RPC_AUTH_ERRORS: &str = "Client.RpcAuthErrors";

    /// RPC failures — connection refused / unavailable.
    pub const CLIENT_RPC_UNAVAILABLE_ERRORS: &str = "Client.RpcUnavailableErrors";

    /// Block read failures (stream error, incomplete, etc.).
    pub const CLIENT_READ_FAILURES: &str = "Client.ReadFailures";

    /// Block write failures.
    pub const CLIENT_WRITE_FAILURES: &str = "Client.WriteFailures";

    // ── Latency counters (cumulative microseconds, divide by ops for avg) ───

    /// Cumulative read latency in microseconds (from open stream to close/eof).
    pub const CLIENT_READ_LATENCY_US: &str = "Client.ReadLatencyUs";

    /// Cumulative write latency in microseconds (from create to complete).
    pub const CLIENT_WRITE_LATENCY_US: &str = "Client.WriteLatencyUs";

    /// Cumulative getStatus RPC latency in microseconds.
    pub const CLIENT_GET_STATUS_LATENCY_US: &str = "Client.GetStatusLatencyUs";

    /// Cumulative listStatus RPC latency in microseconds.
    pub const CLIENT_LIST_STATUS_LATENCY_US: &str = "Client.ListStatusLatencyUs";

    // ── Connection pool gauges ───────────────────────────────────────────────

    /// Number of active (cached) worker connections in the pool.
    pub const CLIENT_WORKER_CONNECTIONS_ACTIVE: &str = "Client.WorkerConnectionsActive";

    /// Total number of worker reconnects performed (counter).
    pub const CLIENT_WORKER_RECONNECTS_TOTAL: &str = "Client.WorkerReconnectsTotal";

    /// Total number of reconnects that were coalesced (deduplicated).
    pub const CLIENT_WORKER_RECONNECTS_COALESCED: &str = "Client.WorkerReconnectsCoalesced";

    // ── Block / data path gauges ─────────────────────────────────────────────

    /// Number of blocks currently being read concurrently (gauge).
    pub const CLIENT_BLOCKS_READ_IN_PROGRESS: &str = "Client.BlocksReadInProgress";

    /// Number of blocks currently being written concurrently (gauge).
    pub const CLIENT_BLOCKS_WRITTEN_IN_PROGRESS: &str = "Client.BlocksWrittenInProgress";

    /// Total blocks successfully read (counter).
    pub const CLIENT_BLOCKS_READ_TOTAL: &str = "Client.BlocksReadTotal";

    /// Total blocks successfully written (counter).
    pub const CLIENT_BLOCKS_WRITTEN_TOTAL: &str = "Client.BlocksWrittenTotal";

    // ── Short-circuit (local mmap) read path (SHORT_CIRCUIT_DESIGN §7.3) ──
    /// Successful `OpenLocalBlock` + mmap sessions.
    pub const CLIENT_SC_OPEN_SUCCESS: &str = "Client.ShortCircuitOpenSuccess";
    /// `OpenLocalBlock` RPC failures (block not local / IO error).
    pub const CLIENT_SC_OPENLOCAL_FAIL: &str = "Client.ShortCircuitOpenLocalFail";
    /// `File::open` failures on the local block path (e.g. EACCES).
    pub const CLIENT_SC_FILE_OPEN_FAIL: &str = "Client.ShortCircuitFileOpenFail";
    /// `Mmap::map` failures (ENOMEM / EINVAL).
    pub const CLIENT_SC_MMAP_FAIL: &str = "Client.ShortCircuitMmapFail";
    /// Total bytes served from the short-circuit (mmap) path.
    pub const CLIENT_SC_READ_BYTES: &str = "Client.ShortCircuitReadBytes";
    /// Number of short-circuit `read` / `read_bytes` / `read_to_slice` calls.
    pub const CLIENT_SC_READ_CALLS: &str = "Client.ShortCircuitReadCalls";
    /// Factory LRU reader-cache hits.
    pub const CLIENT_SC_CACHE_HITS: &str = "Client.ShortCircuitCacheHits";
    /// Factory LRU reader-cache evictions.
    pub const CLIENT_SC_CACHE_EVICTIONS: &str = "Client.ShortCircuitCacheEvictions";
    /// Negative-cache hits (block recently failed SC → skipped, went gRPC).
    pub const CLIENT_SC_NEG_CACHE_HITS: &str = "Client.ShortCircuitNegCacheHits";
    /// Currently-live short-circuit readers (gauge).
    pub const CLIENT_SC_ACTIVE_READERS: &str = "Client.ShortCircuitActiveReaders";
    /// `prefetch` / `prefetch_many` calls.
    pub const CLIENT_SC_PREFETCH_CALLS: &str = "Client.ShortCircuitPrefetchCalls";
    /// Cumulative bytes requested for prefetch.
    pub const CLIENT_SC_PREFETCH_BYTES: &str = "Client.ShortCircuitPrefetchBytes";
    /// Actual `madvise(WILLNEED)` syscalls issued (after coalescing).
    pub const CLIENT_SC_PREFETCH_MADVISE: &str = "Client.ShortCircuitPrefetchMadvise";

    // ── SC top-level decision histogram (FLAMEGRAPH_OPTIMIZATION_PLAN §B1) ──
    //
    // Enum-tagged counters that expose the **caller-visible** outcome of each
    // `try_short_circuit_read` invocation on the **positioned / random** read
    // path (`GoosefsFileReader::next_read_bytes` /
    // `GoosefsFileInStream::read_at`), which is the workload the flame graph
    // in §2 is dominated by. Operators can compute the hit rate directly:
    //
    //     hit_rate = HIT / (HIT + SKIPPED + FALLBACK_OPEN + FALLBACK_READ)
    //
    // The **sequential** read path (`sc_sequential_read`) is intentionally
    // NOT counted here — it decides SC once per block and then reuses the
    // mmap slice for every chunk, so mixing per-chunk sequential counts
    // with per-read positioned counts would give a misleading denominator.
    // Sequential SC throughput remains observable via
    // `Client.ShortCircuitReadCalls` / `Client.ShortCircuitReadBytes`.
    //
    // Fine-grained fallback *reasons* remain observable via the pre-existing
    // `Client.ShortCircuitOpenLocalFail` / `FileOpenFail` / `MmapFail`
    // counters (they act as the "fallback reason histogram" §B1.2 asks for).
    /// SC actually served the read (zero-copy mmap slice). Hit-rate numerator.
    pub const CLIENT_SC_DECISION_HIT: &str = "Client.ShortCircuitDecisionHit";
    /// SC not attempted at all — pre-filter (`should_use`) rejected the block.
    /// Includes: SC disabled by config, block source not local, block size
    /// under the SC size threshold, block on the negative cache, etc.
    pub const CLIENT_SC_DECISION_SKIPPED: &str = "Client.ShortCircuitDecisionSkipped";
    /// SC attempted but the **open** step failed and read fell back to gRPC.
    /// Break down the specific cause via `ShortCircuitOpenLocalFail` /
    /// `ShortCircuitFileOpenFail` / `ShortCircuitMmapFail`.
    pub const CLIENT_SC_DECISION_FALLBACK_OPEN: &str = "Client.ShortCircuitDecisionFallbackOpen";
    /// SC opened successfully but a subsequent **read** failed with a
    /// recoverable error and this individual read fell back to gRPC. The
    /// reader is invalidated; a subsequent read on the same block re-opens.
    pub const CLIENT_SC_DECISION_FALLBACK_READ: &str = "Client.ShortCircuitDecisionFallbackRead";
    /// SC read produced a **semantic** error (`OutOfRange`) that must be
    /// surfaced unchanged (INV-S4) rather than falling back. Should stay at
    /// zero on healthy deployments; a non-zero value indicates real
    /// metadata / block-size drift worth investigating.
    pub const CLIENT_SC_DECISION_SEMANTIC_ERROR: &str = "Client.ShortCircuitDecisionSemanticError";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_inc_and_get() {
        let c = Counter::default();
        assert_eq!(c.get(), 0);

        c.inc(42);
        assert_eq!(c.get(), 42);

        c.inc(8);
        assert_eq!(c.get(), 50);

        c.inc(-10);
        assert_eq!(c.get(), 40);
    }

    #[test]
    fn counter_negative_increment() {
        let c = Counter::default();
        c.inc(-5);
        assert_eq!(c.get(), -5);
    }

    #[test]
    fn gauge_set_and_get() {
        let g = Gauge::default();
        assert_eq!(g.get(), 0);

        g.set(99);
        assert_eq!(g.get(), 99);

        g.set(-10);
        assert_eq!(g.get(), -10);
    }

    #[test]
    fn registry_counter_factory() {
        // First call creates counter
        let c1 = counter("my_counter");
        assert_eq!(c1.get(), 0);
        c1.inc(10);

        // Second call returns same Arc
        let c2 = counter("my_counter");
        assert_eq!(c2.get(), 10);

        // Different name gets different counter
        let c3 = counter("other_counter");
        assert_eq!(c3.get(), 0);
    }

    #[test]
    fn registry_gauge_factory() {
        // First call creates gauge
        let g1 = gauge("my_gauge");
        assert_eq!(g1.get(), 0);
        g1.set(55);

        // Second call returns same Arc
        let g2 = gauge("my_gauge");
        assert_eq!(g2.get(), 55);

        // Different name gets different gauge
        let g3 = gauge("other_gauge");
        assert_eq!(g3.get(), 0);
    }

    #[test]
    fn registry_counter_concurrent() {
        use std::thread;

        let c = counter("concurrent_counter");
        let mut handles = vec![];

        for _ in 0..10 {
            let c = c.clone();
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    c.inc(1);
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(c.get(), 10_000);
    }

    #[test]
    fn name_constants() {
        assert_eq!(name::CLIENT_BYTES_READ_LOCAL, "Client.BytesReadLocal");
        assert_eq!(name::CLIENT_BYTES_WRITTEN_LOCAL, "Client.BytesWrittenLocal");
        assert_eq!(name::CLIENT_BYTES_WRITTEN_UFS, "Client.BytesWrittenUfs");
    }

    #[test]
    fn name_constants_rpc_ops() {
        assert_eq!(name::CLIENT_READ_OPS_TOTAL, "Client.ReadOpsTotal");
        assert_eq!(name::CLIENT_WRITE_OPS_TOTAL, "Client.WriteOpsTotal");
        assert_eq!(name::CLIENT_GET_STATUS_OPS, "Client.GetStatusOps");
        assert_eq!(name::CLIENT_LIST_STATUS_OPS, "Client.ListStatusOps");
        assert_eq!(name::CLIENT_CREATE_FILE_OPS, "Client.CreateFileOps");
        assert_eq!(name::CLIENT_CREATE_DIR_OPS, "Client.CreateDirOps");
        assert_eq!(name::CLIENT_DELETE_OPS, "Client.DeleteOps");
        assert_eq!(name::CLIENT_RENAME_OPS, "Client.RenameOps");
    }

    #[test]
    fn name_constants_errors() {
        assert_eq!(name::CLIENT_RPC_ERRORS_TOTAL, "Client.RpcErrorsTotal");
        assert_eq!(name::CLIENT_RPC_AUTH_ERRORS, "Client.RpcAuthErrors");
        assert_eq!(
            name::CLIENT_RPC_UNAVAILABLE_ERRORS,
            "Client.RpcUnavailableErrors"
        );
        assert_eq!(name::CLIENT_READ_FAILURES, "Client.ReadFailures");
        assert_eq!(name::CLIENT_WRITE_FAILURES, "Client.WriteFailures");
    }

    #[test]
    fn name_constants_latency() {
        assert_eq!(name::CLIENT_READ_LATENCY_US, "Client.ReadLatencyUs");
        assert_eq!(name::CLIENT_WRITE_LATENCY_US, "Client.WriteLatencyUs");
        assert_eq!(
            name::CLIENT_GET_STATUS_LATENCY_US,
            "Client.GetStatusLatencyUs"
        );
        assert_eq!(
            name::CLIENT_LIST_STATUS_LATENCY_US,
            "Client.ListStatusLatencyUs"
        );
    }

    #[test]
    fn name_constants_pool_and_blocks() {
        assert_eq!(
            name::CLIENT_WORKER_CONNECTIONS_ACTIVE,
            "Client.WorkerConnectionsActive"
        );
        assert_eq!(
            name::CLIENT_WORKER_RECONNECTS_TOTAL,
            "Client.WorkerReconnectsTotal"
        );
        assert_eq!(
            name::CLIENT_WORKER_RECONNECTS_COALESCED,
            "Client.WorkerReconnectsCoalesced"
        );
        assert_eq!(
            name::CLIENT_BLOCKS_READ_IN_PROGRESS,
            "Client.BlocksReadInProgress"
        );
        assert_eq!(
            name::CLIENT_BLOCKS_WRITTEN_IN_PROGRESS,
            "Client.BlocksWrittenInProgress"
        );
        assert_eq!(name::CLIENT_BLOCKS_READ_TOTAL, "Client.BlocksReadTotal");
        assert_eq!(
            name::CLIENT_BLOCKS_WRITTEN_TOTAL,
            "Client.BlocksWrittenTotal"
        );
    }

    // ── B1: SC decision histogram constants ───────────────────────

    /// FLAMEGRAPH_OPTIMIZATION_PLAN §B1: five enum-tagged decision
    /// counters exposing the caller-visible SC outcome. Names follow
    /// the `Client.ShortCircuitDecision*` convention so operators can
    /// easily wildcard them in Prometheus / dashboards.
    #[test]
    fn name_constants_sc_decision_histogram() {
        assert_eq!(
            name::CLIENT_SC_DECISION_HIT,
            "Client.ShortCircuitDecisionHit"
        );
        assert_eq!(
            name::CLIENT_SC_DECISION_SKIPPED,
            "Client.ShortCircuitDecisionSkipped"
        );
        assert_eq!(
            name::CLIENT_SC_DECISION_FALLBACK_OPEN,
            "Client.ShortCircuitDecisionFallbackOpen"
        );
        assert_eq!(
            name::CLIENT_SC_DECISION_FALLBACK_READ,
            "Client.ShortCircuitDecisionFallbackRead"
        );
        assert_eq!(
            name::CLIENT_SC_DECISION_SEMANTIC_ERROR,
            "Client.ShortCircuitDecisionSemanticError"
        );
    }

    /// The five decision counters are reachable through the registry
    /// factory (i.e. exported), and repeated `counter(name)` calls
    /// return the same underlying `Arc<Counter>` — a prerequisite for
    /// heartbeat / pushgateway to observe them.
    #[test]
    fn sc_decision_counters_are_registered_and_shared() {
        for cname in [
            name::CLIENT_SC_DECISION_HIT,
            name::CLIENT_SC_DECISION_SKIPPED,
            name::CLIENT_SC_DECISION_FALLBACK_OPEN,
            name::CLIENT_SC_DECISION_FALLBACK_READ,
            name::CLIENT_SC_DECISION_SEMANTIC_ERROR,
        ] {
            let c1 = counter(cname);
            let c2 = counter(cname);
            // Same underlying counter (Arc pointer identity is not
            // guaranteed by the API, but observable state must be).
            let base = c1.get();
            c2.inc(1);
            assert_eq!(
                c1.get(),
                base + 1,
                "counter '{}' must be process-wide shared",
                cname
            );
        }
    }

    /// The decision counter names must be pairwise distinct — a
    /// duplicate would silently merge two decision buckets into one
    /// and destroy the hit-rate calculation.
    #[test]
    fn sc_decision_counter_names_are_pairwise_distinct() {
        let names = [
            name::CLIENT_SC_DECISION_HIT,
            name::CLIENT_SC_DECISION_SKIPPED,
            name::CLIENT_SC_DECISION_FALLBACK_OPEN,
            name::CLIENT_SC_DECISION_FALLBACK_READ,
            name::CLIENT_SC_DECISION_SEMANTIC_ERROR,
        ];
        for i in 0..names.len() {
            for j in (i + 1)..names.len() {
                assert_ne!(names[i], names[j], "duplicate SC decision name");
            }
        }
    }
}
