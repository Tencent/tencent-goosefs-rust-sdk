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
    /// Local short-circuit read bytes (client reads from local Alluxio worker).
    /// Cluster aggregated: true
    pub const CLIENT_BYTES_READ_LOCAL: &str = "Client.BytesReadLocal";

    /// Local short-circuit write bytes (client writes to local Alluxio worker).
    /// Cluster aggregated: true
    pub const CLIENT_BYTES_WRITTEN_LOCAL: &str = "Client.BytesWrittenLocal";

    /// Client direct UFS write bytes (bypass Alluxio layer).
    /// Cluster aggregated: true
    pub const CLIENT_BYTES_WRITTEN_UFS: &str = "Client.BytesWrittenUfs";
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
}
