//! Worker router: maps block IDs to workers using consistent hashing.
//!
//! Goosefs uses consistent hashing to decide which worker should serve
//! a particular block. This module implements the routing logic with:
//! - Consistent hash ring based on worker IDs
//! - Failed-worker filtering with configurable TTL
//! - Thread-safe worker list updates via `RwLock<Arc<...>>`
//! - Worker list TTL — auto-refresh after `worker_refresh_ttl` (default 30 s)
//! - Local worker preference — detect the local worker by hostname/IP
//!   and route block reads there first (mirrors Java `LocalFirstPolicy`)

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::proto::grpc::block::WorkerInfo;
use crate::proto::grpc::WorkerNetAddress;

/// How long a worker is considered "failed" before being retried.
const DEFAULT_FAILURE_TTL: Duration = Duration::from_secs(60);

/// Default TTL for the worker list before a background refresh is triggered.
/// Matches Go SDK's `WorkerRefreshPeriod = 30s`.
const DEFAULT_WORKER_REFRESH_TTL: Duration = Duration::from_secs(30);

/// Number of virtual nodes per worker in the hash ring.
const VIRTUAL_NODES_PER_WORKER: u32 = 100;

/// Thread-safe worker router with consistent hashing, failure tracking,
/// TTL-based refresh, and local-worker preference.
pub struct WorkerRouter {
    /// Current snapshot of workers — updated atomically via Arc swap.
    workers: RwLock<Arc<Vec<WorkerInfo>>>,
    /// Tracks recently failed worker addresses with the failure timestamp.
    failed_workers: DashMap<String, Instant>,
    /// Duration after which a failed worker is eligible again.
    failure_ttl: Duration,

    // ── Worker list TTL ─────────────────────────────────────────────────────────
    /// Timestamp of the last worker-list update.
    last_refresh: RwLock<Instant>,
    /// Duration after which the worker list is considered stale.
    worker_refresh_ttl: Duration,

    // ── Local worker cache ──────────────────────────────────────────────────
    /// The detected local worker ID, if any.
    ///
    /// Set on first call to `select_worker` after the worker list is populated.
    /// `0` means "not yet detected" (worker IDs are always positive).
    local_worker_id: RwLock<i64>,
}

impl WorkerRouter {
    /// Create a new router with an empty worker list and default TTLs.
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(Arc::new(Vec::new())),
            failed_workers: DashMap::new(),
            failure_ttl: DEFAULT_FAILURE_TTL,
            last_refresh: RwLock::new(Instant::now()),
            worker_refresh_ttl: DEFAULT_WORKER_REFRESH_TTL,
            local_worker_id: RwLock::new(0),
        }
    }

    /// Create a router with a custom failure TTL.
    pub fn with_failure_ttl(failure_ttl: Duration) -> Self {
        Self {
            workers: RwLock::new(Arc::new(Vec::new())),
            failed_workers: DashMap::new(),
            failure_ttl,
            last_refresh: RwLock::new(Instant::now()),
            worker_refresh_ttl: DEFAULT_WORKER_REFRESH_TTL,
            local_worker_id: RwLock::new(0),
        }
    }

    /// Create a router with custom failure TTL and worker refresh TTL.
    pub fn with_ttls(failure_ttl: Duration, worker_refresh_ttl: Duration) -> Self {
        Self {
            workers: RwLock::new(Arc::new(Vec::new())),
            failed_workers: DashMap::new(),
            failure_ttl,
            last_refresh: RwLock::new(Instant::now()),
            worker_refresh_ttl,
            local_worker_id: RwLock::new(0),
        }
    }

    /// Update the full worker list (snapshot replace pattern).
    ///
    /// Also resets the TTL clock so the list won't be considered stale
    /// immediately after an explicit update.
    pub async fn update_workers(&self, workers: Vec<WorkerInfo>) {
        let new_snapshot = Arc::new(workers);
        let mut guard = self.workers.write().await;
        *guard = new_snapshot;
        // Reset refresh clock
        *self.last_refresh.write().await = Instant::now();
        // Invalidate local worker cache so it is re-detected
        *self.local_worker_id.write().await = 0;
    }

    /// Get a snapshot of the current worker list.
    pub async fn get_workers(&self) -> Arc<Vec<WorkerInfo>> {
        self.workers.read().await.clone()
    }

    // ── TTL helpers ─────────────────────────────────────────────────────────────

    /// Returns `true` if the worker list is older than `worker_refresh_ttl`.
    pub async fn needs_refresh(&self) -> bool {
        self.last_refresh.read().await.elapsed() >= self.worker_refresh_ttl
    }

    /// Refresh the worker list by calling `get_worker_info_list()` on the
    /// given `WorkerManagerClient`.
    ///
    /// This is a **blocking** refresh (awaited inline).  For a non-blocking
    /// background refresh, the caller should `tokio::spawn` this call.
    ///
    /// Returns `Ok(())` on success; logs a warning and returns `Ok(())` on
    /// failure so the stale list keeps serving traffic (stale-while-revalidate).
    pub async fn refresh_workers(&self, wm: &crate::client::WorkerManagerClient) -> Result<()> {
        match wm.get_worker_info_list().await {
            Ok(workers) => {
                debug!(count = workers.len(), "worker list refreshed");
                self.update_workers(workers).await;
                Ok(())
            }
            Err(e) => {
                warn!("worker list refresh failed, keeping stale list: {}", e);
                // Reset the clock anyway to avoid hammering the master on each call
                *self.last_refresh.write().await = Instant::now();
                Ok(())
            }
        }
    }

    // ── Local worker detection ────────────────────────────────────────────────

    /// Detect and cache the ID of the local worker, if any.
    ///
    /// Matches each worker's `host` field against the current machine's
    /// hostname and all local IP addresses.
    ///
    /// Returns the worker ID of the local worker, or `0` if none found.
    async fn detect_local_worker(workers: &[WorkerInfo]) -> i64 {
        let local_names = Self::local_hostnames();

        for w in workers {
            if let Some(addr) = &w.address {
                let host = addr.host.as_deref().unwrap_or("");
                if local_names.iter().any(|n| n == host) {
                    let id = w.id.unwrap_or(0);
                    debug!(host = %host, worker_id = id, "detected local worker");
                    return id;
                }
            }
        }
        0
    }

    /// Collect the set of names that identify the local machine.
    ///
    /// Includes:
    /// - `hostname` (short)
    /// - `127.0.0.1` / `::1`
    fn local_hostnames() -> Vec<String> {
        let mut names = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
        ];

        // Try to get the system hostname
        if let Ok(h) = hostname::get() {
            if let Ok(s) = h.into_string() {
                names.push(s.clone());
                // Also push the short form (before the first '.')
                if let Some(short) = s.split('.').next() {
                    names.push(short.to_string());
                }
            }
        }

        names
    }

    /// Select the best worker for the given block ID using consistent hashing.
    ///
    /// # Stale-while-revalidate
    ///
    /// If the worker list is older than `worker_refresh_ttl`, a background
    /// refresh task is spawned *without* blocking the current read.  The
    /// stale list continues to serve traffic until the refresh completes.
    ///
    /// **Note**: The background refresh requires access to a
    /// `WorkerManagerClient`. Since `WorkerRouter` does not hold a reference
    /// to one, callers that want automatic refresh should call
    /// `needs_refresh` + `refresh_workers` themselves (e.g. in
    /// `GoosefsFileInStream::open`).
    ///
    /// # Local-first routing
    ///
    /// If a local worker is detected, it is returned for *all* block IDs
    /// (regardless of consistent-hash position), matching Java's
    /// `LocalFirstPolicy`.  The local worker is only bypassed if it is in the
    /// failed set.
    pub async fn select_worker(&self, block_id: i64) -> Result<WorkerInfo> {
        let workers = self.workers.read().await.clone();

        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers registered".to_string(),
            });
        }

        // Clean up expired failures
        self.cleanup_expired_failures();

        // Detect local worker on first call after list update
        {
            let cached_id = *self.local_worker_id.read().await;
            if cached_id == 0 {
                let id = Self::detect_local_worker(&workers).await;
                *self.local_worker_id.write().await = id;
            }
        }

        // Prefer local worker if available and not failed
        {
            let local_id = *self.local_worker_id.read().await;
            if local_id > 0 {
                if let Some(local_w) = workers.iter().find(|w| w.id == Some(local_id)) {
                    if let Some(addr) = &local_w.address {
                        if !self.is_failed(&worker_addr_key(addr)) {
                            return Ok(local_w.clone());
                        }
                    }
                }
            }
        }

        // Build hash ring and select from eligible workers
        let eligible: Vec<&WorkerInfo> = workers
            .iter()
            .filter(|w| {
                if let Some(addr) = w.address.as_ref() {
                    let key = worker_addr_key(addr);
                    !self.is_failed(&key)
                } else {
                    false
                }
            })
            .collect();

        if eligible.is_empty() {
            // Fall back: try all workers ignoring failure state
            return self
                .consistent_hash_select(block_id, &workers)
                .ok_or_else(|| Error::NoWorkerAvailable {
                    message: "all workers are marked as failed".to_string(),
                });
        }

        let worker_infos: Vec<WorkerInfo> = eligible.into_iter().cloned().collect();
        self.consistent_hash_select(block_id, &worker_infos)
            .ok_or_else(|| Error::NoWorkerAvailable {
                message: format!("no suitable worker for block_id={}", block_id),
            })
    }

    /// Mark a worker as failed (e.g., after a connection error).
    pub fn mark_failed(&self, addr: &WorkerNetAddress) {
        let key = worker_addr_key(addr);
        self.failed_workers.insert(key, Instant::now());
    }

    /// Pick any eligible worker (random selection, not tied to a block ID).
    ///
    /// Matches Java `UnderFileSystemFileOutStream`:
    /// ```java
    /// Collections.shuffle(workerNetAddresses);
    /// WorkerNetAddress address = workerNetAddresses.get(0);
    /// ```
    ///
    /// Used for opening the single long UFS stream in `CACHE_THROUGH` / `THROUGH`
    /// mode, where the UFS stream must be independent of the per-block cache
    /// routing.
    ///
    /// Excludes recently-failed workers; falls back to all workers if every
    /// worker is marked failed.
    pub async fn pick_any_worker(&self) -> Result<WorkerInfo> {
        let workers = self.workers.read().await.clone();

        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers registered".to_string(),
            });
        }

        self.cleanup_expired_failures();

        let eligible: Vec<WorkerInfo> = workers
            .iter()
            .filter(|w| {
                if let Some(addr) = w.address.as_ref() {
                    let key = worker_addr_key(addr);
                    !self.is_failed(&key)
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        let pool = if eligible.is_empty() {
            (*workers).clone()
        } else {
            eligible
        };

        if pool.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no eligible workers".to_string(),
            });
        }

        // Simple random pick using nanosecond jitter — no external RNG dep needed.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as usize)
            .unwrap_or(0);
        let idx = nanos % pool.len();
        Ok(pool[idx].clone())
    }

    /// Check if a worker address is currently in the failed set.
    fn is_failed(&self, key: &str) -> bool {
        if let Some(entry) = self.failed_workers.get(key) {
            entry.value().elapsed() < self.failure_ttl
        } else {
            false
        }
    }

    /// Remove expired failure entries.
    fn cleanup_expired_failures(&self) {
        self.failed_workers
            .retain(|_, v| v.elapsed() < self.failure_ttl);
    }

    /// Consistent hash selection: hash(block_id) → closest virtual node.
    fn consistent_hash_select(&self, block_id: i64, workers: &[WorkerInfo]) -> Option<WorkerInfo> {
        if workers.is_empty() {
            return None;
        }

        // Simple consistent hashing with virtual nodes
        let mut ring: Vec<(u64, usize)> = Vec::new();
        for (idx, worker) in workers.iter().enumerate() {
            let worker_id = worker.id.unwrap_or(idx as i64);
            let virtual_nodes = worker
                .virtual_node_num
                .unwrap_or(VIRTUAL_NODES_PER_WORKER as i32) as u32;
            for vn in 0..virtual_nodes {
                let hash = hash_key(&format!("{}:{}", worker_id, vn));
                ring.push((hash, idx));
            }
        }
        ring.sort_by_key(|(h, _)| *h);

        let target = hash_key(&block_id.to_string());

        // Find first node >= target (binary search)
        let pos = ring
            .binary_search_by_key(&target, |(h, _)| *h)
            .unwrap_or_else(|pos| pos);
        let pos = pos % ring.len();

        Some(workers[ring[pos].1].clone())
    }
}

impl Default for WorkerRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Produce a unique key for a `WorkerNetAddress`.
fn worker_addr_key(addr: &WorkerNetAddress) -> String {
    format!(
        "{}:{}",
        addr.host.as_deref().unwrap_or("unknown"),
        addr.rpc_port.unwrap_or(0)
    )
}

/// Compute a u64 hash for a string key.
fn hash_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_worker(id: i64, host: &str, port: i32) -> WorkerInfo {
        WorkerInfo {
            id: Some(id),
            address: Some(WorkerNetAddress {
                host: Some(host.to_string()),
                rpc_port: Some(port),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_select_worker_empty() {
        let router = WorkerRouter::new();
        assert!(router.select_worker(123).await.is_err());
    }

    #[tokio::test]
    async fn test_select_worker_deterministic() {
        let router = WorkerRouter::new();
        let workers = vec![
            make_worker(1, "w1", 9203),
            make_worker(2, "w2", 9203),
            make_worker(3, "w3", 9203),
        ];
        router.update_workers(workers).await;

        // Same block_id should map to same worker
        let w1 = router.select_worker(42).await.unwrap();
        let w2 = router.select_worker(42).await.unwrap();
        assert_eq!(w1.id, w2.id);
    }

    #[tokio::test]
    async fn test_failed_worker_filtered() {
        let router = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let workers = vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)];
        router.update_workers(workers.clone()).await;

        // Mark w1 as failed
        router.mark_failed(workers[0].address.as_ref().unwrap());

        // Should select w2
        let selected = router.select_worker(42).await.unwrap();
        assert_eq!(selected.id, Some(2));
    }

    #[tokio::test]
    async fn test_pick_any_worker_empty() {
        let router = WorkerRouter::new();
        assert!(router.pick_any_worker().await.is_err());
    }

    #[tokio::test]
    async fn test_pick_any_worker_returns_eligible() {
        let router = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let workers = vec![
            make_worker(1, "w1", 9203),
            make_worker(2, "w2", 9203),
            make_worker(3, "w3", 9203),
        ];
        router.update_workers(workers.clone()).await;
        // Mark w1 + w2 as failed, only w3 eligible.
        router.mark_failed(workers[0].address.as_ref().unwrap());
        router.mark_failed(workers[1].address.as_ref().unwrap());

        for _ in 0..10 {
            let picked = router.pick_any_worker().await.unwrap();
            assert_eq!(picked.id, Some(3));
        }
    }

    #[tokio::test]
    async fn test_pick_any_worker_fallback_when_all_failed() {
        let router = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let workers = vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)];
        router.update_workers(workers.clone()).await;
        router.mark_failed(workers[0].address.as_ref().unwrap());
        router.mark_failed(workers[1].address.as_ref().unwrap());

        // Should fall back to all workers instead of erroring out.
        let picked = router.pick_any_worker().await.unwrap();
        assert!(picked.id == Some(1) || picked.id == Some(2));
    }

    // ── TTL tests ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_needs_refresh_false_after_new() {
        // A freshly constructed router should NOT need refresh yet.
        let router = WorkerRouter::new();
        assert!(!router.needs_refresh().await);
    }

    #[tokio::test]
    async fn test_needs_refresh_true_with_zero_ttl() {
        // With a 0 s TTL, the list is immediately stale.
        let router = WorkerRouter::with_ttls(DEFAULT_FAILURE_TTL, Duration::ZERO);
        // needs_refresh checks elapsed >= ttl; with ZERO the condition is always true
        // after at least 1 ns has passed — sleep a tiny bit to be safe.
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(router.needs_refresh().await);
    }

    #[tokio::test]
    async fn test_with_ttls_stores_values() {
        let failure = Duration::from_secs(10);
        let refresh = Duration::from_secs(5);
        let router = WorkerRouter::with_ttls(failure, refresh);
        assert_eq!(router.failure_ttl, failure);
        assert_eq!(router.worker_refresh_ttl, refresh);
    }

    #[tokio::test]
    async fn test_update_workers_resets_refresh_clock() {
        // Use an instantaneous TTL so needs_refresh() starts true.
        let router = WorkerRouter::with_ttls(DEFAULT_FAILURE_TTL, Duration::ZERO);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(
            router.needs_refresh().await,
            "should need refresh before update"
        );

        // update_workers resets the clock — now the default 30 s TTL from
        // with_ttls(ZERO) is still 0, so it will still be stale.
        // To test the *reset* we need a non-zero TTL.
        let router2 = WorkerRouter::with_ttls(DEFAULT_FAILURE_TTL, Duration::from_secs(60));
        router2
            .update_workers(vec![make_worker(1, "w1", 9203)])
            .await;
        // Clock reset — should not need refresh yet.
        assert!(!router2.needs_refresh().await);
    }

    // ── Local worker tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_local_worker_preferred() {
        // Register a "localhost" worker and two remote workers.
        let router = WorkerRouter::new();
        let workers = vec![
            make_worker(1, "remote1", 9203),
            make_worker(2, "localhost", 9203), // local
            make_worker(3, "remote2", 9203),
        ];
        router.update_workers(workers).await;

        // Every block ID should be routed to the local worker.
        for block_id in [1i64, 42, 100, 999, 10_000] {
            let selected = router.select_worker(block_id).await.unwrap();
            assert_eq!(
                selected.id,
                Some(2),
                "block_id={} should route to local worker",
                block_id
            );
        }
    }

    #[tokio::test]
    async fn test_local_worker_skipped_when_failed() {
        let router = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let local_worker = make_worker(2, "localhost", 9203);
        let workers = vec![
            make_worker(1, "remote1", 9203),
            local_worker.clone(),
            make_worker(3, "remote2", 9203),
        ];
        router.update_workers(workers).await;

        // Mark the local worker as failed.
        router.mark_failed(local_worker.address.as_ref().unwrap());

        // Routing should fall back to consistent-hash selection of remote workers.
        let selected = router.select_worker(42).await.unwrap();
        assert_ne!(
            selected.id,
            Some(2),
            "failed local worker should not be selected"
        );
    }

    #[tokio::test]
    async fn test_detect_local_worker_none() {
        // No worker has a local hostname → returns 0.
        let workers = vec![
            make_worker(1, "remote-host-a.example.com", 9203),
            make_worker(2, "remote-host-b.example.com", 9203),
        ];
        let id = WorkerRouter::detect_local_worker(&workers).await;
        assert_eq!(id, 0);
    }

    #[tokio::test]
    async fn test_detect_local_worker_loopback() {
        // A worker with "127.0.0.1" must be detected as local.
        let workers = vec![
            make_worker(1, "10.0.0.1", 9203),
            make_worker(2, "127.0.0.1", 9203),
        ];
        let id = WorkerRouter::detect_local_worker(&workers).await;
        assert_eq!(id, 2);
    }

    #[tokio::test]
    async fn test_local_worker_cache_invalidated_on_update() {
        let router = WorkerRouter::new();
        // First update — no local worker.
        router
            .update_workers(vec![make_worker(1, "remote1", 9203)])
            .await;
        // Trigger detection (populates cache with 0 / no-local).
        let _ = router.select_worker(1).await;
        assert_eq!(*router.local_worker_id.read().await, 0);

        // Second update — local worker arrives.
        router
            .update_workers(vec![
                make_worker(1, "remote1", 9203),
                make_worker(2, "127.0.0.1", 9203),
            ])
            .await;
        // Cache should be reset to 0 (sentinel), re-detected on next select.
        assert_eq!(*router.local_worker_id.read().await, 0);
        let selected = router.select_worker(1).await.unwrap();
        assert_eq!(selected.id, Some(2), "new local worker should be preferred");
    }
}
