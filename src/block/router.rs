//! Worker router: maps block IDs to workers using consistent hashing.
//!
//! GooseFS uses consistent hashing to decide which worker should serve
//! a particular block. This module implements the routing logic with:
//! - Consistent hash ring based on worker IDs
//! - Failed-worker filtering with configurable TTL
//! - Thread-safe worker list updates via `RwLock<Arc<...>>`

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::proto::grpc::block::WorkerInfo;
use crate::proto::grpc::WorkerNetAddress;

/// How long a worker is considered "failed" before being retried.
const DEFAULT_FAILURE_TTL: Duration = Duration::from_secs(60);

/// Number of virtual nodes per worker in the hash ring.
const VIRTUAL_NODES_PER_WORKER: u32 = 100;

/// Thread-safe worker router with consistent hashing and failure tracking.
pub struct WorkerRouter {
    /// Current snapshot of workers — updated atomically via Arc swap.
    workers: RwLock<Arc<Vec<WorkerInfo>>>,
    /// Tracks recently failed worker addresses with the failure timestamp.
    failed_workers: DashMap<String, Instant>,
    /// Duration after which a failed worker is eligible again.
    failure_ttl: Duration,
}

impl WorkerRouter {
    /// Create a new router with an empty worker list.
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(Arc::new(Vec::new())),
            failed_workers: DashMap::new(),
            failure_ttl: DEFAULT_FAILURE_TTL,
        }
    }

    /// Create a router with a custom failure TTL.
    pub fn with_failure_ttl(failure_ttl: Duration) -> Self {
        Self {
            workers: RwLock::new(Arc::new(Vec::new())),
            failed_workers: DashMap::new(),
            failure_ttl,
        }
    }

    /// Update the full worker list (snapshot replace pattern from fluss-rust).
    pub async fn update_workers(&self, workers: Vec<WorkerInfo>) {
        let new_snapshot = Arc::new(workers);
        let mut guard = self.workers.write().await;
        *guard = new_snapshot;
    }

    /// Get a snapshot of the current worker list.
    pub async fn get_workers(&self) -> Arc<Vec<WorkerInfo>> {
        self.workers.read().await.clone()
    }

    /// Select the best worker for the given block ID using consistent hashing.
    ///
    /// Returns the selected `WorkerInfo`, filtering out recently-failed workers.
    pub async fn select_worker(&self, block_id: i64) -> Result<WorkerInfo> {
        let workers = self.workers.read().await.clone();

        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers registered".to_string(),
            });
        }

        // Clean up expired failures
        self.cleanup_expired_failures();

        // Build hash ring and select
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
}
