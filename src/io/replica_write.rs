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

//! Multi-replica write planning, matching Java `GooseFSBlockStore.getOutStream`
//! and `BlockOutStream.executeWithReplication`.
//!
//! # Java authority
//!
//! | Piece | Java |
//! |-------|------|
//! | Replica counts | `GooseFSBlockStore.getOutStream`: `initialReplicas` / `minNeededReplicas` |
//! | Capacity filter | `filterNoSpaceWorkers` (forbidWrite + persist watermark) |
//! | Degrade | `min(initialReplicas, alive)`; ASYNC_THROUGH keeps `durable.min` as a hard floor |
//! | Parallel fan-out | `BlockOutStream.executeWithReplication` (ASYNC_THROUGH && writers > 1) |

use crate::error::{Error, Result};
use crate::proto::grpc::block::WorkerInfo;

/// Planned replica counts for opening a block write stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaWritePlan {
    /// How many DataWriters to try to open.
    ///
    /// ASYNC_THROUGH uses `replication.durable` when it is greater than
    /// `replication.number`; otherwise `replication.number`.
    pub initial_replicas: usize,
    /// Minimum writers that must open (and later succeed) for the write
    /// to be accepted. ASYNC_THROUGH uses `replication.durable.min` as a
    /// hard constraint; other write types share `initial_replicas`.
    pub min_needed_replicas: usize,
    /// Hash-ring candidate width: `max(write.max.node.retry, initialReplicas)`.
    pub max_retry_node: usize,
}

/// Compute replica counts from write type + config, matching Java
/// `GooseFSBlockStore.getOutStream` before worker filtering.
pub(crate) fn replica_write_plan(
    async_through: bool,
    replication_number: i32,
    replication_durable: i32,
    replication_durable_min: i32,
    write_max_node_retry: i32,
) -> Result<ReplicaWritePlan> {
    let replication_number = replication_number.max(1);
    let replication_durable = replication_durable.max(1);
    let replication_durable_min = replication_durable_min.max(1);
    let write_max_node_retry = write_max_node_retry.max(1);

    let initial = if async_through && replication_durable > replication_number {
        replication_durable
    } else {
        replication_number
    };
    let min_needed = if async_through {
        replication_durable_min
    } else {
        initial
    };

    if async_through && initial < min_needed {
        return Err(Error::InvalidArgument {
            message: "min durable replicas can not be satisfied in ASYNC_THROUGH scenario"
                .to_string(),
        });
    }

    Ok(ReplicaWritePlan {
        initial_replicas: initial as usize,
        min_needed_replicas: min_needed as usize,
        max_retry_node: initial.max(write_max_node_retry) as usize,
    })
}

/// Clamp `goosefs.worker.read.cache.min.ratio` like Java `CommonUtils.getCacheMinRatio`.
/// Values outside `[0, 1)` fall back to `0.1`.
pub(crate) fn cache_min_ratio(raw: f64) -> f64 {
    if !(0.0..1.0).contains(&raw) {
        0.1
    } else {
        raw
    }
}

/// Whether a worker has enough persist capacity for ASYNC_THROUGH, matching
/// Java `GooseFSBlockStore.filterNoSpaceWorkers`.
///
/// `persistCapacity = ceil(capacityBytes * (1 - cacheMinRatio))`
/// `remainBytes = persistCapacity - persistUsedBytes`
/// Eligible when `remainBytes >= minRemainBytes` and
/// `remainBytes / persistCapacity >= minRemainRatio`.
pub(crate) fn worker_has_persist_space(
    worker: &WorkerInfo,
    min_remain_bytes: i64,
    min_remain_ratio: f32,
    cache_min_ratio: f64,
) -> bool {
    if worker.forbid_write.unwrap_or(false) {
        return false;
    }
    let capacity = worker.capacity_bytes.unwrap_or(0);
    if capacity <= 0 {
        return false;
    }
    let persist_capacity = ((capacity as f64) * (1.0 - cache_min_ratio)).ceil() as i64;
    if persist_capacity <= 0 {
        return false;
    }
    let persist_used = worker.persist_used_bytes.unwrap_or(0);
    let remain_bytes = persist_capacity - persist_used;
    remain_bytes >= min_remain_bytes
        && (remain_bytes as f32) / (persist_capacity as f32) >= min_remain_ratio
}

/// Filter workers for ASYNC_THROUGH writes.
///
/// 1. Drop `forbid_write` workers.
/// 2. Keep those above the persist watermark.
/// 3. If `allow_fallback` (non-first block) and the watermarked set is
///    smaller than `min_needed`, fall back to all writable workers.
pub(crate) fn filter_no_space_workers(
    workers: &[WorkerInfo],
    allow_fallback: bool,
    min_needed: usize,
    min_remain_bytes: i64,
    min_remain_ratio: f32,
    cache_min_ratio: f64,
) -> Vec<WorkerInfo> {
    if workers.is_empty() {
        return Vec::new();
    }
    let available: Vec<WorkerInfo> = workers
        .iter()
        .filter(|w| !w.forbid_write.unwrap_or(false))
        .cloned()
        .collect();
    let has_space: Vec<WorkerInfo> = available
        .iter()
        .filter(|w| {
            worker_has_persist_space(w, min_remain_bytes, min_remain_ratio, cache_min_ratio)
        })
        .cloned()
        .collect();
    if allow_fallback && has_space.len() < min_needed {
        available
    } else {
        has_space
    }
}

/// Apply `min(replication, alive)` degrade, matching Java after
/// `filterNoSpaceWorkers`.
///
/// ASYNC_THROUGH keeps `min_needed` as a hard floor; if `alive < min_needed`
/// the caller must raise `ResourceExhausted` when opening writers.
/// Other write types lower `min_needed` together with `initial`.
pub(crate) fn degrade_replicas(
    async_through: bool,
    mut initial: usize,
    mut min_needed: usize,
    alive: usize,
) -> (usize, usize) {
    if initial > alive {
        initial = alive;
        if !async_through {
            min_needed = initial;
        }
    }
    (initial, min_needed)
}

/// Java `executeWithReplication`: abort remaining tasks when
/// `failures > writerSize - durableMin`.
pub(crate) fn should_abort_remaining(
    failures: usize,
    writer_size: usize,
    min_needed: usize,
) -> bool {
    failures > writer_size.saturating_sub(min_needed)
}

/// Whether a replica fan-out succeeded (success count vs durable.min).
pub(crate) fn enough_replicas(success: usize, min_needed: usize) -> bool {
    success >= min_needed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::grpc::WorkerNetAddress;

    fn worker(
        id: i64,
        host: &str,
        capacity: i64,
        persist_used: i64,
        forbid_write: bool,
    ) -> WorkerInfo {
        WorkerInfo {
            id: Some(id),
            address: Some(WorkerNetAddress {
                host: Some(host.to_string()),
                rpc_port: Some(9203),
                ..Default::default()
            }),
            capacity_bytes: Some(capacity),
            persist_used_bytes: Some(persist_used),
            forbid_write: Some(forbid_write),
            ..Default::default()
        }
    }

    #[test]
    fn async_through_uses_durable_when_greater_than_number() {
        let plan = replica_write_plan(true, 1, 2, 2, 3).unwrap();
        assert_eq!(plan.initial_replicas, 2);
        assert_eq!(plan.min_needed_replicas, 2);
        assert_eq!(plan.max_retry_node, 3);
    }

    #[test]
    fn async_through_keeps_number_when_durable_not_greater() {
        let plan = replica_write_plan(true, 3, 2, 2, 3).unwrap();
        assert_eq!(plan.initial_replicas, 3);
        assert_eq!(plan.min_needed_replicas, 2);
        assert_eq!(plan.max_retry_node, 3);
    }

    #[test]
    fn must_cache_uses_replication_number() {
        let plan = replica_write_plan(false, 2, 3, 2, 3).unwrap();
        assert_eq!(plan.initial_replicas, 2);
        assert_eq!(plan.min_needed_replicas, 2);
        assert_eq!(plan.max_retry_node, 3);
    }

    #[test]
    fn async_through_durable_below_min_is_invalid() {
        let err = replica_write_plan(true, 1, 1, 2, 3).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn cache_min_ratio_clamps_out_of_range() {
        assert_eq!(cache_min_ratio(0.1), 0.1);
        assert_eq!(cache_min_ratio(-0.1), 0.1);
        assert_eq!(cache_min_ratio(1.0), 0.1);
        assert_eq!(cache_min_ratio(0.0), 0.0);
        assert_eq!(cache_min_ratio(0.5), 0.5);
    }

    #[test]
    fn filter_drops_forbid_write_and_low_space() {
        // persistCapacity = ceil(1TB * 0.9) ≈ 0.9TB; 128MB / 0.015 watermark.
        let tb = 1024i64 * 1024 * 1024 * 1024;
        let plenty = worker(1, "a", tb, 0, false);
        let forbidden = worker(2, "b", tb, 0, true);
        let no_cap = worker(3, "c", 0, 0, false);
        // Almost full: remain << 128MB.
        let full = worker(4, "d", tb, (tb as f64 * 0.9) as i64 - 1024, false);

        let out = filter_no_space_workers(
            &[plenty.clone(), forbidden, no_cap, full],
            false,
            2,
            128 * 1024 * 1024,
            0.015,
            0.1,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, Some(1));
    }

    #[test]
    fn later_block_falls_back_when_watermarked_set_too_small() {
        let tb = 1024i64 * 1024 * 1024 * 1024;
        let plenty = worker(1, "a", tb, 0, false);
        let full = worker(2, "b", tb, (tb as f64 * 0.9) as i64 - 1024, false);

        let first = filter_no_space_workers(
            &[plenty.clone(), full.clone()],
            false,
            2,
            128 * 1024 * 1024,
            0.015,
            0.1,
        );
        assert_eq!(first.len(), 1, "first block is strict");

        let later =
            filter_no_space_workers(&[plenty, full], true, 2, 128 * 1024 * 1024, 0.015, 0.1);
        assert_eq!(later.len(), 2, "later block falls back to writable workers");
    }

    #[test]
    fn degrade_async_through_keeps_durable_min() {
        let (initial, min_needed) = degrade_replicas(true, 3, 2, 1);
        assert_eq!(initial, 1);
        assert_eq!(min_needed, 2);
    }

    #[test]
    fn degrade_must_cache_lowers_min_needed() {
        let (initial, min_needed) = degrade_replicas(false, 3, 3, 1);
        assert_eq!(initial, 1);
        assert_eq!(min_needed, 1);
    }

    #[test]
    fn abort_when_failures_exceed_slack() {
        // 3 writers, min 2 → abort after 2 failures (failures > 3-2).
        assert!(!should_abort_remaining(1, 3, 2));
        assert!(should_abort_remaining(2, 3, 2));
        assert!(enough_replicas(2, 2));
        assert!(!enough_replicas(1, 2));
    }
}
