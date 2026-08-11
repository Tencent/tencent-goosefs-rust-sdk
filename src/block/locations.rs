// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this work except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Probe workers for real block cache locations (Java `checkBlocks` parity).
//!
//! Master `GetStatus` often returns `FileBlockInfo` with **empty** `locations`
//! even when blocks are cached on workers (e.g. CACHE load without
//! `commitLocation`). Java `fs stat --check_replicas=N` and
//! `BaseFileSystem.populateFilePercentage` fix this by:
//!
//! 1. Hash-selecting up to `checkCount` workers per block
//!    (`ClientWorkerManager.getBlockWorkers`);
//! 2. Batching `CheckBlocks` RPCs;
//! 3. Overwriting `BlockInfo.locations` where the worker reports the block
//!    present (`block_exists=true` on GooseFS 2.1.0);
//! 4. Recomputing `inGooseFSPercentage`.
//!
//! This module ports that enrichment so Rust `select_worker_for_read` can
//! prefer workers that actually hold the data.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, warn};

use crate::block::router::{rpc_endpoint, WorkerRouter, WorkerRouterView};
use crate::client::{WorkerClient, WorkerClientPool};
use crate::config::GoosefsConfig;
use crate::error::Result;
use crate::proto::grpc::block::WorkerInfo;
use crate::proto::grpc::file::FileInfo;
use crate::proto::grpc::BlockLocation;

/// Acquire a [`WorkerClient`] for `addr` from a pool or by direct connect.
async fn acquire_worker(
    addr: &str,
    pool: Option<&Arc<WorkerClientPool>>,
    config: &GoosefsConfig,
) -> Result<WorkerClient> {
    if let Some(pool) = pool {
        pool.acquire(addr).await
    } else {
        WorkerClient::connect(addr, config).await
    }
}

/// Ensure `FileInfo.block_ids` is populated from `file_block_infos` when the
/// Master omits `blockIds` in the GetStatus proto (Java `GrpcUtils.toProto`
/// does not serialise them).
pub fn ensure_block_ids_from_file_block_infos(file_info: &mut FileInfo) {
    if !file_info.block_ids.is_empty() || file_info.file_block_infos.is_empty() {
        return;
    }
    let mut pairs: Vec<(i64, i64)> = file_info
        .file_block_infos
        .iter()
        .filter_map(|fbi| {
            let offset = fbi.offset.unwrap_or(0);
            let id = fbi.block_info.as_ref()?.block_id?;
            if id > 0 {
                Some((offset, id))
            } else {
                None
            }
        })
        .collect();
    pairs.sort_by_key(|(offset, _)| *offset);
    file_info.block_ids = pairs.into_iter().map(|(_, id)| id).collect();
}

/// Overwrite `FileInfo` block locations by probing workers via `CheckBlocks`.
///
/// Mirrors Java `GooseFSBlockStore.getFileBlockLocations` +
/// `BaseFileSystem.populateFilePercentage`.
///
/// - `check_count == 0`: no-op (Java when `checkBlockReplicas` unset / 0).
/// - Failures talking to individual workers are logged and skipped; other
///   workers still contribute locations.
/// - Always recomputes `in_goose_fs_percentage` from probed `cached_bytes`
///   when `check_count > 0` and the file has block infos.
pub async fn enrich_file_block_locations_with_router(
    file_info: &mut FileInfo,
    router: &WorkerRouter,
    pool: Option<&Arc<WorkerClientPool>>,
    config: &GoosefsConfig,
    check_count: usize,
) -> Result<()> {
    let view = WorkerRouterView::from_shared(router);
    enrich_file_block_locations(file_info, &view, pool, config, check_count).await
}

/// Same as [`enrich_file_block_locations_with_router`] but takes a
/// [`WorkerRouterView`] (read-path local snapshot).
pub async fn enrich_file_block_locations(
    file_info: &mut FileInfo,
    router: &WorkerRouterView,
    pool: Option<&Arc<WorkerClientPool>>,
    config: &GoosefsConfig,
    check_count: usize,
) -> Result<()> {
    ensure_block_ids_from_file_block_infos(file_info);

    if check_count == 0 || file_info.file_block_infos.is_empty() {
        return Ok(());
    }

    let block_ids: Vec<i64> = file_info
        .file_block_infos
        .iter()
        .filter_map(|fbi| fbi.block_info.as_ref()?.block_id)
        .filter(|id| *id > 0)
        .collect();
    if block_ids.is_empty() {
        return Ok(());
    }

    let fetched = fetch_block_locations(router, pool, config, &block_ids, check_count).await?;

    // Overwrite Master locations with probed ones (Java populateFilePercentage).
    for fbi in &mut file_info.file_block_infos {
        let Some(bi) = fbi.block_info.as_mut() else {
            continue;
        };
        let Some(block_id) = bi.block_id else {
            continue;
        };
        if let Some(locations) = fetched.get(&block_id) {
            bi.locations = locations.clone();
        }
    }

    recompute_in_goosefs_percentage(file_info, &fetched);
    Ok(())
}

/// Hash-select workers per block, batch `CheckBlocks`, return locations with
/// `cached_bytes > 0`.
async fn fetch_block_locations(
    router: &WorkerRouterView,
    pool: Option<&Arc<WorkerClientPool>>,
    config: &GoosefsConfig,
    block_ids: &[i64],
    check_count: usize,
) -> Result<HashMap<i64, Vec<BlockLocation>>> {
    let mut result: HashMap<i64, Vec<BlockLocation>> =
        block_ids.iter().map(|id| (*id, Vec::new())).collect();

    // block_id → candidate workers (Java getBlockWorkers).
    let mut block_to_workers: HashMap<i64, Vec<WorkerInfo>> = HashMap::new();
    for &block_id in block_ids {
        match router.select_workers(block_id, check_count).await {
            Ok(workers) => {
                block_to_workers.insert(block_id, workers);
            }
            Err(e) => {
                warn!(
                    block_id,
                    error = %e,
                    "checkBlocks: failed to select workers for block, skipping"
                );
            }
        }
    }

    // Invert to worker_key → (WorkerInfo, block_ids).
    let mut worker_to_blocks: HashMap<String, (WorkerInfo, HashSet<i64>)> = HashMap::new();
    for (block_id, workers) in &block_to_workers {
        for w in workers {
            let Some(addr) = w.address.as_ref() else {
                continue;
            };
            let key = rpc_endpoint(addr);
            worker_to_blocks
                .entry(key)
                .and_modify(|(_, ids)| {
                    ids.insert(*block_id);
                })
                .or_insert_with(|| (w.clone(), HashSet::from([*block_id])));
        }
    }

    // Parallel CheckBlocks per worker.
    let mut tasks = Vec::with_capacity(worker_to_blocks.len());
    for (endpoint, (worker, ids)) in worker_to_blocks {
        let ids: Vec<i64> = ids.into_iter().collect();
        let pool = pool.cloned();
        let config = config.clone();
        tasks.push(async move {
            let client = match acquire_worker(&endpoint, pool.as_ref(), &config).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        worker = %endpoint,
                        error = %e,
                        "checkBlocks: failed to connect worker"
                    );
                    return (worker, HashMap::new());
                }
            };
            match client.check_blocks(&ids).await {
                Ok(map) => (worker, map),
                Err(e) => {
                    warn!(
                        worker = %endpoint,
                        error = %e,
                        "checkBlocks: RPC failed"
                    );
                    (worker, HashMap::new())
                }
            }
        });
    }

    let outcomes = futures::future::join_all(tasks).await;
    for (worker, exists_map) in outcomes {
        let worker_id = worker.id;
        let address = worker.address.clone();
        for (block_id, exists) in exists_map {
            if !exists {
                continue;
            }
            if let Some(locations) = result.get_mut(&block_id) {
                locations.push(BlockLocation {
                    worker_id,
                    worker_address: address.clone(),
                });
                debug!(
                    block_id,
                    worker_id = ?worker_id,
                    "checkBlocks: block_exists=true on worker"
                );
            }
        }
    }

    Ok(result)
}

/// Recompute `in_goose_fs_percentage` from probed locations.
///
/// GooseFS 2.1.0 `CheckBlocks` returns existence (`bool`) only. When a
/// location exists we count the full block length toward the percentage.
fn recompute_in_goosefs_percentage(
    file_info: &mut FileInfo,
    fetched: &HashMap<i64, Vec<BlockLocation>>,
) {
    let file_length = file_info.length.unwrap_or(0);
    if file_length == 0 {
        file_info.in_goose_fs_percentage = Some(100);
        return;
    }

    // 2.1.0 CheckBlocks is bool existence only — count full block length
    // when a location was found (FILE-mode committed block approximation).
    let mut cache_size: i64 = 0;
    for fbi in &file_info.file_block_infos {
        let Some(bi) = fbi.block_info.as_ref() else {
            continue;
        };
        let Some(block_id) = bi.block_id else {
            continue;
        };
        let locations = fetched
            .get(&block_id)
            .map(|v| v.as_slice())
            .unwrap_or(bi.locations.as_slice());
        if locations.is_empty() {
            continue;
        }
        let block_length = bi.length.unwrap_or(0).max(0);
        cache_size += block_length;
    }

    let pct = ((cache_size.saturating_mul(100)) / file_length).min(100);
    file_info.in_goose_fs_percentage = Some(pct as i32);
}

/// Convenience: enrich when `check_count > 0`, swallowing enrichment errors
/// (Master metadata still usable; routing falls back to hash).
pub async fn maybe_enrich_file_block_locations(
    file_info: &mut FileInfo,
    router: &WorkerRouterView,
    pool: Option<&Arc<WorkerClientPool>>,
    config: &GoosefsConfig,
    check_count: i32,
) {
    let count = check_count.max(0) as usize;
    if count == 0 {
        ensure_block_ids_from_file_block_infos(file_info);
        return;
    }
    if let Err(e) = enrich_file_block_locations(file_info, router, pool, config, count).await {
        warn!(
            error = %e,
            "checkBlocks location enrichment failed; continuing with Master locations"
        );
        ensure_block_ids_from_file_block_infos(file_info);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::grpc::file::FileBlockInfo;
    use crate::proto::grpc::{BlockInfo, WorkerNetAddress};

    fn fbi(block_id: i64, offset: i64, length: i64) -> FileBlockInfo {
        FileBlockInfo {
            block_info: Some(BlockInfo {
                block_id: Some(block_id),
                length: Some(length),
                max_replicas: None,
                locations: vec![],
            }),
            offset: Some(offset),
            ufs_locations: vec![],
            ufs_string_locations: vec![],
        }
    }

    #[test]
    fn test_ensure_block_ids_from_file_block_infos() {
        let mut fi = FileInfo {
            length: Some(200),
            block_size_bytes: Some(100),
            file_block_infos: vec![fbi(20, 100, 100), fbi(10, 0, 100)],
            block_ids: vec![],
            ..Default::default()
        };
        ensure_block_ids_from_file_block_infos(&mut fi);
        assert_eq!(fi.block_ids, vec![10, 20]);
    }

    #[test]
    fn test_ensure_block_ids_preserves_existing() {
        let mut fi = FileInfo {
            file_block_infos: vec![fbi(99, 0, 1)],
            block_ids: vec![1, 2],
            ..Default::default()
        };
        ensure_block_ids_from_file_block_infos(&mut fi);
        assert_eq!(fi.block_ids, vec![1, 2]);
    }

    #[test]
    fn test_recompute_percentage_full() {
        let mut fi = FileInfo {
            length: Some(200),
            file_block_infos: vec![fbi(1, 0, 100), fbi(2, 100, 100)],
            ..Default::default()
        };
        let mut fetched = HashMap::new();
        fetched.insert(
            1,
            vec![BlockLocation {
                worker_id: Some(1),
                worker_address: Some(WorkerNetAddress {
                    host: Some("w1".into()),
                    ..Default::default()
                }),
            }],
        );
        fetched.insert(
            2,
            vec![BlockLocation {
                worker_id: Some(2),
                worker_address: Some(WorkerNetAddress {
                    host: Some("w2".into()),
                    ..Default::default()
                }),
            }],
        );
        // Apply locations onto file_info as enrich would.
        for fbi in &mut fi.file_block_infos {
            let id = fbi.block_info.as_ref().unwrap().block_id.unwrap();
            fbi.block_info.as_mut().unwrap().locations = fetched[&id].clone();
        }
        recompute_in_goosefs_percentage(&mut fi, &fetched);
        assert_eq!(fi.in_goose_fs_percentage, Some(100));
    }

    #[test]
    fn test_recompute_percentage_empty_locations() {
        let mut fi = FileInfo {
            length: Some(200),
            file_block_infos: vec![fbi(1, 0, 100), fbi(2, 100, 100)],
            ..Default::default()
        };
        let fetched: HashMap<i64, Vec<BlockLocation>> = HashMap::new();
        recompute_in_goosefs_percentage(&mut fi, &fetched);
        assert_eq!(fi.in_goose_fs_percentage, Some(0));
    }
}
