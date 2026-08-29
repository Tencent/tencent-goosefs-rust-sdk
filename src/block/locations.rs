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
//! 3. Overwriting `BlockInfo.locations` where the worker reports
//!    `block_cached_bytes > 0` (GooseFS 2.0; 2.1.0 sends bool-as-0/1);
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

/// Result of probing workers for block cache locations.
struct ProbedLocations {
    /// Locations discovered via successful `CheckBlocks` RPCs.
    locations: HashMap<i64, Vec<BlockLocation>>,
    /// Blocks that received ≥1 successful CheckBlocks response (authoritative
    /// even when the returned location list is empty). Blocks missing from
    /// this set had only failed/skipped probes — Master locations are kept.
    probed_ok: HashSet<i64>,
}

/// Overwrite `FileInfo` block locations by probing workers via `CheckBlocks`.
///
/// Mirrors Java `GooseFSBlockStore.getFileBlockLocations` +
/// `BaseFileSystem.populateFilePercentage`, with one intentional hardening:
/// Master locations are retained when every probe for a block failed
/// (connect/RPC error), so a transient failure does not wipe known locations
/// and force hash fallback. A successful probe that reports "not present"
/// still overwrites with `[]` (authoritative negative).
///
/// - `check_count == 0`: no-op (Java when `checkBlockReplicas` unset / 0).
/// - Failures talking to individual workers are logged and skipped; other
///   workers still contribute locations.
/// - Recomputes `in_goose_fs_percentage` from the post-merge locations.
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

    let probed = fetch_block_locations(router, pool, config, &block_ids, check_count).await?;
    apply_probed_locations(file_info, &probed);
    Ok(())
}

/// Apply probe results onto `FileInfo`.
///
/// - Block in `probed_ok`: overwrite Master locations (empty = authoritative miss).
/// - Block not in `probed_ok`: keep Master locations (probe never succeeded).
fn apply_probed_locations(file_info: &mut FileInfo, probed: &ProbedLocations) {
    for fbi in &mut file_info.file_block_infos {
        let Some(bi) = fbi.block_info.as_mut() else {
            continue;
        };
        let Some(block_id) = bi.block_id else {
            continue;
        };
        if !probed.probed_ok.contains(&block_id) {
            continue;
        }
        bi.locations = probed.locations.get(&block_id).cloned().unwrap_or_default();
    }
    recompute_in_goosefs_percentage(file_info);
}

/// Outcome of one worker's CheckBlocks RPC.
enum WorkerCheckOutcome {
    /// RPC succeeded for `queried` block ids (map may omit absent blocks).
    Ok {
        worker: WorkerInfo,
        queried: Vec<i64>,
        /// `block_id → cached_bytes` (`> 0` means present; 2.1.0 wire is 0/1).
        cached_bytes: HashMap<i64, i64>,
    },
    /// Connect or RPC failed — do not treat queried blocks as probed.
    Failed,
}

/// Hash-select workers per block, batch `CheckBlocks`.
async fn fetch_block_locations(
    router: &WorkerRouterView,
    pool: Option<&Arc<WorkerClientPool>>,
    config: &GoosefsConfig,
    block_ids: &[i64],
    check_count: usize,
) -> Result<ProbedLocations> {
    let mut locations: HashMap<i64, Vec<BlockLocation>> = HashMap::new();
    let mut probed_ok: HashSet<i64> = HashSet::new();

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
        let queried: Vec<i64> = ids.into_iter().collect();
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
                    return WorkerCheckOutcome::Failed;
                }
            };
            match client.check_blocks(&queried).await {
                Ok(cached_bytes) => WorkerCheckOutcome::Ok {
                    worker,
                    queried,
                    cached_bytes,
                },
                Err(e) => {
                    warn!(
                        worker = %endpoint,
                        error = %e,
                        "checkBlocks: RPC failed"
                    );
                    WorkerCheckOutcome::Failed
                }
            }
        });
    }

    let outcomes = futures::future::join_all(tasks).await;
    for outcome in outcomes {
        let WorkerCheckOutcome::Ok {
            worker,
            queried,
            cached_bytes,
        } = outcome
        else {
            continue;
        };

        // Successful RPC is authoritative for every queried block id, even
        // when the map omits them or reports cached_bytes=0.
        for &block_id in &queried {
            probed_ok.insert(block_id);
            locations.entry(block_id).or_default();
        }

        let worker_id = worker.id;
        let address = worker.address.clone();
        for (block_id, bytes) in cached_bytes {
            if bytes <= 0 {
                continue;
            }
            locations.entry(block_id).or_default().push(BlockLocation {
                worker_id,
                worker_address: address.clone(),
            });
            debug!(
                block_id,
                worker_id = ?worker_id,
                cached_bytes = bytes,
                "checkBlocks: block present on worker"
            );
        }
    }

    Ok(ProbedLocations {
        locations,
        probed_ok,
    })
}

/// Recompute `in_goose_fs_percentage` from current `FileInfo` locations.
///
/// Enrichment currently stores locations only (not per-worker cached bytes),
/// so a location counts as the full block length — matching FILE-mode
/// committed blocks. PAGE partial cache would need cached_bytes threaded
/// through `BlockLocation` to be more precise.
fn recompute_in_goosefs_percentage(file_info: &mut FileInfo) {
    let file_length = file_info.length.unwrap_or(0);
    if file_length == 0 {
        file_info.in_goose_fs_percentage = Some(100);
        return;
    }

    // Location present → count full block length (FILE-mode approximation).
    let mut cache_size: i64 = 0;
    for fbi in &file_info.file_block_infos {
        let Some(bi) = fbi.block_info.as_ref() else {
            continue;
        };
        if bi.locations.is_empty() {
            continue;
        }
        let block_length = bi.length.unwrap_or(0).max(0);
        cache_size += block_length;
    }

    let pct = ((cache_size.saturating_mul(100)) / file_length).min(100);
    file_info.in_goose_fs_percentage = Some(pct as i32);
}

/// Fill `in_goose_fs_percentage` without probing workers (Java
/// `populateFilePercentage` cheap paths).
///
/// Master `GetStatus` never computes this field (see
/// `MutableInodeFile.generateClientFileInfo`). Java only overwrites it when
/// the caller sets `checkBlockReplicas > 0`. Python `get_status()` has no
/// such argument, so MustCache writes otherwise always report `0` even though
/// the file lives entirely in GooseFS.
///
/// Applied only when CheckBlocks is off (`check_block_replicas == 0`):
/// - empty file → 100
/// - `TO_BE_PERSISTED` (ASYNC_THROUGH pending UFS) → 100 (Java shortcut)
/// - completed MustCache (`cacheable && !persisted`) → 100
/// - otherwise recompute from Master `BlockInfo.locations` (may stay 0)
pub fn fill_in_goosefs_percentage_without_probe(file_info: &mut FileInfo) {
    if file_info.folder.unwrap_or(false) {
        return;
    }
    if file_info.length.unwrap_or(0) == 0 {
        file_info.in_goose_fs_percentage = Some(100);
        return;
    }
    let state = file_info.persistence_state.as_deref().unwrap_or("");
    if state.eq_ignore_ascii_case("TO_BE_PERSISTED") {
        file_info.in_goose_fs_percentage = Some(100);
        return;
    }
    if file_info.completed.unwrap_or(false)
        && file_info.cacheable.unwrap_or(false)
        && !file_info.persisted.unwrap_or(false)
        && (state.is_empty() || state.eq_ignore_ascii_case("NOT_PERSISTED"))
    {
        file_info.in_goose_fs_percentage = Some(100);
        return;
    }
    recompute_in_goosefs_percentage(file_info);
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

    fn master_loc(worker_id: i64, host: &str) -> BlockLocation {
        BlockLocation {
            worker_id: Some(worker_id),
            worker_address: Some(WorkerNetAddress {
                host: Some(host.into()),
                ..Default::default()
            }),
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
        fi.file_block_infos[0]
            .block_info
            .as_mut()
            .unwrap()
            .locations = vec![master_loc(1, "w1")];
        fi.file_block_infos[1]
            .block_info
            .as_mut()
            .unwrap()
            .locations = vec![master_loc(2, "w2")];
        recompute_in_goosefs_percentage(&mut fi);
        assert_eq!(fi.in_goose_fs_percentage, Some(100));
    }

    #[test]
    fn test_recompute_percentage_empty_locations() {
        let mut fi = FileInfo {
            length: Some(200),
            file_block_infos: vec![fbi(1, 0, 100), fbi(2, 100, 100)],
            ..Default::default()
        };
        recompute_in_goosefs_percentage(&mut fi);
        assert_eq!(fi.in_goose_fs_percentage, Some(0));
    }

    #[test]
    fn fill_percentage_must_cache_completed_file_is_100() {
        let mut fi = FileInfo {
            length: Some(4096),
            completed: Some(true),
            folder: Some(false),
            cacheable: Some(true),
            persisted: Some(false),
            persistence_state: Some("NOT_PERSISTED".to_string()),
            file_block_infos: vec![fbi(1, 0, 4096)],
            in_goose_fs_percentage: Some(0),
            ..Default::default()
        };
        fill_in_goosefs_percentage_without_probe(&mut fi);
        assert_eq!(fi.in_goose_fs_percentage, Some(100));
    }

    #[test]
    fn fill_percentage_to_be_persisted_is_100() {
        let mut fi = FileInfo {
            length: Some(100),
            completed: Some(true),
            folder: Some(false),
            cacheable: Some(true),
            persisted: Some(false),
            persistence_state: Some("TO_BE_PERSISTED".to_string()),
            in_goose_fs_percentage: Some(0),
            ..Default::default()
        };
        fill_in_goosefs_percentage_without_probe(&mut fi);
        assert_eq!(fi.in_goose_fs_percentage, Some(100));
    }

    #[test]
    fn fill_percentage_through_file_stays_zero_without_locations() {
        let mut fi = FileInfo {
            length: Some(200),
            completed: Some(true),
            folder: Some(false),
            cacheable: Some(false),
            persisted: Some(true),
            persistence_state: Some("PERSISTED".to_string()),
            file_block_infos: vec![fbi(1, 0, 200)],
            in_goose_fs_percentage: Some(0),
            ..Default::default()
        };
        fill_in_goosefs_percentage_without_probe(&mut fi);
        assert_eq!(fi.in_goose_fs_percentage, Some(0));
    }

    #[test]
    fn fill_percentage_empty_file_is_100() {
        let mut fi = FileInfo {
            length: Some(0),
            completed: Some(true),
            folder: Some(false),
            in_goose_fs_percentage: Some(0),
            ..Default::default()
        };
        fill_in_goosefs_percentage_without_probe(&mut fi);
        assert_eq!(fi.in_goose_fs_percentage, Some(100));
    }

    #[test]
    fn apply_keeps_master_locations_when_probe_failed() {
        let mut fi = FileInfo {
            length: Some(100),
            file_block_infos: vec![fbi(1, 0, 100)],
            ..Default::default()
        };
        fi.file_block_infos[0]
            .block_info
            .as_mut()
            .unwrap()
            .locations = vec![master_loc(9, "master-known")];

        // No successful probe for block 1 → keep Master.
        apply_probed_locations(
            &mut fi,
            &ProbedLocations {
                locations: HashMap::new(),
                probed_ok: HashSet::new(),
            },
        );

        let locs = &fi.file_block_infos[0]
            .block_info
            .as_ref()
            .unwrap()
            .locations;
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].worker_id, Some(9));
        assert_eq!(fi.in_goose_fs_percentage, Some(100));
    }

    #[test]
    fn apply_overwrites_with_empty_on_authoritative_miss() {
        let mut fi = FileInfo {
            length: Some(100),
            file_block_infos: vec![fbi(1, 0, 100)],
            ..Default::default()
        };
        fi.file_block_infos[0]
            .block_info
            .as_mut()
            .unwrap()
            .locations = vec![master_loc(9, "stale")];

        // Probe succeeded but found nothing → wipe stale Master location.
        let mut locations = HashMap::new();
        locations.insert(1, Vec::new());
        apply_probed_locations(
            &mut fi,
            &ProbedLocations {
                locations,
                probed_ok: HashSet::from([1]),
            },
        );

        assert!(fi.file_block_infos[0]
            .block_info
            .as_ref()
            .unwrap()
            .locations
            .is_empty());
        assert_eq!(fi.in_goose_fs_percentage, Some(0));
    }

    #[test]
    fn apply_overwrites_with_probed_locations() {
        let mut fi = FileInfo {
            length: Some(100),
            file_block_infos: vec![fbi(1, 0, 100)],
            ..Default::default()
        };
        fi.file_block_infos[0]
            .block_info
            .as_mut()
            .unwrap()
            .locations = vec![master_loc(9, "stale")];

        let mut locations = HashMap::new();
        locations.insert(1, vec![master_loc(3, "probed")]);
        apply_probed_locations(
            &mut fi,
            &ProbedLocations {
                locations,
                probed_ok: HashSet::from([1]),
            },
        );

        let locs = &fi.file_block_infos[0]
            .block_info
            .as_ref()
            .unwrap()
            .locations;
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].worker_id, Some(3));
        assert_eq!(fi.in_goose_fs_percentage, Some(100));
    }
}
