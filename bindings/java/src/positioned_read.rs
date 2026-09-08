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

//! Shared positioned-read + worker-acquire logic (Python `positioned_read.rs`).

use goosefs_sdk::client::WorkerClient;
use goosefs_sdk::fs::options::InStreamOptions;
use goosefs_sdk::fs::ufs_block_length;
use goosefs_sdk::fs::FileSystem;
use goosefs_sdk::fs::URIStatus;
use goosefs_sdk::io::GrpcBlockReader;
use goosefs_sdk::proto::proto::dataserver::OpenUfsBlockOptions;

use crate::error::Error;
use crate::handle::JavaFsHandle;
use crate::Result;

/// Same value as Java `WorkerClient.DEFAULT_CHUNK_SIZE`.
#[allow(dead_code)]
pub(crate) const DEFAULT_CHUNK_SIZE: i64 = 1 << 20;

pub(crate) fn invalid_arg(message: impl Into<String>) -> Error {
    Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
        message: message.into(),
    })
}

pub(crate) fn validate_positioned_read(
    block_index: i32,
    offset: i64,
    length: i64,
    chunk_size: i64,
) -> Result<usize> {
    if block_index < 0 {
        return Err(invalid_arg(format!(
            "block_index must be non-negative, got {block_index}"
        )));
    }
    if offset < 0 {
        return Err(invalid_arg("offset must be non-negative"));
    }
    if length < -1 {
        return Err(invalid_arg(format!(
            "length must be -1 (read to end of block) or non-negative, got {length}"
        )));
    }
    if chunk_size <= 0 {
        return Err(invalid_arg("chunk_size must be positive"));
    }
    Ok(block_index as usize)
}

pub(crate) fn validate_block_read(offset: i64, length: i64, chunk_size: i64) -> Result<()> {
    if offset < 0 {
        return Err(invalid_arg("offset must be non-negative"));
    }
    if length < 0 {
        return Err(invalid_arg("length must be non-negative"));
    }
    if chunk_size <= 0 {
        return Err(invalid_arg("chunk_size must be positive"));
    }
    Ok(())
}

pub(crate) fn format_worker_addr(addr: &goosefs_sdk::proto::grpc::WorkerNetAddress) -> String {
    format!(
        "{}:{}",
        addr.host.as_deref().unwrap_or("127.0.0.1"),
        addr.rpc_port.unwrap_or(9203)
    )
}

pub(crate) fn resolve_block_id(
    status: &URIStatus,
    block_index: usize,
    path: &str,
) -> Result<(i64, i64)> {
    let mut fbi_pairs: Vec<(i64, i64)> = status
        .block_infos()
        .values()
        .filter_map(|fbi| {
            let id = fbi.block_info.as_ref()?.block_id?;
            if id <= 0 {
                return None;
            }
            Some((fbi.offset.unwrap_or(0), id))
        })
        .collect();
    fbi_pairs.sort_by_key(|(off, _)| *off);
    let fbi_ids: Vec<i64> = fbi_pairs.into_iter().map(|(_, id)| id).collect();
    let block_ids: &[i64] = if !fbi_ids.is_empty() {
        &fbi_ids
    } else {
        &status.block_ids
    };
    if block_ids.is_empty() {
        return Err(invalid_arg(format!(
            "path {path:?} has no blocks (empty file or directory)"
        )));
    }
    if block_index >= block_ids.len() {
        return Err(invalid_arg(format!(
            "block_index={block_index} out of range (file {path:?} has {} block(s))",
            block_ids.len()
        )));
    }
    let block_id = block_ids[block_index];
    Ok((block_id, actual_block_length(status, block_id, block_index)))
}

fn actual_block_length(status: &URIStatus, block_id: i64, block_index: usize) -> i64 {
    let configured = status.block_size_bytes.max(0);
    let block_offset = status
        .get_block_info(block_id)
        .and_then(|fbi| fbi.offset)
        .filter(|o| *o >= 0)
        .unwrap_or_else(|| {
            if configured > 0 {
                (block_index as i64).saturating_mul(configured)
            } else {
                0
            }
        });
    let remaining = status.length.saturating_sub(block_offset);
    if remaining <= 0 {
        return 0;
    }
    let cap = if configured > 0 {
        remaining.min(configured)
    } else {
        remaining
    };
    if let Some(len) = status
        .get_block_info(block_id)
        .and_then(|fbi| fbi.block_info.as_ref())
        .and_then(|bi| bi.length)
        .filter(|len| *len > 0)
    {
        return len.min(cap);
    }
    cap
}

pub(crate) fn open_ufs_block_options(
    status: &URIStatus,
    block_index: usize,
) -> Option<OpenUfsBlockOptions> {
    if status.ufs_path.is_empty() {
        return None;
    }
    let block_size = status.block_size_bytes;
    let offset_in_file = if block_size > 0 {
        (block_index as i64).saturating_mul(block_size)
    } else {
        0
    };
    Some(OpenUfsBlockOptions {
        ufs_path: Some(status.ufs_path.clone()),
        offset_in_file: Some(offset_in_file),
        block_size: Some(ufs_block_length(
            status.length,
            block_size,
            block_index as u64,
        )),
        max_ufs_read_concurrency: Some(InStreamOptions::default().max_ufs_read_concurrency),
        mount_id: Some(status.mount_id),
        no_cache: Some(!status.cacheable),
        user: None,
        caller_type: None,
        file_length: Some(status.length),
    })
}

pub(crate) fn open_ufs_block_options_for_block_id(
    status: &URIStatus,
    block_id: i64,
) -> Option<OpenUfsBlockOptions> {
    let block_index = status.block_ids.iter().position(|&id| id == block_id)?;
    open_ufs_block_options(status, block_index)
}

pub(crate) fn block_locations_from_status(
    status: &URIStatus,
    block_id: i64,
) -> Vec<goosefs_sdk::proto::grpc::BlockLocation> {
    status
        .get_block_info(block_id)
        .and_then(|fbi| fbi.block_info.as_ref())
        .map(|bi| bi.locations.clone())
        .unwrap_or_default()
}

pub(crate) async fn positioned_read(
    handle: JavaFsHandle,
    path: String,
    block_index: usize,
    offset: i64,
    length: i64,
    chunk_size: i64,
) -> Result<Vec<u8>> {
    let status = handle.fs.get_status(&path).await?;
    let (block_id, block_size) = resolve_block_id(&status, block_index, &path)?;
    if offset >= block_size {
        return Err(invalid_arg(format!(
            "offset={offset} >= actual_block_length={block_size}"
        )));
    }
    let effective_length = if length < 0 {
        block_size - offset
    } else {
        length.min(block_size - offset)
    };
    if effective_length == 0 {
        return Ok(Vec::new());
    }
    positioned_read_with_reauth(
        handle,
        &status,
        block_id,
        block_index,
        offset,
        effective_length,
        chunk_size,
    )
    .await
}

async fn positioned_read_with_reauth(
    handle: JavaFsHandle,
    status: &URIStatus,
    block_id: i64,
    block_index: usize,
    offset: i64,
    effective_length: i64,
    chunk_size: i64,
) -> Result<Vec<u8>> {
    let locations = block_locations_from_status(status, block_id);
    let ufs_opts = open_ufs_block_options(status, block_index);
    let replication = handle.ctx.config().file_replication_number;
    let max_retry_node = handle.ctx.config().file_read_max_node_retry;
    let worker_info = handle
        .ctx
        .acquire_router()
        .select_worker_for_read(block_id, &locations, replication, max_retry_node)
        .await?;
    let net_addr = worker_info
        .address
        .as_ref()
        .ok_or_else(|| Error::IllegalState("selected worker has no address".into()))?;
    let worker_addr = format_worker_addr(net_addr);
    let pool = handle.ctx.acquire_worker_pool();
    let client = match pool.acquire(&worker_addr).await {
        Ok(c) => c,
        Err(e) if e.is_authentication_failed() => pool.reconnect(&worker_addr).await?,
        Err(e) => return Err(e.into()),
    };
    let stale_generation = client.generation();
    let bytes = match GrpcBlockReader::positioned_read(
        &client,
        block_id,
        offset,
        effective_length,
        chunk_size,
        ufs_opts.clone(),
    )
    .await
    {
        Ok(b) => b,
        Err(e) if e.is_authentication_failed() => {
            let fresh = pool
                .reconnect_if_stale(&worker_addr, stale_generation)
                .await?;
            GrpcBlockReader::positioned_read(
                &fresh,
                block_id,
                offset,
                effective_length,
                chunk_size,
                ufs_opts,
            )
            .await?
        }
        Err(e) => return Err(e.into()),
    };
    Ok(bytes.to_vec())
}

pub(crate) async fn acquire_worker_for_block(
    handle: JavaFsHandle,
    block_id: i64,
    path: Option<String>,
) -> Result<(WorkerClient, Option<(i64, OpenUfsBlockOptions)>)> {
    let (locations, ufs_opts_for_block) = if let Some(ref p) = path {
        let status = handle.fs.get_status(p).await?;
        let locations = block_locations_from_status(&status, block_id);
        let ufs_opts =
            open_ufs_block_options_for_block_id(&status, block_id).map(|opts| (block_id, opts));
        (locations, ufs_opts)
    } else {
        (Vec::new(), None)
    };
    let replication = handle.ctx.config().file_replication_number;
    let max_retry_node = handle.ctx.config().file_read_max_node_retry;
    let worker_info = handle
        .ctx
        .acquire_router()
        .select_worker_for_read(block_id, &locations, replication, max_retry_node)
        .await?;
    let net_addr = worker_info
        .address
        .as_ref()
        .ok_or_else(|| Error::IllegalState("selected worker has no address".into()))?;
    let worker_addr = format_worker_addr(net_addr);
    let client = handle
        .ctx
        .acquire_worker_pool()
        .acquire(&worker_addr)
        .await?;
    Ok((client, ufs_opts_for_block))
}

#[cfg(test)]
mod tests {
    use goosefs_sdk::proto::grpc::file::{FileBlockInfo, FileInfo};
    use goosefs_sdk::proto::grpc::BlockInfo;

    use super::*;

    fn status_with_blocks(
        length: i64,
        block_size_bytes: i64,
        blocks: &[(i64, i64, i64)],
    ) -> URIStatus {
        let file_block_infos: Vec<FileBlockInfo> = blocks
            .iter()
            .map(|(id, offset, blen)| FileBlockInfo {
                block_info: Some(BlockInfo {
                    block_id: Some(*id),
                    length: Some(*blen),
                    max_replicas: None,
                    locations: vec![],
                }),
                offset: Some(*offset),
                ufs_locations: vec![],
                ufs_string_locations: vec![],
            })
            .collect();
        let block_ids: Vec<i64> = blocks.iter().map(|(id, _, _)| *id).collect();
        URIStatus::from_proto(FileInfo {
            length: Some(length),
            block_size_bytes: Some(block_size_bytes),
            block_ids,
            file_block_infos,
            completed: Some(true),
            ..Default::default()
        })
    }

    fn status_with_ufs_path(length: i64, block_size_bytes: i64, num_blocks: i64) -> URIStatus {
        URIStatus::from_proto(FileInfo {
            length: Some(length),
            block_size_bytes: Some(block_size_bytes),
            block_ids: (1001..1001 + num_blocks).collect(),
            completed: Some(true),
            ufs_path: Some("cosn://bucket/tail.lance".to_string()),
            mount_id: Some(7),
            ..Default::default()
        })
    }

    #[test]
    fn validate_rejects_length_below_minus_one() {
        assert!(validate_positioned_read(0, 0, -2, DEFAULT_CHUNK_SIZE).is_err());
        assert!(validate_positioned_read(0, 0, -1, DEFAULT_CHUNK_SIZE).is_ok());
        assert!(validate_positioned_read(-1, 0, 1, DEFAULT_CHUNK_SIZE).is_err());
    }

    #[test]
    fn resolve_block_id_prefers_file_block_infos() {
        let status = status_with_blocks(100, 64, &[(11, 0, 64), (22, 64, 36)]);
        let (id, len) = resolve_block_id(&status, 1, "/f").unwrap();
        assert_eq!(id, 22);
        assert_eq!(len, 36);
    }

    #[test]
    fn resolve_block_id_out_of_range() {
        let status = status_with_blocks(10, 64, &[(1, 0, 10)]);
        assert!(resolve_block_id(&status, 3, "/f").is_err());
    }

    #[test]
    fn open_ufs_block_options_carry_actual_tail_block_length() {
        let bs = 1 << 20i64;
        let status = status_with_ufs_path(2 * bs + 100, bs, 3);
        let seen: Vec<(i64, i64)> = (0..3)
            .map(|idx| {
                let opts = open_ufs_block_options(&status, idx).unwrap();
                (opts.offset_in_file.unwrap(), opts.block_size.unwrap())
            })
            .collect();
        assert_eq!(seen, vec![(0, bs), (bs, bs), (2 * bs, 100)]);
    }
}
