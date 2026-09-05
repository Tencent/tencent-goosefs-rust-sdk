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

//! Shared positioned-read logic for both async and sync Python bindings.
//!
//! Extracts three pieces of logic that were previously duplicated between
//! `filesystem.rs` (async `AsyncGoosefs`) and `sync_fs.rs` (sync `Goosefs`):
//!
//! 1. `DEFAULT_CHUNK_SIZE` — single source of truth for the default gRPC
//!    chunk size, replacing the former `DEFAULT_POS_READ_CHUNK_SIZE` in
//!    `filesystem.rs` and `DEFAULT_CHUNK_SIZE_BYTES` in `worker.rs`.
//!
//! 2. `resolve_block_id()` — block-id resolution from `URIStatus` (prefer
//!    `file_block_infos` over `block_ids` for freshly-written files).
//!
//! 3. `positioned_read_with_reauth()` — SASL auth-failure retry for the
//!    acquire + read pipeline, ensuring the sync path has the same
//!    resilience as the async path (Critical #1 from code review).
//!
//! ## Testability
//!
//! The auth-retry logic is decomposed into two generic helpers
//! ([`acquire_with_auth_retry`] and [`read_with_auth_retry`]) that accept
//! futures/closures for the pool and read operations.  This allows the
//! production path to pass real SDK calls while tests inject controlled
//! failures without needing a live cluster or a mock gRPC server.
//!
//! The production `positioned_read_with_reauth` function uses
//! `acquire_with_auth_retry` directly (the acquire path has no lifetime
//! issues) and inlines the read-retry logic (because
//! `GrpcBlockReader::positioned_read` borrows `&WorkerClient`, which
//! cannot be moved into a `Box::pin` future that outlives the function).
//! Both paths are covered by the unit tests in the `#[cfg(test)]` module.

use std::sync::Arc;

use goosefs_sdk::client::WorkerClient;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::{ufs_block_length, InStreamOptions, URIStatus};
use goosefs_sdk::io::GrpcBlockReader;
use goosefs_sdk::proto::proto::dataserver::OpenUfsBlockOptions;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::PyResult;

use crate::errors::map_err;
use crate::filesystem::format_worker_addr;

/// Default chunk size for the high-level `positioned_read` helper and the
/// low-level `AsyncWorkerClient.read_block_positioned`.
///
/// Mirrors `goosefs.user.streaming.reader.chunk.size.bytes = 1 MiB` and the
/// Java SDK default for remote-worker `BlockWorker.read_block` RPCs.
/// Larger chunks reduce the number of `offset_received` ACK round-trips at
/// the cost of more in-flight gRPC buffer bytes per RPC.
///
/// Previously duplicated as `DEFAULT_POS_READ_CHUNK_SIZE` (in `filesystem.rs`)
/// and `DEFAULT_CHUNK_SIZE_BYTES` (in `worker.rs`) — now a single canonical
/// definition.
pub(crate) const DEFAULT_CHUNK_SIZE: i64 = 1 << 20;

/// Resolve the block id for a positioned-read from a `URIStatus`.
///
/// Prefers `file_block_infos` over `block_ids` so that files freshly
/// written through this binding can be positioned-read without waiting
/// for a worker block-report. Mirrors the Rust `stress` tool's
/// `pick_positioned_read_block_id` logic.
///
/// # Arguments
///
/// * `status` — `URIStatus` returned by `get_status(path)`.
/// * `block_index` — 0-based index of the block to read.
/// * `path` — GooseFS path (used in error messages only).
///
/// # Returns
///
/// `(block_id, actual_block_length)` on success.
///
/// `actual_block_length` is the number of bytes stored in that block (the
/// trailing / only block of a short file is often smaller than the file's
/// configured `block_size_bytes`). Callers that pass `length=-1` ("read to
/// end of block") must use this value — requesting the configured block size
/// makes [`GrpcBlockReader::positioned_read`] fail with a short-read error
/// when the worker half-closes after delivering the real data.
///
/// # Errors
///
/// * `ValueError` — the file has no blocks, or `block_index` is out of range.
pub(crate) fn resolve_block_id(
    status: &URIStatus,
    block_index: usize,
    path: &str,
) -> PyResult<(i64, i64)> {
    // Order block-info entries by their byte offset within the file so that
    // ``block_index`` keeps its 0-based "Nth block" semantics regardless of
    // HashMap iteration order.
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
        return Err(PyValueError::new_err(format!(
            "path {:?} has no blocks (empty file or directory)",
            path
        )));
    }
    if block_index >= block_ids.len() {
        return Err(PyValueError::new_err(format!(
            "block_index={} out of range (file {:?} has {} block(s))",
            block_index,
            path,
            block_ids.len()
        )));
    }
    let block_id = block_ids[block_index];
    Ok((block_id, actual_block_length(status, block_id, block_index)))
}

/// Bytes stored in `block_id` / `block_index` for this file.
///
/// Master `BlockInfo.length` is not always the stored size of a short last
/// block (`FileLocation.getLength()` can be the configured 64 MiB). Always
/// clamp to `min(file_length - block_offset, configured block_size)` so
/// `offset >= actual_block_length` fails fast as `ValueError` instead of
/// sending an OOB `positioned_read` to the worker (short-read `GoosefsError`).
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

/// `OpenUfsBlockOptions` for a positioned read, matching
/// [`goosefs_sdk::io::GoosefsFileInStream`].
///
/// Through-mode blocks live only in UFS. Passing `None` makes the worker
/// return `Internal` because it cannot open the UFS file.
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
        // The real length of *this* block, not the file's nominal block size —
        // see [`ufs_block_length`].
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

/// [`open_ufs_block_options`] for the block whose id is `block_id`.
///
/// `None` when the id is not in `status.block_ids`, or when the file has no
/// UFS path. Used by `acquire_worker_for_block(path=...)` so a subsequent
/// low-level `read_block_positioned` can send the same `OpenUfsBlockOptions`
/// the high-level path already does. PAGE workers refuse a read that lacks
/// `mount_id` (`PagedBlockReader` leaves `pagedUfsBlockReader` unset).
pub(crate) fn open_ufs_block_options_for_block_id(
    status: &URIStatus,
    block_id: i64,
) -> Option<OpenUfsBlockOptions> {
    let block_index = status.block_ids.iter().position(|&id| id == block_id)?;
    open_ufs_block_options(status, block_index)
}

/// Master `BlockInfo.locations` for `block_id`, or empty when unavailable.
///
/// Empty → [`WorkerRouter::select_worker_for_read`] falls back to consistent
/// hash (same as Rust `FileInStream` / Java `getInStream`).
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

// ── Generic auth-retry helpers (testable) ──────────────────────────────────

/// Acquire a client from the pool, retrying on SASL auth failure.
///
/// This is **recovery point 2** from the auth-retry design: `acquire()`
/// itself fails with `UNAUTHENTICATED` (no `WorkerClient` was produced).
/// The caller falls back to the unconditional `reconnect()` path.
///
/// Generic over the acquire/reconnect futures so that tests can inject
/// controlled failures without a live cluster.
///
/// # Arguments
///
/// * `acquire_fut` — attempts to acquire a pooled client; returns
///   `Err(AuthenticationFailed)` on SASL expiry.
/// * `reconnect_fut` — unconditionally reconnects (single-flight for
///   concurrent callers); returns a fresh client.
///
/// # Returns
///
/// The acquired or reconnected `WorkerClient` on success.
pub(crate) async fn acquire_with_auth_retry<C, F1, F2>(
    acquire_fut: F1,
    reconnect_fut: F2,
) -> PyResult<C>
where
    F1: std::future::Future<Output = goosefs_sdk::error::Result<C>>,
    F2: std::future::Future<Output = goosefs_sdk::error::Result<C>>,
{
    match acquire_fut.await {
        Ok(c) => Ok(c),
        Err(e) if e.is_authentication_failed() => reconnect_fut.await.map_err(map_err),
        Err(e) => Err(map_err(e)),
    }
}

/// Perform a positioned read, retrying on SASL auth failure.
///
/// This is **recovery point 1** from the auth-retry design: an RPC on a
/// cached `WorkerClient` fails with `UNAUTHENTICATED` (SASL stream
/// expired).  The reader calls `reconnect_if_stale(addr, stale_generation)`
/// for a single-flight reconnect, then retries the RPC on the fresh client.
///
/// Generic over the read/reconnect/retry closures so that tests can inject
/// controlled failures without a live cluster.
///
/// # Why a `FnOnce` for `retry_read_fn`?
///
/// `GrpcBlockReader::positioned_read(&worker, ...)` borrows `&WorkerClient`,
/// which cannot be moved into a `Box::pin` future that outlives the owning
/// function.  The `FnOnce(WorkerClient) -> Pin<Box<dyn Future>>` pattern
/// solves this by giving the retry closure ownership of the fresh client,
/// allowing it to borrow `&fresh_client` inside the pinned future.
///
/// In the production path (`positioned_read_with_reauth`) this is handled
/// by inlining the retry logic instead (see the comment there).
///
/// # Arguments
///
/// * `read_fut` — attempts the positioned read; returns
///   `Err(AuthenticationFailed)` on SASL expiry.
/// * `reconnect_fut` — reconnects if the generation is stale; returns a
///   fresh `WorkerClient`.
/// * `retry_read_fn` — given the fresh client, produces a future that
///   retries the positioned read.
///
/// # Returns
///
/// The read data on success.
// Only referenced from `#[cfg(test)]` unit tests below, so the non-test
// build sees it as unused. The lint does not cross `cfg(test)` boundaries.
#[allow(dead_code)]
pub(crate) async fn read_with_auth_retry<T, F1, F2, F3>(
    read_fut: F1,
    reconnect_fut: F2,
    retry_read_fn: F3,
) -> PyResult<T>
where
    F1: std::future::Future<Output = goosefs_sdk::error::Result<T>>,
    F2: std::future::Future<Output = goosefs_sdk::error::Result<WorkerClient>>,
    F3: FnOnce(
        WorkerClient,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = goosefs_sdk::error::Result<T>> + Send>,
    >,
{
    match read_fut.await {
        Ok(v) => Ok(v),
        Err(e) if e.is_authentication_failed() => {
            let fresh = reconnect_fut.await.map_err(map_err)?;
            retry_read_fn(fresh).await.map_err(map_err)
        }
        Err(e) => Err(map_err(e)),
    }
}

/// Perform a positioned read with SASL auth-failure retry.
///
/// This function encapsulates the full read pipeline:
///
/// 1. Route to the responsible worker via `WorkerRouter`.
/// 2. Acquire a pooled `WorkerClient` — retry on SASL auth failure
///    (delegates to [`acquire_with_auth_retry`]).
/// 3. Call `GrpcBlockReader::positioned_read` — retry on SASL auth failure
///    (inlined — see note below).
///
/// Both the async (`AsyncGoosefs`) and sync (`Goosefs`) Python bindings
/// call this shared implementation to avoid logic drift.
///
/// # Auth-failure retry rationale
///
/// A long-lived cached channel can have its SASL stream silently expire
/// on the worker side; the very next `acquire` (which only checks the
/// local cache) returns the stale client and the subsequent
/// `positioned_read` will fail with `Unauthenticated`. Mirror the SDK
/// reader-path policy (`file_reader.rs` / `file_in_stream.rs`): on
/// `is_authentication_failed`, request a single-flight reconnect and
/// retry **once** before giving up.
///
/// This was the root cause of T6 PR-4k Python `fail=1,109,311` — the
/// binding short-circuited the SDK reader-path that has this protection
/// built in.
///
/// # Why inline the read-retry instead of using `read_with_auth_retry`?
///
/// `GrpcBlockReader::positioned_read(&worker, ...)` borrows `&WorkerClient`.
/// To pass the fresh client (from `reconnect_if_stale`) into the retry
/// read, we would need to either:
/// (a) Move the fresh client into a `Box::pin` future that borrows it —
///     this creates a self-referential future, which Rust forbids.
/// (b) Use `read_with_auth_retry` with a `FnOnce(WorkerClient)` that
///     owns the fresh client — this works for tests but adds overhead
///     (dynamic dispatch + allocation) in the production hot path.
///
/// The inline version avoids both issues by simply declaring `fresh` in
/// the same scope as the retry `positioned_read` call, which is the
/// idiomatic Rust pattern for this situation.
pub(crate) async fn positioned_read_with_reauth(
    ctx: Arc<FileSystemContext>,
    status: &URIStatus,
    block_id: i64,
    block_index: usize,
    offset: i64,
    effective_length: i64,
    chunk_size: i64,
) -> PyResult<Vec<u8>> {
    let locations = block_locations_from_status(status, block_id);
    let ufs_opts = open_ufs_block_options(status, block_index);
    // 1. Route to the responsible worker.
    //
    // Prefer Master BlockInfo.locations (Java getInStream / Rust
    // `select_worker_for_read`); empty/unmatched → consistent-hash fallback.
    //
    // NOTE on auth-retry routing strategy: the auth-failure retry below
    // (steps 2 + 3) intentionally does **not** re-route to a different
    // worker.  The failure mode being recovered from (SASL stream
    // expiry on a long-lived cached channel) is a *channel-level*
    // problem on the same worker, not a worker-availability problem —
    // calling `reconnect_if_stale(worker_addr, ...)` rebuilds a fresh
    // TCP+SASL handshake against the **same** address, which is exactly
    // what the SDK reader-path policy does (`file_reader.rs` /
    // `file_in_stream.rs`) and what the server is prepared for.
    //
    // Re-running `select_worker_for_read(block_id, …)` between the
    // failure and the retry would risk landing on a worker that does not
    // host the block, and would not fix any SASL-level failure.
    let replication = ctx.config().file_replication_number;
    let max_retry_node = ctx.config().file_read_max_node_retry;
    let worker_info = ctx
        .acquire_router()
        .select_worker_for_read(block_id, &locations, replication, max_retry_node)
        .await
        .map_err(map_err)?;
    let net_addr = worker_info
        .address
        .as_ref()
        .ok_or_else(|| PyRuntimeError::new_err("selected worker has no address"))?;
    let worker_addr = format_worker_addr(net_addr);

    // 2. Acquire pooled WorkerClient — auth-failure retry.
    //
    // A long-lived cached channel can have its SASL stream silently expire
    // on the worker side; the very next `acquire` (which only checks the
    // local cache) returns the stale client. On `is_authentication_failed`,
    // request a single reconnect and retry once.
    let pool = ctx.acquire_worker_pool();
    let client =
        acquire_with_auth_retry(pool.acquire(&worker_addr), pool.reconnect(&worker_addr)).await?;
    let stale_generation = client.generation();

    // 3. Positioned read — auth-failure retry (inlined).
    //
    // On `is_authentication_failed`, request a single-flight
    // `reconnect_if_stale` (concurrent callers observing the same stale
    // generation share one TCP+SASL handshake) and retry once.
    //
    // NOTE: This retry logic is equivalent to `read_with_auth_retry` but
    // inlined to avoid the `Box::pin` + `FnOnce` overhead and the
    // self-referential-future problem (see method docstring above).
    // Unit tests cover this exact logic via `read_with_auth_retry`.
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
                .await
                .map_err(map_err)?;
            GrpcBlockReader::positioned_read(
                &fresh,
                block_id,
                offset,
                effective_length,
                chunk_size,
                ufs_opts,
            )
            .await
            .map_err(map_err)?
        }
        Err(e) => return Err(map_err(e)),
    };
    Ok(bytes.to_vec())
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use goosefs_sdk::client::WorkerClient;
    use goosefs_sdk::error::Error;
    use goosefs_sdk::proto::grpc::file::{FileBlockInfo, FileInfo};
    use goosefs_sdk::proto::grpc::BlockInfo;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    fn status_with_blocks(
        length: i64,
        block_size_bytes: i64,
        blocks: &[(i64, i64, i64)], // (block_id, offset, block_length)
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

    /// **Regression**: `OpenUfsBlockOptions.block_size` is the real length of
    /// the block, not the file's nominal block size. The Worker asks the UFS for
    /// exactly this many bytes when back-filling the page cache, so an
    /// over-reported partial tail block is never cached:
    ///
    /// ```text
    /// ERROR LocalCacheManager - Failed to read page
    ///   BlockPageId{FileId=paged_block_503316480_size_1048576, PageIndex=0}:
    ///   supposed to read 1048576 bytes, 13 bytes actually read
    /// ```
    ///
    /// Java sends `Math.min(length - blockSize * seq, blockSize)`.
    #[test]
    fn open_ufs_block_options_carry_actual_tail_block_length() {
        let bs = 1 << 20i64;
        let status = status_with_ufs_path(2 * bs + 100, bs, 3);

        let seen: Vec<(i64, i64)> = (0..3)
            .map(|idx| {
                let opts = open_ufs_block_options(&status, idx)
                    .expect("a file with a ufs_path must produce OpenUfsBlockOptions");
                (opts.offset_in_file.unwrap(), opts.block_size.unwrap())
            })
            .collect();

        assert_eq!(
            seen,
            vec![(0, bs), (bs, bs), (2 * bs, 100)],
            "the tail block must advertise 100 bytes, not the nominal {bs}"
        );
    }

    /// A file smaller than one block: block 0 is the tail. Shape of the Lance
    /// manifest / version-hint files that surfaced the bug.
    #[test]
    fn open_ufs_block_options_sub_block_file_reports_file_length() {
        let status = status_with_ufs_path(13, 1 << 20, 1);
        let opts = open_ufs_block_options(&status, 0)
            .expect("a file with a ufs_path must produce OpenUfsBlockOptions");
        assert_eq!(opts.block_size, Some(13));
        assert_eq!(opts.offset_in_file, Some(0));
    }

    #[test]
    fn resolve_block_id_uses_actual_length_for_short_file() {
        // 1 MiB file with 64 MiB configured block size — CI positioned_read
        // example regression: length=-1 must not request 64 MiB.
        let status = status_with_blocks(1 << 20, 64 << 20, &[(1879048192, 0, 1 << 20)]);
        let (id, len) = resolve_block_id(&status, 0, "/blob.bin").unwrap();
        assert_eq!(id, 1879048192);
        assert_eq!(len, 1 << 20);
    }

    #[test]
    fn resolve_block_id_clamps_via_file_length_without_block_info_length() {
        let status = URIStatus::from_proto(FileInfo {
            length: Some(1 << 20),
            block_size_bytes: Some(64 << 20),
            block_ids: vec![42],
            file_block_infos: vec![],
            completed: Some(true),
            ..Default::default()
        });
        let (id, len) = resolve_block_id(&status, 0, "/blob.bin").unwrap();
        assert_eq!(id, 42);
        assert_eq!(len, 1 << 20);
    }

    #[test]
    fn resolve_block_id_keeps_full_middle_block_length() {
        let status = status_with_blocks(
            (64 << 20) + 100,
            64 << 20,
            &[(1, 0, 64 << 20), (2, 64 << 20, 100)],
        );
        let (_, len0) = resolve_block_id(&status, 0, "/big.bin").unwrap();
        let (_, len1) = resolve_block_id(&status, 1, "/big.bin").unwrap();
        assert_eq!(len0, 64 << 20);
        assert_eq!(len1, 100);
    }

    #[test]
    fn resolve_block_id_clamps_inflated_master_block_length() {
        // FileLocation.getLength() / default block size on a 100-byte file.
        let status = status_with_blocks(100, 64 << 20, &[(1, 0, 64 << 20)]);
        let (_, len) = resolve_block_id(&status, 0, "/blob.bin").unwrap();
        assert_eq!(len, 100);
    }

    #[test]
    fn open_ufs_block_options_none_when_ufs_path_empty() {
        let status = status_with_blocks(100, 64 << 20, &[(1, 0, 64 << 20)]);
        assert!(open_ufs_block_options(&status, 0).is_none());
    }

    #[test]
    fn open_ufs_block_options_sets_geometry_from_status() {
        let mut status = status_with_blocks(
            (64 << 20) + 100,
            64 << 20,
            &[(1, 0, 64 << 20), (2, 64 << 20, 100)],
        );
        status.ufs_path = "cosn://bucket/file.bin".into();
        status.mount_id = 7;
        status.cacheable = false;

        let opts = open_ufs_block_options(&status, 1).expect("ufs path present");
        assert_eq!(opts.ufs_path.as_deref(), Some("cosn://bucket/file.bin"));
        assert_eq!(opts.offset_in_file, Some(64 << 20));
        // Block 1 is the 100-byte tail, as the fixture's own `FileBlockInfo`
        // says. This used to assert the nominal 64 MiB, which made the Worker
        // read past the end of the object while back-filling the page cache.
        assert_eq!(opts.block_size, Some(100));
        assert_eq!(opts.mount_id, Some(7));
        assert_eq!(opts.no_cache, Some(true));
        assert_eq!(opts.file_length, Some((64 << 20) + 100));
        assert_eq!(opts.max_ufs_read_concurrency, Some(8));
    }

    #[test]
    fn open_ufs_block_options_for_block_id_uses_that_block_index() {
        let mut status = status_with_blocks(
            (64 << 20) + 100,
            64 << 20,
            &[(1, 0, 64 << 20), (2, 64 << 20, 100)],
        );
        status.ufs_path = "cosn://bucket/file.bin".into();
        status.mount_id = 7;

        let opts = open_ufs_block_options_for_block_id(&status, 2).expect("block 2 present");
        assert_eq!(opts.offset_in_file, Some(64 << 20));
        assert_eq!(opts.block_size, Some(100));
        assert!(open_ufs_block_options_for_block_id(&status, 99).is_none());
    }

    // ── Helper: fabricate a WorkerClient from a never-connected channel ────
    //
    // Mirrors `fake_client` in `src/client/worker.rs`.  The client is usable
    // for anything that only touches the in-memory struct (addr/generation
    // lookups, clone, drop).  Any actual RPC would fail, but the tests below
    // never issue one.

    fn fake_client(addr: &str) -> WorkerClient {
        use tonic::transport::Channel;
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        WorkerClient::from_channel(channel, addr.to_string())
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Recovery point 2: acquire() → auth failure → reconnect() → success
    // ═══════════════════════════════════════════════════════════════════════

    /// Scenario: `acquire()` fails with `AuthenticationFailed`, then
    /// `reconnect()` succeeds with a fresh client.
    ///
    /// Verifies:
    /// - `reconnect_fn` is called exactly once.
    /// - The returned client is the one from `reconnect_fn`.
    /// - No error is propagated.
    #[tokio::test]
    async fn acquire_retry_calls_reconnect_on_auth_failure() {
        let reconnect_count = Arc::new(AtomicUsize::new(0));
        let fresh_client = fake_client("worker:9203");
        let expected_addr = fresh_client.addr().to_string();

        let result = acquire_with_auth_retry(
            async {
                Err(Error::AuthenticationFailed {
                    message: "SASL token expired".into(),
                })
            },
            {
                let count = reconnect_count.clone();
                let client = fresh_client.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(client)
                }
            },
        )
        .await;

        let client = result.expect("acquire_with_auth_retry must succeed after reconnect");
        assert_eq!(reconnect_count.load(Ordering::SeqCst), 1);
        assert_eq!(client.addr(), expected_addr);
    }

    /// Scenario: `acquire()` fails with a non-auth error (e.g. `Unavailable`).
    ///
    /// Verifies:
    /// - `reconnect_fn` is NOT called.
    /// - The error is propagated as a Python exception.
    #[tokio::test]
    async fn acquire_retry_propagates_non_auth_errors() {
        let reconnect_count = Arc::new(AtomicUsize::new(0));

        let result: PyResult<WorkerClient> = acquire_with_auth_retry(
            async {
                Err(Error::GrpcError {
                    message: "worker unavailable".into(),
                    source: Box::new(tonic::Status::unavailable("connection refused")),
                })
            },
            {
                let count = reconnect_count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(fake_client("never-called:9203"))
                }
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(reconnect_count.load(Ordering::SeqCst), 0);
    }

    /// Scenario: `acquire()` succeeds on the first try.
    ///
    /// Verifies:
    /// - `reconnect_fn` is NOT called.
    /// - The original client is returned.
    #[tokio::test]
    async fn acquire_retry_skips_reconnect_on_success() {
        let reconnect_count = Arc::new(AtomicUsize::new(0));
        let original_client = fake_client("worker:9203");
        let expected_addr = original_client.addr().to_string();

        let result = acquire_with_auth_retry(async { Ok(original_client.clone()) }, {
            let count = reconnect_count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(fake_client("never-called:9203"))
            }
        })
        .await;

        let client = result.expect("acquire must succeed on first try");
        assert_eq!(reconnect_count.load(Ordering::SeqCst), 0);
        assert_eq!(client.addr(), expected_addr);
    }

    /// Scenario: `acquire()` fails with auth error, then `reconnect()` also
    /// fails with a non-auth error.
    ///
    /// Verifies: the reconnect error is propagated.
    #[tokio::test]
    async fn acquire_retry_propagates_reconnect_failure() {
        let result: PyResult<WorkerClient> = acquire_with_auth_retry(
            async {
                Err(Error::AuthenticationFailed {
                    message: "SASL expired".into(),
                })
            },
            async {
                Err(Error::GrpcError {
                    message: "reconnect failed".into(),
                    source: Box::new(tonic::Status::unavailable("connection refused")),
                })
            },
        )
        .await;

        assert!(result.is_err());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Recovery point 1: positioned_read() → auth failure →
    //                   reconnect_if_stale() → retry read → success
    // ═══════════════════════════════════════════════════════════════════════

    /// Scenario: `positioned_read()` fails with `AuthenticationFailed`, then
    /// `reconnect_if_stale()` returns a fresh client, and the retry read
    /// succeeds with correct data.
    ///
    /// Verifies:
    /// - `reconnect_fn` is called exactly once.
    /// - `retry_read_fn` is called exactly once with the fresh client.
    /// - The final result contains the data from the retry read.
    #[tokio::test]
    async fn read_retry_calls_reconnect_on_auth_failure() {
        let reconnect_count = Arc::new(AtomicUsize::new(0));
        let retry_read_count = Arc::new(AtomicUsize::new(0));
        let expected_data = vec![0xDE, 0xAD, 0xBE, 0xEF];

        let result: PyResult<bytes::Bytes> = read_with_auth_retry(
            async {
                Err(Error::AuthenticationFailed {
                    message: "SASL token expired on read".into(),
                })
            },
            {
                let count = reconnect_count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(fake_client("worker:9203"))
                }
            },
            {
                let count = retry_read_count.clone();
                let data = expected_data.clone();
                move |fresh_client: WorkerClient| {
                    count.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(
                        fresh_client.addr(),
                        "worker:9203",
                        "retry must receive the fresh client from reconnect"
                    );
                    Box::pin(async move { Ok(bytes::Bytes::from(data)) })
                }
            },
        )
        .await;

        let data = result.expect("read_with_auth_retry must succeed after reconnect");
        assert_eq!(reconnect_count.load(Ordering::SeqCst), 1);
        assert_eq!(retry_read_count.load(Ordering::SeqCst), 1);
        assert_eq!(&data[..], &expected_data[..]);
    }

    /// Scenario: `positioned_read()` succeeds on the first try.
    ///
    /// Verifies:
    /// - `reconnect_fn` and `retry_read_fn` are NOT called.
    /// - The original data is returned.
    #[tokio::test]
    async fn read_retry_skips_reconnect_on_success() {
        let reconnect_count = Arc::new(AtomicUsize::new(0));
        let retry_read_count = Arc::new(AtomicUsize::new(0));
        let original_data = vec![0xCA, 0xFE];

        let result: PyResult<bytes::Bytes> = read_with_auth_retry(
            {
                let data = original_data.clone();
                async move { Ok(bytes::Bytes::from(data)) }
            },
            {
                let count = reconnect_count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(fake_client("never-called:9203"))
                }
            },
            {
                let count = retry_read_count.clone();
                move |_fresh_client| {
                    count.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(bytes::Bytes::from_static(b"never")) })
                }
            },
        )
        .await;

        let data = result.expect("read must succeed on first try");
        assert_eq!(reconnect_count.load(Ordering::SeqCst), 0);
        assert_eq!(retry_read_count.load(Ordering::SeqCst), 0);
        assert_eq!(&data[..], &original_data[..]);
    }

    /// Scenario: `positioned_read()` fails with a non-auth error.
    ///
    /// Verifies:
    /// - `reconnect_fn` and `retry_read_fn` are NOT called.
    /// - The error is propagated.
    #[tokio::test]
    async fn read_retry_propagates_non_auth_errors() {
        let reconnect_count = Arc::new(AtomicUsize::new(0));
        let retry_read_count = Arc::new(AtomicUsize::new(0));

        let result: PyResult<bytes::Bytes> = read_with_auth_retry(
            async {
                Err(Error::NotFound {
                    path: "/missing-block".into(),
                })
            },
            {
                let count = reconnect_count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(fake_client("never-called:9203"))
                }
            },
            {
                let count = retry_read_count.clone();
                move |_fresh_client| {
                    count.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(bytes::Bytes::new()) })
                }
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(reconnect_count.load(Ordering::SeqCst), 0);
        assert_eq!(retry_read_count.load(Ordering::SeqCst), 0);
    }

    /// Scenario: read fails with auth error, reconnect succeeds, but retry
    /// read fails with a non-auth error.
    ///
    /// Verifies: the retry read error is propagated.
    #[tokio::test]
    async fn read_retry_propagates_retry_read_failure() {
        let result: PyResult<bytes::Bytes> = read_with_auth_retry(
            async {
                Err(Error::AuthenticationFailed {
                    message: "SASL expired".into(),
                })
            },
            async { Ok(fake_client("worker:9203")) },
            move |_fresh_client| {
                Box::pin(async {
                    Err(Error::BlockIoError {
                        message: "block not found after reconnect".into(),
                    })
                })
            },
        )
        .await;

        assert!(result.is_err());
    }

    /// Scenario: read fails with auth error, and `reconnect_if_stale()`
    /// itself fails.
    ///
    /// Verifies: the reconnect error is propagated.
    #[tokio::test]
    async fn read_retry_propagates_reconnect_failure() {
        let result: PyResult<bytes::Bytes> = read_with_auth_retry(
            async {
                Err(Error::AuthenticationFailed {
                    message: "SASL expired".into(),
                })
            },
            async {
                Err(Error::GrpcError {
                    message: "worker down during reconnect".into(),
                    source: Box::new(tonic::Status::unavailable("connection refused")),
                })
            },
            move |_fresh_client| Box::pin(async { Ok(bytes::Bytes::new()) }),
        )
        .await;

        assert!(result.is_err());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Error classification tests
    // ═══════════════════════════════════════════════════════════════════════
    //
    // These verify that `is_authentication_failed()` is the gateway for
    // entering the retry path.  If this classification breaks, the retry
    // helpers will silently skip the reconnect — a regression that would
    // resurrect the T6 PR-4k failure.

    /// Verify that `AuthenticationFailed` triggers the retry path.
    #[test]
    fn auth_error_triggers_retry_path() {
        let auth_err = Error::AuthenticationFailed {
            message: "test".into(),
        };
        assert!(auth_err.is_authentication_failed());
    }

    /// Verify that `GrpcError(Unavailable)` does NOT trigger the retry path.
    #[test]
    fn unavailable_error_does_not_trigger_retry_path() {
        let err = Error::GrpcError {
            message: "worker down".into(),
            source: Box::new(tonic::Status::unavailable("connection refused")),
        };
        assert!(!err.is_authentication_failed());
    }

    /// Verify that `BlockIoError` does NOT trigger the retry path.
    #[test]
    fn block_io_error_does_not_trigger_retry_path() {
        let err = Error::BlockIoError {
            message: "read failed".into(),
        };
        assert!(!err.is_authentication_failed());
    }

    /// Verify that gRPC `UNAUTHENTICATED` maps to `AuthenticationFailed`.
    #[test]
    fn grpc_unauthenticated_maps_to_authentication_failed() {
        let status = tonic::Status::unauthenticated("SASL stream expired");
        let err = Error::from(status);
        assert!(err.is_authentication_failed());
    }
}
