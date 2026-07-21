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
use goosefs_sdk::fs::URIStatus;
use goosefs_sdk::io::GrpcBlockReader;
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
/// `(block_id, block_size_bytes)` on success.
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
    Ok((block_ids[block_index], status.block_size_bytes))
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
    block_id: i64,
    offset: i64,
    effective_length: i64,
    chunk_size: i64,
) -> PyResult<Vec<u8>> {
    // 1. Route to the responsible worker.
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
    // Re-running `select_worker(block_id)` between the failure and the
    // retry would risk landing on a worker that does not host the block
    // (block_id → worker mapping is consistent-hashed, so the same call
    // would also tend to return the same worker), and would not fix any
    // SASL-level failure.  Worker-availability problems (e.g. a worker
    // marked failed by a concurrent task) are handled by the SDK
    // reader-path's *separate* worker-failover branch in
    // `file_reader.rs`, which `positioned_read` does not need to
    // duplicate because the binding-layer pool already provides
    // single-flight reconnect.
    let worker_info = ctx
        .acquire_router()
        .select_worker(block_id)
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
        /* ufs_opts */ None,
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
                /* ufs_opts */ None,
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

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
