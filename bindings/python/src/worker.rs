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

//! Low-level Worker block client for Python — `AsyncWorkerClient`.
//!
//! Wraps [`goosefs_sdk::client::WorkerClient`] (the Worker:9203 gRPC
//! `BlockWorker` client) so Python users can perform **direct Worker block
//! reads** without going through the high-level `FileSystem` (`fs.read_at`)
//! façade.
//!
//! ## Why a dedicated low-level class?
//!
//! Python users running random-access / pos-read benchmarks need the same
//! transport choice that `examples/lowlevel_block_read.rs` exposes for the
//! Rust SDK: open a stream straight against a Worker, skip prefetch
//! (`position_short = true`), receive exactly the requested byte range.
//! Until now the Python `goosefs-stress` tool's `--transport block` mode
//! degraded to `fs` because no PyO3 binding existed for `WorkerClient`.
//!
//! ## Wrapping strategy — minimal surface
//!
//! We expose a **single one-shot positioned-read coroutine** rather than
//! the raw `(request_tx, response_stream)` pair returned by
//! [`goosefs_sdk::client::WorkerClient::read_block_positioned`].
//!
//! - Streaming over the PyO3 boundary requires wrapping the response
//!   stream as a Python `__aiter__`, which is non-trivial and re-introduces
//!   exactly the per-chunk PyO3-edge cost we are trying to avoid in the
//!   first place (see `docs/GooseFS_Python_SDK_PROBLEMS_AND_SOLUTIONS.md` ).
//! - The SDK already provides a high-level wrapper —
//!   [`goosefs_sdk::io::GrpcBlockReader::positioned_read`] — which opens
//!   the stream, drains all chunks with proper `offset_received` ACK
//!   flow-control, and returns a single `Bytes`. We delegate to it and
//!   take exactly **one** `PyBytes::new` copy on the way out.
//!
//! Future versions may add a `read_block` non-positioned path or a
//! `WriteBlockHandle` wrapper if/when concrete Python use cases appear.
//!
//! ## Concurrency model — Review  (mirrors `streaming.rs`)
//!
//! [`goosefs_sdk::client::WorkerClient`] itself is `Clone` (it shares an
//! authenticated `tonic::Channel`), so multiple in-flight RPCs on the same
//! `WorkerClient` are safe. We still wrap the inner client in
//! `Arc<AsyncMutex<Option<…>>>` to:
//!
//! 1. Provide an idempotent `close()` that invalidates the handle (`Option::take`)
//!    so subsequent calls fail fast with `RuntimeError("AsyncWorkerClient is closed")`.
//! 2. Allow lock-free fast paths for `addr()` via `try_lock()`.
//!
//! Holding the `AsyncMutex` across `read_block_positioned`'s entire
//! `await` would needlessly serialise concurrent positioned reads.
//! We therefore `lock()` only long enough to `clone()` the inner
//! `WorkerClient` (cheap — just bumps an `Arc` refcount on the SASL
//! channel), then drop the guard before issuing the SDK call. This
//! matches the semantics of [`goosefs_sdk::client::WorkerClientPool::acquire`]
//! which hands callers a fresh `Clone` per acquire.
//!
//! ## GIL discipline
//!
//! All async methods funnel through `pyo3_async_runtimes::tokio::future_into_py`,
//! which releases the GIL for the entire duration of the future. The single
//! `PyBytes::new` call at the end re-acquires the GIL via `Python::attach`
//! for the minimum window needed to allocate the Python `bytes` object.

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex as AsyncMutex;

use goosefs_sdk::client::WorkerClient;
use goosefs_sdk::io::GrpcBlockReader;

use crate::config::PyConfig;
use crate::errors::map_err;
use crate::positioned_read::DEFAULT_CHUNK_SIZE;
use crate::runtime::block_on;

// ─────────────────────────────────────────────────────────────────────────────
// AsyncWorkerClient
// ─────────────────────────────────────────────────────────────────────────────

/// Coroutine-returning low-level Worker block client.
///
/// Wraps [`goosefs_sdk::client::WorkerClient`]. Acquired via the static
/// factory [`AsyncWorkerClient::connect`]; each instance owns one
/// authenticated gRPC channel to one Worker.
///
/// ```python
/// from goosefs import AsyncWorkerClient, Config
///
/// async def pos_read(addr: str, block_id: int, offset: int, length: int) -> bytes:
///     cfg = Config("127.0.0.1:9200")
///     async with await AsyncWorkerClient.connect(addr, cfg) as wc:
///         return await wc.read_block_positioned(block_id, offset, length)
/// ```
///
/// Most users will prefer the higher-level
/// `AsyncGoosefs.positioned_read(path, ...)` (added in stage B) which
/// transparently selects the right Worker via `WorkerRouter` and reuses
/// the shared `WorkerClientPool`. `AsyncWorkerClient` is the escape hatch
/// for callers that already know the Worker address (load-balancing
/// experiments, benchmarks, custom routing).
#[pyclass(module = "goosefs._goosefs", name = "AsyncWorkerClient")]
pub struct PyAsyncWorkerClient {
    inner: Arc<AsyncMutex<Option<WorkerClient>>>,
    addr: String,
}

#[pymethods]
impl PyAsyncWorkerClient {
    /// `await AsyncWorkerClient.connect(addr, config)` → `AsyncWorkerClient`.
    ///
    /// Establishes a TCP+SASL handshake to the Worker at `addr`.
    /// `addr` is `"host:port"` (e.g. `"127.0.0.1:9203"`); `config` is the
    /// same `Config` you would pass to `AsyncGoosefs.connect`.
    ///
    /// Each successful call holds an exclusive gRPC channel — **not**
    /// pooled with other `AsyncWorkerClient` instances. If you need
    /// connection reuse across blocks/threads, prefer
    /// `AsyncGoosefs.acquire_worker_for_block` (stage B).
    #[staticmethod]
    fn connect<'py>(
        py: Python<'py>,
        addr: String,
        config: PyConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        if addr.is_empty() {
            return Err(PyValueError::new_err(
                "AsyncWorkerClient.connect: addr must not be empty",
            ));
        }
        let sdk_cfg = config.inner.clone();
        let addr_owned = addr.clone();
        future_into_py(py, async move {
            let client = WorkerClient::connect(&addr_owned, &sdk_cfg)
                .await
                .map_err(map_err)?;
            let wrapper = PyAsyncWorkerClient {
                inner: Arc::new(AsyncMutex::new(Some(client))),
                addr: addr_owned,
            };
            Python::attach(|py| Ok(Py::new(py, wrapper)?.into_any()))
        })
    }

    /// `await AsyncWorkerClient.connect_simple(addr, connect_timeout_ms=10_000)` → `AsyncWorkerClient`.
    ///
    /// **Deprecated escape hatch** — connects without SASL authentication.
    /// Useful only for talking to test workers configured with
    /// `auth_type=NOSASL`. Production callers should always go through
    /// [`connect`](Self::connect) which uses the same `Config` object as
    /// `AsyncGoosefs.connect`.
    #[staticmethod]
    #[pyo3(signature = (addr, connect_timeout_ms = 10_000))]
    fn connect_simple<'py>(
        py: Python<'py>,
        addr: String,
        connect_timeout_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        if addr.is_empty() {
            return Err(PyValueError::new_err(
                "AsyncWorkerClient.connect_simple: addr must not be empty",
            ));
        }
        // Emit DeprecationWarning — `connect_simple` bypasses SASL auth and
        // should only be used for test workers. Production code must use
        // `connect()` with a proper `Config`.
        let warnings = py.import("warnings")?;
        let deprecation_warning = py.import("builtins")?.getattr("DeprecationWarning")?;
        warnings.call_method1(
            "warn",
            (
                "connect_simple is deprecated — use connect() with proper Config for production use",
                deprecation_warning,
                2, // stacklevel
            ),
        )?;
        let timeout = Duration::from_millis(connect_timeout_ms);
        future_into_py(py, async move {
            let client = WorkerClient::connect_simple(&addr, timeout)
                .await
                .map_err(map_err)?;
            let wrapper = PyAsyncWorkerClient {
                inner: Arc::new(AsyncMutex::new(Some(client))),
                addr,
            };
            Python::attach(|py| Ok(Py::new(py, wrapper)?.into_any()))
        })
    }

    /// `await wc.read_block_positioned(block_id, offset, length, chunk_size=1<<20)` → `bytes`.
    ///
    /// One-shot positioned read against a **single block**. Sends
    /// `position_short = true`, so the Worker:
    /// 1. Skips prefetch / cache promotion.
    /// 2. Closes the response stream after delivering exactly `length` bytes.
    ///
    /// The SDK drains all `ReadResponse` chunks with proper
    /// `offset_received` ACK flow-control and concatenates them into a
    /// single `Bytes` ([`GrpcBlockReader::positioned_read`]); we then
    /// take **one** copy across the PyO3 boundary into a Python `bytes`.
    ///
    /// Arguments:
    ///   block_id   — the block to read from (use `URIStatus.block_ids`
    ///                to enumerate blocks of a file).
    ///   offset     — byte offset within the block (`>= 0`).
    ///   length     — number of bytes to read (`>= 0`; `0` returns `b""`).
    ///   chunk_size — preferred gRPC chunk size in bytes
    ///                (default `1 MiB`, matching
    ///                `goosefs.user.streaming.reader.chunk.size.bytes`).
    ///                Smaller values give more granular flow control;
    ///                larger values reduce ACK round-trips.
    ///
    /// Returns the requested byte range (may be shorter than `length`
    /// only at the end of the block).
    ///
    /// Raises:
    ///   ValueError       — `offset < 0` / `length < 0` / `chunk_size <= 0`.
    ///   RuntimeError     — handle was already `close()`d.
    ///   IoError          — block I/O failure.
    ///   RpcError         — gRPC transport / protocol failure.
    #[pyo3(signature = (block_id, offset, length, chunk_size = DEFAULT_CHUNK_SIZE))]
    fn read_block_positioned<'py>(
        &self,
        py: Python<'py>,
        block_id: i64,
        offset: i64,
        length: i64,
        chunk_size: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Argument validation up front — surface clear `ValueError`s rather
        // than the SDK's lower-level `BlockIoError` from a malformed RPC.
        if offset < 0 {
            return Err(PyValueError::new_err("offset must be non-negative"));
        }
        if length < 0 {
            return Err(PyValueError::new_err("length must be non-negative"));
        }
        if chunk_size <= 0 {
            return Err(PyValueError::new_err("chunk_size must be positive"));
        }
        // Fast path for zero-length reads: avoid an RPC entirely. Matches
        // the semantics of `Bytes::new()` from `read_all`.
        if length == 0 {
            return future_into_py(py, async move {
                Python::attach(|py| Ok(PyBytes::new(py, &[]).unbind()))
            });
        }

        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            // Take the lock only long enough to clone out the inner client.
            // `WorkerClient: Clone` is cheap (Arc-refcount on the SASL
            // channel); cloning lets concurrent positioned reads on the
            // same `AsyncWorkerClient` proceed in parallel.
            let worker = {
                let guard = inner.lock().await;
                guard
                    .as_ref()
                    .ok_or_else(|| PyRuntimeError::new_err("AsyncWorkerClient is closed"))?
                    .clone()
            };

            let bytes = GrpcBlockReader::positioned_read(
                &worker, block_id, offset, length, chunk_size, /* ufs_opts */ None,
            )
            .await
            .map_err(map_err)?;

            // Single copy across the PyO3 boundary — see Bytes-handling
            // discussion in `streaming.rs` module docstring.
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    /// `wc.addr` — the `host:port` address this client is connected to.
    ///
    /// Synchronous; never blocks.
    #[getter]
    fn addr(&self) -> &str {
        &self.addr
    }

    /// `await wc.close()` — release the underlying gRPC channel.
    ///
    /// Idempotent. Subsequent calls to `read_block_positioned` raise
    /// `RuntimeError("AsyncWorkerClient is closed")`.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            // Take and drop the inner client. The SDK's `WorkerClient`
            // holds an `Arc<Option<SaslStreamGuard>>` which will tear
            // down the SASL stream once the last clone is dropped; the
            // tonic channel is similarly Arc-counted internally.
            let mut guard = inner.lock().await;
            let _ = guard.take();
            Ok(())
        })
    }

    /// `async with await AsyncWorkerClient.connect(...) as wc: ...`
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let py_obj = slf.into_pyobject(py)?.into_any().unbind();
        future_into_py(py, async move { Ok(py_obj) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.close(py)
    }

    fn __repr__(&self) -> String {
        format!("AsyncWorkerClient(addr={:?})", self.addr)
    }
}

impl PyAsyncWorkerClient {
    /// Internal constructor used by stage-B `AsyncGoosefs.acquire_worker_for_block`
    /// to wrap a pool-acquired `WorkerClient` without going through
    /// `WorkerClient::connect` (which would do an extra TCP+SASL handshake).
    ///
    /// Not exposed to Python — the only path Python sees is the static
    /// factory `connect` / `connect_simple`.
    #[allow(dead_code)] // wired up in stage B
    pub(crate) fn from_sdk(client: WorkerClient) -> Self {
        let addr = client.addr().to_string();
        Self {
            inner: Arc::new(AsyncMutex::new(Some(client))),
            addr,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WorkerClient (sync escape hatch)
// ─────────────────────────────────────────────────────────────────────────────

/// Synchronous (blocking) low-level Worker block client — sync mirror of
/// [`PyAsyncWorkerClient`].
///
/// This is the sync escape hatch for advanced callers that already know the
/// Worker address and want a one-shot positioned read without going through
/// `Goosefs.positioned_read` (which routes via the master). Most users
/// should prefer `Goosefs.positioned_read(path, offset, length)` — this
/// class exists for benchmarking, custom routing experiments, and parity
/// with the async API surface.
///
/// ```python
/// from goosefs import WorkerClient, Config
///
/// def pos_read(addr: str, block_id: int, offset: int, length: int) -> bytes:
///     cfg = Config("127.0.0.1:9200")
///     with WorkerClient.connect(addr, cfg) as wc:
///         return wc.read_block_positioned(block_id, offset, length)
/// ```
///
/// All methods drive the shared Tokio runtime via `block_on` and therefore
/// must NOT be called from inside an asyncio loop or a Tokio worker
/// thread (same constraint as the sync `Goosefs` class).
#[pyclass(module = "goosefs._goosefs", name = "WorkerClient")]
pub struct PyWorkerClient {
    inner: Arc<AsyncMutex<Option<WorkerClient>>>,
    addr: String,
}

#[pymethods]
impl PyWorkerClient {
    /// `WorkerClient.connect(addr, config)` → `WorkerClient` (sync).
    ///
    /// Establishes a TCP+SASL handshake to the Worker at `addr`.
    /// Synchronous counterpart of [`PyAsyncWorkerClient::connect`].
    #[staticmethod]
    fn connect(addr: String, config: PyConfig) -> PyResult<Self> {
        if addr.is_empty() {
            return Err(PyValueError::new_err(
                "WorkerClient.connect: addr must not be empty",
            ));
        }
        let sdk_cfg = config.inner.clone();
        let addr_for_block = addr.clone();
        let client = block_on(async move {
            WorkerClient::connect(&addr_for_block, &sdk_cfg)
                .await
                .map_err(map_err)
        })?;
        Ok(PyWorkerClient {
            inner: Arc::new(AsyncMutex::new(Some(client))),
            addr,
        })
    }

    /// `WorkerClient.connect_simple(addr, connect_timeout_ms=10_000)` (sync).
    ///
    /// **Deprecated escape hatch** — connects without SASL authentication.
    /// Only useful for test workers configured with `auth_type=NOSASL`.
    #[staticmethod]
    #[pyo3(signature = (addr, connect_timeout_ms = 10_000))]
    fn connect_simple(py: Python<'_>, addr: String, connect_timeout_ms: u64) -> PyResult<Self> {
        if addr.is_empty() {
            return Err(PyValueError::new_err(
                "WorkerClient.connect_simple: addr must not be empty",
            ));
        }
        let warnings = py.import("warnings")?;
        let deprecation_warning = py.import("builtins")?.getattr("DeprecationWarning")?;
        warnings.call_method1(
            "warn",
            (
                "connect_simple is deprecated — use connect() with proper Config for production use",
                deprecation_warning,
                2,
            ),
        )?;
        let timeout = Duration::from_millis(connect_timeout_ms);
        let addr_for_block = addr.clone();
        let client = block_on(async move {
            WorkerClient::connect_simple(&addr_for_block, timeout)
                .await
                .map_err(map_err)
        })?;
        Ok(PyWorkerClient {
            inner: Arc::new(AsyncMutex::new(Some(client))),
            addr,
        })
    }

    /// `wc.read_block_positioned(block_id, offset, length, chunk_size=1<<20)` → `bytes` (sync).
    ///
    /// One-shot positioned read against a single block. See
    /// [`PyAsyncWorkerClient::read_block_positioned`] for full semantics.
    #[pyo3(signature = (block_id, offset, length, chunk_size = DEFAULT_CHUNK_SIZE))]
    fn read_block_positioned<'py>(
        &self,
        py: Python<'py>,
        block_id: i64,
        offset: i64,
        length: i64,
        chunk_size: i64,
    ) -> PyResult<Bound<'py, PyBytes>> {
        if offset < 0 {
            return Err(PyValueError::new_err("offset must be non-negative"));
        }
        if length < 0 {
            return Err(PyValueError::new_err("length must be non-negative"));
        }
        if chunk_size <= 0 {
            return Err(PyValueError::new_err("chunk_size must be positive"));
        }
        if length == 0 {
            return Ok(PyBytes::new(py, &[]));
        }

        let inner = Arc::clone(&self.inner);
        let bytes = block_on(async move {
            let worker = {
                let guard = inner.lock().await;
                guard
                    .as_ref()
                    .ok_or_else(|| PyRuntimeError::new_err("WorkerClient is closed"))?
                    .clone()
            };
            GrpcBlockReader::positioned_read(
                &worker, block_id, offset, length, chunk_size, /* ufs_opts */ None,
            )
            .await
            .map_err(map_err)
        })?;

        Ok(PyBytes::new(py, &bytes))
    }

    /// `wc.addr` — the `host:port` address this client is connected to.
    #[getter]
    fn addr(&self) -> &str {
        &self.addr
    }

    /// `wc.close()` — release the underlying gRPC channel. Idempotent.
    fn close(&self) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        block_on(async move {
            let mut guard = inner.lock().await;
            let _ = guard.take();
        });
        Ok(())
    }

    /// `with WorkerClient.connect(...) as wc: ...`
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__<'py>(
        &self,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<()> {
        self.close()
    }

    fn __repr__(&self) -> String {
        format!("WorkerClient(addr={:?})", self.addr)
    }
}
