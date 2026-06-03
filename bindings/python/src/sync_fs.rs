//! `Goosefs` — synchronous wrapper around `AsyncGoosefs`.
//!
//! This is the blocking façade for users who do not want to deal with
//! `asyncio`. Internally every method calls
//! [`crate::runtime::block_on`] on the shared Tokio runtime, with the GIL
//! released via [`Python::allow_threads`] so other Python threads can keep
//! making progress.
//!
//! ## Lifecycle
//!
//! ```python
//! from goosefs import Goosefs, Config
//!
//! with Goosefs(Config("127.0.0.1:9200")) as fs:
//!     fs.mkdir("/tmp/p3", recursive=True)
//!     assert fs.exists("/tmp/p3")
//! ```
//!
//! ## Safety guards
//!
//! Two classes of correctness bugs are easy to hit with a sync wrapper
//! around an async runtime; both are caught at the Python boundary:
//!
//! 1. **Deadlock from inside an asyncio loop** (Review #17.1). If user
//!    code calls a sync method while a Python `asyncio` event loop is
//!    running on the *same* thread (or while we are already on a Tokio
//!    worker), `runtime.block_on()` would dead-lock the executor. We
//!    detect both situations in [`Self::guarded_block_on`] and surface a
//!    `RuntimeError` instead.
//!
//! 2. **Fork-after-connect** (Review #17.4). gRPC connections, Tokio
//!    runtime worker threads and tonic channels are *not* fork-safe. If
//!    the process forks after `Goosefs(...)` was created and the child
//!    tries to reuse the inherited handle, we abort with `RuntimeError`.
//!    The user must reconnect in the child.

use std::sync::Mutex;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::FileSystem;

use crate::config::PyConfig;
use crate::context::PyFsHandle;
use crate::errors::map_err;
use crate::options::PyDeleteOptions;
use crate::runtime::block_on;
use crate::status::PyURIStatus;

/// Synchronous (blocking) Goosefs filesystem client.
///
/// `Goosefs` is the convenient counterpart to `AsyncGoosefs` for
/// scripts, REPL sessions and any code that does not run inside an
/// `asyncio` event loop.
///
/// **Do not** instantiate or call `Goosefs` from inside a coroutine — use
/// `AsyncGoosefs` instead. Calling sync methods while an asyncio loop is
/// running on the same thread will raise `RuntimeError` rather than
/// dead-lock.
#[pyclass(module = "goosefs._goosefs", name = "Goosefs", weakref)]
pub struct PyGoosefs {
    /// `None` after `close()` — every subsequent op raises `RuntimeError`.
    handle: Mutex<Option<PyFsHandle>>,
    /// PID of the process that created this handle.
    ///
    /// Used to detect post-fork reuse: gRPC connections / Tokio workers
    /// inherited across `fork()` are not safe to reuse, so we refuse to
    /// touch them in the child.
    creator_pid: u32,
}

impl PyGoosefs {
    /// Acquire a clone of the inner handle, enforcing the close + fork
    /// invariants on every call.
    fn handle(&self) -> PyResult<PyFsHandle> {
        // Fork check first — if we are in the child, even reading the
        // mutex is risky because background tokio threads vanish on fork.
        let pid = std::process::id();
        if pid != self.creator_pid {
            return Err(PyRuntimeError::new_err(format!(
                "Goosefs cannot be used after fork (created in pid={}, now in pid={}); \
                 reconnect in the child process",
                self.creator_pid, pid
            )));
        }

        let guard = self
            .handle
            .lock()
            .map_err(|_| PyRuntimeError::new_err("handle mutex poisoned"))?;
        guard
            .clone()
            .ok_or_else(|| PyRuntimeError::new_err("Goosefs is closed"))
    }

    /// Run `fut` to completion on the shared Tokio runtime, releasing the
    /// GIL while we wait. Refuses to run if the caller is inside an
    /// asyncio loop or already on a Tokio worker — both of which would
    /// deadlock `runtime.block_on()`.
    fn guarded_block_on<F, T>(py: Python<'_>, fut: F) -> PyResult<T>
    where
        F: std::future::Future<Output = PyResult<T>> + Send,
        T: Send,
    {
        // 1) Refuse if we are already inside *any* tokio runtime context.
        //    `Handle::try_current()` is the canonical, allocation-free way
        //    to ask "am I on a worker thread right now?".
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(PyRuntimeError::new_err(
                "Goosefs sync methods cannot be invoked from inside a Tokio runtime; \
                 use `AsyncGoosefs` from your async code instead",
            ));
        }

        // 2) Refuse if a Python asyncio event loop is running on this thread.
        //    `asyncio.get_running_loop()` returns the loop or raises
        //    RuntimeError — we exploit that to detect the case without
        //    importing internal symbols.
        let asyncio = py.import("asyncio")?;
        if asyncio.call_method0("get_running_loop").is_ok() {
            return Err(PyRuntimeError::new_err(
                "Goosefs sync methods cannot be invoked from inside an asyncio event loop; \
                 use `AsyncGoosefs` and `await` instead",
            ));
        }

        // 3) Safe to block. Drop the GIL so other Python threads keep moving.
        //    PyO3 0.27 renamed `allow_threads` → `detach`; semantics unchanged.
        py.detach(|| block_on(fut))
    }
}

#[pymethods]
impl PyGoosefs {
    /// `Goosefs(config)` — synchronous connect.
    ///
    /// Performs the master + worker handshake on the shared Tokio runtime
    /// and returns once the connection is ready. Raises `RuntimeError`
    /// if called from inside an `asyncio` event loop.
    #[new]
    fn new(py: Python<'_>, config: &PyConfig) -> PyResult<Self> {
        let cfg = config.inner.clone();
        let ctx = Self::guarded_block_on(py, async move {
            FileSystemContext::connect(cfg).await.map_err(map_err)
        })?;
        Ok(PyGoosefs {
            handle: Mutex::new(Some(PyFsHandle::new(ctx))),
            creator_pid: std::process::id(),
        })
    }

    // ── Status ──────────────────────────────────────────────────────────────

    /// `fs.get_status(path)` → `URIStatus`.
    fn get_status(&self, py: Python<'_>, path: String) -> PyResult<PyURIStatus> {
        let h = self.handle()?;
        Self::guarded_block_on(py, async move {
            let s = h.fs.get_status(&path).await.map_err(map_err)?;
            Ok(PyURIStatus::new(s))
        })
    }

    /// `fs.list_status(path, recursive=False)` → `list[URIStatus]`.
    #[pyo3(signature = (path, *, recursive=false))]
    fn list_status(
        &self,
        py: Python<'_>,
        path: String,
        recursive: bool,
    ) -> PyResult<Vec<PyURIStatus>> {
        let h = self.handle()?;
        Self::guarded_block_on(py, async move {
            let v = h.fs.list_status(&path, recursive).await.map_err(map_err)?;
            Ok(v.into_iter().map(PyURIStatus::new).collect())
        })
    }

    /// `fs.exists(path)` → `bool`.
    fn exists(&self, py: Python<'_>, path: String) -> PyResult<bool> {
        let h = self.handle()?;
        Self::guarded_block_on(py, async move {
            h.fs.exists(&path).await.map_err(map_err)
        })
    }

    // ── Mutations ───────────────────────────────────────────────────────────

    /// `fs.mkdir(path, recursive=False)`.
    ///
    /// Idempotent: creating an already-existing directory is a no-op
    /// (the underlying SDK hard-wires `allow_exists=true`).
    #[pyo3(signature = (path, *, recursive=false))]
    fn mkdir(&self, py: Python<'_>, path: String, recursive: bool) -> PyResult<()> {
        let h = self.handle()?;
        Self::guarded_block_on(py, async move {
            h.fs.mkdir(&path, recursive).await.map_err(map_err)
        })
    }

    /// `fs.delete(path, *, recursive=False, unchecked=False, goosefs_only=False)`.
    #[pyo3(signature = (path, *, recursive=false, unchecked=false, goosefs_only=false))]
    fn delete(
        &self,
        py: Python<'_>,
        path: String,
        recursive: bool,
        unchecked: bool,
        goosefs_only: bool,
    ) -> PyResult<()> {
        let h = self.handle()?;
        let opts = goosefs_sdk::fs::options::DeleteOptions {
            recursive,
            unchecked,
            goosefs_only,
        };
        Self::guarded_block_on(py, async move {
            h.fs.delete(&path, opts).await.map_err(map_err)
        })
    }

    /// `fs.delete_with_options(path, opts)` — same as `delete()` but takes
    /// a pre-built `DeleteOptions` instance.
    fn delete_with_options(
        &self,
        py: Python<'_>,
        path: String,
        options: PyDeleteOptions,
    ) -> PyResult<()> {
        let h = self.handle()?;
        let opts = options.into_sdk();
        Self::guarded_block_on(py, async move {
            h.fs.delete(&path, opts).await.map_err(map_err)
        })
    }

    /// `fs.rename(src, dst)`.
    fn rename(&self, py: Python<'_>, src: String, dst: String) -> PyResult<()> {
        let h = self.handle()?;
        Self::guarded_block_on(py, async move {
            h.fs.rename(&src, &dst).await.map_err(map_err)
        })
    }

    // ── High-level read / write ─────────────────────────────────────────────

    /// `fs.read_file(path)` → `bytes` (full file contents).
    ///
    /// Synchronous counterpart of [`PyAsyncGoosefs::read_file`]; same caveats
    /// about full materialisation in RAM apply (Review #17.1: documented).
    fn read_file<'py>(
        &self,
        py: Python<'py>,
        path: String,
    ) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let h = self.handle()?;
        // We must collect into an owned `Vec<u8>` inside the blocking section
        // because the resulting `PyBytes` can only be constructed once the
        // GIL is re-acquired by `guarded_block_on`'s caller. We *cannot* hold
        // a `Python<'py>` reference inside the future passed to `block_on`,
        // so we copy the bytes out first and then wrap them.
        let buf: Vec<u8> = Self::guarded_block_on(py, async move {
            let bytes = goosefs_sdk::io::GoosefsFileReader::read_file_with_context(
                h.ctx.clone(),
                &path,
            )
            .await
            .map_err(map_err)?;
            // `Bytes::to_vec` is a single copy; same overhead as the async
            // path (which copies through `PyBytes::new`).
            Ok(bytes.to_vec())
        })?;
        Ok(pyo3::types::PyBytes::new(py, &buf))
    }

    /// `fs.read_range(path, offset, length)` → `bytes`.
    fn read_range<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: u64,
        length: u64,
    ) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let h = self.handle()?;
        let buf: Vec<u8> = Self::guarded_block_on(py, async move {
            let bytes = goosefs_sdk::io::GoosefsFileReader::read_range_with_context(
                h.ctx.clone(),
                &path,
                offset,
                length,
            )
            .await
            .map_err(map_err)?;
            Ok(bytes.to_vec())
        })?;
        Ok(pyo3::types::PyBytes::new(py, &buf))
    }

    /// `fs.write_file(path, data, *, write_type=None, block_size_bytes=None, recursive=False)` → `int`.
    ///
    /// Synchronous counterpart of [`PyAsyncGoosefs::write_file`].
    #[pyo3(signature = (path, data, *, write_type=None, block_size_bytes=None, recursive=false))]
    fn write_file(
        &self,
        py: Python<'_>,
        path: String,
        data: &Bound<'_, PyAny>,
        write_type: Option<crate::types::PyWriteType>,
        block_size_bytes: Option<i64>,
        recursive: bool,
    ) -> PyResult<u64> {
        let h = self.handle()?;
        let payload = crate::filesystem::extract_bytes_like(data)?;
        let proto_opts =
            crate::filesystem::build_create_file_options(write_type, block_size_bytes, recursive);
        Self::guarded_block_on(py, async move {
            goosefs_sdk::io::GoosefsFileWriter::write_file_with_context_and_options(
                h.ctx.clone(),
                &path,
                &payload,
                proto_opts,
            )
            .await
            .map_err(map_err)
        })
    }

    // ── Streaming open / create (P5) ────────────────────────────────────────

    /// `fs.open_file(path)` → `FileReader` (sync).
    fn open_file(&self, py: Python<'_>, path: String) -> PyResult<crate::streaming::PyFileReader> {
        let h = self.handle()?;
        let stream = Self::guarded_block_on(py, async move {
            crate::streaming::sdk_open_in_stream(h.ctx.clone(), path).await
        })?;
        Ok(crate::streaming::PyFileReader::from_sdk(stream))
    }

    /// `fs.create_file(path, *, write_type=None, block_size_bytes=None, recursive=False)` → `FileWriter`.
    #[pyo3(signature = (path, *, write_type=None, block_size_bytes=None, recursive=false))]
    fn create_file(
        &self,
        py: Python<'_>,
        path: String,
        write_type: Option<crate::types::PyWriteType>,
        block_size_bytes: Option<i64>,
        recursive: bool,
    ) -> PyResult<crate::streaming::PyFileWriter> {
        let h = self.handle()?;
        let path_for_writer = path.clone();
        let writer = Self::guarded_block_on(py, async move {
            crate::streaming::sdk_create_writer(
                h.ctx.clone(),
                path,
                write_type,
                block_size_bytes,
                recursive,
            )
            .await
        })?;
        Ok(crate::streaming::PyFileWriter::from_sdk(
            writer,
            path_for_writer,
        ))
    }

    // ── Worker block direct-read (P6 stage B) ───────────────────────────────

    /// `fs.acquire_worker_for_block(block_id)` → `AsyncWorkerClient`.
    ///
    /// Synchronous counterpart of [`PyAsyncGoosefs::acquire_worker_for_block`].
    ///
    /// The returned object is **still an `AsyncWorkerClient`** — direct
    /// `read_block_positioned` calls on it must be awaited from an async
    /// context. For purely synchronous code use
    /// [`Self::positioned_read`] which wraps the whole sequence in a
    /// `block_on`. We deliberately do not expose a sync `WorkerClient`
    /// class because the only sensible thing to do on the binding
    /// boundary is the one-shot positioned read, which we already
    /// provide as `positioned_read(path, ...)`.
    fn acquire_worker_for_block(
        &self,
        py: Python<'_>,
        block_id: i64,
    ) -> PyResult<crate::worker::PyAsyncWorkerClient> {
        let h = self.handle()?;
        Self::guarded_block_on(py, async move {
            let worker_info = h.ctx.acquire_router().select_worker(block_id).await.map_err(map_err)?;
            let net_addr = worker_info
                .address
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("selected worker has no address"))?;
            let worker_addr = crate::filesystem::format_worker_addr(net_addr);
            let client = h.ctx.acquire_worker_pool().acquire(&worker_addr).await.map_err(map_err)?;
            Ok(crate::worker::PyAsyncWorkerClient::from_sdk(client))
        })
    }

    /// `fs.positioned_read(path, *, block_index=0, offset=0, length=-1, chunk_size=1<<20)` → `bytes`.
    ///
    /// Synchronous counterpart of [`PyAsyncGoosefs::positioned_read`].
    /// See that method's docstring for full semantics; this version
    /// blocks the calling thread on the shared Tokio runtime via
    /// [`Self::guarded_block_on`].
    ///
    /// **Note on last-block `length=-1`**: for the last block of a file
    /// the actual block size may be smaller than `block_size_bytes`
    /// reported by master, so `length=-1` returns only the remaining
    /// bytes of that block (which may be < `block_size_bytes`).
    #[pyo3(signature = (path, *, block_index=0, offset=0, length=-1, chunk_size=crate::positioned_read::DEFAULT_CHUNK_SIZE))]
    fn positioned_read<'py>(
        &self,
        py: Python<'py>,
        path: String,
        block_index: usize,
        offset: i64,
        length: i64,
        chunk_size: i64,
    ) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        if offset < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "offset must be non-negative",
            ));
        }
        if chunk_size <= 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "chunk_size must be positive",
            ));
        }
        let h = self.handle()?;
        let buf: Vec<u8> = Self::guarded_block_on(py, async move {
            // 1. Resolve URIStatus → block_id + block_size via shared helper.
            let status = h.fs.get_status(&path).await.map_err(map_err)?;
            let (block_id, block_size) =
                crate::positioned_read::resolve_block_id(&status, block_index, &path)?;
            if offset >= block_size {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "offset={} >= block_size_bytes={}",
                    offset, block_size
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

            // 2–4. Route + acquire + read with SASL auth-failure retry.
            //       Delegated to `positioned_read_with_reauth` so both
            //       async and sync paths share the same retry logic
            //       (Critical #1 fix: sync was previously missing this).
            let bytes = crate::positioned_read::positioned_read_with_reauth(
                h.ctx,
                block_id,
                offset,
                effective_length,
                chunk_size,
            )
            .await?;
            Ok(bytes)
        })?;
        Ok(pyo3::types::PyBytes::new(py, &buf))
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────

    /// `fs.close()` — release master + worker connections.
    ///
    /// Idempotent: calling close on an already-closed instance is a no-op.
    /// After close, every other method raises `RuntimeError`.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        // Fork-check up front; we still want to refuse silently-leaking
        // an inherited handle from the child even at close time.
        let pid = std::process::id();
        if pid != self.creator_pid {
            return Err(PyRuntimeError::new_err(format!(
                "Goosefs cannot be used after fork (created in pid={}, now in pid={})",
                self.creator_pid, pid
            )));
        }

        let taken = self
            .handle
            .lock()
            .map(|mut g| g.take())
            .unwrap_or(None);

        if let Some(h) = taken {
            Self::guarded_block_on(py, async move {
                h.ctx.close().await.map_err(map_err)?;
                drop(h.fs);
                drop(h.ctx);
                Ok(())
            })?;
        }
        Ok(())
    }

    // ── sync context-manager protocol ───────────────────────────────────────

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        // Returning `False` propagates exceptions from the `with`-block,
        // matching standard Python context-manager semantics.
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        match self.handle.lock() {
            Ok(g) => match g.as_ref() {
                Some(h) => format!("Goosefs(master={:?})", h.ctx.config().master_addr),
                None => "Goosefs(<closed>)".to_string(),
            },
            Err(_) => "Goosefs(<poisoned>)".to_string(),
        }
    }
}
