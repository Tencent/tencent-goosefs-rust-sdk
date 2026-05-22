//! `GooseFs` — synchronous wrapper around `AsyncGooseFs`.
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
//! from goosefs import GooseFs, Config
//!
//! with GooseFs(Config("127.0.0.1:9200")) as fs:
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
//!    the process forks after `GooseFs(...)` was created and the child
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
/// `GooseFs` is the convenient counterpart to `AsyncGooseFs` for
/// scripts, REPL sessions and any code that does not run inside an
/// `asyncio` event loop.
///
/// **Do not** instantiate or call `GooseFs` from inside a coroutine — use
/// `AsyncGooseFs` instead. Calling sync methods while an asyncio loop is
/// running on the same thread will raise `RuntimeError` rather than
/// dead-lock.
#[pyclass(module = "goosefs._goosefs", name = "GooseFs")]
pub struct PyGooseFs {
    /// `None` after `close()` — every subsequent op raises `RuntimeError`.
    handle: Mutex<Option<PyFsHandle>>,
    /// PID of the process that created this handle.
    ///
    /// Used to detect post-fork reuse: gRPC connections / Tokio workers
    /// inherited across `fork()` are not safe to reuse, so we refuse to
    /// touch them in the child.
    creator_pid: u32,
}

impl PyGooseFs {
    /// Acquire a clone of the inner handle, enforcing the close + fork
    /// invariants on every call.
    fn handle(&self) -> PyResult<PyFsHandle> {
        // Fork check first — if we are in the child, even reading the
        // mutex is risky because background tokio threads vanish on fork.
        let pid = std::process::id();
        if pid != self.creator_pid {
            return Err(PyRuntimeError::new_err(format!(
                "GooseFs cannot be used after fork (created in pid={}, now in pid={}); \
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
            .ok_or_else(|| PyRuntimeError::new_err("GooseFs is closed"))
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
                "GooseFs sync methods cannot be invoked from inside a Tokio runtime; \
                 use `AsyncGooseFs` from your async code instead",
            ));
        }

        // 2) Refuse if a Python asyncio event loop is running on this thread.
        //    `asyncio.get_running_loop()` returns the loop or raises
        //    RuntimeError — we exploit that to detect the case without
        //    importing internal symbols.
        let asyncio = py.import("asyncio")?;
        if asyncio.call_method0("get_running_loop").is_ok() {
            return Err(PyRuntimeError::new_err(
                "GooseFs sync methods cannot be invoked from inside an asyncio event loop; \
                 use `AsyncGooseFs` and `await` instead",
            ));
        }

        // 3) Safe to block. Drop the GIL so other Python threads keep moving.
        //    PyO3 0.27 renamed `allow_threads` → `detach`; semantics unchanged.
        py.detach(|| block_on(fut))
    }
}

#[pymethods]
impl PyGooseFs {
    /// `GooseFs(config)` — synchronous connect.
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
        Ok(PyGooseFs {
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
                "GooseFs cannot be used after fork (created in pid={}, now in pid={})",
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
                Some(h) => format!("GooseFs(master={:?})", h.ctx.config().master_addr),
                None => "GooseFs(<closed>)".to_string(),
            },
            Err(_) => "GooseFs(<poisoned>)".to_string(),
        }
    }
}
