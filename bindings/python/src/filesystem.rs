//! `AsyncGooseFs` — coroutine-returning Goosefs client.
//!
//! Every method returns a Python `awaitable`. Internally the future is run on
//! the shared Tokio runtime (`crate::runtime`) via
//! [`pyo3_async_runtimes::tokio::future_into_py`].
//!
//! ## Lifecycle
//!
//! ```python
//! async with await AsyncGooseFs.connect(cfg) as fs:
//!     await fs.mkdir("/tmp/p2", recursive=True)
//!     status = await fs.get_status("/tmp/p2")
//! # `close()` runs on `__aexit__`, releasing master/worker connections.
//! ```
//!
//! ## Thread-safety
//!
//! `AsyncGooseFs` holds an `Arc<FileSystemContext>` + `Arc<BaseFileSystem>`,
//! so it is `Send + Sync` and a single instance can be shared across
//! `asyncio` tasks.
//!
//! P3 (sync `GooseFs`) and P4/P5 (read/write/streaming) will reuse the same
//! `PyFsHandle` lower-half.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3_async_runtimes::tokio::future_into_py;

use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::FileSystem;

use crate::config::PyConfig;
use crate::context::PyFsHandle;
use crate::errors::map_err;
use crate::options::PyDeleteOptions;
use crate::status::PyURIStatus;

/// Async Goosefs filesystem client.
#[pyclass(module = "goosefs._goosefs", name = "AsyncGooseFs")]
pub struct PyAsyncGooseFs {
    /// `None` after `close()` — every subsequent op raises `RuntimeError`.
    ///
    /// `std::sync::Mutex` is fine here because the lock is never held across
    /// an `.await`; we only use it to clone the inner handle into the future.
    handle: Mutex<Option<PyFsHandle>>,
}

impl PyAsyncGooseFs {
    fn handle(&self) -> PyResult<PyFsHandle> {
        let guard = self
            .handle
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("handle mutex poisoned"))?;
        guard
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("AsyncGooseFs is closed"))
    }
}

#[pymethods]
impl PyAsyncGooseFs {
    /// `await AsyncGooseFs.connect(cfg)` → connected client.
    ///
    /// Bootstrap is async because it performs the initial Master+Worker
    /// handshake. `cfg` is cloned, so the caller may keep using it for
    /// further connections.
    #[staticmethod]
    fn connect<'py>(py: Python<'py>, config: &PyConfig) -> PyResult<Bound<'py, PyAny>> {
        let cfg = config.inner.clone();
        future_into_py(py, async move {
            let ctx = FileSystemContext::connect(cfg).await.map_err(map_err)?;
            let handle = PyFsHandle::new(ctx);
            Python::attach(|py| {
                Py::new(
                    py,
                    PyAsyncGooseFs {
                        handle: Mutex::new(Some(handle)),
                    },
                )
            })
        })
    }

    // ── Status ──────────────────────────────────────────────────────────────

    /// `await fs.get_status(path)` → `URIStatus`.
    fn get_status<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            let s = h.fs.get_status(&path).await.map_err(map_err)?;
            Ok(PyURIStatus::new(s))
        })
    }

    /// `await fs.list_status(path, recursive=False)` → `list[URIStatus]`.
    #[pyo3(signature = (path, *, recursive=false))]
    fn list_status<'py>(
        &self,
        py: Python<'py>,
        path: String,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            let v = h.fs.list_status(&path, recursive).await.map_err(map_err)?;
            Ok(v.into_iter().map(PyURIStatus::new).collect::<Vec<_>>())
        })
    }

    /// `await fs.exists(path)` → `bool`.
    fn exists<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            let b = h.fs.exists(&path).await.map_err(map_err)?;
            Ok(b)
        })
    }

    // ── Mutations ───────────────────────────────────────────────────────────

    /// `await fs.mkdir(path, recursive=False)`.
    ///
    /// Goosefs's `mkdir` is *not* idempotent: creating an existing directory
    /// raises `AlreadyExists`. Pass `recursive=True` to silently create any
    /// missing intermediate components.
    #[pyo3(signature = (path, *, recursive=false))]
    fn mkdir<'py>(
        &self,
        py: Python<'py>,
        path: String,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            h.fs.mkdir(&path, recursive).await.map_err(map_err)?;
            Ok(())
        })
    }

    /// `await fs.delete(path, *, recursive=False, unchecked=False, goosefs_only=False)`.
    ///
    /// All keyword flags map 1:1 to the SDK's `DeleteOptions`. To pass an
    /// already-built `DeleteOptions` instance, call `.delete_with_options()`.
    #[pyo3(signature = (path, *, recursive=false, unchecked=false, goosefs_only=false))]
    fn delete<'py>(
        &self,
        py: Python<'py>,
        path: String,
        recursive: bool,
        unchecked: bool,
        goosefs_only: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        let opts = goosefs_sdk::fs::options::DeleteOptions {
            recursive,
            unchecked,
            goosefs_only,
        };
        future_into_py(py, async move {
            h.fs.delete(&path, opts).await.map_err(map_err)?;
            Ok(())
        })
    }

    /// `await fs.delete_with_options(path, opts)` — same as `delete()` but
    /// takes a pre-built `DeleteOptions` object.
    fn delete_with_options<'py>(
        &self,
        py: Python<'py>,
        path: String,
        options: PyDeleteOptions,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        let opts = options.into_sdk();
        future_into_py(py, async move {
            h.fs.delete(&path, opts).await.map_err(map_err)?;
            Ok(())
        })
    }

    /// `await fs.rename(src, dst)`.
    fn rename<'py>(
        &self,
        py: Python<'py>,
        src: String,
        dst: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            h.fs.rename(&src, &dst).await.map_err(map_err)?;
            Ok(())
        })
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────

    /// `await fs.close()` — shut down master + worker connections.
    ///
    /// Idempotent. After close, every other method raises `RuntimeError`.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Take the handle out under the lock; if already closed, this is a
        // no-op coroutine. We tolerate a poisoned mutex by treating it as
        // already-closed — better than panicking on shutdown.
        let taken = self
            .handle
            .lock()
            .map(|mut g| g.take())
            .unwrap_or(None);
        future_into_py(py, async move {
            if let Some(h) = taken {
                // `FileSystemContext::close(&self)` does not consume the Arc;
                // dropping our remaining refs after the call lets the SDK's
                // background tasks shut down cleanly.
                h.ctx.close().await.map_err(map_err)?;
                drop(h.fs);
                drop(h.ctx);
            }
            Ok(())
        })
    }

    // ── async context-manager protocol ──────────────────────────────────────

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // `async with await connect(...) as fs` — `__aenter__` simply yields
        // `self` once awaited. We cannot return `Py<Self>` directly because
        // the async-runtime expects a Python awaitable.
        let me: Py<Self> = slf.into();
        future_into_py(py, async move { Ok(me) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyType>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Delegate to `close()` so resources are released on context exit.
        self.close(py)
    }

    fn __repr__(&self) -> String {
        match self.handle.lock() {
            Ok(g) => match g.as_ref() {
                Some(h) => format!("AsyncGooseFs(master={:?})", h.ctx.config().master_addr),
                None => "AsyncGooseFs(<closed>)".to_string(),
            },
            Err(_) => "AsyncGooseFs(<poisoned>)".to_string(),
        }
    }
}
