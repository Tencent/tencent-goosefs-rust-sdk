//! `AsyncGoosefs` — coroutine-returning Goosefs client.
//!
//! Every method returns a Python `awaitable`. Internally the future is run on
//! the shared Tokio runtime (`crate::runtime`) via
//! [`pyo3_async_runtimes::tokio::future_into_py`].
//!
//! ## Lifecycle
//!
//! ```python
//! async with await AsyncGoosefs.connect(cfg) as fs:
//!     await fs.mkdir("/tmp/p2", recursive=True)
//!     status = await fs.get_status("/tmp/p2")
//! # `close()` runs on `__aexit__`, releasing master/worker connections.
//! ```
//!
//! ## Thread-safety
//!
//! `AsyncGoosefs` holds an `Arc<FileSystemContext>` + `Arc<BaseFileSystem>`,
//! so it is `Send + Sync` and a single instance can be shared across
//! `asyncio` tasks.
//!
//! P3 (sync `Goosefs`) and P4/P5 (read/write/streaming) will reuse the same
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

/// Extract a bytes-like Python object into an owned `Vec<u8>`.
///
/// Accepts any object implementing the buffer protocol with format `B`/`c`,
/// i.e. `bytes`, `bytearray`, `memoryview` of bytes, `array.array("B", …)`,
/// NumPy `uint8` arrays, etc. **Explicitly rejects `str`** — PyO3's
/// `FromPyObject for Vec<u8>` would happily decode a `str` as Latin-1
/// bytes, which is almost never what the caller meant. We forbid it so a
/// silent-but-wrong write is converted into a clear `TypeError`.
pub(crate) fn extract_bytes_like(data: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if data.is_instance_of::<pyo3::types::PyString>() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "`data` must be a bytes-like object (bytes, bytearray, memoryview); got str. \
             Encode it explicitly with `s.encode(\"utf-8\")` first.",
        ));
    }
    data.extract::<Vec<u8>>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "`data` must be a bytes-like object (bytes, bytearray, memoryview)",
        )
    })
}

/// Build a `CreateFilePOptions` from binding-level parameters.
///
/// Returns `None` only if the caller passed *no* override at all — letting
/// the SDK fall back to its full default path (parent xattr inheritance for
/// `WriteType`, cluster default block size). This is also why we expose
/// the helper as `pub(crate)`: the synchronous wrapper in `sync_fs.rs`
/// reuses the exact same construction logic.
pub(crate) fn build_create_file_options(
    write_type: Option<crate::types::PyWriteType>,
    block_size_bytes: Option<i64>,
    recursive: bool,
) -> Option<goosefs_sdk::proto::grpc::file::CreateFilePOptions> {
    // If every field is at its "no override" value, return None so the SDK
    // takes the fully-default path (which itself does parent-xattr inheritance).
    if write_type.is_none() && block_size_bytes.is_none() && !recursive {
        return None;
    }
    Some(goosefs_sdk::proto::grpc::file::CreateFilePOptions {
        block_size_bytes,
        recursive: Some(recursive),
        write_type: write_type.map(|wt| {
            let sdk_wt: goosefs_sdk::config::WriteType = wt.into();
            goosefs_sdk::proto::grpc::file::WritePType::from(sdk_wt) as i32
        }),
        ..Default::default()
    })
}

/// Async Goosefs filesystem client.
#[pyclass(module = "goosefs._goosefs", name = "AsyncGoosefs")]
pub struct PyAsyncGoosefs {
    /// `None` after `close()` — every subsequent op raises `RuntimeError`.
    ///
    /// `std::sync::Mutex` is fine here because the lock is never held across
    /// an `.await`; we only use it to clone the inner handle into the future.
    handle: Mutex<Option<PyFsHandle>>,
}

impl PyAsyncGoosefs {
    fn handle(&self) -> PyResult<PyFsHandle> {
        let guard = self
            .handle
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("handle mutex poisoned"))?;
        guard
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("AsyncGoosefs is closed"))
    }
}

#[pymethods]
impl PyAsyncGoosefs {
    /// `await AsyncGoosefs.connect(cfg)` → connected client.
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
                    PyAsyncGoosefs {
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

    // ── High-level read / write ─────────────────────────────────────────────

    /// `await fs.read_file(path)` → `bytes` (full file contents).
    ///
    /// Loads the entire file into a single Python `bytes` object. This is the
    /// most convenient API for small-to-medium files (think configs, JSON,
    /// model weights up to a few hundred MB) but it materialises the whole
    /// payload in RAM — for large files prefer the streaming reader that will
    /// land in P5 (`open_file()`).
    ///
    /// Implementation: dispatches to
    /// [`goosefs_sdk::io::GoosefsFileReader::read_file_with_context`], which
    /// internally splits the file into block-sized segments and concatenates
    /// the resulting `Bytes`. The Python `bytes` object is built in a
    /// GIL-reacquired closure via `PyBytes::new`, which copies once from the
    /// SDK's `Bytes` into Python-owned memory.
    fn read_file<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            let bytes = goosefs_sdk::io::GoosefsFileReader::read_file_with_context(
                h.ctx.clone(),
                &path,
            )
            .await
            .map_err(map_err)?;
            // Hand off to Python: `PyBytes::new` performs a single copy. We
            // could in principle use `PyBytes::new_bound_with` to populate the
            // buffer in-place, but the win is marginal and `Bytes::as_ref()`
            // already gives us a contiguous slice.
            Python::attach(|py| {
                Ok(pyo3::types::PyBytes::new(py, bytes.as_ref()).unbind())
            })
        })
    }

    /// `await fs.read_range(path, offset, length)` → `bytes`.
    ///
    /// Read `length` bytes starting at byte `offset`. Both arguments are
    /// non-negative. If `offset + length` exceeds the file length the SDK
    /// will short-read and return whatever is available — no error.
    fn read_range<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: u64,
        length: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            let bytes = goosefs_sdk::io::GoosefsFileReader::read_range_with_context(
                h.ctx.clone(),
                &path,
                offset,
                length,
            )
            .await
            .map_err(map_err)?;
            Python::attach(|py| {
                Ok(pyo3::types::PyBytes::new(py, bytes.as_ref()).unbind())
            })
        })
    }

    /// `await fs.write_file(path, data, *, write_type=None, block_size_bytes=None, recursive=False)` → `int` (bytes written).
    ///
    /// One-shot file create + write + complete. `data` accepts any
    /// bytes-like Python object (`bytes`, `bytearray`, `memoryview`, …) —
    /// PyO3 borrows it as `&[u8]`, and we copy into a Rust `Vec<u8>` so the
    /// future can outlive the GIL acquisition.
    ///
    /// ## Parameters
    ///
    /// * `write_type` — explicit [`WriteType`]. `None` (default) means
    ///   *inherit* from the parent directory's `innerWriteType` xattr,
    ///   falling back to the cluster default. This matches Java/Go SDK
    ///   behaviour.
    /// * `block_size_bytes` — override the per-file block size. `None` uses
    ///   the cluster default.
    /// * `recursive` — create missing parent directories.
    ///
    /// ## Performance notes
    ///
    /// `Vec<u8>` materialisation means we copy the payload once. For very
    /// large writes (e.g. multi-GB) consider the streaming writer landing in
    /// P5 to avoid that copy. (Review #17.1: documented.)
    #[pyo3(signature = (path, data, *, write_type=None, block_size_bytes=None, recursive=false))]
    fn write_file<'py>(
        &self,
        py: Python<'py>,
        path: String,
        data: &Bound<'py, PyAny>,
        write_type: Option<crate::types::PyWriteType>,
        block_size_bytes: Option<i64>,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        // Accept any bytes-like object: `bytes`, `bytearray`, `memoryview`,
        // `array.array("B", ...)`, NumPy `uint8` arrays, … but never `str`.
        // See `extract_bytes_like` for the rationale.
        let payload = extract_bytes_like(data)?;
        let proto_opts = build_create_file_options(write_type, block_size_bytes, recursive);

        future_into_py(py, async move {
            let n = goosefs_sdk::io::GoosefsFileWriter::write_file_with_context_and_options(
                h.ctx.clone(),
                &path,
                &payload,
                proto_opts,
            )
            .await
            .map_err(map_err)?;
            Ok(n)
        })
    }

    // ── Streaming open / create (P5) ────────────────────────────────────────

    /// `await fs.open_file(path)` → `AsyncFileReader`.
    ///
    /// Opens a seekable streaming reader. The returned object holds onto
    /// the shared context, so closing the parent `AsyncGoosefs` is safe
    /// — the reader keeps the connection alive until *its own* `close()`
    /// or garbage collection.
    fn open_file<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            let stream = crate::streaming::sdk_open_in_stream(h.ctx.clone(), path).await?;
            Python::attach(|py| {
                Py::new(py, crate::streaming::PyAsyncFileReader::from_sdk(stream))
                    .map(|p| p.into_any())
            })
        })
    }

    /// `await fs.create_file(path, *, write_type=None, block_size_bytes=None, recursive=False)` → `AsyncFileWriter`.
    ///
    /// Opens a streaming writer. Caller is expected to `close()` (or use
    /// `async with`) to commit the file. Unhandled exceptions inside the
    /// `async with` block trigger `cancel()` instead, so half-written
    /// data is not finalised.
    #[pyo3(signature = (path, *, write_type=None, block_size_bytes=None, recursive=false))]
    fn create_file<'py>(
        &self,
        py: Python<'py>,
        path: String,
        write_type: Option<crate::types::PyWriteType>,
        block_size_bytes: Option<i64>,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        let path_for_writer = path.clone();
        future_into_py(py, async move {
            let writer = crate::streaming::sdk_create_writer(
                h.ctx.clone(),
                path,
                write_type,
                block_size_bytes,
                recursive,
            )
            .await?;
            Python::attach(|py| {
                Py::new(
                    py,
                    crate::streaming::PyAsyncFileWriter::from_sdk(writer, path_for_writer),
                )
                .map(|p| p.into_any())
            })
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
                Some(h) => format!("AsyncGoosefs(master={:?})", h.ctx.config().master_addr),
                None => "AsyncGoosefs(<closed>)".to_string(),
            },
            Err(_) => "AsyncGoosefs(<poisoned>)".to_string(),
        }
    }
}
