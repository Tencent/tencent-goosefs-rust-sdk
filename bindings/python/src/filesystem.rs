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
use crate::positioned_read::{positioned_read_with_reauth, resolve_block_id, DEFAULT_CHUNK_SIZE};
use crate::status::PyURIStatus;
use crate::streaming::PyAsyncFileReader;
use crate::worker::PyAsyncWorkerClient;

/// Build a `\"host:port\"` worker address from a `WorkerNetAddress`.
///
/// Mirrors the canonical formatting used by the SDK in
/// `GoosefsFileInStream::connect_worker` so binding-side direct-block reads
/// land on exactly the same gRPC endpoint as the high-level `read_at` /
/// streaming paths.
///
/// **Note**: Worker `BlockWorker` gRPC service listens on `rpc_port` (9203
/// by default); `data_port` is only used by the Netty short-circuit path,
/// which the Rust SDK does not implement.
pub(crate) fn format_worker_addr(addr: &goosefs_sdk::proto::grpc::WorkerNetAddress) -> String {
    format!(
        "{}:{}",
        addr.host.as_deref().unwrap_or("127.0.0.1"),
        addr.rpc_port.unwrap_or(9203)
    )
}

/// Extract a bytes-like Python object into a `bytes::Bytes`.
///
/// Accepts any object implementing the buffer protocol with format `B`/`c`,
/// i.e. `bytes`, `bytearray`, `memoryview` of bytes, `array.array("B", …)`,
/// NumPy `uint8` arrays, etc. **Explicitly rejects `str`** — PyO3's
/// `FromPyObject for Vec<u8>` would happily decode a `str` as Latin-1
/// bytes, which is almost never what the caller meant. We forbid it so a
/// silent-but-wrong write is converted into a clear `TypeError`.
///
/// # Zero-copy fast path (Part V P2, `abi3-py311` only)
///
/// When built with the `abi3-py311` feature the `pyo3::buffer` module is
/// available, so a C-contiguous read-only buffer is wrapped — **without
/// copying** — into a `Bytes` whose backing owner holds the `PyBuffer`
/// alive. The buffer is released when the last `Bytes` clone is dropped;
/// pyo3's `PyBuffer::Drop` re-acquires the GIL, so dropping the `Bytes` on
/// any Tokio worker thread is sound. The portable `abi3-py39` build falls
/// back to the one-copy `extract::<Vec<u8>>()` path.
pub(crate) fn extract_bytes_like(data: &Bound<'_, PyAny>) -> PyResult<bytes::Bytes> {
    if data.is_instance_of::<pyo3::types::PyString>() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "`data` must be a bytes-like object (bytes, bytearray, memoryview); got str. \
             Encode it explicitly with `s.encode(\"utf-8\")` first.",
        ));
    }

    #[cfg(feature = "abi3-py311")]
    {
        use pyo3::buffer::PyBuffer;
        if let Ok(buf) = PyBuffer::<u8>::get(data) {
            // Only the C-contiguous, read-only case is safe to borrow: a
            // non-contiguous or writable buffer could be mutated by Python
            // mid-write, so fall through to the copy path for those.
            if buf.is_c_contiguous() && buf.readonly() {
                return Ok(bytes::Bytes::from_owner(PyBufferOwner(buf)));
            }
        }
    }

    // Fallback (abi3-py39 or non-contiguous/writable buffer): one copy.
    let v: Vec<u8> = data.extract().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "`data` must be a bytes-like object (bytes, bytearray, memoryview)",
        )
    })?;
    Ok(bytes::Bytes::from(v))
}

/// Owner adaptor that lets `bytes::Bytes` borrow a `PyBuffer`'s memory with
/// zero copy (Part V P2). `AsRef<[u8]>` exposes the contiguous bytes; the
/// `PyBuffer` is released on drop (pyo3 re-acquires the GIL internally).
#[cfg(feature = "abi3-py311")]
struct PyBufferOwner(pyo3::buffer::PyBuffer<u8>);

#[cfg(feature = "abi3-py311")]
unsafe impl Send for PyBufferOwner {}
#[cfg(feature = "abi3-py311")]
unsafe impl Sync for PyBufferOwner {}

#[cfg(feature = "abi3-py311")]
impl AsRef<[u8]> for PyBufferOwner {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: validated `is_c_contiguous()` + `readonly()` at construction;
        // the pointer/length come from a live `PyBuffer` we own, so the slice
        // is valid for the owner's lifetime and the data cannot be mutated.
        unsafe { std::slice::from_raw_parts(self.0.buf_ptr() as *const u8, self.0.len_bytes()) }
    }
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
#[pyclass(module = "goosefs._goosefs", name = "AsyncGoosefs", weakref)]
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

    // ── Batch metadata (Phase 2.1) ───────────────────────────────────────────
    //
    // These collapse N independent metadata RPCs into a *single* PyO3 boundary
    // crossing. The futures are driven concurrently on the Tokio runtime via
    // `stream::iter(..).buffered(BATCH_CONCURRENCY_LIMIT)`, so they are
    // in-flight at the same time instead of being serialised one-by-one at
    // the GIL. This is the lever for "application queries many paths at once":
    // it bypasses the per-op GIL-contention ceiling that single-op calls hit
    // under high thread concurrency (see analysis §3.1 / scheme 1).
    //
    // The concurrency cap (`BATCH_CONCURRENCY_LIMIT`, see `crate::context`)
    // bounds how many RPCs can be in flight per batch; this protects the
    // master from a `paths.len() == 10_000` caller starting ten thousand
    // simultaneous gRPC requests. `buffered` (rather than `buffer_unordered`)
    // also preserves input order without an explicit sort.

    /// `await fs.batch_get_status(paths)` → `list[URIStatus]`.
    ///
    /// Issues `get_status` per path with bounded concurrency (at most
    /// `BATCH_CONCURRENCY_LIMIT` RPCs in flight) and returns the results
    /// in input order. The whole batch fails on the first error (e.g. a
    /// `NotFound` for any path) — use individual `get_status` calls if you
    /// need per-path error isolation.
    ///
    /// **Note**: a failed batch does *not* cancel the in-flight RPCs that
    /// have already been dispatched — the early return only stops feeding
    /// new requests into the buffer. Callers should not rely on "all other
    /// requests are aborted" semantics.
    fn batch_get_status<'py>(
        &self,
        py: Python<'py>,
        paths: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            use futures::stream::{self, StreamExt};
            let fs = h.fs.clone();
            stream::iter(paths.into_iter().map(move |p| {
                let fs = fs.clone();
                async move { fs.get_status(&p).await.map_err(map_err) }
            }))
            .buffered(crate::context::BATCH_CONCURRENCY_LIMIT)
            .map(|r| r.map(PyURIStatus::new))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<PyResult<Vec<_>>>()
        })
    }

    /// `await fs.batch_exists(paths)` → `list[bool]`.
    ///
    /// Issues `exists` per path with bounded concurrency (at most
    /// `BATCH_CONCURRENCY_LIMIT` RPCs in flight) and returns the booleans
    /// in input order.
    fn batch_exists<'py>(
        &self,
        py: Python<'py>,
        paths: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            use futures::stream::{self, StreamExt};
            let fs = h.fs.clone();
            stream::iter(paths.into_iter().map(move |p| {
                let fs = fs.clone();
                async move { fs.exists(&p).await.map_err(map_err) }
            }))
            .buffered(crate::context::BATCH_CONCURRENCY_LIMIT)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<PyResult<Vec<bool>>>()
        })
    }

    /// `await fs.batch_open_file(paths)` → `list[AsyncFileReader]`.
    ///
    /// Opens every path with bounded concurrency (at most
    /// `BATCH_CONCURRENCY_LIMIT` RPCs in flight) and returns the readers
    /// in input order.  The whole batch fails on the first error.
    ///
    /// Unlike calling `fs.open_file()` N times from Python (which crosses
    /// the PyO3 boundary N times and serialises ``Python::attach`` for each
    /// returned reader), this method performs all open RPCs inside a single
    /// Rust future, eliminating GIL contention when launched from many
    /// concurrent asyncio tasks.
    fn batch_open_file<'py>(
        &self,
        py: Python<'py>,
        paths: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            use futures::stream::{self, StreamExt};
            let ctx = h.ctx.clone();
            let results: Vec<_> = stream::iter(paths.into_iter().map(move |p| {
                let ctx = ctx.clone();
                async move { crate::streaming::sdk_open_in_stream(ctx, p).await }
            }))
            .buffered(crate::context::BATCH_CONCURRENCY_LIMIT)
            .collect()
            .await;

            Python::attach(|py| {
                results
                    .into_iter()
                    .map(|r| {
                        let reader = PyAsyncFileReader::from_sdk(r?);
                        Py::new(py, reader).map(|p| p.into_any())
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// `await fs.batch_create_file(paths, *, write_type=None, block_size_bytes=None, recursive=False)` → `list[int]`.
    ///
    /// Creates and closes an empty file at every path with bounded concurrency
    /// (at most `BATCH_CONCURRENCY_LIMIT` RPCs in flight). Returns the number
    /// of bytes written per file (always 0 for empty files) in input order.
    ///
    /// The whole batch fails on the first error. Use individual `write_file`
    /// calls if you need per-path error isolation.
    #[pyo3(signature = (paths, *, write_type=None, block_size_bytes=None, recursive=false))]
    fn batch_create_file<'py>(
        &self,
        py: Python<'py>,
        paths: Vec<String>,
        write_type: Option<crate::types::PyWriteType>,
        block_size_bytes: Option<i64>,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        let proto_opts = build_create_file_options(write_type, block_size_bytes, recursive);
        future_into_py(py, async move {
            use futures::stream::{self, StreamExt};
            let ctx = h.ctx.clone();
            let empty: &[u8] = &[];
            stream::iter(paths.into_iter().map(move |p| {
                let ctx = ctx.clone();
                let opts = proto_opts.clone();
                async move {
                    goosefs_sdk::io::GoosefsFileWriter::write_file_with_context_and_options(
                        ctx, &p, empty, opts,
                    )
                    .await
                    .map_err(map_err)
                }
            }))
            .buffered(crate::context::BATCH_CONCURRENCY_LIMIT)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<PyResult<Vec<u64>>>()
        })
    }

    /// `await fs.batch_create_dir(paths, *, recursive=False)` → `None`.
    ///
    /// Creates a directory at every path with bounded concurrency (at most
    /// `BATCH_CONCURRENCY_LIMIT` RPCs in flight).
    ///
    /// The whole batch fails on the first error.
    #[pyo3(signature = (paths, *, recursive=false))]
    fn batch_create_dir<'py>(
        &self,
        py: Python<'py>,
        paths: Vec<String>,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            use futures::stream::{self, StreamExt};
            let fs = h.fs.clone();
            stream::iter(paths.into_iter().map(move |p| {
                let fs = fs.clone();
                async move { fs.mkdir(&p, recursive).await.map_err(map_err) }
            }))
            .buffered(crate::context::BATCH_CONCURRENCY_LIMIT)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<PyResult<()>>()
        })
    }

    /// `await fs.batch_rename(pairs)` → `None`.
    ///
    /// Renames every `(src, dst)` pair with bounded concurrency (at most
    /// `BATCH_CONCURRENCY_LIMIT` RPCs in flight).
    ///
    /// `pairs` is a flat list of alternating source and destination paths:
    /// `[src_0, dst_0, src_1, dst_1, ...]`. The length must be even.
    ///
    /// The whole batch fails on the first error.
    fn batch_rename<'py>(
        &self,
        py: Python<'py>,
        pairs: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if pairs.len() % 2 != 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "pairs must have even length (flat src, dst, src, dst, ...)",
            ));
        }
        let h = self.handle()?;
        // Collect chunks into owned tuples so the inner async closure
        // does not borrow from `pairs`.
        let chunks: Vec<(String, String)> = pairs
            .chunks_exact(2)
            .map(|c| (c[0].clone(), c[1].clone()))
            .collect();
        future_into_py(py, async move {
            use futures::stream::{self, StreamExt};
            let fs = h.fs.clone();
            stream::iter(chunks.into_iter().map(move |(src, dst)| {
                let fs = fs.clone();
                async move { fs.rename(&src, &dst).await.map_err(map_err) }
            }))
            .buffered(crate::context::BATCH_CONCURRENCY_LIMIT)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<PyResult<()>>()
        })
    }

    /// `await fs.batch_delete(paths, *, recursive=False, unchecked=False, goosefs_only=False)` → `None`.
    ///
    /// Deletes every path with bounded concurrency (at most
    /// `BATCH_CONCURRENCY_LIMIT` RPCs in flight).
    ///
    /// The whole batch fails on the first error.
    #[pyo3(signature = (paths, *, recursive=false, unchecked=false, goosefs_only=false))]
    fn batch_delete<'py>(
        &self,
        py: Python<'py>,
        paths: Vec<String>,
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
            use futures::stream::{self, StreamExt};
            let fs = h.fs.clone();
            stream::iter(paths.into_iter().map(move |p| {
                let fs = fs.clone();
                let o = opts.clone();
                async move { fs.delete(&p, o).await.map_err(map_err) }
            }))
            .buffered(crate::context::BATCH_CONCURRENCY_LIMIT)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<PyResult<()>>()
        })
    }

    /// `await fs.batch_list_status(dirs, *, recursive=False)` → `list[list[URIStatus]]`.
    ///
    /// Lists each directory with bounded concurrency (at most
    /// `BATCH_CONCURRENCY_LIMIT` RPCs in flight) and returns the entries
    /// for each directory in input order as a list-of-lists.
    ///
    /// The whole batch fails on the first error.
    #[pyo3(signature = (dirs, *, recursive=false))]
    fn batch_list_status<'py>(
        &self,
        py: Python<'py>,
        dirs: Vec<String>,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            use futures::stream::{self, StreamExt};
            let fs = h.fs.clone();
            stream::iter(dirs.into_iter().map(move |d| {
                let fs = fs.clone();
                async move {
                    fs.list_status(&d, recursive)
                        .await
                        .map_err(map_err)
                        .map(|entries| {
                            entries
                                .into_iter()
                                .map(PyURIStatus::new)
                                .collect::<Vec<_>>()
                        })
                }
            }))
            .buffered(crate::context::BATCH_CONCURRENCY_LIMIT)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<PyResult<Vec<Vec<PyURIStatus>>>>()
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
            let bytes =
                goosefs_sdk::io::GoosefsFileReader::read_file_with_context(h.ctx.clone(), &path)
                    .await
                    .map_err(map_err)?;
            // Hand off to Python: `PyBytes::new` performs a single copy. We
            // could in principle use `PyBytes::new_bound_with` to populate the
            // buffer in-place, but the win is marginal and `Bytes::as_ref()`
            // already gives us a contiguous slice.
            Python::attach(|py| Ok(pyo3::types::PyBytes::new(py, bytes.as_ref()).unbind()))
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
            Python::attach(|py| Ok(pyo3::types::PyBytes::new(py, bytes.as_ref()).unbind()))
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

    // ── Worker block direct-read (P6 stage B) ───────────────────────────────

    /// `await fs.acquire_worker_for_block(block_id)` → `AsyncWorkerClient`.
    ///
    /// One-stop helper that performs the three steps every direct-block
    /// caller would otherwise have to repeat by hand:
    ///
    /// 1. Pick the responsible worker for `block_id` via the shared
    ///    `WorkerRouter` (consistent hash + local-worker preference +
    ///    failure filtering).
    /// 2. Format the worker's `host:rpc_port` address.
    /// 3. Acquire an authenticated `WorkerClient` from the shared
    ///    `WorkerClientPool` — connection reuse and single-flight reconnect
    ///    on SASL expiry come for free.
    ///
    /// The returned [`AsyncWorkerClient`] wraps the same pooled
    /// [`goosefs_sdk::client::WorkerClient`] used internally by
    /// `read_at` / streaming readers, so direct-block reads issued through
    /// this handle share TCP channels with the rest of the SDK.
    ///
    /// Closing the returned `AsyncWorkerClient` only releases the binding-
    /// level wrapper; the underlying pooled connection stays in the
    /// `FileSystemContext`'s pool for the next caller.
    fn acquire_worker_for_block<'py>(
        &self,
        py: Python<'py>,
        block_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.handle()?;
        future_into_py(py, async move {
            // 1. Route.
            let worker_info = h
                .ctx
                .acquire_router()
                .select_worker(block_id)
                .await
                .map_err(map_err)?;
            let net_addr = worker_info.address.as_ref().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("selected worker has no address")
            })?;
            let worker_addr = format_worker_addr(net_addr);

            // 2. Acquire pooled, authenticated WorkerClient.
            let client = h
                .ctx
                .acquire_worker_pool()
                .acquire(&worker_addr)
                .await
                .map_err(map_err)?;

            // 3. Wrap and hand to Python. We use `from_sdk` (not the
            //    `connect` factory) so we don't perform another TCP+SASL
            //    handshake on top of the already-pooled channel.
            Python::attach(|py| {
                let wrapper = PyAsyncWorkerClient::from_sdk(client);
                Ok(Py::new(py, wrapper)?.into_any())
            })
        })
    }

    /// `await fs.positioned_read(path, block_index=0, offset=0, length=-1, chunk_size=1<<20)` → `bytes`.
    ///
    /// High-level "Worker block direct read" — the Python equivalent of
    /// `examples/lowlevel_block_read.rs` in the Rust SDK.
    ///
    /// Steps performed internally (see also
    /// [`acquire_worker_for_block`](Self::acquire_worker_for_block)):
    ///
    /// 1. `MasterClient::get_status(path)` → resolve `URIStatus`.
    /// 2. Pick `block_ids[block_index]` (defaults to the first block).
    /// 3. Route + pool-acquire `WorkerClient` for that block.
    /// 4. [`GrpcBlockReader::positioned_read`] with `position_short = true`
    ///    → drain the stream into a single `Bytes`.
    /// 5. Single `PyBytes::new` copy across the PyO3 boundary.
    ///
    /// Arguments:
    ///   path        — Goosefs path.
    ///   block_index — which block of the file to read (0-based; default 0).
    ///   offset      — byte offset *inside the chosen block* (default 0).
    ///   length      — bytes to read; `-1` (default) reads from `offset` to
    ///                 the end of the chosen block (clamped to block size).
    ///                 For the **last block** of a file, the actual block size
    ///                 may be smaller than `block_size_bytes`, so `length=-1`
    ///                 returns only the remaining bytes of that block (which
    ///                 may be < `block_size_bytes`).
    ///   chunk_size  — gRPC chunk size, default 1 MiB. Smaller values give
    ///                 finer flow-control granularity at the cost of more
    ///                 ACK round-trips.
    ///
    /// Returns the requested byte range; may be shorter than `length` only
    /// at end-of-block.
    ///
    /// Raises:
    ///   ValueError — invalid block_index / negative offset / chunk_size <= 0.
    ///   NotFound   — `path` does not exist.
    ///   IoError / RpcError — block I/O or gRPC failures.
    #[pyo3(signature = (path, *, block_index=0, offset=0, length=-1, chunk_size=DEFAULT_CHUNK_SIZE))]
    fn positioned_read<'py>(
        &self,
        py: Python<'py>,
        path: String,
        block_index: usize,
        offset: i64,
        length: i64,
        chunk_size: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
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
        future_into_py(py, async move {
            // 1. Resolve URIStatus → block_id + block_size via shared helper.
            //    Prefers `file_block_infos` over `block_ids` for freshly-
            //    written files — see `positioned_read::resolve_block_id` docs.
            let status = h.fs.get_status(&path).await.map_err(map_err)?;
            let (block_id, block_size) = resolve_block_id(&status, block_index, &path)?;
            if offset >= block_size {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "offset={} >= block_size_bytes={}",
                    offset, block_size
                )));
            }
            // -1 ⇒ "read to end of block" (clamped at block size). The SDK
            // also clamps at block boundary, but checking up-front lets us
            // surface a clean ValueError instead of an obscure RPC error.
            //
            // Note: for the **last block** of a file the actual block size
            // may be smaller than `block_size_bytes` reported by master,
            // so `effective_length` may be larger than the real data. The
            // SDK's short-read handling returns only the available bytes.
            let effective_length = if length < 0 {
                block_size - offset
            } else {
                length.min(block_size - offset)
            };
            if effective_length == 0 {
                return Python::attach(|py| Ok(pyo3::types::PyBytes::new(py, &[]).unbind()));
            }

            // 2–4. Route + acquire + read with SASL auth-failure retry.
            //       Delegated to `positioned_read_with_reauth` so both
            //       async and sync paths share the same retry logic.
            let bytes =
                positioned_read_with_reauth(h.ctx, block_id, offset, effective_length, chunk_size)
                    .await?;

            // 5. Single copy across the PyO3 boundary.
            Python::attach(|py| Ok(pyo3::types::PyBytes::new(py, &bytes).unbind()))
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
        let taken = self.handle.lock().map(|mut g| g.take()).unwrap_or(None);
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
