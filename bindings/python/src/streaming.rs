//! Streaming file I/O for Python — `AsyncFileReader` / `AsyncFileWriter`
//! and their synchronous counterparts `FileReader` / `FileWriter`.
//!
//! The async classes wrap [`goosefs_sdk::io::GoosefsFileInStream`] (reader)
//! and [`goosefs_sdk::io::GoosefsFileWriter`] (writer). The sync classes
//! reuse the same async cores via [`crate::sync_fs::PyGoosefs::guarded_block_on`]-equivalent
//! helpers, so the same deadlock and fork-safety guards from P3 apply.
//!
//! ## Concurrency model — Review §17.1
//!
//! The SDK's reader/writer methods take `&mut self`, which means concurrent
//! `await`s on the same Python object would race in Rust. We model this by
//! storing the inner state behind a [`tokio::sync::Mutex`] and acquiring it
//! at the start of each method. `tokio::sync::Mutex` is mandatory here:
//! a `std::sync::Mutex` would dead-lock the Tokio scheduler if the same task
//! tried to re-enter, and (more importantly) would force us to hold a
//! `MutexGuard` across `await` points which is unsound on a multi-threaded
//! runtime that can move tasks across worker threads.
//!
//! Two corollaries:
//!
//! 1. **Single-task discipline.** A reader/writer is *not* designed to be
//!    used concurrently from multiple coroutines on the same instance —
//!    `tokio::sync::Mutex` will serialise such calls, but the underlying
//!    `GoosefsFileInStream` advances `pos` monotonically and is logically
//!    single-threaded (matches the Java client contract).
//! 2. **Lock release on close / drop.** `close()` takes the inner state out
//!    of the mutex (`Option::take`) so subsequent calls fail fast with
//!    `RuntimeError`; the SDK writer's `Drop` impl handles the
//!    "forgot to close" case with a warning log.
//!
//! ## Bytes handling
//!
//! All read methods produce `bytes` via `PyBytes::new` (one copy from the
//! SDK's `Bytes` / `Vec<u8>` into Python-owned memory). All write methods
//! accept any buffer-protocol object (`bytes`, `bytearray`, `memoryview`,
//! `array.array("B", …)`, NumPy `uint8`) through
//! [`crate::filesystem::extract_bytes_like`], which also rejects `str` to
//! avoid silent Latin-1 round-trips.
//!
//! ## Read-until-filled helper
//!
//! The SDK's [`GoosefsFileInStream::read`] is loss-less but may return
//! fewer bytes than requested in a single call (each call corresponds to
//! at most one worker chunk plus any carry-over from a previous oversized
//! chunk). Python callers expect `r.read(n)` to return up to `n` bytes
//! — short reads are legal but inconvenient when streaming through a
//! tight loop. We therefore wrap the SDK in a tiny [`pull_n`] helper that
//! re-enters `stream.read` until the requested length is satisfied or the
//! stream hits EOF. Zero band-aid: the helper relies on the SDK's own
//! correctness guarantees for byte preservation, ordering, and EOF
//! detection.

use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex as AsyncMutex;

use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};

use crate::errors::map_err;
use crate::filesystem::{build_create_file_options, extract_bytes_like};
use crate::runtime::block_on;

// ─────────────────────────────────────────────────────────────────────────────
// Read helpers — thin wrappers over the SDK that satisfy Python's
// `read(n)` "return up to n bytes" expectation by looping over the
// SDK's chunk-bounded reads. The SDK guarantees byte preservation, so
// these helpers carry no state of their own.
// ─────────────────────────────────────────────────────────────────────────────

/// Read up to `want` bytes from `stream`, looping over the SDK's
/// chunk-bounded `read()` until either `want` bytes have been collected
/// or EOF is reached. May return fewer than `want` bytes only at EOF.
///
/// Implementation notes:
/// - We size the scratch buffer to the *remaining* file length capped
///   by `want`, so each loop turn requests as much as the caller still
///   wants — the SDK trims internally to its own chunk boundary.
/// - A zero-byte return from the SDK means the underlying chunk reader
///   reached EOF; we break unconditionally to avoid spinning.
#[allow(dead_code)]
async fn pull_n(stream: &mut GoosefsFileInStream, want: usize) -> PyResult<Vec<u8>> {
    if want == 0 {
        return Ok(Vec::new());
    }
    // Optimization (Phase 1.1): allocate the destination buffer once and read
    // straight into it. The SDK's `read` writes into the provided slice, so
    // each loop turn fills `out[filled..]` in place — no per-iteration `tmp`
    // allocation and no `extend_from_slice` copy. This removes O(N) extra
    // allocations + memcpys for short-read loops.
    let mut out = vec![0u8; want];
    let mut filled = 0;
    while filled < want {
        let n = stream.read(&mut out[filled..]).await.map_err(map_err)?;
        if n == 0 {
            break; // EOF
        }
        filled += n;
    }
    out.truncate(filled);
    Ok(out)
}

/// Read every remaining byte from the current position to EOF.
///
/// Returns the SDK's `Bytes` directly (Phase 1.2): the caller constructs
/// `PyBytes` from `as_ref()`, so there is a single copy into Python-owned
/// memory instead of the previous `Bytes -> Vec<u8> -> PyBytes` double copy.
async fn pull_all(stream: &mut GoosefsFileInStream) -> PyResult<bytes::Bytes> {
    stream.read_all().await.map_err(map_err)
}

/// Prefetch hint: when the reader's prefetch buffer is empty, pull at least
/// this many bytes from the SDK in one shot so subsequent small `read(n)`
/// calls are served from the buffer (Part V P1).
const PREFETCH_HINT_BYTES: usize = 1 << 20; // 1 MiB

/// Pull `max(want, PREFETCH_HINT_BYTES)` bytes (or until EOF) from `stream`,
/// returning `(head, remainder)` where `head` is the first `want` bytes to
/// hand back to the caller and `remainder` is parked in the reader's prefetch
/// buffer for the next call.
///
/// `bytes::Bytes::split_to` is an O(1) refcount split — the remainder shares
/// the same allocation, so subsequent buffered reads slice with zero copy
/// (only the final `PyBytes::new` copies into Python heap).
///
/// Short reads are legal (Python `read(n)` may return ≤ n): when the stream
/// hits EOF before `want`, `head` is simply shorter than requested.
async fn pull_prefetched(
    stream: &mut GoosefsFileInStream,
    want: usize,
) -> PyResult<(bytes::Bytes, bytes::Bytes)> {
    let target = want.max(PREFETCH_HINT_BYTES);
    let mut out = vec![0u8; target];
    let mut filled = 0;
    while filled < target {
        let n = stream.read(&mut out[filled..]).await.map_err(map_err)?;
        if n == 0 {
            break; // EOF
        }
        filled += n;
    }
    out.truncate(filled);
    let mut buf = bytes::Bytes::from(out);
    let take = want.min(buf.len());
    let head = buf.split_to(take);
    Ok((head, buf))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers shared with sync_fs
// ─────────────────────────────────────────────────────────────────────────────

/// Refuse to run a sync streaming method from inside an asyncio loop or
/// a Tokio runtime — same defence as `PyGoosefs::guarded_block_on`.
///
/// Lifted as a free function so both `FileReader` and `FileWriter` can
/// share it without going through the `Goosefs` instance.
fn guarded_block_on<F, T>(py: Python<'_>, fut: F) -> PyResult<T>
where
    F: std::future::Future<Output = PyResult<T>> + Send,
    T: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(PyRuntimeError::new_err(
            "sync streaming methods cannot be invoked from inside a Tokio runtime; \
             use `AsyncFileReader` / `AsyncFileWriter` from your async code instead",
        ));
    }
    let asyncio = py.import("asyncio")?;
    if asyncio.call_method0("get_running_loop").is_ok() {
        return Err(PyRuntimeError::new_err(
            "sync streaming methods cannot be invoked from inside an asyncio event loop; \
             use the async streaming classes and `await` instead",
        ));
    }
    py.detach(|| block_on(fut))
}

// ─────────────────────────────────────────────────────────────────────────────
// AsyncFileReader
// ─────────────────────────────────────────────────────────────────────────────

/// Coroutine-returning seekable file reader.
///
/// Wraps [`goosefs_sdk::io::GoosefsFileInStream`]. The inner stream is
/// `&mut self` in the SDK, so we serialise access through a
/// [`tokio::sync::Mutex`]. Every method takes the lock for the duration
/// of the call; concurrent calls on the same `AsyncFileReader` will be
/// queued, mirroring the Java client's single-threaded contract.
///
/// ```python
/// async with await fs.open_file("/data/file.parquet") as r:
///     hdr  = await r.read(64)
///     await r.seek(1024)
///     mid  = await r.read(4096)
///     tail = await r.read_at(0, 32)   # positioned read, no seek
/// ```
#[pyclass(module = "goosefs._goosefs", name = "AsyncFileReader")]
pub struct PyAsyncFileReader {
    inner: Arc<AsyncMutex<Option<GoosefsFileInStream>>>,
    /// Cached file length so `__len__` / `tell` do not need to acquire
    /// the mutex (and hence cannot deadlock with an in-flight read).
    file_length: i64,
    /// Sequential-read prefetch buffer (Part V P1).
    ///
    /// Holds bytes already pulled from the SDK but not yet delivered to the
    /// caller. Anchored on the logical (user-visible) position: `tell()` is
    /// `stream.pos() - prefetch.len()`. Only ever mutated while the `inner`
    /// `AsyncMutex` guard is held, so it stays consistent with the stream
    /// position under the single-task contract. Cleared on `seek` / `close`.
    /// Bypassed entirely by `read_at` (positioned reads do not move `pos`).
    prefetch: Arc<std::sync::Mutex<bytes::Bytes>>,
}

impl PyAsyncFileReader {
    /// Build a fresh reader from an already-opened SDK stream.
    pub(crate) fn from_sdk(stream: GoosefsFileInStream) -> Self {
        let file_length = stream.len();
        Self {
            inner: Arc::new(AsyncMutex::new(Some(stream))),
            file_length,
            prefetch: Arc::new(std::sync::Mutex::new(bytes::Bytes::new())),
        }
    }
}

#[pymethods]
impl PyAsyncFileReader {
    /// `await reader.read(size=-1)` → `bytes`.
    ///
    /// `size < 0` (the default) means "read all remaining bytes". `size = 0`
    /// returns `b""`.
    #[pyo3(signature = (size=-1))]
    fn read<'py>(&self, py: Python<'py>, size: i64) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let prefetch = Arc::clone(&self.prefetch);
        future_into_py(py, async move {
            // Hold the stream guard for the whole op so the prefetch buffer
            // stays serialized with the SDK position (single-task contract).
            let mut guard = inner.lock().await;
            let stream = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("AsyncFileReader is closed"))?;

            if size < 0 {
                // read-all: drain the prefetch buffer first, then the stream.
                let parked = {
                    let mut pf = prefetch.lock().unwrap();
                    std::mem::take(&mut *pf)
                };
                let rest = pull_all(stream).await?;
                let out: bytes::Bytes = if parked.is_empty() {
                    rest
                } else {
                    let mut merged = bytes::BytesMut::with_capacity(parked.len() + rest.len());
                    merged.extend_from_slice(&parked);
                    merged.extend_from_slice(&rest);
                    merged.freeze()
                };
                return Python::attach(|py| Ok(PyBytes::new(py, out.as_ref()).unbind()));
            }

            let want = size as usize;
            // Fast path: serve from the prefetch buffer (zero-copy slice).
            {
                let mut pf = prefetch.lock().unwrap();
                if !pf.is_empty() {
                    let take = want.min(pf.len());
                    let head = pf.split_to(take);
                    return Python::attach(|py| Ok(PyBytes::new(py, head.as_ref()).unbind()));
                }
            }
            // Slow path: pull a large block, hand back `want`, park the rest.
            let (head, remainder) = pull_prefetched(stream, want).await?;
            {
                *prefetch.lock().unwrap() = remainder;
            }
            Python::attach(|py| Ok(PyBytes::new(py, head.as_ref()).unbind()))
        })
    }

    /// `await reader.read_at(offset, length)` → `bytes`.
    ///
    /// Positioned read: does **not** modify the stream's logical position.
    /// Routed through the SDK's `positioned_read` path under the hood.
    fn read_at<'py>(
        &self,
        py: Python<'py>,
        offset: i64,
        length: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        if offset < 0 {
            return Err(PyValueError::new_err("offset must be non-negative"));
        }
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let stream = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("AsyncFileReader is closed"))?;
            let bytes = stream.read_at(offset, length).await.map_err(map_err)?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    /// `await reader.seek(offset, whence=0)` → new absolute position.
    #[pyo3(signature = (offset, whence=0))]
    fn seek<'py>(&self, py: Python<'py>, offset: i64, whence: i32) -> PyResult<Bound<'py, PyAny>> {
        // Validate `whence` up front (the offset sign check for SEEK_SET is
        // re-done after we resolve the absolute target).
        if !(0..=2).contains(&whence) {
            return Err(PyValueError::new_err(format!(
                "invalid whence value: {whence} (expected 0, 1, or 2)"
            )));
        }
        let inner = Arc::clone(&self.inner);
        let prefetch = Arc::clone(&self.prefetch);
        let file_length = self.file_length;
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let stream = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("AsyncFileReader is closed"))?;

            // Clear the prefetch buffer (anchored on the old position) and
            // recover its length so a relative seek resolves against the
            // *logical* user position, not the prefetch-advanced stream pos.
            let parked_len = {
                let mut pf = prefetch.lock().unwrap();
                let len = pf.len() as i64;
                *pf = bytes::Bytes::new();
                len
            };
            let logical_pos = stream.pos() - parked_len;
            let target = match whence {
                0 => {
                    if offset < 0 {
                        return Err(PyValueError::new_err(
                            "negative seek offset is invalid for whence=0 (SEEK_SET)",
                        ));
                    }
                    offset
                }
                1 => logical_pos + offset,
                _ => file_length + offset, // whence == 2 (SEEK_END)
            };
            let pos = stream.seek(target).await.map_err(map_err)?;
            Ok(pos)
        })
    }

    /// `reader.tell()` → current byte position (sync, no I/O).
    ///
    /// Returns the **logical** position: the SDK stream position minus any
    /// bytes still parked in the prefetch buffer (Part V P1 / A4).
    /// Implemented by acquiring the mutex with `try_lock()` so we don't
    /// block the caller — if a concurrent read is in flight we surface
    /// `RuntimeError` rather than silently waiting.
    fn tell(&self) -> PyResult<i64> {
        match self.inner.try_lock() {
            Ok(guard) => match guard.as_ref() {
                Some(s) => {
                    let parked = self.prefetch.lock().unwrap().len() as i64;
                    Ok(s.pos() - parked)
                }
                None => Err(PyRuntimeError::new_err("AsyncFileReader is closed")),
            },
            Err(_) => Err(PyRuntimeError::new_err(
                "tell() while another read/seek is in flight; await the in-flight call first",
            )),
        }
    }

    /// `len(reader)` → total file length in bytes.
    fn __len__(&self) -> PyResult<usize> {
        if self.file_length < 0 {
            return Err(PyRuntimeError::new_err(
                "file length is negative — corrupt status",
            ));
        }
        Ok(self.file_length as usize)
    }

    /// `await reader.close()` — release the underlying stream.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let prefetch = Arc::clone(&self.prefetch);
        future_into_py(py, async move {
            // Take the inner stream out, then drop it explicitly — the
            // SDK does its cleanup in `Drop`. We don't surface errors:
            // closing a reader is best-effort.
            let mut guard = inner.lock().await;
            let _ = guard.take();
            // Drop any parked prefetch bytes (Part V P1 / A4).
            *prefetch.lock().unwrap() = bytes::Bytes::new();
            Ok(())
        })
    }

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
        format!("AsyncFileReader(length={})", self.file_length)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AsyncFileWriter
// ─────────────────────────────────────────────────────────────────────────────

/// Coroutine-returning streaming file writer.
///
/// Wraps [`goosefs_sdk::io::GoosefsFileWriter`]. Same locking model as
/// `AsyncFileReader`. The user **must** call `close()` (or use
/// `async with`) to finalise the file — otherwise the SDK will emit a
/// warning log on drop and the file will be left in an incomplete state.
#[pyclass(module = "goosefs._goosefs", name = "AsyncFileWriter")]
pub struct PyAsyncFileWriter {
    inner: Arc<AsyncMutex<Option<GoosefsFileWriter>>>,
    path: String,
}

impl PyAsyncFileWriter {
    pub(crate) fn from_sdk(writer: GoosefsFileWriter, path: String) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(Some(writer))),
            path,
        }
    }
}

#[pymethods]
impl PyAsyncFileWriter {
    /// `await writer.write(data)` → number of bytes accepted.
    ///
    /// Accepts any buffer-protocol object except `str` (see
    /// [`crate::filesystem::extract_bytes_like`]).
    fn write<'py>(&self, py: Python<'py>, data: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let payload = extract_bytes_like(data)?;
        let n = payload.len();
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let writer = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("AsyncFileWriter is closed"))?;
            writer.write(&payload).await.map_err(map_err)?;
            Ok(n)
        })
    }

    /// `await writer.close()` — finalise the file. Idempotent.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            if let Some(mut writer) = guard.take() {
                writer.close().await.map_err(map_err)?;
            }
            Ok(())
        })
    }

    /// `await writer.cancel()` — abandon all uncommitted state and let
    /// the master discard the (incomplete) file. Idempotent.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            if let Some(mut writer) = guard.take() {
                writer.cancel().await.map_err(map_err)?;
            }
            Ok(())
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let py_obj = slf.into_pyobject(py)?.into_any().unbind();
        future_into_py(py, async move { Ok(py_obj) })
    }

    /// On unhandled exception inside the `async with` block we
    /// **cancel** instead of close, so the half-written file is not
    /// committed. Matches Java's try-with-resources convention.
    #[pyo3(signature = (exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if exc_type.is_some() {
            self.cancel(py)
        } else {
            self.close(py)
        }
    }

    fn __repr__(&self) -> String {
        format!("AsyncFileWriter(path={:?})", self.path)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FileReader (sync wrapper)
// ─────────────────────────────────────────────────────────────────────────────

/// Synchronous seekable file reader.
///
/// Mirror of `AsyncFileReader`. Each method runs on the shared Tokio
/// runtime via [`block_on`], with the GIL released. The same deadlock
/// guard from P3 applies (see [`guarded_block_on`]).
#[pyclass(module = "goosefs._goosefs", name = "FileReader")]
pub struct PyFileReader {
    // We keep the same `Arc<AsyncMutex<…>>` shape so a future evolution
    // could share state with the async type. For now access is always
    // serialised by the GIL since sync methods never yield.
    inner: Arc<AsyncMutex<Option<GoosefsFileInStream>>>,
    file_length: i64,
    /// Sequential-read prefetch buffer (Part V P1). Because sync methods are
    /// serialised by the GIL, buffered slices are served **without** entering
    /// the Tokio runtime (`guarded_block_on`), eliminating the per-`read`
    /// runtime-scheduling cost that dominates SR-64K. See A4 for the
    /// read/read_at/seek/tell/close interaction rules.
    prefetch: Arc<std::sync::Mutex<bytes::Bytes>>,
}

impl PyFileReader {
    pub(crate) fn from_sdk(stream: GoosefsFileInStream) -> Self {
        let file_length = stream.len();
        Self {
            inner: Arc::new(AsyncMutex::new(Some(stream))),
            file_length,
            prefetch: Arc::new(std::sync::Mutex::new(bytes::Bytes::new())),
        }
    }
}

#[pymethods]
impl PyFileReader {
    #[pyo3(signature = (size=-1))]
    fn read<'py>(&self, py: Python<'py>, size: i64) -> PyResult<Bound<'py, PyBytes>> {
        // Fast path (Part V P1): serve a buffered slice WITHOUT entering the
        // Tokio runtime. Sync methods are GIL-serialised, so the prefetch
        // buffer is safe to touch directly here. This is the path that fires
        // ~15 out of every 16 small `read(64k)` calls on a sequential scan,
        // cutting the per-call `block_on` scheduling cost.
        if size >= 0 {
            let want = size as usize;
            let mut pf = self.prefetch.lock().unwrap();
            if !pf.is_empty() {
                let take = want.min(pf.len());
                let head = pf.split_to(take);
                return Ok(PyBytes::new(py, head.as_ref()));
            }
        }

        let inner = Arc::clone(&self.inner);
        let prefetch = Arc::clone(&self.prefetch);
        if size < 0 {
            // read-all: drain the prefetch buffer first, then the stream.
            let parked = {
                let mut pf = self.prefetch.lock().unwrap();
                std::mem::take(&mut *pf)
            };
            let bytes: bytes::Bytes = guarded_block_on(py, async move {
                let mut guard = inner.lock().await;
                let stream = guard
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("FileReader is closed"))?;
                let rest = pull_all(stream).await?;
                if parked.is_empty() {
                    Ok(rest)
                } else {
                    let mut merged = bytes::BytesMut::with_capacity(parked.len() + rest.len());
                    merged.extend_from_slice(&parked);
                    merged.extend_from_slice(&rest);
                    Ok(merged.freeze())
                }
            })?;
            return Ok(PyBytes::new(py, bytes.as_ref()));
        }

        // Slow path: prefetch buffer empty — pull a large block, hand back
        // `size`, park the rest for subsequent fast-path reads.
        let want = size as usize;
        let (head, remainder): (bytes::Bytes, bytes::Bytes) = guarded_block_on(py, async move {
            let mut guard = inner.lock().await;
            let stream = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("FileReader is closed"))?;
            pull_prefetched(stream, want).await
        })?;
        {
            *prefetch.lock().unwrap() = remainder;
        }
        Ok(PyBytes::new(py, head.as_ref()))
    }

    fn read_at<'py>(
        &self,
        py: Python<'py>,
        offset: i64,
        length: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        if offset < 0 {
            return Err(PyValueError::new_err("offset must be non-negative"));
        }
        let inner = Arc::clone(&self.inner);
        // Return the SDK's `Bytes` directly (Phase 1.3) and build `PyBytes`
        // once via `as_ref()`, dropping the previous `to_vec()` intermediate.
        let bytes: bytes::Bytes = guarded_block_on(py, async move {
            let mut guard = inner.lock().await;
            let stream = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("FileReader is closed"))?;
            stream.read_at(offset, length).await.map_err(map_err)
        })?;
        Ok(PyBytes::new(py, bytes.as_ref()))
    }

    #[pyo3(signature = (offset, whence=0))]
    fn seek(&self, py: Python<'_>, offset: i64, whence: i32) -> PyResult<i64> {
        if !(0..=2).contains(&whence) {
            return Err(PyValueError::new_err(format!(
                "invalid whence value: {whence} (expected 0, 1, or 2)"
            )));
        }
        // Clear the prefetch buffer and convert a relative seek to an
        // absolute logical target (Part V P1 / A4).
        let parked_len = {
            let mut pf = self.prefetch.lock().unwrap();
            let len = pf.len() as i64;
            *pf = bytes::Bytes::new();
            len
        };
        let inner = Arc::clone(&self.inner);
        let file_length = self.file_length;
        guarded_block_on(py, async move {
            let mut guard = inner.lock().await;
            let stream = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("FileReader is closed"))?;
            let logical_pos = stream.pos() - parked_len;
            let target = match whence {
                0 => {
                    if offset < 0 {
                        return Err(PyValueError::new_err(
                            "negative seek offset is invalid for whence=0 (SEEK_SET)",
                        ));
                    }
                    offset
                }
                1 => logical_pos + offset,
                _ => file_length + offset, // whence == 2 (SEEK_END)
            };
            stream.seek(target).await.map_err(map_err)
        })
    }

    fn tell(&self) -> PyResult<i64> {
        match self.inner.try_lock() {
            Ok(guard) => match guard.as_ref() {
                Some(s) => {
                    let parked = self.prefetch.lock().unwrap().len() as i64;
                    Ok(s.pos() - parked)
                }
                None => Err(PyRuntimeError::new_err("FileReader is closed")),
            },
            Err(_) => Err(PyRuntimeError::new_err(
                "tell() while another op is in flight",
            )),
        }
    }

    fn __len__(&self) -> PyResult<usize> {
        if self.file_length < 0 {
            return Err(PyRuntimeError::new_err("file length is negative"));
        }
        Ok(self.file_length as usize)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        // Drop any parked prefetch bytes (Part V P1 / A4).
        *self.prefetch.lock().unwrap() = bytes::Bytes::new();
        let inner = Arc::clone(&self.inner);
        guarded_block_on(py, async move {
            let mut guard = inner.lock().await;
            let _ = guard.take();
            Ok(())
        })
    }

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
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!("FileReader(length={})", self.file_length)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FileWriter (sync wrapper)
// ─────────────────────────────────────────────────────────────────────────────

#[pyclass(module = "goosefs._goosefs", name = "FileWriter")]
pub struct PyFileWriter {
    inner: Arc<AsyncMutex<Option<GoosefsFileWriter>>>,
    path: String,
}

impl PyFileWriter {
    pub(crate) fn from_sdk(writer: GoosefsFileWriter, path: String) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(Some(writer))),
            path,
        }
    }
}

#[pymethods]
impl PyFileWriter {
    fn write(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<usize> {
        let payload = extract_bytes_like(data)?;
        let n = payload.len();
        let inner = Arc::clone(&self.inner);
        guarded_block_on(py, async move {
            let mut guard = inner.lock().await;
            let writer = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("FileWriter is closed"))?;
            writer.write(&payload).await.map_err(map_err)?;
            Ok(n)
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        guarded_block_on(py, async move {
            let mut guard = inner.lock().await;
            if let Some(mut writer) = guard.take() {
                writer.close().await.map_err(map_err)?;
            }
            Ok(())
        })
    }

    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        guarded_block_on(py, async move {
            let mut guard = inner.lock().await;
            if let Some(mut writer) = guard.take() {
                writer.cancel().await.map_err(map_err)?;
            }
            Ok(())
        })
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// On unhandled exception in `with` block, cancel instead of close.
    #[pyo3(signature = (exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<Bound<'_, PyType>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        if exc_type.is_some() {
            self.cancel(py)?;
        } else {
            self.close(py)?;
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!("FileWriter(path={:?})", self.path)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory helpers shared by AsyncGoosefs / Goosefs
// ─────────────────────────────────────────────────────────────────────────────

/// Open a `GoosefsFileInStream` on the shared context — used by both the
/// async (`AsyncGoosefs::open_file`) and sync (`Goosefs::open_file`)
/// front-doors to keep the SDK call site centralised.
pub(crate) async fn sdk_open_in_stream(
    ctx: Arc<FileSystemContext>,
    path: String,
) -> PyResult<GoosefsFileInStream> {
    GoosefsFileInStream::open_with_context(ctx, &path, OpenFileOptions::default())
        .await
        .map_err(map_err)
}

/// Create a new `GoosefsFileWriter` on the shared context.
pub(crate) async fn sdk_create_writer(
    ctx: Arc<FileSystemContext>,
    path: String,
    write_type: Option<crate::types::PyWriteType>,
    block_size_bytes: Option<i64>,
    recursive: bool,
) -> PyResult<GoosefsFileWriter> {
    let proto_opts = build_create_file_options(write_type, block_size_bytes, recursive);
    GoosefsFileWriter::create_with_context(ctx, &path, proto_opts)
        .await
        .map_err(map_err)
}
