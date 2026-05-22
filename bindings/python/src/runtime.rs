//! Shared Tokio runtime for the GooseFS Python binding.
//!
//! Async PyO3 methods use [`pyo3_async_runtimes::tokio::future_into_py`]
//! which already drives futures on a runtime managed by `pyo3-async-runtimes`.
//! For sync methods we expose [`block_on`] which obtains the very same
//! runtime via [`pyo3_async_runtimes::tokio::get_runtime`] — this avoids
//! creating two competing runtimes in the same process.
//!
//! ## Why no `OnceLock` fallback?
//!
//! An earlier draft kept `use std::sync::OnceLock` here as a "fallback"
//! runtime, but `pyo3_async_runtimes::tokio::get_runtime()` is itself
//! lazily initialised behind a `OnceCell`, never panics, and is what the
//! async side already uses. Adding a second runtime would silently break
//! the invariant "everything Python touches runs on the same Tokio
//! reactor". See PYTHON_BINDING_PROGRESS.md §17.2.
//!
//! ## Sync ↔ async safety
//!
//! [`block_on`] **must not** be called from inside a Tokio worker thread
//! (e.g. from the body of a future already running on this runtime), or
//! from a Python `asyncio` event loop. The sync `Goosefs` class is
//! responsible for guarding against the latter case (see P3 §17.1).

use tokio::runtime::Runtime;

/// Returns the process-wide Tokio runtime that backs both async and sync
/// GooseFS APIs.
///
/// This is just a thin alias over [`pyo3_async_runtimes::tokio::get_runtime`]
/// kept here so the rest of the binding does not have to depend on the
/// exact spelling of that crate path.
//
// Allowed because the first call site lands in P2 (`AsyncGoosefs::connect`).
#[allow(dead_code)]
#[inline]
pub fn runtime() -> &'static Runtime {
    pyo3_async_runtimes::tokio::get_runtime()
}

/// Block the current thread until `fut` completes, driving it on the shared
/// runtime.
///
/// Used by the synchronous `Goosefs` class. The async `AsyncGoosefs` class
/// does not call this — it returns Python coroutines directly.
//
// Allowed because the first call site lands in P3 (sync `Goosefs::__new__`).
#[allow(dead_code)]
#[inline]
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    runtime().block_on(fut)
}
