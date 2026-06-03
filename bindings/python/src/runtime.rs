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

/// Install a custom multi-thread Tokio runtime builder (Phase 2.2).
///
/// `pyo3_async_runtimes::tokio::init` only *stores* the builder; the runtime
/// itself is built lazily on the first `get_runtime()` call. We therefore must
/// call this from the module-init function (`_goosefs`), before any
/// `connect()` / `block_on()` ever touches the runtime — otherwise the default
/// builder wins and `init` is a silent no-op.
///
/// Tuning rationale (analysis §2.5 / scheme 4):
/// - `worker_threads`: at least 16 so that highly-concurrent Worker IO
///   (streaming read/write) is not starved by a worker count pinned to the
///   CPU core count. Capped generously to avoid pathological oversubscription.
/// - `max_blocking_threads`: bumped to 64 so blocking SDK hops (DNS, filesystem
///   fallbacks) do not queue behind one another under load.
///
/// Note: this only helps IO tasks that have already released the GIL; it does
/// **not** lift the GIL-serialisation ceiling on short Master-read ops.
pub fn init_custom_runtime() {
    // `available_parallelism` is the std replacement for `num_cpus::get`; it
    // returns the count of logical CPUs visible to the process (respects
    // cgroup limits on Linux). Fall back to 16 if the platform cannot report.
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(16);
    let worker_threads = cpus.max(16);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(worker_threads)
        .max_blocking_threads(64)
        .enable_all();

    // `init` returns `()`; if the runtime was somehow already built (it should
    // not be at module-init time), the stored builder is simply ignored later.
    pyo3_async_runtimes::tokio::init(builder);
}

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
