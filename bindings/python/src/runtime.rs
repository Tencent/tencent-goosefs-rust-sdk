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
//! reactor".
//!
//! ## Sync ↔ async safety
//!
//! [`block_on`] **must not** be called from inside a Tokio worker thread
//! (e.g. from the body of a future already running on this runtime), or
//! from a Python `asyncio` event loop. The sync `Goosefs` class is
//! responsible for guarding against the latter case.

use tokio::runtime::Runtime;

/// Maximum number of blocking threads the shared Tokio runtime is sized
/// for, and the upper bound on in-flight RPCs that the `batch_*`
/// (`batch_get_status` / `batch_exists`) helpers dispatch concurrently.
///
/// **Single source of truth** — this constant is consumed by both
/// [`init_custom_runtime`] (to size `max_blocking_threads`) and by
/// [`crate::context::BATCH_CONCURRENCY_LIMIT`] (which re-exports it as the
/// `buffered(...)` window for `stream::iter(..)`). Tuning either side
/// independently is what the previous "keep aligned" comment was guarding
/// against — sharing a single value guarantees they cannot drift.
///
/// Empirically deep enough to saturate a single master while leaving
/// headroom for non-batch traffic on the same client.
pub const RUNTIME_MAX_BLOCKING_THREADS: usize = 64;

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
/// # Environment overrides (analysis
///
/// - `GOOSEFS_TOKIO_WORKER_THREADS` — override `worker_threads`. Values are
///   clamped to `>=1`. When unset, the default (`cpus.max(16)`) is used.
///   Deployments that want to **cap** the pool per §B4 (`min(cores, 8)`) can
///   set this explicitly, e.g. `GOOSEFS_TOKIO_WORKER_THREADS=8`. Left as an
///   opt-in switch (rather than a default flip) because §B4 is explicit that
///   under-sizing hurts throughput and needs per-workload benchmarking.
/// - `GOOSEFS_TOKIO_MAX_BLOCKING_THREADS` — override `max_blocking_threads`.
///   Same clamp / no-default-change semantics.
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

    // Default: at least 16 workers (see rationale above). §B4 (opt-in) lets
    // deployments override this via env var without a default flip.
    let default_worker_threads = cpus.max(16);
    let worker_threads = env_usize("GOOSEFS_TOKIO_WORKER_THREADS")
        .map(|n| n.max(1))
        .unwrap_or(default_worker_threads);
    let max_blocking_threads = env_usize("GOOSEFS_TOKIO_MAX_BLOCKING_THREADS")
        .map(|n| n.max(1))
        .unwrap_or(RUNTIME_MAX_BLOCKING_THREADS);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .enable_all();

    // `init` returns `()`; if the runtime was somehow already built (it should
    // not be at module-init time), the stored builder is simply ignored later.
    pyo3_async_runtimes::tokio::init(builder);
}

/// Parse an environment variable as `usize`, returning `None` on missing /
/// empty / unparsable values. Used by [`init_custom_runtime`] to expose the
/// runtime knobs called out in analysis.
fn env_usize(key: &str) -> Option<usize> {
    match std::env::var(key) {
        Ok(v) => {
            let trimmed = v.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<usize>().ok()
            }
        }
        Err(_) => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// the env-var override plumbing must
    /// return `None` for missing / empty / unparsable values (falling back
    /// to the built-in default), and the parsed value otherwise. Uses a
    /// process-unique key so parallel test threads do not stomp on each
    /// other.
    #[test]
    fn env_usize_missing_and_empty_return_none() {
        let key = "GOOSEFS_TEST_ENV_USIZE_MISSING_KEY_XYZ_1";
        // SAFETY: single-writer, single-reader — this key is not shared with
        // any other test in the crate.
        std::env::remove_var(key);
        assert_eq!(env_usize(key), None);

        std::env::set_var(key, "");
        assert_eq!(env_usize(key), None);

        std::env::set_var(key, "   ");
        assert_eq!(env_usize(key), None);

        std::env::set_var(key, "not-a-number");
        assert_eq!(env_usize(key), None);

        std::env::remove_var(key);
    }

    #[test]
    fn env_usize_parses_valid_values() {
        let key = "GOOSEFS_TEST_ENV_USIZE_VALID_KEY_XYZ_2";
        std::env::set_var(key, "8");
        assert_eq!(env_usize(key), Some(8));

        std::env::set_var(key, "  16  ");
        assert_eq!(env_usize(key), Some(16));

        std::env::remove_var(key);
    }
}
