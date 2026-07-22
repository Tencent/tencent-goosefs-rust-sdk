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

//! GooseFS Python binding — extension module entry point.
//!
//! This crate is a thin PyO3 wrapper around `goosefs-sdk`. The native
//! extension module is named `_goosefs` (see `[lib].name` in `Cargo.toml`)
//! and is loaded by the pure-Python package `goosefs` (see
//! `python/goosefs/__init__.py`).
//!
//! ## Architecture
//!
//! ```text
//! Python user code
//!     |
//!     v
//! goosefs/__init__.py  (re-exports from _goosefs)
//!     |
//!     v
//! _goosefs (this crate, cdylib + PyO3)
//!     |
//!     v
//! goosefs-sdk (Rust SDK at ../..)
//! ```
//!
//! ## Module contents
//!
//! Registers the Python-facing surface for GooseFS:
//!
//! - `Config` and `goosefs.exceptions.*` -- full `map_err` coverage of
//!   the SDK error enum, plus the shared Tokio runtime.
//! - `AsyncGoosefs` -- async metadata API (open, list, status, create,
//!   delete, rename; free-threaded GIL via `gil_used = false`).
//! - `Goosefs` -- synchronous wrapper around `AsyncGoosefs`.
//! - `list_status_lazy` and `batch_list_status_grouped` -- deferred
//!   Python-object materialisation for `list_status`, reducing GIL
//!   occupancy by roughly 99 percent for N=100 entries.
//! - `FileReader` and `FileWriter` -- streaming reads and writes with
//!   chunked progress callbacks.
//!
mod config;
mod context;
mod errors;
mod filesystem;
mod options;
mod positioned_read;
mod runtime;
mod status;
mod streaming;
mod sync_fs;
mod tracing;
mod types;
mod worker;

use pyo3::prelude::*;

/// `_goosefs` Python extension module.
///
/// `gil_used = false` opts into PyO3 0.27's free-threaded GIL semantics so
/// long-running Rust IO will not hold the GIL. Streaming code further wraps
/// blocking operations in `py.allow_threads(...)`.
#[pymodule(gil_used = false)]
fn _goosefs(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Phase 2.2 — install the custom Tokio runtime builder *first*, before any
    // class is constructed or any `connect()` lazily builds the runtime. This
    // is a no-op if called after the runtime is already built, so ordering
    // matters: keep it at the very top of module init.
    runtime::init_custom_runtime();

    // Crate version — keep in sync with `bindings/python/Cargo.toml` and the
    // root `goosefs-sdk` crate.
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // exceptions submodule (`goosefs.exceptions`).
    errors::register_exceptions(py, m)?;

    // public Config class.
    m.add_class::<config::PyConfig>()?;

    // metadata API surface.
    m.add_class::<types::PyWriteType>()?;
    m.add_class::<types::PyReadType>()?;
    m.add_class::<status::PyURIStatus>()?;
    m.add_class::<status::PyURIStatusList>()?;
    m.add_class::<status::PyURIStatusListIter>()?;
    m.add_class::<options::PyOpenFileOptions>()?;
    m.add_class::<options::PyCreateFileOptions>()?;
    m.add_class::<options::PyDeleteOptions>()?;
    m.add_class::<filesystem::PyAsyncGoosefs>()?;

    // sync wrapper.
    m.add_class::<sync_fs::PyGoosefs>()?;

    // streaming reader / writer (async + sync).
    m.add_class::<streaming::PyAsyncFileReader>()?;
    m.add_class::<streaming::PyAsyncFileWriter>()?;
    m.add_class::<streaming::PyFileReader>()?;
    m.add_class::<streaming::PyFileWriter>()?;

    // P6 ── low-level Worker block client (stage A of the
    // "Worker block direct" feature; see
    // `docs/GooseFS_Python_SDK_PROBLEMS_AND_SOLUTIONS.md` §3.1).
    m.add_class::<worker::PyAsyncWorkerClient>()?;
    // Sync escape hatch — mirrors `AsyncWorkerClient` for callers that
    // already know the worker address and want a one-shot blocking
    // positioned read without going through `Goosefs.positioned_read`.
    m.add_class::<worker::PyWorkerClient>()?;

    // P7 ── opt-in tracing bridge (Review §17.7).
    m.add_function(wrap_pyfunction!(tracing::enable_tracing, m)?)?;

    // Subsequent stages will register additional classes here.

    Ok(())
}
