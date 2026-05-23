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
//! ## Roadmap (per docs/PYTHON_BINDING_PROGRESS.md)
//!
//! - P0 — project skeleton: empty `_goosefs` module that exposes only
//!   `__version__` so `import goosefs` succeeds.
//! - **P1 — config + exceptions (this commit)**: registers `Config`,
//!   `goosefs.exceptions.*` with full `map_err` coverage of the SDK error
//!   enum, and the shared Tokio runtime.
//! - P2 — async metadata API (`AsyncGoosefs`).
//! - P3 — sync wrapper (`Goosefs`).
//! - P4 — high-level `read_file` / `write_file` / `read_range`.
//! - P5 — streaming `FileReader` / `FileWriter`.

mod config;
mod context;
mod errors;
mod filesystem;
mod options;
mod runtime;
mod status;
mod streaming;
mod sync_fs;
mod tracing;
mod types;

use pyo3::prelude::*;

/// `_goosefs` Python extension module.
///
/// `gil_used = false` opts into PyO3 0.27's free-threaded GIL semantics so
/// long-running Rust IO will not hold the GIL. P5 streaming code will further
/// wrap blocking operations in `py.allow_threads(...)`.
#[pymodule(gil_used = false)]
fn _goosefs(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Crate version — keep in sync with `bindings/python/Cargo.toml` and the
    // root `goosefs-sdk` crate. CI will enforce this in P8.
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // P1 ── exceptions submodule (`goosefs.exceptions`).
    errors::register_exceptions(py, m)?;

    // P1 ── public Config class.
    m.add_class::<config::PyConfig>()?;

    // P2 ── metadata API surface.
    m.add_class::<types::PyWriteType>()?;
    m.add_class::<types::PyReadType>()?;
    m.add_class::<status::PyURIStatus>()?;
    m.add_class::<options::PyOpenFileOptions>()?;
    m.add_class::<options::PyCreateFileOptions>()?;
    m.add_class::<options::PyDeleteOptions>()?;
    m.add_class::<filesystem::PyAsyncGoosefs>()?;

    // P3 ── sync wrapper.
    m.add_class::<sync_fs::PyGoosefs>()?;

    // P5 ── streaming reader / writer (async + sync).
    m.add_class::<streaming::PyAsyncFileReader>()?;
    m.add_class::<streaming::PyAsyncFileWriter>()?;
    m.add_class::<streaming::PyFileReader>()?;
    m.add_class::<streaming::PyFileWriter>()?;

    // P7 ── opt-in tracing bridge (Review §17.7).
    m.add_function(wrap_pyfunction!(tracing::enable_tracing, m)?)?;

    // Subsequent stages will register additional classes here.

    Ok(())
}
