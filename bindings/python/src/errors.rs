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

//! Python exception hierarchy for GooseFS errors.
//!
//! Mirrors the design of OpenDAL's Python binding: a single `GoosefsError`
//! base class with one subclass per *category* of failure so users can write
//!
//! ```python
//! try:
//!     fs.get_status("/missing")
//! except NotFound:
//!     ...
//! except GoosefsError:           # catch-all
//!     ...
//! ```
//!
//! Every variant of `goosefs_sdk::error::Error` is mapped explicitly by
//! [`map_err`] — no variant falls through silently. See Review §17.1.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Exception types — registered under `goosefs.exceptions`.
//
// `create_exception!` generates a unit struct + a `PyErr::new::<T>(...)`
// helper. The first argument is the *module path string* used in the type's
// `__module__` attribute; we use `goosefs.exceptions` so users see
// `goosefs.exceptions.NotFound` in tracebacks rather than the underscore-
// prefixed `_goosefs.exceptions.NotFound`.
// ---------------------------------------------------------------------------
create_exception!(goosefs.exceptions, GoosefsError, PyException);
create_exception!(goosefs.exceptions, NotFound, GoosefsError);
create_exception!(goosefs.exceptions, AlreadyExists, GoosefsError);
create_exception!(goosefs.exceptions, PermissionDenied, GoosefsError);
create_exception!(goosefs.exceptions, InvalidArgument, GoosefsError);
create_exception!(goosefs.exceptions, FileIncomplete, GoosefsError);
create_exception!(goosefs.exceptions, DirectoryNotEmpty, GoosefsError);
create_exception!(goosefs.exceptions, IsADirectory, GoosefsError);
create_exception!(goosefs.exceptions, AuthenticationFailed, GoosefsError);
create_exception!(goosefs.exceptions, NoWorkerAvailable, GoosefsError);
create_exception!(goosefs.exceptions, MasterUnavailable, GoosefsError);
create_exception!(goosefs.exceptions, RpcError, GoosefsError);
// `IoError` covers SDK `BlockIoError` — Review §17.8 made this its own class
// so callers can distinguish transient block I/O failures from generic ones.
create_exception!(goosefs.exceptions, IoError, GoosefsError);
create_exception!(goosefs.exceptions, ConfigError, GoosefsError);

/// Register all exception classes under the `goosefs.exceptions` submodule.
///
/// Called from `_goosefs::_goosefs(py, m)` at module init.
pub fn register_exceptions(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new(py, "exceptions")?;

    sub.add("GoosefsError", py.get_type::<GoosefsError>())?;
    sub.add("NotFound", py.get_type::<NotFound>())?;
    sub.add("AlreadyExists", py.get_type::<AlreadyExists>())?;
    sub.add("PermissionDenied", py.get_type::<PermissionDenied>())?;
    sub.add("InvalidArgument", py.get_type::<InvalidArgument>())?;
    sub.add("FileIncomplete", py.get_type::<FileIncomplete>())?;
    sub.add("DirectoryNotEmpty", py.get_type::<DirectoryNotEmpty>())?;
    sub.add("IsADirectory", py.get_type::<IsADirectory>())?;
    sub.add(
        "AuthenticationFailed",
        py.get_type::<AuthenticationFailed>(),
    )?;
    sub.add("NoWorkerAvailable", py.get_type::<NoWorkerAvailable>())?;
    sub.add("MasterUnavailable", py.get_type::<MasterUnavailable>())?;
    sub.add("RpcError", py.get_type::<RpcError>())?;
    sub.add("IoError", py.get_type::<IoError>())?;
    sub.add("ConfigError", py.get_type::<ConfigError>())?;

    parent.add_submodule(&sub)?;
    Ok(())
}

/// Convert a `goosefs_sdk::error::Error` into the most specific Python
/// exception possible.
///
/// Every variant of the upstream enum is matched explicitly. If a new variant
/// is added to `goosefs-sdk` and we forget to update this function, the Rust
/// compiler will fail the build (no `_` arm) — this is intentional, see
/// Review §17.1.
//
// Allowed because the first call site lands in P2 (`AsyncGoosefs::*` methods).
#[allow(dead_code)]
pub fn map_err(e: goosefs_sdk::error::Error) -> PyErr {
    use goosefs_sdk::error::Error as E;
    let msg = e.to_string();
    match e {
        E::NotFound { .. } => NotFound::new_err(msg),
        E::AlreadyExists { .. } => AlreadyExists::new_err(msg),
        E::PermissionDenied { .. } => PermissionDenied::new_err(msg),
        E::InvalidArgument { .. } | E::InvalidPath { .. } => InvalidArgument::new_err(msg),
        E::FileIncomplete { .. } => FileIncomplete::new_err(msg),
        E::DirectoryNotEmpty { .. } => DirectoryNotEmpty::new_err(msg),
        E::OpenDirectory { .. } => IsADirectory::new_err(msg),
        E::AuthenticationFailed { .. } => AuthenticationFailed::new_err(msg),
        E::NoWorkerAvailable { .. } => NoWorkerAvailable::new_err(msg),
        E::MasterUnavailable { .. } => MasterUnavailable::new_err(msg),
        E::ConfigError { .. } => ConfigError::new_err(msg),
        E::GrpcError { .. } | E::TransportError { .. } => RpcError::new_err(msg),
        // Review §17.1 — three variants that the original draft let fall
        // through to the catch-all. Handle them explicitly so the Python
        // exception type carries the correct semantics.
        E::MissingField { field } => {
            GoosefsError::new_err(format!("missing field in response: {field}"))
        }
        E::BlockIoError { message } => IoError::new_err(format!("block IO error: {message}")),
        E::Internal { message, .. } => GoosefsError::new_err(format!("internal error: {message}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use goosefs_sdk::error::Error as E;

    /// Smoke-test that every variant of the upstream enum produces a non-null
    /// `PyErr`. We intentionally do *not* assert on the Python type here —
    /// `Python::with_gil` would be required and PyO3 already initialises the
    /// interpreter from `pytest`. This test runs without a Python interpreter
    /// and only validates that `map_err` does not panic.
    #[test]
    fn map_err_never_panics_for_each_variant() {
        let cases: Vec<E> = vec![
            E::NotFound { path: "/x".into() },
            E::AlreadyExists { path: "/x".into() },
            E::PermissionDenied {
                message: "p".into(),
            },
            E::InvalidArgument {
                message: "i".into(),
            },
            E::InvalidPath {
                path: "/bad".into(),
            },
            E::FileIncomplete {
                message: "f".into(),
            },
            E::DirectoryNotEmpty {
                message: "d".into(),
            },
            E::OpenDirectory { path: "/d".into() },
            E::AuthenticationFailed {
                message: "a".into(),
            },
            E::NoWorkerAvailable {
                message: "w".into(),
            },
            E::MasterUnavailable {
                message: "m".into(),
            },
            E::ConfigError {
                message: "c".into(),
            },
            E::MissingField {
                field: "block_id".into(),
            },
            E::BlockIoError {
                message: "io".into(),
            },
            E::Internal {
                message: "boom".into(),
                source: None,
            },
        ];
        for e in cases {
            // `to_string()` should always succeed and `map_err` should always
            // produce a `PyErr` (which is `!Default`, so we just discard it).
            let _ = map_err(e);
        }
    }
}
