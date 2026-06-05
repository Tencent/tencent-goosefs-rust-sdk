//! `goosefs.enable_tracing()` — opt-in bridge from the SDK's `tracing`
//! events to a stderr subscriber.
//!
//! Review §17.7. We deliberately keep this minimal:
//!
//! 1. **Off by default.** Importing `goosefs` does *not* install any
//!    subscriber, so library users that already configured their own
//!    `tracing_subscriber` (or use `pyo3-log`) keep full control.
//! 2. **Idempotent.** Calling `enable_tracing()` twice is a no-op on the
//!    second call instead of panicking inside
//!    `tracing_subscriber::set_global_default`.
//! 3. **Stderr only, for now.** The `target` parameter accepts only
//!    `"stderr"`. We reserve `"logging"` (Python `logging` bridge) and
//!    `"stdout"` for a future minor release without breaking the API.
//! 4. **`RUST_LOG` wins.** If `RUST_LOG` is set in the environment we use
//!    it verbatim; otherwise we synthesize a filter from the `level`
//!    argument (`"info"` by default).

use std::sync::OnceLock;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

/// Tracks whether `enable_tracing` has already installed a global
/// subscriber. We use `OnceLock<bool>` (rather than `Once`) so we can
/// also remember the chosen level for diagnostic / repr purposes if we
/// ever expose it.
static TRACING_INSTALLED: OnceLock<bool> = OnceLock::new();

/// Install a stderr `tracing` subscriber for `goosefs-sdk` events.
///
/// # Arguments
///
/// * `level` — one of `"trace"`, `"debug"`, `"info"`, `"warn"`,
///   `"error"` (case-insensitive). Used only when `RUST_LOG` is **not**
///   set; if `RUST_LOG` is set we honour it verbatim and ignore `level`.
/// * `target` — sink for log lines. Only `"stderr"` is supported today;
///   anything else raises `ValueError`. (`"logging"` and `"stdout"` are
///   reserved for a future release.)
///
/// # Behaviour
///
/// * The first successful call installs a process-global subscriber.
/// * Subsequent calls are silently ignored — they do **not** raise and
///   do **not** reconfigure the existing subscriber. This matches
///   `tracing_subscriber::fmt::try_init`'s contract and lets users put
///   `enable_tracing()` near the top of every script without worrying
///   about double-import.
/// * If some *other* part of the process already installed a global
///   subscriber (e.g. via `pyo3-log` or a host application), this
///   function returns `RuntimeError` rather than overwriting it.
#[pyfunction]
#[pyo3(signature = (level = "info", *, target = "stderr"))]
pub fn enable_tracing(level: &str, target: &str) -> PyResult<()> {
    // ── Argument validation ────────────────────────────────────────────────
    // We validate *before* the idempotency check so that a typo like
    // `enable_tracing(level="verbouse")` still raises on the second
    // call instead of silently no-op'ing. Fail-fast wins for debugging.

    // Validate `target` early so a typo doesn't silently install a
    // stderr subscriber when the caller meant something else.
    match target.to_ascii_lowercase().as_str() {
        "stderr" => {}
        "logging" | "stdout" => {
            return Err(PyValueError::new_err(format!(
                "target={target:?} is reserved for a future release; \
                 only \"stderr\" is supported today",
            )));
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "target must be \"stderr\"; got {other:?}",
            )));
        }
    }

    // Validate `level` (only meaningful when RUST_LOG is unset, but we
    // still type-check it unconditionally for fail-fast feedback).
    let normalized_level = level.to_ascii_lowercase();
    let level_is_valid = matches!(
        normalized_level.as_str(),
        "trace" | "debug" | "info" | "warn" | "error"
    );
    if !level_is_valid {
        return Err(PyValueError::new_err(format!(
            "level must be one of \
             trace|debug|info|warn|error (case-insensitive); \
             got {level:?}",
        )));
    }

    // ── Idempotency ────────────────────────────────────────────────────────
    // If we've already installed our own subscriber, just return. We
    // deliberately do *not* try to mutate the existing one.
    if TRACING_INSTALLED.get().is_some() {
        return Ok(());
    }

    // ── Build the EnvFilter ────────────────────────────────────────────────
    // `RUST_LOG` overrides everything; otherwise fall back to the
    // user-supplied level. We default the *non-goosefs* crate level to
    // `warn` so enabling DEBUG on goosefs doesn't drown the user in
    // tonic / hyper noise.
    let filter = if std::env::var_os("RUST_LOG").is_some() {
        EnvFilter::try_from_default_env()
            .map_err(|e| PyValueError::new_err(format!("invalid RUST_LOG value: {e}")))?
    } else {
        let directive =
            format!("warn,goosefs_sdk={normalized_level},goosefs_python={normalized_level}");
        EnvFilter::try_new(&directive).map_err(|e| {
            PyValueError::new_err(format!("failed to build EnvFilter from {directive:?}: {e}",))
        })?
    };

    // ── Install ────────────────────────────────────────────────────────────
    // `try_init` returns Err if a global subscriber is already set by
    // someone else (e.g. pyo3-log, a host binary, an earlier test). We
    // propagate that as RuntimeError so the user can decide whether to
    // tear down their existing setup.
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_ansi(false)
        .try_init()
        .map_err(|e| {
            PyRuntimeError::new_err(format!(
                "could not install goosefs tracing subscriber \
                 (another subscriber is already active?): {e}",
            ))
        })?;

    // Mark as installed *after* `try_init` succeeds so a failure path
    // can be retried.
    let _ = TRACING_INSTALLED.set(true);
    Ok(())
}
