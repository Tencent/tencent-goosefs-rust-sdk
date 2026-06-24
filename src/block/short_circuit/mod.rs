//! Short-circuit (local mmap) block read path.
//!
//! When a block is served by the **local** Worker and is physically present on
//! local disk, the client can bypass the gRPC data plane entirely: it asks the
//! Worker (control plane only, via the `OpenLocalBlock` bidi RPC) for the local
//! block file path, `mmap`s it once, and serves all reads as zero-copy slices.
//!
//! This module implements the design in `docs/SHORT_CIRCUIT_DESIGN.md`:
//!
//! - [`LocalBlockReader`] — the per-block data plane (P1).
//! - [`ShortCircuitFactory`] — per-task hot-block LRU + bounded negative cache
//!   + the [`should_use_short_circuit`] decision (P2).
//! - [`AccessHint`] / [`ShortCircuitError`] — the public hint + error types.
//!
//! Consistency is the hard constraint (design §1.1 / §1.3): any error that is
//! *recoverable* falls back transparently to the gRPC path (INV-S1), whereas
//! *semantic* errors (`OutOfRange`) are propagated unchanged (INV-S4).

mod factory;
mod reader;

pub use factory::{
    should_use_short_circuit, ScDecisionCtx, ShortCircuitConfig, ShortCircuitFactory,
};
pub use reader::LocalBlockReader;

/// L1 kernel-readahead hint applied via `madvise` after mapping (design §3.2.1).
///
/// On non-unix targets `madvise` is unavailable and all variants behave like
/// [`AccessHint::Default`] (no hint), which is the safe cross-platform default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessHint {
    /// `MADV_SEQUENTIAL` — widen the kernel readahead window (sequential scan).
    Sequential,
    /// `MADV_RANDOM` — disable readahead (positioned / random reads). This is
    /// the recommended default for the PR-heavy workloads SC targets.
    Random,
    /// No `madvise` — leave the kernel default. Safe everywhere.
    #[default]
    Default,
}

impl AccessHint {
    /// Resolve from the configured `goosefs.client.short.circuit.advise` value
    /// (`"sequential" | "random" | "normal" | "none"`). Unknown values map to
    /// [`AccessHint::Default`].
    pub fn from_advise_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "sequential" | "seq" => AccessHint::Sequential,
            "random" | "rand" => AccessHint::Random,
            // "normal" / "none" / unknown → no explicit hint.
            _ => AccessHint::Default,
        }
    }
}

/// Errors specific to the short-circuit path (design §7.1).
///
/// All variants are *recoverable* (transparently fall back to gRPC, INV-S1)
/// **except** [`ShortCircuitError::OutOfRange`], which is a caller/semantic
/// error that must be surfaced unchanged (INV-S4). Use [`is_semantic`] to
/// classify.
///
/// [`is_semantic`]: ShortCircuitError::is_semantic
#[derive(Debug, thiserror::Error)]
pub enum ShortCircuitError {
    /// The block is not served by the local worker (pre-filter rejected it).
    #[error("short-circuit: block source is not local")]
    NotLocal,

    /// The `OpenLocalBlock` RPC failed (block not local / IO error / auth).
    #[error("short-circuit: OpenLocalBlock failed: {0}")]
    OpenLocalBlock(#[source] Box<crate::error::Error>),

    /// The `OpenLocalBlock` response carried no local path.
    #[error("short-circuit: OpenLocalBlock response had no path")]
    MissingPath,

    /// `File::open` on the local block path failed (e.g. EACCES — uid mismatch).
    #[error("short-circuit: open local block file failed: {0}")]
    FileOpen(#[source] std::io::Error),

    /// `Mmap::map` failed (ENOMEM / EINVAL).
    #[error("short-circuit: mmap failed: {0}")]
    Mmap(#[source] std::io::Error),

    /// `madvise` failed (currently unused on the error path — advise failures
    /// are non-fatal and logged; kept for completeness with design §7.1).
    #[error("short-circuit: madvise failed: {0}")]
    Madvise(#[source] std::io::Error),

    /// A read / prefetch range escaped the logical block — a **semantic**
    /// error that must NOT be swallowed by fallback (INV-S4).
    #[error("short-circuit: out of range (off={off}, len={len}, file_size={file_size})")]
    OutOfRange {
        off: usize,
        len: usize,
        file_size: usize,
    },

    /// A SIGBUS was observed on the mapping (protocol violation). Reserved for
    /// the optional P6 handler; not produced on the normal path.
    #[error("short-circuit: SIGBUS on mapping")]
    SigBus,
}

impl ShortCircuitError {
    /// Whether this error is *semantic* (must propagate, never fall back).
    ///
    /// Only [`ShortCircuitError::OutOfRange`] qualifies (INV-S4); every other
    /// variant is a recoverable failure that should transparently fall back to
    /// the gRPC path (INV-S1).
    pub fn is_semantic(&self) -> bool {
        matches!(self, ShortCircuitError::OutOfRange { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_hint_from_str() {
        assert_eq!(AccessHint::from_advise_str("random"), AccessHint::Random);
        assert_eq!(AccessHint::from_advise_str("RAND"), AccessHint::Random);
        assert_eq!(
            AccessHint::from_advise_str("sequential"),
            AccessHint::Sequential
        );
        assert_eq!(AccessHint::from_advise_str("normal"), AccessHint::Default);
        assert_eq!(AccessHint::from_advise_str("none"), AccessHint::Default);
        assert_eq!(AccessHint::from_advise_str("bogus"), AccessHint::Default);
    }

    #[test]
    fn access_hint_default_is_default_variant() {
        assert_eq!(AccessHint::default(), AccessHint::Default);
    }

    #[test]
    fn only_out_of_range_is_semantic() {
        assert!(ShortCircuitError::OutOfRange {
            off: 0,
            len: 1,
            file_size: 0
        }
        .is_semantic());
        assert!(!ShortCircuitError::NotLocal.is_semantic());
        assert!(!ShortCircuitError::MissingPath.is_semantic());
        assert!(!ShortCircuitError::SigBus.is_semantic());
        assert!(!ShortCircuitError::FileOpen(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        ))
        .is_semantic());
    }
}
