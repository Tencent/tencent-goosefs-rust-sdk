//! `URIStatus` — immutable Python view of a Goosefs file/directory metadata
//! snapshot.
//!
//! Wraps `goosefs_sdk::fs::URIStatus`. The full set of 25 fields is exposed
//! as `@getter`s. We deliberately do not expose `block_infos` / `xattr` from
//! this stage (P2) because:
//!
//! - `block_infos` is only populated for read-path calls (P5). In P2 it would
//!   always be empty / misleading.
//! - `xattr` is `HashMap<String, Vec<u8>>` — handing raw bytes to Python is
//!   safe but the layer is more useful once a typed accessor (e.g.
//!   `get_write_type_xattr()`) lands. Until then `xattr` is exposed as a
//!   plain `dict[str, bytes]` so users can inspect it.
//!
//! ## Equality / hashing
//!
//! Two `URIStatus` are equal iff their `path` AND `last_modification_time_ms`
//! match. This mirrors the natural "same file at same point in time"
//! definition that Python users expect when stat-comparing snapshots.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use goosefs_sdk::fs::URIStatus;

/// Read-only metadata for a single Goosefs path.
#[pyclass(module = "goosefs._goosefs", name = "URIStatus", frozen)]
#[derive(Clone)]
pub struct PyURIStatus {
    pub(crate) inner: URIStatus,
}

impl PyURIStatus {
    pub fn new(inner: URIStatus) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyURIStatus {
    // ── Identity ────────────────────────────────────────────────────────────

    #[getter]
    fn file_id(&self) -> i64 {
        self.inner.file_id
    }

    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[getter]
    fn path(&self) -> String {
        self.inner.path.clone()
    }

    #[getter]
    fn ufs_path(&self) -> String {
        self.inner.ufs_path.clone()
    }

    // ── Geometry ───────────────────────────────────────────────────────────

    #[getter]
    fn length(&self) -> i64 {
        self.inner.length
    }

    #[getter]
    fn block_size_bytes(&self) -> i64 {
        self.inner.block_size_bytes
    }

    #[getter]
    fn block_ids(&self) -> Vec<i64> {
        self.inner.block_ids.clone()
    }

    // ── Timestamps (epoch milliseconds) ────────────────────────────────────

    #[getter]
    fn creation_time_ms(&self) -> i64 {
        self.inner.creation_time_ms
    }

    #[getter]
    fn last_modification_time_ms(&self) -> i64 {
        self.inner.last_modification_time_ms
    }

    #[getter]
    fn last_access_time_ms(&self) -> i64 {
        self.inner.last_access_time_ms
    }

    // ── State flags ────────────────────────────────────────────────────────

    #[getter]
    fn completed(&self) -> bool {
        self.inner.completed
    }

    #[getter]
    fn folder(&self) -> bool {
        self.inner.folder
    }

    #[getter]
    fn cacheable(&self) -> bool {
        self.inner.cacheable
    }

    #[getter]
    fn persisted(&self) -> bool {
        self.inner.persisted
    }

    #[getter]
    fn mount_point(&self) -> bool {
        self.inner.mount_point
    }

    // ── Cache locality ─────────────────────────────────────────────────────

    #[getter]
    fn in_goose_fs_percentage(&self) -> i32 {
        self.inner.in_goose_fs_percentage
    }

    #[getter]
    fn in_memory_percentage(&self) -> i32 {
        self.inner.in_memory_percentage
    }

    // ── Permissions ────────────────────────────────────────────────────────

    #[getter]
    fn owner(&self) -> String {
        self.inner.owner.clone()
    }

    #[getter]
    fn group(&self) -> String {
        self.inner.group.clone()
    }

    #[getter]
    fn mode(&self) -> i32 {
        self.inner.mode
    }

    // ── Misc ───────────────────────────────────────────────────────────────

    #[getter]
    fn persistence_state(&self) -> String {
        self.inner.persistence_state.clone()
    }

    #[getter]
    fn mount_id(&self) -> i64 {
        self.inner.mount_id
    }

    #[getter]
    fn ufs_fingerprint(&self) -> String {
        self.inner.ufs_fingerprint.clone()
    }

    /// Extended attributes as `dict[str, bytes]`.
    ///
    /// Goosefs stores attributes such as `"innerWriteType"` here. Values are
    /// raw bytes because the server does not enforce a charset.
    #[getter]
    fn xattr<'py>(&self, py: Python<'py>) -> PyResult<HashMap<String, Bound<'py, PyBytes>>> {
        let mut out = HashMap::with_capacity(self.inner.xattr.len());
        for (k, v) in &self.inner.xattr {
            out.insert(k.clone(), PyBytes::new(py, v));
        }
        Ok(out)
    }

    #[getter]
    fn symlink(&self) -> Option<String> {
        self.inner.symlink.clone()
    }

    // ── Convenience predicates ─────────────────────────────────────────────

    /// `True` if the path is a completed file *or* a directory — usable for
    /// reads.
    fn is_readable(&self) -> bool {
        self.inner.is_readable()
    }

    /// `True` if the file has been fully written and committed.
    fn is_completed(&self) -> bool {
        self.inner.is_completed()
    }

    /// `True` if the path refers to a directory.
    fn is_folder(&self) -> bool {
        self.inner.is_folder()
    }

    /// `True` if the data has been durably persisted to UFS.
    fn is_persisted(&self) -> bool {
        self.inner.is_persisted()
    }

    /// Number of blocks in this file (`0` for directories).
    fn block_count(&self) -> usize {
        self.inner.block_count()
    }

    // ── Equality / dunder ──────────────────────────────────────────────────

    /// Equality by `(path, last_modification_time_ms)`. See the module
    /// docstring for the rationale.
    fn __eq__(&self, other: &Self) -> bool {
        self.inner.path == other.inner.path
            && self.inner.last_modification_time_ms == other.inner.last_modification_time_ms
    }

    fn __hash__(&self) -> u64 {
        // xxHash3 (same hash Lance uses via `xxhash_rust::xxh3`): fast and
        // non-cryptographic. A Python `__hash__` only needs to be stable for the
        // lifetime of the process. Standardised across the project on xxHash3.
        use std::hash::{Hash, Hasher};
        use xxhash_rust::xxh3::Xxh3Default;
        let mut h = Xxh3Default::default();
        self.inner.path.hash(&mut h);
        self.inner.last_modification_time_ms.hash(&mut h);
        h.finish()
    }

    fn __repr__(&self) -> String {
        format!(
            "URIStatus(path={:?}, length={}, folder={}, completed={}, owner={:?})",
            self.inner.path,
            self.inner.length,
            self.inner.folder,
            self.inner.completed,
            self.inner.owner,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use goosefs_sdk::proto::grpc::file::FileInfo;

    fn folder_status() -> PyURIStatus {
        let fi = FileInfo {
            file_id: Some(1),
            name: Some("d".into()),
            path: Some("/d".into()),
            folder: Some(true),
            completed: Some(false),
            ..Default::default()
        };
        PyURIStatus::new(URIStatus::from_proto(fi))
    }

    #[test]
    fn folder_is_readable_even_when_not_completed() {
        let s = folder_status();
        assert!(s.is_folder());
        assert!(!s.is_completed());
        assert!(s.is_readable()); // matches Java semantics
    }

    #[test]
    fn equality_ignores_unrelated_fields() {
        let a = folder_status();
        let mut b_inner = a.inner.clone();
        b_inner.length = 999; // length differs but path+mtime same
        let b = PyURIStatus::new(b_inner);
        assert!(a.__eq__(&b));
    }
}
