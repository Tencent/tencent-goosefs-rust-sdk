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
use std::sync::Arc;

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

// ── Lazy list view ──────────────────────────────────────────────────────────

/// Lazy list view of `list_status` results.
///
/// Holds the Rust-side `Vec<URIStatus>` in a single Python object without
/// creating N `URIStatus` Python objects upfront. Individual entries are
/// materialised on-demand via `__getitem__` / `__iter__`.
///
/// **Performance**: for a directory with N entries, the eager `list_status`
/// creates N Python objects in the GIL window (~33.4µs for N=100), while this
/// lazy wrapper creates 1 Python object (~0.3µs), reducing GIL occupancy by
/// ~99%. See `docs/perf/ListDir懒加载优化方案.md` §3.
///
/// Accessing `len(lst)` is O(1) and creates zero objects. Accessing `lst[i]`
/// clones one `URIStatus` (Rust struct, ~300-500ns) and creates one Python
/// object. Iterating creates one object per `__next__`.
///
/// **What is lazy**: only the Rust-struct → Python-object materialisation is
/// deferred. The gRPC RPC, prost deserialisation, and `URIStatus::from_proto`
/// all complete during `await list_status_lazy(...)` — the data is fully
/// loaded into `Arc<Vec<URIStatus>>` before the Python object is returned.
#[pyclass(module = "goosefs._goosefs", name = "URIStatusList", frozen)]
pub struct PyURIStatusList {
    pub(crate) inner: Arc<Vec<URIStatus>>,
}

impl PyURIStatusList {
    pub fn new(items: Vec<URIStatus>) -> Self {
        Self {
            inner: Arc::new(items),
        }
    }
}

#[pymethods]
impl PyURIStatusList {
    /// Number of entries. O(1), zero object creation.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Get the i-th entry as a `URIStatus`. Creates one Python object.
    /// Supports negative indexing (e.g. `lst[-1]`).
    fn __getitem__(&self, index: isize) -> PyResult<PyURIStatus> {
        let len = self.inner.len() as isize;
        let idx = if index < 0 { index + len } else { index };
        if idx < 0 || idx >= len {
            return Err(pyo3::exceptions::PyIndexError::new_err(format!(
                "index {index} out of range for length {len}"
            )));
        }
        Ok(PyURIStatus::new(self.inner[idx as usize].clone()))
    }

    /// Iterate over entries. Each `__next__` creates one `URIStatus`.
    fn __iter__(slf: PyRef<'_, Self>) -> PyURIStatusListIter {
        PyURIStatusListIter {
            list: slf.inner.clone(),
            pos: 0,
        }
    }

    fn __repr__(&self) -> String {
        format!("URIStatusList(len={})", self.inner.len())
    }

    /// `True` if the list has entries.
    fn __bool__(&self) -> bool {
        !self.inner.is_empty()
    }
}

/// Iterator yielded by `URIStatusList.__iter__`.
///
/// Holds an `Arc` clone of the backing `Vec<URIStatus>` so the iterator
/// outlives any single borrow of the list. Each `__next__` clones one
/// `URIStatus` and wraps it in a `PyURIStatus`.
#[pyclass(module = "goosefs._goosefs", name = "_URIStatusListIter")]
pub struct PyURIStatusListIter {
    list: Arc<Vec<URIStatus>>,
    pos: usize,
}

#[pymethods]
impl PyURIStatusListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyURIStatus> {
        if self.pos < self.list.len() {
            let item = PyURIStatus::new(self.list[self.pos].clone());
            self.pos += 1;
            Some(item)
        } else {
            None
        }
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

    // ── PyURIStatusList tests ───────────────────────────────────────────

    fn make_test_status(id: i64, name: &str) -> URIStatus {
        URIStatus::from_proto(FileInfo {
            file_id: Some(id),
            name: Some(name.into()),
            path: Some(format!("/d/{name}")),
            ..Default::default()
        })
    }

    fn make_test_list(n: i64) -> PyURIStatusList {
        let items: Vec<URIStatus> = (0..n)
            .map(|i| make_test_status(i, &format!("f{i}")))
            .collect();
        PyURIStatusList::new(items)
    }

    #[test]
    fn test_lazy_list_len() {
        let lst = make_test_list(5);
        assert_eq!(lst.__len__(), 5);
        assert!(lst.__bool__());
    }

    #[test]
    fn test_lazy_list_getitem_positive() {
        let lst = make_test_list(5);
        let s0 = lst.__getitem__(0).unwrap();
        assert_eq!(s0.inner.file_id, 0);
        assert_eq!(s0.inner.name, "f0");
        let s4 = lst.__getitem__(4).unwrap();
        assert_eq!(s4.inner.file_id, 4);
    }

    #[test]
    fn test_lazy_list_getitem_negative() {
        let lst = make_test_list(5);
        let s_last = lst.__getitem__(-1).unwrap();
        assert_eq!(s_last.inner.file_id, 4);
        let s_first = lst.__getitem__(-5).unwrap();
        assert_eq!(s_first.inner.file_id, 0);
    }

    #[test]
    fn test_lazy_list_getitem_out_of_range() {
        let lst = make_test_list(5);
        assert!(lst.__getitem__(5).is_err());
        assert!(lst.__getitem__(-6).is_err());
        assert!(lst.__getitem__(100).is_err());
    }

    #[test]
    fn test_lazy_list_empty() {
        let lst = PyURIStatusList::new(vec![]);
        assert_eq!(lst.__len__(), 0);
        assert!(!lst.__bool__());
        assert!(lst.__getitem__(0).is_err());
        assert!(lst.__getitem__(-1).is_err());
    }

    #[test]
    fn test_lazy_list_iter_full() {
        let lst = make_test_list(3);
        let mut iter = PyURIStatusListIter {
            list: lst.inner.clone(),
            pos: 0,
        };
        let s0 = iter.__next__().unwrap();
        assert_eq!(s0.inner.file_id, 0);
        let s1 = iter.__next__().unwrap();
        assert_eq!(s1.inner.file_id, 1);
        let s2 = iter.__next__().unwrap();
        assert_eq!(s2.inner.file_id, 2);
        assert!(iter.__next__().is_none());
    }

    #[test]
    fn test_lazy_list_iter_empty() {
        let lst = PyURIStatusList::new(vec![]);
        let mut iter = PyURIStatusListIter {
            list: lst.inner.clone(),
            pos: 0,
        };
        assert!(iter.__next__().is_none());
    }

    #[test]
    fn test_lazy_list_repr() {
        let lst = make_test_list(10);
        assert_eq!(lst.__repr__(), "URIStatusList(len=10)");
    }
}
