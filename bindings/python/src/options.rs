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

//! Python wrappers for the SDK's option structs.
//!
//! Each Python class is a thin builder around the SDK type. Conversions back
//! into the SDK happen via `into_sdk()` private helpers (consumed by the
//! `AsyncGoosefs` / `Goosefs` methods in `filesystem.rs`).
//!
//! ## Why `__repr__` everywhere?
//!
//! Review §17.3 — option objects are frequently logged in user code; an
//! empty `<OpenFileOptions object at 0x...>` repr is unhelpful. We build a
//! deterministic, kwargs-style repr so logs are diff-friendly.

use pyo3::prelude::*;

use goosefs_sdk::fs::options::{
    CreateFileOptions as SdkCreateFileOptions, DeleteOptions as SdkDeleteOptions,
    InStreamOptions as SdkInStreamOptions, OpenFileOptions as SdkOpenFileOptions,
};
use goosefs_sdk::fs::write_type::WriteTypeXAttr;

use crate::types::{PyReadType, PyWriteType};

// ---------------------------------------------------------------------------
// OpenFileOptions
// ---------------------------------------------------------------------------

/// Options for opening a Goosefs file for reading.
///
/// ```python
/// from goosefs import OpenFileOptions, ReadType
///
/// opts = OpenFileOptions()                          # default → Cache
/// opts = OpenFileOptions(read_type=ReadType.NoCache) # opt-out of caching
/// ```
#[pyclass(module = "goosefs._goosefs", name = "OpenFileOptions")]
#[derive(Clone)]
pub struct PyOpenFileOptions {
    pub(crate) read_type: PyReadType,
}

#[pymethods]
impl PyOpenFileOptions {
    #[new]
    #[pyo3(signature = (*, read_type=None))]
    fn new(read_type: Option<PyReadType>) -> Self {
        Self {
            read_type: read_type.unwrap_or(PyReadType::Cache),
        }
    }

    #[getter]
    fn read_type(&self) -> PyReadType {
        self.read_type
    }

    fn __repr__(&self) -> String {
        format!("OpenFileOptions(read_type={:?})", self.read_type)
    }
}

impl PyOpenFileOptions {
    /// Lower into the SDK type. Currently only `read_type` is mapped — the
    /// SDK's `InStreamOptions` carries additional knobs (`prefetch_window`,
    /// `max_ufs_read_concurrency`) that are deliberately not exposed in the
    /// MVP surface; they can be added later without breaking callers.
    //
    // Allowed because the first call site lands in P5 (`open_file`).
    #[allow(dead_code)]
    pub(crate) fn into_sdk(self) -> SdkOpenFileOptions {
        let in_stream = SdkInStreamOptions {
            read_type: self.read_type.into(),
            ..Default::default()
        };
        SdkOpenFileOptions {
            in_stream_options: in_stream,
        }
    }
}

// ---------------------------------------------------------------------------
// CreateFileOptions
// ---------------------------------------------------------------------------

/// Options for creating a new Goosefs file.
///
/// `write_type=None` (default) tells the SDK to inherit the write type from
/// the parent directory's `innerWriteType` xattr (Java-compatible behaviour).
/// Pass an explicit [`WriteType`] to override.
///
/// ```python
/// from goosefs import CreateFileOptions, WriteType
///
/// opts = CreateFileOptions(recursive=True, write_type=WriteType.CACHE_THROUGH)
/// ```
#[pyclass(module = "goosefs._goosefs", name = "CreateFileOptions")]
#[derive(Clone)]
pub struct PyCreateFileOptions {
    pub(crate) write_type: Option<PyWriteType>,
    pub(crate) block_size_bytes: Option<i64>,
    pub(crate) replication_max: Option<i32>,
    pub(crate) recursive: bool,
}

#[pymethods]
impl PyCreateFileOptions {
    #[new]
    #[pyo3(signature = (*, write_type=None, block_size_bytes=None, replication_max=None, recursive=false))]
    fn new(
        write_type: Option<PyWriteType>,
        block_size_bytes: Option<i64>,
        replication_max: Option<i32>,
        recursive: bool,
    ) -> Self {
        Self {
            write_type,
            block_size_bytes,
            replication_max,
            recursive,
        }
    }

    #[getter]
    fn write_type(&self) -> Option<PyWriteType> {
        self.write_type
    }

    #[getter]
    fn block_size_bytes(&self) -> Option<i64> {
        self.block_size_bytes
    }

    #[getter]
    fn replication_max(&self) -> Option<i32> {
        self.replication_max
    }

    #[getter]
    fn recursive(&self) -> bool {
        self.recursive
    }

    fn __repr__(&self) -> String {
        format!(
            "CreateFileOptions(write_type={:?}, block_size_bytes={:?}, replication_max={:?}, recursive={})",
            self.write_type, self.block_size_bytes, self.replication_max, self.recursive,
        )
    }
}

impl PyCreateFileOptions {
    // Allowed because the first call site lands in P4/P5 (`create_file` / write_file).
    #[allow(dead_code)]
    pub(crate) fn into_sdk(self) -> SdkCreateFileOptions {
        SdkCreateFileOptions {
            write_type: match self.write_type {
                Some(wt) => WriteTypeXAttr::Explicit(wt.into()),
                None => WriteTypeXAttr::Inherit,
            },
            block_size_bytes: self.block_size_bytes,
            replication_max: self.replication_max,
            recursive: self.recursive,
        }
    }
}

// ---------------------------------------------------------------------------
// DeleteOptions
// ---------------------------------------------------------------------------

/// Options controlling a `delete()` call.
///
/// Defaults match `DeleteOptions::default()` in the SDK: no recursion, no
/// `unchecked`, no `goosefs_only`. The most common case — recursively
/// deleting a directory tree — can be expressed as `DeleteOptions(recursive=True)`.
#[pyclass(module = "goosefs._goosefs", name = "DeleteOptions")]
#[derive(Clone)]
pub struct PyDeleteOptions {
    pub(crate) recursive: bool,
    pub(crate) unchecked: bool,
    pub(crate) goosefs_only: bool,
}

#[pymethods]
impl PyDeleteOptions {
    #[new]
    #[pyo3(signature = (*, recursive=false, unchecked=false, goosefs_only=false))]
    fn new(recursive: bool, unchecked: bool, goosefs_only: bool) -> Self {
        Self {
            recursive,
            unchecked,
            goosefs_only,
        }
    }

    #[getter]
    fn recursive(&self) -> bool {
        self.recursive
    }

    #[getter]
    fn unchecked(&self) -> bool {
        self.unchecked
    }

    #[getter]
    fn goosefs_only(&self) -> bool {
        self.goosefs_only
    }

    fn __repr__(&self) -> String {
        format!(
            "DeleteOptions(recursive={}, unchecked={}, goosefs_only={})",
            self.recursive, self.unchecked, self.goosefs_only,
        )
    }
}

impl PyDeleteOptions {
    pub(crate) fn into_sdk(self) -> SdkDeleteOptions {
        SdkDeleteOptions {
            recursive: self.recursive,
            unchecked: self.unchecked,
            goosefs_only: self.goosefs_only,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_options_round_trip() {
        let py = PyDeleteOptions::new(true, false, true);
        let sdk = py.into_sdk();
        assert!(sdk.recursive);
        assert!(!sdk.unchecked);
        assert!(sdk.goosefs_only);
    }

    #[test]
    fn create_file_inherit_when_no_write_type() {
        let py = PyCreateFileOptions::new(None, Some(4 * 1024 * 1024), None, true);
        let sdk = py.into_sdk();
        assert_eq!(sdk.write_type, WriteTypeXAttr::Inherit);
        assert_eq!(sdk.block_size_bytes, Some(4 * 1024 * 1024));
        assert!(sdk.recursive);
    }

    #[test]
    fn create_file_explicit_write_type() {
        let py = PyCreateFileOptions::new(Some(PyWriteType::CacheThrough), None, None, false);
        let sdk = py.into_sdk();
        assert!(matches!(
            sdk.write_type,
            WriteTypeXAttr::Explicit(goosefs_sdk::config::WriteType::CacheThrough)
        ));
    }

    #[test]
    fn open_file_default_uses_cache() {
        let py = PyOpenFileOptions::new(None);
        let sdk = py.into_sdk();
        assert_eq!(
            sdk.in_stream_options.read_type,
            goosefs_sdk::fs::ReadType::Cache
        );
    }
}
