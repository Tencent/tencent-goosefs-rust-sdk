//! Python-visible enums.
//!
//! `WriteType` and `ReadType` are exposed as `enum.IntEnum`-compatible
//! integer-valued classes via PyO3's `#[pyclass(eq, eq_int)]` so users can
//! write either `WriteType.CACHE_THROUGH` or `WriteType(3)` interchangeably,
//! and they pickle / `==`-compare cleanly.
//!
//! ## Why `IntEnum`-style instead of `enum.Enum`?
//!
//! - Round-trip-friendly: the proto `WritePType` is an `i32`, so an integer
//!   payload makes `Config.write_type → WriteType` conversion trivial.
//! - Backwards-compat: a future caller passing a raw integer (e.g. from a
//!   migration script) still works.
//! - Sortable / hashable for free.

use pyo3::prelude::*;

use goosefs_sdk::config::WriteType as SdkWriteType;
use goosefs_sdk::fs::ReadType as SdkReadType;

/// Mirrors `goosefs_sdk::config::WriteType` (proto `WritePType`).
///
/// Integer values match the protobuf wire format exactly (1..=5) so they can
/// be stored in `Config.write_type` without translation.
#[pyclass(
    module = "goosefs._goosefs",
    name = "WriteType",
    eq,
    eq_int,
    frozen,
    hash
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum PyWriteType {
    MustCache = 1,
    TryCache = 2,
    CacheThrough = 3,
    Through = 4,
    AsyncThrough = 5,
}

#[pymethods]
impl PyWriteType {
    /// Canonical lowercase name (e.g. `"cache_through"`), matching
    /// `WriteType.as_str()` in the SDK.
    fn as_str(&self) -> &'static str {
        SdkWriteType::from(*self).as_str()
    }

    /// Proto integer value (same as `int(self)`).
    #[getter]
    fn value(&self) -> i32 {
        *self as i32
    }

    fn __repr__(&self) -> String {
        format!("WriteType.{:?}", self)
    }

    fn __str__(&self) -> String {
        self.as_str().to_string()
    }

    /// Parse a `WriteType` from its canonical or upper-case string form.
    ///
    /// ```python
    /// WriteType.from_str("cache_through") == WriteType.CACHE_THROUGH
    /// WriteType.from_str("CACHE_THROUGH") == WriteType.CACHE_THROUGH
    /// ```
    #[staticmethod]
    fn from_str(s: &str) -> PyResult<Self> {
        s.parse::<SdkWriteType>()
            .map(Self::from)
            .map_err(pyo3::exceptions::PyValueError::new_err)
    }
}

impl From<SdkWriteType> for PyWriteType {
    fn from(wt: SdkWriteType) -> Self {
        match wt {
            SdkWriteType::MustCache => Self::MustCache,
            SdkWriteType::TryCache => Self::TryCache,
            SdkWriteType::CacheThrough => Self::CacheThrough,
            SdkWriteType::Through => Self::Through,
            SdkWriteType::AsyncThrough => Self::AsyncThrough,
        }
    }
}

impl From<PyWriteType> for SdkWriteType {
    fn from(wt: PyWriteType) -> Self {
        match wt {
            PyWriteType::MustCache => Self::MustCache,
            PyWriteType::TryCache => Self::TryCache,
            PyWriteType::CacheThrough => Self::CacheThrough,
            PyWriteType::Through => Self::Through,
            PyWriteType::AsyncThrough => Self::AsyncThrough,
        }
    }
}

/// Mirrors `goosefs_sdk::fs::ReadType` (proto `ReadPType`).
///
/// Only `NoCache (1)` and `Cache (2)` are exposed; other proto values are
/// reserved by the server.
#[pyclass(
    module = "goosefs._goosefs",
    name = "ReadType",
    eq,
    eq_int,
    frozen,
    hash
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum PyReadType {
    NoCache = 1,
    Cache = 2,
}

#[pymethods]
impl PyReadType {
    #[getter]
    fn value(&self) -> i32 {
        *self as i32
    }

    fn __repr__(&self) -> String {
        format!("ReadType.{:?}", self)
    }
}

impl From<PyReadType> for SdkReadType {
    fn from(rt: PyReadType) -> Self {
        match rt {
            PyReadType::NoCache => Self::NoCache,
            PyReadType::Cache => Self::Cache,
        }
    }
}

impl From<SdkReadType> for PyReadType {
    fn from(rt: SdkReadType) -> Self {
        match rt {
            SdkReadType::NoCache => Self::NoCache,
            SdkReadType::Cache => Self::Cache,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_type_round_trips_through_sdk() {
        for wt in SdkWriteType::ALL {
            let py: PyWriteType = (*wt).into();
            let back: SdkWriteType = py.into();
            assert_eq!(*wt, back);
        }
    }

    #[test]
    fn write_type_proto_values_match() {
        assert_eq!(PyWriteType::MustCache as i32, 1);
        assert_eq!(PyWriteType::TryCache as i32, 2);
        assert_eq!(PyWriteType::CacheThrough as i32, 3);
        assert_eq!(PyWriteType::Through as i32, 4);
        assert_eq!(PyWriteType::AsyncThrough as i32, 5);
    }

    #[test]
    fn read_type_round_trips_through_sdk() {
        for rt in [SdkReadType::NoCache, SdkReadType::Cache] {
            let py: PyReadType = rt.into();
            let back: SdkReadType = py.into();
            assert_eq!(rt, back);
        }
    }
}
