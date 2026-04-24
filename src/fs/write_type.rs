//! `WriteType` xattr key and inheritance helpers.
//!
//! GooseFS supports a per-directory extended attribute
//! `"innerWriteType"` (key: [`WRITE_TYPE_XATTR_KEY`]) that encodes the
//! preferred write strategy for files created under that directory.
//!
//! # Java authority
//!
//! Verified against `DefaultFileSystem.createFile()`:
//! ```java
//! if (!options.isWriteTypeSet()) {
//!     Scope scope = InodeTree.getXAttr(parent, XATTR_WRITE_TYPE_KEY);
//!     if (scope != null) {
//!         options.setWriteType(WriteType.fromProto(scope.getWriteType()));
//!     }
//! }
//! ```
//! The xattr value is the string name of the `WriteType` enum, e.g.
//! `"MUST_CACHE"`, `"CACHE_THROUGH"`, etc.

use crate::config::WriteType;

/// xattr key that carries the inherited write type.
///
/// Set on a **directory** inode; inherited by new files created under it.
pub const WRITE_TYPE_XATTR_KEY: &str = "innerWriteType";

/// A wrapper that either holds an explicit user-set `WriteType` or indicates
/// that the write type should be inherited from the parent directory's xattr.
///
/// Used in [`crate::fs::options::CreateFileOptions`].
#[derive(Debug, Clone, PartialEq)]
pub enum WriteTypeXAttr {
    /// Explicitly set by the caller — do not inherit from xattr.
    Explicit(WriteType),
    /// Not set — inherit from the parent directory xattr (if present).
    Inherit,
}

impl Default for WriteTypeXAttr {
    fn default() -> Self {
        WriteTypeXAttr::Inherit
    }
}

/// Extract the `WriteType` from a file/directory's `xattr` map.
///
/// Looks for the key [`WRITE_TYPE_XATTR_KEY`] in `xattr`, parses the UTF-8
/// string value as a `WriteType`, and returns it.
///
/// Returns `None` if:
/// - the key is absent,
/// - the value is not valid UTF-8,
/// - the string does not map to a known `WriteType`.
///
/// # Go SDK vs Java
///
/// The Go SDK's `GetWriteTypeFromXAttr` behaves identically:
/// `xattr[writeTypeXAttrKey]` → string → `WriteType::FromXAttr(str)`.
///
/// Verified against Java `InodeTree.getXAttr` + `WriteType.fromString`.
pub fn get_write_type_from_xattr(
    xattr: &std::collections::HashMap<String, Vec<u8>>,
) -> Option<WriteType> {
    let raw = xattr.get(WRITE_TYPE_XATTR_KEY)?;
    let s = std::str::from_utf8(raw).ok()?;
    s.parse::<WriteType>().ok()
}

// ── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WriteType;
    use std::collections::HashMap;

    fn xattr(key: &str, val: &str) -> HashMap<String, Vec<u8>> {
        let mut m = HashMap::new();
        m.insert(key.to_string(), val.as_bytes().to_vec());
        m
    }

    #[test]
    fn test_get_must_cache() {
        let x = xattr(WRITE_TYPE_XATTR_KEY, "MUST_CACHE");
        assert_eq!(get_write_type_from_xattr(&x), Some(WriteType::MustCache));
    }

    #[test]
    fn test_get_cache_through() {
        let x = xattr(WRITE_TYPE_XATTR_KEY, "CACHE_THROUGH");
        assert_eq!(get_write_type_from_xattr(&x), Some(WriteType::CacheThrough));
    }

    #[test]
    fn test_get_through() {
        let x = xattr(WRITE_TYPE_XATTR_KEY, "THROUGH");
        assert_eq!(get_write_type_from_xattr(&x), Some(WriteType::Through));
    }

    #[test]
    fn test_get_async_through() {
        let x = xattr(WRITE_TYPE_XATTR_KEY, "ASYNC_THROUGH");
        assert_eq!(get_write_type_from_xattr(&x), Some(WriteType::AsyncThrough));
    }

    #[test]
    fn test_key_absent_returns_none() {
        let x = xattr("other_key", "MUST_CACHE");
        assert_eq!(get_write_type_from_xattr(&x), None);
    }

    #[test]
    fn test_invalid_value_returns_none() {
        let x = xattr(WRITE_TYPE_XATTR_KEY, "NOT_A_WRITE_TYPE");
        assert_eq!(get_write_type_from_xattr(&x), None);
    }

    #[test]
    fn test_empty_map_returns_none() {
        assert_eq!(get_write_type_from_xattr(&HashMap::new()), None);
    }

    #[test]
    fn test_write_type_xattr_default_is_inherit() {
        assert_eq!(WriteTypeXAttr::default(), WriteTypeXAttr::Inherit);
    }
}
