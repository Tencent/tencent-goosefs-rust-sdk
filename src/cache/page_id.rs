//! Page identity and metadata.
//!
//! Mirrors Java `PageId` and `PageInfo`.

use std::sync::Arc;
use std::time::Instant;

/// Identifies a single cache page: a fixed-size window within a file.
///
/// Equivalent to Java `PageId(fileId, pageIndex)`.
///
/// `file_id` must be **stable across opens** of the same file so that the
/// cache can serve hits across streams and processes. The Rust SDK derives it
/// from [`crate::fs::uri_status::URIStatus::file_id`] (the server inode ID);
/// see [`PageId::from_file_offset`] for offset → page-index conversion.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PageId {
    /// Stable file identifier (server inode id rendered as text, or another
    /// stable key). Shared (`Arc<str>`) so cloning a `PageId` is cheap.
    pub file_id: Arc<str>,
    /// Zero-based page index = `offset / page_size`.
    pub page_index: u64,
}

impl PageId {
    /// Build a `PageId` from a file id and an explicit page index.
    pub fn new(file_id: impl Into<Arc<str>>, page_index: u64) -> Self {
        Self {
            file_id: file_id.into(),
            page_index,
        }
    }

    /// Build a `PageId` for the page containing absolute `offset`, given
    /// `page_size`.
    ///
    /// # Panics
    /// Panics if `page_size == 0`.
    pub fn from_file_offset(file_id: impl Into<Arc<str>>, offset: u64, page_size: u64) -> Self {
        assert!(page_size > 0, "page_size must be > 0");
        Self {
            file_id: file_id.into(),
            page_index: offset / page_size,
        }
    }

    /// Absolute byte offset of the start of this page, given `page_size`.
    pub fn page_start(&self, page_size: u64) -> u64 {
        self.page_index * page_size
    }
}

/// Quota scope a page is accounted under.
///
/// Mirrors Java `CacheScope`. Only [`CacheScope::Global`] is used until quota
/// support lands; the variant is kept so the API is stable.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CacheScope {
    /// No per-scope quota; counted against the global capacity.
    #[default]
    Global,
    /// Named scope (e.g. a table or dataset) for quota accounting.
    Named(Arc<str>),
}

/// Metadata for a single cached page.
///
/// Mirrors Java `PageInfo`.
#[derive(Clone, Debug)]
pub struct PageInfo {
    /// The page this metadata describes.
    pub page_id: PageId,
    /// Actual number of valid bytes in the page (the last page of a file may
    /// be smaller than the configured page size).
    pub page_size: u64,
    /// Index into the cache-manager's directory list that holds this page.
    pub dir_index: usize,
    /// When the page was created (used for TTL expiry).
    pub created_at: Instant,
    /// Quota scope this page is accounted under.
    pub scope: CacheScope,
}

impl PageInfo {
    /// Create a new `PageInfo` stamped with the current time.
    pub fn new(page_id: PageId, page_size: u64, dir_index: usize, scope: CacheScope) -> Self {
        Self {
            page_id,
            page_size,
            dir_index,
            created_at: Instant::now(),
            scope,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_index_from_offset() {
        let ps = 1024;
        assert_eq!(PageId::from_file_offset("f", 0, ps).page_index, 0);
        assert_eq!(PageId::from_file_offset("f", 1023, ps).page_index, 0);
        assert_eq!(PageId::from_file_offset("f", 1024, ps).page_index, 1);
        assert_eq!(PageId::from_file_offset("f", 2049, ps).page_index, 2);
    }

    #[test]
    fn page_start_offset() {
        let ps = 1024;
        let id = PageId::new("f", 3);
        assert_eq!(id.page_start(ps), 3072);
    }

    #[test]
    fn page_id_equality_and_clone_is_cheap() {
        let a = PageId::new("file-42", 7);
        let b = a.clone();
        assert_eq!(a, b);
        assert_eq!(a.file_id.as_ref(), "file-42");
    }

    #[test]
    fn default_scope_is_global() {
        assert_eq!(CacheScope::default(), CacheScope::Global);
    }
}
