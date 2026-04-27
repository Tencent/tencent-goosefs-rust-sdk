//! GooseFS URI-level file/directory metadata.
//!
//! [`URIStatus`] is the Rust equivalent of Go SDK's `URIStatus` / Java's
//! `URIStatus`.  It wraps the raw `FileInfo` proto message and exposes a
//! richer, immutable view with O(1) block-info lookup.
//!
//! # Design decisions
//!
//! ## Lazy block-info map → eager build at construction time
//!
//! The Go SDK stores `file_block_infos` as a raw slice and builds the
//! `block_id → FileBlockInfo` map on first access.  Rust's ownership model
//! makes eager construction simpler and avoids `OnceCell` complexity.
//! We build the `HashMap<i64, FileBlockInfo>` once inside `from_proto()`.
//! For typical GooseFS files (≤ 100k blocks of 64 MiB each), the map is at
//! most a few MB — an acceptable trade-off.
//!
//! ## `xattr` stays as `HashMap<String, Vec<u8>>`
//!
//! The proto already uses this representation.  Typed xattr accessors
//! (e.g. `get_write_type_xattr`) are provided as separate helpers rather
//! than encoding them into the struct to keep the type simple.

use std::collections::HashMap;

use crate::proto::grpc::file::{FileBlockInfo, FileInfo};

/// Immutable snapshot of a GooseFS file or directory path.
///
/// Corresponds to Java's `alluxio.client.file.URIStatus` and
/// Go SDK's `wire.URIStatus`.
///
/// All fields are pre-extracted from the raw `FileInfo` proto so that
/// callers do not need to deal with `Option` unwrapping on every access.
/// Defaults are chosen to match the Java server's interpretation:
///
/// | Field                  | Default if absent |
/// |------------------------|-------------------|
/// | `file_id`              | `0`               |
/// | `name`                 | `""`              |
/// | `path`                 | `""`              |
/// | `ufs_path`             | `""`              |
/// | `length`               | `0`               |
/// | `block_size_bytes`     | `0`               |
/// | `creation_time_ms`     | `0`               |
/// | `completed`            | `false`           |
/// | `folder`               | `false`           |
/// | `cacheable`            | `false`           |
/// | `persisted`            | `false`           |
/// | `in_goose_fs_percentage` | `0`             |
/// | `in_memory_percentage` | `0`               |
/// | `mode`                 | `0`               |
/// | `mount_id`             | `0`               |
#[derive(Debug, Clone)]
pub struct URIStatus {
    // ── Identity ────────────────────────────────────────────────────────────
    /// Server-assigned inode ID.
    pub file_id: i64,
    /// Last path component (basename).
    pub name: String,
    /// Full path in the GooseFS namespace (e.g. `/data/my-file.parquet`).
    pub path: String,
    /// Underlying file-system path (empty for in-cache-only files).
    pub ufs_path: String,

    // ── Geometry ────────────────────────────────────────────────────────────
    /// Total length in bytes (`0` for directories).
    pub length: i64,
    /// Block size in bytes as configured on file creation.
    pub block_size_bytes: i64,
    /// Ordered list of block IDs belonging to this file.
    pub block_ids: Vec<i64>,

    // ── Timestamps ──────────────────────────────────────────────────────────
    /// Creation timestamp in milliseconds since Unix epoch.
    pub creation_time_ms: i64,
    /// Last modification timestamp in milliseconds since Unix epoch.
    pub last_modification_time_ms: i64,
    /// Last access timestamp in milliseconds since Unix epoch.
    pub last_access_time_ms: i64,

    // ── State flags ─────────────────────────────────────────────────────────
    /// Whether all blocks have been committed (`CompleteFile` was called).
    ///
    /// A file with `completed = false` is in `INCOMPLETE` state and cannot
    /// be opened for reading.
    pub completed: bool,
    /// Whether the path refers to a directory (inode is a `Directory`).
    pub folder: bool,
    /// Whether the data is allowed to be cached in GooseFS workers.
    pub cacheable: bool,
    /// Whether all data has been durably written to the UFS.
    pub persisted: bool,
    /// Whether this path is a mount-point.
    pub mount_point: bool,

    // ── Cache statistics ────────────────────────────────────────────────────
    /// Percentage of the file's data currently in GooseFS cache (`0`–`100`).
    pub in_goose_fs_percentage: i32,
    /// Percentage of the file's data in worker memory (hot tier).
    pub in_memory_percentage: i32,

    // ── Ownership & permissions ──────────────────────────────────────────────
    /// Owner user name.
    pub owner: String,
    /// Owner group name.
    pub group: String,
    /// POSIX mode bits (e.g. `0o644`).
    pub mode: i32,

    // ── Misc ────────────────────────────────────────────────────────────────
    /// Persistence state string (e.g. `"PERSISTED"`, `"NOT_PERSISTED"`).
    pub persistence_state: String,
    /// Mount-point ID for the UFS this path is backed by.
    pub mount_id: i64,
    /// UFS fingerprint (used for invalidation; may be empty).
    pub ufs_fingerprint: String,

    // ── xattr ───────────────────────────────────────────────────────────────
    /// Extended attributes (`key → raw bytes`).
    ///
    /// The special key `"innerWriteType"` encodes the `WriteType` that should
    /// be inherited by files created under this path.
    pub xattr: HashMap<String, Vec<u8>>,

    /// Symbolic-link target, if this inode is a symlink.
    pub symlink: Option<String>,

    // ── Block-info cache (private) ───────────────────────────────────────────
    /// `block_id → FileBlockInfo` lookup table.
    ///
    /// Built once from `FileInfo.file_block_infos` during `from_proto()`.
    /// Use [`get_block_info`] for O(1) access.
    block_infos: HashMap<i64, FileBlockInfo>,
}

impl URIStatus {
    // ── Construction ────────────────────────────────────────────────────────

    /// Convert a raw `FileInfo` proto message into a `URIStatus`.
    ///
    /// This is the **only** constructor.  It eagerly builds the
    /// `block_id → FileBlockInfo` map so that subsequent [`get_block_info`](URIStatus::get_block_info)
    /// calls are O(1).
    pub fn from_proto(fi: FileInfo) -> Self {
        // Build block-info map from the repeated file_block_infos field.
        // The key is the block_id stored in FileBlockInfo.block_info.block_id.
        let block_infos: HashMap<i64, FileBlockInfo> = fi
            .file_block_infos
            .into_iter()
            .filter_map(|fbi| {
                let id = fbi.block_info.as_ref()?.block_id?;
                Some((id, fbi))
            })
            .collect();

        Self {
            file_id: fi.file_id.unwrap_or(0),
            name: fi.name.unwrap_or_default(),
            path: fi.path.unwrap_or_default(),
            ufs_path: fi.ufs_path.unwrap_or_default(),
            length: fi.length.unwrap_or(0),
            block_size_bytes: fi.block_size_bytes.unwrap_or(0),
            block_ids: fi.block_ids,
            creation_time_ms: fi.creation_time_ms.unwrap_or(0),
            last_modification_time_ms: fi.last_modification_time_ms.unwrap_or(0),
            last_access_time_ms: fi.last_access_time_ms.unwrap_or(0),
            completed: fi.completed.unwrap_or(false),
            folder: fi.folder.unwrap_or(false),
            cacheable: fi.cacheable.unwrap_or(false),
            persisted: fi.persisted.unwrap_or(false),
            mount_point: fi.mount_point.unwrap_or(false),
            in_goose_fs_percentage: fi.in_goose_fs_percentage.unwrap_or(0),
            in_memory_percentage: fi.in_memory_percentage.unwrap_or(0),
            owner: fi.owner.unwrap_or_default(),
            group: fi.group.unwrap_or_default(),
            mode: fi.mode.unwrap_or(0),
            persistence_state: fi.persistence_state.unwrap_or_default(),
            mount_id: fi.mount_id.unwrap_or(0),
            ufs_fingerprint: fi.ufs_fingerprint.unwrap_or_default(),
            xattr: fi.xattr,
            symlink: fi.symlink,
            block_infos,
        }
    }

    // ── State helpers ────────────────────────────────────────────────────────

    /// `true` if the file has been fully written and committed.
    ///
    /// Files with `completed = false` are in `INCOMPLETE` state.
    /// They cannot be opened for reading.
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// `true` if the path refers to a directory.
    #[inline]
    pub fn is_folder(&self) -> bool {
        self.folder
    }

    /// `true` if the path is readable.
    ///
    /// A path is readable if it is a **completed file** OR a **directory**.
    /// An `INCOMPLETE` non-folder file is not readable.
    ///
    /// This matches the Java `FileSystem.exists()` semantics:
    /// - `inode exists && (folder || completed)` → readable
    #[inline]
    pub fn is_readable(&self) -> bool {
        self.folder || self.completed
    }

    /// `true` if the file is fully persisted to the underlying storage.
    #[inline]
    pub fn is_persisted(&self) -> bool {
        self.persisted
    }

    // ── Block-info lookup ────────────────────────────────────────────────────

    /// Look up the `FileBlockInfo` for a given block ID.
    ///
    /// Returns `None` if the block ID is not part of this file, or if the
    /// server did not return `file_block_infos` (e.g. metadata-only calls).
    ///
    /// # Performance
    ///
    /// O(1) — backed by a `HashMap` built during construction.
    #[inline]
    pub fn get_block_info(&self, block_id: i64) -> Option<&FileBlockInfo> {
        self.block_infos.get(&block_id)
    }

    /// Returns `true` if per-block metadata (`FileBlockInfo`) is available.
    ///
    /// The server only populates `file_block_infos` on `GetStatus` calls that
    /// request location info (T6/T5).  This is `false` for metadata-only
    /// results.
    #[inline]
    pub fn has_block_infos(&self) -> bool {
        !self.block_infos.is_empty()
    }

    /// Borrow the full `block_id → FileBlockInfo` map.
    ///
    /// Prefer [`get_block_info`](URIStatus::get_block_info) for single lookups.
    #[inline]
    pub fn block_infos(&self) -> &HashMap<i64, FileBlockInfo> {
        &self.block_infos
    }

    /// Add or replace a `FileBlockInfo` in the cache.
    ///
    /// Used when the caller refreshes block locations from the server
    /// after the initial `GetStatus` (e.g. block-miss retry).
    pub fn add_block_info(&mut self, fbi: FileBlockInfo) {
        if let Some(id) = fbi.block_info.as_ref().and_then(|bi| bi.block_id) {
            self.block_infos.insert(id, fbi);
        }
    }

    // ── Convenience accessors ────────────────────────────────────────────────

    /// Total number of blocks in this file.
    #[inline]
    pub fn block_count(&self) -> usize {
        self.block_ids.len()
    }

    /// Block index (0-based) that contains the given absolute byte offset.
    ///
    /// Returns `None` if `offset >= length` or `block_size_bytes == 0`.
    pub fn block_index_for_offset(&self, offset: i64) -> Option<usize> {
        if self.block_size_bytes <= 0 || offset >= self.length {
            return None;
        }
        Some((offset / self.block_size_bytes) as usize)
    }

    /// Offset within the containing block for a given absolute byte offset.
    ///
    /// Returns `None` if `offset >= length` or `block_size_bytes == 0`.
    pub fn offset_in_block(&self, offset: i64) -> Option<i64> {
        if self.block_size_bytes <= 0 || offset >= self.length {
            return None;
        }
        Some(offset % self.block_size_bytes)
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::grpc::{BlockInfo, WorkerNetAddress};

    fn make_file_block_info(block_id: i64, offset: i64) -> FileBlockInfo {
        FileBlockInfo {
            block_info: Some(BlockInfo {
                block_id: Some(block_id),
                length: Some(64 * 1024 * 1024),
                max_replicas: Some(1),
            }),
            offset: Some(offset),
            ufs_locations: vec![WorkerNetAddress {
                host: Some("worker1".to_string()),
                rpc_port: Some(9203),
                ..Default::default()
            }],
            ufs_string_locations: vec![],
        }
    }

    fn make_file_info() -> FileInfo {
        FileInfo {
            file_id: Some(42),
            name: Some("hello.txt".to_string()),
            path: Some("/data/hello.txt".to_string()),
            ufs_path: Some("cos://bucket/hello.txt".to_string()),
            length: Some(128 * 1024 * 1024), // 2 blocks of 64 MiB
            block_size_bytes: Some(64 * 1024 * 1024),
            creation_time_ms: Some(1_700_000_000_000),
            completed: Some(true),
            folder: Some(false),
            cacheable: Some(true),
            persisted: Some(true),
            block_ids: vec![1001, 1002],
            last_modification_time_ms: Some(1_700_000_001_000),
            owner: Some("alice".to_string()),
            group: Some("staff".to_string()),
            mode: Some(0o644),
            persistence_state: Some("PERSISTED".to_string()),
            mount_id: Some(7),
            in_goose_fs_percentage: Some(100),
            in_memory_percentage: Some(50),
            file_block_infos: vec![
                make_file_block_info(1001, 0),
                make_file_block_info(1002, 64 * 1024 * 1024),
            ],
            xattr: HashMap::from([("innerWriteType".to_string(), b"CACHE_THROUGH".to_vec())]),
            ..Default::default()
        }
    }

    #[test]
    fn test_from_proto_basic_fields() {
        let fi = make_file_info();
        let status = URIStatus::from_proto(fi);

        assert_eq!(status.file_id, 42);
        assert_eq!(status.name, "hello.txt");
        assert_eq!(status.path, "/data/hello.txt");
        assert_eq!(status.ufs_path, "cos://bucket/hello.txt");
        assert_eq!(status.length, 128 * 1024 * 1024);
        assert_eq!(status.block_size_bytes, 64 * 1024 * 1024);
        assert_eq!(status.block_ids, vec![1001, 1002]);
        assert!(status.completed);
        assert!(!status.folder);
        assert!(status.cacheable);
        assert!(status.persisted);
        assert_eq!(status.owner, "alice");
        assert_eq!(status.group, "staff");
        assert_eq!(status.mode, 0o644);
        assert_eq!(status.in_goose_fs_percentage, 100);
        assert_eq!(status.in_memory_percentage, 50);
        assert_eq!(status.mount_id, 7);
    }

    #[test]
    fn test_block_info_map_built_correctly() {
        let fi = make_file_info();
        let status = URIStatus::from_proto(fi);

        assert!(status.has_block_infos());
        assert_eq!(status.block_infos().len(), 2);

        let bi1 = status.get_block_info(1001).expect("block 1001 missing");
        assert_eq!(bi1.offset, Some(0));
        assert_eq!(bi1.block_info.as_ref().unwrap().block_id, Some(1001));

        let bi2 = status.get_block_info(1002).expect("block 1002 missing");
        assert_eq!(bi2.offset, Some(64 * 1024 * 1024));
    }

    #[test]
    fn test_get_block_info_missing_returns_none() {
        let fi = make_file_info();
        let status = URIStatus::from_proto(fi);
        assert!(status.get_block_info(9999).is_none());
    }

    #[test]
    fn test_add_block_info() {
        let fi = make_file_info();
        let mut status = URIStatus::from_proto(fi);

        let new_fbi = make_file_block_info(1003, 128 * 1024 * 1024);
        status.add_block_info(new_fbi);

        assert!(status.get_block_info(1003).is_some());
    }

    #[test]
    fn test_add_block_info_without_block_id_is_noop() {
        let fi = make_file_info();
        let mut status = URIStatus::from_proto(fi);
        let initial_count = status.block_infos().len();

        // FileBlockInfo with no block_info → should not be inserted
        let bad_fbi = FileBlockInfo {
            block_info: None,
            offset: Some(0),
            ufs_locations: vec![],
            ufs_string_locations: vec![],
        };
        status.add_block_info(bad_fbi);
        assert_eq!(status.block_infos().len(), initial_count);
    }

    #[test]
    fn test_is_readable_completed_file() {
        let fi = make_file_info(); // completed=true, folder=false
        let status = URIStatus::from_proto(fi);
        assert!(status.is_readable());
    }

    #[test]
    fn test_is_readable_folder() {
        let fi = FileInfo {
            completed: Some(false),
            folder: Some(true),
            ..Default::default()
        };
        let status = URIStatus::from_proto(fi);
        assert!(status.is_readable());
    }

    #[test]
    fn test_is_not_readable_incomplete_file() {
        let fi = FileInfo {
            completed: Some(false),
            folder: Some(false),
            ..Default::default()
        };
        let status = URIStatus::from_proto(fi);
        assert!(!status.is_readable());
    }

    #[test]
    fn test_block_index_for_offset() {
        let fi = make_file_info(); // length=128MiB, block_size=64MiB
        let status = URIStatus::from_proto(fi);

        assert_eq!(status.block_index_for_offset(0), Some(0));
        assert_eq!(status.block_index_for_offset(64 * 1024 * 1024 - 1), Some(0));
        assert_eq!(status.block_index_for_offset(64 * 1024 * 1024), Some(1));
        assert_eq!(
            status.block_index_for_offset(128 * 1024 * 1024 - 1),
            Some(1)
        );
        // offset == length → None
        assert_eq!(status.block_index_for_offset(128 * 1024 * 1024), None);
    }

    #[test]
    fn test_offset_in_block() {
        let fi = make_file_info();
        let status = URIStatus::from_proto(fi);

        assert_eq!(status.offset_in_block(0), Some(0));
        assert_eq!(status.offset_in_block(100), Some(100));
        assert_eq!(status.offset_in_block(64 * 1024 * 1024), Some(0));
        assert_eq!(status.offset_in_block(64 * 1024 * 1024 + 100), Some(100));
    }

    #[test]
    fn test_xattr_preserved() {
        let fi = make_file_info();
        let status = URIStatus::from_proto(fi);

        let val = status.xattr.get("innerWriteType").expect("xattr missing");
        assert_eq!(val, b"CACHE_THROUGH");
    }

    #[test]
    fn test_empty_file_info_defaults() {
        let status = URIStatus::from_proto(FileInfo::default());

        assert_eq!(status.file_id, 0);
        assert_eq!(status.name, "");
        assert_eq!(status.path, "");
        assert_eq!(status.length, 0);
        assert!(!status.completed);
        assert!(!status.folder);
        assert!(!status.is_readable());
        assert_eq!(status.block_count(), 0);
        assert!(!status.has_block_infos());
    }
}
