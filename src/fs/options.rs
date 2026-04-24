//! Options structs for GooseFS file-system operations.
//!
//! These types are the Rust-native layer that sits in front of the raw proto
//! options (`DeletePOptions`, etc.) and are exposed in the public API.
//!
//! Wave 1 adds:
//! - [`DeleteOptions`] — T3
//!
//! Wave 2 adds:
//! - [`ReadType`]         — T9
//! - [`OpenFileOptions`]  — T9
//! - [`InStreamOptions`]  — T9
//! - [`CreateFileOptions`] — xattr inheritance

use crate::fs::write_type::WriteTypeXAttr;

// ---------------------------------------------------------------------------
// ReadType
// ---------------------------------------------------------------------------

/// Cache strategy for reading a file.
///
/// # Java authority
///
/// Verified against `alluxio.grpc.ReadPType` enum in the proto.  The Java
/// proto defines exactly **two** values: `NO_CACHE = 1`, `CACHE = 2`.
/// The Go SDK also defines `ReadTypeCachePromote` (=2 in Go) but that maps to
/// a *different* Java proto value that is **not** exposed by GooseFS.
/// We only expose `NoCache` and `Cache`.
///
/// | Variant   | Proto value | Description                                  |
/// |-----------|-------------|----------------------------------------------|
/// | `NoCache` | `1`         | Read data without caching it in workers.     |
/// | `Cache`   | `2`         | Read and cache data in the nearest worker.   |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadType {
    /// Read data but do **not** cache it in workers.
    ///
    /// Use for one-off access patterns or large scans where caching would
    /// pollute the cache without benefit.
    NoCache,

    /// Read data and cache it in the nearest worker (default).
    ///
    /// Subsequent reads of the same block from the same or nearby workers
    /// will be served from cache without going to UFS.
    #[default]
    Cache,
}

impl ReadType {
    /// Convert to the proto integer value (`ReadPType`).
    ///
    /// The raw value is sent in `ReadRequest` → `OpenUfsBlockOptions`.
    pub fn to_proto(self) -> i32 {
        match self {
            ReadType::NoCache => 1,
            ReadType::Cache => 2,
        }
    }
}

// ---------------------------------------------------------------------------
// InStreamOptions
// ---------------------------------------------------------------------------

/// Options controlling how an open file stream reads data.
///
/// Passed to [`crate::io::GooseFsFileInStream`] via
/// [`OpenFileOptions`].
///
/// # Defaults (match Java client defaults)
///
/// - `read_type` — `Cache`
/// - `position_short` — `false`
/// - `max_ufs_read_concurrency` — `8`
/// - `prefetch_window` — `1`
#[derive(Debug, Clone)]
pub struct InStreamOptions {
    /// Cache strategy for this read.
    pub read_type: ReadType,

    /// Hint: this is a short / random read.
    ///
    /// When `true`, the underlying `ReadRequest` sets `position_short = true`,
    /// which tells the Worker to skip prefetching and serve the request
    /// directly from UFS or cache without eviction.
    ///
    /// Set automatically by `GooseFsFileInStream` when choosing the
    /// positioned-read path.
    pub position_short: bool,

    /// Maximum number of concurrent UFS read threads the worker may use
    /// for this stream.  `8` matches the Java client default.
    pub max_ufs_read_concurrency: i32,

    /// Initial prefetch window (number of chunks to prefetch).
    ///
    /// `1` = no prefetch beyond current chunk.  The stream may adapt this
    /// value dynamically based on observed access pattern.
    pub prefetch_window: i32,
}

impl Default for InStreamOptions {
    fn default() -> Self {
        Self {
            read_type: ReadType::Cache,
            position_short: false,
            max_ufs_read_concurrency: 8,
            prefetch_window: 1,
        }
    }
}

impl InStreamOptions {
    /// Create a no-cache read options instance.
    pub fn no_cache() -> Self {
        Self {
            read_type: ReadType::NoCache,
            ..Default::default()
        }
    }

    /// Mark this stream as a positioned (random-access) read.
    ///
    /// Sets `position_short = true` to tell the worker to skip prefetch.
    pub fn positioned(mut self) -> Self {
        self.position_short = true;
        self
    }
}

// ---------------------------------------------------------------------------
// OpenFileOptions
// ---------------------------------------------------------------------------

/// Options for opening a GooseFS file for reading.
///
/// # Example
///
/// ```rust
/// use goosefs_sdk::fs::options::{OpenFileOptions, ReadType};
///
/// // Default: cache the data on read
/// let opts = OpenFileOptions::default();
///
/// // Explicitly disable caching for a scan
/// let no_cache = OpenFileOptions {
///     in_stream_options: goosefs_sdk::fs::options::InStreamOptions::no_cache(),
/// };
/// ```
#[derive(Debug, Clone, Default)]
pub struct OpenFileOptions {
    /// Options forwarded to the underlying file input stream.
    pub in_stream_options: InStreamOptions,
}

impl OpenFileOptions {
    /// Create options with default in-stream settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create options that disable worker-side caching.
    pub fn no_cache() -> Self {
        Self {
            in_stream_options: InStreamOptions::no_cache(),
        }
    }
}

// ---------------------------------------------------------------------------
// CreateFileOptions
// ---------------------------------------------------------------------------

/// Options for creating a new GooseFS file.
///
/// # WriteType inheritance
///
/// If `write_type` is [`WriteTypeXAttr::Inherit`] (the default), the caller
/// must resolve the effective `WriteType` by inspecting the parent directory's
/// `"innerWriteType"` xattr before creating the file.
///
/// See [`crate::fs::write_type::get_write_type_from_xattr`].
#[derive(Debug, Clone, Default)]
pub struct CreateFileOptions {
    /// Write strategy for the new file.
    ///
    /// `Inherit` (default) → look up parent directory xattr.
    /// `Explicit(wt)` → override with `wt`, skip xattr lookup.
    pub write_type: WriteTypeXAttr,

    /// Block size in bytes.  `None` → use server/config default.
    pub block_size_bytes: Option<i64>,

    /// Replication factor.  `None` → use server default.
    pub replication_max: Option<i32>,

    /// Whether to create intermediate directories.  Defaults to `false`.
    pub recursive: bool,
}

impl CreateFileOptions {
    /// Create options with an explicit `WriteType`, bypassing xattr lookup.
    pub fn with_write_type(wt: crate::config::WriteType) -> Self {
        Self {
            write_type: WriteTypeXAttr::Explicit(wt),
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// DeleteOptions
// ---------------------------------------------------------------------------

/// Options controlling how a file or directory is deleted.
///
/// # Proto mapping
///
/// Maps to `DeletePOptions` in `file_system_master.proto`:
/// - `recursive`    → `DeletePOptions.recursive`
/// - `unchecked`    → `DeletePOptions.unchecked`
/// - `goosefs_only` → `DeletePOptions.goosefs_only`
///
/// # Java authority
///
/// Verified against `DefaultFileSystemMaster.delete()`:
/// - `recursive`    — delete directory tree recursively.
/// - `unchecked`    — skip the "directory must be empty" check and also allow
///   deletion of **INCOMPLETE** files (files still being written).  Required
///   for `cancel()` to clean up an in-progress write.
/// - `goosefs_only` — remove the path only from the GooseFS namespace; do NOT
///   propagate the delete to the underlying UFS.  Used in CACHE_THROUGH error
///   recovery: when `completeFile` fails after UFS `close` succeeded, we
///   must remove the GooseFS-side metadata without touching the already-written
///   UFS file.
///
/// # Note on Go SDK gap
///
/// The Go SDK's `DeleteOptions` struct does **not** expose `goosefs_only`.
/// The field exists in the proto and is read by the Java server.  Rust must
/// pass it correctly.
#[derive(Debug, Clone, Default)]
pub struct DeleteOptions {
    /// Delete directories recursively.  Required when the target is a
    /// non-empty directory.
    pub recursive: bool,

    /// Skip safety checks (empty-directory enforcement) and allow deleting
    /// files in INCOMPLETE state.  Needed by `GooseFsFileWriter::cancel()`.
    pub unchecked: bool,

    /// Restrict deletion to the GooseFS namespace only; do not propagate to
    /// the underlying storage (UFS).  Used during CACHE_THROUGH error recovery.
    pub goosefs_only: bool,
}

impl DeleteOptions {
    /// Create options for a simple recursive delete (the most common case).
    pub fn recursive() -> Self {
        Self {
            recursive: true,
            ..Default::default()
        }
    }

    /// Create options for cancelling an in-progress file write.
    ///
    /// Sets `unchecked = true` so the Master accepts deletion of an INCOMPLETE
    /// file without raising `FileIncompleteException`.
    pub fn for_cancel() -> Self {
        Self {
            recursive: false,
            unchecked: true,
            goosefs_only: false,
        }
    }

    /// Create options for CACHE_THROUGH error recovery.
    ///
    /// After UFS `close()` succeeds but `completeFile` fails, the caller must
    /// remove the GooseFS metadata entry without deleting the already-written
    /// UFS file.
    pub fn goosefs_only_unchecked() -> Self {
        Self {
            recursive: false,
            unchecked: true,
            goosefs_only: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WriteType;
    use crate::fs::write_type::WriteTypeXAttr;

    // ── DeleteOptions ──────────────────────────────────────────────────────

    #[test]
    fn test_default_delete_options() {
        let opts = DeleteOptions::default();
        assert!(!opts.recursive);
        assert!(!opts.unchecked);
        assert!(!opts.goosefs_only);
    }

    #[test]
    fn test_recursive_helper() {
        let opts = DeleteOptions::recursive();
        assert!(opts.recursive);
        assert!(!opts.unchecked);
        assert!(!opts.goosefs_only);
    }

    #[test]
    fn test_for_cancel_helper() {
        let opts = DeleteOptions::for_cancel();
        assert!(!opts.recursive);
        assert!(opts.unchecked);
        assert!(!opts.goosefs_only);
    }

    #[test]
    fn test_goosefs_only_unchecked_helper() {
        let opts = DeleteOptions::goosefs_only_unchecked();
        assert!(!opts.recursive);
        assert!(opts.unchecked);
        assert!(opts.goosefs_only);
    }

    // ── ReadType ───────────────────────────────────────────────────────────

    #[test]
    fn test_read_type_default_is_cache() {
        assert_eq!(ReadType::default(), ReadType::Cache);
    }

    #[test]
    fn test_read_type_proto_values() {
        assert_eq!(ReadType::NoCache.to_proto(), 1);
        assert_eq!(ReadType::Cache.to_proto(), 2);
    }

    // ── InStreamOptions ────────────────────────────────────────────────────

    #[test]
    fn test_in_stream_defaults() {
        let opts = InStreamOptions::default();
        assert_eq!(opts.read_type, ReadType::Cache);
        assert!(!opts.position_short);
        assert_eq!(opts.max_ufs_read_concurrency, 8);
        assert_eq!(opts.prefetch_window, 1);
    }

    #[test]
    fn test_in_stream_no_cache() {
        let opts = InStreamOptions::no_cache();
        assert_eq!(opts.read_type, ReadType::NoCache);
    }

    #[test]
    fn test_in_stream_positioned() {
        let opts = InStreamOptions::default().positioned();
        assert!(opts.position_short);
    }

    // ── OpenFileOptions ────────────────────────────────────────────────────

    #[test]
    fn test_open_file_default() {
        let opts = OpenFileOptions::default();
        assert_eq!(opts.in_stream_options.read_type, ReadType::Cache);
    }

    #[test]
    fn test_open_file_no_cache() {
        let opts = OpenFileOptions::no_cache();
        assert_eq!(opts.in_stream_options.read_type, ReadType::NoCache);
    }

    // ── CreateFileOptions ──────────────────────────────────────────────────

    #[test]
    fn test_create_file_default_inherits() {
        let opts = CreateFileOptions::default();
        assert_eq!(opts.write_type, WriteTypeXAttr::Inherit);
        assert!(opts.block_size_bytes.is_none());
        assert!(!opts.recursive);
    }

    #[test]
    fn test_create_file_with_write_type() {
        let opts = CreateFileOptions::with_write_type(WriteType::CacheThrough);
        assert_eq!(
            opts.write_type,
            WriteTypeXAttr::Explicit(WriteType::CacheThrough)
        );
    }
}
