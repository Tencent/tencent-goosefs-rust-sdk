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

//! Options structs for Goosefs file-system operations.
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
/// a *different* Java proto value that is **not** exposed by Goosefs.
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
/// Passed to [`crate::io::GoosefsFileInStream`] via
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
    /// Set automatically by `GoosefsFileInStream` when choosing the
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
// UFS block geometry
// ---------------------------------------------------------------------------

/// Length of the block at `block_index`, clamped to what the file actually
/// holds.
///
/// The result belongs in `OpenUfsBlockOptions.block_size`, which despite its
/// name carries the *real* length of that one block, not the file's nominal
/// block size. The two differ for the trailing partial block of every file —
/// and for anything smaller than one block, the trailing block is the whole
/// file.
///
/// # Java authority
///
/// `InStreamOptions.getOpenUfsBlockOptions`:
///
/// ```java
/// long blockSize = Math.min(
///     mStatus.getLength() - mStatus.getBlockSizeBytes() * BlockId.getSequenceNumber(blockId),
///     mStatus.getBlockSizeBytes());
/// BlockInfo info = new BlockInfo().setBlockId(blockId).setLength(blockSize);
/// ... .setBlockSize(info.getLength())
/// ```
///
/// # Why the nominal size is not good enough
///
/// The Worker takes this field as the block length. `PagedBlockStore.cacheBlock`
/// builds `BlockPageId{FileId=paged_block_<id>_size_<block_size>}` and then asks
/// the UFS for that many bytes. Over-reporting makes the read come up short:
///
/// ```text
/// ERROR LocalCacheManager - Failed to read page
///   BlockPageId{FileId=paged_block_503316480_size_1048576, PageIndex=0}:
///   supposed to read 1048576 bytes, 13 bytes actually read
/// WARN  PagedBlockStore - Failed to cache block 503316480 in page mode
/// ```
///
/// Reads still return correct data — the served byte range comes from
/// `ReadRequest.offset` / `length`, not from this field — but the async-cache
/// task gives up, so the tail block never enters the page cache and every read
/// of it goes back to the UFS.
pub fn ufs_block_length(file_length: i64, block_size_bytes: i64, block_index: u64) -> i64 {
    if block_size_bytes <= 0 {
        return 0;
    }
    let block_start = i64::try_from(block_index)
        .ok()
        .and_then(|idx| idx.checked_mul(block_size_bytes))
        .unwrap_or(i64::MAX);
    file_length
        .saturating_sub(block_start)
        .clamp(0, block_size_bytes)
}

// ---------------------------------------------------------------------------
// OpenFileOptions
// ---------------------------------------------------------------------------

/// Options for opening a Goosefs file for reading.
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

/// Options for creating a new Goosefs file.
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
/// - `unchecked`    — skip the UFS-vs-namespace consistency check on
///   recursive deletes of persisted directories, skip the "directory must
///   be empty" check, and allow deleting **INCOMPLETE** files. Java
///   `FileSystemOptions.deleteDefaults` sets this from
///   `goosefs.user.file.delete.unchecked` (default **true**).
/// - `goosefs_only` — remove the path only from the Goosefs namespace; do NOT
///   propagate the delete to the underlying UFS.  Used in CACHE_THROUGH error
///   recovery: when `completeFile` fails after UFS `close` succeeded, we
///   must remove the Goosefs-side metadata without touching the already-written
///   UFS file.
///
/// # Note on Go SDK gap
///
/// The Go SDK's `DeleteOptions` struct does **not** expose `goosefs_only`.
/// The field exists in the proto and is read by the Java server.  Rust must
/// pass it correctly.
#[derive(Debug, Clone)]
pub struct DeleteOptions {
    /// Delete directories recursively.  Required when the target is a
    /// non-empty directory.
    pub recursive: bool,

    /// Skip the UFS consistency check on persisted directory trees (Java
    /// `goosefs.user.file.delete.unchecked`, default true). Also skips the
    /// empty-directory check and allows deleting INCOMPLETE files.
    pub unchecked: bool,

    /// Restrict deletion to the Goosefs namespace only; do not propagate to
    /// the underlying storage (UFS).  Used during CACHE_THROUGH error recovery.
    pub goosefs_only: bool,
}

impl Default for DeleteOptions {
    /// Matches Java `deleteDefaults`: non-recursive, `unchecked=true`,
    /// propagate to UFS.
    fn default() -> Self {
        Self {
            recursive: false,
            unchecked: true,
            goosefs_only: false,
        }
    }
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
    /// remove the Goosefs metadata entry without deleting the already-written
    /// UFS file.
    pub fn goosefs_only_unchecked() -> Self {
        Self {
            recursive: false,
            unchecked: true,
            goosefs_only: true,
        }
    }
}

// ---------------------------------------------------------------------------
// GetStatusOptions / ListStatusOptions
// ---------------------------------------------------------------------------

/// Per-call options for [`crate::fs::FileSystem::get_status_with_options`].
///
/// `None` fields fall back to [`crate::config::GoosefsConfig`].
#[derive(Debug, Clone, Default)]
pub struct GetStatusOptions {
    /// `None` = `GoosefsConfig::file_metadata_sync_interval`.
    /// `Some(0)` = this call skips the metadata cache.
    pub sync_interval_ms: Option<i64>,
    /// `None` = `GoosefsConfig::file_metadata_load_type` (default `ONCE`).
    /// Sent on the Master `GetStatus` RPC (Java `getStatusDefaults`).
    pub load_metadata_type: Option<crate::proto::grpc::file::LoadMetadataPType>,
}

impl GetStatusOptions {
    /// Force this `get_status` to skip the cache (`sync_interval_ms = 0`).
    pub fn always_sync() -> Self {
        Self {
            sync_interval_ms: Some(0),
            load_metadata_type: None,
        }
    }
}

/// Per-call options for [`crate::fs::FileSystem::list_status_with_options`].
///
/// `None` fields fall back to [`crate::config::GoosefsConfig`].
/// `load_metadata_only` is per-call only (Java has no config key).
#[derive(Debug, Clone)]
pub struct ListStatusOptions {
    /// Recurse into child directories. Recursive listings never use the cache.
    pub recursive: bool,
    /// `None` = `GoosefsConfig::file_metadata_sync_interval`.
    pub sync_interval_ms: Option<i64>,
    /// `None` = `GoosefsConfig::file_metadata_load_type` (default `ONCE`).
    /// Sent on the Master RPC for both recursive and non-recursive listings
    /// (Java `listStatusDefaults`). `ALWAYS` also skips the listing cache.
    pub load_metadata_type: Option<crate::proto::grpc::file::LoadMetadataPType>,
    /// When true, skip the listing cache. Per-call only.
    pub load_metadata_only: bool,
}

impl Default for ListStatusOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            sync_interval_ms: None,
            load_metadata_type: None,
            load_metadata_only: false,
        }
    }
}

impl ListStatusOptions {
    /// Non-recursive listing (default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Recursive listing — always bypasses the metadata cache.
    pub fn recursive() -> Self {
        Self {
            recursive: true,
            ..Self::default()
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
        assert!(
            opts.unchecked,
            "Java USER_FILE_DELETE_UNCHECKED default is true"
        );
        assert!(!opts.goosefs_only);
    }

    #[test]
    fn test_recursive_helper() {
        let opts = DeleteOptions::recursive();
        assert!(opts.recursive);
        assert!(
            opts.unchecked,
            "recursive() inherits Default.unchecked = true"
        );
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

    // ── ufs_block_length ───────────────────────────────────────────────────

    /// Java `InStreamOptions.getOpenUfsBlockOptions` clamps the advertised
    /// block length with `Math.min(length - blockSize * seq, blockSize)`.
    /// Sending the nominal size instead makes the Worker's async-cache task
    /// read past the end of the object and drop the block
    /// ("Failed to cache block <id> in page mode").
    #[test]
    fn ufs_block_length_clamps_the_tail_block() {
        let bs = 1 << 20;

        // Full interior blocks report the nominal size.
        assert_eq!(ufs_block_length(3 * bs, bs, 0), bs);
        assert_eq!(ufs_block_length(3 * bs, bs, 1), bs);

        // A file that is an exact multiple has no partial tail.
        assert_eq!(ufs_block_length(3 * bs, bs, 2), bs);

        // Partial tail: 2 MiB + 100 B over 1 MiB blocks.
        assert_eq!(ufs_block_length(2 * bs + 100, bs, 0), bs);
        assert_eq!(ufs_block_length(2 * bs + 100, bs, 2), 100);
    }

    /// The Lance metadata files that surfaced this bug are far smaller than one
    /// block, so block 0 *is* the tail: a 13-byte `latest_version_hint.json`
    /// was advertised as 1048576 bytes.
    #[test]
    fn ufs_block_length_reports_actual_length_for_sub_block_files() {
        let bs = 1 << 20;
        assert_eq!(ufs_block_length(13, bs, 0), 13);
        assert_eq!(ufs_block_length(447, bs, 0), 447);
        assert_eq!(ufs_block_length(0, bs, 0), 0);
    }

    /// Out-of-range indices and a missing block size must not produce a
    /// negative length — the field is sent to the Worker as-is.
    #[test]
    fn ufs_block_length_never_goes_negative() {
        let bs = 1 << 20;
        assert_eq!(ufs_block_length(13, bs, 5), 0);
        assert_eq!(ufs_block_length(13, 0, 0), 0);
        assert_eq!(ufs_block_length(13, -1, 0), 0);
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

    #[test]
    fn test_get_status_options_always_sync() {
        let opts = GetStatusOptions::always_sync();
        assert_eq!(opts.sync_interval_ms, Some(0));
        assert!(opts.load_metadata_type.is_none());
    }

    #[test]
    fn test_list_status_options_recursive_skips_defaults() {
        let opts = ListStatusOptions::recursive();
        assert!(opts.recursive);
        assert!(opts.sync_interval_ms.is_none());
        assert!(!opts.load_metadata_only);
    }
}
