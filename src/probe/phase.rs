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

//! Probe phase name constants, aligned with Java `ProbePhase`.
//!
//! Server-side names must match the trailer keys emitted by
//! `ProbeTimingServerInterceptor` so Rust reports can be diffed against
//! `goosefs fs probe` / `copyFromLocal --probe` output.

/// Client-local phases recorded by this SDK (not present in the server trailer).
pub mod client {
    pub const POOL_ACQUIRE_US: &str = "pool_acquire_us";
    pub const MASTER_CONNECT_US: &str = "master_connect_us";
    pub const WORKER_CONNECT_US: &str = "worker_connect_us";
    pub const REQ_SERIALIZATION_US: &str = "req_serialization_us";
    pub const RESP_DESERIALIZATION_US: &str = "resp_deserialization_us";
    /// Block ReadBlock/WriteBlock stream construction (until the RPC is open).
    pub const OPEN_STREAM_US: &str = "open_stream_us";
    /// Short-circuit local mmap open + copy.
    pub const SHORT_CIRCUIT_US: &str = "short_circuit_us";
    /// Local page-cache batch lookup (not the miss fill / gRPC).
    pub const PAGE_CACHE_US: &str = "page_cache_us";
    /// Page-cache `on_file_open` / version-table check.
    pub const PAGE_CACHE_OPEN_US: &str = "page_cache_open_us";
    /// One copy into the gRPC `Chunk.data` `Vec<u8>` (typically `chunk_size`).
    pub const CHUNK_COPY_US: &str = "chunk_copy_us";
    /// Completing or holding the trailing `< chunk_size` coalescer buffer.
    pub const PENDING_CHUNK_US: &str = "pending_chunk_us";
    /// `mpsc` send of one `WriteRequest` chunk (backpressure = Worker not taking).
    pub const CHUNK_SEND_US: &str = "chunk_send_us";
    /// Flush command + wait for Worker `WriteResponse` ack.
    pub const FLUSH_ACK_US: &str = "flush_ack_us";
    /// Close the WriteBlock request stream (Worker `commitBlock`).
    pub const BLOCK_CLOSE_US: &str = "block_close_us";
    /// Consistent-hash worker pick before opening a block stream.
    pub const SELECT_WORKER_US: &str = "select_worker_us";
    /// UFS WriteBlock `flush()` during file `close()` (CACHE_THROUGH / THROUGH).
    pub const UFS_FLUSH_US: &str = "ufs_flush_us";
    /// UFS WriteBlock stream `close()` during file `close()`.
    pub const UFS_CLOSE_US: &str = "ufs_close_us";
    /// Last cache block `flush` + `commitBlock` during file `close()`.
    pub const LAST_BLOCK_CLOSE_US: &str = "last_block_close_us";
    /// Master `ScheduleAsyncPersistence` after CompleteFile (ASYNC_THROUGH).
    pub const ASYNC_PERSIST_US: &str = "async_persist_us";
    /// Drop cached FileInfo after CompleteFile.
    pub const INVALIDATE_META_US: &str = "invalidate_meta_us";
}

/// Master-side phases from `ProbeTimingInfo.sub_timings_us`.
pub mod master {
    pub const RPC_FRAMEWORK_US: &str = "rpc_framework_us";
    pub const RPC_CALLABLE_US: &str = "rpc_callable_us";
    pub const RPC_RESPONSE_US: &str = "rpc_response_us";
    pub const REQ_DESERIALIZATION_US: &str = "req_deserialization_us";
    pub const CREATE_RPC_CONTEXT_US: &str = "create_rpc_context_us";
    pub const SYNC_METADATA_US: &str = "sync_metadata_us";
    pub const LOAD_METADATA_US: &str = "load_metadata_us";
    pub const LOCK_INODE_PATH_US: &str = "lock_inode_path_us";
    pub const GET_FILE_INFO_INTERNAL_US: &str = "get_file_info_internal_us";
    pub const RESP_SERIALIZATION_US: &str = "resp_serialization_us";
    pub const POPULATE_CAPABILITY_US: &str = "populate_capability_us";
    pub const CREATE_AUDIT_CONTEXT_US: &str = "create_audit_context_us";
    pub const COMPLETE_FILE_INTERNAL_US: &str = "complete_file_internal_us";
    pub const CREATE_FILE_INTERNAL_US: &str = "create_file_internal_us";
    pub const RENAME_INTERNAL_US: &str = "rename_internal_us";
    pub const CREATE_DIRECTORY_INTERNAL_US: &str = "create_directory_internal_us";
    pub const DELETE_INTERNAL_US: &str = "delete_internal_us";
}

/// Worker-side phases from `ProbeTimingInfo.sub_timings_us`.
pub mod worker {
    pub const LOCK_BLOCK_US: &str = "lock_block_us";
    pub const OPEN_BLOCK_US: &str = "open_block_us";
    pub const OPEN_BLOCK_COUNT: &str = "open_block_count";
    pub const DATA_TRANSFER_US: &str = "data_transfer_us";
    pub const READ_IO_COUNT: &str = "read_io_count";
    pub const GET_DATA_BUFFER_COUNT: &str = "get_data_buffer_count";
    pub const RESPONSE_SEND_US: &str = "response_send_us";
    pub const READ_TYPE: &str = "read_type";
    pub const CREATE_BLOCK_REMOTE_US: &str = "create_block_remote_us";
    pub const COMMIT_BLOCK_US: &str = "commit_block_us";
    pub const REQUEST_SPACE_US: &str = "request_space_us";
    pub const GET_TEMP_WRITER_US: &str = "get_temp_writer_us";
    pub const WRITE_TYPE: &str = "write_type";
    /// Worker block store layout (0=BLOCK `LocalFileBlockWriter`, 1=PAGE `PagedBlockWriter`).
    pub const STORE_TYPE: &str = "store_type";
    pub const DATA_WRITE_LOCAL_US: &str = "data_write_local_us";
    /// `FileChannel.map` for one CACHE chunk (child of `data_write_local_us`).
    pub const DATA_WRITE_MMAP_US: &str = "data_write_mmap_us";
    /// Copy into the mapped buffer / page cache (child of `data_write_local_us`).
    pub const DATA_WRITE_COPY_US: &str = "data_write_copy_us";
    /// Unmap / `cleanDirectBuffer` (child of `data_write_local_us`).
    pub const DATA_WRITE_UNMAP_US: &str = "data_write_unmap_us";
    /// `FileChannel.write` gather-write path (child of `data_write_local_us`).
    pub const DATA_WRITE_CHANNEL_US: &str = "data_write_channel_us";
    /// Writer-thread idle waiting for the next client chunk/command.
    pub const AWAIT_CHUNK_US: &str = "await_chunk_us";
    /// SerializingExecutor hop after the previous write task finished.
    pub const EXECUTOR_WAIT_US: &str = "executor_wait_us";
    /// Protobuf `Chunk` `ByteString` → `DataBuffer` (includes any copy).
    pub const CHUNK_BUFFER_US: &str = "chunk_buffer_us";
    /// `BlockDataRateManager.tryApplyBandwidth` wait.
    pub const RATE_LIMIT_US: &str = "rate_limit_us";
    /// Worker `flush()`: CACHE `FileChannel.force` or UFS `OutputStream.flush`.
    pub const FLUSH_US: &str = "flush_us";
    /// `BlockWriter.close()` before `commitBlock`.
    pub const CLOSE_WRITER_US: &str = "close_writer_us";
    pub const DATA_WRITE_UFS_US: &str = "data_write_ufs_us";
    pub const CREATE_UFS_FILE_US: &str = "create_ufs_file_us";
    pub const COMPLETE_UFS_FILE_US: &str = "complete_ufs_file_us";
}

/// Display type used by the report formatter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisplayType {
    Duration,
    Count,
    TypeMarker,
}

/// Returns how a server/client sub-timing key should be rendered.
pub fn display_type(key: &str) -> DisplayType {
    match key {
        worker::WRITE_TYPE | worker::READ_TYPE | worker::STORE_TYPE => DisplayType::TypeMarker,
        worker::OPEN_BLOCK_COUNT | worker::READ_IO_COUNT | worker::GET_DATA_BUFFER_COUNT => {
            DisplayType::Count
        }
        _ => DisplayType::Duration,
    }
}

/// Top-level (non-overlapping) Master server phases.
pub fn master_level1() -> &'static [&'static str] {
    &[
        master::RPC_CALLABLE_US,
        master::RPC_RESPONSE_US,
        master::RPC_FRAMEWORK_US,
    ]
}

/// Children of `rpc_callable_us`.
pub fn master_callable_children() -> &'static [&'static str] {
    &[
        master::CREATE_FILE_INTERNAL_US,
        master::COMPLETE_FILE_INTERNAL_US,
        master::RENAME_INTERNAL_US,
        master::CREATE_DIRECTORY_INTERNAL_US,
        master::DELETE_INTERNAL_US,
        master::GET_FILE_INFO_INTERNAL_US,
        master::LOCK_INODE_PATH_US,
        master::SYNC_METADATA_US,
        master::LOAD_METADATA_US,
        master::CREATE_RPC_CONTEXT_US,
        master::CREATE_AUDIT_CONTEXT_US,
        master::POPULATE_CAPABILITY_US,
        master::REQ_DESERIALIZATION_US,
        master::RESP_SERIALIZATION_US,
    ]
}

/// Strip a trailing `_us` / `_ms` suffix for display, matching Java
/// `ProbeReportFormatter.stripUnitSuffix`.
pub fn strip_unit_suffix(key: &str) -> &str {
    key.strip_suffix("_us")
        .or_else(|| key.strip_suffix("_ms"))
        .unwrap_or(key)
}

/// Human-readable report label. Trailer keys are unchanged.
pub fn display_label(key: &str) -> &str {
    match strip_unit_suffix(key) {
        "data_write_local" => "data_write_local (BlockWriter.append)",
        "data_write_mmap" => "data_write_mmap (FileChannel.map)",
        "data_write_copy" => "data_write_copy (memcpy into mapped pages)",
        "data_write_unmap" => "data_write_unmap (unmap / cleanDirectBuffer)",
        "data_write_channel" => "data_write_channel (FileChannel.write)",
        "await_chunk" => "await_chunk (writer idle for next client chunk)",
        "executor_wait" => "executor_wait (serializing executor hop)",
        "chunk_buffer" => "chunk_buffer (ByteString → DataBuffer)",
        "flush" => "flush (FileChannel.force / UFS flush)",
        "close_writer" => "close_writer (BlockWriter.close)",
        "rate_limit" => "rate_limit (tryApplyBandwidth wait)",
        "chunk_send" => "chunk_send (gRPC send / Worker backpressure)",
        other => other,
    }
}

/// Remainder inside Master `rpc_callable` not covered by named children.
pub fn inner_gap_master_callable() -> &'static str {
    "(inner_gap) (unscoped Master RPC body)"
}

/// Remainder of Master `server_total` outside `rpc_callable` / `rpc_response` / `rpc_framework`.
pub fn inner_gap_master_top() -> &'static str {
    "(inner_gap) (gRPC interceptor / framework)"
}

/// Label for [`worker::DATA_WRITE_LOCAL_US`] from `store_type` and mmap children.
///
/// Page store (`PagedBlockWriter`) copies into `CacheManager` temp pages.
/// Block store (`LocalFileBlockWriter`) mmaps the local block file.
pub fn data_write_local_label(
    store_type: Option<i64>,
    has_block_append_children: bool,
) -> &'static str {
    match store_type {
        Some(1) => "data_write_local (page store: CacheManager.append temp pages)",
        Some(_) => "data_write_local (block store: mmap + copy into local file, no fsync)",
        None if has_block_append_children => {
            "data_write_local (block store: mmap + copy into local file, no fsync)"
        }
        None => "data_write_local (BlockWriter.append)",
    }
}

/// Remainder of `data_write_local` not covered by named children.
pub fn inner_gap_data_write_local(page_store: bool) -> &'static str {
    if page_store {
        "(inner_gap) (unscoped CacheManager.append / page copy)"
    } else {
        "(inner_gap) (unscoped append beyond mmap/copy/unmap)"
    }
}

/// Remainder of Worker WriteBlock processing not covered by named trailer phases.
///
/// When the Worker has not emitted `await_chunk` / `executor_wait` / `flush`,
/// that remainder is almost always idle waiting for the next client chunk
/// (overlaps Client Local `chunk_send`) plus the serializing executor hop.
pub fn inner_gap_worker_write(ufs: bool, pipeline_split: bool) -> &'static str {
    match (ufs, pipeline_split) {
        (true, true) => "(inner_gap) (unscoped UFS handler / gRPC decode)",
        (true, false) => "(inner_gap) (await client chunk + unscoped UFS pipeline)",
        (false, true) => "(inner_gap) (unscoped: gRPC decode / access check / handler glue)",
        (false, false) => "(inner_gap) (await client chunk + executor + unscoped pipeline)",
    }
}

/// Connect phases recorded *inside* [`client::POOL_ACQUIRE_US`].
///
/// `WorkerClientPool::acquire` / `MasterClientPool::pick` wrap the TCP
/// connect, so these must not be added again when summing Client Local.
pub fn nested_in_pool_acquire(key: &str) -> bool {
    key == client::WORKER_CONNECT_US || key == client::MASTER_CONNECT_US
}

/// Sub-phases of [`worker::DATA_WRITE_LOCAL_US`]. Not summed into Worker inner_gap.
pub fn nested_in_data_write_local(key: &str) -> bool {
    matches!(
        key,
        worker::DATA_WRITE_MMAP_US
            | worker::DATA_WRITE_COPY_US
            | worker::DATA_WRITE_UNMAP_US
            | worker::DATA_WRITE_CHANNEL_US
    )
}
