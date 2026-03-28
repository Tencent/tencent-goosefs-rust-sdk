//! High-level file writer that orchestrates the complete write pipeline.
//!
//! `GooseFsFileWriter` ties together all low-level components into a single
//! easy-to-use API, analogous to Java's `GooseFSFileOutStream`:
//!
//! ```text
//! GooseFsFileWriter::create(path, data)
//!   → MasterClient.create_file()
//!   → BlockMapper.plan_write()
//!   → for each block:
//!       → WorkerRouter.select_worker()
//!       → WorkerClient.connect()
//!       → GrpcBlockWriter.open() → write_all() → flush() → close()
//!   → MasterClient.complete_file()
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use goosefs_client::io::GooseFsFileWriter;
//! use goosefs_client::config::GooseFsConfig;
//!
//! # async fn example() -> goosefs_client::error::Result<()> {
//! let config = GooseFsConfig::new("127.0.0.1:9200");
//! let data = b"Hello, GooseFS!";
//!
//! // One-shot write
//! GooseFsFileWriter::write_file(&config, "/my-file.txt", data).await?;
//!
//! // Or use the builder for more control
//! let mut writer = GooseFsFileWriter::create(&config, "/my-file.txt").await?;
//! writer.write(data).await?;
//! writer.close().await?;
//! # Ok(())
//! # }
//! ```

use tracing::{debug, info, warn};

use crate::block::mapper::BlockWritePlan;
use crate::block::router::WorkerRouter;
use crate::client::master::default_file_mode;
use crate::client::worker::WriteBlockOptions;
use crate::client::{MasterClient, WorkerClient, WorkerManagerClient};
use crate::config::GooseFsConfig;
use crate::error::{Error, Result};
use crate::io::writer::GrpcBlockWriter;
use crate::proto::grpc::block::RequestType;
use crate::proto::grpc::file::{CreateFilePOptions, FileInfo};
use crate::proto::proto::dataserver::CreateUfsFileOptions;

/// Write strategy derived from the effective `WritePType`.
///
/// Determines the Worker-side `RequestType`, optional UFS options, and
/// whether `schedule_async_persistence` should be called after `close()`.
#[derive(Clone, Debug)]
struct WriteStrategy {
    /// Worker `RequestType`: `GoosefsBlock` for cache writes, `UfsFile` for THROUGH.
    request_type: RequestType,
    /// UFS file creation options — only set when `request_type == UfsFile`.
    create_ufs_file_options: Option<CreateUfsFileOptions>,
    /// Whether `close()` should call `schedule_async_persistence` (ASYNC_THROUGH).
    need_async_persist: bool,
}

/// Derive the write strategy from `write_type` (i32 enum value) and the
/// `FileInfo` returned by `CreateFile`.
///
/// - MUST_CACHE (1) / TRY_CACHE (2) / unset: `GoosefsBlock`, no UFS.
/// - CACHE_THROUGH (3): `UfsFile` — Worker writes to UFS and caches simultaneously.
/// - THROUGH (4): `UfsFile` + `CreateUfsFileOptions` extracted from `FileInfo`.
/// - ASYNC_THROUGH (5): `GoosefsBlock`, `close()` schedules async persist.
///
/// **Note on CACHE_THROUGH**: In the Java client, CACHE_THROUGH creates parallel
/// streams to both Worker cache and UFS. On the Worker side, `RequestType::UfsFile`
/// writes data to UFS directly; the Worker also caches the data blocks in its local
/// store. This is why CACHE_THROUGH uses `UfsFile` mode — the same as THROUGH —
/// rather than `GoosefsBlock`. Without this, data reaches the cache but never gets
/// persisted to UFS (e.g. COS), because Master only marks metadata as PERSISTED
/// without actually copying data.
fn resolve_write_strategy(write_type: Option<i32>, file_info: &FileInfo) -> WriteStrategy {
    match write_type {
        // CACHE_THROUGH (3) / THROUGH (4): write to UFS via Worker.
        // For CACHE_THROUGH, the Worker also caches the data blocks locally.
        // For THROUGH, the Worker writes directly to UFS without caching.
        Some(3) | Some(4) => WriteStrategy {
            request_type: RequestType::UfsFile,
            create_ufs_file_options: Some(CreateUfsFileOptions {
                ufs_path: file_info.ufs_path.clone(),
                owner: file_info.owner.clone(),
                group: file_info.group.clone(),
                mode: file_info.mode,
                mount_id: file_info.mount_id,
                acl: None,
            }),
            need_async_persist: false,
        },

        // ASYNC_THROUGH: write to cache, schedule async persist after close
        Some(5) => WriteStrategy {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
            need_async_persist: true,
        },

        // MUST_CACHE (1), TRY_CACHE (2), NONE (6), unset:
        // all write to GooseFS cache blocks only; no UFS persistence.
        _ => WriteStrategy {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
            need_async_persist: false,
        },
    }
}

/// High-level file writer that orchestrates the full GooseFS write pipeline.
///
/// This struct encapsulates the complete write flow:
/// 1. `CreateFile` on Master to register the new file
/// 2. Discover workers and set up routing
/// 3. Split data into blocks via `BlockMapper`
/// 4. Write each block to a worker via `GrpcBlockWriter`
/// 5. `CompleteFile` on Master to finalize
pub struct GooseFsFileWriter {
    /// The GooseFS config.
    config: GooseFsConfig,
    /// The file path being written.
    path: String,
    /// Master client for metadata operations.
    master: MasterClient,
    /// Worker router for block → worker mapping.
    router: WorkerRouter,
    /// File info returned by CreateFile.
    file_info: FileInfo,
    /// Total bytes written so far across all blocks (includes flushed + buffered).
    total_bytes_written: u64,
    /// Whether the file has been completed (closed).
    completed: bool,
    /// Write strategy derived from config.write_type + FileInfo.
    write_strategy: WriteStrategy,
    /// Internal buffer for accumulating data before flushing a full block.
    /// This is needed because each block can only be written once — the Worker
    /// commits the block on flush/close and subsequent writes to the same
    /// block_id will fail with AlreadyExists.
    buffer: Vec<u8>,
}

impl GooseFsFileWriter {
    /// Create a new file and prepare for writing.
    ///
    /// This calls `CreateFile` on the Master and discovers available workers.
    /// After creation, call `write()` to send data, then `close()` to finalize.
    pub async fn create(config: &GooseFsConfig, path: &str) -> Result<Self> {
        Self::create_with_options(config, path, None).await
    }

    /// Create a new file with custom options.
    ///
    /// # Arguments
    /// - `config` — GooseFS client configuration
    /// - `path` — File path in GooseFS namespace
    /// - `options` — Optional `CreateFilePOptions` (block size, write type, etc.)
    pub async fn create_with_options(
        config: &GooseFsConfig,
        path: &str,
        options: Option<CreateFilePOptions>,
    ) -> Result<Self> {
        config
            .validate()
            .map_err(|e| Error::ConfigError { message: e })?;

        // 1. Connect to Master (uses MasterInquireClient for HA support)
        let master = MasterClient::connect(config).await?;
        debug!(path = %path, "connected to Master for file creation");

        // 2. Create the file
        let create_options = options.unwrap_or_else(|| {
            let mut opts = CreateFilePOptions {
                block_size_bytes: Some(config.block_size as i64),
                // Default file mode: 0644 (rw-r--r--)
                mode: Some(default_file_mode()),
                // Automatically create parent directories (e.g. for Lance Dataset sub-dirs)
                recursive: Some(true),
                ..Default::default()
            };
            // Apply config-level write_type if set
            if config.write_type.is_some() {
                opts.write_type = config.write_type;
            }
            opts
        });

        // Ensure recursive is set so parent directories are created automatically
        let mut create_options = create_options;
        if create_options.recursive.is_none() {
            create_options.recursive = Some(true);
        }

        let file_info = master.create_file(path, create_options.clone()).await?;
        debug!(
            path = %path,
            file_id = ?file_info.file_id,
            block_size = ?file_info.block_size_bytes,
            "file created on Master"
        );

        // Derive the write strategy from the effective write_type + file info.
        // Priority: CreateFilePOptions.write_type > config.write_type > default (MUST_CACHE).
        let effective_write_type = create_options.write_type.or(config.write_type);
        let write_strategy = resolve_write_strategy(effective_write_type, &file_info);
        debug!(
            write_type = ?effective_write_type,
            request_type = ?write_strategy.request_type,
            need_async_persist = write_strategy.need_async_persist,
            "resolved write strategy"
        );

        // 3. Discover workers (shares inquire client via MasterClient)
        let inquire_client = master.inquire_client().clone();
        let wm = WorkerManagerClient::connect_with_inquire(config, inquire_client).await?;
        let workers = wm.get_worker_info_list().await?;
        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers available for writing".to_string(),
            });
        }
        debug!(worker_count = workers.len(), "discovered workers");

        let router = WorkerRouter::new();
        router.update_workers(workers).await;

        Ok(Self {
            config: config.clone(),
            path: path.to_string(),
            master,
            router,
            file_info,
            total_bytes_written: 0,
            completed: false,
            write_strategy,
            buffer: Vec::new(),
        })
    }

    /// Write data to the file.
    ///
    /// Data is accumulated in an internal buffer and flushed to Workers
    /// automatically when a full block is ready. Call `close()` to flush
    /// any remaining buffered data and finalize the file.
    ///
    /// Can be called multiple times for streaming writes.
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.completed {
            return Err(Error::BlockIoError {
                message: "cannot write to a completed file".to_string(),
            });
        }

        if data.is_empty() {
            return Ok(());
        }

        let block_size = self
            .file_info
            .block_size_bytes
            .unwrap_or(self.config.block_size as i64) as usize;

        // Append data to buffer, flushing full blocks as they accumulate.
        let mut remaining = data;
        while !remaining.is_empty() {
            let space_in_block = block_size - (self.buffer.len() % block_size);
            let to_copy = std::cmp::min(remaining.len(), space_in_block);
            self.buffer.extend_from_slice(&remaining[..to_copy]);
            remaining = &remaining[to_copy..];

            // If buffer has a full block, flush it.
            if self.buffer.len() >= block_size {
                self.flush_buffer(block_size).await?;
            }
        }

        Ok(())
    }

    /// Flush complete blocks from the internal buffer to Workers.
    async fn flush_buffer(&mut self, block_size: usize) -> Result<()> {
        let chunk_size = self.config.chunk_size as usize;

        while self.buffer.len() >= block_size {
            // Take the first block_size bytes from the buffer.
            let block_data: Vec<u8> = self.buffer.drain(..block_size).collect();

            let block_index = self.total_bytes_written / block_size as u64;
            let plan = BlockWritePlan {
                block_index,
                offset_in_block: 0,
                length: block_data.len() as u64,
            };

            self.write_block(&plan, &block_data, block_size as u64, chunk_size)
                .await?;
            self.total_bytes_written += block_data.len() as u64;
        }

        Ok(())
    }

    /// Write a single block's worth of data to a worker.
    async fn write_block(
        &self,
        plan: &BlockWritePlan,
        data: &[u8],
        block_size: u64,
        chunk_size: usize,
    ) -> Result<()> {
        // Generate a block ID for new blocks.
        // In GooseFS, block IDs are typically assigned as:
        //   file_id * MAX_BLOCKS_PER_FILE + block_index
        // We use a simplified scheme here; the actual block ID assignment
        // depends on GooseFS server-side logic. For writing new blocks,
        // we use a deterministic ID based on file_id and block_index.
        let file_id = self.file_info.file_id.unwrap_or(0);
        let block_id = compute_block_id(file_id, plan.block_index);

        // Select a worker for this block
        let worker_info = self.router.select_worker(block_id).await?;
        let addr = worker_info
            .address
            .as_ref()
            .ok_or_else(|| Error::Internal {
                message: "worker has no address".to_string(),
                source: None,
            })?;

        let worker_addr = format!(
            "{}:{}",
            addr.host.as_deref().unwrap_or("127.0.0.1"),
            addr.rpc_port.unwrap_or(9203)
        );

        debug!(
            block_id = block_id,
            block_index = plan.block_index,
            data_len = data.len(),
            worker = %worker_addr,
            "writing block"
        );

        // Connect to the worker
        let worker = WorkerClient::connect(&worker_addr, self.config.connect_timeout).await?;

        // Build write options from the resolved strategy
        let write_opts = WriteBlockOptions {
            request_type: self.write_strategy.request_type,
            create_ufs_file_options: self.write_strategy.create_ufs_file_options.clone(),
        };

        // Open block writer with space reservation = block size
        let mut block_writer =
            GrpcBlockWriter::open(&worker, block_id, block_size as i64, write_opts).await?;

        // Write all data in chunks
        block_writer.write_all(data, chunk_size).await?;

        // Flush to ensure data is persisted on the worker
        let ack_offset = block_writer.flush().await?;
        debug!(
            block_id = block_id,
            ack_offset = ack_offset,
            "block flushed"
        );

        // Close the writer
        block_writer.close().await?;

        Ok(())
    }

    /// Close the file writer, finalizing the file on the Master.
    ///
    /// This flushes any remaining buffered data, then calls `CompleteFile`
    /// to mark the file as fully written. After calling `close()`, the
    /// writer cannot be used again.
    pub async fn close(&mut self) -> Result<()> {
        if self.completed {
            warn!(path = %self.path, "close() called on already-completed file");
            return Ok(());
        }

        // Flush any remaining buffered data as the final (possibly partial) block.
        if !self.buffer.is_empty() {
            let block_size = self
                .file_info
                .block_size_bytes
                .unwrap_or(self.config.block_size as i64) as usize;
            let chunk_size = self.config.chunk_size as usize;

            let remaining_data = std::mem::take(&mut self.buffer);
            let block_index = self.total_bytes_written / block_size as u64;
            let plan = BlockWritePlan {
                block_index,
                offset_in_block: 0,
                length: remaining_data.len() as u64,
            };

            self.write_block(&plan, &remaining_data, block_size as u64, chunk_size)
                .await?;
            self.total_bytes_written += remaining_data.len() as u64;
        }

        // Complete the file with the total bytes written
        let ufs_length = if self.total_bytes_written > 0 {
            Some(self.total_bytes_written as i64)
        } else {
            None
        };

        self.master.complete_file(&self.path, ufs_length).await?;
        self.completed = true;

        // ASYNC_THROUGH: schedule asynchronous persistence to UFS after file is complete.
        if self.write_strategy.need_async_persist {
            debug!(path = %self.path, "scheduling async persistence for ASYNC_THROUGH");
            if let Err(e) = self
                .master
                .schedule_async_persistence(&self.path, None)
                .await
            {
                warn!(
                    path = %self.path,
                    error = %e,
                    "failed to schedule async persistence — file is complete but may not persist to UFS"
                );
            }
        }

        info!(
            path = %self.path,
            total_bytes = self.total_bytes_written,
            "file write completed"
        );

        Ok(())
    }

    /// One-shot convenience method: create file, write all data, and close.
    ///
    /// This is the simplest way to write a file to GooseFS:
    ///
    /// ```rust,no_run
    /// # async fn example() -> goosefs_client::error::Result<()> {
    /// use goosefs_client::io::GooseFsFileWriter;
    /// use goosefs_client::config::GooseFsConfig;
    ///
    /// let config = GooseFsConfig::new("127.0.0.1:9200");
    /// GooseFsFileWriter::write_file(&config, "/my-file.txt", b"Hello!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_file(config: &GooseFsConfig, path: &str, data: &[u8]) -> Result<u64> {
        let mut writer = Self::create(config, path).await?;
        writer.write(data).await?;
        writer.close().await?;
        Ok(writer.total_bytes_written)
    }

    /// One-shot convenience method with custom create options.
    pub async fn write_file_with_options(
        config: &GooseFsConfig,
        path: &str,
        data: &[u8],
        options: CreateFilePOptions,
    ) -> Result<u64> {
        let mut writer = Self::create_with_options(config, path, Some(options)).await?;
        writer.write(data).await?;
        writer.close().await?;
        Ok(writer.total_bytes_written)
    }

    /// Get the total bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.total_bytes_written
    }

    /// Get the file path being written.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Whether the file has been completed.
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Get a reference to the file info.
    pub fn file_info(&self) -> &FileInfo {
        &self.file_info
    }
}

/// Compute a deterministic block ID from file ID (inode ID) and block index.
///
/// GooseFS uses a scheme where block IDs are derived from the file's inode ID:
///
/// ```text
/// Block ID layout (64 bits):
///   [container ID: 40 bits][sequence number: 24 bits]
///
/// container ID = inode_id >> 24   (extract upper 40 bits)
/// block ID     = (container_id << 24) | block_index
/// ```
///
/// This matches the Java implementation in `com.qcloud.cos.goosefs.master.block.BlockId`:
///   - `CONTAINER_ID_BITS = 40`
///   - `SEQUENCE_NUMBER_BITS = 24`
///   - `getContainerId(inodeId) = (inodeId >> 24) & CONTAINER_ID_MASK`
///   - `createBlockId(containerId, seq) = (containerId << 24) | seq`
fn compute_block_id(file_id: i64, block_index: u64) -> i64 {
    const CONTAINER_ID_BITS: u32 = 40;
    const SEQUENCE_NUMBER_BITS: u32 = 64 - CONTAINER_ID_BITS; // 24
    const CONTAINER_ID_MASK: i64 = (1i64 << CONTAINER_ID_BITS) - 1;
    const SEQUENCE_NUMBER_MASK: u64 = (1u64 << SEQUENCE_NUMBER_BITS) - 1;

    // Extract container ID from the inode ID (file_id)
    let container_id = (file_id >> SEQUENCE_NUMBER_BITS) & CONTAINER_ID_MASK;
    let seq = (block_index & SEQUENCE_NUMBER_MASK) as i64;
    (container_id << SEQUENCE_NUMBER_BITS) | seq
}

impl Drop for GooseFsFileWriter {
    fn drop(&mut self) {
        if !self.completed && self.total_bytes_written > 0 {
            warn!(
                path = %self.path,
                bytes_written = self.total_bytes_written,
                "GooseFsFileWriter dropped without calling close() — file may be incomplete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_block_id() {
        // GooseFS inode IDs have the container ID in the upper 40 bits.
        // For inode_id = 33554431 (0x1FFFFFF), container_id = 33554431 >> 24 = 1
        // block_id = (1 << 24) | 0 = 16777216
        let inode_id = 33554431i64; // typical GooseFS inode ID
        assert_eq!(compute_block_id(inode_id, 0), 1 << 24);
        assert_eq!(compute_block_id(inode_id, 1), (1 << 24) | 1);

        // For inode_id with container_id = 2: inode_id = 2 << 24 | anything
        let inode_id_2 = 2i64 << 24;
        assert_eq!(compute_block_id(inode_id_2, 0), 2 << 24);
    }

    #[test]
    fn test_compute_block_id_container_extraction() {
        // Verify container ID extraction matches Java's BlockId.getContainerId()
        const SEQUENCE_NUMBER_BITS: u32 = 24;
        const CONTAINER_ID_MASK: i64 = (1i64 << 40) - 1;

        let file_id = 33554431i64;
        let block_id = compute_block_id(file_id, 3);
        // Extract container ID from block_id
        let container_id = (block_id >> SEQUENCE_NUMBER_BITS) & CONTAINER_ID_MASK;
        assert_eq!(container_id, 1);
        // Extract sequence number from block_id
        assert_eq!(block_id & ((1 << SEQUENCE_NUMBER_BITS) - 1), 3);
    }

    /// Helper to build a minimal FileInfo for strategy tests.
    fn make_test_file_info() -> FileInfo {
        FileInfo {
            file_id: Some(1),
            ufs_path: Some("/ufs/data/test.txt".to_string()),
            owner: Some("hadoop".to_string()),
            group: Some("supergroup".to_string()),
            mode: Some(0o644),
            mount_id: Some(42),
            ..Default::default()
        }
    }

    #[test]
    fn test_strategy_must_cache() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(1), &fi); // MUST_CACHE
        assert_eq!(s.request_type, RequestType::GoosefsBlock);
        assert!(s.create_ufs_file_options.is_none());
        assert!(!s.need_async_persist);
    }

    #[test]
    fn test_strategy_cache_through() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(3), &fi); // CACHE_THROUGH
        assert_eq!(s.request_type, RequestType::GoosefsBlock);
        assert!(s.create_ufs_file_options.is_none());
        assert!(!s.need_async_persist);
    }

    #[test]
    fn test_strategy_through() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(4), &fi); // THROUGH
        assert_eq!(s.request_type, RequestType::UfsFile);
        let ufs_opts = s.create_ufs_file_options.as_ref().unwrap();
        assert_eq!(ufs_opts.ufs_path, Some("/ufs/data/test.txt".to_string()));
        assert_eq!(ufs_opts.owner, Some("hadoop".to_string()));
        assert_eq!(ufs_opts.group, Some("supergroup".to_string()));
        assert_eq!(ufs_opts.mode, Some(0o644));
        assert_eq!(ufs_opts.mount_id, Some(42));
        assert!(!s.need_async_persist);
    }

    #[test]
    fn test_strategy_async_through() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(5), &fi); // ASYNC_THROUGH
        assert_eq!(s.request_type, RequestType::GoosefsBlock);
        assert!(s.create_ufs_file_options.is_none());
        assert!(s.need_async_persist);
    }

    #[test]
    fn test_strategy_default_unset() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(None, &fi);
        assert_eq!(s.request_type, RequestType::GoosefsBlock);
        assert!(s.create_ufs_file_options.is_none());
        assert!(!s.need_async_persist);
    }

    #[test]
    fn test_strategy_try_cache() {
        let fi = make_test_file_info();
        let s = resolve_write_strategy(Some(2), &fi); // TRY_CACHE
        assert_eq!(s.request_type, RequestType::GoosefsBlock);
        assert!(s.create_ufs_file_options.is_none());
        assert!(!s.need_async_persist);
    }
}
