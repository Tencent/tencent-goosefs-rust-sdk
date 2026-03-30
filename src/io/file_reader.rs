//! High-level file reader that orchestrates the complete read pipeline.
//!
//! `GooseFsFileReader` ties together all low-level components into a single
//! easy-to-use API, analogous to Java's `GooseFSFileInStream`:
//!
//! ```text
//! GooseFsFileReader::read_file(path)
//!   → MasterClient.get_status()          — get file metadata + block IDs
//!   → WorkerManagerClient.get_worker_info_list() — discover workers
//!   → BlockMapper.plan_read()            — split file range → block segments
//!   → for each block segment:
//!       → WorkerRouter.select_worker()   — consistent hash routing
//!       → WorkerClient.connect()         — connect to target worker
//!       → GrpcBlockReader.open()         — open streaming read
//!       → reader.read_all()              — read all chunk data
//!   → concatenate results
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use goosefs_client::io::GooseFsFileReader;
//! use goosefs_client::config::GooseFsConfig;
//!
//! # async fn example() -> goosefs_client::error::Result<()> {
//! let config = GooseFsConfig::new("127.0.0.1:9200");
//!
//! // Read entire file
//! let data = GooseFsFileReader::read_file(&config, "/my-file.txt").await?;
//! println!("read {} bytes", data.len());
//!
//! // Range read (offset=100, length=500)
//! let data = GooseFsFileReader::read_range(&config, "/my-file.txt", 100, 500).await?;
//!
//! // Or use the builder for streaming reads
//! let mut reader = GooseFsFileReader::open(&config, "/my-file.txt").await?;
//! while let Some(chunk) = reader.read_next_block().await? {
//!     println!("got {} bytes from block", chunk.len());
//! }
//! # Ok(())
//! # }
//! ```

use bytes::{Bytes, BytesMut};
use tracing::{debug, warn};

use crate::block::mapper::{BlockMapper, BlockReadPlan};
use crate::block::router::WorkerRouter;
use crate::client::{MasterClient, WorkerClient, WorkerManagerClient};
use crate::config::GooseFsConfig;
use crate::error::{Error, Result};
use crate::io::reader::GrpcBlockReader;
use crate::proto::grpc::file::FileInfo;
use crate::proto::proto::dataserver::OpenUfsBlockOptions;

/// High-level file reader that orchestrates the full GooseFS read pipeline.
///
/// This struct encapsulates the complete read flow:
/// 1. `GetStatus` on Master to obtain file metadata (block IDs, block size, length)
/// 2. Discover workers and build consistent hash router
/// 3. Plan the read via `BlockMapper` (map file range → block segments)
/// 4. For each block: select worker → connect → stream-read via `GrpcBlockReader`
/// 5. Concatenate results
pub struct GooseFsFileReader {
    /// The GooseFS config.
    config: GooseFsConfig,
    /// The file path being read.
    path: String,
    /// File info from Master (contains block IDs, block size, length).
    file_info: FileInfo,
    /// Worker router for block → worker mapping.
    router: WorkerRouter,
    /// Block-level read plans (populated on open).
    plans: Vec<BlockReadPlan>,
    /// Index of the next block to read.
    current_plan_index: usize,
    /// Total bytes read so far.
    total_bytes_read: u64,
    /// Requested read offset in the file.
    offset: u64,
    /// Requested read length.
    length: u64,
}

impl GooseFsFileReader {
    /// Open a file for reading from the beginning (full file read).
    ///
    /// This fetches file metadata and discovers workers, but does not start
    /// reading data until `read_next_block()` or `read_all()` is called.
    pub async fn open(config: &GooseFsConfig, path: &str) -> Result<Self> {
        let (file_info, router) = Self::init(config, path).await?;
        let file_length = file_info.length.unwrap_or(0) as u64;
        Self::build(config, path, file_info, router, 0, file_length)
    }

    /// Open a file for range reading.
    ///
    /// # Arguments
    /// - `config` — GooseFS client configuration
    /// - `path` — File path in GooseFS namespace
    /// - `offset` — Start byte offset in the file
    /// - `length` — Number of bytes to read
    pub async fn open_range(
        config: &GooseFsConfig,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Self> {
        let (file_info, router) = Self::init(config, path).await?;
        Self::build(config, path, file_info, router, offset, length)
    }

    /// Internal: connect to master, get file info, discover workers.
    async fn init(config: &GooseFsConfig, path: &str) -> Result<(FileInfo, WorkerRouter)> {
        config
            .validate()
            .map_err(|e| Error::ConfigError { message: e })?;

        // 1. Connect to Master (uses MasterInquireClient for HA support)
        let master = MasterClient::connect(config).await?;
        let file_info = master.get_status(path).await?;

        let file_length = file_info.length.unwrap_or(0);
        if file_length == 0 {
            debug!(path = %path, "file is empty");
        }

        debug!(
            path = %path,
            file_length = file_length,
            block_count = file_info.block_ids.len(),
            block_size = ?file_info.block_size_bytes,
            "fetched file metadata"
        );

        // 2. Discover workers (shares the same inquire client via MasterClient)
        let inquire_client = master.inquire_client().clone();
        let wm = WorkerManagerClient::connect_with_inquire(config, inquire_client).await?;
        let workers = wm.get_worker_info_list().await?;
        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers available for reading".to_string(),
            });
        }
        debug!(worker_count = workers.len(), "discovered workers");

        let router = WorkerRouter::new();
        router.update_workers(workers).await;

        Ok((file_info, router))
    }

    /// Internal: build the reader from file info and router.
    fn build(
        config: &GooseFsConfig,
        path: &str,
        file_info: FileInfo,
        router: WorkerRouter,
        offset: u64,
        length: u64,
    ) -> Result<Self> {
        // Plan the read
        let plans = BlockMapper::plan_read(&file_info, offset, length);
        debug!(
            path = %path,
            offset = offset,
            length = length,
            block_segments = plans.len(),
            "read plan created"
        );

        Ok(Self {
            config: config.clone(),
            path: path.to_string(),
            file_info,
            router,
            plans,
            current_plan_index: 0,
            total_bytes_read: 0,
            offset,
            length,
        })
    }

    /// Read the next block segment and return its data.
    ///
    /// Returns `None` when all block segments have been read.
    /// This is useful for streaming reads where you want to process
    /// data block-by-block.
    pub async fn read_next_block(&mut self) -> Result<Option<Bytes>> {
        loop {
            if self.current_plan_index >= self.plans.len() {
                return Ok(None);
            }

            let plan = &self.plans[self.current_plan_index];

            // Resolve the block ID — prefer FileBlockInfo if available
            let block_id = self.resolve_block_id(plan);
            if block_id <= 0 {
                warn!(
                    block_index = plan.block_index,
                    plan_block_id = plan.block_id,
                    "invalid block ID, skipping block"
                );
                self.current_plan_index += 1;
                continue; // loop instead of recursion
            }

            // Select worker for this block
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
                offset_in_block = plan.offset_in_block,
                length = plan.length,
                worker = %worker_addr,
                "reading block"
            );

            // Connect to the worker (with one retry on a different worker)
            let worker = match WorkerClient::connect(&worker_addr, &self.config).await {
                Ok(w) => w,
                Err(e) => {
                    // Mark worker as failed for future routing
                    if let Some(w_addr) = worker_info.address.as_ref() {
                        self.router.mark_failed(w_addr);
                    }
                    warn!(
                        worker = %worker_addr,
                        error = %e,
                        "worker connection failed, trying another worker"
                    );

                    // Retry: select a different worker and try once more
                    match self.router.select_worker(block_id).await {
                        Ok(retry_worker_info) => {
                            let retry_addr_info =
                                retry_worker_info.address.as_ref().ok_or_else(|| {
                                    Error::Internal {
                                        message: "retry worker has no address".to_string(),
                                        source: None,
                                    }
                                })?;
                            let retry_worker_addr = format!(
                                "{}:{}",
                                retry_addr_info.host.as_deref().unwrap_or("127.0.0.1"),
                                retry_addr_info.rpc_port.unwrap_or(9203)
                            );
                            debug!(retry_worker = %retry_worker_addr, "retrying with different worker");
                            WorkerClient::connect(&retry_worker_addr, &self.config).await?
                        }
                        Err(_) => return Err(e), // No other worker available, propagate original error
                    }
                }
            };

            // Build OpenUfsBlockOptions when the block may reside in UFS.
            // This is required for THROUGH-mode writes (data only in UFS)
            // and useful for UFS-fallback reads in general.
            let ufs_options = self.build_ufs_read_options(plan);

            // Open block reader
            let mut block_reader = GrpcBlockReader::open(
                &worker,
                block_id,
                plan.offset_in_block as i64,
                plan.length as i64,
                self.config.chunk_size as i64,
                ufs_options,
            )
            .await?;

            // Read all data from this block segment
            let data = block_reader.read_all().await?;
            let bytes_read = data.len() as u64;

            self.total_bytes_read += bytes_read;
            self.current_plan_index += 1;

            debug!(
                block_id = block_id,
                bytes_read = bytes_read,
                total_read = self.total_bytes_read,
                complete = block_reader.is_complete(),
                "block read complete"
            );

            return Ok(Some(data));
        }
    }

    /// Read all remaining data and return it as a single `Bytes`.
    ///
    /// This reads all block segments sequentially and concatenates the results.
    pub async fn read_all(&mut self) -> Result<Bytes> {
        let expected_len = self.plans.iter().map(|p| p.length).sum::<u64>();
        let mut buf = BytesMut::with_capacity(expected_len as usize);

        while let Some(chunk) = self.read_next_block().await? {
            buf.extend_from_slice(&chunk);
        }

        Ok(buf.freeze())
    }

    /// Resolve the best block ID for a read plan.
    ///
    /// Prefers the block ID from `file_block_infos` (which contains the actual
    /// assigned block ID from the server) over the ID computed from `block_ids`.
    fn resolve_block_id(&self, plan: &BlockReadPlan) -> i64 {
        // First try file_block_infos for the actual server-assigned block ID
        if let Some(fbi) = self
            .file_info
            .file_block_infos
            .get(plan.block_index as usize)
        {
            if let Some(bi) = &fbi.block_info {
                if let Some(id) = bi.block_id {
                    if id > 0 {
                        return id;
                    }
                }
            }
        }
        // Fall back to block_ids list
        plan.block_id
    }

    /// Build `OpenUfsBlockOptions` for a block that may reside in UFS.
    ///
    /// When data was written with `THROUGH` mode, the block only exists in the
    /// underlying file system (UFS). The Worker needs `OpenUfsBlockOptions` to
    /// know the UFS path, mount ID, and block geometry so it can read the data
    /// from UFS on behalf of the client.
    ///
    /// Returns `None` if the file has no UFS path (i.e. data is cache-only).
    fn build_ufs_read_options(&self, plan: &BlockReadPlan) -> Option<OpenUfsBlockOptions> {
        let ufs_path = self.file_info.ufs_path.as_ref()?;
        if ufs_path.is_empty() {
            return None;
        }

        let block_size = self.file_info.block_size_bytes.unwrap_or(64 * 1024 * 1024);

        // The offset in the file where this block starts
        let offset_in_file = plan.block_index as i64 * block_size;

        Some(OpenUfsBlockOptions {
            ufs_path: Some(ufs_path.clone()),
            offset_in_file: Some(offset_in_file),
            block_size: Some(block_size),
            max_ufs_read_concurrency: None,
            mount_id: self.file_info.mount_id,
            no_cache: Some(!self.file_info.cacheable.unwrap_or(true)),
            user: None,
            caller_type: None,
        })
    }

    /// One-shot convenience: read an entire file and return its contents.
    ///
    /// ```rust,no_run
    /// # async fn example() -> goosefs_client::error::Result<()> {
    /// use goosefs_client::io::GooseFsFileReader;
    /// use goosefs_client::config::GooseFsConfig;
    ///
    /// let config = GooseFsConfig::new("127.0.0.1:9200");
    /// let data = GooseFsFileReader::read_file(&config, "/my-file.txt").await?;
    /// println!("content: {}", String::from_utf8_lossy(&data));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_file(config: &GooseFsConfig, path: &str) -> Result<Bytes> {
        let mut reader = Self::open(config, path).await?;
        reader.read_all().await
    }

    /// One-shot convenience: read a byte range from a file.
    ///
    /// ```rust,no_run
    /// # async fn example() -> goosefs_client::error::Result<()> {
    /// use goosefs_client::io::GooseFsFileReader;
    /// use goosefs_client::config::GooseFsConfig;
    ///
    /// let config = GooseFsConfig::new("127.0.0.1:9200");
    /// let data = GooseFsFileReader::read_range(&config, "/my-file.txt", 100, 500).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_range(
        config: &GooseFsConfig,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let mut reader = Self::open_range(config, path, offset, length).await?;
        reader.read_all().await
    }

    // ── Accessors ────────────────────────────────────────────────

    /// Get the file path being read.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the file info.
    pub fn file_info(&self) -> &FileInfo {
        &self.file_info
    }

    /// Get the total file length (from metadata).
    pub fn file_length(&self) -> u64 {
        self.file_info.length.unwrap_or(0) as u64
    }

    /// Get the total bytes read so far.
    pub fn bytes_read(&self) -> u64 {
        self.total_bytes_read
    }

    /// Get the number of block segments in the read plan.
    pub fn block_count(&self) -> usize {
        self.plans.len()
    }

    /// Get the index of the next block to be read.
    pub fn current_block_index(&self) -> usize {
        self.current_plan_index
    }

    /// Whether all blocks have been read.
    pub fn is_complete(&self) -> bool {
        self.current_plan_index >= self.plans.len()
    }

    /// Get the requested read offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the requested read length.
    pub fn length(&self) -> u64 {
        self.length
    }
}
