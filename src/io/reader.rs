//! gRPC streaming block reader with flow-control ACK.
//!
//! Implements the GooseFS bidirectional streaming read protocol:
//!
//! ```text
//! Client                                    Worker
//!   │  1. ReadRequest(block_id, offset,        │
//!   │     length, chunk_size)                  │
//!   ├─────────────────────────────────────────→│
//!   │  2. ReadResponse(chunk.data)             │
//!   │←─────────────────────────────────────────┤
//!   │  3. ReadRequest(offset_received=N)       │  ← flow-control ACK
//!   ├─────────────────────────────────────────→│
//!   │  4. ReadResponse(chunk.data) ...         │
//!   │←─────────────────────────────────────────┤
//!   │  5. stream ends                          │
//! ```
//!
//! # Positioned read
//!
//! [`GrpcBlockReader::positioned_read`] opens a fresh stream with
//! `position_short = true`, which tells the worker to skip prefetch and serve
//! the exact requested byte range directly.  This path is used by
//! `GooseFsFileInStream` for random seeks that cross a
//! `TRANSFER_POSITIONED_READ_THRESHOLD` (8 KiB) boundary.

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tonic::Streaming;
use tracing::{debug, trace, warn};

use crate::client::WorkerClient;
use crate::error::Result;
use crate::proto::grpc::block::{ReadRequest, ReadResponse};
use crate::proto::proto::dataserver::OpenUfsBlockOptions;

/// A streaming reader for a single GooseFS block.
///
/// Wraps a bidirectional gRPC `ReadBlock` stream and implements
/// flow-control via `offset_received` ACK messages.
pub struct GrpcBlockReader {
    /// Block being read.
    block_id: i64,
    /// Starting offset within the block.
    offset: i64,
    /// Total bytes expected.
    length: i64,
    /// Total bytes received so far.
    bytes_received: i64,
    /// Sender for client → server requests (ACK messages).
    request_tx: mpsc::Sender<ReadRequest>,
    /// Receiver for server → client responses (data chunks).
    response_rx: Streaming<ReadResponse>,
}

impl GrpcBlockReader {
    /// Open a new streaming reader for the specified block range.
    ///
    /// This sends the initial `ReadRequest` and returns a reader
    /// that yields data chunks via `read_chunk()`.
    ///
    /// When reading a block that only exists in UFS (e.g. written with
    /// `THROUGH` mode), pass `Some(OpenUfsBlockOptions { .. })` so the
    /// Worker can locate the data in the underlying storage.
    pub async fn open(
        worker: &WorkerClient,
        block_id: i64,
        offset: i64,
        length: i64,
        chunk_size: i64,
        open_ufs_block_options: Option<OpenUfsBlockOptions>,
    ) -> Result<Self> {
        let (request_tx, response_rx) = worker
            .read_block(block_id, offset, length, chunk_size, open_ufs_block_options)
            .await?;

        debug!(
            block_id = block_id,
            offset = offset,
            length = length,
            "opened GrpcBlockReader"
        );

        Ok(Self {
            block_id,
            offset,
            length,
            bytes_received: 0,
            request_tx,
            response_rx,
        })
    }

    /// Read the next data chunk from the stream.
    ///
    /// Returns `None` when all expected data has been received.
    /// Sends a flow-control `offset_received` ACK after each chunk.
    pub async fn read_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.bytes_received >= self.length {
            return Ok(None);
        }

        match self.response_rx.message().await? {
            Some(resp) => {
                let data = resp.chunk.and_then(|c| c.data).unwrap_or_default();

                if data.is_empty() {
                    // Empty chunk signals end of data
                    return Ok(None);
                }

                self.bytes_received += data.len() as i64;
                trace!(
                    block_id = self.block_id,
                    chunk_len = data.len(),
                    total_received = self.bytes_received,
                    "received chunk"
                );

                // Send flow-control ACK
                let ack = ReadRequest {
                    offset_received: Some(self.offset + self.bytes_received),
                    ..Default::default()
                };
                if self.request_tx.send(ack).await.is_err() {
                    warn!(
                        block_id = self.block_id,
                        "ACK channel closed (read may be complete)"
                    );
                }

                Ok(Some(Bytes::from(data)))
            }
            None => {
                // Stream ended
                Ok(None)
            }
        }
    }

    /// Read all remaining data from this block and return it as a single `Bytes`.
    pub async fn read_all(&mut self) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(self.length as usize);

        while let Some(chunk) = self.read_chunk().await? {
            buf.extend_from_slice(&chunk);
        }

        Ok(buf.freeze())
    }

    /// The block ID being read.
    pub fn block_id(&self) -> i64 {
        self.block_id
    }

    /// Total bytes received so far.
    pub fn bytes_received(&self) -> i64 {
        self.bytes_received
    }

    /// Whether all expected data has been received.
    pub fn is_complete(&self) -> bool {
        self.bytes_received >= self.length
    }

    // ── Positioned read ──────────────────────────────────────────────────────

    /// Perform a one-shot positioned read from `offset` for `length` bytes.
    ///
    /// Opens a **new** gRPC stream with `position_short = true`, reads all
    /// data, and returns it as a single `Bytes`.  The new stream is discarded
    /// after this call.
    ///
    /// # Design
    ///
    /// `position_short = true` instructs the worker to:
    /// 1. Skip prefetch / eviction — serve the range directly from cache or UFS.
    /// 2. Complete the stream after delivering exactly `length` bytes.
    ///
    /// This path is chosen by `GooseFsFileInStream` when the caller uses
    /// `read_at()` (random access) or when the seek distance exceeds the
    /// `TRANSFER_POSITIONED_READ_THRESHOLD` (8 KiB).
    ///
    /// # Arguments
    ///
    /// - `worker`    — connected `WorkerClient`.
    /// - `block_id`  — block to read from.
    /// - `offset`    — byte offset within the block.
    /// - `length`    — number of bytes to read.
    /// - `chunk_size` — preferred gRPC chunk size.
    /// - `open_ufs_block_options` — required for THROUGH-mode blocks.
    pub async fn positioned_read(
        worker: &WorkerClient,
        block_id: i64,
        offset: i64,
        length: i64,
        chunk_size: i64,
        open_ufs_block_options: Option<OpenUfsBlockOptions>,
    ) -> Result<Bytes> {
        let (request_tx, response_rx) = worker
            .read_block_positioned(block_id, offset, length, chunk_size, open_ufs_block_options)
            .await?;

        debug!(
            block_id = block_id,
            offset = offset,
            length = length,
            "positioned_read: opened position_short stream"
        );

        let mut reader = Self {
            block_id,
            offset,
            length,
            bytes_received: 0,
            request_tx,
            response_rx,
        };

        reader.read_all().await
    }
}
