//! gRPC streaming block writer.
//!
//! Implements the GooseFS bidirectional streaming write protocol:
//!
//! ```text
//! 1. Master: CreateFile(path, blockSizeBytes, writeType) → FileInfo
//! 2. For each block:
//!    a. Choose worker via consistent hash
//!    b. WriteBlock bidirectional stream:
//!       - First message: WriteRequestCommand { id, type, spaceToReserve }
//!       - Subsequent messages: Chunk { data }
//!       - flush: WriteRequestCommand { flush=true } → wait WriteResponse
//! 3. Master: CompleteFile(path)
//! ```
//!
//! ## GooseFS Write Protocol Detail
//!
//! The Worker's `WriteBlock` RPC is bidirectional streaming, but the server
//! does **not** send HTTP/2 response headers immediately. Response headers
//! (and the first `WriteResponse`) are only sent when:
//! - The client sends a `flush` command, or
//! - The client closes the request stream (triggering `onCompleted`).
//!
//! This means we cannot `await` the tonic streaming call inline — it would
//! deadlock. Instead, `WorkerClient::write_block()` returns a
//! [`WriteBlockHandle`](crate::client::worker::WriteBlockHandle) that manages
//! a background task.

use bytes::Bytes;
use tracing::{debug, trace};

use crate::client::worker::{WriteBlockHandle, WriteBlockOptions};
use crate::client::WorkerClient;
use crate::error::{Error, Result};
use crate::proto::grpc::block::{write_request, Chunk, WriteRequest, WriteRequestCommand};

/// A streaming writer for a single GooseFS block.
///
/// Wraps a [`WriteBlockHandle`] that manages the background gRPC call.
/// The initial `WriteRequestCommand` is sent during `open()`.
/// Subsequent data is sent via `write_chunk()`.
pub struct GrpcBlockWriter {
    /// Block being written.
    block_id: i64,
    /// Total bytes written so far.
    bytes_written: i64,
    /// Handle to the background WriteBlock gRPC task.
    handle: WriteBlockHandle,
}

impl GrpcBlockWriter {
    /// Open a new streaming writer for the specified block.
    ///
    /// This initiates the `WriteBlock` RPC in a background task and sends
    /// the initial `WriteRequestCommand` with the block ID and space reservation.
    ///
    /// The `options` parameter controls the `RequestType` and optional
    /// `CreateUfsFileOptions` for THROUGH-mode writes.
    pub async fn open(
        worker: &WorkerClient,
        block_id: i64,
        space_to_reserve: i64,
        options: WriteBlockOptions,
    ) -> Result<Self> {
        let handle = worker
            .write_block(block_id, space_to_reserve, options)
            .await?;

        debug!(
            block_id = block_id,
            space_to_reserve = space_to_reserve,
            "opened GrpcBlockWriter"
        );

        Ok(Self {
            block_id,
            bytes_written: 0,
            handle,
        })
    }

    /// Write a data chunk to the block.
    pub async fn write_chunk(&mut self, data: Bytes) -> Result<()> {
        let chunk_len = data.len() as i64;

        let req = WriteRequest {
            value: Some(write_request::Value::Chunk(Chunk {
                data: Some(data.to_vec()),
            })),
        };

        self.handle
            .request_tx
            .send(req)
            .await
            .map_err(|_| Error::BlockIoError {
                message: format!("write channel closed for block_id={}", self.block_id),
            })?;

        self.bytes_written += chunk_len;
        trace!(
            block_id = self.block_id,
            chunk_len = chunk_len,
            total_written = self.bytes_written,
            "wrote chunk"
        );

        Ok(())
    }

    /// Write all data from a byte slice, splitting into chunks of `chunk_size`.
    pub async fn write_all(&mut self, data: &[u8], chunk_size: usize) -> Result<()> {
        let mut offset = 0;

        while offset < data.len() {
            let end = std::cmp::min(offset + chunk_size, data.len());
            let chunk = Bytes::copy_from_slice(&data[offset..end]);
            self.write_chunk(chunk).await?;
            offset = end;
        }

        Ok(())
    }

    /// Flush the current block: send a flush command and wait for the
    /// server to acknowledge with a `WriteResponse`.
    ///
    /// This triggers the server to send its first response (including
    /// HTTP/2 headers), which unblocks the background gRPC task.
    pub async fn flush(&mut self) -> Result<i64> {
        // Send flush command
        let flush_req = WriteRequest {
            value: Some(write_request::Value::Command(WriteRequestCommand {
                flush: Some(true),
                ..Default::default()
            })),
        };

        self.handle
            .request_tx
            .send(flush_req)
            .await
            .map_err(|_| Error::BlockIoError {
                message: format!("flush channel closed for block_id={}", self.block_id),
            })?;

        // Wait for ack from server (forwarded through the background task)
        match self.handle.recv_response().await? {
            Some(resp) => {
                let offset = resp.offset.unwrap_or(self.bytes_written);
                debug!(
                    block_id = self.block_id,
                    ack_offset = offset,
                    "flush acknowledged"
                );
                Ok(offset)
            }
            None => Err(Error::BlockIoError {
                message: format!(
                    "stream ended before flush ack for block_id={}",
                    self.block_id
                ),
            }),
        }
    }

    /// Close the writer by dropping the request channel.
    /// The server will finalize the block (commitBlock).
    pub async fn close(self) -> Result<()> {
        let block_id = self.block_id;
        let bytes_written = self.bytes_written;

        // Dropping the handle's request_tx closes the client→server half
        // of the stream, triggering server-side onCompleted → commitBlock.
        self.handle.close().await?;

        debug!(
            block_id = block_id,
            bytes_written = bytes_written,
            "closed GrpcBlockWriter"
        );
        Ok(())
    }

    /// The block ID being written.
    pub fn block_id(&self) -> i64 {
        self.block_id
    }

    /// Total bytes written so far.
    pub fn bytes_written(&self) -> i64 {
        self.bytes_written
    }
}
