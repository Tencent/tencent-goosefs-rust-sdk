//! gRPC streaming block reader with flow-control ACK.
//!
//! Implements the Goosefs bidirectional streaming read protocol:
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
//! `GoosefsFileInStream` for random seeks that cross a
//! `TRANSFER_POSITIONED_READ_THRESHOLD` (8 KiB) boundary.

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tonic::Streaming;
use tracing::{debug, trace, warn};

use crate::client::WorkerClient;
use crate::error::{Error, Result};
use crate::metrics::name;
use crate::proto::grpc::block::{ReadRequest, ReadResponse};
use crate::proto::proto::dataserver::OpenUfsBlockOptions;

/// A streaming reader for a single Goosefs block.
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

/// Decision of how the reader should treat a single `ReadResponse` (or its
/// absence).
///
/// Extracted as a pure function so the empty-frame / EOF / data-deliver
/// branches can be unit-tested without spinning up a real gRPC stream.
#[derive(Debug, PartialEq, Eq)]
enum ChunkAction {
    /// The server has half-closed the stream — no more data is coming.
    Eof,
    /// The frame carries no data (keep-alive / header-only) but the stream
    /// is still open. Caller must wait for the next frame; emitting `None`
    /// here would silently truncate the read.
    KeepReading,
    /// A data frame: deliver these bytes to the caller.
    Deliver(Bytes),
}

/// Classify a single response from the worker stream into one of three
/// outcomes. Pure function — no I/O, no state, fully unit-testable.
fn classify_response(resp: Option<ReadResponse>) -> ChunkAction {
    match resp {
        None => ChunkAction::Eof,
        Some(r) => {
            let data = r.chunk.and_then(|c| c.data).unwrap_or_default();
            if data.is_empty() {
                ChunkAction::KeepReading
            } else {
                ChunkAction::Deliver(Bytes::from(data))
            }
        }
    }
}

/// Verify that a positioned-read stream delivered every requested byte.
///
/// Pure helper so the H2 short-read guard can be unit-tested without
/// spinning up a real gRPC stream. Returns `Err(Error::Internal{..})`
/// iff the server half-closed before `bytes_received == length`.
fn check_positioned_read_complete(block_id: i64, bytes_received: i64, length: i64) -> Result<()> {
    if bytes_received < length {
        return Err(Error::Internal {
            message: format!(
                "short positioned read on block {}: received {} of {} bytes \
                 (server half-closed early)",
                block_id, bytes_received, length
            ),
            source: None,
        });
    }
    Ok(())
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

        // Instrument: track concurrent block reads
        crate::metrics::gauge(name::CLIENT_BLOCKS_READ_IN_PROGRESS)
            .set(crate::metrics::gauge(name::CLIENT_BLOCKS_READ_IN_PROGRESS).get() + 1);

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

        // Loop instead of recursing: the server may emit several empty
        // keep-alive / header-only frames in a row, and `Box::pin`-ing the
        // recursive call would otherwise heap-allocate per empty frame.
        loop {
            match classify_response(self.response_rx.message().await?) {
                ChunkAction::Eof => {
                    debug!(
                        block_id = self.block_id,
                        bytes_received = self.bytes_received,
                        "stream ended before all expected data received"
                    );
                    return Ok(None);
                }
                ChunkAction::KeepReading => {
                    // The server has sent an empty data frame but has not
                    // closed the stream — typical for keep-alive /
                    // header-only frames. Returning `None` here would be a
                    // short-read for the caller (we'd lie about hitting
                    // EOF before `bytes_received >= length`), which can
                    // silently truncate user data. Wait for the next real
                    // chunk. EOF is correctly detected at the top of this
                    // function via the `bytes_received >= length` check,
                    // or via the `Eof` arm above when the server half-closes.
                    trace!(
                        block_id = self.block_id,
                        bytes_received = self.bytes_received,
                        expected = self.length,
                        "received empty data frame, awaiting next chunk"
                    );
                    continue;
                }
                ChunkAction::Deliver(data) => {
                    self.bytes_received += data.len() as i64;
                    trace!(
                        block_id = self.block_id,
                        chunk_len = data.len(),
                        total_received = self.bytes_received,
                        "received chunk"
                    );

                    // Instrument: increment read bytes counter.
                    // TODO: Distinguish local vs. remote reads via worker address.
                    // For now, conservatively count all reads as local short-circuit.
                    crate::metrics::counter(name::CLIENT_BYTES_READ_LOCAL).inc(data.len() as i64);

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

                    return Ok(Some(data));
                }
            }
        }
    }

    /// Read all remaining data from this block and return it as a single `Bytes`.
    ///
    /// # H2 short-read guarantee
    ///
    /// `read_all` is the *positioned-read* tail (used by
    /// [`Self::positioned_read`]). The caller has constrained `length` to a
    /// range it knows is in-file, so a server-side half-close before
    /// `bytes_received == length` indicates either a truncated stream or a
    /// worker bug — surfacing it as `Error::Internal` lets the upper layer
    /// (`GoosefsFileInStream::read_at`) decide whether to retry or propagate,
    /// instead of returning misaligned data via a silent short read.
    ///
    /// The streaming sequential path uses [`Self::read_chunk`] directly and
    /// is unaffected by this check.
    pub async fn read_all(&mut self) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(self.length as usize);

        while let Some(chunk) = self.read_chunk().await? {
            buf.extend_from_slice(&chunk);
        }

        // Instrument: block read completed
        crate::metrics::counter(name::CLIENT_BLOCKS_READ_TOTAL).inc(1);
        crate::metrics::gauge(name::CLIENT_BLOCKS_READ_IN_PROGRESS)
            .set((crate::metrics::gauge(name::CLIENT_BLOCKS_READ_IN_PROGRESS).get() - 1).max(0));

        // H2: short-read guard. `read_chunk()` returns Ok(None) on either
        // `bytes_received >= length` (the legitimate completion path) or
        // server-half-close (`ChunkAction::Eof` before all bytes arrived).
        // The latter must NOT be presented as a successful read.
        check_positioned_read_complete(self.block_id, self.bytes_received, self.length)?;

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
    /// This path is chosen by `GoosefsFileInStream` when the caller uses
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::grpc::block::Chunk;

    /// Verify that metrics instrumentation in read_chunk is sound.
    /// (Full read path testing is integration-level; here we just verify
    /// that the metrics counter is accessible and callable.)
    #[test]
    fn metrics_counter_accessible() {
        let _counter = crate::metrics::counter(name::CLIENT_BYTES_READ_LOCAL);
        // Just verifying no panics during counter access.
    }

    /// `None` from `Streaming::message()` means the server half-closed.
    #[test]
    fn classify_response_none_is_eof() {
        assert_eq!(classify_response(None), ChunkAction::Eof);
    }

    /// **Regression**: a frame whose `chunk.data` is `None` or an empty
    /// `Vec` MUST be treated as a keep-alive, NOT as EOF — the previous
    /// implementation returned `None` here, which silently short-read user
    /// data when the server emitted any header-only frame.
    #[test]
    fn classify_response_no_chunk_is_keep_reading() {
        let resp = ReadResponse {
            chunk: None,
            ..Default::default()
        };
        assert_eq!(classify_response(Some(resp)), ChunkAction::KeepReading);
    }

    #[test]
    fn classify_response_empty_chunk_is_keep_reading() {
        let resp = ReadResponse {
            chunk: Some(Chunk {
                data: Some(Vec::new()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(classify_response(Some(resp)), ChunkAction::KeepReading);
    }

    #[test]
    fn classify_response_chunk_with_none_data_is_keep_reading() {
        let resp = ReadResponse {
            chunk: Some(Chunk {
                data: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(classify_response(Some(resp)), ChunkAction::KeepReading);
    }

    /// Real data frames must be delivered byte-for-byte unchanged.
    #[test]
    fn classify_response_data_is_delivered() {
        let payload = b"hello world".to_vec();
        let resp = ReadResponse {
            chunk: Some(Chunk {
                data: Some(payload.clone()),
                ..Default::default()
            }),
            ..Default::default()
        };
        match classify_response(Some(resp)) {
            ChunkAction::Deliver(b) => assert_eq!(b.as_ref(), payload.as_slice()),
            other => panic!("expected Deliver, got {:?}", other),
        }
    }

    /// **Regression for H2 (short positioned read)**: when the server
    /// half-closes before delivering the full requested range,
    /// `read_all()` MUST surface an `Error::Internal` instead of returning
    /// a truncated `Bytes`. The pre-fix behaviour returned the partial
    /// buffer silently, which combined with the buggy `cur += length` in
    /// `GoosefsFileInStream::read_at` caused mis-aligned data on the
    /// caller side (random-access read returning wrong bytes).
    #[test]
    fn check_positioned_read_complete_short_read_errors() {
        // Received < expected → Error::Internal with descriptive message.
        let err = check_positioned_read_complete(
            /* block_id */ 16777216, /* bytes_received */ 1024, /* length */ 4096,
        )
        .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("short positioned read on block 16777216"),
            "expected short-read message, got: {}",
            msg
        );
        assert!(
            msg.contains("received 1024 of 4096"),
            "expected received/length pair in message, got: {}",
            msg
        );
    }

    /// **Regression for H2**: the legitimate completion path
    /// (`bytes_received == length`) MUST be Ok.
    #[test]
    fn check_positioned_read_complete_full_read_ok() {
        assert!(check_positioned_read_complete(1, 4096, 4096).is_ok());
    }

    /// **Regression for H2**: any *over-read* (server delivered more than
    /// asked — defensive check, should not happen in practice) is
    /// **not** treated as a short read. Strictly `bytes_received < length`
    /// is the failure condition.
    #[test]
    fn check_positioned_read_complete_over_read_ok() {
        assert!(check_positioned_read_complete(1, 5000, 4096).is_ok());
    }
}
