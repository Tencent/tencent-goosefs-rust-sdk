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
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tonic::Streaming;
use tracing::{debug, trace, warn};

use crate::client::WorkerClient;
use crate::config::GoosefsConfig;
use crate::error::{Error, Result};
use crate::metrics::name;
use crate::proto::grpc::block::{ReadRequest, ReadResponse};
use crate::proto::proto::dataserver::OpenUfsBlockOptions;

/// Per-stream tuning knobs for the sequential read path (Part V R1-B).
///
/// Carries the prefetch window, receive-buffer depth, and flow-control ACK
/// coalescing thresholds resolved from [`GoosefsConfig`].
#[derive(Debug, Clone, Copy)]
pub struct ReadTuning {
    /// Prefetch window in chunks sent on the initial `ReadRequest` (R1-B-a).
    pub prefetch_window: i32,
    /// Receive-buffer depth between the background drain task and the
    /// consumer, in messages (R1-B-b).
    pub buffer_messages: usize,
    /// ACK coalescing threshold in bytes (R1-B-c).
    pub ack_interval_bytes: i64,
    /// ACK coalescing threshold in chunks (R1-B-c).
    pub ack_interval_chunks: u32,
}

impl ReadTuning {
    /// Resolve tuning knobs from the SDK config.
    pub fn from_config(config: &GoosefsConfig) -> Self {
        Self {
            prefetch_window: config.prefetch_window,
            buffer_messages: config.read_buffer_messages.max(1),
            ack_interval_bytes: config.ack_interval_bytes.max(0),
            ack_interval_chunks: config.ack_interval_chunks.max(1),
        }
    }
}

/// An item forwarded by the background stream-drain task to the consumer.
///
/// The explicit `End` sentinel is the linchpin of the C2 ("never silently
/// short-read") invariant: a clean server half-close arrives as `End`,
/// whereas the receiver channel closing *without* an `End` (drain task
/// panicked / was aborted) is treated as an error, never as EOF.
enum StreamItem {
    /// A response frame from the worker.
    Data(ReadResponse),
    /// Clean end-of-stream (server half-closed via `message() == Ok(None)`).
    End,
    /// The drain task observed a transport error.
    Error(Error),
}

/// Where a [`GrpcBlockReader`] pulls response frames from.
enum ChunkSource {
    /// Direct streaming — used by positioned (random) reads. ACKs are sent
    /// per chunk; no background task is spawned (one-shot, low overhead).
    Direct(Streaming<ReadResponse>),
    /// Buffered drain — used by the sequential read path (Part V R1-B-b). A
    /// background task drains the tonic stream into a bounded channel so the
    /// network pull is decoupled from application consumption.
    Buffered {
        rx: mpsc::Receiver<StreamItem>,
        task: JoinHandle<()>,
    },
}

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
    /// Source of server → client responses (data chunks).
    source: ChunkSource,
    /// Bytes received since the last flow-control ACK was emitted.
    bytes_since_last_ack: i64,
    /// Chunks received since the last flow-control ACK was emitted.
    chunks_since_last_ack: u32,
    /// ACK coalescing threshold in bytes (`0` ⇒ ACK every chunk, used by the
    /// Direct positioned-read path to preserve the original behaviour).
    ack_interval_bytes: i64,
    /// ACK coalescing threshold in chunks.
    ack_interval_chunks: u32,
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

/// Decide whether a coalesced flow-control ACK should be emitted now
/// (Part V R1-B-c). Pure function so the policy is unit-testable without a
/// live stream.
///
/// An ACK fires when *either* coalescing threshold is reached, *or* the full
/// requested range has been received (forced final ACK). With
/// `ack_interval_bytes == 0` this degenerates to "ACK every chunk", which is
/// what the Direct positioned-read path uses to preserve original behaviour.
fn should_send_ack(
    bytes_since_last_ack: i64,
    chunks_since_last_ack: u32,
    ack_interval_bytes: i64,
    ack_interval_chunks: u32,
    bytes_received: i64,
    length: i64,
) -> bool {
    bytes_since_last_ack >= ack_interval_bytes
        || chunks_since_last_ack >= ack_interval_chunks
        || bytes_received >= length
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
            .read_block(
                block_id,
                offset,
                length,
                chunk_size,
                None,
                open_ufs_block_options,
            )
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
            source: ChunkSource::Direct(response_rx),
            bytes_since_last_ack: 0,
            chunks_since_last_ack: 0,
            // Direct mode keeps the original ACK-per-chunk behaviour.
            ack_interval_bytes: 0,
            ack_interval_chunks: 1,
        })
    }

    /// Open a sequential block reader with prefetch + buffered drain + ACK
    /// coalescing (Part V R1-B).
    ///
    /// Unlike [`Self::open`], this:
    /// - sends `prefetch_window` on the initial request so the worker keeps
    ///   `(1 + prefetch_window)` chunks in flight (R1-B-a);
    /// - spawns a background task that drains the tonic stream into a bounded
    ///   channel (`buffer_messages` deep), decoupling network pull from
    ///   application consumption (R1-B-b);
    /// - coalesces flow-control ACKs to one per `ack_interval_bytes` /
    ///   `ack_interval_chunks` (plus a forced ACK at EOF), cutting round-trips
    ///   (R1-B-c). **Default is one ACK per chunk** (`ack_interval_*` ⇒ every
    ///   chunk) which is deadlock-safe regardless of the worker's flow-control
    ///   window; the `try_send` path still removes the blocking ACK cost.
    ///   Coalescing (>1 chunk) is opt-in via `GoosefsConfig` for workers
    ///   confirmed to honour `prefetch_window`.
    pub async fn open_sequential(
        worker: &WorkerClient,
        block_id: i64,
        offset: i64,
        length: i64,
        chunk_size: i64,
        open_ufs_block_options: Option<OpenUfsBlockOptions>,
        tuning: ReadTuning,
    ) -> Result<Self> {
        let (request_tx, response_rx) = worker
            .read_block(
                block_id,
                offset,
                length,
                chunk_size,
                Some(tuning.prefetch_window),
                open_ufs_block_options,
            )
            .await?;

        crate::metrics::gauge(name::CLIENT_BLOCKS_READ_IN_PROGRESS)
            .set(crate::metrics::gauge(name::CLIENT_BLOCKS_READ_IN_PROGRESS).get() + 1);

        // Spawn the background drain task. It forwards each frame as a
        // `StreamItem`, emits an explicit `End` sentinel on clean half-close,
        // and forwards transport errors as `Error`. The consumer
        // (`read_chunk`) distinguishes "channel closed with End" (clean EOF)
        // from "channel closed without End" (task aborted/panicked → error),
        // upholding the C2 no-silent-short-read invariant.
        let (chunk_tx, chunk_rx) = mpsc::channel::<StreamItem>(tuning.buffer_messages);
        let mut stream = response_rx;
        let task = tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(resp)) => {
                        if chunk_tx.send(StreamItem::Data(resp)).await.is_err() {
                            // Consumer dropped — stop draining.
                            break;
                        }
                    }
                    Ok(None) => {
                        let _ = chunk_tx.send(StreamItem::End).await;
                        break;
                    }
                    Err(status) => {
                        let _ = chunk_tx.send(StreamItem::Error(Error::from(status))).await;
                        break;
                    }
                }
            }
        });

        debug!(
            block_id = block_id,
            offset = offset,
            length = length,
            prefetch_window = tuning.prefetch_window,
            buffer_messages = tuning.buffer_messages,
            "opened GrpcBlockReader (sequential, buffered)"
        );

        Ok(Self {
            block_id,
            offset,
            length,
            bytes_received: 0,
            request_tx,
            source: ChunkSource::Buffered { rx: chunk_rx, task },
            bytes_since_last_ack: 0,
            chunks_since_last_ack: 0,
            ack_interval_bytes: tuning.ack_interval_bytes,
            ack_interval_chunks: tuning.ack_interval_chunks,
        })
    }

    /// Read the next data chunk from the stream.
    ///
    /// Returns `None` when all expected data has been received.
    /// Sends a flow-control `offset_received` ACK after each chunk (Direct)
    /// or once per coalescing window (Buffered).
    pub async fn read_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.bytes_received >= self.length {
            return Ok(None);
        }

        // Loop instead of recursing: the server may emit several empty
        // keep-alive / header-only frames in a row, and `Box::pin`-ing the
        // recursive call would otherwise heap-allocate per empty frame.
        loop {
            let resp = match &mut self.source {
                ChunkSource::Direct(stream) => match stream.message().await? {
                    None => {
                        debug!(
                            block_id = self.block_id,
                            bytes_received = self.bytes_received,
                            "stream ended before all expected data received"
                        );
                        return Ok(None);
                    }
                    Some(r) => r,
                },
                ChunkSource::Buffered { rx, .. } => match rx.recv().await {
                    Some(StreamItem::Data(r)) => r,
                    Some(StreamItem::End) => {
                        debug!(
                            block_id = self.block_id,
                            bytes_received = self.bytes_received,
                            "buffered stream reached clean EOF"
                        );
                        return Ok(None);
                    }
                    Some(StreamItem::Error(e)) => return Err(e),
                    None => {
                        // C2: the drain task closed the channel WITHOUT an
                        // `End` sentinel. This means it panicked or was
                        // aborted mid-stream — NOT a clean EOF. Surface an
                        // error rather than silently truncating user data.
                        return Err(Error::Internal {
                            message: format!(
                                "read stream drain task ended unexpectedly on block {} \
                                 ({} of {} bytes received)",
                                self.block_id, self.bytes_received, self.length
                            ),
                            source: None,
                        });
                    }
                },
            };

            match classify_response(Some(resp)) {
                // `classify_response(Some(_))` never yields `Eof` — that is
                // reserved for `None`, which is handled in the source match
                // above.
                ChunkAction::Eof => unreachable!("Some(_) cannot classify as Eof"),
                ChunkAction::KeepReading => {
                    trace!(
                        block_id = self.block_id,
                        bytes_received = self.bytes_received,
                        expected = self.length,
                        "received empty data frame, awaiting next chunk"
                    );
                    continue;
                }
                ChunkAction::Deliver(data) => {
                    let len = data.len() as i64;
                    self.bytes_received += len;
                    trace!(
                        block_id = self.block_id,
                        chunk_len = data.len(),
                        total_received = self.bytes_received,
                        "received chunk"
                    );

                    // Instrument: increment read bytes counter.
                    crate::metrics::counter(name::CLIENT_BYTES_READ_LOCAL).inc(len);

                    self.maybe_send_ack(len);

                    return Ok(Some(data));
                }
            }
        }
    }

    /// Decide whether to emit a coalesced flow-control ACK and, if so, send it.
    ///
    /// Sends one `offset_received` ACK per `ack_interval_bytes` /
    /// `ack_interval_chunks` window, plus a forced ACK once the full range has
    /// been received. Uses `try_send`:
    /// - `Full` ⇒ **keep the counters** and retry on the next chunk. Dropping
    ///   an ACK here is a *liveness* concern only, never a correctness one:
    ///   `offset_received` is always `offset + bytes_received`, which is
    ///   monotonic regardless of how many ACKs are skipped (B4-2).
    /// - `Closed` ⇒ the stream is finishing; log and move on.
    fn maybe_send_ack(&mut self, delta: i64) {
        self.bytes_since_last_ack += delta;
        self.chunks_since_last_ack += 1;

        let need_ack = should_send_ack(
            self.bytes_since_last_ack,
            self.chunks_since_last_ack,
            self.ack_interval_bytes,
            self.ack_interval_chunks,
            self.bytes_received,
            self.length,
        );
        if !need_ack {
            return;
        }

        let ack = ReadRequest {
            offset_received: Some(self.offset + self.bytes_received),
            ..Default::default()
        };
        match self.request_tx.try_send(ack) {
            Ok(()) => {
                self.bytes_since_last_ack = 0;
                self.chunks_since_last_ack = 0;
            }
            Err(TrySendError::Full(_)) => {
                // Keep counters; retry next chunk (liveness, not correctness).
            }
            Err(TrySendError::Closed(_)) => {
                warn!(
                    block_id = self.block_id,
                    "ACK channel closed (read may be complete)"
                );
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
            source: ChunkSource::Direct(response_rx),
            bytes_since_last_ack: 0,
            chunks_since_last_ack: 0,
            // Positioned reads ACK every chunk (one-shot, low overhead).
            ack_interval_bytes: 0,
            ack_interval_chunks: 1,
        };

        reader.read_all().await
    }
}

impl Drop for GrpcBlockReader {
    fn drop(&mut self) {
        // Abort the background drain task (if any) so it does not keep
        // draining the stream after the consumer goes away (Part V R1-B-b).
        if let ChunkSource::Buffered { task, .. } = &self.source {
            task.abort();
        }
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

    /// R1-B-c: Direct positioned-read mode (`ack_interval_bytes == 0`,
    /// `ack_interval_chunks == 1`) ACKs on every chunk.
    #[test]
    fn should_send_ack_direct_mode_acks_every_chunk() {
        // 1 chunk, tiny payload, far from completion → still ACKs (chunks>=1).
        assert!(should_send_ack(64, 1, 0, 1, 64, 1_000_000));
    }

    /// R1-B-c: in coalescing mode the ACK is suppressed until a threshold or
    /// completion is hit.
    #[test]
    fn should_send_ack_coalesces_until_threshold() {
        let interval_bytes = 4 * 1024 * 1024;
        let interval_chunks = 4;
        // 2 chunks / 2 MiB so far, mid-stream → no ACK yet.
        assert!(!should_send_ack(
            2 * 1024 * 1024,
            2,
            interval_bytes,
            interval_chunks,
            2 * 1024 * 1024,
            64 * 1024 * 1024
        ));
        // Byte threshold reached → ACK.
        assert!(should_send_ack(
            interval_bytes,
            2,
            interval_bytes,
            interval_chunks,
            interval_bytes,
            64 * 1024 * 1024
        ));
        // Chunk threshold reached → ACK.
        assert!(should_send_ack(
            1024,
            interval_chunks,
            interval_bytes,
            interval_chunks,
            4096,
            64 * 1024 * 1024
        ));
    }

    /// R1-B-c: the final chunk that completes the range always forces an ACK,
    /// even if neither coalescing threshold is met.
    #[test]
    fn should_send_ack_forces_final_ack_at_completion() {
        assert!(should_send_ack(
            64,
            1,
            4 * 1024 * 1024,
            4,
            1_000_000,
            1_000_000
        ));
    }

    /// R1-B: `ReadTuning::from_config` reflects config defaults and clamps.
    #[test]
    fn read_tuning_from_config_defaults_and_clamps() {
        let mut cfg = crate::config::GoosefsConfig::new("127.0.0.1:9200");
        let t = ReadTuning::from_config(&cfg);
        assert_eq!(t.prefetch_window, 8);
        assert_eq!(t.buffer_messages, 16);
        assert_eq!(t.ack_interval_bytes, 0); // ACK every chunk (deadlock-safe default)
        assert_eq!(t.ack_interval_chunks, 1);

        // Degenerate config values are clamped to safe minimums.
        cfg.read_buffer_messages = 0;
        cfg.ack_interval_chunks = 0;
        cfg.ack_interval_bytes = -1;
        let t = ReadTuning::from_config(&cfg);
        assert_eq!(t.buffer_messages, 1);
        assert_eq!(t.ack_interval_chunks, 1);
        assert_eq!(t.ack_interval_bytes, 0);
    }
}
