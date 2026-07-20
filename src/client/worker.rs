//! Goosefs Worker gRPC client for block data read/write.
//!
//! Wraps `BlockWorker` service (Worker:9203) providing:
//! - `read_block` — bidirectional streaming block read
//! - `write_block` — bidirectional streaming block write
//!
//! ## Write Protocol
//!
//! Goosefs Worker's `WriteBlock` is a bidirectional streaming RPC but the server
//! does **not** send HTTP/2 response headers until the client sends a `flush`
//! command or closes the stream. This means tonic's
//! `client.write_block(stream).await` will block until the first server response.
//!
//! To work around this, `write_block()` returns a [`WriteBlockHandle`] that
//! runs the gRPC call in a background tokio task. The caller sends data chunks
//! through the request sender, then calls `flush()` or `close()` on the handle
//! to receive server responses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, instrument, warn};

use arc_swap::ArcSwap;

use crate::auth::{ChannelAuthenticator, ChannelIdInterceptor, SaslStreamGuard};
use crate::config::GoosefsConfig;
use crate::error::{Error, Result};
use crate::proto::grpc::block::{
    block_worker_client::BlockWorkerClient, write_request, OpenLocalBlockRequest,
    OpenLocalBlockResponse, ReadRequest, ReadResponse, RequestType, WriteRequest,
    WriteRequestCommand, WriteResponse,
};
use crate::proto::proto::dataserver::{CreateUfsFileOptions, OpenUfsBlockOptions};
use crate::proto::proto::security::Capability;

/// Options for a `write_block` RPC that control *where* the Worker writes data.
///
/// - `GoosefsBlock` (default): write to Goosefs cache (MUST_CACHE / CACHE_THROUGH / ASYNC_THROUGH)
/// - `UfsFile`: write directly to UFS (THROUGH mode), requires `create_ufs_file_options`
/// - `UfsFallbackBlock`: cache-full fallback to UFS (TRY_CACHE)
#[derive(Clone, Debug)]
pub struct WriteBlockOptions {
    /// The request type sent in the initial `WriteRequestCommand`.
    pub request_type: RequestType,
    /// UFS file creation options (required when `request_type == UfsFile`).
    pub create_ufs_file_options: Option<CreateUfsFileOptions>,
    /// Whether the write is asynchronous (ASYNC_THROUGH write type).
    /// When true, the worker may flush data to UFS asynchronously after the
    /// stream is closed. Defaults to `false`.
    pub async_write: bool,
}

impl Default for WriteBlockOptions {
    fn default() -> Self {
        Self {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
            async_write: false,
        }
    }
}

/// Handle for an in-progress `WriteBlock` bidirectional streaming RPC.
///
/// The gRPC call runs in a background tokio task. The caller sends data through
/// `request_tx` and receives responses via `recv_response()`. When done, call
/// `close()` to drop the request channel and wait for the server to finalize.
pub struct WriteBlockHandle {
    /// Block being written.
    block_id: i64,
    /// Sender for client → server WriteRequest messages (data chunks, flush commands).
    ///
    /// Wrapped in `Option` so that `close()` can `take()` the sender (closing
    /// the client→server half of the stream) without violating the move
    /// semantics imposed by this type's `Drop` impl. `None` after `close()`
    /// has run; senders attempting to use it should treat that as
    /// "stream already closed".
    pub request_tx: Option<mpsc::Sender<WriteRequest>>,
    /// Receiver for server → client WriteResponse messages, forwarded from the background task.
    response_rx: mpsc::Receiver<std::result::Result<WriteResponse, tonic::Status>>,
    /// Handle to the background gRPC task that drives the bidirectional
    /// `write_block` stream.
    ///
    /// The handle is wrapped in `Option` so that `close()` / `cancel()` can
    /// take ownership (`take()`) and either await it (close) or abort it
    /// (cancel). The `Drop` impl below also aborts the task as a safety net
    /// in case the handle is dropped without going through `close`/`cancel`.
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl WriteBlockHandle {
    /// Receive the next `WriteResponse` from the server (e.g., flush ack).
    ///
    /// Returns `None` if the server has closed the response stream.
    pub async fn recv_response(&mut self) -> Result<Option<WriteResponse>> {
        match self.response_rx.recv().await {
            Some(Ok(resp)) => Ok(Some(resp)),
            Some(Err(status)) => Err(Error::GrpcError {
                message: format!(
                    "WriteBlock server error for block_id={}: {}",
                    self.block_id, status
                ),
                source: Box::new(status),
            }),
            None => Ok(None),
        }
    }

    /// Close the write stream by dropping the request sender and wait for
    /// any final response from the server.
    pub async fn close(mut self) -> Result<()> {
        // Drop the request sender to close the client→server half of the stream.
        // The server will then call onCompleted → commitBlock → replySuccess.
        // `take()` returns the sender (or `None` if already taken) — dropping
        // it here closes the stream half.
        drop(self.request_tx.take());
        debug!(
            block_id = self.block_id,
            "closed write stream, waiting for server finalize"
        );
        // Wait for the server's final response (or stream close).
        // This ensures the background task finishes before we return,
        // preventing the Channel from being dropped while the task is still running.
        let mut last_err: Option<Error> = None;
        while let Some(result) = self.response_rx.recv().await {
            match result {
                Ok(_resp) => {
                    debug!(
                        block_id = self.block_id,
                        "received final response from server"
                    );
                }
                Err(status) => {
                    last_err = Some(Error::GrpcError {
                        message: format!(
                            "WriteBlock server error for block_id={}: {}",
                            self.block_id, status
                        ),
                        source: Box::new(status),
                    });
                    break;
                }
            }
        }
        // Join the background task so we surface a panic (rather than silently
        // detaching it). The task should be finished by now because the
        // response stream has been drained to `None` (or we broke out on
        // error). We use a short timeout as a defensive measure so a
        // hypothetical bug in the task does not hang `close()` forever.
        if let Some(handle) = self.task_handle.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    if join_err.is_panic() {
                        warn!(
                            block_id = self.block_id,
                            "WriteBlock background task panicked"
                        );
                    }
                    // Cancelled or panicked — surface as error only if we
                    // do not already have one from the stream.
                    if last_err.is_none() {
                        last_err = Some(Error::Internal {
                            message: format!(
                                "WriteBlock background task ended abnormally for block_id={}: {}",
                                self.block_id, join_err
                            ),
                            source: None,
                        });
                    }
                }
                Err(_) => {
                    warn!(
                        block_id = self.block_id,
                        "WriteBlock background task did not finish within 5s after stream drain; aborting"
                    );
                    // We cannot await again here because the JoinHandle was
                    // moved into `timeout`; the task will be aborted when the
                    // tokio runtime drops the handle.
                }
            }
        }
        if let Some(e) = last_err {
            return Err(e);
        }
        Ok(())
    }

    /// Cancel the write stream without waiting for server finalization.
    ///
    /// Drops the request sender and response receiver immediately and
    /// aborts the background gRPC task so its resources are released
    /// promptly (rather than relying on the implicit "task exits because
    /// channels were dropped" behaviour, which leaves the JoinHandle
    /// detached on drop).
    /// Matches Java's `GrpcBlockingStream.cancel()`.
    pub async fn cancel(mut self) {
        // Abort the background task *before* dropping the channels so the
        // task does not race against the channel-closed signal. We then
        // drop the channels so any pending `Sender::send` / `recv` futures
        // owned by callers fail immediately.
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        // The remaining fields are dropped automatically when `self` goes
        // out of scope at the end of this function — we cannot explicitly
        // `drop(self.request_tx)` here because doing so while a `Drop` impl
        // exists for `WriteBlockHandle` would violate move semantics. The
        // observable behaviour is identical: when the function returns,
        // `self` (and therefore the channels) are dropped.
        debug!(block_id = self.block_id, "cancelled write stream");
    }
}

/// Safety net: aborts the background gRPC task if the handle is dropped
/// without going through `close()` / `cancel()`.
///
/// Without this, an early `?` return on the error path leaves a detached
/// tokio task that can hang indefinitely on `stream.message().await`
/// (e.g. on a half-open server connection that never sends a final response),
/// keeping the underlying tonic Channel alive and leaking resources.
///
/// `cancel()` and `close()` already `take()` the `task_handle`, so on the
/// happy path `task_handle` is `None` here and `abort()` is a no-op —
/// matching the doc-comment on `task_handle` above.
impl Drop for WriteBlockHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.task_handle.take() {
            debug!(
                block_id = self.block_id,
                "WriteBlockHandle dropped without close()/cancel(); aborting background task"
            );
            handle.abort();
        }
    }
}

/// RAII guard that keeps a Worker-side `OpenLocalBlock` session (and therefore
/// the block lock) alive for as long as a short-circuit reader needs the
/// mmap'd block file.
///
/// # Protocol
///
/// `OpenLocalBlock` is a bidirectional streaming RPC ("Replaces
/// ShortCircuitBlockReadHandler"). The client sends a single
/// [`OpenLocalBlockRequest`]; the Worker locks the block, replies with an
/// [`OpenLocalBlockResponse`] carrying the on-disk `path` + logical
/// `block_size`, and **holds the lock until the client half-closes the
/// stream**.
///
/// This guard owns the request `Sender` (the client→server half). Dropping the
/// guard drops the sender, which closes the client half of the stream → the
/// Worker observes `onCompleted` → unlocks the block.
///
/// # Drop / unlock semantics (design §8.2.1)
///
/// `Drop` is synchronous and cannot `await`, so unlock is **eventually
/// released**: dropping the sender signals the close, and the actual
/// `onCompleted`/unlock is finalized by the tonic runtime in the background.
/// The held `_response_stream` keeps the HTTP/2 stream state from being torn
/// down prematurely. Worst case, the Worker's own `OpenLocalBlock` session
/// idle-timeout reclaims the lock (mirroring Java). This satisfies INV-S2:
/// the lock is held for the whole reader lifetime and released in bounded time
/// after Drop, never leaked indefinitely.
pub struct OpenLocalBlockGuard {
    /// Block whose lock this guard holds (for tracing on Drop).
    block_id: i64,
    /// Client→server half of the bidi stream. Dropping this closes the stream
    /// and triggers the Worker-side unlock. Kept in an `Option` only so the
    /// `Drop` impl can emit a trace before the field is dropped.
    _request_tx: Option<mpsc::Sender<OpenLocalBlockRequest>>,
    /// Server→client half. Held (but not polled) so the stream is not torn
    /// down before `_request_tx` closes it; dropped together with the guard.
    ///
    /// Wrapped in a `std::sync::Mutex` purely to make the guard (and therefore
    /// [`LocalBlockReader`](crate::block::short_circuit::LocalBlockReader))
    /// `Sync`: tonic's `Streaming` is `Send` but `!Sync`, which would otherwise
    /// prevent sharing a cached reader across tasks via the process/context-
    /// level [`ShortCircuitFactory`](crate::block::short_circuit::ShortCircuitFactory).
    /// The mutex is never locked — the stream is only kept alive for Drop.
    _response_stream: std::sync::Mutex<Streaming<OpenLocalBlockResponse>>,
}

impl Drop for OpenLocalBlockGuard {
    fn drop(&mut self) {
        debug!(
            block_id = self.block_id,
            "OpenLocalBlockGuard dropped — closing bidi stream, Worker will unlock block"
        );
        // Explicit drop of the sender (the response stream is dropped with the
        // struct) closes the client half → Worker `onCompleted` → unlock.
        let _ = self._request_tx.take();
    }
}

/// Type alias for the authenticated Worker gRPC client.
type AuthenticatedBlockWorkerClient =
    BlockWorkerClient<InterceptedService<Channel, ChannelIdInterceptor>>;

/// Client for `BlockWorker` service on a single worker node.
///
/// Each `WorkerClient` carries a monotonic `generation` tag assigned by
/// [`WorkerClientPool`] at construction time.  The generation allows callers
/// that observed a failure on a specific client to request a **single-flight
/// reconnect** via [`WorkerClientPool::reconnect_if_stale`]: only the first
/// observer of generation `N` actually re-establishes the TCP+SASL
/// connection; all concurrent observers with the same (or older) generation
/// simply receive the already-replaced client.  This collapses the
/// "thundering-herd reconnect" that previously produced hundreds of duplicate
/// `authentication failed` warnings when a SASL session expired.
#[derive(Clone)]
pub struct WorkerClient {
    inner: AuthenticatedBlockWorkerClient,
    addr: String,
    /// Monotonic tag identifying this exact connection instance.
    ///
    /// Two clients cached for the same address must have different
    /// generations; a caller that observes a failure on generation `N` can
    /// ask the pool to reconnect *only if* generation has not advanced yet.
    generation: u64,
    /// Keeps the SASL authentication stream alive for the channel's lifetime.
    _sasl_guard: std::sync::Arc<Option<SaslStreamGuard>>,
}

impl WorkerClient {
    /// Connect to a Goosefs Worker at the given address with authentication.
    ///
    /// Authentication is performed according to `config.auth_type`.
    pub async fn connect(addr: &str, config: &GoosefsConfig) -> Result<Self> {
        let endpoint = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| Error::ConfigError {
                message: format!("invalid worker endpoint: {}", e),
            })?
            .connect_timeout(config.connect_timeout)
            // Set request_timeout: workers are the data plane and most prone
            // to half-open connections. Without this, a hung gRPC stream
            // (`read_block` / `write_block`) can stall indefinitely while
            // the master/metrics/worker_manager paths all already enforce
            // request_timeout.
            .timeout(config.request_timeout);

        let channel = endpoint.connect().await?;

        // Perform SASL authentication based on the configured auth type
        let authenticator =
            ChannelAuthenticator::new(config.auth_type, config.auth_username.clone(), None)
                .with_auth_timeout(config.auth_timeout);

        let mut auth_channel = authenticator.authenticate(channel).await?;
        let sasl_guard = auth_channel.take_sasl_guard();
        debug!(addr = %addr, auth_type = %config.auth_type, "connected to Goosefs Worker");

        Ok(Self {
            inner: BlockWorkerClient::new(auth_channel.channel),
            addr: addr.to_string(),
            generation: 0,
            _sasl_guard: std::sync::Arc::new(sasl_guard),
        })
    }

    /// Connect to a Goosefs Worker with only connect_timeout (backward compatible, NOSASL).
    ///
    /// **Deprecated**: Use `connect(addr, config)` instead for proper authentication.
    pub async fn connect_simple(addr: &str, connect_timeout: Duration) -> Result<Self> {
        let endpoint = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| Error::ConfigError {
                message: format!("invalid worker endpoint: {}", e),
            })?
            .connect_timeout(connect_timeout);

        let channel = endpoint.connect().await?;
        let interceptor = ChannelIdInterceptor::new(uuid::Uuid::new_v4().to_string());
        let intercepted = InterceptedService::new(channel, interceptor);
        debug!(addr = %addr, "connected to Goosefs Worker (no auth)");

        Ok(Self {
            inner: BlockWorkerClient::new(intercepted),
            addr: addr.to_string(),
            generation: 0,
            _sasl_guard: std::sync::Arc::new(None),
        })
    }

    /// Create from an existing tonic channel (useful for testing / channel sharing).
    ///
    /// **Note**: This bypasses authentication.
    pub fn from_channel(channel: Channel, addr: String) -> Self {
        let interceptor = ChannelIdInterceptor::new("test-no-auth".to_string());
        let intercepted = InterceptedService::new(channel, interceptor);
        Self {
            inner: BlockWorkerClient::new(intercepted),
            addr,
            generation: 0,
            _sasl_guard: std::sync::Arc::new(None),
        }
    }

    /// Start a bidirectional streaming ReadBlock RPC.
    ///
    /// Returns: (request_sender, response_stream)
    ///
    /// The caller sends an initial `ReadRequest` with block_id/offset/length,
    /// then sends periodic `offset_received` ACKs. The response stream yields
    /// `ReadResponse` containing `Chunk` data.
    ///
    /// When the block is only stored in UFS (e.g. written with `THROUGH` mode),
    /// `open_ufs_block_options` must be provided so the Worker knows how to
    /// locate and read the data from the underlying storage.
    #[instrument(skip(self, open_ufs_block_options), fields(block_id = %block_id, offset = %offset, length = %length))]
    pub async fn read_block(
        &self,
        block_id: i64,
        offset: i64,
        length: i64,
        chunk_size: i64,
        prefetch_window: Option<i32>,
        open_ufs_block_options: Option<OpenUfsBlockOptions>,
    ) -> Result<(mpsc::Sender<ReadRequest>, Streaming<ReadResponse>)> {
        let (tx, rx) = mpsc::channel::<ReadRequest>(32);

        // Send the initial read request. `prefetch_window` (Part V R1-B-a)
        // tells the worker how many extra chunks it may keep in flight,
        // raising the sequential-stream pipeline depth above the default
        // 4 MiB fallback.
        let initial_request = ReadRequest {
            block_id: Some(block_id),
            offset: Some(offset),
            length: Some(length),
            chunk_size: Some(chunk_size),
            open_ufs_block_options,
            offset_received: None,
            position_short: None,
            request_id: None,
            capability: None,
            block_size: None,
            prefetch_window,
        };
        tx.send(initial_request)
            .await
            .map_err(|_| Error::BlockIoError {
                message: "failed to send initial ReadRequest".to_string(),
            })?;

        let stream = ReceiverStream::new(rx);
        let response = self.inner.clone().read_block(stream).await?;

        Ok((tx, response.into_inner()))
    }

    /// Open a positioned (random-access) block read stream.
    ///
    /// Identical to [`read_block`](Self::read_block) but sets `position_short = true` in the
    /// initial `ReadRequest`, instructing the worker to skip prefetch and
    /// serve the exact requested byte range.
    ///
    /// Used by [`crate::io::reader::GrpcBlockReader::positioned_read`].
    pub async fn read_block_positioned(
        &self,
        block_id: i64,
        offset: i64,
        length: i64,
        chunk_size: i64,
        open_ufs_block_options: Option<OpenUfsBlockOptions>,
    ) -> Result<(mpsc::Sender<ReadRequest>, Streaming<ReadResponse>)> {
        let (tx, rx) = mpsc::channel::<ReadRequest>(32);

        let initial_request = ReadRequest {
            block_id: Some(block_id),
            offset: Some(offset),
            length: Some(length),
            chunk_size: Some(chunk_size),
            open_ufs_block_options,
            offset_received: None,
            position_short: Some(true), // positioned-read hint to worker
            request_id: None,
            capability: None,
            block_size: None,
            prefetch_window: None,
        };
        tx.send(initial_request)
            .await
            .map_err(|_| Error::BlockIoError {
                message: "failed to send initial positioned ReadRequest".to_string(),
            })?;

        let stream = ReceiverStream::new(rx);
        let response = self.inner.clone().read_block(stream).await?;

        Ok((tx, response.into_inner()))
    }

    /// Open a **short-circuit** local block session.
    ///
    /// Drives the `OpenLocalBlock` bidirectional streaming RPC (design §3.1):
    /// sends a single [`OpenLocalBlockRequest`] `{ block_id, capability,
    /// block_size }`, waits for the first [`OpenLocalBlockResponse`] (carrying
    /// the on-disk `path` and the logical `block_size`), and returns it
    /// together with an [`OpenLocalBlockGuard`] that keeps the Worker-side
    /// block lock held for the guard's lifetime.
    ///
    /// The caller (the short-circuit data plane) `mmap`s the returned `path`
    /// and reads bytes locally with zero further RPCs. When the reader is
    /// dropped, the guard closes the stream and the Worker unlocks the block.
    ///
    /// # capability
    ///
    /// `capability` must be `Some` on capability-enabled clusters or the
    /// Worker will reject the request (matching the gRPC read path's
    /// authorization, INV-S3). The source of the capability is resolved by the
    /// caller; `None` means "no capability" (NOSASL / capability-disabled
    /// clusters).
    ///
    /// # Errors
    ///
    /// - [`Error`] from the underlying transport / gRPC status (e.g.
    ///   `NotFound` when the block is not local, `PermissionDenied` /
    ///   `AuthenticationFailed` for capability rejection). The short-circuit
    ///   factory classifies these into fallback-vs-propagate (design §3.6).
    #[instrument(skip(self, capability), fields(block_id = %block_id, block_size = %block_size))]
    pub async fn open_local_block(
        &self,
        block_id: i64,
        block_size: i64,
        capability: Option<Capability>,
    ) -> Result<(OpenLocalBlockResponse, OpenLocalBlockGuard)> {
        let (tx, rx) = mpsc::channel::<OpenLocalBlockRequest>(4);

        let request = OpenLocalBlockRequest {
            block_id: Some(block_id),
            capability,
            block_size: Some(block_size),
        };
        tx.send(request).await.map_err(|_| Error::BlockIoError {
            message: format!("failed to send OpenLocalBlockRequest for block_id={block_id}"),
        })?;

        let stream = ReceiverStream::new(rx);
        let response = self.inner.clone().open_local_block(stream).await?;
        let mut response_stream = response.into_inner();

        // The Worker locks the block and replies with the local path. Pull the
        // first response; the stream then stays open (lock held) until the
        // guard is dropped.
        let first = response_stream
            .message()
            .await?
            .ok_or_else(|| Error::BlockIoError {
                message: format!(
                    "OpenLocalBlock stream for block_id={block_id} closed before any response"
                ),
            })?;

        debug!(
            block_id = block_id,
            path = first.path.as_deref().unwrap_or(""),
            block_size = ?first.block_size,
            "OpenLocalBlock granted local path"
        );

        let guard = OpenLocalBlockGuard {
            block_id,
            _request_tx: Some(tx),
            _response_stream: std::sync::Mutex::new(response_stream),
        };
        Ok((first, guard))
    }

    /// Start a bidirectional streaming WriteBlock RPC.
    ///
    /// Returns a [`WriteBlockHandle`] that manages the background gRPC task.
    /// The caller sends data chunks through `handle.request_tx`, then calls
    /// `handle.recv_response()` to get flush acknowledgements.
    ///
    /// ## Why a background task?
    ///
    /// Goosefs Worker's `WriteBlock` RPC does **not** send HTTP/2 response
    /// headers until the client sends a `flush` command or closes the stream.
    /// tonic's `client.write_block(stream).await` waits for response headers
    /// before resolving, so calling it inline would deadlock — we'd need the
    /// returned sender to send flush, but we can't get the sender until the
    /// call resolves.
    ///
    /// By spawning the gRPC call in a background task and forwarding responses
    /// through an mpsc channel, we decouple request sending from response
    /// receiving.
    #[instrument(skip(self, options), fields(block_id = %block_id))]
    pub async fn write_block(
        &self,
        block_id: i64,
        space_to_reserve: i64,
        options: WriteBlockOptions,
    ) -> Result<WriteBlockHandle> {
        let (tx, rx) = mpsc::channel::<WriteRequest>(32);

        // Build the initial write command
        let initial_command = WriteRequest {
            value: Some(write_request::Value::Command(WriteRequestCommand {
                r#type: Some(options.request_type as i32),
                id: Some(block_id),
                offset: Some(0),
                flush: None,
                create_ufs_file_options: options.create_ufs_file_options,
                space_to_reserve: Some(space_to_reserve),
                capability: None,
                medium_type: None,
                async_write: Some(options.async_write),
            })),
        };

        // Build a composite stream: initial command first, then channel messages.
        let initial_stream = tokio_stream::once(initial_command);
        let subsequent_stream = ReceiverStream::new(rx);
        let combined_stream = initial_stream.chain(subsequent_stream);

        // Channel for forwarding server responses from the background task.
        let (resp_tx, resp_rx) =
            mpsc::channel::<std::result::Result<WriteResponse, tonic::Status>>(8);

        let mut client = self.inner.clone();
        let addr = self.addr.clone();

        let task_handle = tokio::spawn(async move {
            debug!(block_id = block_id, addr = %addr, "WriteBlock gRPC task started");

            // This call blocks until the server sends response headers,
            // which happens on the first flush or stream close.
            let call_result = client.write_block(combined_stream).await;

            match call_result {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    // Forward all server responses to the caller.
                    loop {
                        match stream.message().await {
                            Ok(Some(msg)) => {
                                if resp_tx.send(Ok(msg)).await.is_err() {
                                    debug!(block_id = block_id, "response receiver dropped");
                                    break;
                                }
                            }
                            Ok(None) => {
                                debug!(block_id = block_id, "server closed response stream");
                                break;
                            }
                            Err(status) => {
                                warn!(block_id = block_id, %status, "server response error");
                                let _ = resp_tx.send(Err(status)).await;
                                break;
                            }
                        }
                    }
                }
                Err(status) => {
                    warn!(block_id = block_id, %status, "WriteBlock RPC failed");
                    let _ = resp_tx.send(Err(status)).await;
                }
            }

            debug!(block_id = block_id, "WriteBlock gRPC task finished");
        });

        debug!(block_id = block_id, "WriteBlock handle created");

        Ok(WriteBlockHandle {
            block_id,
            request_tx: Some(tx),
            response_rx: resp_rx,
            task_handle: Some(task_handle),
        })
    }

    /// The worker address this client is connected to.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The monotonic generation tag assigned by the pool.
    ///
    /// Callers should save this value alongside the `WorkerClient` when
    /// starting an RPC; if the RPC fails with an authentication error they
    /// pass the saved generation back to
    /// [`WorkerClientPool::reconnect_if_stale`] to trigger a single-flight
    /// reconnect (de-duplicating concurrent observers of the same failure).
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Connection pool for `WorkerClient` instances.
///
/// Caches authenticated gRPC channels by worker address, avoiding the overhead
/// of re-establishing connections and re-authenticating for every block I/O.
/// Matches Java's `FileSystemContext.acquireBlockWorkerClient()` pattern.
///
/// The pool is thread-safe and can be shared across concurrent workers.
///
/// ## Single-Flight Reconnect
///
/// When a SASL stream silently expires server-side, many concurrent RPCs on
/// the same cached channel will fail simultaneously with UNAUTHENTICATED.
/// Without coordination each observer would independently invoke `reconnect`,
/// producing a "thundering herd" that serialises through the pool's write
/// lock and wastes CPU/RTT on duplicate TCP+SASL handshakes.
///
/// To collapse this herd, each [`WorkerClient`] carries a monotonic
/// `generation` tag.  Callers pass the observed generation back into
/// [`reconnect_if_stale`](Self::reconnect_if_stale) after an auth failure;
/// only the **first** observer of a given generation actually performs the
/// reconnect, all other concurrent observers receive the already-replaced
/// client.  This reduces N concurrent reconnects to exactly 1.
pub struct WorkerClientPool {
    /// Cached worker clients keyed by a **channel key**.
    ///
    /// With `pool_size == 1` the key is just the `"host:port"` address (legacy
    /// behaviour). With `pool_size > 1` the key is `"host:port#slot"`, so each
    /// worker address owns `pool_size` independent channels (each with its own
    /// SASL session + generation). The stored client carries its own
    /// `generation` in-band; readers clone it and inspect `client.generation()`.
    ///
    /// **Wait-free read path (P1)**: stored as an [`ArcSwap`] so the hot
    /// `acquire` path is a single atomic load + map lookup + cheap `WorkerClient`
    /// clone — no `tokio::sync::RwLock` round-trip. The client set changes only
    /// on connect-miss / reconnect / invalidate (all rare), so writers do a
    /// copy-on-write via [`ArcSwap::rcu`]; concurrent writers on different keys
    /// are reconciled by `rcu`'s retry loop, and same-key connects are
    /// single-flighted by the per-key reconnect mutex (see `acquire_by_key`).
    /// Mirrors the `ArcSwap<AuthedState>` model already used by `MasterClient`.
    clients: ArcSwap<HashMap<String, WorkerClient>>,
    /// Per-address async mutex guarding the reconnect critical section.
    ///
    /// Keyed by the same channel key as `clients`. Separated from `clients` so
    /// the reconnect handshake (which performs network I/O) does not hold the
    /// clients-map write lock. Acquiring this mutex for one channel does not
    /// block other channels' reconnects.
    ///
    /// **H2** (`docs/perf/2026-07-07-hotspot-optimizations/README.md`):
    /// changed from `tokio::sync::RwLock<HashMap<…>>` to `DashMap<…>` so the
    /// `reconnect_lock_for` read path is lock-free (shard-level striped lock
    /// inside DashMap, no async `.read().await` round-trip) — the old pattern
    /// contended under 32+ concurrent readers hitting the same address.
    reconnect_locks: DashMap<String, Arc<AsyncMutex<()>>>,
    /// Per-address round-robin counter used to pick the next channel slot when
    /// `pool_size > 1`. Lazily created on first `acquire` for an address.
    ///
    /// **H2**: same `RwLock<HashMap> → DashMap` swap as `reconnect_locks`
    /// above; `next_slot`'s hot path no longer takes an async read lock.
    addr_rr: DashMap<String, Arc<AtomicU64>>,
    /// Number of channels to pool per worker address (≥ 1).
    pool_size: usize,
    /// Monotonic counter used to hand out a unique `generation` for every
    /// freshly-created `WorkerClient` (global across all addresses + slots).
    next_generation: AtomicU64,
    /// Config used to create new connections.
    config: GoosefsConfig,
}

impl WorkerClientPool {
    /// Create a new empty connection pool.
    pub fn new(config: GoosefsConfig) -> Self {
        let pool_size = config.worker_connection_pool_size.max(1);
        Self {
            clients: ArcSwap::from_pointee(HashMap::new()),
            reconnect_locks: DashMap::new(),
            addr_rr: DashMap::new(),
            pool_size,
            // Start generations at 1 so `0` (the default on constructed-but-
            // never-pooled clients) is always "stale" relative to any pooled
            // client — this makes `reconnect_if_stale(addr, 0)` always force
            // a fresh connection when needed.
            next_generation: AtomicU64::new(1),
            config,
        }
    }

    /// Number of channels pooled per worker address.
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Channel-map key for `(addr, slot)`. For a single-channel pool this is
    /// just `addr` (byte-identical to the legacy behaviour).
    ///
    /// **H1** (`docs/perf/2026-07-07-hotspot-optimizations/README.md`):
    /// replaced `format!("{addr}#{slot}")` with a pre-sized `String` +
    /// `itoa::Buffer` to avoid the `core::fmt` machinery on every `acquire`
    /// when `pool_size > 1`.
    fn slot_key(&self, addr: &str, slot: usize) -> String {
        if self.pool_size <= 1 {
            return addr.to_string();
        }
        // `addr` + '#' + up-to-20-digit usize.
        let mut s = String::with_capacity(addr.len() + 21);
        s.push_str(addr);
        s.push('#');
        let mut buf = itoa::Buffer::new();
        s.push_str(buf.format(slot));
        s
    }

    /// Pick the next round-robin slot for `addr` (always `0` when `pool_size == 1`).
    ///
    /// **H2**: the `DashMap` read path is lock-free (shard-level striped
    /// lock inside DashMap, no async `.read().await` round-trip). The old
    /// `tokio::sync::RwLock<HashMap>` pattern took an async read lock on
    /// every `acquire`, which contended under 32+ concurrent readers.
    async fn next_slot(&self, addr: &str) -> usize {
        if self.pool_size <= 1 {
            return 0;
        }
        // Fast path: existing counter for this address.
        if let Some(c) = self.addr_rr.get(addr) {
            return (c.fetch_add(1, Ordering::Relaxed) % self.pool_size as u64) as usize;
        }
        // Miss: lazily create the counter. `entry()` is atomic.
        let c = self
            .addr_rr
            .entry(addr.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        (c.fetch_add(1, Ordering::Relaxed) % self.pool_size as u64) as usize
    }

    /// Acquire a `WorkerClient` for the given address.
    ///
    /// Returns a cached client if one exists, otherwise creates a new connection.
    /// With `pool_size > 1` this round-robins across the per-worker channels so
    /// concurrent block reads spread over multiple HTTP/2 connections. The
    /// tonic `Channel` itself also multiplexes, so each channel handles many
    /// concurrent RPCs.
    pub async fn acquire(&self, addr: &str) -> Result<WorkerClient> {
        let slot = self.next_slot(addr).await;
        let key = self.slot_key(addr, slot);
        self.acquire_by_key(&key, addr).await
    }

    /// Get-or-create the cached client for a specific channel `key`, connecting
    /// to `connect_addr` on a miss.
    async fn acquire_by_key(&self, key: &str, connect_addr: &str) -> Result<WorkerClient> {
        // Fast path (P1): wait-free atomic load + map lookup + cheap clone.
        // Hit on every steady-state block read — no lock, no await.
        if let Some(client) = self.clients.load().get(key).cloned() {
            debug!(key = %key, generation = client.generation, "reusing cached WorkerClient");
            return Ok(client);
        }

        // Miss: serialise the connect for this channel key via the per-key
        // reconnect mutex so concurrent callers for the *same* key share a
        // single TCP+SASL handshake. Callers for *different* keys still connect
        // concurrently (no global lock — unlike the previous coarse write lock).
        let lock = self.reconnect_lock_for(key).await;
        let _guard = lock.lock().await;
        // Double-check after acquiring the mutex (another task may have inserted
        // while we queued).
        if let Some(client) = self.clients.load().get(key).cloned() {
            return Ok(client);
        }

        debug!(key = %key, addr = %connect_addr, "creating new WorkerClient for pool");
        let mut client = WorkerClient::connect(connect_addr, &self.config).await?;
        client.generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.insert_client(key, client.clone());
        Ok(client)
    }

    /// Copy-on-write insert into the `clients` map (wait-free readers).
    ///
    /// Uses [`ArcSwap::rcu`] so concurrent inserts on *different* keys do not
    /// clobber each other (the closure re-runs on contention).
    fn insert_client(&self, key: &str, client: WorkerClient) {
        self.clients.rcu(|cur| {
            let mut next = (**cur).clone();
            next.insert(key.to_string(), client.clone());
            next
        });
    }

    /// Copy-on-write remove from the `clients` map (wait-free readers).
    fn remove_client(&self, key: &str) {
        self.clients.rcu(|cur| {
            let mut next = (**cur).clone();
            next.remove(key);
            next
        });
    }

    /// Remove a worker from the pool (e.g., after a connection failure).
    ///
    /// The next `acquire()` call for this address will create a fresh connection.
    ///
    /// Also drops the per-address reconnect mutex from `reconnect_locks` so
    /// the map does not grow unbounded when many distinct worker addresses
    /// come and go (e.g. worker scale-up / scale-down). It is safe to drop
    /// the mutex here because:
    /// - any caller that already holds an `Arc<AsyncMutex<()>>` clone keeps
    ///   it alive for the duration of its critical section;
    /// - the *next* `reconnect_lock_for()` call for the same address will
    ///   lazily install a fresh mutex.
    /// In the worst case two callers racing across an `invalidate()` may
    /// hold *different* mutex instances briefly, but the cache double-check
    /// inside `reconnect_if_stale` still serialises the actual handshake
    /// via the generation comparison, so correctness is preserved.
    pub async fn invalidate(&self, addr: &str) {
        // Remove every channel slot for this address.
        let keys: Vec<String> = (0..self.pool_size)
            .map(|s| self.slot_key(addr, s))
            .collect();
        // Single copy-on-write pass removing all slots for this address.
        self.clients.rcu(|cur| {
            let mut next = (**cur).clone();
            for k in &keys {
                if next.remove(k).is_some() {
                    debug!(key = %k, "invalidated WorkerClient from pool");
                }
            }
            next
        });
        // Best-effort cleanup of the per-channel reconnect locks + the
        // round-robin counter to prevent unbounded growth of the maps over
        // the lifetime of a long-running process.
        //
        // H2: `DashMap::remove` is a per-shard striped-lock operation — no
        // async write lock round-trip, no blocking other addresses.
        for k in &keys {
            if self.reconnect_locks.remove(k).is_some() {
                debug!(key = %k, "removed reconnect lock for invalidated worker");
            }
        }
        if self.pool_size > 1 {
            self.addr_rr.remove(addr);
        }
    }

    /// Get (or lazily create) the per-address reconnect mutex.
    ///
    /// **H2**: `DashMap::entry` is atomic (shard-level striped lock), so the
    /// double-checked-locking pattern is no longer needed — a single
    /// `entry().or_insert_with()` call replaces the read-then-write pattern.
    async fn reconnect_lock_for(&self, addr: &str) -> Arc<AsyncMutex<()>> {
        self.reconnect_locks
            .entry(addr.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    /// **Single-flight reconnect**: invalidate + reconnect only if the
    /// currently cached client's generation still matches `stale_generation`.
    ///
    /// This is the preferred recovery path on authentication failure.  The
    /// caller passes the `generation()` of the client that just failed;
    /// because every `WorkerClient` carries a unique monotonic generation
    /// allocated by this pool:
    ///
    /// - If another concurrent task has **already** reconnected in response
    ///   to the same underlying SASL expiry, the cached generation will have
    ///   advanced past `stale_generation` and this call returns the
    ///   already-replaced client **without** performing another
    ///   TCP+SASL handshake.
    /// - Otherwise, this call performs exactly one reconnect under the
    ///   per-address mutex.
    ///
    /// Net effect: N concurrent `AuthenticationFailed` observers on the
    /// same channel trigger exactly **one** reconnect instead of N.
    pub async fn reconnect_if_stale(
        &self,
        addr: &str,
        stale_generation: u64,
    ) -> Result<WorkerClient> {
        // Single-channel pool: the channel key is just `addr` (legacy path).
        if self.pool_size <= 1 {
            return self.reconnect_by_key(addr, addr, stale_generation).await;
        }

        // Multi-channel pool: the caller knows only `(addr, generation)`, not
        // which slot failed. Generations are globally unique, so locate the
        // slot whose cached client still carries `stale_generation` and
        // reconnect exactly that channel. If no slot matches, the channel was
        // already replaced by a concurrent task — just hand back a fresh
        // round-robin client (the read will retry on it).
        let mut target_key: Option<String> = None;
        {
            let cache = self.clients.load();
            for s in 0..self.pool_size {
                let k = self.slot_key(addr, s);
                if let Some(c) = cache.get(&k) {
                    if c.generation == stale_generation {
                        target_key = Some(k);
                        break;
                    }
                }
            }
        }
        match target_key {
            Some(key) => self.reconnect_by_key(&key, addr, stale_generation).await,
            None => {
                debug!(
                    addr = %addr,
                    observed = stale_generation,
                    "reconnect coalesced — no slot matches stale generation (already refreshed)"
                );
                crate::metrics::counter(crate::metrics::name::CLIENT_WORKER_RECONNECTS_COALESCED)
                    .inc(1);
                self.acquire(addr).await
            }
        }
    }

    /// Single-flight reconnect for a specific channel `map_key`, connecting to
    /// `connect_addr` on the actual handshake. Coalesces concurrent callers on
    /// the same key via the per-key reconnect mutex + generation re-check.
    async fn reconnect_by_key(
        &self,
        map_key: &str,
        connect_addr: &str,
        stale_generation: u64,
    ) -> Result<WorkerClient> {
        // Take the per-channel reconnect mutex.  Concurrent callers for the
        // same channel serialise here; callers for *different* channels do
        // not block each other.
        let lock = self.reconnect_lock_for(map_key).await;
        let _guard = lock.lock().await;

        // Under the mutex, re-check the cache.  If another task already
        // replaced the stale client while we were queuing, skip the
        // reconnect entirely.
        {
            let cache = self.clients.load();
            if let Some(client) = cache.get(map_key) {
                if client.generation > stale_generation {
                    debug!(
                        key = %map_key,
                        observed = stale_generation,
                        current = client.generation,
                        "reconnect coalesced — another task already refreshed this channel"
                    );
                    crate::metrics::counter(
                        crate::metrics::name::CLIENT_WORKER_RECONNECTS_COALESCED,
                    )
                    .inc(1);
                    return Ok(client.clone());
                }
            }
        }

        // We are the designated reconnect-er: drop the stale entry, then
        // build and install a new one.
        debug!(
            key = %map_key,
            stale_generation = stale_generation,
            "performing single-flight reconnect"
        );
        crate::metrics::counter(crate::metrics::name::CLIENT_WORKER_RECONNECTS_TOTAL).inc(1);
        self.remove_client(map_key);
        let mut fresh = WorkerClient::connect(connect_addr, &self.config).await?;
        fresh.generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.insert_client(map_key, fresh.clone());
        debug!(
            key = %map_key,
            new_generation = fresh.generation,
            "single-flight reconnect installed fresh WorkerClient"
        );
        Ok(fresh)
    }

    /// Invalidate a cached worker connection and immediately reconnect.
    ///
    /// **Prefer [`reconnect_if_stale`](Self::reconnect_if_stale) whenever the
    /// caller holds a reference to the failing `WorkerClient`** — it
    /// deduplicates concurrent reconnects triggered by the same underlying
    /// SASL expiry.
    ///
    /// This unconditional variant is kept for paths where the caller does
    /// not know the generation of the failing client (e.g. a stand-alone
    /// `connect()` failure that never produced a `WorkerClient`).  It
    /// acquires the same per-address reconnect mutex so it still coalesces
    /// against any in-flight `reconnect_if_stale`.
    pub async fn reconnect(&self, addr: &str) -> Result<WorkerClient> {
        // Use `u64::MAX` as "stale" so the handshake always proceeds (current
        // generation can never exceed MAX). This still passes through the
        // per-channel mutex so concurrent callers on the same channel share a
        // single handshake. For a multi-channel pool, reconnect one
        // round-robin slot (the caller has no generation to target).
        let slot = self.next_slot(addr).await;
        let key = self.slot_key(addr, slot);
        self.reconnect_by_key(&key, addr, u64::MAX).await
    }

    /// Create a new pool wrapped in `Arc` for shared ownership.
    pub fn new_shared(config: GoosefsConfig) -> Arc<Self> {
        Arc::new(Self::new(config))
    }

    // ── Test-only helpers ────────────────────────────────────────────
    //
    // These helpers are gated on `cfg(test)` so downstream code cannot
    // accidentally inject bypass-auth clients into the pool.  They exist
    // purely to let the unit tests in this module drive the single-flight
    // reconnect logic without needing a live Worker process to handshake
    // against.

    /// Manually insert a client with a specific `generation` into the
    /// pool for testing.  Returns the previously-cached client, if any.
    #[cfg(test)]
    async fn test_install(&self, addr: &str, mut client: WorkerClient) -> Option<WorkerClient> {
        client.generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let prev = self.clients.load().get(addr).cloned();
        self.insert_client(addr, client);
        prev
    }

    /// Snapshot the current cached generation for `addr` (if any).
    #[cfg(test)]
    async fn test_current_generation(&self, addr: &str) -> Option<u64> {
        self.clients.load().get(addr).map(|c| c.generation)
    }

    /// Snapshot the number of entries in the `reconnect_locks` map.
    #[cfg(test)]
    async fn test_reconnect_locks_len(&self) -> usize {
        self.reconnect_locks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::transport::Channel;

    /// Fabricate a `WorkerClient` from a *never-connected* channel.  The
    /// client is fully usable for anything that only touches the in-memory
    /// struct (addr/generation lookups, clone, drop), which is all the
    /// coalesce tests need.
    fn fake_client(addr: &str) -> WorkerClient {
        // `Channel::from_static` is synchronous and does not open a TCP
        // connection; any actual RPC on this channel would fail but the
        // tests below never issue one.
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        WorkerClient::from_channel(channel, addr.to_string())
    }

    /// Worker-side multi-channel pool: `next_slot` round-robins per address
    /// and `slot_key` composes the channel key (Part V worker-side pool).
    #[tokio::test]
    async fn worker_pool_round_robins_slots() {
        let config = GoosefsConfig::new("127.0.0.1:9200").with_worker_connection_pool_size(4);
        let pool = WorkerClientPool::new(config);
        assert_eq!(pool.pool_size(), 4);
        assert_eq!(pool.slot_key("h:1", 0), "h:1#0");
        assert_eq!(pool.slot_key("h:1", 3), "h:1#3");

        let mut seen = Vec::new();
        for _ in 0..8 {
            seen.push(pool.next_slot("h:1").await);
        }
        assert_eq!(seen, vec![0, 1, 2, 3, 0, 1, 2, 3]);
        // A different address has its own independent counter.
        assert_eq!(pool.next_slot("h:2").await, 0);
    }

    /// Single-channel pool (default) keeps the legacy `addr`-keyed behaviour
    /// byte-for-byte, so existing single-flight tests are unaffected.
    ///
    /// Explicitly forces `worker_connection_pool_size = 1` so this test
    /// exercises the intended `slot_key(addr, 0) == addr` branch regardless
    /// of the SDK-level default (which is `min(cores, 4)` since B3).
    #[tokio::test]
    async fn worker_pool_single_channel_keys_by_addr() {
        let pool = WorkerClientPool::new(
            GoosefsConfig::new("127.0.0.1:9200").with_worker_connection_pool_size(1),
        );
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.slot_key("h:1", 0), "h:1");
        assert_eq!(pool.next_slot("h:1").await, 0);
        assert_eq!(pool.next_slot("h:1").await, 0);
    }

    #[tokio::test]
    async fn test_reconnect_if_stale_coalesces_when_generation_advanced() {
        // Scenario: generation 5 is cached.  Caller A "observes" a failure
        // on gen 5 and calls reconnect_if_stale(5).  Before it enters the
        // critical section, caller B has already replaced gen 5 with gen 6
        // (simulated by manually bumping via test_install).  Caller A must
        // NOT trigger a second reconnect — it should return gen 6 as-is.
        //
        // Pool is pinned to `pool_size = 1` so `test_install(addr, …)` and
        // the pool's own key derivation share the same key `addr`. This
        // isolates the test from the default (`min(cores, 4)`) which uses
        // `addr#slot` keys.
        let pool = WorkerClientPool::new(
            GoosefsConfig::new("127.0.0.1:9200").with_worker_connection_pool_size(1),
        );
        let addr = "test-worker:9203";

        // Install a gen-1 client, then another gen-2 client (simulating
        // "someone else already reconnected").
        pool.test_install(addr, fake_client(addr)).await;
        let gen_before = pool.test_current_generation(addr).await.unwrap();
        pool.test_install(addr, fake_client(addr)).await;
        let gen_after = pool.test_current_generation(addr).await.unwrap();
        assert!(gen_after > gen_before);

        // Caller passes the *old* generation — pool must short-circuit and
        // NOT call WorkerClient::connect (which would fail against a
        // non-existent host and fail the test).
        let result = pool.reconnect_if_stale(addr, gen_before).await;
        assert!(
            result.is_ok(),
            "coalesced reconnect must short-circuit without network I/O, got {:?}",
            result.err()
        );
        let returned = result.unwrap();
        assert_eq!(
            returned.generation(),
            gen_after,
            "caller must receive the already-replaced generation"
        );
        assert_eq!(
            pool.test_current_generation(addr).await,
            Some(gen_after),
            "cached generation must not advance for a coalesced caller"
        );
    }

    #[tokio::test]
    async fn test_reconnect_locks_are_per_address() {
        // Acquiring the reconnect lock for addr-A must not block acquiring
        // the lock for addr-B.  Without per-address locks, unrelated worker
        // reconnects would serialise through one global mutex.
        let pool = WorkerClientPool::new(GoosefsConfig::new("127.0.0.1:9200"));
        let lock_a = pool.reconnect_lock_for("worker-a:9203").await;
        let lock_b = pool.reconnect_lock_for("worker-b:9203").await;

        // Hold A, must still be able to grab B immediately.
        let guard_a = lock_a.lock().await;
        let guard_b = tokio::time::timeout(std::time::Duration::from_millis(50), lock_b.lock())
            .await
            .expect("lock for different address must not be blocked");
        drop(guard_b);
        drop(guard_a);
    }

    /// `invalidate()` must drop the per-address reconnect lock so the
    /// `reconnect_locks` map does not grow unbounded for long-running
    /// processes that connect to many distinct worker addresses (worker
    /// scale-up / scale-down).
    #[tokio::test]
    async fn test_invalidate_clears_reconnect_lock_to_prevent_leak() {
        // Pinned to single-channel so `test_install(addr, …)` uses the same
        // key as `reconnect_lock_for(addr)` (which the multi-channel default
        // would key by `addr#slot`).
        let pool = WorkerClientPool::new(
            GoosefsConfig::new("127.0.0.1:9200").with_worker_connection_pool_size(1),
        );

        // Touch the reconnect-lock map for several addresses (simulates
        // reconnect activity over time).
        for i in 0..10 {
            let addr = format!("ephemeral-worker-{}:9203", i);
            pool.test_install(&addr, fake_client(&addr)).await;
            let _lock = pool.reconnect_lock_for(&addr).await;
        }
        assert_eq!(
            pool.test_reconnect_locks_len().await,
            10,
            "reconnect_locks must be populated by reconnect_lock_for()"
        );

        // Now invalidate them (workers scaled down).
        for i in 0..10 {
            pool.invalidate(&format!("ephemeral-worker-{}:9203", i))
                .await;
        }

        assert_eq!(
            pool.test_reconnect_locks_len().await,
            0,
            "invalidate() must remove the per-address reconnect lock so the \
             map does not leak across worker churn"
        );
    }

    #[tokio::test]
    async fn test_generation_is_monotonic_across_installs() {
        let pool = WorkerClientPool::new(GoosefsConfig::new("127.0.0.1:9200"));
        let addr = "w:9203";

        pool.test_install(addr, fake_client(addr)).await;
        let g1 = pool.test_current_generation(addr).await.unwrap();

        pool.test_install(addr, fake_client(addr)).await;
        let g2 = pool.test_current_generation(addr).await.unwrap();

        pool.test_install(addr, fake_client(addr)).await;
        let g3 = pool.test_current_generation(addr).await.unwrap();

        assert!(g1 < g2, "gen {} not less than {}", g1, g2);
        assert!(g2 < g3, "gen {} not less than {}", g2, g3);
    }

    // ── Auth-retry regression tests ──────────────────────────────────────
    //
    // These tests verify the core sequence of the auth-retry path:
    //   1. acquire() returns a cached (stale) WorkerClient
    //   2. RPC on that client fails with AuthenticationFailed
    //   3. Caller invokes reconnect_if_stale() or reconnect()
    //   4. Pool returns a fresh client (either already installed by another
    //      task, or via a new TCP+SASL handshake)
    //   5. Caller retries the RPC on the fresh client
    //
    // Steps 1–4 are testable at the pool level without a real server (using
    // test_install to simulate reconnect outcomes).  Step 5 requires a real
    // Goosefs cluster and is covered by `tests/auth_retry.rs` integration
    // tests.

    /// **Auth-retry recovery point 1** (RPC failure → single-flight reconnect):
    ///
    /// Simulate the full auth-retry sequence at the pool level:
    /// 1. `acquire()` returns a cached client with generation N
    /// 2. An RPC on that client fails with `AuthenticationFailed`
    /// 3. Another concurrent reader has already reconnected (installed gen N+1)
    /// 4. `reconnect_if_stale(addr, N)` returns the already-installed fresh
    ///    client without a redundant TCP+SASL handshake
    ///
    /// This mirrors the code path in `GoosefsFileReader::read_next_block()`
    /// and `GoosefsFileInStream::read()` / `read_at()`:
    /// ```ignore
    /// Err(e) if e.is_authentication_failed() => {
    ///     let fresh = self.reconnect_worker(&addr, Some(worker_generation)).await?;
    ///     // retry RPC with fresh client
    /// }
    /// ```
    #[tokio::test]
    async fn test_auth_retry_reconnect_if_stale_returns_fresh_after_rpc_failure() {
        // Pinned to single-channel so `test_install`/`acquire` share keys.
        let pool = WorkerClientPool::new(
            GoosefsConfig::new("127.0.0.1:9200").with_worker_connection_pool_size(1),
        );
        let addr = "test-worker:9203";

        // Step 1: Install and acquire a cached client (SASL-stale, but pool
        // doesn't know that yet — the client is valid from the pool's POV).
        pool.test_install(addr, fake_client(addr)).await;
        let stale_client = pool.acquire(addr).await.unwrap();
        let stale_gen = stale_client.generation();

        // Step 2: Simulate that another concurrent reader already detected the
        // auth failure and triggered a reconnect — a fresh client with a higher
        // generation is now cached.
        pool.test_install(addr, fake_client(addr)).await;
        let expected_fresh_gen = pool.test_current_generation(addr).await.unwrap();
        assert!(
            expected_fresh_gen > stale_gen,
            "fresh gen must exceed stale gen"
        );

        // Step 3: This caller's RPC failed with AuthenticationFailed; it calls
        // reconnect_if_stale(addr, stale_gen) for single-flight reconnect.
        // The pool sees generation has already advanced and returns the
        // existing client — no redundant TCP+SASL handshake.
        let fresh_client = pool
            .reconnect_if_stale(addr, stale_gen)
            .await
            .expect("reconnect_if_stale must return Ok when generation advanced");

        assert_eq!(
            fresh_client.generation(),
            expected_fresh_gen,
            "must return the already-installed fresh client (coalesced reconnect)"
        );
        assert!(
            fresh_client.generation() > stale_gen,
            "fresh client generation ({}) must be > stale generation ({})",
            fresh_client.generation(),
            stale_gen
        );

        // Verify pool generation didn't advance further (no duplicate reconnect)
        assert_eq!(
            pool.test_current_generation(addr).await,
            Some(expected_fresh_gen),
            "pool generation must not advance for a coalesced reconnect"
        );
    }

    /// **Auth-retry recovery point 2** (acquire failure → unconditional reconnect):
    ///
    /// When `acquire()` itself fails with `AuthenticationFailed` (e.g. the
    /// connect+auth step returned UNAUTHENTICATED), no `WorkerClient` was
    /// produced and there is no generation to coalesce against.  The caller
    /// falls back to the unconditional `reconnect()` path.
    ///
    /// `reconnect()` is implemented as `reconnect_if_stale(addr, u64::MAX)`.
    /// Since no generation can ever exceed `u64::MAX`, this ALWAYS falls
    /// through to a real `WorkerClient::connect()` — it cannot coalesce.
    /// This is by design: the caller has no WorkerClient to compare
    /// generations against, so a fresh connection is always required.
    ///
    /// This test verifies the `u64::MAX` semantics — that `reconnect_if_stale`
    /// with `u64::MAX` does NOT short-circuit even when a client with a
    /// valid generation exists in the pool.
    ///
    /// Note: Testing the actual reconnect requires a real Goosefs server.
    /// See `tests/auth_retry.rs` for integration test stubs.
    #[tokio::test]
    async fn test_auth_retry_unconditional_reconnect_never_short_circuits() {
        let pool = WorkerClientPool::new(GoosefsConfig::new("127.0.0.1:9200"));
        let addr = "test-worker:9203";

        // Install a client with some generation
        pool.test_install(addr, fake_client(addr)).await;
        let current_gen = pool.test_current_generation(addr).await.unwrap();

        // reconnect_if_stale(addr, u64::MAX) must NOT short-circuit,
        // because current_gen can never exceed u64::MAX.
        // It will try to connect to the real server (which doesn't exist),
        // so we expect a transport error, NOT a successful return of the
        // existing client.
        let result = pool.reconnect_if_stale(addr, u64::MAX).await;
        assert!(
            result.is_err(),
            "reconnect_if_stale(addr, u64::MAX) must NOT short-circuit \
             when generation ({}) < u64::MAX — expected real connect attempt",
            current_gen
        );
    }

    /// **Auth-retry thundering-herd collapse**:
    ///
    /// When a SASL stream expires server-side, N concurrent readers on the
    /// same cached channel will all observe `AuthenticationFailed`
    /// simultaneously.  Without single-flight reconnect, each would
    /// independently invoke `reconnect`, producing N TCP+SASL handshakes.
    ///
    /// With single-flight (`reconnect_if_stale`), only the first observer
    /// of generation N triggers a real reconnect; all other observers with
    /// the same (or older) generation receive the already-replaced client.
    ///
    /// This test simulates: stale gen N → first observer reconnects
    /// (installs N+1) → second observer with stale gen N gets N+1.
    #[tokio::test]
    async fn test_auth_retry_multiple_observers_collapse_to_one_reconnect() {
        // Pinned to single-channel so `test_install`/`acquire` share keys.
        let pool = WorkerClientPool::new(
            GoosefsConfig::new("127.0.0.1:9200").with_worker_connection_pool_size(1),
        );
        let addr = "test-worker:9203";

        // Install initial client (SASL-stale)
        pool.test_install(addr, fake_client(addr)).await;
        let stale_gen = pool.test_current_generation(addr).await.unwrap();

        // First observer detects auth failure, triggers reconnect.
        // (In reality this would call reconnect_if_stale which does
        // WorkerClient::connect; we simulate the outcome with test_install.)
        pool.test_install(addr, fake_client(addr)).await;
        let fresh_gen = pool.test_current_generation(addr).await.unwrap();
        assert!(fresh_gen > stale_gen);

        // Second observer with the same stale generation must get the
        // already-installed fresh client — NO duplicate reconnect.
        let client = pool
            .reconnect_if_stale(addr, stale_gen)
            .await
            .expect("coalesced reconnect must succeed");
        assert_eq!(
            client.generation(),
            fresh_gen,
            "second observer must get the already-installed fresh client"
        );

        // Pool generation must not advance further (no duplicate reconnect)
        assert_eq!(
            pool.test_current_generation(addr).await,
            Some(fresh_gen),
            "generation must not advance for a coalesced observer"
        );

        // Third observer with an even older generation (0 = never-pooled)
        // must also get the fresh client
        let client_old = pool
            .reconnect_if_stale(addr, 0)
            .await
            .expect("observer with gen=0 must also get coalesced client");
        assert_eq!(
            client_old.generation(),
            fresh_gen,
            "observer with stale gen=0 must get the same fresh client"
        );
    }

    /// **Regression for C3**: dropping a `WriteBlockHandle` without going
    /// through `close()` / `cancel()` MUST abort the background gRPC task.
    ///
    /// Pre-fix behaviour: the comment claimed there was a `Drop` safety net
    /// but the impl was missing. An early `?` on the error path therefore
    /// left a detached task forever stuck on `stream.message().await`,
    /// pinning the channel and leaking resources.
    #[tokio::test]
    async fn write_block_handle_drop_aborts_background_task() {
        // Channels must look real but never carry traffic.
        let (tx, _rx) = mpsc::channel::<WriteRequest>(8);
        let (_resp_tx, resp_rx) = mpsc::channel(8);

        // A background task that would otherwise hang forever — Drop must
        // abort it via task_handle.abort().
        let task = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let abort_handle = task.abort_handle();

        let handle = WriteBlockHandle {
            block_id: 42,
            request_tx: Some(tx),
            response_rx: resp_rx,
            task_handle: Some(task),
        };

        assert!(
            !abort_handle.is_finished(),
            "task should still be running before Drop"
        );

        // Drop the handle on the error path (simulating an early `?` return).
        drop(handle);

        // Wait briefly for tokio to process the abort.
        for _ in 0..50 {
            if abort_handle.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            abort_handle.is_finished(),
            "Drop did not abort background task — pre-fix regression"
        );
    }

    /// `close()` already takes the task_handle and awaits it; the subsequent
    /// `Drop` must therefore see `task_handle = None` and be a complete
    /// no-op (no double-abort).
    #[tokio::test]
    async fn write_block_handle_drop_after_close_is_noop() {
        let (tx, _rx) = mpsc::channel::<WriteRequest>(8);
        // Drop the response sender immediately: the channel is closed, so
        // close()'s `response_rx.recv()` loop terminates straight away
        // (matches the real-world "server done" signal).
        let (_, resp_rx) = mpsc::channel(8);

        // Spawn a task that finishes immediately so close()'s join completes.
        let task = tokio::spawn(async {});

        let handle = WriteBlockHandle {
            block_id: 7,
            request_tx: Some(tx),
            response_rx: resp_rx,
            task_handle: Some(task),
        };

        // close() takes ownership of the handle, drains the (already-closed)
        // response stream, joins the task, and returns. After it returns,
        // `self` is dropped — the Drop impl must be a no-op because
        // `task_handle.take()` already happened inside close().
        let close_fut = handle.close();
        let res = tokio::time::timeout(Duration::from_millis(500), close_fut).await;
        assert!(
            res.is_ok(),
            "close() must complete promptly when response stream is closed"
        );
        assert!(
            res.unwrap().is_ok(),
            "close() should succeed on a graceful task"
        );
    }
}
