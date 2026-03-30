//! GooseFS Worker gRPC client for block data read/write.
//!
//! Wraps `BlockWorker` service (Worker:9203) providing:
//! - `read_block` — bidirectional streaming block read
//! - `write_block` — bidirectional streaming block write
//!
//! ## Write Protocol
//!
//! GooseFS Worker's `WriteBlock` is a bidirectional streaming RPC but the server
//! does **not** send HTTP/2 response headers until the client sends a `flush`
//! command or closes the stream. This means tonic's
//! `client.write_block(stream).await` will block until the first server response.
//!
//! To work around this, `write_block()` returns a [`WriteBlockHandle`] that
//! runs the gRPC call in a background tokio task. The caller sends data chunks
//! through the request sender, then calls `flush()` or `close()` on the handle
//! to receive server responses.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, instrument, warn};

use crate::auth::{ChannelAuthenticator, ChannelIdInterceptor, SaslStreamGuard};
use crate::config::GooseFsConfig;
use crate::error::{Error, Result};
use crate::proto::grpc::block::{
    block_worker_client::BlockWorkerClient, write_request, ReadRequest, ReadResponse, RequestType,
    WriteRequest, WriteRequestCommand, WriteResponse,
};
use crate::proto::proto::dataserver::{CreateUfsFileOptions, OpenUfsBlockOptions};

/// Options for a `write_block` RPC that control *where* the Worker writes data.
///
/// - `GoosefsBlock` (default): write to GooseFS cache (MUST_CACHE / CACHE_THROUGH / ASYNC_THROUGH)
/// - `UfsFile`: write directly to UFS (THROUGH mode), requires `create_ufs_file_options`
/// - `UfsFallbackBlock`: cache-full fallback to UFS (TRY_CACHE)
#[derive(Clone, Debug)]
pub struct WriteBlockOptions {
    /// The request type sent in the initial `WriteRequestCommand`.
    pub request_type: RequestType,
    /// UFS file creation options (required when `request_type == UfsFile`).
    pub create_ufs_file_options: Option<CreateUfsFileOptions>,
}

impl Default for WriteBlockOptions {
    fn default() -> Self {
        Self {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
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
    pub request_tx: mpsc::Sender<WriteRequest>,
    /// Receiver for server → client WriteResponse messages, forwarded from the background task.
    response_rx: mpsc::Receiver<std::result::Result<WriteResponse, tonic::Status>>,
    /// Handle to the background gRPC task.
    _task_handle: tokio::task::JoinHandle<()>,
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
                source: status,
            }),
            None => Ok(None),
        }
    }

    /// Close the write stream by dropping the request sender and wait for
    /// any final response from the server.
    pub async fn close(mut self) -> Result<()> {
        // Drop the request sender to close the client→server half of the stream.
        // The server will then call onCompleted → commitBlock → replySuccess.
        drop(self.request_tx);
        debug!(
            block_id = self.block_id,
            "closed write stream, waiting for server finalize"
        );
        // Wait for the server's final response (or stream close).
        // This ensures the background task finishes before we return,
        // preventing the Channel from being dropped while the task is still running.
        while let Some(result) = self.response_rx.recv().await {
            match result {
                Ok(_resp) => {
                    debug!(
                        block_id = self.block_id,
                        "received final response from server"
                    );
                }
                Err(status) => {
                    return Err(Error::GrpcError {
                        message: format!(
                            "WriteBlock server error for block_id={}: {}",
                            self.block_id, status
                        ),
                        source: status,
                    });
                }
            }
        }
        Ok(())
    }

    /// Cancel the write stream without waiting for server finalization.
    ///
    /// Drops the request sender and response receiver immediately.
    /// The server will detect the stream cancellation and clean up.
    /// Matches Java's `GrpcBlockingStream.cancel()`.
    pub async fn cancel(self) {
        drop(self.request_tx);
        drop(self.response_rx);
        debug!(
            block_id = self.block_id,
            "cancelled write stream"
        );
    }
}

/// Type alias for the authenticated Worker gRPC client.
type AuthenticatedBlockWorkerClient =
    BlockWorkerClient<InterceptedService<Channel, ChannelIdInterceptor>>;

/// Client for `BlockWorker` service on a single worker node.
#[derive(Clone)]
pub struct WorkerClient {
    inner: AuthenticatedBlockWorkerClient,
    addr: String,
    /// Keeps the SASL authentication stream alive for the channel's lifetime.
    _sasl_guard: std::sync::Arc<Option<SaslStreamGuard>>,
}

impl WorkerClient {
    /// Connect to a GooseFS Worker at the given address with authentication.
    ///
    /// Authentication is performed according to `config.auth_type`.
    pub async fn connect(addr: &str, config: &GooseFsConfig) -> Result<Self> {
        let endpoint = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| Error::ConfigError {
                message: format!("invalid worker endpoint: {}", e),
            })?
            .connect_timeout(config.connect_timeout);

        let channel = endpoint.connect().await?;

        // Perform SASL authentication based on the configured auth type
        let authenticator =
            ChannelAuthenticator::new(config.auth_type, config.auth_username.clone(), None)
                .with_auth_timeout(config.auth_timeout);

        let mut auth_channel = authenticator.authenticate(channel).await?;
        let sasl_guard = auth_channel.take_sasl_guard();
        debug!(addr = %addr, auth_type = %config.auth_type, "connected to GooseFS Worker");

        Ok(Self {
            inner: BlockWorkerClient::new(auth_channel.channel),
            addr: addr.to_string(),
            _sasl_guard: std::sync::Arc::new(sasl_guard),
        })
    }

    /// Connect to a GooseFS Worker with only connect_timeout (backward compatible, NOSASL).
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
        debug!(addr = %addr, "connected to GooseFS Worker (no auth)");

        Ok(Self {
            inner: BlockWorkerClient::new(intercepted),
            addr: addr.to_string(),
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
        open_ufs_block_options: Option<OpenUfsBlockOptions>,
    ) -> Result<(mpsc::Sender<ReadRequest>, Streaming<ReadResponse>)> {
        let (tx, rx) = mpsc::channel::<ReadRequest>(32);

        // Send the initial read request
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
            prefetch_window: None,
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

    /// Start a bidirectional streaming WriteBlock RPC.
    ///
    /// Returns a [`WriteBlockHandle`] that manages the background gRPC task.
    /// The caller sends data chunks through `handle.request_tx`, then calls
    /// `handle.recv_response()` to get flush acknowledgements.
    ///
    /// ## Why a background task?
    ///
    /// GooseFS Worker's `WriteBlock` RPC does **not** send HTTP/2 response
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
            request_tx: tx,
            response_rx: resp_rx,
            _task_handle: task_handle,
        })
    }

    /// The worker address this client is connected to.
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

/// Connection pool for `WorkerClient` instances.
///
/// Caches authenticated gRPC channels by worker address, avoiding the overhead
/// of re-establishing connections and re-authenticating for every block write.
/// Matches Java's `FileSystemContext.acquireBlockWorkerClient()` pattern.
///
/// The pool is thread-safe and can be shared across concurrent writers.
pub struct WorkerClientPool {
    /// Cached worker clients keyed by `"host:port"` address.
    clients: RwLock<HashMap<String, WorkerClient>>,
    /// Config used to create new connections.
    config: GooseFsConfig,
}

impl WorkerClientPool {
    /// Create a new empty connection pool.
    pub fn new(config: GooseFsConfig) -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Acquire a `WorkerClient` for the given address.
    ///
    /// Returns a cached client if one exists, otherwise creates a new connection.
    /// The tonic `Channel` supports multiplexing, so a single cached client can
    /// handle multiple concurrent RPCs.
    pub async fn acquire(&self, addr: &str) -> Result<WorkerClient> {
        // Fast path: check read lock first
        {
            let cache = self.clients.read().await;
            if let Some(client) = cache.get(addr) {
                debug!(addr = %addr, "reusing cached WorkerClient");
                return Ok(client.clone());
            }
        }

        // Slow path: create new connection under write lock
        let mut cache = self.clients.write().await;
        // Double-check after acquiring write lock (another task may have inserted)
        if let Some(client) = cache.get(addr) {
            return Ok(client.clone());
        }

        debug!(addr = %addr, "creating new WorkerClient for pool");
        let client = WorkerClient::connect(addr, &self.config).await?;
        cache.insert(addr.to_string(), client.clone());
        Ok(client)
    }

    /// Remove a worker from the pool (e.g., after a connection failure).
    ///
    /// The next `acquire()` call for this address will create a fresh connection.
    pub async fn invalidate(&self, addr: &str) {
        let mut cache = self.clients.write().await;
        if cache.remove(addr).is_some() {
            debug!(addr = %addr, "invalidated WorkerClient from pool");
        }
    }

    /// Create a new pool wrapped in `Arc` for shared ownership.
    pub fn new_shared(config: GooseFsConfig) -> Arc<Self> {
        Arc::new(Self::new(config))
    }
}
