//! GooseFS Master gRPC client for file system metadata operations.
//!
//! Wraps `FileSystemMasterClientService` (Master:9200) providing:
//! - `get_status` — stat / head
//! - `list_status` — list directory (server-side streaming)
//! - `create_file` — create a new file
//! - `complete_file` — mark file write complete
//! - `delete` — delete file or directory
//! - `rename` — rename / move
//! - `create_directory` — mkdir -p
//!
//! ## HA / Multi-Master Support
//!
//! When multiple Master addresses are configured, [`MasterClient::connect`]
//! uses [`MasterInquireClient`] to discover the Primary Master before
//! establishing the gRPC channel. If an RPC fails with a retriable error
//! (`Unavailable`, `DeadlineExceeded`), the client will re-discover the
//! Primary and rebuild the channel automatically.

use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{debug, instrument, warn};

use crate::auth::{ChannelAuthenticator, ChannelIdInterceptor};
use crate::client::master_inquire::{create_master_inquire_client, MasterInquireClient};
use crate::config::GooseFsConfig;
use crate::error::{Error, Result};
use crate::proto::grpc::file::{
    file_system_master_client_service_client::FileSystemMasterClientServiceClient,
    CompleteFilePOptions, CompleteFilePRequest, CreateDirectoryPOptions, CreateDirectoryPRequest,
    CreateFilePOptions, CreateFilePRequest, DeletePOptions, DeletePRequest, FileInfo,
    GetStatusPOptions, GetStatusPRequest, ListStatusPOptions, ListStatusPRequest, RenamePOptions,
    RenamePRequest, ScheduleAsyncPersistencePOptions, ScheduleAsyncPersistencePRequest,
};
use crate::proto::grpc::{Bits, PMode};

/// Maximum number of RPC-level retries on retriable errors before giving up.
const MAX_RPC_RETRIES: u32 = 2;

/// Type alias for the authenticated gRPC client.
///
/// Both NOSASL and SIMPLE modes use `InterceptedService` wrapping;
/// the difference is that NOSASL skips the SASL handshake but still injects a channel-id.
type AuthenticatedFsClient =
    FileSystemMasterClientServiceClient<InterceptedService<Channel, ChannelIdInterceptor>>;

/// Default mode for directories: 0755 (rwxr-xr-x)
pub fn default_dir_mode() -> PMode {
    PMode {
        owner_bits: Bits::All as i32,         // rwx
        group_bits: Bits::ReadExecute as i32, // r-x
        other_bits: Bits::ReadExecute as i32, // r-x
    }
}

/// Default mode for files: 0644 (rw-r--r--)
pub fn default_file_mode() -> PMode {
    PMode {
        owner_bits: Bits::ReadWrite as i32, // rw-
        group_bits: Bits::Read as i32,      // r--
        other_bits: Bits::Read as i32,      // r--
    }
}

/// Client for GooseFS `FileSystemMasterClientService` (Master:9200).
///
/// In HA mode, the client holds a reference to the [`MasterInquireClient`]
/// and can automatically re-discover the Primary Master when RPCs fail.
///
/// ## Authentication
///
/// The client supports NOSASL and SIMPLE authentication modes.
/// When `config.auth_type` is `Simple`, the client performs a SASL PLAIN
/// handshake after establishing the gRPC channel, then injects a `channel-id`
/// metadata header into all subsequent RPCs.
#[derive(Clone)]
pub struct MasterClient {
    inner: Arc<RwLock<AuthenticatedFsClient>>,
    config: GooseFsConfig,
    inquire_client: Arc<dyn MasterInquireClient>,
}

impl MasterClient {
    /// Connect to the GooseFS Master.
    ///
    /// In single-master mode, connects directly to `config.master_addr`.
    /// In HA mode (multiple addresses in `config.master_addrs`), uses
    /// [`PollingMasterInquireClient`] to discover the Primary first.
    ///
    /// Authentication is performed according to `config.auth_type`.
    pub async fn connect(config: &GooseFsConfig) -> Result<Self> {
        let inquire_client = create_master_inquire_client(config);
        Self::connect_with_inquire(config, inquire_client).await
    }

    /// Connect using an externally-provided [`MasterInquireClient`].
    ///
    /// This is useful when sharing a single inquire client across multiple
    /// client types (e.g. `MasterClient` + `WorkerManagerClient`).
    pub async fn connect_with_inquire(
        config: &GooseFsConfig,
        inquire_client: Arc<dyn MasterInquireClient>,
    ) -> Result<Self> {
        let primary_addr = inquire_client.get_primary_rpc_address().await?;
        let client = Self::build_authenticated_client(config, &primary_addr).await?;
        debug!(addr = %primary_addr, auth_type = %config.auth_type, "connected to GooseFS Master");

        Ok(Self {
            inner: Arc::new(RwLock::new(client)),
            config: config.clone(),
            inquire_client,
        })
    }

    /// Create from an existing tonic channel (useful for testing / channel sharing).
    ///
    /// **Note**: This bypasses authentication. The channel is wrapped with a
    /// no-op channel-id interceptor for API compatibility.
    pub fn from_channel(channel: Channel, config: GooseFsConfig) -> Self {
        let inquire_client = create_master_inquire_client(&config);
        let interceptor = ChannelIdInterceptor::new("test-no-auth".to_string());
        let intercepted = InterceptedService::new(channel, interceptor);
        Self {
            inner: Arc::new(RwLock::new(FileSystemMasterClientServiceClient::new(
                intercepted,
            ))),
            config,
            inquire_client,
        }
    }

    /// Build a gRPC channel and perform authentication, returning an authenticated client.
    async fn build_authenticated_client(
        config: &GooseFsConfig,
        addr: &str,
    ) -> Result<AuthenticatedFsClient> {
        let channel = Self::build_raw_channel(config, addr).await?;

        // Perform SASL authentication based on the configured auth type
        let authenticator = ChannelAuthenticator::new(
            config.auth_type,
            config.auth_username.clone(),
            None, // impersonation_user: not yet supported
        )
        .with_auth_timeout(config.auth_timeout);

        let auth_channel = authenticator.authenticate(channel).await?;

        Ok(FileSystemMasterClientServiceClient::new(
            auth_channel.channel,
        ))
    }

    /// Build a raw gRPC channel to a specific master address (without authentication).
    async fn build_raw_channel(config: &GooseFsConfig, addr: &str) -> Result<Channel> {
        let endpoint_uri = format!("http://{}", addr);
        let endpoint = Channel::from_shared(endpoint_uri)
            .map_err(|e| Error::ConfigError {
                message: format!("invalid master endpoint: {}", e),
            })?
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout);

        let channel = endpoint.connect().await?;
        Ok(channel)
    }

    /// Reconnect to the Primary Master after a failover.
    ///
    /// Resets the cached Primary in the inquire client, re-discovers the
    /// new Primary, rebuilds the gRPC channel, and re-authenticates.
    async fn reconnect(&self) -> Result<()> {
        // Reset cached primary so the inquire client re-polls all addresses.
        self.inquire_client.reset_cached_primary().await;

        let primary_addr = self.inquire_client.get_primary_rpc_address().await?;
        let client = Self::build_authenticated_client(&self.config, &primary_addr).await?;
        let mut inner = self.inner.write().await;
        *inner = client;
        debug!(addr = %primary_addr, "reconnected to GooseFS Master after failover");
        Ok(())
    }

    /// Execute an RPC with automatic retry on retriable errors.
    ///
    /// On retriable failure, the client reconnects to a (potentially new)
    /// Primary Master and retries up to [`MAX_RPC_RETRIES`] times.
    async fn with_retry<F, Fut, T>(&self, op_name: &str, f: F) -> Result<T>
    where
        F: Fn(AuthenticatedFsClient) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut last_err: Option<Error> = None;

        for attempt in 0..=MAX_RPC_RETRIES {
            let client: AuthenticatedFsClient = {
                let inner = self.inner.read().await;
                inner.clone()
            };

            match f(client).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if err.is_retriable() && attempt < MAX_RPC_RETRIES {
                        warn!(
                            op = op_name,
                            attempt = attempt + 1,
                            max = MAX_RPC_RETRIES,
                            error = %err,
                            "retriable error, reconnecting and retrying"
                        );
                        if let Err(reconnect_err) = self.reconnect().await {
                            warn!(error = %reconnect_err, "reconnect failed");
                            last_err = Some(err);
                            continue;
                        }
                    } else {
                        return Err(err);
                    }
                    last_err = Some(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| Error::Internal {
            message: format!("{}: exhausted all retries", op_name),
            source: None,
        }))
    }

    /// Get the file/directory status (equivalent to `stat` / `head`).
    #[instrument(skip(self), fields(path = %path))]
    pub async fn get_status(&self, path: &str) -> Result<FileInfo> {
        let path = path.to_string();
        self.with_retry("get_status", |mut client| {
            let path = path.clone();
            async move {
                let req = GetStatusPRequest {
                    path: Some(path),
                    options: Some(GetStatusPOptions::default()),
                    request_id: None,
                };
                let resp = client.get_status(req).await?;
                resp.into_inner()
                    .file_info
                    .ok_or_else(|| Error::missing_field("file_info"))
            }
        })
        .await
    }

    /// List the contents of a directory. Returns all FileInfo entries.
    ///
    /// This wraps a **server-side streaming** RPC — the server sends
    /// multiple `ListStatusPResponse` messages, each containing a batch
    /// of `FileInfo`.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn list_status(&self, path: &str, recursive: bool) -> Result<Vec<FileInfo>> {
        let path = path.to_string();
        self.with_retry("list_status", |mut client| {
            let path = path.clone();
            async move {
                let req = ListStatusPRequest {
                    path: Some(path),
                    options: Some(ListStatusPOptions {
                        recursive: Some(recursive),
                        ..Default::default()
                    }),
                    request_id: None,
                };
                let mut stream = client.list_status(req).await?.into_inner();
                let mut result = Vec::new();
                while let Some(resp) = stream.message().await? {
                    result.extend(resp.file_infos);
                }
                Ok(result)
            }
        })
        .await
    }

    /// Create a new file. Returns the `FileInfo` of the created file.
    #[instrument(skip(self, options), fields(path = %path))]
    pub async fn create_file(&self, path: &str, options: CreateFilePOptions) -> Result<FileInfo> {
        let path = path.to_string();
        self.with_retry("create_file", |mut client| {
            let path = path.clone();
            let options = options.clone();
            async move {
                let req = CreateFilePRequest {
                    path: Some(path),
                    options: Some(options),
                };
                let resp = client.create_file(req).await?;
                resp.into_inner()
                    .file_info
                    .ok_or_else(|| Error::missing_field("file_info"))
            }
        })
        .await
    }

    /// Mark a file as completed (called after all blocks are written).
    #[instrument(skip(self), fields(path = %path))]
    pub async fn complete_file(&self, path: &str, ufs_length: Option<i64>) -> Result<()> {
        let path = path.to_string();
        self.with_retry("complete_file", |mut client| {
            let path = path.clone();
            async move {
                let req = CompleteFilePRequest {
                    path: Some(path),
                    options: Some(CompleteFilePOptions {
                        ufs_length,
                        ..Default::default()
                    }),
                    inode_id: None,
                };
                client.complete_file(req).await?;
                Ok(())
            }
        })
        .await
    }

    /// Delete a file or directory.
    #[instrument(skip(self), fields(path = %path, recursive = %recursive))]
    pub async fn delete(&self, path: &str, recursive: bool) -> Result<()> {
        let path = path.to_string();
        self.with_retry("delete", |mut client| {
            let path = path.clone();
            async move {
                let req = DeletePRequest {
                    path: Some(path),
                    options: Some(DeletePOptions {
                        recursive: Some(recursive),
                        ..Default::default()
                    }),
                };
                client.remove(req).await?;
                Ok(())
            }
        })
        .await
    }

    /// Rename (move) a file or directory.
    #[instrument(skip(self), fields(src = %src, dst = %dst))]
    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let src = src.to_string();
        let dst = dst.to_string();
        self.with_retry("rename", |mut client| {
            let src = src.clone();
            let dst = dst.clone();
            async move {
                let req = RenamePRequest {
                    path: Some(src),
                    dst_path: Some(dst),
                    options: Some(RenamePOptions::default()),
                };
                client.rename(req).await?;
                Ok(())
            }
        })
        .await
    }

    /// Create a directory (recursive by default).
    ///
    /// Sets a default mode of `0755` (rwxr-xr-x) so that the corresponding
    /// UFS directory created by GooseFS has usable permissions.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn create_directory(&self, path: &str, recursive: bool) -> Result<()> {
        let path = path.to_string();
        self.with_retry("create_directory", |mut client| {
            let path = path.clone();
            async move {
                let req = CreateDirectoryPRequest {
                    path: Some(path),
                    options: Some(CreateDirectoryPOptions {
                        recursive: Some(recursive),
                        allow_exists: Some(true),
                        mode: Some(default_dir_mode()),
                        ..Default::default()
                    }),
                };
                client.create_directory(req).await?;
                Ok(())
            }
        })
        .await
    }

    /// Schedule asynchronous persistence for a file.
    /// This will persist the file to the underlying storage system.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn schedule_async_persistence(
        &self,
        path: &str,
        persistence_wait_time: Option<i64>,
    ) -> Result<()> {
        let path = path.to_string();
        self.with_retry("schedule_async_persistence", |mut client| {
            let path = path.clone();
            async move {
                let req = ScheduleAsyncPersistencePRequest {
                    path: Some(path),
                    options: Some(ScheduleAsyncPersistencePOptions {
                        common_options: None,
                        persistence_wait_time,
                    }),
                };
                client.schedule_async_persistence(req).await?;
                Ok(())
            }
        })
        .await
    }

    /// Get a reference to the underlying config.
    pub fn config(&self) -> &GooseFsConfig {
        &self.config
    }

    /// Get a reference to the underlying inquire client.
    ///
    /// Useful for sharing the same inquire client with `WorkerManagerClient`.
    pub fn inquire_client(&self) -> &Arc<dyn MasterInquireClient> {
        &self.inquire_client
    }
}
