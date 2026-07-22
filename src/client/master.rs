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

//! Goosefs Master gRPC client for file system metadata operations.
//!
//! Wraps `FileSystemMasterClientService` (Master:9200) providing:
//! - `get_status` — stat / head
//! - `list_status` — list directory (server-side streaming)
//! - `create_file` — create a new file
//! - `complete_file` — mark file write complete (with idempotency operation-ID)
//! - `remove_blocks` — clean up block metadata for in-flight or failed writes
//! - `delete` / `delete_with_options` — delete file or directory
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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{debug, instrument, warn};

use crate::auth::{ChannelAuthenticator, ChannelIdInterceptor, SaslStreamGuard};
use crate::client::master_inquire::{create_master_inquire_client, MasterInquireClient};
use crate::config::GoosefsConfig;
use crate::error::{Error, Result};
use crate::fs::options::DeleteOptions;
use crate::metrics::registry::Counter;
use crate::proto::grpc::file::{
    file_system_master_client_service_client::FileSystemMasterClientServiceClient,
    CompleteFilePOptions, CompleteFilePRequest, CreateDirectoryPOptions, CreateDirectoryPRequest,
    CreateFilePOptions, CreateFilePRequest, DeletePOptions, DeletePRequest, FileInfo,
    FileSystemMasterCommonPOptions, FsOpPId, GetStatusPOptions, GetStatusPRequest,
    ListStatusPOptions, ListStatusPRequest, LoadMetadataPType, RemoveBlocksPRequest,
    RenamePOptions, RenamePRequest, ScheduleAsyncPersistencePOptions,
    ScheduleAsyncPersistencePRequest,
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

/// Immutable snapshot of the authenticated channel state.
///
/// `client` (which holds the tonic `Channel` + `channel-id` interceptor) and
/// `sasl_guard` (which keeps the SASL session alive on the Master side)
/// **must** travel together as a single unit: the Master uses the
/// `channel-id` injected by the interceptor to look up the SASL session,
/// so a stale `sasl_guard` paired with a fresh `client` (or vice versa) would
/// break authentication.
///
/// This struct enforces that pairing in the type system: it is never mutated
/// in place — instead, `MasterClient::reconnect` builds a brand-new
/// `AuthedState` and atomically swaps the `Arc` via `ArcSwap::store`. The old
/// `Arc<AuthedState>` is dropped only after the last in-flight reader
/// releases it, so the old SASL stream cannot be closed while anyone is still
/// using the old channel.
///
/// See `docs/RUST_PYTHON_SDK_OPTIMIZATION.md` Part II §1 + §II.3 for the
/// full consistency rationale.
struct AuthedState {
    client: AuthenticatedFsClient,
    /// Holds the SASL stream alive for the lifetime of `client`. `Option`
    /// because the test-only `from_channel` constructor and NOSASL mode do
    /// not need a SASL session.
    _sasl_guard: Option<SaslStreamGuard>,
}

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

/// Client for Goosefs `FileSystemMasterClientService` (Master:9200).
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
///
/// ## Concurrency model
///
/// `state` is an [`ArcSwap`] holding the immutable
/// `(channel + sasl_guard)` pair. The RPC hot path uses
/// `state.load()` — a wait-free single atomic load — to obtain a snapshot,
/// then clones the lightweight `AuthenticatedFsClient` (which is itself an
/// `Arc`-shared tonic `Channel`).  Failover (`reconnect`) atomically
/// publishes a new snapshot via `state.store(...)`; readers either see the
/// old snapshot (still valid for in-flight requests) or the new one — never
/// a torn mix.
///
/// The hot-path counters (`counter_*`) are cached as `Arc<Counter>` here
/// **outside** of `AuthedState` on purpose: they are process-level metric
/// handles that must outlive any `reconnect` and must not be re-resolved
/// from the global `DashMap` on every RPC. See
/// `docs/RUST_PYTHON_SDK_OPTIMIZATION.md` Part II §II.3 for the placement
/// rule.
#[derive(Clone)]
pub struct MasterClient {
    /// Atomically-swappable authenticated state (channel + SASL guard).
    state: Arc<ArcSwap<AuthedState>>,
    config: GoosefsConfig,
    inquire_client: Arc<dyn MasterInquireClient>,
    /// Per-client in-flight RPC counter, shared across all clones of this
    /// `MasterClient` (the `Arc` makes `#[derive(Clone)]` produce a shared
    /// counter rather than independent ones). Incremented in `with_retry`
    /// on entry and decremented on exit (success, error, or panic via the
    /// RAII guard). The `MasterClientPool::pick` P2C scheduler reads this
    /// to pick the least-loaded channel — crucially this count is accurate
    /// even for `MasterClient`s cloned out of the pool (e.g. by
    /// `GoosefsFileWriter`), which the previous pool-level counter could
    /// not track.
    inflight: Arc<AtomicUsize>,
    // ── Cached metric handles (lifetime-aligned with the MasterClient, not
    //    with any single channel/SASL session — see §9.1). Caching avoids
    //    `crate::metrics::counter(name)` DashMap lookups on every RPC.
    counter_get_status_ops: Arc<Counter>,
    counter_get_status_latency_us: Arc<Counter>,
    counter_list_status_ops: Arc<Counter>,
    counter_list_status_latency_us: Arc<Counter>,
    counter_create_file_ops: Arc<Counter>,
    counter_create_dir_ops: Arc<Counter>,
    counter_delete_ops: Arc<Counter>,
    counter_rename_ops: Arc<Counter>,
    counter_rpc_errors_total: Arc<Counter>,
    counter_rpc_auth_errors: Arc<Counter>,
    counter_rpc_unavailable_errors: Arc<Counter>,
}

impl MasterClient {
    /// Connect to the Goosefs Master.
    ///
    /// In single-master mode, connects directly to `config.master_addr`.
    /// In HA mode (multiple addresses in `config.master_addrs`), uses
    /// [`PollingMasterInquireClient`](crate::client::master_inquire::PollingMasterInquireClient)
    /// to discover the Primary first.
    ///
    /// Authentication is performed according to `config.auth_type`.
    pub async fn connect(config: &GoosefsConfig) -> Result<Self> {
        let inquire_client = create_master_inquire_client(config);
        Self::connect_with_inquire(config, inquire_client).await
    }

    /// Connect using an externally-provided [`MasterInquireClient`].
    ///
    /// This is useful when sharing a single inquire client across multiple
    /// client types (e.g. `MasterClient` + `WorkerManagerClient`).
    pub async fn connect_with_inquire(
        config: &GoosefsConfig,
        inquire_client: Arc<dyn MasterInquireClient>,
    ) -> Result<Self> {
        let primary_addr = inquire_client.get_primary_rpc_address().await?;
        let (client, sasl_guard) = Self::build_authenticated_client(config, &primary_addr).await?;
        debug!(addr = %primary_addr, auth_type = %config.auth_type, "connected to Goosefs Master");

        Ok(Self::from_parts(
            AuthedState {
                client,
                _sasl_guard: sasl_guard,
            },
            config.clone(),
            inquire_client,
        ))
    }

    /// Internal constructor that wires up the `ArcSwap<AuthedState>` and
    /// caches the hot-path metric handles in one place. Both
    /// [`Self::connect_with_inquire`] and [`Self::from_channel`] go through
    /// this so the field-list stays single-sourced.
    fn from_parts(
        state: AuthedState,
        config: GoosefsConfig,
        inquire_client: Arc<dyn MasterInquireClient>,
    ) -> Self {
        Self {
            state: Arc::new(ArcSwap::from_pointee(state)),
            config,
            inquire_client,
            inflight: Arc::new(AtomicUsize::new(0)),
            counter_get_status_ops: crate::metrics::counter(
                crate::metrics::name::CLIENT_GET_STATUS_OPS,
            ),
            counter_get_status_latency_us: crate::metrics::counter(
                crate::metrics::name::CLIENT_GET_STATUS_LATENCY_US,
            ),
            counter_list_status_ops: crate::metrics::counter(
                crate::metrics::name::CLIENT_LIST_STATUS_OPS,
            ),
            counter_list_status_latency_us: crate::metrics::counter(
                crate::metrics::name::CLIENT_LIST_STATUS_LATENCY_US,
            ),
            counter_create_file_ops: crate::metrics::counter(
                crate::metrics::name::CLIENT_CREATE_FILE_OPS,
            ),
            counter_create_dir_ops: crate::metrics::counter(
                crate::metrics::name::CLIENT_CREATE_DIR_OPS,
            ),
            counter_delete_ops: crate::metrics::counter(crate::metrics::name::CLIENT_DELETE_OPS),
            counter_rename_ops: crate::metrics::counter(crate::metrics::name::CLIENT_RENAME_OPS),
            counter_rpc_errors_total: crate::metrics::counter(
                crate::metrics::name::CLIENT_RPC_ERRORS_TOTAL,
            ),
            counter_rpc_auth_errors: crate::metrics::counter(
                crate::metrics::name::CLIENT_RPC_AUTH_ERRORS,
            ),
            counter_rpc_unavailable_errors: crate::metrics::counter(
                crate::metrics::name::CLIENT_RPC_UNAVAILABLE_ERRORS,
            ),
        }
    }

    /// Create from an existing tonic channel (useful for testing / channel sharing).
    ///
    /// **Note**: This bypasses authentication. The channel is wrapped with a
    /// no-op channel-id interceptor for API compatibility.
    pub fn from_channel(channel: Channel, config: GoosefsConfig) -> Self {
        let inquire_client = create_master_inquire_client(&config);
        let interceptor = ChannelIdInterceptor::new("test-no-auth".to_string());
        let intercepted = InterceptedService::new(channel, interceptor);
        Self::from_parts(
            AuthedState {
                client: FileSystemMasterClientServiceClient::new(intercepted),
                _sasl_guard: None,
            },
            config,
            inquire_client,
        )
    }

    /// Build a gRPC channel and perform authentication, returning an authenticated client
    /// and the SASL stream guard that must be kept alive.
    async fn build_authenticated_client(
        config: &GoosefsConfig,
        addr: &str,
    ) -> Result<(AuthenticatedFsClient, Option<SaslStreamGuard>)> {
        let channel = Self::build_raw_channel(config, addr).await?;

        // Perform SASL authentication based on the configured auth type
        let authenticator = ChannelAuthenticator::new(
            config.auth_type,
            config.auth_username.clone(),
            None, // impersonation_user: not yet supported
        )
        .with_auth_timeout(config.auth_timeout);

        let mut auth_channel = authenticator.authenticate(channel).await?;
        let sasl_guard = auth_channel.take_sasl_guard();

        Ok((
            FileSystemMasterClientServiceClient::new(auth_channel.channel),
            sasl_guard,
        ))
    }

    /// Build a raw gRPC channel to a specific master address (without authentication).
    async fn build_raw_channel(config: &GoosefsConfig, addr: &str) -> Result<Channel> {
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
    ///
    /// The new `(client, sasl_guard)` pair is published as a single
    /// [`AuthedState`] via [`ArcSwap::store`], so concurrent readers always
    /// observe a self-consistent snapshot. The old `Arc<AuthedState>` —
    /// containing the previous `sasl_guard` — is kept alive by any in-flight
    /// reader holding the old `Guard`, and is only dropped after the last
    /// such reader releases it. This guarantees that the old SASL stream is
    /// not closed while old-channel requests are still in flight.
    async fn reconnect(&self) -> Result<()> {
        // Reset cached primary so the inquire client re-polls all addresses.
        self.inquire_client.reset_cached_primary().await;

        let primary_addr = self.inquire_client.get_primary_rpc_address().await?;
        let (client, sasl_guard) =
            Self::build_authenticated_client(&self.config, &primary_addr).await?;
        // Single atomic publish: callers either see the previous AuthedState
        // in its entirety, or the new one — never a torn `(new client, old
        // guard)` mix.
        self.state.store(Arc::new(AuthedState {
            client,
            _sasl_guard: sasl_guard,
        }));
        debug!(addr = %primary_addr, "reconnected to Goosefs Master after failover");
        Ok(())
    }

    /// Execute an RPC with automatic retry on retriable errors.
    ///
    /// On retriable failure, the client reconnects to a (potentially new)
    /// Primary Master and retries up to [`MAX_RPC_RETRIES`] times.
    ///
    /// Each RPC attempt is wrapped with an in-flight counter guard so the
    /// P2C scheduler in [`MasterClientPool::pick`] sees an accurate load
    /// even for `MasterClient`s cloned out of the pool (e.g. by
    /// `GoosefsFileWriter`). The counter is shared across clones via the
    /// `Arc<AtomicUsize>` field, and the RAII guard guarantees decrement
    /// on success, error, or future cancellation (task drop).
    async fn with_retry<F, Fut, T>(&self, op_name: &str, mut f: F) -> Result<T>
    where
        // `FnMut` (rather than `Fn`) lets callers move owned state (e.g. the
        // request `path: String`) into the closure on the *first* attempt and
        // only `clone()` it inside the closure when a retry is actually
        // needed. See docs/RUST_PYTHON_SDK_OPTIMIZATION.md Part II §3.
        F: FnMut(AuthenticatedFsClient) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut last_err: Option<Error> = None;

        for attempt in 0..=MAX_RPC_RETRIES {
            // For retry attempts (attempt > 0) we know the previous call hit
            // a retriable error, which usually means the channel is dead.
            // Reconnect *before* re-sending — sending on a stale channel
            // just burns `request_timeout` for no gain. If the reconnect
            // itself fails, skip this attempt (the next iteration will try
            // reconnect again) so we don't consume retries on a known-bad
            // connection.
            if attempt > 0 {
                if let Err(reconnect_err) = self.reconnect().await {
                    warn!(
                        op = op_name,
                        attempt = attempt + 1,
                        error = %reconnect_err,
                        "reconnect failed; will retry reconnect on next attempt"
                    );
                    last_err = Some(Error::Internal {
                        message: format!("master reconnect failed: {}", reconnect_err),
                        source: None,
                    });
                    continue;
                }
            }

            // Wait-free atomic load: replaces the previous
            // `RwLock::read().await` round-trip with a single `Acquire` load.
            // The cloned client shares the underlying `tonic::Channel`
            // (which itself is `Arc`-internally cloneable and Send+Sync), so
            // this is cheap.
            let client: AuthenticatedFsClient = self.state.load().client.clone();

            // Mark this RPC as in-flight for P2C load balancing. The guard
            // decrements on drop — covering success, error, and future
            // cancellation (task drop) paths uniformly.
            self.inflight.fetch_add(1, Ordering::Relaxed);
            let _inflight_guard = InflightGuard(&self.inflight);

            match f(client).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    // Instrument: count RPC errors (use the cached Arc<Counter>
                    // to avoid a DashMap lookup on every error path).
                    self.counter_rpc_errors_total.inc(1);
                    // Classify the error
                    if err.is_authentication_error() {
                        self.counter_rpc_auth_errors.inc(1);
                    } else if err.is_unavailable() {
                        self.counter_rpc_unavailable_errors.inc(1);
                    }

                    if err.is_retriable() && attempt < MAX_RPC_RETRIES {
                        warn!(
                            op = op_name,
                            attempt = attempt + 1,
                            max = MAX_RPC_RETRIES,
                            error = %err,
                            "retriable error; will reconnect and retry"
                        );
                        last_err = Some(err);
                    } else {
                        return Err(err);
                    }
                }
            }
            // `_inflight_guard` drops here on the retry path → fetch_sub.
        }

        Err(last_err.unwrap_or_else(|| Error::Internal {
            message: format!("{}: exhausted all retries", op_name),
            source: None,
        }))
    }

    /// Get the file/directory status (equivalent to `stat` / `head`).
    #[instrument(skip(self), fields(path = %path))]
    pub async fn get_status(&self, path: &str) -> Result<FileInfo> {
        let start = std::time::Instant::now();
        // Allocate the owned path exactly once.
        //
        // The closure captures `path_owned: Option<String>` by `&mut`. On
        // the first attempt we `take()` (move) into the request — zero
        // additional allocation. On a retry attempt (rare) the `Option` is
        // empty, so we re-allocate from `path` (`&str`) one more time. Since
        // `with_retry` accepts `FnMut`, this pattern is sound.
        //
        // Net effect on the success path (the common case): one `String`
        // allocation per `get_status` call instead of two. See
        // docs/RUST_PYTHON_SDK_OPTIMIZATION.md Part II §3.
        let mut path_owned: Option<String> = Some(path.to_string());
        let result = self
            .with_retry("get_status", |mut client| {
                let req_path = path_owned.take().unwrap_or_else(|| path.to_string());
                async move {
                    let req = GetStatusPRequest {
                        path: Some(req_path),
                        options: Some(GetStatusPOptions::default()),
                        request_id: None,
                    };
                    let resp = client.get_status(req).await?;
                    resp.into_inner()
                        .file_info
                        .ok_or_else(|| Error::missing_field("file_info"))
                }
            })
            .await;
        // Instrument: ops count and latency — use the cached Arc<Counter>
        // handles to bypass the global DashMap lookup on the hot path.
        self.counter_get_status_ops.inc(1);
        self.counter_get_status_latency_us
            .inc(start.elapsed().as_micros() as i64);
        result
    }

    /// List the contents of a directory. Returns all FileInfo entries.
    ///
    /// When `recursive` is `true`, the master is asked to load metadata for
    /// every descendant (`load_metadata_type = Always`) — mirroring the Java
    /// `listStatusOptions.setRecursive(true)` default and the `goosefs fs ls -R`
    /// shell behaviour. Without this, the server only returns entries whose
    /// metadata is already loaded, which collapses a deep tree to its first
    /// level.
    ///
    /// This wraps a **server-side streaming** RPC — the server sends
    /// multiple `ListStatusPResponse` messages, each containing a batch
    /// of `FileInfo`.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn list_status(&self, path: &str, recursive: bool) -> Result<Vec<FileInfo>> {
        let start = std::time::Instant::now();
        let path = path.to_string();
        let load_metadata_type = if recursive {
            Some(LoadMetadataPType::Always as i32)
        } else {
            None
        };
        let result = self
            .with_retry("list_status", |mut client| {
                let path = path.clone();
                async move {
                    let req = ListStatusPRequest {
                        path: Some(path),
                        options: Some(ListStatusPOptions {
                            recursive: Some(recursive),
                            load_metadata_type,
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
            .await;
        // Instrument: ops count and latency (cached counter handles).
        self.counter_list_status_ops.inc(1);
        self.counter_list_status_latency_us
            .inc(start.elapsed().as_micros() as i64);
        result
    }

    /// Create a new file. Returns the `FileInfo` of the created file.
    #[instrument(skip(self, options), fields(path = %path))]
    pub async fn create_file(&self, path: &str, options: CreateFilePOptions) -> Result<FileInfo> {
        let path = path.to_string();
        let result = self
            .with_retry("create_file", |mut client| {
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
            .await;
        self.counter_create_file_ops.inc(1);
        result
    }

    /// Mark a file as completed (called after all blocks are written).
    ///
    /// # Idempotent operation ID
    ///
    /// `operation_id` is used by the Master for exactly-once semantics: if the
    /// RPC is retried after a network hiccup the Master detects the duplicate
    /// via `FsOpPId` and returns success without applying the operation twice.
    ///
    /// The caller (`GoosefsFileWriter`) generates a fresh `uuid::Uuid` at
    /// construction time and reuses it across all `complete_file` calls for the
    /// same write session.  The UUID is split into two `i64` halves via
    /// `Uuid::as_u64_pair()`:
    ///
    /// ```text
    /// (high, low) = uuid.as_u64_pair()
    /// FsOpPId { most_significant_bits: high as i64,
    ///           least_significant_bits: low  as i64 }
    /// ```
    ///
    /// This matches Java `UUID.getMostSignificantBits()` / `getLeastSignificantBits()`
    /// as verified in `DefaultFileSystemMaster.completeFile()`.
    ///
    /// # Note on Go SDK bug
    ///
    /// The Go SDK `base_filesystem.go:394-400` accepts an `operationID` parameter
    /// but **never writes it to the proto request**.  The Rust implementation
    /// fixes this: `operation_id` is always wired into `CompleteFilePOptions`.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn complete_file(
        &self,
        path: &str,
        ufs_length: Option<i64>,
        operation_id: Option<FsOpPId>,
    ) -> Result<()> {
        let path = path.to_string();
        self.with_retry("complete_file", |mut client| {
            let path = path.clone();
            async move {
                let common_options = operation_id.map(|op_id| FileSystemMasterCommonPOptions {
                    operation_id: Some(op_id),
                    ..Default::default()
                });
                let req = CompleteFilePRequest {
                    path: Some(path),
                    options: Some(CompleteFilePOptions {
                        ufs_length,
                        common_options,
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

    // -----------------------------------------------------------------------
    // RemoveBlocks RPC
    // -----------------------------------------------------------------------

    /// Request the Master to free block metadata for the given block IDs.
    ///
    /// This is the preferred cleanup path for `GoosefsFileWriter::cancel()`:
    /// it removes only the block metadata on the Master without touching the
    /// file-system namespace entry (the INCOMPLETE inode).
    ///
    /// Falls back to `delete_with_options(unchecked=true)` when this RPC fails.
    ///
    /// # Java authority
    ///
    /// Matches `FileSystemMasterClientServiceHandler.removeBlocks()` →
    /// `DefaultFileSystemMaster.removeBlocks(blockIds)`.
    #[instrument(skip(self, block_ids), fields(block_count = block_ids.len()))]
    pub async fn remove_blocks(&self, block_ids: Vec<i64>) -> Result<()> {
        if block_ids.is_empty() {
            return Ok(());
        }
        let block_ids_clone = block_ids.clone();
        self.with_retry("remove_blocks", |mut client| {
            let block_ids = block_ids_clone.clone();
            async move {
                let req = RemoveBlocksPRequest { block_ids };
                client.remove_blocks(req).await?;
                Ok(())
            }
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Delete with full DeleteOptions
    // -----------------------------------------------------------------------

    /// Delete a file or directory with fine-grained options.
    ///
    /// Prefer this over the legacy [`delete`](Self::delete) wrapper when you need
    /// `unchecked` or `goosefs_only` semantics.
    ///
    /// See [`DeleteOptions`] for field semantics and Java authority notes.
    #[instrument(skip(self, opts), fields(path = %path))]
    pub async fn delete_with_options(&self, path: &str, opts: DeleteOptions) -> Result<()> {
        let path = path.to_string();
        self.with_retry("delete_with_options", |mut client| {
            let path = path.clone();
            let opts = opts.clone();
            async move {
                let req = DeletePRequest {
                    path: Some(path),
                    options: Some(DeletePOptions {
                        recursive: Some(opts.recursive),
                        unchecked: Some(opts.unchecked),
                        goosefs_only: Some(opts.goosefs_only),
                        ..Default::default()
                    }),
                };
                client.remove(req).await?;
                Ok(())
            }
        })
        .await
    }

    /// Delete a file or directory (simple recursive wrapper).
    ///
    /// For `unchecked` or `goosefs_only` deletion use [`delete_with_options`](Self::delete_with_options)
    /// directly.
    #[instrument(skip(self), fields(path = %path, recursive = %recursive))]
    pub async fn delete(&self, path: &str, recursive: bool) -> Result<()> {
        let result = self
            .delete_with_options(
                path,
                DeleteOptions {
                    recursive,
                    ..Default::default()
                },
            )
            .await;
        self.counter_delete_ops.inc(1);
        result
    }

    /// Rename (move) a file or directory.
    #[instrument(skip(self), fields(src = %src, dst = %dst))]
    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let src = src.to_string();
        let dst = dst.to_string();
        let result = self
            .with_retry("rename", |mut client| {
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
            .await;
        self.counter_rename_ops.inc(1);
        result
    }

    /// Create a directory (recursive by default).
    ///
    /// Sets a default mode of `0755` (rwxr-xr-x) so that the corresponding
    /// UFS directory created by Goosefs has usable permissions.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn create_directory(&self, path: &str, recursive: bool) -> Result<()> {
        let path = path.to_string();
        let result = self
            .with_retry("create_directory", |mut client| {
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
            .await;
        self.counter_create_dir_ops.inc(1);
        result
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
    pub fn config(&self) -> &GoosefsConfig {
        &self.config
    }

    /// Get a reference to the underlying inquire client.
    ///
    /// Useful for sharing the same inquire client with `WorkerManagerClient`.
    pub fn inquire_client(&self) -> &Arc<dyn MasterInquireClient> {
        &self.inquire_client
    }
}

// ── In-flight RPC counting (P2C load balancing) ─────────────────────────────

/// RAII guard that decrements a `MasterClient`'s in-flight RPC counter on drop.
///
/// Created in [`MasterClient::with_retry`] around each RPC attempt. The guard
/// is panic-safe and cancellation-safe: if the future is dropped before
/// completion (e.g. `tokio::task` cancellation), the guard's `Drop` still
/// runs, so the counter never leaks.
struct InflightGuard<'a>(&'a AtomicUsize);

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

// ── Master connection pool (Part V R3) ───────────────────────────────────────

/// A P2C adaptive pool of [`MasterClient`]s over independent HTTP/2 channels.
///
/// # Why (Part V R3)
///
/// A single tonic [`Channel`] multiplexes all RPCs over one HTTP/2 connection,
/// which caps concurrency at `SETTINGS_MAX_CONCURRENT_STREAMS` (default 100).
/// Under 256-way concurrency over remote RTT the surplus requests queue in
/// `tower::Buffer`, which is the measured root cause of the remote GetFileStatus
/// / OpenFile regression vs Java (Java defaults to a channel pool). Spreading
/// requests across `master_connection_pool_size` channels removes the queue.
///
/// # HA consistency
///
/// Every pooled client is constructed with the **same** `inquire_client`, so a
/// failover decision is shared: all channels re-discover and switch to the same
/// new Primary, eliminating split-brain. Each channel performs its own SASL
/// handshake and carries a unique `channel-id`, fully compatible with the
/// `ArcSwap<AuthedState>` model.
///
/// `pick()` uses P2C (Power of Two Choices) with a fast PRNG (`fastrand`):
/// two distinct channels are sampled uniformly at random, and the one with
/// fewer in-flight RPCs is selected. Per-channel in-flight counts are tracked
/// inside each [`MasterClient`] (incremented in `with_retry`, decremented on
/// RPC completion), so the count is accurate even for `MasterClient`s cloned
/// out of the pool (e.g. by `GoosefsFileWriter`).
pub struct MasterClientPool {
    clients: Vec<Arc<MasterClient>>,
}

impl MasterClientPool {
    /// Connect a pool of `config.master_connection_pool_size` master clients,
    /// all sharing the supplied `inquire_client`.
    ///
    /// The size is clamped to at least 1, so this is a strict superset of the
    /// previous single-channel behaviour (`size = 1`).
    pub async fn connect_with_inquire(
        config: &GoosefsConfig,
        inquire_client: Arc<dyn MasterInquireClient>,
    ) -> Result<Self> {
        let size = config.master_connection_pool_size.max(1);
        // Connect all channels concurrently to avoid multiplying cold-start
        // latency by `size` (each connect = TCP + SASL handshake). All
        // connections share the same inquire_client, which is safe under
        // concurrent access (it deduplicates primary discovery internally).
        let clients: Vec<Arc<MasterClient>> = futures::future::try_join_all((0..size).map(|_| {
            let config = config.clone();
            let inquire = inquire_client.clone();
            async move {
                MasterClient::connect_with_inquire(&config, inquire)
                    .await
                    .map(Arc::new)
            }
        }))
        .await?;
        debug!(pool_size = size, "MasterClientPool connected");
        Ok(Self { clients })
    }

    /// P2C (Power of Two Choices) scheduling: uniformly sample two distinct
    /// connections at random and select the one with fewer in-flight RPCs.
    ///
    /// Returns a [`PooledClient`] handle that derefs to the chosen
    /// [`MasterClient`]. The per-channel in-flight count is maintained
    /// inside `MasterClient::with_retry` (not by this guard), so it stays
    /// accurate even for clients cloned out of the pool. This is wait-free,
    /// O(1), and avoids the thundering-herd problem of a full-scan min-pick.
    ///
    /// # Why P2C with a PRNG?
    ///
    /// Naïve "scan all, pick min → fetch_add" is a two-step non-atomic
    /// operation under high concurrency: multiple threads simultaneously
    /// select the same "current minimum" connection, creating a thundering
    /// herd that *increases* lock contention on that connection.  P2C
    /// (Power of Two Choices) samples two candidates uniformly at random
    /// and picks the lighter, which is wait-free and O(1). The classical
    /// O(log log n) load bound assumes independent uniform sampling; we
    /// achieve that here with `fastrand`, a fast non-cryptographic PRNG
    /// whose statistical quality is sufficient for load balancing (it does
    /// not need to be cryptographically secure, only uniform).
    pub fn pick(&self) -> PooledClient {
        let n = self.clients.len();
        if n == 1 {
            return PooledClient {
                client: self.clients[0].clone(),
            };
        }
        // Uniformly sample two distinct indices with a fast PRNG. Unlike the
        // previous deterministic `base % n` scheme (where one index was
        // constant for long runs), this gives independent samples per call,
        // which is what the P2C load bound requires.
        let a = fastrand::usize(0..n);
        let b = loop {
            let b = fastrand::usize(0..n);
            if b != a {
                break b;
            }
        };

        // Read per-client in-flight counts (tracked inside each MasterClient
        // via with_retry, so clone-out-of-pool RPCs are counted too).
        let la = self.clients[a].inflight.load(Ordering::Relaxed);
        let lb = self.clients[b].inflight.load(Ordering::Relaxed);
        let idx = if la <= lb { a } else { b };

        debug!(
            a_idx = a,
            b_idx = b,
            a_inflight = la,
            b_inflight = lb,
            picked = idx,
            "P2C pick"
        );

        PooledClient {
            client: self.clients[idx].clone(),
        }
    }

    /// Number of pooled channels.
    pub fn size(&self) -> usize {
        self.clients.len()
    }
}

/// Handle returned by [`MasterClientPool::pick`].
///
/// Wraps an `Arc<MasterClient>` and implements [`Deref`](std::ops::Deref) to
/// `MasterClient` so callers can invoke methods directly:
///
/// ```ignore
/// let pooled = pool.pick();
/// let info = pooled.get_status("/path").await?;
/// // pooled is dropped here (no counter to decrement — in-flight tracking
/// // lives inside MasterClient::with_retry)
/// ```
///
/// The per-channel in-flight RPC counter is maintained by
/// [`MasterClient::with_retry`] (incremented on RPC entry, decremented on
/// exit via an RAII guard), **not** by this handle's `Drop`. This means the
/// count is accurate even when a `MasterClient` is cloned out of the pool
/// (e.g. by `GoosefsFileWriter`) and used long after the `PooledClient` is
/// dropped — the previous pool-level counter could not track those RPCs.
///
/// `Clone` is a cheap `Arc` clone; cloning is safe because the in-flight
/// counter is shared via `Arc<AtomicUsize>` inside the `MasterClient`.
#[derive(Clone)]
pub struct PooledClient {
    pub client: Arc<MasterClient>,
}

impl std::ops::Deref for PooledClient {
    type Target = MasterClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[cfg(test)]
mod tests {
    //! Concurrency-correctness tests for the `ArcSwap<AuthedState>`-based
    //! state model introduced as part of the GetFileStatus performance
    //! optimisation work.  See
    //! `docs/RUST_PYTHON_SDK_OPTIMIZATION.md` Part II §1 / §II.3 / §II.4 for
    //! the rationale and the gating-test requirement.
    //!
    //! These tests intentionally do **not** spin up a real Master server.
    //! They exercise the *type-level* invariant that motivates the change:
    //!
    //! 1. The `(client, sasl_guard)` pair is published as a single
    //!    immutable `Arc<AuthedState>`.  A concurrent reader either sees
    //!    the previous publication in its entirety, or the new one — never
    //!    a torn `(new client, old guard)` mix.
    //!
    //! 2. The previous publication's resources (in particular the SASL
    //!    guard standing in for `SaslStreamGuard` here) are *not* dropped
    //!    until the last reader releases its `Arc`.  This is the
    //!    "old SASL stream cannot be closed while in-flight requests still
    //!    use the old channel" property.
    //!
    //! Both properties are checked against a stand-in payload struct that
    //! mirrors the shape of `AuthedState` (channel + guard).  The real
    //! `AuthedState` is private to the module and uses tonic's generated
    //! client stub which is hard to instantiate in a unit test, so the
    //! stand-in keeps the test focused on the `ArcSwap` semantics that
    //! `MasterClient` relies on.

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use arc_swap::ArcSwap;

    /// Stand-in for `AuthedState`.  The exact field types do not matter for
    /// the property we are testing — what matters is that:
    /// - `epoch` is an immutable per-publication tag (analogue of
    ///   "this channel's `channel-id`")
    /// - `guard` is a resource whose drop must be deferred until no reader
    ///   is using this snapshot any more (analogue of `SaslStreamGuard`).
    struct AuthedStateLike {
        /// Identifies which `store(...)` produced this snapshot.  In the
        /// real code, the tonic `Channel`'s `channel-id` plays the same
        /// role.
        epoch: u64,
        /// Same epoch as above, but inside a separately-allocated leaf —
        /// in the real code the SASL session id would live separately
        /// from the channel-id metadata.  Reading both and asserting they
        /// match is what proves there is no torn read.
        guard_epoch: Arc<u64>,
        /// When the snapshot is dropped, increments the shared counter.
        /// Mirrors `SaslStreamGuard`'s `Drop` impl (which would close the
        /// SASL stream on the Master side).
        drop_counter: Arc<AtomicUsize>,
    }

    impl Drop for AuthedStateLike {
        fn drop(&mut self) {
            self.drop_counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn new_state(epoch: u64, drop_counter: Arc<AtomicUsize>) -> Arc<AuthedStateLike> {
        Arc::new(AuthedStateLike {
            epoch,
            guard_epoch: Arc::new(epoch),
            drop_counter,
        })
    }

    /// Property 1 — atomic publication.
    ///
    /// N reader threads continuously load the current `AuthedState` and
    /// assert that the `(epoch, guard_epoch)` pair is internally
    /// consistent.  Meanwhile a writer thread atomically swaps in fresh
    /// states.  A torn read would produce a snapshot whose `guard_epoch`
    /// disagrees with `epoch` — which can never happen with `ArcSwap`
    /// because the *whole* `Arc<AuthedState>` is replaced as one pointer.
    #[test]
    fn arcswap_publication_is_atomic_under_concurrent_readers() {
        const READERS: usize = 32;
        const RECONNECT_ROUNDS: usize = 200;

        let drop_counter = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(ArcSwap::from(new_state(0, drop_counter.clone())));
        let stop = Arc::new(AtomicBool::new(false));
        // Wait for every reader (and the writer) to be scheduled before the
        // reconnect loop starts — otherwise a slow CI runner can finish all
        // rounds before some reader threads ever enter their loop, which
        // falsely fails with "reader observed nothing".
        let ready = Arc::new(Barrier::new(READERS + 1));

        let mut readers = Vec::with_capacity(READERS);
        for _ in 0..READERS {
            let state = state.clone();
            let stop = stop.clone();
            let ready = ready.clone();
            readers.push(thread::spawn(move || {
                ready.wait();
                let mut observed_epochs: Vec<u64> = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let snap = state.load();
                    // The two fields are written by *different* allocations
                    // in different orders; only the `Arc<AuthedStateLike>`
                    // pointer publication is atomic.  This pair must
                    // always agree.
                    assert_eq!(
                        snap.epoch, *snap.guard_epoch,
                        "torn read: ArcSwap published a half-swapped snapshot",
                    );
                    observed_epochs.push(snap.epoch);
                }
                // One final load after stop so every reader records at least
                // the terminal published epoch even if scheduling was tight.
                let snap = state.load();
                assert_eq!(snap.epoch, *snap.guard_epoch);
                observed_epochs.push(snap.epoch);
                observed_epochs
            }));
        }

        ready.wait();

        // Writer: simulate `reconnect` events by store()'ing fresh states.
        for round in 1..=RECONNECT_ROUNDS {
            state.store(new_state(round as u64, drop_counter.clone()));
            // Yield so readers have a chance to observe each epoch.
            thread::sleep(Duration::from_micros(50));
        }

        stop.store(true, Ordering::Relaxed);
        // Make sure each reader saw at least one swap take effect.
        for r in readers {
            let observed = r.join().expect("reader thread panicked");
            assert!(!observed.is_empty(), "reader observed nothing");
            let max = observed.iter().copied().max().unwrap();
            assert!(
                max >= 1,
                "reader never saw a reconnect-published epoch (max={})",
                max,
            );
        }
    }

    /// Property 2 — no premature drop.
    ///
    /// A reader that has already `load()`ed a snapshot is then *paused*
    /// (e.g. parked between obtaining the channel and finishing the gRPC
    /// round-trip).  During the pause we run many more `store(...)`
    /// rounds.  The reader's `Guard`/`Arc<AuthedState>` keeps the old
    /// snapshot alive, so its `drop_counter` must NOT have ticked for
    /// that particular epoch yet.  Once the reader drops its handle, the
    /// old snapshot finally gets reclaimed.
    ///
    /// This is what guarantees, in the real code, that
    /// `SaslStreamGuard::drop` (which would close the SASL stream on the
    /// Master and unregister the `channel-id`) never fires while there
    /// are still in-flight RPCs holding a clone of the old client.
    #[test]
    fn old_snapshot_outlives_concurrent_swap_until_reader_releases() {
        let drop_counter = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(ArcSwap::from(new_state(1, drop_counter.clone())));

        // Reader grabs a snapshot and *holds it*.
        let held = state.load_full();
        assert_eq!(held.epoch, 1);

        // While the reader is still holding `held`, simulate a flurry of
        // reconnect events.
        for round in 2..=50 {
            state.store(new_state(round, drop_counter.clone()));
        }

        // The held snapshot must still be alive, hence its drop counter
        // contribution has not fired.
        // Other (orphaned-on-store) snapshots may have been dropped — but
        // *not the one we hold*.  Verify by inspecting the held snapshot
        // directly: if it had been dropped we would not be able to read
        // its fields without UB; we additionally assert that the absolute
        // drop count is < total publications, i.e. at least one snapshot
        // (the one we hold) is still alive.
        let observed_drops = drop_counter.load(Ordering::SeqCst);
        // 50 publications happened (epochs 1..=50). The current one in
        // `state` and the one held by `held` must both still be alive →
        // at most 48 drops so far.
        assert!(
            observed_drops <= 48,
            "old snapshot was dropped while a reader still held it: \
             drops = {} (expected <= 48)",
            observed_drops,
        );
        assert_eq!(held.epoch, 1, "held snapshot was mutated in place");

        // Release the reader's hold.
        drop(held);

        // Replace the still-current snapshot too so that *no* live Arc
        // remains, then wait for ArcSwap's lazy reclamation to settle.
        state.store(new_state(999, drop_counter.clone()));
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            // 50 original epochs + 1 final = 51 publications, but the
            // final-stored one is still in `state`, so we expect 50 drops.
            if drop_counter.load(Ordering::SeqCst) >= 50 {
                break;
            }
            if Instant::now() > deadline {
                panic!(
                    "expected >= 50 drops after releasing the held snapshot, \
                     observed {}",
                    drop_counter.load(Ordering::SeqCst),
                );
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    // ── P2C scheduler + in-flight counter tests ───────────────────────────
    //
    // These tests cover the P2C `pick()` selection logic and the
    // `InflightGuard` RAII semantics introduced to fix the "counter does
    // not track cloned MasterClient RPCs" issue. They do **not** perform any
    // network I/O — a lazy tonic channel is used so no connection is
    // established.

    use super::{InflightGuard, MasterClient, MasterClientPool};
    use crate::config::GoosefsConfig;
    use std::panic::AssertUnwindSafe;

    /// Build a `MasterClient` without any network I/O.
    ///
    /// `connect_lazy()` creates a `Channel` that only connects on the first
    /// RPC — which these tests never send. The resulting client has a valid
    /// `inflight: Arc<AtomicUsize>` field (initially 0) that the P2C
    /// scheduler reads.
    fn make_test_master_client() -> MasterClient {
        let endpoint = tonic::transport::Endpoint::from_static("http://localhost:0").connect_lazy();
        MasterClient::from_channel(endpoint, GoosefsConfig::new("localhost:0"))
    }

    /// Build a pool of `n` test clients (no network I/O).
    fn make_test_pool(n: usize) -> MasterClientPool {
        let clients: Vec<Arc<MasterClient>> = (0..n)
            .map(|_| Arc::new(make_test_master_client()))
            .collect();
        MasterClientPool { clients }
    }

    /// Under unequal load, `pick()` must always select the lighter channel.
    #[tokio::test]
    async fn pick_chooses_lighter_candidate() {
        let pool = make_test_pool(2);
        // Artificially load client 0 with 10 in-flight RPCs; client 1 stays idle.
        pool.clients[0].inflight.store(10, Ordering::Relaxed);
        // Regardless of which two candidates are sampled, the lighter one
        // (client 1) must win every time.
        for _ in 0..200 {
            let picked = pool.pick();
            assert!(
                Arc::ptr_eq(&picked.client, &pool.clients[1]),
                "pick() selected the heavier channel"
            );
        }
    }

    /// With `n >= 2`, `pick()` never compares a channel against itself —
    /// the two sampled indices are always distinct.
    #[tokio::test]
    async fn pick_samples_two_distinct_candidates() {
        // With n=1 there is no choice; with n=2 the only possible pair is
        // (0,1), so every pick observes both. Verify that over many picks
        // both channels are returned (proving the loop produces a != b).
        let pool = make_test_pool(2);
        let mut saw_0 = false;
        let mut saw_1 = false;
        // Set equal load so selection is driven purely by the PRNG +
        // tie-break (la <= lb → a), not by load asymmetry.
        for _ in 0..1000 {
            let picked = pool.pick();
            if Arc::ptr_eq(&picked.client, &pool.clients[0]) {
                saw_0 = true;
            } else if Arc::ptr_eq(&picked.client, &pool.clients[1]) {
                saw_1 = true;
            } else {
                panic!("pick() returned a client not in the pool");
            }
        }
        assert!(
            saw_0 && saw_1,
            "pick() never sampled one of the two channels"
        );
    }

    /// Under equal load, `pick()` distributes selections across all channels
    /// (no channel is starved over a reasonable sample size).
    #[tokio::test]
    async fn pick_balances_equal_loads() {
        let n = 4;
        let pool = make_test_pool(n);
        let mut hits = vec![0usize; n];
        for _ in 0..4000 {
            let picked = pool.pick();
            for (i, c) in pool.clients.iter().enumerate() {
                if Arc::ptr_eq(&picked.client, c) {
                    hits[i] += 1;
                    break;
                }
            }
        }
        // Every channel must receive at least one pick (P2C with uniform
        // sampling visits all channels given enough trials). We don't assert
        // a tight distribution — just non-starvation.
        for (i, &h) in hits.iter().enumerate() {
            assert!(h > 0, "channel {} was starved by pick()", i);
        }
    }

    /// `InflightGuard` decrements the counter on normal scope exit.
    #[test]
    fn inflight_counter_decrements_on_normal_exit() {
        let counter = AtomicUsize::new(0);
        {
            counter.fetch_add(1, Ordering::Relaxed);
            let _guard = InflightGuard(&counter);
            assert_eq!(
                counter.load(Ordering::Relaxed),
                1,
                "counter must be 1 while guard alive"
            );
        }
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "counter must return to 0 after guard drops"
        );
    }

    /// `InflightGuard` decrements the counter on early (explicit) drop.
    #[test]
    fn inflight_counter_decrements_on_early_drop() {
        let counter = AtomicUsize::new(0);
        counter.fetch_add(1, Ordering::Relaxed);
        let guard = InflightGuard(&counter);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        drop(guard); // explicit early drop
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "counter must be 0 after early drop"
        );
    }

    /// `InflightGuard` decrements the counter even when the containing
    /// stack unwinds due to a panic (cancellation safety).
    #[test]
    fn inflight_guard_decrements_on_panic() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_for_unwind = counter.clone();
        let result = std::panic::catch_unwind(AssertUnwindSafe(move || {
            counter_for_unwind.fetch_add(1, Ordering::Relaxed);
            let _guard = InflightGuard(&counter_for_unwind);
            assert_eq!(
                counter_for_unwind.load(Ordering::Relaxed),
                1,
                "counter must be 1 while guard alive"
            );
            panic!("simulated panic mid-RPC");
        }));
        assert!(result.is_err(), "test should have panicked");
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "guard must decrement on panic unwind (cancellation safety)"
        );
    }

    /// `MasterClient::clone()` shares the in-flight counter via `Arc`, so
    /// RPCs issued through a cloned client (e.g. by `GoosefsFileWriter`)
    /// are visible to the P2C scheduler. This is the core correctness
    /// property that fixes the "counter does not track cloned MasterClient"
    /// issue.
    #[tokio::test]
    async fn master_client_clone_shares_inflight_counter() {
        let original = make_test_master_client();
        let cloned = original.clone();
        // Mutate via the clone; the original must observe the same value.
        cloned.inflight.store(7, Ordering::Relaxed);
        assert_eq!(
            original.inflight.load(Ordering::Relaxed),
            7,
            "clone must share the in-flight counter (Arc<AtomicUsize>)"
        );
        // And vice-versa.
        original.inflight.store(3, Ordering::Relaxed);
        assert_eq!(
            cloned.inflight.load(Ordering::Relaxed),
            3,
            "mutations via original must be visible to clone"
        );
    }

    /// `PooledClient` implements `Clone` (cheap `Arc` clone) and the clone
    /// shares the same underlying `MasterClient` (and thus the same
    /// in-flight counter).
    #[tokio::test]
    async fn pooled_client_clone_shares_master_client() {
        let pool = make_test_pool(2);
        let picked = pool.pick();
        let cloned = picked.clone();
        assert!(
            Arc::ptr_eq(&picked.client, &cloned.client),
            "PooledClient::clone must share the same Arc<MasterClient>"
        );
    }
}
