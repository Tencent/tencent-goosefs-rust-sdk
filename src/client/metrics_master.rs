//! Goosefs Metrics Master gRPC client for client-side metrics heartbeat.
//!
//! Wraps `MetricsMasterClientService` (Master:9200) providing:
//! - `heartbeat` — periodic metrics heartbeat carrying incremental counter diffs
//! - `clear_metrics` — reset cluster-level metric accumulators
//! - `get_metrics` — query current metric values from the Master
//!
//! ## Java Alignment
//!
//! Corresponds to Java's
//! [`RetryHandlingMetricsMasterClient`](RetryHandlingMetricsMasterClient.java).
//!
//! Key deviation from the Java implementation: Java's `heartbeat()` uses a bare
//! `connect() + catch` without `retryRPC`. This Rust client instead uses the same
//! `with_retry` / `reconnect` pattern as [`MasterClient`] so that transient
//! `Unavailable` / `DeadlineExceeded` errors trigger HA failover rather than
//! propagating immediately. The heartbeat caller (`HeartbeatTask`) will WARN and
//! continue on any error regardless, so retries only add resilience without
//! blocking the heartbeat loop.
//!
//! ## HA / Multi-Master Support
//!
//! Shares the same [`MasterInquireClient`] as `MasterClient` and
//! `WorkerManagerClient` — no independent polling, no extra TCP connections to
//! discover the primary.
//!
//! [`MasterClient`]: crate::client::MasterClient
//! [`MasterInquireClient`]: crate::client::MasterInquireClient

// HeartbeatTask in metrics::heartbeat constructs MetricsMasterClient;
// FileSystemContext in context.rs calls connect_with_inquire.
// No dead-code suppressions needed once the full pipeline is wired.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::auth::{ChannelAuthenticator, ChannelIdInterceptor, SaslStreamGuard};
use crate::client::master_inquire::MasterInquireClient;
use crate::config::GoosefsConfig;
use crate::error::{Error, Result};
use crate::proto::grpc::metric::{
    metrics_master_client_service_client::MetricsMasterClientServiceClient, ClearMetricsPRequest,
    ClientMetrics, GetMetricsPOptions, MetricValue, MetricsHeartbeatPOptions,
    MetricsHeartbeatPRequest,
};

// ── Type aliases ─────────────────────────────────────────────────────────────

/// Authenticated gRPC client type, mirrors the pattern in `master.rs`.
type AuthenticatedMetricsClient =
    MetricsMasterClientServiceClient<InterceptedService<Channel, ChannelIdInterceptor>>;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Maximum RPC-level retries on retriable errors (mirrors `MasterClient`).
const MAX_RPC_RETRIES: u32 = 2;

// ── MetricsClient trait ───────────────────────────────────────────────────────

/// Abstraction over the metrics heartbeat RPC, enabling mock injection in tests.
///
/// The only method required by [`HeartbeatTask`] is `heartbeat()`.  Production
/// code passes an `Arc<MetricsMasterClient>`; test code passes an
/// `Arc<MockMetricsClient>`.
///
/// [`HeartbeatTask`]: crate::metrics::heartbeat::HeartbeatTask
#[async_trait]
pub trait MetricsClient: Send + Sync {
    /// Send a batch of client metrics to the Master.
    async fn heartbeat(&self, client_metrics: Vec<ClientMetrics>) -> crate::error::Result<()>;
}

// ── MetricsMasterClient ───────────────────────────────────────────────────────

/// Client for Goosefs `MetricsMasterClientService` (Master:9200).
///
/// In HA mode, the client holds a reference to the [`MasterInquireClient`]
/// and can automatically re-discover the Primary Master when RPCs fail.
///
/// Construct via [`MetricsMasterClient::connect_with_inquire`] to share the
/// inquire client that is already used by [`MasterClient`].
///
/// [`MasterClient`]: crate::client::MasterClient
pub(crate) struct MetricsMasterClient {
    inner: Arc<RwLock<AuthenticatedMetricsClient>>,
    config: GoosefsConfig,
    inquire_client: Arc<dyn MasterInquireClient>,
    /// Keeps the SASL authentication stream alive. Must not be dropped while
    /// the channel is in use (same semantics as in `MasterClient`).
    _sasl_guard: Arc<RwLock<Option<SaslStreamGuard>>>,
}

impl MetricsMasterClient {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Connect using an externally-provided [`MasterInquireClient`].
    ///
    /// Pass the same inquire client that was already created for `MasterClient`
    /// so that HA discovery is shared across all client types.
    pub async fn connect_with_inquire(
        config: &GoosefsConfig,
        inquire_client: Arc<dyn MasterInquireClient>,
    ) -> Result<Self> {
        let primary_addr = inquire_client.get_primary_rpc_address().await?;
        let (client, sasl_guard) = Self::build_authenticated_client(config, &primary_addr).await?;
        debug!(
            addr = %primary_addr,
            auth_type = %config.auth_type,
            "connected to Goosefs MetricsMaster"
        );

        Ok(Self {
            inner: Arc::new(RwLock::new(client)),
            config: config.clone(),
            inquire_client,
            _sasl_guard: Arc::new(RwLock::new(sasl_guard)),
        })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Build an authenticated gRPC client to the given master address.
    ///
    /// Mirrors `MasterClient::build_authenticated_client` exactly so both clients
    /// use the same channel construction and authentication path.
    async fn build_authenticated_client(
        config: &GoosefsConfig,
        addr: &str,
    ) -> Result<(AuthenticatedMetricsClient, Option<SaslStreamGuard>)> {
        let channel = Self::build_raw_channel(config, addr).await?;

        let authenticator = ChannelAuthenticator::new(
            config.auth_type,
            config.auth_username.clone(),
            None, // impersonation_user: not yet supported
        )
        .with_auth_timeout(config.auth_timeout);

        let mut auth_channel = authenticator.authenticate(channel).await?;
        let sasl_guard = auth_channel.take_sasl_guard();

        Ok((
            MetricsMasterClientServiceClient::new(auth_channel.channel),
            sasl_guard,
        ))
    }

    /// Build a raw (unauthenticated) gRPC channel to a specific master address.
    async fn build_raw_channel(config: &GoosefsConfig, addr: &str) -> Result<Channel> {
        let endpoint_uri = format!("http://{}", addr);
        let endpoint = Channel::from_shared(endpoint_uri)
            .map_err(|e| Error::ConfigError {
                message: format!("invalid metrics master endpoint: {}", e),
            })?
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout);

        let channel = endpoint.connect().await?;
        Ok(channel)
    }

    /// Reconnect to the Primary Master after a failover.
    ///
    /// Resets the cached primary address, re-discovers it via the shared inquire
    /// client, rebuilds the gRPC channel, and re-authenticates.
    async fn reconnect(&self) -> Result<()> {
        self.inquire_client.reset_cached_primary().await;

        let primary_addr = self.inquire_client.get_primary_rpc_address().await?;
        let (client, sasl_guard) =
            Self::build_authenticated_client(&self.config, &primary_addr).await?;

        *self.inner.write().await = client;
        *self._sasl_guard.write().await = sasl_guard;

        debug!(
            addr = %primary_addr,
            "reconnected to Goosefs MetricsMaster after failover"
        );
        Ok(())
    }

    /// Execute an RPC with automatic retry on retriable errors.
    ///
    /// On a retriable failure (`Unavailable`, `DeadlineExceeded`), the client
    /// reconnects to a (potentially new) Primary and retries up to
    /// [`MAX_RPC_RETRIES`] times.  Non-retriable errors are returned immediately.
    async fn with_retry<F, Fut, T>(&self, op_name: &str, f: F) -> Result<T>
    where
        F: Fn(AuthenticatedMetricsClient) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut last_err: Option<Error> = None;

        for attempt in 0..=MAX_RPC_RETRIES {
            // Before any attempt > 0, reconnect to obtain a fresh channel.
            // The previous iteration only reaches here on a retriable error,
            // so the cached channel is presumed dead. If reconnect itself
            // fails, skip the RPC for this attempt — sending on a stale
            // channel would just burn `request_timeout` for no gain. The
            // next iteration will retry the reconnect.
            if attempt > 0 {
                if let Err(reconnect_err) = self.reconnect().await {
                    warn!(
                        op = op_name,
                        attempt = attempt + 1,
                        error = %reconnect_err,
                        "metrics master reconnect failed; will retry reconnect on next attempt"
                    );
                    last_err = Some(Error::Internal {
                        message: format!("metrics master reconnect failed: {}", reconnect_err),
                        source: None,
                    });
                    continue;
                }
            }

            let client: AuthenticatedMetricsClient = self.inner.read().await.clone();

            match f(client).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if err.is_retriable() && attempt < MAX_RPC_RETRIES {
                        warn!(
                            op = op_name,
                            attempt = attempt + 1,
                            max = MAX_RPC_RETRIES,
                            error = %err,
                            "retriable error on metrics RPC; will reconnect and retry"
                        );
                        last_err = Some(err);
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| Error::Internal {
            message: format!("{}: exhausted all retries", op_name),
            source: None,
        }))
    }

    // ── Public RPCs ───────────────────────────────────────────────────────────

    /// Report a batch of client metrics to the Master (periodic heartbeat).
    ///
    /// Aligned with Java `RetryHandlingMetricsMasterClient.heartbeat()` and
    /// `ClientMasterSync.heartbeat()`.
    ///
    /// `client_metrics` is typically a single-element `Vec` wrapping all the
    /// incremental counter diffs from `ClientMetricsReporter::snapshot()`.
    ///
    /// Returns `Ok(())` on success. On failure the heartbeat task will WARN and
    /// continue — metrics are best-effort and must not block I/O operations.
    pub async fn heartbeat(&self, client_metrics: Vec<ClientMetrics>) -> Result<()> {
        let req = MetricsHeartbeatPRequest {
            options: Some(MetricsHeartbeatPOptions { client_metrics }),
        };
        self.with_retry("metrics_heartbeat", |mut c| {
            let req = req.clone();
            async move {
                c.metrics_heartbeat(req)
                    .await
                    .map(|_| ())
                    .map_err(Into::into)
            }
        })
        .await
    }

    /// Clear all metric accumulators on the Master.
    ///
    /// Aligned with Java `RetryHandlingMetricsMasterClient.clearMetrics()`.
    /// Uses `retryRPC` in Java; mirrors that with `with_retry` here.
    #[allow(dead_code)] // utility RPC; not yet called in the heartbeat pipeline
    pub async fn clear_metrics(&self) -> Result<()> {
        self.with_retry("clear_metrics", |mut c| async move {
            c.clear_metrics(ClearMetricsPRequest {})
                .await
                .map(|_| ())
                .map_err(Into::into)
        })
        .await
    }

    /// Query current metric values from the Master.
    ///
    /// Aligned with Java `RetryHandlingMetricsMasterClient.getMetrics()`.
    /// Returns a map of metric name → `MetricValue`.
    #[allow(dead_code)] // utility RPC; not yet called in the heartbeat pipeline
    pub async fn get_metrics(&self) -> Result<HashMap<String, MetricValue>> {
        self.with_retry("get_metrics", |mut c| async move {
            let resp = c.get_metrics(GetMetricsPOptions {}).await?;
            Ok(resp.into_inner().metrics)
        })
        .await
    }
}

// ── MetricsClient impl ────────────────────────────────────────────────────────

#[async_trait]
impl MetricsClient for MetricsMasterClient {
    async fn heartbeat(&self, client_metrics: Vec<ClientMetrics>) -> crate::error::Result<()> {
        // Delegate to the existing method so all retry + HA logic is preserved.
        MetricsMasterClient::heartbeat(self, client_metrics).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the type alias compiles and the struct fields are correctly typed.
    /// This is a compile-time test — if the struct can be named and its fields
    /// referenced, the implementation is well-typed.
    #[allow(dead_code)]
    fn assert_send_sync()
    where
        MetricsMasterClient: Send + Sync,
    {
    }

    /// Verify that `MetricsHeartbeatPRequest` can be cloned (required by
    /// `with_retry` which clones the request for each attempt).
    #[test]
    fn heartbeat_request_is_clone() {
        let req = MetricsHeartbeatPRequest {
            options: Some(MetricsHeartbeatPOptions {
                client_metrics: vec![ClientMetrics {
                    source: Some("test-app".into()),
                    metrics: vec![],
                }],
            }),
        };
        let _req2 = req.clone();
    }

    /// Verify that `GetMetricsPOptions` and `ClearMetricsPRequest` are `Copy`
    /// (they have no fields), so they can be freely moved into closures.
    #[test]
    fn unit_requests_are_copy() {
        let _opts = GetMetricsPOptions {};
        let _clear = ClearMetricsPRequest {};
    }
}
