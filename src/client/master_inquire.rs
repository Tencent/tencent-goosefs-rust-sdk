//! Master discovery clients for GooseFS HA (High Availability).
//!
//! Mirrors the Java `MasterInquireClient` hierarchy:
//!
//! - [`SingleMasterInquireClient`] — used when a single Master address is
//!   configured. Returns the address directly with zero network overhead.
//! - [`PollingMasterInquireClient`] — used when multiple Master addresses are
//!   configured. Polls each address via the `getServiceVersion` gRPC RPC to
//!   find the Primary Master (only the Primary responds successfully).
//!
//! # How Primary detection works
//!
//! In a GooseFS HA cluster, only the **Primary** Master serves client-facing
//! RPCs. Standby Masters reject `getServiceVersion` with `NotFound` (or
//! `Unavailable`). [`PollingMasterInquireClient`] iterates over all configured
//! addresses and returns the first one that responds successfully.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::config::GooseFsConfig;
use crate::error::{Error, Result};
use crate::proto::grpc::version::{
    service_version_client_service_client::ServiceVersionClientServiceClient,
    GetServiceVersionPRequest, ServiceType,
};
use crate::retry::{ExponentialTimeBoundedRetry, RetryPolicy};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Abstraction for Master address discovery.
///
/// Implementations decide how to locate the Primary Master RPC address.
#[async_trait]
pub trait MasterInquireClient: Send + Sync {
    /// Discover and return the Primary Master's RPC address (`host:port`).
    ///
    /// For [`SingleMasterInquireClient`] this is a no-op.
    /// For [`PollingMasterInquireClient`] this polls all addresses.
    async fn get_primary_rpc_address(&self) -> Result<String>;

    /// Return the full list of configured Master RPC addresses.
    fn get_master_rpc_addresses(&self) -> Vec<String>;

    /// Reset the cached Primary address (e.g. after a failover).
    ///
    /// For [`SingleMasterInquireClient`] this is a no-op.
    /// For [`PollingMasterInquireClient`] this clears the internal cache
    /// so the next call to [`get_primary_rpc_address`] will re-poll.
    async fn reset_cached_primary(&self);
}

// ---------------------------------------------------------------------------
// SingleMasterInquireClient
// ---------------------------------------------------------------------------

/// A trivial inquire client for single-master deployments.
///
/// Always returns the one configured address without any network call.
pub struct SingleMasterInquireClient {
    address: String,
}

impl SingleMasterInquireClient {
    pub fn new(address: String) -> Self {
        Self { address }
    }
}

#[async_trait]
impl MasterInquireClient for SingleMasterInquireClient {
    async fn get_primary_rpc_address(&self) -> Result<String> {
        Ok(self.address.clone())
    }

    fn get_master_rpc_addresses(&self) -> Vec<String> {
        vec![self.address.clone()]
    }

    async fn reset_cached_primary(&self) {
        // No-op for single master.
    }
}

// ---------------------------------------------------------------------------
// PollingMasterInquireClient
// ---------------------------------------------------------------------------

/// Discovers the Primary Master by polling `getServiceVersion` on every
/// configured address.
///
/// Only the Primary Master responds successfully to this RPC with
/// `ServiceType::MetaMasterClientService`. Standby nodes return `NotFound`
/// or fail to connect.
pub struct PollingMasterInquireClient {
    addresses: Vec<String>,
    /// Cached Primary address from the last successful discovery.
    cached_primary: Arc<RwLock<Option<String>>>,
    /// Retry configuration.
    max_duration: Duration,
    initial_sleep: Duration,
    max_sleep: Duration,
    /// Timeout for a single ping attempt (connect + RPC deadline).
    polling_timeout: Duration,
}

impl PollingMasterInquireClient {
    pub fn new(
        addresses: Vec<String>,
        max_duration: Duration,
        initial_sleep: Duration,
        max_sleep: Duration,
        polling_timeout: Duration,
    ) -> Self {
        Self {
            addresses,
            cached_primary: Arc::new(RwLock::new(None)),
            max_duration,
            initial_sleep,
            max_sleep,
            polling_timeout,
        }
    }

    /// Try to ping the `getServiceVersion` RPC on a single address.
    ///
    /// Returns `Ok(())` if the address is the Primary Master.
    async fn ping_meta_service(&self, addr: &str) -> std::result::Result<(), PingError> {
        let endpoint_uri = format!("http://{}", addr);

        let endpoint = Channel::from_shared(endpoint_uri)
            .map_err(|e| PingError::Fatal(format!("invalid endpoint for {}: {}", addr, e)))?
            .connect_timeout(self.polling_timeout)
            .timeout(self.polling_timeout);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| PingError::Unavailable(format!("{}: connection failed: {}", addr, e)))?;

        let mut client = ServiceVersionClientServiceClient::new(channel);

        let req = GetServiceVersionPRequest {
            service_type: Some(ServiceType::MetaMasterClientService as i32),
            allowed_on_standby_masters: Some(false),
        };

        match client.get_service_version(req).await {
            Ok(resp) => {
                let version = resp.into_inner().version.unwrap_or(0);
                debug!(addr = %addr, version = version, "primary master detected");
                Ok(())
            }
            Err(status) => match status.code() {
                tonic::Code::NotFound => {
                    // Standby master — skip silently.
                    debug!(addr = %addr, "standby master (NotFound)");
                    Err(PingError::Standby)
                }
                tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Cancelled => {
                    // Transient / timeout errors — skip this address, try the next one.
                    debug!(addr = %addr, code = ?status.code(), "master unavailable or timed out");
                    Err(PingError::Unavailable(format!(
                        "{}: [{}] {}",
                        addr,
                        status.code(),
                        status.message()
                    )))
                }
                _ => {
                    warn!(addr = %addr, code = ?status.code(), msg = %status.message(), "unexpected error pinging master");
                    Err(PingError::Fatal(format!(
                        "{}: [{}] {}",
                        addr,
                        status.code(),
                        status.message()
                    )))
                }
            },
        }
    }

    /// Reset the cached Primary address (e.g. after a failover).
    pub async fn reset_primary(&self) {
        let mut cache = self.cached_primary.write().await;
        *cache = None;
    }
}

#[async_trait]
impl MasterInquireClient for PollingMasterInquireClient {
    async fn get_primary_rpc_address(&self) -> Result<String> {
        // Fast path: return cached primary if available.
        {
            let cache = self.cached_primary.read().await;
            if let Some(ref addr) = *cache {
                // Verify the cached primary is still alive.
                if self.ping_meta_service(addr).await.is_ok() {
                    debug!(addr = %addr, "cached primary still valid");
                    return Ok(addr.clone());
                }
                // Cached primary is stale, fall through to full poll.
                debug!(addr = %addr, "cached primary stale, re-polling");
            }
        }

        // Slow path: poll all addresses.
        let mut retry =
            ExponentialTimeBoundedRetry::new(self.max_duration, self.initial_sleep, self.max_sleep);

        let mut last_errors: Vec<String> = Vec::new();

        while retry.should_retry() {
            last_errors.clear();

            for addr in &self.addresses {
                match self.ping_meta_service(addr).await {
                    Ok(()) => {
                        // Found the Primary!
                        info!(addr = %addr, attempts = retry.attempt_count(), "discovered primary master");
                        let mut cache = self.cached_primary.write().await;
                        *cache = Some(addr.clone());
                        return Ok(addr.clone());
                    }
                    Err(PingError::Standby) => {
                        // Expected for standby nodes, continue.
                        last_errors.push(format!("{}: standby", addr));
                        continue;
                    }
                    Err(PingError::Unavailable(msg)) => {
                        last_errors.push(msg);
                        continue;
                    }
                    Err(PingError::Fatal(msg)) => {
                        last_errors.push(msg);
                        // Fatal error on this address — break this round and retry.
                        break;
                    }
                }
            }

            // Sleep before next round.
            let sleep_dur = retry.next_sleep();
            debug!(
                attempt = retry.attempt_count(),
                sleep_ms = sleep_dur.as_millis(),
                "no primary found this round, sleeping"
            );
            tokio::time::sleep(sleep_dur).await;
        }

        Err(Error::Internal {
            message: format!(
                "failed to find primary master after {} attempts across {} addresses. Last round errors: [{}]",
                retry.attempt_count(),
                self.addresses.len(),
                last_errors.join("; "),
            ),
            source: None,
        })
    }

    fn get_master_rpc_addresses(&self) -> Vec<String> {
        self.addresses.clone()
    }

    async fn reset_cached_primary(&self) {
        self.reset_primary().await;
    }
}

/// Internal error type for ping classification.
enum PingError {
    /// The address is a standby master (returned NotFound).
    Standby,
    /// The address is temporarily unreachable.
    Unavailable(String),
    /// A non-retriable error occurred.
    Fatal(String),
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Create the appropriate [`MasterInquireClient`] based on the config.
///
/// - If only one address is configured → [`SingleMasterInquireClient`].
/// - If multiple addresses → [`PollingMasterInquireClient`].
pub fn create_master_inquire_client(config: &GooseFsConfig) -> Arc<dyn MasterInquireClient> {
    let addrs = config.master_addresses();

    if addrs.len() <= 1 {
        let addr = addrs
            .into_iter()
            .next()
            .unwrap_or_else(|| config.master_addr.clone());
        debug!(addr = %addr, "using SingleMasterInquireClient");
        Arc::new(SingleMasterInquireClient::new(addr))
    } else {
        debug!(addresses = ?addrs, "using PollingMasterInquireClient");
        Arc::new(PollingMasterInquireClient::new(
            addrs,
            config.master_inquire_retry_max_duration,
            config.master_inquire_initial_sleep,
            config.master_inquire_max_sleep,
            config.master_polling_timeout,
        ))
    }
}
