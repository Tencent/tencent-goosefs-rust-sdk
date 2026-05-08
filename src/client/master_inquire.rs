//! Master discovery clients for Goosefs HA (High Availability).
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
//! In a Goosefs HA cluster, only the **Primary** Master serves client-facing
//! RPCs. Standby Masters reject `getServiceVersion` with `NotFound` (or
//! `Unavailable`). [`PollingMasterInquireClient`] iterates over all configured
//! addresses and returns the first one that responds successfully.
//!
//! # Singleflight deduplication
//!
//! When the cached Primary address is stale and multiple concurrent callers
//! all call `get_primary_rpc_address()` at once, only **one** goroutine should
//! issue the expensive polling loop.  The others wait for the result via a
//! `tokio::sync::watch` channel.  This is the Rust equivalent of Go's
//! `singleflight.Group`.
//!
//! Implementation:
//! - A `Mutex<Option<watch::Receiver<PollResult>>>` acts as the singleflight
//!   gate.  `None` means no poll in flight.
//! - The **leader** sets `Some(rx)` while it polls, then sends the result on
//!   the watch channel and sets the gate back to `None`.
//! - **Followers** clone the `rx` from the gate and call `rx.changed().await`
//!   to wait for the leader's result.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::config::GoosefsConfig;
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
    /// so the next call to [`get_primary_rpc_address`](Self::get_primary_rpc_address) will re-poll.
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
// Singleflight gate types
// ---------------------------------------------------------------------------

/// The result broadcast by the polling leader to all waiting followers.
///
/// `Ok(addr)` — the discovered primary address.  
/// `Err(msg)` — the polling loop exhausted all retries; followers should
/// surface this as an [`Error::Internal`].
#[derive(Debug, Clone)]
enum PollResult {
    Ok(String),
    Err(String),
}

/// Singleflight gate: holds a `watch::Receiver` while a poll is in flight,
/// or `None` when no poll is active.
type PollGate = Mutex<Option<watch::Receiver<Option<PollResult>>>>;

// ---------------------------------------------------------------------------
// PollingMasterInquireClient
// ---------------------------------------------------------------------------

/// Discovers the Primary Master by polling `getServiceVersion` on every
/// configured address.
///
/// Only the Primary Master responds successfully to this RPC with
/// `ServiceType::MetaMasterClientService`. Standby nodes return `NotFound`
/// or fail to connect.
///
/// # Singleflight
///
/// Concurrent callers share a single in-flight poll via a `watch` channel.
/// The first caller becomes the **leader** and broadcasts the result; all
/// other callers wait on the same channel receiver.
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
    /// Singleflight gate — `Some(rx)` means a poll is in flight.
    poll_gate: Arc<PollGate>,
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
            poll_gate: Arc::new(Mutex::new(None)),
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

    /// Run the full polling loop and return the result.
    ///
    /// This is the **leader path** called only once even when multiple
    /// concurrent callers are waiting (singleflight).
    async fn poll_for_primary(&self) -> std::result::Result<String, String> {
        let mut retry =
            ExponentialTimeBoundedRetry::new(self.max_duration, self.initial_sleep, self.max_sleep);

        let mut last_errors: Vec<String> = Vec::new();

        while retry.should_retry() {
            last_errors.clear();

            for addr in &self.addresses {
                match self.ping_meta_service(addr).await {
                    Ok(()) => {
                        info!(addr = %addr, attempts = retry.attempt_count(), "discovered primary master");
                        // Update the shared cache.
                        let mut cache = self.cached_primary.write().await;
                        *cache = Some(addr.clone());
                        return Ok(addr.clone());
                    }
                    Err(PingError::Standby) => {
                        last_errors.push(format!("{}: standby", addr));
                        continue;
                    }
                    Err(PingError::Unavailable(msg)) => {
                        last_errors.push(msg);
                        continue;
                    }
                    Err(PingError::Fatal(msg)) => {
                        last_errors.push(msg);
                        break;
                    }
                }
            }

            let sleep_dur = retry.next_sleep();
            debug!(
                attempt = retry.attempt_count(),
                sleep_ms = sleep_dur.as_millis(),
                "no primary found this round, sleeping"
            );
            tokio::time::sleep(sleep_dur).await;
        }

        Err(format!(
            "failed to find primary master after {} attempts across {} addresses. Last round errors: [{}]",
            retry.attempt_count(),
            self.addresses.len(),
            last_errors.join("; "),
        ))
    }
}

#[async_trait]
impl MasterInquireClient for PollingMasterInquireClient {
    async fn get_primary_rpc_address(&self) -> Result<String> {
        // ── Fast path: return cached primary if still alive ──────────────────
        {
            let cache = self.cached_primary.read().await;
            if let Some(ref addr) = *cache {
                if self.ping_meta_service(addr).await.is_ok() {
                    debug!(addr = %addr, "cached primary still valid");
                    return Ok(addr.clone());
                }
                debug!(addr = %addr, "cached primary stale, re-polling");
            }
        }

        // ── Singleflight gate ────────────────────────────────────────────────
        //
        // Try to become the **leader** (first caller to find the gate = None).
        // If another caller is already the leader, become a **follower** and
        // wait for the broadcast result.

        let rx_opt: Option<watch::Receiver<Option<PollResult>>> = {
            let mut gate = self.poll_gate.lock().await;
            match &*gate {
                Some(existing_rx) => {
                    // Another goroutine is already polling — clone the receiver
                    // and wait as a follower.
                    debug!("singleflight follower: waiting for in-flight poll");
                    Some(existing_rx.clone())
                }
                None => {
                    // We are the leader.  Create the watch channel and
                    // install the receiver in the gate before releasing
                    // the lock so followers can attach.
                    let (tx, rx) = watch::channel::<Option<PollResult>>(None);
                    *gate = Some(rx);
                    drop(gate); // Release the lock before the expensive poll.

                    debug!("singleflight leader: starting primary poll");

                    let result = self.poll_for_primary().await;

                    // Broadcast result to all followers.
                    let broadcast = match &result {
                        Ok(addr) => PollResult::Ok(addr.clone()),
                        Err(msg) => PollResult::Err(msg.clone()),
                    };
                    // `send` fails only when all receivers are gone, which is
                    // harmless — nobody was waiting.
                    let _ = tx.send(Some(broadcast));

                    // Clear the gate so future callers start fresh.
                    let mut gate2 = self.poll_gate.lock().await;
                    *gate2 = None;

                    return result.map_err(|msg| Error::Internal {
                        message: msg,
                        source: None,
                    });
                }
            }
        };

        // ── Follower path: wait for the leader's result ───────────────────────
        if let Some(mut rx) = rx_opt {
            // Wait until the value changes from None (initial sentinel) to Some.
            loop {
                // `changed()` returns Err only if the sender is dropped, which
                // means the leader panicked — treat as a transient error and
                // fall back to a fresh poll.
                if rx.changed().await.is_err() {
                    warn!("singleflight leader dropped channel, follower retrying");
                    return self.get_primary_rpc_address().await;
                }

                let value = rx.borrow().clone();
                match value {
                    Some(PollResult::Ok(addr)) => {
                        debug!(addr = %addr, "singleflight follower received primary");
                        return Ok(addr);
                    }
                    Some(PollResult::Err(msg)) => {
                        return Err(Error::Internal {
                            message: msg,
                            source: None,
                        });
                    }
                    None => {
                        // Spurious wake (should not happen with watch, but be safe).
                        continue;
                    }
                }
            }
        }

        // Unreachable: either the leader branch or the follower branch returns.
        Err(Error::Internal {
            message: "singleflight logic error: neither leader nor follower path returned"
                .to_string(),
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
pub fn create_master_inquire_client(config: &GoosefsConfig) -> Arc<dyn MasterInquireClient> {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_single_master_returns_address() {
        let client = SingleMasterInquireClient::new("master:19998".to_string());
        assert_eq!(
            client.get_primary_rpc_address().await.unwrap(),
            "master:19998"
        );
        assert_eq!(
            client.get_master_rpc_addresses(),
            vec!["master:19998".to_string()]
        );
    }

    #[tokio::test]
    async fn test_single_master_reset_is_noop() {
        let client = SingleMasterInquireClient::new("master:19998".to_string());
        client.reset_cached_primary().await;
        // Still returns the same address.
        assert_eq!(
            client.get_primary_rpc_address().await.unwrap(),
            "master:19998"
        );
    }

    /// Verify that `poll_gate` starts as `None` (no poll in flight).
    #[tokio::test]
    async fn test_polling_client_gate_starts_empty() {
        let client = PollingMasterInquireClient::new(
            vec!["a:1".to_string(), "b:2".to_string()],
            Duration::from_millis(100),
            Duration::from_millis(10),
            Duration::from_millis(50),
            Duration::from_millis(50),
        );
        let gate = client.poll_gate.lock().await;
        assert!(gate.is_none(), "gate should start empty");
    }

    /// Verify `get_master_rpc_addresses` returns all configured addresses.
    #[tokio::test]
    async fn test_polling_client_addresses() {
        let addrs = vec!["host1:19998".to_string(), "host2:19998".to_string()];
        let client = PollingMasterInquireClient::new(
            addrs.clone(),
            Duration::from_millis(100),
            Duration::from_millis(10),
            Duration::from_millis(50),
            Duration::from_millis(50),
        );
        assert_eq!(client.get_master_rpc_addresses(), addrs);
    }

    /// Verify `reset_cached_primary` clears the cache.
    #[tokio::test]
    async fn test_polling_client_reset_clears_cache() {
        let client = PollingMasterInquireClient::new(
            vec!["host:19998".to_string()],
            Duration::from_millis(100),
            Duration::from_millis(10),
            Duration::from_millis(50),
            Duration::from_millis(50),
        );
        // Manually populate the cache.
        {
            let mut cache = client.cached_primary.write().await;
            *cache = Some("host:19998".to_string());
        }
        client.reset_cached_primary().await;
        let cache = client.cached_primary.read().await;
        assert!(cache.is_none(), "cache should be cleared after reset");
    }

    /// Verify that concurrent callers share a single poll: the leader sends
    /// the result on the watch channel and followers receive it without
    /// issuing their own polls.
    ///
    /// We simulate the singleflight mechanism directly without a real gRPC
    /// server by using the `watch` channel internals.
    #[tokio::test]
    async fn test_singleflight_gate_broadcast() {
        // Create a watch channel as the leader would.
        let (tx, rx) = watch::channel::<Option<PollResult>>(None);

        // Simulate a follower cloning the receiver.
        let mut follower_rx = rx.clone();

        // Counter to track how many times the follower receives a value.
        let received = Arc::new(AtomicUsize::new(0));
        let received_clone = received.clone();

        // Spawn follower task.
        let follower = tokio::spawn(async move {
            follower_rx.changed().await.unwrap();
            let value = follower_rx.borrow().clone();
            if let Some(PollResult::Ok(addr)) = value {
                received_clone.fetch_add(1, Ordering::SeqCst);
                addr
            } else {
                panic!("expected Ok result");
            }
        });

        // Leader sends the result after a small delay.
        tokio::time::sleep(Duration::from_millis(5)).await;
        tx.send(Some(PollResult::Ok("primary:19998".to_string())))
            .unwrap();

        let addr = follower.await.unwrap();
        assert_eq!(addr, "primary:19998");
        assert_eq!(received.load(Ordering::SeqCst), 1);
    }
}
