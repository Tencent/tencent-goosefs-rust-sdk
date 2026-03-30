//! GooseFS Worker Manager client for worker discovery.
//!
//! Wraps `WorkerManagerMasterClientService` (Master:9200) to fetch
//! the list of live workers and their addresses.
//!
//! ## HA / Multi-Master Support
//!
//! When multiple Master addresses are configured, uses
//! [`MasterInquireClient`] to discover the Primary Master.

use std::sync::Arc;

use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{debug, instrument};

use crate::auth::{ChannelAuthenticator, ChannelIdInterceptor};
use crate::client::master_inquire::{create_master_inquire_client, MasterInquireClient};
use crate::config::GooseFsConfig;
use crate::error::{Error, Result};
use crate::proto::grpc::block::{
    worker_manager_master_client_service_client::WorkerManagerMasterClientServiceClient,
    GetWorkerInfoListPOptions, WorkerInfo,
};

/// Type alias for the authenticated WorkerManager gRPC client.
type AuthenticatedWorkerMgrClient =
    WorkerManagerMasterClientServiceClient<InterceptedService<Channel, ChannelIdInterceptor>>;

/// Client for `WorkerManagerMasterClientService` (Master:9200).
///
/// Used to discover the live worker list for block routing.
#[derive(Clone)]
pub struct WorkerManagerClient {
    inner: AuthenticatedWorkerMgrClient,
}

impl WorkerManagerClient {
    /// Connect to the GooseFS Master for worker management.
    ///
    /// In HA mode, discovers the Primary Master first via the inquire client.
    pub async fn connect(config: &GooseFsConfig) -> Result<Self> {
        let inquire_client = create_master_inquire_client(config);
        Self::connect_with_inquire(config, inquire_client).await
    }

    /// Connect using an externally-provided [`MasterInquireClient`].
    ///
    /// This allows sharing the same inquire client with `MasterClient`,
    /// avoiding redundant Primary discovery.
    pub async fn connect_with_inquire(
        config: &GooseFsConfig,
        inquire_client: Arc<dyn MasterInquireClient>,
    ) -> Result<Self> {
        let primary_addr = inquire_client.get_primary_rpc_address().await?;
        let endpoint_uri = format!("http://{}", primary_addr);

        let endpoint = Channel::from_shared(endpoint_uri)
            .map_err(|e| Error::ConfigError {
                message: format!("invalid master endpoint: {}", e),
            })?
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout);

        let channel = endpoint.connect().await?;

        // Perform SASL authentication based on the configured auth type
        let authenticator =
            ChannelAuthenticator::new(config.auth_type, config.auth_username.clone(), None)
                .with_auth_timeout(config.auth_timeout);

        let auth_channel = authenticator.authenticate(channel).await?;
        debug!(addr = %primary_addr, auth_type = %config.auth_type, "connected to WorkerManagerMasterClientService");

        Ok(Self {
            inner: WorkerManagerMasterClientServiceClient::new(auth_channel.channel),
        })
    }

    /// Create from an existing tonic channel.
    ///
    /// **Note**: This bypasses authentication.
    pub fn from_channel(channel: Channel) -> Self {
        let interceptor = ChannelIdInterceptor::new("test-no-auth".to_string());
        let intercepted = InterceptedService::new(channel, interceptor);
        Self {
            inner: WorkerManagerMasterClientServiceClient::new(intercepted),
        }
    }

    /// Fetch the full list of workers from the Master.
    #[instrument(skip(self))]
    pub async fn get_worker_info_list(&self) -> Result<Vec<WorkerInfo>> {
        let req = GetWorkerInfoListPOptions {};

        let resp = self.inner.clone().get_worker_info_list(req).await?;

        Ok(resp.into_inner().worker_infos)
    }
}
