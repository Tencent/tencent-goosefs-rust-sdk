//! Channel authenticator — manages the SASL authentication lifecycle for gRPC channels.
//!
//! Corresponds to Java's `ChannelAuthenticator` + `AuthenticatedChannelClientDriver`.
//!
//! ## Authentication Flow
//!
//! ```text
//! NOSASL mode:
//!   Client ──── use Channel directly, no SASL handshake ────→ Server
//!
//! SIMPLE mode:
//!   Client ──── SaslMessage(CHALLENGE, PLAIN initial response, clientId, SIMPLE) ──→ Server
//!   Client ←── SaslMessage(SUCCESS) ─────────────────────────────────────────────── Server
//!   Client ──── subsequent RPCs carry channel-id metadata ───────────────────────→ Server
//! ```

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Status, Streaming};
use tracing::debug;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::proto::grpc::sasl::{
    sasl_authentication_service_client::SaslAuthenticationServiceClient, SaslMessage,
};

use super::sasl_client::{PlainSaslClientHandler, SaslClientHandler};

// ── AuthType enum ────────────────────────────────────────────

/// GooseFS authentication type.
///
/// Corresponds to Java's `AuthType` enum and the configuration key
/// `goosefs.security.authentication.type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AuthType {
    /// No authentication — skip SASL handshake, use the gRPC channel directly.
    NoSasl,
    /// Simple authentication (default) — transmit username via PLAIN SASL; server does not verify password.
    Simple,
    // TODO: implement as needed
    // /// Custom authentication — server verifies via a custom AuthenticationProvider.
    // Custom,
    // /// Kerberos authentication — mutual authentication via Kerberos GSSAPI.
    // Kerberos,
}

impl AuthType {
    /// Return the canonical lowercase string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthType::NoSasl => "nosasl",
            AuthType::Simple => "simple",
        }
    }
}

impl fmt::Display for AuthType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AuthType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "nosasl" | "no_sasl" => Ok(AuthType::NoSasl),
            "simple" => Ok(AuthType::Simple),
            // TODO: support later
            "custom" => Err(
                "CUSTOM authentication is not yet implemented; use NOSASL or SIMPLE".to_string(),
            ),
            "kerberos" => Err(
                "KERBEROS authentication is not yet implemented; use NOSASL or SIMPLE".to_string(),
            ),
            _ => Err(format!(
                "unknown authentication type '{}'. Currently supported: nosasl, simple",
                s
            )),
        }
    }
}

impl Default for AuthType {
    fn default() -> Self {
        AuthType::Simple
    }
}

// ── channel-id interceptor ───────────────────────────────────

/// gRPC metadata key used to carry the channel-id in every RPC request.
///
/// Corresponds to Java's `ChannelIdInjector.S_CLIENT_ID_KEY`.
const CHANNEL_ID_METADATA_KEY: &str = "channel-id";

/// Channel-id interceptor — injects the channel-id into the metadata of every RPC request.
///
/// After successful authentication, the server associates the authenticated session
/// with this channel-id. All subsequent RPC requests must carry it.
///
/// Corresponds to Java's `ChannelIdInjector`.
#[derive(Clone)]
pub struct ChannelIdInterceptor {
    channel_id: String,
}

impl ChannelIdInterceptor {
    /// Create a new channel-id interceptor.
    pub fn new(channel_id: String) -> Self {
        Self { channel_id }
    }
}

impl tonic::service::Interceptor for ChannelIdInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request.metadata_mut().insert(
            CHANNEL_ID_METADATA_KEY,
            self.channel_id
                .parse()
                .map_err(|_| Status::internal("invalid channel-id"))?,
        );
        Ok(request)
    }
}

// ── ChannelAuthenticator ─────────────────────────────────────

/// Channel authenticator — manages the SASL authentication lifecycle for gRPC channels.
///
/// Corresponds to Java's `ChannelAuthenticator`.
///
/// ## Usage
///
/// ```rust,no_run
/// use goosefs_client::auth::{AuthType, ChannelAuthenticator};
/// use tonic::transport::Channel;
///
/// # async fn example() -> goosefs_client::error::Result<()> {
/// let channel = Channel::from_static("http://127.0.0.1:9200").connect().await?;
/// let authenticator = ChannelAuthenticator::new(
///     AuthType::Simple,
///     "testuser".to_string(),
///     None,
/// );
/// let auth_channel = authenticator.authenticate(channel).await?;
/// // Use auth_channel to create gRPC clients...
/// # Ok(())
/// # }
/// ```
pub struct ChannelAuthenticator {
    /// Authentication type.
    auth_type: AuthType,
    /// Login username.
    username: String,
    /// Password ("noPassword" in SIMPLE mode).
    password: String,
    /// Optional impersonation user.
    impersonation_user: Option<String>,
    /// Authentication timeout.
    auth_timeout: Duration,
}

/// Authenticated channel — wraps the original Channel with a channel-id interceptor.
///
/// **Important**: In SIMPLE mode, the SASL bidirectional stream must remain open
/// for the entire lifetime of the authenticated channel. The server uses the stream
/// as a long-poll on authentication state:
/// - Client closing the stream → server unregisters the channel-id (unauthenticated)
/// - Server closing the stream → client is no longer authenticated
///
/// The `_sasl_guard` field holds the stream handles to keep them alive.
/// Corresponds to Java's `AuthenticatedChannelClientDriver` which also keeps
/// the `StreamObserver` open after the handshake completes.
pub struct AuthenticatedChannel {
    /// gRPC service with channel-id interceptor.
    pub channel: InterceptedService<Channel, ChannelIdInterceptor>,
    /// The channel-id assigned during authentication.
    pub channel_id: String,
    /// Guard that keeps the SASL authentication stream alive.
    ///
    /// In SIMPLE mode, dropping this will close the bidirectional SASL stream,
    /// causing the server to unregister the channel-id and reject subsequent RPCs.
    /// In NOSASL mode, this is `None`.
    _sasl_guard: Option<SaslStreamGuard>,
}

impl AuthenticatedChannel {
    /// Take ownership of the SASL stream guard.
    ///
    /// The caller **must** keep the returned guard alive for as long as the
    /// authenticated channel is in use. Dropping the guard will close the
    /// SASL stream, causing the server to revoke authentication.
    pub fn take_sasl_guard(&mut self) -> Option<SaslStreamGuard> {
        self._sasl_guard.take()
    }
}

/// Holds the SASL stream handles to prevent them from being dropped.
///
/// When this guard is dropped, the mpsc sender is dropped, which closes the
/// client→server half of the bidirectional stream. The server's `onCompleted()`
/// handler then calls `unregisterChannel()`, invalidating the authentication.
///
/// This type is intentionally opaque — callers only need to hold it, not interact with it.
pub struct SaslStreamGuard {
    /// Sender side of the SASL message channel — keeps the client→server stream open.
    _tx: mpsc::Sender<SaslMessage>,
    /// Server→client response stream — keeps the server→client stream open.
    _response_stream: Streaming<SaslMessage>,
}

// SAFETY: SaslStreamGuard is only held behind Arc<RwLock<Option<SaslStreamGuard>>>
// and is never accessed concurrently. The inner `Streaming<SaslMessage>` contains
// `dyn Decoder + Send` which is not `Sync`, but we only need to keep the stream
// alive (not read from it concurrently). The RwLock provides the necessary
// synchronization for any access.
unsafe impl Send for SaslStreamGuard {}
unsafe impl Sync for SaslStreamGuard {}

impl ChannelAuthenticator {
    /// Create a new channel authenticator.
    ///
    /// # Arguments
    /// - `auth_type`: authentication type
    /// - `username`: login username
    /// - `impersonation_user`: optional impersonation user
    pub fn new(auth_type: AuthType, username: String, impersonation_user: Option<String>) -> Self {
        Self {
            auth_type,
            username,
            password: "noPassword".to_string(),
            impersonation_user,
            auth_timeout: Duration::from_secs(30),
        }
    }

    /// Set the authentication timeout.
    pub fn with_auth_timeout(mut self, timeout: Duration) -> Self {
        self.auth_timeout = timeout;
        self
    }

    /// Set the password (usually not needed in SIMPLE mode).
    pub fn with_password(mut self, password: String) -> Self {
        self.password = password;
        self
    }

    /// Get the authentication type.
    pub fn auth_type(&self) -> AuthType {
        self.auth_type
    }

    /// Authenticate the gRPC channel and return an authenticated channel.
    ///
    /// ## NOSASL mode
    /// Skips the SASL handshake and returns the channel wrapped with a channel-id interceptor.
    ///
    /// ## SIMPLE mode
    /// 1. Generate a unique channel-id (UUID)
    /// 2. Perform SASL handshake via `SaslAuthenticationService.authenticate` bidirectional streaming RPC
    /// 3. On success, return the channel wrapped with a channel-id interceptor
    pub async fn authenticate(&self, channel: Channel) -> Result<AuthenticatedChannel> {
        match self.auth_type {
            AuthType::NoSasl => self.authenticate_nosasl(channel),
            AuthType::Simple => self.authenticate_simple(channel).await,
        }
    }

    /// NOSASL mode — skip authentication, wrap the channel directly.
    fn authenticate_nosasl(&self, channel: Channel) -> Result<AuthenticatedChannel> {
        debug!(auth_type = "NOSASL", "skipping SASL authentication");
        // Still inject a channel-id in NOSASL mode for API consistency
        let channel_id = Uuid::new_v4().to_string();
        let interceptor = ChannelIdInterceptor::new(channel_id.clone());
        Ok(AuthenticatedChannel {
            channel: InterceptedService::new(channel, interceptor),
            channel_id,
            _sasl_guard: None,
        })
    }

    /// SIMPLE mode — authenticate via PLAIN SASL handshake.
    ///
    /// Corresponds to the SIMPLE branch in Java's `ChannelAuthenticator.authenticate()`:
    /// 1. Create `SaslClientHandlerPlain`
    /// 2. Create `AuthenticatedChannelClientDriver`
    /// 3. Perform handshake via `SaslAuthenticationService.authenticate` bidirectional stream
    /// 4. Wait for authentication to complete
    /// 5. Inject `ChannelIdInjector`
    async fn authenticate_simple(&self, channel: Channel) -> Result<AuthenticatedChannel> {
        let channel_id = Uuid::new_v4().to_string();
        let channel_ref = format!("rust-client-{}", &channel_id[..8]);

        debug!(
            auth_type = "SIMPLE",
            username = %self.username,
            channel_id = %channel_id,
            "starting SASL PLAIN authentication"
        );

        // 1. Create PLAIN SASL client handler
        let sasl_handler = PlainSaslClientHandler::new_simple(
            &self.username,
            &self.password,
            self.impersonation_user.as_deref(),
        );

        // 2. Generate initial SASL message
        let initial_message = sasl_handler.initial_message(&channel_id, &channel_ref)?;

        // 3. Create bidirectional streaming RPC
        let (tx, rx) = mpsc::channel::<SaslMessage>(8);

        // Send initial message
        tx.send(initial_message)
            .await
            .map_err(|_| Error::Internal {
                message: "failed to send initial SASL message".to_string(),
                source: None,
            })?;

        let stream = ReceiverStream::new(rx);
        let mut sasl_client = SaslAuthenticationServiceClient::new(channel.clone());

        // 4. Initiate authentication RPC
        let response = tokio::time::timeout(self.auth_timeout, sasl_client.authenticate(stream))
            .await
            .map_err(|_| Error::Internal {
                message: format!(
                    "SASL authentication timed out ({}ms)",
                    self.auth_timeout.as_millis()
                ),
                source: None,
            })?
            .map_err(|status| Error::GrpcError {
                message: format!("SASL authentication RPC failed: {}", status),
                source: status,
            })?;

        let mut response_stream = response.into_inner();

        // 5. Process server responses
        let auth_result = tokio::time::timeout(self.auth_timeout, async {
            while let Some(server_msg) =
                response_stream
                    .message()
                    .await
                    .map_err(|status| Error::GrpcError {
                        message: format!("SASL authentication response error: {}", status),
                        source: status,
                    })?
            {
                match sasl_handler.handle_message(&server_msg)? {
                    Some(client_response) => {
                        // Continue handshake, send response
                        tx.send(client_response)
                            .await
                            .map_err(|_| Error::Internal {
                                message: "failed to send SASL response message".to_string(),
                                source: None,
                            })?;
                    }
                    None => {
                        // Authentication succeeded
                        debug!(
                            channel_id = %channel_id,
                            "SASL PLAIN authentication succeeded"
                        );
                        return Ok::<(), Error>(());
                    }
                }
            }

            Err(Error::Internal {
                message: "SASL authentication stream closed unexpectedly without receiving SUCCESS"
                    .to_string(),
                source: None,
            })
        })
        .await
        .map_err(|_| Error::Internal {
            message: format!(
                "timed out waiting for SASL authentication response ({}ms)",
                self.auth_timeout.as_millis()
            ),
            source: None,
        })?;

        auth_result?;

        // 6. Authentication succeeded, create channel with channel-id interceptor.
        //    IMPORTANT: Keep the SASL stream alive! The server uses the bidirectional
        //    stream as a long-poll on authentication state. If the client closes the
        //    stream (by dropping tx), the server calls onCompleted() → unregisterChannel(),
        //    which invalidates the channel-id and causes subsequent RPCs to fail with
        //    "Channel is not authenticated".
        //    See Java's AuthenticatedChannelClientDriver which also keeps the stream open.
        let interceptor = ChannelIdInterceptor::new(channel_id.clone());
        Ok(AuthenticatedChannel {
            channel: InterceptedService::new(channel, interceptor),
            channel_id,
            _sasl_guard: Some(SaslStreamGuard {
                _tx: tx,
                _response_stream: response_stream,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::service::Interceptor;

    #[test]
    fn test_auth_type_from_str() {
        assert_eq!("nosasl".parse::<AuthType>().unwrap(), AuthType::NoSasl);
        assert_eq!("NOSASL".parse::<AuthType>().unwrap(), AuthType::NoSasl);
        assert_eq!("no_sasl".parse::<AuthType>().unwrap(), AuthType::NoSasl);
        assert_eq!("simple".parse::<AuthType>().unwrap(), AuthType::Simple);
        assert_eq!("SIMPLE".parse::<AuthType>().unwrap(), AuthType::Simple);
    }

    #[test]
    fn test_auth_type_from_str_unsupported() {
        assert!("custom".parse::<AuthType>().is_err());
        assert!("kerberos".parse::<AuthType>().is_err());
        assert!("invalid".parse::<AuthType>().is_err());
    }

    #[test]
    fn test_auth_type_default() {
        assert_eq!(AuthType::default(), AuthType::Simple);
    }

    #[test]
    fn test_auth_type_display() {
        assert_eq!(AuthType::NoSasl.to_string(), "nosasl");
        assert_eq!(AuthType::Simple.to_string(), "simple");
    }

    #[test]
    fn test_channel_id_interceptor() {
        let mut interceptor = ChannelIdInterceptor::new("test-id-123".to_string());
        let request = Request::new(());
        let result = interceptor.call(request).unwrap();
        let channel_id = result
            .metadata()
            .get(CHANNEL_ID_METADATA_KEY)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(channel_id, "test-id-123");
    }
}
