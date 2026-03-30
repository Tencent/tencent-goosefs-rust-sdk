//! Authentication module — GooseFS gRPC channel authentication support.
//!
//! The GooseFS server supports multiple authentication methods. After establishing
//! a gRPC channel, the client must complete a SASL handshake via the
//! `SaslAuthenticationService.authenticate` bidirectional streaming RPC.
//!
//! ## Currently Supported Authentication Types
//!
//! | Type | Description |
//! |------|-------------|
//! | `NOSASL` | No authentication; use the gRPC channel directly without SASL handshake |
//! | `SIMPLE` | Simple authentication; transmit username via PLAIN SASL; server does not verify password |
//!
//! ## TODO: Future Support
//!
//! - `CUSTOM` — Custom authentication (server-side custom AuthenticationProvider)
//! - `KERBEROS` — Kerberos GSSAPI authentication
//! - `DELEGATION_TOKEN` — Delegation token authentication (auto-downgrade under Kerberos mode)
//! - `CAPABILITY_TOKEN` — Capability token authentication (Client→Worker under Kerberos mode)

mod authenticator;
mod sasl_client;

pub use authenticator::{
    AuthType, AuthenticatedChannel, ChannelAuthenticator, ChannelIdInterceptor,
};
pub use sasl_client::SaslClientHandler;
