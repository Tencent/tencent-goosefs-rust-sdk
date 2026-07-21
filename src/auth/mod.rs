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

//! Authentication module — Goosefs gRPC channel authentication support.
//!
//! The Goosefs server supports multiple authentication methods. After establishing
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
    AuthType, AuthenticatedChannel, ChannelAuthenticator, ChannelIdInterceptor, SaslStreamGuard,
};
pub use sasl_client::SaslClientHandler;
