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

//! SASL client handler — handles sending and receiving SASL handshake messages.
//!
//! Corresponds to Java's `AbstractSaslClientHandler` + `SaslClientHandlerPlain`.
//!
//! ## PLAIN SASL Mechanism
//!
//! The PLAIN mechanism initial response format is: `\0<username>\0<password>`
//! (RFC 4616: `[authzid] NUL authcid NUL passwd`)
//!
//! In SIMPLE mode:
//! - `authzid` (authorization identity) = empty (or impersonation user)
//! - `authcid` (authentication identity) = username
//! - `passwd` = "noPassword" (server does not verify)

use crate::error::Result;
use crate::proto::grpc::sasl::{ChannelAuthenticationScheme, SaslMessage, SaslMessageType};

/// SASL client handler trait.
///
/// Corresponds to Java's `SaslClientHandler` interface.
pub trait SaslClientHandler: Send + Sync {
    /// Generate the initial SASL message (sent to the server to start the handshake).
    ///
    /// Corresponds to Java's `handleMessage(null)` — generates the initial message when `None` is passed.
    fn initial_message(&self, client_id: &str, channel_ref: &str) -> Result<SaslMessage>;

    /// Process a SASL message from the server and generate a client response.
    ///
    /// Returns `Ok(Some(msg))` if the handshake needs to continue,
    /// returns `Ok(None)` if authentication succeeded (received SUCCESS message).
    fn handle_message(&self, message: &SaslMessage) -> Result<Option<SaslMessage>>;

    /// Get the authentication scheme.
    fn auth_scheme(&self) -> ChannelAuthenticationScheme;
}

/// PLAIN SASL client handler — used for SIMPLE and CUSTOM authentication.
///
/// Corresponds to Java's `SaslClientHandlerPlain`.
///
/// The PLAIN mechanism requires only one handshake round:
/// 1. Client sends initial message (containing PLAIN-encoded username/password)
/// 2. Server verifies and returns SUCCESS
pub struct PlainSaslClientHandler {
    /// Authentication scheme (SIMPLE or CUSTOM).
    auth_scheme: ChannelAuthenticationScheme,
    /// PLAIN SASL initial response: `\0<username>\0<password>`.
    initial_response: Vec<u8>,
}

impl PlainSaslClientHandler {
    /// Create a PLAIN SASL handler for SIMPLE mode.
    ///
    /// # Arguments
    /// - `username`: login username (corresponds to the `User` principal in Java's `Subject`)
    /// - `password`: password ("noPassword" in SIMPLE mode)
    /// - `impersonation_user`: optional impersonation user (corresponds to SASL's authzid)
    pub fn new_simple(username: &str, password: &str, impersonation_user: Option<&str>) -> Self {
        Self::new(
            ChannelAuthenticationScheme::Simple,
            username,
            password,
            impersonation_user,
        )
    }

    /// Create a PLAIN SASL handler with the specified authentication scheme.
    fn new(
        auth_scheme: ChannelAuthenticationScheme,
        username: &str,
        password: &str,
        impersonation_user: Option<&str>,
    ) -> Self {
        // PLAIN SASL initial response format (RFC 4616):
        // message = [authzid] NUL authcid NUL passwd
        //
        // In Java's Sasl.createSaslClient with the PLAIN mechanism,
        // the authorizationId parameter corresponds to impersonation_user,
        // while authcid/passwd are provided via the CallbackHandler.
        let authzid = impersonation_user.unwrap_or("");
        let initial_response = format!("{}\0{}\0{}", authzid, username, password).into_bytes();

        Self {
            auth_scheme,
            initial_response,
        }
    }
}

impl SaslClientHandler for PlainSaslClientHandler {
    fn initial_message(&self, client_id: &str, channel_ref: &str) -> Result<SaslMessage> {
        Ok(SaslMessage {
            message_type: Some(SaslMessageType::Challenge as i32),
            message: Some(self.initial_response.clone()),
            client_id: Some(client_id.to_string()),
            authentication_scheme: Some(self.auth_scheme as i32),
            channel_ref: Some(channel_ref.to_string()),
        })
    }

    fn handle_message(&self, message: &SaslMessage) -> Result<Option<SaslMessage>> {
        let msg_type = message
            .message_type
            .and_then(|v| SaslMessageType::try_from(v).ok())
            .unwrap_or(SaslMessageType::Challenge);

        match msg_type {
            SaslMessageType::Challenge => {
                // Server sent a CHALLENGE; reply with our PLAIN response.
                // (Normally the PLAIN mechanism has no additional challenges,
                // but we handle it for robustness.)
                Ok(Some(SaslMessage {
                    message_type: Some(SaslMessageType::Challenge as i32),
                    message: Some(self.initial_response.clone()),
                    client_id: None,
                    authentication_scheme: None,
                    channel_ref: None,
                }))
            }
            SaslMessageType::Success => {
                // Authentication succeeded
                Ok(None)
            }
        }
    }

    fn auth_scheme(&self) -> ChannelAuthenticationScheme {
        self.auth_scheme
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_sasl_initial_response_format() {
        let handler = PlainSaslClientHandler::new_simple("testuser", "noPassword", None);
        // Format: \0username\0password (authzid is empty)
        assert_eq!(handler.initial_response, b"\0testuser\0noPassword");
    }

    #[test]
    fn test_plain_sasl_with_impersonation_user() {
        let handler =
            PlainSaslClientHandler::new_simple("testuser", "noPassword", Some("proxyuser"));
        // Format: authzid\0username\0password
        assert_eq!(handler.initial_response, b"proxyuser\0testuser\0noPassword");
    }

    #[test]
    fn test_plain_sasl_initial_message() {
        let handler = PlainSaslClientHandler::new_simple("testuser", "noPassword", None);
        let msg = handler
            .initial_message("test-client-id", "test-channel")
            .unwrap();

        assert_eq!(msg.message_type, Some(SaslMessageType::Challenge as i32));
        assert_eq!(msg.message, Some(b"\0testuser\0noPassword".to_vec()));
        assert_eq!(msg.client_id, Some("test-client-id".to_string()));
        assert_eq!(
            msg.authentication_scheme,
            Some(ChannelAuthenticationScheme::Simple as i32)
        );
        assert_eq!(msg.channel_ref, Some("test-channel".to_string()));
    }

    #[test]
    fn test_plain_sasl_handle_success() {
        let handler = PlainSaslClientHandler::new_simple("testuser", "noPassword", None);
        let server_msg = SaslMessage {
            message_type: Some(SaslMessageType::Success as i32),
            message: None,
            client_id: None,
            authentication_scheme: None,
            channel_ref: None,
        };
        let result = handler.handle_message(&server_msg).unwrap();
        assert!(
            result.is_none(),
            "SUCCESS message should return None indicating auth complete"
        );
    }

    #[test]
    fn test_plain_sasl_handle_challenge() {
        let handler = PlainSaslClientHandler::new_simple("testuser", "noPassword", None);
        let server_msg = SaslMessage {
            message_type: Some(SaslMessageType::Challenge as i32),
            message: Some(vec![]),
            client_id: None,
            authentication_scheme: None,
            channel_ref: None,
        };
        let result = handler.handle_message(&server_msg).unwrap();
        assert!(
            result.is_some(),
            "CHALLENGE message should return a response"
        );
    }
}
