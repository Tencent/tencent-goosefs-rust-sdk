//! Authentication demo — demonstrates GooseFS SASL authentication.
//!
//! This example shows how to:
//! 1. Use `ChannelAuthenticator` directly (low-level API) for both NOSASL and SIMPLE modes
//! 2. Use `MasterClient::connect` with different auth configurations (high-level API)
//! 3. Verify that authenticated channels work by performing actual RPCs
//!
//! Prerequisites:
//!   A running GooseFS Master on 127.0.0.1:9200 (default).
//!
//! Usage:
//!   cargo run --example auth_demo
//!
//! To test with a custom master address:
//!   GOOSEFS_MASTER_ADDR=10.0.0.1:9200 cargo run --example auth_demo

use std::time::Duration;

use goosefs_client::auth::{AuthType, ChannelAuthenticator};
use goosefs_client::client::MasterClient;
use goosefs_client::config::GooseFsConfig;
use goosefs_client::error::Result;
use tonic::transport::Channel;

/// Resolve the master address from environment or use default.
fn master_addr() -> String {
    std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
}

/// Build a raw (unauthenticated) gRPC channel to the master.
async fn build_raw_channel(addr: &str) -> Result<Channel> {
    let endpoint = Channel::from_shared(format!("http://{}", addr))
        .map_err(|e| goosefs_client::error::Error::ConfigError {
            message: format!("invalid endpoint: {}", e),
        })?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30));
    let channel = endpoint.connect().await?;
    Ok(channel)
}

/// Demo 1: Low-level NOSASL authentication.
///
/// In NOSASL mode, no SASL handshake is performed. The channel is wrapped
/// with a channel-id interceptor for API consistency, but the server does
/// not require any credentials.
async fn demo_nosasl_low_level(addr: &str) -> Result<()> {
    println!("  Building raw gRPC channel to {}...", addr);
    let channel = build_raw_channel(addr).await?;

    let authenticator = ChannelAuthenticator::new(
        AuthType::NoSasl,
        "testuser".to_string(),
        None, // no impersonation
    );

    println!("  Authenticating with NOSASL mode...");
    let auth_channel = authenticator.authenticate(channel).await?;

    println!("  ✅ NOSASL authentication succeeded!");
    println!("     channel-id: {}", auth_channel.channel_id);
    println!("     (No SASL handshake performed; channel-id is for API consistency)");

    Ok(())
}

/// Demo 2: Low-level SIMPLE authentication.
///
/// In SIMPLE mode, the client performs a PLAIN SASL handshake with the server:
/// 1. Client sends: SaslMessage(CHALLENGE, PLAIN initial response, clientId, SIMPLE)
/// 2. Server replies: SaslMessage(SUCCESS)
/// 3. All subsequent RPCs carry the channel-id in metadata.
async fn demo_simple_low_level(addr: &str) -> Result<()> {
    println!("  Building raw gRPC channel to {}...", addr);
    let channel = build_raw_channel(addr).await?;

    let username = whoami();
    let authenticator = ChannelAuthenticator::new(
        AuthType::Simple,
        username.clone(),
        None, // no impersonation
    )
    .with_auth_timeout(Duration::from_secs(10));

    println!(
        "  Authenticating with SIMPLE mode (username: {})...",
        username
    );
    let auth_channel = authenticator.authenticate(channel).await?;

    println!("  ✅ SIMPLE (PLAIN SASL) authentication succeeded!");
    println!("     channel-id: {}", auth_channel.channel_id);
    println!("     username:   {}", username);

    Ok(())
}

/// Demo 3: High-level MasterClient with NOSASL config.
///
/// `MasterClient::connect` handles authentication internally based on
/// the `GooseFsConfig.auth_type` setting.
async fn demo_nosasl_master_client(addr: &str) -> Result<()> {
    let config = GooseFsConfig::new(addr).with_auth_type(AuthType::NoSasl);

    println!(
        "  Connecting MasterClient with auth_type={}...",
        config.auth_type
    );
    let master = MasterClient::connect(&config).await?;

    // Verify the connection works by listing the root directory
    println!("  Verifying connection: listing root directory...");
    let entries = master.list_status("/", false).await?;
    println!("  ✅ MasterClient (NOSASL) connected and working!");
    println!("     Root directory has {} entries", entries.len());
    for entry in entries.iter().take(5) {
        println!("       - {}", entry.path.as_deref().unwrap_or("<unknown>"));
    }
    if entries.len() > 5 {
        println!("       ... and {} more", entries.len() - 5);
    }

    Ok(())
}

/// Demo 4: High-level MasterClient with SIMPLE config.
///
/// This is the default and most common authentication mode.
async fn demo_simple_master_client(addr: &str) -> Result<()> {
    let username = whoami();
    let config = GooseFsConfig::new(addr)
        .with_auth_type(AuthType::Simple)
        .with_auth_username(&username);

    println!(
        "  Connecting MasterClient with auth_type={}, username={}...",
        config.auth_type, config.auth_username
    );
    let master = MasterClient::connect(&config).await?;

    // Verify the connection works by getting root status
    println!("  Verifying connection: getting root status...");
    let root_info = master.get_status("/").await?;
    println!("  ✅ MasterClient (SIMPLE) connected and working!");
    println!("     Root path:  {:?}", root_info.path);
    println!("     Is folder:  {:?}", root_info.folder);
    println!("     Owner:      {:?}", root_info.owner);

    Ok(())
}

/// Demo 5: Default config (SIMPLE mode is the default).
///
/// When no auth_type is explicitly set, `GooseFsConfig::default()` uses
/// SIMPLE mode with the current OS username.
async fn demo_default_config(addr: &str) -> Result<()> {
    let config = GooseFsConfig::new(addr);

    println!(
        "  Default config: auth_type={}, username={}",
        config.auth_type, config.auth_username
    );
    let master = MasterClient::connect(&config).await?;

    println!("  Verifying connection: getting root status...");
    let root_info = master.get_status("/").await?;
    println!("  ✅ Default config works!");
    println!("     Root owner: {:?}", root_info.owner);

    Ok(())
}

/// Get the current OS username.
fn whoami() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("GooseFS Authentication Demo");
    println!("===========================\n");

    let addr = master_addr();
    println!("Master address: {}\n", addr);

    // --- Low-level ChannelAuthenticator API ---

    println!("1. Low-level NOSASL authentication");
    println!("-----------------------------------");
    match demo_nosasl_low_level(&addr).await {
        Ok(()) => {}
        Err(e) => println!("  ❌ Failed: {:?}", e),
    }

    println!("\n2. Low-level SIMPLE (PLAIN SASL) authentication");
    println!("------------------------------------------------");
    match demo_simple_low_level(&addr).await {
        Ok(()) => {}
        Err(e) => println!("  ❌ Failed: {:?}", e),
    }

    // --- High-level MasterClient API ---

    println!("\n3. MasterClient with NOSASL config");
    println!("-----------------------------------");
    match demo_nosasl_master_client(&addr).await {
        Ok(()) => {}
        Err(e) => println!("  ❌ Failed: {:?}", e),
    }

    println!("\n4. MasterClient with SIMPLE config");
    println!("-----------------------------------");
    match demo_simple_master_client(&addr).await {
        Ok(()) => {}
        Err(e) => println!("  ❌ Failed: {:?}", e),
    }

    println!("\n5. MasterClient with default config (SIMPLE)");
    println!("----------------------------------------------");
    match demo_default_config(&addr).await {
        Ok(()) => {}
        Err(e) => println!("  ❌ Failed: {:?}", e),
    }

    println!("\n===========================");
    println!("Authentication demo complete!");

    Ok(())
}
