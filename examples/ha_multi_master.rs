//! Example: GooseFS HA (High Availability) multi-master configuration.
//!
//! Demonstrates how to configure the Rust client with multiple Master
//! addresses for automatic Primary discovery and failover.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example ha_multi_master -- <master1:port> <master2:port> [master3:port ...]
//! ```
//!
//! Or with a single master (falls back to `SingleMasterInquireClient`):
//! ```bash
//! cargo run --example ha_multi_master -- 10.0.0.1:9200
//! ```

use goosefs_client::client::MasterClient;
use goosefs_client::config::GooseFsConfig;
use std::time::Duration;

#[tokio::main]
async fn main() -> goosefs_client::error::Result<()> {
    // Parse master addresses from command-line arguments.
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("Usage: ha_multi_master <master1:port> [master2:port] [master3:port ...]");
        eprintln!();
        eprintln!("Example:");
        eprintln!("  Single master:   ha_multi_master 10.0.0.1:9200");
        eprintln!("  HA (3 masters):  ha_multi_master 10.0.0.1:9200 10.0.0.2:9200 10.0.0.3:9200");
        std::process::exit(1);
    }

    // Create config — single or HA depending on number of addresses.
    let config = if args.len() == 1 {
        println!("▸ Single-master mode: {}", args[0]);
        GooseFsConfig::new(&args[0])
    } else {
        println!(
            "▸ Multi-master mode with {} masters: {:?}",
            args.len(),
            args
        );
        let mut cfg = GooseFsConfig::new_ha(args.clone());
        // Tune timeouts for faster Primary discovery on local networks.
        // polling_timeout: per-address connect+RPC deadline for each ping.
        cfg.master_polling_timeout = Duration::from_secs(3);
        // Limit total retry budget so we don't block for 2 minutes
        // when no Primary is reachable.
        cfg.master_inquire_retry_max_duration = Duration::from_secs(15);
        cfg.master_inquire_initial_sleep = Duration::from_millis(100);
        cfg.master_inquire_max_sleep = Duration::from_secs(2);
        cfg
    };

    println!("  is_multi_master = {}", config.is_multi_master());
    println!("  master_addresses = {:?}", config.master_addresses());
    println!(
        "  master_polling_timeout = {:?}",
        config.master_polling_timeout
    );

    // Connect to the Master — this will automatically discover the Primary
    // in HA mode via PollingMasterInquireClient.
    println!("\n▸ Connecting to GooseFS Master...");
    let master = MasterClient::connect(&config).await?;
    println!("  ✓ Connected successfully!");

    // Try listing the root directory to verify connectivity.
    println!("\n▸ Listing root directory /...");
    match master.list_status("/", false).await {
        Ok(entries) => {
            println!("  ✓ Found {} entries:", entries.len());
            for entry in entries.iter().take(10) {
                let name = entry.path.as_deref().unwrap_or("<unknown>");
                let is_dir = entry.folder.unwrap_or(false);
                let size = entry.length.unwrap_or(0);
                let kind = if is_dir { "DIR " } else { "FILE" };
                println!("    {} {} ({} bytes)", kind, name, size);
            }
            if entries.len() > 10 {
                println!("    ... and {} more", entries.len() - 10);
            }
        }
        Err(e) => {
            println!("  ✗ Error: {}", e);
        }
    }

    println!("\n▸ Done!");
    Ok(())
}
