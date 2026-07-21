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

//! Example: Goosefs HA (High Availability) multi-master configuration.
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

use goosefs_sdk::client::MasterClient;
use goosefs_sdk::config::GoosefsConfig;
use std::time::Duration;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
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

    // Create config — automatically selects single or multi-master mode.
    let mut config = GoosefsConfig::from_addresses(args.clone());

    // Tune timeouts for faster Primary discovery on local networks.
    if config.is_multi_master() {
        config.master_polling_timeout = Duration::from_secs(3);
        config.master_inquire_retry_max_duration = Duration::from_secs(15);
        config.master_inquire_initial_sleep = Duration::from_millis(100);
        config.master_inquire_max_sleep = Duration::from_secs(2);
    }

    let mode = if config.is_multi_master() {
        format!("Multi-master ({} masters)", args.len())
    } else {
        format!("Single-master: {}", args[0])
    };
    println!("▸ {}", mode);

    println!("  is_multi_master = {}", config.is_multi_master());
    println!("  master_addresses = {:?}", config.master_addresses());
    println!(
        "  master_polling_timeout = {:?}",
        config.master_polling_timeout
    );

    // Connect to the Master — this will automatically discover the Primary
    // in HA mode via PollingMasterInquireClient.
    println!("\n▸ Connecting to Goosefs Master...");
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
