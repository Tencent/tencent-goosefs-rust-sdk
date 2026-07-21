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

//! Low-level block streaming read example.
//!
//! Demonstrates the low-level block read pipeline:
//! `MasterClient::get_status()` → `WorkerRouter` → `GrpcBlockReader`
//!
//! This example reads a file block-by-block using the underlying gRPC streaming API,
//! which gives full control over flow-control ACK and chunk-level processing.
//!
//! ⚠️ Prerequisite: run `highlevel_file_rw` first to create a file with actual data blocks:
//!   cargo run --example highlevel_file_rw
//!
//! Usage:
//!   cargo run --example lowlevel_block_read

use goosefs_sdk::block::WorkerRouter;
use goosefs_sdk::client::{MasterClient, WorkerClient, WorkerManagerClient};
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::GrpcBlockReader;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Goosefs Low-level Block Streaming Read Demo");
    println!("==========================");

    // Connect to Goosefs Master
    println!("\n1. Connecting to Goosefs Master...");
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let master = MasterClient::connect(&config).await?;
    println!("Connected!");

    // Check if the test file exists
    // ⚠️ The file must have been written with actual data blocks via a Worker
    // (e.g. /e2e-test/hello.txt created by the highlevel_file_rw example)
    // Files created by lowlevel_create_file only have metadata, no data blocks — reading will fail/hang
    println!("\n2. Checking test file...");
    let test_file_path = "/e2e-test/hello.txt";

    let file_info = match master.get_status(test_file_path).await {
        Ok(info) => info,
        Err(_) => {
            println!(
                "Test file {} does not exist. Please run the highlevel_file_rw example first.",
                test_file_path
            );
            println!("Run: cargo run --example highlevel_file_rw");
            return Ok(());
        }
    };

    println!("File info:");
    println!("  Path: {:?}", file_info.path);
    println!("  Length: {:?} bytes", file_info.length);
    println!("  Block size: {:?}", file_info.block_size_bytes);
    println!("  File ID: {:?}", file_info.file_id);
    println!("  Block IDs: {:?}", file_info.block_ids);
    println!("  File block info: {:?}", file_info.file_block_infos);

    // Discover workers and build router
    println!("\n3. Discovering workers and building router...");
    let wm = WorkerManagerClient::connect(&config).await?;
    let workers = wm.get_worker_info_list().await?;
    println!("Discovered {} workers", workers.len());

    let router = WorkerRouter::new();
    router.update_workers(workers).await;

    // Map file range to block-level read plan
    println!("\n4. Mapping file range to block-level read plan...");
    let file_length = file_info.length.unwrap_or(0) as u64;

    // Use the actual block ID from file info
    let block_id = if let Some(first_block) = file_info.file_block_infos.first() {
        if let Some(block_info) = &first_block.block_info {
            block_info.block_id.unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    println!("Using block ID: {}", block_id);
    println!("File length: {} bytes", file_length);

    // Stream-read blocks
    println!("\n5. Streaming block read...");
    let mut total_bytes_read = 0;

    if block_id > 0 {
        println!("\nReading block (ID: {})...", block_id);

        // Select worker
        let worker_info = match router.select_worker(block_id).await {
            Ok(info) => info,
            Err(e) => {
                println!("Failed to select worker: {:?}", e);
                return Ok(());
            }
        };

        let addr = worker_info.address.as_ref().unwrap();
        let worker_addr = format!(
            "{}:{}",
            addr.host.as_deref().unwrap_or("127.0.0.1"),
            addr.rpc_port.unwrap_or(9203)
        );

        println!("  Connecting to worker: {}", worker_addr);

        // Connect to worker
        let worker = match WorkerClient::connect(&worker_addr, &config).await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to connect to worker: {:?}", e);
                return Ok(());
            }
        };

        // Create block reader
        let mut reader = match GrpcBlockReader::open(
            &worker,
            block_id,
            0, // read from the beginning of the block
            file_length as i64,
            config.chunk_size as i64,
            None, // no UFS options needed for cache reads
        )
        .await
        {
            Ok(reader) => reader,
            Err(e) => {
                println!("Failed to create block reader: {:?}", e);
                return Ok(());
            }
        };

        // Read block data
        let data = match reader.read_all().await {
            Ok(data) => data,
            Err(e) => {
                println!("Failed to read block data: {:?}", e);
                return Ok(());
            }
        };

        total_bytes_read += data.len();

        println!(
            "  Read {} bytes (complete: {})",
            data.len(),
            reader.is_complete()
        );

        // Display content (if it's a text file)
        if let Ok(content) = String::from_utf8(data.to_vec()) {
            println!(
                "  Content preview: {:?}",
                if content.len() > 50 {
                    format!("{}...", &content[..50])
                } else {
                    content
                }
            );
        }
    } else {
        println!("Invalid block ID, cannot read");
    }

    println!("\n6. Read complete");
    println!("Total bytes read: {} bytes", total_bytes_read);
    println!("Total file length: {} bytes", file_length);

    if total_bytes_read == file_length as usize {
        println!("✅ Successfully read the entire file!");
    } else {
        println!("⚠️  Bytes read does not match file length");
    }

    Ok(())
}
