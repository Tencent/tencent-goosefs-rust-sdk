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

//! File and directory metadata CRUD example.
//!
//! Demonstrates `MasterClient` metadata operations:
//! - `create_directory` — create directories (with recursive option)
//! - `create_file` / `complete_file` — create file entries
//! - `get_status` — get file/directory info (size, persistence state, etc.)
//! - `list_status` — list directory contents
//! - `rename` — move/rename files and directories
//! - `delete` — delete files and directories (with recursive option)
//!
//! WriteType controls where data is physically persisted:
//!
//! | WriteType        | Worker cache | UFS (COS/S3/HDFS) |
//! |------------------|--------------|--------------------|
//! | MUST_CACHE       | ✅ (default)  | ❌                 |
//! | CACHE_THROUGH    | ✅            | ✅ (sync on close)  |
//! | THROUGH          | ❌            | ✅ (direct)         |
//! | ASYNC_THROUGH    | ✅            | ✅ (async after close) |
//!
//! Usage:
//!   cargo run --example metadata_crud

use std::sync::Arc;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to Goosefs via FileSystemContext (loads goosefs-site.properties automatically)
    println!("Connecting to Goosefs Master...");
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    let master = ctx.acquire_master();
    println!("Connected!");

    // Clean up existing test directory
    println!("\n1. Cleaning up existing test directory...");
    match master.delete("/test-demo", true).await {
        Ok(_) => println!("Deleted existing test directory"),
        Err(e) => println!(
            "Failed to delete test directory or it does not exist: {:?}",
            e
        ),
    }

    // Create test directory
    println!("\n2. Creating test directory...");
    master.create_directory("/test-demo", true).await?;
    println!("Directory /test-demo created");

    // Create test file
    println!("\n2. Creating test file...");
    let mut create_options = CreateFilePOptions::default();
    create_options.block_size_bytes = Some(64 * 1024 * 1024); // 64MB block size
    master
        .create_file("/test-demo/hello.txt", create_options)
        .await?;
    println!("File /test-demo/hello.txt created");

    // Write file content
    println!("\n3. Writing file content...");
    let content = "Hello, Goosefs! This is a test file.\nWelcome to Goosefs Rust Client!\nTimestamp: 2026-03-26 18:06:32";
    // File write logic needs to be implemented; the current API may not support direct writes
    println!("Content: {}", content);
    println!("Note: the current API may require writing via the Worker client");

    // Mark file as complete (simulating write completion)
    println!("\n4. Marking file as complete...");
    master
        .complete_file("/test-demo/hello.txt", Some(content.len() as i64), None)
        .await?;
    println!(
        "File marked complete, content length: {} bytes",
        content.len()
    );

    // Get file status
    println!("\n4. Getting file status...");
    let file_info = master.get_status("/test-demo/hello.txt").await?;
    println!("File info: {:?}", file_info);
    println!("Path: {:?}", file_info.path);
    println!("Length: {:?} bytes", file_info.length);
    println!("Is directory: {:?}", file_info.folder);

    // List directory contents
    println!("\n5. Listing directory contents...");
    let entries = master.list_status("/test-demo", false).await?;
    println!("Directory /test-demo contains {} entries:", entries.len());
    for entry in &entries {
        println!(
            "  - {} ({}, {} bytes)",
            entry.path.as_deref().unwrap_or("unknown"),
            if entry.folder.unwrap_or(false) {
                "dir"
            } else {
                "file"
            },
            entry.length.unwrap_or(0)
        );
    }

    // Delete existing world.txt file
    println!("\n6. Deleting existing world.txt...");
    match master.delete("/test-demo/world.txt", false).await {
        Ok(_) => println!("Deleted existing world.txt"),
        Err(e) => println!("Failed to delete world.txt or file does not exist: {:?}", e),
    }

    // Rename file
    println!("\n7. Renaming file...");
    master
        .rename("/test-demo/hello.txt", "/test-demo/world.txt")
        .await?;
    println!("File renamed to /test-demo/world.txt");

    // Verify rename
    println!("\n7. Verifying rename...");
    let entries = master.list_status("/test-demo", false).await?;
    for entry in &entries {
        println!("  - {}", entry.path.as_deref().unwrap_or("unknown"));
    }

    // // Delete the file
    // println!("\n8. Deleting the file...");
    // master.delete("/test-demo/world.txt", false).await?;
    // println!("File deleted successfully");
    //
    // // Delete the directory
    // println!("\n9. Deleting the directory...");
    // master.delete("/test-demo", true).await?;
    // println!("Directory deleted successfully");

    println!("\n✅ All file operation tests complete!");

    ctx.close().await?;

    Ok(())
}
