//! High-level file write and read example using `GooseFsFileWriter` / `GooseFsFileReader`.
//!
//! This example demonstrates the **recommended** high-level API:
//! 1. One-shot write via `GooseFsFileWriter::write_file()`
//! 2. Full-file read via `GooseFsFileReader::read_file()`
//! 3. Range read via `GooseFsFileReader::read_range()`
//! 4. Streaming block-by-block read via `GooseFsFileReader::open()` + `read_next_block()`
//! 5. Builder-pattern multi-chunk write via `GooseFsFileWriter::create()` + `write()` + `close()`
//! 6. Write with `CACHE_THROUGH` mode for durable persistence
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
//!   cargo run --example highlevel_file_rw

use goosefs_sdk::client::MasterClient;
use goosefs_sdk::config::GooseFsConfig;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GooseFsFileReader, GooseFsFileWriter};
use goosefs_sdk::WritePType;

#[tokio::main]
async fn main() -> Result<()> {
    println!("GooseFS End-to-End File Read/Write Demo (High-level API)");
    println!("========================================================");

    let config = GooseFsConfig::new("127.0.0.1:9200");

    // ── Step 0: Cleanup ──────────────────────────────────────────
    println!("\n0. Cleaning up existing test files...");
    let master = MasterClient::connect(&config).await?;
    match master.delete("/e2e-test/hello.txt", false).await {
        Ok(_) => println!("  Deleted old file"),
        Err(_) => println!("  Old file does not exist, skipping"),
    }
    match master.create_directory("/e2e-test", true).await {
        Ok(_) => println!("  Directory /e2e-test created"),
        Err(_) => println!("  Directory already exists, skipping"),
    }

    // ── Step 1: Write a file ─────────────────────────────────────
    println!("\n1. Writing file /e2e-test/hello.txt ...");
    let content = "Hello, GooseFS! This file was written via the high-level API.\n\
                   Line 2: GooseFS Rust Client end-to-end test.\n\
                   Line 3: Supports auto-chunking, consistent-hash routing, gRPC streaming write.\n\
                   Line 4: CompleteFile is called automatically after writing.";

    let bytes_written =
        GooseFsFileWriter::write_file(&config, "/e2e-test/hello.txt", content.as_bytes()).await?;
    println!("  ✅ Write complete: {} bytes", bytes_written);

    // ── Step 2: Read the file back ───────────────────────────────
    println!("\n2. Reading file /e2e-test/hello.txt ...");
    let data = GooseFsFileReader::read_file(&config, "/e2e-test/hello.txt").await?;
    let read_content = String::from_utf8_lossy(&data);
    println!("  ✅ Read complete: {} bytes", data.len());
    println!("  Content:\n  ---");
    for line in read_content.lines() {
        println!("  {}", line);
    }
    println!("  ---");

    // Verify content matches
    if read_content == content {
        println!("  ✅ Content verification passed: write and read match!");
    } else {
        println!("  ❌ Content mismatch!");
        println!(
            "  Written length: {}, Read length: {}",
            content.len(),
            data.len()
        );
    }

    // ── Step 3: Range read ───────────────────────────────────────
    println!("\n3. Range read (offset=0, length=20) ...");
    let range_data = GooseFsFileReader::read_range(&config, "/e2e-test/hello.txt", 0, 20).await?;
    println!("  ✅ Range read complete: {} bytes", range_data.len());
    println!("  Content: {:?}", String::from_utf8_lossy(&range_data));

    // ── Step 4: Streaming read ───────────────────────────────────
    println!("\n4. Streaming block-by-block read...");
    let mut reader = GooseFsFileReader::open(&config, "/e2e-test/hello.txt").await?;
    println!(
        "  File length: {} bytes, blocks: {}",
        reader.file_length(),
        reader.block_count()
    );

    let mut block_idx = 0;
    while let Some(chunk) = reader.read_next_block().await? {
        println!("  Block {}: {} bytes", block_idx, chunk.len());
        block_idx += 1;
    }
    println!(
        "  ✅ Streaming read complete: {} blocks, {} bytes",
        block_idx,
        reader.bytes_read()
    );

    // ── Step 5: Write with builder pattern ───────────────────────
    println!("\n5. Writing multi-chunk data with builder pattern...");
    match master.delete("/e2e-test/multi.txt", false).await {
        Ok(_) => {}
        Err(_) => {}
    }

    let mut writer = GooseFsFileWriter::create(&config, "/e2e-test/multi.txt").await?;
    writer.write(b"First chunk of data. ").await?;
    writer.write(b"Second chunk of data. ").await?;
    writer.write(b"Third and final chunk.").await?;
    writer.close().await?;
    println!(
        "  ✅ Multi-chunk write complete: {} bytes",
        writer.bytes_written()
    );

    // Verify
    let multi_data = GooseFsFileReader::read_file(&config, "/e2e-test/multi.txt").await?;
    println!("  Verify: {:?}", String::from_utf8_lossy(&multi_data));

    // ── Step 6: Write with CACHE_THROUGH mode ────────────────
    println!("\n6. Writing with CACHE_THROUGH mode (cache + sync persist to UFS)...");
    match master.delete("/e2e-test/durable.txt", false).await {
        Ok(_) => {}
        Err(_) => {}
    }
    let durable_config =
        GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::CacheThrough);
    let durable_content = b"This data is written to cache AND persisted to UFS synchronously.";
    let durable_bytes =
        GooseFsFileWriter::write_file(&durable_config, "/e2e-test/durable.txt", durable_content)
            .await?;
    println!("  ✅ CACHE_THROUGH write complete: {} bytes", durable_bytes);

    // Verify persistence status
    let durable_info = master.get_status("/e2e-test/durable.txt").await?;
    println!(
        "  Persistence status: persisted={:?}",
        durable_info.persisted.unwrap_or(false)
    );

    println!("\n========================================================");
    println!("✅ High-level API test complete!");
    Ok(())
}
