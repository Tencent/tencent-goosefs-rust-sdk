//! High-level file write and read example using `GoosefsFileWriter` / `GoosefsFileReader`.
//!
//! This example demonstrates the **recommended** high-level API:
//! 1. Create a `FileSystemContext` once (the only call that does TCP+SASL)
//! 2. One-shot write via `GoosefsFileWriter::write_file_with_context()`
//! 3. Full-file read via `GoosefsFileReader::read_file_with_context()`
//! 4. Range read via `GoosefsFileReader::read_range_with_context()`
//! 5. Streaming block-by-block read via `GoosefsFileReader::open_with_context()` + `read_next_block()`
//! 6. Builder-pattern multi-chunk write via `GoosefsFileWriter::create_with_context()` + `write()` + `close()`
//! 7. Write with `CACHE_THROUGH` mode for durable persistence
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

use std::sync::Arc;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use goosefs_sdk::WritePType;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Goosefs End-to-End File Read/Write Demo (High-level API)");
    println!("========================================================");

    // ── Step 0: Build FileSystemContext (the ONLY network I/O) ───
    println!("\n0. Creating FileSystemContext (one-time TCP+SASL handshake)...");
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    println!("  ✅ Context created — Master + WorkerManager connected");

    // Reuse the master from context for cleanup / metadata queries.
    let master = ctx.acquire_master();

    // ── Cleanup ──────────────────────────────────────────────────
    println!("\n   Cleaning up existing test files...");
    match master.delete("/e2e-test/hello.txt", false).await {
        Ok(_) => println!("   Deleted old file"),
        Err(_) => println!("   Old file does not exist, skipping"),
    }
    match master.create_directory("/e2e-test", true).await {
        Ok(_) => println!("   Directory /e2e-test created"),
        Err(_) => println!("   Directory already exists, skipping"),
    }

    // ── Step 1: One-shot write ───────────────────────────────────
    println!("\n1. Writing file /e2e-test/hello.txt ...");
    let content = "Hello, Goosefs! This file was written via the high-level API.\n\
                   Line 2: Goosefs Rust Client end-to-end test.\n\
                   Line 3: Supports auto-chunking, consistent-hash routing, gRPC streaming write.\n\
                   Line 4: CompleteFile is called automatically after writing.";

    let bytes_written = GoosefsFileWriter::write_file_with_context(
        ctx.clone(),
        "/e2e-test/hello.txt",
        content.as_bytes(),
    )
    .await?;
    println!("  ✅ Write complete: {} bytes", bytes_written);

    // ── Step 2: Read the file back ───────────────────────────────
    println!("\n2. Reading file /e2e-test/hello.txt ...");
    let data =
        GoosefsFileReader::read_file_with_context(ctx.clone(), "/e2e-test/hello.txt").await?;
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
    let range_data =
        GoosefsFileReader::read_range_with_context(ctx.clone(), "/e2e-test/hello.txt", 0, 20)
            .await?;
    println!("  ✅ Range read complete: {} bytes", range_data.len());
    println!("  Content: {:?}", String::from_utf8_lossy(&range_data));

    // ── Step 4: Streaming read ───────────────────────────────────
    println!("\n4. Streaming block-by-block read...");
    let mut reader =
        GoosefsFileReader::open_with_context(ctx.clone(), "/e2e-test/hello.txt").await?;
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

    let mut writer =
        GoosefsFileWriter::create_with_context(ctx.clone(), "/e2e-test/multi.txt", None).await?;
    writer.write(b"First chunk of data. ").await?;
    writer.write(b"Second chunk of data. ").await?;
    writer.write(b"Third and final chunk.").await?;
    writer.close().await?;
    println!(
        "  ✅ Multi-chunk write complete: {} bytes",
        writer.bytes_written()
    );

    // Verify
    let multi_data =
        GoosefsFileReader::read_file_with_context(ctx.clone(), "/e2e-test/multi.txt").await?;
    println!("  Verify: {:?}", String::from_utf8_lossy(&multi_data));

    // ── Step 6: Write with CACHE_THROUGH mode ────────────────────
    println!("\n6. Writing with CACHE_THROUGH mode (cache + sync persist to UFS)...");
    match master.delete("/e2e-test/durable.txt", false).await {
        Ok(_) => {}
        Err(_) => {}
    }
    let durable_content = b"This data is written to cache AND persisted to UFS synchronously.";
    let durable_options = CreateFilePOptions {
        write_type: Some(WritePType::CacheThrough as i32),
        recursive: Some(true),
        ..Default::default()
    };
    let durable_bytes = GoosefsFileWriter::write_file_with_context_and_options(
        ctx.clone(),
        "/e2e-test/durable.txt",
        durable_content,
        Some(durable_options),
    )
    .await?;
    println!("  ✅ CACHE_THROUGH write complete: {} bytes", durable_bytes);

    // Verify persistence status
    let durable_info = master.get_status("/e2e-test/durable.txt").await?;
    println!(
        "  Persistence status: persisted={:?}",
        durable_info.persisted.unwrap_or(false)
    );

    // ── Cleanup: close context ───────────────────────────────────
    ctx.close().await?;

    println!("\n========================================================");
    println!("✅ High-level API test complete!");
    Ok(())
}
