//! Context-based file read/write example using `FileSystemContext`.
//!
//! This example demonstrates the **context-based** high-level API, which
//! reuses persistent Master/Worker connections via `FileSystemContext`:
//!
//! 1. Create a `FileSystemContext` once (the only call that does TCP+SASL)
//! 2. Write a file via `GooseFsFileWriter::create_with_context()`
//! 3. Read the file back via `GooseFsFileReader::open_with_context()`
//! 4. Range read via `GooseFsFileReader::open_range_with_context()`
//! 5. One-shot convenience: `read_file_with_context()` / `read_range_with_context()`
//! 6. Write with custom `CreateFilePOptions` (e.g. CACHE_THROUGH mode)
//!
//! # Context vs Config API comparison
//!
//! | Aspect           | Config API (legacy)                | Context API (recommended)              |
//! |------------------|------------------------------------|----------------------------------------|
//! | Write entry      | `create(&config, path)`            | `create_with_context(ctx, path, opts)` |
//! | Read entry       | `open(&config, path)`              | `open_with_context(ctx, path)`         |
//! | First parameter  | `&GooseFsConfig`                   | `Arc<FileSystemContext>`               |
//! | Write options    | Built internally from config       | Explicit `Option<CreateFilePOptions>`  |
//! | Connection cost  | New TCP+SASL per call              | Zero handshake (reused from context)   |
//!
//! Usage:
//!   cargo run --example context_file_rw

use std::sync::Arc;

use goosefs_sdk::config::GooseFsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GooseFsFileReader, GooseFsFileWriter};
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use goosefs_sdk::WritePType;

#[tokio::main]
async fn main() -> Result<()> {
    println!("GooseFS Context-Based File Read/Write Demo");
    println!("===========================================");

    // ── Step 0: Build FileSystemContext (the ONLY network I/O) ───
    //
    // This establishes persistent connections to Master + WorkerManager,
    // fetches the initial worker list, and starts a background refresh task.
    // All subsequent operations reuse these connections — zero TCP+SASL.
    println!("\n0. Creating FileSystemContext (one-time TCP+SASL handshake)...");
    let config = GooseFsConfig::new("127.0.0.1:9200");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    println!("  ✅ Context created — Master + WorkerManager connected");

    // We also need a MasterClient for cleanup operations.
    // Reuse the one from context (zero cost).
    let master = ctx.acquire_master();

    // ── Cleanup ──────────────────────────────────────────────────
    println!("\n   Cleaning up existing test files...");
    match master.delete("/ctx-test/hello.txt", false).await {
        Ok(_) => println!("   Deleted old /ctx-test/hello.txt"),
        Err(_) => println!("   /ctx-test/hello.txt does not exist, skipping"),
    }
    match master.delete("/ctx-test/custom.txt", false).await {
        Ok(_) => println!("   Deleted old /ctx-test/custom.txt"),
        Err(_) => println!("   /ctx-test/custom.txt does not exist, skipping"),
    }
    match master.create_directory("/ctx-test", true).await {
        Ok(_) => println!("   Directory /ctx-test created"),
        Err(_) => println!("   Directory /ctx-test already exists, skipping"),
    }

    // ── Step 1: Write a file via create_with_context (default options) ──
    //
    // `create_with_context` accepts `Option<CreateFilePOptions>`:
    //   - `None` → uses default options derived from config (block_size, write_type, etc.)
    //   - `Some(opts)` → uses the provided options (see Step 5 below)
    //
    // Compare with legacy API:
    //   Legacy:  GooseFsFileWriter::create(&config, path)
    //   Context: GooseFsFileWriter::create_with_context(ctx, path, None)
    println!("\n1. Writing file via create_with_context (default options)...");
    let content = "Hello from context-based API!\n\
                   Line 2: Zero TCP+SASL handshake for this write.\n\
                   Line 3: Connections reused from FileSystemContext.\n\
                   Line 4: This is the recommended API for production use.";

    let mut writer =
        GooseFsFileWriter::create_with_context(ctx.clone(), "/ctx-test/hello.txt", None).await?;
    writer.write(content.as_bytes()).await?;
    writer.close().await?;
    println!(
        "  ✅ Write complete: {} bytes (zero new connections)",
        writer.bytes_written()
    );

    // ── Step 2: Read the file back via open_with_context ─────────
    //
    // Compare with legacy API:
    //   Legacy:  GooseFsFileReader::open(&config, path)
    //   Context: GooseFsFileReader::open_with_context(ctx, path)
    println!("\n2. Reading file via open_with_context...");
    let mut reader =
        GooseFsFileReader::open_with_context(ctx.clone(), "/ctx-test/hello.txt").await?;
    println!(
        "  File length: {} bytes, blocks: {}",
        reader.file_length(),
        reader.block_count()
    );

    let data = reader.read_all().await?;
    let read_content = String::from_utf8_lossy(&data);
    println!("  ✅ Read complete: {} bytes", data.len());
    println!("  Content:\n  ---");
    for line in read_content.lines() {
        println!("  {}", line);
    }
    println!("  ---");

    // Verify content matches
    if read_content == content {
        println!("  ✅ Content verification passed!");
    } else {
        println!("  ❌ Content mismatch!");
    }

    // ── Step 3: Range read via open_range_with_context ───────────
    //
    // Compare with legacy API:
    //   Legacy:  GooseFsFileReader::open_range(&config, path, offset, length)
    //   Context: GooseFsFileReader::open_range_with_context(ctx, path, offset, length)
    println!("\n3. Range read via open_range_with_context (offset=0, length=29)...");
    let mut range_reader =
        GooseFsFileReader::open_range_with_context(ctx.clone(), "/ctx-test/hello.txt", 0, 29)
            .await?;
    let range_data = range_reader.read_all().await?;
    println!("  ✅ Range read: {} bytes", range_data.len());
    println!("  Content: {:?}", String::from_utf8_lossy(&range_data));

    // ── Step 4: One-shot convenience methods ─────────────────────
    //
    // These combine open + read_all in a single call:
    //   read_file_with_context(ctx, path)
    //   read_range_with_context(ctx, path, offset, length)
    println!("\n4. One-shot read_file_with_context...");
    let oneshot_data =
        GooseFsFileReader::read_file_with_context(ctx.clone(), "/ctx-test/hello.txt").await?;
    println!("  ✅ One-shot read: {} bytes", oneshot_data.len());

    println!("   One-shot read_range_with_context (offset=0, length=10)...");
    let oneshot_range =
        GooseFsFileReader::read_range_with_context(ctx.clone(), "/ctx-test/hello.txt", 0, 10)
            .await?;
    println!(
        "  ✅ One-shot range read: {:?}",
        String::from_utf8_lossy(&oneshot_range)
    );

    // ── Step 5: Streaming block-by-block read via context ────────
    println!("\n5. Streaming block-by-block read via context...");
    let mut stream_reader =
        GooseFsFileReader::open_with_context(ctx.clone(), "/ctx-test/hello.txt").await?;
    let mut block_idx = 0;
    while let Some(chunk) = stream_reader.read_next_block().await? {
        println!("  Block {}: {} bytes", block_idx, chunk.len());
        block_idx += 1;
    }
    println!(
        "  ✅ Streaming read complete: {} blocks, {} bytes",
        block_idx,
        stream_reader.bytes_read()
    );

    // ── Step 6: Write with custom CreateFilePOptions ─────────────
    //
    // The context API exposes `Option<CreateFilePOptions>` directly,
    // allowing fine-grained control over block size, write type, etc.
    //
    // Compare with legacy API:
    //   Legacy:  GooseFsFileWriter::create_with_options(&config, path, Some(opts))
    //   Context: GooseFsFileWriter::create_with_context(ctx, path, Some(opts))
    println!("\n6. Writing with custom CreateFilePOptions (CACHE_THROUGH)...");
    let custom_options = CreateFilePOptions {
        block_size_bytes: Some(32 * 1024 * 1024), // 32MB block size
        recursive: Some(true),
        write_type: Some(WritePType::CacheThrough as i32),
        ..Default::default()
    };

    let mut custom_writer = GooseFsFileWriter::create_with_context(
        ctx.clone(),
        "/ctx-test/custom.txt",
        Some(custom_options),
    )
    .await?;
    custom_writer
        .write(b"Data written with CACHE_THROUGH via context API.")
        .await?;
    custom_writer.close().await?;
    println!(
        "  ✅ Custom write complete: {} bytes",
        custom_writer.bytes_written()
    );

    // Verify the custom-written file
    let custom_data =
        GooseFsFileReader::read_file_with_context(ctx.clone(), "/ctx-test/custom.txt").await?;
    println!("  Verify: {:?}", String::from_utf8_lossy(&custom_data));

    // Check persistence status
    let custom_info = master.get_status("/ctx-test/custom.txt").await?;
    println!(
        "  Persistence status: persisted={:?}",
        custom_info.persisted.unwrap_or(false)
    );

    // ── Step 7: Multi-chunk write via context ────────────────────
    println!("\n7. Multi-chunk write via context...");
    match master.delete("/ctx-test/multi.txt", false).await {
        Ok(_) => {}
        Err(_) => {}
    }

    let mut multi_writer =
        GooseFsFileWriter::create_with_context(ctx.clone(), "/ctx-test/multi.txt", None).await?;
    multi_writer.write(b"Chunk 1: context-based. ").await?;
    multi_writer.write(b"Chunk 2: zero handshake. ").await?;
    multi_writer.write(b"Chunk 3: connection reuse.").await?;
    multi_writer.close().await?;
    println!(
        "  ✅ Multi-chunk write: {} bytes",
        multi_writer.bytes_written()
    );

    let multi_data =
        GooseFsFileReader::read_file_with_context(ctx.clone(), "/ctx-test/multi.txt").await?;
    println!("  Verify: {:?}", String::from_utf8_lossy(&multi_data));

    // ── Cleanup: close context ───────────────────────────────────
    println!("\n8. Closing FileSystemContext...");
    ctx.close().await?;
    println!("  ✅ Context closed — background tasks stopped");

    println!("\n===========================================");
    println!("✅ Context-based API demo complete!");
    println!("\nKey takeaway: FileSystemContext is created ONCE, then all");
    println!("read/write operations reuse its persistent connections.");
    Ok(())
}
