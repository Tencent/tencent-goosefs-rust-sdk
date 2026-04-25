//! Demonstrates the **4 write types** supported by GooseFS.
//!
//! Each write type controls where data is physically persisted:
//!
//! | WriteType        | Worker cache | UFS (COS/S3/HDFS) |
//! |------------------|--------------|--------------------|
//! | MUST_CACHE       | ✅ (default)  | ❌                 |
//! | CACHE_THROUGH    | ✅            | ✅ (sync on close)  |
//! | THROUGH          | ❌            | ✅ (direct)         |
//! | ASYNC_THROUGH    | ✅            | ✅ (async after close) |
//!
//! Usage:
//!   cargo run --example write_types

use std::sync::Arc;

use goosefs_sdk::config::GooseFsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GooseFsFileReader, GooseFsFileWriter};
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use goosefs_sdk::WritePType;

#[tokio::main]
async fn main() -> Result<()> {
    println!("GooseFS WriteType Demo");
    println!("=======================");

    let ctx: Arc<FileSystemContext> =
        FileSystemContext::connect(GooseFsConfig::new("127.0.0.1:9200")).await?;

    // Initialize: cleanup & create test directory
    let master = ctx.acquire_master();
    match master.delete("/write-type-demo", true).await {
        Ok(_) => println!("Cleaned up old test directory"),
        Err(_) => {}
    }
    master.create_directory("/write-type-demo", true).await?;
    println!("Test directory /write-type-demo created\n");

    // ────────────────────────────────────────────────────────
    // 1. MUST_CACHE — Write to Worker cache only, no UFS persistence
    // ────────────────────────────────────────────────────────
    println!("━━━ 1. MUST_CACHE (default) ━━━");
    println!("  Data is written to Worker cache only, not persisted to underlying storage.");
    {
        let opts = CreateFilePOptions {
            write_type: Some(WritePType::MustCache as i32),
            recursive: Some(true),
            ..Default::default()
        };
        let data = b"MUST_CACHE: data lives only in GooseFS cache.";
        let bytes = GooseFsFileWriter::write_file_with_context_and_options(
            ctx.clone(),
            "/write-type-demo/must_cache.txt",
            data,
            Some(opts),
        )
        .await?;
        println!("  ✅ Write complete: {} bytes", bytes);

        // Read back and verify
        let read = GooseFsFileReader::read_file_with_context(
            ctx.clone(),
            "/write-type-demo/must_cache.txt",
        )
        .await?;
        assert_eq!(read.as_ref(), data.as_slice());
        println!("  ✅ Read verification passed");
    }

    // ────────────────────────────────────────────────────────
    // 2. CACHE_THROUGH — Write to cache + Master syncs to UFS on CompleteFile
    // ────────────────────────────────────────────────────────
    println!("\n━━━ 2. CACHE_THROUGH ━━━");
    println!("  Data is written to cache; Master auto-syncs to UFS on CompleteFile.");
    {
        let opts = CreateFilePOptions {
            write_type: Some(WritePType::CacheThrough as i32),
            recursive: Some(true),
            ..Default::default()
        };
        let data = b"CACHE_THROUGH: written to cache, Master syncs to UFS on CompleteFile.";
        let bytes = GooseFsFileWriter::write_file_with_context_and_options(
            ctx.clone(),
            "/write-type-demo/cache_through.txt",
            data,
            Some(opts),
        )
        .await?;
        println!("  ✅ Write complete: {} bytes", bytes);

        let read = GooseFsFileReader::read_file_with_context(
            ctx.clone(),
            "/write-type-demo/cache_through.txt",
        )
        .await?;
        assert_eq!(read.as_ref(), data.as_slice());
        println!("  ✅ Read verification passed");
    }

    // ────────────────────────────────────────────────────────
    // 3. THROUGH — Direct write to UFS, bypassing cache
    // ────────────────────────────────────────────────────────
    println!("\n━━━ 3. THROUGH ━━━");
    println!("  Data is written directly to UFS (COS/S3/HDFS), bypassing cache.");
    println!("  Worker uses UfsFile + CreateUfsFileOptions to complete the write.");
    {
        let opts = CreateFilePOptions {
            write_type: Some(WritePType::Through as i32),
            recursive: Some(true),
            ..Default::default()
        };
        let data = b"THROUGH: data written directly to UFS, bypassing cache.";
        let bytes = GooseFsFileWriter::write_file_with_context_and_options(
            ctx.clone(),
            "/write-type-demo/through.txt",
            data,
            Some(opts),
        )
        .await?;
        println!("  ✅ Write complete: {} bytes", bytes);

        let read =
            GooseFsFileReader::read_file_with_context(ctx.clone(), "/write-type-demo/through.txt")
                .await?;
        assert_eq!(read.as_ref(), data.as_slice());
        println!("  ✅ Read verification passed");
    }

    // ────────────────────────────────────────────────────────
    // 4. ASYNC_THROUGH — Write to cache, async persistence scheduled after close()
    // ────────────────────────────────────────────────────────
    println!("\n━━━ 4. ASYNC_THROUGH ━━━");
    println!("  Data is written to cache; close() automatically calls scheduleAsyncPersistence.");
    println!("  Data will eventually be persisted to UFS asynchronously.");
    {
        let opts = CreateFilePOptions {
            write_type: Some(WritePType::AsyncThrough as i32),
            recursive: Some(true),
            ..Default::default()
        };
        let data = b"ASYNC_THROUGH: written to cache, async persistence scheduled after close.";
        let bytes = GooseFsFileWriter::write_file_with_context_and_options(
            ctx.clone(),
            "/write-type-demo/async_through.txt",
            data,
            Some(opts),
        )
        .await?;
        println!("  ✅ Write complete: {} bytes", bytes);
        println!("  ℹ️  close() has already called scheduleAsyncPersistence internally");

        let read = GooseFsFileReader::read_file_with_context(
            ctx.clone(),
            "/write-type-demo/async_through.txt",
        )
        .await?;
        assert_eq!(read.as_ref(), data.as_slice());
        println!("  ✅ Read verification passed");

        // Check persistence status
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let file_info = master
            .get_status("/write-type-demo/async_through.txt")
            .await?;
        println!(
            "  Persistence status: persisted={:?}, state={:?}",
            file_info.persisted, file_info.persistence_state
        );
    }

    // ────────────────────────────────────────────────────────
    // Summary
    // ────────────────────────────────────────────────────────
    println!("\n━━━ Summary ━━━");
    let entries = master.list_status("/write-type-demo", false).await?;
    println!(
        "Directory /write-type-demo contains {} files:",
        entries.len()
    );
    for entry in &entries {
        println!(
            "  {} — {} bytes, persisted: {:?}",
            entry.path.as_deref().unwrap_or("?"),
            entry.length.unwrap_or(0),
            entry.persisted.unwrap_or(false),
        );
    }

    ctx.close().await?;

    println!("\n=======================");
    println!("✅ All WriteType demos complete!");
    println!("\nTip: verify files with:");
    println!("  ./bin/goosefs fs ls /write-type-demo");
    println!("  ./bin/goosefs fs stat /write-type-demo/through.txt");

    Ok(())
}
