//! Async persistence scheduling example.
//!
//! Demonstrates two approaches to async persistence:
//!
//! 1. **ASYNC_THROUGH mode**: Uses `GoosefsFileWriter` with `WritePType::AsyncThrough`.
//!    Data is written to Worker cache first, then `close()` automatically triggers
//!    async persistence — no manual scheduling needed.
//!
//! 2. **MUST_CACHE + manual schedule**: Uses `GoosefsFileWriter` with the default
//!    `WritePType::MustCache` to write data to cache, then manually calls
//!    `MasterClient::schedule_async_persistence()` to trigger persistence.
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
//!   cargo run --example async_persistence

use std::sync::Arc;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::GoosefsFileWriter;
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use goosefs_sdk::WritePType;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Goosefs Async Persistence Demo");
    println!("===============================");

    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;

    // ── Step 0: Cleanup & create directory ────────────────────────
    println!("\n0. Cleaning up existing test directory...");
    let master = ctx.acquire_master();
    match master.delete("/persisted-demo", true).await {
        Ok(_) => println!("  Deleted old test directory"),
        Err(_) => println!("  Old directory does not exist, skipping"),
    }
    master.create_directory("/persisted-demo", true).await?;
    println!("  Directory /persisted-demo created");

    // ── Step 1: ASYNC_THROUGH mode ───────────────────────────────
    println!("\n━━━ 1. ASYNC_THROUGH mode (automatic async persistence) ━━━");
    println!("  Data is written to Worker cache first; close() automatically schedules async persistence to UFS.");
    {
        let opts = CreateFilePOptions {
            write_type: Some(WritePType::AsyncThrough as i32),
            recursive: Some(true),
            ..Default::default()
        };
        let content = b"This file is written with ASYNC_THROUGH mode.\n\
                        Data goes to Worker cache first, then persisted to UFS asynchronously.\n\
                        Goosefs Rust Client async persistence demo.";

        let bytes_written = GoosefsFileWriter::write_file_with_context_and_options(
            ctx.clone(),
            "/persisted-demo/async_through.txt",
            content,
            Some(opts),
        )
        .await?;
        println!("  ✅ Write complete: {} bytes", bytes_written);

        // Check initial status
        let info = master
            .get_status("/persisted-demo/async_through.txt")
            .await?;
        println!("  Initial status:");
        println!("    Persisted: {:?}", info.persisted.unwrap_or(false));
        println!("    Persistence state: {:?}", info.persistence_state);
    }

    // ── Step 2: MUST_CACHE + manual schedule ─────────────────────
    println!("\n━━━ 2. MUST_CACHE + manual persistence scheduling ━━━");
    println!("  Data is written to Worker cache only, then manually schedule async persistence.");
    {
        let opts = CreateFilePOptions {
            write_type: Some(WritePType::MustCache as i32),
            recursive: Some(true),
            ..Default::default()
        };
        let content = b"This file is written with MUST_CACHE mode.\n\
                        After writing, we manually schedule async persistence.\n\
                        Goosefs Rust Client manual persistence demo.";

        let bytes_written = GoosefsFileWriter::write_file_with_context_and_options(
            ctx.clone(),
            "/persisted-demo/manual_persist.txt",
            content,
            Some(opts),
        )
        .await?;
        println!("  ✅ Write complete: {} bytes", bytes_written);

        // Check status before scheduling
        let info_before = master
            .get_status("/persisted-demo/manual_persist.txt")
            .await?;
        println!("  Status before persistence:");
        println!(
            "    Persisted: {:?}",
            info_before.persisted.unwrap_or(false)
        );
        println!("    Persistence state: {:?}", info_before.persistence_state);

        // Manually schedule async persistence
        println!("  Scheduling async persistence...");
        master
            .schedule_async_persistence("/persisted-demo/manual_persist.txt", None)
            .await?;
        println!("  ✅ Persistence scheduled");
    }

    // ── Step 3: Wait and check final status ──────────────────────
    println!("\n━━━ 3. Waiting for persistence to complete ━━━");
    println!("  Waiting 15 seconds for JobWorker to finish persistence...");
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    // Check final status for both files
    let files = [
        "/persisted-demo/async_through.txt",
        "/persisted-demo/manual_persist.txt",
    ];

    let mut all_persisted = true;
    for path in &files {
        let info = master.get_status(path).await?;
        let persisted = info.persisted.unwrap_or(false);
        let state = info.persistence_state.as_deref().unwrap_or("unknown");
        println!("  {} — persisted={}, state={}", path, persisted, state);
        if !persisted {
            all_persisted = false;
        }
    }

    if all_persisted {
        println!("\n✅ All files have been successfully persisted to UFS!");
    } else {
        println!("\n⚠️  Some files are not yet persisted; they may need more time.");
        println!("  Check status with:");
        println!("  ./bin/goosefs fs ls /persisted-demo");
    }

    // ── Step 4: List directory contents ──────────────────────────
    println!("\n━━━ 4. Directory contents ━━━");
    let entries = master.list_status("/persisted-demo", false).await?;
    println!("  /persisted-demo contains {} entries:", entries.len());
    for entry in &entries {
        println!(
            "  - {} ({} bytes, persisted={})",
            entry.path.as_deref().unwrap_or("unknown"),
            entry.length.unwrap_or(0),
            entry.persisted.unwrap_or(false),
        );
    }

    ctx.close().await?;

    println!("\n✅ Async persistence demo complete!");
    Ok(())
}
