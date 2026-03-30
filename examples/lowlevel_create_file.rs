//! Low-level file creation example (metadata only, no data written).
//!
//! Demonstrates the low-level `MasterClient` API for creating file entries
//! in the GooseFS namespace via `CreateFile` + `CompleteFile`.
//! This does NOT write any data blocks — it only creates metadata.
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
//!   cargo run --example lowlevel_create_file

use goosefs_client::client::MasterClient;
use goosefs_client::config::GooseFsConfig;
use goosefs_client::error::Result;
use goosefs_client::proto::grpc::file::CreateFilePOptions;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to GooseFS Master
    println!("Connecting to GooseFS Master...");
    let config = GooseFsConfig::new("127.0.0.1:9200");
    let master = MasterClient::connect(&config).await?;
    println!("Connected!");

    // Create test-demo directory first
    println!("\nCreating test-demo directory...");
    match master.create_directory("/test-demo", true).await {
        Ok(_) => println!("Directory /test-demo created"),
        Err(e) => println!("Failed to create directory or it already exists: {:?}", e),
    }

    // Clean up existing files
    println!("\nCleaning up existing files...");
    match master.delete("/test-demo/world.txt", false).await {
        Ok(_) => println!("Deleted existing world.txt"),
        Err(e) => println!("Failed to delete world.txt or file does not exist: {:?}", e),
    }

    // Create file
    println!("\nCreating world.txt...");
    let mut create_options = CreateFilePOptions::default();
    create_options.block_size_bytes = Some(64 * 1024 * 1024); // 64MB block size
    master
        .create_file("/test-demo/world.txt", create_options)
        .await?;
    println!("File /test-demo/world.txt created");

    // ⚠️ Note: this only sets the metadata length; no actual data blocks are written to Workers.
    // A real write flow requires:
    //   1. Select target Worker via consistent hashing
    //   2. Establish a gRPC bidirectional stream with the Worker
    //   3. Stream data blocks to the Worker cache
    //   4. Worker-side commitBlock
    // For actual data writing, see the highlevel_file_rw example.
    let fake_length = 169i64; // Simulated file length (no actual data blocks on Workers)

    // Mark file as complete (only sets completed=true + length on Master side)
    println!("\nMarking file as complete (metadata only)...");
    master
        .complete_file("/test-demo/world.txt", Some(fake_length))
        .await?;
    println!(
        "File metadata marked complete, length set to {} bytes",
        fake_length
    );

    // Get file status
    println!("\nGetting file status...");
    let file_info = master.get_status("/test-demo/world.txt").await?;
    println!(
        "File length: {:?} bytes (note: this is metadata-only length, no actual data blocks on Workers)",
        file_info.length
    );

    println!("\n✅ world.txt file metadata created!");
    println!(
        "⚠️  Note: this example only demonstrates CreateFile + CompleteFile metadata operations."
    );
    println!(
        "   The file exists in the Master namespace, but has no actual data blocks on Workers."
    );
    println!("   Running `goosefs fs cat /test-demo/world.txt` will hang because no data blocks can be read.");
    println!("   To create a readable/writable file, use the highlevel_file_rw example:");
    println!("   cargo run --example highlevel_file_rw");

    Ok(())
}
