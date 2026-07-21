//! Streaming block-by-block file read example using `GoosefsFileReader`.
//!
//! This example highlights the **peak-memory trade-off** between the two
//! read APIs exposed by [`goosefs_sdk::io::GoosefsFileReader`]:
//!
//! | API                               | Peak memory      | Use when                          |
//! |-----------------------------------|------------------|-----------------------------------|
//! | `read_file_with_context` / `read_all` | `O(file)`        | File fits comfortably in RAM      |
//! | `open_with_context` + `read_next_block` | `O(single block)`| Large files / streaming pipelines |
//!
//! `read_next_block` returns **one block's `Bytes` buffer per call**, so the
//! process never holds more than a single block (≤ `block_size_bytes`, by
//! default 64 MiB) in memory — regardless of how large the underlying file is.
//!
//! Steps:
//!
//! 0. Create a `FileSystemContext`.
//! 1. Write a multi-block test file (so streaming is actually meaningful).
//! 2. **Full read** via `read_file_with_context`     → `O(file)` peak memory.
//! 3. **Streaming read** via `open_with_context` + `read_next_block`
//!    → `O(single block)` peak memory; verify byte count + content.
//! 4. **Streaming range read** via `open_range_with_context` + `read_next_block`
//!    → same streaming benefit, restricted to `[offset, offset+length)`.
//! 5. **Inspection** of `file_length` / `block_count` / `current_block_index`
//!    / `bytes_read` during iteration.
//!
//! Usage:
//!   cargo run --example streaming_file_read

use std::sync::Arc;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;

/// Path of the test file inside Goosefs.
const TEST_PATH: &str = "/streaming-test/data.bin";

/// Size of the synthetic payload. Default block_size_bytes in goosefs is
/// typically 64 MiB, so for local testing we keep the file modestly sized
/// while still likely spanning multiple *chunks* (chunk_size, usually 1 MiB)
/// so that streaming truly iterates multiple reads.
///
/// 4 MiB is a safe default:
///   - Large enough to exercise `read_next_block` in a meaningful loop
///   - Small enough to finish quickly on a laptop-class master/worker
const PAYLOAD_SIZE: usize = 4 * 1024 * 1024;

/// Block size for this demo. We deliberately override the default (64 MiB)
/// with a much smaller 1 MiB block so `PAYLOAD_SIZE / BLOCK_SIZE == 4`
/// blocks — enough to make the streaming loop visibly iterate multiple
/// times and to demonstrate the O(single block) memory bound.
const BLOCK_SIZE: i64 = 1 * 1024 * 1024;

/// Build a deterministic payload: `(i % 251) as u8`. 251 is prime, so the
/// pattern does not align with any power-of-two block boundary, and any
/// window of the payload can be verified against its absolute offset.
fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Verify that `slice` equals the expected payload window starting at `offset`.
fn verify_slice(slice: &[u8], offset: usize) -> bool {
    slice
        .iter()
        .enumerate()
        .all(|(i, &b)| b == ((offset + i) % 251) as u8)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Goosefs Streaming File Read Demo (GoosefsFileReader::read_next_block)");
    println!("=====================================================================");

    // ── Step 0: Build FileSystemContext ─────────────────────────
    println!("\n0. Creating FileSystemContext...");
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    let master = ctx.acquire_master();
    println!("  ✅ Context ready");

    // ── Prepare directory + clean old file ──────────────────────
    println!("\n   Preparing test directory and file...");
    let _ = master.delete(TEST_PATH, false).await;
    match master.create_directory("/streaming-test", true).await {
        Ok(_) => println!("   Directory /streaming-test created"),
        Err(_) => println!("   Directory /streaming-test already exists"),
    }

    // ── Step 1: Write a multi-chunk payload ─────────────────────
    println!(
        "\n1. Writing {}-byte test payload ({} MiB)...",
        PAYLOAD_SIZE,
        PAYLOAD_SIZE / (1024 * 1024)
    );
    let payload = make_payload(PAYLOAD_SIZE);
    // Override block_size_bytes so the file is split into multiple blocks
    // and the streaming demo iterates more than once.
    let create_opts = CreateFilePOptions {
        block_size_bytes: Some(BLOCK_SIZE),
        recursive: Some(true),
        ..Default::default()
    };
    let mut writer =
        GoosefsFileWriter::create_with_context(ctx.clone(), TEST_PATH, Some(create_opts)).await?;
    // Write in 256 KiB chunks to exercise the writer as well.
    for chunk in payload.chunks(256 * 1024) {
        writer.write(chunk).await?;
    }
    writer.close().await?;
    println!(
        "  ✅ Wrote {} bytes (block_size = {} MiB → expected {} blocks)",
        writer.bytes_written(),
        BLOCK_SIZE / (1024 * 1024),
        PAYLOAD_SIZE as i64 / BLOCK_SIZE
    );

    // ── Step 2: Full read — O(file) peak memory ─────────────────
    //
    // `read_file_with_context` concatenates every block into a single
    // contiguous `Bytes`. Convenient, but peak memory is proportional
    // to the file size. Do NOT use this for multi-GiB files.
    println!("\n2. FULL read via read_file_with_context (peak memory = O(file))...");
    let all = GoosefsFileReader::read_file_with_context(ctx.clone(), TEST_PATH).await?;
    println!(
        "  ✅ read_all returned {} bytes in one contiguous buffer",
        all.len()
    );
    assert_eq!(all.len(), PAYLOAD_SIZE);
    assert!(verify_slice(&all, 0), "full-read content mismatch");
    drop(all); // release the big buffer before the streaming demo

    // ── Step 3: Streaming read — O(single block) peak memory ───
    //
    // `read_next_block` returns *one block* per call. The caller's peak
    // memory is bounded by a single block buffer (≤ block_size_bytes,
    // 64 MiB by default) regardless of total file size.
    println!(
        "\n3. STREAMING read via open_with_context + read_next_block \
         (peak memory = O(single block))..."
    );
    let mut reader = GoosefsFileReader::open_with_context(ctx.clone(), TEST_PATH).await?;
    println!(
        "   file_length = {}, block_count = {}",
        reader.file_length(),
        reader.block_count()
    );

    let mut total: u64 = 0;
    let mut max_block_size: usize = 0;
    let mut block_idx = 0usize;

    // Classic streaming loop: process each block, then drop it.
    // Memory in flight = at most one block (`chunk`) + small bookkeeping.
    while let Some(chunk) = reader.read_next_block().await? {
        // Verify the block's content against its absolute offset.
        let offset = total as usize;
        assert!(
            verify_slice(&chunk, offset),
            "streaming content mismatch at offset {}",
            offset
        );

        total += chunk.len() as u64;
        max_block_size = max_block_size.max(chunk.len());
        println!(
            "   block #{:>2}: {:>8} bytes  | reader.current_block_index={}  reader.bytes_read={}",
            block_idx,
            chunk.len(),
            reader.current_block_index(),
            reader.bytes_read()
        );
        block_idx += 1;
        // `chunk` is dropped here — peak memory stays at O(single block).
    }

    println!(
        "  ✅ Streaming done: {} blocks, {} bytes total, \
         largest single block seen = {} bytes",
        block_idx, total, max_block_size
    );
    assert_eq!(total as usize, PAYLOAD_SIZE);
    assert_eq!(reader.bytes_read(), PAYLOAD_SIZE as u64);

    // ── Step 4: Streaming range read ────────────────────────────
    //
    // `open_range_with_context` narrows the streaming iterator to
    // `[offset, offset+length)`. Same O(single block) memory profile.
    let range_off: u64 = 1_000_000;
    let range_len: u64 = 2_000_000;
    println!(
        "\n4. STREAMING range read via open_range_with_context \
         (offset={}, length={})...",
        range_off, range_len
    );
    let mut range_reader =
        GoosefsFileReader::open_range_with_context(ctx.clone(), TEST_PATH, range_off, range_len)
            .await?;

    let mut range_total: u64 = 0;
    let mut range_blocks = 0usize;
    while let Some(chunk) = range_reader.read_next_block().await? {
        let abs_off = range_off as usize + range_total as usize;
        assert!(
            verify_slice(&chunk, abs_off),
            "range-streaming content mismatch at absolute offset {}",
            abs_off
        );
        range_total += chunk.len() as u64;
        range_blocks += 1;
    }
    println!(
        "  ✅ Range streaming done: {} blocks, {} bytes (requested {} bytes)",
        range_blocks, range_total, range_len
    );
    assert_eq!(range_total, range_len);

    // ── Step 5: Empty-loop semantics after exhaustion ───────────
    //
    // Once `read_next_block` has returned `None`, further calls must
    // keep returning `None` (not error). This makes it safe to use
    // inside `while let Some(_)` without extra guards.
    println!("\n5. Post-exhaustion: read_next_block keeps returning None...");
    assert!(reader.read_next_block().await?.is_none());
    assert!(reader.read_next_block().await?.is_none());
    println!("  ✅ Idempotent exhaustion confirmed");

    // ── Cleanup ──────────────────────────────────────────────────
    println!("\n6. Cleanup...");
    let _ = master.delete(TEST_PATH, false).await;
    ctx.close().await?;
    println!("  ✅ Context closed");

    println!("\n=====================================================================");
    println!("✅ Streaming file read demo complete!");
    println!("\nTakeaways:");
    println!("  • read_file_with_context / read_all   → peak memory = O(file)");
    println!("  • open_with_context + read_next_block → peak memory = O(single block)");
    println!("  • Prefer the streaming API for files that may not fit in RAM,");
    println!("    or whenever you process data block-by-block (ETL, parquet, etc.).");
    Ok(())
}
