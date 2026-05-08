//! Seekable dual-path file read example using `GoosefsFileInStream`.
//!
//! This example demonstrates the full API surface of
//! [`goosefs_sdk::io::GoosefsFileInStream`], which is the low-level
//! seekable stream powering the higher-level `GoosefsFileReader` and
//! `BaseFileSystem::open_file`.
//!
//! It exercises both read paths:
//!
//! 1. **Sequential path** (`block_in_stream`) — via `read()`, small forward
//!    seeks (< 8 KiB within the same block), and `read_all()`.
//! 2. **Positioned path** (`positioned_read`) — via `read_at()`, large seeks
//!    (≥ 8 KiB) and backward/cross-block seeks.
//!
//! Steps:
//!
//! 0. Create a `FileSystemContext` (the only TCP+SASL handshake).
//! 1. Prepare a test file large enough to span multiple chunks so the
//!    sequential and positioned paths are both meaningful.
//! 2. Open a `GoosefsFileInStream` via `open_with_context`.
//! 3. Sequential `read()` into a fixed-size buffer.
//! 4. Small forward `seek()` (< 8 KiB) — stays on the sequential path.
//! 5. Large `seek()` (≥ 8 KiB) — switches to positioned path on next `read()`.
//! 6. Random `read_at()` at several offsets.
//! 7. `seek_from(SeekFrom::End(-N))` + `read_all()` to drain the tail.
//! 8. Rewind with `seek(0)` and verify `read_all()` matches the written data.
//!
//! Usage:
//!   cargo run --example seekable_file_read

use std::io::SeekFrom;
use std::sync::Arc;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};

/// Path of the test file inside Goosefs.
const TEST_PATH: &str = "/seekable-test/data.bin";

/// Size of the synthetic test payload. 256 KiB is large enough to:
/// - span multiple default chunks (typically 1 MiB or less per chunk),
/// - comfortably exceed the `TRANSFER_POSITIONED_READ_THRESHOLD` (8 KiB),
///   so that large seeks actually switch to the positioned-read path.
const PAYLOAD_SIZE: usize = 256 * 1024;

/// Build a deterministic payload so reads can be verified byte-for-byte.
/// Each byte is `(i % 251) as u8` — 251 is prime, giving a non-trivial
/// pattern that still lets us recompute any byte by its offset.
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
    println!("Goosefs Seekable File Read Demo (GoosefsFileInStream)");
    println!("=======================================================");

    // ── Step 0: Build FileSystemContext ─────────────────────────
    println!("\n0. Creating FileSystemContext...");
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    let master = ctx.acquire_master();
    println!("  ✅ Context ready");

    // ── Cleanup + ensure parent directory ───────────────────────
    println!("\n   Preparing test directory and file...");
    let _ = master.delete(TEST_PATH, false).await;
    match master.create_directory("/seekable-test", true).await {
        Ok(_) => println!("   Directory /seekable-test created"),
        Err(_) => println!("   Directory /seekable-test already exists"),
    }

    // ── Step 1: Write the synthetic test payload ────────────────
    println!(
        "\n1. Writing {}-byte test payload to {}...",
        PAYLOAD_SIZE, TEST_PATH
    );
    let payload = make_payload(PAYLOAD_SIZE);
    let mut writer = GoosefsFileWriter::create_with_context(ctx.clone(), TEST_PATH, None).await?;
    writer.write(&payload).await?;
    writer.close().await?;
    println!("  ✅ Wrote {} bytes", writer.bytes_written());

    // ── Step 2: Open a seekable input stream ────────────────────
    println!("\n2. Opening GoosefsFileInStream via open_with_context...");
    let opts = OpenFileOptions::default();
    let mut stream = GoosefsFileInStream::open_with_context(ctx.clone(), TEST_PATH, opts).await?;
    println!(
        "  ✅ Stream opened: len={} pos={} eof={}",
        stream.len(),
        stream.pos(),
        stream.is_eof()
    );

    // ── Step 3: Sequential read into a fixed buffer ─────────────
    println!("\n3. Sequential read() of first 4 KiB (sequential path)...");
    let mut buf = vec![0u8; 4096];
    let mut total = 0usize;
    while total < buf.len() {
        let n = stream.read(&mut buf[total..]).await?;
        if n == 0 {
            break;
        }
        total += n;
    }
    println!("  ✅ Read {} bytes, pos now = {}", total, stream.pos());
    assert_eq!(total, 4096);
    assert!(verify_slice(&buf[..total], 0), "sequential prefix mismatch");
    println!("  ✅ Content verified against payload[0..4096]");

    // ── Step 4: Small forward seek (< 8 KiB, same block) ────────
    //
    // Stays on the sequential path: the existing block_in_stream skips
    // bytes instead of being rebuilt.
    println!("\n4. Small forward seek by +2 KiB (sequential path preserved)...");
    let target = stream.pos() + 2 * 1024;
    let new_pos = stream.seek(target).await?;
    println!("  ✅ seek -> {} (target {})", new_pos, target);
    assert_eq!(new_pos, target);

    let mut small = vec![0u8; 512];
    let n = stream.read(&mut small).await?;
    println!("  ✅ read {} bytes after small seek", n);
    assert!(n > 0);
    assert!(
        verify_slice(&small[..n], target as usize),
        "small-seek content mismatch"
    );

    // ── Step 5: Large seek (≥ 8 KiB) — switches to positioned ───
    //
    // The large-seek branch drops `block_in_stream`; the subsequent
    // read rebuilds it at the new offset. For truly random workloads
    // callers should prefer `read_at` (Step 6), which uses the
    // positioned-read gRPC path directly.
    println!("\n5. Large seek by +64 KiB (drops sequential stream)...");
    let big_target = stream.pos() + 64 * 1024;
    let new_pos = stream.seek(big_target).await?;
    println!("  ✅ seek -> {} (target {})", new_pos, big_target);
    assert_eq!(new_pos, big_target);

    let mut after_big = vec![0u8; 1024];
    let mut got = 0usize;
    while got < after_big.len() {
        let n = stream.read(&mut after_big[got..]).await?;
        if n == 0 {
            break;
        }
        got += n;
    }
    println!("  ✅ read {} bytes after large seek", got);
    assert!(
        verify_slice(&after_big[..got], big_target as usize),
        "large-seek content mismatch"
    );

    // ── Step 6: Random read_at() at several offsets ─────────────
    //
    // `read_at` does NOT change the stream's position and always uses
    // the positioned-read path (position_short=true).
    println!("\n6. Random read_at() samples (positioned path)...");
    let pos_before = stream.pos();
    for &off in &[0i64, 123, 8192, 100 * 1024, (PAYLOAD_SIZE as i64) - 777] {
        let want_len = 777usize;
        let data = stream.read_at(off, want_len).await?;
        let expected_len = ((PAYLOAD_SIZE as i64 - off).max(0) as usize).min(want_len);
        assert_eq!(
            data.len(),
            expected_len,
            "read_at length mismatch at off={}",
            off
        );
        assert!(
            verify_slice(&data, off as usize),
            "read_at content mismatch at off={}",
            off
        );
        println!(
            "  ✅ read_at(offset={}, n={}) -> {} bytes",
            off,
            want_len,
            data.len()
        );
    }
    assert_eq!(
        stream.pos(),
        pos_before,
        "read_at must not move the stream position"
    );
    println!(
        "  ✅ Stream position unchanged after read_at: {}",
        stream.pos()
    );

    // ── Step 7: seek_from(SeekFrom::End(-N)) + read_all ─────────
    println!("\n7. seek_from(End(-1024)) then read_all()...");
    let tail_start = stream.seek_from(SeekFrom::End(-1024)).await?;
    println!("  ✅ seek to tail offset = {}", tail_start);
    assert_eq!(tail_start, (PAYLOAD_SIZE as i64) - 1024);

    let tail = stream.read_all().await?;
    println!(
        "  ✅ read_all returned {} bytes, pos = {}, eof = {}",
        tail.len(),
        stream.pos(),
        stream.is_eof()
    );
    assert_eq!(tail.len(), 1024);
    assert!(verify_slice(&tail, tail_start as usize));
    assert!(stream.is_eof());

    // ── Step 8: Rewind and full read_all ────────────────────────
    println!("\n8. Rewind with seek(0) and full read_all()...");
    stream.seek(0).await?;
    assert_eq!(stream.pos(), 0);
    let full = stream.read_all().await?;
    println!("  ✅ read_all returned {} bytes", full.len());
    assert_eq!(full.len(), PAYLOAD_SIZE);
    assert_eq!(&full[..], &payload[..], "full-file content mismatch");
    println!("  ✅ Full-file content matches original payload");

    // ── Cleanup ──────────────────────────────────────────────────
    println!("\n9. Cleanup...");
    let _ = master.delete(TEST_PATH, false).await;
    ctx.close().await?;
    println!("  ✅ Context closed");

    println!("\n=======================================================");
    println!("✅ Seekable file read demo complete!");
    println!("\nCovered APIs:");
    println!("  • GoosefsFileInStream::open_with_context");
    println!("  • read / seek / seek_from / read_at / read_all");
    println!("  • len / pos / is_eof / remaining / is_empty");
    println!("  • Sequential path (small seek)  vs  positioned path (large seek / read_at)");
    Ok(())
}
