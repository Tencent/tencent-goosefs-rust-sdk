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

//! End-to-end demo: drive a Goosefs file through `tokio::io::AsyncRead`
//! + `AsyncSeek` via [`GoosefsAsyncReader`].
//!
//! This example proves that the SDK is now first-class compatible with
//! the wider tokio ecosystem — the same handle can be passed to
//! `tokio::io::copy`, wrapped in `BufReader` for line-oriented reads,
//! seeked with the standard `AsyncSeekExt` API, and read to end with
//! `AsyncReadExt::read_to_end`.
//!
//! Steps:
//! 0. Connect a `FileSystemContext`.
//! 1. Write a deterministic 256 KiB payload (single TCP+SASL handshake).
//! 2. Open a `GoosefsFileInStream`, wrap it in `GoosefsAsyncReader`.
//! 3. `tokio::io::copy(&mut reader, &mut Vec::new())` — full sequential read.
//! 4. `seek(SeekFrom::Start(N))` then `read_exact(&mut buf)` — random access.
//! 5. `seek(SeekFrom::End(-N))` then `read_to_end(&mut buf)` — tail read.
//! 6. Verify every byte byte-for-byte against the deterministic generator.
//!
//! Usage:
//!   cargo run --example async_read_trait

use std::io::SeekFrom;
use std::sync::Arc;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsAsyncReader, GoosefsFileInStream, GoosefsFileWriter};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Path of the test file inside Goosefs.
const TEST_PATH: &str = "/async-read-trait/data.bin";

/// Size of the synthetic test payload. 256 KiB spans multiple chunks at
/// the default `chunk_size` (1 MiB or less depending on cluster config),
/// which is enough to exercise the SDK's chunk-overflow path through
/// the trait surface.
const PAYLOAD_SIZE: usize = 256 * 1024;

/// Deterministic payload — `(i % 251) as u8` per byte, same scheme as
/// `examples/seekable_file_read.rs`. 251 is prime, giving a non-trivial
/// pattern while letting us recompute any byte from its offset.
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let master_addr =
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    let config = GoosefsConfig::new(&master_addr);

    println!("== async_read_trait demo ==");
    println!("master: {master_addr}");
    println!("path:   {TEST_PATH}\n");

    // 0) Establish the shared context (single handshake). `connect`
    //    already returns `Arc<FileSystemContext>` so we don't wrap.
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    let master = ctx.acquire_master();

    // 1) Prepare the payload.
    let payload = make_payload(PAYLOAD_SIZE);
    println!("[1] writing {} bytes via GoosefsFileWriter…", payload.len());

    // Idempotent prep: drop any leftover from a previous run, then
    // ensure the parent directory exists.
    let _ = master.delete(TEST_PATH, false).await;
    match master.create_directory("/async-read-trait", true).await {
        Ok(_) | Err(_) => {} // best-effort; OK if it already exists
    }
    {
        let mut writer =
            GoosefsFileWriter::create_with_context(ctx.clone(), TEST_PATH, None).await?;
        writer.write(&payload).await?;
        writer.close().await?;
    }

    // 2) Open the stream and wrap it in the trait adapter.
    let stream =
        GoosefsFileInStream::open_with_context(ctx.clone(), TEST_PATH, OpenFileOptions::default())
            .await?;
    let mut reader = GoosefsAsyncReader::new(stream);

    // 3) `tokio::io::copy` exercises the full AsyncRead loop with a
    //    BufWriter sink — pure trait-level usage, zero SDK awareness.
    println!("[2] tokio::io::copy → Vec<u8> (full sequential read via AsyncRead)…");
    let mut sink: Vec<u8> = Vec::with_capacity(PAYLOAD_SIZE);
    let copied = tokio::io::copy(&mut reader, &mut sink)
        .await
        .map_err(io_err)?;
    assert_eq!(
        copied as usize, PAYLOAD_SIZE,
        "copy reported {copied} bytes, expected {PAYLOAD_SIZE}"
    );
    assert_eq!(sink.len(), PAYLOAD_SIZE, "sink size mismatch");
    assert!(
        verify_slice(&sink, 0),
        "byte-for-byte mismatch on full read"
    );
    println!("    ok — copied {copied} bytes");

    // 4) Random access via `AsyncSeekExt::seek` + `AsyncReadExt::read_exact`.
    println!("[3] seek(Start(60_000)) + read_exact(4096) (random access via AsyncSeek)…");
    let pos = reader.seek(SeekFrom::Start(60_000)).await.map_err(io_err)?;
    assert_eq!(pos, 60_000, "seek reported wrong position");
    let mut chunk = vec![0u8; 4096];
    reader.read_exact(&mut chunk).await.map_err(io_err)?;
    assert!(
        verify_slice(&chunk, 60_000),
        "byte-for-byte mismatch on random read"
    );
    println!("    ok — 4096 bytes at offset 60000 verified");

    // 5) Tail read via `seek(End(-N))` + `read_to_end`.
    println!("[4] seek(End(-1024)) + read_to_end (tail read)…");
    let tail_off = reader.seek(SeekFrom::End(-1024)).await.map_err(io_err)?;
    assert_eq!(
        tail_off,
        (PAYLOAD_SIZE - 1024) as u64,
        "End-relative seek wrong position"
    );
    let mut tail: Vec<u8> = Vec::new();
    let n = reader.read_to_end(&mut tail).await.map_err(io_err)?;
    assert_eq!(n, 1024, "tail read returned {n} bytes, expected 1024");
    assert!(
        verify_slice(&tail, PAYLOAD_SIZE - 1024),
        "byte-for-byte mismatch on tail read"
    );
    println!("    ok — 1024-byte tail verified");

    // 6) Round-trip: rewind to the head and verify identity.
    println!("[5] seek(Start(0)) + read_to_end (full round-trip)…");
    reader.seek(SeekFrom::Start(0)).await.map_err(io_err)?;
    let mut all: Vec<u8> = Vec::with_capacity(PAYLOAD_SIZE);
    let n = reader.read_to_end(&mut all).await.map_err(io_err)?;
    assert_eq!(n, PAYLOAD_SIZE, "round-trip wrong size");
    assert_eq!(all, payload, "round-trip payload mismatch");
    println!("    ok — full {PAYLOAD_SIZE}-byte round-trip verified");

    // 7) Recover the underlying stream — proves `into_inner` works
    //    after I/O completes. We can't `expect()` because
    //    `GoosefsAsyncReader: !Debug` (it holds a `dyn Future`); use
    //    a manual `match` instead.
    let inner = match reader.into_inner() {
        Ok(s) => s,
        Err(_) => panic!("reader still has an in-flight op after all reads completed"),
    };
    println!(
        "[6] reader.into_inner() ok — underlying stream pos={}",
        inner.pos()
    );

    // 8) Cleanup.
    let _ = master.delete(TEST_PATH, false).await;

    println!("\n== all checks passed ==");
    Ok(())
}

/// Promote `tokio::io::Error` into the SDK's `Result` type for `?`.
fn io_err(e: std::io::Error) -> goosefs_sdk::error::Error {
    goosefs_sdk::error::Error::Internal {
        message: format!("io error: {e}"),
        source: None,
    }
}
