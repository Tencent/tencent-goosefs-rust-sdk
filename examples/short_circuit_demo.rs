//! End-to-end short-circuit (local mmap) read demo.
//!
//! Exercises the short-circuit data plane wired into the positioned-read path
//! (`GoosefsFileInStream::read_at`) against a **locally running** Goosefs
//! cluster. When the block is served by the local worker and is on local disk,
//! reads are served by a zero-copy `mmap` slice; otherwise the path
//! transparently falls back to gRPC (identical bytes, INV-S1).
//!
//! What it verifies:
//!  1. `read_at` at several offsets returns byte-for-byte the written content.
//!  2. The full `read_all()` matches the source.
//!  3. Whether the short-circuit path actually fired (via SC metrics) or fell
//!     back to gRPC — both are correct; the metric just tells you which ran.
//!
//! Usage:
//!   cargo run --example short_circuit_demo
//!   GOOSEFS_MASTER_ADDR=host:9200 cargo run --example short_circuit_demo

use std::sync::Arc;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
use goosefs_sdk::metrics::{counter, gauge, name};

const PATH: &str = "/sc-test/blob.bin";
/// ~4 MiB deterministic payload (single block on default block sizes).
const SIZE: usize = 4 * 1024 * 1024;

/// Deterministic, position-dependent byte pattern so a wrong offset is caught.
fn pattern(i: usize) -> u8 {
    // mix the index so adjacent bytes differ and offsets are distinguishable
    ((i.wrapping_mul(2654435761) >> 13) ^ i) as u8
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Goosefs Short-Circuit (local mmap) Read Demo");
    println!("============================================");

    let master_addr =
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string());
    // The local dev cluster runs NOSASL by default; override with
    // GOOSEFS_AUTH_TYPE=simple (or set GOOSEFS_AUTH_USERNAME) if needed.
    let auth_type = std::env::var("GOOSEFS_AUTH_TYPE").unwrap_or_else(|_| "nosasl".to_string());
    println!("\n0. Connecting to Goosefs at {master_addr} (auth={auth_type}) ...");
    let config = GoosefsConfig::new(master_addr)
        .with_auth_type_str(&auth_type)
        .map_err(|e| goosefs_sdk::error::Error::ConfigError { message: e })?;
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    println!("   ✅ context connected");

    // Diagnostic: print the registered workers so we can see whether the
    // local worker is detectable (drives `source_is_local`).
    let workers = ctx.acquire_router().get_workers().await;
    println!("   registered workers:");
    for w in workers.iter() {
        if let Some(a) = &w.address {
            println!(
                "     id={:?} host={:?} rpc_port={:?}",
                w.id, a.host, a.rpc_port
            );
        }
    }
    println!(
        "   local hostname: {:?}",
        hostname::get().ok().and_then(|h| h.into_string().ok())
    );

    let master = ctx.acquire_master();
    let _ = master.delete(PATH, false).await;
    let _ = master.create_directory("/sc-test", true).await;

    // ── 1. Write the test blob ───────────────────────────────────────────
    println!("\n1. Writing {} bytes to {PATH} ...", SIZE);
    let payload: Vec<u8> = (0..SIZE).map(pattern).collect();
    let mut writer = GoosefsFileWriter::create_with_context(ctx.clone(), PATH, None).await?;
    writer.write(&payload).await?;
    writer.close().await?;
    println!("   ✅ wrote {} bytes", writer.bytes_written());

    // Snapshot SC metrics before reading so we can tell if SC actually fired.
    let open_before = counter(name::CLIENT_SC_OPEN_SUCCESS).get();
    let openfail_before = counter(name::CLIENT_SC_OPENLOCAL_FAIL).get();
    let scbytes_before = counter(name::CLIENT_SC_READ_BYTES).get();
    let hits_before = counter(name::CLIENT_SC_CACHE_HITS).get();

    // ── 2. Positioned reads at several offsets ───────────────────────────
    println!("\n2. Positioned read_at checks (short-circuit path) ...");
    let mut stream =
        GoosefsFileInStream::open_with_context(ctx.clone(), PATH, OpenFileOptions::default())
            .await?;

    let cases: &[(i64, usize)] = &[
        (0, 4096),
        (4095, 4098),               // crosses a 4 KiB page boundary
        (1_000_003, 65536),         // unaligned offset, 64 KiB
        ((SIZE - 100) as i64, 100), // tail
    ];
    let mut all_ok = true;
    for &(off, len) in cases {
        let got = stream.read_at(off, len).await?;
        let want = &payload[off as usize..off as usize + len];
        let ok = got.as_ref() == want;
        all_ok &= ok;
        println!(
            "   off={:>9} len={:>6} -> {} ({} bytes)",
            off,
            len,
            if ok { "✅ match" } else { "❌ MISMATCH" },
            got.len()
        );
    }

    // ── 3. Full read_all matches source ──────────────────────────────────
    println!("\n3. Full read_all() verification ...");
    let mut full_stream =
        GoosefsFileInStream::open_with_context(ctx.clone(), PATH, OpenFileOptions::default())
            .await?;
    let all = full_stream.read_all().await?;
    let full_ok = all.len() == payload.len() && all.as_ref() == payload.as_slice();
    all_ok &= full_ok;
    println!(
        "   read {} bytes -> {}",
        all.len(),
        if full_ok { "✅ match" } else { "❌ MISMATCH" }
    );

    // ── 4. Report whether short-circuit fired ────────────────────────────
    let open_after = counter(name::CLIENT_SC_OPEN_SUCCESS).get();
    let openfail_after = counter(name::CLIENT_SC_OPENLOCAL_FAIL).get();
    let scbytes_after = counter(name::CLIENT_SC_READ_BYTES).get();
    let hits_after = counter(name::CLIENT_SC_CACHE_HITS).get();

    println!("\n4. Short-circuit metrics (delta over this run):");
    println!("   sc_open_success      : {}", open_after - open_before);
    println!("   sc_openlocal_fail    : {}", openfail_after - openfail_before);
    println!("   sc_read_bytes        : {}", scbytes_after - scbytes_before);
    println!("   sc_cache_hits        : {}", hits_after - hits_before);
    println!(
        "   sc_active_readers    : {}",
        gauge(name::CLIENT_SC_ACTIVE_READERS).get()
    );

    if open_after > open_before && scbytes_after > scbytes_before {
        println!("\n   🚀 Short-circuit path ACTIVE — reads served from local mmap.");
    } else if openfail_after > openfail_before {
        println!(
            "\n   ↩️  OpenLocalBlock was rejected (block not local / capability) — \
             reads fell back to gRPC (still correct)."
        );
    } else {
        println!(
            "\n   ↩️  Short-circuit did not fire (local worker not detected, or \
             block not local) — reads used gRPC (still correct)."
        );
    }

    // ── cleanup ──────────────────────────────────────────────────────────
    println!("\n5. Cleanup ...");
    let _ = master.delete(PATH, false).await;
    ctx.close().await?;

    println!("\n============================================");
    if all_ok {
        println!("✅ All reads verified byte-for-byte (INV-S1 / INV-D2 hold).");
    } else {
        println!("❌ Some reads did NOT match — investigate!");
        std::process::exit(1);
    }
    Ok(())
}
