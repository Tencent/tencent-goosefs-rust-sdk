//! Client metrics & heartbeat example.
//!
//! Demonstrates the client-side metrics pipeline:
//!
//! 1. Connecting with `metrics_enabled = true` (default) automatically spawns a
//!    background `HeartbeatTask` that reports incremental counter deltas to the
//!    GooseFS Master via the `MetricsHeartbeat` RPC.
//! 2. Application code (and the SDK's `io` layer) increments named counters
//!    via the global registry: `metrics::counter(name).inc(n)`.
//! 3. File I/O through `FileSystemContext` automatically updates the
//!    `Client.BytesReadLocal` / `Client.BytesWrittenLocal` counters.
//! 4. `FileSystemContext::close()` performs a final flush so the last delta
//!    reaches the Master before shutdown.
//! 5. With `metrics_enabled = false` no heartbeat task is spawned at all
//!    (zero background work, zero RPC overhead).
//!
//! Usage:
//!   cargo run --example metrics_heartbeat
//!
//! Tip: run with `RUST_LOG=info` to see the SDK's heartbeat / flush logs.

use std::sync::Arc;
use std::time::Duration;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::metrics;

const TEST_DIR: &str = "/metrics-demo";
const TEST_FILE: &str = "/metrics-demo/payload.bin";

#[tokio::main]
async fn main() -> Result<()> {
    println!("Goosefs Client Metrics & Heartbeat Demo");
    println!("=========================================");

    // ── Part 1: connect with metrics enabled (default) ──────────────────
    //
    // `metrics_enabled` defaults to `true` in `GoosefsConfig`. We tune the
    // heartbeat interval to a small value (2 s, the minimum is 1 s) so the
    // demo can observe at least one report cycle before exiting.
    println!("\n1. Building config with metrics enabled (interval = 2 s, app_id set)...");
    let config = GoosefsConfig::new("127.0.0.1:9200")
        .with_metrics_enabled(true)
        .with_metrics_heartbeat_interval(Duration::from_secs(2))
        .with_metrics_heartbeat_timeout(Duration::from_secs(1)) // < interval
        .with_app_id("metrics-demo-app");

    println!("   metrics_enabled            = {}", config.metrics_enabled);
    println!(
        "   metrics_heartbeat_interval = {:?}",
        config.metrics_heartbeat_interval
    );
    println!(
        "   metrics_heartbeat_timeout  = {:?}",
        config.metrics_heartbeat_timeout
    );
    println!(
        "   metrics_max_batch_size     = {}",
        config.metrics_max_batch_size
    );
    println!("   app_id                     = {:?}", config.app_id);

    println!("\n2. Connecting FileSystemContext (this spawns the heartbeat task)...");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    println!("   ✅ Context connected; HeartbeatTask is running in the background.");

    let master = ctx.acquire_master();

    // ── Part 2: snapshot baseline counters ──────────────────────────────
    //
    // Counters live in a process-global registry. We grab the well-known
    // SDK-managed counters that the io layer increments automatically.
    let bytes_written_local = metrics::counter(metrics::name::CLIENT_BYTES_WRITTEN_LOCAL);
    let bytes_read_local = metrics::counter(metrics::name::CLIENT_BYTES_READ_LOCAL);

    let baseline_written = bytes_written_local.get();
    let baseline_read = bytes_read_local.get();
    println!(
        "\n3. Baseline counters: written_local = {}, read_local = {}",
        baseline_written, baseline_read
    );

    // ── Part 3: a custom application-level counter ──────────────────────
    //
    // Anyone can register additional counters; the heartbeat task picks
    // them up automatically (only non-zero deltas are reported).
    let app_ops = metrics::counter("Client.DemoOpsCount");
    app_ops.inc(1);

    // ── Part 4: drive real I/O so the SDK auto-increments counters ──────
    println!("\n4. Performing I/O to drive the auto-tracked counters...");
    // Cleanup any leftovers from previous runs.
    let _ = master.delete(TEST_FILE, false).await;
    let _ = master.create_directory(TEST_DIR, true).await;

    // Write a non-trivial payload so the delta is visible.
    let payload: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 251) as u8).collect();
    let mut writer = GoosefsFileWriter::create_with_context(ctx.clone(), TEST_FILE, None).await?;
    writer.write(&payload).await?;
    writer.close().await?;
    app_ops.inc(1);
    println!("   ✅ Wrote {} bytes to {}", payload.len(), TEST_FILE);

    // Read it back.
    let read_back = GoosefsFileReader::read_file_with_context(ctx.clone(), TEST_FILE).await?;
    app_ops.inc(1);
    println!(
        "   ✅ Read {} bytes back from {}",
        read_back.len(),
        TEST_FILE
    );
    assert_eq!(read_back.len(), payload.len(), "round-trip size mismatch");

    let after_written = bytes_written_local.get();
    let after_read = bytes_read_local.get();
    println!(
        "\n5. Counters after I/O:   written_local = {} (Δ {}), read_local = {} (Δ {})",
        after_written,
        after_written - baseline_written,
        after_read,
        after_read - baseline_read,
    );
    println!("   Custom counter Client.DemoOpsCount = {}", app_ops.get());

    // ── Part 5: let the heartbeat task report at least once ─────────────
    //
    // With a 2 s interval, sleeping ~3 s guarantees one report cycle.
    println!("\n6. Sleeping 3 s to let the heartbeat task report counter deltas...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("   ✅ Heartbeat reported (see master logs / RUST_LOG=info for details).");

    // ── Part 6: graceful close = final flush ────────────────────────────
    println!("\n7. Closing context (final heartbeat flush is performed)...");
    ctx.close().await?;
    println!("   ✅ Context closed; heartbeat task stopped after final flush.");

    // ── Part 7: metrics_enabled = false (zero overhead) ─────────────────
    //
    // When metrics are disabled the SDK skips spawning the task entirely.
    println!("\n8. Re-connecting with metrics_enabled = false (no heartbeat task)...");
    let off_config = GoosefsConfig::new("127.0.0.1:9200").with_metrics_enabled(false);
    let off_ctx = FileSystemContext::connect(off_config).await?;
    println!("   ✅ Connected without heartbeat task.");
    off_ctx.close().await?;
    println!("   ✅ Closed (no metrics RPC was issued).");

    println!("\n=========================================");
    println!("✅ Metrics & heartbeat demo complete!");
    println!("\nKey takeaways:");
    println!("  • Counters live in a process-global registry — get them via");
    println!("    `metrics::counter(name)` and call `.inc(n)` from anywhere.");
    println!("  • The SDK auto-increments Client.BytesReadLocal /");
    println!("    Client.BytesWrittenLocal during file I/O.");
    println!("  • A single HeartbeatTask per FileSystemContext reports");
    println!("    *delta* (non-zero) counters every `metrics_heartbeat_interval`.");
    println!("  • `close()` performs a final flush; `metrics_enabled = false`");
    println!("    disables the entire pipeline at zero cost.");

    Ok(())
}
