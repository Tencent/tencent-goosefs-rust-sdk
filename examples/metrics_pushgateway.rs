//! Prometheus Pushgateway metrics reporting example.
//!
//! Demonstrates pushing GooseFS client metrics to a local Prometheus Pushgateway.
//!
//! ## Modes
//!
//! - **Default (with Master)**: connects to GooseFS, performs real I/O, then pushes metrics.
//! - **`--no-master`**: pushes simulated metric values without requiring a GooseFS cluster.
//!
//! ## Prerequisites
//!
//! 1. A running Prometheus Pushgateway at `http://127.0.0.1:9091`
//! 2. (Default mode only) A running GooseFS cluster (Master at `127.0.0.1:9200`)
//!
//! ## Usage
//!
//! ```bash
//! # With GooseFS Master
//! cargo run --example metrics_pushgateway
//!
//! # Without GooseFS Master (simulated data)
//! cargo run --example metrics_pushgateway -- --no-master
//! ```
//!
//! After running, check Pushgateway UI at <http://127.0.0.1:9091> to see the metrics.

use std::sync::Arc;
use std::time::Duration;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::metrics;
use goosefs_sdk::metrics::pushgateway::{PushgatewayConfig, PushgatewayTask};

const TEST_DIR: &str = "/pushgateway-demo";
const TEST_FILE: &str = "/pushgateway-demo/payload.bin";

#[tokio::main]
async fn main() -> Result<()> {
    let no_master = std::env::args().any(|a| a == "--no-master");

    if no_master {
        run_without_master().await;
    } else {
        run_with_master().await?;
    }

    Ok(())
}

// ─── Mode 1: simulated metrics, no GooseFS Master required ───────────────────

async fn run_without_master() {
    println!("GooseFS Client — Push All Metrics to Pushgateway (no Master)");
    println!("=============================================================\n");

    // 1. Spawn Pushgateway reporter
    let config = PushgatewayConfig::new("http://127.0.0.1:9091", "goosefs_client")
        .with_instance("rust-client-demo")
        .with_push_interval(Duration::from_secs(5))
        .with_label("env", "local");

    println!("Push URL: {}\n", config.push_url());
    let task = PushgatewayTask::spawn(config);

    // 2. Pre-register all metrics with simulated values
    // Throughput counters
    metrics::counter(metrics::name::CLIENT_BYTES_READ_LOCAL).inc(131072); // 128 KB
    metrics::counter(metrics::name::CLIENT_BYTES_WRITTEN_LOCAL).inc(294912); // 288 KB
    metrics::counter(metrics::name::CLIENT_BYTES_WRITTEN_UFS).inc(65536); // 64 KB

    // RPC operation counters
    metrics::counter(metrics::name::CLIENT_READ_OPS_TOTAL).inc(10);
    metrics::counter(metrics::name::CLIENT_WRITE_OPS_TOTAL).inc(8);
    metrics::counter(metrics::name::CLIENT_GET_STATUS_OPS).inc(42);
    metrics::counter(metrics::name::CLIENT_LIST_STATUS_OPS).inc(15);
    metrics::counter(metrics::name::CLIENT_CREATE_FILE_OPS).inc(6);
    metrics::counter(metrics::name::CLIENT_CREATE_DIR_OPS).inc(3);
    metrics::counter(metrics::name::CLIENT_DELETE_OPS).inc(2);
    metrics::counter(metrics::name::CLIENT_RENAME_OPS).inc(1);

    // Error counters
    metrics::counter(metrics::name::CLIENT_RPC_ERRORS_TOTAL).inc(4);
    metrics::counter(metrics::name::CLIENT_RPC_AUTH_ERRORS).inc(1);
    metrics::counter(metrics::name::CLIENT_RPC_UNAVAILABLE_ERRORS).inc(3);
    metrics::counter(metrics::name::CLIENT_READ_FAILURES).inc(2);
    metrics::counter(metrics::name::CLIENT_WRITE_FAILURES).inc(1);

    // Latency counters (cumulative microseconds)
    metrics::counter(metrics::name::CLIENT_READ_LATENCY_US).inc(52000); // ~52ms total
    metrics::counter(metrics::name::CLIENT_WRITE_LATENCY_US).inc(78000); // ~78ms total
    metrics::counter(metrics::name::CLIENT_GET_STATUS_LATENCY_US).inc(12500); // ~12.5ms total
    metrics::counter(metrics::name::CLIENT_LIST_STATUS_LATENCY_US).inc(35000); // ~35ms total

    // Connection pool gauges
    metrics::gauge(metrics::name::CLIENT_WORKER_CONNECTIONS_ACTIVE).set(3);
    metrics::counter(metrics::name::CLIENT_WORKER_RECONNECTS_TOTAL).inc(5);
    metrics::counter(metrics::name::CLIENT_WORKER_RECONNECTS_COALESCED).inc(2);

    // Block I/O
    metrics::gauge(metrics::name::CLIENT_BLOCKS_READ_IN_PROGRESS).set(1);
    metrics::gauge(metrics::name::CLIENT_BLOCKS_WRITTEN_IN_PROGRESS).set(0);
    metrics::counter(metrics::name::CLIENT_BLOCKS_READ_TOTAL).inc(10);
    metrics::counter(metrics::name::CLIENT_BLOCKS_WRITTEN_TOTAL).inc(8);

    println!("✅ All metrics registered with simulated values.\n");
    println!("Metrics summary:");
    println!("  ── Throughput ──");
    println!(
        "  bytes_read_local          = {}",
        metrics::counter(metrics::name::CLIENT_BYTES_READ_LOCAL).get()
    );
    println!(
        "  bytes_written_local       = {}",
        metrics::counter(metrics::name::CLIENT_BYTES_WRITTEN_LOCAL).get()
    );
    println!(
        "  bytes_written_ufs         = {}",
        metrics::counter(metrics::name::CLIENT_BYTES_WRITTEN_UFS).get()
    );
    println!("  ── RPC Operations ──");
    println!(
        "  read_ops_total            = {}",
        metrics::counter(metrics::name::CLIENT_READ_OPS_TOTAL).get()
    );
    println!(
        "  write_ops_total           = {}",
        metrics::counter(metrics::name::CLIENT_WRITE_OPS_TOTAL).get()
    );
    println!(
        "  get_status_ops            = {}",
        metrics::counter(metrics::name::CLIENT_GET_STATUS_OPS).get()
    );
    println!(
        "  list_status_ops           = {}",
        metrics::counter(metrics::name::CLIENT_LIST_STATUS_OPS).get()
    );
    println!(
        "  create_file_ops           = {}",
        metrics::counter(metrics::name::CLIENT_CREATE_FILE_OPS).get()
    );
    println!(
        "  create_dir_ops            = {}",
        metrics::counter(metrics::name::CLIENT_CREATE_DIR_OPS).get()
    );
    println!(
        "  delete_ops                = {}",
        metrics::counter(metrics::name::CLIENT_DELETE_OPS).get()
    );
    println!(
        "  rename_ops                = {}",
        metrics::counter(metrics::name::CLIENT_RENAME_OPS).get()
    );
    println!("  ── Errors ──");
    println!(
        "  rpc_errors_total          = {}",
        metrics::counter(metrics::name::CLIENT_RPC_ERRORS_TOTAL).get()
    );
    println!(
        "  rpc_auth_errors           = {}",
        metrics::counter(metrics::name::CLIENT_RPC_AUTH_ERRORS).get()
    );
    println!(
        "  rpc_unavailable_errors    = {}",
        metrics::counter(metrics::name::CLIENT_RPC_UNAVAILABLE_ERRORS).get()
    );
    println!(
        "  read_failures             = {}",
        metrics::counter(metrics::name::CLIENT_READ_FAILURES).get()
    );
    println!(
        "  write_failures            = {}",
        metrics::counter(metrics::name::CLIENT_WRITE_FAILURES).get()
    );
    println!("  ── Latency (cumulative μs) ──");
    println!(
        "  read_latency_us           = {}",
        metrics::counter(metrics::name::CLIENT_READ_LATENCY_US).get()
    );
    println!(
        "  write_latency_us          = {}",
        metrics::counter(metrics::name::CLIENT_WRITE_LATENCY_US).get()
    );
    println!(
        "  get_status_latency_us     = {}",
        metrics::counter(metrics::name::CLIENT_GET_STATUS_LATENCY_US).get()
    );
    println!(
        "  list_status_latency_us    = {}",
        metrics::counter(metrics::name::CLIENT_LIST_STATUS_LATENCY_US).get()
    );
    println!("  ── Connection Pool ──");
    println!(
        "  worker_connections_active = {}",
        metrics::gauge(metrics::name::CLIENT_WORKER_CONNECTIONS_ACTIVE).get()
    );
    println!(
        "  worker_reconnects_total   = {}",
        metrics::counter(metrics::name::CLIENT_WORKER_RECONNECTS_TOTAL).get()
    );
    println!(
        "  worker_reconnects_coalesced = {}",
        metrics::counter(metrics::name::CLIENT_WORKER_RECONNECTS_COALESCED).get()
    );
    println!("  ── Block I/O ──");
    println!(
        "  blocks_read_in_progress   = {}",
        metrics::gauge(metrics::name::CLIENT_BLOCKS_READ_IN_PROGRESS).get()
    );
    println!(
        "  blocks_written_in_progress = {}",
        metrics::gauge(metrics::name::CLIENT_BLOCKS_WRITTEN_IN_PROGRESS).get()
    );
    println!(
        "  blocks_read_total         = {}",
        metrics::counter(metrics::name::CLIENT_BLOCKS_READ_TOTAL).get()
    );
    println!(
        "  blocks_written_total      = {}",
        metrics::counter(metrics::name::CLIENT_BLOCKS_WRITTEN_TOTAL).get()
    );

    // 3. Wait for push cycle
    println!("\n⏳ Waiting 6s for Pushgateway push cycle...");
    tokio::time::sleep(Duration::from_secs(6)).await;
    println!("✅ Push cycle complete!");

    // 4. Shutdown
    task.shutdown().await;
    println!("✅ Pushgateway reporter shut down (final push done).");
    println!("\n🔗 Check Pushgateway UI: http://127.0.0.1:9091/#");
}

// ─── Mode 2: real I/O with GooseFS Master ────────────────────────────────────

async fn run_with_master() -> Result<()> {
    println!("GooseFS Client → Prometheus Pushgateway Demo");
    println!("=============================================");

    // ── Step 1: Configure and spawn the Pushgateway reporter ────────────
    println!("\n1. Configuring Pushgateway reporter...");

    let pushgateway_config = PushgatewayConfig::new("http://127.0.0.1:9091", "goosefs_client")
        .with_instance("rust-client-demo")
        .with_push_interval(Duration::from_secs(5))
        .with_label("env", "local");

    println!("   endpoint    = {}", pushgateway_config.endpoint);
    println!("   job         = {}", pushgateway_config.job);
    println!("   instance    = {:?}", pushgateway_config.instance);
    println!("   interval    = {:?}", pushgateway_config.push_interval);
    println!("   push_url    = {}", pushgateway_config.push_url());

    let pushgateway_task: PushgatewayTask = PushgatewayTask::spawn(pushgateway_config);
    println!("   ✅ Pushgateway reporter task started.");

    // ── Step 2: Connect to GooseFS ──────────────────────────────────────
    println!("\n2. Connecting to GooseFS Master...");
    let config = GoosefsConfig::new("127.0.0.1:9200")
        .with_metrics_enabled(true)
        .with_metrics_heartbeat_interval(Duration::from_secs(5))
        .with_metrics_heartbeat_timeout(Duration::from_secs(3))
        .with_app_id("pushgateway-demo");

    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    println!("   ✅ Connected to GooseFS.");

    let master = ctx.acquire_master();

    // ── Step 3: Pre-register all metrics ────────────────────────────────
    let bytes_written = metrics::counter(metrics::name::CLIENT_BYTES_WRITTEN_LOCAL);
    let bytes_read = metrics::counter(metrics::name::CLIENT_BYTES_READ_LOCAL);
    metrics::counter(metrics::name::CLIENT_BYTES_WRITTEN_UFS);

    metrics::counter(metrics::name::CLIENT_READ_OPS_TOTAL);
    metrics::counter(metrics::name::CLIENT_WRITE_OPS_TOTAL);
    metrics::counter(metrics::name::CLIENT_GET_STATUS_OPS);
    metrics::counter(metrics::name::CLIENT_LIST_STATUS_OPS);
    metrics::counter(metrics::name::CLIENT_CREATE_FILE_OPS);
    metrics::counter(metrics::name::CLIENT_CREATE_DIR_OPS);
    metrics::counter(metrics::name::CLIENT_DELETE_OPS);
    metrics::counter(metrics::name::CLIENT_RENAME_OPS);

    metrics::counter(metrics::name::CLIENT_RPC_ERRORS_TOTAL);
    metrics::counter(metrics::name::CLIENT_RPC_AUTH_ERRORS);
    metrics::counter(metrics::name::CLIENT_RPC_UNAVAILABLE_ERRORS);
    metrics::counter(metrics::name::CLIENT_READ_FAILURES);
    metrics::counter(metrics::name::CLIENT_WRITE_FAILURES);

    metrics::counter(metrics::name::CLIENT_READ_LATENCY_US);
    metrics::counter(metrics::name::CLIENT_WRITE_LATENCY_US);
    metrics::counter(metrics::name::CLIENT_GET_STATUS_LATENCY_US);
    metrics::counter(metrics::name::CLIENT_LIST_STATUS_LATENCY_US);

    metrics::gauge(metrics::name::CLIENT_WORKER_CONNECTIONS_ACTIVE);
    metrics::counter(metrics::name::CLIENT_WORKER_RECONNECTS_TOTAL);
    metrics::counter(metrics::name::CLIENT_WORKER_RECONNECTS_COALESCED);

    metrics::gauge(metrics::name::CLIENT_BLOCKS_READ_IN_PROGRESS);
    metrics::gauge(metrics::name::CLIENT_BLOCKS_WRITTEN_IN_PROGRESS);
    metrics::counter(metrics::name::CLIENT_BLOCKS_READ_TOTAL);
    metrics::counter(metrics::name::CLIENT_BLOCKS_WRITTEN_TOTAL);

    println!(
        "\n3. Pre-registered all metrics. Baseline: written_local = {}, read_local = {}",
        bytes_written.get(),
        bytes_read.get()
    );

    // ── Step 4: Register custom application counters ────────────────────
    let ops_counter = metrics::counter("Client.DemoOpsTotal");
    let active_gauge = metrics::gauge("Client.DemoActiveConnections");
    active_gauge.set(1);
    println!("\n4. Registered custom metrics:");
    println!("   - Client.DemoOpsTotal (counter)");
    println!("   - Client.DemoActiveConnections (gauge = 1)");

    // ── Step 5: Perform I/O to generate real metrics ────────────────────
    println!("\n5. Performing file I/O...");

    // Cleanup
    let _ = master.delete(TEST_FILE, false).await;
    let _ = master.create_directory(TEST_DIR, true).await;

    // Write
    let payload: Vec<u8> = (0..(128 * 1024)).map(|i| (i % 251) as u8).collect();
    let mut writer = GoosefsFileWriter::create_with_context(ctx.clone(), TEST_FILE, None).await?;
    writer.write(&payload).await?;
    writer.close().await?;
    ops_counter.inc(1);
    println!("   ✅ Wrote {} bytes", payload.len());

    // Read
    let data = GoosefsFileReader::read_file_with_context(ctx.clone(), TEST_FILE).await?;
    ops_counter.inc(1);
    println!("   ✅ Read {} bytes", data.len());

    // Update gauge
    active_gauge.set(2);
    ops_counter.inc(1);

    println!(
        "\n6. Counters after I/O: written = {}, read = {}, ops = {}, active = {}",
        bytes_written.get(),
        bytes_read.get(),
        ops_counter.get(),
        active_gauge.get()
    );

    // ── Step 6: Wait for the Pushgateway reporter to fire ───────────────
    println!("\n7. Waiting 6s for Pushgateway reporter to push metrics...");
    println!("   (Check http://127.0.0.1:9091 to see the metrics)");
    tokio::time::sleep(Duration::from_secs(6)).await;
    println!("   ✅ At least one push cycle should have completed.");

    // Do more I/O to see updated values on next push
    println!("\n8. Performing more I/O for a second push cycle...");
    for i in 0..5 {
        let path = format!("{}/file_{}.dat", TEST_DIR, i);
        let data: Vec<u8> = vec![i as u8; 32 * 1024];
        let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), &path, None).await?;
        w.write(&data).await?;
        w.close().await?;
        ops_counter.inc(1);
    }
    active_gauge.set(5);
    println!("   ✅ Wrote 5 additional files (160 KB total).");
    println!(
        "   Updated counters: ops = {}, active = {}",
        ops_counter.get(),
        active_gauge.get()
    );

    println!("\n9. Waiting 6s for second push cycle...");
    tokio::time::sleep(Duration::from_secs(6)).await;
    println!("   ✅ Second push completed. Check Pushgateway UI for updated values.");

    // ── Step 7: Shutdown ────────────────────────────────────────────────
    println!("\n10. Shutting down...");
    pushgateway_task.shutdown().await;
    println!("    ✅ Pushgateway reporter stopped (final push performed).");

    ctx.close().await?;
    println!("    ✅ GooseFS context closed.");

    // Cleanup test files
    for i in 0..5 {
        let path = format!("{}/file_{}.dat", TEST_DIR, i);
        let _ = master.delete(&path, false).await;
    }
    let _ = master.delete(TEST_FILE, false).await;
    let _ = master.delete(TEST_DIR, false).await;

    println!("\n=============================================");
    println!("✅ Pushgateway demo complete!");
    println!("\nMetrics visible at Pushgateway:");
    println!("  ── Throughput ──");
    println!("  • goosefs_client_bytes_read_local");
    println!("  • goosefs_client_bytes_written_local");
    println!("  ── RPC Operations ──");
    println!("  • goosefs_client_get_status_ops");
    println!("  • goosefs_client_list_status_ops");
    println!("  • goosefs_client_create_file_ops");
    println!("  • goosefs_client_create_dir_ops");
    println!("  • goosefs_client_delete_ops");
    println!("  • goosefs_client_rename_ops");
    println!("  ── Latency ──");
    println!("  • goosefs_client_get_status_latency_us");
    println!("  • goosefs_client_list_status_latency_us");
    println!("  ── Errors ──");
    println!("  • goosefs_client_rpc_errors_total");
    println!("  • goosefs_client_rpc_auth_errors");
    println!("  • goosefs_client_rpc_unavailable_errors");
    println!("  ── Block I/O ──");
    println!("  • goosefs_client_blocks_read_total");
    println!("  • goosefs_client_blocks_written_total");
    println!("  • goosefs_client_blocks_read_in_progress");
    println!("  • goosefs_client_blocks_written_in_progress");
    println!("  ── Connection Pool ──");
    println!("  • goosefs_client_worker_reconnects_total");
    println!("  • goosefs_client_worker_reconnects_coalesced");
    println!("  ── Custom ──");
    println!("  • goosefs_client_demo_ops_total");
    println!("  • goosefs_client_demo_active_connections");
    println!("\nPushgateway UI: http://127.0.0.1:9091/#");

    Ok(())
}
