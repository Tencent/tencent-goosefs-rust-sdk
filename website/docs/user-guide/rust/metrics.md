---
sidebar_position: 6
---

# Metrics

When `metrics_enabled = true` (the default), each `FileSystemContext` spawns a background `HeartbeatTask` that periodically reports **incremental counter deltas** to the GooseFS Master via `MetricsHeartbeat`. Optionally, metrics can also be pushed to a Prometheus Pushgateway.

```rust
use std::sync::Arc;
use std::time::Duration;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::metrics;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let mut config = GoosefsConfig::new("127.0.0.1:9200");
    config.metrics_enabled = true;
    config.metrics_heartbeat_interval = Duration::from_secs(10);
    // Optional Pushgateway (requires default feature `metrics-pushgateway`)
    // config.metrics_pushgateway_endpoint = Some("http://127.0.0.1:9091".into());

    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;

    // Application counters / gauges via the global registry
    let counter = metrics::counter("Client.AppOps");
    counter.inc();

    ctx.close().await?;
    Ok(())
}
```

## Reporting Channels

1. **Heartbeat** — cluster-aggregated metrics to Master (`isClusterAggregated=true`)
2. **Pushgateway** — all metrics via HTTP POST for Prometheus/Grafana

Java-compatible cluster-aggregated counters include `Client.BytesReadLocal`, `Client.BytesWrittenLocal`, and `Client.BytesWrittenUfs`. The Rust client also exports RPC op counts, error counters, latency cumulatives, and `Client.Cache*` page-cache metrics.

Full catalogue: [`docs/METRICS.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/METRICS.md).

Examples:

```bash
cargo run --example metrics_heartbeat
cargo run --example metrics_pushgateway
```
