---
sidebar_position: 12
---

# Metrics

The GooseFS Python client can export Prometheus-compatible metrics via a background heartbeat to a Pushgateway or by exposing a scrape endpoint. Metrics are **disabled by default**.

## Enabling

```bash
# Environment variables
export GOOSEFS_METRICS_ENABLED=true
```

```properties
# goosefs-site.properties
goosefs.user.metrics.enabled=true
```

```python
from goosefs import Config

# Metrics are configured at the Config level, inherited from env/properties.
cfg = Config("127.0.0.1:9200")
# No additional Python-side code needed — the background heartbeat
# starts automatically when the FileSystemContext is connected.
```

## Available Metrics

| Metric                              | Type      | Description                              |
| ----------------------------------- | --------- | ---------------------------------------- |
| `goosefs_master_rpc_count`          | counter   | Total Master RPCs sent                   |
| `goosefs_master_rpc_latency_ms`     | histogram | Per-RPC latency (Master)                 |
| `goosefs_worker_rpc_count`          | counter   | Total Worker RPCs sent                   |
| `goosefs_worker_rpc_latency_ms`     | histogram | Per-RPC latency (Worker)                 |
| `goosefs_short_circuit_count`       | counter   | Short-circuit reads attempted            |
| `goosefs_short_circuit_fallback`    | counter   | Short-circuit reads that fell back to gRPC |
| `goosefs_client_cache_hit`          | counter   | Page cache hits                          |
| `goosefs_client_cache_miss`         | counter   | Page cache misses                        |
| `goosefs_client_cache_eviction`     | counter   | Page cache evictions                     |

## Pushgateway

```bash
export GOOSEFS_METRICS_ENABLED=true
export GOOSEFS_METRICS_PUSHGATEWAY_ADDR=http://pushgateway:9091
export GOOSEFS_METRICS_HEARTBEAT_INTERVAL_SECS=15
```

The client sends metrics to the Pushgateway at the configured interval. Use Prometheus to scrape the Pushgateway.

## Observability Without Metrics

For lightweight debugging without a metrics backend, set `RUST_LOG`:

```bash
# See all RPCs
RUST_LOG=goosefs_sdk::client=debug python your_script.py

# See only cache activity
RUST_LOG=goosefs_sdk::cache=debug python your_script.py

# See everything
RUST_LOG=goosefs_sdk=debug python your_script.py
```
