# GooseFS Rust Client Metrics Documentation

This document describes all metrics reported by the GooseFS Rust Client to Prometheus Pushgateway, including metrics aligned with the Java Client and additional metrics introduced by the Rust Client.

## Metrics Reporting Channels

The Rust Client supports two metrics reporting channels:

1. **Heartbeat Reporting**: Reports metrics with `isClusterAggregated=true` to the GooseFS Master via the Master Heartbeat RPC for cluster-level aggregation.
2. **Pushgateway Reporting**: Pushes all metrics to Prometheus Pushgateway via HTTP POST, enabling Prometheus/Grafana visualization and alerting.

---

## 1. Java Client Compatible Metrics (Cluster Aggregated)

The following 3 metrics are aligned with the Java Client's `MetricKey` definitions and are reported to the Master via heartbeat for cluster aggregation:

| Internal Name | Prometheus Metric Name | Type | Cluster Aggregated | Description |
|---------|-------------------|------|:--------:|------|
| `Client.BytesReadLocal` | `goosefs_client_bytes_read_local` | Counter | ✅ | Bytes read via local short-circuit (client reads directly from local worker) |
| `Client.BytesWrittenLocal` | `goosefs_client_bytes_written_local` | Counter | ✅ | Bytes written via local short-circuit (client writes directly to local worker) |
| `Client.BytesWrittenUfs` | `goosefs_client_bytes_written_ufs` | Counter | ✅ | Bytes written directly to UFS (bypassing GooseFS cache layer) |

> **Note**: These metrics are reported via `FileSystemMasterClient.heartbeat()` in the Java Client. The Rust Client follows the same mechanism.

---

## 2. Rust Client Additional Metrics

The following metrics are introduced by the Rust Client to address the Java Client's lack of latency, error rate, operation count, and other analytical dimensions beyond throughput.

### 2.1 RPC Operation Counts

Tracks the invocation count of each Master RPC, useful for identifying hot operations and traffic patterns.

| Internal Name | Prometheus Metric Name | Type | Description |
|---------|-------------------|------|------|
| `Client.ReadOpsTotal` | `goosefs_client_read_ops_total` | Counter | File read operation count |
| `Client.WriteOpsTotal` | `goosefs_client_write_ops_total` | Counter | File write operation count |
| `Client.GetStatusOps` | `goosefs_client_get_status_ops` | Counter | getStatus (stat) RPC count |
| `Client.ListStatusOps` | `goosefs_client_list_status_ops` | Counter | listStatus (ls) RPC count |
| `Client.CreateFileOps` | `goosefs_client_create_file_ops` | Counter | createFile RPC count |
| `Client.CreateDirOps` | `goosefs_client_create_dir_ops` | Counter | createDirectory RPC count |
| `Client.DeleteOps` | `goosefs_client_delete_ops` | Counter | delete RPC count |
| `Client.RenameOps` | `goosefs_client_rename_ops` | Counter | rename RPC count |

**Use Cases**:
- `rate(goosefs_client_get_status_ops[5m])` — stat calls per second
- Detect "ls storms" (abnormally fast growth of `list_status_ops`)

### 2.2 Error / Failure Counts

Tracks various RPC errors for quick diagnosis of network issues, authentication problems, or server-side anomalies.

| Internal Name | Prometheus Metric Name | Type | Description |
|---------|-------------------|------|------|
| `Client.RpcErrorsTotal` | `goosefs_client_rpc_errors_total` | Counter | Total RPC errors (including pre-retry failures) |
| `Client.RpcAuthErrors` | `goosefs_client_rpc_auth_errors` | Counter | gRPC UNAUTHENTICATED errors (authentication failure) |
| `Client.RpcUnavailableErrors` | `goosefs_client_rpc_unavailable_errors` | Counter | gRPC UNAVAILABLE errors (connection refused / network unreachable) |
| `Client.ReadFailures` | `goosefs_client_read_failures` | Counter | Block read failures (stream errors, incomplete reads, etc.) |
| `Client.WriteFailures` | `goosefs_client_write_failures` | Counter | Block write failures |

**Use Cases**:
- `goosefs_client_rpc_errors_total / sum(all_ops)` — error rate alerting
- `rpc_unavailable_errors` growing → network partition or Master down
- `rpc_auth_errors` growing → expired credentials or misconfiguration

### 2.3 Latency Metrics (Cumulative Microseconds)

Tracks cumulative duration of various operations; combined with operation counts to compute average latency.

| Internal Name | Prometheus Metric Name | Type | Description |
|---------|-------------------|------|------|
| `Client.ReadLatencyUs` | `goosefs_client_read_latency_us` | Counter | Cumulative read latency (μs) |
| `Client.WriteLatencyUs` | `goosefs_client_write_latency_us` | Counter | Cumulative write latency (μs) |
| `Client.GetStatusLatencyUs` | `goosefs_client_get_status_latency_us` | Counter | Cumulative getStatus RPC latency (μs) |
| `Client.ListStatusLatencyUs` | `goosefs_client_list_status_latency_us` | Counter | Cumulative listStatus RPC latency (μs) |

**Use Cases**:
- **Average latency** = `rate(latency_us[5m]) / rate(ops[5m])`
- Example: `rate(goosefs_client_get_status_latency_us[5m]) / rate(goosefs_client_get_status_ops[5m])` = average getStatus duration (μs)

### 2.4 Connection Pool Metrics

Tracks the health and reconnection behavior of the Worker connection pool.

| Internal Name | Prometheus Metric Name | Type | Description |
|---------|-------------------|------|------|
| `Client.WorkerConnectionsActive` | `goosefs_client_worker_connections_active` | Gauge | Current number of cached active Worker connections |
| `Client.WorkerReconnectsTotal` | `goosefs_client_worker_reconnects_total` | Counter | Actual Worker reconnection count |
| `Client.WorkerReconnectsCoalesced` | `goosefs_client_worker_reconnects_coalesced` | Counter | Coalesced (deduplicated) reconnection requests |

**Use Cases**:
- `reconnects_total` continuously growing → unstable network or frequent Worker restarts
- `reconnects_coalesced / reconnects_total` — reconnection deduplication efficiency
- `worker_connections_active` — connection pool capacity planning

### 2.5 Block I/O Metrics

Tracks block-level read/write concurrency and completion counts.

| Internal Name | Prometheus Metric Name | Type | Description |
|---------|-------------------|------|------|
| `Client.BlocksReadInProgress` | `goosefs_client_blocks_read_in_progress` | Gauge | Number of blocks currently being read concurrently |
| `Client.BlocksWrittenInProgress` | `goosefs_client_blocks_written_in_progress` | Gauge | Number of blocks currently being written concurrently |
| `Client.BlocksReadTotal` | `goosefs_client_blocks_read_total` | Counter | Total blocks successfully read |
| `Client.BlocksWrittenTotal` | `goosefs_client_blocks_written_total` | Counter | Total blocks successfully written |

**Use Cases**:
- `blocks_read_in_progress` / `blocks_written_in_progress` — observe concurrent I/O depth
- `rate(blocks_read_total[5m])` — block reads completed per second (IOPS dimension)

---

### 2.6 Short-Circuit Read Metrics

Tracks the local-mmap ("short-circuit", SC) read path
([`docs/SHORT_CIRCUIT_DESIGN.md`](SHORT_CIRCUIT_DESIGN.md)). Two
disjoint layers:

**(a) Fine-grained per-step counters** (already present, exported here for completeness):

| Internal Name | Type | Description |
|---|---|---|
| `Client.ShortCircuitOpenSuccess` | Counter | Successful `OpenLocalBlock` + mmap sessions |
| `Client.ShortCircuitOpenLocalFail` | Counter | `OpenLocalBlock` RPC failures (block not local / IO / auth) |
| `Client.ShortCircuitFileOpenFail` | Counter | `File::open` failures on the local block path (e.g. EACCES) |
| `Client.ShortCircuitMmapFail` | Counter | `Mmap::map` failures (ENOMEM / EINVAL) |
| `Client.ShortCircuitReadCalls` | Counter | Number of SC `read` / `read_bytes` / `read_to_slice` calls |
| `Client.ShortCircuitReadBytes` | Counter | Total bytes served from the SC (mmap) path |
| `Client.ShortCircuitCacheHits` | Counter | Factory LRU reader-cache hits |
| `Client.ShortCircuitCacheEvictions` | Counter | Factory LRU reader-cache evictions |
| `Client.ShortCircuitNegCacheHits` | Counter | Negative-cache hits (recently-failed block → SC skipped) |
| `Client.ShortCircuitActiveReaders` | Gauge | Currently-live SC readers |
| `Client.ShortCircuitPrefetchCalls` | Counter | `prefetch` / `prefetch_many` calls |
| `Client.ShortCircuitPrefetchBytes` | Counter | Cumulative bytes requested for prefetch |
| `Client.ShortCircuitPrefetchMadvise` | Counter | Actual `madvise(WILLNEED)` syscalls issued (after coalescing) |

**(b) Top-level decision histogram** (added per FLAMEGRAPH_OPTIMIZATION_PLAN §B1)
— five enum-tagged counters exposing the caller-visible SC outcome for each
positioned/random read attempt:

| Internal Name | Type | Description |
|---|---|---|
| `Client.ShortCircuitDecisionHit` | Counter | SC actually served the read (zero-copy mmap slice). Hit-rate numerator. |
| `Client.ShortCircuitDecisionSkipped` | Counter | SC not attempted — pre-filter (`should_use`) rejected the block: SC disabled by config, block source not local, block size below threshold, block on the negative cache, or the reader has no SC factory attached. |
| `Client.ShortCircuitDecisionFallbackOpen` | Counter | SC attempted but the **open** step failed and the read fell back to gRPC. Break down the cause via `ShortCircuitOpenLocalFail` / `ShortCircuitFileOpenFail` / `ShortCircuitMmapFail`. |
| `Client.ShortCircuitDecisionFallbackRead` | Counter | SC opened successfully but a subsequent **read** failed with a recoverable error and this individual read fell back to gRPC. |
| `Client.ShortCircuitDecisionSemanticError` | Counter | SC read produced a semantic error (`OutOfRange`) that must be surfaced unchanged (INV-S4). Should stay at `0` on healthy deployments. |

**Hit-rate calculation** (Prometheus / PromQL):

```promql
sum(rate(Client_ShortCircuitDecisionHit[5m]))
/
sum(rate(Client_ShortCircuitDecisionHit[5m]))
+ sum(rate(Client_ShortCircuitDecisionSkipped[5m]))
+ sum(rate(Client_ShortCircuitDecisionFallbackOpen[5m]))
+ sum(rate(Client_ShortCircuitDecisionFallbackRead[5m]))
```

The FLAMEGRAPH_OPTIMIZATION_PLAN §B1 target is `≥ 0.95` on the profiling
host. If hit-rate is low, use the fine-grained counters in (a) to
identify the top fallback reason and drive per-cause fixes.

**Scope**: the decision histogram covers the **positioned / random** read
path (`GoosefsFileReader::next_read_bytes`, `GoosefsFileInStream::read_at`),
which is the workload the flame graph is dominated by. The **sequential**
read path decides SC once per block and reuses the mmap slice for every
chunk, so it is intentionally excluded from this histogram — its throughput
remains observable via `ShortCircuitReadCalls` / `ShortCircuitReadBytes`.

---

## 3. Metric Naming Convention

### Internal Name → Prometheus Name Conversion Rules

```
Client.BytesReadLocal  →  goosefs_client_bytes_read_local
```

Conversion rules:
1. Add `goosefs_` prefix
2. Replace `.` with `_`
3. Convert CamelCase to snake_case (insert `_` before uppercase letters)
4. Convert everything to lowercase

### Type Descriptions

- **Counter**: Monotonically increasing cumulative value. Prometheus automatically computes `rate()` and `increase()`.
- **Gauge**: Value that can increase or decrease. Reflects current state.

---

## 4. Grafana Alert Rule Suggestions

```yaml
# Error rate alert (error rate > 5% over 5 minutes)
- alert: GooseFSClientHighErrorRate
  expr: |
    rate(goosefs_client_rpc_errors_total[5m])
    / (rate(goosefs_client_get_status_ops[5m]) + rate(goosefs_client_list_status_ops[5m]) + rate(goosefs_client_create_file_ops[5m]) + rate(goosefs_client_delete_ops[5m]) + rate(goosefs_client_rename_ops[5m]))
    > 0.05
  for: 3m
  labels:
    severity: warning

# getStatus average latency alert (> 100ms)
- alert: GooseFSClientHighGetStatusLatency
  expr: |
    rate(goosefs_client_get_status_latency_us[5m]) / rate(goosefs_client_get_status_ops[5m]) > 100000
  for: 5m
  labels:
    severity: warning

# Frequent Worker reconnection alert
- alert: GooseFSClientFrequentReconnects
  expr: rate(goosefs_client_worker_reconnects_total[5m]) > 1
  for: 3m
  labels:
    severity: warning
```

---

## 5. Pushgateway Configuration

Pushgateway push is disabled by default and must be explicitly enabled via configuration. Three configuration methods are supported:

### 5.1 Configuration Fields

| Field | Type | Default | Description |
|------|------|--------|------|
| `pushgateway_enabled` | bool | `false` | Whether to enable Pushgateway push |
| `pushgateway_endpoint` | String | `"http://127.0.0.1:9091"` | Pushgateway address |
| `pushgateway_push_interval` | Duration | `10s` | Push interval |
| `pushgateway_job` | String | `"goosefs_client"` | Prometheus job label |
| `pushgateway_instance` | Option\<String\> | `None` | Prometheus instance label (auto-assigned by Pushgateway based on IP if not set) |

### 5.2 Code Configuration (Builder Pattern)

```rust
use std::time::Duration;
use goosefs_sdk::config::GoosefsConfig;

let config = GoosefsConfig::new("10.0.0.1:9200")
    .with_pushgateway_enabled(true)
    .with_pushgateway_endpoint("http://10.0.0.2:9091")
    .with_pushgateway_push_interval(Duration::from_secs(15))
    .with_pushgateway_job("my_service")
    .with_pushgateway_instance("host-001");
```

### 5.3 Environment Variable Configuration

| Environment Variable | Description | Example |
|---------|------|------|
| `GOOSEFS_USER_METRICS_COLLECTION_ENABLED` | Master switch: enable metrics collection + heartbeat | `true` / `false` |
| `GOOSEFS_USER_METRICS_HEARTBEAT_INTERVAL_MS` | Heartbeat interval to the Master (milliseconds, `≥ 1000`) | `10000` |
| `GOOSEFS_USER_APP_ID` | Client tag attached to every heartbeat | `my_service` |
| `GOOSEFS_METRICS_PUSHGATEWAY_ENABLED` | Enable Prometheus Pushgateway push | `true` |
| `GOOSEFS_METRICS_PUSHGATEWAY_ENDPOINT` | Pushgateway address | `http://10.0.0.2:9091` |
| `GOOSEFS_METRICS_PUSHGATEWAY_PUSH_INTERVAL_MS` | Push interval (milliseconds) | `15000` |
| `GOOSEFS_METRICS_PUSHGATEWAY_JOB` | job label | `my_service` |
| `GOOSEFS_METRICS_PUSHGATEWAY_INSTANCE` | instance label | `host-001` |

```bash
# Disable both master heartbeat and pushgateway from the shell:
export GOOSEFS_USER_METRICS_COLLECTION_ENABLED=false

# Or enable pushgateway only:
export GOOSEFS_METRICS_PUSHGATEWAY_ENABLED=true
export GOOSEFS_METRICS_PUSHGATEWAY_ENDPOINT=http://10.0.0.2:9091
cargo run --example metrics_pushgateway
```

> Environment variables are overlaid on top of `goosefs-site.properties`
> and the `properties=` dict, so an operator can always disable the
> heartbeat without touching application code.

### 5.4 Properties File Configuration (`goosefs-site.properties`)

```properties
# Master switch for the metrics heartbeat pipeline (default: true).
goosefs.user.metrics.collection.enabled=true
goosefs.user.metrics.heartbeat.interval=10000

# Optional Prometheus Pushgateway sink.
goosefs.metrics.pushgateway.enabled=true
goosefs.metrics.pushgateway.endpoint=http://10.0.0.2:9091
goosefs.metrics.pushgateway.push.interval=15000
goosefs.metrics.pushgateway.job=my_service
goosefs.metrics.pushgateway.instance=host-001
```

### 5.5 Activation Timing

When `pushgateway_enabled=true`, `FileSystemContext::connect()` automatically spawns a `PushgatewayTask` in the background — no additional code required. On `close()`, the task is gracefully shut down with a final push.

> **Note**: `pushgateway_enabled` and `metrics_enabled` are two independent switches:
> - `metrics_enabled` controls reporting metrics to the Master via Heartbeat
> - `pushgateway_enabled` controls pushing metrics to the Prometheus Pushgateway
> - Both can be enabled simultaneously without interference

---

## 6. Python SDK Metrics Support

The Python SDK (`goosefs` package) is a PyO3 binding built directly on the Rust SDK. It shares the same `FileSystemContext` internally, which means **all 27 metrics, heartbeat reporting, and Pushgateway push are fully supported** without any additional Python-side code.

### 6.1 How It Works

When you call `Goosefs(config)` (sync) or `await AsyncGoosefs.connect(config)` (async), the underlying Rust `FileSystemContext::connect()` is invoked, which automatically:

1. Starts the **metrics heartbeat task** (when `metrics_enabled = true`) — periodically reports cluster-aggregated metrics to the GooseFS Master.
2. Starts the **Pushgateway push task** (when `pushgateway_enabled = true`) — periodically pushes all 27 metrics to Prometheus Pushgateway.

On `close()` (or exiting a `with` / `async with` block), both tasks are gracefully shut down with a final flush.

### 6.2 Configuration via Properties Dict

```python
from goosefs import Config, Goosefs

cfg = Config("10.0.0.1:9200", properties={
    # Enable heartbeat reporting to Master
    "goosefs.user.metrics.collection.enabled": "true",
    # Enable Pushgateway push
    "goosefs.metrics.pushgateway.enabled": "true",
    "goosefs.metrics.pushgateway.endpoint": "http://10.0.0.2:9091",
    "goosefs.metrics.pushgateway.push.interval": "15000",
    "goosefs.metrics.pushgateway.job": "my_python_service",
    "goosefs.metrics.pushgateway.instance": "host-001",
})

with Goosefs(cfg) as fs:
    # Metrics are automatically collected and reported in the background
    data = fs.read_file("/data/test.txt")
```

### 6.3 Configuration via Properties File

```python
from goosefs import Config, Goosefs

# Load all settings (including metrics) from goosefs-site.properties
cfg = Config.from_properties_file("/etc/goosefs/goosefs-site.properties")

with Goosefs(cfg) as fs:
    status = fs.get_status("/data")
```

### 6.4 Configuration via Environment Variables

```bash
export GOOSEFS_METRICS_PUSHGATEWAY_ENABLED=true
export GOOSEFS_METRICS_PUSHGATEWAY_ENDPOINT=http://10.0.0.2:9091
export GOOSEFS_METRICS_PUSHGATEWAY_PUSH_INTERVAL_MS=15000
python my_app.py
```

### 6.5 Async Usage

```python
import asyncio
from goosefs import Config, AsyncGoosefs

async def main():
    cfg = Config("10.0.0.1:9200", properties={
        "goosefs.user.metrics.collection.enabled": "true",
        "goosefs.metrics.pushgateway.enabled": "true",
        "goosefs.metrics.pushgateway.endpoint": "http://10.0.0.2:9091",
    })

    async with await AsyncGoosefs.connect(cfg) as fs:
        data = await fs.read_file("/data/test.txt")
        # Metrics (read ops, bytes read, latency, etc.) are tracked automatically

asyncio.run(main())
```

### 6.6 Inspecting Metrics Configuration

```python
cfg = Config("10.0.0.1:9200", properties={
    "goosefs.user.metrics.collection.enabled": "true",
})

print(cfg.metrics_enabled)  # True — heartbeat reporting is enabled
```

### 6.7 Feature Matrix

| Feature | Python SDK | Notes |
|---------|:---:|-------|
| All 27 metrics collection | ✅ | Same metrics as Rust SDK |
| Heartbeat reporting to Master | ✅ | `metrics_enabled = true` |
| Pushgateway push | ✅ | `pushgateway_enabled = true` |
| Graceful shutdown with final flush | ✅ | On `close()` or `with` block exit |
| Properties dict configuration | ✅ | Same keys as `goosefs-site.properties` |
| Properties file configuration | ✅ | `Config.from_properties_file(path)` |
| Environment variable configuration | ✅ | Same env vars as Rust SDK |

> **Note**: The Python SDK does not require any additional dependencies or setup for metrics. The metrics infrastructure is entirely handled by the underlying Rust runtime — Python users simply configure and connect.

---

## 7. Verification

### Quick Verification Without GooseFS Master

```bash
# Start Pushgateway
docker run -d -p 9091:9091 prom/pushgateway

# Push all metrics (simulated data, no GooseFS Master required)
cargo run --example metrics_pushgateway -- --no-master

# Open Pushgateway UI
open http://127.0.0.1:9091/#
```

### Full Verification With GooseFS Cluster

```bash
# Ensure GooseFS Master is running at 127.0.0.1:9200
cargo run --example metrics_pushgateway
```

### Enable Pushgateway Push via Environment Variables

```bash
# No code changes needed — environment variables are sufficient
GOOSEFS_METRICS_PUSHGATEWAY_ENABLED=true cargo run --example metrics_pushgateway
```

---

## 8. Metrics Summary

| Category | Count | Source |
|------|:--------:|------|
| Throughput (Java-aligned) | 3 | Aligned with Java MetricKey |
| RPC Operation Counts | 8 | Rust addition |
| Error/Failure Counts | 5 | Rust addition |
| Latency | 4 | Rust addition |
| Connection Pool | 3 | Rust addition |
| Block I/O | 4 | Rust addition |
| **Total** | **27** | — |

> The Java Client originally reports only 3 throughput metrics. The Rust Client adds 24 additional metrics covering operation counts, error monitoring, latency analysis, connection pool health, and Block I/O concurrency depth.
