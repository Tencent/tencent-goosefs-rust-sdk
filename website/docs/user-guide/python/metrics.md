---
sidebar_position: 12
---

# Metrics

The GooseFS Python client reports metrics to the master through a heartbeat enabled by default. An optional Prometheus Pushgateway exporter is disabled by default; the client does not expose a scrape endpoint.

## Configuration

```bash
# Environment variables
export GOOSEFS_USER_METRICS_COLLECTION_ENABLED=true       # default: true
export GOOSEFS_USER_METRICS_HEARTBEAT_INTERVAL_MS=10000  # default: 10000 (10s)
```

```properties
# goosefs-site.properties
goosefs.user.metrics.collection.enabled=true
goosefs.user.metrics.heartbeat.interval.ms=10000
```

```python
from goosefs import Config

# Metrics are configured at the Config level, inherited from env/properties.
cfg = Config("127.0.0.1:9200")
# No additional Python-side code needed — the background heartbeat
# starts automatically when the FileSystemContext is connected.
```

## Pushgateway

To push metrics to a Prometheus Pushgateway:

```bash
export GOOSEFS_METRICS_PUSHGATEWAY_ENABLED=true
export GOOSEFS_METRICS_PUSHGATEWAY_ENDPOINT=http://pushgateway:9091
export GOOSEFS_METRICS_PUSHGATEWAY_PUSH_INTERVAL_MS=15000
export GOOSEFS_METRICS_PUSHGATEWAY_JOB=goosefs-client
export GOOSEFS_METRICS_PUSHGATEWAY_INSTANCE=$(hostname)
```

The client pushes metrics to the Pushgateway at the configured interval. Use Prometheus to scrape the Pushgateway.

## Observability Without Metrics

For lightweight debugging without a metrics backend, call `goosefs.enable_tracing()` near script startup to install the tracing subscriber, then set `RUST_LOG`:

```bash
# See all RPCs
RUST_LOG=goosefs_sdk::client=debug python your_script.py

# See only cache activity
RUST_LOG=goosefs_sdk::cache=debug python your_script.py

# See everything
RUST_LOG=goosefs_sdk=debug python your_script.py
```
