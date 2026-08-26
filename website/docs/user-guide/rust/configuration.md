---
sidebar_position: 3
---

# Configuration

The client loads configuration from multiple sources. When the same parameter is set in multiple places, the **highest-priority** source wins:

```text
Priority (highest → lowest):

  1. Environment variables (GOOSEFS_*)
  2. Properties config file (goosefs-site.properties)
  3. Built-in defaults
```

Use `GoosefsConfig::from_properties_auto()` to apply the full priority chain. When you construct a context via `FileSystemContext::connect(config)`, a background `ConfigRefresher` is started automatically (default interval 60s) and reloads transparent-acceleration switches from properties/env.

## Minimal Setup

```rust
use goosefs_sdk::config::GoosefsConfig;

// Single master
let config = GoosefsConfig::new("127.0.0.1:9200");

// Or discover from env / properties
let config = GoosefsConfig::from_properties_auto()?;
```

Common environment variables:

| Variable                         | Purpose                                       |
| -------------------------------- | --------------------------------------------- |
| `GOOSEFS_MASTER_ADDR`            | Master host:port (or comma-separated HA list) |
| `GOOSEFS_AUTH_TYPE`              | `nosasl` / `simple` / …                       |
| `GOOSEFS_USER`                   | Username for SIMPLE auth                      |
| `GOOSEFS_CONF` / properties path | Location of `goosefs-site.properties`         |
| `GOOSEFS_USER_FILE_REPLICATION_NUMBER` | Block-worker selection count (default `1`) |
| `GOOSEFS_USER_FILE_REPLICATION_DURABLE` | ASYNC_THROUGH replica target before persist (default `2`) |
| `GOOSEFS_USER_FILE_REPLICATION_DURABLE_MIN` | ASYNC_THROUGH minimum successful replicas (default `2`) |
| `GOOSEFS_USER_FILE_READ_MAX_NODE_RETRY` | Read candidate pool width / Java `maxRetryNode` (default `3`) |
| `GOOSEFS_USER_FILE_READ_MAX_NODE_RETRY` | Read candidate pool width / Java `maxRetryNode` (default `3`) |
| `GOOSEFS_USER_FILE_CHECK_BLOCK_REPLICAS` | CheckBlocks probe count; `0` disables (default) |
| `GOOSEFS_METADATA_CACHE_ENABLED` | Client metadata cache switch (default `true`) |
| `GOOSEFS_METADATA_CACHE_EXPIRATION` | Metadata cache TTL (`10min`, `30s`, or raw ms) |
| `GOOSEFS_METADATA_CACHE_MAX_SIZE` | Metadata cache LRU capacity (default `100000`) |

## Write / Read Types

| Enum                      | Typical use                                     |
| ------------------------- | ----------------------------------------------- |
| `WriteType::MustCache`    | Cache only (no UFS persist)                     |
| `WriteType::CacheThrough` | Write cache + UFS synchronously                 |
| `WriteType::Through`      | Write UFS directly                              |
| `WriteType::AsyncThrough` | Write cache, persist UFS asynchronously         |
| `ReadType::Cache`         | Populate worker cache on miss                   |
| `ReadType::NoCache`       | Do not back-fill worker/client cache write path |

## Client Local Page Cache (opt-in)

Disabled by default. Enable via fields, properties, or env:

| Property key                                  | Field                            | Default              |
| --------------------------------------------- | -------------------------------- | -------------------- |
| `goosefs.user.client.cache.enabled`           | `client_cache_enabled`           | `false`              |
| `goosefs.user.client.cache.page.size`         | `client_cache_page_size`         | `1MB`                |
| `goosefs.user.client.cache.size`              | `client_cache_size`              | `20 GiB`             |
| `goosefs.user.client.cache.dirs`              | `client_cache_dirs`              | `/tmp/goosefs_cache` |
| `goosefs.user.client.cache.eviction.policy`   | `client_cache_evictor`           | `LFU` (`LRU` / `S3FIFO`) |
| `goosefs.user.client.cache.sync.read.enabled` | `client_cache_sync_read_enabled` | `false` (Linux only; analytical workloads on local NVMe — see [Page Cache → Sync pread read mode](./page-cache#sync-pread-read-mode-linux-only)) |

See [Page Cache](./page-cache) for a full walkthrough.

## Client Metadata Cache (on by default)

Enabled by default (the Java client defaults it to `false`). `get_status` / `exists` / `open_file` / non-recursive `list_status` share one process-local TTL-bounded LRU:

| Property key                                  | Field                       | Default  |
| --------------------------------------------- | --------------------------- | -------- |
| `goosefs.user.metadata.cache.enabled`         | `metadata_cache_enabled`    | `true`   |
| `goosefs.user.metadata.cache.max.size`        | `metadata_cache_max_size`   | `100000` |
| `goosefs.user.metadata.cache.expiration.time` | `metadata_cache_expiration` | `10min`  |

This replaces the removed `FileInfo` open cache (`goosefs.user.file.info.cache.*`). See [Metadata Cache](./metadata-cache) for hit/bypass rules, invalidation, and metrics.

## Worker Connection Pool

Default `worker_connection_pool_size` is `min(cores, 4)` (capped), using `available_parallelism` so cgroup CPU limits are respected on Linux. Opt back to a single channel with:

```rust
config.with_worker_connection_pool_size(1);
// or property: goosefs.client.worker.connection.pool.size=1
```

## Full Parameter Reference

The complete field / env / properties / options matrix lives in the repository:

[`docs/CLIENT_CONFIGURATION.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_CONFIGURATION.md)
