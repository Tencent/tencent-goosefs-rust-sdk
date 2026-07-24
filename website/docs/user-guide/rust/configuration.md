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

| Property key                                | Field                    | Default              |
| ------------------------------------------- | ------------------------ | -------------------- |
| `goosefs.user.client.cache.enabled`         | `client_cache_enabled`   | `false`              |
| `goosefs.user.client.cache.page.size`       | `client_cache_page_size` | `1MB`                |
| `goosefs.user.client.cache.size`            | `client_cache_size`      | `20 GiB`             |
| `goosefs.user.client.cache.dirs`            | `client_cache_dirs`      | `/tmp/goosefs_cache` |
| `goosefs.user.client.cache.eviction.policy` | `client_cache_evictor`   | `LFU`                |

See [Page Cache](./page-cache) for a full walkthrough.

## Worker Connection Pool

Default `worker_connection_pool_size` is `min(cores, 4)` (capped), using `available_parallelism` so cgroup CPU limits are respected on Linux. Opt back to a single channel with:

```rust
config.with_worker_connection_pool_size(1);
// or property: goosefs.client.worker.connection.pool.size=1
```

## Full Parameter Reference

The complete field / env / properties / options matrix lives in the repository:

[`docs/CLIENT_CONFIGURATION.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_CONFIGURATION.md)
