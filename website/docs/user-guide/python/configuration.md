---
sidebar_position: 4
---

# Configuration

The Python binding shares the same Rust core configuration. Settings can be provided through the `Config` builder, environment variables, or a properties file. When the same parameter appears in multiple sources, the **highest-priority** source wins:

```text
Priority (highest → lowest):

  1. Environment variables (GOOSEFS_*)
  2. Properties config file (goosefs-site.properties)
  3. Built-in defaults
```

## Minimal Setup

```python
from goosefs import Config

# Single master (GOOSEFS_* env overrides are applied automatically)
cfg = Config("127.0.0.1:9200")

# From a goosefs-site.properties file
cfg = Config.from_properties_file("/etc/goosefs/goosefs-site.properties")

# From a goosefs:// URI with inline params
cfg = Config.from_uri("goosefs://127.0.0.1:9200/?auth.type=simple")
```

## Environment Variables

| Variable                              | Purpose                                       |
| ------------------------------------- | --------------------------------------------- |
| `GOOSEFS_MASTER_ADDR`                 | Master host:port (or comma-separated HA list) |
| `GOOSEFS_AUTH_TYPE`                   | `nosasl` / `simple` / `custom`                |
| `GOOSEFS_AUTH_USERNAME`               | Username for SIMPLE auth                      |
| `GOOSEFS_MASTER_CONNECTION_POOL_SIZE` | Master gRPC channel pool size (default 1)     |
| `GOOSEFS_MASTER_POOL_SCHEDULE`        | `roundrobin` / `p2c`                          |
| `GOOSEFS_WORKER_CONNECTION_POOL_SIZE` | Per-worker gRPC channel pool size             |
| `GOOSEFS_FILE_INFO_CACHE_TTL_MS`      | Client-side FileInfo cache TTL (0 = disabled) |
| `GOOSEFS_FILE_INFO_CACHE_CAPACITY`    | FileInfo LRU cache capacity                   |

## Write / Read Types

| Enum                   | Typical use                                     |
| ---------------------- | ----------------------------------------------- |
| `WriteType.MustCache`    | Cache only (no UFS persist)                     |
| `WriteType.TryCache`     | Try cache first, fall back to Through on error  |
| `WriteType.CacheThrough` | Write cache + UFS synchronously                 |
| `WriteType.Through`      | Write UFS directly                              |
| `WriteType.AsyncThrough` | Write cache, persist UFS asynchronously         |

```python
from goosefs import Config, Goosefs, WriteType

cfg = Config("127.0.0.1:9200")
fs = Goosefs(cfg)  # sync; use AsyncGoosefs for async
fs.write_file("/data/file.bin", b"payload", write_type=WriteType.CacheThrough)
```

## Master Connection Pool

The master connection pool spreads concurrent metadata RPCs across multiple HTTP/2 channels. Default size is **1** (single channel, backward-compatible) with **round-robin** scheduling. Raise to 4-8 with P2C scheduling for high-concurrency remote scenarios.

```python
# Via env
# export GOOSEFS_MASTER_CONNECTION_POOL_SIZE=8
# export GOOSEFS_MASTER_POOL_SCHEDULE=p2c

# Via properties file
# goosefs.user.master.connection.pool.size=8
# goosefs.user.master.pool.schedule=p2c

# Via storage options (OpenDAL / Lance)
# storage_options={"goosefs_master_connection_pool_size": "8", ...}
```

## Client Local Page Cache (opt-in)

Disabled by default. Enable via env or properties:

| Property key                                | Env var                                    | Default              |
| ------------------------------------------- | ------------------------------------------ | -------------------- |
| `goosefs.user.client.cache.enabled`         | `GOOSEFS_USER_CLIENT_CACHE_ENABLED`        | `false`              |
| `goosefs.user.client.cache.page.size`       | `GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE`      | `1048576` (1 MB)     |
| `goosefs.user.client.cache.size`            | `GOOSEFS_USER_CLIENT_CACHE_SIZE`           | `21474836480` (20 GiB) |
| `goosefs.user.client.cache.dirs`            | `GOOSEFS_USER_CLIENT_CACHE_DIRS`           | `/tmp/goosefs_cache` |

See [Page Cache](./page-cache) for a full walkthrough.

## Full Parameter Reference

The complete field / env / properties / storage-options matrix lives in the repository:

[`docs/CLIENT_CONFIGURATION.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_CONFIGURATION.md)
