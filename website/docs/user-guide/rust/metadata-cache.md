---
sidebar_position: 5
---

# Metadata Cache

Besides the [page cache](./page-cache) for file *data*, the SDK ships an optional **client-side metadata cache** aligned with the GooseFS Java client's `goosefs.user.metadata.cache.*` semantics. When enabled, `get_status` / `exists` / `open_file` / non-recursive `list_status` share one process-local TTL-bounded LRU, so repeated metadata lookups of the same paths no longer hit the Master.

It replaces the earlier `FileInfo` open cache (`goosefs.user.file.info.cache.*`), which has been removed.

## Behavior

- **Disabled by default** — nothing changes unless you opt in.
- **Three entry kinds per path** — a `get_status` slot, a directory listing, and a negative (`NotFound`) marker. One LRU key may hold both a status slot and a listing.
- **Write-time TTL** — entries expire `expiration` after insertion (Java Guava `expireAfterWrite`); status and listing under the same key share the insertion timestamp. Expired entries are dropped lazily on lookup.
- **`open` reuses the cached status** — a prior `get_status` hit means `open_file` issues **zero** extra `getStatus` RPCs.
- **Write paths self-invalidate** — `mkdir` / `delete` / `rename` invalidate the path **and its parent** after the RPC succeeds.
- **Incomplete files never count as hits** — a cached `INCOMPLETE` file falls through to the Master.
- **Process-local** — writes from *other* clients are not observed until the TTL elapses. Keep the TTL short (or leave the cache off) when out-of-band writers must be visible immediately.

## When the cache is bypassed

| Situation                                                     | Effect                                             |
| ------------------------------------------------------------- | -------------------------------------------------- |
| `ListStatusOptions.recursive = true`                          | Never cached (Master-side BFS walk each time)       |
| `load_metadata_type = ALWAYS`                                 | Listing cache skipped                              |
| `ListStatusOptions.load_metadata_only = true`                 | Listing cache skipped                              |
| `sync_interval_ms == 0` (`file_metadata_sync_interval`)        | Cache not consulted, but the RPC result is written back |
| `metadata_cache_expiration <= 0`                              | Cache is not constructed at all, even when enabled |

## Configuration

| Field                       | Properties key                                | Env var                            | Storage option                     | Default    |
| --------------------------- | --------------------------------------------- | ---------------------------------- | ---------------------------------- | ---------- |
| `metadata_cache_enabled`    | `goosefs.user.metadata.cache.enabled`         | `GOOSEFS_METADATA_CACHE_ENABLED`   | `goosefs_metadata_cache_enabled`   | `false`    |
| `metadata_cache_max_size`   | `goosefs.user.metadata.cache.max.size`        | `GOOSEFS_METADATA_CACHE_MAX_SIZE`  | `goosefs_metadata_cache_max_size`  | `100000`   |
| `metadata_cache_expiration` | `goosefs.user.metadata.cache.expiration.time` | `GOOSEFS_METADATA_CACHE_EXPIRATION`| `goosefs_metadata_cache_expiration`| `10min`    |

`max.size` values below `1` are clamped to `1`. The expiration accepts the Java `parseTimeSize` form (`10min`, `30s`, `2day`) or raw milliseconds.

```properties
# goosefs-site.properties
goosefs.user.metadata.cache.enabled=true
goosefs.user.metadata.cache.max.size=100000
goosefs.user.metadata.cache.expiration.time=1min
```

```bash
export GOOSEFS_METADATA_CACHE_ENABLED=true
export GOOSEFS_METADATA_CACHE_EXPIRATION=1min
export GOOSEFS_METADATA_CACHE_MAX_SIZE=100000
```

## Example

```rust
use std::sync::Arc;
use std::time::Duration;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::fs::{BaseFileSystem, FileSystem};

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let config = GoosefsConfig::new("127.0.0.1:9200")
        .with_metadata_cache_enabled(true)
        .with_metadata_cache_expiration(Duration::from_secs(60))
        .with_metadata_cache_max_size(50_000);

    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    let fs = BaseFileSystem::from_context(ctx.clone());

    let _cold = fs.get_status("/data/file.parquet").await?; // miss → Master RPC
    let _warm = fs.get_status("/data/file.parquet").await?; // hit  → no RPC

    // Reuses the cached status: no extra getStatus RPC.
    let _stream = fs
        .open_file("/data/file.parquet", OpenFileOptions::default())
        .await?;

    // Drop a path (and its parent listing) after an out-of-band mutation.
    ctx.invalidate_metadata("/data/file.parquet", true);

    ctx.close().await?;
    Ok(())
}
```

## Observability

| Metric                              | Type    | Meaning                                       |
| ----------------------------------- | ------- | --------------------------------------------- |
| `Client.MetadataCacheHits`          | Counter | Status / listing hits                         |
| `Client.MetadataCacheMisses`        | Counter | Misses, including TTL expiry                  |
| `Client.MetadataCacheExpirations`   | Counter | Entries dropped because the TTL elapsed       |
| `Client.MetadataCacheInvalidations` | Counter | Explicit write-path invalidations             |
| `Client.MetadataCacheNegativeHits`  | Counter | Negative-cache (`NotFound`) hits              |
| `Client.MetadataCacheSize`          | Gauge   | Current LRU entry count                       |
| `Client.MetadataCacheEnabled`       | Gauge   | `1` when a cache was constructed              |

`Client.GetStatusOps` counts Master RPCs only — cache hits are **not** counted, so a healthy cache shows a falling `GetStatusOps` rate alongside a rising `MetadataCacheHits` rate.

Full catalogue: [`docs/METRICS.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/METRICS.md). Configuration matrix: [`docs/CLIENT_CONFIGURATION.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_CONFIGURATION.md).
