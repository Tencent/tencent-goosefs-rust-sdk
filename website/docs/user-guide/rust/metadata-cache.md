---
sidebar_position: 5
---

# Metadata Cache

Besides the [page cache](./page-cache) for file *data*, the SDK ships a **client-side metadata cache** aligned with the GooseFS Java client's `goosefs.user.metadata.cache.*` semantics. `get_status` / `exists` / `open_file` / non-recursive `list_status` share one process-local TTL-bounded LRU, so repeated metadata lookups of the same paths no longer hit the Master. It is **on by default** (see below).

It replaces the earlier `FileInfo` open cache (`goosefs.user.file.info.cache.*`), which has been removed.

## Behavior

- **Enabled by default** — this diverges from the Java client, whose `goosefs.user.metadata.cache.enabled` default is `false`. Every reader open resolves its `FileInfo` through this cache, so with it off a workload of many small ranged reads pays one Master `get_status` RPC per read. That RPC also hides the [page cache](./page-cache): a page-cache hit over io_uring costs tens of microseconds, so a per-open Master round-trip dwarfs it and the read waits on metadata rather than on disk. Set the switch to `false` to opt out.
- **Three entry kinds per path** — a `get_status` slot, a directory listing, and a negative (`NotFound`) marker. One LRU key may hold both a status slot and a listing.
- **Write-time TTL** — entries expire `expiration` after insertion (Java Guava `expireAfterWrite`); status and listing under the same key share the insertion timestamp. Expired entries are dropped lazily on lookup.
- **`open` reuses the cached status** — a prior `get_status` hit means `open_file` issues **zero** extra `getStatus` RPCs.
- **Write paths self-invalidate** — `mkdir` / `delete` / `rename` invalidate the path **and its parent** after the RPC succeeds.
- **Incomplete files never count as hits** — a cached `INCOMPLETE` file falls through to the Master.
- **Process-local** — writes from *other* clients are not observed until the TTL elapses. Keep the TTL short (or leave the cache off) when out-of-band writers must be visible immediately.

## When the cache is bypassed

| Situation | Effect |
| --- | --- |
| `ListStatusOptions.recursive = true` | Listing never cached (client-side BFS each time). |
| `load_metadata_type = ALWAYS` (`file_metadata_load_type`) | **Listing** cache skipped (no read/write). Does **not** skip `get_status` / `exists` / `open` client cache. Master still re-loads from UFS on those RPCs. |
| `ListStatusOptions.load_metadata_only = true` | Listing cache skipped (no read/write). Per-call only. |
| `sync_interval_ms == 0` (`file_metadata_sync_interval`) | `get_status`: skip read, still write back. `list_status`: skip read **and** write. Per-call: `GetStatusOptions::always_sync()` or `ListStatusOptions.sync_interval_ms = Some(0)`. |
| `metadata_cache_expiration <= 0` | Cache is not constructed at all, even when enabled. |

`ALWAYS` is a listing-cache skip flag. To force every **status** lookup to skip the client cache, set `file_metadata_sync_interval=0` (or `GetStatusOptions::always_sync()`). `load_metadata_type` is still sent on `get_status` (default `ONCE`) so the Master can load missing UFS paths.

## Configuration

| Field | Properties key | Env var | Storage option | Default |
| --- | --- | --- | --- | --- |
| `metadata_cache_enabled` | `goosefs.user.metadata.cache.enabled` | `GOOSEFS_METADATA_CACHE_ENABLED` | `goosefs_metadata_cache_enabled` | `true` |
| `metadata_cache_max_size` | `goosefs.user.metadata.cache.max.size` | `GOOSEFS_METADATA_CACHE_MAX_SIZE` | `goosefs_metadata_cache_max_size` | `100000` |
| `metadata_cache_expiration` | `goosefs.user.metadata.cache.expiration.time` | `GOOSEFS_METADATA_CACHE_EXPIRATION` | `goosefs_metadata_cache_expiration` | `10min` |
| `file_metadata_sync_interval` | `goosefs.user.file.metadata.sync.interval` | `GOOSEFS_FILE_METADATA_SYNC_INTERVAL` | `goosefs_file_metadata_sync_interval` | `-1` |
| `file_metadata_load_type` | `goosefs.user.file.metadata.load.type` | `GOOSEFS_FILE_METADATA_LOAD_TYPE` | `goosefs_file_metadata_load_type` | `ONCE` |

`max.size` values below `1` are clamped to `1`. Expiration and sync interval accept the Java `parseTimeSize` form (`10min`, `30s`, `2day`) or a raw millisecond number. A bare `-1` / `0` is milliseconds.

### `file_metadata_load_type` values

The value is sent as `load_metadata_type` on every `get_status` **and** `list_status` RPC (Java `getStatusDefaults` / `listStatusDefaults`). It has two effects: one on the client listing cache, one on how the Master treats the under file system (UFS). An unset `get_status` field is proto `NEVER`, so the default `ONCE` must be sent or COS-only files fail OpenDAL `stat` with `NotFound`.

| Value | Client listing cache | Master behaviour | Use when |
| --- | --- | --- | --- |
| `ONCE` (default) | Used normally (read + write) | Loads a path's metadata from the UFS the first time it is accessed, then serves it from the Master namespace | Default. GooseFS is the only writer, or a stale window of one TTL is acceptable |
| `ALWAYS` | **Skipped** — every call goes to the Master, and the result is not cached | Re-loads metadata from the UFS on every call, so files written out-of-band (directly to COS/HDFS) become visible immediately | Another system writes into the UFS behind GooseFS and the listing must be fresh |
| `NEVER` | Used normally (read + write) | Never touches the UFS; only what is already in the Master namespace is returned, so unloaded UFS files stay invisible | Pure GooseFS namespace, and you want to avoid UFS round-trips entirely |

`ALWAYS` costs a UFS round-trip **and** a Master RPC per list, so it is the slowest option — scope it to the calls that need freshness via `ListStatusOptions.load_metadata_type` instead of setting it globally where possible. Values are case-insensitive; an unrecognised value is ignored and the default `ONCE` is kept.

### `file_metadata_sync_interval` values

| Value | Effect |
| --- | --- |
| `-1` (default) | Does not skip the cache. `get_status` and non-recursive `list_status` may be served from the LRU. |
| `0` | Skips the cache on every call: `get_status` re-reads from the Master but still writes the result back; `list_status` neither reads nor writes the listing cache. |
| `> 0` (e.g. `5s`) | Parsed and stored, but the skip check only tests for `0`, so it currently behaves like `-1`. |

Unlike `load_metadata_type`, this knob affects **both** `get_status` and `list_status`, and it does not change what the Master does with the UFS.

```properties
# goosefs-site.properties
goosefs.user.metadata.cache.enabled=true
goosefs.user.metadata.cache.max.size=100000
goosefs.user.metadata.cache.expiration.time=1min
goosefs.user.file.metadata.sync.interval=-1
goosefs.user.file.metadata.load.type=ONCE
```

```bash
export GOOSEFS_METADATA_CACHE_ENABLED=true
export GOOSEFS_METADATA_CACHE_EXPIRATION=1min
export GOOSEFS_METADATA_CACHE_MAX_SIZE=100000
export GOOSEFS_FILE_METADATA_SYNC_INTERVAL=-1
export GOOSEFS_FILE_METADATA_LOAD_TYPE=ONCE

# Force every list to the Master (and tell Master to always load from UFS):
# export GOOSEFS_FILE_METADATA_LOAD_TYPE=ALWAYS

# Force every get_status / list_status to skip the cache:
# export GOOSEFS_FILE_METADATA_SYNC_INTERVAL=0
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
