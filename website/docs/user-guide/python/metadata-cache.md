---
sidebar_position: 9
---

# Metadata Cache

Besides the [page cache](./page-cache) for file *data*, the client ships a **metadata cache** aligned with the GooseFS Java client's `goosefs.user.metadata.cache.*` semantics. `get_status`, `exists`, `open_file` and non-recursive `list_status` (including their `batch_*` / `*_grouped` variants) share one process-local TTL-bounded LRU, so repeated metadata lookups of the same paths no longer hit the master. It is **on by default** (see below).

It replaces the earlier `FileInfo` open cache (`GOOSEFS_FILE_INFO_CACHE_TTL_MS` / `GOOSEFS_FILE_INFO_CACHE_CAPACITY`), which has been removed.

Because the Python binding shares the Rust configuration core, the cache is available through env / properties with **no binding change**.

## Behavior

- **Enabled by default** — this diverges from the Java client, whose `goosefs.user.metadata.cache.enabled` default is `false`. Every file open resolves its status through this cache, so with it off a workload of many small ranged reads pays one master `get_status` RPC per read. That RPC also hides the [page cache](./page-cache): a page-cache hit over io_uring costs tens of microseconds, so a per-open master round-trip dwarfs it and the read waits on metadata rather than on disk. Set the switch to `false` to opt out.
- **Three entry kinds per path** — a status slot, a directory listing, and a negative (`NotFound`) marker.
- **Write-time TTL** — entries expire `expiration` after insertion; status and listing under the same path share the insertion timestamp.
- **`open_file()` reuses the cached status** — a prior `get_status` hit means the open issues no extra master RPC.
- **Write paths self-invalidate** — `mkdir` / `delete` / `rename` drop the path **and its parent** after the RPC succeeds.
- **Incomplete files never count as hits** — a cached `INCOMPLETE` file falls through to the master.
- **Process-local** — writes from *other* clients are not observed until the TTL elapses. Keep the TTL short (or leave the cache off) when out-of-band writers must be visible immediately.

## When the cache is bypassed

| Situation | Effect |
| --- | --- |
| `list_status(..., recursive=True)` | Listing never cached (client-side walk each time). |
| `goosefs.user.file.metadata.load.type=ALWAYS` | **Listing** cache skipped (no read/write). Does **not** skip `get_status` / `exists` / `open_file`. |
| `goosefs.user.file.metadata.sync.interval=0` | `get_status`: skip read, still write back. `list_status`: skip read **and** write. |
| `goosefs.user.metadata.cache.expiration.time<=0` | Cache is not constructed at all, even when enabled. |

`ALWAYS` is a listing-only flag. To force every **status** lookup to the master, set `GOOSEFS_FILE_METADATA_SYNC_INTERVAL=0`.

## Tuning the Cache

The switch is already `true` by default; set it explicitly to be
self-documenting, or to `false` to opt out.

```bash
export GOOSEFS_METADATA_CACHE_ENABLED=true
export GOOSEFS_METADATA_CACHE_EXPIRATION=1min       # 10min default; also accepts raw ms
export GOOSEFS_METADATA_CACHE_MAX_SIZE=100000
export GOOSEFS_FILE_METADATA_SYNC_INTERVAL=-1       # default; 0 skips cache on every get/list
export GOOSEFS_FILE_METADATA_LOAD_TYPE=ONCE         # ONCE / ALWAYS / NEVER (ALWAYS skips listing cache)

# Force every list to the master:
# export GOOSEFS_FILE_METADATA_LOAD_TYPE=ALWAYS
```

```properties
# goosefs-site.properties
goosefs.user.metadata.cache.enabled=true
goosefs.user.metadata.cache.expiration.time=1min
goosefs.user.metadata.cache.max.size=100000
goosefs.user.file.metadata.sync.interval=-1
goosefs.user.file.metadata.load.type=ONCE
```

Or inline via the `Config` builder:

```python
from goosefs import Config

cfg = Config(
    "127.0.0.1:9200",
    properties={
        "goosefs.user.metadata.cache.enabled": "true",
        "goosefs.user.metadata.cache.expiration.time": "1min",
        "goosefs.user.metadata.cache.max.size": "100000",
        "goosefs.user.file.metadata.sync.interval": "-1",
        "goosefs.user.file.metadata.load.type": "ONCE",
    },
)
```

For Lance / OpenDAL callers the same knobs are exposed as storage options:

```python
import lance

ds = lance.dataset(
    "gfs://bucket/dataset.lance",
    storage_options={
        "goosefs_metadata_cache_enabled": "true",
        "goosefs_metadata_cache_expiration": "1min",
        "goosefs_metadata_cache_max_size": "100000",
        "goosefs_file_metadata_sync_interval": "-1",
        "goosefs_file_metadata_load_type": "ONCE",
    },
)
```

## Example

```python
import asyncio
from goosefs import AsyncGoosefs, Config

async def main():
    cfg = Config(
        "127.0.0.1:9200",
        properties={
            "goosefs.user.metadata.cache.enabled": "true",
            "goosefs.user.metadata.cache.expiration.time": "1min",
        },
    )
    async with await AsyncGoosefs.connect(cfg) as fs:
        await fs.get_status("/data/file.parquet")   # miss → master RPC
        await fs.get_status("/data/file.parquet")   # hit  → no RPC

        # Batch metadata benefits too: repeated paths are served from the cache.
        await fs.batch_get_status(["/data/a.parquet", "/data/b.parquet"])

        # Non-recursive listings are cached; recursive ones are not.
        await fs.list_status("/data", recursive=False)

        # delete/rename/mkdir invalidate the path and its parent automatically.
        await fs.delete("/data/file.parquet")

asyncio.run(main())
```

## Configuration Reference

| Properties key | Env var | Storage option | Default |
| --- | --- | --- | --- |
| `goosefs.user.metadata.cache.enabled` | `GOOSEFS_METADATA_CACHE_ENABLED` | `goosefs_metadata_cache_enabled` | `true` |
| `goosefs.user.metadata.cache.max.size` | `GOOSEFS_METADATA_CACHE_MAX_SIZE` | `goosefs_metadata_cache_max_size` | `100000` |
| `goosefs.user.metadata.cache.expiration.time` | `GOOSEFS_METADATA_CACHE_EXPIRATION` | `goosefs_metadata_cache_expiration` | `10min` |
| `goosefs.user.file.metadata.sync.interval` | `GOOSEFS_FILE_METADATA_SYNC_INTERVAL` | `goosefs_file_metadata_sync_interval` | `-1` |
| `goosefs.user.file.metadata.load.type` | `GOOSEFS_FILE_METADATA_LOAD_TYPE` | `goosefs_file_metadata_load_type` | `ONCE` |

Values below `1` for `max.size` are clamped to `1`. Expiration and sync interval accept `10min` / `30s` / `2day` or a raw millisecond number (`-1` / `0` are milliseconds).

### `goosefs.user.file.metadata.load.type` values

The value is sent on every `list_status` RPC, so it has **two** effects: one on the client cache, one on how the master treats the under file system (UFS). It is not sent on `get_status`, so it never affects `get_status` / `exists` / `open_file`.

| Value | Client listing cache | Master behaviour | Use when |
| --- | --- | --- | --- |
| `ONCE` (default) | Used normally (read + write) | Loads a path's metadata from the UFS the first time it is accessed, then serves it from the master namespace | Default. GooseFS is the only writer, or a stale window of one TTL is acceptable |
| `ALWAYS` | **Skipped** — every call goes to the master, and the result is not cached | Re-loads metadata from the UFS on every call, so files written out-of-band (directly to COS/HDFS) become visible immediately | Another system writes into the UFS behind GooseFS and the listing must be fresh |
| `NEVER` | Used normally (read + write) | Never touches the UFS; only what is already in the master namespace is returned, so unloaded UFS files stay invisible | Pure GooseFS namespace, and you want to avoid UFS round-trips entirely |

`ALWAYS` costs a UFS round-trip **and** a master RPC per list, so it is the slowest option. Values are case-insensitive; an unrecognised value is ignored and the default `ONCE` is kept.

### `goosefs.user.file.metadata.sync.interval` values

| Value | Effect |
| --- | --- |
| `-1` (default) | Does not skip the cache. `get_status` and non-recursive `list_status` may be served from the LRU. |
| `0` | Skips the cache on every call: `get_status` re-reads from the master but still writes the result back; `list_status` neither reads nor writes the listing cache. |
| `> 0` (e.g. `5s`) | Parsed and stored, but the skip check only tests for `0`, so it currently behaves like `-1`. |

Unlike `load.type`, this knob affects **both** `get_status` and `list_status`, and it does not change what the master does with the UFS.

## Observability

| Metric                              | Type    | Meaning                                 |
| ----------------------------------- | ------- | --------------------------------------- |
| `Client.MetadataCacheHits`          | Counter | Status / listing hits                   |
| `Client.MetadataCacheMisses`        | Counter | Misses, including TTL expiry            |
| `Client.MetadataCacheExpirations`   | Counter | Entries dropped because the TTL elapsed |
| `Client.MetadataCacheInvalidations` | Counter | Explicit write-path invalidations       |
| `Client.MetadataCacheNegativeHits`  | Counter | Negative-cache (`NotFound`) hits        |
| `Client.MetadataCacheSize`          | Gauge   | Current LRU entry count                 |
| `Client.MetadataCacheEnabled`       | Gauge   | `1` when a cache was constructed        |

`Client.GetStatusOps` counts master RPCs only — cache hits are **not** counted.

For hit/miss logs without a metrics backend:

```bash
RUST_LOG=goosefs_sdk::metadata_cache=debug python your_script.py
```

See the Rust [Metadata Cache](../rust/metadata-cache) page and [`docs/CLIENT_CONFIGURATION.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_CONFIGURATION.md) for the full matrix.
