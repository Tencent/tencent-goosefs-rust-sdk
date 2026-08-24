---
sidebar_position: 9
---

# Metadata Cache

Besides the [page cache](./page-cache) for file *data*, the client ships an optional **metadata cache** aligned with the GooseFS Java client's `goosefs.user.metadata.cache.*` semantics. When enabled, `get_status`, `exists`, `open_file` and non-recursive `list_status` (including their `batch_*` / `*_grouped` variants) share one process-local TTL-bounded LRU, so repeated metadata lookups of the same paths no longer hit the master.

It replaces the earlier `FileInfo` open cache (`GOOSEFS_FILE_INFO_CACHE_TTL_MS` / `GOOSEFS_FILE_INFO_CACHE_CAPACITY`), which has been removed.

Because the Python binding shares the Rust configuration core, the cache is available through env / properties with **no binding change**.

## Behavior

- **Disabled by default** — nothing changes unless you opt in.
- **Three entry kinds per path** — a status slot, a directory listing, and a negative (`NotFound`) marker.
- **Write-time TTL** — entries expire `expiration` after insertion; status and listing under the same path share the insertion timestamp.
- **`open_file()` reuses the cached status** — a prior `get_status` hit means the open issues no extra master RPC.
- **Write paths self-invalidate** — `mkdir` / `delete` / `rename` drop the path **and its parent** after the RPC succeeds.
- **Incomplete files never count as hits** — a cached `INCOMPLETE` file falls through to the master.
- **Process-local** — writes from *other* clients are not observed until the TTL elapses. Keep the TTL short (or leave the cache off) when out-of-band writers must be visible immediately.

## When the cache is bypassed

| Situation                                | Effect                                                |
| ---------------------------------------- | ----------------------------------------------------- |
| `list_status(..., recursive=True)`       | Never cached (master-side walk each time)              |
| `goosefs.user.file.metadata.load.type=ALWAYS` | Listing cache skipped                            |
| `goosefs.user.file.metadata.sync.interval=0` | Cache not consulted, but results are written back |
| `goosefs.user.metadata.cache.expiration.time<=0` | Cache is not constructed at all, even when enabled |

## Enabling the Cache

```bash
export GOOSEFS_METADATA_CACHE_ENABLED=true
export GOOSEFS_METADATA_CACHE_EXPIRATION=1min       # 10min default; also accepts raw ms
export GOOSEFS_METADATA_CACHE_MAX_SIZE=100000
```

```properties
# goosefs-site.properties
goosefs.user.metadata.cache.enabled=true
goosefs.user.metadata.cache.expiration.time=1min
goosefs.user.metadata.cache.max.size=100000
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

| Properties key                                | Env var                             | Storage option                      | Default  |
| --------------------------------------------- | ----------------------------------- | ----------------------------------- | -------- |
| `goosefs.user.metadata.cache.enabled`         | `GOOSEFS_METADATA_CACHE_ENABLED`    | `goosefs_metadata_cache_enabled`    | `false`  |
| `goosefs.user.metadata.cache.max.size`        | `GOOSEFS_METADATA_CACHE_MAX_SIZE`   | `goosefs_metadata_cache_max_size`   | `100000` |
| `goosefs.user.metadata.cache.expiration.time` | `GOOSEFS_METADATA_CACHE_EXPIRATION` | `goosefs_metadata_cache_expiration` | `10min`  |

Values below `1` for `max.size` are clamped to `1`. The expiration accepts `10min` / `30s` / `2day` or raw milliseconds.

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
