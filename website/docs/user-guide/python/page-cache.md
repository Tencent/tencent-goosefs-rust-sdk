---
sidebar_position: 8
---

# Page Cache

The client-side page cache stores recently-read file blocks in local memory, avoiding repeated network round-trips for hot data. It is **disabled by default** and must be opted in per file via `OpenFileOptions`.

## Behavior

- **Cache hit**: data served from local memory — zero RPCs.
- **Cache miss**: data fetched from the worker, back-filled into the cache.
- **Eviction**: LRU or LFU (configurable), evicts pages when the cache reaches its size limit.
- **Consistency**: the cache is **not** invalidated on out-of-band writes by other clients. Use `ReadType.NoCache` for fresh reads if you suspect concurrent writers.

## Enabling the Cache

### Global configuration

```bash
# Environment variables
export GOOSEFS_CLIENT_CACHE_ENABLED=true
export GOOSEFS_CLIENT_CACHE_SIZE=20GiB
export GOOSEFS_CLIENT_CACHE_PAGE_SIZE=1MB
export GOOSEFS_CLIENT_CACHE_DIRS=/tmp/goosefs_cache
```

```properties
# goosefs-site.properties
goosefs.user.client.cache.enabled=true
goosefs.user.client.cache.size=20GB
goosefs.user.client.cache.page.size=1MB
goosefs.user.client.cache.dirs=/tmp/goosefs_cache
goosefs.user.client.cache.eviction.policy=LFU
```

### Per-file read type

```python
from goosefs import AsyncGoosefs, Config, OpenFileOptions, ReadType

async with await AsyncGoosefs.connect(Config("127.0.0.1:9200")) as fs:
    # Cache-backed read (default when cache is enabled)
    reader = await fs.open_file("/data/hot.parquet")

    # Explicitly skip cache for a cold read
    reader = await fs.open_file(
        "/data/cold.parquet",
        # OpenFileOptions can be passed via the streaming API
    )
```

:::note
`read_file()` and `read_range()` (one-shot reads) go **worker-direct** and bypass the client page cache. Only `open_file()` (streaming read) consults the cache.
:::

## Example

```python
import asyncio
from goosefs import Config, AsyncGoosefs

async def main():
    cfg = Config("127.0.0.1:9200")
    # Enable page cache via env or properties before connecting
    async with await AsyncGoosefs.connect(cfg) as fs:
        # First read — cache miss, fetched from worker
        reader = await fs.open_file("/data/hot.parquet")
        data1 = await reader.read()
        await reader.close()

        # Second read — cache hit, served from local memory
        reader = await fs.open_file("/data/hot.parquet")
        data2 = await reader.read()
        await reader.close()

        assert data1 == data2

asyncio.run(main())
```

## Observability

Set `RUST_LOG=goosefs_sdk::cache=debug` to see cache hit/miss logs:

```bash
RUST_LOG=goosefs_sdk::cache=debug python your_script.py
```

## Configuration Reference

| Property key                                | Env var                          | Default              |
| ------------------------------------------- | -------------------------------- | -------------------- |
| `goosefs.user.client.cache.enabled`         | `GOOSEFS_CLIENT_CACHE_ENABLED`   | `false`              |
| `goosefs.user.client.cache.page.size`       | `GOOSEFS_CLIENT_CACHE_PAGE_SIZE` | `1MB`                |
| `goosefs.user.client.cache.size`            | `GOOSEFS_CLIENT_CACHE_SIZE`      | `20 GiB`             |
| `goosefs.user.client.cache.dirs`            | `GOOSEFS_CLIENT_CACHE_DIRS`      | `/tmp/goosefs_cache` |
| `goosefs.user.client.cache.eviction.policy` | `GOOSEFS_CLIENT_CACHE_EVICTOR`   | `LFU`                |
