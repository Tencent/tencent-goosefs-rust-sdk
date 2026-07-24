---
sidebar_position: 8
---

# Page Cache

The client-side page cache stores recently read file pages on **local disk**, avoiding repeated network round-trips for hot data. It is **disabled by default** and is enabled globally through configuration; streaming `open_file()` reads use the cache-enabled read type by default.

## Behavior

- **Cache hit**: file data is served from local disk without a worker data RPC; opening the file may still require master metadata lookup.
- **Cache miss**: data fetched from the worker, back-filled into the cache.
- **Eviction**: LRU or LFU (configurable), evicts pages when the cache reaches its size limit.
- **Consistency**: the cache is **not** invalidated on out-of-band writes by other clients. Use `ReadType.NoCache` for fresh reads if you suspect concurrent writers.

## Enabling the Cache

### Global configuration

```bash
# Environment variables (values are integer byte counts, not human-readable)
export GOOSEFS_USER_CLIENT_CACHE_ENABLED=true
export GOOSEFS_USER_CLIENT_CACHE_SIZE=21474836480        # 20 GiB in bytes
export GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE=1048576       # 1 MB in bytes
export GOOSEFS_USER_CLIENT_CACHE_DIRS=/tmp/goosefs_cache
```

```properties
# goosefs-site.properties
goosefs.user.client.cache.enabled=true
goosefs.user.client.cache.size=20GB
goosefs.user.client.cache.page.size=1MB
goosefs.user.client.cache.dirs=/tmp/goosefs_cache
goosefs.user.client.cache.eviction.policy=LFU
```

### Per-file read behavior

```python
from goosefs import AsyncGoosefs, Config

async with await AsyncGoosefs.connect(Config("127.0.0.1:9200")) as fs:
    # open_file() uses the cache-enabled read type by default.
    # When the page cache is enabled globally, streaming reads
    # consult the cache automatically.
    reader = await fs.open_file("/data/hot.parquet")
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

        # Second read — cache hit (requires sequential read cache enabled:
        # GOOSEFS_USER_CLIENT_CACHE_SEQUENTIAL_READ_ENABLED=true)
        reader = await fs.open_file("/data/hot.parquet")
        data2 = await reader.read()
        await reader.close()

        assert data1 == data2

asyncio.run(main())
```

## Observability

Call `goosefs.enable_tracing(level="debug")` near the start of the script to install the tracing subscriber, then set `RUST_LOG` to filter cache hit/miss logs:

```bash
RUST_LOG=goosefs_sdk::cache=debug python your_script.py
```

## Configuration Reference

| Property key                                | Env var                                    | Default              |
| ------------------------------------------- | ------------------------------------------ | -------------------- |
| `goosefs.user.client.cache.enabled`         | `GOOSEFS_USER_CLIENT_CACHE_ENABLED`        | `false`              |
| `goosefs.user.client.cache.page.size`       | `GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE`      | `1048576` (1 MB)     |
| `goosefs.user.client.cache.size`            | `GOOSEFS_USER_CLIENT_CACHE_SIZE`           | `21474836480` (20 GiB) |
| `goosefs.user.client.cache.dirs`            | `GOOSEFS_USER_CLIENT_CACHE_DIRS`           | `/tmp/goosefs_cache` |
| `goosefs.user.client.cache.eviction.policy` | `GOOSEFS_USER_CLIENT_CACHE_EVICTION_POLICY`| `LFU`                |
