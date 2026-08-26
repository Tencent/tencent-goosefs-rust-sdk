---
sidebar_position: 8
---

# Page Cache

The client-side page cache stores recently read file pages on **local disk**, avoiding repeated network round-trips for hot data. It is **disabled by default** and is enabled globally through configuration; streaming `open_file()` reads use the cache-enabled read type by default.

## Behavior

- **Cache hit**: file data is served from local disk without a worker data RPC; opening the file may still require master metadata lookup.
- **Cache miss**: data fetched from the worker, back-filled into the cache.
- **Eviction**: LRU (the default, matching the Java client), LFU or S3-FIFO — configurable; evicts pages when the cache reaches its size limit. `LFU` and `S3FIFO` are scan-resistant: a one-shot sequential read will not flush the hot working set, which `LRU` cannot promise.
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

| Property key                                  | Env var                                      | Default              |
| --------------------------------------------- | -------------------------------------------- | -------------------- |
| `goosefs.user.client.cache.enabled`           | `GOOSEFS_USER_CLIENT_CACHE_ENABLED`          | `false`              |
| `goosefs.user.client.cache.page.size`         | `GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE`        | `1048576` (1 MB)     |
| `goosefs.user.client.cache.size`              | `GOOSEFS_USER_CLIENT_CACHE_SIZE`             | `21474836480` (20 GiB) |
| `goosefs.user.client.cache.dirs`              | `GOOSEFS_USER_CLIENT_CACHE_DIRS`             | `/tmp/goosefs_cache` |
| `goosefs.user.client.cache.eviction.policy`   | `GOOSEFS_USER_CLIENT_CACHE_EVICTION_POLICY`  | `LFU` (`LRU` / `S3FIFO`) |
| `goosefs.user.client.cache.sync.read.enabled` | `GOOSEFS_USER_CLIENT_CACHE_SYNC_READ_ENABLED`| `false` (Linux only; see [Sync pread read mode](#sync-pread-read-mode-linux-only)) |

## Sync pread read mode (Linux only)

On Linux the cache backend defaults to `io_uring` (with a transparent fallback to `tokio::fs` when io_uring is unavailable). The SDK exposes an opt-in **synchronous `pread` read mode** for the io_uring backend, intended for complex analytical workloads (large scans, high cache hit rate) where plain `pread` outperforms io_uring on local NVMe — no SQE/CQE round-trip, no channel hop to the background uring threads, and no CPU spent by the uring spin/yield loop.

Because the Python binding shares the Rust configuration core, this switch is available to Python users through env / properties with **no binding change**.

| Property key                                  | Env var                                      | Default |
| --------------------------------------------- | -------------------------------------------- | ------- |
| `goosefs.user.client.cache.sync.read.enabled` | `GOOSEFS_USER_CLIENT_CACHE_SYNC_READ_ENABLED`| `false` |

### When to enable

- Analytical scans with a **high cache hit rate** running on **local NVMe**, where profiling shows io_uring submission/completion overhead dominating.
- The working set mostly fits the OS page cache (~µs per read); cold-working-set scans with frequent disk reads may be faster with io_uring's overlapped reads.

### When to keep off (default)

- **Latency-sensitive point-lookup workloads** — the calling worker is blocked for the duration of each read.
- **HDD / NFS / Lustre** cache directories — there is **no read timeout** on a sync `pread` (the io_uring path uses a 30 s `URING_OP_TIMEOUT`); a slow device can block the worker unbounded.
- Cold-working-set scans where disk reads dominate.

### Threading caveats

- The calling **worker is blocked** for the duration of each syscall. Cache misses never reach the store (the manager returns early), so the block is bounded by *local* read latency.
- Batched reads become **serial preads on one worker** — fine when the working set is OS-page-cache-hot, lossy when it is not.
- The page fd cache and dir fd cache are shared with the io_uring path; on-disk layout is unchanged, so the flag can be flipped across restarts freely.
- Write / delete paths always stay on io_uring regardless of this switch.

### Example

```bash
# Environment variables
export GOOSEFS_USER_CLIENT_CACHE_ENABLED=true
export GOOSEFS_USER_CLIENT_CACHE_DIRS=/var/cache/goosefs   # local NVMe
export GOOSEFS_USER_CLIENT_CACHE_SYNC_READ_ENABLED=true    # analytical workload
```

```properties
# goosefs-site.properties
goosefs.user.client.cache.enabled=true
goosefs.user.client.cache.dirs=/var/cache/goosefs
goosefs.user.client.cache.sync.read.enabled=true
```

Or inline via the `Config` builder:

```python
from goosefs import Config

cfg = Config(
    "127.0.0.1:9200",
    properties={
        "goosefs.user.client.cache.enabled": "true",
        "goosefs.user.client.cache.dirs": "/var/cache/goosefs",  # local NVMe
        "goosefs.user.client.cache.sync.read.enabled": "true",   # analytical workload
    },
)
```

::::note
This switch only affects the **read path** of the io_uring backend on Linux. On non-Linux hosts, or when io_uring is unavailable, the cache uses `tokio::fs` and this option has no effect. See the Rust [Page Cache → Sync pread read mode](../rust/page-cache#sync-pread-read-mode-linux-only) page and [`docs/CLIENT_CONFIGURATION.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_CONFIGURATION.md#sync-pread-read-mode-linux-only) for the full design notes.
::::
