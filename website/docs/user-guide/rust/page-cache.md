---
sidebar_position: 4
---

# Page Cache

The SDK ships an optional **client-side local page cache** that mirrors the GooseFS Java client's `goosefs.user.client.cache.*` semantics. When enabled, ranges read from a worker/UFS are cached on local disk in fixed-size pages; subsequent reads of the same range are served from disk.

## Behavior

- **Disabled by default** — existing behavior is unchanged unless you opt in
- **Best-effort** — misses/errors fall back to the worker; correctness is never affected
- **Transparent** — `read_at` on `GoosefsFileInStream` routes through the cache; sequential `read` bypasses it unless `client_cache_sequential_read_enabled` is set
- **Overwrite-safe** — on reopen, `(length, last_modification_time)` invalidates stale pages
- **Survives restarts** — pages and identity metadata are restored from disk

## Example

```rust
use std::sync::Arc;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::io::GoosefsFileInStream;
use goosefs_sdk::fs::options::OpenFileOptions;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let mut config = GoosefsConfig::new("127.0.0.1:9200");
    config.client_cache_enabled = true;
    config.client_cache_page_size = 1024 * 1024;           // 1 MiB
    config.client_cache_size = 1024 * 1024 * 1024;          // 1 GiB per dir
    config.client_cache_dirs = vec!["/tmp/goosefs_cache".into()];

    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    let mut s = GoosefsFileInStream::open_with_context(
        ctx.clone(),
        "/data/big.parquet",
        OpenFileOptions::default(),
    ).await?;

    let _cold = s.read_at(0, 1 << 20).await?; // miss → worker + back-fill
    let _warm = s.read_at(0, 1 << 20).await?; // hit  → local disk

    ctx.close().await?;
    Ok(())
}
```

## Observability

Cache effectiveness is exposed via `Client.Cache*` metrics (`CacheBytesReadCache`, `CacheBytesReadExternal`, `CachePages`, `CacheBytesEvicted`, …), reported through the same heartbeat / Pushgateway pipeline.

Try the bundled demo:

```bash
cargo run --example page_cache_demo
```

## Sync pread read mode (Linux only)

When the cache backend is `io_uring` (the default on Linux), the SDK exposes an opt-in **synchronous `pread` read mode** for the `UringPageStore`. It is intended for complex analytical workloads (large scans, high cache hit rate) where plain `pread` outperforms io_uring on local NVMe — no SQE/CQE round-trip, no channel hop to the background uring threads, and no CPU spent by the uring spin/yield loop.

| Field                                | Property                                       | Env var                                       | Default |
| ------------------------------------ | ---------------------------------------------- | --------------------------------------------- | ------- |
| `client_cache_sync_read_enabled`     | `goosefs.user.client.cache.sync.read.enabled`  | `GOOSEFS_USER_CLIENT_CACHE_SYNC_READ_ENABLED` | `false` |

### When to enable

- Analytical scans with a **high cache hit rate** running on **local NVMe**, where profiling shows io_uring submission/completion overhead dominating.
- The working set mostly fits the OS page cache (~µs per read); cold-working-set scans with frequent disk reads may be faster with io_uring's overlapped reads.

### When to keep off (default)

- **Latency-sensitive point-lookup workloads** that share the tokio runtime — the calling worker is blocked for the duration of each read.
- **HDD / NFS / Lustre** cache directories — there is **no read timeout** on a sync `pread` (the io_uring path uses a 30 s `URING_OP_TIMEOUT`); a slow device can block the worker unbounded.
- Cold-working-set scans where disk reads dominate.

### Threading caveats

- The calling **tokio worker is blocked** for the duration of each syscall. Cache misses never reach the store (the manager returns early), so the block is bounded by *local* read latency.
- Batched reads (`get_batch_bytes` via `join_all`) run on one task, so in sync mode a batch becomes **serial preads on one worker** — fine when the working set is OS-page-cache-hot, lossy when it is not.
- The page fd cache and dir fd cache are shared with the io_uring path; on-disk layout is unchanged, so the flag can be flipped across restarts freely.
- Write / delete paths always stay on io_uring regardless of this switch.

### Example

```rust
use goosefs_sdk::config::GoosefsConfig;

let mut config = GoosefsConfig::new("127.0.0.1:9200");
config.client_cache_enabled = true;
config.client_cache_dirs = vec!["/var/cache/goosefs".into()];  // local NVMe
config.client_cache_uring_enabled = true;
config.client_cache_sync_read_enabled = true;                  // analytical workload
```

Or via properties:

```properties
goosefs.user.client.cache.enabled=true
goosefs.user.client.cache.uring.enabled=true
goosefs.user.client.cache.sync.read.enabled=true
```

Design notes: [`docs/CLIENT_CONFIGURATION.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_CONFIGURATION.md#sync-pread-read-mode-linux-only).

Design notes: [`docs/CLIENT_PAGE_CACHE_DESIGN.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_PAGE_CACHE_DESIGN.md).
