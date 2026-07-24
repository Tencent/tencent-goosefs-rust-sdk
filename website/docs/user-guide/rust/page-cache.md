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

Design notes: [`docs/CLIENT_PAGE_CACHE_DESIGN.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_PAGE_CACHE_DESIGN.md).
