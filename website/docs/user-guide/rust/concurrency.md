---
sidebar_position: 11
---

# Concurrency Model

The GooseFS Rust SDK is fully async (`tokio`) and designed for high-concurrency workloads. This page covers the key concurrency concepts every user should know.

## `FileSystemContext`: One Per Process

`FileSystemContext` owns the Master/Worker connection pools, config refresher, and metrics tasks. Build it once and share via `Arc`:

```rust
use std::sync::Arc;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::fs::{BaseFileSystem, FileSystem};

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(GoosefsConfig::new("127.0.0.1:9200")).await?;
    let fs = BaseFileSystem::from_context(ctx.clone());

    // Spawn many concurrent operations sharing the same context.
    let mut handles = Vec::new();
    for i in 0..100 {
        let fs = fs.clone();
        handles.push(tokio::spawn(async move {
            fs.exists(&format!("/data/file-{}", i)).await
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    ctx.close().await?;
    Ok(())
}
```

:::note
`FileSystemContext` is `Send + Sync` and can be freely cloned as `Arc<FileSystemContext>`. All internal pools are thread-safe.
:::

## Connection Pools

### Master connection pool

The master pool maintains multiple HTTP/2 channels to the master. Default size is **1** (single channel). Raise to 4-8 for high-concurrency remote scenarios:

```rust
use goosefs_sdk::config::{GoosefsConfig, MasterPoolSchedule};

let mut config = GoosefsConfig::new("127.0.0.1:9200");
config.master_connection_pool_size = 8;
config.master_connection_pool_schedule = MasterPoolSchedule::P2C;
```

See [Configuration](./configuration) for env var / properties alternatives.

### Worker connection pool

The worker pool is per-worker-address. Default is `min(cores, 4)`. Each `WorkerClient::acquire()` returns an owned `WorkerClient`; dropping it returns the channel to the pool.

### `ConfigRefresher`

A background task reloads transparent-acceleration switches from properties/env. `ConfigRefresher` uses a 30s expiry duration, while `FileSystemContext::connect()` starts an eager load followed by refresh checks every 60s.

## Async I/O with `tokio`

All SDK methods are `async fn` and run on the `tokio` runtime. They never block the calling thread — I/O is driven by `tokio`'s reactor.

```rust
// Concurrent reads with tokio::join!
let (a, b) = tokio::join!(
    GoosefsFileReader::read_file_with_context(ctx.clone(), "/data/a"),
    GoosefsFileReader::read_file_with_context(ctx.clone(), "/data/b"),
);
```

### `GoosefsFileInStream` is not `Sync`

`GoosefsFileInStream` holds internal cursor state and requires `&mut self` for `read` / `seek`. It is `Send` but not `Sync` — use one stream per task:

```rust
// ✅ One stream per task — safe.
let mut tasks = Vec::new();
for path in paths {
    let ctx = ctx.clone();
    tasks.push(tokio::spawn(async move {
        let mut s = GoosefsFileInStream::open_with_context(
            ctx,
            &path,
            goosefs_sdk::fs::options::OpenFileOptions::new(),
        ).await?;
        s.read_all().await
    }));
}
```

### `GoosefsFileWriter` must be closed or cancelled

Dropping a `GoosefsFileWriter` without `close()` or `cancel()` triggers best-effort cleanup, but in-flight blocks may leak if no tokio runtime is available. Always call one or the other:

```rust
// ✅ Explicit close on success path.
writer.close().await?;

// ✅ Cancel on error path.
if let Err(e) = writer.write(data).await {
    writer.cancel().await.ok();
    return Err(e);
}
```

## Multi-Master (HA)

```rust
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;

let addrs = vec!["10.0.0.1:9200".to_string(), "10.0.0.2:9200".to_string(), "10.0.0.3:9200".to_string()];
let config = GoosefsConfig::from_addresses(addrs);
let ctx = FileSystemContext::connect(config).await?;
```

Two or more addresses → multi-master mode. The client polls to discover the Primary automatically. If the Primary fails, the client retries on the next replica.

## Graceful Shutdown

```rust
// Close the context to stop its background tasks and mark it as closed.
ctx.close().await?;
assert!(ctx.is_closed());
```

`close()` is idempotent. After close, `is_closed()` returns `true` and the context's background refresh and metrics tasks have stopped.
