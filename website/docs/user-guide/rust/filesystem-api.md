---
sidebar_position: 2
---

# FileSystem API

The recommended entry point is `BaseFileSystem` backed by a shared `FileSystemContext`. Build the context once per process — it owns Master/Worker connection pools, config refresh, and metrics tasks.

```rust
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::{BaseFileSystem, FileSystem, OpenFileOptions};
use std::io::SeekFrom;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx = FileSystemContext::connect(config).await?;
    let fs = BaseFileSystem::from_context(ctx);

    let status = fs.get_status("/data/file.parquet").await?;
    println!("length = {}", status.length);

    let entries = fs.list_status("/data", false).await?;
    for e in &entries {
        println!("  {} ({} bytes)", e.name, e.length);
    }

    let mut stream = fs.open_file("/data/file.parquet", OpenFileOptions::default()).await?;
    let data = stream.read(1024).await?;
    stream.seek(SeekFrom::Start(4096)).await?;
    let _ = stream.read_at(8192, 256).await?;

    Ok(())
}
```

## High-Level Write

```rust
use std::sync::Arc;
use goosefs_sdk::config::{GoosefsConfig, WriteType};
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::BaseFileSystem;
use goosefs_sdk::fs::options::CreateFileOptions;
use goosefs_sdk::io::GoosefsFileWriter;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;

    // One-shot write
    GoosefsFileWriter::write_file_with_context(ctx.clone(), "/data/hello.txt", b"Hello, GooseFS!").await?;

    // Streaming write
    let mut writer = GoosefsFileWriter::create_with_context(ctx.clone(), "/data/large-file.bin", None).await?;
    writer.write(b"first chunk ").await?;
    writer.write(b"second chunk").await?;
    writer.close().await?;

    // WriteType via FileSystem API
    let fs = BaseFileSystem::from_context(ctx.clone());
    let opts = CreateFileOptions::with_write_type(WriteType::CacheThrough);
    fs.write_file("/data/durable.txt", b"persisted!", opts).await?;

    ctx.close().await?;
    Ok(())
}
```

## High-Level Read

```rust
use std::sync::Arc;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::io::GoosefsFileReader;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;

    let data = GoosefsFileReader::read_file_with_context(ctx.clone(), "/data/hello.txt").await?;
    let range = GoosefsFileReader::read_range_with_context(ctx.clone(), "/data/hello.txt", 100, 500).await?;

    let mut reader = GoosefsFileReader::open_with_context(ctx.clone(), "/data/hello.txt").await?;
    while let Some(chunk) = reader.read_next_block().await? {
        println!("got {} bytes", chunk.len());
    }

    ctx.close().await?;
    Ok(())
}
```

:::tip
`GoosefsFileInStream` (via `fs.open_file`) is the path that consults the **client local page cache**. One-shot `GoosefsFileReader::read_file` / `read_range` go worker-direct and skip the cache.
:::

## Multi-Master (HA)

```rust
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::client::MasterClient;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let addrs = vec![
        "10.0.0.1:9200".to_string(),
        "10.0.0.2:9200".to_string(),
        "10.0.0.3:9200".to_string(),
    ];
    let config = GoosefsConfig::from_addresses(addrs);
    let master = MasterClient::connect(&config).await?;
    let _ = master.list_status("/", false).await?;
    Ok(())
}
```

One address → single-master. Two or more → multi-master (polls to discover the Primary).
