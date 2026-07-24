---
sidebar_position: 7
---

# Streaming Read / Write

The Rust SDK provides three I/O types for streaming file access:

- **`GoosefsFileReader`** — block-by-block sequential reader (worker-direct, skips client cache).
- **`GoosefsFileInStream`** — streaming reader with `seek` / `read_at` (consults client page cache when enabled).
- **`GoosefsFileWriter`** — streaming writer with `write` / `flush` / `close` / `cancel`.

For one-shot reads/writes, see the convenience methods on [FileSystem API](./filesystem-api).

## Streaming Read: `GoosefsFileInStream`

`GoosefsFileInStream` is the recommended streaming reader. It supports `seek`, `read`, and positioned `read_at`, and is the only read path that consults the [client page cache](./page-cache).

```rust
use std::sync::Arc;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::io::GoosefsFileInStream;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let ctx = FileSystemContext::connect(GoosefsConfig::new("127.0.0.1:9200")).await?;

    let mut stream = GoosefsFileInStream::open_with_context(ctx.clone(), "/data/large.bin").await?;

    // Sequential read into a caller-provided buffer.
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    println!("read {} bytes", n);

    // Seek to an absolute position.
    stream.seek(8192).await?;

    // Positioned read — does not move the cursor.
    let tail = stream.read_at(4096, 512).await?;
    println!("positioned read: {} bytes", tail.len());

    // Read the entire remaining file.
    let all = stream.read_all().await?;

    ctx.close().await?;
    Ok(())
}
```

### `seek_from` with `std::io::SeekFrom`

```rust
use std::io::SeekFrom;

// SEEK_SET / SEEK_CUR / SEEK_END
stream.seek_from(SeekFrom::Start(0)).await?;
stream.seek_from(SeekFrom::Current(100)).await?;
stream.seek_from(SeekFrom::End(-50)).await?;
```

### Inspection methods

| Method | Returns | Description |
|--------|---------|-------------|
| `pos()` | `i64` | Current byte position |
| `len()` | `i64` | File length |
| `is_empty()` | `bool` | `len() == 0` |
| `is_eof()` | `bool` | `pos() >= len()` |
| `remaining()` | `i64` | `len() - pos()` |

### Concurrency model

`GoosefsFileInStream` holds an internal cursor state and is **not `Sync`**. Use one stream per task; for parallel reads, open multiple streams or use `read_at()` (positioned, does not conflict with the cursor, but still requires `&mut self`).

## Streaming Read: `GoosefsFileReader`

`GoosefsFileReader` reads block-by-block directly from the worker, bypassing the client page cache. Use it for bulk sequential reads where cache is unnecessary.

```rust
use goosefs_sdk::io::GoosefsFileReader;

let mut reader = GoosefsFileReader::open_with_context(ctx.clone(), "/data/bulk.bin").await?;
while let Some(chunk) = reader.read_next_block().await? {
    println!("got {} bytes", chunk.len());
}
```

## Streaming Write: `GoosefsFileWriter`

```rust
use goosefs_sdk::fs::{BaseFileSystem, options::CreateFileOptions};
use goosefs_sdk::config::WriteType;

let fs = BaseFileSystem::from_context(ctx.clone());
let opts = CreateFileOptions::with_write_type(WriteType::CacheThrough);
let mut writer = fs.create_file("/data/output.bin", opts).await?;

writer.write(b"first chunk ").await?;
writer.write(b"second chunk").await?;
writer.flush().await?;   // push to worker (does not commit)
writer.close().await?;   // finalise and commit to master
```

### Cancel vs Close

```rust
// close() — commits the file to master. Idempotent.
writer.close().await?;

// cancel() — abandons all uncommitted state. Idempotent.
// Use on error paths to clean up half-written files.
writer.cancel().await?;
```

:::warning
Dropping a `GoosefsFileWriter` without calling `close()` or `cancel()` triggers a best-effort cleanup, but in-flight blocks or UFS files may leak if no tokio runtime is available. Always call `close()` or `cancel()` explicitly.
:::

### One-shot write

```rust
// Create + write + close in one call.
GoosefsFileWriter::write_file_with_context(ctx.clone(), "/data/hello.txt", b"Hello, GooseFS!").await?;
```

## Inspection methods (writer)

| Method | Returns | Description |
|--------|---------|-------------|
| `bytes_written()` | `u64` | Total bytes accepted by `write()` |
| `path()` | `&str` | File path |
| `is_completed()` | `bool` | `close()` has been called |
| `is_cancelled()` | `bool` | `cancel()` has been called |
| `file_info()` | `&FileInfo` | Metadata from `create_file` |
