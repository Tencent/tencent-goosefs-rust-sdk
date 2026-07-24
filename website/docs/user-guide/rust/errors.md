---
sidebar_position: 8
---

# Error Handling

All GooseFS SDK operations return `Result<T, goosefs_sdk::error::Error>`. The `Error` enum is implemented with `thiserror` and implements `std::error::Error` (with `Display` and `source`), so it integrates with the standard Rust error ecosystem.

## Error variants

| Variant | Typical cause |
|---------|---------------|
| `NotFound { path }` | Path does not exist |
| `AlreadyExists { path }` | `rename` to an existing destination |
| `PermissionDenied { message }` | ACL or auth failure |
| `InvalidArgument { message }` | Malformed path, bad offset |
| `InvalidPath { path }` | Path violates GooseFS naming rules |
| `FileIncomplete { message }` | File still being written (not `close()`d) |
| `DirectoryNotEmpty { message }` | Non-recursive `delete` on non-empty dir |
| `OpenDirectory { path }` | Tried to read a directory as a file |
| `AuthenticationFailed { message }` | SASL handshake failed |
| `NoWorkerAvailable { message }` | No healthy worker for the block |
| `MasterUnavailable { message }` | All master replicas unreachable |
| `ConfigError { message }` | Invalid configuration |
| `GrpcError { message, source }` | gRPC protocol error (from `tonic::Status`) |
| `TransportError { message, source }` | gRPC connection error |
| `BlockIoError { message }` | Local I/O failure (block read/write) |
| `MissingField { field }` | Required protobuf field missing in response |
| `Internal { message, source }` | Generic internal error |

## Usage

```rust
use goosefs_sdk::error::Error;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::config::GoosefsConfig;

#[tokio::main]
async fn main() {
    let ctx = FileSystemContext::connect(GoosefsConfig::new("127.0.0.1:9200"))
        .await
        .unwrap();

    match ctx.acquire_master().get_status("/data/missing").await {
        Ok(status) => println!("found: {:?}", status.path),
        Err(Error::NotFound { path }) => println!("not found: {}", path),
        Err(Error::PermissionDenied { message }) => eprintln!("denied: {}", message),
        Err(e) => eprintln!("other error: {}", e),
    }
}
```

## Type aliases

The crate exports a convenience alias so you don't need to write the full path:

```rust
// src/error.rs
pub type Result<T> = std::result::Result<T, Error>;
```

```rust
use goosefs_sdk::error::Result;
use bytes::Bytes;

async fn read_file(ctx: &std::sync::Arc<goosefs_sdk::context::FileSystemContext>) -> Result<Bytes> {
    goosefs_sdk::io::GoosefsFileReader::read_file_with_context(ctx.clone(), "/data/file").await
}
```

## Converting to `tonic::Status`

The SDK implements `From<tonic::Status>` for `Error`, so gRPC failures are automatically mapped at the client boundary. No manual conversion is needed.

## Common patterns

### Retry on transient errors

```rust
use goosefs_sdk::error::Error;
use bytes::Bytes;
use std::time::Duration;

async fn retry_read(ctx: &std::sync::Arc<goosefs_sdk::context::FileSystemContext>, path: &str) -> Result<Bytes> {
    for attempt in 0..3 {
        match goosefs_sdk::io::GoosefsFileReader::read_file_with_context(ctx.clone(), path).await {
            Ok(data) => return Ok(data),
            Err(Error::GrpcError { .. } | Error::TransportError { .. } | Error::MasterUnavailable { .. } | Error::NoWorkerAvailable { .. }) if attempt < 2 => {
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
```

### Distinguish "not found" from other errors

```rust
use goosefs_sdk::error::Error;

match ctx.acquire_master().get_status(path).await {
    Ok(status) => Some(status),
    Err(Error::NotFound { .. }) => None,  // expected — path may not exist yet
    Err(e) => return Err(e),
}
```
