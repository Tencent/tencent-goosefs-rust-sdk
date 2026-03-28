# GooseFS Rust gRPC Client

![Experimental](https://img.shields.io/badge/status-experimental-orange)
![Rust](https://img.shields.io/badge/rust-1.75%2B-blue)
![License](https://img.shields.io/badge/license-Apache--2.0-green)

A native Rust client library that communicates directly with [GooseFS](https://cloud.tencent.com/document/product/1424) Master/Worker via gRPC (tonic/protobuf).

## Why GooseFS?

[GooseFS](https://cloud.tencent.com/document/product/1424) is a high-performance distributed caching file system built on top of COS (Cloud Object Storage). It accelerates data access for big data and AI/ML workloads by providing a unified namespace and intelligent caching layer between compute engines and cloud storage.

## Why GooseFS Rust Client?

This is a standalone Rust gRPC client crate (Layer 3) in the **Lance → OpenDAL → GooseFS** architecture. It talks directly to GooseFS Master and Worker services over gRPC, enabling:

- **Native performance** — Zero-copy block streaming with bidirectional gRPC, no JNI/FFI overhead
- **Async-first** — Built entirely on `tokio` + `tonic` for high-concurrency I/O
- **Lance integration** — Designed as the foundation for the OpenDAL GooseFS backend powering Lance vector storage acceleration

```text
┌────────────────────────────────────────────────────────────────┐
│  Layer 1 — Lance Provider (lance-io / ObjectStore)             │
├────────────────────────────────────────────────────────────────┤
│  Layer 2 — OpenDAL GooseFS Service (opendal::services)         │
├────────────────────────────────────────────────────────────────┤
│  Layer 3 — GooseFS Rust gRPC Client  ← this crate             │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ★ High-Level API (recommended)                          │  │
│  │  GooseFsFileWriter — end-to-end file write pipeline      │  │
│  │  GooseFsFileReader — end-to-end file read pipeline       │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  MasterClient    — File metadata CRUD    (Master:9200)   │  │
│  │  WorkerMgrClient — Worker discovery      (Master:9200)   │  │
│  │  VersionClient   — Service handshake     (Master:9200)   │  │
│  │  WorkerClient    — Block streaming       (Worker:9203)   │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  BlockMapper     — file range → block read plans         │  │
│  │  WorkerRouter    — consistent hash block → worker        │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  GrpcBlockReader — bidirectional streaming read          │  │
│  │  GrpcBlockWriter — bidirectional streaming write         │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Step 1: Start a GooseFS Cluster

#### Requirements

GooseFS runs on all UNIX-like environments (Linux, macOS). Make sure you have:

- **Java 11** (required — set `JAVA_HOME` accordingly)
- A running GooseFS Master (default RPC port `9200`) and at least one Worker (default data port `9203`)

```shell
# Example: start GooseFS locally (adjust paths to your installation)
export JAVA_HOME=/path/to/jdk-11
cd /path/to/goosefs
./bin/goosefs-start.sh local SudoMount
```

Verify the cluster is healthy:

```shell
./bin/goosefs fs ls /
```

#### Requirements (Rust side)

- **Rust 1.75+** — Install via [rustup](https://www.rust-lang.org/tools/install)
- **protoc** — Protocol Buffers compiler (needed by `tonic-build` at compile time)

```shell
# macOS
brew install protobuf

# Ubuntu / Debian
sudo apt install -y protobuf-compiler
```

### Step 2: Build the Client

```shell
git clone <repo-url> goosefs-client-rust
cd goosefs-client-rust
cargo build
```

### Step 3: Use as a Dependency

Add to your project's `Cargo.toml`:

```toml
[dependencies]
goosefs-client-rs = { path = "../goosefs-client-rust" }
tokio = { version = "1", features = ["full"] }
```

### Example: File Metadata Operations

```rust
use goosefs_client::client::MasterClient;
use goosefs_client::config::GooseFsConfig;

#[tokio::main]
async fn main() -> goosefs_client::error::Result<()> {
    // 1. Connect to GooseFS Master
    let config = GooseFsConfig::new("127.0.0.1:9200");
    let master = MasterClient::connect(&config).await?;

    // 2. Create a directory
    master.create_directory("/data/my-dataset", true).await?;

    // 3. Stat a file
    let file_info = master.get_status("/data/my-dataset").await?;
    println!("path: {:?}, length: {:?}", file_info.path, file_info.length);

    // 4. List directory contents
    let entries = master.list_status("/data", false).await?;
    for entry in &entries {
        println!("  {:?} ({:?} bytes)", entry.path, entry.length);
    }

    // 5. Rename
    master.rename("/data/my-dataset", "/data/renamed-dataset").await?;

    // 6. Delete
    master.delete("/data/renamed-dataset", true).await?;

    Ok(())
}
```

### Example: Multi-Master Connection

```rust
use goosefs_client::config::GooseFsConfig;
use goosefs_client::client::MasterClient;

#[tokio::main]
async fn main() -> goosefs_client::error::Result<()> {
    // Unified constructor: automatically selects single or multi-master mode.
    // 1 address  → single-master
    // 2+ addresses → multi-master (polls all to discover Primary)
    let addrs = vec![
        "10.0.0.1:9200".to_string(),
        "10.0.0.2:9200".to_string(),
        "10.0.0.3:9200".to_string(),
    ];
    let config = GooseFsConfig::from_addresses(addrs);
    println!("is_multi_master = {}", config.is_multi_master());

    let master = MasterClient::connect(&config).await?;
    let entries = master.list_status("/", false).await?;
    for e in &entries {
        println!("  {:?}", e.path);
    }
    Ok(())
}
```

### Example: High-Level File Write (Recommended)

```rust
use goosefs_client::io::GooseFsFileWriter;
use goosefs_client::config::GooseFsConfig;
use goosefs_client::WritePType;

#[tokio::main]
async fn main() -> goosefs_client::error::Result<()> {
    let config = GooseFsConfig::new("127.0.0.1:9200");

    // One-shot write: creates file, writes data, completes file in one call
    // Default WriteType is MUST_CACHE (data in cache only)
    GooseFsFileWriter::write_file(&config, "/data/hello.txt", b"Hello, GooseFS!").await?;

    // Or use the builder for multi-chunk streaming writes
    let mut writer = GooseFsFileWriter::create(&config, "/data/large-file.bin").await?;
    writer.write(b"first chunk ").await?;
    writer.write(b"second chunk ").await?;
    writer.write(b"final chunk").await?;
    writer.close().await?;
    println!("wrote {} bytes", writer.bytes_written());

    // ── Write with different WriteTypes ──

    // CACHE_THROUGH — write to cache + sync persist to UFS (COS/S3/HDFS)
    let ct_config = GooseFsConfig::new("127.0.0.1:9200")
        .with_write_type(WritePType::CacheThrough);
    GooseFsFileWriter::write_file(&ct_config, "/data/durable.txt", b"persisted!").await?;

    // THROUGH — write directly to UFS, bypass cache
    let th_config = GooseFsConfig::new("127.0.0.1:9200")
        .with_write_type(WritePType::Through);
    GooseFsFileWriter::write_file(&th_config, "/data/direct.txt", b"direct to UFS").await?;

    // ASYNC_THROUGH — write to cache, async persist after close()
    let at_config = GooseFsConfig::new("127.0.0.1:9200")
        .with_write_type(WritePType::AsyncThrough);
    GooseFsFileWriter::write_file(&at_config, "/data/async.txt", b"eventually persisted").await?;
    // close() automatically calls scheduleAsyncPersistence

    Ok(())
}
```

### Example: High-Level File Read (Recommended)

```rust
use goosefs_client::io::GooseFsFileReader;
use goosefs_client::config::GooseFsConfig;

#[tokio::main]
async fn main() -> goosefs_client::error::Result<()> {
    let config = GooseFsConfig::new("127.0.0.1:9200");

    // One-shot: read entire file
    let data = GooseFsFileReader::read_file(&config, "/data/hello.txt").await?;
    println!("content: {}", String::from_utf8_lossy(&data));

    // Range read: read 500 bytes starting at offset 100
    let range = GooseFsFileReader::read_range(&config, "/data/hello.txt", 100, 500).await?;

    // Streaming read: process block-by-block
    let mut reader = GooseFsFileReader::open(&config, "/data/hello.txt").await?;
    while let Some(chunk) = reader.read_next_block().await? {
        println!("got {} bytes from block", chunk.len());
    }

    Ok(())
}
```

### Example: Block-Level Streaming Read

```rust
use goosefs_client::client::{MasterClient, WorkerClient, WorkerManagerClient};
use goosefs_client::block::{BlockMapper, WorkerRouter};
use goosefs_client::io::GrpcBlockReader;
use goosefs_client::config::GooseFsConfig;

#[tokio::main]
async fn main() -> goosefs_client::error::Result<()> {
    let config = GooseFsConfig::new("127.0.0.1:9200");

    // 1. Get file metadata
    let master = MasterClient::connect(&config).await?;
    let file_info = master.get_status("/data/my-file.parquet").await?;

    // 2. Discover workers and build router
    let wm = WorkerManagerClient::connect(&config).await?;
    let workers = wm.get_worker_info_list().await?;
    let router = WorkerRouter::new();
    router.update_workers(workers).await;

    // 3. Map file range to block-level read plans
    let plans = BlockMapper::plan_read(&file_info, 0, file_info.length.unwrap_or(0) as u64);

    // 4. Stream-read each block
    for plan in &plans {
        let worker_info = router.select_worker(plan.block_id).await?;
        let addr = worker_info.address.as_ref().unwrap();
        let worker_addr = format!(
            "{}:{}",
            addr.host.as_deref().unwrap_or("127.0.0.1"),
            addr.rpc_port.unwrap_or(9203)
        );

        let worker = WorkerClient::connect(&worker_addr, config.connect_timeout).await?;
        let mut reader = GrpcBlockReader::open(
            &worker,
            plan.block_id,
            plan.offset_in_block as i64,
            plan.length as i64,
            config.chunk_size as i64,
        ).await?;

        let data = reader.read_all().await?;
        println!(
            "block {} — read {} bytes (complete: {})",
            plan.block_id,
            data.len(),
            reader.is_complete()
        );
    }

    Ok(())
}
```

## Modules

| Module | Description |
|--------|-------------|
| **`io::GooseFsFileWriter`** | **High-level file writer** — one-shot `write_file()` or builder pattern `create()` → `write()` → `close()`. Supports all 4 WriteTypes (MUST_CACHE / CACHE_THROUGH / THROUGH / ASYNC_THROUGH). Orchestrates `CreateFile` → `WriteStrategy` → `BlockMapper` → `WorkerRouter` → `GrpcBlockWriter` → `CompleteFile` [→ `ScheduleAsyncPersistence`]. **Note**: CACHE_THROUGH and THROUGH both use `UfsFile` mode — Worker writes directly to UFS (CACHE_THROUGH also caches data locally). |
| **`io::GooseFsFileReader`** | **High-level file reader** — one-shot `read_file()` / `read_range()` or streaming `open()` → `read_next_block()`. Orchestrates `GetStatus` → `BlockMapper` → `WorkerRouter` → `GrpcBlockReader` |
| `client::MasterClient` | File system metadata CRUD — `get_status`, `list_status`, `create_file`, `complete_file`, `delete`, `rename`, `create_directory`, `schedule_async_persistence` |
| `client::WorkerManagerClient` | Worker discovery — `get_worker_info_list` |
| `client::WorkerClient` | Bidirectional streaming block read/write — `read_block`, `write_block(options: WriteBlockOptions)` |
| `client::WriteBlockOptions` | Controls `RequestType` (GoosefsBlock / UfsFile) and optional `CreateUfsFileOptions` for CACHE_THROUGH and THROUGH-mode writes |
| `block::BlockMapper` | Converts file-level byte ranges into block-level read/write plans |
| `block::WorkerRouter` | Consistent-hash routing of block IDs to workers with failure tracking |
| `io::GrpcBlockReader` | Low-level streaming block reader with flow-control ACK |
| `io::GrpcBlockWriter` | Low-level streaming block writer with chunk splitting and flush |
| `config::GooseFsConfig` | Connection configuration — timeouts, block size, chunk size, root path, **`write_type`** (WritePType), multi-master settings (`master_addrs`, `master_polling_timeout`, retry params), unified constructor `from_addresses()` |
| `WritePType` | Write type enum — `MustCache`, `TryCache`, `CacheThrough`, `Through`, `AsyncThrough`, `None` |
| `error::Error` | Unified error type with gRPC status code mapping and retriable detection |

## gRPC Services

This client wraps **4 GooseFS gRPC services** defined in 11 proto files:

| Service | Port | Proto | Key RPCs |
|---------|------|-------|----------|
| `FileSystemMasterClientService` | Master:9200 | `file_system_master.proto` | GetStatus, ListStatus, CreateFile, CompleteFile, Delete, Rename, CreateDirectory … (37 RPCs) |
| `BlockWorker` | Worker:9203 | `block_worker.proto` | ReadBlock *(bidi-stream)*, WriteBlock *(bidi-stream)*, AsyncCache, RemoveBlock … (12 RPCs) |
| `WorkerManagerMasterClientService` | Master:9200 | `worker_manager_master.proto` | GetWorkerInfoList, GetCapacityBytes, GetUsedBytes … (9 RPCs) |
| `ServiceVersionClientService` | Master:9200 | `version.proto` | GetServiceVersion |

## Project Structure

```
goosefs-client-rust/
├── Cargo.toml              # crate manifest
├── build.rs                # tonic-build proto compilation
├── proto/                  # GooseFS protobuf definitions (11 files)
│   ├── grpc/               #   Master/Worker service protos
│   └── proto/              #   Shared data types (security, acl, status)
├── src/
│   ├── lib.rs              # crate root & proto module tree
│   ├── config.rs           # GooseFsConfig
│   ├── error.rs            # Error enum + From impls
│   ├── client/
│   │   ├── master.rs       # MasterClient
│   │   ├── worker.rs       # WorkerClient
│   │   └── worker_manager.rs # WorkerManagerClient
│   ├── block/
│   │   ├── mapper.rs       # BlockMapper (file → block plans)
│   │   └── router.rs       # WorkerRouter (consistent hash)
│   ├── io/
│   │   ├── file_reader.rs  # GooseFsFileReader (high-level)
│   │   ├── file_writer.rs  # GooseFsFileWriter (high-level)
│   │   ├── reader.rs       # GrpcBlockReader (low-level)
│   │   └── writer.rs       # GrpcBlockWriter (low-level)
│   └── generated/          # prost/tonic generated code (git-ignored)
├── examples/
│   ├── highlevel_file_rw.rs     # ★ High-level file read/write (recommended)
│   ├── write_types.rs           # ★ WriteType comparison (MUST_CACHE/CACHE_THROUGH/THROUGH/ASYNC_THROUGH)
│   ├── ha_multi_master.rs       # ★ Multi-master mode (auto single/multi via from_addresses)
│   ├── lowlevel_block_read.rs   # Low-level block streaming read
│   ├── lowlevel_create_file.rs  # Low-level file creation (metadata only)
│   ├── metadata_crud.rs         # File/directory metadata CRUD
│   └── async_persistence.rs     # Async persistence scheduling
└── target/                 # build artifacts (git-ignored)
```

## Development

### Build

```shell
cargo build
```

### Test

```shell
cargo test
```

### Build with Release Optimizations

```shell
cargo build --release
```

### Re-generate Proto Code

Proto files are compiled automatically by `build.rs` during `cargo build`. The generated Rust code is written to `src/generated/` and is git-ignored. To force regeneration:

```shell
cargo clean
cargo build
```

### Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `tonic` | 0.12 | gRPC framework (HTTP/2 + protobuf) |
| `prost` | 0.13 | Protobuf code generation & runtime |
| `tokio` | 1.x | Async runtime |
| `tokio-stream` | 0.1 | Stream utilities for bidirectional gRPC |
| `bytes` | 1.x | Zero-copy byte buffers |
| `thiserror` | 2.x | Ergonomic error derives |
| `dashmap` | 6.x | Concurrent hash map (failure tracking) |
| `tracing` | 0.1 | Structured logging |
| `serde` | 1.x | Config serialization |

## GooseFS Compatibility

| GooseFS Version | Java | Status |
|----------------|------|--------|
| Latest (JDK 11) | Java 11 | ✅ Supported |

> **Note:** GooseFS requires **Java 11**. Make sure `JAVA_HOME` points to a JDK 11 installation when running the GooseFS cluster.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
