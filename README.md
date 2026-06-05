# Goosefs Rust gRPC Client

![Experimental](https://img.shields.io/badge/status-experimental-orange)
![Rust](https://img.shields.io/badge/rust-1.88%2B-blue)
![Version](https://img.shields.io/badge/version-0.1.5-blue)
![License](https://img.shields.io/badge/license-Apache--2.0-green)

A native Rust client library that communicates directly with [Goosefs](https://cloud.tencent.com/document/product/1424) Master/Worker via gRPC (tonic/protobuf).

## What's New in v0.1.5

- **Prometheus Pushgateway support** — New `PushgatewayTask` periodically pushes all client metrics (counters & gauges) to a Prometheus Pushgateway endpoint via HTTP POST in the standard text exposition format. Configure with `PushgatewayConfig::new(endpoint, job)` and spawn a background task. See [`docs/METRICS.md`](docs/METRICS.md) for architecture and usage.
- **Python SDK bindings** — New `bindings/python/` sub-crate providing a full-featured Python client (`goosefs` package) built on PyO3 + maturin. Supports sync/async APIs, streaming I/O, metrics, and tracing. See [`bindings/python/README.md`](bindings/python/README.md).
- **AsyncRead/AsyncSeek** — `GoosefsAsyncReader` now implements `tokio::io::AsyncRead` + `tokio::io::AsyncSeek`, enabling seamless integration with the tokio ecosystem.
- **FileInStream read fix** — Fixed a bug where `GoosefsFileInStream::read` could drop bytes when the caller-supplied buffer was smaller than the available chunk data.
- **Dependency updates** — Pinned dependency versions (prost 0.14.1, tokio 1.23+, rand 0.9.1, reqwest 0.12 with `rustls-tls`). Adapted to rand 0.9 API changes (`thread_rng` → `rng`, `gen_range` → `random_range`).
- **No breaking API changes** — Drop-in upgrade from `0.1.3` / `0.1.4`; downstream `OpenDAL` / `Lance` integrations require no code changes.

## Why Goosefs?

[Goosefs](https://cloud.tencent.com/document/product/1424) is a high-performance distributed caching file system built on top of COS (Cloud Object Storage). It accelerates data access for big data and AI/ML workloads by providing a unified namespace and intelligent caching layer between compute engines and cloud storage.

## Why Goosefs Rust Client?

This is a standalone Rust gRPC client crate (Layer 3) in the **Lance → OpenDAL → Goosefs** architecture. It talks directly to Goosefs Master and Worker services over gRPC, enabling:

- **Native performance** — Zero-copy block streaming with bidirectional gRPC, no JNI/FFI overhead
- **Async-first** — Built entirely on `tokio` + `tonic` for high-concurrency I/O
- **Lance integration** — Designed as the foundation for the OpenDAL Goosefs backend powering Lance vector storage acceleration

```text
┌────────────────────────────────────────────────────────────────┐
│  Layer 1 — Lance Provider (lance-io / ObjectStore)             │
├────────────────────────────────────────────────────────────────┤
│  Layer 2 — OpenDAL Goosefs Service (opendal::services)         │
├────────────────────────────────────────────────────────────────┤
│  Layer 3 — Goosefs Rust gRPC Client  ← this crate             │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ★ FileSystem Abstraction (recommended entry point)      │  │
│  │  FileSystem trait + BaseFileSystem                       │  │
│  │  FileSystemContext — shared connection pool              │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  ★ High-Level I/O                                        │  │
│  │  GoosefsFileInStream — seekable dual-path read stream   │  │
│  │  GoosefsFileWriter — end-to-end file write pipeline      │  │
│  │  GoosefsFileReader — end-to-end file read pipeline       │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  MasterClient    — File metadata CRUD    (Master:9200)   │  │
│  │  WorkerMgrClient — Worker discovery      (Master:9200)   │  │
│  │  VersionClient   — Service handshake     (Master:9200)   │  │
│  │  WorkerClient    — Block streaming       (Worker:9203)   │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  ChannelAuthenticator — SASL auth (NOSASL / SIMPLE)      │  │
│  │  SaslClientHandler    — PLAIN SASL handshake             │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  BlockMapper     — file range → block read plans         │  │
│  │  WorkerRouter    — consistent hash + local-first routing │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  GrpcBlockReader — streaming + positioned read           │  │
│  │  GrpcBlockWriter — bidirectional streaming write         │  │
│  ├──────────────────────────────────────────────────────────┤  │
│   │  Metrics Registry — global counters / gauges             │  │
│   │  HeartbeatTask    — periodic delta report → Master       │  │
│   │  PushgatewayTask  — periodic push to Prometheus GW       │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Step 1: Start a Goosefs Cluster

#### Requirements

Goosefs runs on all UNIX-like environments (Linux, macOS). Make sure you have:

- **Java 11** (required — set `JAVA_HOME` accordingly)
- A running Goosefs Master (default RPC port `9200`) and at least one Worker (default data port `9203`)

```shell
# Example: start Goosefs locally (adjust paths to your installation)
export JAVA_HOME=/path/to/jdk-11
cd /path/to/goosefs
./bin/goosefs-start.sh local SudoMount
```

Verify the cluster is healthy:

```shell
./bin/goosefs fs ls /
```

#### Requirements (Rust side)

- **Rust 1.88+** — Install via [rustup](https://www.rust-lang.org/tools/install)
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
goosefs-sdk = { path = "../goosefs-client-rust" }
tokio = { version = "1", features = ["full"] }
```

### Example: File Metadata Operations

```rust
use goosefs_sdk::client::MasterClient;
use goosefs_sdk::config::GoosefsConfig;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    // 1. Connect to Goosefs Master
    let config = GoosefsConfig::new("127.0.0.1:9200");
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
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::client::MasterClient;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    // Unified constructor: automatically selects single or multi-master mode.
    // 1 address  → single-master
    // 2+ addresses → multi-master (polls all to discover Primary)
    let addrs = vec![
        "10.0.0.1:9200".to_string(),
        "10.0.0.2:9200".to_string(),
        "10.0.0.3:9200".to_string(),
    ];
    let config = GoosefsConfig::from_addresses(addrs);
    println!("is_multi_master = {}", config.is_multi_master());

    let master = MasterClient::connect(&config).await?;
    let entries = master.list_status("/", false).await?;
    for e in &entries {
        println!("  {:?}", e.path);
    }
    Ok(())
}
```

### Example: FileSystem API (Recommended)

```rust
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::{BaseFileSystem, FileSystem, OpenFileOptions};
use std::io::SeekFrom;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    // Build once per application — one TCP+SASL handshake, shared across all ops
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx = FileSystemContext::connect(config).await?;
    let fs = BaseFileSystem::from_context(ctx);

    // Metadata operations (all reuse the same Master connection)
    let status = fs.get_status("/data/file.parquet").await?;
    println!("length = {}", status.length);

    let entries = fs.list_status("/data", false).await?;
    for e in &entries {
        println!("  {} ({} bytes)", e.name, e.length);
    }

    let exists = fs.exists("/data/file.parquet").await?;
    println!("exists = {}", exists);

    // Open a seekable file input stream
    let mut stream = fs.open_file("/data/file.parquet", OpenFileOptions::default()).await?;

    // Sequential read
    let data = stream.read(1024).await?;
    println!("read {} bytes", data.len());

    // Seek to a position
    stream.seek(SeekFrom::Start(4096)).await?;
    let data = stream.read(512).await?;

    // Random read (does not change current position)
    let data = stream.read_at(8192, 256).await?;
    println!("random read {} bytes", data.len());

    Ok(())
}
```

### Example: High-Level File Write (Recommended)

```rust
use goosefs_sdk::io::GoosefsFileWriter;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::WritePType;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let config = GoosefsConfig::new("127.0.0.1:9200");

    // One-shot write: creates file, writes data, completes file in one call
    // Default WriteType is MUST_CACHE (data in cache only)
    GoosefsFileWriter::write_file(&config, "/data/hello.txt", b"Hello, Goosefs!").await?;

    // Or use the builder for multi-chunk streaming writes
    let mut writer = GoosefsFileWriter::create(&config, "/data/large-file.bin").await?;
    writer.write(b"first chunk ").await?;
    writer.write(b"second chunk ").await?;
    writer.write(b"final chunk").await?;
    writer.close().await?;
    println!("wrote {} bytes", writer.bytes_written());

    // ── Write with different WriteTypes ──

    // CACHE_THROUGH — write to cache + sync persist to UFS (COS/S3/HDFS)
    let ct_config = GoosefsConfig::new("127.0.0.1:9200")
        .with_write_type(WritePType::CacheThrough);
    GoosefsFileWriter::write_file(&ct_config, "/data/durable.txt", b"persisted!").await?;

    // THROUGH — write directly to UFS, bypass cache
    let th_config = GoosefsConfig::new("127.0.0.1:9200")
        .with_write_type(WritePType::Through);
    GoosefsFileWriter::write_file(&th_config, "/data/direct.txt", b"direct to UFS").await?;

    // ASYNC_THROUGH — write to cache, async persist after close()
    let at_config = GoosefsConfig::new("127.0.0.1:9200")
        .with_write_type(WritePType::AsyncThrough);
    GoosefsFileWriter::write_file(&at_config, "/data/async.txt", b"eventually persisted").await?;
    // close() automatically calls scheduleAsyncPersistence

    Ok(())
}
```

### Example: High-Level File Read (Recommended)

```rust
use goosefs_sdk::io::GoosefsFileReader;
use goosefs_sdk::config::GoosefsConfig;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let config = GoosefsConfig::new("127.0.0.1:9200");

    // One-shot: read entire file
    let data = GoosefsFileReader::read_file(&config, "/data/hello.txt").await?;
    println!("content: {}", String::from_utf8_lossy(&data));

    // Range read: read 500 bytes starting at offset 100
    let range = GoosefsFileReader::read_range(&config, "/data/hello.txt", 100, 500).await?;

    // Streaming read: process block-by-block
    let mut reader = GoosefsFileReader::open(&config, "/data/hello.txt").await?;
    while let Some(chunk) = reader.read_next_block().await? {
        println!("got {} bytes from block", chunk.len());
    }

    Ok(())
}
```

### Example: Client Metrics & Heartbeat

The SDK ships a built-in client-metrics pipeline. When `metrics_enabled = true`
(the default), each `FileSystemContext` spawns a background `HeartbeatTask`
that periodically reports **incremental counter deltas** to the GooseFS Master
via the `MetricsHeartbeat` RPC. The `io` layer auto-increments well-known
counters (`Client.BytesReadLocal`, `Client.BytesWrittenLocal`), and your
application can register additional counters/gauges via the global registry.

```rust
use std::sync::Arc;
use std::time::Duration;

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::metrics;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    // 1. Build a config with metrics enabled (default = true).
    //    Tune the heartbeat interval / timeout and tag the client with an app_id.
    let config = GoosefsConfig::new("127.0.0.1:9200")
        .with_metrics_enabled(true)
        .with_metrics_heartbeat_interval(Duration::from_secs(10)) // min = 1 s
        .with_metrics_heartbeat_timeout(Duration::from_secs(3))   // < interval
        .with_app_id("my-app");

    // 2. Connecting the context spawns the HeartbeatTask in the background.
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;

    // 3. Drive some I/O — the SDK auto-increments Client.BytesReadLocal /
    //    Client.BytesWrittenLocal during file read/write.
    GoosefsFileWriter::write_file_with_context(ctx.clone(), "/demo.bin", b"hello").await?;
    let _ = GoosefsFileReader::read_file_with_context(ctx.clone(), "/demo.bin").await?;

    // 4. Register and increment a custom counter — the heartbeat picks it
    //    up automatically (only non-zero deltas are reported).
    let app_ops = metrics::counter("Client.DemoOpsCount");
    app_ops.inc(1);

    // 5. Read SDK-managed counters at any time (process-global registry).
    let read_local = metrics::counter(metrics::name::CLIENT_BYTES_READ_LOCAL).get();
    let written_local = metrics::counter(metrics::name::CLIENT_BYTES_WRITTEN_LOCAL).get();
    println!("read_local = {}, written_local = {}", read_local, written_local);

    // 6. close() performs a final heartbeat flush before shutdown.
    ctx.close().await?;
    Ok(())
}
```

To disable the entire metrics pipeline (no background task, no RPC overhead):

```rust
let config = GoosefsConfig::new("127.0.0.1:9200")
    .with_metrics_enabled(false);
```

**Configuration knobs**

| Field | Default | Description |
|-------|---------|-------------|
| `metrics_enabled` | `true` | Master switch — when `false` the heartbeat task is not spawned. |
| `metrics_heartbeat_interval` | `10 s` | Period between heartbeat reports. Must be `>= 1 s`. |
| `metrics_heartbeat_timeout` | `3 s` | Per-RPC timeout. Must be `>= 1 s` and `< metrics_heartbeat_interval`. |
| `metrics_max_batch_size` | `512` | Max number of metric entries packed into a single heartbeat. |
| `app_id` | `None` | Optional client tag attached to every heartbeat (useful for grouping in Master logs). |

**Built-in counter names** (re-exported from `goosefs_sdk::metrics::name`):

- `Client.BytesReadLocal` — bytes read via local short-circuit (auto-incremented by the `io` layer).
- `Client.BytesWrittenLocal` — bytes written via local short-circuit (auto-incremented).
- `Client.BytesWrittenUfs` — bytes written directly to UFS (bypassing the cache).

> **Tip:** Run `cargo run --example metrics_heartbeat` for an end-to-end demo that exercises both `metrics_enabled = true` and `metrics_enabled = false`. Set `RUST_LOG=info` to see the SDK's heartbeat / flush logs.

### Example: Authentication

```rust
use goosefs_sdk::auth::AuthType;
use goosefs_sdk::client::MasterClient;
use goosefs_sdk::config::GoosefsConfig;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    // Default: SIMPLE mode with current OS username
    let config = GoosefsConfig::new("127.0.0.1:9200");
    let master = MasterClient::connect(&config).await?;
    let entries = master.list_status("/", false).await?;
    println!("root has {} entries", entries.len());

    // Explicit NOSASL mode (no SASL handshake)
    let config = GoosefsConfig::new("127.0.0.1:9200")
        .with_auth_type(AuthType::NoSasl);
    let master = MasterClient::connect(&config).await?;

    // Explicit SIMPLE mode with custom username
    let config = GoosefsConfig::new("127.0.0.1:9200")
        .with_auth_type(AuthType::Simple)
        .with_auth_username("myuser");
    let master = MasterClient::connect(&config).await?;

    Ok(())
}
```

#### Authentication Guide

Goosefs supports two authentication modes. The Rust client must use the mode that matches the server-side configuration, otherwise RPCs will be rejected with `Unauthenticated`.

**Authentication Modes**

| Mode | Server Config | Description |
|------|--------------|-------------|
| **NOSASL** | `goosefs.security.authentication.type=NOSASL` | No SASL handshake. The client generates a local channel-id for API consistency, but the server does not verify any credentials. Suitable for development/testing environments. |
| **SIMPLE** | `goosefs.security.authentication.type=SIMPLE` | PLAIN SASL handshake. The client sends a username via a bidirectional gRPC stream (`SaslAuthenticationService/Authenticate`), and the server returns a channel-id upon success. All subsequent RPCs carry this channel-id in gRPC metadata. This is the **default and recommended** mode. |

**Server-Side Configuration**

Set the authentication type in `conf/goosefs-site.properties` on the Goosefs Master/Worker:

```properties
# Option 1: SIMPLE authentication (recommended, default)
goosefs.security.authentication.type=SIMPLE

# Option 2: No authentication (development only)
# goosefs.security.authentication.type=NOSASL
```

> **Important:** After changing the authentication type, you must restart the Goosefs cluster for the change to take effect.

**Client-Side Configuration**

```rust
use goosefs_sdk::auth::AuthType;
use goosefs_sdk::config::GoosefsConfig;
use std::time::Duration;

// ── SIMPLE mode (default) ──
// GoosefsConfig::new() defaults to SIMPLE + current OS username.
// No extra configuration needed in most cases.
let config = GoosefsConfig::new("127.0.0.1:9200");

// ── SIMPLE mode with explicit username ──
let config = GoosefsConfig::new("127.0.0.1:9200")
    .with_auth_type(AuthType::Simple)
    .with_auth_username("myuser");

// ── SIMPLE mode with custom auth timeout ──
let config = GoosefsConfig::new("127.0.0.1:9200")
    .with_auth_type(AuthType::Simple)
    .with_auth_username("myuser")
    .with_auth_timeout(Duration::from_secs(30));

// ── NOSASL mode ──
// Use only when the server is configured with NOSASL.
let config = GoosefsConfig::new("127.0.0.1:9200")
    .with_auth_type(AuthType::NoSasl);
```

**Default Behavior**

| Config Field | Default Value | Description |
|-------------|---------------|-------------|
| `auth_type` | `AuthType::Simple` | Authentication mode |
| `auth_username` | Current OS username (`$USER` / `$USERNAME`) | Username sent during SASL handshake |
| `auth_timeout` | 10 seconds | Timeout for the SASL authentication handshake |

**Common Errors**

| Error | Cause | Solution |
|-------|-------|----------|
| `Channel: xxx is not authenticated` | Client uses NOSASL but server requires SIMPLE | Change client to `.with_auth_type(AuthType::Simple)` |
| `SASL authentication failed` | Server uses NOSASL but client sends SASL handshake | Change client to `.with_auth_type(AuthType::NoSasl)` |
| `Connection timeout during auth` | Network issue or server not responding | Check server status; increase `auth_timeout` |

> **Tip:** Run `cargo run --example auth_demo` for a comprehensive authentication demo that tests both modes.

### Example: Block-Level Streaming Read

```rust
use goosefs_sdk::client::{MasterClient, WorkerClient, WorkerManagerClient};
use goosefs_sdk::block::{BlockMapper, WorkerRouter};
use goosefs_sdk::io::GrpcBlockReader;
use goosefs_sdk::config::GoosefsConfig;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let config = GoosefsConfig::new("127.0.0.1:9200");

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
| **`fs::FileSystem`** | **FileSystem trait** — high-level async interface (`get_status`, `list_status`, `exists`, `open_file`, `create_file`, `mkdir`, `delete`, `rename`). Object-safe via `async_trait`, `Send+Sync+'static`. |
| **`fs::BaseFileSystem`** | **Production FileSystem implementation** — supports shared-context mode via `FileSystemContext` and legacy per-call mode. Implements WriteType xattr inheritance. `exists()` follows Java semantics (INCOMPLETE non-folder → false). |
| **`context::FileSystemContext`** | **Shared connection pool** — three-layer architecture eliminating repeated TCP+SASL handshakes. Holds `Arc<MasterClient>` + `Arc<WorkerClientPool>` + `Arc<WorkerRouter>`. Background worker-list refresh (30s) and config hot-reload (60s). |
| **`io::GoosefsFileInStream`** | **Seekable dual-path file input stream** — sequential reads via `block_in_stream` (streaming, prefetch) and random reads via `positioned_read` (`position_short=true`). Auto-switches based on 8 KiB threshold. Supports `seek(SeekFrom)` and `read_at()`. |
| **`io::GoosefsFileWriter`** | **High-level file writer** — one-shot `write_file()` or builder pattern `create()` → `write()` → `close()`. Supports all 4 WriteTypes. Cancel/close state machine with UUID-based idempotent `FsOpPId`. |
| `io::GoosefsFileReader` | **High-level file reader** — one-shot `read_file()` / `read_range()` or streaming `open()` → `read_next_block()`. Orchestrates `GetStatus` → `BlockMapper` → `WorkerRouter` → `GrpcBlockReader` |
| `io::GoosefsAsyncReader` | **AsyncRead/AsyncSeek adapter** — wraps `GoosefsFileInStream` and implements `tokio::io::AsyncRead` + `tokio::io::AsyncSeek` for seamless integration with the tokio I/O ecosystem. |
| `fs::URIStatus` | Immutable file/directory metadata snapshot converted from proto `FileInfo`. Typed accessors for all metadata fields. |
| `fs::options` | Rust-native options structs — `OpenFileOptions`, `CreateFileOptions`, `DeleteOptions`, `InStreamOptions`, `ReadType` |
| `auth::ChannelAuthenticator` | SASL authentication for gRPC channels — supports `NOSASL` (no handshake) and `SIMPLE` (PLAIN SASL) |
| `auth::AuthType` | Authentication type enum — `NoSasl`, `Simple` (default). Corresponds to Java's `goosefs.security.authentication.type` |
| `client::MasterClient` | File system metadata CRUD — `get_status`, `list_status`, `create_file`, `complete_file` (with idempotent `FsOpPId`), `remove_blocks`, `delete`, `rename`, `create_directory`, `schedule_async_persistence` |
| `client::MasterInquireClient` | Master discovery with singleflight deduplication — only one task polls when multiple callers need the primary address simultaneously |
| `client::WorkerManagerClient` | Worker discovery — `get_worker_info_list` |
| `client::WorkerClient` | Bidirectional streaming block read/write — `read_block`, `read_block_positioned` (`position_short=true`), `write_block(options: WriteBlockOptions)` |
| `client::WorkerClientPool` | Connection pool for reusing authenticated worker gRPC channels |
| `block::BlockMapper` | Converts file-level byte ranges into block-level read/write plans |
| `block::WorkerRouter` | Consistent-hash routing with TTL-based worker list refresh (30s), local-worker preference (mirrors Java `LocalFirstPolicy`), and failure tracking |
| `io::GrpcBlockReader` | Low-level streaming block reader with flow-control ACK + `positioned_read()` for random access |
| `io::GrpcBlockWriter` | Low-level streaming block writer with chunk splitting and flush |
| `config::GoosefsConfig` | Connection configuration — 30+ settings including properties file parsing, YAML auto-config, `ConfigRefresher` hot-reload, `TransparentAccelerationSwitch`, timeouts, block/chunk size, write/read types, auth, multi-master, worker routing |
| `WritePType` | Write type enum — `MustCache`, `TryCache`, `CacheThrough`, `Through`, `AsyncThrough`, `None` |
| `metrics::registry` | **Global metrics registry** — process-wide thread-safe `Counter` / `Gauge` factories (`metrics::counter(name)`, `metrics::gauge(name)`) plus the `metrics::name::*` constants for SDK-managed counters. |
| `metrics::HeartbeatTask` | **Background heartbeat task** — owned by `FileSystemContext`, periodically computes counter deltas via `ClientMetricsReporter` and ships them to the Master through `MetricsHeartbeat`. Honors `metrics_heartbeat_interval` / `metrics_heartbeat_timeout` / `metrics_max_batch_size` and performs a final flush on `close()`. |
| `metrics::pushgateway` | **Prometheus Pushgateway reporter** — `PushgatewayTask` periodically collects all metrics from the global registry and pushes them to a Pushgateway endpoint via HTTP POST in Prometheus text exposition format. Configurable job/instance labels, push interval, and graceful shutdown. |
| `error::Error` | Unified error type with domain-specific variants (`FileIncomplete`, `DirectoryNotEmpty`, `OpenDirectory`, `InvalidPath`, `AuthenticationFailed`) mapped from Java server exceptions |

## gRPC Services

This client wraps **5 Goosefs gRPC services** defined in 12 proto files:

| Service | Port | Proto | Key RPCs |
|---------|------|-------|----------|
| `FileSystemMasterClientService` | Master:9200 | `file_system_master.proto` | GetStatus, ListStatus, CreateFile, CompleteFile, Delete, Rename, CreateDirectory … (37 RPCs) |
| `BlockWorker` | Worker:9203 | `block_worker.proto` | ReadBlock *(bidi-stream)*, WriteBlock *(bidi-stream)*, AsyncCache, RemoveBlock … (12 RPCs) |
| `WorkerManagerMasterClientService` | Master:9200 | `worker_manager_master.proto` | GetWorkerInfoList, GetCapacityBytes, GetUsedBytes … (9 RPCs) |
| `ServiceVersionClientService` | Master:9200 | `version.proto` | GetServiceVersion |
| `SaslAuthenticationService` | Master:9200 / Worker:9203 | `sasl_server.proto` | Authenticate *(bidi-stream)* — SASL handshake for channel authentication |

## Project Structure

```
goosefs-client-rust/
├── Cargo.toml              # crate manifest
├── build.rs                # tonic-build proto compilation
├── proto/                  # Goosefs protobuf definitions (11 files)
│   ├── grpc/               #   Master/Worker service protos
│   └── proto/              #   Shared data types (security, acl, status)
├── src/
│   ├── lib.rs              # crate root & proto module tree
│   ├── config.rs           # GoosefsConfig (properties/YAML/hot-reload, 30+ keys)
│   ├── context.rs          # ★ FileSystemContext (shared connection pool)
│   ├── error.rs            # Error enum (domain-specific variants)
│   ├── auth/
│   │   ├── mod.rs          # Auth module root
│   │   ├── authenticator.rs # ChannelAuthenticator + AuthType
│   │   └── sasl_client.rs  # PLAIN SASL handshake handler
│   ├── client/
│   │   ├── master.rs       # MasterClient (idempotent FsOpPId)
│   │   ├── master_inquire.rs # MasterInquireClient (singleflight)
│   │   ├── worker.rs       # WorkerClient + WorkerClientPool
│   │   └── worker_manager.rs # WorkerManagerClient
│   ├── block/
│   │   ├── mapper.rs       # BlockMapper (file → block plans)
│   │   └── router.rs       # WorkerRouter (consistent hash + TTL + local-first)
│   ├── fs/                 # ★ FileSystem abstraction layer
│   │   ├── mod.rs          # Module root + re-exports
│   │   ├── filesystem.rs   # FileSystem trait (async_trait)
│   │   ├── base_filesystem.rs # BaseFileSystem (production impl)
│   │   ├── options.rs      # OpenFileOptions, CreateFileOptions, etc.
│   │   ├── uri_status.rs   # URIStatus (immutable metadata snapshot)
│   │   └── write_type.rs   # WriteType xattr helpers
│   ├── io/
│   │   ├── file_in_stream.rs # ★ GoosefsFileInStream (seekable dual-path)
│   │   ├── async_reader.rs  # ★ GoosefsAsyncReader (AsyncRead + AsyncSeek)
│   │   ├── file_reader.rs  # GoosefsFileReader (high-level)
│   │   ├── file_writer.rs  # GoosefsFileWriter (cancel/close state machine)
│   │   ├── reader.rs       # GrpcBlockReader (streaming + positioned)
│   │   └── writer.rs       # GrpcBlockWriter (low-level)
│   ├── metrics/            # ★ Client metrics & heartbeat pipeline
│   │   ├── mod.rs          # Module root + public re-exports
│   │   ├── registry.rs     # Global Counter/Gauge registry + name constants
│   │   ├── reporter.rs     # ClientMetricsReporter (snapshot + delta calc)
│   │   ├── heartbeat.rs    # HeartbeatTask (periodic MetricsHeartbeat RPC)
│   │   └── pushgateway.rs  # ★ PushgatewayTask (Prometheus Pushgateway push)
│   └── generated/          # prost/tonic generated code (checked-in; shipped with the crate)
├── examples/
│   ├── highlevel_file_rw.rs     # ★ High-level file read/write (recommended)
│   ├── streaming_file_read.rs   # ★ Streaming read — constant O(block) memory
│   ├── seekable_file_read.rs    # ★ Seekable read via GoosefsFileInStream (seek / read_at)
│   ├── context_file_rw.rs       # ★ FileSystemContext shared connection pool
│   ├── write_types.rs           # ★ WriteType comparison
│   ├── ha_multi_master.rs       # ★ Multi-master mode
│   ├── auth_demo.rs             # ★ Authentication demo (NOSASL / SIMPLE)
│   ├── metrics_heartbeat.rs     # ★ Client metrics & heartbeat demo
│   ├── lowlevel_block_read.rs   # Low-level block streaming read
│   ├── lowlevel_create_file.rs  # Low-level file creation (metadata only)
│   ├── metadata_crud.rs         # File/directory metadata CRUD
│   └── async_persistence.rs     # Async persistence scheduling
├── tests/
│   └── connection_reuse.rs      # Connection reuse integration test
├── bindings/
│   └── python/              # ★ Python SDK (PyO3 + maturin)
│       ├── python/goosefs/  #   Python package source
│       ├── src/             #   Rust PyO3 bridge
│       └── pyproject.toml   #   Build configuration
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

This crate ships **pre-generated** protobuf code under [`src/generated/`](src/generated/), so downstream users do **NOT** need `protoc` installed to build `goosefs-sdk` — a regular `cargo build` just works out of the box.

The regeneration flow is **opt-in** and only required when you modify any `.proto` file under [`proto/`](proto/). To regenerate:

```shell
# Requires `protoc` (>= 3.15) on PATH.
GOOSEFS_SDK_REGEN_PROTO=1 cargo build
```

The updated `.rs` files will be written back to `src/generated/` — **commit them** along with your `.proto` changes so that downstream users continue to get a zero-`protoc` build.

> **Why the opt-in design?** Running `tonic-build::compile_protos` on every `cargo build` would force all downstream users to install `protoc`, and would also break `cargo publish` verification (the package tarball is read-only). Shipping pre-generated code follows the same approach as [`etcd-client`](https://crates.io/crates/etcd-client) and [`tonic-health`](https://crates.io/crates/tonic-health).

### Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `tonic` | 0.14 | gRPC framework (HTTP/2 + protobuf) |
| `prost` | 0.14 | Protobuf code generation & runtime |
| `tokio` | 1.x | Async runtime |
| `tokio-stream` | 0.1 | Stream utilities for bidirectional gRPC |
| `bytes` | 1.x | Zero-copy byte buffers |
| `thiserror` | 2.x | Ergonomic error derives |
| `dashmap` | 6.x | Concurrent hash map (failure tracking) |
| `tracing` | 0.1 | Structured logging |
| `serde` | 1.x | Config serialization |
| `uuid` | 1.x | Channel-id generation for SASL authentication |
| `hostname` | 0.3 | Local worker detection for routing preference |
| `reqwest` | 0.12 | HTTP client for Pushgateway push |
| `rand` | 0.9 | Random jitter for retry backoff |
| `async-trait` | 0.1 | Async trait support for FileSystem trait |

## Goosefs Compatibility

| Goosefs Version | Java | Status |
|----------------|------|--------|
| Latest (JDK 11) | Java 11 | ✅ Supported |

> **Note:** Goosefs requires **Java 11**. Make sure `JAVA_HOME` points to a JDK 11 installation when running the Goosefs cluster.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
