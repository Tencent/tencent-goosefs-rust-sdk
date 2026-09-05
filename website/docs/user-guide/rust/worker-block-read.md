---
sidebar_position: 10
---

# Worker Block Direct Read

For workloads that need block-level control (e.g., columnar access patterns, partial block reads), the Rust SDK exposes a low-level pipeline: `WorkerRouter` selects the worker responsible for a block, and `GrpcBlockReader` (backed by `WorkerClient`) reads the raw block bytes with full flow-control ACK control.

## High-level convenience: `GoosefsFileReader`

Most users should use `GoosefsFileReader::read_range_with_context` or `GoosefsFileInStream::read_at` instead of the low-level APIs described here. The high-level APIs resolve the block → worker mapping internally.

```rust
use goosefs_sdk::io::GoosefsFileReader;

// Read bytes [100, 600) from a file.
let data = GoosefsFileReader::read_range_with_context(ctx.clone(), "/data/file", 100, 500).await?;
```

## Low-level pipeline

The low-level pipeline gives full control over flow-control ACK and chunk-level processing:

```
MasterClient::get_status(path)
  → WorkerRouter::select_worker(block_id)
    → WorkerClient::connect(addr, config)
      → GrpcBlockReader::open(worker, block_id, offset, ...)
        → reader.read_chunk() / reader.read_all()
```

### Step 1: Get file metadata and block list

```rust
use goosefs_sdk::client::MasterClient;

let master = ctx.acquire_master();
let status = master.get_status("/data/large.parquet").await?;
let block_ids = &status.block_ids;
```

### Step 2: Route to the responsible worker

```rust
use goosefs_sdk::block::WorkerRouter;

let router = ctx.acquire_router();
let worker_info = router.select_worker(block_ids[0]).await?;
```

:::note
`select_worker` mirrors the Java client's consistent-hash policy: `murmur3_128` over the block id with per-worker virtual nodes. Workers report their own `WorkerInfo.virtual_node_num`; when the field is unset the client falls back to the GooseFS 2.0 default `goosefs.master.consistent.hash.virtual.node.num.per.worker = 5000`, so Rust and Java clients pick the same worker for the same block. Candidate-pool width and replica count follow `GOOSEFS_USER_FILE_READ_MAX_NODE_RETRY` / `GOOSEFS_USER_FILE_REPLICATION_NUMBER` (see [Configuration](./configuration)).
:::

### Step 3: Connect to the worker and open a block reader

```rust
use goosefs_sdk::client::WorkerClient;
use goosefs_sdk::io::GrpcBlockReader;

// WorkerInfo.address is Option<WorkerNetAddress>; propagate any missing field.
let addr = worker_info.address.as_ref()
    .ok_or_else(|| goosefs_sdk::error::Error::MissingField { field: "address".into() })?;
let host = addr.host.as_deref()
    .ok_or_else(|| goosefs_sdk::error::Error::MissingField { field: "address.host".into() })?;
let rpc_port = addr.rpc_port
    .ok_or_else(|| goosefs_sdk::error::Error::MissingField { field: "address.rpc_port".into() })?;
let worker_addr = format!("{host}:{rpc_port}");
let worker = WorkerClient::connect(&worker_addr, &ctx.config()).await?;

// One-shot: read the entire first block.
let block_size = status.block_size_bytes.unwrap_or(ctx.config().block_size as i64);
let read_len = status.length.unwrap_or(0).min(block_size);
let mut reader = GrpcBlockReader::open(
    &worker, block_ids[0], 0, read_len, ctx.config().chunk_size as i64, None,
).await?;
let data = reader.read_all().await?;
println!("read {} bytes from block {}", data.len(), block_ids[0]);
```

### Step 4: Read chunk-by-chunk (with flow-control ACK)

```rust
use goosefs_sdk::io::GrpcBlockReader;

let mut reader = GrpcBlockReader::open(
    &worker, block_ids[0], 0, read_len, ctx.config().chunk_size as i64, None,
).await?;
while let Some(chunk) = reader.read_chunk().await? {
    process(&chunk);
    // ACK the chunk to signal the worker to send more data.
    // GrpcBlockReader handles ACK internally when the chunk is consumed.
}
println!("received {} bytes total", reader.bytes_received());
```

`GrpcBlockReader::open` internally calls `WorkerClient::read_block` with `position_short = false` (sequential streaming). For positioned reads, `GrpcBlockReader` includes `read_chunk` which incrementally ACKs received bytes to maintain flow control.

## `GrpcBlockReader` API

| Method | Description |
|--------|-------------|
| `open(worker, block_id, offset, length, chunk_size, options)` | Open a block reader (calls `WorkerClient::read_block` internally) |
| `read_chunk()` | Read one chunk; returns `None` at EOF. Handles flow-control ACK. |
| `read_all()` | Read all remaining bytes |
| `positioned_read(worker, block_id, offset, length, chunk_size)` | One-shot positioned read from a specific block |
| `block_id()` | The block being read |
| `bytes_received()` | Total bytes received so far |
| `is_complete()` | Whether the read is complete |

## `WorkerClient` API

| Method | Description |
|--------|-------------|
| `connect(addr, config)` | Connect with SASL auth (production) |
| `connect_simple(addr, timeout)` | Deprecated, unauthenticated escape hatch (test-only) |
| `read_block(block_id, offset, length, ...)` | Start a streaming read (returns `(Sender<ReadRequest>, Streaming<ReadResponse>)` — use `GrpcBlockReader::open` instead) |
| `read_block_positioned(block_id, offset, length, ...)` | Start a positioned read (returns channel pair — use `GrpcBlockReader::positioned_read` instead) |
| `write_block(...)` | Block write (for streaming writers) |
| `addr()` | Worker `host:port` |
| `generation()` | Monotonic connection-generation tag used by pooled reconnect logic |
| `close(self)` | Close the connection (consumes the client) |

:::note
`WorkerClient` is `Clone`; clones share the underlying tonic channel. `WorkerClientPool::acquire()` returns a cheap clone of a cached client, and the pool retains its cached connection until it is invalidated or dropped.
:::

## `WorkerRouter` API

| Method | Description |
|--------|-------------|
| `select_worker(block_id)` | Pick the worker holding this block |
| `get_workers()` | Snapshot of all known workers |
| `mark_failed(addr)` | Mark a worker as temporarily unavailable |
| `needs_refresh()` | Whether the worker list is stale |

## When to use

| Scenario | Recommended API |
|----------|----------------|
| Read a small range from a large file | `GoosefsFileReader::read_range_with_context` |
| Read a byte range at a file offset | `GoosefsFileInStream::read_at` |
| Multiple chunked reads from a block | `GrpcBlockReader::open` + `read_chunk()` |
| Full-file sequential read | `GoosefsFileInStream::read_all` or `GoosefsFileReader::read_next_block` |

See `examples/lowlevel_block_read.rs` for a complete end-to-end example.
