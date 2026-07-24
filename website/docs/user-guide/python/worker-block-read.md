---
sidebar_position: 11
---

# Worker Block Direct Read

For workloads that need fine-grained control over block-level reads (e.g., columnar access patterns, partial block reads), the Python binding exposes a low-level Worker block client that bypasses the file abstraction and reads directly from a specific block on a specific worker.

## High-Level API: `positioned_read`

```python
async with await AsyncGoosefs.connect(cfg) as fs:
    # Read bytes [offset, offset+length) from block at index 0
    data = await fs.positioned_read(
        "/data/large.parquet",
        block_index=0,      # which block (default 0)
        offset=1024,        # byte offset within the block
        length=512,         # bytes to read (default -1 = to end of block)
        chunk_size=64 * 1024,  # gRPC streaming chunk size
    )
```

`positioned_read` resolves `path` → picks `block_ids[block_index]` → routes to the responsible Worker via the shared `WorkerRouter` → streams the requested byte range from that worker.

:::note
For the last block of a file, the actual block size may be smaller than `block_size_bytes` reported by master. `length=-1` returns only the remaining bytes of that block.
:::

## Low-Level API: `acquire_worker_for_block`

```python
async with await AsyncGoosefs.connect(cfg) as fs:
    # Get the file's block list
    status = await fs.get_status("/data/large.parquet")
    block_ids = status.block_ids

    # Acquire a direct Worker client for a specific block
    worker = await fs.acquire_worker_for_block(block_ids[0])
    try:
        # Read arbitrary ranges from this block
        data = await worker.read_block_positioned(block_ids[0], offset=0, length=4096)
    finally:
        await worker.close()
```

### `AsyncWorkerClient` methods

| Method                     | Description                                          |
| -------------------------- | ---------------------------------------------------- |
| `connect(addr, config)`    | Connect directly to a worker (static factory)        |
| `connect_simple(addr)`     | Deprecated, unauthenticated test-only connection; use `connect(addr, config)` in production |
| `read_block_positioned(id, offset, length)` | Positioned read from a specific block |
| `close()`                  | Release the wrapper (underlying channel stays pooled) |

:::tip
Closing an `AsyncWorkerClient` only releases the Python-side wrapper. The underlying authenticated gRPC channel stays in the `FileSystemContext`'s pool for reuse — no reconnection cost on the next acquire.
:::

## Sync API

```python
with Goosefs(cfg) as fs:
    data = fs.positioned_read("/data/large.parquet", offset=0, length=4096)
```

## When to Use

| Scenario                              | Recommended API            |
| ------------------------------------- | -------------------------- |
| Read a small range from a large file  | `read_range()` (file-level) |
| Read a specific block by index        | `positioned_read()`        |
| Multiple reads from the same block    | `acquire_worker_for_block()` + `read_block_positioned()` |
| Full-file sequential read             | `open_file()` (streaming)   |

For most use cases, `read_range()` or `open_file()` is simpler and sufficient. Use the Worker block APIs only when you need block-level granularity.
