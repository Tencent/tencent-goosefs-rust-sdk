---
sidebar_position: 6
---

# Streaming Read / Write

For files that don't fit in memory or need incremental processing, use the streaming APIs: `AsyncFileReader` (via `open_file`) and `AsyncFileWriter` (via `create_file`).

## Streaming Read

```python
reader = await fs.open_file("/data/large.bin")

# Read all remaining bytes
data = await reader.read()           # size < 0 (default) = read to EOF

# Read up to N bytes
chunk = await reader.read(4096)      # may return fewer if near EOF

# Positioned read (does not move the cursor)
tail = await reader.read_at(offset=4096, length=512)

# Seek
await reader.seek(8192)              # whence=0 (SEEK_SET) by default
await reader.seek(100, whence=1)     # SEEK_CUR
await reader.seek(-50, whence=2)     # SEEK_END

# Tell (synchronous — does not block)
pos = reader.tell()

# File length
total = len(reader)

await reader.close()
```

### Context manager

```python
async with await fs.open_file("/data/large.bin") as reader:
    while chunk := await reader.read(4096):
        process(chunk)
```

### Concurrency model

`AsyncFileReader` is backed by a single `tokio::sync::Mutex`. Concurrent calls on the same handle are **serialised** — calling `read()` while another `read()` is in flight queues behind it. `tell()` is synchronous and raises `RuntimeError` if a read/seek is in flight.

For true parallel reads, open multiple readers. `read_at()` preserves the cursor position, but calls on the same reader are still serialised by its mutex.

## Streaming Write

```python
writer = await fs.create_file("/data/output.bin")

n = await writer.write(b"first chunk ")   # returns bytes accepted (= len(data))
await writer.write(b"second chunk")
await writer.close()                        # commits the file to master
```

### Context manager and cancellation

```python
async with await fs.create_file("/data/output.bin") as writer:
    await writer.write(b"data")
# close() called automatically on exit
```

:::warning
On unhandled exception inside an `async with` block, the writer is **cancelled** (not closed). Half-written files are **not** committed to the master — the partial blocks are cleaned up so no stale data remains. This differs from `close()` which commits the file.
:::

### WriteType

```python
from goosefs import WriteType

writer = await fs.create_file(
    "/data/durable.bin",
    write_type=WriteType.CacheThrough,
    block_size_bytes=256 * 1024 * 1024,  # 256 MiB
    recursive=True,                       # create parent dirs if needed
)
```

## Bytes-like Input

`write()` accepts any object implementing the buffer protocol **except** `str`:

```python
await writer.write(b"bytes")           # bytes
await writer.write(bytearray(1024))    # bytearray
await writer.write(memoryview(buf))    # memoryview (zero-copy only for read-only C-contiguous buffers in abi3-py311 builds)
# await writer.write("string")         # TypeError — use .encode() first
```
