---
sidebar_position: 3
---

# FileSystem API

The Python binding exposes two parallel APIs:

- **`AsyncGoosefs`** — coroutine-based, the primary API for `asyncio` applications.
- **`Goosefs`** — synchronous wrapper that drives the async core on a shared tokio runtime. Use for scripts, REPL, and non-async code.

Both share the same underlying `FileSystemContext` (connection pools, config refresher, metrics). Construct once per process and reuse.

## Connecting

```python
from goosefs import Config, AsyncGoosefs, Goosefs

cfg = Config("127.0.0.1:9200")

# Async (preferred)
async with await AsyncGoosefs.connect(cfg) as fs:
    status = await fs.get_status("/data")

# Sync
with Goosefs(cfg) as fs:
    status = fs.get_status("/data")
```

:::caution
`Goosefs` (sync) methods raise `RuntimeError` if called from inside a running asyncio event loop — this prevents a deadlock between the sync wrapper's tokio blocking call and the event loop. Use `AsyncGoosefs` inside async code.
:::

## Metadata Operations

```python
# Single-path
status = await fs.get_status("/data/file.parquet")
exists = await fs.exists("/data/missing")

# List (eager — materialises all URIStatus objects immediately)
entries = await fs.list_status("/data", recursive=False)
for e in entries:
    print(f"  {e.name} ({e.length} bytes)")

# List (lazy — returns URIStatusList, materialises on demand)
grouped = await fs.list_status_grouped("/data", recursive=False)
print(f"  {len(grouped)} entries")   # O(1), zero object creation
first = grouped[0]                    # materialises one URIStatus
```

See [Batch APIs](./batch-api) for concurrent multi-path operations.

## High-Level Read / Write

```python
# One-shot read (entire file into memory)
data = await fs.read_file("/data/hello.txt")

# Range read (offset + length; may span multiple block RPCs)
chunk = await fs.read_range("/data/hello.txt", offset=100, length=500)

# One-shot write (create + write + close in one call)
n = await fs.write_file("/data/hello.txt", b"Hello, GooseFS!")

# WriteType
from goosefs import WriteType
await fs.write_file("/data/durable.bin", payload, write_type=WriteType.CacheThrough)
```

## Streaming Read / Write

```python
# Streaming read
reader = await fs.open_file("/data/large.bin")
data = await reader.read(4096)        # read up to 4096 bytes
await reader.seek(8192)               # seek to offset
chunk = await reader.read_at(0, 256)  # positioned read (doesn't move cursor)
await reader.close()

# Streaming write
writer = await fs.create_file("/data/output.bin")
await writer.write(b"first chunk ")
await writer.write(b"second chunk")
await writer.close()
```

See [Streaming](./streaming) for the full `AsyncFileReader` / `AsyncFileWriter` API.

## Directory Operations

```python
await fs.mkdir("/data/subdir", recursive=True)
await fs.rename("/data/old.txt", "/data/new.txt")
await fs.delete("/data/new.txt")
await fs.delete("/data/tree", recursive=True)
```

## Multi-Master (HA)

```python
cfg = Config("10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200")
# or via env: GOOSEFS_MASTER_ADDR=10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200

async with await AsyncGoosefs.connect(cfg) as fs:
    await fs.exists("/")
```

Two or more addresses → multi-master mode (polls to discover the Primary automatically).

## Context Manager

Both `AsyncGoosefs` and `Goosefs` support context managers (`async with` / `with`) that call `close()` on exit. An `atexit` safety net attempts to close forgotten synchronous handles and warns about forgotten async handles.
