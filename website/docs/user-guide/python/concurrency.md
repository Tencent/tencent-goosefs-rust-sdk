---
sidebar_position: 13
---

# Concurrency and Process Safety

The GooseFS Python binding runs on a shared Tokio runtime and interacts with the GIL in specific ways. This page covers the three safety guards every user should know.

## Sync vs Async

| API             | Use from                    | Mechanism                          |
| --------------- | --------------------------- | ---------------------------------- |
| `AsyncGoosefs`  | `async` / `asyncio` code    | Native coroutines on Tokio         |
| `Goosefs`       | Sync code (scripts, REPL)   | Blocks on a shared Tokio runtime   |

### Deadlock guard

`Goosefs` (sync) methods **raise `RuntimeError`** if called from inside a running asyncio event loop. This prevents a deadlock between the sync wrapper's blocking `tokio` call and the event loop thread.

```python
import asyncio
from goosefs import Goosefs, Config

fs = Goosefs(Config("127.0.0.1:9200"))

async def bad():
    # This will raise RuntimeError — use AsyncGoosefs instead
    fs.exists("/data")

asyncio.run(bad())
# RuntimeError: Goosefs sync methods cannot be called from inside
# an asyncio event loop — use AsyncGoosefs instead.
```

**Fix**: use `AsyncGoosefs` inside async code, or run the sync call in a thread executor.

## Fork Safety

`Goosefs` / `AsyncGoosefs` instances are **NOT safe across `os.fork()`**. The Tokio runtime, connection pools, and background tasks all live in the parent process's memory. A forked child inherits a broken snapshot.

### Guard

The synchronous `Goosefs` client records its creator PID in `__new__`, and subsequent calls from a different PID raise `RuntimeError`. `AsyncGoosefs` is also unsafe after a fork, but currently does not provide this guard — never reuse either client in a child process.

```python
import os
from goosefs import Goosefs, Config

fs = Goosefs(Config("127.0.0.1:9200"))

pid = os.fork()
if pid == 0:
    # Child process
    fs.exists("/")  # RuntimeError: Goosefs instance was created in a
                     # different process (fork detected). Reconnect.
```

**Fix**: the child must create its own `Goosefs` / `AsyncGoosefs` instance.

### Multiprocessing

```python
from multiprocessing import Process
from goosefs import Goosefs, Config

def worker():
    # Create a NEW instance in the child process
    fs = Goosefs(Config("127.0.0.1:9200"))
    fs.exists("/")

p = Process(target=worker)
p.start()
p.join()
```

## Atexit Safety Net

If you forget to close a synchronous `Goosefs` instance, the binding's `atexit` handler attempts to close it at process shutdown. Unclosed `AsyncGoosefs` instances cannot be driven without an event loop, so the handler emits a `ResourceWarning` and relies on the OS to reclaim their sockets.

```python
from goosefs import Goosefs, Config

fs = Goosefs(Config("127.0.0.1:9200"))
fs.write_file("/data/x", b"hello")
# Forgot to close — the atexit handler will close it on exit
```

:::note
The atexit handler is a safety net, not a replacement for explicit cleanup. In long-running processes, leaked handles keep connections open and consume worker pool slots. Always use `with Goosefs(...) as fs:` or call `fs.close()` explicitly.
:::

## GIL and Tokio

The Python binding uses PyO3 to bridge Python and Rust. Key interactions:

- **Rust async tasks run on Tokio** — they do not hold the GIL while waiting on I/O.
- **PyO3 boundary crossings acquire the GIL** — each call from Python to Rust (and back) briefly acquires the GIL.
- **Batch APIs reduce GIL crossings** — one `batch_get_status(100_paths)` takes the GIL once instead of 100 times. See [Batch APIs](./batch-api).
- **`URIStatusList` defers GIL work** — `list_status_grouped` returns a lazy container that materialises `URIStatus` objects one at a time on `__getitem__`, reducing peak GIL occupancy.

### Practical advice

- Use **batch APIs** for multi-path operations (10x-100x faster under GIL contention).
- Use **`list_status_grouped`** instead of `list_status` when you only need `len()` or a few entries.
- Use **`AsyncGoosefs`** with `asyncio.gather` for concurrent I/O — the GIL is released during each `await`.
- Avoid calling sync `Goosefs` methods in a tight loop from async code — use the async API instead.
