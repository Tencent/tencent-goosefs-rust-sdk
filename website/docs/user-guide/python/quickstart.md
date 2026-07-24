---
sidebar_position: 2
---

# Quickstart

## Synchronous API

```python
from goosefs import Config, Goosefs

with Goosefs(Config("127.0.0.1:9200")) as fs:
    fs.mkdir("/hello", recursive=True)
    fs.write_file("/hello/world.txt", b"hi")
    assert fs.read_file("/hello/world.txt") == b"hi"
    fs.delete("/hello", recursive=True)
```

## Asynchronous API

```python
import asyncio
from goosefs import Config, AsyncGoosefs

async def main():
    async with AsyncGoosefs(Config("127.0.0.1:9200")) as fs:
        await fs.mkdir("/hello", recursive=True)
        await fs.write_file("/hello/world.txt", b"hi")
        data = await fs.read_file("/hello/world.txt")
        assert data == b"hi"

asyncio.run(main())
```

## Enabling Logs

The binding does not install a `tracing` subscriber by default. Enable explicitly:

```python
import goosefs
goosefs.enable_tracing(level="debug")
```

`RUST_LOG` (when set) overrides the `level` argument.

## Batch APIs

Both sync and async clients expose batch metadata / lifecycle helpers (`batch_get_status`, `batch_exists`, `batch_create_file`, `batch_list_status`, …). Each batch uses one PyO3 boundary crossing and bounded concurrency; the first error in input order is returned.
