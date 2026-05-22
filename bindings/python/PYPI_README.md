# GooseFS Python Client

[![PyPI](https://img.shields.io/pypi/v/goosefs.svg)](https://pypi.org/project/goosefs/)
[![Python Versions](https://img.shields.io/pypi/pyversions/goosefs.svg)](https://pypi.org/project/goosefs/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

`goosefs` is the official Python client for [Tencent Cloud GooseFS](https://cloud.tencent.com/product/goosefs),
powered by a native Rust implementation (the [`goosefs-sdk`](https://crates.io/crates/goosefs-sdk) crate)
exposed through [PyO3](https://pyo3.rs/).

> **Status:** alpha — the API is shaping up. See the
> [development roadmap](https://git.woa.com/tencent-cloud-datalake/goosefs-client-rust/blob/main/docs/PYTHON_BINDING_PROGRESS.md).

## Install

```bash
pip install goosefs
```

Pre-built wheels are provided for:

- Linux x86_64 / aarch64 (manylinux_2_28)
- macOS x86_64 / arm64

Windows wheels are best-effort and may be added in a later release.

## Quick start

```python
from goosefs import Config, GooseFs

cfg = Config("127.0.0.1:9200")
with GooseFs(cfg) as fs:
    fs.mkdir("/data", recursive=True)
    fs.write_file("/data/hello.txt", b"hello, goosefs")
    print(fs.read_file("/data/hello.txt"))
```

For asynchronous code:

```python
import asyncio
from goosefs import AsyncGooseFs, Config

async def main() -> None:
    async with await AsyncGooseFs.connect(Config("127.0.0.1:9200")) as fs:
        await fs.mkdir("/data", recursive=True)
        await fs.write_file("/data/x.bin", b"...", write_type="MUST_CACHE")
        data = await fs.read_file("/data/x.bin")
        print(len(data), "bytes")

asyncio.run(main())
```

## License

Apache License 2.0. See `LICENSE` in the repository root.
