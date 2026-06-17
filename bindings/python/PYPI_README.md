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

## Building From Source (Optional)

Most users should just `pip install goosefs`. Build from source only if you need
an unreleased change or a platform without a pre-built wheel.

**Prerequisites:** Python 3.9+, Rust 1.88+ ([rustup](https://rustup.rs/)).

```bash
git clone https://git.woa.com/tencent-cloud-datalake/goosefs-client-rust.git
cd goosefs-client-rust/bindings/python
```

Install [maturin](https://www.maturin.rs/):

```bash
pip install maturin
```

Build and install:

```bash
# Development mode (editable)
maturin develop

# Or build a wheel
maturin build --release
pip install target/wheels/goosefs-*.whl
```

To produce a portable Linux wheel (usable on Tencent Cloud Linux), cross-compile
with [zig](https://ziglang.org/):

```bash
rustup target add x86_64-unknown-linux-gnu
pip install ziglang
maturin build --release --target x86_64-unknown-linux-gnu --manylinux 2_28 --zig
```

Verify:

```python
import goosefs
print("GooseFS Python bindings installed:", goosefs.__version__)
```

## Quick start

```python
from goosefs import Config, Goosefs

cfg = Config("127.0.0.1:9200")
with Goosefs(cfg) as fs:
    fs.mkdir("/data", recursive=True)
    fs.write_file("/data/hello.txt", b"hello, goosefs")
    print(fs.read_file("/data/hello.txt"))
```

For asynchronous code:

```python
import asyncio
from goosefs import AsyncGoosefs, Config, WriteType

async def main() -> None:
    async with await AsyncGoosefs.connect(Config("127.0.0.1:9200")) as fs:
        await fs.mkdir("/data", recursive=True)
        await fs.write_file("/data/x.bin", b"...", write_type=WriteType.MustCache)
        data = await fs.read_file("/data/x.bin")
        print(len(data), "bytes")

asyncio.run(main())
```

## Type stubs

The package ships PEP 561-compliant type stubs (`goosefs/*.pyi`) and a
`py.typed` marker, so `mypy` and `pyright` get full IntelliSense out of
the box. Stubs are kept in lock-step with the runtime by
`mypy.stubtest` in CI:

```bash
cd bindings/python
uv run python -m mypy.stubtest goosefs
```

## Thread / process safety

- `Goosefs` and `AsyncGoosefs` are **safe to share across threads**.
- Both are **NOT safe across `os.fork()`** — child processes must
  reconnect.
- `Goosefs` synchronous methods refuse to run from inside a Tokio worker
  or asyncio event loop and raise `RuntimeError` instead of deadlocking.
- File handles (`FileReader` / `FileWriter` / their async siblings)
  are **NOT safe to share across threads or tasks** — open one per
  worker.

## License

Apache License 2.0. See `LICENSE` in the repository root.
