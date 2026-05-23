# GooseFS Python Client

[![PyPI](https://img.shields.io/pypi/v/goosefs.svg)](https://pypi.org/project/goosefs/)
[![Python Versions](https://img.shields.io/pypi/pyversions/goosefs.svg)](https://pypi.org/project/goosefs/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

`goosefs` is the official Python client for [Tencent Cloud GooseFS](https://cloud.tencent.com/product/goosefs), implemented natively in Rust (the [`goosefs-sdk`](https://crates.io/crates/goosefs-sdk) crate) and bridged to Python via [PyO3](https://pyo3.rs/).

| | |
| --- | --- |
| **Package / Import name** | `goosefs` (PyPI package name and import name are identical) |
| **Python support** | abi3 wheel, runtime floor CPython 3.9+ |
| **Platforms** | Linux x86_64 / aarch64 (manylinux_2_28), macOS x86_64 / arm64; Windows best-effort |
| **API style** | Synchronous blocking (`Goosefs`) + coroutine-based async (`AsyncGoosefs`) |
| **Status** | alpha — see the [development roadmap](../../docs/PYTHON_BINDING_PROGRESS.md) |

## Documentation Map

- **PyPI landing page**: [`PYPI_README.md`](./PYPI_README.md) — install, quickstart, thread / fork safety, type stubs
- **Five runnable examples**: [`examples/`](./examples/)
  - [`quickstart.py`](./examples/quickstart.py) — synchronous one-pager: connect → mkdir → write → read → delete
  - [`async_demo.py`](./examples/async_demo.py) — `AsyncGoosefs` + `asyncio.gather` concurrent fan-out
  - [`streaming.py`](./examples/streaming.py) — streaming reader / writer: chunked read & write + seek + read_at
  - [`with_pyarrow.py`](./examples/with_pyarrow.py) — Arrow Table → Parquet → GooseFS round-trip
  - [`pandas_csv.py`](./examples/pandas_csv.py) — pandas DataFrame ↔ CSV round-trip
- **Development guide (build, test, release)**: [`DEVELOPMENT.md`](./DEVELOPMENT.md)
- **Changelog**: [`CHANGELOG.md`](./CHANGELOG.md)
- **Full development tracker**: [`docs/PYTHON_BINDING_PROGRESS.md`](../../docs/PYTHON_BINDING_PROGRESS.md)
- **Complete API type stubs (with docstrings)**: [`python/goosefs/__init__.pyi`](./python/goosefs/__init__.pyi)

## One-Minute Try-Out

```bash
pip install goosefs
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
```

```python
from goosefs import Config, Goosefs

with Goosefs(Config("127.0.0.1:9200")) as fs:
    fs.mkdir("/hello", recursive=True)
    fs.write_file("/hello/world.txt", b"hi")
    assert fs.read_file("/hello/world.txt") == b"hi"
```

For more scenarios, just run the scripts under `examples/` — each file is self-contained and independently runnable.

## Install Variants

| Install command | Extra dependencies pulled in | Use case |
| --- | --- | --- |
| `pip install goosefs` | binding only | Just the GooseFS core API |
| `pip install 'goosefs[arrow]'` | + `pyarrow` | Arrow / Parquet workflows |
| `pip install 'goosefs[pandas]'` | + `pandas` + `pyarrow` | DataFrame workflows |
| `pip install 'goosefs[examples]'` | + `pyarrow` + `pandas` | Run all 5 example scripts |

## Enabling Logs

The binding does **not** install any `tracing` subscriber by default — log control is fully delegated to the host program. Enable it explicitly when you need to debug:

```python
import goosefs
goosefs.enable_tracing(level="debug")
```

See [`PYPI_README.md`](./PYPI_README.md) and the `enable_tracing` docstring in [`__init__.pyi`](./python/goosefs/__init__.pyi) for details. The `RUST_LOG` environment variable (when set) overrides the `level` argument.

## Feedback

* Bugs / feature requests: please open an issue in the repository (label `python-binding`)
* Internal users: contact the GooseFS team for prioritized support
* Code contributions are welcome — please read [`DEVELOPMENT.md`](./DEVELOPMENT.md) first

## License

Apache License 2.0. See the `LICENSE` file at the repository root for details.
