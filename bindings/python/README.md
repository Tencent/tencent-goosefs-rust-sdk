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
| **Status** | alpha — see [`CHANGELOG.md`](CHANGELOG.md) and [`DEVELOPMENT.md`](DEVELOPMENT.md) |

## What's New

- **v0.1.8** — aligned with `goosefs-sdk` 0.1.8. Default
  `worker_connection_pool_size` bumped from `1` to `min(cores, 4)`
  (capped); restore legacy behaviour with
  `.with_worker_connection_pool_size(1)` or
  `goosefs.client.worker.connection.pool.size=1`. Drop-in upgrade from
  0.1.7 — see [`CHANGELOG.md`](./CHANGELOG.md).

- **v0.1.7** — aligned with `goosefs-sdk` 0.1.7. Version bump tracking
  the underlying SDK; no Python-surface API changes.

- **v0.1.6** — aligned with `goosefs-sdk` 0.1.6. Two major SDK-side
  data-plane features land automatically for every Python read:

  - **Client-side local page cache** — opt-in, disk-backed page cache
    mirroring the GooseFS Java client's `goosefs.user.client.cache.*`
    semantics (LRU/LFU eviction, multi-directory `HashAllocator`, TTL
    lazy expiry, restart restore, overwrite invalidation). Best-effort
    by design — misses / errors always fall back to the worker without
    affecting read correctness. Enable via config / ENV / properties;
    see `bindings/python/examples/page_cache.py` and
    [`docs/CLIENT_PAGE_CACHE_DESIGN.md`](../../docs/CLIENT_PAGE_CACHE_DESIGN.md).
  - **Short-Circuit local mmap read** — when the client and worker are
    co-located, block reads are served via zero-copy `mmap` (with
    `madvise` prefetch and optional Transparent Huge Pages) instead of
    the gRPC data plane. Local worker is auto-detected by interface
    bind; every recoverable error transparently falls back to gRPC.
    See [`docs/SHORT_CIRCUIT_DESIGN.md`](../../docs/SHORT_CIRCUIT_DESIGN.md).

  New Python-side surface:

  - **Batch file-lifecycle APIs**: `AsyncGoosefs.batch_open_file` /
    `batch_create_file` / `batch_create_dir` / `batch_rename` /
    `batch_delete` / `batch_list_status`, plus their sync `Goosefs`
    counterparts. Single Tokio spawn per batch, first-error-wins,
    one PyO3 boundary crossing per batch instead of N.
  - **`goosefs.WorkerClient`** — synchronous mirror of
    `AsyncWorkerClient`. Blocking escape hatch for callers that already
    know a worker address and want a one-shot positioned read without
    going through `Goosefs.positioned_read` (which routes via the
    master).

    ```python
    from goosefs import WorkerClient, Config

    with WorkerClient.connect("127.0.0.1:9203", Config("127.0.0.1:9200")) as wc:
        data = wc.read_block_positioned(block_id, offset=0, length=64 * 1024)
    ```

  Also inherits the Worker/router wait-free hot-path rewrite (`ArcSwap`
  based `WorkerClientPool` / `WorkerRouter`, +11–21% throughput and
  ~50–65% p999 reduction on positioned-read block traffic), deferred
  `WorkerRouter` initialization for metadata-only workloads, and a
  batch of SDK-side correctness fixes (HA primary discovery
  cancel-safety, `WriteBlockHandle` Drop abort, `GoosefsFileInStream`
  forward-seek byte-loss, `GoosefsFileWriter` Drop cleanup,
  `LogSampler` clock-jump safety, etc.). Drop-in upgrade from 0.1.5 —
  no breaking API changes. See [`CHANGELOG.md`](./CHANGELOG.md) for the
  full list.

- **v0.1.5** — aligned with `goosefs-sdk` 0.1.5. Inherits Prometheus
  Pushgateway support, the `GoosefsAsyncReader` (`AsyncRead` +
  `AsyncSeek`) adapter, and the `GoosefsFileInStream::read` short-read
  byte-loss fix from the underlying SDK. No Python API change — drop-in
  upgrade from 0.1.4.
- **v0.1.4** — Python-binding-only performance release:
  - **Batch metadata APIs**: `AsyncGoosefs.batch_get_status(paths)` /
    `batch_exists(paths)` and their sync counterparts on `Goosefs`.
    A single PyO3 boundary crossing per batch; under a real cluster
    (500 paths, median of 7 iterations) sync `get_status` improves
    **2.67×** vs a sequential loop and is also faster than a
    16-thread pool.
  - **Custom Tokio runtime**: `worker_threads =
    available_parallelism().max(16)`, `max_blocking_threads = 64`,
    registered via `pyo3_async_runtimes::tokio::init` at module init.
  - **Read-path copy elimination**: `pull_n` fills in place,
    `pull_all` returns `bytes::Bytes`, and `read_file` /
    `read_range` / `read_at` drop a `to_vec()`.

See [`CHANGELOG.md`](./CHANGELOG.md) for the full release history.

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
