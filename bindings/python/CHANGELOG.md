# Changelog

This document records all notable changes to the `goosefs` Python binding. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and version numbers follow [SemVer](https://semver.org/).

> Note: Version numbers stay aligned with the underlying `goosefs-sdk` crate. While P8 (CI/Wheel) is on hold, the wheels under this directory are released internally only (produced manually via `maturin build --release`); public PyPI releases will start from 0.1.0 once P8/P9 are restarted.

## [Unreleased]

### Added

- None yet

### Changed

- None yet

### Fixed

- None yet

---

## [0.1.3] — 2026-05-28

### Added

- **`AsyncWorkerClient`** — low-level coroutine-based block reader for a
  single Goosefs Worker. Wraps `goosefs_sdk::client::WorkerClient`. The
  Python side gets a thin one-shot positioned-read coroutine that
  delegates to `goosefs_sdk::io::GrpcBlockReader::positioned_read` and
  takes a single `PyBytes::new` copy across the PyO3 boundary; raw
  `(request_tx, response_stream)` is intentionally not exposed.
  - `AsyncWorkerClient.connect(addr, config)` — full SASL handshake.
  - `AsyncWorkerClient.connect_simple(addr, connect_timeout_ms=10_000)` —
    deprecated NOSASL escape hatch for test workers.
  - `AsyncWorkerClient.read_block_positioned(block_id, offset, length, chunk_size=1<<20)`
    → `bytes`.
  - `AsyncWorkerClient.addr` / `AsyncWorkerClient.close()` /
    `__aenter__` / `__aexit__`.
- **`AsyncGoosefs.acquire_worker_for_block(block_id)`** —
  router-driven worker selection on the shared `WorkerClientPool`.
  Returns an `AsyncWorkerClient` that wraps the *pooled*
  `WorkerClient` (no extra TCP+SASL handshake).
- **`AsyncGoosefs.positioned_read(path, *, block_index=0, offset=0, length=-1, chunk_size=1<<20)`**
  — high-level Worker block direct read. One-line Python equivalent of
  `examples/lowlevel_block_read.rs`; resolves URI → picks
  `block_ids[block_index]` → routes via `WorkerRouter` → drains a
  positioned-read stream into a single `bytes`.
- **`Goosefs.acquire_worker_for_block` / `Goosefs.positioned_read`** —
  synchronous counterparts of the above two methods, sharing the same
  deadlock + fork guards as the rest of the sync surface.
- New module `bindings/python/src/worker.rs` + companion type stubs in
  `python/goosefs/__init__.pyi`.

### Changed

- The Python stress tool (`tmp/goosefs_stress_python`) no longer
  silently falls back from `--transport=block` to the `fs` path and no
  longer emits `"BlockTransport"` in `summary.missingOperations`.
  `bench/worker.py::_positioned_read` now branches on transport: `block`
  goes through `AsyncGoosefs.positioned_read` (true direct Worker read),
  `fs` keeps the old `read_at` path as a control group.

### Fixed

- None yet

---

## [0.1.2] — 2026-05-23

The first internally usable alpha release. Covers all milestones from P0 through P7.

### Added

#### Core API

- **`Config`**: builds the client configuration. Supports single-node / HA multi-master, a `properties` dict, `from_properties_file`, plus 12 commonly used field getters (`master_addr` / `master_addrs` / `block_size` / `chunk_size` / `root` / `use_vpc_mapping` / `auth_type` / `auth_username` / `metrics_enabled` / `connect_timeout_ms` / `request_timeout_ms` / `write_type`).
- **`AsyncGoosefs`** — coroutine-based async client:
  - Metadata: `get_status` / `list_status` / `exists` / `mkdir` / `delete` / `delete_with_options` / `rename`
  - One-shot read/write: `read_file` / `read_range` / `write_file`
  - Streaming factories: `open_file` / `create_file`
  - Lifecycle: `close()` / `__aenter__` / `__aexit__` / static factory `connect(config)`
- **`Goosefs`** — blocking sync client, API-equivalent to `AsyncGoosefs`, plus:
  - tokio runtime / asyncio loop **deadlock guard** (Review §17.1): calling sync methods from inside a tokio worker or asyncio event loop raises `RuntimeError` instead of deadlocking.
  - **fork safety guard** (Review §17.4): records `creator_pid`; calls from a child process refuse to reuse the handle, preventing shared-socket bugs.
- **Streaming file handles**:
  - `AsyncFileReader` / `FileReader`: `read(n=-1)` / `read_at(offset, length)` / `seek(offset, whence=0)` / `tell()` / `close()` / `__len__` (file length)
  - `AsyncFileWriter` / `FileWriter`: `write(data)` / `close()` / `cancel()` (`with` block automatically calls `cancel` instead of `close` on exception)
  - All support `with` / `async with` context managers.
- **Types / options**:
  - `WriteType` enum: `MustCache` / `CacheThrough` / `Through` / `AsyncThrough` / `TryCache`, with case-insensitive `from_str()`.
  - `ReadType` enum: `Cache` / `NoCache`.
  - `OpenFileOptions` / `CreateFileOptions` / `DeleteOptions`: build reusable configs to pass to the underlying SDK.
  - `URIStatus`: a metadata snapshot with 25 fields, including `is_readable()` / `is_completed()` / `is_folder()` / `is_persisted()` / `block_count()` predicates.
- **Exception hierarchy**: 14 named exception subclasses (the `GoosefsError` base class plus `NotFound` / `AlreadyExists` / `PermissionDenied` / `Unauthorized` / `InvalidArgument` / `IoError` / `Network` / `Timeout` / `Cancelled` / `Unavailable` / `ConfigError` / `Unimplemented` / `Internal`), with full mapping from the SDK's 16 `error::Error` variants (no `_` catch-all branch).
- **Module-level helpers**:
  - `goosefs.enable_tracing(level="info", *, target="stderr")` — (Review §17.7) opt-in `tracing` bridge to stderr; idempotent and respects `RUST_LOG` first; reserves `target="logging"` / `"stdout"` for future minor releases.
  - `goosefs.__version__` — kept in sync with the underlying SDK version.
  - `goosefs.exceptions` submodule — auto-injected into `sys.modules`, supports `from goosefs.exceptions import NotFound`.

#### Wrapping & Constraints

- **`bytes-like` input validation** (P4): `write_file` / `FileWriter.write` accept `bytes` / `bytearray` / `memoryview` / `array.array("B", …)` / NumPy `uint8` and any other buffer-protocol input; **`str` is explicitly rejected** with `TypeError` to avoid implicit Latin-1 decoding.
- **atexit fallback cleanup** (Review §17.4): `Goosefs` and `AsyncGoosefs` are tracked automatically via `weakref.WeakSet`; on interpreter exit:
  - Unclosed sync handles → silent `close()` (idempotent; already-closed becomes a no-op).
  - Unclosed async handles → `ResourceWarning` (atexit cannot `await close()`, so the leak is surfaced to the user).

#### Documentation & Examples

- Five runnable examples: `quickstart.py` / `async_demo.py` / `streaming.py` / `with_pyarrow.py` / `pandas_csv.py`.
- Complete type stubs (PEP 561): `python/goosefs/__init__.pyi` (541 lines) + `python/goosefs/exceptions.pyi` (105 lines) + a `py.typed` marker.
- `mypy.stubtest` is strictly checked in CI to keep stubs and runtime signatures consistent.
- Documentation: `README.md` / `PYPI_README.md` / `DEVELOPMENT.md` / `CHANGELOG.md` (this file).

#### Packaging / Installation

- abi3 wheel, runtime floor CPython 3.9.
- Platform support: Linux x86_64 / aarch64 (manylinux_2_28), macOS x86_64 / arm64; Windows best-effort.
- Optional dependencies: `goosefs[arrow]` (pyarrow), `goosefs[pandas]` (pandas + pyarrow), `goosefs[examples]` (pyarrow + pandas).

### Changed

- **Underlying SDK upgrade**: `goosefs-sdk` 0.1.2 syncs the upstream `proto` files and fixes the `WorkerInfo.sync_cache_rate_limit` wire-type mismatch.
- **SDK short-read byte-loss root fix** (P5.5-A): `GoosefsFileInStream::read` introduces `carry_over: BytesMut`; oversized chunk overflow bytes are parked into `carry_over`, and `pos()` / `remaining()` / `is_eof()` are switched to a "user perspective". The binding side no longer needs the `ReaderState` workaround (P5.5-B has been removed).
- **SDK `tokio::io::AsyncRead + AsyncSeek` adapter** (P5.5-C): added `GoosefsAsyncReader`, so downstream consumers (opendal / JNI / C bindings) can directly use ecosystem tools such as `tokio::io::copy` / `BufReader`.

### Fixed

- Fixed a spurious panic in async `connect` on PyO3 0.27 caused by switching between `Python::detach` / `allow_threads` (P3).
- Fixed `enable_tracing` silently swallowing bad arguments on the second-call path because parameter validation was being skipped (caught by tests).

### Security

- No related changes.

---

## Notes

- 0.1.0 / 0.1.1 were used as internal milestones during P0–P5 and were never publicly released; this changelog starts from 0.1.2 (the first releasable version with P7 complete).
- For the full development cadence and per-stage deliverables, see [`docs/PYTHON_BINDING_PROGRESS.md`](../../docs/PYTHON_BINDING_PROGRESS.md).
- Version-number changes related to PyPI releases (including the 0.1.0 public release) will be backfilled once P8/P9 are restarted. P8 (CI / Wheel) and P9 (canary + regression) are both currently on hold; see [`docs/PYTHON_BINDING_PROGRESS.md`](../../docs/PYTHON_BINDING_PROGRESS.md) for details.
