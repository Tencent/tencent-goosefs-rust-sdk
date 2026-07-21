# Changelog

This document records all notable changes to the `goosefs` Python binding. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and version numbers follow [SemVer](https://semver.org/).

> Note: Version numbers stay aligned with the underlying `goosefs-sdk` crate. While P8 (CI/Wheel) is on hold, the wheels under this directory are released internally only (produced manually via `maturin build --release`); public PyPI releases will start from 0.1.0 once P8/P9 are restarted.

## [Unreleased]

### Changed

- **Default `worker_connection_pool_size` bumped from `1` to `min(cores, 4)`**
  (SDK, FLAMEGRAPH_OPTIMIZATION_PLAN §B3). This is the **only** default-
  behaviour change in this release cycle; every other flame-graph
  optimisation (A3 file-info cache, B2 range coalesce, B4 tokio worker
  cap) is opt-in. `available_parallelism` is used, so cgroup CPU limits
  are respected on Linux (containers see the container's core count, not
  the host's), and the value is capped at `DEFAULT_WORKER_CONNECTION_POOL_MAX`
  so big-core hosts do not fan out to dozens of channels per worker.
  Falls back to the legacy `1` when the platform cannot report the CPU
  count.
  - **Operational impact.** Each pooled channel establishes its own
    HTTP/2 connection and performs an **independent SASL handshake** on
    first use, so first-open latency and steady-state FD / RAM cost per
    worker scale with the pool size. On hosts with many co-resident
    Python processes (e.g. a 16-core box running 16 workers × 4 channels
    each = 64 channels/worker on the master side), operators should
    observe worker-side FD counts and master-side authentication
    request rate during rollout, and confirm no rate-limit / auth-flood
    alarm fires on the GooseFS master. To restore the legacy single-
    channel behaviour explicitly, set `.with_worker_connection_pool_size(1)`
    on the config builder or the `goosefs.client.worker.connection.pool.size=1`
    property.

---

## [0.1.7] — 2026-07-16

Aligned with `goosefs-sdk` 0.1.7. Version bump tracking the underlying SDK
release; no Python-surface API changes. `bindings/python/Cargo.toml` version
`0.1.6` → `0.1.7`, kept in sync with the root crate; `goosefs.__version__`
now reports `0.1.7`.

---

## [0.1.6] — 2026-07-02

Aligned with `goosefs-sdk` 0.1.6. This release delivers two major new
data-plane features (**client-side local page cache** and **short-circuit
local mmap read**), a wait-free rewrite of the Worker/router hot paths,
a full set of **batch metadata / lifecycle APIs**, and a large batch of
SDK-side correctness fixes. All Python surfaces inherit these
improvements transparently — most require no API change.

### Added

- **Client-side local page cache** (SDK). New opt-in, disk-backed page
  cache mirroring the GooseFS Java client's
  `goosefs.user.client.cache.*` semantics. `LocalCacheManager` provides
  striped page locks + a single metadata mutex, LRU/LFU evictors, a
  multi-directory `HashAllocator`, bounded async write-back, TTL lazy
  expiry with a background sweeper, restart restore, and overwrite
  invalidation via `on_file_open`. Integrated into
  `GoosefsFileInStream::read` / `read_at` through `read_through_cache`;
  `ReadType::NoCache` still serves hits but skips back-fill. Best-effort
  by design — misses / errors always fall back to the worker without
  affecting read correctness. Enabled via config fields plus ENV /
  properties / storage-option keys; adds `Client.Cache*` metrics
  including `HitRate`, `SpaceUsedCount`, and external read time. See
  [`docs/CLIENT_PAGE_CACHE_DESIGN.md`](../../docs/CLIENT_PAGE_CACHE_DESIGN.md)
  and [`docs/CLIENT_CONFIGURATION.md`](../../docs/CLIENT_CONFIGURATION.md).
  A new `bindings/python/examples/page_cache.py` example plus the
  `bindings/python/tests/test_page_cache.py` regression suite exercise
  the feature end-to-end from Python.

- **Short-Circuit local mmap read path** (SDK). New `short_circuit`
  module that bypasses the gRPC data plane when the client and worker
  are co-located. `LocalBlockReader` performs zero-copy reads via
  read-only `mmap` with `madvise` prefetch and optional Transparent
  Huge Pages (THP). `ShortCircuitFactory` provides per-task hot-block
  caches, negative caching, a `CapabilityProvider` hook, and a
  context-shared factory. A dedicated `SIGBUS` diagnostic handler
  surfaces mmap faults with actionable diagnostics and manages
  process-level signal installation. Local worker is auto-detected by
  interface bind. Every recoverable error transparently falls back to
  the standard gRPC path. Wired into both sequential (`read()`) and
  positioned-read (`read_at()`) paths through `file_in_stream` /
  `context` / `config`. Server-side companion: new `OpenLocalBlock`
  RPC + `OpenLocalBlockGuard` for block-lock lifecycle. Ships with an
  `sc_pr_ab` benchmark comparing local mmap vs gRPC positioned-read,
  gated E2E integration tests, and an INV-S3 / INV-D1 / INV-D2 /
  INV-S1 / INV-S2 / INV-S5 consistency regression suite. See
  [`docs/SHORT_CIRCUIT_DESIGN.md`](../../docs/SHORT_CIRCUIT_DESIGN.md).
  Python inherits this transparently — every `open_file` / `read_file`
  / `read_range` / `positioned_read` call automatically prefers the
  local mmap path when the target block resides on the co-located
  worker.

- **Batch metadata / lifecycle APIs.** `BaseFileSystem` gains a full
  batch surface (`batch_open_file`, `batch_create_file`,
  `batch_create_dir`, `batch_rename`, `batch_delete`,
  `batch_list_status`) that fans out over the concurrent path with a
  shared `Arc<BaseFileSystem>` (single Tokio spawn per batch,
  first-error-wins). Exposed to the Python binding as:

  - `AsyncGoosefs.batch_open_file(paths, options=None)` /
    `batch_create_file(paths, options=None)` /
    `batch_create_dir(paths, options=None)` /
    `batch_rename(pairs)` /
    `batch_delete(paths, options=None)` /
    `batch_list_status(paths, options=None)`.
  - Synchronous `Goosefs.batch_*` counterparts, sharing the same
    deadlock + fork guards as the rest of the sync surface.

  One PyO3 boundary crossing per batch instead of N. Type stubs
  (`python/goosefs/__init__.pyi`) updated for both classes. Includes a
  regression test (`tests/test_batch_open_file_leak.py`) that verifies
  `batch_open_file` no longer leaks worker-side reader handles when
  one path in the batch fails partway through.

- **`goosefs.WorkerClient`** — synchronous mirror of
  `AsyncWorkerClient`. New blocking escape hatch for callers that
  already know a worker address and want a one-shot
  `read_block_positioned` without going through
  `Goosefs.positioned_read` (which routes via the master). Mirrors the
  async surface 1:1: `WorkerClient.connect(addr, config)` static
  factory, `WorkerClient.connect_simple(addr, ...)` (deprecated
  NOSASL), instance methods `read_block_positioned` / `close`, `addr`
  getter, and a regular `with` context manager. Same Tokio-runtime
  guarantees as the sync `Goosefs` class — must not be called from
  inside an asyncio loop or a Tokio worker thread.

  ```python
  from goosefs import WorkerClient, Config

  with WorkerClient.connect("127.0.0.1:9203", Config("127.0.0.1:9200")) as wc:
      data = wc.read_block_positioned(block_id, offset=0, length=64 * 1024)
  ```

  Exported from `goosefs._goosefs`, re-exported from `goosefs`, and
  listed in `__all__` (top-level package + type stub).

### Changed

- **Underlying SDK upgrade**: `goosefs-sdk` 0.1.5 → 0.1.6.
- **`bindings/python/Cargo.toml`** version `0.1.5` → `0.1.6`, kept in
  sync with the root crate; `goosefs.__version__` now reports `0.1.6`.
- **Wait-free Worker / router hot paths** (SDK, transparent to Python).
  `WorkerClientPool.clients` and
  `WorkerRouter.workers` / `hash_ring` / `local_worker_id` are now
  `ArcSwap` instead of `RwLock<HashMap>`, mirroring the existing
  `ArcSwap<AuthedState>` model on `MasterClient`. The acquire and
  `select_worker` hot paths become a single atomic load + map lookup +
  cheap clone (no async `RwLock` round-trip); writes use
  `ArcSwap::rcu` copy-on-write, and same-key reconnects are still
  single-flighted by the per-key mutex — generation / single-flight /
  invalidate semantics preserved. Local A/B (`--transport=block`,
  64 threads / 16 MiB): 64 KiB `742.8 → 897.1 MiB/s` (+20.8%),
  256 KiB `1381.8 → 1434.3` (+3.8%), 1 MiB `1564.4 → 1742.4` (+11.4%);
  p999 −64% (64 KiB) / −52% (256 KiB). Every Python read routing
  through the pool / router picks up the improvement automatically.
- **Deferred `WorkerRouter` initialization** (SDK). `WorkerManager` is
  now optional and only initialized on the first write, so
  metadata-only workloads (batch APIs, `list_status`, `get_status`,
  `exists`, `mkdir`, `rename`, `delete`) no longer pay the Worker-plane
  setup cost on connect. `WorkerManager` also compatibility-degrades
  against older Master versions.
- `tests/test_worker_block_direct.py` translated from Chinese to
  English; the previously-`@pytest.mark.xfail`'d sync-`WorkerClient`
  export checks are now regular passing tests guarding the new public
  surface.

### Fixed

The Python binding inherits the following SDK-side correctness fixes
from the same release. No Python API change is required to benefit from
them — every Python read / write / metadata call already routes through
the affected paths.

- **HA primary discovery cancel-safety** (SDK C1). `PollingMasterInquireClient`
  no longer wedges when the singleflight leader's `poll_for_primary` is
  cancelled by an outer `timeout` / `select!` or panics: a new RAII
  `LeaderGuard` always broadcasts a transient error to followers and
  resets the gate so the next caller can become a fresh leader. Previously
  followers fell into infinite recursion against a dead `watch::Receiver`.
- **`WriteBlockHandle::Drop`** (SDK C3). Dropping a write handle on an
  early-error path now aborts the background gRPC task instead of leaking
  it as a detached future stuck on `stream.message().await`. Affects
  every Python writer surface (`AsyncFileWriter`, `FileWriter`,
  `write_file`).
- **`GoosefsFileInStream::seek` short forward seek byte-loss** (SDK C4).
  Small forward seeks within the same block (< 8 KiB) used to silently
  drop the chunk-tail bytes beyond the seek target; the next read then
  returned data from the wrong offset. Fixed by parking the over-pulled
  bytes into the carry-over buffer.
- **`GoosefsFileWriter::Drop` best-effort cleanup** (SDK C5). When a
  writer is dropped without `close()` / `cancel()` (e.g. an early `?` on
  the error path or a panic), Drop now spawns a best-effort cleanup task
  that cancels in-flight cache / UFS streams and either calls
  `master.remove_blocks` or falls back to `delete(unchecked=true)` so
  that worker temp blocks and INCOMPLETE inodes are not left behind.
- **`LogSampler` clock-jump safety** (SDK C6). Heartbeat WARN
  rate-limiter now uses monotonic `Instant` instead of `SystemTime`;
  NTP / administrator clock adjustments can no longer suppress all WARN
  logs until the wall-clock catches up.
- **`MetricsMasterClient::with_retry`** (SDK C2) — and its master
  counterpart — now reconnect at the *top* of the next retry attempt and
  skip the RPC if reconnect itself fails, instead of burning
  `request_timeout` against the same known-dead channel.
- **`WorkerClient::connect`** now sets `request_timeout` matching
  master / metrics / worker-manager. Previously a half-open data-plane
  connection could hang `read_block` / `write_block` indefinitely.
- **`config::parse_byte_size` overflow** now surfaces an error instead
  of silently wrapping in release builds (e.g. `"99999999999GB"` used to
  parse to a tiny block size and cause hard-to-diagnose I/O misbehaviour).
- **`WriteType::From<WritePType>`** removed (used to panic on
  `Unspecified` / `None`, both legal proto values from the server).
  Use `WriteType::try_from_proto` which returns `Result`.
- **`block::WorkerRouter` performance**: the consistent-hash ring is now
  pre-built once on `update_workers` (O(log N) `binary_search` per
  request) rather than rebuilt-and-sorted per request; local-worker probe
  is cached as `Option<Option<i64>>` so "no local worker" no longer
  re-runs `hostname::get()` on every `select_worker`; `pick_any_worker`
  uses `rand::Rng::random_range` rather than a `subsec_nanos()` modulo
  for proper load spreading.
- **`io::reader` short-read** on server-emitted empty / keep-alive frames.
  An empty data frame in the middle of a block stream no longer
  short-reads as EOF; the reader keeps draining until either the byte
  budget is met or the server half-closes.
- **`fs::base_filesystem::resolve_write_type`** now distinguishes
  `NotFound` (silent fallback to config default) from other RPC errors
  (warn before falling back), so transient master errors no longer
  silently change the persistence semantics of newly-created files.
- **`ExponentialBackoffRetry`** off-by-one — the second retry now uses
  `base_sleep * 2` as documented (used to stay at `base_sleep`).
- **`ExponentialTimeBoundedRetry::should_retry`** — `current_sleep * 2`
  now uses `saturating_mul` and can no longer panic under pathological
  configurations.
- **`batch_open_file` resource leak** (Python). Fixed a resource leak
  where `AsyncGoosefs.batch_open_file` / `Goosefs.batch_open_file`
  could leave worker-side reader handles open when one path in the
  batch failed midway; regression covered by
  `tests/test_batch_open_file_leak.py`.

### Notes

- No breaking API changes — drop-in upgrade from `0.1.5`.
- Public PyPI publication is still gated on P8 (CI/Wheel) and P9
  (canary + regression); wheels in this directory continue to be
  produced manually via `maturin build --release` for internal use.

---

## [0.1.5] — 2026-06-04

Aligned with `goosefs-sdk` 0.1.5. Underlying SDK adds Prometheus
Pushgateway support, exposes `GoosefsAsyncReader` (`tokio::io::AsyncRead`
+ `AsyncSeek`), and fixes a `GoosefsFileInStream::read` short-read
byte-loss edge case; the Python binding inherits these fixes
transparently with no API change.

### Changed

- **Underlying SDK upgrade**: `goosefs-sdk` 0.1.3 → 0.1.5. Pinned
  dependency versions (prost 0.14.1, tokio 1.23+, rand 0.9.1, reqwest
  0.12 with `rustls-tls`); adapted to rand 0.9 API changes
  (`thread_rng` → `rng`, `gen_range` → `random_range`).
- `bindings/python/Cargo.toml` version `0.1.4` → `0.1.5`, kept in sync
  with the root crate; `goosefs.__version__` now reports `0.1.5`.

### Fixed

- Inherits the SDK-side `GoosefsFileInStream::read` fix that prevented
  bytes from being dropped when the caller-supplied buffer was smaller
  than the available chunk data.

### Notes

- No breaking API changes — drop-in upgrade from `0.1.4` / `0.1.3`.
- Public PyPI publication is still gated on P8 (CI/Wheel) and P9
  (canary + regression); wheels in this directory continue to be
  produced manually via `maturin build --release` for internal use.

---

## [0.1.4] — 2026-06-03

Python-binding-only performance release. No public API additions to
the underlying `goosefs-sdk` crate; all changes live under
`bindings/python/`. Aligned with `goosefs-sdk` 0.1.4.

### Added

- **`AsyncGoosefs.batch_get_status(paths)` / `batch_exists(paths)`** —
  coroutine-based batch metadata APIs. Each path is mapped to a
  future sharing the same `Arc<BaseFileSystem>` and driven
  concurrently via `futures::future::join_all`; results are returned
  in input order, and the first error fails the whole batch. One
  PyO3 boundary crossing per batch instead of N.
- **`Goosefs.batch_get_status(paths)` / `batch_exists(paths)`** —
  synchronous counterparts. Single `guarded_block_on` + `join_all`,
  releasing the GIL only once for the entire batch.
- **Custom Tokio runtime** — new `runtime::init_custom_runtime()`
  registered via `pyo3_async_runtimes::tokio::init` at module init.
  `worker_threads = available_parallelism().max(16)`,
  `max_blocking_threads = 64`, `enable_all()`. Uses std
  `available_parallelism` (no `num_cpus` dependency).
- Type stubs (`python/goosefs/__init__.pyi`) updated with
  `batch_get_status` / `batch_exists` signatures for both
  `AsyncGoosefs` and `Goosefs`.

### Changed

- **Read-path copy elimination** (`streaming.rs` / `sync_fs.rs`):
  - `pull_n`: pre-allocate `vec![0u8; want]` and fill in place via
    `stream.read(&mut out[filled..])`, removing per-iteration `tmp`
    allocation and `extend_from_slice` copy.
  - `pull_all`: return type switched from `Vec<u8>` to `bytes::Bytes`,
    dropping a `to_vec()`.
  - Async `PyAsyncFileReader::read`: split into `size<0` (Bytes) /
    `else` (Vec) branches, each with a single `PyBytes::new`.
  - Sync `PyFileReader::read` / `read_at`: unified to `Bytes`,
    carrying `Bytes` out of the blocking section.
  - `Goosefs::read_file` / `read_range`: blocking section returns
    `Bytes`; after GIL reacquire, `PyBytes::new(py, bytes.as_ref())`
    eliminates the `to_vec()` double copy.
- **Underlying SDK upgrade**: `goosefs-sdk` 0.1.3 → 0.1.4 (version
  alignment; no public API change).
- `bindings/python/Cargo.toml` version `0.1.2` → `0.1.4` to align with
  the root crate; `goosefs.__version__` updated to `0.1.4`
  accordingly.

### Skipped / Deferred

- **`extract_bytes_like` PyBuffer zero-copy** — attempted via
  `PyBuffer<u8>`, but `pyo3::buffer` is cfg-gated out under
  `abi3-py39` (`#![cfg(any(not(Py_LIMITED_API), Py_3_11))]`).
  Enabling would require raising the abi3 floor to 3.11 and dropping
  3.9/3.10 support — not worth it. Reverted to the portable
  `extract::<Vec<u8>>()` path; the rationale and re-enable
  condition are documented inline.
- **Free-threaded Python 3.13+ / sync `tokio::sync::Mutex` removal** —
  groundwork only (`#[pymodule(gil_used = false)]` is set), deferred
  until the CPython / PyO3 ecosystem matures.

### Verified

- `cargo build -p goosefs-python`, `cargo clippy`, `read_lints` —
  all clean.
- `uv run maturin develop` produced
  `goosefs-0.1.4-cp39-abi3-*.whl`; `goosefs.__version__` reports
  `0.1.4`.
- `uv run pytest -q` → 11 passed (cluster-dependent integration
  tests gated by `GOOSEFS_MASTER_ADDR`).
- Real-cluster benchmark (single-node GooseFS, 500 paths, 7 iters,
  16 threads) — batch API median latency vs sequential loop:
  - Sync `get_status`: 100.67 ms → **37.68 ms** (**2.67×**),
    also faster than ThreadPool(16) at 45.67 ms.
  - Sync `exists`: 88.46 ms → **36.51 ms** (**2.42×**).
  - Async `get_status`: gather 41.15 ms → **36.23 ms** (**1.14×**).
  - Read throughput baseline (Phase 1 copy-elimination):
    4 KiB 2.9 MiB/s, 256 KiB 184 MiB/s, 4 MiB 701 MiB/s,
    16 MiB **948 MiB/s**.

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
