# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

Rust crate versions (`goosefs-sdk`) and Python package versions (`goosefs`) are
kept aligned. Python-specific notes also appear in
[`bindings/python/CHANGELOG.md`](bindings/python/CHANGELOG.md).

## [Unreleased]

### Added

- **Inode fencing on `complete_file`** — `MasterClient::complete_file` now takes
  an `inode_id`, which `GoosefsFileWriter` fills from the `create_file`
  response. The Master feeds it to `checkClientMismatch`, so a completion whose
  path no longer resolves to that inode fails with `NotFound` instead of
  landing on a different writer's file. The Java client has always sent this
  field; passing `None` reproduces the old unfenced behaviour.
- **Stale INCOMPLETE inode reclamation** — `MasterClient::try_reclaim_stale_incomplete`
  removes the inode a writer leaves behind when it dies between `createFile`
  and `completeFile`, which otherwise occupies the path indefinitely (GooseFS
  puts no lease on those inodes). The removal is a compare-and-swap on the
  inode's mtime via the new `DeleteOptions::ttl` / `ttl_expect_mtime` fields, so
  a writer that finishes inside the race window keeps its file and the caller
  gets `ReclaimOutcome::Contended`. Since the Master stamps mtime only in
  `createFile` and `completeFileInternal`, age is the only staleness signal
  available — pick `stale_after` above the slowest legitimate write.
- `Error::TtlExpectMtimeMismatch` for the refused compare-and-swap, mapped from
  the bare gRPC `UNKNOWN` that `TtlExpectMtimeNotMatchException` arrives as.

### Changed

- Align consistent-hash fallback virtual nodes per worker with GooseFS 2.0
  (`goosefs.master.consistent.hash.virtual.node.num.per.worker` default
  `200` → `5000`). Used only when `WorkerInfo.virtual_node_num` is unset.

## [0.1.9] — 2026-08-04

### Added

- **Windows CI + wheels** — `ci.yml` / `ci_bindings_python.yml` matrices include
  `windows-latest` (check, unit tests, offline benches, native `win_amd64`
  wheel). `io-uring` stays Linux-only via target cfg; manylinux zig builds
  remain unix-only.
- **Master connection pool P2C scheduling** — `master_connection_pool_size`
  (default `1`) plus `master_connection_pool_schedule` (`RoundRobin` /
  `P2C`). With `P2C`, the pool samples two channels and picks the one with
  fewer in-flight RPCs, spreading concurrent metadata traffic across multiple
  HTTP/2 connections. Configurable via builder, env
  (`GOOSEFS_MASTER_CONNECTION_POOL_SIZE` / `GOOSEFS_MASTER_POOL_SCHEDULE`),
  properties, and storage options.
- **Sync `pread` read mode for `UringPageStore`** — opt-in
  `client_cache_sync_read_enabled` serves cache-hit reads with synchronous
  `pread`/`openat` on the calling thread instead of io_uring SQE/CQE (Linux
  only; intended for local-NVMe analytical workloads). Write/delete paths
  stay on io_uring.
- **Python lazy `list_status` API** — `list_status_grouped` /
  `batch_list_status_grouped` return a lazy `URIStatusList` that materialises
  `URIStatus` objects on demand (indexing / iteration), cutting GIL occupancy
  for large directories vs eager `list_status`.
- **Python batch API examples & integration tests** —
  `examples/batch_files.py`, `examples/batch_status.py`, plus tests for
  grouped list-status, metadata, and read/write paths.
- **Docs site** — Docusaurus user guides (Rust + Python) published to
  [GitHub Pages](https://tencent.github.io/tencent-goosefs-rust-sdk/); package
  `homepage` / `documentation` point at the site. Website-only changes skip
  code CI.

### Fixed

- **RustSec advisories** — bump `lru` to `>=0.16.3` (RUSTSEC-2026-0002) and
  PyO3 / `pyo3-async-runtimes` to `0.29` (RUSTSEC-2026-0176 / 0177); enable
  Dependabot for Cargo / Actions / npm (website).

### Changed

- Version bump: `goosefs-sdk` / `goosefs` `0.1.8` → `0.1.9`.

## [0.1.8] — 2026-07-21

### Changed

- Default `worker_connection_pool_size` bumped from `1` to `min(cores, 4)`
  (capped via `available_parallelism`); restore legacy behaviour with
  `.with_worker_connection_pool_size(1)` or
  `goosefs.client.worker.connection.pool.size=1`.
- Open-source scrub: public contribution docs, scrubbed internal paths / registry
  instructions, and Docker fixture image override via `GOOSEFS_IMAGE`.
- Version bump: `goosefs-sdk` / `goosefs` `0.1.7` → `0.1.8`.
