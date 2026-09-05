# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

Rust crate versions (`goosefs-sdk`) and Python package versions (`goosefs`) are
kept aligned. Python-specific notes also appear in
[`bindings/python/CHANGELOG.md`](bindings/python/CHANGELOG.md).

## [Unreleased]

### Removed

- **Short-circuit (local mmap) read path.** The `goosefs_sdk::block::short_circuit`
  module, the `OpenLocalBlock` client wrapper (`WorkerClient::open_local_block`
  and `OpenLocalBlockGuard`), `WorkerRouter::is_block_source_local`, all eleven
  `short_circuit_*` config fields with their builders, `GOOSEFS_SHORT_CIRCUIT_*`
  env vars, `goosefs.{user,client}.short.circuit.*` properties and
  `goosefs_short_circuit_*` storage options, and every `Client.ShortCircuit*`
  metric are gone. The path was disabled by default and unused in practice.

  **Impact:** reads that previously took the local mmap path now always use the
  gRPC data plane, which was already the fallback whenever short-circuit was
  off or a block was not local — so byte-level behaviour is unchanged. Code
  setting a `short_circuit_*` field or calling a `with_short_circuit_*` builder
  no longer compiles; delete the call. Unknown env vars, properties and storage
  options are ignored, so configuration files need no change. The
  `OpenLocalBlock` / `CreateLocalBlock` RPCs remain in the generated protobuf
  because they are part of the upstream GooseFS `BlockWorker` service; the SDK
  simply no longer calls them. Drops the `memmap2` dependency.

### Fixed

- **`$GOOSEFS_CONF_DIR/goosefs-site.properties` is discovered again.**
  `discover_config_file()` looked up the Java property name `goosefs.conf.dir`
  as an environment variable instead of `GOOSEFS_CONF_DIR`, so the documented
  conf-dir env var was ignored. `$GOOSEFS_CONFIG_FILE` was unaffected.

- **`GoosefsFileReader` now sends `maxUfsReadConcurrency`, fixing the hang on
  the second read of a path.** `GoosefsFileInStream` and the Python binding's
  `positioned_read` already sent the Java default of `8`; the one-shot reader
  behind `read_file` / `read_range` left `OpenUfsBlockOptions.max_ufs_read_concurrency`
  unset. An absent `optional int32` decodes as `0`, and the Worker admits a UFS
  block read only while that block's session count is *below* the limit — so
  the first read of a block succeeded (no session entry yet) and every read
  after it waited forever on a permit that could never be granted.

  This is why the failure looked like a lock leak: it keyed on the block rather
  than the client, survived process restarts, and crossed verbs — a
  `positioned_read` or `open_file` (both of which sent `8`) would leave the
  session entry behind and the next `read_file` of that path would wedge.
  Reads of a not-yet-read path always worked, which is what kept it out of the
  single-read test paths.

  Scope: only reads served from UFS are affected — `THROUGH`-written files, and
  any block that is no longer in the worker's cache. A block still cached on
  the worker never opens a UFS block session, so freshly written
  `MUST_CACHE` / `CACHE_THROUGH` / `ASYNC_THROUGH` files read back fine.

- **`get_status` / OpenDAL `stat` now send `loadMetadataType=ONCE`**, matching
  Java `FileSystemOptions.getStatusDefaults`. The previous empty
  `GetStatusPOptions` left the field unset; Master proto default is `NEVER`,
  so COS/UFS files not yet in the GooseFS namespace returned
  `NotFound` (`Path "..." does not exist.`). `exists` / `open_file` /
  `GoosefsFileReader` share this GetStatus path. Per-call override:
  `GetStatusOptions.load_metadata_type`. Set
  `GOOSEFS_FILE_METADATA_LOAD_TYPE=NEVER` to keep the old Master behaviour.

- **`MasterClient::list_status` now sends `loadMetadataType` on non-recursive
  listings too**, matching Java `FileSystemOptions.listStatusDefaults`. Only the
  recursive BFS resolved a load type before, so a direct
  `list_status(path, false)` — the call OpenDAL `list` makes — left the field
  unset. Master reads it as `NEVER`, which forces `loadDescendantType=NONE` and
  rejects a UFS-only directory, so COS objects missing from the inode tree were
  silently absent from listings. Both entry points also send `syncIntervalMs`
  from the config now, instead of letting Master fall back to its own
  `goosefs.user.file.metadata.sync.interval`. New
  `MasterClient::list_status_with_options` takes both values explicitly.

- **Write-path RPCs now send Java `commonDefaults`**. `create_file`,
  `create_directory`, `delete`, `rename`, and `complete_file` carry
  `commonOptions.syncIntervalMs` from the client config and a per-call
  `operationId` (generated once and reused across retries, matching
  `goosefs.user.file.include.operation.id`). `schedule_async_persistence`
  sends `syncIntervalMs` only, like Java `scheduleAsyncPersistDefaults`.
  `create_directory` still uses `allowExists=true` (OpenDAL `mkdir -p`);
  only the missing commonOptions/opId are added.

- **`rename` reads `goosefs.user.file.persist.on.rename`** (default `false`),
  matching Java `renameDefaults`. Set `GOOSEFS_USER_FILE_PERSIST_ON_RENAME=true`
  (or the site property / `goosefs_file_persist_on_rename` storage option)
  to async-persist the destination on rename.

- **`DeleteOptions` default `unchecked` is now `true`**, matching Java
  `goosefs.user.file.delete.unchecked`. Recursive deletes of persisted
  directories no longer run the UFS consistency check that Java skips.
  Pass `unchecked: false` to keep the old checked behaviour.

### Changed

- **The client metadata cache is now enabled by default** (`metadata_cache_enabled`,
  `goosefs.user.metadata.cache.enabled`, `GOOSEFS_METADATA_CACHE_ENABLED`). This
  deliberately **diverges from the Java client**, whose default is `false`.

  Reason: every reader open resolves its `FileInfo` through
  `FileSystemContext::get_file_info_cached`. With the cache off that call falls
  straight through to a Master `get_status` RPC, so a workload that opens one
  reader per small ranged read — which is exactly what the OpenDAL `goosefs`
  service does, one `GoosefsFileReader` per `read` — paid one Master round-trip
  per read. Nothing in the read path made that visible; it simply capped QPS.

  This is also what makes the **local page cache + io_uring** path pay off.
  A page-cache hit served over io_uring costs tens of microseconds, so a
  per-open Master RPC on the same read is orders of magnitude larger and hides
  the entire benefit of caching the data locally: the read never waits on disk,
  it waits on metadata. Turning the metadata cache on is therefore a
  prerequisite for the page cache to show up in end-to-end numbers, and users
  running that combination asked for it to be the default rather than a knob
  every deployment has to rediscover.

  TTL (`10min`) and capacity (`100000`) are unchanged and still Java-aligned.
  Set `GOOSEFS_METADATA_CACHE_ENABLED=false` to restore the previous behaviour.
  Worth doing when the file set mutates behind the client faster than
  `metadata_cache_expiration`, since the cache is process-local and out-of-band
  writers are not observed until the TTL elapses (write paths through this
  client still self-invalidate the path and its parent).

- **Default page cache eviction policy is now `LRU`** (was `LFU`), matching the
  Java client's `goosefs.user.client.cache.eviction.policy` default
  (`LRUCacheEvictor`). `docs/CLIENT_PAGE_CACHE_DESIGN.md` had specified
  `default LRU` all along — the implementation had diverged from it, so this
  brings the two back in line.

  Set `GOOSEFS_USER_CLIENT_CACHE_EVICTION_POLICY=LFU` to keep the previous
  behaviour. Worth doing if the workload periodically scans data that must not
  displace the hot set: `LFU` (W-TinyLFU) and `S3FIFO` are scan-resistant,
  `LRU` is not — a full scan under `LRU` evicts everything.

  Note `LRU` is the only policy where a live `CacheEntry` pins its record out
  of the eviction order, so it is now the default path for that constraint.
  Every hot path in `manager.rs` copies what it needs and drops the guard
  before any `.await`; `reads_do_not_stall_eviction_under_lru` guards this and
  is a P0 test.

- **Page cache metadata and eviction moved from `moka` to
  [`foyer`](https://crates.io/crates/foyer-memory).** Metadata, eviction order
  and byte accounting now live in one sharded cache per directory instead of a
  `DashMap` + a moka cache + an `AtomicU64`.

  That split was the problem: with moka's own capacity disabled, choosing a
  victim meant `iter().min_by_key()` over every resident page — measured at
  ~7ms with 100k pages (100 GB of 1 MiB pages), and ~73µs amortised per request
  even at a 99% hit rate, since the hit rate only changes how *often* the scan
  runs. Eviction is now a shard-local pop off an intrusive list: ~300ns, and
  flat as the cache grows (1.1–1.3x across a 100x range in page count, versus
  74–187x before).

  Read-hit metadata lookups improved as a side effect, because one shard
  operation now does what previously took a `DashMap` read plus a moka insert:
  p99 at 32 concurrent readers went from 151.58µs to 3.67µs, and the tail no
  longer collapses under load (85x inflation from 1 to 64 threads, versus 5x
  now). End-to-end read latency is dominated by disk IO and is unchanged.

  The io_uring page fd cache moved to foyer as well, using S3-FIFO for its
  scan resistance.

- **Fixed**: `init_uring_config` treated a queue depth or thread count of `0` as
  `1` rather than falling back to the default, contradicting its own
  documentation. Since the depth sizes both the submission channel and the
  io_uring SQ, and submission uses `try_send`, a depth of 1 made writes fail with
  `WouldBlock` -- one `put` issues three ops. Any caller leaving the uring fields
  at their zero value was affected.

  Verified on a TencentOS NVMe host after the fix (1 KiB pages, average latency):

  | Concurrency | tokio::fs | io_uring | speedup |
  |---|---|---|---|
  | 1 | 26.6µs | 17.6µs | 1.51x |
  | 8 | 22.2µs | 16.4µs | 1.36x |
  | 16 | 36.7µs | 20.9µs | 1.76x |
  | 32 | 77.2µs | 32.7µs | 2.36x |

  On the same host the three eviction policies land within 0.88-1.08x of each
  other with no consistent winner, which is the expected result for a pure
  cache-hit read path: it measures disk IO, and the metadata work the policy
  governs is nanoseconds against tens of microseconds of it. Use
  `BENCH_EVICTION_PRESSURE=1` to compare policies.

- `CacheEvictorType` gains an `S3Fifo` variant
  (`goosefs.user.client.cache.eviction.policy=S3FIFO`). The default remains
  `LFU`: S3-FIFO measured ~0.3µs better at p99, which does not justify changing
  established behaviour. It is worth selecting for scan-heavy workloads.

- Page TTL is enforced on access only. foyer has no iteration, so the
  background sweeper is gone. Expired pages were already never *served* — that
  check was always on the read path — but the space held by an expired page
  nobody touches again is now reclaimed by capacity eviction rather than on a
  timer.

- Disk capacity is now a soft bound. Evicted pages have their files deleted by
  a background reaper, so usage can briefly exceed `dir_capacity` by the reaper
  backlog: at most 1024 pages (1 GiB with 1 MiB pages), which fits inside the
  5% overhead already reserved. Watch `Client.CacheReapQueueDepth`.

### Added

- `Client.CacheReapQueueDepth`, `Client.CacheReapDropped`,
  `Client.CacheReapSkippedReadmitted` and `Client.CachePageFdTtlExpired`
  metrics for the eviction reaper and the fd cache's lazy TTL.

- `LocalCacheManager::close()`, which waits for queued evictions to be
  processed. Optional — dropping the manager also stops the reaper, but then
  queued victims are left as orphan files for the next `restore()` to reclaim.

### Removed

- **`moka` dependency.** Replaced by `foyer-memory` + `foyer-common`
  (deliberately not the umbrella `foyer` crate, which would pull in a storage
  engine that overlaps our own `PageStore`).

- **`cache::evictor` module** (`CacheEvictor` trait, `MokaCacheEvictor`,
  `build_evictor`). Eviction is internal to the metadata cache now. This is a
  breaking change for anyone who imported these directly; the replacement is to
  select a policy via `CacheEvictorType`.

- `LocalCacheManager::sweep_expired()`, together with the background sweeper
  task. See the TTL note above.

### Breaking

- `LocalCacheManager::create()` returns `Result<Arc<Self>>` rather than
  `Result<Self>`, because the reaper task holds a `Weak<Self>`. Callers that
  wrapped the result in `Arc::new` should drop that. `from_config()` already
  returned `Arc<Self>` and is unaffected, so code going through it — which is
  everything in this repository outside tests — needs no change.

- MSRV is unchanged at 1.88. foyer's published MSRV is 1.85.0; the 1.91 in its
  README is its own dev-workspace toolchain, not a constraint on consumers.

### Added

- **Client metadata cache** replacing the `FileInfo` open cache — one
  process-local TTL-bounded LRU shared by `get_status` / `exists` / `open` /
  non-recursive `list_status` (status + listing + negative entries), aligned
  with Java `goosefs.user.metadata.cache.*`
  (`metadata_cache_enabled` / `max_size` / `expiration`, default off, `10min`
  TTL, `100000` entries). Write paths invalidate the path and its parent after
  a successful RPC; new `Client.MetadataCache*` counters/gauges are reported
  through the existing heartbeat / Pushgateway pipeline. The removed knobs
  `goosefs.user.file.info.cache.ttl.ms` / `.capacity`
  (`GOOSEFS_FILE_INFO_CACHE_TTL_MS` / `GOOSEFS_FILE_INFO_CACHE_CAPACITY`)
  have no replacement other than the new keys.

### Changed

- Align consistent-hash fallback virtual nodes per worker with GooseFS 2.0
  (`goosefs.master.consistent.hash.virtual.node.num.per.worker` default
  `200` → `5000`). Used only when `WorkerInfo.virtual_node_num` is unset.
- **GooseFS 2.0 client protos synced** while keeping `CheckBlocks` bool wire
  compatibility. Recursive `list_status` is now a client-side BFS owned by
  `MasterClient` (GooseFS 2.0 dropped `ListStatusPOptions.recursive`), and the
  resolved `load_metadata_type` (default `ONCE`) is sent for both recursive and
  non-recursive listings, matching Java `listStatusDefaults()`.
- Write path slices chunks directly from the caller buffer instead of draining
  an intermediate `pending_chunk`, removing one copy per chunk on the
  `GoosefsFileWriter` hot path.

### Docs

- Documentation site: new **Metadata Cache** pages for Rust and Python,
  metadata-cache metrics in both Metrics pages, recursive-listing semantics on
  the FileSystem API pages, and consistent-hash worker-selection notes on the
  Rust Worker Block Direct Read page.

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
