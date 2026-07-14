# Goosefs Rust Client — Configuration Parameter Reference

> **Version**: 0.2.0 | **Date**: 2026-07-08

This document provides a comprehensive reference for all configuration parameters
supported by the Goosefs Rust Client (`goosefs-sdk`).

---

## Table of Contents

1. [Configuration Loading Priority](#1-configuration-loading-priority)
2. [GoosefsConfig Fields](#2-goosefsconfig-fields)
   - [Connection Settings](#21-connection-settings)
   - [Data Transfer Settings](#22-data-transfer-settings)
   - [Authentication Settings](#23-authentication-settings)
   - [Master Inquire / HA Settings](#24-master-inquire--ha-settings)
   - [Config Manager Settings](#25-config-manager-settings)
   - [Transparent Acceleration Settings](#26-transparent-acceleration-settings)
   - [Authorization Settings](#27-authorization-settings)
   - [Client Local Page Cache Settings](#28-client-local-page-cache-settings)
   - [Short-Circuit (Local mmap) Read Settings](#29-short-circuit-local-mmap-read-settings)
   - [Miscellaneous Settings](#210-miscellaneous-settings)
3. [Environment Variables](#3-environment-variables)
4. [Storage Option Keys](#4-storage-option-keys)
5. [Properties File Keys](#5-properties-file-keys)
6. [Operation Options](#6-operation-options)
   - [OpenFileOptions](#61-openfileoptions)
   - [CreateFileOptions](#62-createfileoptions)
   - [DeleteOptions](#63-deleteoptions)
   - [InStreamOptions](#64-instreamoptions)
7. [Enums](#7-enums)
   - [WriteType](#71-writetype)
   - [ReadType](#72-readtype)
   - [AuthType](#73-authtype)
   - [WriteTypeXAttr](#74-writetypexattr)
   - [CacheEvictorType](#75-cacheevictortype)
8. [Configuration File Format](#8-configuration-file-format)
9. [Configuration Examples](#9-configuration-examples)

---

## 1. Configuration Loading Priority

The client loads configuration from multiple sources. When the same parameter
is set in multiple sources, the **highest-priority** source wins.

```text
Priority (highest → lowest):

  1. Environment variables (GOOSEFS_*)
  2. Properties config file (goosefs-site.properties)
  3. Built-in defaults
```

Use `GoosefsConfig::from_properties_auto()` to apply the full priority chain
automatically.

> **⚠️ Default Behavior**: When building a filesystem context via
> `FileSystemContext::connect(config)`, a `ConfigRefresher` is **automatically
> created** internally and a background config hot-reload task is started
> (runs every 60s). This background task **calls
> `GoosefsConfig::from_properties_auto()` by default** to reload the config
> file and environment variables, refreshing the transparent acceleration
> switches (`transparent_acceleration_enabled` /
> `transparent_acceleration_cosranger_enabled`).
>
> In other words, **users do not need to call `from_properties_auto()`
> manually** — as long as the client is constructed via
> `FileSystemContext::connect()` or `BaseFileSystem::connect()`, automatic
> config discovery and hot-reload are already running in the background.
>
> Full call chain:
> ```text
> FileSystemContext::connect(config)
>   └── ConfigRefresher::from_config(&config)   // initialize with the provided config
>   └── start_config_refresh_task()              // start background tokio task
>         ├── [immediate] config_refresher.refresh_transparent_acceleration_switch()
>         │     └── load_if_expire()             // eagerly load config on first connect
>         │           └── reload_properties()
>         │                 └── GoosefsConfig::from_properties_auto()  ← called immediately
>         └── every 60s loop:
>               └── config_refresher.refresh_transparent_acceleration_switch()
>                     └── load_if_expire()       // check 30s expiry
>                           └── reload_properties()
>                                 └── GoosefsConfig::from_properties_auto()  ← called automatically
> ```

### Config File Search Paths

When auto-discovering the properties file, the client searches in this order
(mirrors Java's `SITE_CONF_DIR` property):

| Priority | Path | Source |
|----------|------|--------|
| 1 | `$GOOSEFS_CONFIG_FILE` | Explicit file path (Rust-only convenience) |
| 2 | `$GOOSEFS_CONF_DIR/goosefs-site.properties` | Mirrors Java `goosefs.conf.dir` |
| 3 | `$GOOSEFS_HOME/conf/goosefs-site.properties` | Fallback when `GOOSEFS_CONF_DIR` unset |
| 4 | `~/.goosefs/goosefs-site.properties` | User home directory |
| 5 | `/etc/goosefs/goosefs-site.properties` | System-wide |

---

## 2. GoosefsConfig Fields

> **Source of truth for default values.** The `Default` column in this section
> is authoritative. §3 (Environment Variables), §4 (Storage Option Keys), §5
> (Properties File Keys) and §9.6 (Summary table) mirror the same values for
> lookup convenience. **When changing a default, update all four locations
> plus [`src/config.rs`](../src/config.rs) in the same commit** to avoid the
> documentation drifting from the code.
>
> **Rust-only fields.** A handful of knobs — the Part V streaming-read tuning
> (`master_connection_pool_size`, `prefetch_window`, `read_buffer_messages`,
> `ack_interval_bytes`, `ack_interval_chunks`) and the range-coalesce trio
> (`range_coalesce_*`) — are exposed **only through the `GoosefsConfig`
> builder / struct** and are deliberately absent from §3, §4, §5. Setting the
> corresponding Java-style key in `goosefs-site.properties` or as `GOOSEFS_*`
> env vars is silently ignored by the current SDK.

### 2.1 Connection Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `master_addr` | `String` | `"127.0.0.1:9200"` | Primary master address in `host:port` format. For single-master deployments. |
| `master_addrs` | `Vec<String>` | `[]` (empty) | Multiple master addresses for HA deployments. When >1 address, the client uses `PollingMasterInquireClient` to discover the Primary Master. If empty, `master_addr` is used. |
| `connect_timeout` | `Duration` | `30s` | Connect timeout for gRPC channels. |
| `request_timeout` | `Duration` | `5min` (300s) | Request timeout for individual RPCs. |
| `use_vpc_mapping` | `bool` | `false` | Whether to use VPC mapping addresses from `WorkerNetAddress`. |
| `root` | `String` | `""` (empty) | Root path prefix for all operations (e.g. `/goosefs-data`). All paths are prepended with this prefix. |
| `master_connection_pool_size` | `usize` | `1` | Number of independent Master gRPC channels to pool. `1` = legacy single-channel. Raising it (e.g. `4`/`8`) spreads concurrent metadata RPCs across multiple HTTP/2 connections, avoiding `SETTINGS_MAX_CONCURRENT_STREAMS` queueing under high concurrency / remote RTT. All pooled clients share one inquire client so HA failover stays consistent. **Set programmatically** via `with_master_connection_pool_size()`. (Optimization doc Part V R3.) |
| `worker_connection_pool_size` | `usize` | `min(cores, 4)` (since B3) | Number of independent gRPC channels to pool **per worker**. `1` restores the legacy single-channel-per-worker behaviour. The default now spreads concurrent block reads across multiple HTTP/2 connections to the same worker, lifting the per-connection throughput cap; each channel does its own SASL handshake. **Set programmatically** via `with_worker_connection_pool_size()`. **Operational note (since B3):** because each pooled channel performs an independent SASL handshake and holds its own FDs, raising the pool trades first-open latency and steady-state FD / RAM per worker for concurrency. `available_parallelism` is used so cgroup CPU limits are respected (containers see the container's core count) and the value is capped at `DEFAULT_WORKER_CONNECTION_POOL_MAX` so big-core hosts do not fan out to dozens of channels per worker. Operators rolling out this default on many-worker deployments should observe worker-side FD counts and master-side auth request rate; set `1` explicitly to opt out. (Optimization doc Part V R4 + FLAMEGRAPH_OPTIMIZATION_PLAN §B3.) |

#### 2.1.1 URI form (`gfs://…`)

For parity with the Java client and Hadoop-style paths, the SDK also
accepts a **URI form** that packs masters + root path into one string:

```text
gfs://<host:port>[,<host:port>...][/<root-path>]
```

Rules — deliberately identical to the plain comma-list form used by
`goosefs.master.rpc.addresses` / `GOOSEFS_MASTER_ADDR`, so nothing new
to memorise:

- Authority segment is split on `,` (whitespace around each entry is
  trimmed; empty entries are dropped).
- Path segment (if any) becomes [`root`](#21-connection-settings). A
  trailing `/` is stripped; a bare `/` collapses to no root.
- The `gfs://` scheme is mandatory — bare `host:port` lists keep going
  through the legacy path.

Entry points that accept the URI form:

| Language | Call | Example |
|---|---|---|
| Rust | `GoosefsConfig::from_uri(...)` | `GoosefsConfig::from_uri("gfs://m1:9200,m2:9200,m3:9200/data")?` |
| Rust | `GOOSEFS_MASTER_ADDR` env var | `export GOOSEFS_MASTER_ADDR="gfs://m1:9200,m2:9200/data"` |
| Python | `Config(uri)` / `Config.from_uri(uri)` | `Config("gfs://m1:9200,m2:9200,m3:9200/data")` |

The URI form is 100 % additive: existing single-address, comma-list,
properties-file, and env-var callers keep working unchanged.

### 2.2 Data Transfer Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `block_size` | `u64` | `67108864` (64 MiB) | Default block size in bytes for new files. Matches Goosefs server default. |
| `chunk_size` | `u64` | `1048576` (1 MiB) | Chunk size for streaming read/write RPCs. Each gRPC message carries one chunk. |
| `write_type` | `Option<i32>` | `None` | Default write type for newly created files. `None` = use server default (typically `MustCache`). See [WriteType](#71-writetype) for values. |
| `prefetch_window` | `i32` | `8` | Sequential-read prefetch window in chunks (sent in the first `ReadRequest`); lets the worker keep up to `(1 + prefetch_window)` chunks in flight. Mirrors Java `goosefs.user.streaming.reader.max.prefetch.window`. **Set programmatically** via `with_prefetch_window()`. (Optimization doc Part V R1-B-a.) **Note**: distinct from the per-open `InStreamOptions.prefetch_window` (default `1`, see §6.4). |
| `read_buffer_messages` | `usize` | `16` | Receive-buffer depth (in messages) between the background stream-drain task and the consumer. Mirrors Java `goosefs.user.streaming.reader.buffer.size.messages`. (Optimization doc Part V R1-B-b.) |
| `ack_interval_bytes` | `i64` | `0` | Flow-control ACK coalescing threshold in bytes. `0` = ACK every chunk (deadlock-safe default). Coalescing (`>0`, e.g. 4 MiB) is opt-in and only safe on workers that honour `prefetch_window`. **Set programmatically** via `with_ack_interval_bytes()`. (Optimization doc Part V R1-B-c.) |
| `ack_interval_chunks` | `u32` | `1` | Flow-control ACK coalescing threshold in chunks (`1` = every chunk). Companion to `ack_interval_bytes`. |

> **Performance tuning knobs**. Most of these knobs
> (`master_connection_pool_size`, `prefetch_window`, `read_buffer_messages`,
> `ack_interval_bytes`, `ack_interval_chunks`) are still **set programmatically
> only** via `GoosefsConfig` builder methods — they have no environment-variable,
> properties-file, or storage-option entry points.
>
> The three knobs targeted by `FLAMEGRAPH_OPTIMIZATION_PLAN` §A3 / §B3
> (`worker_connection_pool_size`, `file_info_cache_ttl`, `file_info_cache_capacity`)
> **do** have full env / properties / storage-option support since 0.2.1 — see
> §3, §4, §5 and §9.6 for the exact keys.
>
> See [`docs/RUST_PYTHON_SDK_OPTIMIZATION.md`](RUST_PYTHON_SDK_OPTIMIZATION.md)
> Part V for when and how to raise them, and §9.6 below for an example.

### 2.3 Authentication Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auth_type` | `AuthType` | `Simple` | Authentication type. Controls how the client authenticates with Goosefs Master/Worker. See [AuthType](#73-authtype). |
| `auth_username` | `String` | Current OS user (`$USER`) | Username for authentication. Used in SIMPLE mode as the login identity. |
| `auth_timeout` | `Duration` | `30s` | Maximum time to wait for SASL handshake completion. |

### 2.4 Master Inquire / HA Settings

These settings control the behavior of the Primary Master discovery process
in HA (multi-master) deployments.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `master_inquire_retry_max_duration` | `Duration` | `2min` (120s) | Maximum total duration for master inquire retries. |
| `master_inquire_initial_sleep` | `Duration` | `50ms` | Initial sleep time between master inquire polling rounds. Uses exponential backoff. |
| `master_inquire_max_sleep` | `Duration` | `3s` | Maximum sleep time between master inquire polling rounds (backoff cap). |
| `master_polling_timeout` | `Duration` | `30s` | Timeout for a single master polling ping RPC (`getServiceVersion`). Independent of `connect_timeout`. Mirrors Java's `goosefs.user.master.polling.timeout`. |

### 2.5 Config Manager Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `config_manager_rpc_addresses` | `Vec<String>` | `[]` (empty) | Config manager RPC addresses. When set, the client can fetch dynamic configuration from the config manager. |
| `config_rpc_port` | `u16` | `9214` | Config manager RPC port. |

### 2.6 Transparent Acceleration Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `transparent_acceleration_enabled` | `bool` | `true` | Whether transparent acceleration is enabled. Mirrors Java's `goosefs.user.client.transparent_acceleration.enabled`. |
| `transparent_acceleration_cosranger_enabled` | `bool` | `false` | Whether transparent acceleration cosranger is enabled. Mirrors Java's `goosefs.user.client.transparent_acceleration.cosranger.enabled`. |

### 2.7 Authorization Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `authorization_permission_enabled` | `bool` | `false` | Whether access control based on file permission is enabled. Mirrors Java's `goosefs.security.authorization.permission.enabled`. |
| `login_impersonation_username` | `String` | `"_HDFS_USER_"` | Impersonation username for SIMPLE/CUSTOM authentication. `"_HDFS_USER_"` = impersonate the Hadoop client user. `"_NONE_"` = disable impersonation. |

### 2.8 Client Local Page Cache Settings

The optional **client-side local page cache** caches worker/UFS reads on local
disk in fixed-size pages, serving repeat reads without a worker round-trip.
**Disabled by default**; best-effort (misses/errors fall back to the worker and
never affect correctness). Mirrors Java's `goosefs.user.client.cache.*`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `client_cache_enabled` | `bool` | `false` | Master switch for the local page cache. When `false`, all reads bypass the cache (unchanged behavior). |
| `client_cache_page_size` | `u64` | `1048576` (1 MiB) | Page size in bytes. Reads are split into pages of this size. |
| `client_cache_size` | `u64` | `21474836480` (20 GiB) | Per-directory capacity in bytes. ~5% is reserved for filesystem/metadata overhead. |
| `client_cache_dirs` | `Vec<String>` | `["/tmp/goosefs_cache"]` | Cache directories. Multiple dirs spread pages by file affinity (`HashAllocator`). |
| `client_cache_evictor` | `CacheEvictorType` | `Lfu` | Eviction policy when a directory is full. See [CacheEvictorType](#75-cacheevictortype). |
| `client_cache_async_write_enabled` | `bool` | `true` | Whether missed pages are back-filled asynchronously (bounded write-back pool). `false` = fill inline before the read returns. |
| `client_cache_async_write_threads` | `usize` | `16` | Async write-back concurrency (permits). Excess fills are dropped (`CachePutAsyncRejectionErrors`). |
| `client_cache_quota_enabled` | `bool` | `false` | Whether per-scope quota accounting is enabled (currently treated as Global). |
| `client_cache_ttl_secs` | `u64` | `0` | Page time-to-live in seconds. `0` = no expiry. Expired pages are dropped lazily on `get` and by a background sweeper. |
| `client_cache_sequential_read_enabled` | `bool` | `false` | Whether **sequential** reads (`read`) are routed through the cache. Random reads (`read_at`) always consult the cache when enabled. Off by default: routing large sequential scans through fixed-size pages turns one streamed request into many per-page positioned reads (read amplification), and a `NoCache` sequential read would re-fetch a whole page per small buffer with no caching benefit. Enable only when sequential reads are expected to be re-read. |
| `client_cache_uring_enabled` | `bool` | `true` on Linux / `false` on other platforms | **io_uring backend selector (P4, Linux 5.1+).** When `true` and io_uring is available at runtime, cache-hit reads use io_uring SQE/CQE instead of `tokio::fs` `spawn_blocking`, eliminating the per-hit thread-switch overhead (the dominant cost in the 300 QPS `clientcache_oncpu_3` profile). Falls back transparently to `LocalPageStore` (tokio::fs) when io_uring is unavailable, so the setting is safe to leave on by default. See [`docs/CLIENT_PAGE_CACHE_IO_URING_DESIGN.md`](CLIENT_PAGE_CACHE_IO_URING_DESIGN.md). |
| `client_cache_uring_queue_depth` | `usize` | `32768` | **io_uring SQ/CQ depth.** Per-ring entry capacity. Raise further (e.g. `65536`) for high-concurrency workloads to avoid SQ-full back-pressure; lower to reduce per-process kernel memory. `0` falls back to the built-in default of 32768. |
| `client_cache_uring_thread_count` | `usize` | `2` | **io_uring background thread count.** Each thread owns one `IoUring` instance; requests are dispatched round-robin. Raise to `4` on hosts with many idle cores and high concurrency; the threads spend most of their time in `io_uring_enter`, so over-provisioning wastes RAM without throughput gain. `0` falls back to the built-in default of 2. |
| `file_info_cache_ttl` | `Duration` | `30s` (enabled) | Client-side `FileInfo` (metadata) cache TTL. **On by default per FLAMEGRAPH_OPTIMIZATION_PLAN §A3** (30 s) — amortises the per-open `MasterClient::get_status` cost when the same file is opened multiple times inside one query. Set to `Duration::ZERO` to opt out (disable the cache). The SDK **explicitly invalidates** the entry on every write / delete / rename issued through this client, so the staleness window only affects **out-of-band** mutations by other writers. |
| `file_info_cache_capacity` | `usize` | `16384` | Maximum `(path, FileInfo)` LRU entries kept when `file_info_cache_ttl > 0`. Values `< 1` are clamped to `1`. |
| `range_coalesce_enabled` | `bool` | `false` (**disabled**) | Whether [`GoosefsFileReader::read_ranges_with_context`] merges adjacent input ranges into fewer, larger `read_range` calls. **Opt-in per FLAMEGRAPH_OPTIMIZATION_PLAN §B2.** When off (default), the multi-range API serves each input verbatim — behaviour is bit-identical to a caller-side loop. When on, adjacent ranges within `range_coalesce_gap_bytes` are merged (subject to `range_coalesce_max_bytes`) and the payload is spliced back so each output slice is byte-identical to a standalone `read_range`. Trades small over-read (`≤ Σ gap_i` bytes) for a large drop in H2 stream count on Lance / DuckDB scan patterns. **Failure semantics.** Because a merged fetch shares one transport with all its constituent input ranges, a fetch failure fails **all** those ranges together (this matches the failure model the underlying H2 layer would produce anyway, but it does enlarge the blast radius compared with per-range independent reads — enable per-workload if failure isolation between adjacent small ranges matters). |
| `range_coalesce_gap_bytes` | `u64` | `65536` (64 KiB) | Maximum permitted gap between two adjacent input ranges for them to be merged. Consulted only when `range_coalesce_enabled = true`. |
| `range_coalesce_max_bytes` | `u64` | `4194304` (4 MiB) | Upper bound on any single **merged** fetch. A caller-requested range whose own length already exceeds this cap is served as one fetch of that size (splitting a single caller request would violate the byte-equivalence contract) — the cap only prevents *merging* from ballooning the request. Values `< 1` are clamped to `1`. |

#### Page-Cache Backend: tokio::fs vs io_uring (Linux 5.1+)

When `client_cache_enabled = true`, the page cache picks one of two disk
backends at construction time (`LocalCacheManager::create`):

| Backend | Activated when | Per-cache-hit cost | Notes |
|---|---|---|---|
| `LocalPageStore` (tokio::fs) | `client_cache_uring_enabled = false`, OR non-Linux, OR Linux kernel < 5.1, OR `io_uring::IoUring::new(4)` probe fails | 3 × `spawn_blocking` (open + seek + read) | Universal, always available. Current ~300 QPS ceiling on a single core. |
| `UringPageStore` (io_uring) | `client_cache_uring_enabled = true` AND Linux ≥ 5.1 AND probe succeeds | 1 SQE (read, fd cached — P4) or 3 SQEs (cold fd) | Eliminates `spawn_blocking` from the cache-hit hot path. Expected ~900–1200 QPS on the same hardware. |

Both backends share the **same on-disk layout**
(`<dir>/<page_size>/<bucket>/<file_id>/<page_index>` and the `.identity`
sidecar) so a process can freely switch backends across restarts without
orphaning cached pages.

**Cross-backend compatibility**: a page written by the tokio::fs backend is
readable by the io_uring backend (and vice-versa) — the byte format is
identical. See §10 of
[`docs/CLIENT_PAGE_CACHE_IO_URING_DESIGN.md`](CLIENT_PAGE_CACHE_IO_URING_DESIGN.md)
for the disk-format parity test matrix.

**Observability** (see §8 of the design doc): `Client.CacheUringBackendActive`
gauge reports `1` when io_uring is active, `0` otherwise. `Client.CacheUring*`
counters/gauges report SQE/CQE throughput, in-flight requests and error
counts. **Config-vs-runtime mismatch** (e.g. `client_cache_uring_enabled = true`
on a non-Linux host) is logged at `WARN` and the client transparently falls
back to `LocalPageStore`.

> The cache lives on the `FileSystemContext` and is shared by every reader it
> opens. On (re)open the cache compares the file's `(length,
> last_modification_time)` and invalidates stale pages if the file changed.
> This identity is also persisted on disk alongside the pages, so overwrite
> detection survives a process restart (pages restored from a previous run are
> re-validated on the next open). Effectiveness is observable via
> `Client.Cache*` metrics (e.g. `CacheBytesReadCache` vs
> `CacheBytesReadExternal`). See
> [`docs/CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md).
>
> **Consistency caveat (best-effort)**: overwrite detection depends on the
> `mtime` granularity reported by the backing UFS. On a UFS with only
> second-level `mtime`, two equal-length writes within the same second (or any
> same-`(length, mtime)` in-place overwrite) are indistinguishable and may
> serve stale pages until eviction/TTL. Use a short `client_cache_ttl_secs`
> where millisecond `mtime` precision is not guaranteed.
>
> **Which read paths use the cache**: only the seekable streaming reader
> (`GoosefsFileInStream` / Python `fs.open_file(...)` → `read` / `read_at`).
> Random `read_at` always consults the cache; **sequential `read` bypasses it
> by default** (`client_cache_sequential_read_enabled = false`) to avoid read
> amplification. The one-shot `GoosefsFileReader::read_file` / `read_range` and
> `positioned_read` helpers use the worker-direct path and bypass the local
> page cache.

### 2.9 Short-Circuit (Local mmap) Read Settings

When the block being read lives on a **worker co-located with the client**
(same host), the SDK can skip the gRPC data plane entirely and `mmap` the
block file from the worker's tiered storage directly (design details in
[`docs/SHORT_CIRCUIT_DESIGN.md`](SHORT_CIRCUIT_DESIGN.md)). This is called the
**short-circuit (SC) read path** and is byte-equivalent to the gRPC path
(regression suite: [`tests/sc_consistency.rs`](../tests/sc_consistency.rs),
[`tests/short_circuit_e2e.rs`](../tests/short_circuit_e2e.rs)).

> **All three configuration paths are wired up.** Every field below can be set
> programmatically on `GoosefsConfig`, via a `GOOSEFS_SHORT_CIRCUIT_*`
> environment variable, via `goosefs.user.short.circuit.*` /
> `goosefs.client.short.circuit.*` in `goosefs-site.properties`, or via a
> `goosefs_short_circuit_*` storage option (Lance / OpenDAL). See §3, §4 and
> §5 for the canonical key names.
>
> **When does SC engage?** Only when [`block::WorkerRouter`](../src/block/router.rs)
> resolves the target block to the local host (mirrors Java `LocalFirstPolicy`).
> Reads served by a remote worker always fall back to the gRPC data plane
> regardless of these switches. See
> [`docs/PAGE_CACHE_VS_SHORT_CIRCUIT.md`](PAGE_CACHE_VS_SHORT_CIRCUIT.md) for
> how SC interacts with the client-side page cache (§2.8).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `short_circuit_enabled` | `bool` | `false` | Master kill switch for the SC path. **Disabled by default** since 0.1.6 — the 2026-07-07 hotspot analysis showed that enabling this switch materially reduced throughput on Lance/DuckDB high-concurrency vector search (~600 QPS vs ~900 QPS with SC off), and the demo binary reference flame graph contains no SC frames either. Set to `true` to opt back into the local mmap fast path on deployments that genuinely benefit from it (e.g. co-located small-object reads with a warm block cache). Mirrors Java `goosefs.user.short.circuit.enabled` semantically. See `docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md` §C6. |
| `short_circuit_cache_capacity` | `usize` | `64` | Per-task LRU capacity for hot-block SC readers (kept inside `ShortCircuitFactory`). Raising it reduces `mmap` / `open` churn on workloads that keep re-touching the same blocks; each cached reader holds an `mmap` region so tune against per-process VMA / FD budget. |
| `short_circuit_cache_ttl` | `Duration` | `30s` | Idle TTL after which a cached SC reader is dropped even if the LRU is not full. Protects against ever-growing mapping tables on long-lived processes. |
| `short_circuit_neg_cache_ttl` | `Duration` | `5s` | Negative-cache TTL: after a block fails to open via SC (e.g. worker moved it out of its committed tier), the client will not retry SC for that block for this long and falls back to gRPC. Prevents thrashing when a block is genuinely non-SC-eligible. |
| `short_circuit_advise` | `String` | `"random"` | L1 kernel readahead hint issued via `madvise` on the mapping. Accepted values: `"sequential"` / `"random"` / `"normal"` / `"none"` (case-insensitive). `random` matches the positioned-read workload SC is optimised for; switch to `sequential` for scan-heavy Parquet/Arrow readers. |
| `short_circuit_prefetch_enabled` | `bool` | `true` | L2 application-level prefetch master switch. When `false`, `ShortCircuitReader::prefetch` / `prefetch_many` degrade to no-ops (mapping still exists, but no proactive `madvise(WILLNEED)` is issued). |
| `short_circuit_prefetch_coalesce_gap` | `usize` | `65536` (64 KiB) | Maximum gap between adjacent ranges that `prefetch_many` will merge into a single `madvise` call. Only consulted when `short_circuit_prefetch_enabled = true`. |
| `short_circuit_prefetch_max_batch` | `usize` | `1024` | Upper bound on how many `madvise` calls a single `prefetch_many` may issue. Prevents a pathological caller from spending unbounded time in kernel syscalls when passing thousands of tiny disjoint ranges. |
| `short_circuit_min_block_size` | `i64` | `0` (disabled) | Minimum block size (bytes) required to attempt SC. Blocks smaller than this skip SC and go through gRPC. `0` means "no lower bound". Useful when SC's fixed per-block `mmap`/`open` cost outweighs the transfer savings on very small blocks. |
| `short_circuit_sigbus_handler` | `bool` | `true` | Install a process-global `SIGBUS` handler that diagnoses and `abort`s on a mapping fault. A `SIGBUS` on a committed, locked SC block indicates a **protocol violation (INV-D1)** — aborting surfaces it loudly rather than silently returning torn/stale bytes (design §3.2 / §8.1). Linux/macOS only; a no-op elsewhere. Turn off only if the host process already installs its own `SIGBUS` handler and cannot chain. |
| `short_circuit_thp` | `bool` | `false` (**experimental**) | Request Transparent Huge Pages for the block mapping via `madvise(MADV_HUGEPAGE)`. Linux only and effective only where file-backed THP is supported (recent kernels + specific tmpfs mounts); a no-op elsewhere. |

**Metrics.** `Client.ShortCircuit*` counters expose per-path hit / fallback
/ bytes counters; the integration tests (`sc_*.rs`, `short_circuit_e2e.rs`)
assert on them to verify SC actually engages. See
[`src/metrics/registry.rs`](../src/metrics/registry.rs) §7.3 for the full list.

**Programmatic example.**

```rust
use std::time::Duration;
use goosefs_sdk::config::GoosefsConfig;

let mut config = GoosefsConfig::new("127.0.0.1:9200");

// A/B comparison: turn SC off to isolate the gRPC data path.
config.short_circuit_enabled = false;

// Or: keep SC on but tune it for a scan-heavy Parquet workload.
config.short_circuit_enabled = true;
config.short_circuit_advise = "sequential".to_string();
config.short_circuit_cache_capacity = 256;                // larger hot-block LRU
config.short_circuit_cache_ttl = Duration::from_secs(60);
config.short_circuit_min_block_size = 4 * 1024 * 1024;    // skip SC below 4 MiB
```

### 2.10 Miscellaneous Settings

| Constant | Value | Description |
|----------|-------|-------------|
| `IMPERSONATION_NONE` | `"_NONE_"` | Sentinel value to disable impersonation. |
| `DEFAULT_MASTER_PORT` | `9200` | Default Goosefs Master RPC port. |
| `DEFAULT_WORKER_PORT` | `9203` | Default Goosefs Worker data port. |
| `DEFAULT_CONFIG_RPC_PORT` | `9214` | Default Config Manager RPC port. |
| `DEFAULT_CONFIG_EXPIRE_MS` | `30000` (30s) | Config expiry time for `ConfigRefresher` hot-reload. |

---

## 3. Environment Variables

All environment variables are optional. When set, they override the corresponding
properties file values and built-in defaults.

| Environment Variable | GoosefsConfig Field | Default | Description |
|---------------------|---------------------|---------|-------------|
| `GOOSEFS_MASTER_ADDR` | `master_addr` / `master_addrs` | `"127.0.0.1:9200"` (single) / `[]` (HA list) | Master address(es). Three accepted forms: single `host:port`; comma-separated list `addr1:port,addr2:port` for HA; or a Hadoop-style URI `gfs://addr1:port,addr2:port/root-path` (URI form also seeds `root`). |
| `GOOSEFS_WRITE_TYPE` | `write_type` | `None` (server default, typically `MustCache`) | Default write type. Accepted: `must_cache`, `try_cache`, `cache_through`, `through`, `async_through` (case-insensitive). |
| `GOOSEFS_BLOCK_SIZE` | `block_size` | `67108864` (64 MiB) | Block size in bytes (plain integer). |
| `GOOSEFS_CHUNK_SIZE` | `chunk_size` | `1048576` (1 MiB) | Chunk size in bytes (plain integer). |
| `GOOSEFS_AUTH_TYPE` | `auth_type` | `Simple` | Authentication type. Accepted: `nosasl`, `simple` (case-insensitive). |
| `GOOSEFS_AUTH_USERNAME` | `auth_username` | current OS user (`$USER`) | Authentication username. |
| `GOOSEFS_CONFIG_FILE` | — | — | Explicit path to a config file (Rust-only convenience, highest priority). |
| `GOOSEFS_CONF_DIR` | — | — | Goosefs configuration directory (mirrors Java `goosefs.conf.dir`). |
| `GOOSEFS_HOME` | — | — | Goosefs installation home directory. |
| `GOOSEFS_CONFIG_MANAGER_RPC_ADDRESSES` | `config_manager_rpc_addresses` | `[]` (empty) | Config manager RPC addresses (comma-separated). |
| `GOOSEFS_CONFIG_RPC_PORT` | `config_rpc_port` | `9214` | Config manager RPC port. |
| `GOOSEFS_TRANSPARENT_ACCELERATION_ENABLED` | `transparent_acceleration_enabled` | `true` | Transparent acceleration enabled (`true`/`false`). |
| `GOOSEFS_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED` | `transparent_acceleration_cosranger_enabled` | `false` | Transparent acceleration cosranger enabled (`true`/`false`). |
| `GOOSEFS_AUTHORIZATION_PERMISSION_ENABLED` | `authorization_permission_enabled` | `false` | Authorization permission enabled (`true`/`false`). |
| `GOOSEFS_LOGIN_IMPERSONATION_USERNAME` | `login_impersonation_username` | `"_HDFS_USER_"` | Login impersonation username. |
| `GOOSEFS_USER_CLIENT_CACHE_ENABLED` | `client_cache_enabled` | `false` | Enable the local page cache (`true`/`false`). |
| `GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE` | `client_cache_page_size` | `1048576` (1 MiB) | Page size in bytes (plain integer). |
| `GOOSEFS_USER_CLIENT_CACHE_SIZE` | `client_cache_size` | `21474836480` (20 GiB) | Per-directory capacity in bytes (plain integer). |
| `GOOSEFS_USER_CLIENT_CACHE_DIRS` | `client_cache_dirs` | `["/tmp/goosefs_cache"]` | Cache directories (comma-separated). |
| `GOOSEFS_USER_CLIENT_CACHE_EVICTION_POLICY` | `client_cache_evictor` | `Lfu` | Eviction policy: `LRU` / `LFU` (case-insensitive). |
| `GOOSEFS_USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED` | `client_cache_async_write_enabled` | `true` | Async back-fill enabled (`true`/`false`). |
| `GOOSEFS_USER_CLIENT_CACHE_ASYNC_WRITE_THREADS` | `client_cache_async_write_threads` | `16` | Async write-back concurrency (plain integer). |
| `GOOSEFS_USER_CLIENT_CACHE_QUOTA_ENABLED` | `client_cache_quota_enabled` | `false` | Quota accounting enabled (`true`/`false`). |
| `GOOSEFS_USER_CLIENT_CACHE_TTL_SECONDS` | `client_cache_ttl_secs` | `0` (no expiry) | Page TTL in seconds (`0` = no expiry). |
| `GOOSEFS_USER_CLIENT_CACHE_SEQUENTIAL_READ_ENABLED` | `client_cache_sequential_read_enabled` | `false` | Route sequential reads through the cache (`true`/`false`). |
| `GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED` | `client_cache_uring_enabled` | `true` on Linux / `false` on other platforms | Use the io_uring page-cache backend (`true`/`false`). Falls back to tokio::fs when io_uring is unavailable. |
| `GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH` | `client_cache_uring_queue_depth` | `32768` | io_uring SQ/CQ depth (plain integer). `0` is ignored. |
| `GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT` | `client_cache_uring_thread_count` | `2` | io_uring background thread count (plain integer). `0` is ignored. |
| `GOOSEFS_WORKER_CONNECTION_POOL_SIZE` | `worker_connection_pool_size` | `min(cores, 4)` | Per-worker gRPC channel pool size (plain integer). `0` is clamped to `1`; non-numeric values are ignored (default kept). See FLAMEGRAPH_OPTIMIZATION_PLAN §B3. |
| `GOOSEFS_FILE_INFO_CACHE_TTL_MS` | `file_info_cache_ttl` | `30000` (30 s) | Client-side `FileInfo` cache TTL in **milliseconds**. Default is 30 s (cache enabled). `0` disables the cache; any positive value controls staleness bound for out-of-band mutations. See FLAMEGRAPH_OPTIMIZATION_PLAN §A3. |
| `GOOSEFS_FILE_INFO_CACHE_CAPACITY` | `file_info_cache_capacity` | `16384` | Maximum `(path, FileInfo)` LRU entries when the FileInfo cache is enabled (plain integer). `0` is clamped to `1`. |
| `GOOSEFS_SHORT_CIRCUIT_ENABLED` | `short_circuit_enabled` | `false` | Master kill switch for the short-circuit local-mmap read path (`true`/`false`). **Disabled by default** since 0.1.6 (see §2.9 and `docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md` §C6). |
| `GOOSEFS_SHORT_CIRCUIT_CACHE_CAPACITY` | `short_circuit_cache_capacity` | `64` | Per-task LRU capacity for hot-block SC readers (plain integer). |
| `GOOSEFS_SHORT_CIRCUIT_CACHE_TTL_MS` | `short_circuit_cache_ttl` | `30000` (30s) | Idle TTL of a cached SC reader in **milliseconds**. |
| `GOOSEFS_SHORT_CIRCUIT_NEG_CACHE_TTL_MS` | `short_circuit_neg_cache_ttl` | `5000` (5s) | Negative-cache TTL in **milliseconds** — how long a block that failed SC is kept off the SC path. |
| `GOOSEFS_SHORT_CIRCUIT_ADVISE` | `short_circuit_advise` | `"random"` | L1 `madvise` readahead hint: `sequential` / `random` / `normal` / `none` (case-insensitive; validated by `ShortCircuitFactory`). |
| `GOOSEFS_SHORT_CIRCUIT_PREFETCH_ENABLED` | `short_circuit_prefetch_enabled` | `true` | L2 application-level prefetch master switch (`true`/`false`). |
| `GOOSEFS_SHORT_CIRCUIT_PREFETCH_COALESCE_GAP` | `short_circuit_prefetch_coalesce_gap` | `65536` (64 KiB) | Max gap (bytes) between adjacent ranges merged by `prefetch_many`. |
| `GOOSEFS_SHORT_CIRCUIT_PREFETCH_MAX_BATCH` | `short_circuit_prefetch_max_batch` | `1024` | Upper bound on `madvise` calls per `prefetch_many`. |
| `GOOSEFS_SHORT_CIRCUIT_MIN_BLOCK_SIZE` | `short_circuit_min_block_size` | `0` (no minimum) | Minimum block size (bytes) required to attempt SC. Blocks smaller than this skip SC. |
| `GOOSEFS_SHORT_CIRCUIT_SIGBUS_HANDLER` | `short_circuit_sigbus_handler` | `true` | Install a process-global SIGBUS diagnostic handler (`true`/`false`). Linux/macOS only. |
| `GOOSEFS_SHORT_CIRCUIT_THP` | `short_circuit_thp` | `false` | Request Transparent Huge Pages via `madvise(MADV_HUGEPAGE)` (`true`/`false`). Linux only, **experimental**. |

---

## 4. Storage Option Keys

These constants are used in `storage_options` maps (e.g. Lance's
`DatasetBuilder::with_storage_option` or OpenDAL config).

| Constant | Key String | Default | Description |
|----------|-----------|---------|-------------|
| `STORAGE_OPT_MASTER_ADDR` | `goosefs_master_addr` | `"127.0.0.1:9200"` | Master address(es). Supports HA: `"addr1:port,addr2:port"`. |
| `STORAGE_OPT_WRITE_TYPE` | `goosefs_write_type` | `None` (server default, typically `MustCache`) | Default write type (case-insensitive). |
| `STORAGE_OPT_BLOCK_SIZE` | `goosefs_block_size` | `67108864` (64 MiB) | Block size in bytes. |
| `STORAGE_OPT_CHUNK_SIZE` | `goosefs_chunk_size` | `1048576` (1 MiB) | Chunk size in bytes. |
| `STORAGE_OPT_AUTH_TYPE` | `goosefs_auth_type` | `Simple` | Authentication type (case-insensitive). |
| `STORAGE_OPT_AUTH_USERNAME` | `goosefs_auth_username` | current OS user (`$USER`) | Authentication username. |
| `STORAGE_OPT_CONFIG_MANAGER_RPC_ADDRESSES` | `goosefs_config_manager_rpc_addresses` | `[]` (empty) | Config manager RPC addresses. |
| `STORAGE_OPT_CONFIG_RPC_PORT` | `goosefs_config_rpc_port` | `9214` | Config manager RPC port. |
| `STORAGE_OPT_TRANSPARENT_ACCELERATION_ENABLED` | `goosefs_transparent_acceleration_enabled` | `true` | Transparent acceleration enabled. |
| `STORAGE_OPT_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED` | `goosefs_transparent_acceleration_cosranger_enabled` | `false` | Transparent acceleration cosranger enabled. |
| `STORAGE_OPT_AUTHORIZATION_PERMISSION_ENABLED` | `goosefs_authorization_permission_enabled` | `false` | Authorization permission enabled. |
| `STORAGE_OPT_LOGIN_IMPERSONATION_USERNAME` | `goosefs_login_impersonation_username` | `"_HDFS_USER_"` | Login impersonation username. |
| `STORAGE_OPT_CLIENT_CACHE_ENABLED` | `goosefs_client_cache_enabled` | `false` | Enable the local page cache. |
| `STORAGE_OPT_CLIENT_CACHE_PAGE_SIZE` | `goosefs_client_cache_page_size` | `1048576` (1 MiB) | Page size in bytes. |
| `STORAGE_OPT_CLIENT_CACHE_SIZE` | `goosefs_client_cache_size` | `21474836480` (20 GiB) | Per-directory capacity in bytes. |
| `STORAGE_OPT_CLIENT_CACHE_DIRS` | `goosefs_client_cache_dirs` | `["/tmp/goosefs_cache"]` | Cache directories (comma-separated). |
| `STORAGE_OPT_CLIENT_CACHE_EVICTOR` | `goosefs_client_cache_eviction_policy` | `Lfu` | Eviction policy (`LRU`/`LFU`). |
| `STORAGE_OPT_CLIENT_CACHE_URING_ENABLED` | `goosefs_client_cache_uring_enabled` | `true` on Linux / `false` on other platforms | Use the io_uring page-cache backend. |
| `STORAGE_OPT_CLIENT_CACHE_URING_QUEUE_DEPTH` | `goosefs_client_cache_uring_queue_depth` | `32768` | io_uring SQ/CQ depth (integer as string). |
| `STORAGE_OPT_CLIENT_CACHE_URING_THREAD_COUNT` | `goosefs_client_cache_uring_thread_count` | `2` | io_uring background thread count (integer as string). |
| `STORAGE_OPT_WORKER_CONNECTION_POOL_SIZE` | `goosefs_worker_connection_pool_size` | `min(cores, 4)` | Per-worker gRPC channel pool size (integer as string). `0` is clamped to `1`. |
| `STORAGE_OPT_FILE_INFO_CACHE_TTL_MS` | `goosefs_file_info_cache_ttl_ms` | `30000` (30 s) | Client-side `FileInfo` cache TTL in **milliseconds** (integer as string). Default is 30 s (cache enabled). `0` disables the cache; any positive value controls staleness bound for out-of-band mutations. |
| `STORAGE_OPT_FILE_INFO_CACHE_CAPACITY` | `goosefs_file_info_cache_capacity` | `16384` | `FileInfo` LRU capacity when the cache is enabled (integer as string). `0` is clamped to `1`. |
| `STORAGE_OPT_SHORT_CIRCUIT_ENABLED` | `goosefs_short_circuit_enabled` | `false` | Master kill switch for the short-circuit local-mmap read path. **Disabled by default** since 0.1.6. |
| `STORAGE_OPT_SHORT_CIRCUIT_CACHE_CAPACITY` | `goosefs_short_circuit_cache_capacity` | `64` | Per-task LRU capacity for hot-block SC readers. |
| `STORAGE_OPT_SHORT_CIRCUIT_CACHE_TTL_MS` | `goosefs_short_circuit_cache_ttl_ms` | `30000` (30s) | Idle TTL of a cached SC reader in **milliseconds**. |
| `STORAGE_OPT_SHORT_CIRCUIT_NEG_CACHE_TTL_MS` | `goosefs_short_circuit_neg_cache_ttl_ms` | `5000` (5s) | Negative-cache TTL in **milliseconds**. |
| `STORAGE_OPT_SHORT_CIRCUIT_ADVISE` | `goosefs_short_circuit_advise` | `"random"` | L1 `madvise` readahead hint: `sequential` / `random` / `normal` / `none`. |
| `STORAGE_OPT_SHORT_CIRCUIT_PREFETCH_ENABLED` | `goosefs_short_circuit_prefetch_enabled` | `true` | L2 application-level prefetch master switch. |
| `STORAGE_OPT_SHORT_CIRCUIT_PREFETCH_COALESCE_GAP` | `goosefs_short_circuit_prefetch_coalesce_gap` | `65536` (64 KiB) | Max gap (bytes) between adjacent ranges merged by `prefetch_many`. |
| `STORAGE_OPT_SHORT_CIRCUIT_PREFETCH_MAX_BATCH` | `goosefs_short_circuit_prefetch_max_batch` | `1024` | Upper bound on `madvise` calls per `prefetch_many`. |
| `STORAGE_OPT_SHORT_CIRCUIT_MIN_BLOCK_SIZE` | `goosefs_short_circuit_min_block_size` | `0` (no minimum) | Minimum block size (bytes) required to attempt SC. |
| `STORAGE_OPT_SHORT_CIRCUIT_SIGBUS_HANDLER` | `goosefs_short_circuit_sigbus_handler` | `true` | Install a process-global SIGBUS diagnostic handler. Linux/macOS only. |
| `STORAGE_OPT_SHORT_CIRCUIT_THP` | `goosefs_short_circuit_thp` | `false` | Request Transparent Huge Pages via `madvise(MADV_HUGEPAGE)`. Linux only, **experimental**. |

> **Note**: `STORAGE_OPT_*` keys are string constants exposed by the SDK for
> external consumers such as `opendal_service_goosefs` or Lance's
> `DatasetBuilder::with_storage_option`. The mapping from a
> `storage_options` map to `GoosefsConfig` builder methods is performed by the
> integrating layer (e.g. OpenDAL service) — the SDK itself only exports the
> canonical key strings so both sides agree on the naming.

> For storage-option deployments, the async-write / quota / TTL /
> sequential-read knobs on the *client-side page cache* are not exposed as
> dedicated `goosefs_*` keys; set them via properties or environment variables
> (§3, §5) instead.

---

## 5. Properties File Keys

These keys are used in `goosefs-site.properties` files (Java-style `key=value` format).

| Properties Key | GoosefsConfig Field | Value Format | Default | Description |
|---------------|---------------------|--------------|---------|-------------|
| `goosefs.master.hostname` | `master_addr` (host part) | hostname/IP | `"127.0.0.1"` | Master hostname. Combined with `goosefs.master.rpc.port` to form `master_addr`. |
| `goosefs.master.rpc.port` | `master_addr` (port part) | integer | `9200` | Master RPC port. |
| `goosefs.master.rpc.addresses` | `master_addr` + `master_addrs` | comma-separated `host:port` | `[]` (empty) | HA master addresses. First address becomes `master_addr`. |
| `goosefs.config.manager.rpc.addresses` | `config_manager_rpc_addresses` | comma-separated `host:port` | `[]` (empty) | Config manager RPC addresses. |
| `goosefs.config.rpc.port` | `config_rpc_port` | integer | `9214` | Config manager RPC port. |
| `goosefs.security.authentication.type` | `auth_type` | `NOSASL` / `SIMPLE` | `SIMPLE` | Authentication type. |
| `goosefs.security.login.username` | `auth_username` | string | current OS user (`$USER`) | Login username. |
| `goosefs.security.authorization.permission.enabled` | `authorization_permission_enabled` | `true` / `false` | `false` | Permission-based access control. |
| `goosefs.security.login.impersonation.username` | `login_impersonation_username` | string | `"_HDFS_USER_"` | Impersonation username. |
| `goosefs.user.file.writetype.default` | `write_type` | `MUST_CACHE` / `TRY_CACHE` / `CACHE_THROUGH` / `THROUGH` / `ASYNC_THROUGH` | unset (server default, typically `MUST_CACHE`) | Default write type. |
| `goosefs.user.block.size.bytes.default` | `block_size` | byte size (e.g. `64MB`, `512KB`, `134217728`) | `67108864` (64 MiB) | Default block size. Supports `KB`/`MB`/`GB` suffixes. |
| `goosefs.user.network.data.transfer.chunk.size` | `chunk_size` | byte size (e.g. `1MB`, `512KB`) | `1048576` (1 MiB) | Streaming chunk size. Supports `KB`/`MB`/`GB` suffixes. |
| `goosefs.user.client.transparent_acceleration.enabled` | `transparent_acceleration_enabled` | `true` / `false` | `true` | Transparent acceleration. |
| `goosefs.user.client.transparent_acceleration.cosranger.enabled` | `transparent_acceleration_cosranger_enabled` | `true` / `false` | `false` | Transparent acceleration cosranger. |
| `goosefs.user.client.cache.enabled` | `client_cache_enabled` | `true` / `false` | `false` | Enable the local page cache. |
| `goosefs.user.client.cache.page.size` | `client_cache_page_size` | byte size (e.g. `1MB`) | `1048576` (1 MiB) | Page size. Supports `KB`/`MB`/`GB` suffixes. |
| `goosefs.user.client.cache.size` | `client_cache_size` | byte size (e.g. `20GB`) | `21474836480` (20 GiB) | Per-directory capacity. Supports `KB`/`MB`/`GB` suffixes. |
| `goosefs.user.client.cache.dirs` | `client_cache_dirs` | comma-separated paths | `/tmp/goosefs_cache` | Cache directories. |
| `goosefs.user.client.cache.eviction.policy` | `client_cache_evictor` | `LRU` / `LFU` | `LFU` | Eviction policy. |
| `goosefs.user.client.cache.async.write.enabled` | `client_cache_async_write_enabled` | `true` / `false` | `true` | Async back-fill. |
| `goosefs.user.client.cache.async.write.threads` | `client_cache_async_write_threads` | integer | `16` | Async write-back concurrency. |
| `goosefs.user.client.cache.quota.enabled` | `client_cache_quota_enabled` | `true` / `false` | `false` | Quota accounting. |
| `goosefs.user.client.cache.ttl.seconds` | `client_cache_ttl_secs` | integer (seconds) | `0` (no expiry) | Page TTL. `0` = no expiry. |
| `goosefs.user.client.cache.sequential.read.enabled` | `client_cache_sequential_read_enabled` | `true` / `false` | `false` | Route sequential reads through the cache (off by default). |
| `goosefs.user.client.cache.uring.enabled` | `client_cache_uring_enabled` | `true` / `false` | `true` on Linux / `false` elsewhere | Use the io_uring page-cache backend. Falls back to tokio::fs when unavailable. |
| `goosefs.user.client.cache.uring.queue.depth` | `client_cache_uring_queue_depth` | integer | `32768` | io_uring SQ/CQ depth. `0` falls back to default. |
| `goosefs.user.client.cache.uring.thread.count` | `client_cache_uring_thread_count` | integer | `2` | io_uring background thread count. `0` falls back to default. |
| `goosefs.user.worker.connection.pool.size` | `worker_connection_pool_size` | integer | `min(cores, 4)` | Per-worker gRPC channel pool size. `0` is clamped to `1`. See FLAMEGRAPH_OPTIMIZATION_PLAN §B3. |
| `goosefs.user.file.info.cache.ttl.ms` | `file_info_cache_ttl` | integer (milliseconds) | `30000` (30 s) | Client-side `FileInfo` cache TTL. Default is 30 s (cache enabled). `0` disables the cache; any positive value controls staleness bound for out-of-band mutations. See FLAMEGRAPH_OPTIMIZATION_PLAN §A3. |
| `goosefs.user.file.info.cache.capacity` | `file_info_cache_capacity` | integer | `16384` | `FileInfo` LRU capacity when the cache is enabled. `0` is clamped to `1`. |
| `goosefs.user.short.circuit.enabled` | `short_circuit_enabled` | `true` / `false` | `false` | Master kill switch for the short-circuit local-mmap read path. **Disabled by default** since 0.1.6 (see §2.9). |
| `goosefs.client.short.circuit.cache.capacity` | `short_circuit_cache_capacity` | integer | `64` | Per-task LRU capacity for hot-block SC readers. |
| `goosefs.client.short.circuit.cache.ttl.ms` | `short_circuit_cache_ttl` | integer (milliseconds) | `30000` (30s) | Idle TTL of a cached SC reader. |
| `goosefs.client.short.circuit.neg.cache.ttl.ms` | `short_circuit_neg_cache_ttl` | integer (milliseconds) | `5000` (5s) | Negative-cache TTL. |
| `goosefs.client.short.circuit.advise` | `short_circuit_advise` | `sequential` / `random` / `normal` / `none` | `random` | L1 `madvise` readahead hint. Validated by `ShortCircuitFactory`. |
| `goosefs.client.short.circuit.prefetch.enabled` | `short_circuit_prefetch_enabled` | `true` / `false` | `true` | L2 application-level prefetch master switch. |
| `goosefs.client.short.circuit.prefetch.coalesce.gap` | `short_circuit_prefetch_coalesce_gap` | integer (bytes) | `65536` (64 KiB) | Max gap between adjacent ranges merged by `prefetch_many`. |
| `goosefs.client.short.circuit.prefetch.max.batch` | `short_circuit_prefetch_max_batch` | integer | `1024` | Upper bound on `madvise` calls per `prefetch_many`. |
| `goosefs.client.short.circuit.min.block.size` | `short_circuit_min_block_size` | integer (bytes) | `0` (no minimum) | Minimum block size required to attempt SC. |
| `goosefs.client.short.circuit.sigbus.handler` | `short_circuit_sigbus_handler` | `true` / `false` | `true` | Install a process-global SIGBUS diagnostic handler. Linux/macOS only. |
| `goosefs.client.short.circuit.thp` | `short_circuit_thp` | `true` / `false` | `false` | Request Transparent Huge Pages. Linux only, **experimental**. |

---

## 6. Operation Options

### 6.1 OpenFileOptions

Options for opening a Goosefs file for reading. Passed to `FileSystem::open_file()`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `in_stream_options` | `InStreamOptions` | See [InStreamOptions](#64-instreamoptions) | Options forwarded to the underlying file input stream. |

**Factory methods:**

| Method | Description |
|--------|-------------|
| `OpenFileOptions::default()` | Default: cache data on read. |
| `OpenFileOptions::new()` | Same as `default()`. |
| `OpenFileOptions::no_cache()` | Disable worker-side caching for this read. |

### 6.2 CreateFileOptions

Options for creating a new Goosefs file. Passed to `FileSystem::create_file()`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_type` | `WriteTypeXAttr` | `Inherit` | Write strategy. `Inherit` = look up parent directory xattr. `Explicit(wt)` = override with specified `WriteType`. |
| `block_size_bytes` | `Option<i64>` | `None` | Block size in bytes. `None` = use server/config default. |
| `replication_max` | `Option<i32>` | `None` | Replication factor. `None` = use server default. |
| `recursive` | `bool` | `false` | Whether to create intermediate directories. |

**Factory methods:**

| Method | Description |
|--------|-------------|
| `CreateFileOptions::default()` | Default: inherit write type from parent xattr. |
| `CreateFileOptions::with_write_type(wt)` | Explicit `WriteType`, bypassing xattr lookup. |

### 6.3 DeleteOptions

Options controlling how a file or directory is deleted. Passed to `FileSystem::delete()`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `recursive` | `bool` | `false` | Delete directories recursively. Required for non-empty directories. |
| `unchecked` | `bool` | `false` | Skip safety checks (empty-directory enforcement) and allow deleting INCOMPLETE files. Needed by `GoosefsFileWriter::cancel()`. |
| `goosefs_only` | `bool` | `false` | Restrict deletion to Goosefs namespace only; do not propagate to UFS. Used during CACHE_THROUGH error recovery. |

**Factory methods:**

| Method | Description |
|--------|-------------|
| `DeleteOptions::default()` | Non-recursive, checked, propagate to UFS. |
| `DeleteOptions::recursive()` | Simple recursive delete (most common case). |
| `DeleteOptions::for_cancel()` | For cancelling an in-progress file write (`unchecked = true`). |
| `DeleteOptions::goosefs_only_unchecked()` | For CACHE_THROUGH error recovery (`unchecked + goosefs_only`). |

### 6.4 InStreamOptions

Options controlling how an open file stream reads data. Used internally by
`GoosefsFileInStream`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `read_type` | `ReadType` | `Cache` | Cache strategy for this read. See [ReadType](#72-readtype). |
| `position_short` | `bool` | `false` | Hint: this is a short/random read. When `true`, the Worker skips prefetching. Set automatically by `GoosefsFileInStream` for positioned-read path. |
| `max_ufs_read_concurrency` | `i32` | `8` | Maximum concurrent UFS read threads the worker may use for this stream. |
| `prefetch_window` | `i32` | `1` | Initial prefetch window (number of chunks). `1` = no prefetch beyond current chunk. |

**Factory methods:**

| Method | Description |
|--------|-------------|
| `InStreamOptions::default()` | Cache mode, no position_short, 8 UFS concurrency, 1 prefetch. |
| `InStreamOptions::no_cache()` | No-cache read. |
| `InStreamOptions::default().positioned()` | Mark as positioned (random-access) read. |

---

## 7. Enums

### 7.1 WriteType

Controls how data is persisted when writing files.

| Variant | Proto Value (`i32`) | String Representation | Description |
|---------|--------------------|-----------------------|-------------|
| `MustCache` | `1` | `must_cache` / `MUST_CACHE` | Write to Goosefs cache only; no UFS persistence. |
| `TryCache` | `2` | `try_cache` / `TRY_CACHE` | Try to cache; fall back to `Through` if cache is full. |
| `CacheThrough` | `3` | `cache_through` / `CACHE_THROUGH` | Write to cache **and** synchronously persist to UFS. |
| `Through` | `4` | `through` / `THROUGH` | Write directly to UFS, bypassing cache. |
| `AsyncThrough` | `5` | `async_through` / `ASYNC_THROUGH` | Write to cache, asynchronously persist to UFS later. |

**String parsing** is case-insensitive. Both `snake_case` and `UPPER_SNAKE_CASE` are accepted.

**Conversions:**

```rust
use goosefs_sdk::config::WriteType;
use goosefs_sdk::WritePType;

// String → WriteType
let wt: WriteType = "cache_through".parse().unwrap();

// WriteType → String
assert_eq!(wt.to_string(), "cache_through");

// WriteType → WritePType (proto)
let pt = WritePType::from(wt);

// WritePType → WriteType
let wt2 = WriteType::from(pt);

// WriteType → i32
let i = wt.as_i32(); // 3
```

### 7.2 ReadType

Cache strategy for reading a file.

| Variant | Proto Value (`i32`) | Description |
|---------|---------------------|-------------|
| `NoCache` | `1` | Read data without caching it in workers. Use for one-off access or large scans. |
| `Cache` (default) | `2` | Read and cache data in the nearest worker. Subsequent reads served from cache. |

### 7.3 AuthType

Authentication type for gRPC connections.

| Variant | String Representation | Description |
|---------|----------------------|-------------|
| `NoSasl` | `nosasl` / `NOSASL` | No authentication — skip SASL handshake, use gRPC channel directly. |
| `Simple` (default) | `simple` / `SIMPLE` | Simple authentication — transmit username via PLAIN SASL; server does not verify password. |

**String parsing** is case-insensitive.

### 7.4 WriteTypeXAttr

Wrapper for write type inheritance in `CreateFileOptions`.

| Variant | Description |
|---------|-------------|
| `Inherit` (default) | Not set — inherit from the parent directory's `innerWriteType` xattr. |
| `Explicit(WriteType)` | Explicitly set by the caller — do not inherit from xattr. |

The xattr key is `"innerWriteType"` (`WRITE_TYPE_XATTR_KEY`). The value is the
`UPPER_SNAKE_CASE` string name of the `WriteType` enum (e.g. `"CACHE_THROUGH"`).

### 7.5 CacheEvictorType

Eviction policy for the client local page cache (`client_cache_evictor`).

| Variant | String Representation | Description |
|---------|----------------------|-------------|
| `Lfu` (default) | `LFU` | Least-Frequently-Used — evicts the page with the lowest access count. |
| `Lru` | `LRU` | Least-Recently-Used — evicts the page untouched for the longest time. |

**String parsing** is case-insensitive. Mirrors Java's
`goosefs.user.client.cache.eviction.policy`.

---

## 8. Configuration File Format

The client supports Java-style `goosefs-site.properties` files:

```properties
# Goosefs Client Configuration
# Lines starting with '#' or '!' are comments.
# Key and value are separated by '=' or ':'.

# Master connection
goosefs.master.hostname=10.0.0.1
goosefs.master.rpc.port=9200

# HA mode (overrides hostname+port above)
# goosefs.master.rpc.addresses=10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200

# Authentication
goosefs.security.authentication.type=SIMPLE
goosefs.security.login.username=myuser

# Write strategy
goosefs.user.file.writetype.default=CACHE_THROUGH

# Data transfer
goosefs.user.block.size.bytes.default=64MB
goosefs.user.network.data.transfer.chunk.size=1MB

# Transparent acceleration
goosefs.user.client.transparent_acceleration.enabled=true
goosefs.user.client.transparent_acceleration.cosranger.enabled=false

# Authorization
goosefs.security.authorization.permission.enabled=false
goosefs.security.login.impersonation.username=_HDFS_USER_

# Client local page cache (disabled by default)
# goosefs.user.client.cache.enabled=true
# goosefs.user.client.cache.page.size=1MB
# goosefs.user.client.cache.size=512MB
# goosefs.user.client.cache.dirs=/data/goosefs_cache
# goosefs.user.client.cache.eviction.policy=LRU
# goosefs.user.client.cache.async.write.enabled=true
# goosefs.user.client.cache.async.write.threads=16
# goosefs.user.client.cache.ttl.seconds=0
# goosefs.user.client.cache.sequential.read.enabled=false
# --- io_uring backend (Linux 5.1+, optional) ---
# goosefs.user.client.cache.uring.enabled=true
# goosefs.user.client.cache.uring.queue.depth=16384
# goosefs.user.client.cache.uring.thread.count=2
```

### Byte Size Format

Properties that accept byte sizes support the following suffixes (case-insensitive):

| Suffix | Multiplier | Example |
|--------|-----------|---------|
| `GB` | 1,073,741,824 | `1GB` = 1,073,741,824 bytes |
| `MB` | 1,048,576 | `64MB` = 67,108,864 bytes |
| `KB` | 1,024 | `512KB` = 524,288 bytes |
| (none) | 1 | `1048576` = 1,048,576 bytes |

---

## 9. Configuration Examples

### 9.1 Programmatic Configuration

```rust
use goosefs_sdk::config::{GoosefsConfig, WriteType};
use goosefs_sdk::auth::AuthType;

// Single master with defaults
let config = GoosefsConfig::new("127.0.0.1:9200");

// Single master with custom settings
let config = GoosefsConfig::new("10.0.0.1:9200")
    .with_auth_type(AuthType::Simple)
    .with_auth_username("myuser")
    .with_write_type_enum(WriteType::CacheThrough);

// HA mode with multiple masters
let config = GoosefsConfig::new_ha(vec![
    "10.0.0.1:9200".to_string(),
    "10.0.0.2:9200".to_string(),
    "10.0.0.3:9200".to_string(),
]);

// Auto-detect single/multi master
let config = GoosefsConfig::from_addresses(vec![
    "10.0.0.1:9200".to_string(),
]);

// Load from properties file
let config = GoosefsConfig::from_properties("/etc/goosefs/goosefs-site.properties")
    .expect("failed to load config");

// Auto-discover config file + overlay env vars
let config = GoosefsConfig::from_properties_auto()
    .expect("failed to auto-load config");

// Load from environment variables only
let config = GoosefsConfig::from_env();
```

### 9.2 Environment Variable Configuration

```bash
# Single master
export GOOSEFS_MASTER_ADDR="10.0.0.1:9200"

# HA mode
export GOOSEFS_MASTER_ADDR="10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200"

# Write type
export GOOSEFS_WRITE_TYPE="cache_through"

# Authentication
export GOOSEFS_AUTH_TYPE="simple"
export GOOSEFS_AUTH_USERNAME="myuser"

# Data transfer
export GOOSEFS_BLOCK_SIZE="67108864"
export GOOSEFS_CHUNK_SIZE="1048576"

# Explicit config file path
export GOOSEFS_CONFIG_FILE="/path/to/goosefs-site.properties"
```

### 9.3 FileSystem API with Configuration

```rust
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::{BaseFileSystem, FileSystem, OpenFileOptions, CreateFileOptions};
use goosefs_sdk::fs::options::DeleteOptions;
use goosefs_sdk::config::WriteType;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    // Build config (auto-discover properties + env vars)
    let config = GoosefsConfig::from_properties_auto()
        .unwrap_or_else(|_| GoosefsConfig::new("127.0.0.1:9200"));

    // Create shared context (one TCP+SASL handshake, reused everywhere)
    let ctx = FileSystemContext::connect(config).await?;
    let fs = BaseFileSystem::from_context(ctx);

    // Read with default options (cache enabled)
    let mut stream = fs.open_file("/data/file.parquet", OpenFileOptions::default()).await?;

    // Read without caching
    let mut stream = fs.open_file("/data/file.parquet", OpenFileOptions::no_cache()).await?;

    // Create file with explicit write type
    let opts = CreateFileOptions::with_write_type(WriteType::CacheThrough);
    let mut writer = fs.create_file("/data/output.dat", opts).await?;

    // Delete recursively
    fs.delete("/data/old_dir", DeleteOptions::recursive()).await?;

    Ok(())
}
```

### 9.4 ConfigRefresher (Hot-Reload)

> **Default Behavior**: When using `FileSystemContext::connect(config)`,
> `ConfigRefresher` is **automatically created and a background refresh task
> is started** — no manual management required. The background task checks
> every **60 seconds**; if more than **30 seconds** (`DEFAULT_CONFIG_EXPIRE_MS`)
> have elapsed since the last load, it automatically calls
> `GoosefsConfig::from_properties_auto()` to reload the config file and
> environment variables.
>
> The manual usage below is only needed when **not** using `FileSystemContext`.

```rust
use goosefs_sdk::config::{ConfigRefresher, GoosefsConfig};

// Approach 1 (recommended): automatic management via FileSystemContext
// let ctx = FileSystemContext::connect(config).await?;
// ConfigRefresher is already running in the background, refreshing every 60s

// Approach 2 (manual): only needed when not using FileSystemContext
let config = GoosefsConfig::from_properties_auto().unwrap_or_default();
let refresher = ConfigRefresher::from_config(&config);

// Manually trigger a refresh (internally checks 30s expiry; skips if not expired):
let switch = refresher.refresh_transparent_acceleration_switch();
println!("acceleration={}, cosranger={}", switch.enabled, switch.cosranger_enabled);

// Lock-free read of cached values (no disk I/O):
let switch = refresher.current_switch();
```

> **Note**: `ConfigRefresher` only refreshes the two transparent acceleration
> switch parameters (`enabled` and `cosranger_enabled`). It does **not** affect
> other user-set config fields (e.g. `master_addr`, `block_size`, `write_type`).
> The user's `GoosefsConfig` object is never modified by the refresher.
>
> **Background Task Lifecycle**: The background refresh task is automatically
> terminated when `FileSystemContext::close()` is called. If the
> `FileSystemContext` is dropped without calling `close()`, the task will also
> be terminated when the tokio runtime shuts down.

### 9.5 Client Local Page Cache

```rust
use std::sync::Arc;

use goosefs_sdk::config::{CacheEvictorType, GoosefsConfig};
use goosefs_sdk::context::FileSystemContext;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let mut config = GoosefsConfig::new("127.0.0.1:9200");

    // Enable the local page cache (off by default).
    config.client_cache_enabled = true;
    config.client_cache_page_size = 1024 * 1024;          // 1 MiB pages
    config.client_cache_size = 1024 * 1024 * 1024;        // 1 GiB per dir
    config.client_cache_dirs = vec!["/data/goosefs_cache".into()];
    config.client_cache_evictor = CacheEvictorType::Lru;  // or Lfu
    config.client_cache_async_write_enabled = true;       // async back-fill
    config.client_cache_ttl_secs = 0;                     // 0 = no expiry
    config.client_cache_sequential_read_enabled = false;  // sequential reads bypass the cache by default

    // io_uring backend (Linux 5.1+ only; falls back to tokio::fs on other platforms).
    // Defaults are shown explicitly — `true` on Linux / `false` elsewhere.
    config.client_cache_uring_enabled = cfg!(target_os = "linux");
    config.client_cache_uring_queue_depth = 16384;        // SQ/CQ depth
    config.client_cache_uring_thread_count = 2;           // background uring threads

    // The cache is initialized inside connect() and shared by all readers.
    let ctx: Arc<FileSystemContext> = FileSystemContext::connect(config).await?;
    // ... reads via GoosefsFileReader / GoosefsFileInStream transparently
    //     consult and fill the cache ...
    ctx.close().await?;
    Ok(())
}
```

Equivalent via properties / environment variables:

```bash
export GOOSEFS_USER_CLIENT_CACHE_ENABLED=true
export GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE=1048576
export GOOSEFS_USER_CLIENT_CACHE_SIZE=1073741824
export GOOSEFS_USER_CLIENT_CACHE_DIRS=/data/goosefs_cache
export GOOSEFS_USER_CLIENT_CACHE_EVICTION_POLICY=LRU
# io_uring backend (Linux 5.1+; optional — defaults are sensible)
export GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED=true
export GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH=16384
export GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT=2
```

> See [`docs/CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md)
> for the full design and [`examples/page_cache_demo.rs`](../examples/page_cache_demo.rs)
> for a runnable cold-miss → warm-hit demonstration.

### 9.6 Performance Tuning (Connection Pools & Streaming Read)

These knobs target high-concurrency / high-RTT (remote-cluster) workloads. They
default to backward-compatible values; raise them only when a benchmark shows a
bottleneck (see [`docs/RUST_PYTHON_SDK_OPTIMIZATION.md`](RUST_PYTHON_SDK_OPTIMIZATION.md)
Part V and [`docs/FLAMEGRAPH_OPTIMIZATION_PLAN.md`](FLAMEGRAPH_OPTIMIZATION_PLAN.md)
§A3 / §B3).

#### Programmatic

```rust
use std::time::Duration;
use goosefs_sdk::config::GoosefsConfig;

let config = GoosefsConfig::new("10.0.0.1:9200")
    // Master metadata path: pool channels to avoid HTTP/2 stream queueing
    // under high concurrency over remote RTT (Part V R3).
    .with_master_connection_pool_size(8)
    // Worker IO path: pool channels per worker to lift per-connection
    // throughput cap (Part V R4 / FLAMEGRAPH_OPTIMIZATION_PLAN §B3).
    .with_worker_connection_pool_size(4)
    // Client-side FileInfo cache: amortise per-open MasterClient::get_status
    // when the same file is opened multiple times inside one query
    // (FLAMEGRAPH_OPTIMIZATION_PLAN §A3). Opt-in: 0 = disabled (default).
    .with_file_info_cache_ttl(Duration::from_millis(2000))
    .with_file_info_cache_capacity(8192)
    // Sequential-read throughput: widen the prefetch window (Part V R1-B-a)…
    .with_prefetch_window(16)
    // …and coalesce flow-control ACKs (only on workers that honour the
    // prefetch window) to cut ACK round-trips (Part V R1-B-c).
    .with_ack_interval_bytes(8 * 1024 * 1024);
```

#### Environment variables

The three FLAMEGRAPH_OPTIMIZATION_PLAN §A3 / §B3 knobs are also exposed via
env vars (picked up by `GoosefsConfig::from_env()` /
`GoosefsConfig::from_properties_auto()`):

```bash
export GOOSEFS_WORKER_CONNECTION_POOL_SIZE=8
export GOOSEFS_FILE_INFO_CACHE_TTL_MS=2000
export GOOSEFS_FILE_INFO_CACHE_CAPACITY=8192
```

#### Properties file

Or the equivalent lines in `goosefs-site.properties`:

```properties
goosefs.user.worker.connection.pool.size=8
goosefs.user.file.info.cache.ttl.ms=2000
goosefs.user.file.info.cache.capacity=8192
```

#### Storage options (Lance / OpenDAL)

The SDK exposes canonical `STORAGE_OPT_*` string constants; the integrating
layer (`opendal_service_goosefs`) maps them to the corresponding builder
methods:

```python
ds = lance.dataset(
    "gfs://…",
    storage_options={
        "goosefs_worker_connection_pool_size": "8",
        "goosefs_file_info_cache_ttl_ms": "2000",
        "goosefs_file_info_cache_capacity": "8192",
    },
)
```

#### Summary table

| Knob | Raise for | Default | Typical value | Env var | Properties key | Storage option |
|------|-----------|---------|---------------|---------|----------------|----------------|
| `master_connection_pool_size` | High-concurrency metadata RPCs over remote RTT | `1` | `4`–`8` | *(programmatic only)* | *(programmatic only)* | *(programmatic only)* |
| `worker_connection_pool_size` | Single-process high-throughput block reads | `min(cores, 4)` | `4`–`8` | `GOOSEFS_WORKER_CONNECTION_POOL_SIZE` | `goosefs.user.worker.connection.pool.size` | `goosefs_worker_connection_pool_size` |
| `file_info_cache_ttl` | Repeated opens of the same file inside one query | `30000` (30 s) | `1s`–`5s` | `GOOSEFS_FILE_INFO_CACHE_TTL_MS` | `goosefs.user.file.info.cache.ttl.ms` | `goosefs_file_info_cache_ttl_ms` |
| `file_info_cache_capacity` | Wide fan-out workloads (many distinct paths) | `16384` | `16384`–`32768` | `GOOSEFS_FILE_INFO_CACHE_CAPACITY` | `goosefs.user.file.info.cache.capacity` | `goosefs_file_info_cache_capacity` |
| `prefetch_window` | Sequential (SR) read throughput | `8` | `16` | *(programmatic only)* | *(programmatic only)* | *(programmatic only)* |
| `ack_interval_bytes` | SR throughput, **only** on workers honouring prefetch | `0` (ACK every chunk) | `4MB`–`8MB` | *(programmatic only)* | *(programmatic only)* | *(programmatic only)* |
| `short_circuit_enabled` | Kill switch for the local mmap read path (see §2.9) | `false` | `false` (default; safe for Lance/DuckDB); `true` to opt into the local mmap fast path on co-located workloads that benefit | `GOOSEFS_SHORT_CIRCUIT_ENABLED` | `goosefs.user.short.circuit.enabled` | `goosefs_short_circuit_enabled` |

### 9.7 Short-Circuit (Local mmap) Reads

The short-circuit (SC) path is **on by default** whenever a co-located worker
is discovered — no configuration required. The knobs below only matter when
you want to (a) turn SC off for A/B comparison against the gRPC data plane,
or (b) tune SC for a specific workload profile (scan-heavy vs random /
positioned reads). See §2.9 for the semantics of each field and
[`docs/SHORT_CIRCUIT_DESIGN.md`](SHORT_CIRCUIT_DESIGN.md) for the design.

#### Programmatic

```rust
use std::time::Duration;
use goosefs_sdk::config::GoosefsConfig;

// A/B: force every read through the gRPC data plane (bypasses SC even on a
// local worker). Handy for isolating SC-specific regressions.
let ab_config = GoosefsConfig::new("127.0.0.1:9200")
    .with_short_circuit_enabled(false);

// Scan-heavy Parquet / Arrow tuning: sequential madvise hint, larger hot-block
// LRU with a longer idle TTL, skip SC below 4 MiB blocks.
let scan_config = GoosefsConfig::new("127.0.0.1:9200")
    .with_short_circuit_enabled(true)
    .with_short_circuit_advise("sequential")
    .with_short_circuit_cache_capacity(256)
    .with_short_circuit_cache_ttl(Duration::from_secs(60))
    .with_short_circuit_min_block_size(4 * 1024 * 1024);

// Point-lookup / positioned-read tuning (SC's default sweet spot): keep the
// `random` hint but shorten the negative cache so a briefly non-SC-eligible
// block gets retried quickly.
let point_config = GoosefsConfig::new("127.0.0.1:9200")
    .with_short_circuit_advise("random")
    .with_short_circuit_neg_cache_ttl(Duration::from_secs(1));

// Experimental Linux-only knobs: request Transparent Huge Pages, disable the
// process-global SIGBUS handler (only if the host process installs its own).
let advanced_config = GoosefsConfig::new("127.0.0.1:9200")
    .with_short_circuit_thp(true)
    .with_short_circuit_sigbus_handler(false);
```

#### Environment variables

```bash
# Kill switch — set to `false` for A/B comparison vs the gRPC data plane.
export GOOSEFS_SHORT_CIRCUIT_ENABLED=true

# Scan-heavy tuning.
export GOOSEFS_SHORT_CIRCUIT_ADVISE=sequential
export GOOSEFS_SHORT_CIRCUIT_CACHE_CAPACITY=256
export GOOSEFS_SHORT_CIRCUIT_CACHE_TTL_MS=60000
export GOOSEFS_SHORT_CIRCUIT_MIN_BLOCK_SIZE=4194304

# Prefetch tuning.
export GOOSEFS_SHORT_CIRCUIT_PREFETCH_ENABLED=true
export GOOSEFS_SHORT_CIRCUIT_PREFETCH_COALESCE_GAP=131072   # 128 KiB
export GOOSEFS_SHORT_CIRCUIT_PREFETCH_MAX_BATCH=2048

# Reliability / negative cache.
export GOOSEFS_SHORT_CIRCUIT_NEG_CACHE_TTL_MS=1000

# Experimental (Linux only).
export GOOSEFS_SHORT_CIRCUIT_THP=false
export GOOSEFS_SHORT_CIRCUIT_SIGBUS_HANDLER=true
```

#### Properties file

Equivalent lines in `goosefs-site.properties` (note the key prefix split:
`goosefs.user.short.circuit.enabled` mirrors the Java API surface for the
kill switch; the other 10 keys are Rust-SDK-specific and use the
`goosefs.client.short.circuit.*` prefix):

```properties
# Kill switch (Java-compatible key).
goosefs.user.short.circuit.enabled=true

# Reader LRU & TTLs.
goosefs.client.short.circuit.cache.capacity=256
goosefs.client.short.circuit.cache.ttl.ms=60000
goosefs.client.short.circuit.neg.cache.ttl.ms=1000

# madvise hint + block-size gate.
goosefs.client.short.circuit.advise=sequential
goosefs.client.short.circuit.min.block.size=4194304

# Prefetch tuning.
goosefs.client.short.circuit.prefetch.enabled=true
goosefs.client.short.circuit.prefetch.coalesce.gap=131072
goosefs.client.short.circuit.prefetch.max.batch=2048

# Experimental (Linux only).
goosefs.client.short.circuit.sigbus.handler=true
goosefs.client.short.circuit.thp=false
```

#### Storage options (Lance / OpenDAL)

Every SC knob is exposed as a `goosefs_short_circuit_*` storage option; the
integrating layer (`opendal_service_goosefs`) forwards each key to the
corresponding `with_short_circuit_*` builder method. All values are passed
as **strings** — integers included.

```python
import lance

ds = lance.dataset(
    "gfs://…",
    storage_options={
        # Kill switch: turn SC off to isolate the gRPC data path.
        "goosefs_short_circuit_enabled": "false",

        # …or keep SC on and tune it (uncomment as needed):
        # "goosefs_short_circuit_advise": "sequential",
        # "goosefs_short_circuit_cache_capacity": "256",
        # "goosefs_short_circuit_cache_ttl_ms": "60000",
        # "goosefs_short_circuit_min_block_size": "4194304",
        # "goosefs_short_circuit_prefetch_coalesce_gap": "131072",
        # "goosefs_short_circuit_prefetch_max_batch": "2048",
    },
)
```

#### Verifying that SC actually engaged

After the process has served some reads, inspect the `Client.ShortCircuit*`
counters exposed by [`src/metrics/registry.rs`](../src/metrics/registry.rs)
§7.3 — an increasing byte counter proves SC was on the read path. If those
counters stay at zero even though `short_circuit_enabled = true`, the block
was routed to a **remote** worker (SC is physically impossible cross-host);
see [`docs/PAGE_CACHE_VS_SHORT_CIRCUIT.md`](PAGE_CACHE_VS_SHORT_CIRCUIT.md)
for the interaction with the client-side page cache. The gating-grade
regression suites [`tests/sc_consistency.rs`](../tests/sc_consistency.rs)
and [`tests/short_circuit_e2e.rs`](../tests/short_circuit_e2e.rs) show how
to assert on these counters programmatically.
