# Goosefs Rust Client — Configuration Parameter Reference

> **Version**: 0.2.0 | **Date**: 2026-06-16

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
   - [Miscellaneous Settings](#29-miscellaneous-settings)
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
| `worker_connection_pool_size` | `usize` | `1` | Number of independent gRPC channels to pool **per worker**. `1` = legacy single-channel-per-worker. Raising it (e.g. `4`) round-robins block reads across multiple HTTP/2 connections to the same worker, lifting the per-connection throughput cap. Each channel does its own SASL handshake. **Set programmatically** via `with_worker_connection_pool_size()`. (Optimization doc Part V R4.) |

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

> **Performance tuning knobs** (`master_connection_pool_size`,
> `worker_connection_pool_size`, `prefetch_window`, `read_buffer_messages`,
> `ack_interval_bytes`, `ack_interval_chunks`) are currently **set
> programmatically only** via `GoosefsConfig` builder methods — they have no
> environment-variable, properties-file, or storage-option entry points. See
> [`docs/RUST_PYTHON_SDK_OPTIMIZATION.md`](RUST_PYTHON_SDK_OPTIMIZATION.md)
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
| `client_cache_size` | `u64` | `536870912` (512 MiB) | Per-directory capacity in bytes. ~5% is reserved for filesystem/metadata overhead. |
| `client_cache_dirs` | `Vec<String>` | `["/tmp/goosefs_cache"]` | Cache directories. Multiple dirs spread pages by file affinity (`HashAllocator`). |
| `client_cache_evictor` | `CacheEvictorType` | `Lru` | Eviction policy when a directory is full. See [CacheEvictorType](#75-cacheevictortype). |
| `client_cache_async_write_enabled` | `bool` | `true` | Whether missed pages are back-filled asynchronously (bounded write-back pool). `false` = fill inline before the read returns. |
| `client_cache_async_write_threads` | `usize` | `16` | Async write-back concurrency (permits). Excess fills are dropped (`CachePutAsyncRejectionErrors`). |
| `client_cache_quota_enabled` | `bool` | `false` | Whether per-scope quota accounting is enabled (currently treated as Global). |
| `client_cache_ttl_secs` | `u64` | `0` | Page time-to-live in seconds. `0` = no expiry. Expired pages are dropped lazily on `get` and by a background sweeper. |
| `client_cache_sequential_read_enabled` | `bool` | `false` | Whether **sequential** reads (`read`) are routed through the cache. Random reads (`read_at`) always consult the cache when enabled. Off by default: routing large sequential scans through fixed-size pages turns one streamed request into many per-page positioned reads (read amplification), and a `NoCache` sequential read would re-fetch a whole page per small buffer with no caching benefit. Enable only when sequential reads are expected to be re-read. |

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

### 2.9 Miscellaneous Settings

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

| Environment Variable | GoosefsConfig Field | Description |
|---------------------|---------------------|-------------|
| `GOOSEFS_MASTER_ADDR` | `master_addr` / `master_addrs` | Master address(es). Three accepted forms: single `host:port`; comma-separated list `addr1:port,addr2:port` for HA; or a Hadoop-style URI `gfs://addr1:port,addr2:port/root-path` (URI form also seeds `root`). |
| `GOOSEFS_WRITE_TYPE` | `write_type` | Default write type. Accepted: `must_cache`, `try_cache`, `cache_through`, `through`, `async_through` (case-insensitive). |
| `GOOSEFS_BLOCK_SIZE` | `block_size` | Block size in bytes (plain integer). |
| `GOOSEFS_CHUNK_SIZE` | `chunk_size` | Chunk size in bytes (plain integer). |
| `GOOSEFS_AUTH_TYPE` | `auth_type` | Authentication type. Accepted: `nosasl`, `simple` (case-insensitive). |
| `GOOSEFS_AUTH_USERNAME` | `auth_username` | Authentication username. |
| `GOOSEFS_CONFIG_FILE` | — | Explicit path to a config file (Rust-only convenience, highest priority). |
| `GOOSEFS_CONF_DIR` | — | Goosefs configuration directory (mirrors Java `goosefs.conf.dir`). |
| `GOOSEFS_HOME` | — | Goosefs installation home directory. |
| `GOOSEFS_CONFIG_MANAGER_RPC_ADDRESSES` | `config_manager_rpc_addresses` | Config manager RPC addresses (comma-separated). |
| `GOOSEFS_CONFIG_RPC_PORT` | `config_rpc_port` | Config manager RPC port. |
| `GOOSEFS_TRANSPARENT_ACCELERATION_ENABLED` | `transparent_acceleration_enabled` | Transparent acceleration enabled (`true`/`false`). |
| `GOOSEFS_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED` | `transparent_acceleration_cosranger_enabled` | Transparent acceleration cosranger enabled (`true`/`false`). |
| `GOOSEFS_AUTHORIZATION_PERMISSION_ENABLED` | `authorization_permission_enabled` | Authorization permission enabled (`true`/`false`). |
| `GOOSEFS_LOGIN_IMPERSONATION_USERNAME` | `login_impersonation_username` | Login impersonation username. |
| `GOOSEFS_USER_CLIENT_CACHE_ENABLED` | `client_cache_enabled` | Enable the local page cache (`true`/`false`). |
| `GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE` | `client_cache_page_size` | Page size in bytes (plain integer). |
| `GOOSEFS_USER_CLIENT_CACHE_SIZE` | `client_cache_size` | Per-directory capacity in bytes (plain integer). |
| `GOOSEFS_USER_CLIENT_CACHE_DIRS` | `client_cache_dirs` | Cache directories (comma-separated). |
| `GOOSEFS_USER_CLIENT_CACHE_EVICTION_POLICY` | `client_cache_evictor` | Eviction policy: `LRU` / `LFU` (case-insensitive). |
| `GOOSEFS_USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED` | `client_cache_async_write_enabled` | Async back-fill enabled (`true`/`false`). |
| `GOOSEFS_USER_CLIENT_CACHE_ASYNC_WRITE_THREADS` | `client_cache_async_write_threads` | Async write-back concurrency (plain integer). |
| `GOOSEFS_USER_CLIENT_CACHE_QUOTA_ENABLED` | `client_cache_quota_enabled` | Quota accounting enabled (`true`/`false`). |
| `GOOSEFS_USER_CLIENT_CACHE_TTL_SECONDS` | `client_cache_ttl_secs` | Page TTL in seconds (`0` = no expiry). |
| `GOOSEFS_USER_CLIENT_CACHE_SEQUENTIAL_READ_ENABLED` | `client_cache_sequential_read_enabled` | Route sequential reads through the cache (`true`/`false`). |

---

## 4. Storage Option Keys

These constants are used in `storage_options` maps (e.g. Lance's
`DatasetBuilder::with_storage_option` or OpenDAL config).

| Constant | Key String | Description |
|----------|-----------|-------------|
| `STORAGE_OPT_MASTER_ADDR` | `goosefs_master_addr` | Master address(es). Supports HA: `"addr1:port,addr2:port"`. |
| `STORAGE_OPT_WRITE_TYPE` | `goosefs_write_type` | Default write type (case-insensitive). |
| `STORAGE_OPT_BLOCK_SIZE` | `goosefs_block_size` | Block size in bytes. |
| `STORAGE_OPT_CHUNK_SIZE` | `goosefs_chunk_size` | Chunk size in bytes. |
| `STORAGE_OPT_AUTH_TYPE` | `goosefs_auth_type` | Authentication type (case-insensitive). |
| `STORAGE_OPT_AUTH_USERNAME` | `goosefs_auth_username` | Authentication username. |
| `STORAGE_OPT_CONFIG_MANAGER_RPC_ADDRESSES` | `goosefs_config_manager_rpc_addresses` | Config manager RPC addresses. |
| `STORAGE_OPT_CONFIG_RPC_PORT` | `goosefs_config_rpc_port` | Config manager RPC port. |
| `STORAGE_OPT_TRANSPARENT_ACCELERATION_ENABLED` | `goosefs_transparent_acceleration_enabled` | Transparent acceleration enabled. |
| `STORAGE_OPT_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED` | `goosefs_transparent_acceleration_cosranger_enabled` | Transparent acceleration cosranger enabled. |
| `STORAGE_OPT_AUTHORIZATION_PERMISSION_ENABLED` | `goosefs_authorization_permission_enabled` | Authorization permission enabled. |
| `STORAGE_OPT_LOGIN_IMPERSONATION_USERNAME` | `goosefs_login_impersonation_username` | Login impersonation username. |
| `STORAGE_OPT_CLIENT_CACHE_ENABLED` | `goosefs_client_cache_enabled` | Enable the local page cache. |
| `STORAGE_OPT_CLIENT_CACHE_PAGE_SIZE` | `goosefs_client_cache_page_size` | Page size in bytes. |
| `STORAGE_OPT_CLIENT_CACHE_SIZE` | `goosefs_client_cache_size` | Per-directory capacity in bytes. |
| `STORAGE_OPT_CLIENT_CACHE_DIRS` | `goosefs_client_cache_dirs` | Cache directories (comma-separated). |
| `STORAGE_OPT_CLIENT_CACHE_EVICTOR` | `goosefs_client_cache_eviction_policy` | Eviction policy (`LRU`/`LFU`). |

> For storage-option deployments, the async-write / quota / TTL / sequential-read
> knobs are not exposed as dedicated `goosefs_*` keys; set them via properties
> or environment variables (§3, §5) instead.

---

## 5. Properties File Keys

These keys are used in `goosefs-site.properties` files (Java-style `key=value` format).

| Properties Key | GoosefsConfig Field | Value Format | Description |
|---------------|---------------------|--------------|-------------|
| `goosefs.master.hostname` | `master_addr` (host part) | hostname/IP | Master hostname. Combined with `goosefs.master.rpc.port` to form `master_addr`. |
| `goosefs.master.rpc.port` | `master_addr` (port part) | integer | Master RPC port. Default: `9200`. |
| `goosefs.master.rpc.addresses` | `master_addr` + `master_addrs` | comma-separated `host:port` | HA master addresses. First address becomes `master_addr`. |
| `goosefs.config.manager.rpc.addresses` | `config_manager_rpc_addresses` | comma-separated `host:port` | Config manager RPC addresses. |
| `goosefs.config.rpc.port` | `config_rpc_port` | integer | Config manager RPC port. |
| `goosefs.security.authentication.type` | `auth_type` | `NOSASL` / `SIMPLE` | Authentication type. |
| `goosefs.security.login.username` | `auth_username` | string | Login username. |
| `goosefs.security.authorization.permission.enabled` | `authorization_permission_enabled` | `true` / `false` | Permission-based access control. |
| `goosefs.security.login.impersonation.username` | `login_impersonation_username` | string | Impersonation username. |
| `goosefs.user.file.writetype.default` | `write_type` | `MUST_CACHE` / `TRY_CACHE` / `CACHE_THROUGH` / `THROUGH` / `ASYNC_THROUGH` | Default write type. |
| `goosefs.user.block.size.bytes.default` | `block_size` | byte size (e.g. `64MB`, `512KB`, `134217728`) | Default block size. Supports `KB`/`MB`/`GB` suffixes. |
| `goosefs.user.network.data.transfer.chunk.size` | `chunk_size` | byte size (e.g. `1MB`, `512KB`) | Streaming chunk size. Supports `KB`/`MB`/`GB` suffixes. |
| `goosefs.user.client.transparent_acceleration.enabled` | `transparent_acceleration_enabled` | `true` / `false` | Transparent acceleration. |
| `goosefs.user.client.transparent_acceleration.cosranger.enabled` | `transparent_acceleration_cosranger_enabled` | `true` / `false` | Transparent acceleration cosranger. |
| `goosefs.user.client.cache.enabled` | `client_cache_enabled` | `true` / `false` | Enable the local page cache. |
| `goosefs.user.client.cache.page.size` | `client_cache_page_size` | byte size (e.g. `1MB`) | Page size. Supports `KB`/`MB`/`GB` suffixes. |
| `goosefs.user.client.cache.size` | `client_cache_size` | byte size (e.g. `512MB`) | Per-directory capacity. Supports `KB`/`MB`/`GB` suffixes. |
| `goosefs.user.client.cache.dirs` | `client_cache_dirs` | comma-separated paths | Cache directories. |
| `goosefs.user.client.cache.eviction.policy` | `client_cache_evictor` | `LRU` / `LFU` | Eviction policy. |
| `goosefs.user.client.cache.async.write.enabled` | `client_cache_async_write_enabled` | `true` / `false` | Async back-fill. |
| `goosefs.user.client.cache.async.write.threads` | `client_cache_async_write_threads` | integer | Async write-back concurrency. |
| `goosefs.user.client.cache.quota.enabled` | `client_cache_quota_enabled` | `true` / `false` | Quota accounting. |
| `goosefs.user.client.cache.ttl.seconds` | `client_cache_ttl_secs` | integer (seconds) | Page TTL. `0` = no expiry. |
| `goosefs.user.client.cache.sequential.read.enabled` | `client_cache_sequential_read_enabled` | `true` / `false` | Route sequential reads through the cache (off by default). |

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
| `Lru` (default) | `LRU` | Least-Recently-Used — evicts the page untouched for the longest time. |
| `Lfu` | `LFU` | Least-Frequently-Used — evicts the page with the lowest access count. |

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
    config.client_cache_size = 512 * 1024 * 1024;         // 512 MiB per dir
    config.client_cache_dirs = vec!["/data/goosefs_cache".into()];
    config.client_cache_evictor = CacheEvictorType::Lru;  // or Lfu
    config.client_cache_async_write_enabled = true;       // async back-fill
    config.client_cache_ttl_secs = 0;                     // 0 = no expiry
    config.client_cache_sequential_read_enabled = false;  // sequential reads bypass the cache by default

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
export GOOSEFS_USER_CLIENT_CACHE_SIZE=536870912
export GOOSEFS_USER_CLIENT_CACHE_DIRS=/data/goosefs_cache
export GOOSEFS_USER_CLIENT_CACHE_EVICTION_POLICY=LRU
```

> See [`docs/CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md)
> for the full design and [`examples/page_cache_demo.rs`](../examples/page_cache_demo.rs)
> for a runnable cold-miss → warm-hit demonstration.

### 9.6 Performance Tuning (Connection Pools & Streaming Read)

These knobs target high-concurrency / high-RTT (remote-cluster) workloads. They
default to backward-compatible values; raise them only when a benchmark shows a
bottleneck (see [`docs/RUST_PYTHON_SDK_OPTIMIZATION.md`](RUST_PYTHON_SDK_OPTIMIZATION.md)
Part V). They are set programmatically (no env/properties keys).

```rust
use goosefs_sdk::config::GoosefsConfig;

let config = GoosefsConfig::new("10.0.0.1:9200")
    // Master metadata path: pool channels to avoid HTTP/2 stream queueing
    // under high concurrency over remote RTT (Part V R3).
    .with_master_connection_pool_size(8)
    // Worker IO path: pool channels per worker to lift per-connection
    // throughput cap (Part V R4).
    .with_worker_connection_pool_size(4)
    // Sequential-read throughput: widen the prefetch window (Part V R1-B-a)…
    .with_prefetch_window(16)
    // …and coalesce flow-control ACKs (only on workers that honour the
    // prefetch window) to cut ACK round-trips (Part V R1-B-c).
    .with_ack_interval_bytes(8 * 1024 * 1024);
```

| Knob | Raise for | Typical value |
|------|-----------|---------------|
| `master_connection_pool_size` | High-concurrency metadata RPCs over remote RTT | `4`–`8` |
| `worker_connection_pool_size` | Single-process high-throughput block reads | `4` |
| `prefetch_window` | Sequential (SR) read throughput | `16` |
| `ack_interval_bytes` | SR throughput, **only** on workers honouring prefetch | `4MB`–`8MB` |
