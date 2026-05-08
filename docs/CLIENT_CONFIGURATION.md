# Goosefs Rust Client — Configuration Parameter Reference

> **Version**: 0.2.0 | **Date**: 2026-04-24

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
   - [Miscellaneous Settings](#28-miscellaneous-settings)
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

### 2.2 Data Transfer Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `block_size` | `u64` | `67108864` (64 MiB) | Default block size in bytes for new files. Matches Goosefs server default. |
| `chunk_size` | `u64` | `1048576` (1 MiB) | Chunk size for streaming read/write RPCs. Each gRPC message carries one chunk. |
| `write_type` | `Option<i32>` | `None` | Default write type for newly created files. `None` = use server default (typically `MustCache`). See [WriteType](#71-writetype) for values. |

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

### 2.8 Miscellaneous Settings

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
| `GOOSEFS_MASTER_ADDR` | `master_addr` / `master_addrs` | Master address(es). Comma-separated for HA: `"addr1:port,addr2:port"`. |
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
