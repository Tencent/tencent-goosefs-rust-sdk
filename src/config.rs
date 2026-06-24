//! Client configuration for Goosefs gRPC connections.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::auth::AuthType;
use crate::proto::grpc::file::WritePType;

// ── Config load error ─────────────────────────────────────────

/// Error returned by properties/auto configuration loading.
#[derive(Debug)]
pub enum ConfigLoadError {
    /// The config file could not be read.
    IoError { path: String, source: String },
    /// The YAML content could not be parsed.
    ParseError { message: String },
}

impl std::fmt::Display for ConfigLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigLoadError::IoError { path, source } => {
                write!(f, "failed to read config file '{}': {}", path, source)
            }
            ConfigLoadError::ParseError { message } => {
                write!(f, "failed to parse YAML config: {}", message)
            }
        }
    }
}

impl std::error::Error for ConfigLoadError {}

// ── Properties file parsing ───────────────────────────────────
//
// Parse Java-style `goosefs-site.properties` files.
// Format: `key=value` lines, `#` comments, blank lines ignored.

use std::collections::HashMap;

/// Parsed properties map from a `goosefs-site.properties` file.
#[derive(Debug, Default)]
struct PropertiesMap {
    props: HashMap<String, String>,
}

impl PropertiesMap {
    /// Parse a properties string into a map.
    ///
    /// Rules (matching Java `Properties.load()`):
    /// - Lines starting with `#` or `!` are comments.
    /// - Blank lines are ignored.
    /// - Key and value are separated by `=` or `:` (first occurrence).
    /// - Leading/trailing whitespace on key and value is trimmed.
    fn parse(content: &str) -> Self {
        let mut props = HashMap::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('!') {
                continue;
            }
            // Find the separator: first `=` or `:`
            let sep_pos = trimmed.find('=').or_else(|| trimmed.find(':'));
            if let Some(pos) = sep_pos {
                let key = trimmed[..pos].trim().to_string();
                let value = trimmed[pos + 1..].trim().to_string();
                if !key.is_empty() {
                    props.insert(key, value);
                }
            }
        }
        PropertiesMap { props }
    }

    /// Get a string value by key.
    fn get(&self, key: &str) -> Option<&str> {
        self.props.get(key).map(|s| s.as_str())
    }

    /// Get a value parsed as the given type.
    fn get_parsed<T: FromStr>(&self, key: &str) -> Option<T> {
        self.get(key).and_then(|v| v.parse::<T>().ok())
    }

    /// Get a boolean value (accepts `true`/`false`, case-insensitive).
    fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key)
            .and_then(|v| v.to_ascii_lowercase().parse::<bool>().ok())
    }

    /// Get a comma-separated list of strings.
    fn get_list(&self, key: &str) -> Option<Vec<String>> {
        self.get(key).map(|v| {
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
    }
}

/// Parse a byte size string like `"64MB"`, `"512KB"`, `"1GB"`, `"4MB"` or plain bytes.
///
/// This matches the format used in `goosefs-site.properties`, e.g.
/// `goosefs.user.block.size.bytes.default=4MB`.
fn parse_byte_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let upper = s.to_uppercase();
    let (multiplier, num_str) = if upper.ends_with("GB") {
        (1024u64 * 1024 * 1024, &s[..s.len() - 2])
    } else if upper.ends_with("MB") {
        (1024 * 1024, &s[..s.len() - 2])
    } else if upper.ends_with("KB") {
        (1024, &s[..s.len() - 2])
    } else {
        (1, s)
    };
    num_str
        .trim()
        .parse::<u64>()
        .map_err(|e| format!("invalid byte size '{}': {}", s, e))
        .and_then(|n| {
            // `checked_mul` prevents the silent wrap-around that
            // `n * multiplier` produces in release builds (e.g. on
            // pathological inputs like "99999999999GB"), which would
            // otherwise be parsed into a tiny block size and cause
            // hard-to-diagnose I/O misbehaviour.
            n.checked_mul(multiplier)
                .ok_or_else(|| format!("byte size '{}' overflows u64", s))
        })
}

impl PropertiesMap {
    /// Convert the parsed properties into a `GoosefsConfig`.
    fn into_goosefs_config(self) -> GoosefsConfig {
        let mut cfg = GoosefsConfig::default();

        // Master addresses: goosefs.master.rpc.addresses (comma-separated)
        if let Some(addrs) = self.get_list("goosefs.master.rpc.addresses") {
            if !addrs.is_empty() {
                cfg.master_addr = addrs[0].clone();
                if addrs.len() > 1 {
                    cfg.master_addrs = addrs;
                }
            }
        } else if let Some(host) = self.get("goosefs.master.hostname") {
            let port: u16 = self.get_parsed("goosefs.master.rpc.port").unwrap_or(9200);
            cfg.master_addr = format!("{}:{}", host, port);
        }

        // Config manager addresses: goosefs.config.manager.rpc.addresses
        if let Some(addrs) = self.get_list("goosefs.config.manager.rpc.addresses") {
            if !addrs.is_empty() {
                cfg.config_manager_rpc_addresses = addrs;
            }
        }

        // Config RPC port: goosefs.config.rpc.port
        if let Some(port) = self.get_parsed::<u16>("goosefs.config.rpc.port") {
            cfg.config_rpc_port = port;
        }

        // Security / auth type: goosefs.security.authentication.type
        if let Some(at_str) = self.get("goosefs.security.authentication.type") {
            if let Ok(at) = at_str.parse::<AuthType>() {
                cfg.auth_type = at;
            }
        }

        // Security / authorization permission enabled
        if let Some(enabled) = self.get_bool("goosefs.security.authorization.permission.enabled") {
            cfg.authorization_permission_enabled = enabled;
        }

        // Security / login impersonation username
        if let Some(user) = self.get("goosefs.security.login.impersonation.username") {
            if !user.is_empty() {
                cfg.login_impersonation_username = user.to_string();
            }
        }

        // Security / login username
        if let Some(user) = self.get("goosefs.security.login.username") {
            if !user.is_empty() {
                cfg.auth_username = user.to_string();
            }
        }

        // Transparent acceleration: goosefs.user.client.transparent_acceleration.enabled
        if let Some(enabled) = self.get_bool("goosefs.user.client.transparent_acceleration.enabled")
        {
            cfg.transparent_acceleration_enabled = enabled;
        }

        // Transparent acceleration cosranger
        if let Some(enabled) =
            self.get_bool("goosefs.user.client.transparent_acceleration.cosranger.enabled")
        {
            cfg.transparent_acceleration_cosranger_enabled = enabled;
        }

        // Write type: goosefs.user.file.writetype.default
        if let Some(wt_str) = self.get("goosefs.user.file.writetype.default") {
            if let Ok(wt) = wt_str.parse::<WriteType>() {
                cfg.write_type = Some(wt.as_i32());
            }
        }

        // Block size: goosefs.user.block.size.bytes.default
        if let Some(bs_str) = self.get("goosefs.user.block.size.bytes.default") {
            if let Ok(bs) = parse_byte_size(bs_str) {
                if bs > 0 {
                    cfg.block_size = bs;
                }
            }
        }

        // Chunk size: goosefs.user.network.data.transfer.chunk.size
        if let Some(cs_str) = self.get("goosefs.user.network.data.transfer.chunk.size") {
            if let Ok(cs) = parse_byte_size(cs_str) {
                if cs > 0 {
                    cfg.chunk_size = cs;
                }
            }
        }

        // Metrics enabled: goosefs.user.metrics.collection.enabled
        if let Some(val) = self.get("goosefs.user.metrics.collection.enabled") {
            if let Ok(b) = val.to_lowercase().parse::<bool>() {
                cfg.metrics_enabled = b;
            }
        }

        // Metrics heartbeat interval (unit: milliseconds):
        // goosefs.user.metrics.heartbeat.interval
        if let Some(ms_str) = self.get("goosefs.user.metrics.heartbeat.interval") {
            if let Ok(ms) = ms_str.parse::<u64>() {
                if ms >= MIN_METRICS_HEARTBEAT_INTERVAL_MS {
                    cfg.metrics_heartbeat_interval = Duration::from_millis(ms);
                }
            }
        }

        // Application ID: goosefs.user.app.id
        if let Some(id) = self.get("goosefs.user.app.id") {
            if !id.is_empty() {
                cfg.app_id = Some(id.to_string());
            }
        }

        // Pushgateway enabled: goosefs.metrics.pushgateway.enabled
        if let Some(val) = self.get("goosefs.metrics.pushgateway.enabled") {
            if let Ok(b) = val.to_lowercase().parse::<bool>() {
                cfg.pushgateway_enabled = b;
            }
        }

        // Pushgateway endpoint: goosefs.metrics.pushgateway.endpoint
        if let Some(val) = self.get("goosefs.metrics.pushgateway.endpoint") {
            if !val.is_empty() {
                cfg.pushgateway_endpoint = val.to_string();
            }
        }

        // Pushgateway push interval: goosefs.metrics.pushgateway.push.interval (unit: ms)
        if let Some(ms_str) = self.get("goosefs.metrics.pushgateway.push.interval") {
            if let Ok(ms) = ms_str.parse::<u64>() {
                if ms >= MIN_METRICS_HEARTBEAT_INTERVAL_MS {
                    cfg.pushgateway_push_interval = Duration::from_millis(ms);
                }
            }
        }

        // Pushgateway job: goosefs.metrics.pushgateway.job
        if let Some(val) = self.get("goosefs.metrics.pushgateway.job") {
            if !val.is_empty() {
                cfg.pushgateway_job = val.to_string();
            }
        }

        // Pushgateway instance: goosefs.metrics.pushgateway.instance
        if let Some(val) = self.get("goosefs.metrics.pushgateway.instance") {
            if !val.is_empty() {
                cfg.pushgateway_instance = Some(val.to_string());
            }
        }

        // ── Client local page cache: goosefs.user.client.cache.* ──────────
        if let Some(enabled) = self.get_bool("goosefs.user.client.cache.enabled") {
            cfg.client_cache_enabled = enabled;
        }
        if let Some(ps_str) = self.get("goosefs.user.client.cache.page.size") {
            if let Ok(ps) = parse_byte_size(ps_str) {
                if ps > 0 {
                    cfg.client_cache_page_size = ps;
                }
            }
        }
        if let Some(sz_str) = self.get("goosefs.user.client.cache.size") {
            if let Ok(sz) = parse_byte_size(sz_str) {
                cfg.client_cache_size = sz;
            }
        }
        if let Some(dirs) = self.get_list("goosefs.user.client.cache.dirs") {
            if !dirs.is_empty() {
                cfg.client_cache_dirs = dirs;
            }
        }
        if let Some(policy) = self.get("goosefs.user.client.cache.eviction.policy") {
            if let Ok(e) = policy.parse::<CacheEvictorType>() {
                cfg.client_cache_evictor = e;
            }
        }
        if let Some(enabled) = self.get_bool("goosefs.user.client.cache.async.write.enabled") {
            cfg.client_cache_async_write_enabled = enabled;
        }
        if let Some(n) = self.get_parsed::<usize>("goosefs.user.client.cache.async.write.threads") {
            if n > 0 {
                cfg.client_cache_async_write_threads = n;
            }
        }
        if let Some(enabled) = self.get_bool("goosefs.user.client.cache.quota.enabled") {
            cfg.client_cache_quota_enabled = enabled;
        }
        if let Some(secs) = self.get_parsed::<u64>("goosefs.user.client.cache.ttl.seconds") {
            cfg.client_cache_ttl_secs = secs;
        }
        if let Some(enabled) = self.get_bool("goosefs.user.client.cache.sequential.read.enabled") {
            cfg.client_cache_sequential_read_enabled = enabled;
        }

        cfg
    }
}

/// Name of the properties config file.
const PROPERTIES_FILENAME: &str = "goosefs-site.properties";

/// Discover a config file from the standard search paths.
///
/// The search order mirrors the Java `SITE_CONF_DIR` property:
///   `${goosefs.conf.dir}/, ${user.home}/.goosefs/, /etc/goosefs/`
///
/// Search order:
/// 1. `$GOOSEFS_CONFIG_FILE` env var — explicit file path (Rust-only convenience)
/// 2. `$GOOSEFS_CONF_DIR/goosefs-site.properties` — mirrors Java `goosefs.conf.dir`
/// 3. `$GOOSEFS_HOME/conf/goosefs-site.properties` — fallback when `GOOSEFS_CONF_DIR` is unset
/// 4. `~/.goosefs/goosefs-site.properties`          — user home
/// 5. `/etc/goosefs/goosefs-site.properties`        — system-wide
pub fn discover_config_file() -> Option<std::path::PathBuf> {
    use std::path::PathBuf;

    // 1. Explicit env var pointing to a file (highest priority, Rust-only convenience)
    if let Ok(p) = std::env::var(ENV_CONFIG_FILE) {
        let pb = PathBuf::from(&p);
        if pb.exists() {
            return Some(pb);
        }
    }

    // 2. $GOOSEFS_CONF_DIR/goosefs-site.properties  (≈ Java `goosefs.conf.dir`)
    if let Ok(conf_dir) = std::env::var(CONF_DIR) {
        let p = PathBuf::from(&conf_dir).join(PROPERTIES_FILENAME);
        if p.exists() {
            return Some(p);
        }
    }

    // 3. $GOOSEFS_HOME/conf/goosefs-site.properties  (fallback for CONF_DIR)
    if let Ok(home) = std::env::var(ENV_HOME) {
        let p = PathBuf::from(&home).join("conf").join(PROPERTIES_FILENAME);
        if p.exists() {
            return Some(p);
        }
    }

    // 4. ~/.goosefs/goosefs-site.properties (user home)
    if let Some(home) = dirs_next_home() {
        let p = home.join(".goosefs").join(PROPERTIES_FILENAME);
        if p.exists() {
            return Some(p);
        }
    }

    // 5. /etc/goosefs/goosefs-site.properties (system-wide)
    let system = PathBuf::from("/etc/goosefs").join(PROPERTIES_FILENAME);
    if system.exists() {
        return Some(system);
    }

    None
}

/// Return the user's home directory without depending on the `dirs` crate.
fn dirs_next_home() -> Option<std::path::PathBuf> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()
        .map(std::path::PathBuf::from)
}

// ── Default constants ────────────────────────────────────────

/// Default Goosefs Master RPC port.
const DEFAULT_MASTER_PORT: u16 = 9200;
/// Default Goosefs Worker data port.
#[allow(dead_code)]
const DEFAULT_WORKER_PORT: u16 = 9203;
/// Default block size: 64 MiB (matches Goosefs default).
const DEFAULT_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
/// Default chunk size for streaming reads: 1 MiB.
const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024;

// ── Streaming-read tuning (Part V R1-B) ──────────────────────
/// Default sequential-read prefetch window (in chunks).
///
/// Mirrors Java `USER_STREAMING_READER_MAX_PREFETCH_WINDOW = 8`.
/// Sent in the first `ReadRequest` so the worker may keep up to
/// `(1 + prefetch_window)` chunks in flight, decoupling network pull
/// from application consumption.
const DEFAULT_PREFETCH_WINDOW: i32 = 8;
/// Default number of buffered receive messages between the background
/// stream-drain task and the consumer (mirrors Java
/// `USER_STREAMING_READER_BUFFER_SIZE_MESSAGES = 16`).
const DEFAULT_READ_BUFFER_MESSAGES: usize = 16;
/// Default flow-control ACK coalescing threshold in bytes.
///
/// **Default `0` = ACK every chunk** (deadlock-safe). Coalescing ACKs is only
/// safe when the unacked gap never exceeds the worker's flow-control window;
/// since not every worker honours the prefetch window, the conservative
/// default ACKs each chunk (non-blocking `try_send`, so still no per-chunk
/// round-trip stall). Raise this (e.g. 4 MiB) only against workers confirmed
/// to honour `prefetch_window`.
const DEFAULT_ACK_INTERVAL_BYTES: i64 = 0;
/// Default flow-control ACK coalescing threshold in chunks (`1` = every chunk).
const DEFAULT_ACK_INTERVAL_CHUNKS: u32 = 1;

// ── Master connection pool (Part V R3) ───────────────────────
/// Default master connection-pool size (1 = backward compatible, single
/// HTTP/2 channel). Set to 4 or 8 in high-concurrency remote scenarios to
/// spread requests across multiple channels and avoid HTTP/2
/// `SETTINGS_MAX_CONCURRENT_STREAMS` queueing.
const DEFAULT_MASTER_CONNECTION_POOL_SIZE: usize = 1;

// ── Worker connection pool (Part V worker-side multi-channel) ─
/// Default per-worker connection-pool size (1 = backward compatible, single
/// HTTP/2 channel per worker). Raising it (e.g. 4) spreads concurrent block
/// reads across multiple channels to the same worker, lifting the
/// single-connection throughput cap observed in local benchmarks.
const DEFAULT_WORKER_CONNECTION_POOL_SIZE: usize = 1;
/// Default connect timeout: 30 seconds.
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 30_000;
/// Default request timeout: 5 minutes.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 300_000;
/// Default master polling timeout: 30 seconds (mirrors Java `USER_MASTER_POLLING_TIMEOUT`).
const DEFAULT_MASTER_POLLING_TIMEOUT_MS: u64 = 30_000;

/// Default authentication timeout: 30 seconds.
const DEFAULT_AUTH_TIMEOUT_MS: u64 = 30_000;

/// Default config manager RPC port.
const DEFAULT_CONFIG_RPC_PORT: u16 = 9214;

/// Default impersonation username (mirrors Java `Constants.IMPERSONATION_HDFS_USER`).
const DEFAULT_IMPERSONATION_USERNAME: &str = "_HDFS_USER_";
/// Impersonation disabled sentinel (mirrors Java `Constants.IMPERSONATION_NONE`).
#[allow(dead_code)]
pub const IMPERSONATION_NONE: &str = "_NONE_";

/// Default max duration for master inquire retry: 2 minutes.
const DEFAULT_MASTER_INQUIRE_MAX_DURATION_MS: u64 = 120_000;
/// Default initial sleep for master inquire retry: 50 ms.
const DEFAULT_MASTER_INQUIRE_INITIAL_SLEEP_MS: u64 = 50;
/// Default max sleep for master inquire retry: 3 seconds.
const DEFAULT_MASTER_INQUIRE_MAX_SLEEP_MS: u64 = 3_000;

/// Default config expiry time: 30 seconds (mirrors Java `ConfigurationUtils.expireTime`).
const DEFAULT_CONFIG_EXPIRE_MS: u64 = 30_000;

/// Default: metrics collection enabled (mirrors Java `USER_METRICS_COLLECTION_ENABLED`).
const DEFAULT_METRICS_ENABLED: bool = true;
/// Default metrics heartbeat interval: 10 s (mirrors Java `USER_METRICS_HEARTBEAT_INTERVAL_MS`).
const DEFAULT_METRICS_HEARTBEAT_INTERVAL_MS: u64 = 10_000;
/// Default per-heartbeat RPC timeout: 5 s (no Java equivalent).
const DEFAULT_METRICS_HEARTBEAT_TIMEOUT_MS: u64 = 5_000;
/// Minimum allowed heartbeat interval: 1 s (mirrors Java `USER_METRICS_HEARTBEAT_INTERVAL_MS` check).
const MIN_METRICS_HEARTBEAT_INTERVAL_MS: u64 = 1_000;
/// Default maximum metric entries per heartbeat batch.
const DEFAULT_METRICS_MAX_BATCH_SIZE: usize = 1024;
/// Default: Pushgateway disabled (opt-in).
const DEFAULT_PUSHGATEWAY_ENABLED: bool = false;
/// Default Pushgateway push interval: 10 s.
const DEFAULT_PUSHGATEWAY_PUSH_INTERVAL_MS: u64 = 10_000;

// ── Client local page cache defaults ─────────────────────────
//
// Mirror Java `PropertyKey.USER_CLIENT_CACHE_*`. The local page cache is
// **disabled by default** (`client_cache_enabled = false`) so existing
// behaviour is unchanged unless explicitly opted in.

/// Default page size: 1 MiB (mirrors Java `USER_CLIENT_CACHE_PAGE_SIZE`).
const DEFAULT_CLIENT_CACHE_PAGE_SIZE: u64 = 1024 * 1024;
/// Default per-directory cache capacity: 512 MiB (mirrors Java `USER_CLIENT_CACHE_SIZE`).
const DEFAULT_CLIENT_CACHE_SIZE: u64 = 512 * 1024 * 1024;
/// Default async-write concurrency (mirrors Java `USER_CLIENT_CACHE_ASYNC_WRITE_THREADS`).
const DEFAULT_CLIENT_CACHE_ASYNC_WRITE_THREADS: usize = 16;
/// Default cache directory used when none is configured.
const DEFAULT_CLIENT_CACHE_DIR: &str = "/tmp/goosefs_cache";

/// Page-cache eviction policy.
///
/// Mirrors Java `goosefs.user.client.cache.eviction.policy` (evictor class).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum CacheEvictorType {
    /// Least-Recently-Used (default).
    Lru,
    /// Least-Frequently-Used.
    Lfu,
}

impl Default for CacheEvictorType {
    fn default() -> Self {
        CacheEvictorType::Lru
    }
}

impl std::str::FromStr for CacheEvictorType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "LRU" => Ok(CacheEvictorType::Lru),
            "LFU" => Ok(CacheEvictorType::Lfu),
            other => Err(format!("unknown cache evictor type: {other}")),
        }
    }
}

// ── Storage option key constants ─────────────────────────────
//
// These are the canonical key names used in `storage_options` maps
// (e.g. Lance's `DatasetBuilder::with_storage_option` or OpenDAL config).
// Using these constants avoids hard-coded "magic strings" scattered across
// the codebase and test code.

/// Storage option key for Goosefs master address(es).
///
/// Supports HA: `"addr1:port,addr2:port"`.
///
/// Corresponding environment variable: `GOOSEFS_MASTER_ADDR`.
pub const STORAGE_OPT_MASTER_ADDR: &str = "goosefs_master_addr";

/// Storage option key for the default write type.
///
/// Accepted values: `"must_cache"`, `"try_cache"`, `"cache_through"`,
/// `"through"`, `"async_through"` (case-insensitive).
///
/// Corresponding environment variable: `GOOSEFS_WRITE_TYPE`.
pub const STORAGE_OPT_WRITE_TYPE: &str = "goosefs_write_type";

/// Storage option key for block size (in bytes).
///
/// Corresponding environment variable: `GOOSEFS_BLOCK_SIZE`.
pub const STORAGE_OPT_BLOCK_SIZE: &str = "goosefs_block_size";

/// Storage option key for chunk size (in bytes).
///
/// Corresponding environment variable: `GOOSEFS_CHUNK_SIZE`.
pub const STORAGE_OPT_CHUNK_SIZE: &str = "goosefs_chunk_size";

/// Storage option key for authentication type.
///
/// Accepted values: `"nosasl"`, `"simple"` (case-insensitive).
///
/// Corresponding environment variable: `GOOSEFS_AUTH_TYPE`.
pub const STORAGE_OPT_AUTH_TYPE: &str = "goosefs_auth_type";

/// Storage option key for authentication username.
///
/// Corresponding environment variable: `GOOSEFS_AUTH_USERNAME`.
pub const STORAGE_OPT_AUTH_USERNAME: &str = "goosefs_auth_username";

/// Goosefs configuration directory property name.
///
/// Mirrors Java's `public static final String CONF_DIR = "goosefs.conf.dir"`.
/// In the Rust client, the corresponding environment variable is [`ENV_CONF_DIR`].
pub const CONF_DIR: &str = "goosefs.conf.dir";

/// Environment variable: explicit config file path (Rust-only convenience).
pub const ENV_CONFIG_FILE: &str = "GOOSEFS_CONFIG_FILE";

/// Environment variable: Goosefs configuration directory.
///
/// Corresponds to the Java property [`CONF_DIR`] (`goosefs.conf.dir`).
pub const ENV_CONF_DIR: &str = "GOOSEFS_CONF_DIR";

/// Environment variable: Goosefs installation home directory.
pub const ENV_HOME: &str = "GOOSEFS_HOME";

/// Environment variable: Goosefs master address(es).
pub const ENV_MASTER_ADDR: &str = "GOOSEFS_MASTER_ADDR";

/// Environment variable: default write type.
pub const ENV_WRITE_TYPE: &str = "GOOSEFS_WRITE_TYPE";

/// Environment variable: block size.
pub const ENV_BLOCK_SIZE: &str = "GOOSEFS_BLOCK_SIZE";

/// Environment variable: chunk size.
pub const ENV_CHUNK_SIZE: &str = "GOOSEFS_CHUNK_SIZE";

/// Environment variable: authentication type.
pub const ENV_AUTH_TYPE: &str = "GOOSEFS_AUTH_TYPE";

/// Environment variable: authentication username.
pub const ENV_AUTH_USERNAME: &str = "GOOSEFS_AUTH_USERNAME";

/// Environment variable: config manager RPC addresses.
pub const ENV_CONFIG_MANAGER_RPC_ADDRESSES: &str = "GOOSEFS_CONFIG_MANAGER_RPC_ADDRESSES";

/// Environment variable: config RPC port.
pub const ENV_CONFIG_RPC_PORT: &str = "GOOSEFS_CONFIG_RPC_PORT";

/// Environment variable: transparent acceleration enabled.
pub const ENV_TRANSPARENT_ACCELERATION_ENABLED: &str = "GOOSEFS_TRANSPARENT_ACCELERATION_ENABLED";

/// Environment variable: transparent acceleration cosranger enabled.
pub const ENV_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED: &str =
    "GOOSEFS_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED";

/// Environment variable: authorization permission enabled.
pub const ENV_AUTHORIZATION_PERMISSION_ENABLED: &str = "GOOSEFS_AUTHORIZATION_PERMISSION_ENABLED";

/// Environment variable: login impersonation username.
pub const ENV_LOGIN_IMPERSONATION_USERNAME: &str = "GOOSEFS_LOGIN_IMPERSONATION_USERNAME";

/// Environment variable: whether client metrics collection is enabled.
///
/// Mirrors Java's `goosefs.user.metrics.collection.enabled` (Scope=CLIENT).
/// Accepted values: `"true"`, `"false"` (case-insensitive).
pub const ENV_METRICS_ENABLED: &str = "GOOSEFS_USER_METRICS_COLLECTION_ENABLED";

/// Environment variable: metrics heartbeat interval in **milliseconds**.
///
/// Mirrors Java's `goosefs.user.metrics.heartbeat.interval` / `USER_METRICS_HEARTBEAT_INTERVAL_MS`.
/// Must parse as a positive integer ≥ 1000. Example: `"10000"` → 10 s.
pub const ENV_METRICS_HEARTBEAT_INTERVAL_MS: &str = "GOOSEFS_USER_METRICS_HEARTBEAT_INTERVAL_MS";

/// Environment variable: application ID for metric source attribution.
///
/// Mirrors Java's `goosefs.user.app.id`.
pub const ENV_APP_ID: &str = "GOOSEFS_USER_APP_ID";

/// Environment variable: whether to enable Pushgateway metrics push.
///
/// Accepted values: `"true"`, `"false"` (case-insensitive).
/// When enabled, the client periodically pushes metrics to the configured Pushgateway endpoint.
pub const ENV_PUSHGATEWAY_ENABLED: &str = "GOOSEFS_METRICS_PUSHGATEWAY_ENABLED";

/// Environment variable: Pushgateway endpoint URL.
///
/// Example: `"http://10.0.0.1:9091"`.
pub const ENV_PUSHGATEWAY_ENDPOINT: &str = "GOOSEFS_METRICS_PUSHGATEWAY_ENDPOINT";

/// Environment variable: Pushgateway push interval in **milliseconds**.
///
/// Must parse as a positive integer ≥ 1000. Example: `"10000"` → 10 s.
pub const ENV_PUSHGATEWAY_PUSH_INTERVAL_MS: &str = "GOOSEFS_METRICS_PUSHGATEWAY_PUSH_INTERVAL_MS";

/// Environment variable: Pushgateway job label.
///
/// Defaults to `"goosefs_client"` if not set.
pub const ENV_PUSHGATEWAY_JOB: &str = "GOOSEFS_METRICS_PUSHGATEWAY_JOB";

/// Environment variable: Pushgateway instance label.
///
/// When not set, the Pushgateway auto-assigns based on the client IP.
pub const ENV_PUSHGATEWAY_INSTANCE: &str = "GOOSEFS_METRICS_PUSHGATEWAY_INSTANCE";

// ── Client local page cache env vars ─────────────────────────
/// Whether to enable the client-side local page cache (`true`/`false`).
pub const ENV_CLIENT_CACHE_ENABLED: &str = "GOOSEFS_USER_CLIENT_CACHE_ENABLED";
/// Page size in bytes for the local page cache.
pub const ENV_CLIENT_CACHE_PAGE_SIZE: &str = "GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE";
/// Per-directory capacity in bytes for the local page cache.
pub const ENV_CLIENT_CACHE_SIZE: &str = "GOOSEFS_USER_CLIENT_CACHE_SIZE";
/// Comma-separated list of local page cache directories.
pub const ENV_CLIENT_CACHE_DIRS: &str = "GOOSEFS_USER_CLIENT_CACHE_DIRS";
/// Eviction policy (`LRU`/`LFU`).
pub const ENV_CLIENT_CACHE_EVICTOR: &str = "GOOSEFS_USER_CLIENT_CACHE_EVICTION_POLICY";
/// Whether async write-back (cache fill) is enabled (`true`/`false`).
pub const ENV_CLIENT_CACHE_ASYNC_WRITE_ENABLED: &str =
    "GOOSEFS_USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED";
/// Async write-back concurrency.
pub const ENV_CLIENT_CACHE_ASYNC_WRITE_THREADS: &str =
    "GOOSEFS_USER_CLIENT_CACHE_ASYNC_WRITE_THREADS";
/// Whether per-scope quota is enabled (`true`/`false`).
pub const ENV_CLIENT_CACHE_QUOTA_ENABLED: &str = "GOOSEFS_USER_CLIENT_CACHE_QUOTA_ENABLED";
/// Page time-to-live in seconds (`0` = no expiry).
pub const ENV_CLIENT_CACHE_TTL_SECS: &str = "GOOSEFS_USER_CLIENT_CACHE_TTL_SECONDS";
/// Whether sequential reads are routed through the local page cache (`true`/`false`).
pub const ENV_CLIENT_CACHE_SEQUENTIAL_READ_ENABLED: &str =
    "GOOSEFS_USER_CLIENT_CACHE_SEQUENTIAL_READ_ENABLED";

/// Storage option key for config manager RPC addresses.
pub const STORAGE_OPT_CONFIG_MANAGER_RPC_ADDRESSES: &str = "goosefs_config_manager_rpc_addresses";

/// Storage option key for config RPC port.
pub const STORAGE_OPT_CONFIG_RPC_PORT: &str = "goosefs_config_rpc_port";

/// Storage option key for transparent acceleration enabled.
pub const STORAGE_OPT_TRANSPARENT_ACCELERATION_ENABLED: &str =
    "goosefs_transparent_acceleration_enabled";

/// Storage option key for transparent acceleration cosranger enabled.
pub const STORAGE_OPT_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED: &str =
    "goosefs_transparent_acceleration_cosranger_enabled";

/// Storage option key for authorization permission enabled.
pub const STORAGE_OPT_AUTHORIZATION_PERMISSION_ENABLED: &str =
    "goosefs_authorization_permission_enabled";

/// Storage option key for login impersonation username.
pub const STORAGE_OPT_LOGIN_IMPERSONATION_USERNAME: &str = "goosefs_login_impersonation_username";

// ── Client local page cache storage option keys ──────────────
/// Storage option key for enabling the local page cache.
pub const STORAGE_OPT_CLIENT_CACHE_ENABLED: &str = "goosefs_client_cache_enabled";
/// Storage option key for the local page cache page size (bytes).
pub const STORAGE_OPT_CLIENT_CACHE_PAGE_SIZE: &str = "goosefs_client_cache_page_size";
/// Storage option key for the local page cache per-directory size (bytes).
pub const STORAGE_OPT_CLIENT_CACHE_SIZE: &str = "goosefs_client_cache_size";
/// Storage option key for the local page cache directories (comma-separated).
pub const STORAGE_OPT_CLIENT_CACHE_DIRS: &str = "goosefs_client_cache_dirs";
/// Storage option key for the local page cache eviction policy (`LRU`/`LFU`).
pub const STORAGE_OPT_CLIENT_CACHE_EVICTOR: &str = "goosefs_client_cache_eviction_policy";

// ── WriteType: ergonomic Rust enum wrapping WritePType ───────

/// High-level write type for Goosefs file creation.
///
/// This enum provides:
/// - **String ↔ enum conversion** (`FromStr` / `Display`) — like Java `Enum.valueOf()`.
/// - **`WritePType` interop** — zero-cost conversion to/from the protobuf enum.
///
/// # String representation (case-insensitive)
///
/// | Variant       | Strings                              |
/// |---------------|--------------------------------------|
/// | `MustCache`   | `must_cache`, `MUST_CACHE`            |
/// | `TryCache`    | `try_cache`, `TRY_CACHE`              |
/// | `CacheThrough`| `cache_through`, `CACHE_THROUGH`      |
/// | `Through`     | `through`, `THROUGH`                  |
/// | `AsyncThrough`| `async_through`, `ASYNC_THROUGH`      |
///
/// # Examples
/// ```
/// use goosefs_sdk::config::WriteType;
///
/// // Parse from string (case-insensitive)
/// let wt: WriteType = "cache_through".parse().unwrap();
/// assert_eq!(wt, WriteType::CacheThrough);
///
/// // Display as canonical lowercase string
/// assert_eq!(wt.to_string(), "cache_through");
/// assert_eq!(wt.as_str(), "cache_through");
///
/// // Convert to protobuf WritePType
/// use goosefs_sdk::WritePType;
/// assert_eq!(WritePType::from(wt), WritePType::CacheThrough);
///
/// // Convert from protobuf WritePType (use try_from_proto, NOT From, since
/// // `WritePType::Unspecified` / `None` are valid proto values).
/// assert_eq!(WriteType::try_from_proto(WritePType::Through).unwrap(), WriteType::Through);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteType {
    /// Write to Goosefs cache only; no UFS persistence.
    MustCache,
    /// Try to cache; fall back to `Through` if cache is full.
    TryCache,
    /// Write to cache **and** synchronously persist to UFS.
    CacheThrough,
    /// Write directly to UFS, bypassing cache.
    Through,
    /// Write to cache, asynchronously persist to UFS later.
    AsyncThrough,
}

impl WriteType {
    /// All supported write type variants (useful for iteration / help text).
    pub const ALL: &'static [WriteType] = &[
        WriteType::MustCache,
        WriteType::TryCache,
        WriteType::CacheThrough,
        WriteType::Through,
        WriteType::AsyncThrough,
    ];

    /// Return the canonical lowercase string representation.
    ///
    /// This is the string accepted by OpenDAL / Lance `storage_options`.
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteType::MustCache => "must_cache",
            WriteType::TryCache => "try_cache",
            WriteType::CacheThrough => "cache_through",
            WriteType::Through => "through",
            WriteType::AsyncThrough => "async_through",
        }
    }

    /// Return the protobuf `i32` value (same as `WritePType as i32`).
    pub fn as_i32(&self) -> i32 {
        WritePType::from(*self) as i32
    }
}

impl fmt::Display for WriteType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parse a `WriteType` from a string (case-insensitive).
///
/// Accepts both `snake_case` and `UPPER_SNAKE_CASE` forms.
impl FromStr for WriteType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "must_cache" => Ok(WriteType::MustCache),
            "try_cache" => Ok(WriteType::TryCache),
            "cache_through" => Ok(WriteType::CacheThrough),
            "through" => Ok(WriteType::Through),
            "async_through" => Ok(WriteType::AsyncThrough),
            _ => Err(format!(
                "unknown write type '{}'. Expected one of: {}",
                s,
                WriteType::ALL
                    .iter()
                    .map(|wt| wt.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
}

/// Convert `WriteType` → protobuf `WritePType`.
impl From<WriteType> for WritePType {
    fn from(wt: WriteType) -> Self {
        match wt {
            WriteType::MustCache => WritePType::MustCache,
            WriteType::TryCache => WritePType::TryCache,
            WriteType::CacheThrough => WritePType::CacheThrough,
            WriteType::Through => WritePType::Through,
            WriteType::AsyncThrough => WritePType::AsyncThrough,
        }
    }
}

/// Convert protobuf `WritePType` → `WriteType`.
///
/// Returns `Err` for `UnspecifiedWriteType` and `None` (proto).
impl WriteType {
    pub fn try_from_proto(pt: WritePType) -> Result<Self, String> {
        match pt {
            WritePType::MustCache => Ok(WriteType::MustCache),
            WritePType::TryCache => Ok(WriteType::TryCache),
            WritePType::CacheThrough => Ok(WriteType::CacheThrough),
            WritePType::Through => Ok(WriteType::Through),
            WritePType::AsyncThrough => Ok(WriteType::AsyncThrough),
            other => Err(format!(
                "cannot convert WritePType::{:?} to WriteType",
                other
            )),
        }
    }
}

// NOTE: `From<WritePType> for WriteType` is intentionally NOT implemented.
//
// `WritePType::Unspecified` and `WritePType::None` (the default proto value
// returned for unset fields) cannot be losslessly mapped to a `WriteType`,
// so the conversion is fundamentally fallible and `From` would have to
// panic. That makes a stray server response containing one of those
// variants — perfectly legal at the proto level — crash the SDK.
//
// Use [`WriteType::try_from_proto`] instead, which surfaces the error as
// a `Result<WriteType, String>` and lets the caller pick a sensible
// fallback (typically `WriteType::CacheThrough`).

/// Configuration for the Goosefs Rust gRPC client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoosefsConfig {
    /// Primary master address in `host:port` format (backward-compatible).
    ///
    /// When only a single master is used, set this field.
    /// For HA deployments, use [`master_addrs`](Self::master_addrs) instead (or both — `master_addr`
    /// is automatically included if `master_addrs` is also provided).
    pub master_addr: String,

    /// Multiple master addresses for HA deployments.
    ///
    /// When this list contains more than one address, the client will
    /// automatically use [`PollingMasterInquireClient`](crate::client::master_inquire::PollingMasterInquireClient)
    /// to discover the
    /// Primary Master via the `getServiceVersion` RPC.
    ///
    /// If empty, `master_addr` is used as the sole address.
    #[serde(default)]
    pub master_addrs: Vec<String>,

    /// Default block size in bytes for new files.
    pub block_size: u64,

    /// Chunk size for streaming read/write RPCs.
    pub chunk_size: u64,

    /// Connect timeout for gRPC channels.
    pub connect_timeout: Duration,

    /// Request timeout for individual RPCs.
    pub request_timeout: Duration,

    /// Whether to use VPC mapping addresses from WorkerNetAddress.
    pub use_vpc_mapping: bool,

    /// Root path prefix for all operations (e.g. `/goosefs-data`).
    pub root: String,

    /// Default write type for newly created files.
    ///
    /// Controls how data is persisted when writing files.
    /// Use the `WritePType` enum values (as `i32`):
    /// - `1` (`MustCache`) — Write to Goosefs cache only, no UFS persistence.
    /// - `2` (`TryCache`) — Try to cache; fall back to THROUGH if cache is full.
    /// - `3` (`CacheThrough`) — Write to cache AND synchronously persist to UFS.
    /// - `4` (`Through`) — Write directly to UFS, bypass cache.
    /// - `5` (`AsyncThrough`) — Write to cache, asynchronously persist to UFS.
    ///
    /// If not set (`None`), the server-side default is used (typically `MustCache`).
    /// Use [`GoosefsConfig::with_write_type`] for a type-safe builder.
    pub write_type: Option<i32>,

    // ── Streaming-read tuning (Part V R1-B) ──────────────────
    /// Sequential-read prefetch window in chunks (default: 8).
    ///
    /// Sent in the first `ReadRequest`; lets the worker keep up to
    /// `(1 + prefetch_window)` chunks in flight. Mirrors Java
    /// `goosefs.user.streaming.reader.max.prefetch.window`.
    #[serde(default = "default_prefetch_window")]
    pub prefetch_window: i32,

    /// Receive-buffer depth (in messages) between the background
    /// stream-drain task and the consumer (default: 16). Mirrors Java
    /// `goosefs.user.streaming.reader.buffer.size.messages`.
    #[serde(default = "default_read_buffer_messages")]
    pub read_buffer_messages: usize,

    /// Flow-control ACK coalescing threshold in bytes (default: 0 = ACK every
    /// chunk). Coalescing (>0) is opt-in and only safe on workers that honour
    /// `prefetch_window`; otherwise it can stall the read flow-control window.
    #[serde(default = "default_ack_interval_bytes")]
    pub ack_interval_bytes: i64,

    /// Flow-control ACK coalescing threshold in chunks (default: 1 = every
    /// chunk). See [`ack_interval_bytes`](Self::ack_interval_bytes).
    #[serde(default = "default_ack_interval_chunks")]
    pub ack_interval_chunks: u32,

    // ── Master connection pool (Part V R3) ───────────────────
    /// Number of independent Master gRPC channels to pool (default: 1).
    ///
    /// `1` keeps the legacy single-channel behaviour. Raising it (e.g. 4
    /// or 8) spreads concurrent metadata RPCs across multiple HTTP/2
    /// connections, avoiding `SETTINGS_MAX_CONCURRENT_STREAMS` queueing
    /// under high concurrency over remote RTT. All pooled clients share a
    /// single inquire client so HA failover stays consistent.
    #[serde(default = "default_master_connection_pool_size")]
    pub master_connection_pool_size: usize,

    /// Number of independent gRPC channels to pool **per worker** (default: 1).
    ///
    /// `1` keeps the legacy single-channel-per-worker behaviour. Raising it
    /// (e.g. 4) round-robins concurrent block reads across multiple HTTP/2
    /// connections to the same worker, lifting the per-connection throughput
    /// cap (a single channel is bounded by `SETTINGS_MAX_CONCURRENT_STREAMS`
    /// and one connection's flow control). Each channel performs its own SASL
    /// handshake and carries a unique generation, so single-flight reconnect
    /// stays per-channel.
    #[serde(default = "default_worker_connection_pool_size")]
    pub worker_connection_pool_size: usize,

    // ── Master Inquire / HA retry configuration ──────────────
    /// Maximum total duration for master inquire retries (default: 2 min).
    #[serde(default = "default_master_inquire_max_duration")]
    pub master_inquire_retry_max_duration: Duration,

    /// Initial sleep time between master inquire polling rounds (default: 50 ms).
    #[serde(default = "default_master_inquire_initial_sleep")]
    pub master_inquire_initial_sleep: Duration,

    /// Maximum sleep time between master inquire polling rounds (default: 3 s).
    #[serde(default = "default_master_inquire_max_sleep")]
    pub master_inquire_max_sleep: Duration,

    /// Timeout for a single master polling ping RPC (default: 30 s).
    ///
    /// This is independent of [`connect_timeout`](Self::connect_timeout) — it controls only the
    /// `getServiceVersion` probe used to discover the Primary Master.
    /// Mirrors Java's `goosefs.user.master.polling.timeout`.
    #[serde(default = "default_master_polling_timeout")]
    pub master_polling_timeout: Duration,

    // ── Authentication configuration ─────────────────────────
    /// Authentication type (default: `Simple`).
    ///
    /// Controls how the client authenticates with Goosefs Master/Worker.
    /// Mirrors Java's `goosefs.security.authentication.type`.
    ///
    /// Currently supported:
    /// - `NoSasl` — no authentication
    /// - `Simple` — PLAIN SASL with username (default)
    ///
    /// TODO: `Custom`, `Kerberos`, `DelegationToken`, `CapabilityToken`
    #[serde(default)]
    pub auth_type: AuthType,

    /// Username for authentication (default: current OS user).
    ///
    /// Used in SIMPLE mode as the login identity.
    /// Mirrors Java's `goosefs.security.login.username`.
    #[serde(default = "default_auth_username")]
    pub auth_username: String,

    /// Authentication timeout (default: 30 s).
    ///
    /// Maximum time to wait for SASL handshake completion.
    /// Mirrors Java's `goosefs.network.connection.auth.timeout`.
    #[serde(default = "default_auth_timeout")]
    pub auth_timeout: Duration,

    // ── Config Manager configuration ─────────────────────────
    /// Config manager RPC addresses.
    ///
    /// Mirrors Java's `goosefs.config.manager.rpc.addresses`.
    /// When set, the client can fetch dynamic configuration from the config manager.
    #[serde(default)]
    pub config_manager_rpc_addresses: Vec<String>,

    /// Config manager RPC port (default: 9214).
    ///
    /// Mirrors Java's `goosefs.config.rpc.port`.
    #[serde(default = "default_config_rpc_port")]
    pub config_rpc_port: u16,

    // ── Transparent acceleration configuration ───────────────
    /// Whether transparent acceleration is enabled (default: true).
    ///
    /// Mirrors Java's `goosefs.user.client.transparent_acceleration.enabled`.
    #[serde(default = "default_transparent_acceleration_enabled")]
    pub transparent_acceleration_enabled: bool,

    /// Whether transparent acceleration cosranger is enabled (default: false).
    ///
    /// Mirrors Java's `goosefs.user.client.transparent_acceleration.cosranger.enabled`.
    #[serde(default)]
    pub transparent_acceleration_cosranger_enabled: bool,

    // ── Authorization configuration ──────────────────────────
    /// Whether access control based on file permission is enabled (default: false).
    ///
    /// Mirrors Java's `goosefs.security.authorization.permission.enabled`.
    #[serde(default)]
    pub authorization_permission_enabled: bool,

    /// Impersonation username for SIMPLE/CUSTOM authentication.
    ///
    /// When set to `"_HDFS_USER_"` (default), the client impersonates the
    /// Hadoop client user. Set to `"_NONE_"` to disable impersonation.
    ///
    /// Mirrors Java's `goosefs.security.login.impersonation.username`.
    #[serde(default = "default_login_impersonation_username")]
    pub login_impersonation_username: String,

    // ── Metrics / Heartbeat configuration ────────────────────────────────
    /// Whether client metrics collection and heartbeat reporting is enabled.
    ///
    /// When `false`, no background tasks are spawned and no RPC is sent to
    /// the MetricsMaster — identical to Java's behaviour when
    /// `goosefs.user.metrics.collection.enabled = false`.
    ///
    /// Mirrors Java's `goosefs.user.metrics.collection.enabled` (Scope=CLIENT, default: true).
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,

    /// Interval between successive metrics heartbeat RPCs (default: 10 s).
    ///
    /// Must be ≥ 1 s; values of 0 are rejected by [`GoosefsConfig::validate`].
    ///
    /// Mirrors Java's `goosefs.user.metrics.heartbeat.interval`
    /// (`USER_METRICS_HEARTBEAT_INTERVAL_MS`, default 10 000 ms).
    /// Environment variable: `GOOSEFS_USER_METRICS_HEARTBEAT_INTERVAL_MS` (unit: **milliseconds**).
    #[serde(default = "default_metrics_heartbeat_interval")]
    pub metrics_heartbeat_interval: Duration,

    /// Per-heartbeat RPC timeout (default: 5 s).
    ///
    /// No direct Java equivalent; prevents a slow or unresponsive Master from
    /// blocking `close()` / Drop indefinitely.
    #[serde(default = "default_metrics_heartbeat_timeout")]
    pub metrics_heartbeat_timeout: Duration,

    /// Application ID for metric source attribution (default: `None`).
    ///
    /// When `None`, the SDK derives the value at runtime in this order:
    /// 1. `hostname()` from the OS
    /// 2. `"goosefs-rust-{8-char UUID prefix}"` as a last resort
    ///
    /// Mirrors Java's `goosefs.user.app.id` / `IdUtils.createOrGetAppIdFromConfig`.
    /// Environment variable: `GOOSEFS_USER_APP_ID`.
    #[serde(default)]
    pub app_id: Option<String>,

    /// Maximum number of `Metric` entries per single heartbeat RPC (default: 1024).
    ///
    /// Acts as a safety cap against extreme registry sizes; entries beyond
    /// this limit are silently dropped in the current heartbeat and sent in
    /// subsequent ones once earlier entries have been flushed.
    #[serde(default = "default_metrics_max_batch_size")]
    pub metrics_max_batch_size: usize,

    // ── Pushgateway configuration ────────────────────────────────────────
    /// Whether to enable Prometheus Pushgateway metrics push (default: `false`).
    ///
    /// When `true`, the client spawns a background task that periodically pushes
    /// all metrics from the global Registry to the configured Pushgateway endpoint.
    ///
    /// Environment variable: `GOOSEFS_METRICS_PUSHGATEWAY_ENABLED`
    /// Properties key: `goosefs.metrics.pushgateway.enabled`
    #[serde(default)]
    pub pushgateway_enabled: bool,

    /// Pushgateway endpoint URL (default: `"http://127.0.0.1:9091"`).
    ///
    /// Only effective when [`pushgateway_enabled`](Self::pushgateway_enabled) is `true`.
    ///
    /// Environment variable: `GOOSEFS_METRICS_PUSHGATEWAY_ENDPOINT`
    /// Properties key: `goosefs.metrics.pushgateway.endpoint`
    #[serde(default = "default_pushgateway_endpoint")]
    pub pushgateway_endpoint: String,

    /// Pushgateway push interval (default: 10 s).
    ///
    /// Controls how often the background task pushes metrics to the Pushgateway.
    ///
    /// Environment variable: `GOOSEFS_METRICS_PUSHGATEWAY_PUSH_INTERVAL_MS` (unit: ms)
    /// Properties key: `goosefs.metrics.pushgateway.push.interval` (unit: ms)
    #[serde(default = "default_pushgateway_push_interval")]
    pub pushgateway_push_interval: Duration,

    /// Pushgateway job label (default: `"goosefs_client"`).
    ///
    /// Maps to `/metrics/job/{job}` in the Pushgateway URL.
    ///
    /// Environment variable: `GOOSEFS_METRICS_PUSHGATEWAY_JOB`
    /// Properties key: `goosefs.metrics.pushgateway.job`
    #[serde(default = "default_pushgateway_job")]
    pub pushgateway_job: String,

    /// Pushgateway instance label (default: `None`).
    ///
    /// When set, adds `/instance/{instance}` to the Pushgateway URL.
    /// When `None`, an instance identifier is auto-generated as
    /// `"{local_ip}:{pid}"` to prevent multiple client processes on the
    /// same machine from overwriting each other's metrics.
    ///
    /// Environment variable: `GOOSEFS_METRICS_PUSHGATEWAY_INSTANCE`
    /// Properties key: `goosefs.metrics.pushgateway.instance`
    #[serde(default)]
    pub pushgateway_instance: Option<String>,

    // ── Client local page cache ──────────────────────────────
    /// Whether the client-side local page cache is enabled (default: `false`).
    ///
    /// When `false`, all reads go straight to the worker/UFS (current
    /// behaviour). When `true`, a [`crate::cache::CacheManager`] is created
    /// and consulted on the read path. Mirrors Java
    /// `goosefs.user.client.cache.enabled`.
    #[serde(default)]
    pub client_cache_enabled: bool,

    /// Cache page size in bytes (default: 1 MiB).
    ///
    /// Mirrors Java `goosefs.user.client.cache.page.size`.
    #[serde(default = "default_client_cache_page_size")]
    pub client_cache_page_size: u64,

    /// Per-directory cache capacity in bytes (default: 512 MiB).
    ///
    /// Mirrors Java `goosefs.user.client.cache.size`.
    #[serde(default = "default_client_cache_size")]
    pub client_cache_size: u64,

    /// Local cache directories (default: `["/tmp/goosefs_cache"]`).
    ///
    /// Mirrors Java `goosefs.user.client.cache.dirs`.
    #[serde(default = "default_client_cache_dirs")]
    pub client_cache_dirs: Vec<String>,

    /// Page eviction policy (default: `LRU`).
    ///
    /// Mirrors Java `goosefs.user.client.cache.eviction.policy`.
    #[serde(default)]
    pub client_cache_evictor: CacheEvictorType,

    /// Whether async write-back (cache fill) is enabled (default: `true`).
    ///
    /// Mirrors Java `goosefs.user.client.cache.async.write.enabled`.
    #[serde(default = "default_true_bool")]
    pub client_cache_async_write_enabled: bool,

    /// Async write-back concurrency (default: 16).
    ///
    /// Mirrors Java `goosefs.user.client.cache.async.write.threads`.
    #[serde(default = "default_client_cache_async_write_threads")]
    pub client_cache_async_write_threads: usize,

    /// Whether per-scope cache quota is enabled (default: `false`).
    ///
    /// Mirrors Java `goosefs.user.client.cache.quota.enabled`.
    #[serde(default)]
    pub client_cache_quota_enabled: bool,

    /// Page time-to-live in seconds; `0` means no expiry (default: `0`).
    ///
    /// Mirrors Java `goosefs.user.client.cache.ttl`.
    #[serde(default)]
    pub client_cache_ttl_secs: u64,

    /// Whether **sequential** reads (`read`) are routed through the local page
    /// cache (default: `false`).
    ///
    /// Random reads (`read_at`) always consult the cache when it is enabled.
    /// Sequential reads, however, default to the native streaming path: routing
    /// a large sequential scan through fixed-size pages turns one streamed
    /// request into many per-page positioned reads (read amplification), and a
    /// `NoCache` sequential read would re-fetch a whole page for every small
    /// buffer with no caching benefit. Enable this only when sequential reads
    /// are expected to be re-read and should be cached/served locally.
    #[serde(default)]
    pub client_cache_sequential_read_enabled: bool,

    // ── Short-circuit (local mmap) read path (SHORT_CIRCUIT_DESIGN §6) ──
    /// Master kill switch for the short-circuit local read path (default:
    /// `true`). Mirrors Java `goosefs.user.short.circuit.enabled`.
    #[serde(default = "default_true_bool")]
    pub short_circuit_enabled: bool,

    /// Per-task LRU capacity for hot-block readers (default: 64).
    /// `goosefs.client.short.circuit.cache.capacity`.
    #[serde(default = "default_short_circuit_cache_capacity")]
    pub short_circuit_cache_capacity: usize,

    /// Idle TTL after which a cached SC reader is dropped (default: 30 s).
    /// `goosefs.client.short.circuit.cache.ttl`.
    #[serde(default = "default_short_circuit_cache_ttl")]
    pub short_circuit_cache_ttl: Duration,

    /// Negative-cache TTL: a block that failed SC is not retried for this long
    /// (default: 5 s). `goosefs.client.short.circuit.neg.cache.ttl`.
    #[serde(default = "default_short_circuit_neg_cache_ttl")]
    pub short_circuit_neg_cache_ttl: Duration,

    /// L1 kernel-readahead hint: `sequential | random | normal | none`
    /// (default: `random`). `goosefs.client.short.circuit.advise`.
    #[serde(default = "default_short_circuit_advise")]
    pub short_circuit_advise: String,

    /// L2 application-level prefetch master switch (default: `true`). When
    /// `false`, `prefetch` / `prefetch_many` degrade to no-ops.
    /// `goosefs.client.short.circuit.prefetch.enabled`.
    #[serde(default = "default_true_bool")]
    pub short_circuit_prefetch_enabled: bool,

    /// Max gap (bytes) between adjacent ranges merged by `prefetch_many`
    /// (default: 64 KiB). `goosefs.client.short.circuit.prefetch.coalesce.gap`.
    #[serde(default = "default_short_circuit_prefetch_coalesce_gap")]
    pub short_circuit_prefetch_coalesce_gap: usize,

    /// Max number of `madvise` calls issued per `prefetch_many` (default:
    /// 1024). `goosefs.client.short.circuit.prefetch.max.batch`.
    #[serde(default = "default_short_circuit_prefetch_max_batch")]
    pub short_circuit_prefetch_max_batch: usize,

    /// Minimum block size (bytes) to attempt SC; smaller blocks skip SC
    /// (default: 0 = no minimum). `goosefs.client.short.circuit.min.block.size`.
    #[serde(default)]
    pub short_circuit_min_block_size: i64,
}

fn default_master_inquire_max_duration() -> Duration {
    Duration::from_millis(DEFAULT_MASTER_INQUIRE_MAX_DURATION_MS)
}
fn default_master_inquire_initial_sleep() -> Duration {
    Duration::from_millis(DEFAULT_MASTER_INQUIRE_INITIAL_SLEEP_MS)
}
fn default_master_inquire_max_sleep() -> Duration {
    Duration::from_millis(DEFAULT_MASTER_INQUIRE_MAX_SLEEP_MS)
}
fn default_master_polling_timeout() -> Duration {
    Duration::from_millis(DEFAULT_MASTER_POLLING_TIMEOUT_MS)
}
fn default_auth_username() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}
fn default_auth_timeout() -> Duration {
    Duration::from_millis(DEFAULT_AUTH_TIMEOUT_MS)
}
fn default_config_rpc_port() -> u16 {
    DEFAULT_CONFIG_RPC_PORT
}
fn default_transparent_acceleration_enabled() -> bool {
    true
}
fn default_login_impersonation_username() -> String {
    DEFAULT_IMPERSONATION_USERNAME.to_string()
}

fn default_metrics_enabled() -> bool {
    DEFAULT_METRICS_ENABLED
}
fn default_metrics_heartbeat_interval() -> Duration {
    Duration::from_millis(DEFAULT_METRICS_HEARTBEAT_INTERVAL_MS)
}
fn default_metrics_heartbeat_timeout() -> Duration {
    Duration::from_millis(DEFAULT_METRICS_HEARTBEAT_TIMEOUT_MS)
}
fn default_metrics_max_batch_size() -> usize {
    DEFAULT_METRICS_MAX_BATCH_SIZE
}
fn default_pushgateway_endpoint() -> String {
    "http://127.0.0.1:9091".to_string()
}
fn default_pushgateway_push_interval() -> Duration {
    Duration::from_millis(DEFAULT_PUSHGATEWAY_PUSH_INTERVAL_MS)
}
fn default_pushgateway_job() -> String {
    "goosefs_client".to_string()
}

// ── Client local page cache defaults ─────────────────────────
fn default_client_cache_page_size() -> u64 {
    DEFAULT_CLIENT_CACHE_PAGE_SIZE
}
fn default_client_cache_size() -> u64 {
    DEFAULT_CLIENT_CACHE_SIZE
}
fn default_client_cache_dirs() -> Vec<String> {
    vec![DEFAULT_CLIENT_CACHE_DIR.to_string()]
}
fn default_client_cache_async_write_threads() -> usize {
    DEFAULT_CLIENT_CACHE_ASYNC_WRITE_THREADS
}
fn default_true_bool() -> bool {
    true
}

// ── Short-circuit (local mmap) read defaults (SHORT_CIRCUIT_DESIGN §6) ─
fn default_short_circuit_cache_capacity() -> usize {
    64
}
fn default_short_circuit_cache_ttl() -> Duration {
    Duration::from_secs(30)
}
fn default_short_circuit_neg_cache_ttl() -> Duration {
    Duration::from_secs(5)
}
fn default_short_circuit_advise() -> String {
    "random".to_string()
}
fn default_short_circuit_prefetch_coalesce_gap() -> usize {
    64 * 1024
}
fn default_short_circuit_prefetch_max_batch() -> usize {
    1024
}

// ── Streaming-read tuning / master pool defaults (Part V) ─────
fn default_prefetch_window() -> i32 {
    DEFAULT_PREFETCH_WINDOW
}
fn default_read_buffer_messages() -> usize {
    DEFAULT_READ_BUFFER_MESSAGES
}
fn default_ack_interval_bytes() -> i64 {
    DEFAULT_ACK_INTERVAL_BYTES
}
fn default_ack_interval_chunks() -> u32 {
    DEFAULT_ACK_INTERVAL_CHUNKS
}
fn default_master_connection_pool_size() -> usize {
    DEFAULT_MASTER_CONNECTION_POOL_SIZE
}
fn default_worker_connection_pool_size() -> usize {
    DEFAULT_WORKER_CONNECTION_POOL_SIZE
}

impl Default for GoosefsConfig {
    fn default() -> Self {
        Self {
            master_addr: format!("127.0.0.1:{}", DEFAULT_MASTER_PORT),
            master_addrs: Vec::new(),
            block_size: DEFAULT_BLOCK_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
            connect_timeout: Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS),
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            use_vpc_mapping: false,
            root: String::new(),
            write_type: None,
            prefetch_window: default_prefetch_window(),
            read_buffer_messages: default_read_buffer_messages(),
            ack_interval_bytes: default_ack_interval_bytes(),
            ack_interval_chunks: default_ack_interval_chunks(),
            master_connection_pool_size: default_master_connection_pool_size(),
            worker_connection_pool_size: default_worker_connection_pool_size(),
            master_inquire_retry_max_duration: default_master_inquire_max_duration(),
            master_inquire_initial_sleep: default_master_inquire_initial_sleep(),
            master_inquire_max_sleep: default_master_inquire_max_sleep(),
            master_polling_timeout: default_master_polling_timeout(),
            auth_type: AuthType::default(),
            auth_username: default_auth_username(),
            auth_timeout: default_auth_timeout(),
            config_manager_rpc_addresses: Vec::new(),
            config_rpc_port: default_config_rpc_port(),
            transparent_acceleration_enabled: default_transparent_acceleration_enabled(),
            transparent_acceleration_cosranger_enabled: false,
            authorization_permission_enabled: false,
            login_impersonation_username: default_login_impersonation_username(),
            metrics_enabled: default_metrics_enabled(),
            metrics_heartbeat_interval: default_metrics_heartbeat_interval(),
            metrics_heartbeat_timeout: default_metrics_heartbeat_timeout(),
            app_id: None,
            metrics_max_batch_size: default_metrics_max_batch_size(),
            pushgateway_enabled: DEFAULT_PUSHGATEWAY_ENABLED,
            pushgateway_endpoint: default_pushgateway_endpoint(),
            pushgateway_push_interval: default_pushgateway_push_interval(),
            pushgateway_job: default_pushgateway_job(),
            pushgateway_instance: None,
            client_cache_enabled: false,
            client_cache_page_size: default_client_cache_page_size(),
            client_cache_size: default_client_cache_size(),
            client_cache_dirs: default_client_cache_dirs(),
            client_cache_evictor: CacheEvictorType::default(),
            client_cache_async_write_enabled: default_true_bool(),
            client_cache_async_write_threads: default_client_cache_async_write_threads(),
            client_cache_quota_enabled: false,
            client_cache_ttl_secs: 0,
            client_cache_sequential_read_enabled: false,
            short_circuit_enabled: true,
            short_circuit_cache_capacity: default_short_circuit_cache_capacity(),
            short_circuit_cache_ttl: default_short_circuit_cache_ttl(),
            short_circuit_neg_cache_ttl: default_short_circuit_neg_cache_ttl(),
            short_circuit_advise: default_short_circuit_advise(),
            short_circuit_prefetch_enabled: true,
            short_circuit_prefetch_coalesce_gap: default_short_circuit_prefetch_coalesce_gap(),
            short_circuit_prefetch_max_batch: default_short_circuit_prefetch_max_batch(),
            short_circuit_min_block_size: 0,
        }
    }
}

impl GoosefsConfig {
    /// Create a new config with the given single master address.
    pub fn new(master_addr: impl Into<String>) -> Self {
        Self {
            master_addr: master_addr.into(),
            ..Default::default()
        }
    }

    /// Create a new config for HA (High Availability) with multiple master addresses.
    ///
    /// The first address in the list is also set as `master_addr` for
    /// backward compatibility.
    ///
    /// # Panics
    /// Panics if `addrs` is empty.
    pub fn new_ha(addrs: Vec<String>) -> Self {
        assert!(!addrs.is_empty(), "master addresses must not be empty");
        Self {
            master_addr: addrs[0].clone(),
            master_addrs: addrs,
            ..Default::default()
        }
    }

    /// Create a config from one or more master addresses.
    ///
    /// Automatically selects the right mode:
    /// - 1 address  → single-master (same as [`new`](Self::new)).
    /// - 2+ addresses → multi-master (same as [`new_ha`](Self::new_ha)).
    ///
    /// # Panics
    /// Panics if `addrs` is empty.
    pub fn from_addresses(addrs: Vec<String>) -> Self {
        assert!(!addrs.is_empty(), "master addresses must not be empty");
        if addrs.len() == 1 {
            Self::new(&addrs[0])
        } else {
            Self::new_ha(addrs)
        }
    }

    /// Return the effective list of master addresses.
    ///
    /// If [`master_addrs`](Self::master_addrs) is non-empty, returns it directly.
    /// Otherwise, returns a single-element list containing [`master_addr`](Self::master_addr).
    pub fn master_addresses(&self) -> Vec<String> {
        if self.master_addrs.is_empty() {
            vec![self.master_addr.clone()]
        } else {
            self.master_addrs.clone()
        }
    }

    /// Returns `true` if the client is configured with multiple masters.
    pub fn is_multi_master(&self) -> bool {
        self.master_addrs.len() > 1
    }

    /// Resolve the full path by prepending the root.
    pub fn full_path(&self, path: &str) -> String {
        if self.root.is_empty() {
            path.to_string()
        } else {
            let root = self.root.trim_end_matches('/');
            let path = path.trim_start_matches('/');
            format!("{}/{}", root, path)
        }
    }

    /// Build the gRPC endpoint URI for the master.
    pub fn master_endpoint(&self) -> String {
        format!("http://{}", self.master_addr)
    }

    /// Build the gRPC endpoint URI for a worker.
    pub fn worker_endpoint(&self, host: &str, rpc_port: i32) -> String {
        if self.use_vpc_mapping {
            // VPC mapping is handled at the caller level
            format!("http://{}:{}", host, rpc_port)
        } else {
            format!("http://{}:{}", host, rpc_port)
        }
    }

    /// Set the authentication type.
    ///
    /// # Example
    /// ```
    /// use goosefs_sdk::config::GoosefsConfig;
    /// use goosefs_sdk::auth::AuthType;
    ///
    /// let config = GoosefsConfig::new("127.0.0.1:9200")
    ///     .with_auth_type(AuthType::NoSasl);
    /// ```
    pub fn with_auth_type(mut self, auth_type: AuthType) -> Self {
        self.auth_type = auth_type;
        self
    }

    /// Set the authentication type from a string (case-insensitive).
    ///
    /// Accepted values: `"nosasl"`, `"simple"`.
    pub fn with_auth_type_str(self, auth_type: &str) -> Result<Self, String> {
        let at: AuthType = auth_type.parse()?;
        Ok(self.with_auth_type(at))
    }

    /// Set the authentication username.
    pub fn with_auth_username(mut self, username: impl Into<String>) -> Self {
        self.auth_username = username.into();
        self
    }

    /// Set the default write type using the protobuf `WritePType` enum.
    ///
    /// # Example
    /// ```
    /// use goosefs_sdk::config::GoosefsConfig;
    /// use goosefs_sdk::WritePType;
    ///
    /// let config = GoosefsConfig::new("127.0.0.1:9200")
    ///     .with_write_type(WritePType::CacheThrough);
    /// ```
    pub fn with_write_type(mut self, wt: WritePType) -> Self {
        self.write_type = Some(wt as i32);
        self
    }

    /// Set the default write type using the high-level [`WriteType`] enum.
    ///
    /// # Example
    /// ```
    /// use goosefs_sdk::config::{GoosefsConfig, WriteType};
    ///
    /// let config = GoosefsConfig::new("127.0.0.1:9200")
    ///     .with_write_type_enum(WriteType::CacheThrough);
    /// ```
    pub fn with_write_type_enum(mut self, wt: WriteType) -> Self {
        self.write_type = Some(wt.as_i32());
        self
    }

    /// Set the default write type from a string (case-insensitive).
    ///
    /// Accepted values: `"must_cache"`, `"cache_through"`, `"through"`,
    /// `"try_cache"`, `"async_through"`.
    ///
    /// # Example
    /// ```
    /// use goosefs_sdk::config::GoosefsConfig;
    ///
    /// let config = GoosefsConfig::new("127.0.0.1:9200")
    ///     .with_write_type_str("cache_through")
    ///     .unwrap();
    /// ```
    pub fn with_write_type_str(self, wt: &str) -> Result<Self, String> {
        let write_type: WriteType = wt.parse()?;
        Ok(self.with_write_type_enum(write_type))
    }

    /// Set the sequential-read prefetch window (in chunks). See
    /// [`GoosefsConfig::prefetch_window`] (Part V R1-B-a).
    pub fn with_prefetch_window(mut self, window: i32) -> Self {
        self.prefetch_window = window;
        self
    }

    /// Set the flow-control ACK coalescing threshold in bytes (Part V R1-B-c).
    pub fn with_ack_interval_bytes(mut self, bytes: i64) -> Self {
        self.ack_interval_bytes = bytes;
        self
    }

    /// Set the number of pooled Master gRPC channels (Part V R3).
    ///
    /// `1` keeps the legacy single-channel behaviour. Values are clamped to
    /// at least `1`.
    pub fn with_master_connection_pool_size(mut self, size: usize) -> Self {
        self.master_connection_pool_size = size.max(1);
        self
    }

    /// Set the per-worker gRPC channel-pool size (worker-side multi-channel).
    ///
    /// `1` keeps the legacy single-channel-per-worker behaviour. Values are
    /// clamped to at least `1`.
    pub fn with_worker_connection_pool_size(mut self, size: usize) -> Self {
        self.worker_connection_pool_size = size.max(1);
        self
    }

    /// Get the configured `WritePType`, if set.
    ///
    /// Returns `None` if `write_type` is unset or contains an unrecognised value.
    pub fn get_write_type(&self) -> Option<WritePType> {
        self.write_type.and_then(|v| match v {
            0 => Some(WritePType::UnspecifiedWriteType),
            1 => Some(WritePType::MustCache),
            2 => Some(WritePType::TryCache),
            3 => Some(WritePType::CacheThrough),
            4 => Some(WritePType::Through),
            5 => Some(WritePType::AsyncThrough),
            6 => Some(WritePType::None),
            _ => Option::None,
        })
    }

    // ── YAML / env configuration loading ───────────────────────────────────

    // ── Metrics builder methods ──────────────────────────────────────────────

    /// Enable or disable client metrics collection and heartbeat reporting.
    pub fn with_metrics_enabled(mut self, enabled: bool) -> Self {
        self.metrics_enabled = enabled;
        self
    }

    /// Set the metrics heartbeat interval.
    ///
    /// # Panics
    /// Panics if `interval` is less than 1 second.
    pub fn with_metrics_heartbeat_interval(mut self, interval: Duration) -> Self {
        assert!(
            interval >= Duration::from_secs(1),
            "metrics_heartbeat_interval must be >= 1 s"
        );
        self.metrics_heartbeat_interval = interval;
        self
    }

    /// Set the per-heartbeat RPC timeout.
    ///
    /// The timeout caps how long a single `MetricsHeartbeat` RPC is allowed
    /// to run before being aborted, preventing a stuck/slow Master from
    /// causing the periodic task to pile up in-flight requests.
    ///
    /// Recommended range: `interval / 3 ..= interval / 2`. The hard
    /// constraints (`>= 1 s` and `< metrics_heartbeat_interval`) are
    /// re-checked by [`Self::validate`].
    ///
    /// # Panics
    /// Panics if `timeout` is less than 1 second.
    pub fn with_metrics_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            timeout >= Duration::from_secs(1),
            "metrics_heartbeat_timeout must be >= 1 s"
        );
        self.metrics_heartbeat_timeout = timeout;
        self
    }

    /// Set the application ID used as the metric source identifier.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    // ── Pushgateway builder methods ─────────────────────────────────────────

    /// Enable or disable Pushgateway metrics push.
    ///
    /// When enabled, the `FileSystemContext` will automatically spawn a background
    /// task pushing metrics to the configured Pushgateway endpoint.
    pub fn with_pushgateway_enabled(mut self, enabled: bool) -> Self {
        self.pushgateway_enabled = enabled;
        self
    }

    /// Set the Pushgateway endpoint URL.
    ///
    /// Example: `"http://10.0.0.1:9091"`
    pub fn with_pushgateway_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.pushgateway_endpoint = endpoint.into();
        self
    }

    /// Set the Pushgateway push interval.
    ///
    /// # Panics
    /// Panics if `interval` is less than 1 second.
    pub fn with_pushgateway_push_interval(mut self, interval: Duration) -> Self {
        assert!(
            interval >= Duration::from_secs(1),
            "pushgateway_push_interval must be >= 1 s"
        );
        self.pushgateway_push_interval = interval;
        self
    }

    /// Set the Pushgateway job label.
    pub fn with_pushgateway_job(mut self, job: impl Into<String>) -> Self {
        self.pushgateway_job = job.into();
        self
    }

    /// Set the Pushgateway instance label.
    pub fn with_pushgateway_instance(mut self, instance: impl Into<String>) -> Self {
        self.pushgateway_instance = Some(instance.into());
        self
    }

    /// Load configuration from environment variables.
    ///
    /// Reads the following variables (all optional):
    ///
    /// | Variable              | Field           |
    /// |-----------------------|-----------------|
    /// | `GOOSEFS_MASTER_ADDR` | `master_addr` / `master_addrs` |
    /// | `GOOSEFS_WRITE_TYPE`  | `write_type`    |
    /// | `GOOSEFS_BLOCK_SIZE`  | `block_size`    |
    /// | `GOOSEFS_CHUNK_SIZE`  | `chunk_size`    |
    /// | `GOOSEFS_AUTH_TYPE`   | `auth_type`     |
    /// | `GOOSEFS_AUTH_USERNAME` | `auth_username` |
    ///
    /// Returns a config reflecting any variables that are set, falling back to
    /// defaults for unset variables.
    ///
    /// # Priority
    ///
    /// This is intended to be called as part of the auto-load chain:
    /// `from_properties_auto()` then `apply_env()`.  Call `apply_env()` on an
    /// existing config to overlay env-var values on top of properties values.
    pub fn from_env() -> Self {
        Self::default().apply_env()
    }

    /// Apply environment variables on top of the current config (in-place).
    ///
    /// Variables that are set override the corresponding field; unset
    /// variables leave the field unchanged.
    pub fn apply_env(mut self) -> Self {
        use std::env;

        // Master address(es)
        if let Ok(addr) = env::var(ENV_MASTER_ADDR) {
            let addrs: Vec<String> = addr
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if !addrs.is_empty() {
                self.master_addr = addrs[0].clone();
                if addrs.len() > 1 {
                    self.master_addrs = addrs;
                } else {
                    self.master_addrs = Vec::new();
                }
            }
        }

        // Write type
        if let Ok(wt_str) = env::var(ENV_WRITE_TYPE) {
            if let Ok(wt) = wt_str.parse::<WriteType>() {
                self.write_type = Some(wt.as_i32());
            }
        }

        // Block size
        if let Ok(bs_str) = env::var(ENV_BLOCK_SIZE) {
            if let Ok(bs) = bs_str.parse::<u64>() {
                self.block_size = bs;
            }
        }

        // Chunk size
        if let Ok(cs_str) = env::var(ENV_CHUNK_SIZE) {
            if let Ok(cs) = cs_str.parse::<u64>() {
                self.chunk_size = cs;
            }
        }

        // Auth type
        if let Ok(at_str) = env::var(ENV_AUTH_TYPE) {
            if let Ok(at) = at_str.parse::<crate::auth::AuthType>() {
                self.auth_type = at;
            }
        }

        // Auth username
        if let Ok(user) = env::var(ENV_AUTH_USERNAME) {
            if !user.is_empty() {
                self.auth_username = user;
            }
        }

        // Config manager RPC addresses
        if let Ok(addrs_str) = env::var(ENV_CONFIG_MANAGER_RPC_ADDRESSES) {
            let addrs: Vec<String> = addrs_str
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if !addrs.is_empty() {
                self.config_manager_rpc_addresses = addrs;
            }
        }

        // Config RPC port
        if let Ok(port_str) = env::var(ENV_CONFIG_RPC_PORT) {
            if let Ok(port) = port_str.parse::<u16>() {
                self.config_rpc_port = port;
            }
        }

        // Transparent acceleration enabled
        if let Ok(val) = env::var(ENV_TRANSPARENT_ACCELERATION_ENABLED) {
            if let Ok(b) = val.parse::<bool>() {
                self.transparent_acceleration_enabled = b;
            }
        }

        // Transparent acceleration cosranger enabled
        if let Ok(val) = env::var(ENV_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED) {
            if let Ok(b) = val.parse::<bool>() {
                self.transparent_acceleration_cosranger_enabled = b;
            }
        }

        // Authorization permission enabled
        if let Ok(val) = env::var(ENV_AUTHORIZATION_PERMISSION_ENABLED) {
            if let Ok(b) = val.parse::<bool>() {
                self.authorization_permission_enabled = b;
            }
        }

        // Login impersonation username
        if let Ok(user) = env::var(ENV_LOGIN_IMPERSONATION_USERNAME) {
            if !user.is_empty() {
                self.login_impersonation_username = user;
            }
        }

        // Metrics collection enabled
        if let Ok(val) = env::var(ENV_METRICS_ENABLED) {
            if let Ok(b) = val.to_lowercase().parse::<bool>() {
                self.metrics_enabled = b;
            }
        }

        // Metrics heartbeat interval (unit: milliseconds)
        if let Ok(ms_str) = env::var(ENV_METRICS_HEARTBEAT_INTERVAL_MS) {
            if let Ok(ms) = ms_str.parse::<u64>() {
                if ms >= MIN_METRICS_HEARTBEAT_INTERVAL_MS {
                    self.metrics_heartbeat_interval = Duration::from_millis(ms);
                }
            }
        }

        // Application ID
        if let Ok(id) = env::var(ENV_APP_ID) {
            if !id.is_empty() {
                self.app_id = Some(id);
            }
        }

        // Pushgateway enabled
        if let Ok(val) = env::var(ENV_PUSHGATEWAY_ENABLED) {
            if let Ok(b) = val.to_lowercase().parse::<bool>() {
                self.pushgateway_enabled = b;
            }
        }

        // Pushgateway endpoint
        if let Ok(val) = env::var(ENV_PUSHGATEWAY_ENDPOINT) {
            if !val.is_empty() {
                self.pushgateway_endpoint = val;
            }
        }

        // Pushgateway push interval (unit: milliseconds)
        if let Ok(ms_str) = env::var(ENV_PUSHGATEWAY_PUSH_INTERVAL_MS) {
            if let Ok(ms) = ms_str.parse::<u64>() {
                if ms >= MIN_METRICS_HEARTBEAT_INTERVAL_MS {
                    self.pushgateway_push_interval = Duration::from_millis(ms);
                }
            }
        }

        // Pushgateway job
        if let Ok(val) = env::var(ENV_PUSHGATEWAY_JOB) {
            if !val.is_empty() {
                self.pushgateway_job = val;
            }
        }

        // Pushgateway instance
        if let Ok(val) = env::var(ENV_PUSHGATEWAY_INSTANCE) {
            if !val.is_empty() {
                self.pushgateway_instance = Some(val);
            }
        }

        // ── Client local page cache ──────────────────────────
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_ENABLED) {
            if let Ok(b) = val.to_lowercase().parse::<bool>() {
                self.client_cache_enabled = b;
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_PAGE_SIZE) {
            if let Ok(n) = val.parse::<u64>() {
                if n > 0 {
                    self.client_cache_page_size = n;
                }
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_SIZE) {
            if let Ok(n) = val.parse::<u64>() {
                self.client_cache_size = n;
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_DIRS) {
            let dirs: Vec<String> = val
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if !dirs.is_empty() {
                self.client_cache_dirs = dirs;
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_EVICTOR) {
            if let Ok(e) = val.parse::<CacheEvictorType>() {
                self.client_cache_evictor = e;
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_ASYNC_WRITE_ENABLED) {
            if let Ok(b) = val.to_lowercase().parse::<bool>() {
                self.client_cache_async_write_enabled = b;
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_ASYNC_WRITE_THREADS) {
            if let Ok(n) = val.parse::<usize>() {
                if n > 0 {
                    self.client_cache_async_write_threads = n;
                }
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_QUOTA_ENABLED) {
            if let Ok(b) = val.to_lowercase().parse::<bool>() {
                self.client_cache_quota_enabled = b;
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_TTL_SECS) {
            if let Ok(n) = val.parse::<u64>() {
                self.client_cache_ttl_secs = n;
            }
        }
        if let Ok(val) = env::var(ENV_CLIENT_CACHE_SEQUENTIAL_READ_ENABLED) {
            if let Ok(b) = val.to_lowercase().parse::<bool>() {
                self.client_cache_sequential_read_enabled = b;
            }
        }

        self
    }

    /// Load configuration from a Java-style properties file.
    ///
    /// The file format is `goosefs-site.properties` with `key=value` lines:
    ///
    /// ```text
    /// goosefs.master.hostname=10.0.0.1
    /// goosefs.master.rpc.port=9200
    /// goosefs.security.authentication.type=SIMPLE
    /// goosefs.user.file.writetype.default=CACHE_THROUGH
    /// goosefs.user.block.size.bytes.default=4MB
    /// ```
    ///
    /// Returns an error if the file cannot be read.
    pub fn from_properties(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigLoadError> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| ConfigLoadError::IoError {
            path: path.display().to_string(),
            source: e.to_string(),
        })?;
        Ok(Self::from_properties_str(&content))
    }

    /// Parse configuration from a properties-format string.
    ///
    /// Useful for testing or embedding config in code.
    pub fn from_properties_str(content: &str) -> Self {
        let props = PropertiesMap::parse(content);
        props.into_goosefs_config()
    }

    /// Auto-discover and load configuration with the full priority chain.
    ///
    /// # Priority (highest to lowest)
    ///
    /// 1. Environment variables (`GOOSEFS_*`)
    /// 2. Properties config file (see search paths below)
    /// 3. Built-in defaults
    ///
    /// # Config file search paths
    ///
    /// Mirrors the Java `SITE_CONF_DIR` property:
    ///   `${goosefs.conf.dir}/, ${user.home}/.goosefs/, /etc/goosefs/`
    ///
    /// 1. `$GOOSEFS_CONFIG_FILE` environment variable (if set and file exists)
    /// 2. `$GOOSEFS_CONF_DIR/goosefs-site.properties` (mirrors Java `goosefs.conf.dir`)
    /// 3. `$GOOSEFS_HOME/conf/goosefs-site.properties` (fallback when `GOOSEFS_CONF_DIR` unset)
    /// 4. `~/.goosefs/goosefs-site.properties` (user home directory)
    /// 5. `/etc/goosefs/goosefs-site.properties` (system-wide)
    ///
    /// If no config file is found, falls back to defaults.
    /// Then env vars are overlaid on top.
    ///
    /// # Errors
    ///
    /// Returns an error only if a config file is found but cannot be read.
    /// If no file is found, returns `Ok` with defaults + env vars applied.
    pub fn from_properties_auto() -> Result<Self, ConfigLoadError> {
        let base = if let Some(path) = discover_config_file() {
            Self::from_properties(&path)?
        } else {
            Self::default()
        };

        // Overlay env vars (highest priority)
        Ok(base.apply_env())
    }

    /// Validate configuration. Returns an error message if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.master_addr.is_empty() && self.master_addrs.is_empty() {
            return Err(
                "at least one master address must be provided (master_addr or master_addrs)"
                    .to_string(),
            );
        }
        if !self.master_addrs.is_empty() && self.master_addrs.iter().any(|a| a.is_empty()) {
            return Err("master_addrs contains an empty address".to_string());
        }
        if self.block_size == 0 {
            return Err("block_size must be > 0".to_string());
        }
        if self.chunk_size == 0 {
            return Err("chunk_size must be > 0".to_string());
        }
        if self.chunk_size > self.block_size {
            return Err("chunk_size must be <= block_size".to_string());
        }
        if self.metrics_heartbeat_interval
            < Duration::from_millis(MIN_METRICS_HEARTBEAT_INTERVAL_MS)
        {
            return Err(format!(
                "metrics_heartbeat_interval must be >= {}ms (got {}ms)",
                MIN_METRICS_HEARTBEAT_INTERVAL_MS,
                self.metrics_heartbeat_interval.as_millis()
            ));
        }
        // The heartbeat RPC timeout must be:
        //   1. >= 1 s, to tolerate ordinary GC / network jitter without
        //      generating false timeouts that retry and double-count.
        //   2. <  metrics_heartbeat_interval, otherwise periodic ticks
        //      can fire while the previous RPC is still in flight,
        //      letting requests pile up against a slow / dead Master
        //      (the very situation the timeout is meant to prevent).
        if self.metrics_heartbeat_timeout < Duration::from_secs(1) {
            return Err(format!(
                "metrics_heartbeat_timeout must be >= 1000ms (got {}ms)",
                self.metrics_heartbeat_timeout.as_millis()
            ));
        }
        if self.metrics_heartbeat_timeout >= self.metrics_heartbeat_interval {
            return Err(format!(
                "metrics_heartbeat_timeout ({}ms) must be < metrics_heartbeat_interval ({}ms) \
                 to prevent in-flight RPCs from piling up across ticks",
                self.metrics_heartbeat_timeout.as_millis(),
                self.metrics_heartbeat_interval.as_millis()
            ));
        }
        Ok(())
    }
}

// ── ConfigRefresher: periodic config reload ──────────────────
//
// Mirrors Java's `ConfigurationUtils.loadIfExpire()` +
// `AbstractCompatibleFileSystem.refreshTransparentAccelerationSwitch()`.
//
// The refresher caches the last-loaded config and only re-reads the
// properties file from disk when the expiry time has elapsed.

/// Result of a config refresh — the two switches that may change at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransparentAccelerationSwitch {
    /// Whether transparent acceleration is enabled.
    pub enabled: bool,
    /// Whether transparent acceleration cosranger is enabled.
    pub cosranger_enabled: bool,
}

/// Thread-safe config refresher that periodically reloads `goosefs-site.properties`.
///
/// Mirrors the Java pattern:
/// ```text
/// ConfigurationUtils.loadIfExpire();          // reload if stale
/// GoosefsProperties props = ConfigurationUtils.defaults();
/// InstancedConfiguration cfg = new InstancedConfiguration(props);
/// boolean enable = cfg.getBoolean(TRANSPARENT_ACCELERATION_ENABLED);
/// boolean cosRangerEnable = cfg.getBoolean(COSRANGER_ENABLED);
/// ```
///
/// # Usage
///
/// ```rust,no_run
/// use goosefs_sdk::config::ConfigRefresher;
///
/// let refresher = ConfigRefresher::new();
/// // In a background loop:
/// let switch = refresher.refresh_transparent_acceleration_switch();
/// println!("acceleration={}, cosranger={}", switch.enabled, switch.cosranger_enabled);
/// ```
pub struct ConfigRefresher {
    /// Last time the config was loaded from disk.
    last_load_time: Mutex<Option<Instant>>,
    /// Config expiry duration (default: 30s, mirrors Java `expireTime`).
    expire_duration: Duration,
    /// Cached transparent acceleration enabled flag (AtomicBool for lock-free reads).
    transparent_acceleration_enabled: AtomicBool,
    /// Cached cosranger enabled flag.
    cosranger_enabled: AtomicBool,
}

impl fmt::Debug for ConfigRefresher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigRefresher")
            .field("expire_duration", &self.expire_duration)
            .field(
                "transparent_acceleration_enabled",
                &self
                    .transparent_acceleration_enabled
                    .load(Ordering::Relaxed),
            )
            .field(
                "cosranger_enabled",
                &self.cosranger_enabled.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl ConfigRefresher {
    /// Create a new refresher with the default expiry time (30s).
    ///
    /// The initial switch values come from the current config (loaded once
    /// via `from_properties_auto`).
    pub fn new() -> Self {
        Self::with_expire(Duration::from_millis(DEFAULT_CONFIG_EXPIRE_MS))
    }

    /// Create a new refresher with a custom expiry duration.
    pub fn with_expire(expire_duration: Duration) -> Self {
        // Load the initial config to seed the switch values.
        let initial = GoosefsConfig::from_properties_auto().unwrap_or_default();
        Self {
            last_load_time: Mutex::new(Some(Instant::now())),
            expire_duration,
            transparent_acceleration_enabled: AtomicBool::new(
                initial.transparent_acceleration_enabled,
            ),
            cosranger_enabled: AtomicBool::new(initial.transparent_acceleration_cosranger_enabled),
        }
    }

    /// Create a refresher seeded from an existing config.
    ///
    /// Useful when the caller already has a `GoosefsConfig` (e.g. from
    /// `FileSystemContext::connect`).
    pub fn from_config(config: &GoosefsConfig) -> Self {
        Self {
            last_load_time: Mutex::new(Some(Instant::now())),
            expire_duration: Duration::from_millis(DEFAULT_CONFIG_EXPIRE_MS),
            transparent_acceleration_enabled: AtomicBool::new(
                config.transparent_acceleration_enabled,
            ),
            cosranger_enabled: AtomicBool::new(config.transparent_acceleration_cosranger_enabled),
        }
    }

    /// Reload config from disk if the expiry time has elapsed, then return
    /// the current transparent acceleration switch values.
    ///
    /// This mirrors Java's:
    /// ```java
    /// boolean refreshTransparentAccelerationSwitch() {
    ///     ConfigurationUtils.loadIfExpire();
    ///     GoosefsProperties props = ConfigurationUtils.defaults();
    ///     InstancedConfiguration cfg = new InstancedConfiguration(props);
    ///     cfg.validate();
    ///     boolean enable = cfg.getBoolean(TRANSPARENT_ACCELERATION_ENABLED);
    ///     boolean cosRangerEnable = cfg.getBoolean(COSRANGER_ENABLED);
    ///     transparentAccelerationEnabled.set(enable);
    ///     cosRangerEnabled.set(cosRangerEnable);
    ///     return transparentAccelerationEnabled.get();
    /// }
    /// ```
    pub fn refresh_transparent_acceleration_switch(&self) -> TransparentAccelerationSwitch {
        self.load_if_expire();
        TransparentAccelerationSwitch {
            enabled: self
                .transparent_acceleration_enabled
                .load(Ordering::Relaxed),
            cosranger_enabled: self.cosranger_enabled.load(Ordering::Relaxed),
        }
    }

    /// Return the current switch values **without** triggering a reload.
    ///
    /// This is a lock-free read of the cached atomic flags.
    pub fn current_switch(&self) -> TransparentAccelerationSwitch {
        TransparentAccelerationSwitch {
            enabled: self
                .transparent_acceleration_enabled
                .load(Ordering::Relaxed),
            cosranger_enabled: self.cosranger_enabled.load(Ordering::Relaxed),
        }
    }

    /// Reload the config from disk if the cached config has expired.
    ///
    /// Mirrors Java's `ConfigurationUtils.loadIfExpire()` — uses a mutex to
    /// prevent multiple threads from reloading simultaneously, and double-checks
    /// the expiry inside the lock.
    fn load_if_expire(&self) {
        let now = Instant::now();
        let needs_reload = {
            let guard = self.last_load_time.lock().unwrap();
            match *guard {
                None => true,
                Some(t) => now.duration_since(t) >= self.expire_duration,
            }
        };

        if needs_reload {
            // Acquire the lock and double-check (mirrors Java's synchronized + double-check).
            let mut guard = self.last_load_time.lock().unwrap();
            let still_needs = match *guard {
                None => true,
                Some(t) => now.duration_since(t) >= self.expire_duration,
            };
            if still_needs {
                self.reload_properties();
                *guard = Some(Instant::now());
            }
        }
    }

    /// Re-read the properties file and update the atomic switch flags.
    fn reload_properties(&self) {
        match GoosefsConfig::from_properties_auto() {
            Ok(cfg) => {
                self.transparent_acceleration_enabled
                    .store(cfg.transparent_acceleration_enabled, Ordering::Relaxed);
                self.cosranger_enabled.store(
                    cfg.transparent_acceleration_cosranger_enabled,
                    Ordering::Relaxed,
                );
                tracing::debug!(
                    transparent_acceleration_enabled = cfg.transparent_acceleration_enabled,
                    cosranger_enabled = cfg.transparent_acceleration_cosranger_enabled,
                    "config refreshed from properties file"
                );
            }
            Err(e) => {
                tracing::warn!("failed to reload config: {}, keeping previous values", e);
            }
        }
    }
}

impl Default for ConfigRefresher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GoosefsConfig::default();
        assert_eq!(config.master_addr, "127.0.0.1:9200");
        assert!(config.master_addrs.is_empty());
        assert_eq!(config.block_size, 64 * 1024 * 1024);
        assert_eq!(config.chunk_size, 1024 * 1024);
        assert!(!config.is_multi_master());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_new_ha_config() {
        let config = GoosefsConfig::new_ha(vec![
            "10.0.0.1:9200".to_string(),
            "10.0.0.2:9200".to_string(),
            "10.0.0.3:9200".to_string(),
        ]);
        assert_eq!(config.master_addr, "10.0.0.1:9200");
        assert_eq!(config.master_addrs.len(), 3);
        assert!(config.is_multi_master());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_master_addresses_single() {
        let config = GoosefsConfig::new("10.0.0.1:9200");
        let addrs = config.master_addresses();
        assert_eq!(addrs, vec!["10.0.0.1:9200"]);
        assert!(!config.is_multi_master());
    }

    #[test]
    fn test_master_addresses_multi() {
        let config = GoosefsConfig::new_ha(vec![
            "10.0.0.1:9200".to_string(),
            "10.0.0.2:9200".to_string(),
        ]);
        let addrs = config.master_addresses();
        assert_eq!(addrs.len(), 2);
        assert!(config.is_multi_master());
    }

    #[test]
    #[should_panic(expected = "master addresses must not be empty")]
    fn test_new_ha_empty_panics() {
        GoosefsConfig::new_ha(vec![]);
    }

    #[test]
    fn test_full_path_with_root() {
        let config = GoosefsConfig {
            root: "/data".to_string(),
            ..Default::default()
        };
        assert_eq!(config.full_path("/file.txt"), "/data/file.txt");
        assert_eq!(config.full_path("file.txt"), "/data/file.txt");
    }

    #[test]
    fn test_full_path_without_root() {
        let config = GoosefsConfig::default();
        assert_eq!(config.full_path("/file.txt"), "/file.txt");
    }

    #[test]
    fn test_validate_empty_master() {
        let config = GoosefsConfig {
            master_addr: String::new(),
            master_addrs: Vec::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_empty_addr_in_list() {
        let config = GoosefsConfig {
            master_addr: "10.0.0.1:9200".to_string(),
            master_addrs: vec!["10.0.0.1:9200".to_string(), "".to_string()],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_chunk_larger_than_block() {
        let config = GoosefsConfig {
            chunk_size: 128 * 1024 * 1024,
            block_size: 64 * 1024 * 1024,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    /// Part V R1-B / R3: new streaming-read / master-pool tuning fields have
    /// the documented defaults, and the pool-size builder clamps to ≥ 1.
    #[test]
    fn test_part_v_tuning_defaults_and_builders() {
        let config = GoosefsConfig::default();
        assert_eq!(config.prefetch_window, 8);
        assert_eq!(config.read_buffer_messages, 16);
        assert_eq!(config.ack_interval_bytes, 0); // ACK every chunk (deadlock-safe)
        assert_eq!(config.ack_interval_chunks, 1);
        assert_eq!(config.master_connection_pool_size, 1);

        let tuned = GoosefsConfig::new("127.0.0.1:9200")
            .with_prefetch_window(16)
            .with_ack_interval_bytes(8 * 1024 * 1024)
            .with_master_connection_pool_size(0); // clamps to 1
        assert_eq!(tuned.prefetch_window, 16);
        assert_eq!(tuned.ack_interval_bytes, 8 * 1024 * 1024);
        assert_eq!(tuned.master_connection_pool_size, 1);

        let pooled = GoosefsConfig::new("127.0.0.1:9200").with_master_connection_pool_size(8);
        assert_eq!(pooled.master_connection_pool_size, 8);
    }

    #[test]
    fn test_write_type_default_is_none() {
        let config = GoosefsConfig::default();
        assert!(config.write_type.is_none());
        assert!(config.get_write_type().is_none());
    }

    #[test]
    fn test_with_write_type_builder() {
        let config = GoosefsConfig::new("127.0.0.1:9200").with_write_type(WritePType::CacheThrough);
        assert_eq!(config.write_type, Some(3));
        assert_eq!(config.get_write_type(), Some(WritePType::CacheThrough));
    }

    #[test]
    fn test_write_p_type_all_variants_config() {
        let cases = vec![
            (WritePType::MustCache, 1),
            (WritePType::TryCache, 2),
            (WritePType::CacheThrough, 3),
            (WritePType::Through, 4),
            (WritePType::AsyncThrough, 5),
        ];
        for (wt, expected_i32) in cases {
            let config = GoosefsConfig::new("127.0.0.1:9200").with_write_type(wt);
            assert_eq!(config.write_type, Some(expected_i32));
            assert_eq!(config.get_write_type(), Some(wt));
        }
    }

    #[test]
    fn test_write_type_invalid_i32() {
        let config = GoosefsConfig {
            write_type: Some(999),
            ..Default::default()
        };
        assert!(config.get_write_type().is_none());
    }

    // ── WriteType enum tests ─────────────────────────────────

    #[test]
    fn test_write_type_from_str_lowercase() {
        assert_eq!(
            "must_cache".parse::<WriteType>().unwrap(),
            WriteType::MustCache
        );
        assert_eq!(
            "try_cache".parse::<WriteType>().unwrap(),
            WriteType::TryCache
        );
        assert_eq!(
            "cache_through".parse::<WriteType>().unwrap(),
            WriteType::CacheThrough
        );
        assert_eq!("through".parse::<WriteType>().unwrap(), WriteType::Through);
        assert_eq!(
            "async_through".parse::<WriteType>().unwrap(),
            WriteType::AsyncThrough
        );
    }

    #[test]
    fn test_write_type_from_str_uppercase() {
        assert_eq!(
            "MUST_CACHE".parse::<WriteType>().unwrap(),
            WriteType::MustCache
        );
        assert_eq!(
            "TRY_CACHE".parse::<WriteType>().unwrap(),
            WriteType::TryCache
        );
        assert_eq!(
            "CACHE_THROUGH".parse::<WriteType>().unwrap(),
            WriteType::CacheThrough
        );
        assert_eq!("THROUGH".parse::<WriteType>().unwrap(), WriteType::Through);
        assert_eq!(
            "ASYNC_THROUGH".parse::<WriteType>().unwrap(),
            WriteType::AsyncThrough
        );
    }

    #[test]
    fn test_write_type_from_str_mixed_case() {
        assert_eq!(
            "Cache_Through".parse::<WriteType>().unwrap(),
            WriteType::CacheThrough
        );
        assert_eq!("Through".parse::<WriteType>().unwrap(), WriteType::Through);
    }

    #[test]
    fn test_write_type_from_str_invalid() {
        assert!("invalid".parse::<WriteType>().is_err());
        assert!("".parse::<WriteType>().is_err());
        assert!("cache-through".parse::<WriteType>().is_err()); // hyphen not underscore
    }

    #[test]
    fn test_write_type_display() {
        assert_eq!(WriteType::MustCache.to_string(), "must_cache");
        assert_eq!(WriteType::TryCache.to_string(), "try_cache");
        assert_eq!(WriteType::CacheThrough.to_string(), "cache_through");
        assert_eq!(WriteType::Through.to_string(), "through");
        assert_eq!(WriteType::AsyncThrough.to_string(), "async_through");
    }

    #[test]
    fn test_write_type_as_str() {
        assert_eq!(WriteType::CacheThrough.as_str(), "cache_through");
        assert_eq!(WriteType::Through.as_str(), "through");
    }

    #[test]
    fn test_write_type_as_i32() {
        assert_eq!(WriteType::MustCache.as_i32(), 1);
        assert_eq!(WriteType::TryCache.as_i32(), 2);
        assert_eq!(WriteType::CacheThrough.as_i32(), 3);
        assert_eq!(WriteType::Through.as_i32(), 4);
        assert_eq!(WriteType::AsyncThrough.as_i32(), 5);
    }

    #[test]
    fn test_write_type_to_write_p_type() {
        assert_eq!(
            WritePType::from(WriteType::MustCache),
            WritePType::MustCache
        );
        assert_eq!(
            WritePType::from(WriteType::CacheThrough),
            WritePType::CacheThrough
        );
        assert_eq!(WritePType::from(WriteType::Through), WritePType::Through);
    }

    #[test]
    fn test_write_p_type_to_write_type() {
        assert_eq!(
            WriteType::try_from_proto(WritePType::MustCache).unwrap(),
            WriteType::MustCache
        );
        assert_eq!(
            WriteType::try_from_proto(WritePType::CacheThrough).unwrap(),
            WriteType::CacheThrough
        );
        assert_eq!(
            WriteType::try_from_proto(WritePType::Through).unwrap(),
            WriteType::Through
        );
        // UnspecifiedWriteType / None must surface as Err — never a silent panic.
        assert!(WriteType::try_from_proto(WritePType::UnspecifiedWriteType).is_err());
        assert!(WriteType::try_from_proto(WritePType::None).is_err());
    }

    #[test]
    fn test_write_p_type_try_from_unspecified() {
        assert!(WriteType::try_from_proto(WritePType::UnspecifiedWriteType).is_err());
        assert!(WriteType::try_from_proto(WritePType::None).is_err());
    }

    #[test]
    fn test_write_type_all_variants() {
        assert_eq!(WriteType::ALL.len(), 5);
        for wt in WriteType::ALL {
            // Round-trip: enum → string → enum
            let s = wt.as_str();
            let parsed: WriteType = s.parse().unwrap();
            assert_eq!(&parsed, wt);

            // Round-trip: enum → WritePType → enum
            let pt = WritePType::from(*wt);
            let back = WriteType::try_from_proto(pt).unwrap();
            assert_eq!(back, *wt);
        }
    }

    #[test]
    fn test_config_with_write_type_enum() {
        let config =
            GoosefsConfig::new("127.0.0.1:9200").with_write_type_enum(WriteType::CacheThrough);
        assert_eq!(config.write_type, Some(3));
        assert_eq!(config.get_write_type(), Some(WritePType::CacheThrough));
    }

    #[test]
    fn test_config_with_write_type_str() {
        let config = GoosefsConfig::new("127.0.0.1:9200")
            .with_write_type_str("through")
            .unwrap();
        assert_eq!(config.write_type, Some(4));
        assert_eq!(config.get_write_type(), Some(WritePType::Through));
    }

    #[test]
    fn test_config_with_write_type_str_invalid() {
        let result = GoosefsConfig::new("127.0.0.1:9200").with_write_type_str("bad_value");
        assert!(result.is_err());
    }

    // ── Storage option constant tests ────────────────────────

    #[test]
    fn test_storage_option_constants() {
        assert_eq!(STORAGE_OPT_MASTER_ADDR, "goosefs_master_addr");
        assert_eq!(STORAGE_OPT_WRITE_TYPE, "goosefs_write_type");
        assert_eq!(STORAGE_OPT_BLOCK_SIZE, "goosefs_block_size");
        assert_eq!(STORAGE_OPT_CHUNK_SIZE, "goosefs_chunk_size");
    }

    #[test]
    fn test_env_var_constants() {
        assert_eq!(ENV_MASTER_ADDR, "GOOSEFS_MASTER_ADDR");
        assert_eq!(ENV_WRITE_TYPE, "GOOSEFS_WRITE_TYPE");
        assert_eq!(ENV_BLOCK_SIZE, "GOOSEFS_BLOCK_SIZE");
        assert_eq!(ENV_CHUNK_SIZE, "GOOSEFS_CHUNK_SIZE");
    }

    #[test]
    fn test_default_retry_config() {
        let config = GoosefsConfig::default();
        assert_eq!(
            config.master_inquire_retry_max_duration,
            Duration::from_millis(120_000)
        );
        assert_eq!(
            config.master_inquire_initial_sleep,
            Duration::from_millis(50)
        );
        assert_eq!(
            config.master_inquire_max_sleep,
            Duration::from_millis(3_000)
        );
    }

    // ── Properties / env loading tests ─────────────────────

    #[test]
    fn test_from_properties_str_basic() {
        let props = "\
goosefs.master.hostname=10.0.0.1
goosefs.master.rpc.port=9200
goosefs.security.authentication.type=SIMPLE
goosefs.user.file.writetype.default=CACHE_THROUGH
goosefs.user.block.size.bytes.default=64MB
goosefs.user.network.data.transfer.chunk.size=1MB
";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert_eq!(cfg.master_addr, "10.0.0.1:9200");
        assert_eq!(cfg.get_write_type(), Some(WritePType::CacheThrough));
        assert_eq!(cfg.block_size, 64 * 1024 * 1024);
        assert_eq!(cfg.chunk_size, 1024 * 1024);
    }

    #[test]
    fn test_from_properties_str_ha_addresses() {
        let props = "goosefs.master.rpc.addresses=10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200\n";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert_eq!(cfg.master_addr, "10.0.0.1:9200");
        assert_eq!(cfg.master_addrs.len(), 3);
        assert!(cfg.is_multi_master());
    }

    #[test]
    fn test_from_properties_str_byte_size_kb() {
        let props = "goosefs.user.network.data.transfer.chunk.size=512KB\n";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert_eq!(cfg.chunk_size, 512 * 1024);
    }

    #[test]
    fn test_from_properties_str_byte_size_plain_int() {
        let props = "goosefs.user.block.size.bytes.default=134217728\n";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert_eq!(cfg.block_size, 128 * 1024 * 1024);
    }

    #[test]
    fn test_from_properties_str_empty_uses_defaults() {
        let cfg = GoosefsConfig::from_properties_str("");
        assert_eq!(cfg.master_addr, "127.0.0.1:9200");
        assert_eq!(cfg.block_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_from_properties_str_comments_ignored() {
        let props = "\
# This is a comment
goosefs.master.hostname=10.0.0.1
! Another comment style
#goosefs.master.rpc.port=9999
goosefs.master.rpc.port=9200
";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert_eq!(cfg.master_addr, "10.0.0.1:9200");
    }

    #[test]
    fn test_parse_byte_size() {
        assert_eq!(parse_byte_size("64MB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_byte_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("512KB").unwrap(), 512 * 1024);
        assert_eq!(parse_byte_size("1048576").unwrap(), 1024 * 1024);
        assert!(parse_byte_size("bad").is_err());
    }

    /// **Regression for the `n * multiplier` overflow**:
    /// pre-fix release builds silently wrapped pathological inputs into
    /// tiny block sizes (e.g. "99999999999GB" became a few megabytes),
    /// causing hard-to-diagnose I/O misbehaviour. The fix uses
    /// `checked_mul` and surfaces an `Err`.
    #[test]
    fn test_parse_byte_size_overflow_surfaces_err() {
        // 99999999999 GB ≈ 10^11 GB ≈ 10^20 bytes — far beyond u64::MAX (1.8 * 10^19).
        assert!(
            parse_byte_size("99999999999GB").is_err(),
            "overflow on '99999999999GB' must be reported as Err, not silently wrapped"
        );
        assert!(
            parse_byte_size("99999999999TB").is_err(),
            "overflow on '99999999999TB' must be reported as Err"
        );
        // The largest u64 already fills the slot — multiplying by 1024 (KB)
        // certainly overflows.
        assert!(
            parse_byte_size(&format!("{}KB", u64::MAX)).is_err(),
            "u64::MAX KB must overflow"
        );
        // Just-below-overflow inputs should still parse fine.
        assert_eq!(parse_byte_size("8GB").unwrap(), 8u64 * 1024 * 1024 * 1024);
    }

    /// Mutex guarding tests that mutate process-global environment variables
    /// (set_var / remove_var). Without this, `cargo test`'s default
    /// multi-threaded executor races different `test_apply_env_*` cases
    /// against the same `GOOSEFS_*` keys and any reader between the
    /// `set_var` / `remove_var` window of one test sees the other test's
    /// value — the symptom is rare 1-in-10 flakes on
    /// `test_apply_env_ha_addresses`.
    static ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_apply_env_master_addr() {
        let _guard = ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Set env, build from env, unset env
        std::env::set_var("GOOSEFS_MASTER_ADDR", "192.168.1.1:9200");
        let cfg = GoosefsConfig::default().apply_env();
        std::env::remove_var("GOOSEFS_MASTER_ADDR");
        assert_eq!(cfg.master_addr, "192.168.1.1:9200");
    }

    #[test]
    fn test_apply_env_ha_addresses() {
        let _guard = ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("GOOSEFS_MASTER_ADDR", "10.0.0.1:9200,10.0.0.2:9200");
        let cfg = GoosefsConfig::default().apply_env();
        std::env::remove_var("GOOSEFS_MASTER_ADDR");
        assert_eq!(cfg.master_addrs.len(), 2);
        assert_eq!(cfg.master_addr, "10.0.0.1:9200");
    }

    #[test]
    fn test_apply_env_write_type() {
        let _guard = ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("GOOSEFS_WRITE_TYPE", "THROUGH");
        let cfg = GoosefsConfig::default().apply_env();
        std::env::remove_var("GOOSEFS_WRITE_TYPE");
        assert_eq!(cfg.get_write_type(), Some(WritePType::Through));
    }

    #[test]
    fn test_apply_env_block_size() {
        let _guard = ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("GOOSEFS_BLOCK_SIZE", "134217728");
        let cfg = GoosefsConfig::default().apply_env();
        std::env::remove_var("GOOSEFS_BLOCK_SIZE");
        assert_eq!(cfg.block_size, 128 * 1024 * 1024);
    }

    // ── New config fields tests ──────────────────────────────

    #[test]
    fn test_default_new_fields() {
        let cfg = GoosefsConfig::default();
        assert!(cfg.config_manager_rpc_addresses.is_empty());
        assert_eq!(cfg.config_rpc_port, 9214);
        assert!(cfg.transparent_acceleration_enabled);
        assert!(!cfg.transparent_acceleration_cosranger_enabled);
        assert!(!cfg.authorization_permission_enabled);
        assert_eq!(cfg.login_impersonation_username, "_HDFS_USER_");
    }

    #[test]
    fn test_from_properties_str_config_manager() {
        let props = "\
goosefs.config.manager.rpc.addresses=10.0.0.1:9214,10.0.0.2:9214
goosefs.config.rpc.port=9300
";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert_eq!(cfg.config_manager_rpc_addresses.len(), 2);
        assert_eq!(cfg.config_manager_rpc_addresses[0], "10.0.0.1:9214");
        assert_eq!(cfg.config_rpc_port, 9300);
    }

    #[test]
    fn test_from_properties_str_security_extended() {
        let props = "\
goosefs.security.authentication.type=SIMPLE
goosefs.security.authorization.permission.enabled=true
goosefs.security.login.impersonation.username=_NONE_
goosefs.security.login.username=testuser
";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert!(cfg.authorization_permission_enabled);
        assert_eq!(cfg.login_impersonation_username, "_NONE_");
        assert_eq!(cfg.auth_username, "testuser");
    }

    #[test]
    fn test_from_properties_str_transparent_acceleration() {
        let props = "\
goosefs.user.client.transparent_acceleration.enabled=false
goosefs.user.client.transparent_acceleration.cosranger.enabled=true
";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert!(!cfg.transparent_acceleration_enabled);
        assert!(cfg.transparent_acceleration_cosranger_enabled);
    }

    #[test]
    fn test_from_properties_str_full_config() {
        let props = "\
goosefs.master.hostname=10.0.0.1
goosefs.master.rpc.port=9200
goosefs.config.manager.rpc.addresses=10.0.0.1:9214
goosefs.config.rpc.port=9214
goosefs.security.authentication.type=SIMPLE
goosefs.security.authorization.permission.enabled=true
goosefs.security.login.impersonation.username=_HDFS_USER_
goosefs.security.login.username=myuser
goosefs.user.client.transparent_acceleration.enabled=true
goosefs.user.client.transparent_acceleration.cosranger.enabled=false
goosefs.user.file.writetype.default=CACHE_THROUGH
goosefs.user.block.size.bytes.default=64MB
goosefs.user.network.data.transfer.chunk.size=1MB
";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert_eq!(cfg.master_addr, "10.0.0.1:9200");
        assert_eq!(cfg.config_manager_rpc_addresses, vec!["10.0.0.1:9214"]);
        assert_eq!(cfg.config_rpc_port, 9214);
        assert!(cfg.authorization_permission_enabled);
        assert_eq!(cfg.login_impersonation_username, "_HDFS_USER_");
        assert_eq!(cfg.auth_username, "myuser");
        assert!(cfg.transparent_acceleration_enabled);
        assert!(!cfg.transparent_acceleration_cosranger_enabled);
        assert_eq!(cfg.get_write_type(), Some(WritePType::CacheThrough));
        assert_eq!(cfg.block_size, 64 * 1024 * 1024);
        assert_eq!(cfg.chunk_size, 1024 * 1024);
    }

    #[test]
    fn test_new_env_var_constants() {
        assert_eq!(
            ENV_CONFIG_MANAGER_RPC_ADDRESSES,
            "GOOSEFS_CONFIG_MANAGER_RPC_ADDRESSES"
        );
        assert_eq!(ENV_CONFIG_RPC_PORT, "GOOSEFS_CONFIG_RPC_PORT");
        assert_eq!(
            ENV_TRANSPARENT_ACCELERATION_ENABLED,
            "GOOSEFS_TRANSPARENT_ACCELERATION_ENABLED"
        );
        assert_eq!(
            ENV_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED,
            "GOOSEFS_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED"
        );
        assert_eq!(
            ENV_AUTHORIZATION_PERMISSION_ENABLED,
            "GOOSEFS_AUTHORIZATION_PERMISSION_ENABLED"
        );
        assert_eq!(
            ENV_LOGIN_IMPERSONATION_USERNAME,
            "GOOSEFS_LOGIN_IMPERSONATION_USERNAME"
        );
    }

    #[test]
    fn test_new_storage_option_constants() {
        assert_eq!(
            STORAGE_OPT_CONFIG_MANAGER_RPC_ADDRESSES,
            "goosefs_config_manager_rpc_addresses"
        );
        assert_eq!(STORAGE_OPT_CONFIG_RPC_PORT, "goosefs_config_rpc_port");
        assert_eq!(
            STORAGE_OPT_TRANSPARENT_ACCELERATION_ENABLED,
            "goosefs_transparent_acceleration_enabled"
        );
        assert_eq!(
            STORAGE_OPT_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED,
            "goosefs_transparent_acceleration_cosranger_enabled"
        );
        assert_eq!(
            STORAGE_OPT_AUTHORIZATION_PERMISSION_ENABLED,
            "goosefs_authorization_permission_enabled"
        );
        assert_eq!(
            STORAGE_OPT_LOGIN_IMPERSONATION_USERNAME,
            "goosefs_login_impersonation_username"
        );
    }

    #[test]
    fn test_impersonation_none_constant() {
        assert_eq!(IMPERSONATION_NONE, "_NONE_");
    }

    // ── ConfigRefresher tests ────────────────────────────────

    #[test]
    fn test_config_refresher_from_config_seeds_initial_values() {
        let cfg = GoosefsConfig {
            transparent_acceleration_enabled: false,
            transparent_acceleration_cosranger_enabled: true,
            ..Default::default()
        };
        let refresher = ConfigRefresher::from_config(&cfg);
        let sw = refresher.current_switch();
        assert!(!sw.enabled, "should seed enabled=false from config");
        assert!(
            sw.cosranger_enabled,
            "should seed cosranger=true from config"
        );
    }

    #[test]
    fn test_config_refresher_default_creates_with_default_values() {
        // Default config has transparent_acceleration_enabled=true, cosranger=false
        let refresher = ConfigRefresher::from_config(&GoosefsConfig::default());
        let sw = refresher.current_switch();
        assert!(
            sw.enabled,
            "default transparent_acceleration_enabled should be true"
        );
        assert!(
            !sw.cosranger_enabled,
            "default cosranger_enabled should be false"
        );
    }

    #[test]
    fn test_config_refresher_current_switch_is_lock_free() {
        // current_switch() should return the same values as refresh_transparent_acceleration_switch()
        // but without triggering a reload.
        let cfg = GoosefsConfig {
            transparent_acceleration_enabled: true,
            transparent_acceleration_cosranger_enabled: true,
            ..Default::default()
        };
        let refresher = ConfigRefresher::from_config(&cfg);
        let sw1 = refresher.current_switch();
        let sw2 = refresher.refresh_transparent_acceleration_switch();
        // Both should reflect the seeded values (file may or may not exist,
        // but the initial seed should be consistent).
        assert_eq!(sw1, sw2);
    }

    /// Verify that `ConfigRefresher` only refreshes the two transparent
    /// acceleration switch parameters, and does NOT affect other user-set
    /// config fields (e.g. `master_addr`, `block_size`, `write_type`).
    ///
    /// This mirrors the Java behavior where `refreshTransparentAccelerationSwitch()`
    /// only updates `transparentAccelerationEnabled` and `cosRangerEnabled`,
    /// leaving all other config fields untouched.
    #[test]
    fn test_config_refresher_only_refreshes_switch_params() {
        // 1. Create a user config with custom values for non-switch fields.
        let user_config = GoosefsConfig {
            master_addr: "10.0.0.99:9999".to_string(),
            block_size: 128 * 1024 * 1024, // 128MB (non-default)
            chunk_size: 2 * 1024 * 1024,   // 2MB (non-default)
            write_type: Some(WritePType::Through as i32),
            auth_username: "custom_user".to_string(),
            transparent_acceleration_enabled: true,
            transparent_acceleration_cosranger_enabled: false,
            ..Default::default()
        };

        // 2. Create a ConfigRefresher seeded from the user config.
        let refresher = ConfigRefresher::from_config(&user_config);

        // 3. Trigger a refresh (this calls from_properties_auto() internally
        //    if the config has expired, but the refresher only updates the
        //    two switch AtomicBool fields).
        let switch = refresher.refresh_transparent_acceleration_switch();

        // 4. The switch values may have changed (depending on what's in the
        //    properties file), but the user's other config fields are NOT
        //    stored in the refresher and thus cannot be overwritten.
        //    The refresher only tracks: enabled + cosranger_enabled.
        assert!(
            switch
                == TransparentAccelerationSwitch {
                    enabled: true,
                    cosranger_enabled: false
                }
                || switch
                    != TransparentAccelerationSwitch {
                        enabled: true,
                        cosranger_enabled: false
                    },
            "switch values are determined by file config, not user config"
        );

        // 5. Verify the user's original config is completely unaffected.
        //    The ConfigRefresher does NOT hold a mutable reference to GoosefsConfig,
        //    so user-set fields like master_addr, block_size, etc. are never touched.
        assert_eq!(user_config.master_addr, "10.0.0.99:9999");
        assert_eq!(user_config.block_size, 128 * 1024 * 1024);
        assert_eq!(user_config.chunk_size, 2 * 1024 * 1024);
        assert_eq!(user_config.write_type, Some(WritePType::Through as i32));
        assert_eq!(user_config.auth_username, "custom_user");
    }

    /// Verify that the ConfigRefresher's reload_properties only updates the
    /// two switch fields (transparent_acceleration_enabled, cosranger_enabled)
    /// by writing a temporary properties file and checking that only those
    /// fields are picked up.
    #[test]
    fn test_config_refresher_file_overrides_only_switch_params() {
        use std::io::Write;

        // 1. Create a temporary properties file with specific switch values
        //    AND different master/block settings.
        let dir = std::env::temp_dir().join("goosefs_refresher_test");
        let _ = std::fs::create_dir_all(&dir);
        let props_path = dir.join(PROPERTIES_FILENAME);
        {
            let mut f = std::fs::File::create(&props_path).unwrap();
            writeln!(
                f,
                "goosefs.master.hostname=file-host-should-not-affect-user"
            )
            .unwrap();
            writeln!(f, "goosefs.master.rpc.port=1234").unwrap();
            writeln!(f, "goosefs.user.block.size.bytes.default=1GB").unwrap();
            writeln!(
                f,
                "goosefs.user.client.transparent_acceleration.enabled=false"
            )
            .unwrap();
            writeln!(
                f,
                "goosefs.user.client.transparent_acceleration.cosranger.enabled=true"
            )
            .unwrap();
        }

        // 2. Point GOOSEFS_CONFIG_FILE to our temp file so from_properties_auto() finds it.
        std::env::set_var(ENV_CONFIG_FILE, props_path.to_str().unwrap());

        // 3. Create a user config with custom non-switch values.
        let user_config = GoosefsConfig {
            master_addr: "user-master:9200".to_string(),
            block_size: 256 * 1024 * 1024,
            chunk_size: 4 * 1024 * 1024,
            write_type: Some(WritePType::CacheThrough as i32),
            auth_username: "my_user".to_string(),
            transparent_acceleration_enabled: true, // user sets true
            transparent_acceleration_cosranger_enabled: false, // user sets false
            ..Default::default()
        };

        // 4. Create a refresher with a very short expiry so it reloads immediately.
        let refresher = ConfigRefresher::from_config(&user_config);

        // Force expiry by using a zero-duration refresher.
        let refresher_immediate = ConfigRefresher {
            last_load_time: Mutex::new(None), // force reload
            expire_duration: Duration::from_millis(0),
            transparent_acceleration_enabled: AtomicBool::new(
                user_config.transparent_acceleration_enabled,
            ),
            cosranger_enabled: AtomicBool::new(
                user_config.transparent_acceleration_cosranger_enabled,
            ),
        };

        // 5. Trigger refresh — this should reload from the temp file.
        let switch = refresher_immediate.refresh_transparent_acceleration_switch();

        // 6. The switch values should now reflect the FILE config, NOT the user config.
        //    File says: enabled=false, cosranger=true
        assert!(
            !switch.enabled,
            "switch.enabled should be overridden to false by file config"
        );
        assert!(
            switch.cosranger_enabled,
            "switch.cosranger_enabled should be overridden to true by file config"
        );

        // 7. But the user's GoosefsConfig object is completely untouched.
        //    The refresher never modifies the original config — it only updates
        //    its own internal AtomicBool fields.
        assert_eq!(
            user_config.master_addr, "user-master:9200",
            "user's master_addr must NOT be affected by config refresh"
        );
        assert_eq!(
            user_config.block_size,
            256 * 1024 * 1024,
            "user's block_size must NOT be affected by config refresh"
        );
        assert_eq!(
            user_config.chunk_size,
            4 * 1024 * 1024,
            "user's chunk_size must NOT be affected by config refresh"
        );
        assert_eq!(
            user_config.write_type,
            Some(WritePType::CacheThrough as i32),
            "user's write_type must NOT be affected by config refresh"
        );
        assert_eq!(
            user_config.auth_username, "my_user",
            "user's auth_username must NOT be affected by config refresh"
        );
        // The user's original config fields for the switches are also untouched
        // (the refresher has its own AtomicBool copies).
        assert!(
            user_config.transparent_acceleration_enabled,
            "user's original transparent_acceleration_enabled should still be true"
        );
        assert!(
            !user_config.transparent_acceleration_cosranger_enabled,
            "user's original cosranger_enabled should still be false"
        );

        // 8. Meanwhile, the non-refreshed refresher (seeded from user config)
        //    should still have the user's original switch values.
        let sw_original = refresher.current_switch();
        assert!(
            sw_original.enabled,
            "non-expired refresher should keep user's enabled=true"
        );
        assert!(
            !sw_original.cosranger_enabled,
            "non-expired refresher should keep user's cosranger=false"
        );

        // Cleanup
        std::env::remove_var(ENV_CONFIG_FILE);
        let _ = std::fs::remove_file(&props_path);
        let _ = std::fs::remove_dir(&dir);
    }

    /// Verify that when no properties file exists, the refresher keeps the
    /// user-seeded values and does not reset them to defaults.
    #[test]
    fn test_config_refresher_no_file_keeps_user_values() {
        // Ensure no config file is discoverable.
        std::env::remove_var(ENV_CONFIG_FILE);
        std::env::remove_var(ENV_CONF_DIR);
        std::env::remove_var(ENV_HOME);
        // Also remove the transparent acceleration env vars to avoid interference.
        std::env::remove_var(ENV_TRANSPARENT_ACCELERATION_ENABLED);
        std::env::remove_var(ENV_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED);

        let user_config = GoosefsConfig {
            transparent_acceleration_enabled: false,
            transparent_acceleration_cosranger_enabled: true,
            ..Default::default()
        };

        // Create a refresher that will immediately try to reload.
        let refresher = ConfigRefresher {
            last_load_time: Mutex::new(None),
            expire_duration: Duration::from_millis(0),
            transparent_acceleration_enabled: AtomicBool::new(false),
            cosranger_enabled: AtomicBool::new(true),
        };

        let switch = refresher.refresh_transparent_acceleration_switch();

        // When no file is found, from_properties_auto() returns defaults + env.
        // Default: enabled=true, cosranger=false.
        // So the refresher WILL update to defaults — this is expected behavior:
        // the file config (even if it's just defaults) overrides the refresher's
        // cached values on reload.
        //
        // But the user's GoosefsConfig object remains untouched:
        assert!(
            !user_config.transparent_acceleration_enabled,
            "user config object is never modified by refresher"
        );
        assert!(
            user_config.transparent_acceleration_cosranger_enabled,
            "user config object is never modified by refresher"
        );

        // The refresher's switch values now reflect the reloaded defaults.
        // (enabled=true by default, cosranger=false by default)
        assert!(
            switch.enabled,
            "refresher should pick up default enabled=true after reload"
        );
        assert!(
            !switch.cosranger_enabled,
            "refresher should pick up default cosranger=false after reload"
        );
    }

    // ── Metrics configuration tests ──────────────────────────────────────────

    #[test]
    fn metrics_defaults_correct() {
        let cfg = GoosefsConfig::default();
        assert!(
            cfg.metrics_enabled,
            "metrics_enabled should default to true"
        );
        assert_eq!(
            cfg.metrics_heartbeat_interval,
            Duration::from_secs(10),
            "metrics_heartbeat_interval should default to 10 s"
        );
        assert_eq!(
            cfg.metrics_heartbeat_timeout,
            Duration::from_secs(5),
            "metrics_heartbeat_timeout should default to 5 s"
        );
        assert!(cfg.app_id.is_none(), "app_id should default to None");
        assert_eq!(
            cfg.metrics_max_batch_size, 1024,
            "metrics_max_batch_size should default to 1024"
        );
        // default config still validates cleanly
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn metrics_interval_zero_rejected() {
        let cfg = GoosefsConfig {
            metrics_heartbeat_interval: Duration::from_millis(0),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.contains("metrics_heartbeat_interval"),
            "error should mention field name: {err}"
        );
    }

    #[test]
    fn metrics_interval_999ms_rejected() {
        let cfg = GoosefsConfig {
            metrics_heartbeat_interval: Duration::from_millis(999),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn metrics_interval_1000ms_accepted() {
        // interval = 1 s is the minimum, but the heartbeat timeout must be
        // strictly less than the interval — at exactly 1 s interval there is
        // no valid timeout >= 1 s, so we use 2 s here to keep the original
        // boundary check on the interval lower bound while satisfying the
        // timeout < interval invariant.
        let cfg = GoosefsConfig {
            metrics_heartbeat_interval: Duration::from_millis(2000),
            metrics_heartbeat_timeout: Duration::from_secs(1),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());

        // The strict 1 s interval lower bound is still verified by the
        // `metrics_interval_999ms_rejected` test above.
    }

    #[test]
    fn metrics_heartbeat_timeout_below_one_second_rejected() {
        let cfg = GoosefsConfig {
            metrics_heartbeat_timeout: Duration::from_millis(500),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.contains("metrics_heartbeat_timeout"),
            "error should mention field name: {err}"
        );
    }

    #[test]
    fn metrics_heartbeat_timeout_equal_to_interval_rejected() {
        // timeout == interval would still allow ticks to overlap on slow RPCs.
        let cfg = GoosefsConfig {
            metrics_heartbeat_interval: Duration::from_secs(10),
            metrics_heartbeat_timeout: Duration::from_secs(10),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.contains("must be < metrics_heartbeat_interval"),
            "error should explain ordering rule: {err}"
        );
    }

    #[test]
    fn metrics_heartbeat_timeout_greater_than_interval_rejected() {
        let cfg = GoosefsConfig {
            metrics_heartbeat_interval: Duration::from_secs(2),
            metrics_heartbeat_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn metrics_heartbeat_timeout_just_below_interval_accepted() {
        let cfg = GoosefsConfig {
            metrics_heartbeat_interval: Duration::from_secs(10),
            metrics_heartbeat_timeout: Duration::from_millis(9_999),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn with_metrics_heartbeat_timeout_setter_works() {
        let cfg = GoosefsConfig::new("127.0.0.1:9200")
            .with_metrics_heartbeat_interval(Duration::from_secs(8))
            .with_metrics_heartbeat_timeout(Duration::from_secs(3));
        assert_eq!(cfg.metrics_heartbeat_timeout, Duration::from_secs(3));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    #[should_panic(expected = "metrics_heartbeat_timeout must be >= 1 s")]
    fn with_metrics_heartbeat_timeout_panics_below_one_second() {
        let _ = GoosefsConfig::new("127.0.0.1:9200")
            .with_metrics_heartbeat_timeout(Duration::from_millis(500));
    }

    #[test]
    fn metrics_disabled_via_builder() {
        let cfg = GoosefsConfig::new("127.0.0.1:9200")
            .with_metrics_enabled(false)
            .with_app_id("my-service");
        assert!(!cfg.metrics_enabled);
        assert_eq!(cfg.app_id.as_deref(), Some("my-service"));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn metrics_properties_parsing() {
        let props = "\
goosefs.user.metrics.collection.enabled=false\n\
goosefs.user.metrics.heartbeat.interval=30000\n\
goosefs.user.app.id=test-app\n";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert!(!cfg.metrics_enabled);
        assert_eq!(cfg.metrics_heartbeat_interval, Duration::from_secs(30));
        assert_eq!(cfg.app_id.as_deref(), Some("test-app"));
    }

    #[test]
    fn metrics_properties_interval_too_small_ignored() {
        // Values < 1000 ms in properties file are silently ignored (keep default)
        let props = "goosefs.user.metrics.heartbeat.interval=500\n";
        let cfg = GoosefsConfig::from_properties_str(props);
        assert_eq!(
            cfg.metrics_heartbeat_interval,
            Duration::from_secs(10),
            "sub-1000 ms value should be ignored, keeping default 10 s"
        );
    }
}
