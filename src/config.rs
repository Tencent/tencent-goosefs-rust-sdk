//! Client configuration for GooseFS gRPC connections.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::proto::grpc::file::WritePType;

// ── Default constants ────────────────────────────────────────

/// Default GooseFS Master RPC port.
const DEFAULT_MASTER_PORT: u16 = 9200;
/// Default GooseFS Worker data port.
#[allow(dead_code)]
const DEFAULT_WORKER_PORT: u16 = 9203;
/// Default block size: 64 MiB (matches GooseFS default).
const DEFAULT_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
/// Default chunk size for streaming reads: 1 MiB.
const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024;
/// Default connect timeout: 30 seconds.
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 30_000;
/// Default request timeout: 5 minutes.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 300_000;
/// Default master polling timeout: 30 seconds (mirrors Java `USER_MASTER_POLLING_TIMEOUT`).
const DEFAULT_MASTER_POLLING_TIMEOUT_MS: u64 = 30_000;

/// Default max duration for master inquire retry: 2 minutes.
const DEFAULT_MASTER_INQUIRE_MAX_DURATION_MS: u64 = 120_000;
/// Default initial sleep for master inquire retry: 50 ms.
const DEFAULT_MASTER_INQUIRE_INITIAL_SLEEP_MS: u64 = 50;
/// Default max sleep for master inquire retry: 3 seconds.
const DEFAULT_MASTER_INQUIRE_MAX_SLEEP_MS: u64 = 3_000;

// ── Storage option key constants ─────────────────────────────
//
// These are the canonical key names used in `storage_options` maps
// (e.g. Lance's `DatasetBuilder::with_storage_option` or OpenDAL config).
// Using these constants avoids hard-coded "magic strings" scattered across
// the codebase and test code.

/// Storage option key for GooseFS master address(es).
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

/// Environment variable: GooseFS master address(es).
pub const ENV_MASTER_ADDR: &str = "GOOSEFS_MASTER_ADDR";

/// Environment variable: default write type.
pub const ENV_WRITE_TYPE: &str = "GOOSEFS_WRITE_TYPE";

/// Environment variable: block size.
pub const ENV_BLOCK_SIZE: &str = "GOOSEFS_BLOCK_SIZE";

/// Environment variable: chunk size.
pub const ENV_CHUNK_SIZE: &str = "GOOSEFS_CHUNK_SIZE";

// ── WriteType: ergonomic Rust enum wrapping WritePType ───────

/// High-level write type for GooseFS file creation.
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
/// use goosefs_client::config::WriteType;
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
/// use goosefs_client::WritePType;
/// assert_eq!(WritePType::from(wt), WritePType::CacheThrough);
///
/// // Convert from protobuf WritePType
/// assert_eq!(WriteType::from(WritePType::Through), WriteType::Through);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteType {
    /// Write to GooseFS cache only; no UFS persistence.
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

/// Convenience: `WritePType` → `WriteType` (panics on Unspecified/None).
impl From<WritePType> for WriteType {
    fn from(pt: WritePType) -> Self {
        Self::try_from_proto(pt).expect("cannot convert Unspecified/None WritePType to WriteType")
    }
}

/// Configuration for the GooseFS Rust gRPC client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GooseFsConfig {
    /// Primary master address in `host:port` format (backward-compatible).
    ///
    /// When only a single master is used, set this field.
    /// For HA deployments, use [`master_addrs`] instead (or both — `master_addr`
    /// is automatically included if `master_addrs` is also provided).
    pub master_addr: String,

    /// Multiple master addresses for HA deployments.
    ///
    /// When this list contains more than one address, the client will
    /// automatically use [`PollingMasterInquireClient`] to discover the
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
    /// - `1` (`MustCache`) — Write to GooseFS cache only, no UFS persistence.
    /// - `2` (`TryCache`) — Try to cache; fall back to THROUGH if cache is full.
    /// - `3` (`CacheThrough`) — Write to cache AND synchronously persist to UFS.
    /// - `4` (`Through`) — Write directly to UFS, bypass cache.
    /// - `5` (`AsyncThrough`) — Write to cache, asynchronously persist to UFS.
    ///
    /// If not set (`None`), the server-side default is used (typically `MustCache`).
    /// Use [`GooseFsConfig::with_write_type`] for a type-safe builder.
    pub write_type: Option<i32>,

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
    /// This is independent of [`connect_timeout`] — it controls only the
    /// `getServiceVersion` probe used to discover the Primary Master.
    /// Mirrors Java's `goosefs.user.master.polling.timeout`.
    #[serde(default = "default_master_polling_timeout")]
    pub master_polling_timeout: Duration,
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

impl Default for GooseFsConfig {
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
            master_inquire_retry_max_duration: default_master_inquire_max_duration(),
            master_inquire_initial_sleep: default_master_inquire_initial_sleep(),
            master_inquire_max_sleep: default_master_inquire_max_sleep(),
            master_polling_timeout: default_master_polling_timeout(),
        }
    }
}

impl GooseFsConfig {
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
    /// - 1 address  → single-master (same as [`new`]).
    /// - 2+ addresses → multi-master (same as [`new_ha`]).
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
    /// If [`master_addrs`] is non-empty, returns it directly.
    /// Otherwise, returns a single-element list containing [`master_addr`].
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

    /// Set the default write type using the protobuf `WritePType` enum.
    ///
    /// # Example
    /// ```
    /// use goosefs_client::config::GooseFsConfig;
    /// use goosefs_client::WritePType;
    ///
    /// let config = GooseFsConfig::new("127.0.0.1:9200")
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
    /// use goosefs_client::config::{GooseFsConfig, WriteType};
    ///
    /// let config = GooseFsConfig::new("127.0.0.1:9200")
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
    /// use goosefs_client::config::GooseFsConfig;
    ///
    /// let config = GooseFsConfig::new("127.0.0.1:9200")
    ///     .with_write_type_str("cache_through")
    ///     .unwrap();
    /// ```
    pub fn with_write_type_str(self, wt: &str) -> Result<Self, String> {
        let write_type: WriteType = wt.parse()?;
        Ok(self.with_write_type_enum(write_type))
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GooseFsConfig::default();
        assert_eq!(config.master_addr, "127.0.0.1:9200");
        assert!(config.master_addrs.is_empty());
        assert_eq!(config.block_size, 64 * 1024 * 1024);
        assert_eq!(config.chunk_size, 1024 * 1024);
        assert!(!config.is_multi_master());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_new_ha_config() {
        let config = GooseFsConfig::new_ha(vec![
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
        let config = GooseFsConfig::new("10.0.0.1:9200");
        let addrs = config.master_addresses();
        assert_eq!(addrs, vec!["10.0.0.1:9200"]);
        assert!(!config.is_multi_master());
    }

    #[test]
    fn test_master_addresses_multi() {
        let config = GooseFsConfig::new_ha(vec![
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
        GooseFsConfig::new_ha(vec![]);
    }

    #[test]
    fn test_full_path_with_root() {
        let config = GooseFsConfig {
            root: "/data".to_string(),
            ..Default::default()
        };
        assert_eq!(config.full_path("/file.txt"), "/data/file.txt");
        assert_eq!(config.full_path("file.txt"), "/data/file.txt");
    }

    #[test]
    fn test_full_path_without_root() {
        let config = GooseFsConfig::default();
        assert_eq!(config.full_path("/file.txt"), "/file.txt");
    }

    #[test]
    fn test_validate_empty_master() {
        let config = GooseFsConfig {
            master_addr: String::new(),
            master_addrs: Vec::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_empty_addr_in_list() {
        let config = GooseFsConfig {
            master_addr: "10.0.0.1:9200".to_string(),
            master_addrs: vec!["10.0.0.1:9200".to_string(), "".to_string()],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_chunk_larger_than_block() {
        let config = GooseFsConfig {
            chunk_size: 128 * 1024 * 1024,
            block_size: 64 * 1024 * 1024,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_write_type_default_is_none() {
        let config = GooseFsConfig::default();
        assert!(config.write_type.is_none());
        assert!(config.get_write_type().is_none());
    }

    #[test]
    fn test_with_write_type_builder() {
        let config = GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::CacheThrough);
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
            let config = GooseFsConfig::new("127.0.0.1:9200").with_write_type(wt);
            assert_eq!(config.write_type, Some(expected_i32));
            assert_eq!(config.get_write_type(), Some(wt));
        }
    }

    #[test]
    fn test_write_type_invalid_i32() {
        let config = GooseFsConfig {
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
        assert_eq!(WriteType::from(WritePType::MustCache), WriteType::MustCache);
        assert_eq!(
            WriteType::from(WritePType::CacheThrough),
            WriteType::CacheThrough
        );
        assert_eq!(WriteType::from(WritePType::Through), WriteType::Through);
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
            let back = WriteType::from(pt);
            assert_eq!(back, *wt);
        }
    }

    #[test]
    fn test_config_with_write_type_enum() {
        let config =
            GooseFsConfig::new("127.0.0.1:9200").with_write_type_enum(WriteType::CacheThrough);
        assert_eq!(config.write_type, Some(3));
        assert_eq!(config.get_write_type(), Some(WritePType::CacheThrough));
    }

    #[test]
    fn test_config_with_write_type_str() {
        let config = GooseFsConfig::new("127.0.0.1:9200")
            .with_write_type_str("through")
            .unwrap();
        assert_eq!(config.write_type, Some(4));
        assert_eq!(config.get_write_type(), Some(WritePType::Through));
    }

    #[test]
    fn test_config_with_write_type_str_invalid() {
        let result = GooseFsConfig::new("127.0.0.1:9200").with_write_type_str("bad_value");
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
        let config = GooseFsConfig::default();
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
}
