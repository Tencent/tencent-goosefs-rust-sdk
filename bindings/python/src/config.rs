//! `Config` — Python wrapper around `goosefs_sdk::config::GoosefsConfig`.
//!
//! ## Construction
//!
//! ```python
//! from goosefs import Config
//!
//! cfg = Config("127.0.0.1:9200")
//! cfg = Config("m1:9200", properties={
//!     "goosefs.user.block.size.bytes.default": "4MB",
//!     "goosefs.user.write.type.default": "MUST_CACHE",
//! })
//! cfg = Config.from_properties_file("/etc/goosefs/goosefs-site.properties")
//! ```
//!
//! ## Precedence
//!
//! Every `PyConfig` constructor (`Config(...)`, `Config.from_uri(...)`,
//! `Config.from_properties_file(...)`) applies configuration in the same
//! order as `GoosefsConfig::from_properties_auto` on the Rust side:
//!
//!   1. built-in defaults,
//!   2. properties file / `properties=` dict / `gfs://` URI,
//!   3. `GOOSEFS_*` environment variables (highest priority).
//!
//! This means, for example, exporting
//! `GOOSEFS_USER_METRICS_COLLECTION_ENABLED=false` in the shell that
//! launches a Python process disables the metrics heartbeat regardless
//! of what the `properties=` dict or `goosefs-site.properties` file says.
//!
//! ## Implementation note
//!
//! The SDK already knows how to parse a `goosefs-site.properties` file via
//! `GoosefsConfig::from_properties_str`. Rather than reimplement the (long)
//! list of recognised property keys here, the constructor serialises the
//! Python `properties` dict back into the canonical `key=value\n` format
//! and feeds it to the SDK parser. This keeps Python and Rust in lock-step:
//! when the SDK learns a new property key, Python immediately understands
//! it without any binding change.

use goosefs_sdk::config::GoosefsConfig;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::errors::ConfigError;

/// Python-visible configuration object.
///
/// Wraps an owned `GoosefsConfig`; cloned cheaply when passed to
/// `AsyncGoosefs.connect` / `Goosefs(...)` (P2/P3).
#[pyclass(module = "goosefs._goosefs", name = "Config")]
#[derive(Clone)]
pub struct PyConfig {
    pub(crate) inner: GoosefsConfig,
}

#[pymethods]
impl PyConfig {
    /// `Config(master_addr: str, *, properties: dict[str, str] | None = None)`
    ///
    /// `master_addr` accepts three forms:
    ///
    /// * Single `host:port` (single-master).
    /// * Comma-separated list (`m1:9200,m2:9200,m3:9200`) for HA.
    /// * A `gfs://` URI (`gfs://m1:9200,m2:9200/root-path`) — the path
    ///   segment (if any) becomes [`Config.root`].
    ///
    /// Property keys are the same as those accepted by
    /// `goosefs-site.properties`.
    #[new]
    #[pyo3(signature = (master_addr, *, properties=None))]
    fn new(master_addr: &str, properties: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        // Build the address-only base config. Two accepted forms:
        //   * `gfs://...` URI  — sniffed by scheme prefix
        //   * bare comma list  — legacy path, unchanged
        let mut cfg = if master_addr.trim_start().starts_with("gfs://") {
            GoosefsConfig::from_uri(master_addr.trim())
                .map_err(|e| ConfigError::new_err(e.to_string()))?
        } else {
            let addrs: Vec<String> = master_addr
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if addrs.is_empty() {
                return Err(ConfigError::new_err(
                    "master_addr must be a non-empty 'host:port', comma-separated list, or 'gfs://' URI",
                ));
            }
            if addrs.len() == 1 {
                GoosefsConfig::new(&addrs[0])
            } else {
                GoosefsConfig::new_ha(addrs)
            }
        };

        // …then layer the user's `properties=` dict on top by serialising it
        // to canonical Java-properties format and re-parsing through the SDK.
        // This is a deliberate trade-off: we accept a tiny string round-trip
        // in exchange for not having to mirror every property key/parser in
        // this binding.
        if let Some(props) = properties {
            let mut buf = String::new();
            for (k, v) in props.iter() {
                let key: String = k
                    .extract()
                    .map_err(|e| ConfigError::new_err(format!("property key must be str: {e}")))?;
                let val: String = v.extract().map_err(|e| {
                    ConfigError::new_err(format!("property value for '{key}' must be str: {e}"))
                })?;
                if key.contains('=') || key.contains('\n') {
                    return Err(ConfigError::new_err(format!(
                        "property key may not contain '=' or newline: {key:?}"
                    )));
                }
                if val.contains('\n') {
                    return Err(ConfigError::new_err(format!(
                        "property value for {key:?} may not contain newline"
                    )));
                }
                buf.push_str(&key);
                buf.push('=');
                buf.push_str(&val);
                buf.push('\n');
            }
            // Apply on top of the address-seeded config. `from_properties_str`
            // builds a *fresh* config, so we have to re-set the address(es)
            // afterwards if the user did not encode them in `properties`.
            let parsed = GoosefsConfig::from_properties_str(&buf);
            // Preserve master addresses chosen above; properties may legitim-
            // ately omit them.
            let preserved_addr = cfg.master_addr.clone();
            let preserved_addrs = std::mem::take(&mut cfg.master_addrs);
            let preserved_root = std::mem::take(&mut cfg.root);
            cfg = parsed;
            if cfg.master_addr.is_empty() {
                cfg.master_addr = preserved_addr;
            }
            if cfg.master_addrs.is_empty() {
                cfg.master_addrs = preserved_addrs;
            }
            // Preserve a URI-derived root too — properties files rarely set
            // `goosefs.root`, but when they do the caller intent is explicit
            // and wins over the URI form.
            if cfg.root.is_empty() {
                cfg.root = preserved_root;
            }
        }

        // Overlay `GOOSEFS_*` environment variables on top of the caller's
        // explicit configuration. This matches the precedence documented
        // above (defaults → properties → env) and mirrors the SDK's own
        // `from_properties_auto` helper. In particular this is what makes
        // `GOOSEFS_USER_METRICS_COLLECTION_ENABLED=false` actually reach
        // `PyConfig.metrics_enabled` — without this step the env var was
        // parsed by the SDK constants but never applied to Python configs.
        let cfg = cfg.apply_env();

        Ok(Self { inner: cfg })
    }

    /// Load a config from a Goosefs `gfs://` URI.
    ///
    /// Convenience wrapper around [`GoosefsConfig::from_uri`]:
    ///
    /// ```python
    /// cfg = Config.from_uri("gfs://m1:9200,m2:9200,m3:9200/data")
    /// ```
    ///
    /// `properties` — if provided — is applied on top of the URI-derived
    /// config, mirroring the constructor's behaviour.
    #[staticmethod]
    #[pyo3(signature = (uri, *, properties=None))]
    fn from_uri(uri: &str, properties: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        // Reuse `__new__` — the constructor already sniffs the `gfs://`
        // scheme and layers `properties=` on top identically.
        Self::new(uri, properties)
    }

    /// Load a config from a `goosefs-site.properties` file on disk.
    ///
    /// `GOOSEFS_*` environment variables are overlaid on top of the file
    /// (see the module-level *Precedence* note).
    #[staticmethod]
    fn from_properties_file(path: &str) -> PyResult<Self> {
        let cfg = GoosefsConfig::from_properties(path)
            .map_err(|e| ConfigError::new_err(e.to_string()))?
            .apply_env();
        Ok(Self { inner: cfg })
    }

    // ------------------------------------------------------------------
    // Read-only accessors. Only the most commonly inspected fields are
    // exposed — additional getters can be added on demand. We deliberately
    // do *not* expose setters: a `Config` is immutable once constructed
    // (callers should build a new one via `properties=`).
    // ------------------------------------------------------------------

    #[getter]
    fn master_addr(&self) -> String {
        self.inner.master_addr.clone()
    }

    #[getter]
    fn master_addrs(&self) -> Vec<String> {
        self.inner.master_addresses()
    }

    #[getter]
    fn block_size(&self) -> u64 {
        self.inner.block_size
    }

    #[getter]
    fn chunk_size(&self) -> u64 {
        self.inner.chunk_size
    }

    #[getter]
    fn root(&self) -> String {
        self.inner.root.clone()
    }

    #[getter]
    fn use_vpc_mapping(&self) -> bool {
        self.inner.use_vpc_mapping
    }

    #[getter]
    fn auth_type(&self) -> String {
        self.inner.auth_type.to_string()
    }

    #[getter]
    fn auth_username(&self) -> String {
        self.inner.auth_username.clone()
    }

    #[getter]
    fn metrics_enabled(&self) -> bool {
        self.inner.metrics_enabled
    }

    #[getter]
    fn connect_timeout_ms(&self) -> u128 {
        self.inner.connect_timeout.as_millis()
    }

    #[getter]
    fn request_timeout_ms(&self) -> u128 {
        self.inner.request_timeout.as_millis()
    }

    /// Returns the default write type as the proto enum integer (1..=4) or
    /// `None` if the user did not override the default.
    #[getter]
    fn write_type(&self) -> Option<i32> {
        self.inner.write_type
    }

    fn __repr__(&self) -> String {
        format!(
            "Config(master_addr={:?}, block_size={}, auth_type={:?})",
            self.inner.master_addr, self.inner.block_size, self.inner.auth_type
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// We construct `PyConfig` directly via its private fields rather than
    /// through `__new__` because the latter requires `&Bound<PyDict>` which
    /// in turn requires a live Python interpreter — overkill for verifying
    /// the address-parsing helper logic.
    #[test]
    fn ha_form_accepts_comma_separated_addresses() {
        // Mimic what `__new__` does for the address parsing branch:
        let raw = "m1:9200, m2:9200 ,m3:9200";
        let addrs: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        assert_eq!(addrs.len(), 3);
        let cfg = GoosefsConfig::new_ha(addrs.clone());
        assert_eq!(cfg.master_addr, "m1:9200");
        assert_eq!(cfg.master_addresses(), addrs);
    }

    #[test]
    fn empty_address_is_rejected() {
        let raw = "  , ,";
        let addrs: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        assert!(addrs.is_empty());
    }

    #[test]
    fn uri_form_populates_addresses_and_root() {
        // Mirrors what `__new__` does when it sees a `gfs://` prefix — we
        // exercise the SDK helper directly to keep the test free of a live
        // Python interpreter.
        let cfg = GoosefsConfig::from_uri(
            "gfs://172.16.16.27:9200,172.16.16.23:9200,172.16.16.38:9200/xxxx",
        )
        .expect("well-formed URI");
        assert_eq!(cfg.master_addr, "172.16.16.27:9200");
        assert_eq!(cfg.master_addresses().len(), 3);
        assert_eq!(cfg.root, "/xxxx");
    }
}
