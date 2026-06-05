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
    /// `master_addr` accepts either a single `host:port` (single-master) or
    /// a comma-separated list (`m1:9200,m2:9200,m3:9200`) for HA. Property
    /// keys are the same as those accepted by `goosefs-site.properties`.
    #[new]
    #[pyo3(signature = (master_addr, *, properties=None))]
    fn new(master_addr: &str, properties: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        // Detect HA form (comma-separated). `from_addresses` panics on empty
        // input, so we filter blanks first.
        let addrs: Vec<String> = master_addr
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if addrs.is_empty() {
            return Err(ConfigError::new_err(
                "master_addr must be a non-empty 'host:port' or comma-separated list",
            ));
        }

        // Start from the address-only config…
        let mut cfg = if addrs.len() == 1 {
            GoosefsConfig::new(&addrs[0])
        } else {
            GoosefsConfig::new_ha(addrs)
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
            cfg = parsed;
            if cfg.master_addr.is_empty() {
                cfg.master_addr = preserved_addr;
            }
            if cfg.master_addrs.is_empty() {
                cfg.master_addrs = preserved_addrs;
            }
        }

        Ok(Self { inner: cfg })
    }

    /// Load a config from a `goosefs-site.properties` file on disk.
    #[staticmethod]
    fn from_properties_file(path: &str) -> PyResult<Self> {
        let cfg = GoosefsConfig::from_properties(path)
            .map_err(|e| ConfigError::new_err(e.to_string()))?;
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
}
