---
sidebar_position: 4
---

# Configuration

The Java binding uses the same Rust core configuration as Python. Settings can be provided through the `Config` constructors, environment variables, or a properties file. When the same parameter appears in multiple sources, the **highest-priority** source wins:

```text
Priority (highest → lowest):

  1. Environment variables (GOOSEFS_*)
  2. Properties config file (goosefs-site.properties)
  3. Built-in defaults
```

Keys are parsed natively (`GoosefsConfig::from_properties_str` / `from_uri`). Do not re-list property names in Java.

## Minimal setup

```java
import com.tencent.goosefs.Config;
import java.util.Map;

Config cfg = new Config("127.0.0.1:9200");

cfg = new Config("127.0.0.1:9200", Map.of(
        "goosefs.user.network.rpc.connect.timeout", "5sec"));

cfg = Config.fromPropertiesFile("/etc/goosefs/goosefs-site.properties");

cfg = Config.fromUri("gfs://127.0.0.1:9200/data");
```

## Environment variables

| Variable | Purpose |
| --- | --- |
| `GOOSEFS_MASTER_ADDR` | Master host:port (or comma-separated HA list) |
| `GOOSEFS_AUTH_TYPE` | `nosasl` / `simple` / `custom` |
| `GOOSEFS_AUTH_USERNAME` | Username for SIMPLE auth |
| `GOOSEFS_USER_CLIENT_CACHE_ENABLED` | Client page cache (default `false`) |
| `GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE` | Page size (default 1 MiB) |
| `GOOSEFS_USER_CLIENT_CACHE_SIZE` | Cache capacity (default 20 GiB) |
| `GOOSEFS_USER_CLIENT_CACHE_DIRS` | Cache directories |
| `GOOSEFS_METADATA_CACHE_ENABLED` | Metadata cache (default `true`) |

`goosefs.user.client.cache.*` properties flow through `Config` unchanged.

## Write / read types

```java
import com.tencent.goosefs.CreateFileOptions;
import com.tencent.goosefs.WriteType;

fs.writeFile("/data/file.bin", payload, CreateFileOptions.builder()
        .writeType(WriteType.CACHE_THROUGH)
        .build());
```

| Enum | Typical use |
| --- | --- |
| `WriteType.MUST_CACHE` | Cache only (no UFS persist) |
| `WriteType.TRY_CACHE` | Try cache first, fall back to Through on error |
| `WriteType.CACHE_THROUGH` | Write cache + UFS synchronously |
| `WriteType.THROUGH` | Write UFS directly |
| `WriteType.ASYNC_THROUGH` | Write cache, persist UFS asynchronously |

## Full parameter reference

[`docs/CLIENT_CONFIGURATION.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/CLIENT_CONFIGURATION.md)
