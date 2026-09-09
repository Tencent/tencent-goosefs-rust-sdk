---
sidebar_position: 1
---

# Installation

The GooseFS Rust client is published to [crates.io](https://crates.io/crates/goosefs-sdk) as `goosefs-sdk`.

```toml
[dependencies]
goosefs-sdk = "0.2"
tokio = { version = "1", features = ["full"] }
```

Until the crate is published (or to track `main`):

```toml
[dependencies]
goosefs-sdk = { git = "https://github.com/Tencent/tencent-goosefs-rust-sdk" }
tokio = { version = "1", features = ["full"] }
```

## Feature Flags

The default feature set is empty, so downstream crates only compile the core gRPC client. Enable optional capabilities explicitly:

```toml
[dependencies]
# Core gRPC client only
goosefs-sdk = "0.2"

# Process-local metadata cache (on by default once this feature is compiled in)
goosefs-sdk = { version = "0.2", features = ["metadata-cache"] }

# Portable page cache; add `page-cache-io-uring` on Linux for the io_uring backend
goosefs-sdk = { version = "0.2", features = ["page-cache"] }

# Pushgateway exporter (pulls in reqwest)
goosefs-sdk = { version = "0.2", features = ["metrics-pushgateway"] }

# Opt-in protobuf regeneration (developers only)
goosefs-sdk = { version = "0.2", features = ["regen-proto"] }
```

| Feature               | Default | Purpose                                                     |
| --------------------- | ------- | ----------------------------------------------------------- |
| `metadata-cache`      | no      | Process-local status and listing cache (`lru`)              |
| `page-cache`          | no      | Portable disk-backed page cache (`foyer-*`, `tokio/fs`)     |
| `page-cache-io-uring` | no      | Linux io_uring page-cache backend (includes `page-cache`)   |
| `metrics-pushgateway` | no      | HTTP Pushgateway exporter (`reqwest`)                       |
| `full-client`         | no      | All runtime capabilities above                              |
| `regen-proto`         | no      | Rebuild stubs from `proto/` via `GOOSEFS_SDK_REGEN_PROTO=1` |

## Requirements

- **Rust 1.88+** — install via [rustup](https://rustup.rs/)
- A reachable GooseFS Master (and Workers for data-plane I/O)

Downstream builds do **not** need `protoc`. Pre-generated protobuf code lives under `src/generated/`. Install `protoc` only if you change files under `proto/` and need to regenerate.

## Building from Source

```bash
git clone https://github.com/Tencent/tencent-goosefs-rust-sdk.git
cd tencent-goosefs-rust-sdk
cargo build
cargo test
```

Regenerate protobuf code after editing `.proto` files:

```bash
GOOSEFS_SDK_REGEN_PROTO=1 cargo build
```

## Local GooseFS Cluster (Docker)

```bash
bash scripts/ci/goosefs-up.sh
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
export GOOSEFS_AUTH_TYPE=simple
```

If pulls from `goosefs.tencentcloudcr.com` fail, mirror the image and override:

```bash
export GOOSEFS_IMAGE=ghcr.io/<org>/goosefs:v2.1.0.1
bash scripts/ci/goosefs-up.sh
```

API docs on docs.rs: [https://docs.rs/goosefs-sdk](https://docs.rs/goosefs-sdk).
