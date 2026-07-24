---
sidebar_position: 1
---

# Installation

The GooseFS Rust client is published to [crates.io](https://crates.io/crates/goosefs-sdk) as `goosefs-sdk`.

```toml
[dependencies]
goosefs-sdk = "0.1"
tokio = { version = "1", features = ["full"] }
```

Until the crate is published (or to track `main`):

```toml
[dependencies]
goosefs-sdk = { git = "https://github.com/Tencent/tencent-goosefs-rust-sdk" }
tokio = { version = "1", features = ["full"] }
```

## Feature Flags

```toml
[dependencies]
# Default: includes metrics Pushgateway exporter (reqwest)
goosefs-sdk = "0.1"

# Smaller dependency graph when you only need the gRPC client
goosefs-sdk = { version = "0.1", default-features = false }

# Opt-in protobuf regeneration (developers only)
goosefs-sdk = { version = "0.1", features = ["regen-proto"] }
```

| Feature               | Default | Purpose                                                     |
| --------------------- | ------- | ----------------------------------------------------------- |
| `metrics-pushgateway` | yes     | HTTP Pushgateway exporter (`reqwest`)                       |
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
