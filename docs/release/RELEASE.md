# Release Guide

This document describes how to package and publish the **Rust crate** `goosefs-sdk` to a Cargo registry (crates.io or the Tencent internal Cargo Registry).

> Looking to release the **Python** client (`goosefs`)? See [`PYTHON_RELEASE.md`](PYTHON_RELEASE.md) instead — it builds native manylinux wheels via maturin and publishes to PyPI.

## Prerequisites

Make sure the following tools are installed:

```bash
# Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Confirm cargo is available
cargo --version
```

## Pre-Release Checks

Before publishing, please make sure the following checks are completed:

```bash
# 1. Run tests
cargo test

# 2. Check the docs for warnings
cargo doc --no-deps

# 3. Check the package contents (without actually uploading)
cargo publish --dry-run

# 4. List the files that will be packaged
cargo package --list
```

---

## Option 1: Publish to the Official crates.io

### Upload Command

```bash
cargo publish --token <your-crates-io-token>
```

### Install Verification

```bash
cargo add goosefs-sdk
```

### Project URL

- https://crates.io/crates/goosefs-sdk

---

## Option 2: Publish to the Tencent Internal Cargo Registry

### Configure the Registry

Add to `~/.cargo/config.toml`:

```toml
[registries.tencent]
index = "TODO: Tencent internal Cargo Registry URL"
```

### Upload Command

```bash
cargo publish --registry tencent --token <your-token>
```

### Install Verification

Add the dependency to your project's `Cargo.toml`:

```toml
[dependencies]
goosefs-sdk = { version = "0.1", registry = "tencent" }
```

Or, after configuring the default registry globally via `.cargo/config.toml`, use it directly:

```toml
[dependencies]
goosefs-sdk = "0.1"
```

---

## Argument Reference

| Argument | Description |
|----------|-------------|
| `--token` | Access token (for crates.io, create one at https://crates.io/settings/tokens) |
| `--registry` | Target registry name (defaults to crates.io when omitted) |
| `--dry-run` | Simulate the publish only, do not actually upload |
| `--allow-dirty` | Allow publishing with uncommitted changes (not recommended) |

## Full Release Flow

```bash
# 1. Confirm the version number is updated (the `version` field in Cargo.toml)
grep '^version' Cargo.toml

# 2. Confirm all tests pass
cargo test

# 3. Confirm the docs have no warnings
cargo doc --no-deps

# 4. Simulate the publish and inspect package contents
cargo publish --dry-run

# 5a. Publish to the official crates.io
cargo publish --token <your-crates-io-token>

# 5b. Or publish to the Tencent internal registry
cargo publish --registry tencent --token <your-token>

# 6. Create a Git tag
git tag v0.1.5
git push origin v0.1.5
```

## Notes

1. Before releasing a new version, always update the `version` field in `Cargo.toml`.
2. crates.io **does not allow deleting or overwriting published versions**; you can only yank (mark as not recommended).
3. After publishing to crates.io, the source code inside the crate package becomes publicly visible (even if the Git repository is private).
4. It is recommended to run `cargo publish --dry-run` for verification before releasing.
5. Keep tokens safe and never commit them to a code repository.
6. crates.io tokens can be created and managed at https://crates.io/settings/tokens.
7. If you do not want the source code to be publicly visible, use the Tencent internal Cargo Registry (Option 2).
