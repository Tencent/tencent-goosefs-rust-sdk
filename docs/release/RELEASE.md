# Release Guide

This document describes how to package and publish the **Rust crate** `goosefs-sdk`
to [crates.io](https://crates.io/).

> Looking to release the **Python** client (`goosefs`)? See
> [`PYTHON_RELEASE.md`](PYTHON_RELEASE.md) instead — it builds native manylinux
> wheels via maturin and publishes to PyPI.

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

## Publish to crates.io

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

## Argument Reference

| Argument | Description |
|----------|-------------|
| `--token` | Access token (create one at https://crates.io/settings/tokens) |
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

# 5. Publish to crates.io
cargo publish --token <your-crates-io-token>

# 6. Create a Git tag
git tag v0.1.7
git push origin v0.1.7
```

## Notes

1. Before releasing a new version, always update the `version` field in `Cargo.toml`
   (keep `bindings/python` versions aligned).
2. crates.io **does not allow deleting or overwriting published versions**; you can
   only yank (mark as not recommended).
3. After publishing, the source inside the crate package is publicly visible.
4. Run `cargo publish --dry-run` before releasing.
5. Keep tokens safe and never commit them to the repository.
6. Update [`CHANGELOG.md`](../../CHANGELOG.md) for the release notes.
