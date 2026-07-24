---
sidebar_position: 1
---

# Contributing

Thanks for your interest in contributing to the Tencent GooseFS Rust / Python SDK.

## Before you start

1. Search [existing issues](https://github.com/Tencent/tencent-goosefs-rust-sdk/issues) or open a new one to discuss larger changes.
2. Fork the repository and create a topic branch from `main`.
3. Keep pull requests focused. Prefer small, reviewable diffs.

By contributing, you agree that your contributions will be licensed under the Apache License, Version 2.0. Please follow the [Code of Conduct](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/CODE_OF_CONDUCT.md).

## Development setup

Requirements:

- Rust **1.88+**
- Optional: Docker (integration tests / examples)
- Optional: Python 3.9+ and [uv](https://docs.astral.sh/uv/) for the Python binding

```bash
git clone https://github.com/Tencent/tencent-goosefs-rust-sdk.git
cd tencent-goosefs-rust-sdk
cargo build
cargo test
```

### Local GooseFS cluster

```bash
bash scripts/ci/goosefs-up.sh
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
export GOOSEFS_AUTH_TYPE=simple

bash scripts/ci/run_rust_integration.sh
bash scripts/ci/run_rust_examples.sh
bash scripts/ci/goosefs-down.sh
```

### Python binding

```bash
cd bindings/python
uv sync --all-extras --group dev --group test
uv run maturin develop --uv
uv run pytest -v
```

## Pull requests

- Describe **why** the change is needed and how you tested it.
- Match existing style; run `cargo fmt` and `cargo clippy` for Rust.
- Update docs or examples when user-facing behavior changes.
- Do not commit secrets or machine-local absolute paths.

### PR title convention

```text
[area] Short summary
```

Examples: `[sdk] Reduce log verbosity`, `[sdk][py] Add retry to Python upload`. Areas are lowercase identifiers such as `sdk`, `py`, `ci`, `docs`, `rust`.

## Website docs

This documentation site lives under `website/`. To preview locally:

```bash
cd website
npm install
npm start
```
