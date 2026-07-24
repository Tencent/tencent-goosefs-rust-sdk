---
sidebar_position: 1
---

# Installation

`goosefs` is the official Python client for GooseFS. It is implemented in Rust (`goosefs-sdk`) and bridged via [PyO3](https://pyo3.rs/).

```bash
pip install goosefs
```

| Install command                   | Extra dependencies     | Use case                |
| --------------------------------- | ---------------------- | ----------------------- |
| `pip install goosefs`             | binding only           | Core API                |
| `pip install 'goosefs[arrow]'`    | + `pyarrow`            | Arrow / Parquet         |
| `pip install 'goosefs[pandas]'`   | + `pandas` + `pyarrow` | DataFrame workflows     |
| `pip install 'goosefs[examples]'` | + `pyarrow` + `pandas` | Run all example scripts |

## Requirements

- CPython **3.9+** (abi3 wheel)
- Platforms: Linux x86_64 / aarch64 (manylinux_2_28), macOS x86_64 / arm64; Windows best-effort
- A reachable GooseFS Master

## Build from Source

```bash
cd bindings/python
uv sync --all-extras --group dev --group test
uv run maturin develop --uv
uv run pytest -v
```

See [`bindings/python/DEVELOPMENT.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/bindings/python/DEVELOPMENT.md).
