# Development Guide

Local build, test, and release flow for the `goosefs` Python binding.

## Prerequisites

| Tool | Recommended Version | Purpose |
| --- | --- | --- |
| **Rust** | 1.86+ stable | Install via `rustup`; cargo is bundled. |
| **Python** | 3.9+ | 3.10/3.11/3.12 recommended, matching the CI matrix. |
| **uv** | 0.5+ | The project's pinned Python package manager; do not mix in pip / poetry. |
| **maturin** | 1.5+ | Pulled in via the `dev` dependency group, no manual install required. |

The repository root has `rust-toolchain.toml` pinning the Rust version; this directory pins the Python toolchain via `[dependency-groups]` in `pyproject.toml`.

## One-Time Environment Setup

```bash
cd bindings/python
uv sync --all-extras --group dev --group test
```

This step will:
* Create `.venv/`
* Install runtime dependencies
* Install dev tools (`maturin` / `ruff` / `mypy`)
* Install test dependencies (`pytest` / `pytest-asyncio` / `pytest-timeout` / the `mypy>=1.19.1` used by `mypy.stubtest`)
* Install example dependencies (`pyarrow` / `pandas`, since `--all-extras` includes `[examples]`)

## Local Development Loop

### 1. Edit + Build

```bash
# After modifying Rust code
uv run maturin develop --uv
```

`maturin develop` compiles the cdylib and installs the abi3 wheel as editable, taking 5–10 seconds per round.

### 2. Run the Full Test Suite (requires a live cluster)

```bash
# Start a GooseFS cluster (see the README at the repository root)
# Then:
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
uv run --group test pytest -q
```

Expected result: **125 passed** (as of P7).

If `GOOSEFS_MASTER_ADDR` is unset, conftest skips the cluster-dependent cases at collection time and only runs `test_errors.py` (exception hierarchy) + `test_tracing.py` (argument validation), about 13 cases.

### 3. Run lint / Type Checks / Stub Consistency

The four-piece set, all enforced by CI:

```bash
# Rust
cargo clippy --all-targets -- -D warnings
cargo test --lib

# Python
uv run ruff check python/goosefs tests examples
uv run ruff format --check python/goosefs tests examples
uv run --group test mypy python/goosefs
uv run --group test python -m mypy.stubtest goosefs
```

### 4. Manually Run the 5 Examples (live cluster)

```bash
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
for f in examples/0*.py; do echo "==> $f"; uv run python "$f" || break; done
```

## Project Structure At A Glance

```
bindings/python/
├── Cargo.toml          # cdylib + rlib, pyo3 0.27, depends on ../../goosefs-sdk
├── pyproject.toml      # maturin config + uv dependency groups + ruff/mypy/pytest config
├── PYPI_README.md      # Long description on PyPI (user perspective)
├── README.md           # Entry point for this directory (developer + docs navigation)
├── DEVELOPMENT.md      # This document
├── CHANGELOG.md        # Version change log
│
├── src/                # PyO3 wrapper Rust code
│   ├── lib.rs          # #[pymodule] entry, registers all pyclasses
│   ├── config.rs       # Config
│   ├── context.rs      # Internal PyFsHandle
│   ├── errors.rs       # 14 exception subclasses + map_err
│   ├── filesystem.rs   # AsyncGoosefs
│   ├── sync_fs.rs      # Goosefs (sync wrapper)
│   ├── streaming.rs    # 4 file handle classes
│   ├── status.rs       # URIStatus
│   ├── options.rs      # Open/Create/Delete Options
│   ├── types.rs        # WriteType / ReadType
│   ├── runtime.rs      # Shared tokio runtime
│   └── tracing.rs      # enable_tracing
│
├── python/goosefs/
│   ├── __init__.py     # Re-exports + atexit fallback + sys.modules injection
│   ├── __init__.pyi    # Type stubs (541 lines, validated by stubtest)
│   ├── exceptions.pyi  # Stubs for the 14 exception subclasses
│   └── py.typed        # PEP 561 marker
│
├── examples/           # 5 runnable examples
└── tests/              # pytest suite (with conftest.py gating on the cluster)
```

## Stub Maintenance

* Modified a Rust-side pyclass / pyfunction → you **must** manually sync `python/goosefs/__init__.pyi`.
* Modified a runtime exception subclass → sync `python/goosefs/exceptions.pyi`.
* `mypy.stubtest goosefs` strictly validates that the stubs match the real signatures.
* We deliberately do not pull in `pyo3-stub-gen`: the current PyO3 0.27 generator does not fully support our use of `#[pyclass(eq, eq_int, frozen, hash)]` / `#[pyo3(get)]`; writing stubs by hand lets us precisely express constraints like "Awaitable[T]" and "not shareable across tasks/threads".

## Debugging Tips

```python
import goosefs
goosefs.enable_tracing(level="debug")  # SDK + binding logs go to stderr
```

You can also use `RUST_LOG`, which overrides the `level` parameter of `enable_tracing`:

```bash
RUST_LOG="debug,h2=warn,hyper=warn" python my_script.py
```

## Performance Baseline

Performance benchmarks are **on hold** together with P9 (canary + regression); they will be restarted when the project finalizes a unified CI pipeline plan. They will live under the `bench/` directory and cover read/write latency, throughput (with reference targets to be decided: Java SDK / native Rust SDK / OpenDAL adapter), and metadata RPS.

## Release

Quick commands for building and publishing the `goosefs` wheel:

```bash
# Build a wheel for the local platform (quick check)
uv run maturin build --release

# Build a manylinux wheel for Linux (usable on Tencent Cloud Linux)
rustup target add x86_64-unknown-linux-gnu
uv run --with ziglang maturin build --release \
    --target x86_64-unknown-linux-gnu --manylinux 2_28 --zig --out dist

# Publish to PyPI
uv run maturin publish
```

Preferred release path (version check, manylinux zig build, optional upload):

```bash
# from repo root
bash scripts/release/python.sh
bash scripts/release/python.sh --publish
```

See [`../../docs/release/PYTHON_RELEASE.md`](../../docs/release/PYTHON_RELEASE.md).

> **Release automation:** GitHub Actions already builds wheels in CI. Tag-triggered
> PyPI Trusted Publisher (OIDC) publish may be added later; until then, release
> manually via the scripts above and confirm version alignment between `goosefs-sdk`
> and the Python binding `Cargo.toml` before publishing.
