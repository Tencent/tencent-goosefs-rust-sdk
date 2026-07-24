---
sidebar_position: 2
---

# Release Python Package

Publish the PyO3 / maturin wheel to [PyPI](https://pypi.org/project/goosefs/). Linux releases must be **manylinux** wheels.

## Script (preferred)

From the repository root:

```bash
# Build manylinux_2_28 wheels for x86_64 + aarch64 via zig
bash scripts/release/python.sh

# Build only one arch
bash scripts/release/python.sh --arch x86_64

# Build natively on a Linux host (no zig cross-compile)
bash scripts/release/python.sh --native

# Upload to PyPI
export MATURIN_PYPI_TOKEN=...
bash scripts/release/python.sh --publish
```

| Flag                          | Meaning                                     |
| ----------------------------- | ------------------------------------------- |
| `--arch x86_64\|aarch64\|all` | Which Linux arch to build (default: both)   |
| `--manylinux 2_28\|2_17`      | manylinux tag (default: `2_28`)             |
| `--native`                    | Build on the current Linux host without zig |
| `--publish`                   | Upload `bindings/python/dist/*.whl` to PyPI |
| `--skip-build`                | Do not rebuild; only upload                 |

## Checklist

1. Bump `version` in root `Cargo.toml` **and** `bindings/python/Cargo.toml` (keep identical).
2. Update changelogs.
3. Ensure CI is green on `main`.
4. Run the script above.
5. Tag and push:

```bash
git tag py-v0.1.8
git push origin py-v0.1.8
```

The wheel is `abi3-py39`: one Linux wheel covers CPython 3.9+. Prefer manylinux; never commit tokens.
