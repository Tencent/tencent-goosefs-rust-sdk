# Release Guide — Python (`goosefs`)

Publish the PyO3 / maturin wheel to [PyPI](https://pypi.org/project/goosefs/).

For the Rust crate, see [`RELEASE.md`](RELEASE.md).

`goosefs` is a native extension: Linux releases must be **manylinux** wheels so
they are not tied to the build machine's glibc.

## Script (preferred)

From the repository root:

```bash
# Build manylinux_2_28 wheels for x86_64 + aarch64 via zig (works on macOS too)
bash scripts/release/python.sh

# Build only one arch
bash scripts/release/python.sh --arch x86_64

# Build natively on a Linux host (no zig cross-compile)
bash scripts/release/python.sh --native

# Upload existing / freshly built wheels to PyPI
export MATURIN_PYPI_TOKEN=...   # https://pypi.org/manage/account/
bash scripts/release/python.sh --publish
# or, if wheels already exist under bindings/python/dist/:
bash scripts/release/python.sh --publish --skip-build
```

Useful flags:

| Flag | Meaning |
|------|---------|
| `--arch x86_64\|aarch64\|all` | Which Linux arch to build (default: both) |
| `--manylinux 2_28\|2_17` | manylinux tag (default: `2_28`) |
| `--native` | Build on the current Linux host without zig |
| `--publish` | Upload `bindings/python/dist/*.whl` to PyPI |
| `--skip-build` | Do not rebuild; only upload |

The script always checks that root `Cargo.toml` and `bindings/python/Cargo.toml`
versions match.

CI (`ci_bindings_python.yml`) also runs the zig manylinux path (`x86_64` +
`aarch64`) so release artifacts are verified on every relevant push/PR.

## Manual checklist (still required)

1. Bump `version` in root `Cargo.toml` **and** `bindings/python/Cargo.toml` (keep identical).
2. Update [`bindings/python/CHANGELOG.md`](../../bindings/python/CHANGELOG.md) / root changelog.
3. Ensure CI is green on `main`.
4. Run the script above.
5. Tag and push:

```bash
git tag py-v0.1.8
git push origin py-v0.1.8
```

## Notes

- Prefer **manylinux** Linux wheels. A plain `--compatibility linux` wheel can
  fail on older glibc hosts (`GLIBC_x.y not found`).
- `2_28` is the default baseline (CentOS 8 / TencentOS 3 / Ubuntu 20.04+). Use
  `--manylinux 2_17` only when you must support glibc 2.17 (CentOS 7 era).
- The wheel is `abi3-py39`: one Linux wheel covers CPython 3.9+.
- Build Windows wheels on a Windows host with MSVC (best-effort; not covered by
  this script).
- PyPI does **not** allow re-uploading the same version.
- Never commit tokens; use `MATURIN_PYPI_TOKEN` (or `UV_PUBLISH_TOKEN`) only.
