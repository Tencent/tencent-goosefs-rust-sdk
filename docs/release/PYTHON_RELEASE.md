# Python Release Guide

This document describes how to package and publish the `goosefs` Python client
to a PyPI repository (official PyPI or the Tencent internal PyPI mirror), and how
to make sure the published wheels are **installable and usable on Tencent Cloud
Linux environments**.

Unlike a pure-Python package, `goosefs` is a **native extension** built from Rust
via [PyO3](https://pyo3.rs/) + [maturin](https://www.maturin.rs/). To run on
Tencent Cloud Linux (TencentOS Server / CentOS / Ubuntu, etc.) the wheel must be
built as a **manylinux** wheel so that it does not depend on the build machine's
glibc/toolchain.

The project root for all commands below is `bindings/python`.

```bash
cd bindings/python
```

## Prerequisites

| Tool | Recommended Version | Purpose |
| --- | --- | --- |
| **Rust** | 1.88+ stable | Install via `rustup`; cargo is bundled. |
| **Python** | 3.9+ | The wheel is `abi3-py39`, one wheel covers 3.9+. |
| **uv** | 0.5+ | Project package manager; runs maturin and pulls `ziglang`. |
| **maturin** | 1.5+ | Build backend that produces the wheels. |
| **ziglang** | latest | C cross-compiler/linker for the zig build (Approach A). |
| **twine** | 4.0+ | Optional; only needed for manual upload to a generic PyPI mirror. |

```bash
# Rust toolchain
 curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# maturin + twine (twine is optional, see "Upload" below)
 pip install "maturin>=1.5,<2.0" twine
```

## Pre-Release Checks

```bash
# 1. Confirm the version is updated and the SDK / binding versions are aligned
 grep '^version' ../../Cargo.toml          # goosefs-sdk
 grep '^version' Cargo.toml                # goosefs-python (must match)

# 2. Run the test suite (requires a live GooseFS cluster)
 export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
 uv run --group test pytest -q

# 3. Lint / type / stub checks
 uv run ruff check python/goosefs tests examples
 uv run --group test mypy python/goosefs
 uv run --group test python -m mypy.stubtest goosefs
```

---

## Build (manylinux wheel)

To guarantee the wheel runs on Tencent Cloud Linux, build a **manylinux**
wheel. Pick **one** of the two approaches below.

### What does `--manylinux 2_28` mean?

`goosefs` is a native extension, so its compiled `.so` links against the build
machine's **glibc**. glibc is only **backward compatible** (a newer system can
run something built on an older glibc, but not the other way around). If you
build on a fresh OS and ship that wheel to an older Tencent Cloud Linux host,
`import goosefs` fails with errors like:

```
ImportError: /lib64/libc.so.6: version `GLIBC_2.35' not found
```

The [manylinux](https://peps.python.org/pep-0600/) standard fixes this: it builds
against an **older baseline glibc** and only links a set of libraries present on
every distro, so one wheel runs across a wide range of Linux hosts.

`2_28` is that baseline — it means **"this wheel requires glibc ≥ 2.28"**. Choose
the tag according to the **oldest** target you must support:

| Tag | glibc baseline | Typical compatible systems |
| --- | --- | --- |
| `manylinux_2_17` (a.k.a. `manylinux2014`) | 2.17 | CentOS 7, TencentOS 2, almost every Linux still in use — **broadest** |
| `manylinux_2_28` | 2.28 | CentOS 8 / TencentOS 3, Ubuntu 20.04+, Debian 10+ — newer but still common |
| `manylinux_2_34` | 2.34 | Ubuntu 22.04+ and other recent systems |

A smaller number runs on more machines (wider compatibility) at the cost of older
system libraries. Recommendation:

- Targeting **newer** hosts (TencentOS 3 / CentOS 8 / Ubuntu 20.04+) → use
  `--manylinux 2_28` (the default below).
- Need to also support **legacy** CentOS 7 / TencentOS 2 (glibc 2.17) → use
  `--manylinux 2_17` instead.
- Unsure of the target's glibc? Run `ldd --version` on that host; it must be
  **≥** the tag's baseline.

### What does `--target x86_64-unknown-linux-gnu` mean?

`--target` tells the Rust compiler **which platform to generate code for**, using
a [target triple](https://doc.rust-lang.org/rustc/platform-support.html) — a name
of the form `<arch>-<vendor>-<os>-<abi>`. For example,
`x86_64-unknown-linux-gnu` reads as:

| Part | Value | Meaning |
| --- | --- | --- |
| arch | `x86_64` | 64-bit Intel/AMD CPU |
| vendor | `unknown` | no specific vendor (the conventional placeholder) |
| os | `linux` | Linux operating system |
| abi | `gnu` | the GNU C library (glibc) ABI |

When you build on a macOS machine, cargo defaults to your **host** triple (e.g.
`aarch64-apple-darwin`), which would produce a macOS binary. Passing
`--target x86_64-unknown-linux-gnu` overrides that so the wheel targets **64-bit
Linux on glibc** instead — exactly what Tencent Cloud Linux x86_64 instances run.
This is why cross-compiling requires both `rustup target add <triple>` (the Rust
std for that platform) and zig (the C toolchain/linker for it).

Common triples for Tencent Cloud Linux releases:

| Triple | Use for |
| --- | --- |
| `x86_64-unknown-linux-gnu` | Standard Intel/AMD instances (most common) |
| `aarch64-unknown-linux-gnu` | ARM instances (e.g. Tencent Cloud ARM) |

> The `gnu` (glibc) ABI is the right choice for Tencent Cloud Linux. The
> alternative `musl` ABI (`*-unknown-linux-musl`) targets statically-linked
> Alpine-style systems and is not needed here.

### What about Windows?

Linux and Windows use different triples — there is **no manylinux on Windows**.
For a wheel that runs on Windows, use the **MSVC** ABI triple:

| Triple | Use for |
| --- | --- |
| `x86_64-pc-windows-msvc` | 64-bit Windows (standard, recommended) |
| `aarch64-pc-windows-msvc` | ARM64 Windows (best-effort) |

The resulting wheel is tagged `win_amd64` (not `manylinux`), e.g.
`goosefs-<version>-cp39-abi3-win_amd64.whl`.

> **Build Windows wheels on a Windows host.** Unlike Linux, Windows wheels are
> not cross-compiled with zig in this project — the reliable path is to run
> maturin natively on a Windows machine (or a Windows CI runner) with the MSVC
> toolchain (Visual Studio Build Tools) installed:
>
> ```bash
> rustup target add x86_64-pc-windows-msvc
> maturin build --release --out dist
> ```
>
> Windows is **best-effort** for `goosefs` (see the classifiers note in
> `pyproject.toml`); the primary, fully-supported targets are Linux and macOS.
> The `*-pc-windows-gnu` (MinGW) ABI exists but is not used here — prefer MSVC.

### Approach A — Cross-compile with zig (recommended on macOS, no Docker)

`maturin --zig` uses [ziglang](https://ziglang.org/) as the C cross-compiler /
linker, so you can build a manylinux Linux wheel **directly on macOS** without
Docker or a Linux host. This is the verified flow used for the current release.

```bash
 # 1. Add the Rust std/core for the Linux target (one-time).
 #    zig only provides the C toolchain; Rust still needs its own target std.
 rustup target add x86_64-unknown-linux-gnu

 # 2. Build the manylinux_2_28 wheel (ziglang is pulled in on the fly via uv).
 cd bindings/python
 uv run --with ziglang maturin build --release \
     --target x86_64-unknown-linux-gnu \
     --manylinux 2_28 --zig --out dist
```

On success you get, e.g.:

```
📦 Built wheel for abi3 Python ≥ 3.9 to dist/goosefs-0.1.5-cp39-abi3-manylinux_2_28_x86_64.whl
```

For ARM (Tencent Cloud ARM instances) add the aarch64 target and build again:

```bash
 rustup target add aarch64-unknown-linux-gnu
 uv run --with ziglang maturin build --release \
     --target aarch64-unknown-linux-gnu \
     --manylinux 2_28 --zig --out dist
```

> Troubleshooting:
> - `error[E0463]: can't find crate for 'std' / 'core'` → the Rust target is not
>   installed; run `rustup target add <target>` (step 1). If it persists, the
>   target may be on a different toolchain than the one building — check with
>   `rustup show` and add it explicitly: `rustup target add --toolchain <ver> <target>`.
> - `pip: bad interpreter` after a Homebrew Python upgrade → the venv references a
>   removed interpreter; recreate it with `rm -rf .venv && uv sync` and use
>   `python -m pip` instead of the broken `pip` shim.

### Approach B — Build directly on a Tencent Cloud Linux host

SSH into a Tencent Cloud Linux instance (the same OS family you will deploy to),
install the toolchain, and build natively:

```bash
# On the Tencent Cloud Linux instance
 curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
 source "$HOME/.cargo/env"
 pip install "maturin>=1.5,<2.0"

 cd bindings/python
 maturin build --release --manylinux 2_28 --out dist
```

> Approach B requires `auditwheel` (bundled with maturin) and a compatible glibc.
> On older systems where `--manylinux 2_28` fails, fall back to
> `--manylinux 2_17` (broadest compatibility) or `--compatibility linux` for an
> instance-local wheel that is only guaranteed to work on that exact host.

Regardless of which approach you use, a wheel is generated in `dist/`, e.g.:

- `goosefs-<version>-cp39-abi3-manylinux_2_28_x86_64.whl`
- `goosefs-<version>-cp39-abi3-manylinux_2_28_aarch64.whl` (ARM)

Build both `x86_64` and `aarch64` wheels if you deploy to both Intel and ARM
Tencent Cloud instances (see the per-approach ARM commands above).

---

## Option 1: Upload to Official PyPI

`maturin` can upload directly (it wraps twine):

```bash
 maturin upload dist/*.whl \
     --username __token__ \
     --password <your-pypi-token>
```

Or use twine:

```bash
 uvx twine upload dist/* \
     --username __token__ \
     --password <your-pypi-token>
```

### Installation Verification (on Tencent Cloud Linux)

```bash
 pip install goosefs
 python -c "import goosefs; print(goosefs.__version__)"
```

### Project URL

- https://pypi.org/project/goosefs/

---

## Option 2: Upload to Tencent Internal PyPI Repository

### Upload Command

```bash
 uvx twine upload dist/*.whl \
     --repository-url https://mirrors.tencent.com/repository/pypi/tencent_pypi/simple \
     --username <username> \
     --password <Token>
```

### Installation Verification (on Tencent Cloud Linux)

```bash
 pip install goosefs -i https://mirrors.tencent.com/pypi/simple
 python -c "import goosefs; print(goosefs.__version__)"
```

---

## Parameter Description

| Parameter | Description |
| --- | --- |
| `--manylinux` | Target manylinux tag (`2_28` recommended, `2_17` for broadest compatibility). |
| `--target` | Cross-compile target, e.g. `aarch64-unknown-linux-gnu` for ARM. |
| `--out` | Output directory for the built wheels (`dist`). |
| `--repository-url` | Repository URL (can be omitted for official PyPI). |
| `--username` | Username (use `__token__` for official PyPI). |
| `--password` | Access Token. |

## Full Release Process

```bash
# 0. Enter the binding directory
 cd bindings/python

# 1. Clean old build artifacts (optional)
 rm -rf dist/

# 2. Confirm versions are aligned
 grep '^version' ../../Cargo.toml
 grep '^version' Cargo.toml

# 3. Build the manylinux wheel(s) — x86_64 (zig cross-compile, no Docker)
 rustup target add x86_64-unknown-linux-gnu
 uv run --with ziglang maturin build --release \
     --target x86_64-unknown-linux-gnu --manylinux 2_28 --zig --out dist

# 3b. (optional) Build the aarch64 wheel for ARM instances
 rustup target add aarch64-unknown-linux-gnu
 uv run --with ziglang maturin build --release \
     --target aarch64-unknown-linux-gnu --manylinux 2_28 --zig --out dist

# 4a. Upload to official PyPI
 uvx twine upload dist/* --username __token__ --password <your-pypi-token>

# 4b. Or upload to the Tencent internal repository
 uvx twine upload dist/* \
     --repository-url https://mirrors.tencent.com/repository/pypi/tencent_pypi/simple \
     --username <username> \
     --password <Token>

# 5. Create a Git tag
 git tag py-v0.1.5
 git push origin py-v0.1.5
```

## Notes

1. Before releasing a new version, update the `version` field in both
   `bindings/python/Cargo.toml` and the root `../../Cargo.toml`, and keep them
   **identical** (CI enforces version alignment).
2. **Always build a manylinux wheel** for Linux releases. A wheel built with
   `--compatibility linux` (or no manylinux tag) embeds the build machine's glibc
   and may fail to import on a different Tencent Cloud Linux host.
3. The wheel is `abi3-py39`: a single Linux wheel covers CPython 3.9 through 3.13,
   so there is no need to build one wheel per Python minor version.
4. Build separate wheels for `x86_64` and `aarch64` if you deploy to both Intel
   and ARM Tencent Cloud instances.
5. PyPI **does not allow re-uploading the same version**; bump the version to
   re-publish.
6. Keep tokens safe and never commit them to the code repository (env-only).
7. Official PyPI tokens can be created at https://pypi.org/manage/account/.
8. For local developer verification (no release), use `maturin develop --uv`;
   to produce a release wheel use the zig flow in **Approach A**
   (`uv run --with ziglang maturin build --release --target <triple> --manylinux 2_28 --zig`).
9. On macOS, **Approach A (zig)** is the simplest path — it needs no Docker and no
   Linux host, just `rustup target add <triple>` plus `ziglang`.

