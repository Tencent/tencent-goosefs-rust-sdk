---
sidebar_position: 2
---

# Release Python Package

Publish the PyO3 / maturin wheel to [PyPI](https://pypi.org/project/goosefs/). Linux releases must be **manylinux** wheels.

## GitHub Actions (preferred)

Workflow: [Publish Python SDK](https://github.com/Tencent/tencent-goosefs-rust-sdk/actions/workflows/publish-python-sdk.yml).

Builds and uploads Linux manylinux (`x86_64` + `aarch64`), macOS arm64, Windows `win_amd64`, and the sdist.

1. Bump `version` in root `Cargo.toml` **and** `bindings/python/Cargo.toml` (keep identical).
2. Update changelogs.
3. Merge the workflow file (and the version bump) to `main`. GitHub Actions only lists a `workflow_dispatch` workflow after it exists on the default branch.
4. Ensure CI is green (including **Bindings Python**).
5. Publish from the Actions UI (below) **or** push a matching tag: `git tag v0.2.1 && git push origin v0.2.1`.

### Run from the Actions UI

1. Open **Actions** → **Publish Python SDK** → **Run workflow**.
2. First run with **dry_run** checked (build wheels only; nothing is uploaded).
3. After dry_run succeeds, run again **without** dry_run to upload.

The first real publish creates the GitHub Environment `pypi`. Add **Required reviewers** on that environment later if you want a human gate.

Auth (configure once): PyPI Trusted Publishing (OIDC) for workflow `publish-python-sdk.yml` / environment `pypi`, or repository secret `MATURIN_PYPI_TOKEN`. Store only the PyPI API token (`pypi-...`); `username = __token__` is already hardcoded in the workflow.

## Script (local)

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

The local script only builds Linux manylinux wheels. Prefer the GitHub Actions workflow for a full multi-platform PyPI release.

The wheel is `abi3-py39`: one wheel per platform covers CPython 3.9+. Never commit tokens.
