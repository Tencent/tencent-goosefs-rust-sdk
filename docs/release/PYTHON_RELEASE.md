# Release Guide — Python (`goosefs`)

Publish the PyO3 / maturin wheel to [PyPI](https://pypi.org/project/goosefs/).

For the Rust crate, see [`RELEASE.md`](RELEASE.md).
For the Java binding, see [`JAVA_RELEASE.md`](JAVA_RELEASE.md).

`goosefs` is a native extension: Linux releases must be **manylinux** wheels so
they are not tied to the build machine's glibc.

## GitHub Actions (preferred)

Workflow: [Publish Python SDK](https://github.com/Tencent/tencent-goosefs-rust-sdk/actions/workflows/publish-python-sdk.yml)
(`.github/workflows/publish-python-sdk.yml`).

Builds and uploads:

- Linux **manylinux_2_28** `x86_64` + `aarch64` (zig, same path as `scripts/release/python.sh`)
- macOS arm64 (`macos-14`)
- Windows `win_amd64` (MSVC)
- sdist

1. Bump `version` in root `Cargo.toml`, `bindings/python/Cargo.toml`, `bindings/java/Cargo.toml`, and `bindings/java/pom.xml` (keep identical).
2. Update changelogs.
3. Merge the workflow file (and the version bump) to `main`. GitHub Actions only
   lists a `workflow_dispatch` workflow after it exists on the default branch.
4. Ensure CI is green (including **Bindings Python**).
5. Publish from the Actions UI (below) **or** push a matching tag:
   `git tag py-v0.2.1 && git push origin py-v0.2.1`.
6. Create the GitHub Release if you have not already.

### Run from the Actions UI

1. Open **Actions** → **Publish Python SDK** → **Run workflow**.
2. First run with **dry_run** checked. This only builds wheels; nothing is
   uploaded to PyPI.
3. After dry_run succeeds, run the workflow again **without** dry_run to upload.

The first real (non-dry-run) publish creates the GitHub Environment `pypi`.
Add **Required reviewers** on that environment later if you want a human gate
before upload.

Auth (pick one; configure once):

| Method | Setup |
|--------|--------|
| Trusted Publishing (OIDC) | PyPI project settings → Publishing: owner `Tencent`, repo `tencent-goosefs-rust-sdk`, workflow `publish-python-sdk.yml`, environment `pypi` |
| API token | Repository secret `MATURIN_PYPI_TOKEN` (Settings → Secrets and variables → Actions) |

For the API-token path, a `~/.pypirc` that looks like:

```ini
[pypi]
username = __token__
password = pypi-...
```

maps to GitHub as follows: do **not** store the username (`__token__` is already
hardcoded in the workflow). Store only the `password` value (the full `pypi-...`
string, including the prefix) as `MATURIN_PYPI_TOKEN`.

Repository secrets are visible to the publish job even when it uses the `pypi`
environment.

## Script (local)

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
versions match. A fresh build first deletes leftover `*.whl` / `*.tar.gz` under
`bindings/python/dist/` so a previous version cannot be uploaded. `--skip-build`
keeps existing wheels, but still removes files whose version does not match the
current `Cargo.toml` before calling `maturin upload`.

The local script only builds Linux manylinux wheels. For a full PyPI release prefer
the GitHub Actions workflow, which also produces macOS, Windows, and the sdist.
If you publish locally, copy extra platform wheels (Windows / macOS) into
`bindings/python/dist/` after the Linux build, then `--publish --skip-build`.

CI (`ci_bindings_python.yml`) still builds those wheels on every relevant
push/PR so release artifacts stay verified before you publish.

## Notes

- Prefer **manylinux** Linux wheels. A plain `--compatibility linux` wheel can
  fail on older glibc hosts (`GLIBC_x.y not found`).
- `2_28` is the default baseline (CentOS 8 / TencentOS 3 / Ubuntu 20.04+). Use
  `--manylinux 2_17` only when you must support glibc 2.17 (CentOS 7 era).
- The wheel is `abi3-py39`: one Linux wheel covers CPython 3.9+.
- PyPI does **not** allow re-uploading the same version.
- Never commit tokens; use Trusted Publishing or `MATURIN_PYPI_TOKEN`.
