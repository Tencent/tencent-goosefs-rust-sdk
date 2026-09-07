---
sidebar_position: 1
---

# Release Rust Crate

Publish `goosefs-sdk` to [crates.io](https://crates.io/crates/goosefs-sdk). For the Python package, see [Release Python](./python-release).

## GitHub Actions (preferred)

Workflow: [Publish Rust SDK](https://github.com/Tencent/tencent-goosefs-rust-sdk/actions/workflows/publish-rust-sdk.yml).

1. Bump `version` in root `Cargo.toml` **and** `bindings/python/Cargo.toml` (keep identical).
2. Update [`CHANGELOG.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/CHANGELOG.md).
3. Merge the workflow file (and the version bump) to `main`. GitHub Actions only lists a `workflow_dispatch` workflow after it exists on the default branch.
4. Ensure CI is green.
5. Publish from the Actions UI (below) **or** push a matching tag: `git tag v0.2.1 && git push origin v0.2.1`.

### Run from the Actions UI

1. Open **Actions** → **Publish Rust SDK** → **Run workflow**.
2. First run with **dry_run** checked (`cargo publish --dry-run` only; nothing is uploaded).
3. After dry_run succeeds, run again **without** dry_run to upload.

The first real publish creates the GitHub Environment `crates-io`. Add **Required reviewers** on that environment later if you want a human gate.

Auth (configure once): crates.io Trusted Publishing (OIDC) for workflow `publish-rust-sdk.yml` / environment `crates-io`, or repository secret `CARGO_REGISTRY_TOKEN`. The token is enough; do not store a crates.io username.

## Script (local)

From the repository root:

```bash
# Preflight: version alignment, cargo test, cargo doc, cargo publish --dry-run
bash scripts/release/rust.sh

# Real publish
export CARGO_REGISTRY_TOKEN=...   # https://crates.io/settings/tokens
bash scripts/release/rust.sh --publish
```

| Flag            | Meaning                                       |
| --------------- | --------------------------------------------- |
| `--publish`     | Upload to crates.io (default is dry-run only) |
| `--skip-tests`  | Skip `cargo test`                             |
| `--allow-dirty` | Pass `--allow-dirty` to `cargo publish`       |

crates.io does **not** allow overwriting a published version. Never commit tokens; use Trusted Publishing or `CARGO_REGISTRY_TOKEN`.
