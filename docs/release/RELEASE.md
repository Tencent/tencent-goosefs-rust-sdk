# Release Guide — Rust (`goosefs-sdk`)

Publish the Rust crate to [crates.io](https://crates.io/crates/goosefs-sdk).

For the Python package, see [`PYTHON_RELEASE.md`](PYTHON_RELEASE.md).

## GitHub Actions (preferred)

Workflow: [Publish Rust SDK](https://github.com/Tencent/tencent-goosefs-rust-sdk/actions/workflows/publish-rust-sdk.yml)
(`.github/workflows/publish-rust-sdk.yml`).

1. Bump `version` in root `Cargo.toml` **and** `bindings/python/Cargo.toml` (keep identical).
2. Update [`CHANGELOG.md`](../../CHANGELOG.md).
3. Merge the workflow file (and the version bump) to `main`. GitHub Actions only
   lists a `workflow_dispatch` workflow after it exists on the default branch.
4. Ensure CI is green.
5. Publish from the Actions UI (below) **or** push a matching tag:
   `git tag v0.2.1 && git push origin v0.2.1`.
6. Create the GitHub Release if you have not already.

### Run from the Actions UI

1. Open **Actions** → **Publish Rust SDK** → **Run workflow**.
2. First run with **dry_run** checked. This only runs `cargo publish --dry-run`;
   nothing is uploaded to crates.io.
3. After dry_run succeeds, run the workflow again **without** dry_run to upload.

The first real (non-dry-run) publish creates the GitHub Environment `crates-io`.
Add **Required reviewers** on that environment later if you want a human gate
before upload.

Auth (pick one; configure once):

| Method | Setup |
|--------|--------|
| Trusted Publishing (OIDC) | crates.io crate settings → Trusted Publishing: owner `Tencent`, repo `tencent-goosefs-rust-sdk`, workflow `publish-rust-sdk.yml`, environment `crates-io` |
| API token | Repository secret `CARGO_REGISTRY_TOKEN` (Settings → Secrets and variables → Actions). Do not store a crates.io username; the token is enough. |

Repository secrets are visible to the publish job even when it uses the
`crates-io` environment.

## Script (local)

From the repository root:

```bash
# Preflight: version alignment, cargo test, cargo doc, cargo publish --dry-run
bash scripts/release/rust.sh

# Real publish
export CARGO_REGISTRY_TOKEN=...   # https://crates.io/settings/tokens
bash scripts/release/rust.sh --publish
```

Useful flags:

| Flag | Meaning |
|------|---------|
| `--publish` | Upload to crates.io (default is dry-run only) |
| `--skip-tests` | Skip `cargo test` |
| `--allow-dirty` | Pass `--allow-dirty` to `cargo publish` (not recommended) |

## Notes

- crates.io does **not** allow overwriting a published version (only yank).
- Never commit tokens; use Trusted Publishing or `CARGO_REGISTRY_TOKEN`.
- After publish, the crate tarball source is public.
