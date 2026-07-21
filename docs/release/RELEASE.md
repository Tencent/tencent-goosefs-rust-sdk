# Release Guide — Rust (`goosefs-sdk`)

Publish the Rust crate to [crates.io](https://crates.io/crates/goosefs-sdk).

For the Python package, see [`PYTHON_RELEASE.md`](PYTHON_RELEASE.md).

## Script (preferred)

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

## Manual checklist (still required)

1. Bump `version` in root `Cargo.toml` **and** `bindings/python/Cargo.toml` (keep identical).
2. Update [`CHANGELOG.md`](../../CHANGELOG.md).
3. Ensure CI is green on `main`.
4. Run the script above.
5. Tag and push:

```bash
git tag v0.1.7
git push origin v0.1.7
```

## Notes

- crates.io does **not** allow overwriting a published version (only yank).
- Never commit tokens; use `CARGO_REGISTRY_TOKEN` in the environment only.
- After publish, the crate tarball source is public.
