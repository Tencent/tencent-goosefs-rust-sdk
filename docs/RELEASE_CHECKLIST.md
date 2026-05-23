# Release Checklist

A pre-release task list for `goosefs-sdk`.

## 1. Crate Metadata

| Item | Current Status | Action |
|------|---------------|--------|
| Package name | `goosefs-sdk` | ✅ Final package name confirmed as `goosefs-sdk` |
| Version | `0.1.0` | ✅ First release stays at `0.1.0` |
| description | Present | ✅ |
| license | `Apache-2.0` | ✅ |
| authors | `Goosefs Team` | ✅ |
| repository | Missing | TODO: Fill in the repository URL (if going public) |
| homepage | Missing | TODO: Fill in the project homepage |
| keywords | Missing | TODO: Suggested `["goosefs", "grpc", "storage", "distributed-filesystem", "cache"]` |
| categories | Missing | TODO: Suggested `["network-programming", "filesystem"]` |
| readme | Missing | TODO: Add `readme = "README.md"` |

## 2. File Checks

- [ ] `README.md` content is complete with basic usage examples
- [ ] `LICENSE` file is present (Apache-2.0)
- [ ] `.gitignore` excludes unnecessary files
- [ ] Inspect `cargo package --list` output and confirm the packaged file set is reasonable

## 3. API Review

| Module | Current Visibility | Recommendation | Decision |
|--------|-------------------|---------------|----------|
| `auth` | `pub` | Keep public | TODO |
| `block` | `pub` | Consider whether it needs to be public (low-level API) | TODO |
| `client` | `pub` | Keep public (low-level gRPC client) | TODO |
| `config` | `pub` | Keep public | TODO |
| `error` | `pub` | Keep public | TODO |
| `io` | `pub` | Keep public (recommended high-level API entry point) | TODO |
| `retry` | `pub` | Consider whether it needs to be public (internal implementation) | TODO |
| `proto` | `pub` | Consider marking as unstable / `#[doc(hidden)]` | TODO |

## 4. Generated Proto Code

- [ ] Confirm whether the protobuf code under `src/generated/` is suitable for direct release
- [ ] Decide on the visibility strategy for the `proto` module:
  - **Option A**: Keep `pub mod proto`, document it as "advanced usage / no stability guarantee"
  - **Option B**: Change to `pub(crate) mod proto`, only expose via the high-level API
- [ ] Decide whether `.proto` source files should be included in the crate package (controlled via `include`/`exclude` in `Cargo.toml`)

## 5. Documentation

- [ ] Top-level `lib.rs` documentation (crate-level doc)
- [ ] Doc comments on core types
- [ ] Run `cargo doc --no-deps` and confirm no warnings
- [ ] Example code compiles successfully (`cargo test --doc`)

## 6. Quality Assurance

- [ ] `cargo test` all pass
- [ ] `cargo clippy` no warnings
- [ ] `cargo fmt --check` formatting check passes
- [ ] `cargo publish --dry-run` simulated publish succeeds

## 7. CI/CD

- [ ] TODO: Set up CI pipeline (GitHub Actions / internal pipeline)
- [ ] TODO: Configure the `CARGO_REGISTRY_TOKEN` secret
- [ ] TODO: Configure release-tag-triggered auto publish (optional)

## 8. Release Execution

```bash
# 1. Final verification
cargo test
cargo publish --dry-run

# 2. Create a Git tag
git tag v0.1.0
git push origin v0.1.0

# 3. Publish
cargo publish --token <token>

# 4. Verify
# After waiting a few minutes
cargo add goosefs-sdk  # or the final package name
```

## 9. Post-Release

- [ ] Verify the crate can be installed and used normally
- [ ] TODO: Notify relevant teams / users
- [ ] Update dependency notes in internal documentation
