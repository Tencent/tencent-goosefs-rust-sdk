---
name: New Release
about: Use this template to start tracking a new release
title: "Tracking issues of GooseFS Rust SDK ${version} Release"
labels: ["release"]
---

This issue tracks tasks for the GooseFS Rust / Python SDK `${version}` release.

## Tasks

### Blockers

<!-- Blockers that must be completed before the release. -->

### Prepare

- [ ] Bump version
  - [ ] Rust crate (`Cargo.toml` / workspace)
  - [ ] Python binding (`bindings/python`)
- [ ] Update changelog / release notes
- [ ] Confirm CI is green on `main`
- [ ] Smoke-test against a live GooseFS cluster (Docker fixture or staging)

### Publish

- [ ] Tag the release (`vX.Y.Z`)
- [ ] Publish Rust crate to crates.io
- [ ] Publish Python wheels to PyPI
- [ ] Create GitHub Release with notes and artifacts

### Announce

- [ ] Announce the release to stakeholders / related repos (OpenDAL GooseFS service, Lance, etc.)
