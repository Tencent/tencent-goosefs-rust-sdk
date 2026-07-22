# Contributing

Thanks for your interest in contributing to the Tencent GooseFS Rust / Python SDK.

## Before you start

1. Search [existing issues](https://github.com/Tencent/tencent-goosefs-rust-sdk/issues)
   or open a new one to discuss larger changes.
2. Fork the repository and create a topic branch from `main`.
3. Keep pull requests focused. Prefer small, reviewable diffs.

By contributing, you agree that your contributions will be licensed under the
Apache License, Version 2.0 (see [`LICENSE`](LICENSE)).

Please follow the [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).

## Development setup

Requirements:

- Rust **1.88+** ([rustup](https://rustup.rs/))
- Optional: Docker (for integration tests / examples against a live GooseFS)
- Optional: Python 3.9+ and [uv](https://docs.astral.sh/uv/) for the Python binding

```bash
git clone https://github.com/Tencent/tencent-goosefs-rust-sdk.git
cd tencent-goosefs-rust-sdk
cargo build
cargo test
```

Protobuf code under `src/generated/` is checked in. Downstream builds do **not**
need `protoc`. Only regenerate when you change files under `proto/`:

```bash
GOOSEFS_SDK_REGEN_PROTO=1 cargo build
```

### Local GooseFS cluster (Docker)

```bash
bash scripts/ci/goosefs-up.sh
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
export GOOSEFS_AUTH_TYPE=simple

# Rust ignored IT + examples (same scripts as CI)
bash scripts/ci/run_rust_integration.sh
bash scripts/ci/run_rust_examples.sh

bash scripts/ci/goosefs-down.sh
```

The default image is hosted on Tencent Cloud Container Registry. If pulls from
GitHub-hosted runners are slow or fail, override the image (for example after
mirroring it to GHCR/Docker Hub):

```bash
export GOOSEFS_IMAGE=ghcr.io/<org>/goosefs:v2.1.0.1
bash scripts/ci/goosefs-up.sh
```

### Python binding

```bash
cd bindings/python
uv sync --all-extras --group dev --group test
uv run maturin develop --uv
uv run pytest -v
```

See [`bindings/python/DEVELOPMENT.md`](bindings/python/DEVELOPMENT.md).

## Pull requests

- Describe **why** the change is needed and how you tested it.
- Match the existing code style; run `cargo fmt` and `cargo clippy` for Rust.
- Update docs or examples when user-facing behavior changes.
- Do not commit secrets, credentials, or machine-local absolute paths.

### PR title convention (this is what makes commits link to their PR)

Give every pull request a title of the form:

```
[area] Short summary
```

for example:

```
[sdk] Reduce log verbosity in SplitGenerator
[sdk][py] Add retry to Python upload
```

The `area` tag is a lowercase identifier (letters, digits, and `/`, `-`, `_`
separators), e.g. `sdk`, `py`, `ci`, `docs`, `rust`. Multiple areas may be
combined: `[sdk][py]`. Titles starting with `Revert`, `Release`, `Bump`,
`Merge`, `Initial`, or `chore(release):` are exempt from the `[area]` prefix.

**Why this matters:** PRs are squash-merged. On squash-merge, GitHub
automatically appends the PR number to the commit title, producing a commit
that links back to the PR — the same behavior as Apache Fluss:

```
[sdk] Reduce log verbosity in SplitGenerator (#3700)
                                                 ^^^^^^^^ GitHub adds this
```

The `[area] Summary` convention standardizes squash-merged commit titles.
GitHub's squash-merge behavior — not the area prefix — appends the clickable
`(#NNNN)` PR link on merge. A CI workflow
(`.github/workflows/pr_title.yml`) validates the title on every PR; an advisory
local hook can be installed with `bash scripts/hooks/install-hooks.sh`.

> Note: squash-merge must stay enabled for the repository (the default for
> GitHub). If a PR is instead merge-committed, no `(#NNNN)` is appended.

## Security

Report vulnerabilities privately via [`SECURITY.md`](SECURITY.md). Do not file
public issues for security reports.

## License headers

Source files use the Apache-2.0 header from [`license-header.txt`](license-header.txt).
CI checks headers with `scripts/ci/check_license_headers.py`.
