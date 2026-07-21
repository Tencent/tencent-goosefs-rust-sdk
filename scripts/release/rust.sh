#!/usr/bin/env bash
# Copyright (C) 2026 Tencent. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Release helper for the Rust crate `goosefs-sdk` (crates.io).
#
# Usage (from repo root):
#   bash scripts/release/rust.sh              # preflight + dry-run (default)
#   bash scripts/release/rust.sh --publish    # real crates.io publish
#   bash scripts/release/rust.sh --skip-tests # skip cargo test
#
# Auth for --publish:
#   export CARGO_REGISTRY_TOKEN=...   # https://crates.io/settings/tokens
#
# Version bumps and git tags are intentional manual steps — see docs/release/RELEASE.md.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PUBLISH=0
SKIP_TESTS=0
ALLOW_DIRTY=0

usage() {
  cat <<'EOF'
Release helper for the Rust crate goosefs-sdk (crates.io).

Usage (from repo root):
  bash scripts/release/rust.sh              # preflight + dry-run (default)
  bash scripts/release/rust.sh --publish    # real crates.io publish
  bash scripts/release/rust.sh --skip-tests # skip cargo test
  bash scripts/release/rust.sh --allow-dirty

Auth for --publish:
  export CARGO_REGISTRY_TOKEN=...

See docs/release/RELEASE.md.
EOF
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --publish) PUBLISH=1; shift ;;
    --skip-tests) SKIP_TESTS=1; shift ;;
    --allow-dirty) ALLOW_DIRTY=1; shift ;;
    -h|--help) usage 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage 1
      ;;
  esac
done

version_of() {
  # Extract package version from a Cargo.toml (first `version = "..."` under [package]).
  awk '
    /^\[package\]/ { in_pkg=1; next }
    /^\[/ { in_pkg=0 }
    in_pkg && /^version[[:space:]]*=/ {
      gsub(/"/, "", $3); print $3; exit
    }
  ' "$1"
}

SDK_VER="$(version_of Cargo.toml)"
PY_VER="$(version_of bindings/python/Cargo.toml)"

echo "==> versions"
echo "    goosefs-sdk:     ${SDK_VER}"
echo "    goosefs-python:  ${PY_VER}"
if [[ "${SDK_VER}" != "${PY_VER}" ]]; then
  echo "error: version mismatch — keep Cargo.toml and bindings/python/Cargo.toml aligned" >&2
  exit 1
fi

if [[ "${SKIP_TESTS}" -eq 0 ]]; then
  echo "==> cargo test"
  cargo test
else
  echo "==> skipping cargo test (--skip-tests)"
fi

echo "==> cargo doc --no-deps"
cargo doc --no-deps

EXTRA=()
if [[ "${ALLOW_DIRTY}" -eq 1 ]]; then
  EXTRA+=(--allow-dirty)
fi

# Bash 3.2 (macOS /bin/bash) + `set -u` treats an empty `"${EXTRA[@]}"` as
# unbound; `${EXTRA[@]+"${EXTRA[@]}"}` expands to nothing safely.
if [[ "${PUBLISH}" -eq 0 ]]; then
  echo "==> cargo publish --dry-run"
  cargo publish --dry-run ${EXTRA[@]+"${EXTRA[@]}"}
  echo
  echo "Dry-run OK. To publish for real:"
  echo "  export CARGO_REGISTRY_TOKEN=..."
  echo "  bash scripts/release/rust.sh --publish"
  echo
  echo "Then tag: git tag v${SDK_VER} && git push origin v${SDK_VER}"
  exit 0
fi

if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "error: set CARGO_REGISTRY_TOKEN before --publish" >&2
  exit 1
fi

echo "==> cargo publish"
cargo publish --token "${CARGO_REGISTRY_TOKEN}" ${EXTRA[@]+"${EXTRA[@]}"}
echo
echo "Published goosefs-sdk ${SDK_VER} to crates.io."
echo "Tag when ready: git tag v${SDK_VER} && git push origin v${SDK_VER}"
