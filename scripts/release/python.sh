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

# Release helper for the Python package `goosefs` (PyPI / manylinux wheels).
#
# Usage (from repo root):
#   bash scripts/release/python.sh                 # build linux x86_64 + aarch64 (zig)
#   bash scripts/release/python.sh --arch x86_64   # only one arch
#   bash scripts/release/python.sh --native        # build on current Linux host (no zig)
#   bash scripts/release/python.sh --publish       # build (if needed) + upload to PyPI
#   bash scripts/release/python.sh --publish --skip-build  # upload existing dist/*
#
# Auth for --publish (pick one):
#   export MATURIN_PYPI_TOKEN=...     # preferred (maturin upload)
#   export UV_PUBLISH_TOKEN=...       # alias accepted by this script
#
# Version bumps and git tags are intentional manual steps — see
# docs/release/PYTHON_RELEASE.md.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PY_DIR="${ROOT}/bindings/python"
cd "$ROOT"

PUBLISH=0
SKIP_BUILD=0
NATIVE=0
MANYLINUX="${MANYLINUX:-2_28}"
ARCHS=("x86_64" "aarch64")

usage() {
  cat <<'EOF'
Release helper for the Python package goosefs (PyPI / manylinux wheels).

Usage (from repo root):
  bash scripts/release/python.sh                 # build linux x86_64 + aarch64 (zig)
  bash scripts/release/python.sh --arch x86_64   # only one arch
  bash scripts/release/python.sh --native        # build on current Linux host
  bash scripts/release/python.sh --publish       # build + upload to PyPI
  bash scripts/release/python.sh --publish --skip-build

Auth for --publish:
  export MATURIN_PYPI_TOKEN=...   # or UV_PUBLISH_TOKEN

See docs/release/PYTHON_RELEASE.md.
EOF
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --publish) PUBLISH=1; shift ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --native) NATIVE=1; shift ;;
    --manylinux)
      MANYLINUX="${2:?}"
      shift 2
      ;;
    --arch)
      case "${2:?}" in
        x86_64|amd64) ARCHS=("x86_64") ;;
        aarch64|arm64) ARCHS=("aarch64") ;;
        all) ARCHS=("x86_64" "aarch64") ;;
        *)
          echo "unknown --arch: $2 (expected x86_64|aarch64|all)" >&2
          exit 1
          ;;
      esac
      shift 2
      ;;
    -h|--help) usage 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage 1
      ;;
  esac
done

version_of() {
  awk '
    /^\[package\]/ { in_pkg=1; next }
    /^\[/ { in_pkg=0 }
    in_pkg && /^version[[:space:]]*=/ {
      gsub(/"/, "", $3); print $3; exit
    }
  ' "$1"
}

triple_for() {
  case "$1" in
    x86_64) echo "x86_64-unknown-linux-gnu" ;;
    aarch64) echo "aarch64-unknown-linux-gnu" ;;
    *)
      echo "unsupported arch: $1" >&2
      exit 1
      ;;
  esac
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

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  mkdir -p "${PY_DIR}/dist"
  if [[ "${NATIVE}" -eq 1 ]]; then
    echo "==> maturin build --native (manylinux ${MANYLINUX})"
    (
      cd "${PY_DIR}"
      uv run maturin build --release --manylinux "${MANYLINUX}" --out dist
    )
  else
    for arch in "${ARCHS[@]}"; do
      triple="$(triple_for "${arch}")"
      echo "==> rustup target add ${triple}"
      rustup target add "${triple}"
      echo "==> maturin build --zig ${triple} (manylinux ${MANYLINUX})"
      (
        cd "${PY_DIR}"
        uv run --with ziglang maturin build --release \
          --target "${triple}" \
          --manylinux "${MANYLINUX}" \
          --zig \
          --out dist
      )
    done
  fi
  echo "==> wheels in ${PY_DIR}/dist"
  ls -la "${PY_DIR}/dist"
else
  echo "==> skipping build (--skip-build)"
fi

if [[ "${PUBLISH}" -eq 0 ]]; then
  echo
  echo "Build OK. To upload to PyPI:"
  echo "  export MATURIN_PYPI_TOKEN=..."
  echo "  bash scripts/release/python.sh --publish --skip-build"
  echo
  echo "Then tag: git tag py-v${PY_VER} && git push origin py-v${PY_VER}"
  exit 0
fi

TOKEN="${MATURIN_PYPI_TOKEN:-${UV_PUBLISH_TOKEN:-}}"
if [[ -z "${TOKEN}" ]]; then
  echo "error: set MATURIN_PYPI_TOKEN (or UV_PUBLISH_TOKEN) before --publish" >&2
  exit 1
fi

shopt -s nullglob
wheels=("${PY_DIR}/dist"/*.whl)
if [[ ${#wheels[@]} -eq 0 ]]; then
  echo "error: no wheels in ${PY_DIR}/dist — build first or drop --skip-build" >&2
  exit 1
fi

echo "==> maturin upload (${#wheels[@]} wheel(s))"
(
  cd "${PY_DIR}"
  # maturin reads the token from env when --password is omitted in newer
  # versions; pass explicitly for compatibility.
  uv run maturin upload dist/*.whl --username __token__ --password "${TOKEN}"
)

echo
echo "Published goosefs ${PY_VER} to PyPI."
echo "Tag when ready: git tag py-v${PY_VER} && git push origin py-v${PY_VER}"
