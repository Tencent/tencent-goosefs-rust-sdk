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

# Release helper for the Java JNI package `com.tencent.goosefs:goosefs`.
#
# Usage (from repo root):
#   bash scripts/release/java.sh                 # linux-x86_64 + linux-aarch_64 (zig, glibc 2.28)
#                                                # plus osx-aarch_64 when run on Darwin
#   bash scripts/release/java.sh --classifier linux-x86_64
#   bash scripts/release/java.sh --native        # host classifier, no zig
#   bash scripts/release/java.sh --check         # version lockstep only
#
# Does not upload anywhere. Publishing target (Maven Central vs internal) is
# still open; this script only produces the classified-JAR layout. See
# docs/release/JAVA_RELEASE.md.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JAVA_DIR="${ROOT}/bindings/java"
cd "$ROOT"

CHECK_ONLY=0
NATIVE=0
CLASSIFIERS=()

usage() {
  cat <<'EOF'
Release helper for the Java JNI package com.tencent.goosefs:goosefs.

Usage (from repo root):
  bash scripts/release/java.sh                      # linux-x86_64 + linux-aarch_64 (zig)
                                                    # plus osx-aarch_64 on Darwin
  bash scripts/release/java.sh --classifier linux-x86_64
  bash scripts/release/java.sh --arch x86_64        # linux-x86_64 only
  bash scripts/release/java.sh --native             # host classifier, no zig
  bash scripts/release/java.sh --check              # version lockstep only

Linux gnu JARs use cargo-zigbuild with glibc 2.28. This script never publishes.

See docs/release/JAVA_RELEASE.md.
EOF
  exit "${1:-0}"
}

arch_to_classifier() {
  case "$1" in
    x86_64|amd64) echo "linux-x86_64" ;;
    aarch64|arm64) echo "linux-aarch_64" ;;
    *)
      echo "unknown --arch: $1 (expected x86_64|aarch64|all)" >&2
      exit 1
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) CHECK_ONLY=1; shift ;;
    --native) NATIVE=1; shift ;;
    --classifier)
      CLASSIFIERS+=("${2:?}")
      shift 2
      ;;
    --arch)
      case "${2:?}" in
        all)
          CLASSIFIERS+=("linux-x86_64" "linux-aarch_64")
          ;;
        *)
          CLASSIFIERS+=("$(arch_to_classifier "$2")")
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

pom_version() {
  awk '
    /<version>/ {
      gsub(/.*<version>/, "");
      gsub(/<\/version>.*/, "");
      gsub(/[[:space:]]/, "");
      print;
      exit
    }
  ' "$1"
}

SDK_VER="$(version_of Cargo.toml)"
PY_VER="$(version_of bindings/python/Cargo.toml)"
JAVA_CRATE_VER="$(version_of bindings/java/Cargo.toml)"
JAVA_POM_VER="$(pom_version bindings/java/pom.xml)"

echo "==> versions"
echo "    goosefs-sdk:     ${SDK_VER}"
echo "    goosefs-python:  ${PY_VER}"
echo "    goosefs-java:    ${JAVA_CRATE_VER}"
echo "    goosefs pom:     ${JAVA_POM_VER}"
if [[ "${SDK_VER}" != "${PY_VER}" || "${SDK_VER}" != "${JAVA_CRATE_VER}" || "${SDK_VER}" != "${JAVA_POM_VER}" ]]; then
  echo "error: version mismatch — keep Cargo.toml, bindings/python/Cargo.toml, bindings/java/Cargo.toml, and bindings/java/pom.xml aligned" >&2
  exit 1
fi

if [[ "${CHECK_ONLY}" -eq 1 ]]; then
  echo "==> version lockstep OK"
  exit 0
fi

if [[ "${NATIVE}" -eq 1 && ${#CLASSIFIERS[@]} -gt 0 ]]; then
  echo "error: --native cannot be combined with --classifier / --arch" >&2
  exit 1
fi

host_classifier() {
  local os arch classifier
  os="$(uname -s)"
  arch="$(uname -m)"
  case "${os}" in
    Darwin) classifier="osx" ;;
    Linux) classifier="linux" ;;
    MINGW*|MSYS*|CYGWIN*) classifier="windows" ;;
    *)
      echo "error: unsupported host OS: ${os}" >&2
      exit 1
      ;;
  esac
  case "${arch}" in
    arm64|aarch64) classifier="${classifier}-aarch_64" ;;
    x86_64|amd64) classifier="${classifier}-x86_64" ;;
    *)
      echo "error: unsupported host arch: ${arch}" >&2
      exit 1
      ;;
  esac
  if [[ "${classifier}" == linux-* ]] && { [[ -e /lib/ld-musl-x86_64.so.1 ]] || [[ -e /lib/ld-musl-aarch64.so.1 ]] || compgen -G "/lib/ld-musl-*.so.1" >/dev/null || compgen -G "/usr/lib/ld-musl-*.so.1" >/dev/null; }; then
    classifier="${classifier}-musl"
  fi
  echo "${classifier}"
}

if [[ "${NATIVE}" -eq 1 ]]; then
  CLASSIFIERS=("$(host_classifier)")
elif [[ ${#CLASSIFIERS[@]} -eq 0 ]]; then
  CLASSIFIERS=("linux-x86_64" "linux-aarch_64")
  if [[ "$(uname -s)" == "Darwin" ]]; then
    CLASSIFIERS+=("osx-aarch_64")
  fi
fi

use_zigbuild() {
  local classifier="$1"
  if [[ "${NATIVE}" -eq 1 ]]; then
    echo "false"
    return
  fi
  case "${classifier}" in
    linux-x86_64|linux-aarch_64) echo "true" ;;
    *) echo "false" ;;
  esac
}

need_zig=0
for classifier in "${CLASSIFIERS[@]}"; do
  if [[ "$(use_zigbuild "${classifier}")" == "true" ]]; then
    need_zig=1
    break
  fi
done
if [[ "${need_zig}" -eq 1 ]]; then
  if ! cargo zigbuild --help >/dev/null 2>&1; then
    echo "error: cargo-zigbuild is required for linux gnu classified JARs" >&2
    echo "  cargo install cargo-zigbuild" >&2
    echo "  and install Zig 0.13+" >&2
    exit 1
  fi
  if ! command -v zig >/dev/null 2>&1; then
    echo "error: zig is required for cargo-zigbuild (glibc 2.28)" >&2
    exit 1
  fi
fi

DIST="${JAVA_DIR}/dist"
mkdir -p "${DIST}"
echo "==> cleaning leftover JARs in ${DIST}"
find "${DIST}" -maxdepth 1 -name "goosefs-*.jar" -print -delete

copy_jar() {
  local src="$1"
  if [[ ! -f "${src}" ]]; then
    echo "error: expected artifact missing: ${src}" >&2
    exit 1
  fi
  cp -f "${src}" "${DIST}/"
  echo "    $(basename "${src}")"
}

for classifier in "${CLASSIFIERS[@]}"; do
  zig="$(use_zigbuild "${classifier}")"
  echo "==> package classifier=${classifier} zigbuild=${zig} profile=release"
  (
    cd "${JAVA_DIR}"
    ./mvnw -B -DskipTests -Prelease \
      "-Djni.classifier=${classifier}" \
      "-Dcargo-build.enableZigbuild=${zig}" \
      package
  )

  copy_jar "${JAVA_DIR}/target/goosefs-${SDK_VER}.jar"
  copy_jar "${JAVA_DIR}/target/goosefs-${SDK_VER}-${classifier}.jar"
  copy_jar "${JAVA_DIR}/target/goosefs-${SDK_VER}-sources.jar"
  copy_jar "${JAVA_DIR}/target/goosefs-${SDK_VER}-javadoc.jar"

  if jar tf "${DIST}/goosefs-${SDK_VER}.jar" | grep -q '^native/'; then
    echo "error: unclassified JAR must not contain native/" >&2
    exit 1
  fi
  if ! jar tf "${DIST}/goosefs-${SDK_VER}-${classifier}.jar" | grep -q "^native/${classifier}/"; then
    echo "error: classified JAR missing native/${classifier}/" >&2
    exit 1
  fi
done

echo "==> artifacts in ${DIST}"
ls -la "${DIST}"
echo
echo "Dry-run OK. Downstream needs the unclassified JAR plus one classified JAR."
echo "Publishing target is still open (Maven Central vs internal registry)."
echo
echo "Local install (current host classifier):"
echo "  cd bindings/java && ./mvnw -B -DskipTests install"
