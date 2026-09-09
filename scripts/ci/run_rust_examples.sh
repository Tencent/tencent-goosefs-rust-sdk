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

# Run Rust examples that need a live GooseFS cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export GOOSEFS_MASTER_ADDR="${GOOSEFS_MASTER_ADDR:-127.0.0.1:9200}"
export GOOSEFS_AUTH_TYPE="${GOOSEFS_AUTH_TYPE:-simple}"
# Docker fixture is one worker; Java ASYNC_THROUGH defaults require 2 replicas.
# Examples that call `GoosefsConfig::new` (no apply_env) still pin this in
# source; export here for any path that does overlay env.
export GOOSEFS_USER_FILE_REPLICATION_DURABLE="${GOOSEFS_USER_FILE_REPLICATION_DURABLE:-1}"
export GOOSEFS_USER_FILE_REPLICATION_DURABLE_MIN="${GOOSEFS_USER_FILE_REPLICATION_DURABLE_MIN:-1}"

EXAMPLES=(
  highlevel_file_rw
  context_file_rw
  metadata_crud
  write_types
  streaming_file_read
  seekable_file_read
  async_read_trait
  async_persistence
  lowlevel_create_file
  lowlevel_block_read
  auth_demo
  page_cache_demo
  reader_page_cache_demo
  metrics_heartbeat
  verify_checkblocks_locations
)

# Default features are empty. Enable only what each example's Cargo.toml
# `required-features` asks for — do not pull in `full-client` (reqwest, etc.).
# Keep this map in sync with the `[[example]]` tables in Cargo.toml.
example_features() {
  case "$1" in
    page_cache_demo|reader_page_cache_demo) printf '%s' 'page-cache' ;;
    metrics_pushgateway) printf '%s' 'metrics-pushgateway' ;;
  esac
}

run_example() {
  local name="$1"
  shift
  local feats
  feats="$(example_features "$name")"
  echo "==> example: ${name}"
  if [[ -n "$feats" ]]; then
    echo "    features: $feats"
    cargo run --example "$name" --features "$feats" "$@"
  else
    cargo run --example "$name" "$@"
  fi
}

echo "==> Building examples"
cargo build --examples

for name in "${EXAMPLES[@]}"; do
  run_example "$name"
done

run_example ha_multi_master -- "${GOOSEFS_MASTER_ADDR}"

# metrics_pushgateway needs a Pushgateway on :9091; only run when present.
if python3 -c 'import socket; socket.create_connection(("127.0.0.1", 9091), 1).close()'; then
  run_example metrics_pushgateway
else
  echo "==> skip metrics_pushgateway (no Pushgateway on 127.0.0.1:9091)"
fi

echo "All Rust examples finished."
