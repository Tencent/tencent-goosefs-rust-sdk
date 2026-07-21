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
  short_circuit_demo
  metrics_heartbeat
)

echo "==> Building examples"
cargo build --examples

for name in "${EXAMPLES[@]}"; do
  echo "==> example: ${name}"
  cargo run --example "${name}"
done

echo "==> example: ha_multi_master (single master)"
cargo run --example ha_multi_master -- "${GOOSEFS_MASTER_ADDR}"

# metrics_pushgateway needs a Pushgateway on :9091; only run when present.
if python3 -c 'import socket; socket.create_connection(("127.0.0.1", 9091), 1).close()'; then
  echo "==> example: metrics_pushgateway"
  cargo run --example metrics_pushgateway
else
  echo "==> skip metrics_pushgateway (no Pushgateway on 127.0.0.1:9091)"
fi

echo "All Rust examples finished."
