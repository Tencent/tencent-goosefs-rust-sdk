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

# Run ignored Rust integration tests that work against the Docker GooseFS fixture.
#
# Short-circuit suites (short_circuit_e2e / sc_consistency / sc_inv_s3) need a
# co-located worker block store on the host filesystem and are excluded here.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export GOOSEFS_MASTER_ADDR="${GOOSEFS_MASTER_ADDR:-127.0.0.1:9200}"
export GOOSEFS_AUTH_TYPE="${GOOSEFS_AUTH_TYPE:-simple}"

echo "==> integration: page_cache_e2e"
cargo test --test page_cache_e2e -- --ignored --nocapture

echo "==> integration: page_cache_consistency"
cargo test --test page_cache_consistency -- --ignored --nocapture --test-threads=1

echo "==> integration: master_rename_no_replace"
cargo test --test master_rename_no_replace -- --ignored --nocapture --test-threads=1

echo "==> integration: auth_retry (ignored)"
cargo test --test auth_retry -- --ignored --nocapture

echo "==> integration: connection_reuse (ignored)"
cargo test --test connection_reuse -- --ignored --nocapture

echo "==> integration: metrics_heartbeat (ignored)"
cargo test --test metrics_heartbeat -- --ignored --nocapture

echo "Rust Docker integration tests finished."
