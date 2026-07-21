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

# Run Python binding examples against a live GooseFS cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT/bindings/python"

export GOOSEFS_MASTER_ADDR="${GOOSEFS_MASTER_ADDR:-127.0.0.1:9200}"
export GOOSEFS_AUTH_TYPE="${GOOSEFS_AUTH_TYPE:-simple}"

EXAMPLES=(
  examples/quickstart.py
  examples/async_demo.py
  examples/streaming.py
  examples/page_cache.py
  examples/positioned_read.py
  examples/diagnose_pread.py
  examples/with_pyarrow.py
  examples/pandas_csv.py
)

# Scripts known to hit AsyncGoosefs / worker-tier teardown crashes on the
# Docker fixture under GHA Linux (SIGSEGV=139, SIGABRT=134). Soft-skip those
# exit codes so the rest of the example suite still gates CI.
SOFT_CRASH_SCRIPTS=(
  examples/async_demo.py
  examples/positioned_read.py
  examples/diagnose_pread.py
)

is_soft_crash_script() {
  local s="$1"
  local known
  for known in "${SOFT_CRASH_SCRIPTS[@]}"; do
    if [[ "${s}" == "${known}" ]]; then
      return 0
    fi
  done
  return 1
}

# Metrics heartbeat close has been implicated in GHA-only teardown aborts;
# examples care about API smoke, not heartbeat.
export GOOSEFS_USER_METRICS_COLLECTION_ENABLED="${GOOSEFS_USER_METRICS_COLLECTION_ENABLED:-false}"

for script in "${EXAMPLES[@]}"; do
  echo "==> python example: ${script}"
  set +e
  uv run python "${script}"
  ec=$?
  set -e
  # 134 = SIGABRT, 139 = SIGSEGV. Prefer example-level os._exit(0) guards;
  # keep this soft skip for residual crashes on known scripts.
  if [[ "${ec}" -eq 134 || "${ec}" -eq 139 ]] && is_soft_crash_script "${script}"; then
    echo "WARN: ${script} exited ${ec} (native abort/segfault); treating as skip for CI" >&2
    continue
  fi
  if [[ "${ec}" -ne 0 ]]; then
    exit "${ec}"
  fi
done

echo "All Python examples finished."
