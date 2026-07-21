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

# Run Python binding benchmarks against a live GooseFS cluster (CI-sized).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT/bindings/python"

export GOOSEFS_MASTER_ADDR="${GOOSEFS_MASTER_ADDR:-127.0.0.1:9200}"
export GOOSEFS_AUTH_TYPE="${GOOSEFS_AUTH_TYPE:-simple}"
export GFS_ADDR="${GFS_ADDR:-${GOOSEFS_MASTER_ADDR}}"
export GFS_SIZE_MB="${GFS_SIZE_MB:-8}"
export GFS_IO_KB="${GFS_IO_KB:-64}"
export GFS_CONC="${GFS_CONC:-4}"
export GFS_READS="${GFS_READS:-4}"
export GFS_POOL="${GFS_POOL:-1}"
export GFS_WPOOL="${GFS_WPOOL:-1}"
export GFS_MODE="${GFS_MODE:-both}"
export GFS_TAG="${GFS_TAG:-ci}"

echo "==> python bench: bench_perf_opt.py"
uv run python benchmarks/bench_perf_opt.py --paths 50 --iters 2 --threads 4

echo "==> python bench: bench_pr_concurrency.py"
uv run python benchmarks/bench_pr_concurrency.py

echo "==> python bench: bench_sr_sw.py"
uv run python benchmarks/bench_sr_sw.py

echo "Python benchmarks finished."
