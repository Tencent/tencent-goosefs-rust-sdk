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

# Run Rust benchmarks. Offline benches always run; cluster benches need GooseFS.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

MODE="${1:-all}" # offline | cluster | all

run_offline() {
  echo "==> offline: cache_evictor_bench"
  BENCH_NUM_PAGES="${BENCH_NUM_PAGES:-200}" \
  BENCH_CONCURRENCY="${BENCH_CONCURRENCY:-1,4}" \
  BENCH_ITERS_PER_TASK="${BENCH_ITERS_PER_TASK:-200}" \
  BENCH_USE_URING="${BENCH_USE_URING:-0}" \
    cargo run --release --example cache_evictor_bench

  echo "==> offline: cache_uring_bench"
  BENCH_NUM_PAGES="${BENCH_NUM_PAGES:-200}" \
  BENCH_ITERS_PER_TASK="${BENCH_ITERS_PER_TASK:-200}" \
    cargo run --release --example cache_uring_bench

  echo "==> offline: master_hotpath (criterion, short)"
  cargo bench --bench master_hotpath -- \
    --warm-up-time 1 --measurement-time 2 --sample-size 10
}

run_cluster() {
  export GOOSEFS_MASTER_ADDR="${GOOSEFS_MASTER_ADDR:-127.0.0.1:9200}"
  export GOOSEFS_AUTH_TYPE="${GOOSEFS_AUTH_TYPE:-simple}"
  export GFS_ADDR="${GFS_ADDR:-${GOOSEFS_MASTER_ADDR}}"

  # Keep CI runtime bounded.
  export GFS_SIZE_MB="${GFS_SIZE_MB:-8}"
  export GFS_IO_KB="${GFS_IO_KB:-64}"
  export GFS_CONC="${GFS_CONC:-4}"
  export GFS_READS="${GFS_READS:-4}"
  export GFS_POOL="${GFS_POOL:-2}"
  export GFS_WPOOL="${GFS_WPOOL:-1}"
  export GFS_META_OPS="${GFS_META_OPS:-200}"
  export GFS_META_CONC="${GFS_META_CONC:-16}"
  export GFS_TAG="${GFS_TAG:-ci}"

  echo "==> cluster: partv_perf_verify (size=${GFS_SIZE_MB}MiB)"
  cargo run --release --example partv_perf_verify

  echo "==> cluster: repro_concurrent_write"
  cargo run --release --example repro_concurrent_write -- 2 2
}

case "${MODE}" in
  offline) run_offline ;;
  cluster) run_cluster ;;
  all)
    run_offline
    run_cluster
    ;;
  *)
    echo "usage: $0 [offline|cluster|all]" >&2
    exit 2
    ;;
esac

echo "Rust benchmarks (${MODE}) finished."
