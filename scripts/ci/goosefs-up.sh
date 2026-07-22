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

# Start the GooseFS Docker fixture used by CI.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT/fixtures/goosefs"

COMPOSE_FILE="docker-compose-goosefs.yml"
CONTAINER_NAME="goosefs-sdk-goosefs"
# GooseFS listens on these ports: 9200 Master RPC, 9201 Master Web,
# 9202 Master Secondary (embedded), 9203 Worker RPC, 9204 Worker Web,
# 9212 WorkerManager RPC. Used by the diagnostics dump below.
GOOSEFS_PORTS="9200 9201 9202 9203 9204 9212"

# Print a best-effort diagnostics dump when the fixture fails to become
# healthy. Each probe is guarded with `|| true` / `2>/dev/null` so a single
# failing probe (e.g. container already gone) never masks the others or
# turns this helper into a hard failure.
diagnose() {
  echo
  echo "=================================================================="
  echo "GooseFS fixture failed to become healthy. Collecting diagnostics..."
  echo "=================================================================="
  echo
  echo "=== docker compose ps ==="
  docker compose -f "$COMPOSE_FILE" ps || true
  echo
  echo "=== docker inspect: OOMKilled / Health / ExitCode ==="
  docker inspect "$CONTAINER_NAME" --format '
State.Status:    {{.State.Status}}
State.Running:   {{.State.Running}}
State.OOMKilled: {{.State.OOMKilled}}
State.ExitCode:  {{.State.ExitCode}}
State.Restarting:{{.State.Restarting}}
State.StartedAt: {{.State.StartedAt}}
State.Error:     {{.State.Error}}
Health.Status:  {{if .State.Health}}{{.State.Health.Status}} (failing streak: {{.State.Health.FailingStreak}}){{else}}(no healthcheck){{end}}' 2>/dev/null || echo "(could not inspect container)"
  echo
  echo "=== listening GooseFS ports inside container ($GOOSEFS_PORTS) ==="
  docker exec "$CONTAINER_NAME" bash -c '
    if command -v ss >/dev/null 2>&1; then
      ss -ltnp
    elif command -v netstat >/dev/null 2>&1; then
      netstat -ltnp
    else
      echo "no ss/netstat available"
    fi
  ' 2>/dev/null | grep -E ":(9200|9201|9202|9203|9204|9212)" || echo "(none of the GooseFS ports are listening)"
  echo
  echo "=== java processes inside container ==="
  docker exec "$CONTAINER_NAME" bash -c '
    if command -v jps >/dev/null 2>&1; then
      jps -l
    else
      ps -ef | grep -i "[g]oosefs"
    fi
  ' 2>/dev/null || echo "(could not query processes)"
  echo
  echo "=== master log (tail 80) ==="
  docker exec "$CONTAINER_NAME" bash -c 'tail -n 80 /opt/goosefs/logs/master.log 2>/dev/null || echo "(master.log not found)"' 2>/dev/null || echo "(could not read master.log)"
  echo
  echo "=== worker log (tail 40) ==="
  docker exec "$CONTAINER_NAME" bash -c 'tail -n 40 /opt/goosefs/logs/worker.log 2>/dev/null || echo "(worker.log not found)"' 2>/dev/null || echo "(could not read worker.log)"
  echo
  echo "=== container logs (tail 60) ==="
  docker logs --tail 60 "$CONTAINER_NAME" 2>&1 || true
  echo
  echo "=== /dev/shm (host tmpfs used by the JVM) ==="
  docker exec "$CONTAINER_NAME" df -h /dev/shm 2>/dev/null || echo "(could not read /dev/shm)"
  echo
}

# `docker compose up --wait` has a `--wait-timeout` that defaults to 0
# (wait forever). Without it, an unhealthy fixture (e.g. the master never
# listening on 9200) hangs the job indefinitely and the diagnostics branch
# below never runs. Bound it so we fail fast and dump diagnostics instead.
GOOSEFS_WAIT_TIMEOUT="${GOOSEFS_WAIT_TIMEOUT:-300}"
echo "Waiting up to ${GOOSEFS_WAIT_TIMEOUT}s for the GooseFS fixture to become healthy..."
if ! docker compose -f "$COMPOSE_FILE" up -d --wait --wait-timeout "$GOOSEFS_WAIT_TIMEOUT"; then
  diagnose
  echo "ERROR: GooseFS fixture did not become healthy within ${GOOSEFS_WAIT_TIMEOUT}s."
  exit 1
fi

docker compose -f "$COMPOSE_FILE" ps
echo
echo "export GOOSEFS_MASTER_ADDR=127.0.0.1:9200"
echo "export GOOSEFS_AUTH_TYPE=simple"
if [[ -n "${GOOSEFS_IMAGE:-}" ]]; then
  echo "# used GOOSEFS_IMAGE=${GOOSEFS_IMAGE}"
fi
