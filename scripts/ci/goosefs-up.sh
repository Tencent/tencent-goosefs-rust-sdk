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
docker compose -f docker-compose-goosefs.yml up -d --wait
docker compose -f docker-compose-goosefs.yml ps
echo
echo "export GOOSEFS_MASTER_ADDR=127.0.0.1:9200"
echo "export GOOSEFS_AUTH_TYPE=simple"
if [[ -n "${GOOSEFS_IMAGE:-}" ]]; then
  echo "# used GOOSEFS_IMAGE=${GOOSEFS_IMAGE}"
fi
