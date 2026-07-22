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
#
# Install the repository's git hooks. The commit-message convention hook is
# optional but recommended; it reminds authors to use the
# "[area] Summary" PR-title convention so that squash-merged commits link
# back to their PR (the same behavior as Apache Fluss).
#
# Usage:
#   bash scripts/hooks/install-hooks.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOOKS_SRC="$ROOT/scripts/hooks"
# Ask Git for the real hooks path: in a linked worktree .git is a file, not a
# directory, so hardcoding "$ROOT/.git/hooks" would make mkdir fail.
HOOKS_DST="$(git -C "$ROOT" rev-parse --path-format=absolute --git-path hooks)"

mkdir -p "$HOOKS_DST"

for hook in pre-commit; do
  src="$HOOKS_SRC/$hook"
  [ -f "$src" ] || continue
  install -m 0755 "$src" "$HOOKS_DST/$hook"
  echo "installed $hook -> .git/hooks/$hook"
done

# Point git at the bundled hooks directory as well, so future hooks (e.g.
# commit-msg) are picked up automatically.
git config core.hooksPath scripts/hooks || true

echo "Done. Advisory hooks installed."
echo "See CONTRIBUTING.md for the '[area] Summary' PR-title convention."
