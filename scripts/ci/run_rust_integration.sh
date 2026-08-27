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

# Run ignored Rust integration tests against the Docker GooseFS fixture.
#
# Targets are DISCOVERED from `tests/*.rs`, not listed by hand. This script
# used to enumerate every `--test` explicitly, which silently skips any suite
# added later: a new file runs nowhere until someone remembers to edit this
# script, and a green CI says nothing about it. Discovery inverts that — a new
# suite runs by default, and skipping one takes a deliberate entry in SKIP
# below.
#
# NOT COVERED HERE: the io_uring page store tests. Everything run below is a
# `--test <file>`, i.e. an integration test under `tests/`; the uring tests are
# `#[ignore]`d unit tests inside `src/cache/store/uring/store.rs`, so `--lib` is
# never named and nothing under `tests/` exercises that store. `ci.yml` skips
# them too (`nextest` ignores `#[ignore]` by default). They therefore run only
# when someone executes them by hand on an io_uring-capable host.
#
# That is a real gap — it hid a bug that closed stderr and aborted the test
# process partway through the suite. Adding
#
#     cargo test --lib uring -- --ignored --nocapture --test-threads=1
#
# is the obvious fix but is UNVERIFIED on this runner: if `OP_OPENAT` is denied
# (EPERM, which is why the tests are ignored in the first place), `temp_store`
# probes, returns `None`, and every test skips silently — leaving the job green
# while covering nothing. Check that the runner can perform an io_uring OPENAT
# before adding the line, otherwise it buys false confidence rather than
# coverage. See the module comment in `store.rs` for the full picture.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export GOOSEFS_MASTER_ADDR="${GOOSEFS_MASTER_ADDR:-127.0.0.1:9200}"
export GOOSEFS_AUTH_TYPE="${GOOSEFS_AUTH_TYPE:-simple}"

# Default features are empty; page-cache / metadata-cache tests declare
# `required-features`. Keep one feature set so cargo does not rebuild
# between targets.
FEATURES=(--features full-client)

# Suites that cannot pass against the Docker fixture, e.g. because they need a
# co-located worker block store on the host filesystem, which the containerised
# worker does not give the test process access to. Empty today: the only such
# suites were the short-circuit ones, removed with that read path.
SKIP=""

skipped() {
  case " $SKIP " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

# Keep SKIP honest: a renamed or deleted suite would otherwise leave a dead
# entry here that quietly excuses nothing, and the next suite to take that
# name would be skipped without anyone asking for it.
for name in $SKIP; do
  if [[ ! -f "tests/$name.rs" ]]; then
    echo "error: SKIP lists '$name' but tests/$name.rs does not exist." >&2
    echo "       Remove the stale entry, or fix the name." >&2
    exit 1
  fi
done

targets=""
while IFS= read -r file; do
  name="$(basename "$file" .rs)"
  if skipped "$name"; then
    echo "==> skipping $name (listed in SKIP)"
    continue
  fi
  targets="$targets $name"
done < <(find tests -maxdepth 1 -name '*.rs' | sort)

if [[ -z "${targets// /}" ]]; then
  echo "error: no integration test targets found under tests/." >&2
  exit 1
fi

# `--test-threads=1` throughout: these suites share one cluster, and several
# assert on cluster-wide state (worker capacity, metrics counters) that a
# concurrent suite can move underneath them.
failed=""
for name in $targets; do
  echo "==> integration: $name"
  if ! cargo test --test "$name" "${FEATURES[@]}" -- --ignored --nocapture --test-threads=1; then
    # Keep going so one broken suite does not mask the state of the rest;
    # the script still exits non-zero below.
    echo "!!! integration suite failed: $name" >&2
    failed="$failed $name"
  fi
done

if [[ -n "${failed// /}" ]]; then
  echo "Rust Docker integration tests FAILED:$failed" >&2
  exit 1
fi

echo "Rust Docker integration tests finished:$targets"
