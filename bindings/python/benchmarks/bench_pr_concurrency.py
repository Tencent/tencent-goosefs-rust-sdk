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

"""B1 verification — Python random-read (PR) concurrency benchmark.

Mirrors the **PR section** of the Rust example
``benchmarks/partv_perf_verify.rs`` so the two can be compared
**apples-to-apples**: identical file size, IO size, endpoint, total reads
and — crucially — the *same number of concurrent in-flight ``read_at``
calls*.

Why this exists (Part V optimisation analysis, hypothesis
B1): the remote stress reported "PR-1M Python beats Rust +47%". But the
Python ``read_at`` binding calls the **same** SDK ``read_at`` as Rust plus
one extra PyO3 hop, so at *matched* concurrency Python can only be
**slower**. Any apparent reversal must therefore come from either:

  (a) upper-layer concurrency mismatch — the Python harness issued more
      concurrent independent ``read_at`` calls than the Rust one, or
  (b) methodology difference — different file size / endpoint / buffer.

This script settles (a) vs (b) on a **local cluster**: it sweeps a matched
concurrency level for both the sync (ThreadPool) and async (gather) models
and reports aggregate MB/s, with byte-for-byte verification (C1/C2).

Decision rule
-------------
* If Python <= Rust at *every matched concurrency* (run the Rust example
  with ``GFS_POOL=1 GFS_WPOOL=1`` to match the published wheel's defaults)
  -> the remote reversal is a harness/methodology artifact. R1-A2 needs **no
  SDK change**; only document the recommended caller concurrency.
* If Python > Rust at *identical* matched concurrency & workload -> a real
  SDK-path anomaly worth investigating (would contradict the code reading).

Important fairness note
-----------------------
The published Python wheel cannot set ``worker_connection_pool_size`` /
``master_connection_pool_size`` (``Config(properties=...)`` does not parse
those keys), so Python always runs pool=1. Run the Rust comparison with
``GFS_POOL=1 GFS_WPOOL=1``.

Usage
-----
::

    # install the binding into the repo .venv first (from repo root):
    #   cd bindings/python && VIRTUAL_ENV=../../.venv maturin develop --release

    cd bindings/python
    GFS_ADDR=127.0.0.1:9200 \
    GFS_SIZE_MB=128 GFS_IO_KB=1024 GFS_CONC=16 GFS_READS=8 GFS_MODE=both \
      ../../.venv/bin/python benchmarks/bench_pr_concurrency.py

    # matched-concurrency sweep
    for c in 1 8 16 32 64; do
      GFS_CONC=$c ../../.venv/bin/python benchmarks/bench_pr_concurrency.py
    done
"""

from __future__ import annotations

# ── crash diagnostics ────────────────────────────────────────────────────────
# Enable the fault handler as early as possible so that a native crash dumps a
# Python + C-level traceback to stderr instead of dying silently:
#   * SIGSEGV (illegal memory access) → exit code 139  ← the historical CI hang
#   * SIGTERM (CI timeout / cancel)   → exit code 143  (also dump the live stack)
# SIGKILL (137, OOM) cannot be caught in-process — see kernel/cgroup OOM logs.
import faulthandler
import os
import signal
import sys

faulthandler.enable()

# Register SIGTERM with faulthandler's *native* handler. A Python-level
# signal.signal handler only runs once the main thread returns to executing
# Python bytecode; if it is blocked inside a native/Rust call (exactly the
# hang we want to diagnose), the handler never fires before the CI runner
# escalates the timeout. chain=True keeps the default termination behavior so
# the job still fails fast with the conventional SIGTERM status (128 + 15 = 143).
faulthandler.register(signal.SIGTERM, file=sys.stderr, all_threads=True, chain=True)
# ─────────────────────────────────────────────────────────────────────────────

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

from goosefs import AsyncGoosefs, Config, Goosefs

TEST_DIR = "/partv-bench"

_U64 = (1 << 64) - 1


class XorShift:
    """Identical to the Rust example's PRNG so both harnesses visit the
    exact same offset sequence per task index (removes offset distribution
    as a variable in the B1 comparison)."""

    def __init__(self, seed: int):
        self.s = (seed | 1) & _U64

    def next(self) -> int:
        x = self.s
        x ^= x >> 12
        x = (x ^ (x << 25)) & _U64
        x ^= x >> 27
        self.s = x
        return (x * 0x2545F4914F6CDD1D) & _U64


def _task_seed(t: int) -> int:
    # Mirror Rust: 0x9E3779B97F4A7C15 ^ (t as u64 + 1)
    return (0x9E3779B97F4A7C15 ^ (t + 1)) & _U64


# ── env helpers ──────────────────────────────────────────────────────────────
def _env(key: str, default):
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    return type(default)(raw)


def test_path() -> str:
    tag = os.environ.get("GFS_TAG", "")
    return f"{TEST_DIR}/data-{tag}.bin" if tag else f"{TEST_DIR}/data.bin"


# ── deterministic payload (identical to the Rust example) ────────────────────
def payload_byte(pos: int) -> int:
    return pos % 251


_PATTERN = bytes(payload_byte(i) for i in range(251))


def verify_slice(data: bytes, offset: int) -> bool:
    """True if ``data`` matches the deterministic payload at ``offset``.

    The payload repeats every 251 bytes, so we compare against a rotated
    view of the precomputed pattern rather than recomputing per byte.
    """
    if not data:
        return True
    start = offset % 251
    rotated = _PATTERN[start:] + _PATTERN[:start]
    full, rem = divmod(len(data), 251)
    expected = rotated * full + rotated[:rem]
    return data == expected


def mib_per_s(num_bytes: int, secs: float) -> float:
    return (num_bytes / (1024 * 1024)) / max(secs, 1e-9)


# ── setup: write a deterministic file once (reused if present) ───────────────
def ensure_file(fs: Goosefs, path: str, file_size: int) -> None:
    try:
        st = fs.get_status(path)
        if st.length == file_size:
            print(f"[setup] reusing existing {file_size // (1024 * 1024)} MiB file → {path}")
            return
        fs.delete(path, recursive=False)
    except Exception:
        pass

    try:
        fs.mkdir(TEST_DIR, recursive=True)
    except Exception:
        pass

    print(f"[setup] writing {file_size // (1024 * 1024)} MiB test file → {path} ...")
    # The payload depends on the absolute offset, so each 8 MiB chunk is built
    # from a rotated view of the precomputed 251-byte pattern.
    chunk = 8 * 1024 * 1024
    t0 = time.perf_counter()
    written = 0
    with fs.create_file(path, recursive=True) as w:
        while written < file_size:
            this = min(chunk, file_size - written)
            start = written % 251
            rotated = _PATTERN[start:] + _PATTERN[:start]
            full, rem = divmod(this, 251)
            buf = rotated * full + rotated[:rem]
            w.write(buf)
            written += this
    secs = time.perf_counter() - t0
    print(
        f"[setup] wrote {file_size // (1024 * 1024)} MiB in {secs:.2f}s "
        f"({mib_per_s(file_size, secs):.0f} MiB/s)"
    )


# ── PR sync (ThreadPoolExecutor) — the model most likely used by the stress ──
def pr_sync(
    fs: Goosefs, path: str, file_size: int, io_size: int, conc: int, reads_per_task: int
) -> tuple[float, int]:
    max_off = max(file_size - io_size, 1)

    def worker(seed: int) -> tuple[int, int]:
        rng = XorShift(_task_seed(seed))
        local_bytes = 0
        local_mism = 0
        reader = fs.open_file(path)
        try:
            for _ in range(reads_per_task):
                off = rng.next() % max_off
                data = reader.read_at(off, io_size)
                if not verify_slice(data, off):
                    local_mism += 1
                local_bytes += len(data)
        finally:
            reader.close()
        return local_bytes, local_mism

    t0 = time.perf_counter()
    total_bytes = 0
    total_mism = 0
    with ThreadPoolExecutor(max_workers=conc) as ex:
        for b, m in ex.map(worker, range(conc)):
            total_bytes += b
            total_mism += m
    secs = time.perf_counter() - t0
    print(
        f"    [sync ThreadPool({conc})] {conc * reads_per_task} reads, "
        f"{total_bytes / (1024 * 1024):.1f} MiB in {secs:.3f}s → "
        f"{mib_per_s(total_bytes, secs):.0f} MiB/s "
        f"{'✅' if total_mism == 0 else f'❌ {total_mism} mismatch'}"
    )
    return mib_per_s(total_bytes, secs), total_mism


# ── PR async (asyncio.gather, bounded by semaphore) ──────────────────────────
async def _pr_async(
    addr: str, path: str, file_size: int, io_size: int, conc: int, reads_per_task: int
) -> tuple[float, int]:
    max_off = max(file_size - io_size, 1)
    fs = await AsyncGoosefs.connect(Config(addr))
    sem = asyncio.Semaphore(conc)

    async def one(seed: int) -> tuple[int, int]:
        rng = XorShift(_task_seed(seed))
        local_bytes = 0
        local_mism = 0
        reader = await fs.open_file(path)
        try:
            for _ in range(reads_per_task):
                off = rng.next() % max_off
                async with sem:
                    data = await reader.read_at(off, io_size)
                if not verify_slice(data, off):
                    local_mism += 1
                local_bytes += len(data)
        finally:
            await reader.close()
        return local_bytes, local_mism

    t0 = time.perf_counter()
    results = await asyncio.gather(*(one(i) for i in range(conc)))
    secs = time.perf_counter() - t0
    await fs.close()
    total_bytes = sum(b for b, _ in results)
    total_mism = sum(m for _, m in results)
    print(
        f"    [async gather({conc})]    {conc * reads_per_task} reads, "
        f"{total_bytes / (1024 * 1024):.1f} MiB in {secs:.3f}s → "
        f"{mib_per_s(total_bytes, secs):.0f} MiB/s "
        f"{'✅' if total_mism == 0 else f'❌ {total_mism} mismatch'}"
    )
    return mib_per_s(total_bytes, secs), total_mism


def main() -> int:
    addr = _env("GFS_ADDR", "127.0.0.1:9200")
    size_mb = _env("GFS_SIZE_MB", 128)
    io_kb = _env("GFS_IO_KB", 1024)
    conc = _env("GFS_CONC", 16)
    reads_per_task = _env("GFS_READS", 8)
    mode = _env("GFS_MODE", "both")  # sync | async | both

    file_size = size_mb * 1024 * 1024
    io_size = io_kb * 1024
    path = test_path()

    print("B1 PR concurrency benchmark (Python)")
    print("====================================")
    print(f"  master      = {addr}")
    print(f"  file size   = {size_mb} MiB")
    print(f"  io size     = {io_kb} KiB")
    print(f"  PR readers  = {conc} (x{reads_per_task} reads each)")
    print(f"  mode        = {mode}")
    print("  NOTE: published wheel runs worker/master pool = 1; compare the")
    print("        Rust example with GFS_POOL=1 GFS_WPOOL=1 for fairness.\n")

    fs = Goosefs(Config(addr))
    ensure_file(fs, path, file_size)

    print("\n[PR] random read")
    rc = 0
    if mode in ("sync", "both"):
        _, m = pr_sync(fs, path, file_size, io_size, conc, reads_per_task)
        rc |= 1 if m else 0
    fs.close()

    if mode in ("async", "both"):
        _, m = asyncio.run(_pr_async(addr, path, file_size, io_size, conc, reads_per_task))
        rc |= 1 if m else 0

    print("\n====================================")
    print(
        "✅ PR benchmark complete — reads verified."
        if rc == 0
        else "❌ PR benchmark FAILED — byte mismatch detected."
    )
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
