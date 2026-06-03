"""Benchmark for the Phase 1/2 Python SDK performance work.

This script quantifies the three levers landed on ``feature/python-sdk-perf-opt``
against a *real* GooseFS cluster:

* **Phase 2.1 — batch metadata API.** Compares resolving ``N`` paths via a
  per-op Python loop, a ``ThreadPoolExecutor`` of per-op calls, and the single
  ``batch_get_status`` / ``batch_exists`` call. The batch call collapses ``N``
  PyO3 boundary crossings into one and drives the ``N`` RPCs on the Tokio
  runtime with bounded concurrency (``stream::buffered`` capped at
  ``BATCH_CONCURRENCY_LIMIT``) to avoid unbounded fan-out against the master,
  so it should win once ``N`` is non-trivial.
* **Phase 2.2 — custom Tokio runtime.** The threaded and ``asyncio.gather``
  scenarios exercise concurrent in-flight IO, which is where the bumped
  ``worker_threads`` / ``max_blocking_threads`` help.
* **Phase 1 — read-path copy elimination.** A ``read_file`` throughput probe at
  a few payload sizes. There is no A/B here (the old code path is gone), so this
  is reported as an absolute MB/s baseline for regression tracking.

Run::

    cd bindings/python
    export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
    uv run python benchmarks/bench_perf_opt.py            # defaults
    uv run python benchmarks/bench_perf_opt.py --paths 500 --iters 7 --threads 16

All scratch state is created under ``/bench-pysdk/<uuid>`` and removed on exit.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor

from goosefs import AsyncGoosefs, Config, Goosefs

# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


def _timeit(fn: Callable[[], object], iters: int, warmup: int = 1) -> list[float]:
    """Run ``fn`` ``iters`` times (after ``warmup`` discarded runs); return the
    per-iteration wall times in seconds."""
    for _ in range(warmup):
        fn()
    samples: list[float] = []
    for _ in range(iters):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return samples


def _fmt_row(label: str, samples: list[float], n_ops: int) -> tuple[str, float]:
    """Format a result row and return (text, median_seconds)."""
    med = statistics.median(samples)
    p_lo = min(samples)
    ops_per_s = n_ops / med if med > 0 else float("inf")
    return (
        f"  {label:<34} median={med * 1e3:8.2f} ms  "
        f"best={p_lo * 1e3:8.2f} ms  {ops_per_s:10.0f} ops/s",
        med,
    )


def _speedup_line(baseline_med: float, candidate_med: float, name: str) -> str:
    if candidate_med <= 0:
        return f"  -> {name}: n/a"
    return f"  -> {name} speedup vs sequential: {baseline_med / candidate_med:5.2f}x"


# ---------------------------------------------------------------------------
# Setup / teardown
# ---------------------------------------------------------------------------


def setup_paths(fs: Goosefs, root: str, n: int) -> list[str]:
    """Create ``n`` paths under ``root`` and return them.

    We use directories (pure metadata) rather than files: the metadata
    benchmarks only need ``get_status`` / ``exists`` targets, and every
    ``write_file`` — even of one byte — reserves a full block (64 MiB by
    default) on the worker, which exhausts a small dev cluster's block store.
    """
    fs.mkdir(root, recursive=True)
    paths = [f"{root}/f{i:05d}" for i in range(n)]
    # Create concurrently via a thread pool to keep setup fast.
    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(lambda p: fs.mkdir(p, recursive=True), paths))
    return paths


# ---------------------------------------------------------------------------
# Sync metadata benchmarks (Phase 2.1 + 2.2)
# ---------------------------------------------------------------------------


def bench_sync_metadata(
    fs: Goosefs, paths: Sequence[str], iters: int, threads: int
) -> None:
    n = len(paths)
    plist = list(paths)
    print(f"\n[sync] get_status over {n} paths  (iters={iters}, threads={threads})")

    def seq_get() -> None:
        for p in plist:
            fs.get_status(p)

    def threaded_get() -> None:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            list(pool.map(fs.get_status, plist))

    def batch_get() -> None:
        fs.batch_get_status(plist)

    seq = _timeit(seq_get, iters)
    thr = _timeit(threaded_get, iters)
    bat = _timeit(batch_get, iters)

    seq_txt, seq_med = _fmt_row("sequential loop", seq, n)
    thr_txt, thr_med = _fmt_row(f"ThreadPool({threads})", thr, n)
    bat_txt, bat_med = _fmt_row("batch_get_status", bat, n)
    print(seq_txt)
    print(thr_txt)
    print(bat_txt)
    print(_speedup_line(seq_med, thr_med, f"ThreadPool({threads})"))
    print(_speedup_line(seq_med, bat_med, "batch_get_status"))

    print(f"\n[sync] exists over {n} paths")

    def seq_exists() -> None:
        for p in plist:
            fs.exists(p)

    def batch_exists() -> None:
        fs.batch_exists(plist)

    se = _timeit(seq_exists, iters)
    be = _timeit(batch_exists, iters)
    se_txt, se_med = _fmt_row("sequential loop", se, n)
    be_txt, be_med = _fmt_row("batch_exists", be, n)
    print(se_txt)
    print(be_txt)
    print(_speedup_line(se_med, be_med, "batch_exists"))


# ---------------------------------------------------------------------------
# Async metadata benchmarks (Phase 2.1 + 2.2)
# ---------------------------------------------------------------------------


async def bench_async_metadata(
    afs: AsyncGoosefs, paths: Sequence[str], iters: int
) -> None:
    n = len(paths)
    plist = list(paths)
    print(f"\n[async] get_status over {n} paths  (iters={iters})")

    async def gather_get() -> None:
        await asyncio.gather(*(afs.get_status(p) for p in plist))

    async def batch_get() -> None:
        await afs.batch_get_status(plist)

    async def _atimeit(coro_fn: Callable[[], object], warmup: int = 1) -> list[float]:
        for _ in range(warmup):
            await coro_fn()
        out: list[float] = []
        for _ in range(iters):
            t0 = time.perf_counter()
            await coro_fn()
            out.append(time.perf_counter() - t0)
        return out

    g = await _atimeit(gather_get)
    b = await _atimeit(batch_get)
    g_txt, g_med = _fmt_row("asyncio.gather(get_status)", g, n)
    b_txt, b_med = _fmt_row("batch_get_status", b, n)
    print(g_txt)
    print(b_txt)
    print(_speedup_line(g_med, b_med, "batch_get_status"))


# ---------------------------------------------------------------------------
# Read-path throughput (Phase 1)
# ---------------------------------------------------------------------------


def bench_read_throughput(fs: Goosefs, root: str, iters: int) -> None:
    # Keep sizes modest and clean each blob up immediately: a small dev
    # worker reserves a full block per file, so accumulating large blobs
    # would exhaust the block store.
    sizes = [4 * 1024, 256 * 1024, 4 * 1024 * 1024, 16 * 1024 * 1024]
    print(f"\n[sync] read_file throughput  (iters={iters})")
    for size in sizes:
        path = f"{root}/blob_{size}"
        try:
            fs.write_file(path, os.urandom(size), recursive=True)

            def read_once(p: str = path, sz: int = size) -> None:
                data = fs.read_file(p)
                assert len(data) == sz

            samples = _timeit(read_once, iters)
            med = statistics.median(samples)
            mbps = (size / (1024 * 1024)) / med if med > 0 else float("inf")
            human = (
                f"{size // 1024}KiB" if size < 1024 * 1024 else f"{size // (1024 * 1024)}MiB"
            )
            print(f"  {human:>8}: median={med * 1e3:8.2f} ms  {mbps:8.1f} MiB/s")
        finally:
            try:
                fs.delete(path)
            except Exception:  # noqa: BLE001
                pass


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", type=int, default=200, help="number of metadata paths")
    parser.add_argument("--iters", type=int, default=5, help="timed iterations per case")
    parser.add_argument("--threads", type=int, default=16, help="ThreadPool size for sync concurrency")
    parser.add_argument("--skip-read", action="store_true", help="skip read-throughput probe")
    args = parser.parse_args()

    master = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master:
        print("GOOSEFS_MASTER_ADDR is not set (e.g. 127.0.0.1:9200)", file=sys.stderr)
        raise SystemExit(2)

    cfg = Config(master)
    root = f"/bench-pysdk/{uuid.uuid4().hex[:8]}"
    print(f"master   = {master}")
    print(f"root     = {root}")
    print(f"paths    = {args.paths}")

    fs = Goosefs(cfg)
    try:
        t0 = time.perf_counter()
        paths = setup_paths(fs, root, args.paths)
        print(f"setup    = created {len(paths)} files in {time.perf_counter() - t0:.2f}s")

        bench_sync_metadata(fs, paths, args.iters, args.threads)
        if not args.skip_read:
            bench_read_throughput(fs, root, args.iters)
    finally:
        try:
            fs.delete(root, recursive=True)
            print(f"\ncleanup  = removed {root}")
        except Exception as exc:  # noqa: BLE001
            print(f"cleanup failed: {exc}", file=sys.stderr)
        fs.close()

    # Async section uses its own connection / event loop.
    async def _run_async() -> None:
        afs = await AsyncGoosefs.connect(cfg)
        aroot = f"/bench-pysdk/{uuid.uuid4().hex[:8]}-async"
        await afs.mkdir(aroot, recursive=True)
        apaths = [f"{aroot}/f{i:05d}" for i in range(args.paths)]
        await asyncio.gather(*(afs.mkdir(p, recursive=True) for p in apaths))
        try:
            await bench_async_metadata(afs, apaths, args.iters)
        finally:
            try:
                await afs.delete(aroot, recursive=True)
            finally:
                await afs.close()

    asyncio.run(_run_async())


if __name__ == "__main__":
    main()
