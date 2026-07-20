"""End-to-end benchmark for the GetFileStatus hot-path optimisations.

This script targets the three landed levers documented in the design doc
``docs/RUST_PYTHON_SDK_OPTIMIZATION.md`` Part II (Master metadata path):

* **§1 ArcSwap shared state** — read side becomes lock-free, so contention-free
  scaling improves with thread count. Validated by scenario B (concurrent QPS).
* **§3 Path FnMut + take** — first-attempt success skips one ``String``
  allocation per call. Validated by scenario A (single-threaded steady-state).
* **§4 Counter handle cache** — ``Lazy<HashMap<&'static str, Arc<Counter>>>``
  removes the ``RwLock<HashMap>`` lookup on every RPC. Also validated by A/B.

The script is API-compatible with v0.1.5 (the baseline tag ``8a14e9e``), so the
same script can be installed into two venvs — one with the baseline wheel and
one with the optimised wheel — and produce directly comparable numbers.

Run::

    cd bindings/python
    export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
    uv run python benchmarks/bench_get_status_hotpath.py            # defaults
    uv run python benchmarks/bench_get_status_hotpath.py \
        --paths 200 --calls 4000 --threads 1,4,16,64,256 --json out.json

Pass ``--json <file>`` from both venvs and feed the two files to the companion
``scripts/run_ab_compare.sh`` (or ``--compare a.json b.json``) to print the
side-by-side delta table.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from typing import Any

from goosefs import Config, Goosefs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _percentile(samples_ns: Sequence[int], pct: float) -> float:
    """Inclusive nearest-rank percentile, returns nanoseconds."""
    if not samples_ns:
        return 0.0
    s = sorted(samples_ns)
    # Nearest-rank: rank = ceil(pct/100 * N), 1-indexed.
    rank = max(1, int(-(-len(s) * pct // 100)))
    return float(s[min(rank - 1, len(s) - 1)])


def _stats_from_samples(samples_ns: Sequence[int]) -> dict[str, float]:
    """Build a result dict from per-call latencies (nanoseconds)."""
    if not samples_ns:
        return {
            "count": 0,
            "p50_us": 0.0,
            "p90_us": 0.0,
            "p99_us": 0.0,
            "p999_us": 0.0,
            "mean_us": 0.0,
            "min_us": 0.0,
            "max_us": 0.0,
        }
    return {
        "count": len(samples_ns),
        "p50_us": _percentile(samples_ns, 50) / 1_000.0,
        "p90_us": _percentile(samples_ns, 90) / 1_000.0,
        "p99_us": _percentile(samples_ns, 99) / 1_000.0,
        "p999_us": _percentile(samples_ns, 99.9) / 1_000.0,
        "mean_us": statistics.fmean(samples_ns) / 1_000.0,
        "min_us": float(min(samples_ns)) / 1_000.0,
        "max_us": float(max(samples_ns)) / 1_000.0,
    }


@dataclass
class ScenarioResult:
    name: str
    threads: int
    total_calls: int
    wall_seconds: float
    qps: float
    latency: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def _drive_get_status(
    fs: Goosefs,
    paths: Sequence[str],
    calls_per_thread: int,
    thread_count: int,
) -> tuple[float, list[int]]:
    """Run ``calls_per_thread`` ``get_status`` calls per worker thread, all
    against the same path pool (round-robin).

    Returns ``(wall_seconds, per_call_latencies_ns)``.
    """
    n_paths = len(paths)

    def worker(worker_id: int) -> list[int]:
        local_paths = paths
        local_get = fs.get_status
        local_perf = time.perf_counter_ns
        out: list[int] = [0] * calls_per_thread
        offset = (worker_id * 7919) % n_paths  # decorrelate threads
        for i in range(calls_per_thread):
            p = local_paths[(offset + i) % n_paths]
            t0 = local_perf()
            local_get(p)
            out[i] = local_perf() - t0
        return out

    samples: list[int] = []
    t_wall = time.perf_counter()
    if thread_count == 1:
        samples = worker(0)
    else:
        with ThreadPoolExecutor(max_workers=thread_count) as pool:
            futs = [pool.submit(worker, w) for w in range(thread_count)]
            for f in as_completed(futs):
                samples.extend(f.result())
    wall = time.perf_counter() - t_wall
    return wall, samples


def scenario_single_thread(fs: Goosefs, paths: Sequence[str], calls: int) -> ScenarioResult:
    """Scenario A: single-threaded steady state.

    Tightest signal for §3 (path take) and §4 (counter cache) — no concurrency,
    so any per-call overhead removed shows up directly in median latency.
    """
    wall, lat = _drive_get_status(fs, paths, calls_per_thread=calls, thread_count=1)
    return ScenarioResult(
        name="single_thread",
        threads=1,
        total_calls=calls,
        wall_seconds=wall,
        qps=calls / wall if wall > 0 else float("inf"),
        latency=_stats_from_samples(lat),
    )


def scenario_concurrency(
    fs: Goosefs,
    paths: Sequence[str],
    calls_per_thread: int,
    thread_count: int,
) -> ScenarioResult:
    """Scenario B: concurrent QPS at a given thread count.

    Validates §1 (ArcSwap on the read side). Higher ``thread_count`` should
    show progressively larger speedups vs the baseline because the old
    ``RwLock<Client>`` serialised all readers on every RPC.
    """
    wall, lat = _drive_get_status(
        fs, paths, calls_per_thread=calls_per_thread, thread_count=thread_count
    )
    total = calls_per_thread * thread_count
    return ScenarioResult(
        name=f"concurrent_{thread_count}t",
        threads=thread_count,
        total_calls=total,
        wall_seconds=wall,
        qps=total / wall if wall > 0 else float("inf"),
        latency=_stats_from_samples(lat),
    )


def scenario_exists(fs: Goosefs, paths: Sequence[str], calls: int) -> ScenarioResult:
    """``exists`` walks the same hot path, just with a different return type;
    useful as a sanity check that the optimisation extends to all metadata
    RPCs, not only ``get_status``."""
    n = len(paths)
    local_paths = paths
    local_exists = fs.exists
    local_perf = time.perf_counter_ns
    out: list[int] = [0] * calls
    t_wall = time.perf_counter()
    for i in range(calls):
        p = local_paths[i % n]
        t0 = local_perf()
        local_exists(p)
        out[i] = local_perf() - t0
    wall = time.perf_counter() - t_wall
    return ScenarioResult(
        name="single_thread_exists",
        threads=1,
        total_calls=calls,
        wall_seconds=wall,
        qps=calls / wall if wall > 0 else float("inf"),
        latency=_stats_from_samples(out),
    )


# ---------------------------------------------------------------------------
# Setup / teardown
# ---------------------------------------------------------------------------


def setup_paths(fs: Goosefs, root: str, n: int) -> list[str]:
    """Create ``n`` directories under ``root``.

    We use directories on purpose: ``write_file`` would reserve a full block
    per file (default 64 MiB) and exhaust a small dev cluster's block store.
    For ``get_status`` / ``exists`` the URI status is identical in shape.
    """
    fs.mkdir(root, recursive=True)
    paths = [f"{root}/p{i:05d}" for i in range(n)]
    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(lambda p: fs.mkdir(p, recursive=True), paths))
    return paths


def warmup(fs: Goosefs, paths: Sequence[str], rounds: int = 2) -> None:
    """Touch every path a few times before timing begins.

    This primes:
      * the SDK's connection pool / SASL handshake,
      * any Master-side path cache,
      * the Tokio runtime worker threads,
      * the PyO3 ``GILPool`` per-thread state once the threaded scenarios start.
    """
    for _ in range(rounds):
        for p in paths:
            fs.get_status(p)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _print_scenario(r: ScenarioResult) -> None:
    lat = r.latency
    print(
        f"  {r.name:<22} threads={r.threads:<3} calls={r.total_calls:<8} "
        f"wall={r.wall_seconds * 1000:9.1f} ms  "
        f"qps={r.qps:10.0f}  "
        f"p50={lat['p50_us']:7.1f}us p99={lat['p99_us']:8.1f}us "
        f"p999={lat['p999_us']:9.1f}us"
    )


def _emit_json(out_path: str, payload: dict[str, Any]) -> None:
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    print(f"  wrote JSON results to {out_path}")


# ---------------------------------------------------------------------------
# A/B comparison (post-hoc)
# ---------------------------------------------------------------------------


def _load_results(path: str) -> dict[str, ScenarioResult]:
    with open(path, encoding="utf-8") as fh:
        payload = json.load(fh)
    out: dict[str, ScenarioResult] = {}
    for r in payload["scenarios"]:
        out[r["name"]] = ScenarioResult(**r)
    return out


def compare_runs(baseline_path: str, candidate_path: str) -> None:
    base = _load_results(baseline_path)
    cand = _load_results(candidate_path)
    print("\n=== A/B comparison ===")
    print(f"  baseline  = {baseline_path}")
    print(f"  candidate = {candidate_path}")
    header = (
        f"  {'scenario':<22} {'qps_base':>10} {'qps_opt':>10} {'qps_x':>7}  "
        f"{'p50_base':>9} {'p50_opt':>9} {'p50_d%':>7}  "
        f"{'p99_base':>9} {'p99_opt':>9} {'p99_d%':>7}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))
    for name in sorted(set(base) & set(cand)):
        b = base[name]
        c = cand[name]
        qps_x = c.qps / b.qps if b.qps else float("inf")
        p50_b = b.latency["p50_us"]
        p50_c = c.latency["p50_us"]
        p50_d = (p50_c - p50_b) / p50_b * 100 if p50_b else 0.0
        p99_b = b.latency["p99_us"]
        p99_c = c.latency["p99_us"]
        p99_d = (p99_c - p99_b) / p99_b * 100 if p99_b else 0.0
        print(
            f"  {name:<22} {b.qps:>10.0f} {c.qps:>10.0f} {qps_x:>6.2f}x  "
            f"{p50_b:>8.1f}u {p50_c:>8.1f}u {p50_d:>+6.1f}%  "
            f"{p99_b:>8.1f}u {p99_c:>8.1f}u {p99_d:>+6.1f}%"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_threads(spec: str) -> list[int]:
    out = [int(x.strip()) for x in spec.split(",") if x.strip()]
    if any(t < 1 for t in out):
        raise argparse.ArgumentTypeError("thread counts must be >= 1")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", type=int, default=200, help="size of the path pool")
    parser.add_argument(
        "--calls",
        type=int,
        default=4000,
        help="single-threaded call count (also calls/thread for concurrent scenarios)",
    )
    parser.add_argument(
        "--threads",
        type=_parse_threads,
        default=[1, 4, 16, 64, 256],
        help="comma-separated thread counts for the concurrent scenario",
    )
    parser.add_argument(
        "--json",
        type=str,
        default=None,
        help="write the full result set to this JSON file (for A/B compare)",
    )
    parser.add_argument(
        "--label",
        type=str,
        default=None,
        help="freeform label baked into the JSON output (e.g. 'baseline-8a14e9e')",
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("BASELINE_JSON", "CANDIDATE_JSON"),
        default=None,
        help="skip the bench and print an A/B comparison from two JSON files",
    )
    parser.add_argument("--skip-exists", action="store_true", help="skip the exists() probe")
    args = parser.parse_args()

    # Compare-only mode does not touch the cluster.
    if args.compare is not None:
        compare_runs(*args.compare)
        return

    master = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master:
        print("GOOSEFS_MASTER_ADDR is not set (e.g. 127.0.0.1:9200)", file=sys.stderr)
        raise SystemExit(2)

    cfg = Config(master)
    root = f"/bench-getstatus/{uuid.uuid4().hex[:8]}"
    label = args.label or "unlabelled"
    print(f"label    = {label}")
    print(f"master   = {master}")
    print(f"root     = {root}")
    print(f"paths    = {args.paths}")
    print(f"calls    = {args.calls}")
    print(f"threads  = {args.threads}")

    fs = Goosefs(cfg)
    results: list[ScenarioResult] = []
    try:
        t0 = time.perf_counter()
        paths = setup_paths(fs, root, args.paths)
        print(f"setup    = created {len(paths)} dirs in {time.perf_counter() - t0:.2f}s")

        print("warmup   = priming connections + caches ...")
        warmup(fs, paths, rounds=2)

        print("\n[A] single-threaded steady-state get_status")
        r = scenario_single_thread(fs, paths, args.calls)
        _print_scenario(r)
        results.append(r)

        if not args.skip_exists:
            print("\n[A'] single-threaded steady-state exists")
            r = scenario_exists(fs, paths, args.calls)
            _print_scenario(r)
            results.append(r)

        print("\n[B] concurrent get_status sweep (calls/thread = --calls)")
        for t in args.threads:
            r = scenario_concurrency(fs, paths, args.calls, t)
            _print_scenario(r)
            results.append(r)
    finally:
        try:
            fs.delete(root, recursive=True)
            print(f"\ncleanup  = removed {root}")
        except Exception as exc:  # noqa: BLE001
            print(f"cleanup failed: {exc}", file=sys.stderr)
        fs.close()

    if args.json:
        _emit_json(
            args.json,
            {
                "label": label,
                "master": master,
                "paths": args.paths,
                "calls": args.calls,
                "thread_sweep": args.threads,
                "scenarios": [asdict(r) for r in results],
            },
        )


if __name__ == "__main__":
    main()
