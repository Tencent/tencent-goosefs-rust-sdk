"""SR / SW throughput benchmark (P1 / P2 verification).

Measures Python-side **sequential read** (exercises the P1 prefetch buffer,
`FileReader.read(n)`) and **sequential write** (exercises the P2
`extract_bytes_like` write path, `FileWriter.write(chunk)`) throughput at a
configurable buffer size, against a live cluster.

Pair with the Rust side for an apples-to-apples comparison:
  - SR-64K Rust: `benchmarks/partv_perf_verify.rs` `[2]` "64KiB-buf scan" line.
  - run both with the same `GFS_ADDR` / `GFS_SIZE_MB` / `GFS_IO_KB`.

Sequential read is byte-for-byte verified (consistency C1/C2). Write is not
verified here (the subsequent SR pass over the same file validates contents).

Usage::

    cd bindings/python
    # SR-64K (P1 focus): small buffer is where Python lagged Rust the most
    GFS_ADDR=127.0.0.1:9200 GFS_SIZE_MB=128 GFS_IO_KB=64 GFS_TAG=srsw \
      ../../.venv/bin/python benchmarks/bench_sr_sw.py

    # buffer sweep
    for kb in 64 256 1024; do
      GFS_IO_KB=$kb GFS_TAG=srsw ../../.venv/bin/python benchmarks/bench_sr_sw.py
    done
"""

from __future__ import annotations

import os
import time

from goosefs import Config, Goosefs

TEST_DIR = "/partv-bench"

_PATTERN = bytes(i % 251 for i in range(251))


def _env(key: str, default):
    raw = os.environ.get(key)
    return type(default)(raw) if raw not in (None, "") else default


def test_path() -> str:
    tag = os.environ.get("GFS_TAG", "")
    return f"{TEST_DIR}/srsw-{tag}.bin" if tag else f"{TEST_DIR}/srsw.bin"


def _chunk(offset: int, length: int) -> bytes:
    """Deterministic payload slice `[offset, offset+length)` (pattern repeats
    every 251 bytes)."""
    start = offset % 251
    rotated = _PATTERN[start:] + _PATTERN[:start]
    full, rem = divmod(length, 251)
    return rotated * full + rotated[:rem]


def mib_s(n: int, secs: float) -> float:
    return (n / (1024 * 1024)) / max(secs, 1e-9)


def main() -> int:
    addr = _env("GFS_ADDR", "127.0.0.1:9200")
    size_mb = _env("GFS_SIZE_MB", 128)
    io_kb = _env("GFS_IO_KB", 64)
    file_size = size_mb * 1024 * 1024
    io = io_kb * 1024
    path = test_path()

    print("SR / SW throughput benchmark (Python, P1/P2)")
    print("============================================")
    print(f"  master = {addr}  file = {size_mb} MiB  buffer = {io_kb} KiB  path = {path}\n")

    fs = Goosefs(Config(addr))
    try:
        fs.delete(path, recursive=False)
    except Exception:
        pass
    try:
        fs.mkdir(TEST_DIR, recursive=True)
    except Exception:
        pass

    # ── SW: sequential write in `io`-sized chunks (P2 write path) ────────────
    t0 = time.perf_counter()
    written = 0
    with fs.create_file(path, recursive=True) as w:
        while written < file_size:
            this = min(io, file_size - written)
            w.write(_chunk(written, this))
            written += this
    sw_secs = time.perf_counter() - t0
    print(
        f"  SW {io_kb:>5} KiB: {file_size / (1024 * 1024):.0f} MiB in {sw_secs:.3f}s "
        f"→ {mib_s(file_size, sw_secs):.0f} MiB/s"
    )

    # ── SR: sequential read in `io`-sized chunks (P1 prefetch buffer) ────────
    r = fs.open_file(path)
    t0 = time.perf_counter()
    read_total = 0
    ok = True
    while True:
        data = r.read(io)
        if not data:
            break
        if data != _chunk(read_total, len(data)):
            ok = False
        read_total += len(data)
    sr_secs = time.perf_counter() - t0
    r.close()
    print(
        f"  SR {io_kb:>5} KiB: {read_total / (1024 * 1024):.0f} MiB in {sr_secs:.3f}s "
        f"→ {mib_s(read_total, sr_secs):.0f} MiB/s "
        f"{'✅' if ok and read_total == file_size else '❌'}"
    )

    try:
        fs.delete(path, recursive=False)
    except Exception:
        pass
    fs.close()
    print("\n============================================")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
