"""Streaming reader & writer — chunked I/O without buffering whole files.

``read_file`` / ``write_file`` are convenient one-shots, but they materialise
the entire payload in Python memory. For files larger than a few MiB you
want the streaming API: ``open_file`` / ``create_file`` return file-like
objects that you read / write in chunks of your chosen size.

This example writes a 4 MiB blob in 64 KiB chunks, then exercises the three
read patterns:

* sequential ``read(n)`` from the start
* ``seek`` + ``read`` from the middle
* random ``read_at(offset, length)`` without moving the cursor

Prerequisites — same as ``quickstart.py``::

    export GOOSEFS_MASTER_ADDR=127.0.0.1:9200

Run::

    python examples/streaming.py
"""

from __future__ import annotations

import os
import sys

from goosefs import Config, Goosefs, WriteType

CHUNK = 64 * 1024  # 64 KiB
TOTAL = 4 * 1024 * 1024  # 4 MiB


def _generate_payload(size: int) -> bytes:
    """A deterministic, easy-to-verify payload: repeating 256-byte block."""
    block = bytes(range(256))
    full, tail = divmod(size, 256)
    return block * full + block[:tail]


def main() -> None:
    master = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master:
        print(
            "GOOSEFS_MASTER_ADDR is not set. Example: export GOOSEFS_MASTER_ADDR=127.0.0.1:9200",
            file=sys.stderr,
        )
        raise SystemExit(2)

    cfg = Config(master)

    with Goosefs(cfg) as fs:
        path_dir = "/streaming_demo"
        path_file = f"{path_dir}/blob.bin"

        fs.mkdir(path_dir, recursive=True)
        # Make the example re-runnable.
        if fs.exists(path_file):
            fs.delete(path_file)

        payload = _generate_payload(TOTAL)
        assert len(payload) == TOTAL

        # ─── Streaming write ──────────────────────────────────────────────
        # CacheThrough writes both to the worker tier and to UFS, so the
        # data survives a worker eviction. Pick MustCache for ephemeral
        # tmp-style files, Through to bypass the cache entirely.
        with fs.create_file(path_file, write_type=WriteType.CacheThrough) as w:
            written = 0
            while written < TOTAL:
                end = min(written + CHUNK, TOTAL)
                n = w.write(payload[written:end])
                written += n
        print(f"[write]  {path_file}  {written} bytes in {TOTAL // CHUNK}-ish chunks")

        status = fs.get_status(path_file)
        assert status.length == TOTAL
        print(f"[stat]   length={status.length}  completed={status.is_completed()}")

        # ─── Pattern 1: sequential streaming read ─────────────────────────
        with fs.open_file(path_file) as r:
            buf = bytearray()
            while True:
                chunk = r.read(CHUNK)
                if not chunk:
                    break
                buf.extend(chunk)
        assert bytes(buf) == payload
        print(f"[read seq]  {len(buf)} bytes verified")

        # ─── Pattern 2: seek + read from the middle ───────────────────────
        midpoint = TOTAL // 2
        with fs.open_file(path_file) as r:
            new_pos = r.seek(midpoint)
            assert new_pos == midpoint
            assert r.tell() == midpoint
            tail = r.read()  # read to EOF
        assert tail == payload[midpoint:]
        print(f"[read mid]  seek({midpoint}) + read() -> {len(tail)} bytes")

        # ─── Pattern 3: random read_at without disturbing the cursor ──────
        with fs.open_file(path_file) as r:
            r.seek(0)  # cursor at the very beginning
            slice_a = r.read_at(1024, 256)  # arbitrary middle slice
            slice_b = r.read_at(TOTAL - 512, 512)  # last 512 bytes
            # Cursor is still at the start because read_at is positional.
            assert r.tell() == 0
        assert slice_a == payload[1024:1280]
        assert slice_b == payload[TOTAL - 512 :]
        print("[read_at]   2 random slices verified, cursor unchanged")

        # Cleanup so the example is re-runnable.
        fs.delete(path_file)
        fs.delete(path_dir)
        print(f"[delete] {path_dir}")


if __name__ == "__main__":
    main()
