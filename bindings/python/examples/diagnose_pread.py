"""Diagnostic: trace exactly what path Python `read_at` takes.

Usage:
    cd bindings/python
    GOOSEFS_MASTER_ADDR=127.0.0.1:9200 RUST_LOG=goosefs_sdk=debug \
        .venv/bin/python examples/diagnose_pread.py

This is intentionally NOT collected by pytest (no `test_` prefix).
Moved from the package root to `examples/` so it does not pollute the
wheel package (see code review Warning #3).
"""

from __future__ import annotations

import os
import socket
import sys
import time
import uuid

import goosefs
from goosefs import Config, Goosefs, WriteType


def banner(title: str) -> None:
    print(f"\n========== {title} ==========", flush=True)


def main() -> int:
    banner("0. Environment")
    print(f"  hostname    = {socket.gethostname()}", flush=True)
    print(f"  fqdn        = {socket.getfqdn()}", flush=True)
    print(f"  master      = {os.environ.get('GOOSEFS_MASTER_ADDR', '<unset>')}", flush=True)
    print(f"  python      = {sys.version.split()[0]}", flush=True)
    print(f"  goosefs ver = {getattr(goosefs, '__version__', '?')}", flush=True)

    # Install Rust tracing bridge — RUST_LOG alone is not enough because
    # PyO3 extensions need an explicit subscriber install.
    goosefs.enable_tracing(level="debug")

    master_addr = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master_addr:
        print("GOOSEFS_MASTER_ADDR is not set. Example: export GOOSEFS_MASTER_ADDR=127.0.0.1:9200")
        sys.exit(1)
    auth_type = os.environ.get("GOOSEFS_AUTH_TYPE", "NOSASL")

    cfg = Config(
        master_addr,
        properties={"goosefs.security.authentication.type": auth_type},
    )
    fs = Goosefs(cfg)

    base = "/tmp/pygoosefs-diag"
    try:
        fs.mkdir(base, recursive=True)
    except Exception:
        pass

    name = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"
    path = f"{base}/{name}.bin"

    # 1 MiB payload — smaller than default 64 MiB block_size, so the
    # whole file lives in a single block. That keeps the read_at vs
    # read comparison clean (no cross-block plumbing).
    payload = bytes(range(256)) * 4 * 1024  # 1 MiB
    print(f"\n  test path   = {path}", flush=True)
    print(f"  payload     = {len(payload)} bytes ({len(payload) / 1024:.0f} KiB)", flush=True)

    try:
        banner("1. write_file (default WriteType — typically CACHE_THROUGH)")
        fs.write_file(path, payload)
        print("  write OK", flush=True)

        banner("2. get_status")
        status = fs.get_status(path)
        for attr in [
            "length",
            "block_size_bytes",
            "block_ids",
            "ufs_path",
            "in_goosefs_percentage",
            "persistence_state",
            "completed",
            "cacheable",
            "mount_id",
        ]:
            if hasattr(status, attr):
                val = getattr(status, attr)
                if isinstance(val, list) and len(val) > 8:
                    val = f"{val[:4]}...({len(val)} items)"
                print(f"    status.{attr:<22} = {val}", flush=True)

        banner("3. open_file → read_at(0, 4096)  [look for `positioned_read`]")
        with fs.open_file(path) as r:
            chunk = r.read_at(0, 4096)
            print(f"  read_at(0, 4096)    -> {len(chunk)} bytes", flush=True)

            banner("4. read_at(512*1024, 4096)  [mid-file]")
            chunk = r.read_at(512 * 1024, 4096)
            print(f"  read_at(512K, 4096) -> {len(chunk)} bytes", flush=True)

            banner("5. sequential read(4096) [look for `block_in_stream`]")
            head = r.read(4096)
            print(f"  read(4096)          -> {len(head)} bytes", flush=True)

        banner("6. write_file (WriteType.Through) — UFS only, no cache")
        path_through = path + ".through"
        try:
            fs.write_file(path_through, payload, write_type=WriteType.Through)
            print(f"  wrote {path_through} (THROUGH)", flush=True)

            banner("7. read_at on THROUGH file [should always go via UFS]")
            with fs.open_file(path_through) as r:
                chunk = r.read_at(0, 4096)
                print(f"  read_at(0, 4096)    -> {len(chunk)} bytes", flush=True)
        except Exception as exc:
            print(f"  THROUGH path failed: {exc!r}", flush=True)

    finally:
        for p in (path, path + ".through"):
            try:
                fs.delete(p, unchecked=True)
            except Exception:
                pass
        fs.close()

    banner("DONE — review the goosefs_sdk debug lines above")
    return 0


if __name__ == "__main__":
    sys.exit(main())
