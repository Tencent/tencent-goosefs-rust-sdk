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

"""Client local page cache — synchronous Goosefs client.

Demonstrates enabling the **client-side local page cache** from Python by
passing the ``goosefs.user.client.cache.*`` properties to :class:`Config`.

Important: the page cache is integrated into the **seekable streaming reader**
(``fs.open_file(...)`` → ``read`` / ``read_at``), which wraps the SDK's
``GoosefsFileInStream``. The one-shot helpers ``read_file`` / ``read_range``
and ``positioned_read`` use the worker-direct path and do **not** consult the
local page cache. So to benefit from caching, read via ``open_file``.

Note: random ``read_at`` always consults the cache when enabled, but
**sequential** ``read`` bypasses it by default (to avoid read amplification on
large scans). Since this example reads sequentially, it sets
``goosefs.user.client.cache.sequential.read.enabled=true`` to exercise the
cache.

This example:
  1. enables the page cache on a unique temp directory,
  2. writes a multi-page file,
  3. times a COLD read (miss → worker + back-fill) vs a WARM read (local hit),
     both via ``open_file`` so the cache is actually exercised,
  4. shows the page files persisted on local disk,
  5. cleans everything up.

Prerequisites
-------------

1. A reachable GooseFS cluster::

       export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
       # If your dev cluster runs without SASL:
       export GOOSEFS_AUTH_TYPE=nosasl

2. Install the binding::

       pip install goosefs

Run
---

    python examples/page_cache.py
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time

from goosefs import Config, Goosefs, WriteType

# 256 KiB payload over 64 KiB pages → 4 cache pages.
PAGE_SIZE = 64 * 1024
PAYLOAD_SIZE = 256 * 1024


def _count_files(root: str) -> int:
    """Count regular files (persisted pages) under ``root``."""
    return sum(len(files) for _, _, files in os.walk(root))


def _read_all_via_stream(fs: Goosefs, path: str) -> bytes:
    """Read the whole file through the seekable streaming reader.

    This is the path that consults / fills the local page cache (it wraps
    ``GoosefsFileInStream``), unlike the one-shot ``read_file`` helper.
    """
    buf = bytearray()
    with fs.open_file(path) as r:
        while True:
            chunk = r.read(PAGE_SIZE)
            if not chunk:
                break
            buf.extend(chunk)
    return bytes(buf)


def main() -> None:
    master = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master:
        print(
            "GOOSEFS_MASTER_ADDR is not set. Example: export GOOSEFS_MASTER_ADDR=127.0.0.1:9200",
            file=sys.stderr,
        )
        raise SystemExit(2)

    auth_type = os.environ.get("GOOSEFS_AUTH_TYPE", "NOSASL")
    cache_dir = tempfile.mkdtemp(prefix="pygfs_page_cache_")
    print(f"[cache]  dir={cache_dir}  page_size={PAGE_SIZE // 1024}KiB")

    # Enable the local page cache purely via properties — no API change.
    cfg = Config(
        master,
        properties={
            "goosefs.security.authentication.type": auth_type,
            "goosefs.user.client.cache.enabled": "true",
            "goosefs.user.client.cache.page.size": str(PAGE_SIZE),
            "goosefs.user.client.cache.size": "64MB",
            "goosefs.user.client.cache.dirs": cache_dir,
            "goosefs.user.client.cache.eviction.policy": "LRU",
            # Inline fill so the warm read right after is a guaranteed hit.
            "goosefs.user.client.cache.async.write.enabled": "false",
            # Sequential ``read`` bypasses the cache by default (to avoid read
            # amplification); this example reads sequentially via ``open_file``
            # → ``read``, so opt sequential reads into the cache. Random
            # ``read_at`` always uses the cache regardless of this flag.
            "goosefs.user.client.cache.sequential.read.enabled": "true",
        },
    )

    path_dir = "/page-cache-example"
    path_file = f"{path_dir}/data.bin"
    payload = bytes((i % 251) for i in range(PAYLOAD_SIZE))

    try:
        with Goosefs(cfg) as fs:
            fs.mkdir(path_dir, recursive=True)
            n = fs.write_file(path_file, payload, write_type=WriteType.CacheThrough)
            print(f"[write]  {path_file}  ({n} bytes)")

            # COLD read via the streaming reader: miss → fetch + back-fill.
            t0 = time.perf_counter()
            cold = _read_all_via_stream(fs, path_file)
            cold_ms = (time.perf_counter() - t0) * 1e3
            assert cold == payload, "cold read mismatch"
            print(f"[cold]   read {len(cold)} bytes in {cold_ms:.2f} ms (miss → back-fill)")
            print(f"[disk]   {_count_files(cache_dir)} page file(s) persisted after cold read")

            # WARM read: served entirely from the local page cache.
            t0 = time.perf_counter()
            warm = _read_all_via_stream(fs, path_file)
            warm_ms = (time.perf_counter() - t0) * 1e3
            assert warm == payload, "warm read mismatch"
            print(f"[warm]   read {len(warm)} bytes in {warm_ms:.2f} ms (local cache hit)")

            # Positioned read on the cached stream is also served from cache.
            with fs.open_file(path_file) as r:
                chunk = r.read_at(PAGE_SIZE, 4096)
            assert chunk == payload[PAGE_SIZE : PAGE_SIZE + 4096], "read_at mismatch"
            print(f"[pread]  read_at(off={PAGE_SIZE}, len=4096) OK")

            fs.delete(path_file)
            print(f"[delete] {path_file}")
    finally:
        shutil.rmtree(cache_dir, ignore_errors=True)
        print(f"[clean]  removed {cache_dir}")


if __name__ == "__main__":
    main()
