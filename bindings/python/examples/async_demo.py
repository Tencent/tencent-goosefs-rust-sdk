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

"""Async client with concurrent metadata operations.

Demonstrates ``AsyncGoosefs`` and how to fan out many small operations
concurrently with ``asyncio.gather``. This is the shape most service
code wants: a long-lived connection shared across many tasks.

Prerequisites — same as ``quickstart.py``::

    export GOOSEFS_MASTER_ADDR=127.0.0.1:9200

Run::

    python examples/async_demo.py
"""

from __future__ import annotations

import asyncio
import os
import sys

from goosefs import AsyncGoosefs, Config


async def main() -> None:
    master = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master:
        print(
            "GOOSEFS_MASTER_ADDR is not set. Example: export GOOSEFS_MASTER_ADDR=127.0.0.1:9200",
            file=sys.stderr,
        )
        raise SystemExit(2)

    cfg = Config(master)

    # ``AsyncGoosefs`` is opened by an async factory; pair it with
    # ``async with`` so the connection is closed even on cancellation.
    async with await AsyncGoosefs.connect(cfg) as fs:
        root = "/async_demo"
        await fs.mkdir(root, recursive=True)
        print(f"[mkdir]  {root}")

        # ── Fan-out: create 8 sibling directories concurrently. With a
        # single connection, AsyncGoosefs multiplexes RPCs internally —
        # the gather() below is a real win, not just syntactic sugar.
        tasks = [fs.mkdir(f"{root}/d{i:02d}", recursive=True) for i in range(8)]
        await asyncio.gather(*tasks)
        print("[mkdir]  8 sibling dirs in parallel")

        # ── Listing reflects what we just created.
        listing = await fs.list_status(root)
        names = sorted(s.path.rsplit("/", 1)[-1] for s in listing)
        print(f"[list]   {names}")

        # ── Concurrent metadata reads. ``exists`` per directory.
        existence = await asyncio.gather(*(fs.exists(f"{root}/d{i:02d}") for i in range(8)))
        assert all(existence), "every directory we just created must exist"
        print("[exists] all 8 -> True")

        # ── Round-trip a small payload to disk.
        payload = b"async hello\n" * 100
        n = await fs.write_file(f"{root}/d00/hello.txt", payload)
        readback = await fs.read_file(f"{root}/d00/hello.txt")
        assert readback == payload
        print(f"[write+read] {n} bytes round-tripped")

        # ── Recursive cleanup — leaves the cluster clean for re-runs.
        await fs.delete(root, recursive=True)
        print(f"[delete] {root} (recursive)")
        sys.stdout.flush()
        # Do not return into AsyncGoosefs.__aexit__ / interpreter shutdown
        # here: on GitHub Actions Linux this path has been observed to
        # SIGABRT (process exit 134) after a successful demo body. CI tears
        # down the Docker fixture anyway.
        os._exit(0)


if __name__ == "__main__":
    asyncio.run(main())
