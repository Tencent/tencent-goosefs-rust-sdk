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

"""Batch metadata-status APIs.

Demonstrates the four batch status APIs on ``AsyncGoosefs``:

* ``list_status_grouped``  — single path, lazy materialisation (returns
  one ``URIStatusList`` Python object instead of N ``URIStatus`` objects
  inside the GIL window).
* ``batch_list_status_grouped`` — same idea across multiple directories,
  returns a ``list[URIStatusList]``.
* ``batch_get_status``     — fan out ``get_status`` over N paths with
  bounded concurrency, returns ``list[URIStatus]`` in input order.
* ``batch_exists``         — fan out ``exists`` over N paths, returns
  ``list[bool]`` in input order.

The lazy ``*_grouped`` APIs are useful under high-concurrency
Pandas/Dask workloads where the GIL cost of materialising N ``URIStatus``
objects dominates. Prefer the eager variants (``list_status`` /
``batch_list_status``) when you need a plain ``list[URIStatus]`` for
slicing or library interop.

Prerequisites
-------------

1. A reachable GooseFS cluster. Set the master address via env var::

       export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
       export GOOSEFS_AUTH_TYPE=simple

2. Install the binding::

       pip install goosefs

Run
---

    python examples/batch_status.py
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
            "GOOSEFS_MASTER_ADDR is not set. "
            "Example: export GOOSEFS_MASTER_ADDR=127.0.0.1:9200",
            file=sys.stderr,
        )
        raise SystemExit(2)

    cfg = Config(master)

    async with await AsyncGoosefs.connect(cfg) as fs:
        scratch = "/batch-status-example"
        if await fs.exists(scratch):
            await fs.delete(scratch, recursive=True)
        await fs.mkdir(scratch)
        print(f"Created scratch dir: {scratch}")

        # ----------------------------------------------------------------
        # 1. Create a small tree so every status API has something to list.
        # ----------------------------------------------------------------
        dirs = [f"{scratch}/d{i}" for i in range(3)]
        await fs.batch_create_dir(dirs)
        for d in dirs:
            await fs.write_file(f"{d}/file-0.txt", b"hello")
        await fs.write_file(f"{scratch}/root.txt", b"root-level")
        print(f"Created {len(dirs)} subdirs + 1 root file")

        # ----------------------------------------------------------------
        # 2. list_status_grouped — single path, lazy.
        #
        #    `len()` is O(1) and creates zero Python URIStatus objects;
        #    `entries[i]` materialises one on demand.
        # ----------------------------------------------------------------
        grouped = await fs.list_status_grouped(scratch)
        print(f"\nlist_status_grouped({scratch!r}):")
        print(f"  type      = {type(grouped).__name__}")
        print(f"  len       = {len(grouped)}")
        print(f"  first name = {grouped[0].name if len(grouped) > 0 else None}")

        # Negative indexing and iteration work like a list.
        last = grouped[-1]
        print(f"  last name  = {last.name}")

        # ----------------------------------------------------------------
        # 3. batch_list_status_grouped — multiple dirs, lazy per dir.
        #
        #    Returns list[URIStatusList] in input order. Each entry is a
        #    lazy handle that only creates Python objects when indexed.
        # ----------------------------------------------------------------
        groups = await fs.batch_list_status_grouped(dirs, recursive=False)
        print(f"\nbatch_list_status_grouped({dirs!r}):")
        print(f"  groups       = {len(groups)}")
        for i, g in enumerate(groups):
            print(f"  group[{i}] len = {len(g)}, first = {g[0].name if len(g) > 0 else None}")

        # ----------------------------------------------------------------
        # 4. batch_get_status — eager list[URIStatus], input order preserved.
        # ----------------------------------------------------------------
        statuses = await fs.batch_get_status([scratch, *dirs])
        print(f"\nbatch_get_status([{scratch}, ...]):")
        print(f"  count = {len(statuses)}")
        print(f"  paths = {[s.path for s in statuses]}")
        assert all(s.is_folder() for s in statuses), "expected all directories"

        # ----------------------------------------------------------------
        # 5. batch_exists — list[bool] in input order.
        # ----------------------------------------------------------------
        probe = [scratch, f"{scratch}/missing", f"{dirs[0]}/file-0.txt"]
        exists = await fs.batch_exists(probe)
        print(f"\nbatch_exists({probe!r}):")
        print(f"  result = {exists}")
        assert exists == [True, False, True]

        # ----------------------------------------------------------------
        # Cleanup.
        # ----------------------------------------------------------------
        await fs.delete(scratch, recursive=True)
        print(f"\nDeleted {scratch}")


if __name__ == "__main__":
    asyncio.run(main())
