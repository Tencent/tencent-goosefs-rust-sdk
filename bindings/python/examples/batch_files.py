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

"""Batch file-lifecycle APIs.

Demonstrates the five batch mutation / resource-holding APIs on
``AsyncGoosefs``:

* ``batch_create_file`` — create N empty files with bounded concurrency.
* ``batch_create_dir``  — create N directories with bounded concurrency.
* ``batch_open_file``   — open N read streams with bounded concurrency
  (resource-holding; on partial failure all opened streams are dropped
  to avoid connection leaks).
* ``batch_rename``      — rename N ``(src, dst)`` pairs (flat list:
  ``[src_0, dst_0, src_1, dst_1, ...]``).
* ``batch_delete``      — delete N paths with bounded concurrency.

All five fan out RPCs with at most ``BATCH_CONCURRENCY_LIMIT`` in flight
and fail the whole batch on the first error. Use the per-path variants
(``create_file`` / ``mkdir`` / ``rename`` / ``delete`` / ``open_file``)
when you need per-path error isolation.

Prerequisites
-------------

1. A reachable GooseFS cluster::

       export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
       export GOOSEFS_AUTH_TYPE=simple

2. Install the binding::

       pip install goosefs

Run
---

    python examples/batch_files.py
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
        scratch = "/batch-files-example"
        await fs.delete_with_options(
            scratch,
            recursive=True,
            unchecked=True,
        )
        await fs.mkdir(scratch)
        print(f"Created scratch dir: {scratch}")

        # ----------------------------------------------------------------
        # 1. batch_create_dir — N mkdirs in parallel.
        # ----------------------------------------------------------------
        dirs = [f"{scratch}/dir{i}" for i in range(4)]
        await fs.batch_create_dir(dirs)
        exists = await fs.batch_exists(dirs)
        print(f"\nbatch_create_dir({len(dirs)} dirs):")
        print(f"  all created = {all(exists)}")
        assert all(exists), "expected all dirs to exist"

        # ----------------------------------------------------------------
        # 2. batch_create_file — N empty files in parallel.
        #    Returns list[int] of bytes written per file (0 for empty).
        # ----------------------------------------------------------------
        files = [f"{dirs[i]}/file-{i}.bin" for i in range(len(dirs))]
        written = await fs.batch_create_file(files)
        print(f"\nbatch_create_file({len(files)} files):")
        print(f"  bytes written = {written}")
        assert all(n == 0 for n in written), "empty files should report 0 bytes"

        # ----------------------------------------------------------------
        # 3. Write some real content so batch_open_file has data to read.
        # ----------------------------------------------------------------
        payloads = [f"payload-{i}".encode() for i in range(len(files))]
        for path, data in zip(files, payloads):
            await fs.write_file(path, data)
        print(f"\nWrote payloads to {len(files)} files")

        # ----------------------------------------------------------------
        # 4. batch_open_file — N read streams in parallel.
        #    On partial failure all opened streams are dropped to avoid
        #    worker-connection leaks.
        # ----------------------------------------------------------------
        readers = await fs.batch_open_file(files)
        print(f"\nbatch_open_file({len(files)} files):")
        print(f"  opened = {len(readers)} streams")
        contents = []
        for r, expected in zip(readers, payloads):
            data = await r.read()
            contents.append(data)
            assert data == expected, f"read mismatch: {data!r} != {expected!r}"
        print(f"  all contents verified = {contents == payloads}")

        # ----------------------------------------------------------------
        # 5. batch_rename — N (src, dst) pairs as a flat list.
        #    [src_0, dst_0, src_1, dst_1, ...]. Length must be even.
        # ----------------------------------------------------------------
        renamed = [f"{dirs[i]}/renamed-{i}.bin" for i in range(len(files))]
        # Build the flat [src_0, dst_0, src_1, dst_1, ...] list.
        pairs: list[str] = []
        for src, dst in zip(files, renamed):
            pairs.extend([src, dst])
        await fs.batch_rename(pairs)
        # Old paths gone, new paths present.
        old_exists = await fs.batch_exists(files)
        new_exists = await fs.batch_exists(renamed)
        print(f"\nbatch_rename({len(files)} pairs):")
        print(f"  old gone   = {not any(old_exists)}")
        print(f"  new exists = {all(new_exists)}")
        assert not any(old_exists), "old paths should be gone after rename"
        assert all(new_exists), "new paths should exist after rename"

        # ----------------------------------------------------------------
        # 6. batch_delete — N deletes in parallel.
        #    recursive=True lets us wipe the whole scratch tree in one call.
        # ----------------------------------------------------------------
        to_delete = dirs + renamed
        await fs.batch_delete(to_delete, recursive=True)
        after = await fs.batch_exists(to_delete)
        print(f"\nbatch_delete({len(to_delete)} paths):")
        print(f"  all gone = {not any(after)}")
        assert not any(after), "all paths should be gone after delete"

        # ----------------------------------------------------------------
        # Final cleanup of the scratch root.
        # ----------------------------------------------------------------
        await fs.delete(scratch, recursive=True)
        print(f"\nDeleted {scratch}")


if __name__ == "__main__":
    asyncio.run(main())
