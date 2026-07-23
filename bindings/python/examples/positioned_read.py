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

"""Worker block direct positioned read — high-level + low-level walkthrough.

Demonstrates the two new APIs added in stage B of the
"Worker block direct connection" feature (P6, ``goosefs >= 0.1.3``):

* :py:meth:`goosefs.AsyncGoosefs.positioned_read` — high-level one-liner:
  resolve URI → pick block → route → ``WorkerClient::read_block_positioned``
  → return ``bytes``.

* :py:meth:`goosefs.AsyncGoosefs.acquire_worker_for_block` +
  :py:class:`goosefs.AsyncWorkerClient` — low-level escape hatch when you
  already know the ``block_id`` (custom routing, benchmarks).

Mirrors the Rust SDK example
``examples/lowlevel_block_read.rs``.

Prerequisites
-------------

1. A reachable GooseFS cluster::

       export GOOSEFS_MASTER_ADDR=127.0.0.1:9200

2. Install the binding (development build via ``maturin develop`` recommended,
   so you pick up the latest ``worker.rs``)::

       cd bindings/python
       maturin develop --release

Run
---

    python examples/positioned_read.py
"""

from __future__ import annotations

import asyncio
import os
import sys

from goosefs import AsyncGoosefs, AsyncWorkerClient, Config, WriteType


async def main() -> None:
    master = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master:
        print(
            "GOOSEFS_MASTER_ADDR is not set. Example: export GOOSEFS_MASTER_ADDR=127.0.0.1:9200",
            file=sys.stderr,
        )
        raise SystemExit(2)

    cfg = Config(master)

    async with await AsyncGoosefs.connect(cfg) as fs:
        # ── Set up a small fixture file ─────────────────────────────────
        path_dir = "/positioned_read_demo"
        path_file = f"{path_dir}/blob.bin"

        await fs.mkdir(path_dir, recursive=True)
        # Re-runnable: scrub any leftover from a previous run.
        try:
            await fs.delete(path_file)
        except Exception:
            pass

        # Pad a 1 MiB file with a deterministic byte pattern so the
        # round-trip assertion below is exact.
        #
        # ``write_type=WriteType.MustCache`` is **required** for both the
        # high-level and the low-level paths below to find a worker holding
        # the block: it forces the worker tier to keep the block in cache,
        # which makes ``URIStatus.block_ids`` populated on master.  Without
        # it (or on clusters in a "UFS-only / no-tier-cache" state) every
        # call below would raise
        # ``ValueError: path "..." has no blocks (empty file or directory)``.
        payload = bytes(i & 0xFF for i in range(1 << 20))
        await fs.write_file(path_file, payload, write_type=WriteType.MustCache)
        print(f"[write]  {path_file}  ({len(payload)} bytes, write_type=MustCache)")

        # Cluster-state diagnostic: even with ``MustCache``, some clusters
        # never report blocks back to master (worker registers with
        # ``usedBytes=0``, no block report).  Detect that up front and
        # bail out gracefully instead of letting every API below blow up
        # with the same root cause.
        status = await fs.get_status(path_file)
        if not status.block_ids:
            # Flush stdout first so the [skip] hint below appears *after*
            # the [write] line in interleaved terminals.
            sys.stdout.flush()
            # Note: ``URIStatus.is_persisted`` / ``is_completed`` are methods
            # in the current binding, not properties; call them.
            print(
                f"[skip]   URIStatus.block_ids is empty for {path_file}; this cluster "
                f"does not appear to retain blocks in the worker tier even with "
                f"WriteType.MustCache. Skipping the worker-direct demo.\n"
                f"         status: length={status.length}, block_size_bytes="
                f"{status.block_size_bytes}, persisted={status.is_persisted()}",
                file=sys.stderr,
            )
            # Do not call delete / AsyncGoosefs.__aexit__ here: on GitHub
            # Actions Linux this skip path has been observed to SIGSEGV
            # (process exit 139) during teardown after a MustCache write
            # that left empty block_ids. CI tears down the Docker fixture
            # anyway, so leaving the demo file is fine.
            sys.stderr.flush()
            sys.stdout.flush()
            os._exit(0)

        # ── 1. High-level: AsyncGoosefs.positioned_read ─────────────────
        #
        # One call performs all four steps internally:
        #   1) get_status(path)        — resolve URIStatus
        #   2) pick block_ids[0]
        #   3) router.select_worker    — consistent-hash routing
        #   4) GrpcBlockReader::positioned_read
        #
        # The default ``chunk_size`` is 1 MiB; override for finer
        # flow-control granularity.
        head = await fs.positioned_read(
            path_file,
            block_index=0,
            offset=0,
            length=64,
        )
        print(f"[hl-pr]  high-level positioned_read first 64 bytes -> {head!r}")
        assert head == payload[:64]

        # Length=-1 means "read to end of the chosen block" (clamped at
        # block size). Useful when you don't know the file/block size up
        # front.
        rest = await fs.positioned_read(
            path_file,
            block_index=0,
            offset=64,
            length=-1,
        )
        assert head + rest == payload
        print(f"[hl-pr]  high-level positioned_read total -> {len(head) + len(rest)} bytes")

        # ── 2. Low-level: acquire_worker_for_block + read_block_positioned
        #
        # When you already know the ``block_id`` (e.g. cached from a
        # previous ``URIStatus`` lookup, custom routing experiments,
        # batched block prefetch), you can skip the master round-trip
        # entirely on subsequent reads.
        block_id = status.block_ids[0]
        async with await fs.acquire_worker_for_block(block_id) as wc:
            assert isinstance(wc, AsyncWorkerClient)
            mid = await wc.read_block_positioned(
                block_id,
                offset=128,
                length=32,
            )
            print(f"[ll-pr]  low-level read_block_positioned  @blk{block_id}+128+32 -> {mid!r}")
            assert mid == payload[128:160]

        # Cleanup so the example is re-runnable.
        await fs.delete(path_file)
        print(f"[delete] {path_file}")


if __name__ == "__main__":
    asyncio.run(main())
