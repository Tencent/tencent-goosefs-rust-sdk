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

"""Integration tests for writes that sit on, or cross, a GooseFS block boundary.

Why this file exists separately from :file:`test_streaming_async.py`
-------------------------------------------------------------------

The streaming suites write a payload smaller than one block, so they exercise
only the final-block close. That is the half of the picture that behaves the
same everywhere. The other half does not: whether a write succeeds at all can
depend on which block store the worker runs.

A cache write is sliced into blocks, and switching blocks mid-file flushes the
one being left behind — the SDK does this because Java
``GooseFSFileOutStream.getNextBlock()`` does. On a ``FILE`` worker that flush is
implemented and the write completes. On a ``PAGE`` worker
``PagedBlockWriter.flush()`` throws ``UnsupportedOperationException``, so the
same write fails partway through. Until this file, CI only ever booted the
fixture in its default ``FILE`` mode, which is why that difference went
unnoticed.

Which write types that breaks splits three ways, and the split is the whole
point of parameterising over them. Measured on a PAGE worker against both SDKs,
so the expectations below are not this SDK's opinion — they are what the worker
does, and the Java client agrees cell for cell:

===============  ==================  ===================================
WriteType        single block        crosses a block boundary
===============  ==================  ===================================
MUST_CACHE       Java OK, Rust OK    Java FAIL, Rust FAIL (cannot flush)
CACHE_THROUGH    Java OK, Rust OK    Java OK, Rust OK (degrades UFS-only)
ASYNC_THROUGH    Java OK, Rust OK    Java FAIL, Rust FAIL (cannot flush)
THROUGH          Java OK, Rust OK    Java OK, Rust OK
===============  ==================  ===================================

The reasons behind the column on the right:

* ``MUST_CACHE`` and ``ASYNC_THROUGH`` keep the data in the cache and nowhere
  else at write time, so a flush the worker cannot perform is fatal. Neither can
  fall back to the UFS either: degradation is only available before the first
  block opens (see :file:`test_write_degrade.py`), and by definition a mid-file
  switch is past that point.
* ``CACHE_THROUGH`` opens a cache stream too, but ``resolve_write_strategy``
  pairs it with a single long-lived UFS stream carrying the same bytes. The cache
  copy can therefore be abandoned and the file still completes — so this
  succeeds, and the test verifies the bytes rather than settling for the absence
  of an exception.
* ``THROUGH`` gets ``cache_stream: false``, so no block is ever opened, let alone
  flushed.

The shapes below cross a boundary with a 1 MiB block size rather than by writing
tens of MiB at the 64 MiB default. It is the same event — a mid-file
``getNextBlock()`` — at a fraction of the cost.

What the shapes pin down
------------------------

* ``exactly_one_block`` — a block filled by the file's very last byte must not
  count as a mid-file switch. The SDK used to close such a block eagerly, which
  sent ``flush:true`` for what is really a final-block close and broke PAGE for
  a file the size of a whole block. Java defers the same decision by testing
  ``while (tLen > 0)`` before asking for a new block.
* ``exactly_two_blocks`` — the same deferral, but downstream of a real switch,
  so the two paths cannot mask each other.
* ``just_over_one_block`` — the cheapest genuine mid-file switch.
* ``several_blocks_partial_tail`` — several switches plus a short tail block.
  The tail is what the ``OpenUfsBlockOptions.block_size`` fix is about: the
  field carries the block's actual length, not the nominal block size, so
  reading back a file whose last block is partial has to work too.

Every case verifies the bytes, not just the absence of an exception. The payload
is position-sensitive so a dropped, duplicated or reordered block shows up as a
mismatch instead of passing on a repeating pattern.
"""

from __future__ import annotations

import pytest
from goosefs import AsyncGoosefs, Goosefs, WriteType
from goosefs.exceptions import GoosefsError

# Small enough that crossing a boundary costs a few MiB rather than hundreds
# (the SDK default is 64 MiB), and a multiple of the fixture's
# ``goosefs.worker.page.store.page.size`` of 1 MB so a PAGE worker's paging does
# not interact with the boundary behaviour under test.
BLOCK_SIZE = 1024 * 1024

_WRITE_TYPES = [
    ("must_cache", WriteType.MustCache),
    ("cache_through", WriteType.CacheThrough),
    ("async_through", WriteType.AsyncThrough),
    ("through", WriteType.Through),
]

# (label, length, expected block count). The block count is asserted rather than
# assumed: it is what proves ``block_size_bytes`` took effect and that the
# geometry is the one the label claims.
_SHAPES = [
    ("exactly_one_block", BLOCK_SIZE, 1),
    ("just_over_one_block", BLOCK_SIZE + 1, 2),
    ("exactly_two_blocks", 2 * BLOCK_SIZE, 2),
    ("several_blocks_partial_tail", 3 * BLOCK_SIZE + 1234, 4),
]

_SHAPE_IDS = [s[0] for s in _SHAPES]
_WRITE_TYPE_IDS = [w[0] for w in _WRITE_TYPES]

# The sync client goes through the same ``GoosefsFileWriter``, so re-running the
# whole matrix through it would buy coverage of the blocking shim only. These
# two shapes are the ones that differ from each other at the boundary.
_SYNC_SHAPES = [s for s in _SHAPES if s[0] in ("exactly_one_block", "just_over_one_block")]
_SYNC_SHAPE_IDS = [s[0] for s in _SYNC_SHAPES]


def _payload(length: int) -> bytes:
    """``length`` bytes whose content encodes its own offset.

    Built from 256-byte chunks stamped with a running counter. A plain repeating
    pattern would be blind to exactly the failures worth catching here: block
    boundaries fall on multiples of 256, so two swapped or duplicated blocks
    would compare equal.
    """
    chunks = []
    filled = 0
    index = 0
    while filled < length:
        chunk = f"{index:016d}".encode() + bytes(range(240))
        chunks.append(chunk)
        filled += len(chunk)
        index += 1
    return b"".join(chunks)[:length]


# Write types whose only copy of the data at write time is the cache stream.
# CACHE_THROUGH is deliberately absent: it opens a cache stream as well, but the
# UFS stream holds the same bytes, so losing the cache copy costs nothing the
# file needs. THROUGH opens no cache stream at all.
_CACHE_ONLY_WRITE_TYPES = (WriteType.MustCache, WriteType.AsyncThrough)


def _flush_unsupported(store_type: str, write_type: WriteType, length: int) -> bool:
    """Whether this cluster is expected to reject the write outright.

    True only for a genuine mid-file block switch, on a PAGE worker, under a
    write type that has no second copy to fall back on. Asserting the failure —
    instead of skipping — keeps the worker-side gap visible: when
    ``PagedBlockWriter.flush()`` grows a real implementation these cases start
    passing, and this predicate (along with the ``TODO(worker-page-flush)`` notes
    in ``src/io/file_writer.rs``) is what should then be deleted.
    """
    return store_type == "PAGE" and write_type in _CACHE_ONLY_WRITE_TYPES and length > BLOCK_SIZE


def _assert_flush_error(excinfo: pytest.ExceptionInfo[GoosefsError]) -> None:
    message = str(excinfo.value).lower()
    assert "flush" in message, (
        "a mid-file block switch on a PAGE worker must fail because the worker "
        f"cannot flush a paged block, but the error was: {excinfo.value}"
    )


# ---------------------------------------------------------------------------
# Async
# ---------------------------------------------------------------------------


@pytest.mark.timeout(180)
@pytest.mark.parametrize("shape_label,length,want_blocks", _SHAPES, ids=_SHAPE_IDS)
@pytest.mark.parametrize("wt_label,wt", _WRITE_TYPES, ids=_WRITE_TYPE_IDS)
async def test_async_write_at_block_boundary(
    async_fs: AsyncGoosefs,
    tmp_dir: str,
    worker_block_store_type: str,
    wt_label: str,
    wt: WriteType,
    shape_label: str,
    length: int,
    want_blocks: int,
) -> None:
    path = f"{tmp_dir}/{shape_label}-{wt_label}.bin"
    payload = _payload(length)

    writer = await async_fs.create_file(
        path, recursive=True, write_type=wt, block_size_bytes=BLOCK_SIZE
    )

    if _flush_unsupported(worker_block_store_type, wt, length):
        with pytest.raises(GoosefsError) as excinfo:
            await writer.write(payload)
            await writer.close()
        _assert_flush_error(excinfo)
        # The stream is already broken, so drop the uncommitted state rather
        # than leaving a half-written file for the ``tmp_dir`` teardown.
        await writer.cancel()
        return

    assert await writer.write(payload) == length
    await writer.close()

    status = await async_fs.get_status(path)
    assert status.is_completed()
    assert status.length == length
    assert status.block_size_bytes == BLOCK_SIZE
    assert status.block_count() == want_blocks, (
        f"{length} bytes at a {BLOCK_SIZE}-byte block size should occupy {want_blocks} block(s)"
    )
    assert await async_fs.read_file(path) == payload


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------


@pytest.mark.timeout(180)
@pytest.mark.parametrize("shape_label,length,want_blocks", _SYNC_SHAPES, ids=_SYNC_SHAPE_IDS)
@pytest.mark.parametrize("wt_label,wt", _WRITE_TYPES, ids=_WRITE_TYPE_IDS)
def test_sync_write_at_block_boundary(
    sync_fs: Goosefs,
    sync_tmp_dir: str,
    worker_block_store_type: str,
    wt_label: str,
    wt: WriteType,
    shape_label: str,
    length: int,
    want_blocks: int,
) -> None:
    path = f"{sync_tmp_dir}/{shape_label}-{wt_label}.bin"
    payload = _payload(length)

    writer = sync_fs.create_file(path, recursive=True, write_type=wt, block_size_bytes=BLOCK_SIZE)

    if _flush_unsupported(worker_block_store_type, wt, length):
        with pytest.raises(GoosefsError) as excinfo:
            writer.write(payload)
            writer.close()
        _assert_flush_error(excinfo)
        writer.cancel()
        return

    assert writer.write(payload) == length
    writer.close()

    status = sync_fs.get_status(path)
    assert status.is_completed()
    assert status.length == length
    assert status.block_count() == want_blocks
    assert sync_fs.read_file(path) == payload
