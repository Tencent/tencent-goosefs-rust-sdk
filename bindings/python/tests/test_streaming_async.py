"""Integration tests for P5 streaming I/O — `AsyncFileReader` / `AsyncFileWriter`.

Covers:

* Round-trip: incremental writes via ``AsyncFileWriter`` produce a file that
  ``AsyncFileReader`` reads back byte-identical.
* ``read(size)`` with explicit chunk size and ``read(-1)`` for read-to-EOF.
* ``read_at`` (positioned read) does not move the logical cursor.
* ``seek`` with all three ``whence`` values plus ``tell`` / ``__len__``.
* ``async with`` lifecycle: normal exit closes (commits) the writer, exception
  inside the block cancels (no commit). Same for the reader (close on exit).
* ``cancel()`` semantics: the path must not be readable afterwards.
* Failure modes: read after close, write after close, invalid whence, etc.

These tests assume a running cluster reachable via ``GOOSEFS_MASTER_ADDR`` —
see ``conftest.py``.
"""

from __future__ import annotations

import os
import struct

import pytest

from goosefs import (
    AsyncFileReader,
    AsyncFileWriter,
    AsyncGoosefs,
    WriteType,
    exceptions,
)


# ---------------------------------------------------------------------------
# Round-trip basics
# ---------------------------------------------------------------------------


async def test_writer_then_reader_roundtrip(async_fs: AsyncGoosefs, tmp_dir: str):
    path = f"{tmp_dir}/hello.bin"
    payload = b"hello, streaming world!\n"

    writer = await async_fs.create_file(path, write_type=WriteType.MustCache)
    assert isinstance(writer, AsyncFileWriter)
    n = await writer.write(payload)
    assert n == len(payload)
    await writer.close()

    reader = await async_fs.open_file(path)
    assert isinstance(reader, AsyncFileReader)
    assert len(reader) == len(payload)
    got = await reader.read()  # default: read to EOF
    await reader.close()
    assert got == payload


async def test_writer_multiple_writes_concatenated(
    async_fs: AsyncGoosefs, tmp_dir: str
):
    """Three sequential writes form a single contiguous file."""
    path = f"{tmp_dir}/concat.bin"
    chunks = [b"AAAA", b"BBBB", b"CCCC"]

    async with await async_fs.create_file(path) as w:
        for c in chunks:
            assert await w.write(c) == len(c)

    async with await async_fs.open_file(path) as r:
        assert len(r) == sum(len(c) for c in chunks)
        full = await r.read()
        assert full == b"".join(chunks)


# ---------------------------------------------------------------------------
# Reader: read(), read_at(), seek(), tell()
# ---------------------------------------------------------------------------


async def test_reader_read_in_chunks(async_fs: AsyncGoosefs, tmp_dir: str):
    """Calling read(n) repeatedly drains the stream block-by-block."""
    path = f"{tmp_dir}/chunks.bin"
    # 1 MiB of recognisable data (each 4-byte word encodes its index).
    n_words = 256 * 1024
    body = b"".join(struct.pack("<I", i) for i in range(n_words))
    async with await async_fs.create_file(path) as w:
        await w.write(body)

    async with await async_fs.open_file(path) as r:
        assert len(r) == len(body)
        # Read in 64 KiB chunks until EOF.
        chunk = 64 * 1024
        buf = bytearray()
        while True:
            piece = await r.read(chunk)
            if not piece:
                break
            buf += piece
        assert bytes(buf) == body


async def test_reader_read_at_does_not_move_cursor(
    async_fs: AsyncGoosefs, tmp_dir: str
):
    path = f"{tmp_dir}/positioned.bin"
    body = bytes(range(256)) * 16  # 4 KiB
    async with await async_fs.create_file(path) as w:
        await w.write(body)

    async with await async_fs.open_file(path) as r:
        # Cursor starts at 0.
        assert r.tell() == 0
        # read_at should not move it.
        slice_ = await r.read_at(1024, 32)
        assert slice_ == body[1024:1056]
        assert r.tell() == 0
        # A normal read still starts from 0.
        head = await r.read(8)
        assert head == body[:8]
        assert r.tell() == 8


async def test_reader_seek_set_cur_end(async_fs: AsyncGoosefs, tmp_dir: str):
    path = f"{tmp_dir}/seek.bin"
    body = b"0123456789" * 100  # 1000 bytes
    async with await async_fs.create_file(path) as w:
        await w.write(body)

    async with await async_fs.open_file(path) as r:
        # SEEK_SET (whence=0)
        await r.seek(50)
        assert r.tell() == 50
        assert (await r.read(5)) == body[50:55]
        # SEEK_CUR (whence=1)
        await r.seek(10, 1)
        assert r.tell() == 65
        assert (await r.read(5)) == body[65:70]
        # SEEK_END (whence=2): negative offset from end.
        await r.seek(-10, 2)
        assert r.tell() == 990
        assert (await r.read()) == body[990:]
        # At EOF
        assert r.tell() == 1000


async def test_reader_invalid_whence_raises_value_error(
    async_fs: AsyncGoosefs, tmp_dir: str
):
    path = f"{tmp_dir}/badseek.bin"
    async with await async_fs.create_file(path) as w:
        await w.write(b"abc")
    async with await async_fs.open_file(path) as r:
        with pytest.raises(ValueError):
            await r.seek(0, 9)  # only 0/1/2 are valid
        with pytest.raises(ValueError):
            await r.seek(-1, 0)  # negative absolute offset


async def test_reader_read_at_negative_offset_raises(
    async_fs: AsyncGoosefs, tmp_dir: str
):
    path = f"{tmp_dir}/badreadat.bin"
    async with await async_fs.create_file(path) as w:
        await w.write(b"x")
    async with await async_fs.open_file(path) as r:
        with pytest.raises(ValueError):
            await r.read_at(-1, 4)


# ---------------------------------------------------------------------------
# Lifecycle: async with, close(), use-after-close, GC
# ---------------------------------------------------------------------------


async def test_async_with_writer_commits_on_normal_exit(
    async_fs: AsyncGoosefs, tmp_dir: str
):
    path = f"{tmp_dir}/commit.bin"
    async with await async_fs.create_file(path) as w:
        await w.write(b"committed")
    # File must exist and be readable.
    async with await async_fs.open_file(path) as r:
        assert (await r.read()) == b"committed"


async def test_async_with_writer_cancels_on_exception(
    async_fs: AsyncGoosefs, tmp_dir: str
):
    """An unhandled exception inside the ``async with`` block triggers
    ``cancel()`` rather than ``close()``, so the file is not committed.
    """
    path = f"{tmp_dir}/aborted.bin"

    class _Boom(RuntimeError):
        pass

    with pytest.raises(_Boom):
        async with await async_fs.create_file(path) as w:
            await w.write(b"partial data")
            raise _Boom("user error after partial write")

    # After cancel(), the file must NOT be visible to the metadata layer.
    # Note: ``read_file`` may currently return b"" for a cancelled-but-not-yet
    # GC'd path because the master sometimes still serves a length-0 inode.
    # The strongest available contract is ``exists() == False``.
    assert not await async_fs.exists(path)


async def test_writer_explicit_cancel_then_path_not_readable(
    async_fs: AsyncGoosefs, tmp_dir: str
):
    path = f"{tmp_dir}/explicit-cancel.bin"
    w = await async_fs.create_file(path)
    await w.write(b"discarded")
    await w.cancel()
    # Idempotent: cancel again is a no-op.
    await w.cancel()

    # Same contract as above: the path must not exist after cancel().
    assert not await async_fs.exists(path)


async def test_double_close_is_idempotent(async_fs: AsyncGoosefs, tmp_dir: str):
    path = f"{tmp_dir}/double-close.bin"
    w = await async_fs.create_file(path)
    await w.write(b"x")
    await w.close()
    await w.close()  # must not raise

    r = await async_fs.open_file(path)
    await r.close()
    await r.close()  # must not raise


async def test_write_after_close_raises(async_fs: AsyncGoosefs, tmp_dir: str):
    path = f"{tmp_dir}/write-after-close.bin"
    w = await async_fs.create_file(path)
    await w.write(b"hi")
    await w.close()
    with pytest.raises(RuntimeError):
        await w.write(b"too late")


async def test_read_after_close_raises(async_fs: AsyncGoosefs, tmp_dir: str):
    path = f"{tmp_dir}/read-after-close.bin"
    async with await async_fs.create_file(path) as w:
        await w.write(b"abc")
    r = await async_fs.open_file(path)
    await r.close()
    with pytest.raises(RuntimeError):
        await r.read(1)
    with pytest.raises(RuntimeError):
        await r.seek(0)
    # tell() also fails, but with the "closed" message.
    with pytest.raises(RuntimeError):
        r.tell()


# ---------------------------------------------------------------------------
# Buffer protocol: writer accepts bytes / bytearray / memoryview
# ---------------------------------------------------------------------------


async def test_writer_accepts_bytearray_and_memoryview(
    async_fs: AsyncGoosefs, tmp_dir: str
):
    path = f"{tmp_dir}/bufproto.bin"
    async with await async_fs.create_file(path) as w:
        await w.write(b"AAAA")
        await w.write(bytearray(b"BBBB"))
        await w.write(memoryview(b"CCCC"))
    async with await async_fs.open_file(path) as r:
        assert (await r.read()) == b"AAAABBBBCCCC"


async def test_writer_rejects_str(async_fs: AsyncGoosefs, tmp_dir: str):
    path = f"{tmp_dir}/strreject.bin"
    w = await async_fs.create_file(path)
    try:
        with pytest.raises(TypeError):
            await w.write("not bytes")  # type: ignore[arg-type]
    finally:
        await w.cancel()


# ---------------------------------------------------------------------------
# Larger payloads (still small enough to be CI-friendly)
# ---------------------------------------------------------------------------


async def test_writer_large_payload_roundtrip(async_fs: AsyncGoosefs, tmp_dir: str):
    """Write 4 MiB in 64 KiB chunks, then read back in different chunk sizes."""
    path = f"{tmp_dir}/large.bin"
    block = os.urandom(64 * 1024)
    n_blocks = 64  # 64 * 64 KiB = 4 MiB
    expected = block * n_blocks

    async with await async_fs.create_file(path) as w:
        for _ in range(n_blocks):
            await w.write(block)

    async with await async_fs.open_file(path) as r:
        assert len(r) == len(expected)
        # Re-read with an odd chunk size to exercise the short-read loop.
        chunk_size = 33 * 1024
        buf = bytearray()
        while True:
            piece = await r.read(chunk_size)
            if not piece:
                break
            buf += piece
        assert bytes(buf) == expected
