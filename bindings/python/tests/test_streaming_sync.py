"""Integration tests for P5 streaming I/O — sync ``FileReader`` / ``FileWriter``.

Mirrors :mod:`test_streaming_async` for the synchronous facade. The same
deadlock and fork guards from P3 apply: see ``guarded_block_on`` in
``src/streaming.rs``. These tests never touch asyncio, so they validate
the sync wrapper end-to-end.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from goosefs import FileReader, FileWriter, Goosefs, WriteType


# ---------------------------------------------------------------------------
# Round-trip basics
# ---------------------------------------------------------------------------


def test_sync_writer_then_reader_roundtrip(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/sync-hello.bin"
    payload = b"sync streaming hello\n"

    w = sync_fs.create_file(path, write_type=WriteType.MustCache)
    assert isinstance(w, FileWriter)
    assert w.write(payload) == len(payload)
    w.close()

    r = sync_fs.open_file(path)
    assert isinstance(r, FileReader)
    assert len(r) == len(payload)
    assert r.read() == payload
    r.close()


def test_sync_with_writer_commits_on_normal_exit(
    sync_fs: Goosefs, sync_tmp_dir: str
):
    path = f"{sync_tmp_dir}/with-commit.bin"
    with sync_fs.create_file(path) as w:
        w.write(b"committed")
    with sync_fs.open_file(path) as r:
        assert r.read() == b"committed"


def test_sync_with_writer_cancels_on_exception(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/with-cancel.bin"

    class _Boom(RuntimeError):
        pass

    with pytest.raises(_Boom):
        with sync_fs.create_file(path) as w:
            w.write(b"will be discarded")
            raise _Boom("user error after partial write")

    # After cancel(), the path must not exist. ``read_file`` may currently
    # return b"" for a cancelled-but-not-yet GC'd path because the master
    # sometimes still serves a length-0 inode. ``exists`` is the strongest
    # available contract.
    assert not sync_fs.exists(path)


def test_sync_explicit_cancel(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/explicit-cancel.bin"
    w = sync_fs.create_file(path)
    w.write(b"discarded")
    w.cancel()
    w.cancel()  # idempotent

    assert not sync_fs.exists(path)


# ---------------------------------------------------------------------------
# Reader: chunked read, read_at, seek, tell
# ---------------------------------------------------------------------------


def test_sync_reader_read_in_chunks(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/sync-chunks.bin"
    body = b"x" * (256 * 1024)  # 256 KiB
    with sync_fs.create_file(path) as w:
        w.write(body)

    with sync_fs.open_file(path) as r:
        out = bytearray()
        while True:
            piece = r.read(33 * 1024)
            if not piece:
                break
            out += piece
        assert bytes(out) == body


def test_sync_reader_read_at_does_not_move_cursor(
    sync_fs: Goosefs, sync_tmp_dir: str
):
    path = f"{sync_tmp_dir}/sync-pread.bin"
    body = bytes(range(256)) * 16  # 4 KiB
    with sync_fs.create_file(path) as w:
        w.write(body)

    with sync_fs.open_file(path) as r:
        assert r.tell() == 0
        chunk = r.read_at(100, 16)
        assert chunk == body[100:116]
        assert r.tell() == 0
        head = r.read(4)
        assert head == body[:4]
        assert r.tell() == 4


def test_sync_reader_seek_set_cur_end(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/sync-seek.bin"
    body = b"abcdefghij" * 100
    with sync_fs.create_file(path) as w:
        w.write(body)

    with sync_fs.open_file(path) as r:
        assert r.seek(50) == 50
        assert r.read(5) == body[50:55]
        assert r.seek(10, 1) == 65
        assert r.read(5) == body[65:70]
        assert r.seek(-5, 2) == len(body) - 5
        assert r.read() == body[-5:]


def test_sync_reader_invalid_whence_raises(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/sync-badseek.bin"
    with sync_fs.create_file(path) as w:
        w.write(b"abc")
    with sync_fs.open_file(path) as r:
        with pytest.raises(ValueError):
            r.seek(0, 99)
        with pytest.raises(ValueError):
            r.seek(-1, 0)


# ---------------------------------------------------------------------------
# Use-after-close
# ---------------------------------------------------------------------------


def test_sync_double_close_is_idempotent(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/sync-dblclose.bin"
    w = sync_fs.create_file(path)
    w.write(b"x")
    w.close()
    w.close()

    r = sync_fs.open_file(path)
    r.close()
    r.close()


def test_sync_write_after_close_raises(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/sync-write-after-close.bin"
    w = sync_fs.create_file(path)
    w.write(b"hi")
    w.close()
    with pytest.raises(RuntimeError):
        w.write(b"too late")


def test_sync_read_after_close_raises(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/sync-read-after-close.bin"
    with sync_fs.create_file(path) as w:
        w.write(b"abc")
    r = sync_fs.open_file(path)
    r.close()
    with pytest.raises(RuntimeError):
        r.read(1)


# ---------------------------------------------------------------------------
# Deadlock guard: must refuse calls from inside an asyncio loop.
# ---------------------------------------------------------------------------


def test_sync_methods_blocked_inside_running_event_loop(
    sync_fs: Goosefs, sync_tmp_dir: str
):
    """A sync ``FileWriter`` / ``FileReader`` method called from inside a
    running asyncio loop must raise ``RuntimeError`` rather than deadlocking
    or silently scheduling a `block_on` on the loop's executor.
    """
    path = f"{sync_tmp_dir}/loop-guarded.bin"
    # Pre-create a file with seed data so we can also test the reader.
    with sync_fs.create_file(path) as w:
        w.write(b"hello")

    async def _hostile():
        # This must blow up on entry (not hang).
        with pytest.raises(RuntimeError):
            sync_fs.create_file(f"{sync_tmp_dir}/from-loop.bin")
        with pytest.raises(RuntimeError):
            sync_fs.open_file(path)

    asyncio.run(_hostile())


# ---------------------------------------------------------------------------
# Larger payload — sync flavour
# ---------------------------------------------------------------------------


def test_sync_writer_large_payload_roundtrip(sync_fs: Goosefs, sync_tmp_dir: str):
    path = f"{sync_tmp_dir}/sync-large.bin"
    block = os.urandom(32 * 1024)
    n_blocks = 64  # 2 MiB
    expected = block * n_blocks

    with sync_fs.create_file(path) as w:
        for _ in range(n_blocks):
            w.write(block)

    with sync_fs.open_file(path) as r:
        assert len(r) == len(expected)
        chunk_size = 17 * 1024
        out = bytearray()
        while True:
            piece = r.read(chunk_size)
            if not piece:
                break
            out += piece
        assert bytes(out) == expected
