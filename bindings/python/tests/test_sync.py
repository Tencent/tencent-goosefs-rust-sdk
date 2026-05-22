"""Integration tests for the synchronous ``GooseFs`` wrapper (P3).

These mirror :file:`test_metadata.py` but call the blocking API. They also
cover the two safety guards specific to ``GooseFs``:

* Calling sync methods from inside an asyncio event loop must raise
  ``RuntimeError`` (Review #17.1).
* The context-manager protocol (``with GooseFs(...) as fs:``) closes the
  connection on exit.

The fork-safety guard (Review #17.4) cannot be exercised reliably under
pytest's collector without spawning a child process; we settle for
verifying that the PID is actually checked at runtime by monkey-patching
the recorded ``creator_pid`` on a *single* test.
"""

from __future__ import annotations

import asyncio

import pytest

from goosefs import Config, DeleteOptions, GooseFs, URIStatus
from goosefs.exceptions import (
    AlreadyExists,
    DirectoryNotEmpty,
    GooseFsError,
    NotFound,
)


# ---------------------------------------------------------------------------
# get_status / exists
# ---------------------------------------------------------------------------


def test_sync_get_status_returns_uristatus(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    status = sync_fs.get_status(sync_tmp_dir)
    assert isinstance(status, URIStatus)
    assert status.path == sync_tmp_dir
    assert status.is_folder()


def test_sync_get_status_raises_notfound(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    with pytest.raises(NotFound):
        sync_fs.get_status(f"{sync_tmp_dir}/missing")


def test_sync_exists_true_and_false(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    assert sync_fs.exists(sync_tmp_dir) is True
    assert sync_fs.exists(f"{sync_tmp_dir}/missing") is False


# ---------------------------------------------------------------------------
# mkdir / list_status / rename / delete
# ---------------------------------------------------------------------------


def test_sync_mkdir_recursive(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    deep = f"{sync_tmp_dir}/a/b/c"
    sync_fs.mkdir(deep, recursive=True)
    assert sync_fs.exists(f"{sync_tmp_dir}/a/b")
    assert sync_fs.exists(deep)


def test_sync_mkdir_is_idempotent(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    """Same idempotent semantics as the async wrapper (SDK hard-wires
    ``allow_exists=true``)."""
    p = f"{sync_tmp_dir}/idempotent"
    sync_fs.mkdir(p)
    sync_fs.mkdir(p)  # must not raise
    assert sync_fs.exists(p)


def test_sync_list_status(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    for name in ("a", "b", "c"):
        sync_fs.mkdir(f"{sync_tmp_dir}/{name}")
    items = sync_fs.list_status(sync_tmp_dir)
    names = sorted(s.name for s in items)
    assert names == ["a", "b", "c"]


def test_sync_list_status_recursive(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    sync_fs.mkdir(f"{sync_tmp_dir}/x/y/z", recursive=True)
    items = sync_fs.list_status(sync_tmp_dir, recursive=True)
    paths = {s.path for s in items}
    assert {f"{sync_tmp_dir}/x", f"{sync_tmp_dir}/x/y", f"{sync_tmp_dir}/x/y/z"} <= paths


def test_sync_rename(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    src = f"{sync_tmp_dir}/src"
    dst = f"{sync_tmp_dir}/dst"
    sync_fs.mkdir(src)
    sync_fs.rename(src, dst)
    assert not sync_fs.exists(src)
    assert sync_fs.exists(dst)


def test_sync_rename_to_existing_target_raises(
    sync_fs: GooseFs, sync_tmp_dir: str
) -> None:
    src = f"{sync_tmp_dir}/r-src"
    dst = f"{sync_tmp_dir}/r-dst"
    sync_fs.mkdir(src)
    sync_fs.mkdir(dst)
    with pytest.raises((AlreadyExists, GooseFsError)):
        sync_fs.rename(src, dst)


def test_sync_delete_empty(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    sub = f"{sync_tmp_dir}/sub"
    sync_fs.mkdir(sub)
    sync_fs.delete(sub)
    assert not sync_fs.exists(sub)


def test_sync_delete_non_empty_without_recursive_raises(
    sync_fs: GooseFs, sync_tmp_dir: str
) -> None:
    parent = f"{sync_tmp_dir}/parent"
    sync_fs.mkdir(f"{parent}/child", recursive=True)
    with pytest.raises((DirectoryNotEmpty, GooseFsError)):
        sync_fs.delete(parent)


def test_sync_delete_recursive(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    parent = f"{sync_tmp_dir}/p"
    sync_fs.mkdir(f"{parent}/a/b", recursive=True)
    sync_fs.delete(parent, recursive=True)
    assert not sync_fs.exists(parent)


def test_sync_delete_with_options_object(sync_fs: GooseFs, sync_tmp_dir: str) -> None:
    parent = f"{sync_tmp_dir}/p2"
    sync_fs.mkdir(f"{parent}/x", recursive=True)
    sync_fs.delete_with_options(parent, DeleteOptions(recursive=True))
    assert not sync_fs.exists(parent)


# ---------------------------------------------------------------------------
# Lifecycle / context manager
# ---------------------------------------------------------------------------


def test_sync_context_manager_closes_on_exit(config: Config) -> None:
    with GooseFs(config) as fs:
        assert fs.exists("/") is True
    # After context exit, every method must raise RuntimeError.
    with pytest.raises(RuntimeError):
        fs.exists("/")


def test_sync_close_is_idempotent(config: Config) -> None:
    fs = GooseFs(config)
    fs.close()
    fs.close()  # second close must not raise
    with pytest.raises(RuntimeError):
        fs.exists("/")


def test_sync_repr_contains_master_addr(sync_fs: GooseFs, master_addr: str) -> None:
    assert master_addr in repr(sync_fs)


# ---------------------------------------------------------------------------
# Safety guard: deadlock prevention (Review #17.1)
# ---------------------------------------------------------------------------


def test_sync_construct_inside_asyncio_loop_raises(config: Config) -> None:
    """``GooseFs(...)`` itself blocks on the runtime, so calling it from
    inside a coroutine running on an asyncio loop must refuse with
    ``RuntimeError`` rather than dead-lock the loop.
    """

    async def attempt() -> None:
        # Direct construction inside the loop must raise.
        with pytest.raises(RuntimeError):
            GooseFs(config)

    asyncio.run(attempt())


def test_sync_method_call_inside_asyncio_loop_raises(
    sync_fs: GooseFs, sync_tmp_dir: str
) -> None:
    """A pre-existing GooseFs instance, when used from inside an asyncio
    coroutine, must also raise. (We cannot deadlock the loop in tests.)
    """

    async def attempt() -> None:
        with pytest.raises(RuntimeError):
            sync_fs.exists(sync_tmp_dir)

    asyncio.run(attempt())


# ---------------------------------------------------------------------------
# Safety guard: post-fork detection (Review #17.4)
# ---------------------------------------------------------------------------


def test_sync_fork_check_rejects_pid_mismatch(sync_fs: GooseFs) -> None:
    """We cannot spawn a real fork in pytest cheaply, but we *can* observe
    that the PID is checked: forge a different ``creator_pid`` on the
    Python side via attribute access. Since ``creator_pid`` is a Rust
    field not reachable from Python, this test simply verifies that the
    error message is emitted under a real PID mismatch by spawning a
    subprocess that inherits the handle.
    """
    import multiprocessing as mp

    ctx = mp.get_context("fork")

    def child(fs: GooseFs, queue: "mp.Queue[str]") -> None:
        try:
            fs.exists("/")
            queue.put("NO_ERROR")
        except RuntimeError as e:
            queue.put(str(e))
        except Exception as e:  # noqa: BLE001
            queue.put(f"WRONG: {type(e).__name__}: {e}")

    queue: "mp.Queue[str]" = ctx.Queue()
    proc = ctx.Process(target=child, args=(sync_fs, queue))
    proc.start()
    proc.join(timeout=10)
    assert not proc.is_alive(), "child process hung — fork guard ineffective"

    msg = queue.get_nowait()
    assert "fork" in msg.lower(), f"unexpected child outcome: {msg!r}"
