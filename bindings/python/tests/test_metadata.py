"""Metadata-API integration tests for ``AsyncGooseFs``.

These tests require a real GooseFS cluster reachable at
``$GOOSEFS_MASTER_ADDR`` (see ``conftest.py``). Each test runs in an
isolated scratch directory provided by the ``tmp_dir`` fixture.
"""

from __future__ import annotations

import pytest

from goosefs import AsyncGooseFs, DeleteOptions, URIStatus
from goosefs.exceptions import (
    DirectoryNotEmpty,
    GooseFsError,
    NotFound,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# get_status / exists
# ---------------------------------------------------------------------------


async def test_get_status_returns_uristatus_for_directory(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    status = await async_fs.get_status(tmp_dir)
    assert isinstance(status, URIStatus)
    assert status.path == tmp_dir
    assert status.is_folder()
    assert status.is_readable()
    assert status.length == 0
    # Common metadata fields are populated even for an empty directory.
    assert status.creation_time_ms > 0
    assert status.last_modification_time_ms > 0


async def test_get_status_raises_notfound(async_fs: AsyncGooseFs, tmp_dir: str) -> None:
    missing = f"{tmp_dir}/does-not-exist"
    with pytest.raises(NotFound):
        await async_fs.get_status(missing)


async def test_exists_returns_false_for_missing(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    assert await async_fs.exists(f"{tmp_dir}/missing") is False


async def test_exists_returns_true_for_directory(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    assert await async_fs.exists(tmp_dir) is True


# ---------------------------------------------------------------------------
# mkdir
# ---------------------------------------------------------------------------


async def test_mkdir_creates_directory(async_fs: AsyncGooseFs, tmp_dir: str) -> None:
    new_dir = f"{tmp_dir}/sub"
    await async_fs.mkdir(new_dir)
    assert await async_fs.exists(new_dir)


async def test_mkdir_recursive_creates_intermediate_dirs(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    deep = f"{tmp_dir}/a/b/c"
    await async_fs.mkdir(deep, recursive=True)
    assert await async_fs.exists(f"{tmp_dir}/a")
    assert await async_fs.exists(f"{tmp_dir}/a/b")
    assert await async_fs.exists(deep)


async def test_mkdir_existing_is_idempotent(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    """``mkdir`` is idempotent (POSIX ``mkdir -p`` semantics).

    The underlying ``CreateDirectoryPOptions.allow_exists=true`` is hard-wired
    by ``goosefs-sdk::client::master::create_directory``, so calling ``mkdir``
    on an already-existing directory must succeed without raising.
    Users who need an exclusive-create check should call ``exists()`` first.
    """
    new_dir = f"{tmp_dir}/sub"
    await async_fs.mkdir(new_dir)
    # Second call must be a no-op (no AlreadyExists).
    await async_fs.mkdir(new_dir)
    assert await async_fs.exists(new_dir)


# ---------------------------------------------------------------------------
# list_status
# ---------------------------------------------------------------------------


async def test_list_status_empty_directory(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    items = await async_fs.list_status(tmp_dir)
    assert items == []


async def test_list_status_returns_children(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    for name in ("a", "b", "c"):
        await async_fs.mkdir(f"{tmp_dir}/{name}")
    items = await async_fs.list_status(tmp_dir)
    names = sorted(s.name for s in items)
    assert names == ["a", "b", "c"]
    for s in items:
        assert s.is_folder()


async def test_list_status_recursive_walks_tree(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    await async_fs.mkdir(f"{tmp_dir}/x/y/z", recursive=True)
    items = await async_fs.list_status(tmp_dir, recursive=True)
    paths = {s.path for s in items}
    assert f"{tmp_dir}/x" in paths
    assert f"{tmp_dir}/x/y" in paths
    assert f"{tmp_dir}/x/y/z" in paths


# ---------------------------------------------------------------------------
# rename
# ---------------------------------------------------------------------------


async def test_rename_moves_directory(async_fs: AsyncGooseFs, tmp_dir: str) -> None:
    src = f"{tmp_dir}/src"
    dst = f"{tmp_dir}/dst"
    await async_fs.mkdir(src)
    await async_fs.rename(src, dst)
    assert not await async_fs.exists(src)
    assert await async_fs.exists(dst)


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


async def test_delete_empty_directory(async_fs: AsyncGooseFs, tmp_dir: str) -> None:
    sub = f"{tmp_dir}/sub"
    await async_fs.mkdir(sub)
    await async_fs.delete(sub)
    assert not await async_fs.exists(sub)


async def test_delete_non_empty_without_recursive_raises(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    parent = f"{tmp_dir}/parent"
    await async_fs.mkdir(parent)
    await async_fs.mkdir(f"{parent}/child")
    # GooseFS may surface this as DirectoryNotEmpty; allow GooseFsError as a
    # broader fallback in case the server returns a different specific code.
    with pytest.raises((DirectoryNotEmpty, GooseFsError)):
        await async_fs.delete(parent)


async def test_delete_recursive_removes_tree(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    parent = f"{tmp_dir}/parent"
    await async_fs.mkdir(f"{parent}/a/b", recursive=True)
    await async_fs.delete(parent, recursive=True)
    assert not await async_fs.exists(parent)


async def test_delete_with_options_object(
    async_fs: AsyncGooseFs, tmp_dir: str
) -> None:
    parent = f"{tmp_dir}/parent2"
    await async_fs.mkdir(f"{parent}/x", recursive=True)
    await async_fs.delete_with_options(parent, DeleteOptions(recursive=True))
    assert not await async_fs.exists(parent)


# ---------------------------------------------------------------------------
# Lifecycle / context manager
# ---------------------------------------------------------------------------


async def test_async_context_manager_closes_on_exit(config) -> None:
    fs = await AsyncGooseFs.connect(config)
    async with fs as inner:
        assert inner is fs
        await inner.exists("/")
    # After exit, every method must reject calls.
    with pytest.raises(RuntimeError):
        await fs.exists("/")


async def test_close_is_idempotent(config) -> None:
    fs = await AsyncGooseFs.connect(config)
    await fs.close()
    # Second close should not raise.
    await fs.close()
    with pytest.raises(RuntimeError):
        await fs.exists("/")


async def test_repr_contains_master_addr(async_fs: AsyncGooseFs, master_addr: str) -> None:
    assert master_addr in repr(async_fs)
