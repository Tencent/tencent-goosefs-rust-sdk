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

"""Metadata-API integration tests for ``AsyncGoosefs``.

These tests require a real GooseFS cluster reachable at
``$GOOSEFS_MASTER_ADDR`` (see ``conftest.py``). Each test runs in an
isolated scratch directory provided by the ``tmp_dir`` fixture.
"""

from __future__ import annotations

import pytest
from goosefs import AsyncGoosefs, DeleteOptions, URIStatus
from goosefs.exceptions import (
    DirectoryNotEmpty,
    GoosefsError,
    NotFound,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# get_status / exists
# ---------------------------------------------------------------------------


async def test_get_status_returns_uristatus_for_directory(
    async_fs: AsyncGoosefs, tmp_dir: str
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


async def test_get_status_raises_notfound(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    missing = f"{tmp_dir}/does-not-exist"
    with pytest.raises(NotFound):
        await async_fs.get_status(missing)


async def test_exists_returns_false_for_missing(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    assert await async_fs.exists(f"{tmp_dir}/missing") is False


async def test_exists_returns_true_for_directory(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    assert await async_fs.exists(tmp_dir) is True


# ---------------------------------------------------------------------------
# batch_get_status / batch_exists (Phase 2.1)
# ---------------------------------------------------------------------------


async def test_batch_exists_mixed(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    sub = f"{tmp_dir}/sub"
    await async_fs.mkdir(sub)
    paths = [tmp_dir, sub, f"{tmp_dir}/missing"]
    results = await async_fs.batch_exists(paths)
    assert results == [True, True, False]


async def test_batch_exists_empty(async_fs: AsyncGoosefs) -> None:
    assert await async_fs.batch_exists([]) == []


async def test_batch_get_status_returns_in_order(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    a = f"{tmp_dir}/a"
    b = f"{tmp_dir}/b"
    await async_fs.mkdir(a)
    await async_fs.mkdir(b)
    statuses = await async_fs.batch_get_status([a, b, tmp_dir])
    assert [s.path for s in statuses] == [a, b, tmp_dir]
    assert all(isinstance(s, URIStatus) for s in statuses)


async def test_batch_get_status_fails_whole_batch_on_missing(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    with pytest.raises(NotFound):
        await async_fs.batch_get_status([tmp_dir, f"{tmp_dir}/missing"])


# ---------------------------------------------------------------------------
# mkdir
# ---------------------------------------------------------------------------


async def test_mkdir_creates_directory(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    new_dir = f"{tmp_dir}/sub"
    await async_fs.mkdir(new_dir)
    assert await async_fs.exists(new_dir)


async def test_mkdir_recursive_creates_intermediate_dirs(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    deep = f"{tmp_dir}/a/b/c"
    await async_fs.mkdir(deep, recursive=True)
    assert await async_fs.exists(f"{tmp_dir}/a")
    assert await async_fs.exists(f"{tmp_dir}/a/b")
    assert await async_fs.exists(deep)


async def test_mkdir_existing_is_idempotent(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
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


async def test_list_status_empty_directory(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    items = await async_fs.list_status(tmp_dir)
    assert items == []


async def test_list_status_returns_children(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    for name in ("a", "b", "c"):
        await async_fs.mkdir(f"{tmp_dir}/{name}")
    items = await async_fs.list_status(tmp_dir)
    names = sorted(s.name for s in items)
    assert names == ["a", "b", "c"]
    for s in items:
        assert s.is_folder()


async def test_list_status_recursive_walks_tree(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    await async_fs.mkdir(f"{tmp_dir}/x/y/z", recursive=True)
    items = await async_fs.list_status(tmp_dir, recursive=True)
    paths = {s.path for s in items}
    assert f"{tmp_dir}/x" in paths
    assert f"{tmp_dir}/x/y" in paths
    assert f"{tmp_dir}/x/y/z" in paths


# ---------------------------------------------------------------------------
# rename
# ---------------------------------------------------------------------------


async def test_rename_moves_directory(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    src = f"{tmp_dir}/src"
    dst = f"{tmp_dir}/dst"
    await async_fs.mkdir(src)
    await async_fs.rename(src, dst)
    assert not await async_fs.exists(src)
    assert await async_fs.exists(dst)


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


async def test_delete_empty_directory(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    sub = f"{tmp_dir}/sub"
    await async_fs.mkdir(sub)
    await async_fs.delete(sub)
    assert not await async_fs.exists(sub)


async def test_delete_non_empty_without_recursive_raises(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    parent = f"{tmp_dir}/parent"
    await async_fs.mkdir(parent)
    await async_fs.mkdir(f"{parent}/child")
    # GooseFS may surface this as DirectoryNotEmpty; allow GoosefsError as a
    # broader fallback in case the server returns a different specific code.
    with pytest.raises((DirectoryNotEmpty, GoosefsError)):
        await async_fs.delete(parent)


async def test_delete_recursive_removes_tree(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    parent = f"{tmp_dir}/parent"
    await async_fs.mkdir(f"{parent}/a/b", recursive=True)
    await async_fs.delete(parent, recursive=True)
    assert not await async_fs.exists(parent)


async def test_delete_with_options_object(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    parent = f"{tmp_dir}/parent2"
    await async_fs.mkdir(f"{parent}/x", recursive=True)
    await async_fs.delete_with_options(parent, DeleteOptions(recursive=True))
    assert not await async_fs.exists(parent)


# ---------------------------------------------------------------------------
# Lifecycle / context manager
# ---------------------------------------------------------------------------


async def test_async_context_manager_closes_on_exit(config) -> None:
    fs = await AsyncGoosefs.connect(config)
    async with fs as inner:
        assert inner is fs
        await inner.exists("/")
    # After exit, every method must reject calls.
    with pytest.raises(RuntimeError):
        await fs.exists("/")


async def test_close_is_idempotent(config) -> None:
    fs = await AsyncGoosefs.connect(config)
    await fs.close()
    # Second close should not raise.
    await fs.close()
    with pytest.raises(RuntimeError):
        await fs.exists("/")


async def test_repr_contains_master_addr(async_fs: AsyncGoosefs, master_addr: str) -> None:
    assert master_addr in repr(async_fs)


# ---------------------------------------------------------------------------
# batch_create_dir / batch_create_file / batch_rename / batch_delete
# ---------------------------------------------------------------------------


async def test_batch_create_dir_creates_all(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    dirs = [f"{tmp_dir}/bd{i}" for i in range(3)]
    await async_fs.batch_create_dir(dirs)
    exists = await async_fs.batch_exists(dirs)
    assert exists == [True, True, True]
    for d in dirs:
        status = await async_fs.get_status(d)
        assert status.is_folder()


async def test_batch_create_dir_recursive(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    # recursive=True lets the parent path be created on the fly.
    nested = [f"{tmp_dir}/parent/child1", f"{tmp_dir}/parent/child2"]
    await async_fs.batch_create_dir(nested, recursive=True)
    assert await async_fs.batch_exists(nested) == [True, True]


async def test_batch_create_file_creates_empty_files(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    files = [f"{tmp_dir}/bf{i}" for i in range(3)]
    written = await async_fs.batch_create_file(files)
    # Empty files report 0 bytes written.
    assert written == [0, 0, 0]
    statuses = await async_fs.batch_get_status(files)
    for s in statuses:
        assert not s.is_folder()
        assert s.length == 0


async def test_batch_rename_moves_all(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    src = [f"{tmp_dir}/src{i}" for i in range(2)]
    dst = [f"{tmp_dir}/dst{i}" for i in range(2)]
    await async_fs.batch_create_file(src)
    # pairs is a flat list: [src_0, dst_0, src_1, dst_1, ...]
    pairs: list[str] = []
    for s, d in zip(src, dst):
        pairs.extend([s, d])
    await async_fs.batch_rename(pairs)
    assert await async_fs.batch_exists(src) == [False, False]
    assert await async_fs.batch_exists(dst) == [True, True]


async def test_batch_rename_odd_length_raises(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    with pytest.raises(ValueError):
        await async_fs.batch_rename([f"{tmp_dir}/a", f"{tmp_dir}/b", f"{tmp_dir}/c"])


async def test_batch_delete_removes_all(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    paths = [f"{tmp_dir}/del{i}" for i in range(3)]
    await async_fs.batch_create_file(paths)
    await async_fs.batch_delete(paths)
    assert await async_fs.batch_exists(paths) == [False, False, False]


async def test_batch_delete_recursive(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    parent = f"{tmp_dir}/tree"
    await async_fs.mkdir(f"{parent}/sub")
    await async_fs.write_file(f"{parent}/root.txt", b"x")
    await async_fs.write_file(f"{parent}/sub/child.txt", b"x")
    # Non-recursive delete of a non-empty dir would fail.
    await async_fs.batch_delete([parent], recursive=True)
    assert await async_fs.exists(parent) is False


async def test_batch_delete_unchecked_missing_path(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    # unchecked=True makes missing paths a no-op instead of NotFound.
    await async_fs.batch_delete([f"{tmp_dir}/never-existed"], unchecked=True)
    # No exception raised — test passes.
