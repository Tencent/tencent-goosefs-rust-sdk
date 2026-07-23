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

"""Integration tests for the lazy ``list_status_grouped`` /
``batch_list_status_grouped`` APIs.

These mirror the eager ``list_status`` / ``batch_list_status`` coverage
but exercise the lazy ``URIStatusList`` return type: ``len()`` is O(1)
with zero object creation, and ``entries[i]`` materialises one
``URIStatus`` on demand.
"""

from __future__ import annotations

import pytest
from goosefs import AsyncGoosefs, Goosefs, URIStatus, URIStatusList

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# list_status_grouped (single path, async)
# ---------------------------------------------------------------------------


async def test_list_status_grouped_returns_uri_status_list(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    # Create a few entries under tmp_dir.
    for name in ("a", "b", "c"):
        await async_fs.write_file(f"{tmp_dir}/{name}", b"x")

    grouped = await async_fs.list_status_grouped(tmp_dir)
    assert isinstance(grouped, URIStatusList)
    assert len(grouped) == 3


async def test_list_status_grouped_indexing_materialises_uristatus(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    await async_fs.write_file(f"{tmp_dir}/file-0", b"hello")
    grouped = await async_fs.list_status_grouped(tmp_dir)

    # Positive indexing.
    first = grouped[0]
    assert isinstance(first, URIStatus)
    assert first.name == "file-0"

    # Negative indexing.
    last = grouped[-1]
    assert isinstance(last, URIStatus)
    assert last.name == "file-0"


async def test_list_status_grouped_out_of_range_raises(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    await async_fs.write_file(f"{tmp_dir}/only", b"x")
    grouped = await async_fs.list_status_grouped(tmp_dir)
    with pytest.raises(IndexError):
        _ = grouped[5]
    with pytest.raises(IndexError):
        _ = grouped[-10]


async def test_list_status_grouped_iter_full_and_empty(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    # Empty list.
    empty = await async_fs.list_status_grouped(tmp_dir)
    assert list(empty) == []

    # Non-empty list.
    for name in ("a", "b", "c"):
        await async_fs.write_file(f"{tmp_dir}/{name}", b"x")
    grouped = await async_fs.list_status_grouped(tmp_dir)
    names = [s.name for s in grouped]
    assert names == ["a", "b", "c"]


async def test_list_status_grouped_recursive(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    await async_fs.mkdir(f"{tmp_dir}/sub")
    await async_fs.write_file(f"{tmp_dir}/sub/child", b"x")
    grouped = await async_fs.list_status_grouped(tmp_dir, recursive=True)
    # listStatus recursive returns descendants (sub/ + sub/child), not the
    # directory itself, so we expect at least 2 entries.
    assert len(grouped) >= 2


async def test_list_status_grouped_bool(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    empty = await async_fs.list_status_grouped(tmp_dir)
    assert not empty

    await async_fs.write_file(f"{tmp_dir}/x", b"x")
    non_empty = await async_fs.list_status_grouped(tmp_dir)
    assert non_empty


# ---------------------------------------------------------------------------
# batch_list_status_grouped (multiple dirs, async)
# ---------------------------------------------------------------------------


async def test_batch_list_status_grouped_returns_list_in_order(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    dirs = [f"{tmp_dir}/d{i}" for i in range(3)]
    await async_fs.batch_create_dir(dirs)
    # Put a different number of files in each dir.
    for i, d in enumerate(dirs):
        for j in range(i + 1):
            await async_fs.write_file(f"{d}/file-{j}", b"x")

    groups = await async_fs.batch_list_status_grouped(dirs, recursive=False)
    assert len(groups) == 3
    for i, g in enumerate(groups):
        assert isinstance(g, URIStatusList)
        assert len(g) == i + 1, f"dir {i} should have {i + 1} entries"


async def test_batch_list_status_grouped_empty_dirs(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    dirs = [f"{tmp_dir}/empty-{i}" for i in range(3)]
    await async_fs.batch_create_dir(dirs)
    groups = await async_fs.batch_list_status_grouped(dirs, recursive=False)
    assert len(groups) == 3
    assert all(len(g) == 0 for g in groups)
    assert all(not g for g in groups)


async def test_batch_list_status_grouped_recursive(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    parent = f"{tmp_dir}/parent"
    await async_fs.mkdir(parent)
    await async_fs.mkdir(f"{parent}/sub")
    await async_fs.write_file(f"{parent}/root.txt", b"x")
    await async_fs.write_file(f"{parent}/sub/child.txt", b"x")

    groups = await async_fs.batch_list_status_grouped([parent], recursive=True)
    assert len(groups) == 1
    # parent's descendants: sub/ + root.txt + sub/child.txt = 3 entries.
    assert len(groups[0]) >= 3


# ---------------------------------------------------------------------------
# Sync wrapper coverage
# ---------------------------------------------------------------------------


def test_sync_list_status_grouped(sync_fs: Goosefs, sync_tmp_dir: str) -> None:
    for name in ("a", "b"):
        sync_fs.write_file(f"{sync_tmp_dir}/{name}", b"x")
    grouped = sync_fs.list_status_grouped(sync_tmp_dir)
    assert isinstance(grouped, URIStatusList)
    assert len(grouped) == 2
    assert isinstance(grouped[0], URIStatus)


def test_sync_batch_list_status_grouped(sync_fs: Goosefs, sync_tmp_dir: str) -> None:
    dirs = [f"{sync_tmp_dir}/sd{i}" for i in range(2)]
    sync_fs.batch_create_dir(dirs)
    sync_fs.write_file(f"{dirs[0]}/f", b"x")
    groups = sync_fs.batch_list_status_grouped(dirs, recursive=False)
    assert len(groups) == 2
    assert len(groups[0]) == 1
    assert len(groups[1]) == 0
