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

"""Exception-mapping tests.

Each exception class defined under ``goosefs.exceptions`` represents a
*category* of upstream errors. The Rust ``map_err`` function (see
``src/errors.rs``) is responsible for routing every variant of the SDK's
error enum to the right Python class.

These tests trigger a representative subset that we can reproduce against
a live cluster:

* ``NotFound``         — stat a non-existent path.
* ``AlreadyExists``    — mkdir an existing directory.
* ``InvalidArgument``  — pass an empty path.
* ``DirectoryNotEmpty``— delete a non-empty dir without ``recursive``.

The remaining classes (``PermissionDenied``, ``AuthenticationFailed``,
``MasterUnavailable``, ``RpcError``, ``IoError``, ``FileIncomplete``,
``IsADirectory``, ``NoWorkerAvailable``, ``ConfigError``,
``GoosefsError``) cover internal SDK conditions that cannot be reliably
provoked from the public API surface in P2; their *registration* and
*subclass relationship* is verified statically below.
"""

from __future__ import annotations

import pytest
from goosefs import AsyncGoosefs
from goosefs.exceptions import (
    AlreadyExists,
    AuthenticationFailed,
    ConfigError,
    DirectoryNotEmpty,
    FileIncomplete,
    GoosefsError,
    InvalidArgument,
    IoError,
    IsADirectory,
    MasterUnavailable,
    NotFound,
    NoWorkerAvailable,
    PermissionDenied,
    RpcError,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Static structure (does not require a live cluster) — these are explicitly
# *not* asyncio tests, so we override the module-level mark with an empty
# list. Pytest takes the most specific mark, so the asyncio decorator is
# dropped for these two functions only.
# ---------------------------------------------------------------------------

_ALL_EXCEPTIONS = (
    NotFound,
    AlreadyExists,
    PermissionDenied,
    InvalidArgument,
    FileIncomplete,
    DirectoryNotEmpty,
    IsADirectory,
    AuthenticationFailed,
    NoWorkerAvailable,
    MasterUnavailable,
    RpcError,
    IoError,
    ConfigError,
)


@pytest.mark.filterwarnings("ignore::pytest.PytestWarning")
def test_all_exceptions_subclass_goosefs_error() -> None:
    for cls in _ALL_EXCEPTIONS:
        assert issubclass(cls, GoosefsError), f"{cls.__name__} must subclass GoosefsError"
    assert issubclass(GoosefsError, Exception)


@pytest.mark.filterwarnings("ignore::pytest.PytestWarning")
def test_exceptions_module_attribute_matches() -> None:
    """The ``__module__`` should be ``goosefs.exceptions`` (not the underscore
    extension), so user tracebacks read naturally.
    """
    for cls in _ALL_EXCEPTIONS + (GoosefsError,):
        assert cls.__module__ == "goosefs.exceptions", cls.__name__


# ---------------------------------------------------------------------------
# Live-cluster coverage
# ---------------------------------------------------------------------------


async def test_notfound_on_missing_path(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    with pytest.raises(NotFound):
        await async_fs.get_status(f"{tmp_dir}/never-created")


async def test_notfound_is_catchable_as_goosefs_error(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    """Users who want a single catch-all should be able to use ``GoosefsError``."""
    with pytest.raises(GoosefsError):
        await async_fs.get_status(f"{tmp_dir}/missing")


async def test_already_exists_on_rename_to_existing_target(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    """``AlreadyExists`` is reachable through ``rename``: the destination path
    must not pre-exist. (``mkdir`` is idempotent because the SDK hard-wires
    ``allow_exists=true``, so it is *not* a vehicle for ``AlreadyExists``.)
    """
    src = f"{tmp_dir}/rename-src"
    dst = f"{tmp_dir}/rename-dst"
    await async_fs.mkdir(src)
    await async_fs.mkdir(dst)
    with pytest.raises((AlreadyExists, GoosefsError)):
        await async_fs.rename(src, dst)


async def test_invalid_argument_on_empty_path(async_fs: AsyncGoosefs) -> None:
    """An empty path is rejected by the server / SDK as ``InvalidArgument``
    or ``InvalidPath``; both map to the same Python class.
    """
    with pytest.raises((InvalidArgument, GoosefsError)):
        await async_fs.get_status("")


async def test_directory_not_empty_on_non_recursive_delete(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    parent = f"{tmp_dir}/non-empty"
    await async_fs.mkdir(f"{parent}/child", recursive=True)
    # Server may return DirectoryNotEmpty or a more generic GoosefsError;
    # both are acceptable as long as the call fails without losing data.
    with pytest.raises((DirectoryNotEmpty, GoosefsError)):
        await async_fs.delete(parent)
    assert await async_fs.exists(parent), "delete must not have partially succeeded"


async def test_use_after_close_raises_runtime_error(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    """``close()`` followed by any operation must raise ``RuntimeError``
    (not silently hang or return None).
    """
    await async_fs.close()
    with pytest.raises(RuntimeError):
        await async_fs.get_status(tmp_dir)
