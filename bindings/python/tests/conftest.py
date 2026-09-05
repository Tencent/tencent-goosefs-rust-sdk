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

"""Shared pytest fixtures for the GooseFS Python integration tests.

The tests in this directory require a running GooseFS cluster reachable at
``$GOOSEFS_MASTER_ADDR``. If the environment variable is not set, the tests
are skipped at *collection* time so that ``pytest`` still succeeds in CI
environments without a deployed cluster.

To run them locally::

    # Terminal 1: start the Docker fixture from the repo root. Pass
    # GOOSEFS_WORKER_BLOCK_STORE_TYPE=PAGE to boot the paged block store
    # instead of the default FILE one.
    bash scripts/ci/goosefs-up.sh

    # Terminal 2:
    cd bindings/python
    export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
    export GOOSEFS_AUTH_TYPE=simple
    export GOOSEFS_WORKER_BLOCK_STORE_TYPE=FILE   # or PAGE, must match the cluster
    uv run pytest -v
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from goosefs import AsyncGoosefs, Config, Goosefs

# ---------------------------------------------------------------------------
# Skip everything when no cluster is configured.
# ---------------------------------------------------------------------------

GOOSEFS_MASTER_ADDR = os.environ.get("GOOSEFS_MASTER_ADDR")

# Which block store the worker under test runs: ``FILE`` or ``PAGE``. ``FILE`` is
# both GooseFS's own default for ``goosefs.worker.block.store.type`` and the
# Docker fixture's, so an unset variable describes the cluster accurately rather
# than merely guessing at it.
WORKER_BLOCK_STORE_TYPE = os.environ.get("GOOSEFS_WORKER_BLOCK_STORE_TYPE", "FILE").strip().upper()

# Collection-time skip: avoid even constructing fixtures when unconfigured.
collect_ignore_glob = (
    []
    if GOOSEFS_MASTER_ADDR
    else [
        "test_metadata.py",
        "test_errors.py",
        "test_sync.py",
        "test_read_write.py",
        "test_streaming_async.py",
        "test_streaming_sync.py",
        "test_atexit.py",
        "test_page_cache.py",
        "test_write_degrade.py",
        "test_block_boundary_write_types.py",
    ]
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def master_addr() -> str:
    """The cluster address under test. Skip the session if unset."""
    if not GOOSEFS_MASTER_ADDR:
        pytest.skip("GOOSEFS_MASTER_ADDR is not set; skipping integration tests")
    return GOOSEFS_MASTER_ADDR


@pytest.fixture(scope="session")
def worker_block_store_type() -> str:
    """Which block store the worker under test runs: ``"FILE"`` or ``"PAGE"``.

    Write behaviour genuinely differs between the two, so a test that straddles
    a block boundary cannot state an expectation without knowing the mode:
    PAGE's ``PagedBlockWriter.flush()`` throws ``UnsupportedOperationException``,
    and the SDK sends ``flush:true`` on a mid-file block switch because Java
    ``GooseFSFileOutStream.getNextBlock()`` does.

    Read from ``$GOOSEFS_WORKER_BLOCK_STORE_TYPE``, which both the Docker
    fixture and :file:`scripts/ci/goosefs-up.sh` export. It is a declaration
    about the cluster rather than something probed from it — probing would mean
    performing the very write these tests exist to pin down, which would make
    them agree with whatever the worker happens to do.
    """
    if WORKER_BLOCK_STORE_TYPE not in ("FILE", "PAGE"):
        pytest.fail(
            f"GOOSEFS_WORKER_BLOCK_STORE_TYPE must be FILE or PAGE, got {WORKER_BLOCK_STORE_TYPE!r}"
        )
    return WORKER_BLOCK_STORE_TYPE


@pytest.fixture(scope="session")
def config(master_addr: str) -> Config:
    """A reusable, immutable :class:`Config` for the whole test session.

    The Docker fixture is a single-worker cluster. Java-aligned
    ``ASYNC_THROUGH`` defaults (``replication.durable`` /
    ``replication.durable.min`` = 2) cannot be satisfied with one
    worker, so this suite pins both to 1.
    """
    return Config(
        master_addr,
        properties={
            "goosefs.user.file.replication.durable": "1",
            "goosefs.user.file.replication.durable.min": "1",
        },
    )


@pytest_asyncio.fixture
async def async_fs(config: Config) -> AsyncIterator[AsyncGoosefs]:
    """An :class:`AsyncGoosefs` connected for the duration of the test.

    Each test gets its own connection so a hung close in one test cannot
    poison the next.
    """
    fs = await AsyncGoosefs.connect(config)
    try:
        yield fs
    finally:
        # `close()` is idempotent — double-close is fine if the test also
        # closed manually.
        await fs.close()


@pytest_asyncio.fixture
async def tmp_dir(async_fs: AsyncGoosefs) -> AsyncIterator[str]:
    """A unique scratch directory under ``/tmp/pygoosefs-tests/``.

    Cleaned up recursively at teardown. The path is safe to use even if a
    previous test crashed — ``uuid4`` ensures no collisions.
    """
    base = "/tmp/pygoosefs-tests"
    # Best-effort root creation — ignore AlreadyExists.
    try:
        await async_fs.mkdir(base, recursive=True)
    except Exception:  # noqa: BLE001 — first-run vs subsequent-run races
        pass

    name = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    path = f"{base}/{name}"
    await async_fs.mkdir(path, recursive=True)

    try:
        yield path
    finally:
        # Recursive cleanup; tolerate already-deleted paths from the test
        # itself (e.g. a test that exercises `delete`).
        try:
            await async_fs.delete(path, recursive=True)
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# Sync fixtures (for ``test_sync.py``)
# ---------------------------------------------------------------------------


@pytest.fixture
def sync_fs(config: Config):
    """A blocking :class:`Goosefs` connected for the duration of the test.

    Constructed inside the test thread (no asyncio loop running) so the
    runtime guards in ``Goosefs`` accept the call. ``close()`` runs in a
    ``try/finally`` to release the connection even on test failure.
    """
    fs = Goosefs(config)
    try:
        yield fs
    finally:
        # ``close()`` is idempotent.
        fs.close()


@pytest.fixture
def sync_tmp_dir(sync_fs: Goosefs):
    """Sync analogue of :func:`tmp_dir`. Creates and recursively cleans up
    a uuid-stamped scratch directory.
    """
    base = "/tmp/pygoosefs-tests"
    try:
        sync_fs.mkdir(base, recursive=True)
    except Exception:  # noqa: BLE001
        pass

    name = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    path = f"{base}/{name}"
    sync_fs.mkdir(path, recursive=True)
    try:
        yield path
    finally:
        try:
            sync_fs.delete(path, recursive=True)
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# Asyncio loop policy
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    """Use the default policy; tests are explicitly ``asyncio_mode = auto``
    via ``pyproject.toml`` so individual tests do not need decorators."""
    return asyncio.DefaultEventLoopPolicy()
