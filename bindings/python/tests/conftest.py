"""Shared pytest fixtures for the GooseFS Python integration tests.

The tests in this directory require a running GooseFS cluster reachable at
``$GOOSEFS_MASTER_ADDR``. If the environment variable is not set, the tests
are skipped at *collection* time so that ``pytest`` still succeeds in CI
environments without a deployed cluster.

To run them locally::

    # In one terminal: bring up master + worker (per AGENTS.md / project memo)
    cd /opt/sourcecode/cos/goosefs
    ./bin/goosefs formatMaster
    ./bin/goosefs-start.sh master
    ./bin/goosefs formatWorker
    ./bin/goosefs-start.sh worker

    # In another terminal:
    cd /opt/sourcecode/cos/goosefs-client-rust/bindings/python
    export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
    uv run pytest -v
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import AsyncIterator

import pytest
import pytest_asyncio

from goosefs import AsyncGooseFs, Config


# ---------------------------------------------------------------------------
# Skip everything when no cluster is configured.
# ---------------------------------------------------------------------------

GOOSEFS_MASTER_ADDR = os.environ.get("GOOSEFS_MASTER_ADDR")

# Collection-time skip: avoid even constructing fixtures when unconfigured.
collect_ignore_glob = (
    [] if GOOSEFS_MASTER_ADDR else ["test_metadata.py", "test_errors.py"]
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
def config(master_addr: str) -> Config:
    """A reusable, immutable :class:`Config` for the whole test session."""
    return Config(master_addr)


@pytest_asyncio.fixture
async def async_fs(config: Config) -> AsyncIterator[AsyncGooseFs]:
    """An :class:`AsyncGooseFs` connected for the duration of the test.

    Each test gets its own connection so a hung close in one test cannot
    poison the next.
    """
    fs = await AsyncGooseFs.connect(config)
    try:
        yield fs
    finally:
        # `close()` is idempotent — double-close is fine if the test also
        # closed manually.
        await fs.close()


@pytest_asyncio.fixture
async def tmp_dir(async_fs: AsyncGooseFs) -> AsyncIterator[str]:
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
# Asyncio loop policy
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    """Use the default policy; tests are explicitly ``asyncio_mode = auto``
    via ``pyproject.toml`` so individual tests do not need decorators."""
    return asyncio.DefaultEventLoopPolicy()
