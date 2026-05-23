"""Tests for the atexit safety-net (Review §17.4).

These tests verify that:

1. ``Goosefs(cfg)`` and ``AsyncGoosefs.connect(cfg)`` register the resulting
   instance into the module-level ``WeakSet``.
2. The WeakSet drops the entry once the user releases their last reference
   (no leaks).
3. The atexit hook calls ``close()`` on synchronous handles and emits a
   ``ResourceWarning`` for async handles (verified via subprocess so we can
   observe interpreter-shutdown behaviour without contaminating the pytest
   process).
"""

from __future__ import annotations

import gc
import os
import subprocess
import sys
import textwrap

import goosefs
import pytest
from goosefs import AsyncGoosefs, Config, Goosefs

# Reuse the conftest convention: ``GOOSEFS_MASTER_ADDR`` env var gates the
# whole tests/ tree (see conftest.py). When unset, conftest skips at
# collection time, so by the time these tests run the value is guaranteed
# present.
MASTER_ADDR = os.environ.get("GOOSEFS_MASTER_ADDR", "")


# ─────────────────────────────────────────────────────────────────────────────
# WeakSet registration / liveness
# ─────────────────────────────────────────────────────────────────────────────


def test_sync_handle_is_registered_on_construction(sync_fs: Goosefs) -> None:
    """``Goosefs(cfg)`` must add ``self`` to ``_active_handles``."""
    # Note: the ``sync_fs`` fixture itself constructed a Goosefs, so it must
    # already be in the WeakSet.
    assert sync_fs in goosefs._active_handles


def test_sync_handle_is_dropped_when_user_releases_it() -> None:
    """The WeakSet must not hold references — letting it leak handles."""
    fs = Goosefs(Config(MASTER_ADDR))
    assert fs in goosefs._active_handles
    # Capture a weakref so we can observe collection.
    import weakref

    ref = weakref.ref(fs)

    fs.close()
    del fs
    gc.collect()

    # After GC, the ref should have died and the WeakSet should be free of it.
    assert ref() is None
    # No way to assert "the WeakSet shrunk" without races, but iterating
    # should at least not crash.
    list(goosefs._active_handles)


@pytest.mark.asyncio
async def test_async_handle_is_registered_on_connect() -> None:
    """``AsyncGoosefs.connect(cfg)`` must register the resolved instance."""
    afs = await AsyncGoosefs.connect(Config(MASTER_ADDR))
    try:
        assert afs in goosefs._active_handles
    finally:
        await afs.close()


@pytest.mark.asyncio
async def test_async_connect_preserves_signature() -> None:
    """Wrapping ``connect`` must not break its ``staticmethod`` call style.

    Passing ``cfg`` as the only positional argument must still work — this
    is the exact public usage shown in the README.
    """
    cfg = Config(MASTER_ADDR)
    afs = await AsyncGoosefs.connect(cfg)
    await afs.close()


# ─────────────────────────────────────────────────────────────────────────────
# atexit behaviour (run in a fresh subprocess so we can observe shutdown)
# ─────────────────────────────────────────────────────────────────────────────
#
# We can't drive ``atexit`` from inside the pytest process: pytest installs
# its own handlers, and forcing a shutdown would derail the rest of the
# session. So we spawn a clean Python subprocess, run a small script that
# leaks a handle, and inspect its stderr.


def _run_subprocess(script: str) -> tuple[str, str, int]:
    """Run ``script`` in a fresh interpreter; return (stdout, stderr, rc)."""
    completed = subprocess.run(
        [sys.executable, "-W", "default::ResourceWarning", "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.stdout, completed.stderr, completed.returncode


def test_atexit_closes_unclosed_sync_handle() -> None:
    """Sync handle leaked at script exit must be silently closed by atexit
    (no ResourceWarning, no traceback)."""
    script = textwrap.dedent(
        f"""
        import goosefs
        from goosefs import Config, Goosefs
        fs = Goosefs(Config({MASTER_ADDR!r}))
        print("OPENED", flush=True)
        # Intentionally do NOT close.
        """
    )
    stdout, stderr, rc = _run_subprocess(script)
    assert rc == 0, f"subprocess crashed: rc={rc}\n--- stderr ---\n{stderr}"
    assert "OPENED" in stdout
    # No ResourceWarning for the sync path.
    assert "ResourceWarning" not in stderr, (
        f"unexpected ResourceWarning on sync atexit path:\n{stderr}"
    )
    # And critically: no traceback from the atexit hook itself.
    assert "Traceback" not in stderr, f"atexit hook raised:\n{stderr}"


def test_atexit_warns_for_unclosed_async_handle() -> None:
    """Async handle leaked at script exit must emit ResourceWarning (because
    we can't drive ``await close()`` in atexit)."""
    script = textwrap.dedent(
        f"""
        import asyncio, builtins, goosefs
        from goosefs import Config, AsyncGoosefs

        async def go():
            return await AsyncGoosefs.connect(Config({MASTER_ADDR!r}))

        afs = asyncio.run(go())
        # Pin a strong reference so the WeakSet entry survives to atexit.
        builtins._leaked = afs
        print("OPENED_ASYNC", flush=True)
        """
    )
    stdout, stderr, rc = _run_subprocess(script)
    assert rc == 0, f"subprocess crashed: rc={rc}\n--- stderr ---\n{stderr}"
    assert "OPENED_ASYNC" in stdout
    assert "ResourceWarning" in stderr, (
        f"expected ResourceWarning for unclosed AsyncGoosefs, got:\n{stderr}"
    )
    assert "AsyncGoosefs instance was not closed" in stderr
    # atexit hook must never raise.
    assert "Traceback" not in stderr, f"atexit hook raised:\n{stderr}"


def test_atexit_handles_already_closed_handles() -> None:
    """Calling ``close()`` on an already-closed sync handle from atexit
    must be a silent no-op (the user closed it cleanly themselves)."""
    script = textwrap.dedent(
        f"""
        import goosefs
        from goosefs import Config, Goosefs
        fs = Goosefs(Config({MASTER_ADDR!r}))
        fs.close()  # User closes it cleanly.
        # WeakSet still holds a weak ref until ``fs`` is dropped, which
        # only happens at module teardown — exercising the
        # idempotent-close path of the atexit hook.
        print("CLOSED_CLEANLY", flush=True)
        """
    )
    stdout, stderr, rc = _run_subprocess(script)
    assert rc == 0
    assert "CLOSED_CLEANLY" in stdout
    assert "Traceback" not in stderr
    assert "ResourceWarning" not in stderr


# ─────────────────────────────────────────────────────────────────────────────
# Sanity: the wrapping must not break basic functionality
# ─────────────────────────────────────────────────────────────────────────────


def test_sync_construction_still_works(sync_tmp_dir: str, sync_fs: Goosefs) -> None:
    """A trivial round-trip after constructor wrapping — guards against
    regressions where the wrapper accidentally drops args."""
    path = f"{sync_tmp_dir}/atexit_sanity"
    sync_fs.mkdir(path, recursive=True)
    assert sync_fs.exists(path)


@pytest.mark.asyncio
async def test_async_connect_still_works() -> None:
    """Same sanity check for AsyncGoosefs.connect after staticmethod
    wrapping."""
    afs = await AsyncGoosefs.connect(Config(MASTER_ADDR))
    try:
        # Trivial metadata call; if connect were broken we'd raise here.
        assert await afs.exists("/") is True
    finally:
        await afs.close()
