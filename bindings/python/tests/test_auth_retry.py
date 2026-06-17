"""Auth-retry regression guard tests for the Python binding.

Purpose
-------
These tests guard against regressions in the SASL auth-failure retry
mechanism that was added as Critical #1 from the code review.

When a cached ``WorkerClient``'s SASL stream expires server-side, the
next RPC fails with ``UNAUTHENTICATED``.  The binding must auto-reconnect
and retry once at **two recovery points**:

1. **Acquire path** — ``pool.acquire()`` fails → ``pool.reconnect()``
2. **Read path** — ``positioned_read()`` fails → ``reconnect_if_stale()``
   → retry read

The core retry logic lives in Rust
(``positioned_read.rs::acquire_with_auth_retry`` /
``read_with_auth_retry``) and is covered by 13 Rust unit tests.
This file provides Python-level guards on two layers:

* **Layer 1 — Import-time guards** (always run, no cluster needed):
  Verify the binding exposes the code paths that exercise auth-retry.
* **Layer 2 — Cluster behavior** (needs ``$GOOSEFS_MASTER_ADDR``):
  End-to-end smoke tests that exercise the ``positioned_read`` pipeline
  which is the entry point for the auth-retry logic.

Triggering a genuine SASL expiry in an integration test would require
controlling the worker's authentication timeout, which is impractical in
CI.  Instead, we verify the *entry points* exist and work correctly under
normal conditions; the actual retry-or-fail logic is unit-tested in Rust.
"""

from __future__ import annotations

import os

import pytest

# ---------------------------------------------------------------------------
# Layer 1 — Import-time guards (always run, no cluster needed)
# ---------------------------------------------------------------------------


def test_async_goosefs_has_positioned_read() -> None:
    """``AsyncGoosefs.positioned_read`` must exist — it is the high-level
    entry point that delegates to ``positioned_read_with_reauth``."""
    import goosefs

    assert hasattr(goosefs.AsyncGoosefs, "positioned_read"), (
        "Auth-retry regression: AsyncGoosefs.positioned_read missing — "
        "the async path no longer exercises the auth-retry pipeline"
    )


def test_sync_goosefs_has_positioned_read() -> None:
    """``Goosefs.positioned_read`` must exist — the sync path shares the
    same ``positioned_read_with_reauth`` function (Critical #1 fix)."""
    import goosefs

    assert hasattr(goosefs.Goosefs, "positioned_read"), (
        "Auth-retry regression: Goosefs.positioned_read missing — "
        "the sync path no longer exercises the auth-retry pipeline"
    )


def test_auth_retry_helpers_are_importable() -> None:
    """The Rust helpers ``acquire_with_auth_retry`` and
    ``read_with_auth_retry`` are not exposed to Python (they are
    ``pub(crate)``), but we can verify the *effect* of the auth-retry
    code by checking that the ``goosefs`` module compiled with the
    positioned_read module.  An import failure here would indicate the
    module was accidentally removed or the build is stale."""
    import goosefs

    # The module should load successfully — any compilation error in
    # positioned_read.rs would prevent the extension from loading.
    assert goosefs.AsyncGoosefs is not None
    assert goosefs.Goosefs is not None


def test_rpc_error_exception_exists() -> None:
    """``goosefs.exceptions.RpcError`` must be available — it is the
    error type raised when the auth-retry path exhausts retries and
    the worker remains unreachable or unauthenticated."""
    import goosefs

    assert hasattr(goosefs.exceptions, "RpcError"), (
        "Auth-retry regression: goosefs.exceptions.RpcError missing — "
        "retry-exhaustion errors cannot be caught"
    )


# ---------------------------------------------------------------------------
# Layer 2 — Cluster behavior (needs $GOOSEFS_MASTER_ADDR)
# ---------------------------------------------------------------------------

_MASTER = os.environ.get("GOOSEFS_MASTER_ADDR")


@pytest.mark.skipif(
    not _MASTER,
    reason="GOOSEFS_MASTER_ADDR not set; skipping cluster-bound auth-retry test",
)
class TestAsyncPositionedReadAuthRetryGuard:
    """End-to-end smoke tests for the async ``positioned_read`` path.

    These tests write a file, then read it back via ``positioned_read``
    to exercise the full pipeline: route → acquire → positioned_read.
    Under normal conditions (no SASL expiry), the retry path is not
    triggered, but the test still validates that the entry point works
    and would reach the retry code if needed.

    To *force* a SASL expiry, you would need to reduce the worker's
    ``goosefs.security.authentication.token.max.lifetime`` and wait for
    the stream to expire — this is a manual test, not automated.
    """

    @pytest.mark.asyncio
    async def test_async_positioned_read_round_trip(self, async_fs, tmp_dir) -> None:
        """Write a small file and read it back via ``positioned_read``.

        Exercises the full pipeline:
          route → acquire(pool) → positioned_read(worker, block_id, ...)

        If auth-retry is broken (e.g. the sync path is missing the retry),
        this test would fail with an ``UNAUTHENTICATED`` error on a
        long-lived cluster where the SASL stream has expired.
        """
        path = f"{tmp_dir}/auth-retry-async.bin"
        payload = b"auth-retry-guard-async" * 100
        await async_fs.write_file(path, payload)

        status = await async_fs.get_status(path)
        assert status.length == len(payload), (
            f"file length mismatch: {status.length} != {len(payload)}"
        )

        # ``positioned_read`` is the entry point for the auth-retry pipeline.
        # Under normal conditions this succeeds on the first try.
        data = await async_fs.positioned_read(path, offset=0, length=len(payload))
        assert data == payload, (
            "positioned_read did not round-trip the payload — auth-retry entry point may be broken"
        )

    @pytest.mark.asyncio
    async def test_async_positioned_read_with_offset(self, async_fs, tmp_dir) -> None:
        """``positioned_read`` with non-zero offset must slice correctly."""
        path = f"{tmp_dir}/auth-retry-async-offset.bin"
        payload = b"0123456789ABCDEF" * 64
        await async_fs.write_file(path, payload)

        # Read from offset 16 with length 32 — should NOT start from 0.
        data = await async_fs.positioned_read(path, offset=16, length=32)
        assert data == payload[16:48], (
            f"positioned_read(offset=16, length=32) returned wrong slice: "
            f"got {data[:16]!r}..., expected {payload[16:32]!r}..."
        )


@pytest.mark.skipif(
    not _MASTER,
    reason="GOOSEFS_MASTER_ADDR not set; skipping cluster-bound auth-retry test",
)
class TestSyncPositionedReadAuthRetryGuard:
    """Sync counterpart of ``TestAsyncPositionedReadAuthRetryGuard``.

    The sync ``Goosefs.positioned_read`` calls the same
    ``positioned_read_with_reauth`` function as the async path, so the
    auth-retry logic is shared.  This test validates that the sync
    binding correctly reaches that shared code path.
    """

    def test_sync_positioned_read_round_trip(self, sync_fs, sync_tmp_dir) -> None:
        """Write a small file and read it back via sync ``positioned_read``.

        This is the Critical #1 regression guard: before the fix, the
        sync path had **no** auth-retry and would fail with
        ``UNAUTHENTICATED`` on a long-lived cluster.
        """
        path = f"{sync_tmp_dir}/auth-retry-sync.bin"
        payload = b"auth-retry-guard-sync" * 100
        sync_fs.write_file(path, payload)

        status = sync_fs.get_status(path)
        assert status.length == len(payload)

        data = sync_fs.positioned_read(path, offset=0, length=len(payload))
        assert data == payload, (
            "sync positioned_read did not round-trip — "
            "auth-retry entry point may be broken (Critical #1 regression)"
        )

    def test_sync_positioned_read_with_offset(self, sync_fs, sync_tmp_dir) -> None:
        """``positioned_read`` with non-zero offset must slice correctly."""
        path = f"{sync_tmp_dir}/auth-retry-sync-offset.bin"
        payload = b"0123456789ABCDEF" * 64
        sync_fs.write_file(path, payload)

        data = sync_fs.positioned_read(path, offset=16, length=32)
        assert data == payload[16:48], (
            "sync positioned_read(offset=16, length=32) returned wrong slice"
        )
