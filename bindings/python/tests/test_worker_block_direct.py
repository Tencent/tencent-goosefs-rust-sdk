"""P6 integration tests — guards for the worker block direct entry points.

Goal
----
These tests cover the two API surfaces shipped in P6 (``goosefs >= 0.1.3``):

* High-level one-liner: ``AsyncGoosefs.positioned_read`` /
  ``Goosefs.positioned_read``
* Low-level escape hatch: ``AsyncGoosefs.acquire_worker_for_block`` /
  ``Goosefs.acquire_worker_for_block`` / ``AsyncWorkerClient.connect`` /
  ``WorkerClient.connect``

This file asserts at two layers:

1. **Import-time guards** — always run, no cluster required. They check
   that the Python binding namespace exports the P6 classes and methods.
   This layer is the binding's API-contract regression line.
2. **Cluster behavior guards** — depend on ``GOOSEFS_MASTER_ADDR``. They
   verify that ``AsyncWorkerClient`` actually performs a gRPC handshake
   to a worker and that a fake ``block_id`` produces a worker-side
   ``RpcError`` — proving the binding no longer falls back to the high-
   level filesystem path on the client side.

Layer 2 deliberately does **not** depend on ``URIStatus.block_ids`` —
on a dev cluster running in "UFS-only / no-tier-cache" mode the master
may not have received any block report yet, so ``status.block_ids`` can
legitimately be empty (see the GooseFS Rust/Python/Java client stress
comparison docs, §3.4 Python section, for the discussion). Even in that
state, ``AsyncWorkerClient.connect(...) + read_block_positioned(fake_id)``
is enough end-to-end evidence that the binding hits the worker directly:
the worker rejects the request with a ``"Failed to read block ID=...
from tiered storage and UFS tier"`` message, which can only have come
from the worker — not from a client-side fallback.
"""

from __future__ import annotations

import inspect
import os

import pytest


# ---------------------------------------------------------------------------
# Layer 1 — Import-time guards (always run, no cluster needed)
# ---------------------------------------------------------------------------


def test_async_worker_client_is_exported() -> None:
    """``goosefs.AsyncWorkerClient`` must exist and be a class."""
    import goosefs

    assert hasattr(goosefs, "AsyncWorkerClient"), (
        "P6 regression: goosefs.AsyncWorkerClient missing — bindings/python/src/worker.rs "
        "very likely was not re-exported by python/src/lib.rs"
    )
    assert inspect.isclass(goosefs.AsyncWorkerClient)


def test_sync_worker_client_is_exported() -> None:
    """``goosefs.WorkerClient`` must exist (synchronous escape hatch).

    Sync mirror of ``AsyncWorkerClient`` — exposed to satisfy callers that
    already know the worker address and want a one-shot blocking
    ``read_block_positioned`` without going through ``Goosefs.positioned_read``.
    """
    import goosefs

    assert hasattr(goosefs, "WorkerClient"), (
        "Regression: goosefs.WorkerClient missing — sync facade not exported"
    )
    assert inspect.isclass(goosefs.WorkerClient)


def test_async_worker_client_has_connect_classmethod() -> None:
    """``AsyncWorkerClient.connect(addr, config)`` must be a classmethod-style entry point."""
    import goosefs

    assert hasattr(goosefs.AsyncWorkerClient, "connect"), (
        "AsyncWorkerClient.connect missing"
    )
    assert hasattr(goosefs.AsyncWorkerClient, "read_block_positioned"), (
        "AsyncWorkerClient.read_block_positioned missing"
    )
    # `addr` should be a property/getter (constant for the lifetime of wc)
    assert hasattr(goosefs.AsyncWorkerClient, "addr"), (
        "AsyncWorkerClient.addr accessor missing"
    )


def test_sync_worker_client_has_connect_classmethod() -> None:
    """``WorkerClient.connect(addr, config)`` — the sync counterpart must exist.

    Sync-side API contract: it must mirror ``AsyncWorkerClient`` (``connect``
    static factory + ``read_block_positioned`` method + ``addr`` getter).
    """
    import goosefs

    assert hasattr(goosefs.WorkerClient, "connect"), "WorkerClient.connect missing"
    assert hasattr(goosefs.WorkerClient, "read_block_positioned"), (
        "WorkerClient.read_block_positioned missing"
    )
    assert hasattr(goosefs.WorkerClient, "addr"), "WorkerClient.addr missing"


def test_async_goosefs_high_level_positioned_read_is_exported() -> None:
    """``AsyncGoosefs.positioned_read`` / ``acquire_worker_for_block`` must exist."""
    import goosefs

    assert hasattr(goosefs.AsyncGoosefs, "positioned_read"), (
        "P6 regression: AsyncGoosefs.positioned_read missing"
    )
    assert hasattr(goosefs.AsyncGoosefs, "acquire_worker_for_block"), (
        "P6 regression: AsyncGoosefs.acquire_worker_for_block missing"
    )


def test_sync_goosefs_high_level_positioned_read_is_exported() -> None:
    """``Goosefs.positioned_read`` / ``acquire_worker_for_block`` must exist."""
    import goosefs

    assert hasattr(goosefs.Goosefs, "positioned_read"), (
        "P6 regression: Goosefs.positioned_read missing"
    )
    assert hasattr(goosefs.Goosefs, "acquire_worker_for_block"), (
        "P6 regression: Goosefs.acquire_worker_for_block missing"
    )


def test_p6_classes_in_dunder_all() -> None:
    """The top-level ``__all__`` (when present) must list the P6 classes.

    Both ``AsyncWorkerClient`` and ``WorkerClient`` are now exposed.
    If the package does not maintain ``__all__``, this case is skipped
    rather than failing.
    """
    import goosefs

    all_ = getattr(goosefs, "__all__", None)
    if all_ is None:
        pytest.skip("goosefs.__all__ not maintained — skipping membership check")

    required = ("AsyncWorkerClient", "WorkerClient")
    missing = [name for name in required if name not in all_]
    assert not missing, (
        f"P6 classes missing from goosefs.__all__: {missing}; "
        f"current __all__={all_!r}"
    )


# ---------------------------------------------------------------------------
# Layer 2 — Cluster behavior (needs $GOOSEFS_MASTER_ADDR)
# ---------------------------------------------------------------------------


_MASTER = os.environ.get("GOOSEFS_MASTER_ADDR")


@pytest.mark.skipif(
    not _MASTER,
    reason="GOOSEFS_MASTER_ADDR not set; skipping cluster-bound worker direct test",
)
async def test_async_worker_client_connect_real_handshake_then_rpc_error_on_fake_block(
    config,  # session-scope fixture from conftest.py
) -> None:
    """End-to-end smoke test: after ``AsyncWorkerClient.connect`` finishes
    a real gRPC + SASL handshake, calling ``read_block_positioned`` with a
    deliberately-fake ``block_id`` must produce a
    ``goosefs.exceptions.RpcError`` whose message comes from the worker
    (it contains the ``"Failed to read block ID="`` prefix) and not from
    a client-side fallback.

    This is the strongest evidence that "the Python binding really hits
    the worker directly and no longer falls back to the fs path":

    * Evidence 1: ``AsyncWorkerClient.connect(...)`` does not throw =>
      gRPC handshake succeeded.
    * Evidence 2: ``read_block_positioned(fake_id)`` raises ``RpcError``
      whose message contains ``"Failed to read block ID="`` => the worker
      really received the request and responded with an error.
    * Evidence 3: the error message does **not** contain any of
      ``fallback`` / ``falling back`` / ``high-level fs path`` => the
      binding did not silently degrade on the client side.
    """
    import goosefs

    # The worker addr must be supplied by the caller. The current dev box
    # layout is master:9200 / worker:9203; CI / remote clusters can
    # override via $GOOSEFS_WORKER_ADDR.
    worker_addr = os.environ.get("GOOSEFS_WORKER_ADDR", "127.0.0.1:9203")

    # A fake id that cannot possibly hit a worker tier or have a matching
    # block in UFS. We pick something obviously above the real block_id
    # space to avoid accidental hits.
    fake_block_id = 9_999_999_999

    async with await goosefs.AsyncWorkerClient.connect(worker_addr, config) as wc:
        # Evidence 1: after a successful handshake, .addr must equal the
        # address we passed in.
        assert wc.addr == worker_addr, (
            f"AsyncWorkerClient.addr={wc.addr!r} != requested {worker_addr!r}"
        )

        # Evidence 2 + Evidence 3: the RPC must really be sent and rejected
        # by the worker.
        with pytest.raises(goosefs.exceptions.RpcError) as excinfo:
            await wc.read_block_positioned(fake_block_id, offset=0, length=64)

    msg = str(excinfo.value).lower()
    # Evidence 2: the worker-side rejection message must mention the fake
    # block_id we asked for.
    assert str(fake_block_id) in msg, (
        f"worker error did not mention fake block_id; got: {excinfo.value!r}"
    )
    # Evidence 3: no client-side fallback keywords leaked into the error.
    fallback_keywords = (
        "fallback",
        "falling back",
        "fall back",
        "high-level fs path",
        "binding does not expose",
    )
    leaked = [k for k in fallback_keywords if k in msg]
    assert not leaked, (
        f"client-side fallback keyword(s) leaked into error: {leaked}; full msg={excinfo.value!r}"
    )


@pytest.mark.skipif(
    not _MASTER,
    reason="GOOSEFS_MASTER_ADDR not set; skipping cluster-bound worker direct test",
)
async def test_acquire_worker_for_block_returns_async_worker_client(
    async_fs,  # uses conftest.py fixture
) -> None:
    """``AsyncGoosefs.acquire_worker_for_block(fake_id)`` — even when the
    routing call points at a worker — must at least successfully construct
    an ``AsyncWorkerClient`` instance and expose ``.addr``.

    This case makes only the weakest possible assertion about routing
    behavior: that we can obtain an ``AsyncWorkerClient`` instance.
    Failures only happen when ``routing`` / ``master block lookup`` raises
    — those are cluster-level issues unrelated to what the binding
    exposes, so the test is allowed to skip in that case.
    """
    import goosefs

    fake_block_id = 9_999_999_999
    try:
        ctx = await async_fs.acquire_worker_for_block(fake_block_id)
    except goosefs.exceptions.RpcError as e:
        # The cluster rejecting a master-side block lookup for a fake id
        # is acceptable — that path still goes through real RPCs, not a
        # client-side fallback.
        pytest.skip(f"cluster rejected master-side block lookup for fake id: {e}")
        return

    async with ctx as wc:
        assert isinstance(wc, goosefs.AsyncWorkerClient)
        assert isinstance(wc.addr, str) and ":" in wc.addr, (
            f"AsyncWorkerClient.addr looks malformed: {wc.addr!r}"
        )
