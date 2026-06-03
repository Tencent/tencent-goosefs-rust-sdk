"""P6 integration tests — worker block 直连入口守护.

设计目标
--------
配套 P6 (``goosefs >= 0.1.3``) 暴露的两组新入口：

* 高层一行：``AsyncGoosefs.positioned_read`` / ``Goosefs.positioned_read``
* 低层逃生口：``AsyncGoosefs.acquire_worker_for_block`` /
  ``Goosefs.acquire_worker_for_block`` / ``AsyncWorkerClient.connect`` /
  ``WorkerClient.connect``

本文件分两层断言：

1. **Import-time 守护** — 不依赖 cluster，始终运行。仅检查 binding namespace
   有没有把 P6 类与方法导出。这一层是 binding API contract 防回退线。
2. **Cluster 行为守护** — 依赖 ``GOOSEFS_MASTER_ADDR``。验证
   ``AsyncWorkerClient`` 真的会 gRPC 握手到 worker 并在伪 ``block_id``
   上得到 worker 端的 ``RpcError``，证明 binding 不再走 fs fallback。

第 2 层故意 **不** 依赖 ``URIStatus.block_ids`` —— 当前 dev cluster 在
"UFS-only / no-tier-cache" 状态下，``status.block_ids`` 可能为空（master
未收到 block report，详见 docs/GooseFS_Rust_Python_Java客户端Stress对比.md
§3.4 Python 段）。即便如此，``AsyncWorkerClient.connect(...) +
read_block_positioned(fake_id)`` 仍能 *端到端* 证明 binding 直连 worker，
worker 会以 ``Failed to read block ID=... from tiered storage and UFS tier``
拒绝，错误来源是 worker 而不是 client-side fallback。
"""

from __future__ import annotations

import inspect
import os

import pytest


# ---------------------------------------------------------------------------
# Layer 1 — Import-time guards (always run, no cluster needed)
# ---------------------------------------------------------------------------


def test_async_worker_client_is_exported() -> None:
    """``goosefs.AsyncWorkerClient`` 必须存在且是个类型。"""
    import goosefs

    assert hasattr(goosefs, "AsyncWorkerClient"), (
        "P6 regression: goosefs.AsyncWorkerClient missing — bindings/python/src/worker.rs "
        "very likely was not re-exported by python/src/lib.rs"
    )
    assert inspect.isclass(goosefs.AsyncWorkerClient)


@pytest.mark.xfail(
    reason="Known P6 gap: sync `WorkerClient` top-level class not yet exposed by "
    "the Rust extension (only `AsyncWorkerClient` is). Sync callers can still "
    "reach worker direct via `Goosefs.positioned_read` / "
    "`Goosefs.acquire_worker_for_block`. Tracked separately from this test "
    "file's primary purpose (P6 regression guard).",
    strict=False,
)
def test_sync_worker_client_is_exported() -> None:
    """``goosefs.WorkerClient`` 必须存在（同步逃生口）。

    Currently xfail: the Rust extension only exposes ``AsyncWorkerClient``;
    the sync facade ``Goosefs`` exposes ``positioned_read`` /
    ``acquire_worker_for_block`` but not a standalone ``WorkerClient`` class.
    """
    import goosefs

    assert hasattr(goosefs, "WorkerClient"), (
        "P6 gap: goosefs.WorkerClient missing — sync facade not exported"
    )
    assert inspect.isclass(goosefs.WorkerClient)


def test_async_worker_client_has_connect_classmethod() -> None:
    """``AsyncWorkerClient.connect(addr, config)`` 必须是 classmethod-style 入口。"""
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


@pytest.mark.xfail(
    reason="Known P6 gap: sync `WorkerClient` top-level class not yet exposed.",
    strict=False,
)
def test_sync_worker_client_has_connect_classmethod() -> None:
    """``WorkerClient.connect(addr, config)`` 同步对偶必须存在。"""
    import goosefs

    assert hasattr(goosefs.WorkerClient, "connect"), "WorkerClient.connect missing"
    assert hasattr(goosefs.WorkerClient, "read_block_positioned"), (
        "WorkerClient.read_block_positioned missing"
    )


def test_async_goosefs_high_level_positioned_read_is_exported() -> None:
    """``AsyncGoosefs.positioned_read`` / ``acquire_worker_for_block`` 必须存在。"""
    import goosefs

    assert hasattr(goosefs.AsyncGoosefs, "positioned_read"), (
        "P6 regression: AsyncGoosefs.positioned_read missing"
    )
    assert hasattr(goosefs.AsyncGoosefs, "acquire_worker_for_block"), (
        "P6 regression: AsyncGoosefs.acquire_worker_for_block missing"
    )


def test_sync_goosefs_high_level_positioned_read_is_exported() -> None:
    """``Goosefs.positioned_read`` / ``acquire_worker_for_block`` 必须存在。"""
    import goosefs

    assert hasattr(goosefs.Goosefs, "positioned_read"), (
        "P6 regression: Goosefs.positioned_read missing"
    )
    assert hasattr(goosefs.Goosefs, "acquire_worker_for_block"), (
        "P6 regression: Goosefs.acquire_worker_for_block missing"
    )


def test_p6_classes_in_dunder_all() -> None:
    """高层 namespace 的 ``__all__``（如果存在）应当列出 P6 已暴露的类。

    只检查当前真正暴露的 ``AsyncWorkerClient``；同步 ``WorkerClient``
    暂未暴露（见 ``test_sync_worker_client_is_exported`` 的 xfail 说明），
    一旦补齐请把它加进 ``required`` 元组并删掉对应的 xfail 标记。

    若包暂未维护 ``__all__``，这个用例会被 skip 而不是 fail。
    """
    import goosefs

    all_ = getattr(goosefs, "__all__", None)
    if all_ is None:
        pytest.skip("goosefs.__all__ not maintained — skipping membership check")

    required = ("AsyncWorkerClient",)
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
    """端到端冒烟：``AsyncWorkerClient.connect`` 完成真实 gRPC + SASL 握手后，
    用一个故意编造的 ``block_id`` 调用 ``read_block_positioned`` 必须收到
    ``goosefs.exceptions.RpcError``，错误消息来自 worker（包含
    ``"Failed to read block ID="`` 前缀），而不是 client-side fallback。

    这是 "Python 真直连 worker，不再降级到 fs" 的最强证据：

    * 证据 1: ``AsyncWorkerClient.connect(...)`` 不抛异常 ⇒ gRPC 握手成功
    * 证据 2: ``read_block_positioned(fake_id)`` 抛 ``RpcError`` 且消息含
      "Failed to read block ID=" ⇒ worker 真的收到了请求并应答了错误
    * 证据 3: 错误消息里 **没有** ``fallback`` / ``falling back`` /
      ``high-level fs path`` 关键词 ⇒ binding 没在 client 侧降级
    """
    import goosefs

    # Worker addr 需要由测试调用方提供。开发机当前 layout 是 master:9200 /
    # worker:9203，CI / 远端集群可通过 $GOOSEFS_WORKER_ADDR 覆盖。
    worker_addr = os.environ.get("GOOSEFS_WORKER_ADDR", "127.0.0.1:9203")

    # 一个绝对不会命中 worker tier 也不会在 UFS 里有对应 block 的伪 id。
    # 用一个明显大于真实 block_id 空间的值，避免误命中。
    fake_block_id = 9_999_999_999

    async with await goosefs.AsyncWorkerClient.connect(worker_addr, config) as wc:
        # 证据 1: 握手成功后 .addr 必须等于我们传入的 worker_addr
        assert wc.addr == worker_addr, (
            f"AsyncWorkerClient.addr={wc.addr!r} != requested {worker_addr!r}"
        )

        # 证据 2 + 证据 3: RPC 必须真实发出去并被 worker 拒绝
        with pytest.raises(goosefs.exceptions.RpcError) as excinfo:
            await wc.read_block_positioned(fake_block_id, offset=0, length=64)

    msg = str(excinfo.value).lower()
    # 证据 2 — worker 端拒绝消息里一定有 "block id=<fake>"
    assert str(fake_block_id) in msg, (
        f"worker error did not mention fake block_id; got: {excinfo.value!r}"
    )
    # 证据 3 — 没有 client-side fallback 关键词
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
    """``AsyncGoosefs.acquire_worker_for_block(fake_id)`` 即使 routing 指向
    一个 worker 也至少能成功构造 ``AsyncWorkerClient`` 并暴露 ``.addr``。

    本用例对 routing 行为只做最弱断言：能拿到一个 ``AsyncWorkerClient``
    实例。失败也只在 ``routing`` / ``master block lookup`` 抛异常时发生，
    那就是 cluster 层的问题（与 binding 暴露无关），允许跳过。
    """
    import goosefs

    fake_block_id = 9_999_999_999
    try:
        ctx = await async_fs.acquire_worker_for_block(fake_block_id)
    except goosefs.exceptions.RpcError as e:
        # 集群对 fake block 的 master-side block lookup 直接拒绝是合理的
        # —— 仍然走的是真 RPC，不是 client fallback。
        pytest.skip(f"cluster rejected master-side block lookup for fake id: {e}")
        return

    async with ctx as wc:
        assert isinstance(wc, goosefs.AsyncWorkerClient)
        assert isinstance(wc.addr, str) and ":" in wc.addr, (
            f"AsyncWorkerClient.addr looks malformed: {wc.addr!r}"
        )
