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

"""GooseFS Python client (Rust-powered).

Quick start::

    import asyncio
    from goosefs import AsyncGoosefs, Config, WriteType

    async def main():
        cfg = Config("127.0.0.1:9200")
        async with await AsyncGoosefs.connect(cfg) as fs:
            await fs.mkdir("/tmp/p2", recursive=True)
            status = await fs.get_status("/tmp/p2")
            print(status)

    asyncio.run(main())

For synchronous (blocking) workflows, use ``Goosefs``::

    from goosefs import Goosefs, Config

    with Goosefs(Config("127.0.0.1:9200")) as fs:
        fs.mkdir("/tmp/p3", recursive=True)
        assert fs.exists("/tmp/p3")

Worker block direct positioned read (``goosefs >= 0.1.3``)::

    # High-level one-liner: resolve URI -> pick block -> route -> direct
    # WorkerClient::read_block_positioned -> bytes (one PyO3 copy).
    data = await fs.positioned_read("/data/blob.bin",
                                    block_index=0,
                                    offset=0,
                                    length=64 * 1024)

    # Low-level escape hatch when you already know the block_id:
    async with await fs.acquire_worker_for_block(block_id) as wc:
        data = await wc.read_block_positioned(block_id, 0, 64 * 1024)

The native extension module is named ``goosefs._goosefs`` and is built from
``bindings/python/src/lib.rs``. End users should import from ``goosefs``
directly; the underscore-prefixed module is an implementation detail.
"""

import atexit as _atexit
import functools as _functools
import sys as _sys
import warnings as _warnings
import weakref as _weakref
from typing import Any as _Any

# Re-export everything the native extension exposes.
from ._goosefs import *  # noqa: F401,F403
from ._goosefs import (  # noqa: F401
    AsyncFileReader,
    AsyncFileWriter,
    AsyncGoosefs,
    AsyncWorkerClient,
    Config,
    CreateFileOptions,
    DeleteOptions,
    FileReader,
    FileWriter,
    Goosefs,
    OpenFileOptions,
    ReadType,
    URIStatus,
    URIStatusList,
    WorkerClient,
    WriteType,
    __version__,
    enable_tracing,
    exceptions,
)

# PyO3-created submodules are not automatically wired into ``sys.modules``,
# so ``from goosefs.exceptions import NotFound`` would otherwise raise
# ``ModuleNotFoundError``. Register the alias explicitly. This mirrors what
# Apache OpenDAL's Python binding does in its ``__init__.py``.
_sys.modules[__name__ + ".exceptions"] = exceptions

__all__ = [
    "AsyncFileReader",
    "AsyncFileWriter",
    "AsyncGoosefs",
    "AsyncWorkerClient",
    "Config",
    "CreateFileOptions",
    "DeleteOptions",
    "FileReader",
    "FileWriter",
    "Goosefs",
    "OpenFileOptions",
    "ReadType",
    "URIStatus",
    "URIStatusList",
    "WorkerClient",
    "WriteType",
    "__version__",
    "enable_tracing",
    "exceptions",
]


# ─────────────────────────────────────────────────────────────────────────────
# atexit safety-net (Review )
# ─────────────────────────────────────────────────────────────────────────────
# Users *should* close every ``Goosefs`` / ``AsyncGoosefs`` they open, either
# via ``close()`` or by using the (async) context-manager protocol. In real
# code that doesn't always happen — scripts crash, REPL sessions exit, fixtures
# forget to tear down. The safety-net below makes sure that, when the
# interpreter shuts down, we at least *attempt* a clean release of every
# tracked handle.
#
# Design notes:
#   * ``weakref.WeakSet`` is used so the safety-net never keeps a handle alive
#     longer than the user's own references. ``Goosefs`` and ``AsyncGoosefs``
#     enable ``weakref`` in their ``#[pyclass]`` declaration specifically for
#     this purpose.
#   * We wrap ``Goosefs.__init__`` and ``AsyncGoosefs.connect`` with thin
#     proxies that register the resulting instance into the WeakSet. Wrapping
#     the constructors (rather than asking users to call a register function)
#     means the safety-net is invisible — users get it for free.
#   * On atexit we only call ``close()`` on **synchronous** ``Goosefs``
#     instances. ``AsyncGoosefs.close()`` returns an awaitable, and at
#     interpreter-shutdown there is no reliable event loop to drive it (the
#     user's ``asyncio.run`` has already returned, or its loop has been
#     finalised by ``asyncio`` itself). We therefore emit a
#     ``ResourceWarning`` for any unclosed async handle and let the OS reclaim
#     the underlying socket. Users who want graceful async shutdown should
#     ``await fs.close()`` themselves; the warning makes the omission visible
#     during development.
# Every step inside the atexit hook is wrapped in a broad ``except`` —
#     interpreter shutdown is a fragile environment (modules being torn down,
#     threads being joined) and a stray exception here would surface as an
#     ugly traceback long after the user's program has logically finished.

# WeakSet of every Goosefs / AsyncGoosefs we've ever handed back to user code.
# Entries vanish automatically once the user drops their last reference.
_active_handles: "_weakref.WeakSet[_Any]" = _weakref.WeakSet()


def _register_handle(handle: _Any) -> None:
    """Track ``handle`` so the atexit hook can close it if the user forgets.

    Failing to register is non-fatal: the user gets a fully functional
    handle, they just lose the safety-net for that one instance.
    """
    try:
        _active_handles.add(handle)
    except Exception:  # pragma: no cover — defensive only
        # Truly never raise from inside a constructor wrapper.
        pass


# ── Wrap ``Goosefs.__init__`` ────────────────────────────────────────────────
# ``Goosefs`` is constructed synchronously, so we can register right after
# ``__init__`` returns.
#
# Subtle PyO3 detail: all real construction happens in the Rust ``#[new]``
# function, which Python invokes as ``__new__``. The ``__init__`` slot on a
# PyO3 class is just inherited ``object.__init__``, which **does not accept
# extra arguments** (it raises ``TypeError`` when ``__new__`` is overridden
# and ``__init__`` is not). So we must NOT forward ``*args`` to the original
# ``__init__`` — we just leave construction to ``__new__`` and register.

_orig_goosefs_init = Goosefs.__init__  # noqa: F841  (kept for parity with AsyncGoosefs wrapping)


def _goosefs_init_with_tracking(self: _Any, config: _Any) -> None:
    # Intentionally do NOT call ``object.__init__(self, config)``: the Rust
    # ``#[new]`` already fully constructed ``self``, and ``object.__init__``
    # would reject the extra ``config`` argument. We keep ``config`` in the
    # signature only so ``inspect`` / stubtest see the correct shape
    # ``(self, config)`` rather than the over-broad ``*args, **kwargs``.
    del config  # unused — see comment above
    _register_handle(self)


Goosefs.__init__ = _goosefs_init_with_tracking  # type: ignore[method-assign]


# ── Wrap ``AsyncGoosefs.connect`` ────────────────────────────────────────────
# ``connect`` is an async factory: calling it returns an awaitable that
# resolves to the ``AsyncGoosefs`` instance. We can't register the instance
# until the awaitable resolves, hence the ``async def`` wrapper.

_orig_async_connect = AsyncGoosefs.connect


# Implementation note: ``_async_connect_with_tracking`` is intentionally a
# *plain* function (not ``async def``) that returns a freshly-built
# coroutine. The reason is stubtest: the runtime ``async def`` form is
# detected via ``inspect.iscoroutinefunction``, which would force the .pyi
# stub to use ``async def``. Keeping a plain function preserves the
# pre-existing stub shape ``def connect(config) -> Awaitable[AsyncGoosefs]``
# and matches every other PyO3-generated coroutine factory in this module.
def _async_connect_with_tracking(config: _Any) -> _Any:
    async def _await_then_register() -> _Any:
        handle = await _orig_async_connect(config)
        _register_handle(handle)
        return handle

    return _await_then_register()


# Carry over docstring / qualname etc. from the original.
_functools.update_wrapper(_async_connect_with_tracking, _orig_async_connect)


# ``connect`` is exposed as a static method on the Rust side, so wrap it in
# ``staticmethod`` to preserve the call convention (``AsyncGoosefs.connect(cfg)``
# rather than ``AsyncGoosefs.connect(self, cfg)``).
AsyncGoosefs.connect = staticmethod(_async_connect_with_tracking)  # type: ignore[method-assign]


# ── atexit hook ──────────────────────────────────────────────────────────────


def _close_active_handles_at_exit() -> None:
    """Best-effort cleanup of every unclosed handle at interpreter shutdown.

    Called by :mod:`atexit`. Never raises — interpreter shutdown is too
    fragile a moment to surface exceptions to the user.
    """
    # Snapshot the set: ``WeakSet`` iteration during shutdown can race with
    # the GC tearing down the very objects we're iterating over. ``list()``
    # gives us a stable, strong-reference view for the duration of the loop.
    try:
        snapshot = list(_active_handles)
    except Exception:  # pragma: no cover — defensive only
        return

    for handle in snapshot:
        try:
            cls_name = type(handle).__name__
        except Exception:  # pragma: no cover — defensive only
            cls_name = "<unknown>"

        if cls_name == "Goosefs":
            # Synchronous handle: drive ``close()`` directly. The native
            # ``close()`` is idempotent, so calling it on an already-closed
            # instance is a harmless no-op.
            try:
                handle.close()
            except Exception:
                # Swallow everything — atexit must never raise.
                pass
        else:
            # AsyncGoosefs (or anything else we registered). We can't drive
            # an awaitable here; surface a ResourceWarning so the user sees
            # the leak during development. ``stacklevel=0`` because there is
            # no useful caller frame at interpreter shutdown.
            try:
                _warnings.warn(
                    f"{cls_name} instance was not closed before interpreter "
                    "shutdown; the underlying connection will be reclaimed by "
                    "the OS. Call ``await fs.close()`` (or use ``async with``) "
                    "to release it cleanly.",
                    ResourceWarning,
                    stacklevel=2,
                )
            except Exception:
                pass


_atexit.register(_close_active_handles_at_exit)
