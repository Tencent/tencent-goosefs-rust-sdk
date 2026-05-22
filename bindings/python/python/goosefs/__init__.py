"""GooseFS Python client (Rust-powered).

Quick start::

    import asyncio
    from goosefs import AsyncGooseFs, Config, WriteType

    async def main():
        cfg = Config("127.0.0.1:9200")
        async with await AsyncGooseFs.connect(cfg) as fs:
            await fs.mkdir("/tmp/p2", recursive=True)
            status = await fs.get_status("/tmp/p2")
            print(status)

    asyncio.run(main())

For synchronous (blocking) workflows, use ``GooseFs``::

    from goosefs import GooseFs, Config

    with GooseFs(Config("127.0.0.1:9200")) as fs:
        fs.mkdir("/tmp/p3", recursive=True)
        assert fs.exists("/tmp/p3")

The native extension module is named ``goosefs._goosefs`` and is built from
``bindings/python/src/lib.rs``. End users should import from ``goosefs``
directly; the underscore-prefixed module is an implementation detail.
"""

import sys as _sys

# Re-export everything the native extension exposes.
from ._goosefs import *  # noqa: F401,F403
from ._goosefs import (  # noqa: F401
    AsyncGooseFs,
    Config,
    CreateFileOptions,
    DeleteOptions,
    GooseFs,
    OpenFileOptions,
    ReadType,
    URIStatus,
    WriteType,
    __version__,
    exceptions,
)

# PyO3-created submodules are not automatically wired into ``sys.modules``,
# so ``from goosefs.exceptions import NotFound`` would otherwise raise
# ``ModuleNotFoundError``. Register the alias explicitly. This mirrors what
# Apache OpenDAL's Python binding does in its ``__init__.py``.
_sys.modules[__name__ + ".exceptions"] = exceptions

__all__ = [
    "AsyncGooseFs",
    "Config",
    "CreateFileOptions",
    "DeleteOptions",
    "GooseFs",
    "OpenFileOptions",
    "ReadType",
    "URIStatus",
    "WriteType",
    "__version__",
    "exceptions",
]
