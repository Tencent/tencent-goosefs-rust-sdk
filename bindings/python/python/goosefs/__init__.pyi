"""Type stubs for the GooseFS Python client.

These stubs document the public API exported from the native ``_goosefs``
extension module (built from ``bindings/python/src/lib.rs``). They are the
authoritative reference for IDE auto-completion, ``mypy``, and ``pyright``;
the runtime behaviour is implemented in Rust and is verified to match the
stubs by ``stubtest`` in CI.

Thread / process safety
-----------------------
- ``Goosefs`` (sync) and ``AsyncGoosefs`` instances are **safe to share
  across threads** of the same process. Internally each instance pins a
  shared ``FileSystemContext`` that owns long-lived gRPC channels and
  worker pools; the underlying SDK uses lock-free / fine-grained locking
  and releases the GIL during all blocking I/O (``Python::detach``).
- Instances are **NOT safe across ``os.fork()``** — a forked child must
  build a fresh instance. ``Goosefs`` records its creator PID in
  ``__new__`` and refuses any subsequent call from a different PID with
  ``RuntimeError`` (see Review §17.4).
- ``Goosefs`` synchronous methods refuse to run from inside a Tokio
  worker thread or an asyncio event loop and raise ``RuntimeError``
  (Review §17.1). Use the matching ``AsyncGoosefs`` coroutine instead.
- ``FileReader`` / ``FileWriter`` (and their async siblings) are **NOT
  safe to share across threads / tasks**. Each handle wraps a single
  in-flight stream that is serialised by an internal mutex; concurrent
  ``read``/``seek`` will surface ``RuntimeError`` rather than silently
  block.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import Any, final

from typing_extensions import Self

from goosefs import exceptions as exceptions

__version__: str

# ──────────────────────────────────────────────────────────────────────────
# Module-level helpers
# ──────────────────────────────────────────────────────────────────────────

def enable_tracing(level: str = ..., *, target: str = ...) -> None:
    """Install a stderr ``tracing`` subscriber for ``goosefs-sdk`` events.

    Off by default — importing ``goosefs`` does not configure logging.
    Call this once near the top of your program to opt in.

    Args:
        level: One of ``"trace"``, ``"debug"``, ``"info"``, ``"warn"``,
            ``"error"`` (case-insensitive). Used only when the
            ``RUST_LOG`` environment variable is **not** set; if it is
            set, ``RUST_LOG`` wins and ``level`` is ignored.
        target: Sink for log lines. Only ``"stderr"`` is supported in
            this release. ``"stdout"`` and ``"logging"`` are reserved
            and currently raise ``ValueError``.

    Raises:
        ValueError: ``level`` or ``target`` is not one of the accepted
            values.
        RuntimeError: Another ``tracing`` subscriber is already active
            in the process (e.g. from ``pyo3-log`` or a host
            application). The first call wins; this function will not
            replace an existing subscriber.

    Notes:
        Calling :func:`enable_tracing` more than once is a silent no-op
        on every call after the first — it does **not** reconfigure the
        already-installed subscriber.
    """
    ...

# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

@final
class WriteType:
    """Cache / persist policy for newly-created files.

    Mirrors ``goosefs_sdk::config::WriteType`` and the proto
    ``WritePType`` integer values (1..=5). Instances compare equal to the
    matching integer (``WriteType.CacheThrough == 3``) and are hashable.

    The variant identifiers use Rust's ``UpperCamelCase``
    (``MustCache`` / ``CacheThrough`` / …). The Java-style
    ``UPPER_SNAKE_CASE`` aliases (``MUST_CACHE`` / ``CACHE_THROUGH`` /
    …) are accepted by :meth:`from_str` for parsing strings, but are
    *not* available as attributes on the class itself.
    """

    MustCache: WriteType
    TryCache: WriteType
    CacheThrough: WriteType
    Through: WriteType
    AsyncThrough: WriteType

    @property
    def value(self) -> int: ...
    def as_str(self) -> str:
        """Canonical lower-case name (e.g. ``"cache_through"``)."""
        ...
    @staticmethod
    def from_str(s: str) -> WriteType:
        """Parse from canonical or upper-case form (case-insensitive)."""
        ...

@final
class ReadType:
    """Cache policy for read-path operations.

    Mirrors ``goosefs_sdk::fs::ReadType`` (proto ``ReadPType`` 1..=2).
    """

    NoCache: ReadType
    Cache: ReadType

    @property
    def value(self) -> int: ...

# ─────────────────────────────────────────────────────────────────────────────
# Options
# ─────────────────────────────────────────────────────────────────────────────

@final
class OpenFileOptions:
    """Read-side options accepted by ``open_file``."""

    def __new__(cls, *, read_type: ReadType | None = ...) -> OpenFileOptions: ...
    @property
    def read_type(self) -> ReadType: ...

@final
class CreateFileOptions:
    """Write-side options accepted by ``create_file`` / ``write_file``.

    ``write_type=None`` (default) inherits the parent directory's
    ``innerWriteType`` xattr — the Java-compatible behaviour. Pass an
    explicit :class:`WriteType` to override.
    """

    def __new__(
        cls,
        *,
        write_type: WriteType | None = ...,
        block_size_bytes: int | None = ...,
        replication_max: int | None = ...,
        recursive: bool = ...,
    ) -> CreateFileOptions: ...
    @property
    def write_type(self) -> WriteType | None: ...
    @property
    def block_size_bytes(self) -> int | None: ...
    @property
    def replication_max(self) -> int | None: ...
    @property
    def recursive(self) -> bool: ...

@final
class DeleteOptions:
    """Options for ``delete_with_options``."""

    def __new__(
        cls,
        *,
        recursive: bool = ...,
        unchecked: bool = ...,
        goosefs_only: bool = ...,
    ) -> DeleteOptions: ...
    @property
    def recursive(self) -> bool: ...
    @property
    def unchecked(self) -> bool: ...
    @property
    def goosefs_only(self) -> bool: ...

# ─────────────────────────────────────────────────────────────────────────────
# URIStatus
# ─────────────────────────────────────────────────────────────────────────────

@final
class URIStatus:
    """Immutable metadata snapshot for a Goosefs path.

    Returned by ``get_status`` / ``list_status``. Two instances compare
    equal iff their ``(path, last_modification_time_ms)`` match.
    """

    @property
    def file_id(self) -> int: ...
    @property
    def name(self) -> str: ...
    @property
    def path(self) -> str: ...
    @property
    def ufs_path(self) -> str: ...
    @property
    def length(self) -> int: ...
    @property
    def block_size_bytes(self) -> int: ...
    @property
    def block_ids(self) -> list[int]: ...
    @property
    def creation_time_ms(self) -> int: ...
    @property
    def last_modification_time_ms(self) -> int: ...
    @property
    def last_access_time_ms(self) -> int: ...
    @property
    def completed(self) -> bool: ...
    @property
    def folder(self) -> bool: ...
    @property
    def cacheable(self) -> bool: ...
    @property
    def persisted(self) -> bool: ...
    @property
    def mount_point(self) -> bool: ...
    @property
    def in_goose_fs_percentage(self) -> int: ...
    @property
    def in_memory_percentage(self) -> int: ...
    @property
    def owner(self) -> str: ...
    @property
    def group(self) -> str: ...
    @property
    def mode(self) -> int: ...
    @property
    def persistence_state(self) -> str: ...
    @property
    def mount_id(self) -> int: ...
    @property
    def ufs_fingerprint(self) -> str: ...
    @property
    def xattr(self) -> dict[str, bytes]: ...
    @property
    def symlink(self) -> str | None: ...
    def is_readable(self) -> bool:
        """``True`` if the path is a completed file or a directory."""
        ...
    def is_completed(self) -> bool: ...
    def is_folder(self) -> bool: ...
    def is_persisted(self) -> bool: ...
    def block_count(self) -> int:
        """Number of blocks (``0`` for directories)."""
        ...
    def __eq__(self, other: object, /) -> bool: ...
    def __hash__(self) -> int: ...

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

@final
class Config:
    """Goosefs client configuration.

    ``master_addr`` is either ``"host:port"`` or a comma-separated HA
    list (``"m1:9200,m2:9200,m3:9200"``). Property keys are the same
    ones accepted by ``goosefs-site.properties``.
    """

    def __new__(
        cls,
        master_addr: str,
        *,
        properties: Mapping[str, str] | None = ...,
    ) -> Config: ...
    @staticmethod
    def from_properties_file(path: str) -> Config: ...
    @property
    def master_addr(self) -> str: ...
    @property
    def master_addrs(self) -> list[str]: ...
    @property
    def block_size(self) -> int: ...
    @property
    def chunk_size(self) -> int: ...
    @property
    def root(self) -> str: ...
    @property
    def use_vpc_mapping(self) -> bool: ...
    @property
    def auth_type(self) -> str: ...
    @property
    def auth_username(self) -> str: ...
    @property
    def metrics_enabled(self) -> bool: ...
    @property
    def connect_timeout_ms(self) -> int: ...
    @property
    def request_timeout_ms(self) -> int: ...
    @property
    def write_type(self) -> int | None:
        """Default ``WriteType`` as the proto integer (1..=5), or
        ``None`` if no explicit default was configured."""
        ...

# ─────────────────────────────────────────────────────────────────────────────
# Bytes-like alias
# ─────────────────────────────────────────────────────────────────────────────
#
# Anything implementing the buffer protocol *except* ``str`` is accepted
# by ``write_file`` / ``FileWriter.write`` / ``AsyncFileWriter.write``.
# We model that as a small union; ``str`` is rejected at runtime with
# ``TypeError`` to prevent silent Latin-1 encoding.
_BytesLike = bytes | bytearray | memoryview

# ─────────────────────────────────────────────────────────────────────────────
# Streaming readers / writers
# ─────────────────────────────────────────────────────────────────────────────

@final
class AsyncFileReader:
    """Async streaming reader. Acquired via ``AsyncGoosefs.open_file``.

    All I/O methods return awaitables. The underlying stream is owned by
    a single ``tokio::sync::Mutex<Option<…>>`` — concurrent calls on the
    same handle are serialised; a concurrent call while another is in
    flight surfaces ``RuntimeError`` from ``tell()`` (which never
    blocks) but otherwise queues.
    """

    def read(self, size: int = ...) -> Awaitable[bytes]:
        """``size < 0`` (default): read all remaining bytes.
        ``size = 0``: returns ``b""``."""
        ...
    def read_at(self, offset: int, length: int) -> Awaitable[bytes]:
        """Positioned read; does not change the logical position."""
        ...
    def seek(self, offset: int, whence: int = ...) -> Awaitable[int]:
        """``whence`` follows ``io.SEEK_SET / SEEK_CUR / SEEK_END`` (0/1/2).
        Returns the new absolute byte position."""
        ...
    def tell(self) -> int:
        """Synchronous; raises ``RuntimeError`` if a read/seek is in flight."""
        ...
    def close(self) -> Awaitable[None]: ...
    def __len__(self) -> int: ...
    def __aenter__(self) -> Awaitable[AsyncFileReader]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: Any | None = ...,
    ) -> Awaitable[None]: ...

@final
class AsyncFileWriter:
    """Async streaming writer. Acquired via ``AsyncGoosefs.create_file``.

    On unhandled exception inside an ``async with`` block the writer is
    **cancelled** rather than closed — half-written files are not
    committed to the master.
    """

    def write(self, data: _BytesLike) -> Awaitable[int]:
        """Returns the number of bytes accepted (``len(data)``)."""
        ...
    def close(self) -> Awaitable[None]:
        """Finalise and commit the file. Idempotent."""
        ...
    def cancel(self) -> Awaitable[None]:
        """Abandon all uncommitted state. Idempotent."""
        ...
    def __aenter__(self) -> Awaitable[AsyncFileWriter]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: Any | None = ...,
    ) -> Awaitable[None]: ...

@final
class FileReader:
    """Synchronous streaming reader. Acquired via ``Goosefs.open_file``.

    Each blocking call releases the GIL via ``Python::detach`` and is
    guarded against deadlock when invoked from a Tokio worker / asyncio
    loop (Review §17.1).
    """

    def read(self, size: int = ...) -> bytes: ...
    def read_at(self, offset: int, length: int) -> bytes: ...
    def seek(self, offset: int, whence: int = ...) -> int: ...
    def tell(self) -> int: ...
    def close(self) -> None: ...
    def __len__(self) -> int: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: Any | None = ...,
    ) -> bool: ...

@final
class FileWriter:
    """Synchronous streaming writer. Acquired via ``Goosefs.create_file``.

    On unhandled exception inside a ``with`` block the writer is
    **cancelled** rather than closed.
    """

    def write(self, data: _BytesLike) -> int: ...
    def close(self) -> None: ...
    def cancel(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: Any | None = ...,
    ) -> bool: ...

# ─────────────────────────────────────────────────────────────────────────────
# Filesystem facade — async
# ─────────────────────────────────────────────────────────────────────────────

@final
class AsyncGoosefs:
    """Coroutine-based filesystem client.

    Construct with the static factory ``await AsyncGoosefs.connect(cfg)``;
    the constructor is intentionally not exposed because connecting is
    asynchronous (a single TCP+SASL handshake per master replica).
    """

    @staticmethod
    def connect(config: Config) -> Awaitable[AsyncGoosefs]: ...

    # ── Metadata
    def get_status(self, path: str) -> Awaitable[URIStatus]: ...
    def list_status(self, path: str, *, recursive: bool = ...) -> Awaitable[list[URIStatus]]: ...
    def exists(self, path: str) -> Awaitable[bool]: ...
    def mkdir(self, path: str, *, recursive: bool = ...) -> Awaitable[None]: ...
    def delete(
        self,
        path: str,
        *,
        recursive: bool = ...,
        unchecked: bool = ...,
        goosefs_only: bool = ...,
    ) -> Awaitable[None]: ...
    def delete_with_options(self, path: str, options: DeleteOptions) -> Awaitable[None]: ...
    def rename(self, src: str, dst: str) -> Awaitable[None]: ...

    # ── High-level (one-shot) read/write
    def read_file(self, path: str) -> Awaitable[bytes]: ...
    def read_range(self, path: str, offset: int, length: int) -> Awaitable[bytes]: ...
    def write_file(
        self,
        path: str,
        data: _BytesLike,
        *,
        write_type: WriteType | None = ...,
        block_size_bytes: int | None = ...,
        recursive: bool = ...,
    ) -> Awaitable[int]: ...

    # ── Streaming
    def open_file(self, path: str) -> Awaitable[AsyncFileReader]: ...
    def create_file(
        self,
        path: str,
        *,
        write_type: WriteType | None = ...,
        block_size_bytes: int | None = ...,
        recursive: bool = ...,
    ) -> Awaitable[AsyncFileWriter]: ...

    # ── Lifecycle
    def close(self) -> Awaitable[None]: ...
    def __aenter__(self) -> Awaitable[AsyncGoosefs]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: Any | None = ...,
    ) -> Awaitable[None]: ...

# ─────────────────────────────────────────────────────────────────────────────
# Filesystem facade — sync
# ─────────────────────────────────────────────────────────────────────────────

@final
class Goosefs:
    """Blocking filesystem client — same API surface as
    :class:`AsyncGoosefs`, but every method blocks the calling thread.

    All blocking calls release the GIL and are guarded against being
    invoked from a Tokio worker or an asyncio event loop (raises
    ``RuntimeError`` instead of deadlocking, Review §17.1).

    Not safe across ``os.fork()`` — a child process must reconnect.
    """

    def __new__(cls, config: Config) -> Goosefs: ...

    # The ``__init__`` slot is ordinarily ``object.__init__`` for a PyO3
    # class (construction happens in ``__new__``), but ``goosefs/__init__.py``
    # replaces it with a thin wrapper that registers ``self`` into the
    # atexit safety-net (Review §17.4). The wrapper accepts the same
    # ``config`` argument as ``__new__`` to keep the signature consistent
    # with what ``Goosefs(config)`` produces.
    def __init__(self, config: Config) -> None: ...

    # ── Metadata
    def get_status(self, path: str) -> URIStatus: ...
    def list_status(self, path: str, *, recursive: bool = ...) -> list[URIStatus]: ...
    def exists(self, path: str) -> bool: ...
    def mkdir(self, path: str, *, recursive: bool = ...) -> None: ...
    def delete(
        self,
        path: str,
        *,
        recursive: bool = ...,
        unchecked: bool = ...,
        goosefs_only: bool = ...,
    ) -> None: ...
    def delete_with_options(self, path: str, options: DeleteOptions) -> None: ...
    def rename(self, src: str, dst: str) -> None: ...

    # ── High-level (one-shot) read/write
    def read_file(self, path: str) -> bytes: ...
    def read_range(self, path: str, offset: int, length: int) -> bytes: ...
    def write_file(
        self,
        path: str,
        data: _BytesLike,
        *,
        write_type: WriteType | None = ...,
        block_size_bytes: int | None = ...,
        recursive: bool = ...,
    ) -> int: ...

    # ── Streaming
    def open_file(self, path: str) -> FileReader: ...
    def create_file(
        self,
        path: str,
        *,
        write_type: WriteType | None = ...,
        block_size_bytes: int | None = ...,
        recursive: bool = ...,
    ) -> FileWriter: ...

    # ── Lifecycle
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: Any | None = ...,
    ) -> bool: ...

__all__ = [
    "AsyncFileReader",
    "AsyncFileWriter",
    "AsyncGoosefs",
    "Config",
    "CreateFileOptions",
    "DeleteOptions",
    "FileReader",
    "FileWriter",
    "Goosefs",
    "OpenFileOptions",
    "ReadType",
    "URIStatus",
    "WriteType",
    "__version__",
    "enable_tracing",
    "exceptions",
]
