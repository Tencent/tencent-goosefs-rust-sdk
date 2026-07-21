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
    @staticmethod
    def from_uri(uri: str, *, properties: Mapping[str, str] | None = ...) -> Config: ...
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

# ─────────────────────────────────────────────────────────────────────────
# Low-level Worker block client
# ─────────────────────────────────────────────────────────────────────────

@final
class AsyncWorkerClient:
    """Low-level coroutine-based block reader for a single Goosefs Worker.

    Wraps ``goosefs_sdk::client::WorkerClient``. Each instance owns one
    authenticated gRPC channel to one Worker (``host:port``). Use this
    class when you already know which Worker holds the block you want to
    read — typically benchmarks, custom routing experiments, or
    re-implementations of GooseFS-stress' ``--transport block`` mode.

    For ordinary file reads prefer the high-level
    ``AsyncGoosefs.open_file`` / ``read_range`` (or the
    ``AsyncGoosefs.positioned_read`` / ``acquire_worker_for_block``
    helpers added in stage B), which transparently select the right
    Worker via ``WorkerRouter`` and reuse the shared
    ``WorkerClientPool``.

    The handle is safe to share across coroutines; the underlying
    ``WorkerClient`` is ``Clone`` and concurrent
    ``read_block_positioned`` calls will multiplex on the same gRPC
    channel. Calling ``close()`` invalidates the handle — any subsequent
    method raises ``RuntimeError``.
    """

    @staticmethod
    def connect(addr: str, config: Config) -> Awaitable[AsyncWorkerClient]:
        """Open an authenticated gRPC channel to ``addr`` (``host:port``).

        The handshake follows ``config.auth_type``; pass the same
        ``Config`` you would give to ``AsyncGoosefs.connect``.
        """
        ...
    @staticmethod
    def connect_simple(addr: str, connect_timeout_ms: int = ...) -> Awaitable[AsyncWorkerClient]:
        """Connect without SASL authentication — test workers only.

        .. deprecated::
            ``connect_simple`` bypasses SASL authentication and is only
            suitable for NOSASL test workers. Production code should use
            :meth:`connect` with a proper :class:`Config`. Emits
            ``DeprecationWarning`` on every call.
        """
        ...
    def read_block_positioned(
        self,
        block_id: int,
        offset: int,
        length: int,
        chunk_size: int = ...,
    ) -> Awaitable[bytes]:
        """One-shot positioned read against a single block.

        Sends ``position_short = true`` so the Worker skips prefetch and
        closes the response stream after delivering exactly ``length``
        bytes. Returns the requested byte range as a single ``bytes``
        object (one copy across the PyO3 boundary).

        ``chunk_size`` defaults to 1 MiB, matching
        ``goosefs.user.streaming.reader.chunk.size.bytes``.

        Raises ``ValueError`` for negative offset/length or non-positive
        chunk_size; ``RuntimeError`` if the handle was already closed;
        ``IoError`` / ``RpcError`` for transport failures.
        """
        ...
    @property
    def addr(self) -> str:
        """The ``host:port`` this client is connected to."""
        ...
    def close(self) -> Awaitable[None]:
        """Idempotent. Subsequent reads raise ``RuntimeError``."""
        ...
    def __aenter__(self) -> Awaitable[AsyncWorkerClient]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: Any | None = ...,
    ) -> Awaitable[None]: ...

# ─────────────────────────────────────────────────────────────────────────
# Sync mirror — WorkerClient
# ─────────────────────────────────────────────────────────────────────────

@final
class WorkerClient:
    """Synchronous (blocking) low-level Worker block client.

    Sync mirror of :class:`AsyncWorkerClient`. Drives the shared Tokio
    runtime via ``block_on``, so it must NOT be called from inside an
    asyncio loop or a Tokio worker thread (same constraint as the
    sync :class:`Goosefs` class).

    Most callers should prefer :meth:`Goosefs.positioned_read` which
    routes via the master and reuses the shared connection pool — this
    class is the escape hatch for already-known worker addresses
    (benchmarks, custom routing experiments).
    """

    addr: str
    @staticmethod
    def connect(addr: str, config: Config) -> WorkerClient:
        """Open an authenticated gRPC channel to ``addr`` (``host:port``).

        The handshake follows ``config.auth_type``; pass the same
        ``Config`` you would give to ``Goosefs(...)``.
        """
        ...
    @staticmethod
    def connect_simple(addr: str, connect_timeout_ms: int = ...) -> WorkerClient:
        """Connect without SASL authentication — test workers only.

        .. deprecated::
            Bypasses SASL auth; only suitable for NOSASL test workers.
            Production code should use :meth:`connect` with a proper
            :class:`Config`. Emits ``DeprecationWarning`` on every call.
        """
        ...
    def read_block_positioned(
        self,
        block_id: int,
        offset: int,
        length: int,
        chunk_size: int = ...,
    ) -> bytes:
        """One-shot positioned read — sync counterpart of
        :meth:`AsyncWorkerClient.read_block_positioned`.
        """
        ...
    def close(self) -> None:
        """Idempotent. Subsequent reads raise ``RuntimeError``."""
        ...
    def __enter__(self) -> WorkerClient: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_value: BaseException | None = ...,
        traceback: Any | None = ...,
    ) -> None: ...

# ─────────────────────────────────────────────────────────────────────────
# Filesystem facade — async
# ─────────────────────────────────────────────────────────────────────────

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
    def exists(self, path: str) -> Awaitable[bool]:
        ...
        # Maximum number of RPCs allowed in flight for batch operations.
    MAX_BATCH_RPC_IN_FLIGHT = 64

    def batch_get_status(self, paths: list[str]) -> Awaitable[list[URIStatus]]:
        """Concurrent ``get_status`` for every path (single PyO3 crossing).

        Results are returned in input order. Concurrency is bounded
        internally (at most `MAX_BATCH_RPC_IN_FLIGHT` RPCs in flight) so passing thousands of
        paths will *not* fan out thousands of simultaneous gRPC streams
        to the master.

        The whole batch fails on the first error (e.g. a ``NotFound`` for
        any path). Note that a failed batch does *not* cancel the RPCs
        that are already in flight — the early return only stops feeding
        new requests into the buffer."""
        ...
    def batch_exists(self, paths: list[str]) -> Awaitable[list[bool]]:
        """Concurrent ``exists`` for every path; booleans in input order.

        Concurrency is bounded internally (at most `MAX_BATCH_RPC_IN_FLIGHT` RPCs in flight)."""
        ...
    def batch_open_file(self, paths: list[str]) -> Awaitable[list[AsyncFileReader]]:
        """Concurrent ``open_file`` for every path (single PyO3 crossing).

        Returns readers in input order. Concurrency is bounded internally
        (at most `MAX_BATCH_RPC_IN_FLIGHT` RPCs in flight)."""
        ...
    def batch_create_file(
        self,
        paths: list[str],
        *,
        write_type: WriteType | None = ...,
        block_size_bytes: int | None = ...,
        recursive: bool = ...,
    ) -> Awaitable[list[int]]:
        """Concurrent empty-file create+write+close for every path.

        Returns bytes-written per file (always 0 for empty files) in
        input order. Concurrency is bounded internally (at most `MAX_BATCH_RPC_IN_FLIGHT` RPCs
        in flight). The whole batch fails on the first error."""
        ...
    def batch_create_dir(
        self,
        paths: list[str],
        *,
        recursive: bool = ...,
    ) -> Awaitable[None]:
        """Concurrent ``mkdir`` for every path (single PyO3 crossing).

        Concurrency is bounded internally (at most `MAX_BATCH_RPC_IN_FLIGHT` RPCs in flight).
        The whole batch fails on the first error."""
        ...
    def batch_rename(
        self,
        pairs: list[str],
    ) -> Awaitable[None]:
        """Concurrent ``rename`` for every (src, dst) pair.

        ``pairs`` is a flat list: ``[src_0, dst_0, src_1, dst_1, ...]``.
        Length must be even. Concurrency is bounded internally (at most
        `MAX_BATCH_RPC_IN_FLIGHT` RPCs in flight). The whole batch fails on the first error."""
        ...
    def batch_delete(
        self,
        paths: list[str],
        *,
        recursive: bool = ...,
        unchecked: bool = ...,
        goosefs_only: bool = ...,
    ) -> Awaitable[None]:
        """Concurrent ``delete`` for every path (single PyO3 crossing).

        Concurrency is bounded internally (at most `MAX_BATCH_RPC_IN_FLIGHT` RPCs in flight).
        """
        ...
    def batch_list_status(
        self,
        dirs: list[str],
        *,
        recursive: bool = ...,
    ) -> Awaitable[list[list[URIStatus]]]:
        """Concurrent ``list_status`` for every directory (single PyO3 crossing).

        Returns entries for each directory in input order as a list-of-lists.
        The whole batch fails on the first error."""
        ...
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

    # ── Worker block direct-read (P6 stage B)
    def acquire_worker_for_block(
        self,
        block_id: int,
    ) -> Awaitable[AsyncWorkerClient]:
        """Pick the responsible Worker for ``block_id`` (router + pool acquire).

        Returns a binding-level wrapper around the *pooled*
        ``WorkerClient`` — closing it only releases the wrapper, the
        underlying authenticated channel stays in the
        ``FileSystemContext``'s pool.
        """
        ...
    def positioned_read(
        self,
        path: str,
        *,
        block_index: int = ...,
        offset: int = ...,
        length: int = ...,
        chunk_size: int = ...,
    ) -> Awaitable[bytes]:
        """High-level Worker block direct read.

        Resolves ``path`` → picks ``block_ids[block_index]`` → routes to
        the responsible Worker via the shared ``WorkerRouter`` →
        positioned-reads the requested byte range from that block in a
        single round-trip. ``length=-1`` (default) reads from ``offset``
        to the end of the chosen block.

        **Note on last-block ``length=-1``**: for the last block of a
        file the actual block size may be smaller than
        ``block_size_bytes`` reported by master, so ``length=-1``
        returns only the remaining bytes of that block (which may be
        < ``block_size_bytes``).

        Mirrors the Rust SDK's ``examples/lowlevel_block_read.rs``.
        """
        ...

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
    def batch_get_status(self, paths: list[str]) -> list[URIStatus]:
        """Concurrent ``get_status`` for every path (single GIL release).

        Results are returned in input order. Concurrency is bounded
        internally (at most 64 RPCs in flight) so passing thousands of
        paths will *not* fan out thousands of simultaneous gRPC streams
        to the master.

        The whole batch fails on the first error (e.g. a ``NotFound`` for
        any path). Note that a failed batch does *not* cancel the RPCs
        that are already in flight."""
        ...
    def batch_exists(self, paths: list[str]) -> list[bool]:
        """Concurrent ``exists`` for every path; booleans in input order.

        Concurrency is bounded internally (at most 64 RPCs in flight)."""
        ...
    def batch_create_file(
        self,
        paths: list[str],
        *,
        write_type: WriteType | None = ...,
        block_size_bytes: int | None = ...,
        recursive: bool = ...,
    ) -> list[int]:
        """Concurrent empty-file create+write+close for every path.

        Returns bytes-written per file. Concurrency is bounded
        internally (at most 64 RPCs in flight)."""
        ...
    def batch_create_dir(self, paths: list[str], *, recursive: bool = ...) -> None:
        """Concurrent ``mkdir`` for every path (single GIL release).

        Concurrency is bounded internally (at most 64 RPCs in flight)."""
        ...
    def batch_rename(self, pairs: list[str]) -> None:
        """Concurrent ``rename`` for every (src, dst) pair.

        ``pairs`` is flat: ``[src_0, dst_0, ...]``. Length must be even."""
        ...
    def batch_delete(
        self,
        paths: list[str],
        *,
        recursive: bool = ...,
        unchecked: bool = ...,
        goosefs_only: bool = ...,
    ) -> None:
        """Concurrent ``delete`` for every path (single GIL release).

        Concurrency is bounded internally (at most 64 RPCs in flight)."""
        ...
    def batch_list_status(
        self,
        dirs: list[str],
        *,
        recursive: bool = ...,
    ) -> list[list[URIStatus]]:
        """Concurrent ``list_status`` for every directory (single GIL release).

        Returns entries for each directory in input order as a list-of-lists.
        The whole batch fails on the first error."""
        ...
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

    # ── Worker block direct-read (P6 stage B)
    def acquire_worker_for_block(
        self,
        block_id: int,
    ) -> AsyncWorkerClient:
        """Pick the responsible Worker for ``block_id``.

        The returned object is still an :class:`AsyncWorkerClient` — its
        ``read_block_positioned`` method must be ``await``-ed from an
        async context. Pure-sync callers should prefer
        :meth:`positioned_read` which wraps the whole sequence in a
        synchronous ``block_on``.
        """
        ...
    def positioned_read(
        self,
        path: str,
        *,
        block_index: int = ...,
        offset: int = ...,
        length: int = ...,
        chunk_size: int = ...,
    ) -> bytes:
        """Synchronous counterpart of
        :meth:`AsyncGoosefs.positioned_read`. Blocks the calling thread.

        **Note on last-block ``length=-1``**: for the last block of a
        file the actual block size may be smaller than
        ``block_size_bytes`` reported by master, so ``length=-1``
        returns only the remaining bytes of that block (which may be
        < ``block_size_bytes``).
        """
        ...

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
    "WorkerClient",
    "WriteType",
    "__version__",
    "enable_tracing",
    "exceptions",
]
