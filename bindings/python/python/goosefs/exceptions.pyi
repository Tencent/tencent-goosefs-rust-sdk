"""Type stubs for ``goosefs.exceptions``.

Every variant of ``goosefs_sdk::error::Error`` is mapped to one of the
classes below by the Rust binding's ``map_err`` (no fall-through to a
generic catch-all; see the design plan). All classes
inherit from :class:`GoosefsError`, which itself inherits from
``Exception`` — so ``except GoosefsError`` is a safe, exhaustive
fallback.

Mapping reference (binding ``src/errors.rs``)::

    SDK Error variant        →  Python exception
    ─────────────────────────   ───────────────────
    NotFound                 →  NotFound
    AlreadyExists            →  AlreadyExists
    PermissionDenied         →  PermissionDenied
    InvalidArgument          \\
    InvalidPath              /  →  InvalidArgument
    FileIncomplete           →  FileIncomplete
    DirectoryNotEmpty        →  DirectoryNotEmpty
    OpenDirectory            →  IsADirectory
    AuthenticationFailed     →  AuthenticationFailed
    NoWorkerAvailable        →  NoWorkerAvailable
    MasterUnavailable        →  MasterUnavailable
    ConfigError              →  ConfigError
    GrpcError                \\
    TransportError           /  →  RpcError
    BlockIoError             →  IoError
    MissingField             →  GoosefsError  (with descriptive message)
    Internal                 →  GoosefsError  (with descriptive message)
"""

from __future__ import annotations

class GoosefsError(Exception):
    """Base class for every error raised by the Goosefs client."""

class NotFound(GoosefsError):
    """The requested path does not exist."""

class AlreadyExists(GoosefsError):
    """The requested path already exists (e.g. ``rename`` to an existing
    destination)."""

class PermissionDenied(GoosefsError):
    """The caller is not authorised to perform the requested operation."""

class InvalidArgument(GoosefsError):
    """The arguments to a call are malformed or violate a precondition
    (e.g. an empty path, an offset greater than file length)."""

class FileIncomplete(GoosefsError):
    """The file exists but has not yet been finalised — typically raised
    when reading a file that another writer has not closed."""

class DirectoryNotEmpty(GoosefsError):
    """``delete()`` was called on a non-empty directory without
    ``recursive=True``."""

class IsADirectory(GoosefsError):
    """A file-only operation (e.g. ``open_file``) was called on a
    directory."""

class AuthenticationFailed(GoosefsError):
    """SASL / Kerberos / token authentication did not succeed against
    the master or a worker."""

class NoWorkerAvailable(GoosefsError):
    """No worker can serve the request (e.g. all workers are down or
    cannot host the requested block)."""

class MasterUnavailable(GoosefsError):
    """All configured masters are unreachable. With HA enabled this is
    raised only after every replica has been tried."""

class RpcError(GoosefsError):
    """A gRPC or transport-level failure occurred. Includes both
    ``GrpcError`` (server returned a status) and ``TransportError`` (the
    channel itself failed)."""

class IoError(GoosefsError):
    """A block-level read or write failed. Distinct from
    :class:`RpcError` because block I/O can transiently fail without the
    control-plane connection being affected."""

class ConfigError(GoosefsError):
    """The supplied ``Config`` (or ``goosefs-site.properties`` file) is
    invalid."""

__all__ = [
    "GoosefsError",
    "NotFound",
    "AlreadyExists",
    "PermissionDenied",
    "InvalidArgument",
    "FileIncomplete",
    "DirectoryNotEmpty",
    "IsADirectory",
    "AuthenticationFailed",
    "NoWorkerAvailable",
    "MasterUnavailable",
    "RpcError",
    "IoError",
    "ConfigError",
]
