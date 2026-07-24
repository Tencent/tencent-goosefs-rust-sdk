---
sidebar_position: 7
---

# Error Handling

Every error raised by the GooseFS Python client inherits from `GoosefsError`, which itself inherits from `Exception`. This means `except GoosefsError` is a safe, exhaustive fallback that catches every SDK-specific error without swallowing unrelated exceptions like `KeyboardInterrupt` or `ValueError`.

## Exception Hierarchy

```
Exception
└── GoosefsError
    ├── NotFound              # path does not exist
    ├── AlreadyExists         # rename to existing destination
    ├── PermissionDenied      # ACL / auth failure
    ├── InvalidArgument       # malformed path, bad offset, etc.
    ├── FileIncomplete        # file still being written
    ├── DirectoryNotEmpty     # non-recursive delete on non-empty dir
    ├── IsADirectory          # tried to read a directory as a file
    ├── AuthenticationFailed  # SASL handshake failed
    ├── NoWorkerAvailable     # no healthy worker for the block
    ├── MasterUnavailable     # all master replicas unreachable
    ├── ConfigError           # invalid configuration
    ├── RpcError              # gRPC transport / protocol error
    └── IoError               # local I/O (block read/write)
```

## Usage

```python
from goosefs import AsyncGoosefs, Config
from goosefs.exceptions import GoosefsError, NotFound, PermissionDenied

async with await AsyncGoosefs.connect(Config("127.0.0.1:9200")) as fs:
    try:
        status = await fs.get_status("/data/missing")
    except NotFound:
        print("path does not exist")
    except PermissionDenied as e:
        print(f"access denied: {e}")
    except GoosefsError as e:
        # Catch-all for any other SDK error
        print(f"goosefs error: {e}")
```

## Mapping Reference

The Rust binding maps every `goosefs_sdk::error::Error` variant to a specific Python exception — there is **no fall-through** to a generic catch-all:

| SDK Error variant      | Python exception        |
| ---------------------- | ----------------------- |
| `NotFound`             | `NotFound`              |
| `AlreadyExists`        | `AlreadyExists`         |
| `PermissionDenied`     | `PermissionDenied`      |
| `InvalidArgument`      | `InvalidArgument`       |
| `InvalidPath`          | `InvalidArgument`       |
| `FileIncomplete`       | `FileIncomplete`        |
| `DirectoryNotEmpty`    | `DirectoryNotEmpty`     |
| `OpenDirectory`        | `IsADirectory`          |
| `AuthenticationFailed` | `AuthenticationFailed`  |
| `NoWorkerAvailable`    | `NoWorkerAvailable`     |
| `MasterUnavailable`    | `MasterUnavailable`     |
| `ConfigError`          | `ConfigError`           |
| `GrpcError`            | `RpcError`              |
| `TransportError`       | `RpcError`              |
| `BlockIoError`         | `IoError`               |
| `MissingField`         | `GoosefsError` (with descriptive message) |
| `Internal`             | `GoosefsError` (with descriptive message) |

## Common Patterns

### Retry on transient errors

```python
from goosefs.exceptions import RpcError, MasterUnavailable, NoWorkerAvailable

TRANSIENT = (RpcError, MasterUnavailable, NoWorkerAvailable)

for attempt in range(3):
    try:
        return await fs.read_file(path)
    except TRANSIENT:
        if attempt == 2:
            raise
        await asyncio.sleep(2 ** attempt)
```

### Distinguish "not found" from other errors

```python
try:
    status = await fs.get_status(path)
except NotFound:
    return None  # expected — path may not exist yet
```
