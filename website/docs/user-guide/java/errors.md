---
sidebar_position: 7
---

# Error Handling

SDK failures are thrown as `GoosefsException`, an unchecked `RuntimeException` with a `Code` enum. Local validation may throw `IllegalArgumentException` or `IllegalStateException` instead.

## Codes

```
GoosefsException
  Code:
    Unexpected            # MissingField / Internal, with a descriptive message
    NotFound
    AlreadyExists         # rename onto an existing destination
    PermissionDenied
    InvalidArgument       # malformed path, bad offset, length < -1, …
    FileIncomplete
    DirectoryNotEmpty
    IsADirectory
    AuthenticationFailed
    NoWorkerAvailable
    MasterUnavailable
    ConfigError
    RpcError              # gRPC / transport
    IoError               # local block I/O
```

`mkdir` on an existing directory is **idempotent** (SDK `allow_exists=true`) and does not throw `AlreadyExists`.

## Usage

```java
import com.tencent.goosefs.Config;
import com.tencent.goosefs.Goosefs;
import com.tencent.goosefs.GoosefsException;

try (Goosefs fs = Goosefs.connect(new Config("127.0.0.1:9200"))) {
    try {
        fs.getStatus("/data/missing");
    } catch (GoosefsException e) {
        if (e.getCode() == GoosefsException.Code.NotFound) {
            System.out.println("path does not exist");
        } else if (e.getCode() == GoosefsException.Code.PermissionDenied) {
            System.out.println("access denied: " + e.getMessage());
        } else {
            System.out.println("goosefs error: " + e.getMessage());
        }
    }
}
```

Unlike Python, Java does not use a subclass per error. Branch on `getCode()`.

Transient codes that callers typically retry: `RpcError`, `MasterUnavailable`, `NoWorkerAvailable`.
