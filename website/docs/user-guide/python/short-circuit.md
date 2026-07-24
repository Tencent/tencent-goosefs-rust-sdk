---
sidebar_position: 9
---

# Short-Circuit Read

Short-circuit read allows a client co-located with a GooseFS worker to read block data directly from the worker's local storage via memory-mapped I/O, **bypassing the gRPC data path entirely**. This is the fastest read path and is used automatically when available.

## How It Works

1. The client opens a file and resolves the block → worker mapping.
2. If the client and worker are on the **same host**, the client attempts a short-circuit open.
3. On success, data is read via `mmap` of the worker's block file — no gRPC, no protobuf encoding, no network.
4. On any recoverable failure (permission denied, file not found, etc.), the client **transparently falls back** to the gRPC read path.

## Enabling

Short-circuit is **disabled by default**. Enable via configuration:

```bash
# Environment variable
export GOOSEFS_SHORT_CIRCUIT_ENABLED=true
```

```properties
# goosefs-site.properties
goosefs.user.short.circuit.enabled=true
```

```python
from goosefs import Config

# The Config builder inherits the setting from env / properties.
# No explicit Python-side toggle is needed.
cfg = Config("127.0.0.1:9200")
```

## Capability Authorization

On clusters with capability enforcement enabled, short-circuit reads require a valid capability from the master. If the capability is missing or invalid, the client falls back to gRPC — the read still succeeds, just slower.

## When Short-Circuit Engages

| Condition                                | Short-circuit? |
| ---------------------------------------- | -------------- |
| Client and worker on same host, SC enabled | ✅ Yes         |
| Client and worker on different hosts     | ❌ No (gRPC)   |
| SC disabled in config                    | ❌ No (gRPC)   |
| Capability required but not available    | ❌ No (gRPC fallback) |
| Worker local file missing or unreadable  | ❌ No (gRPC fallback) |

The fallback is **automatic and transparent** — the application sees identical data regardless of which path is used.

## Verifying Short-Circuit

Set `RUST_LOG=goosefs_sdk::block::short_circuit=debug` to see whether short-circuit is engaging:

```bash
RUST_LOG=goosefs_sdk::block::short_circuit=debug python your_script.py
```

Look for `short-circuit open succeeded` (engaged) vs `falling back to gRPC` (not engaged).
