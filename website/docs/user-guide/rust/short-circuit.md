---
sidebar_position: 5
---

# Short-Circuit Read

When the client and a GooseFS Worker are co-located, the SDK can bypass the gRPC data plane and read blocks via zero-copy `mmap` (with `madvise` prefetch and optional Transparent Huge Pages).

## Highlights

- Local worker is auto-detected by interface bind
- Per-task hot-block caches and negative caching
- Every recoverable error transparently falls back to the standard gRPC path
- Wired into both sequential `read()` and positioned `read_at()`

```bash
cargo run --example short_circuit_demo
```

Design notes: [`docs/SHORT_CIRCUIT_DESIGN.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/SHORT_CIRCUIT_DESIGN.md).
