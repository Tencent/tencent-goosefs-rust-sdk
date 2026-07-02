# Short-Circuit Positioned-Read A/B — 2026-06-24

Bench: `cargo run --release --example sc_pr_ab` (see `benchmarks/sc_pr_ab.rs`).
Compares random `read_at` throughput + latency with the short-circuit (local
mmap) path **on** vs **off** (gRPC), everything else equal, page cache hot.

## Environment

| | |
|---|---|
| OS | Darwin 25.5.0 (macOS), arm64 |
| CPU | Apple M4 Pro |
| RAM | 48 GiB |
| Cluster | local GooseFS worker (NOSASL), `127.0.0.1:9200`, worker host = LAN IP |
| Build | `--release` (lto=fat, codegen-units=1) |

> Note: macOS arm64 dev box, not a TencentOS/CVM production node, and no perf
> flamegraph (perf is Linux-only — see SHORT_CIRCUIT_DESIGN §5.2.1 for the
> Linux SOP). Numbers are the **hot page-cache** case the design targets.

## Parameters

`GFS_SIZE_MB=64  GFS_IO_KB=64  GFS_READS=15000` — random offsets, single task.

## Results

| path | throughput | ops/s | p50 | p99 | p999 |
|---|---|---|---|---|---|
| gRPC | 356.4 MiB/s | 5,702 | 168 µs | 261 µs | 356 µs |
| **SC** | **~109 GiB/s** | **~1.75 M** | **<1 µs** | **<1 µs** | **<1 µs** |

**Throughput ×307 · p99 ×261 better.** `short-circuit: ACTIVE`.

## Interpretation

- gRPC pays a worker round-trip per positioned read (~168 µs p50), capping
  single-task throughput at the per-op RTT.
- SC serves each read as a zero-copy `mmap` slice (`read_bytes` →
  `Bytes::from_owner` + `.slice`), i.e. a pure pointer/length op on
  already-resident pages — sub-microsecond, bounded only by memory bandwidth.
- The ×307 here exceeds the design's §5.2 prediction (×6–8) because that table
  assumes a Java SC baseline (which already does local mmap), whereas this A/B
  baseline is the **gRPC data plane** (network round-trip). Against a Java-SC
  baseline the expected win is the §5.2 range.

## Consistency

All reads were verified byte-for-byte against the source in the E2E tests
(`tests/short_circuit_e2e.rs`, incl. SC-vs-gRPC equality, INV-S1/INV-D2). This
bench measures performance only.

## How to reproduce

```bash
GOOSEFS_AUTH_TYPE=nosasl GFS_SIZE_MB=64 GFS_IO_KB=64 GFS_READS=15000 \
  cargo run --release --example sc_pr_ab
```
