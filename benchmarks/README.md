# Benchmarks Usage Guide

This directory contains **benchmark / stress-test / reproduction** cases for the GooseFS Rust SDK. Runnable benchmarks are declared as `example` targets (see `Cargo.toml`); they connect to the local Master at `127.0.0.1:9200` by default. Before running, make sure the local cluster is up (you should see Master/Worker listening via `lsof -i:9200`).

> Introductory functional API examples (`highlevel_file_rw`, `context_file_rw`, `streaming_file_read`, `metadata_crud`, `auth_demo`, etc.) live in the [`../examples`](../examples) directory.

```bash
# Runnable benchmarks / reproductions (example targets)
cargo run --release --example <name>

# criterion statistical micro-benchmarks (master_hotpath)
cargo bench --bench master_hotpath
```

| Benchmark | Type | Description |
|-----------|------|-------------|
| `partv_perf_verify` | `--example` | **End-to-end Part V optimization verification + stress test** (random read / sequential read / Master pool / Worker pool), including byte-for-byte correctness checks. **This document's focus.** |
| `repro_concurrent_write` | `--example` | Concurrent-write reproduction case |
| `master_hotpath` | `--bench` (criterion) | GetFileStatus hot-path statistical micro-benchmark (`ArcSwap` / counter caching / path move optimization comparison) |

---

## `partv_perf_verify` — Performance Verification / Stress Test

One run covers four areas and performs **byte-for-byte verification for random read + full-file sequential read** (upholding the consistency red lines C1 byte-exact / C2 never silently short-read):

| Step | Optimization Point Verified | Output Line |
|------|----------------------------|-------------|
| `[1]` Concurrent random read (PR) | **R2** `read_at` single-block fast path + **Worker multi-channel pool** | `... reads, ... MiB in ...s → ... MiB/s` + `✅ byte-for-byte verification passed` |
| `[2]` Sequential read (SR) | **R1-B** prefetch / background drain / ACK | `64KiB-buf scan: ...` and `read_all : ...` |
| `[3]` Master metadata throughput | **R3** Master multi-channel pool | `pool=1 ...` vs `pool=N ...` + `Δ ...%` |

### Parameters (all overridable via environment variables)

| Environment Variable | Default | Meaning |
|----------------------|---------|---------|
| `GFS_ADDR` | `127.0.0.1:9200` | Master address (change this for remote clusters) |
| `GFS_SIZE_MB` | `128` | Test file size (MiB). **Note full caching**: MUST_CACHE does not spill to UFS, so the file must be ≤ Worker cache capacity, otherwise evicted blocks will cause read failures |
| `GFS_IO_KB` | `1024` | IO size (KiB) for a single `read_at` / `read`. 1MB=1024, 8MB=8192, 16MB=16384 |
| `GFS_CONC` | `16` | Number of concurrent readers (shared by PR and SR) |
| `GFS_READS` | `8` | Number of random reads issued per PR reader (total random reads = `CONC × READS`) |
| `GFS_POOL` | `8` | **Master** connection pool size (`master_connection_pool_size`, corresponds to R3) |
| `GFS_WPOOL` | `1` | Per-**Worker** channel pool size (`worker_connection_pool_size`, multi-channel on the Worker side) |
| `GFS_META_OPS` | `20000` | Total `get_status` count in step `[3]` |
| `GFS_META_CONC` | `256` | Metadata concurrency in step `[3]` |
| `GFS_TAG` | (empty) | Test file path suffix; for multi-process parallelism each uses an independent file (e.g. `GFS_TAG=p1` → `/partv-bench/data-p1.bin`) |

> Always run with `--release` (fat LTO is enabled); otherwise the numbers are not representative.

---

## Recipe: Verifying the Worker Multi-Channel Pool (`GFS_WPOOL` sweep)

This is exactly the scenario for verifying whether adding worker channels in a single process can raise random-read throughput from ~1.6 GiB/s to ~4 GiB/s.

Approach: **fix the file fully cached, fix concurrency, only sweep `GFS_WPOOL = 1/2/4/8`**, and compare PR random-read throughput.

```bash
cd /opt/sourcecode/cos/goosefs-client-rust

for W in 1 2 4 8; do
  ( GFS_TAG=w$W \
    GFS_SIZE_MB=256 GFS_IO_KB=8192 GFS_CONC=32 GFS_READS=16 \
    GFS_POOL=4 GFS_WPOOL=$W \
    GFS_META_OPS=1 GFS_META_CONC=4 \
    ./target/release/examples/partv_perf_verify > /tmp/wp_$W.log 2>&1 &
    echo $! > /tmp/wp.pid )
  # watchdog: wait at most 90s, kill if hung
  P=$(cat /tmp/wp.pid)
  for i in $(seq 1 90); do kill -0 $P 2>/dev/null || break; sleep 1; done
  kill -9 $P 2>/dev/null
  printf 'WPOOL=%s\n' "$W"
  grep -E 'reads,|read_all|64KiB-buf' /tmp/wp_$W.log
done
```

> Build once with `cargo build --release --example partv_perf_verify`; the loop then runs the binary directly to avoid recompiling each round.

Why these parameter values:

- `GFS_IO_KB=8192` (8MB large IO) + `GFS_CONC=32`: generate enough concurrency to saturate a single channel, so the difference becomes visible.
- `GFS_SIZE_MB=256`: single file fully cached, avoiding eviction interference (reduce per process when running multiple processes in parallel).
- `GFS_META_OPS=1`: this recipe only looks at read throughput, so the metadata step just goes through the motions.
- `GFS_TAG=w$W`: each WPOOL uses an independent file, with no interference between them.

Expected conclusion: **PR random read rises significantly as WPOOL increases** (single channel → multi-channel removes the single-connection bottleneck); **SR sequential read stays roughly flat** (a single stream only advances on one block at any moment, is inherently serial, and cannot span channels).

---

## Recipe: Different IO Sizes (8MB / 16MB)

```bash
GFS_SIZE_MB=256 GFS_IO_KB=8192  cargo run --release --example partv_perf_verify   # 8MB IO
GFS_SIZE_MB=256 GFS_IO_KB=16384 cargo run --release --example partv_perf_verify   # 16MB IO
```

## Recipe: Master Pool Comparison (R3)

```bash
# Step [3] automatically runs pool=1 vs pool=GFS_POOL and prints Δ%
GFS_POOL=8 GFS_META_OPS=20000 GFS_META_CONC=256 cargo run --release --example partv_perf_verify
```

## Recipe: Multi-Process Aggregate Ceiling (probing the local-machine ceiling)

When a single process is limited by a single worker channel, use multiple processes each with independent channels to stack up and find the aggregate ceiling:

```bash
for i in 1 2 3 4; do
  ( GFS_TAG=q$i GFS_SIZE_MB=128 GFS_IO_KB=8192 GFS_CONC=24 GFS_READS=20 \
    GFS_WPOOL=2 GFS_META_OPS=1 \
    ./target/release/examples/partv_perf_verify > /tmp/agg_$i.log 2>&1 ) &
done
wait
grep -h 'reads,' /tmp/agg_*.log | sed -E 's/.*→ ([0-9]+) MiB.*/\1/' \
  | awk '{s+=$1} END{printf "aggregated PR ≈ %d MiB/s = %.2f GiB/s\n", s, s/1024}'
```

---

## Common Issues

- **Read error 'block not found' / cache eviction**: the file is too large for the Worker cache (MUST_CACHE does not spill to UFS by default). Reduce `GFS_SIZE_MB`, or shrink the per-process file when running multiple processes in parallel.
- **Process hangs and won't exit**: use the watchdog loop above as a fallback; normal multi-chunk sequential reads won't hang (the early ACK-merge deadlock has been fixed, with per-chunk ACK by default).
- **Low numbers on localhost**: the local loopback goes through the full gRPC data plane (encode → TCP → decode → copy), not FUSE short-circuit; remote clusters gain more from R3/Worker pools under RTT.
- **Remote stress testing**: just set `GFS_ADDR=<remote-master>:9200`; the rest of the parameters stay the same.

---

## `master_hotpath` — GetFileStatus Hot-Path Micro-Benchmark (criterion)

Statistical micro-benchmark that compares Master metadata hot-path optimizations (`ArcSwap<AuthedState>` / counter caching as a field / path move). Uses the criterion harness (`harness = false`):

```bash
cargo bench --bench master_hotpath
```

---

## `repro_concurrent_write` — Concurrent-Write Reproduction

```bash
cargo run --release --example repro_concurrent_write
```


