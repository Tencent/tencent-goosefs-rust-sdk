# Short-Circuit Read: `io_uring` Backend Feasibility Analysis

> Companion to [`SHORT_CIRCUIT_DESIGN.md`](./SHORT_CIRCUIT_DESIGN.md).
> Scope: whether the current whole-block-mmap short-circuit (SC) data plane
> in `src/block/short_circuit/reader.rs` can (or should) be extended with an
> `io_uring`-based backend, and what such an extension would look like.
>
> Status: **analysis only, not scheduled**. No code change is proposed by
> this document. Adopt only when a workload actually saturates the mmap
> backend and can quantify the win.

---

## 1. TL;DR

| Question | Answer |
|---|---|
| Does the current SC data plane benefit from `io_uring`? | **No.** SC reads are already zero-syscall (mmap slices); `io_uring` has nothing to shave off. |
| Does the Worker (Java server) need any change? | **No.** `OpenLocalBlock` only exchanges a filesystem `path` + `block_size`; the Worker is agnostic to the client-side I/O backend. |
| Is `io_uring` ever useful for SC? | **Only** for narrow workloads: huge blocks, extremely sparse positioned reads with `O_DIRECT` (page-cache bypass), or callers that always memcpy out of the mapping anyway. |
| If we do it, what is the shape? | Add a **second, opt-in backend** behind a `LocalBlockBackend` enum in `LocalBlockReader`; keep mmap as the default; reuse the entire INV-D1..D4 / INV-S1..S5 regression suite. |

---

## 2. Current SC Data Plane (baseline)

Verified against [`src/block/short_circuit/reader.rs`](../src/block/short_circuit/reader.rs)
and the server-side [`ShortCircuitBlockReadHandler.java`](https://github.com/tencent/goosefs)
(`core/server/worker/.../grpc/ShortCircuitBlockReadHandler.java`).

### 2.1 Control plane

`OpenLocalBlock` bidi RPC does **only three things** on the Worker:

1. `mWorker.lockBlock(sessionId, blockId)` — take a block-level read lock.
2. `mWorker.readBlock(...)` — resolve the `BlockMeta`.
3. Return `OpenLocalBlockResponse{ path, blockSize }`.

The Worker never touches a byte of block data. It has no notion of what the
client will do with `path` afterwards.

### 2.2 Data plane (client-side)

- Open: `File::open(path)` → `unsafe { Mmap::map(&file) }` (whole-block
  read-only) → `drop(file)` → `madvise(hint)`.
- Read (`read` / `read_bytes` / `read_to_slice`): pure pointer arithmetic on
  `self.mmap[off..off+len]`. **Zero syscalls per read.**
- Prefetch (`prefetch` / `prefetch_many`): `madvise(MADV_WILLNEED)`, one
  syscall per coalesced range.
- Close: `Drop` runs `munmap` then closes the gRPC session (releases the
  Worker-side read lock).

This is a **path-based short-circuit** (HDFS terminology: "legacy SCR" as
opposed to "SCR with `SCM_RIGHTS`"). The client owns the fd; the Worker
owns the lock; nothing else crosses the boundary.

---

## 3. What Could `io_uring` Replace?

Mapping every `io_uring` capability against the current SC path:

| `io_uring` capability | What it would replace | Verdict |
|---|---|---|
| Batched `OP_READ` / `OP_READV` (fewer syscalls) | Nothing — SC has no `read`/`pread` on the hot path | **No gain** |
| `OP_READ_FIXED` (registered buffers) | Nothing — mapping bypasses user buffers entirely | **No gain** |
| Async I/O (avoid `spawn_blocking`) | Nothing — page faults are handled by the kernel synchronously; `io_uring` cannot async-ify a page fault on a mapping | **No gain** |
| `OP_MADVISE` / `OP_FADVISE` | The current per-range `madvise(WILLNEED)` calls | Marginal (1 syscall/range → batched) |
| `OP_READAHEAD` (kernel-level readahead into page cache) | Roughly what `MADV_WILLNEED` already achieves | Roughly equivalent |
| Registered files (`IORING_REGISTER_FILES`) | Nothing — the fd is dropped after `mmap` | **Not applicable** |
| SQPOLL (kernel-side polling) | Nothing — data path is already zero-syscall | **No gain** |

**Conclusion.** Under the "whole-block mmap + slice" model, `io_uring` has
no meaningful surface to optimize. To make `io_uring` matter, the data
model itself has to change.

---

## 4. When `io_uring` Actually Helps

Two workloads plausibly benefit — both require **replacing** mmap with
`pread`-style access (buffered or `O_DIRECT`) at least on the SC path:

### 4.1 Huge blocks + extremely sparse positioned reads

Symptoms:

- Blocks in the hundreds of MiB range.
- Access pattern is sparse point-reads (e.g. Lance IVF-PQ code lookup,
  vector index shard probing).
- Cold reads currently pay a synchronous **major fault** on the reader
  thread; `MADV_WILLNEED` is fire-and-forget with no completion signal.

`io_uring` fit:

- Batch N `OP_READ`s in one `io_uring_enter`; get precise completions.
- Combine with `O_DIRECT` to skip the page cache entirely — avoids
  double-caching against the UFS/page-cache tier and eliminates
  fault-driven latency spikes on a hot NUMA node.
- Cap concurrency via a bounded queue depth per reader.

### 4.2 Throughput reads that always memcpy out anyway

If callers always copy the mmap slice into a private, aligned buffer
(e.g. `read_to_slice` heavy paths, format-specific decoders that need a
contiguous decoded buffer), the "mmap → memcpy" two-hop can be collapsed
into a single "pread directly into the target buffer" using
`OP_READ_FIXED` with registered buffers. On NVMe this typically saturates
device bandwidth better than mmap under page-cache pressure.

**If neither profile matches the target workload, `io_uring` is a
distraction.**

---

## 5. Server-side Impact

**None.** The Worker requires zero changes.

The `OpenLocalBlock` protocol is path-based (§2.1). Concretely:

| Concern | Why it's a client-only change |
|---|---|
| File permissions | Whatever `uid/gid/mode` allows `File::open(path)` today allows `pread`/`io_uring` too. No new server-side ACL. |
| Lock lifetime | Worker holds the block read lock for the lifetime of the bidi stream (session). This is unchanged: the client's `OpenLocalBlockGuard` still bounds the reader's lifetime; the choice of I/O backend does not touch the stream. |
| fd retention | mmap drops the fd after mapping; `io_uring` must retain the fd (or use registered files) until the reader is dropped. This is a purely client-side struct-field concern. |
| Alignment (`O_DIRECT`) | Block files on the Worker are already sequential regular files; the file's starting offset is naturally 4 KiB aligned. Any tail padding / read alignment is handled entirely by the client. |
| Rolling upgrade | Because Worker is unaware of the client backend, old (mmap) and new (uring) clients can coexist against the **same** cluster and even the **same** block file (the block read lock supports concurrent readers). No proto version bump, no capability negotiation. |

This is the single strongest argument for adding `io_uring` as a **second
backend rather than a replacement**: the change is local to
goosefs-client-rust and its regression suite.

---

## 6. Proposed Shape (if we decide to build it)

Not scheduled. Documented so a future implementor does not have to
re-derive the boundaries.

### 6.1 Backend abstraction

Inside `LocalBlockReader` (`src/block/short_circuit/reader.rs`), replace
the concrete `mmap: Arc<Mmap>` field with:

```rust
enum LocalBlockBackend {
    Mmap(Arc<Mmap>),                  // existing, default
    #[cfg(target_os = "linux")]
    IoUring(Arc<UringPreadBackend>),  // new, opt-in
}
```

- Public API (`read`, `read_bytes`, `read_to_slice`, `prefetch`,
  `prefetch_many`, `bounds_check`, `file_size`) stays identical.
- Upstream call sites (`factory.rs`, `BlockInStream`,
  `GoosefsFileReader`) do not need to change.
- The choice is per-reader, driven by config (§6.4). A reader that fails
  to construct on the `IoUring` path falls back to `Mmap` transparently
  (still respecting INV-S1: any recoverable SC error falls further back
  to gRPC).

### 6.2 Crate choice

| Crate | Fit | Notes |
|---|---|---|
| `tokio-uring` | Poor | Requires its own runtime, incompatible with the existing `tokio` multi-thread runtime this crate uses. |
| **`io-uring`** (tokio-rs) | **Recommended** | Low-level SQE/CQE access; runs on a dedicated driver thread, bridges completions back to `tokio` via `oneshot` channels. |
| `glommio` / `monoio` | No | thread-per-core model conflicts with the crate's runtime layout. |

### 6.3 Data-plane sketch

- **Open**: `File::open(path, O_RDONLY | (O_DIRECT if configured))`;
  register the fd via `IORING_REGISTER_FILES`; pre-allocate an aligned
  buffer pool via `IORING_REGISTER_BUFFERS`.
- **`read_bytes(off, len)`**: acquire a registered buffer, submit
  `OP_READ_FIXED`, wait for CQE, wrap the buffer in `Bytes` whose owner
  returns the buffer to the pool on drop.
- **`read_to_slice(off, dst)`**: submit directly against `dst` when
  alignment permits; otherwise stage through a registered buffer + one
  memcpy.
- **`prefetch(off, len)`**: `OP_READAHEAD` (kernel ≥ 5.6), never occupies
  a user buffer.
- **`prefetch_many`**: one `io_uring_enter` for the whole coalesced batch.

### 6.4 Configuration

Reserved keys (do not add to `PropertyKey` until implementation):

```
goosefs.client.short.circuit.backend           = mmap | uring        # default: mmap
goosefs.client.short.circuit.uring.direct      = true | false        # default: false
goosefs.client.short.circuit.uring.queue.depth = <N>                 # default: 128
goosefs.client.short.circuit.uring.buffer.count = <N>                # default: 64
goosefs.client.short.circuit.uring.buffer.size  = <bytes>            # default: 1 MiB
```

### 6.5 Consistency regression

The existing SC regression suite must pass unchanged with the uring
backend enabled:

- [`tests/sc_consistency.rs`](../tests/sc_consistency.rs)
- [`tests/sc_inv_s3.rs`](../tests/sc_inv_s3.rs)
- [`tests/short_circuit_e2e.rs`](../tests/short_circuit_e2e.rs)

Backend-specific pitfalls to audit before merging:

| Invariant | Uring-specific risk |
|---|---|
| INV-D2 (logical `[..file_size]` window) | `O_DIRECT` alignment must not leak physical tail bytes past `file_size`. |
| INV-D3 (`Bytes` outlives reader) | mmap relies on `Arc<Mmap>`; uring must keep buffer refcounted so a dropped reader does not return a live `Bytes`'s buffer to the pool. |
| INV-S4 (`OutOfRange` is semantic) | `bounds_check` must run **before** submission; a uring `-EINVAL` from mis-alignment must not be reported as `OutOfRange`. |
| INV-S1 (fallback safety) | Any uring I/O error is a *recoverable* SC error and must fall back to gRPC — never propagate as a data-plane error. |

---

## 7. Recommendation

1. **Do not implement now.** The current on-CPU flame graph
   ([`docs/perf/2026-07-06-oncpu-goose-vs-local/README.md`](./perf/2026-07-06-oncpu-goose-vs-local/README.md))
   shows the SC data plane is already close to the theoretical floor;
   the wins on the table are in the gRPC / control-plane path, not here.
2. **Prerequisites for revisiting.** A workload that (a) actually
   saturates the mmap backend, (b) matches §4.1 or §4.2, and (c) can
   quantify a target speedup vs. mmap on the same block sizes.
3. **When we do implement, keep mmap.** Ship uring as a second backend
   behind an off-by-default flag; go through the full regression matrix
   above before flipping the default anywhere.
4. **Server team is not on the critical path.** The Worker requires zero
   changes; this stays a client-only initiative.

---

## 8. Cross-references

- [`SHORT_CIRCUIT_DESIGN.md`](./SHORT_CIRCUIT_DESIGN.md) — SC contract,
  invariants, control-plane details.
- [`PAGE_CACHE_VS_SHORT_CIRCUIT.md`](./PAGE_CACHE_VS_SHORT_CIRCUIT.md) —
  when SC applies vs. page cache.
- [`perf/2026-07-06-oncpu-goose-vs-local/README.md`](./perf/2026-07-06-oncpu-goose-vs-local/README.md)
  — most recent on-CPU comparison; establishes that SC is not currently
  the bottleneck.
- Server-side handler: `core/server/worker/src/main/java/com/qcloud/cos/goosefs/worker/grpc/ShortCircuitBlockReadHandler.java`
  in the `goosefs` (Java) repository.
