# Page Cache vs Short Circuit Read in goosefs-client-rust

> Project: `goosefs-client-rust`
> Related designs:
> - [`CLIENT_PAGE_CACHE_DESIGN.md`](./CLIENT_PAGE_CACHE_DESIGN.md)
> - [`SHORT_CIRCUIT_DESIGN.md`](./SHORT_CIRCUIT_DESIGN.md)
> Last updated: 2026-06-24

This document explains the difference between the **client-side Page Cache** and
the **Short Circuit (SC) read** path, why both exist, and when each one should
be used. They sit at **different layers** of the read stack and are
**complementary**, not mutually exclusive.

---

## 1. TL;DR

| Dimension | Page Cache (client-side) | Short Circuit (SC) |
|---|---|---|
| Essence | Cache a **copy** of remote data on the local client disk | **Bypass gRPC** and `mmap` the Worker block file directly |
| Problem solved | Avoid re-fetching the **same data** over the network | Avoid gRPC overhead when the Worker is on the **same host** |
| Data source | First fetched via the normal read path, then persisted locally | The local Worker's already-cached block file |
| Deployment requirement | Works regardless of where the Worker is | Client and Worker **must be co-located** on the same host |
| Storage medium | Local disk on the client (e.g. `/tmp/goosefs_cache`) | Worker block file (shared inode via `mmap`) |
| Granularity | Fixed-size **page** (default 1 MiB) | Whole **block** reader (default 64 / 512 MiB) |
| Extra space cost | Yes — client keeps a second copy | No — `mmap` shares the Worker's inode |
| Speeds up first read of cold data | No (still has to fetch from origin) | Yes (first read is already a local `mmap`) |
| Speeds up repeated reads of hot data | Yes (very strong, in-process) | Yes (relies on the OS page cache) |
| Failure mode | Best-effort: miss / error falls back transparently | Must fall back to gRPC, with strict consistency invariants (INV-D1~D4 / INV-S1~S5) |

One-line summary:

> **Page Cache** solves *"I already pulled this data once over the network,
> don't pull it again."*
> **Short Circuit** solves *"the data is already on this same machine in the
> Worker, don't go through gRPC at all."*
>
> One is **cross-node caching**; the other is **same-host zero-copy**. They
> address different layers and can coexist.

---

## 2. Where each one sits in the read stack

### 2.1 Baseline path (no optimization)

```
Client ──gRPC──▶ Worker ──UFS / local disk──▶ block data
```

### 2.2 Page Cache hit

```
Client ──read local cache file──▶ return (never leaves the process)
```

### 2.3 Page Cache miss

```
Client ──gRPC──▶ Worker ──▶ data returned ──▶ also written back into the local cache
```

### 2.4 Short Circuit

```
Client ──gRPC (control plane only: OpenLocalBlock)──▶ Worker (returns block file path)
Client ──mmap of the local block file──▶ &[u8] / Bytes (zero syscall, zero copy on the data plane)
```

### 2.5 Combined (both enabled)

```
GoosefsFileInStream::read_at(offset, n)
  ├─ Page Cache hit?  ─ yes ─▶ return (fastest)
  └─ miss
       └─ resolve external read range
            ├─ Worker is local?  ─ yes ─▶ Short Circuit (mmap)
            └─ Worker is remote ────────▶ gRPC ReadBlock
       └─ asynchronously back-fill into the Page Cache
```

So:
- 1st read: page cache miss → SC if co-located → back-fill page cache.
- 2nd read: page cache hit → SC is not even invoked.

---

## 3. Key differences in detail

### 3.1 Network layer being eliminated

- **Page Cache** removes the cost of *repeatedly* hitting a **remote** Worker
  (network → local disk).
- **Short Circuit** removes the cost of cross-process **gRPC on the same host**
  (gRPC serialization / network stack → raw pointer into `mmap`).

### 3.2 Co-location requirement

- **Page Cache**: not required. The Worker can be anywhere; we just cache the
  bytes that came back.
- **Short Circuit**: hard requirement. The router decides via
  `WorkerRouter::is_local_worker(id)`, and the Worker must already have the
  block on local disk. If either condition fails, SC is skipped and the read
  falls back to gRPC.

### 3.3 Correctness model

- **Page Cache**: best-effort. A miss, a corrupted entry, or any I/O error
  simply falls back to the origin read path. Correctness is "do no harm".
- **Short Circuit**: must satisfy strict invariants documented in
  `SHORT_CIRCUIT_DESIGN.md` §1.3:
  - **INV-D1**: Worker does not truncate / replace / rewrite a block while
    a reader holds the lock.
  - **INV-D2**: bytes returned from the `mmap` slice equal bytes returned by
    a Worker-side `pread` of the same range.
  - **INV-D3**: `Bytes` returned from `read_bytes` keeps its backing `mmap`
    alive via `Arc<Mmap>`.
  - **INV-D4**: `prefetch` / `prefetch_many` are pure `MADV_WILLNEED` hints,
    they never modify or return bytes.
  - **INV-S1..S5**: SC↔gRPC fallback is byte-for-byte equivalent, the
    OpenLocalBlock lock is held for the reader's lifetime, capability auth is
    consistent, error classification is stable, and the three read APIs return
    identical content for identical `(offset, len)` inputs.
- **SIGBUS** (Worker truncating a block while it is mapped) is treated as a
  protocol violation: the SIGBUS handler logs diagnostics and `abort`s rather
  than returning torn or stale bytes. Deployments on untrusted file systems
  should switch to `io.mode=pread` instead of relying on `mmap`.

### 3.4 Space amplification

- **Page Cache**: extra disk space on the client (default budget per cache
  directory, e.g. 512 MiB). Eviction is LRU/LFU with TTL and quotas.
- **Short Circuit**: zero extra space — the `mmap` shares the Worker's
  existing inode.

### 3.5 Granularity and reuse

- **Page Cache**: page-level (1 MiB), with full eviction policy, TTL, per-dir
  quotas, multi-tier (memory + disk).
- **Short Circuit**: block-level reader reuse via a per-task LRU; eviction of
  the underlying file is the Worker's responsibility.

### 3.6 Threading model interaction

- **Page Cache** sits above the read path in the client; reads from local
  cache files use the standard async I/O machinery.
- **Short Circuit** is `mmap`-based. The data plane is a synchronous slice
  on the calling thread. This is fast on **page-cache-hot** data (minor page
  fault, microsecond range), but a **major page fault on cold data does
  synchronous disk I/O on a tokio worker thread**. The SC design therefore
  requires either:
  1. `prefetch` / `prefetch_many` to bring pages resident before handing
     `&[u8]` / `Bytes` to async consumers, or
  2. running the touching read inside `spawn_blocking` (gated by
     `goosefs.client.short.circuit.open.blocking` or caller policy).

---

## 4. When to use which

### 4.1 Use **Page Cache** when

- AI training / analytics pipelines that read the same data across **multiple
  epochs** — the second epoch onward is essentially network-free.
- High-concurrency reads of **hot small files** that the client process can
  absorb locally.
- Random small I/O on columnar formats (Parquet / ORC) — page granularity
  fits naturally.
- **Client and Worker are on different hosts** — this is the scenario where
  SC cannot help but Page Cache still fully applies.
- Network is the bottleneck (cross-DC, slow links, remote UFS).

### 4.2 Use **Short Circuit** when

- Client and Worker are deployed on the same host (typical compute–storage
  co-located setup, e.g. training nodes also running a Worker).
- The block is already hot in the Worker's OS page cache.
- High-concurrency PositionedRead workloads — the SC design targets
  `≥ Java × 5` throughput and `p99 ≤ Java / 10`.
- Large sequential reads or columnar random reads — `mmap` plus
  `MADV_RANDOM / SEQUENTIAL / WILLNEED` hints clearly outperform gRPC.
- Latency-sensitive paths — an SC hit can serve in `< 50 µs`
  (page fault + memcpy only).

### 4.3 Combining both

Page Cache and Short Circuit live at different layers and **stack cleanly**:
the first read fills the Page Cache (using SC underneath if co-located), and
subsequent reads of the same range short-circuit even further by hitting the
client-local cache without invoking the SC reader at all.

---

## 5. Decision matrix

| Deployment / workload | Recommendation |
|---|---|
| Client and Worker co-located | **Enable SC**. Enable Page Cache too if the workload re-reads data. |
| Client and Worker on different hosts | SC is unusable (auto fallback to gRPC). **Strongly enable Page Cache**, especially for multi-epoch / hot files. |
| AI training (multi-epoch) + co-located | **Enable both**: SC accelerates the cold first epoch, Page Cache serves subsequent epochs. |
| One-shot scan, almost no re-reads | SC depending on deployment. Page Cache offers limited benefit; can stay off to avoid disk usage. |
| Memory- or disk-constrained edge clients | Disable Page Cache (avoid space amplification). Keep SC if applicable (zero extra space). |
| Untrusted / unstable filesystem | Prefer `io.mode=pread` over `mmap`-based SC, or disable SC entirely. Page Cache is unaffected. |

---

## 6. Quick reference

| Question | Answer |
|---|---|
| Does Page Cache require a local Worker? | No. |
| Does Short Circuit require a local Worker? | Yes. |
| Does Page Cache use extra disk space? | Yes (configurable budget). |
| Does Short Circuit use extra disk space? | No (shared inode via `mmap`). |
| Does Page Cache speed up cold first reads? | No. |
| Does Short Circuit speed up cold first reads? | Yes, when the Worker is local. |
| Do they conflict? | No. They operate at different layers and can be enabled together. |
| What happens on failure? | Page Cache: silent fallback to origin. SC: fallback to gRPC under strict consistency invariants. |

---

## 7. References

- [`CLIENT_PAGE_CACHE_DESIGN.md`](./CLIENT_PAGE_CACHE_DESIGN.md) — full design
  of the client-side Page Cache (granularity, eviction, tiering, back-fill).
- [`SHORT_CIRCUIT_DESIGN.md`](./SHORT_CIRCUIT_DESIGN.md) — full design of the
  Rust SC path (mmap strategy, `madvise` matrix, three-tier prefetch model,
  consistency invariants, fallback rules).
- [`CLIENT_CONFIGURATION.md`](./CLIENT_CONFIGURATION.md) — client
  configuration options for both features.
