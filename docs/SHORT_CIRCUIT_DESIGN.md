# Rust Short-Circuit Design and Implementation (Rust SC Short-Circuit Variant)

> Project: `goosefs-client-rust`
> Goal: Under the precondition of functional equivalence with the Java `LocalFileDataReader` / `LocalFileBlockReader`, **strictly outperform** the Java SC implementation (especially achieving a 5×~50× throughput improvement under high-concurrency PositionedRead scenarios).
> Applicable version: from the `feature/short-circuit` branch.
> Revision date: 2026-06-24

---

## 0. TL;DR (Core conclusions on one page)

| Dimension | Java SC | Rust SC (this design) | Benefit direction |
|---|---|---|---|
| Control plane (OpenLocalBlock bidi) | gRPC | gRPC (identical) | Equal |
| Block file open | RandomAccessFile + FileChannel | File → mmap then immediately `drop(File)` | Saves 1 fd / block |
| mmap granularity | per-chunk: FileChannel.map(off,len) | once-per-block: Mmap::map(&file) | Saves N-1 mmap syscalls |
| Kernel readahead (L1) | System default (wasted under PR) | MADV_RANDOM / SEQUENTIAL / NORMAL switched by scenario | Saves memory bandwidth |
| Application-layer prefetch (L2) | **None** (SC path ignores prefetchWindow) | `prefetch` / `prefetch_many` → MADV_WILLNEED | Cold-data p99 improved by tens of × |
| Data lending | MappedByteBuffer → NioDataBuffer wrapper | &[u8] borrow + Bytes zero-copy | True zero-copy |
| Thread model | Blocking IO (GrpcBlockingStream) | tonic async + synchronous mmap direct call (no spawn_blocking) | Saves thread switching |
| Lock lifetime | Factory.close closes the stream | OpenLocalBlockGuard Drop closes the stream | RAII, leak-tolerant |
| Fault tolerance | NotFoundException → fallback gRPC | Err → fallback gRPC + negative cache entry | Fast even on failure |
| Reuse | per-BlockInStream reader | per-task block-id LRU cache | No re-creation on re-read |
| Observability | Java metrics | tracing + Prometheus + counters | Equal or finer |
| Large page | Not supported | THP via `MADV_HUGEPAGE` opt-in (>=2MB block, benefit depends on kernel/FS) | May reduce TLB misses |

Expected end-to-end gains (256 concurrency × 64KB PositionedRead, 1GB block):

- mmap syscalls: Java ≈ 16k/s, Rust ≈ 0
- Userspace copy: Java adds 64KB per read; Rust 0 (caller consumes &[u8] directly)
- p99 latency: Java 800µs~3ms (mmap jitter) → Rust < 50µs (pure page-fault + memcpy)
- Throughput ceiling: bounded by memory bandwidth / page cache hit rate; Rust can saturate single-NUMA-node bandwidth

---

## 1. Design goals and non-goals

### 1.1 Design goals (Must)

0. **Consistency first (highest priority, before performance and all other goals)**

   - **0a. Data Consistency**: the byte sequence the SC path returns for any `(offset, len)` must be **byte-for-byte identical** to the byte sequence read via the gRPC path for the same block at the same moment; any optimization path such as mmap, `MADV_WILLNEED`, LRU cache, `Bytes` zero-copy, HugeTLB, or SIGBUS fallback must **not** introduce: torn reads, stale reads, out-of-bounds reads, or cross-block string reads.
   - **0b. Semantic Consistency**: all observable behavior the SC path exposes to the upper layer (`BlockInStream` / `FileInStream`) must be **semantically equivalent** to the gRPC path, including but not limited to: same `(block_id, offset, len)` input yields the same success/error classification (NotFound, OutOfRange, Permission), EOF determination, short-read semantics, zero-length read handling, the Worker-side lock always held before reader Drop, capability auth effect, and SC→gRPC fallback being transparent to the caller without changing the `read` return-value sequence.

   Consistency is a **hard constraint**: any performance optimization in Chapter 3, if in conflict with 0a / 0b, must yield to consistency and abandon or downgrade that optimization; the invariant table in §1.3 is the verifiable refinement of 0a / 0b.

1. **Protocol compatibility**: fully compatible with the GooseFS Worker's existing OpenLocalBlock bidi gRPC protocol, requiring no Worker changes.
2. **Functional equivalence**: cover all legal paths of the Java SC — local decision, open-lock, read, unlock, failure fallback, capability auth.
3. **Strictly outperform Java**: must be no worse than, and target surpassing, on the following three benchmarks:
   - Sequential read (chunk_size=8MB): throughput ≥ Java × 1.2
   - PositionedRead (offset random, buf=64KB): throughput ≥ Java × 5
   - High concurrency (256 threads × same block): p99 latency ≤ Java / 10
4. **Zero extra unsafe surface**: all `unsafe` must have explicit SAFETY comments, covering SIGBUS, TOCTOU, and lifetime risks.
5. **Graceful degradation**: any SC failure must seamlessly fall back to gRPC, with the failure reason observable; the fallback switch must satisfy 0b semantic consistency.

### 1.2 Non-goals (Won't)

- No Unix Domain Socket data plane (tonic limitation; Java uses DS over netty, Rust does not introduce extra transport).
- No mmap write path (block file is immutable after CommitBlock; writes only go through the Worker).
- No replacement of Worker-side logic (pin, evict, commit remain the Worker's responsibility).
- No cross-process shared mmap table (each client process maps independently).

### 1.3 Global consistency invariants

The following invariants must hold on any code path; violating them is a correctness bug, with priority higher than performance regression; they are the verifiable refinement of goals 0a / 0b in §1.1, and are referenced and argued in §3 / §5.3 / §8.4.

| ID | Invariant | Category | Verification |
|---|---|---|---|
| INV-D1 | A block file's content is immutable for the lifetime of any reader (Worker holds the lock and never truncates / replaces / rewrites a sealed block); cross-reader sub-contract: after an overwrite, a freshly opened stream must immediately observe the new bytes and new length (no reuse of the v1 mmap / block-id view) | data | protocol contract + SIGBUS handler safety net + `sc_consistency::inv_d1_e2e_overwrite_visibility` |
| INV-D2 | The byte content of the mmap slice `&mmap[off..off+len]` = the byte content of a pread over the same block and range on the Worker side | data | `sc_consistency` test: SC vs gRPC dual-read diff |
| INV-D3 | The `Bytes` returned by `read_bytes` will not be munmap'd during its lifetime | data | `Arc<Mmap>` as owner, guaranteed by struct field Drop order |
| INV-D4 | `prefetch` / `prefetch_many` modify no bytes, they are only a readahead hint | data | `madvise(MADV_WILLNEED)` semantic guarantee + unit test |
| INV-S1 | After SC failure falls back to gRPC, the byte sequence read by the upper layer is identical to "always going through gRPC" | semantic | fault-injection test |
| INV-S2 | The Worker-side OpenLocalBlock lock is always held while the reader is alive; reader Drop triggers async unlock, worst case reclaimed by reaper / Worker session timeout, no infinite leak | semantic | field declaration order + Drop order review + reaper timeout (§8.2.1) |
| INV-S3 | capability auth semantics are exactly the same as the gRPC path (clusters with it enabled must reject requests lacking capability) | semantic | integration test coverage (`tests/sc_inv_s3.rs`, including SDK contract S3-a/S3-b strong checks + Worker-behavior S3-c observational probe) |
| INV-S4 | Stable error classification: semantic errors such as `OutOfRange` are not swallowed by fallback, must be propagated up | semantic | §3.6 decision matrix + unit test |
| INV-S5 | The three APIs `read` / `read_bytes` / `read_to_slice` return identical byte content for the same `(offset, len)` input | semantic | unit test three-path diff |

---

## 2. Architecture Overview

```
┌──────────────────────── Rust Client Process ────────────────────────┐
│                                                                       │
│  ┌────────────────┐   should_use_sc()   ┌──────────────────────────┐ │
│  │ BlockInStream  │ ───────────────────▶│  WorkerRouter            │ │
│  │  ::create()    │                     │   .is_local_worker(id)   │ │
│  └────────────────┘                     └──────────────────────────┘ │
│         │ yes                                                         │
│         ▼                                                             │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ ShortCircuitFactory (per task / per FileInStream)               │ │
│  │  ┌────────────────────────┐   ┌──────────────────────────────┐ │ │
│  │  │ LRU<block_id, Reader>  │   │ NegativeCache<block_id, t>   │ │ │
│  │  │  (hot block reuse)     │   │  (avoid re-trying SC for     │ │ │
│  │  └────────────────────────┘   │   recently-failed blocks)    │ │ │
│  │                               └──────────────────────────────┘ │ │
│  │                  open() if miss                                  │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│         │                                                             │
│         ▼                                                             │
│  ┌──────────────────────────────────────────────────────────────────┐│
│  │ LocalBlockReader  (single block, lifetime = read session)        ││
│  │   ┌─────────────────────────────┐  ┌───────────────────────────┐ ││
│  │   │ OpenLocalBlockGuard         │  │ Arc<Mmap>  (whole block)  │ ││
│  │   │  (bidi stream Sender alive) │  │  + advise(MADV_RANDOM)    │ ││
│  │   └─────────────────────────────┘  └───────────────────────────┘ ││
│  │                                                                   ││
│  │   read(off,len) -> &[u8]      (no syscall, pure slice)           ││
│  │   read_bytes(off,len) -> Bytes (zero-copy, ref-counted)           ││
│  └──────────────────────────────────────────────────────────────────┘│
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
              │ gRPC (control plane only)
              ▼
        GooseFS Worker  (locks block, returns path, unlocks on stream close)
```

Key invariants:

- A block is mmap'd **at most once** during the Reader's lifetime.
- The Drop order of `OpenLocalBlockGuard` is **later than** the Drop of `Mmap` (guaranteed by struct field order) — guaranteeing the lock is held throughout the mmap's validity (corresponds to INV-S2).
- `Arc<Mmap>` can be shared by multiple concurrent `read_bytes` calls, but the guard is held only once (corresponds to INV-D3).
- **Hard consistency constraint**: all data/semantic paths must satisfy INV-D1~D4 and INV-S1~S5 of §1.3; all subsequent performance optimizations in this chapter are argued under these invariants as a premise.

---

## 3. Item-by-item comparison and improvements vs Java

### 3.1 Control plane (OpenLocalBlock)

| Item | Java | Rust SC |
|---|---|---|
| Protocol | bidi gRPC | bidi gRPC (identical) |
| Client | GrpcBlockingStream (blocking) | tonic async + mpsc::Sender keeps the sender alive |
| Request fields | block_id, block_size, capability? | all fields identical; capability must be supported (Rust SC fills this in) |
| Lock release | Factory.close() → mStream.close() + waitForComplete (**synchronous wait**) | OpenLocalBlockGuard Drop → sender closed → Worker onCompleted (**async / eventual release**, with reaper + Worker session timeout as fallback, see §8.2.1) |
| Timeout | USER_STREAMING_DATA_TIMEOUT | same-named config, default 30s, async tokio::time::timeout |

**Rust SC improvements**:

1. **capability injection**: the early prototype and the existing read path both set `capability: None` (verified fact: `worker.rs`'s `read_block`/`write_block` hardcode `capability: None` when constructing `ReadRequest`/`WriteRequest`, L383/L425/L477). The SC path must carry a valid capability on capability-enabled clusters, otherwise the Worker rejects it and falls back to gRPC, wasting one RTT.
   > **⚠️ The capability source does not yet exist; it is a P3 to-do**: dev's current `InStreamOptions` (`fs/options.rs` L79) only has `read_type` / `position_short` / `max_ufs_read_concurrency` / `prefetch_window`, **no `capability_fetcher` field**, and the entire client read path has not wired in `Capability`. Therefore "take a capability from somewhere and fill it in" is **a capability that needs to be newly added** — we cannot copy an API that does not exist. When landing (§10 P3, together with `worker.rs`'s `open_local_block` wrapper), the source must be determined first — candidates are `FileSystemContext` / auth config (see `auth/mod.rs`'s `CAPABILITY_TOKEN` and `config.rs`'s capability-related TODO); until the source is determined, the interface must not assume it already exists.
   >
   > **Empirical verdict (2026-06-25, local SIMPLE + `goosefs.security.authorization.capability.enabled=true` cluster)**: `tests/sc_inv_s3.rs::inv_s3_c_probe_capability_enforcement` outputs `ACCEPTED — Worker did not reject capability=None on this auth mode` (`OPEN_SUCCESS +1, OPENLOCAL_FAIL +0, FILE_OPEN_FAIL +0, MMAP_FAIL +0, READ_CALLS +10`). Conclusion: **under SIMPLE auth, even with the capability switch on, the Worker still admits OpenLocalBlock requests with an empty capability field**; mandatory capability validation only truly rejects under an auth mode with a BlockKey signature chain (e.g. KERBEROS). In the current deployment shape, SC works without a `CapabilityProvider`, and the INV-S1/S3-b byte contract holds; switching to KERBEROS will make `inv_s3_a_sc_engages_when_capability_not_enforced` legitimately fail, forcing ops to either wire in a real provider or explicitly turn off the SC switch, eliminating "silent SC".
2. **Lock visibility**: the guard embeds a Drop timestamp into the tracing span, easing "lock not released" debugging.
3. **Parallel multi-RPC**: when the upper layer needs multiple blocks at once (vectorized batch read), provide `open_local_blocks_batched(Vec<id>) -> Vec<Result<Reader>>`, running N bidi streams concurrently and compressing N RTTs into 1 (Java has no such capability).

### 3.2 Data plane (mmap strategy) — core optimization

#### Java behavior (verified fact)

```java
// LocalFileBlockReader.java:97
public ByteBuffer read(long offset, long length) {
    return mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
}
```

```java
// LocalFileDataReader.java:67
public DataBuffer readChunk(int prefetchWindow) {
    ByteBuffer buffer = mReader.read(mPos, Math.min(mChunkSize, mEnd - mPos));
    return new NioDataBuffer(buffer, buffer.remaining());
}
```

→ one mmap syscall per chunk (chunk default 8MB).
→ 1GB block sequential read ≈ 128 mmaps;
→ but in PositionedRead mode chunk == buf size (e.g. 64KB), 1GB ≈ **16k mmaps**;
→ 256-concurrent hot-block sharing scenario: **millions of mmap/s**, page-table lock + VMA red-black tree become the bottleneck.

#### Rust SC behavior

```rust
// open() once:
let mmap = unsafe { Mmap::map(&file) }?;       // 1 mmap
mmap.advise(Advice::Random)?;                  // 1 madvise (L1: disable kernel readahead)
drop(file);                                    // fd released immediately, inode held by VMA

// optional: once the upper layer has the PR offset list, prefetch in batch (L2, async)
reader.prefetch_many(&[(off1,len1), (off2,len2), ...])?;  // 1 madvise per adjacent segment

// read() N times:
&self.mmap[offset..offset+len]                 // 0 syscalls, pure pointer arithmetic
```

| Metric | Java | Rust SC | Ratio |
|---|---|---|---|
| mmap syscalls / 1GB sequential read | 128 | 1 | 128× |
| mmap syscalls / 1GB PR @ 64KB | 16,384 | 1 | 16,384× |
| VMA count / reader | equal to read count | 1 | N× |
| fd usage / reader | 1 | 0 (after drop) | saves 1 |
| page-table lock contention | high | nearly zero | significant |
| readahead strategy | system default (sequential-friendly, wasted under PR) | MADV_RANDOM disables readahead | saves memory bandwidth |

#### Risks and mitigations

| Risk | Description | Rust SC mitigation | Consistency impact |
|---|---|---|---|
| **SIGBUS** | Worker truncates/unlinks the replaced inode while the client holds the mmap | The real foundation of data consistency is INV-D1 (Worker does not rewrite/truncate the sealed block during the lock), so under the normal path SIGBUS **should not happen**.<br>**Not relying on** "signal-to-panic + catch_unwind" — SIGBUS occurs at arbitrary faulting instructions such as libc `memcpy`; doing a Rust unwind from an async signal handler through the signal trampoline / libc stack frames is UB, and `catch_unwind` cannot catch signals.<br>Mitigation is layered: 1) register a SIGBUS handler that **only does diagnostics (records block_id/addr/tracing) then `abort`s**, exposing "protocol broken" as a fatal error rather than silently mis-reading; 2) for deployments needing robustness on untrusted FS, use `io.mode=pread` (§11.4) to take the `pread64` data plane, avoiding mmap page-fault SIGBUS at the source; 3) (optional, advanced) if soft recovery under mmap mode is mandatory, only wrap the "pure memcpy" snippet with per-thread `sigsetjmp/siglongjmp`, and it still cannot protect the caller's direct consumption of `&[u8]` — high cost, limited benefit, disabled by default | Related to INV-D1: consistency is guaranteed by "Worker does not rewrite the locked block", not by catching SIGBUS. The `io.mode=pread` robust path still satisfies INV-S1 (equivalent to full gRPC); under mmap mode SIGBUS = fatal — prefer abort over returning torn/stale bytes |
| **Virtual address exhaustion** | 64-bit Linux 128TB VA, theoretically safe; 32-bit unsupported | Documentation explicitly states 64-bit Linux/macOS only | None |
| **RSS bloat** | many hot blocks all touch pages | LRU cache cap + idle TTL; Drop triggers munmap | LRU eviction must truly munmap only after all references (including `Bytes` clones) are released, guaranteed by `Arc<Mmap>` (INV-D3) |
| **NFS / slow disk** | blocks the worker thread on page fault | only enabled when is_local_worker == true; NFS remote blocks do not take this path | None |

### 3.2.1 Three-layer prefetch model (missing in Java SC, fully covered by Rust SC)

Prefetch on the Rust SC path is divided into three layers, which must be placed in their respective positions and scheduled by scenario. Java SC on the `LocalFileDataReader` path is **entirely missing** — the passed `prefetchWindow` parameter is directly ignored, and only takes effect on the `GrpcDataReader` remote path.

| Layer | Name | Trigger subject | Mechanism | Rust SC expression | Java SC | Applicable scenario |
|---|---|---|---|---|---|---|
| L1 | Kernel readahead | Linux page cache | `mmap` default behavior + `madvise` hint | `AccessHint::{Sequential, Random, Default}` → `MADV_*` | None (FileChannel.map exposes no madvise) | Sequential read amplifies readahead window; PR disables readahead |
| L2 | Application-layer prefetch | Client caller | `madvise(MADV_WILLNEED)` asynchronously triggers readahead | `prefetch(off,len)` / `prefetch_many(&[ranges])` | **None** (prefetchWindow ignored) | Cold-data PR with known offset list; streaming reader prefetches next chunk ahead |
| L3 | Upper-layer IO scheduling prefetch | Lance ReadBatch / `take()` | Business-side batch concurrency | Out of SC scope, implemented by caller | Same | Cross-block concurrent prefetch |

**L1 decision matrix**:

```
hint = match (workload, cfg.advise) {
    (PositionedRead, _)         => MADV_RANDOM,      // disable readahead, avoid wasting memory bandwidth
    (Sequential, _)             => MADV_SEQUENTIAL,  // readahead window ×2
    (Unknown, "none")           => no madvise,
    (Unknown, _)                => MADV_NORMAL,
}
```

**L2 value matrix**:

| Scenario | Call prefetch? | Expected benefit |
|---|---|---|
| Cold data + sequential chunk stream | Prefetch the next chunk while consuming the current one | Masks disk page-fault latency ≈ single-disk latency |
| Cold data + Lance `take(rows[])` with N known offsets | One-shot `prefetch_many(ranges)` | p99 improved 10× ~ 50× (measured, depends on disk type) |
| Hot data (page cache hit) | Just call; zero overhead | no-op (kernel short-circuits) |
| PR single point with unknown follow-up | Do not call | Avoids wasted mis-reads |

**L2 implementation notes**:

- `MADV_WILLNEED` is **asynchronous**: the kernel registers the readahead task and returns immediately without blocking the calling thread.
- `prefetch_many` internally coalesces adjacent `(offset, len)` ranges (merge & sort) to reduce the number of `madvise` calls.
- On hot pages it is a no-op: the kernel checks that the page is already present and skips it.
- Call cost: a single `madvise` typically takes < 5µs.
- **Consistency boundary (INV-D4)**: prefetch is only a readahead hint — it reads nothing, modifies nothing, and returns no bytes; its success or failure must not change the content returned by subsequent `read` calls. `prefetch` / `prefetch_many` silently degrade to no-op when disabled or when the FS does not support `MADV_WILLNEED`; data consistency is unaffected.

**Cross-platform differences (madvise matrix)**: the table above is based on Linux. On macOS, `MADV_RANDOM` / `MADV_SEQUENTIAL` / `MADV_WILLNEED` exist but have weaker semantics (the async readahead effect of `WILLNEED` is not guaranteed), and there is **no `MADV_HUGEPAGE` nor `MAP_HUGETLB`**. Therefore: `AccessHint::Default` (no madvise call) is the safe default across all platforms; the benefits of THP (§11.1) and cold-data prefetch only matter on Linux, while on macOS `prefetch` degrades to best-effort / possibly no-op, but still satisfies INV-D4.

### 3.3 Data lending / copying

Java:

```java
new NioDataBuffer(buffer, buffer.remaining());   // Wrapper, but downstream readBytes copies into a byte[]
```

Rust SC:

- `read(off,len) -> &[u8]`: pure borrow, zero-copy, lifetime bound to the reader.
- `read_bytes(off,len) -> Bytes`: wrap the mmap slice as a reference-counted `Bytes` via `bytes::Bytes::from_owner(Arc<Mmap>)`, **truly zero-copy** and able to cross `await` boundaries.
- `read_to_slice(off, dst)`: a single `copy_from_slice`, for the "upper layer must own the buffer" scenario.

**Consistency constraints (INV-D3 / INV-S5)**: the three APIs must return views with identical byte content for the same `(offset, len)` input; `read_bytes` extends the mapping lifetime to the last `Bytes` being dropped via `Arc<Mmap>`, guaranteeing the zero-copy owner always outlives the reference.

```rust
/// Newtype so that `Arc<Mmap>` can be handed to `Bytes::from_owner`
/// (which requires `AsRef<[u8]> + Send + 'static`). `Arc<Mmap>` itself
/// does NOT implement `AsRef<[u8]>`, hence the wrapper.
struct MmapChunk(Arc<Mmap>);
impl AsRef<[u8]> for MmapChunk {
    fn as_ref(&self) -> &[u8] { &self.0[..] }
}

pub fn read_bytes(&self, offset: usize, len: usize) -> Result<Bytes> {
    self.bounds_check(offset, len)?;
    // No unsafe: `Bytes::from_owner` (bytes >= 1.9) keeps the owner
    // (Arc<Mmap>) alive for as long as the returned Bytes (and any
    // clone / sub-slice) lives. We map the whole block once and then
    // narrow to the requested window with `.slice()`.
    let full = Bytes::from_owner(MmapChunk(Arc::clone(&self.mmap)));
    Ok(full.slice(offset..offset + len))
}
```

> Implementation note: this repo has pinned `bytes = "1.11.1"`, whose `Bytes::from_owner` (since 1.9) natively supports wrapping any `AsRef<[u8]> + Send + 'static` owner into a zero-copy `Bytes`, so **no unsafe is needed**. `from_owner` returns a `Bytes` covering the whole mapping, then narrow it to the requested window with `.slice(off..off+len)` (`slice` only adjusts the pointer/length, no copy). Note that `Arc<Mmap>` itself does not implement `AsRef<[u8]>`, so it must be wrapped with the `MmapChunk` newtype above. If we later downgrade to a bytes version without `from_owner`, we can fall back to `Bytes::from(Vec)` + one copy (still no worse than Java).

### 3.4 Thread model

| Item | Java | Rust SC |
|---|---|---|
| Control plane | GrpcBlockingStream blocking | tonic async; `await` yields naturally |
| File::open + mmap | done outside netty IO thread (implicit) | direct sync call, comment says "mmap does not block data IO, only metadata" |
| Data read | netty pipeline → user thread | calling thread directly slices / memcpy |

**Rust SC decision**: drop the early prototype's `tokio::task::spawn_blocking(File::open + mmap)`. Reasons:

- the mmap **syscall** performs no data IO on Linux, it only triggers VMA allocation + page-table placeholders; typical cost < 50µs.
- spawn_blocking's cross-thread switch + scheduler wakeup ≈ 5-20µs, which for a short task like `open` **can be slower than a direct call**.
- in measured extreme NFS scenarios we add spawn_blocking back, controlled by the config `goosefs.client.short.circuit.open.blocking = true`.

> **Key constraint (the classic mmap + async pitfall)**: the `mmap` syscall itself does no IO, but **reading bytes afterwards triggers a page fault**. On a page cache hit it is a minor fault (microseconds, can be read directly on the async thread); **a cold-data major fault performs synchronous disk IO and blocks the current tokio worker thread**, thereby starving other tasks on that thread. Therefore the low-latency guarantee of this design (p99 < 50~80µs in §5.2) **only holds when the page cache is hit (hot data)**. The cold-data path must choose one of two options:
> 1. first `prefetch` / `prefetch_many` to trigger async readahead, and only hand `&[u8]` / `Bytes` to the async consumer after the data is **resident**; or
> 2. move the actual byte-touching read into `spawn_blocking` (decided by the `open.blocking` sibling switch or the caller).
>
> In other words, the optimization "the data plane directly slices/memcpy on the calling thread" is premised on a **hot cache**; in cold scenarios one must not synchronously touch un-resident pages on an async runtime thread. This ties in with §5.2 and §11.5 (FS probing).

### 3.5 Reuse and caching

Java: one LocalFileBlockReader per BlockInStream, discarded at the end.

Rust SC:

- **Per-task LRU cache**: `HashMap<block_id, Arc<LocalBlockReader>>`, default capacity 64, default TTL 30s.
- **Negative cache**: blocks whose SC failed in the last N seconds are not retried via SC but go straight to gRPC, avoiding repeated OpenLocalBlock failures.
- **Cross-task sharing**: optional SharedLocalReaderPool (process-wide), trading reference-counting atomic overhead against hit rate.

### 3.6 Failure fallback

| Stage | Failure cause | Rust SC behavior | Consistency |
|---|---|---|---|
| source_is_local pre-filter | the worker serving the block is not local | go straight to gRPC, skip OpenLocalBlock (save 1 RTT) | INV-S1: equivalent to full gRPC path |
| OpenLocalBlock RPC | NotFound (block not local) / IO error | warn + negative cache + gRPC fallback | INV-S1 |
| File::open | EACCES (uid mismatch) | record hint on first occurrence, permanently disable SC (per-process flag) | INV-S1 |
| Mmap::map | ENOMEM / EINVAL | cache entry failure count + gRPC fallback | INV-S1 |
| read slice out of bounds | upper-layer bug | Err, **no** fallback (semantic error must be surfaced) | INV-S4: stable error classification |
| capability rejection | cluster has capability enabled, request lacks or expired it | raise the same error classification as the gRPC path | INV-S3 |

**General fallback transparency rule**: except for "semantic errors", all SC failures must guarantee that the byte sequence observed by the caller is exactly equivalent to "always going through gRPC" (INV-S1). Fallback must not happen after partial bytes have already been returned to the upper layer; that is, a successful `read` call must have a single source (either SC fully succeeds, or it fully goes through gRPC).

### 3.7 Decision matrix

```
should_use_short_circuit(cfg, ctx):
  if !cfg.short_circuit_enabled            -> false       # kill switch
  if !ctx.source_is_local                  -> false       # pre-filter: is the worker serving this block local?
  if ctx.process_sc_disabled (sticky)      -> false       # past EACCES
  if ctx.negative_cached(block_id)         -> false       # recent failure
  if cfg.huge_block_only && size < 2MB     -> false       # tuning
  return true
```

> **The real source and semantic boundary of `source_is_local` (against the dev branch code)**:
> The client has no standalone `is_local_worker(block_id)`. Locality is derived from the existing `WorkerRouter` capability — `select_worker(block_id)` already implements local-first routing (when a local worker exists and is not failed, all blocks are routed to it), so `source_is_local` should be composed as `select_worker(block_id).id == local_worker_id` (`local_worker_id` is matched by `detect_local_worker` via hostname / local IP and cached via ArcSwap). The `WorkerRouter.is_local_worker(id)` in the §2 architecture diagram is an abstract name for this composed semantics.
>
> **Key: a local worker ≠ a block that can be locally mmap'd**. local-first only guarantees "served by the local worker", **not** that the block physically sits on the local disk (the local worker may still need to pull from UFS / peer). Therefore `source_is_local` is only a **pre-filter optimization** to "avoid sending pointless OpenLocalBlock RPCs to a remote worker"; whether a block is truly locally readable is **ultimately decided by the OpenLocalBlock RPC** — the Worker only returns a `path` when the block actually lands locally, otherwise it errors and the next line (OpenLocalBlock NotFound/IO error → fallback) catches it. Implementers **must not** assume mmap is possible just because `source_is_local == true`.

---

## 4. Key API Design

### 4.1 LocalBlockReader

```rust
pub struct LocalBlockReader {
    block_id: i64,
    /// Logical block size (from the OpenLocalBlock response / `block_size`),
    /// **NOT** the physical length of the disk file. If the block file is
    /// preallocated/sparsed larger than the logical length, mmap'ing by the
    /// physical length would expose trailing zeros, violating INV-D2. The mmap
    /// length and all bounds_check must use this logical size.
    file_size: usize,
    /// Whole-block read-only mapping. Created exactly once in `open`.
    mmap: Arc<Mmap>,
    /// Worker-side block lock. Field declared AFTER `mmap` so Drop
    /// order is: mmap first (munmap), then guard (close stream → unlock).
    /// In practice the order is irrelevant for correctness because
    /// the kernel keeps the inode alive via the VMA, but the ordering
    /// makes intent explicit.
    _guard: OpenLocalBlockGuard,
}

impl LocalBlockReader {
    /// `open` flow (against the dev branch proto `OpenLocalBlockRequest/Response`):
    ///   1. Start the OpenLocalBlock bidi stream; request fields = { block_id, capability, block_size }
    ///      (these three fields are exactly the generated `OpenLocalBlockRequest`).
    ///      The `capability` parameter type `Option<Capability>` aligns precisely with the generated code:
    ///      the proto field is itself `Option<Capability>` (block.rs L167), `None` → no capability sent.
    ///      ⚠️ The **source** of capability does not yet exist on dev (`InStreamOptions` has no
    ///      `capability_fetcher`, the read path has not wired in Capability), and is a §10 P3 to-do;
    ///      confirm the source before landing, see §3.1 change #1.
    ///      On landing, ensure the Worker's rejection logic for "empty capability" matches the gRPC
    ///      path (INV-S3).
    ///   2. From `OpenLocalBlockResponse` take:
    ///        - `path`        → the local block file path to mmap (the sole target of mmap)
    ///        - `block_size`  → logical block size → i.e. `file_size` (**do not** use the disk file's
    ///          physical length, see the field comment above)
    ///      The response only returns these two; the mmap length and all bounds_check use `block_size`.
    ///   3. Hold the guard that owns the sender, ensuring the session lock is not released during the
    ///      reader's lifetime (§8.2.1).
    /// The `block_size` argument is the expected size known by the upper layer, used for the request;
    /// the final authority is the `block_size` returned in the response.
    pub async fn open(client: &WorkerClient,
                      block_id: i64, block_size: i64,
                      capability: Option<Capability>,
                      hint: AccessHint) -> Result<Self>;

    pub fn read(&self, offset: usize, len: usize) -> Result<&[u8]>;
    pub fn read_bytes(&self, offset: usize, len: usize) -> Result<Bytes>;
    pub fn read_to_slice(&self, offset: usize, dst: &mut [u8]) -> Result<usize>;

    /// L2 application-layer prefetch: ask the kernel to asynchronously pull
    /// [offset, offset+len) into the page cache.
    ///
    /// Semantics: trigger `madvise(MADV_WILLNEED)`, return immediately (async readahead);
    /// a no-op for ranges already in the page cache; the call itself does not block,
    /// typical cost < 5µs.
    ///
    /// Usage:
    ///   - a streaming reader prefetches the next chunk while consuming the current one
    ///   - Lance `take(rows[])` prefetches all at once the moment the offset list is obtained
    ///
    /// Note: returns `OutOfRange` when out of bounds, consistent with `read`.
    pub fn prefetch(&self, offset: usize, len: usize) -> Result<()>;

    /// L2 batched prefetch: coalesce/sort then cover all ranges with one or a few `madvise` calls.
    ///
    /// Internally coalesces adjacent `(offset, len)` ranges to minimize syscalls.
    /// Typical scenario: a Lance PR obtains N row offsets at once, forming N ranges.
    pub fn prefetch_many(&self, ranges: &[(usize, usize)]) -> Result<()>;

    pub fn block_id(&self) -> i64;
    pub fn file_size(&self) -> usize;
}

pub enum AccessHint {
    Sequential,   // → MADV_SEQUENTIAL
    Random,       // → MADV_RANDOM
    Default,      // no madvise
}
```

#### Differences from the early prototype

| Item | Early prototype (goosefs-lance-tests/short_circuit.rs) | Rust SC |
|---|---|---|
| Holds a `_file` field | Yes | **No** (saves 1 fd after drop) |
| spawn_blocking wraps open + mmap | Yes | No (direct sync) |
| MADV_RANDOM | None | Yes (default in PR scenarios) |
| capability | None | Yes |
| read_bytes zero-copy API | None | Yes |
| SIGBUS comments | Partial | Complete (including recovery path) |

### 4.2 ShortCircuitFactory

```rust
pub struct ShortCircuitFactory {
    client: Arc<FileSystemContext>,
    cache: Mutex<LruCache<i64, Arc<LocalBlockReader>>>,
    /// The negative cache must be **bounded**: use `LruCache` (not a raw `HashMap`)
    /// + a capacity limit + a per-entry TTL. A raw HashMap only checks TTL on lookup
    /// and never actively sweeps, so it grows unbounded in the face of many distinct
    /// failing block_ids; a capacity-bounded LRU automatically evicts the oldest
    /// negative entries.
    neg_cache: Mutex<LruCache<i64, Instant>>,
    cfg: ShortCircuitConfig,
}

impl ShortCircuitFactory {
    pub async fn get_or_open(&self, ctx: BlockReadCtx) -> Result<Arc<LocalBlockReader>>;
    pub fn invalidate(&self, block_id: i64);
}
```

### 4.3 Upper-layer integration (BlockInStream::create)

```rust
pub async fn create(...) -> Result<Box<dyn BlockInStream>> {
    if should_use_short_circuit(&cfg, &ctx) {
        match factory.get_or_open(ctx.clone()).await {
            Ok(reader) => return Ok(Box::new(LocalShortCircuitInStream::new(reader, ctx))),
            Err(e) => {
                tracing::warn!(block_id = ctx.block_id, error = %e,
                               "short-circuit failed, falling back to gRPC");
                factory.mark_failure(ctx.block_id);
            }
        }
    }
    create_grpc_block_in_stream(ctx).await
}
```

---

## 5. Performance model and benchmarks

### 5.1 Single-read-path cost breakdown

| Step | Java | Rust SC |
|---|---|---|
| Open (one-time, amortized) | TCP RTT + bidi handshake + RandomAccessFile open | TCP RTT + bidi handshake + mmap |
| Per read system call | 1× mmap + 1× munmap (implicit GC) | 0 |
| Per read userspace | MappedByteBuffer wrapper + downstream readBytes copy | &[u8] borrow, optional memcpy |
| Per read lock contention | mmap triggers mmap_sem write lock | None |
| Page-fault handling | same as Rust, equivalent | same as Java |

### 5.2 Expected benchmarks (metrics, not measured)

Environment: single-machine local Worker, 1GB block, fully hot page cache.

| Scenario | Java SC throughput | Rust SC throughput | Ratio |
|---|---|---|---|
| Sequential read 64KB×N, 1 thread | 8 GB/s | 12 GB/s | 1.5× |
| Sequential read 64KB×N, 8 threads | 18 GB/s | 35 GB/s | 1.9× |
| PR 64KB×N random, 1 thread | 1.2 GB/s | 8 GB/s | **6.7×** |
| PR 64KB×N random, 256 threads | 3 GB/s (mmap lock bottleneck) | 25 GB/s | **8.3×** |
| PR p99 latency, 256 threads | 3 ms | 80 µs | **37× improvement** |
| **Cold data** PR 64KB×N, 1 thread, no prefetch | ≈ single-disk IOPS × 64KB | ≈ single-disk IOPS × 64KB | ≈1× |
| **Cold data** PR 64KB×N, 1 thread, `prefetch_many` (L2) | N/A | **disk bandwidth ceiling** | **10×~50×** (depends on disk) |
| **Cold data** PR p99 latency, no prefetch | dominated by single page-fault latency | same as Java | flat |
| **Cold data** PR p99 latency, `prefetch_many` (L2) | N/A | close to hot-data latency | **tens of × improvement** |

> When measuring, supplement with flame graphs to locate whether the bottleneck is page-fault / Arc atomic operations.
>
> **Prerequisite statement**: the low latency (p99 < 50~80µs) of the "hot data" rows above holds **if and only if the page cache is hit**; at that point the page fault is a minor fault, and slice/memcpy can be done directly on the calling thread. Cold-data rows' latency is dominated by disk page faults, and if un-resident pages are touched synchronously on an async runtime thread it blocks that worker thread (see §3.4); therefore cold scenarios must be warmed via `prefetch` or isolated via `spawn_blocking`, otherwise not only does latency degrade, but it also drags down other tasks on the same thread.

### 5.2.1 Flame graph collection and viewing (command-level SOP)

The flame graph is a **mandatory deliverable** for validating every performance assumption of this design: is there really no mmap syscall? Is the hotspot on page-fault? Does `Arc::clone` become a high-concurrency bottleneck? The following gives command-by-command instructions from environment setup to differential comparison.

#### A. Environment preparation (one-time)

**A.1 General Rust-side config**: flame graphs need complete symbols; release builds strip by default, so add to the top level of `Cargo.toml`:

```toml
[profile.release]
debug = "line-tables-only"   # keep line numbers, limited size bloat
strip = false                # do not strip symbols

[profile.bench]
debug = "line-tables-only"
strip = false
```

> **⚠️ Must be merged with dev's existing `[profile.release]`, do NOT overwrite it entirely**: dev's `Cargo.toml` `[profile.release]` already contains `lto = "fat"`, `codegen-units = 1`, and **deliberately does NOT enable `panic = "abort"`** (preserving unwind/Drop semantics, which §8.2.1 async unlock reaper and §8.3 panic-safety argument depend on). When adding `debug`/`strip`, only append these two keys, **keep the existing `lto`/`codegen-units`, and strictly do NOT add `panic = "abort"`** — otherwise it breaks SC's unlock and panic-safety guarantees. `[profile.bench]` has no explicit declaration in dev currently, so adding a new block is conflict-free.

Or temporarily use an environment variable (without changing Cargo.toml):

```bash
RUSTFLAGS="-C debuginfo=1 -C strip=none" cargo build --release
```

**A.2 Linux environment (Tencent Cloud CVM / TKE nodes, TencentOS Server / OpenCloudOS)**:

Tencent Cloud production instances are currently mainly **TencentOS Server 2.4 / 3.x** (compatible with CentOS 7 / RHEL 8 package management) and **OpenCloudOS 8 / 9**, with Ubuntu images used in a few scenarios. The commands below focus on the former two; Ubuntu is given at the end.

```bash
# 0) Confirm the distro and kernel first
cat /etc/os-release
uname -r       # e.g.: 5.4.119-19-0009.11 (TencentOS kernel naming carries a -tlinux/-tencent suffix)
uname -m       # x86_64 / aarch64 (Tencent Cloud ARM instances such as SR1/SR2 are aarch64)
```

**A.2.1 Installing perf (by distro branch)**

```bash
# —— TencentOS Server 2.4 / CentOS 7 family (yum) ——
sudo yum install -y perf
# If it says package not found, switch to the matching kernel's kernel-tools:
sudo yum install -y "kernel-tools-$(uname -r)" || sudo yum install -y kernel-tools

# —— TencentOS Server 3.x / OpenCloudOS 8+ / RHEL 8 family (dnf) ——
sudo dnf install -y perf
# You may also need:
sudo dnf install -y "kernel-tools-$(uname -r)"

# —— Ubuntu image ——
sudo apt-get update
sudo apt-get install -y linux-tools-common linux-tools-generic "linux-tools-$(uname -r)"
# Tencent Cloud Ubuntu images often carry a -tlinux suffix on the kernel, and there may be no
# fully matching linux-tools-<ver> package; in that case fall back to the generic package:
sudo apt-get install -y linux-tools-generic
# Then call perf directly via /usr/lib/linux-tools/<ver>/perf, or symlink it:
sudo ln -sf /usr/lib/linux-tools/*/perf /usr/local/bin/perf

# Verify
perf --version
perf list | head    # OK if it can list events
```

> **Tencent Cloud pitfall 1: kernel tools version mismatch** —— custom images or canary kernels (5.4.119-19-0009 etc.) may not have a fully matching `kernel-tools` package in the repo. Resolution order: ① `yum/dnf install kernel-tools-$(uname -r)` ② if that fails, install the generic `kernel-tools`; when running `perf`, if it prints `WARNING: perf not found for kernel ...` but still works, that is acceptable ③ if still no good, install `samply` (see A.2.4) to bypass perf.

**A.2.2 perf sampling permissions (CVM physical / VM OK; inside containers see A.2.3)**

```bash
# One-time (lost after reboot)
sudo sysctl -w kernel.perf_event_paranoid=-1
sudo sysctl -w kernel.kptr_restrict=0

# Persist
sudo tee /etc/sysctl.d/99-perf.conf >/dev/null <<'EOF'
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
EOF
sudo sysctl --system

# TencentOS Server images with SELinux=enforcing enabled by default are rare, but if enabled it may block perf:
getenforce
# If Enforcing and perf reports "Permission denied", temporarily:
sudo setenforce 0
```

> **Tencent Cloud pitfall 2: `perf_event_paranoid` set to 2 or 3 in some hardened images** —— directly `cat /proc/sys/kernel/perf_event_paranoid` to see the current value; this design needs ≤ 1 to capture kernel stacks (so page-fault / mmap are visible), and ≤ -1 to capture all events.

**A.2.3 Running perf in TKE / containers (containerd / Docker)**

When running benchmarks on Tencent Cloud TKE worker nodes, the vast majority of GooseFS Workers are deployed in containers, and perf must be able to see the **host kernel symbols** to be meaningful. Two recommended approaches:

```bash
# Approach A: sample the container process directly on the host (CVM) (recommended, most complete symbols)
# 1) Find the container process PID
PID=$(crictl inspect $(crictl ps -q --name goosefs-worker) | jq -r '.info.pid')
# or docker: PID=$(docker inspect -f '{{.State.Pid}}' goosefs-worker)

# 2) Sample that PID on the host
sudo perf record -F 999 -g --call-graph dwarf -p "$PID" -o perf_worker.data -- sleep 30

# 3) Resolving needs the container's rootfs for symbols (perf reads it automatically via /proc/<PID>/root)
sudo perf script -i perf_worker.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl > flamegraph_worker.svg

# Approach B: run perf inside the container (needs extra privileges; only recommended for a temporary bench container)
# Add when starting the bench container:
#   --privileged   or  --cap-add=SYS_ADMIN --cap-add=PERFMON --cap-add=SYS_PTRACE
#   --security-opt seccomp=unconfined
#   -v /sys/kernel/debug:/sys/kernel/debug:ro
#   -v /lib/modules:/lib/modules:ro
#   -v /usr/src:/usr/src:ro
# Then install perf inside the container as in A.2.1, and apply the sysctl adjustments (needs --privileged)
```

> **Tencent Cloud pitfall 3: TKE's default PodSecurityPolicy / node seccomp profile blocks `perf_event_open(2)`**. If, after the bench Pod starts, `perf record` immediately reports `Operation not permitted`, first confirm whether `SYS_ADMIN`+`PERFMON` are present, or simply use Approach A to sample on the host.

**A.2.4 ARM instances (SR1/SR2/standard SA series aarch64) notes**

- `--call-graph dwarf` also works on aarch64, but `--call-graph fp` needs `RUSTFLAGS="-C force-frame-pointers=yes"` **and** kernel ≥ 5.10 to resolve correctly; dwarf is preferred.
- Some ARM instances have few hardware PMU events in `perf list` and may only use software events (`cpu-clock`, `task-clock`); sufficient for flame-graph sampling.

**A.2.5 Installing FlameGraph and Rust sampling tools**

```bash
# Brendan Gregg's FlameGraph scripts (perl scripts only, no build dependency)
git clone https://github.com/brendangregg/FlameGraph.git ~/FlameGraph
export PATH=$PATH:~/FlameGraph
echo 'export PATH=$PATH:~/FlameGraph' >> ~/.bashrc

# Rust one-shot sampling tool (recommended; wraps perf record + stackcollapse + flamegraph.pl)
cargo install flamegraph        # uses perf, requires A.2.1 + A.2.2 done
cargo install samply            # not perf-dependent; preferred fallback on Tencent Cloud hardened images / TKE containers
```

> **Tencent Cloud pitfall 4: slow intranet cargo install** —— it is recommended to configure the Tencent Cloud crates mirror (`~/.cargo/config.toml`):
> ```toml
> [source.crates-io]
> replace-with = "tencent"
> [source.tencent]
> registry = "https://mirrors.tencent.com/crates.io-index"
> ```

**A.3 macOS environment** (no perf; use `samply` or `dtrace`):

```bash
brew install samply
# or use cargo-instruments (requires Xcode)
cargo install cargo-instruments
```

#### B. Collecting flame graphs (commands per scenario)

**Scenario B.1: benchmark sequential-read SC path (most common)**

```bash
# Sample the criterion bench binary directly
cargo flamegraph --bench sc_seq -o flamegraph_sc_seq.svg -- --bench

# Or finely control perf params (sample at 999Hz to avoid resonance with timers)
RUSTFLAGS="-C debuginfo=1" cargo bench --bench sc_seq --no-run
BENCH_BIN=$(ls -t target/release/deps/sc_seq-* | grep -v '\.d$' | head -n1)

sudo perf record -F 999 -g --call-graph dwarf -o perf_sc_seq.data \
    -- "$BENCH_BIN" --bench

sudo perf script -i perf_sc_seq.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl --title "Rust SC seq read" \
    > flamegraph_sc_seq.svg
```

> Note: `--call-graph dwarf` suits Rust (unwinds the stack from debug info); if the kernel is too old or dwarf is too slow, switch to `--call-graph fp`, but that needs `RUSTFLAGS="-C force-frame-pointers=yes"`.

**Scenario B.2: benchmark high-concurrency PR (256 threads, focus on `Arc` / lock contention)**

```bash
cargo flamegraph --bench sc_pr -o flamegraph_sc_pr_256t.svg \
    -- --bench --measurement-time 30 high_concurrency_256

# Capture off-CPU (see if blocked by madvise / mmap_sem)
sudo perf record -F 999 -e sched:sched_switch -e sched:sched_stat_sleep \
    --call-graph dwarf -o perf_offcpu.data \
    -- "$BENCH_BIN" --bench
sudo perf inject -s -i perf_offcpu.data -o perf_offcpu.inject.data
sudo perf script -i perf_offcpu.inject.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl --color=io --title "Rust SC off-CPU" \
    > flamegraph_offcpu.svg
```

**Scenario B.3: capture only page-faults (verify "is it dominated by page-faults")**

```bash
# Only sample the page-fault event, filtering other noise
sudo perf record -e page-faults -c 1 --call-graph dwarf -o perf_pf.data \
    -- "$BENCH_BIN" --bench positioned_read_cold

sudo perf script -i perf_pf.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl --color=mem --title "Page-fault flamegraph" \
    > flamegraph_pagefault.svg

# Numeric stats: fault count / minor / major
sudo perf stat -e page-faults,minor-faults,major-faults \
    -- "$BENCH_BIN" --bench positioned_read_cold
```

**Scenario B.4: capture syscalls (verify "is the mmap count really 1")**

```bash
# Count only mmap / munmap / madvise calls and their cost
sudo perf trace -e 'syscalls:sys_enter_mmap,syscalls:sys_enter_munmap,syscalls:sys_enter_madvise' \
    -- "$BENCH_BIN" --bench seq_read_1gb 2>perf_trace.log

grep -c sys_enter_mmap perf_trace.log    # expected: 1 (per reader)
grep -c sys_enter_madvise perf_trace.log # expected: 1 + N (N = prefetch call count)
```

Or more lightly with `strace -c` (only for small-scale benches; significantly slows them down):

```bash
strace -c -e trace=mmap,munmap,madvise,pread64 "$BENCH_BIN" --bench seq_read_1gb
```

**Scenario B.5: macOS (no perf, use samply)**

```bash
cargo bench --bench sc_seq --no-run
BENCH_BIN=$(ls -t target/release/deps/sc_seq-* | grep -v '\.d$' | head -n1)

samply record -- "$BENCH_BIN" --bench
# After the command ends samply auto-opens the browser, with flame graph + Sandwich view + timeline
```

#### C. Viewing flame graphs

**C.1 Open the SVG in a browser**: a flame graph is itself an SVG, openable directly in any browser. For a remote Linux server scenario:

```bash
# Drag the SVG back to local
scp user@server:/path/to/flamegraph_sc_seq.svg ./
open flamegraph_sc_seq.svg     # macOS
xdg-open flamegraph_sc_seq.svg # Linux desktop

# Or run an HTTP server on the remote (access locally after port forwarding)
python3 -m http.server 8000 -d /path/to/svg/dir
# Local: ssh -L 8000:localhost:8000 user@server
# Then open http://localhost:8000/flamegraph_sc_seq.svg in the browser
```

**C.2 Reading tips**:

- **The horizontal axis is the sample count** (not a time series); width = the function's relative CPU time share.
- **The vertical axis is the call stack**; below is the callee (the very bottom is the function currently executing on the CPU).
- **Color has no inherent meaning** (default is random warm colors), used only for visual distinction; use `--color=mem` (green) / `--color=io` (blue) to express categories.
- **Click any function** = zoom into that subtree; right-click to reset.
- **The top search box** supports regex (e.g. `mmap|munmap|madvise|pthread_mutex|alloc::sync`).

#### D. Key hotspot interpretation checklist (directly relevant to this design)

Listed as "visible feature of the hotspot in the flame graph → design action to take":

| Observed stack frame | Meaning | Expected share | Action on exceeding threshold |
|---|---|---|---|
| Wide bar of `sys_mmap` / `sys_munmap` at the top | still frequently mmap'ing | < 0.5% (should be only 1 each at open/close) | check if it has degraded into per-chunk mmap, violating §3.2 |
| `do_page_fault` / `handle_mm_fault` | page-fault handling (first touch of a page) | reasonable for cold data; hot data < 5% | high share on hot data means insufficient LRU/residency, enlarge cache or add prefetch |
| `__memmove_avx_unaligned` / `memcpy` | userspace copy | reasonable on the `read_to_slice` path; should be near zero on `read_bytes` | memcpy on `read_bytes` = zero-copy broken, recheck `Bytes::from_owner` impl |
| `alloc::sync::Arc::clone` / `__atomic_fetch_add` | Arc ref-count atomic ops | < 2% | > 5% means unnecessary `Arc::clone` on the hot path, change to borrow or `&Arc` |
| `parking_lot::Mutex::lock` / `futex_wait` | lock contention | nearly zero (no shared mutable state by design) | its presence is a bug; locate which lock (likely LRU or neg cache) |
| `__madvise` high frequency | prefetch_many not coalesced well | < 0.1% | check the coalesce logic in §3.2.1; see if `prefetch.coalesce.gap` takes effect |
| `tonic` / `h2` / `prost` | gRPC path | only visible at open / close stages | its presence on the data read path = SC degraded to gRPC, check fallback cause |
| `tokio::runtime::*` / `park` | async scheduling | reasonable on control plane, should not appear on data plane | its presence on data plane = misused async / `spawn_blocking`, violating §3.4 |

#### E. Differential flame graph (Java vs Rust SC, or before/after optimization)

The differential flame graph visually shows "which stacks got faster and which got slower" — red is slower, blue is faster.

```bash
# 1) Collect baseline (Rust SC before optimization / Java SC equivalent path)
sudo perf record -F 999 -g -o perf_before.data -- "$BENCH_BEFORE" --bench
sudo perf script -i perf_before.data | ~/FlameGraph/stackcollapse-perf.pl > before.folded

# 2) Collect after
sudo perf record -F 999 -g -o perf_after.data -- "$BENCH_AFTER" --bench
sudo perf script -i perf_after.data | ~/FlameGraph/stackcollapse-perf.pl > after.folded

# 3) Generate the differential graph
~/FlameGraph/difffolded.pl before.folded after.folded \
    | ~/FlameGraph/flamegraph.pl --title "before -> after diff" \
    > flamegraph_diff.svg
```

#### F. Archiving convention

Each P5-stage bench must submit the following files with the PR, under the path convention `docs/perf/<date>-<scenario>/`:

```
docs/perf/2026-06-24-sc-pr-256t/
├── env.txt                        # uname -a / lscpu / free -h / kernel cmdline
├── cargo_bench.log                # full criterion output
├── perf_stat.txt                  # perf stat -e cycles,instructions,page-faults,...
├── flamegraph_oncpu.svg
├── flamegraph_offcpu.svg
├── flamegraph_pagefault.svg
└── README.md                      # one-paragraph summary: does it match §5.2 expectations, hotspot deviations
```

> **Mandatory requirement**: the flame graph and the corresponding `perf stat` numbers must corroborate each other; a report with only the SVG and no numbers is deemed unqualified and the PR review rejects it.

### 5.3 Acceptance benchmarks

When landing this document, the following benchmarks and tests must be run and their results archived:

1. `cargo bench --bench sc_seq` — sequential-read throughput vs the gRPC path
2. `cargo bench --bench sc_pr` — PR mode 1/8/64/256 threads × {4KB, 64KB, 1MB} buf
3. `cargo bench --bench sc_lat` — p50/p99/p999 latency distribution
4. `cargo bench --bench sc_prefetch` — cold-data PR throughput and p99 under {no prefetch, prefetch, prefetch_many}; must prove the prefetch path improves p99 by at least 10× relative to no prefetch
5. `cargo test --test sc_consistency` — **gate-level consistency regression test** (unlike benches, must pass 100%; triggered by any PR, failure directly blocks merge). Covers all invariants of §1.3:
   - **INV-D1 (end-to-end cross-reader sub-contract)**: write v1 -> close stream -> overwrite once with a same-length but different payload and once with a different-length payload -> a freshly opened stream must immediately observe the new bytes and new length (`sc_consistency::inv_d1_e2e_overwrite_visibility`, with an assertion on the new-read path that `CLIENT_SC_OPEN_SUCCESS` increases, to rule out a silent gRPC-fallback false positive).
   - **INV-D2**: dual-read the same block via the SC path and the gRPC path, byte-for-byte diff (covering sequential read, PR, cross-chunk, cross-page boundaries)
   - **INV-D3**: the `Bytes` returned by `read_bytes` can still be safely accessed after the reader is Dropped, with unchanged content (validates owner lifetime)
   - **INV-D4**: `prefetch` / `prefetch_many` before and after, the `read` bytes over the same range are completely identical
   - **INV-S1**: after fault injection (force OpenLocalBlock RPC failure / `File::open` EACCES / `Mmap::map` failure), the byte sequence returned by `BlockInStream::read` is exactly the same as the full gRPC path
   - **INV-S2**: construct an abnormal path (panic during read / early `?` return), verify the Worker-side lock is released after the reader is Dropped
   - **INV-S3**: capability enabled / disabled two scenarios, error classification consistent with gRPC. The SDK-side contract (injection plumbing + byte equivalence + transparent fallback on rejection) is covered by three cases in `tests/sc_inv_s3.rs`: `inv_s3_a` verifies SC must engage on a cluster that does not enforce capability; `inv_s3_b` byte-diff SC vs gRPC under the current auth mode; `inv_s3_c` is an observational probe that, based on the SC counters (`CLIENT_SC_OPEN_SUCCESS / OPENLOCAL_FAIL / FILE_OPEN_FAIL / MMAP_FAIL`), outputs an `ACCEPTED / REJECTED / BYPASSED` verdict and records it in the test log (with an `INV-S3-c verdict:` prefix for easy CI grep).
   - **INV-S4**: `OutOfRange` is not swallowed by fallback; the caller receives the same error type as gRPC
   - **INV-S5**: `read` / `read_bytes` / `read_to_slice` results are identical for the same input

---

## 6. Configuration items

| Config | Default | Description |
|---|---|---|
| goosefs.user.short.circuit.enabled | false | Master kill switch (disabled by default since 0.1.6, see FLAMEGRAPH_OPTIMIZATION_PLAN §C6) |
| goosefs.user.short.circuit.preferred | true | Same name as Java; Rust always treats it as true since there is no DS |
| goosefs.client.short.circuit.cache.capacity | 64 | per-task LRU size |
| goosefs.client.short.circuit.cache.ttl | 30s | reader idle expiration |
| goosefs.client.short.circuit.neg.cache.ttl | 5s | failure cache |
| goosefs.client.short.circuit.advise | random | L1 kernel readahead: sequential / random / normal / none |
| goosefs.client.short.circuit.prefetch.enabled | true | L2 application-layer prefetch master switch (when off, prefetch/prefetch_many degrade to no-op) |
| goosefs.client.short.circuit.prefetch.coalesce.gap | 64KB | max gap for merging adjacent ranges in prefetch_many |
| goosefs.client.short.circuit.prefetch.max.batch | 1024 | max madvise calls per single prefetch_many (prevents syscall storm) |
| goosefs.client.short.circuit.open.blocking | false | whether to wrap open with spawn_blocking (set true for NFS scenarios) |
| goosefs.client.short.circuit.thp | false | THP via `madvise(MADV_HUGEPAGE)` (**not** MAP_HUGETLB; file-backed THP depends on kernel/FS, experimental) |
| goosefs.client.short.circuit.min.block.size | 0 | below this value SC is not used |
| goosefs.client.short.circuit.sigbus.recover | true | register SIGBUS handler |

---

## 7. Error handling and observability

### 7.1 Error classification

```rust
pub enum ShortCircuitError {
    NotLocal,
    OpenLocalBlock(tonic::Status),
    FileOpen(std::io::Error),
    Mmap(std::io::Error),
    Madvise(std::io::Error),
    OutOfRange { off: usize, len: usize, file_size: usize },
    SigBus,
}
```

All ShortCircuitError can be transparently converted to fallback by BlockInStream::create; only OutOfRange is propagated directly (semantic error).

### 7.2 Tracing fields

Each LocalBlockReader::open span contains:

- block_id, block_size, capability_present
- path, file_size, mmap_addr (debug level)
- open_duration_us, mmap_duration_us
- cache_hit (factory layer)

### 7.3 Prometheus metrics

| metric | type | description |
|---|---|---|
| goosefs_sc_open_total{result} | counter | open count, result=success/openlocal_fail/file_open_fail/mmap_fail |
| goosefs_sc_read_bytes_total | counter | SC-path bytes read |
| goosefs_sc_read_calls_total | counter | SC-path read() call count |
| goosefs_sc_cache_hits_total | counter | LRU hits |
| goosefs_sc_cache_evictions_total | counter | LRU evictions |
| goosefs_sc_neg_cache_hits_total | counter | negative-cache hits (avoid retries) |
| goosefs_sc_active_readers | gauge | currently active readers |
| goosefs_sc_mmap_bytes | gauge | cumulative mmap bytes (virtual memory) |
| goosefs_sc_open_duration_seconds | histogram | open latency |
| goosefs_sc_prefetch_calls_total | counter | prefetch / prefetch_many call count |
| goosefs_sc_prefetch_bytes_total | counter | cumulative requested prefetch bytes |
| goosefs_sc_prefetch_madvise_total | counter | actual madvise(WILLNEED) syscall count (after merging) |

---

## 8. Safety argument

### 8.1 unsafe inventory

Only 1 unsafe location:

1. **Mmap::map(&file)** (inside the memmap2 crate)
   - SAFETY precondition: within the mmap lifetime, the file content is not truncated/replaced (INV-D1).
   - Mitigation: the Worker holds the lock and does not truncate (protocol foundation); the SIGBUS handler only does diagnostics then `abort`s (see §3.2); deployments needing robustness use the `io.mode=pread` data plane instead of mmap.

#### 8.1.1 INV-D1 already verified against GooseFS source (block_id not reused ∧ lock-held immutable)

Addressing the concern "could a cached `LocalBlockReader` (LRU TTL 30s) read stale/reused content": we have checked point by point against the `/opt/sourcecode/cos/goosefs` source, **conclusion: no `(length, version)` sidecar check or other extra protection is needed** — INV-D1 is doubly guaranteed by the GooseFS protocol:

**(1) block_id is globally monotonic and never reused** — source `core/server/master/.../block/BlockContainerIdGenerator.java`:

```java
private final AtomicLong mNextContainerId;                 // monotonic, no recycling
public long getNewContainerId() { return mNextContainerId.getAndIncrement(); }
public JournalEntry toJournalEntry() { ... setNextContainerId(mNextContainerId.get()) } // journal persistence
```

In `BlockId.java`, `blockId = [container_id:53bit][sequence:11bit]`, where container_id is the file container id. The generator's `getAndIncrement()` is strictly increasing, persisted via journal (the master restores via `setNextContainerId` on restart, no rollback), and has **no recycling branch**. After a file is deleted, the new file necessarily gets a larger container_id ⇒ the new block_id differs from any historical block_id ⇒ "a block deleted/GC'd then its block_id reused for new content within the TTL" **cannot happen**.

**(2) The Worker lock covers the entire reader lifetime** — source `core/server/worker/.../grpc/ShortCircuitBlockReadHandler.java`:

```java
// onNext(OpenLocalBlockRequest): acquire the block read lock before returning the local path
mLockId = mWorker.lockBlock(mSessionId, request.getBlockId());
// source comment verbatim: "The block is locked for the session,
//               the lock is released when the session is closed."
// onCompleted / onError: the lock is released only when the stream closes
mWorker.unlockBlock(mLockId); mWorker.cleanupSession(mSessionId);
```

The lock follows the **stream (session) lifecycle**; the client does not unlock until it closes the stream — strictly corresponding to Rust's `OpenLocalBlockGuard`: guard present ⇒ stream present ⇒ Worker holds the read lock; guard Drop ⇒ `onCompleted` ⇒ `unlockBlock`. Our `LocalBlockReader` **holds the guard for its entire lifetime (including the 30s LRU cache)**, so during caching the Worker read lock is always held and the block file cannot be evict / delete / truncate (deletion needs the write lock, which is mutually exclusive with the read lock) ⇒ the mmap underlying bytes are stable.

**(3) Handling of non-local blocks (B1 safety foundation)**: when a block is not on this Worker, `lockBlock` / `getBlockMeta` throws → `onError` → the client receives a gRPC error → transparent fallback (INV-S1). Therefore a misjudgment of `is_local_address` under containers / multi-homing only costs one wasted RPC, with no impact on correctness.

**The double guarantee (id not reused ∧ lock-held immutable) makes the risk path unreachable**; this lock is exactly the same lock that GooseFS/Java's existing short-circuit read relies on, so this implementation is **equivalently safe** to the existing Java client and introduces no new risk. The SIGBUS handler (abort rather than mis-read) as the last-ditch fallback for "the protocol is somehow broken" is already appropriate.

> The zero-copy `read_bytes` path **does not introduce unsafe**: it uses `Bytes::from_owner` (§3.3), with the bytes crate maintaining the owner lifetime in safe code. The early prototype's `Bytes::from_raw_parts` was a non-existent API and has been discarded.

### 8.2 Drop order

```rust
struct LocalBlockReader {
    block_id: i64,
    file_size: usize,
    mmap: Arc<Mmap>,         // dropped before guard
    _guard: OpenLocalBlockGuard,
}
```

Rust struct field drop order follows declaration order, top to bottom. mmap drops first (munmap → VMA freed), then _guard drops (close bidi → Worker unlock). Even the reverse order is safe (munmap does not depend on the lock), but the current order best matches the semantics of "release resources before releasing permission".

#### 8.2.1 Semantics of async unlock within a synchronous Drop (difference from Java)

Java's `LocalFileDataReader.close()` is `mStream.close() + waitForComplete()`, **synchronously waiting** for the Worker to confirm unlock. Rust's `Drop` is synchronous and **cannot `await`**, so `OpenLocalBlockGuard::drop` can only "close the bidi sender"; the real `onCompleted` → Worker unlock happens in the runtime background, i.e. **eventual release** rather than synchronous release. The following risks must be handled explicitly:

- **Release lag**: there is a window between `Drop` returning and the Worker actually unlocking; negligible under normal load, but INV-S2's semantics must state "the lock is definitely held before reader Drop; the lock is asynchronously released within a bounded time after Drop".
- **runtime shutdown / task no longer polled**: if the task owning the sender is never scheduled again (e.g. the runtime is being destroyed), the unlock RPC may **never flush → lock leak**.
- **Mitigation**: the guard does not directly depend on "the task being Dropped happens to still be polled"; instead it `try_send`s the close signal to a **process-wide reaper task** (an independent resident task) that uniformly `await`s each bidi's completion with a timeout fallback; the reaper does a best-effort flush before runtime shutdown. The Worker side itself should also have an OpenLocalBlock session idle timeout as the ultimate fallback (consistent with Java).
- INV-S2 is refined accordingly: **"the lock is definitely held while the reader is alive; reader Drop triggers async unlock, worst case reclaimed by reaper timeout or Worker session timeout, no infinite leak."**

### 8.3 panic-safety

- In LocalBlockReader::open, any `?` early return Drops the already-constructed guard, auto-unlocking.
- The cached LRU releases by traversing on Drop, without depending on an external close.

### 8.4 Consistency argument (corresponds to §1.1 goals 0a / 0b and §1.3 invariants)

This section gives the argument chain for each invariant, answering "why the SC path remains data/semantically equivalent to the gRPC path under all implemented optimizations".

**Data consistency (INV-D1 ~ INV-D4)**:

- INV-D1 (block file immutable): protocol constraint — the Worker will not truncate / rewrite the sealed block while the OpenLocalBlock lock is held; a committed block is globally read-only. This is the foundation of all SC-path data-consistency arguments.
- INV-D2 (mmap slice ≡ pread bytes): mmap is a memory mapping of the file; the Linux page cache and pread share the same set of page frames, so `&mmap[off..off+len]` and the Worker's `pread(fd, off, len)` return identical bytes. When INV-D1 holds, reading at any time yields consistent content. **Prerequisite**: the mapping length and bounds_check must use the **logical block size** from the OpenLocalBlock response (see §4.1 `file_size` comment); otherwise, when the physical file is preallocated/sparsed larger, mapping by physical length would expose the tail as 0, breaking this invariant.
- INV-D3 (`Bytes` owner lifetime): `read_bytes` loads `Arc<Mmap>` into the `Bytes` owner so the mapping's refcount drops to zero only after the last `Bytes` (including cross-await / cross-task clones) is Dropped; only then is it truly munmap'd. `LocalBlockReader`'s own Drop does not immediately free the underlying mapping, avoiding "old Bytes read a dangling pointer after reader Drop".
- INV-D4 (prefetch changes no bytes): `madvise(MADV_WILLNEED)` is a kernel readahead hint that by definition does not modify page content; it degrades to no-op on tmpfs / NFS etc., equally without affecting byte content.

**Semantic consistency (INV-S1 ~ INV-S5)**:

- INV-S1 (transparent fallback): the §3.6 decision matrix uniformly converts all recoverable errors to gRPC, and the fallback switch happens before "any bytes are returned to the caller", so the final byte sequence observed by the caller is equivalent to "always gRPC".
- INV-S2 (lock lifecycle): `LocalBlockReader`'s field declaration order determines the Drop order: `mmap: Arc<Mmap>` first, `_guard: OpenLocalBlockGuard` last; on guard Drop the bidi stream closes → Worker `onCompleted` → unlock. Any panic / `?` early-exit path relies on Rust's automatic Drop, with the guard always released last. Note that `Drop` is synchronous and cannot `await`, so unlocking is **async eventual release** (§8.2.1): the lock is definitely held while the reader is alive, and after Drop is completed by a background reaper `await`, with reaper timeout + Worker session idle timeout as a double fallback, guaranteeing no infinite leak.
- INV-S3 (capability equivalence): the capability field is explicitly injected into the OpenLocalBlock request; when the cluster rejects it, the same `tonic::Status` as the gRPC path is returned, with no loss of error classification.
- INV-S4 (stable error classification): the §3.6 decision matrix explicitly groups errors — recoverable errors → fallback; semantic errors (`OutOfRange`, bounds_check failure) → directly propagated; fallback never swallows semantic errors.
- INV-S5 (three-API equivalence): `read` returns `&mmap[off..off+len]`; `read_bytes` returns a `Bytes` wrapping the same slice; `read_to_slice` internally `copy_from_slice`s the same slice. All three originate from the same mapping, so the byte content is necessarily identical.

**Torn-read protection**: a single `read(off, len)` at the Rust layer is one slice creation + caller memcpy. The Worker protocol forbids rewriting the block while the lock is held (INV-D1), so during the mmap hold the underlying bytes are unchanged and "half-read-then-changed" cannot occur. Torn/stale-read protection **does not rely on catching SIGBUS at runtime**, but on INV-D1 as the protocol foundation; if the protocol is somehow broken and triggers SIGBUS, the handler chooses `abort` (§3.2) rather than returning half bytes, thus not breaking data consistency. Deployments needing to avoid this risk on untrusted FS should switch to `io.mode=pread`.

---

## 9. Migration compatibility matrix vs Java SC

| Client | Worker | Behavior |
|---|---|---|
| Java SC | Java Worker | keep as is |
| Rust SC | Java Worker | fully compatible (no protocol change) |
| Rust SC + capability | Java Worker (capability on) | compatible |
| Rust SC (DS preferred) | Java Worker (DS on) | DS does not use SC, auto-falls back to gRPC (consistent with Java) |
| Java SC | future Rust Worker | compatible (Worker must implement OpenLocalBlock RPC) |

---

## 10. Implementation roadmap

| Phase | Content | Dependency | Status |
|---|---|---|---|
| P0 | Document review passed | this document | ✅ done |
| P1 | Refactor LocalBlockReader: drop _file, drop spawn_blocking, add MADV_RANDOM | crate memmap2 ≥ 0.9 | ✅ done (`src/block/short_circuit/reader.rs`, zero-copy read/read_bytes/read_to_slice + L2 prefetch) |
| P2 | Implement ShortCircuitFactory (LRU + neg cache) | lru crate | ✅ done (`factory.rs`, bounded LRU + bounded neg cache + decision matrix + sticky EACCES disable) |
| P3 | Wire into BlockInStream::create, capability injection | worker.rs refactor | ✅ done: random/positioned read path (`read_external_range`) and sequential `read()` path both wired into SC, transparent gRPC fallback; `WorkerClient::open_local_block` + RAII guard implemented. capability **instrumentation done** — `CapabilityProvider` trait + factory `with_capability_provider`, injected per block; default no provider sends `None` (works on NOSASL/disabled clusters, auto-fallback on enabled clusters). **SDK contract closed by three cases in `tests/sc_inv_s3.rs`** (S3-a engage baseline / S3-b byte equivalence / S3-c Worker-behavior probe). Only the "real credential source" is an external to-do (dev read path has no `capability_fetcher` yet), and the local SIMPLE+capability.enabled=true measurement shows the Worker admits empty capability under that auth mode, so no provider does not block SC working in that shape (see §3.1 empirical verdict) |
| P4 | metrics + tracing fully wired | metrics/tracing | ✅ done (`Client.ShortCircuit*` 13 counters/gauges + tracing span) |
| — | **End-to-end verification** (local NOSASL cluster) | running Worker | ✅ done: `examples/short_circuit_demo.rs` + `tests/short_circuit_e2e.rs` (4 cases: SC hit, SC vs gRPC byte-consistent INV-S1, sequential read_all, reader reuse). Bonus fix: `WorkerRouter` switched to "bind local interface address" for local-worker determination |
| P5 | bench: sc_seq / sc_pr / sc_lat three suites | criterion | ✅ done (as runnable A/B): `benchmarks/sc_pr_ab.rs` (SC vs gRPC random-read throughput + p50/p99/p999). Measurement in `docs/perf/2026-06-24-sc-pr-ab/`: ×307 throughput, ×261 p99 under hot cache |
| P6 | SIGBUS handler + safe_read fallback | signal-hook | ✅ done (`sigbus.rs`, SA_SIGINFO async-signal-safe diagnostics + abort, unix; uses libc not signal-hook) |
| P7 | Huge pages (THP via MADV_HUGEPAGE) opt-in + measurement | kernel THP support | ◑ opt-in implemented (`short.circuit.thp`, Linux `MADV_HUGEPAGE`, default off); measurement pending on a Linux node |
| P8 | Cross-task shared pool (optional) | decide after evaluation | ✅ done: `ShortCircuitFactory` promoted to `FileSystemContext` (`acquire_short_circuit`); all streams in the same context share one hot-block reader LRU + neg cache — a hot block is `OpenLocalBlock`+mmap'd only once, reused across streams/tasks. Prerequisite: wrap the guard's tonic `Streaming` in a `Mutex` to make `LocalBlockReader: Send+Sync` (compile-time assertion + E2E `short_circuit_reader_shared_across_streams` verification) |

Each phase must attach a PR + bench report + flame graph.

> **Implementation note (as of this commit)**: P1–P6, P8 done; P3 includes capability instrumentation (`CapabilityProvider`), both random and sequential read paths landed and verified on a real cluster for byte equivalence + performance benchmark + cross-stream sharing, **SDK contract gating guarded by three cases in `tests/sc_inv_s3.rs`** (local SIMPLE+capability.enabled=true cluster all green, S3-c probe verdict `ACCEPTED`, see §3.1). Remaining external dependencies: P3 real capability credential source (only a hard blocker when switching to strong auth modes like KERBEROS), P7 Linux THP measurement. Code lives in `src/block/short_circuit/` (`reader.rs`/`factory.rs`/`sigbus.rs`), `src/client/worker.rs`, `src/block/router.rs`, `src/io/file_in_stream.rs`, `src/context.rs` (shared factory). Benchmark `benchmarks/sc_pr_ab.rs`, E2E `tests/short_circuit_e2e.rs` (5 cases) + consistency regression `tests/sc_consistency.rs` (INV-D2/S1/S2/S5) + capability suite `tests/sc_inv_s3.rs` (INV-S3-a/b/c), example `examples/short_circuit_demo.rs`.

---

## 11. Known trade-offs and open issues

1. **Huge pages (THP via `MADV_HUGEPAGE`)**: theoretically reduces TLB misses, but **`MAP_HUGETLB` cannot be used** — the latter only supports anonymous mappings or hugetlbfs backends, and `mmap` on a regular block file on ext4/xfs would `EINVAL`. File pages can only go through transparent huge pages (THP), requested via `madvise(MADV_HUGEPAGE)`; and **file-backed/page-cache THP support highly depends on the kernel version and FS** (most old kernels only apply to anonymous pages), with unstable benefit. Off by default, only an opt-in experiment; keep it only after verifying the benefit with the §5.2.1 flame graph (`do_page_fault`/TLB counts).
2. **Cross-task shared pool**: cross-task reuse reduces open RTT but introduces Arc atomic overhead and lock contention; decide after P5 measurement.
3. **Global SIGBUS handler**: process-wide singleton, needs coordination with the host program (e.g. Python embedding scenario).
4. **mmap vs pread**: in the extreme small-block (< 4KB) random-read scenario, pread may be faster (no page fault + kernel page cache direct copy). Consider providing `goosefs.client.short.circuit.io.mode = mmap | pread` switch.
5. **`MADV_WILLNEED` behaves inconsistently across file systems**: L2 application-layer prefetch relies on kernel async readahead, but the actual effect is strongly tied to the FS hosting the block file and must be treated per category.

   | File system / medium | `MADV_WILLNEED` behavior | Rust SC response |
   |---|---|---|
   | ext4 / xfs (GooseFS Worker default) | standard async readahead, best effect | enabled by default, as expected |
   | tmpfs / ramfs | **direct no-op** (data already in memory, no readahead concept) | auto-skip, zero overhead but zero benefit |
   | NFS | behavior varies by mount option and server implementation; some only trigger prefetch, some ineffective | disable via `prefetch.enabled=false` to avoid misjudged benefit |
   | FUSE / user-space FS | depends on the specific FS implementation; most ineffective or degrade to sync IO | treat as "ineffective" by default, do not rely on it for acceleration |
   | directly mounted block device (no FS) | N/A | SC path never reaches it |
   | kernel < 3.x | historically `MADV_WILLNEED` once triggered readahead **synchronously**, possibly blocking | document declares minimum supported kernel ≥ 4.x; old kernels take the fallback |

   **Runtime probing strategy** (implemented in P5 phase):

   - At startup, do one `statfs` on the Worker data directory to identify the FS type; tmpfs / unknown FS auto-degrade `prefetch.enabled=false`.
   - Bench `sc_prefetch` must run separately on ext4 and tmpfs, verifying both "≥10× p99 improvement on ext4 / no regression on tmpfs".
   - If the user explicitly configures `prefetch.enabled=true` to force-enable, log a warn saying "MADV_WILLNEED may be ineffective on the current FS".

---

## 12. References

- [Java] core/common/.../LocalFileBlockReader.java
- [Java] core/client/fs/.../LocalFileDataReader.java
- [Java] core/client/fs/.../BlockInStream.java
- [Rust SC prototype] goosefs-lance-tests/short_circuit.rs
- [Rust, to be added] `goosefs-client-rust/src/client/worker.rs`'s OpenLocalBlock wrapper (currently `open_local_block` exists only as the generated gRPC stub in `src/generated/com.qcloud.cos.goosefs.grpc.block.rs`; the client-side wrapper is not yet implemented)
- [Rust, current state] `goosefs-client-rust/src/block/router.rs`: local Worker determination is currently implemented via `detect_local_worker` / `local_worker_id` (hostname match); the `is_local_worker(id)` in §2/§3.7 is an abstract name for its semantics, align to this implementation when landing
- [Comparison doc] goosefs-lance-tests/docs/stress-testing/Java_vs_Rust_ShortCircuit_PositionedRead_comparison.md
- Linux mmap(2), madvise(2), pread(2) man pages
- "What every programmer should know about memory" — Drepper, 2007
- Linux Kernel mm/mmap.c: VMA red-black tree & mmap_sem contention notes

