# Flame Graph Optimisation Plan — GooseFS Client (Rust SDK)

Status: Draft (2026-07-06, updated 2026-07-07)
Owner: Rust SDK team
Source data:
- [`docs/perf/2026-07-06-oncpu-goose-vs-local/README.md`](perf/2026-07-06-oncpu-goose-vs-local/README.md)
- [`docs/perf/2026-07-07-hotspot-optimizations/README.md`](perf/2026-07-07-hotspot-optimizations/README.md)
  (oncpu_3 baseline vs. oncpu_4 demo 1200 QPS vs. oncpu_5 after
  multi-conn pool + short-circuit-off, ~900 QPS; oncpu_7 after
  C1/C2/C3 landed — `select_worker` inclusive 11.24 % → 1.13 %, but a
  second `arc_swap` hotspot emerged on the reader Drop path, motivating
  C7/C8 below)

---

## 1. Purpose

This document turns the raw on-CPU flame graph comparison
([GooseFS vs local](perf/2026-07-06-oncpu-goose-vs-local/README.md))
into an **actionable optimisation plan**: what to change, where in the
code base, expected impact, blast radius, verification, and rollout
order.

Scope: `goosefs-client-rust` (this repo) and its Python bindings.
Non-goal: any change to the GooseFS master or worker.

## 2. Baseline recap (from the flame graph)

- Local (LocalFS) run: on-CPU time is dominated by *useful* compute
  (HNSW, `l2_u8_avx2`, DuckDB Bind/Plan/Executor). I/O is **1.8 %**.
- GooseFS run: on-CPU time is dominated by transport + client-side
  bookkeeping. Notable frames (self%):
  - `WorkerRouter::update_workers` **6.84 %**,
    `build_hash_ring` **6.71 %**,
    `init_with_context` **10.4 %** — every open rebuilds the
    consistent-hash ring.
  - `alloc::fmt::format_inner` **5.68 %** +
    `RawVec…::finish_grow/do_reserve` ~6 % — `format!` on hot paths.
  - `hyper H2ClientFuture::poll` **18.1 %** +
    `PipeToSendStream::poll` **15.75 %** — H2 stream count is high.
  - `_raw_spin_lock` **13.2 %**, `__x64_sys_futex` **12.7 %**,
    `futex_wait_setup` **10.6 %** — kernel side of the tokio worker
    pool talking to H2.
  - `MasterClient::get_status` **2.8 %** — one metadata call per open.

## 2.1 Second-round baseline (2026-07-07)

After landing the FileInfoCache + multi-connection worker pool and
flipping `GOOSEFS_SHORT_CIRCUIT_ENABLED=false`, QPS moved from
~100–400 (unstable) to **~900 (stable)**. A fresh set of on-CPU
flame graphs (`oncpu_3.svg` baseline, `oncpu_4.svg` demo binary at
1200 QPS reference, `oncpu_5.svg` current branch at 900 QPS) revealed
a **new** dominant bottleneck that A1/A2 did not capture:

| Metric (self%) | oncpu_3 (baseline) | oncpu_4 (demo, 1200 QPS) | oncpu_5 (post B3+B1, 900 QPS) |
|---|---|---|---|
| `arc_swap::Debt::pay_all` (all copies) | **12.70 %** | 0.00 % | ~25 % under `select_worker` |
| `wait_for_readers` (inc) | 12.92 % | 0.00 % | ~25 % |
| `WorkerRouter::select_worker` (inc) | 7.10 % | 1.69 % | **11.24 %** |
| `pthread_mutex_lock` + `unlock_usercnt` | 16.44 % | 4.74 % | small |
| `DashMap::_retain` (self) | 1.86 % | 1.07 % | still present |
| `l2_u8_avx2` (useful vector compute) | 0.71 % | **6.15 %** | mid |
| `HNSW::search` (useful index compute) | 0.40 % | 1.81 % | mid |
| `hyper h2` / `h2::Connection::poll` | ~0 % | 6–8 % | 5.22 % |
| `__cna_queued_spin_lock_slowpath` | ~6 % | tiny | 0.33 % ✅ |
| `MasterClient::get_status` (heavy path) | present | tiny | tiny (cache hit) |
| `build_hash_ring` on the hot path | present | absent | absent ✅ |

Key take-aways:

1. B3 (`worker_connection_pool_size`) and B1's short-circuit-off
   experiment successfully collapsed kernel spin-lock, `get_status`,
   and `build_hash_ring` overhead. This validates that the earlier
   A1 refactor and B3 default-bump direction were correct.
2. The **new #1 bottleneck is inside `select_worker` itself** — not
   the consistent-hash math (0.09 % self), but four independent
   `ArcSwap::load` calls plus a rogue `ArcSwap::store` on the
   local-worker probe path fired **every scoped snapshot**.
3. The demo binary (oncpu_4) proves that once the router hot path
   is fixed, `select_worker` inclusive time drops to ~1.7 % and
   DuckDB scheduler-lock cascades disappear. This is the target
   the C-series items below aim at.

## 3. Optimisation items

Each item has: **Goal**, **Where**, **Design**, **Risk**, **Rollout**,
**Verification**.

### A1. Reuse `WorkerRouter` across file opens

- **Goal**: Remove the ~13 % `init_with_context` self time caused by
  building a fresh `WorkerRouter` (and hash ring) on every open.
- **Where**:
  - [`src/io/file_reader.rs`](../src/io/file_reader.rs) —
    `GoosefsFileReader::init_with_context`
  - [`src/block/router.rs`](../src/block/router.rs) —
    `WorkerRouter::new`, `WorkerRouter::update_workers`,
    `build_hash_ring`
  - [`src/context.rs`](../src/context.rs) —
    `FileSystemContext::acquire_router` (or equivalent accessor)
- **Design**:
  1. Preferred: `init_with_context` obtains an
     `Arc<WorkerRouter>` from the context and stores it on the reader.
     `mark_failed` becomes a **per-reader override layer** (a small
     `HashSet<worker_id>` on the reader) that the router consults;
     the shared ring is not mutated by any reader.
  2. If per-reader router isolation must remain, clone the two
     `ArcSwap` snapshots (`workers` + `hash_ring`) into a lightweight
     wrapper — do **not** call `build_hash_ring` on the hot path.
  3. Defensive add-on: in `WorkerRouter::update_workers`, xxh3 the
     sorted `(id, host, port)` list and skip `build_hash_ring` when
     the fingerprint matches the currently published one.
- **Risk**: very low. The router is already read-mostly; failure
  isolation is the only behavioural nuance and is preserved by the
  per-reader override set.
- **Rollout**: single PR; behind no feature flag (pure refactor).
- **Verification**:
  - Rerun the same Lance/DuckDB workload; expect
    `init_with_context` self% to drop from ~10.4 % to <1 %.
  - Micro-benchmark `benchmarks/master_hotpath.rs` (or a new
    `router_open_bench.rs`) measuring 10⁵ opens/s.
  - Existing router unit tests + a new test asserting
    `Arc::strong_count(&ctx_router)` grows by exactly 1 per open.
- **Est. saving**: **~13 %** on-CPU.

### A2. Remove `format!` on hash / router hot paths

- **Goal**: Kill `alloc::fmt::format_inner` (5.68 %) and the
  associated `RawVec` growth/copy (~6 %) on paths hit per-block.
- **Where**: [`src/block/router.rs`](../src/block/router.rs)
  - `build_hash_ring` (per virtual node,
    `hash_key(&format!("{worker_id}:{vn}"))`)
  - `worker_addr_key`
    (`format!("{host}:{port}")` per failed-worker DashMap op)
  - `consistent_hash_select_with_ring`
    (`hash_key(&block_id.to_string())` per block route)
- **Design**:
  - Feed the hasher raw bytes directly:
    ```rust
    let mut h = Xxh3::new();
    h.update(&worker_id.to_le_bytes());
    h.update(&(vn as u32).to_le_bytes());
    let hash = h.digest();
    ```
    Same for `block_id: i64` on the block route path.
  - Replace `worker_addr_key: String` with a compact stable key —
    prefer the worker's `id: i64` if it is stable; otherwise a
    `(SmolStr, i32)` newtype. Update `failed_workers: DashMap<Key, _>`
    accordingly.
- **Risk**: very low. Hash **values** change (bytes vs formatted
  strings), so all peers using this ring must run the **same** SDK
  version — but the ring is **client-local** (not exchanged with
  master or workers), so this is a self-contained change.
- **Rollout**: same PR as A1 (they touch the same file) or immediately
  after.
- **Verification**:
  - Router unit tests that assert stability of the hash function
    within one process (not cross-version).
  - Confirm distribution properties (variance across virtual nodes)
    with a statistical test in the router bench.
  - Flame graph rerun: expect `format_inner` self% ≈ 0 and the
    `finish_grow`/`do_reserve_and_handle` frames shrink.
- **Est. saving**: **~5 %** on-CPU.

### A3. Optional short-TTL `FileInfo` cache

- **Goal**: Amortise `MasterClient::get_status` (2.8 %) when the same
  file is opened multiple times inside one query.
- **Where**: [`src/context.rs`](../src/context.rs) —
  `FileSystemContext`, plus the `init_with_context` call site in
  [`src/io/file_reader.rs`](../src/io/file_reader.rs).
- **Design**:
  - Add `file_info_cache: Option<moka::sync::Cache<PathKey, FileInfo>>`
    to `FileSystemContext`, gated by config
    `goosefs.client.file.info.cache.ttl.ms` (default `0` = disabled).
  - On `init_with_context`, look up the cache first; on miss, call
    master, populate.
  - Invalidate on: local write path, explicit `drop_cache`, TTL.
- **Risk**: medium — metadata staleness up to TTL. Users who mutate
  files out-of-band would see stale `length`/`block_ids`. That is why
  it is opt-in.
- **Rollout**: default off. Enable per-workload after A/B.
- **Verification**:
  - New integration test that opens the same path 100× and asserts
    `MasterClient::get_status` counter increments only once.
  - A/B via `bindings/python/benchmarks/run_ab_compare.sh` with the
    Lance workload.
- **Est. saving**: **~1–2 %** on-CPU (workload dependent).

### B1. Audit and improve short-circuit hit rate

- **Goal**: Recover the `hyper` H2 + `opendal` transport time (>40 %
  self across the H2 + opendal frames) whenever a **local** worker is
  present, by ensuring the short-circuit path
  ([`src/block/short_circuit`](../src/block/short_circuit/)) is
  actually taken.
- **Where**:
  - `short_circuit_success` / `short_circuit_fallback` metrics
    ([`src/metrics/mod.rs`](../src/metrics/mod.rs) or equivalent).
  - `is_local_address` and address matching in
    [`src/block/short_circuit/mod.rs`](../src/block/short_circuit/mod.rs).
  - `open_local_block` fallback branches — TCP fallback failures,
    UDS/domain-socket errors, `worker_mode` mismatches.
- **Design (audit steps, not new code)**:
  1. Publish current SC hit / fallback / disabled counters as
     Prometheus/Log fields (they exist; ensure they are exported).
  2. Under the same profiling workload, capture the **fallback reason
     histogram** (already emitted; if not, add one enum-tagged
     counter).
  3. Root-cause the top fallback reason(s):
     - Address matcher fails when master reports `hostname` but
       client resolves `IP`, or vice versa. Fix: normalise both sides
       through `getaddrinfo` / `if_addrs` on startup.
     - Worker not in "SC-enabled" mode. Fix: detect and log at
       first open, not per read.
     - UDS path missing / permission denied. Fix: fail-fast at
       `FileSystemContext` init, not per file.
- **Risk**: medium — behavioural changes in address matching can
  toggle SC on/off on live deployments. Every fix must be behind a
  metric-driven canary.
- **Rollout**: metrics first, then per-cause fixes as separate PRs.
- **Verification**:
  - `short_circuit_success / (success + fallback) ≥ 0.95` on the
    profiling host.
  - Flame graph rerun: expect `hyper H2ClientFuture::poll` and
    `opendal BufferStream::poll_next` self% to drop by roughly the
    same fraction as the SC-hit-rate improvement.
- **Est. saving**: **10–20 %+** on-CPU (situational, capped by
  fraction of blocks that are actually local).

### B2. Coalesce adjacent ranges before `get_ranges`

- **Goal**: Reduce H2 stream count on
  `<Arc<T> as ObjectStore>::get_ranges` (40 % self). Each Lance small
  `get_range` currently becomes one H2 stream; the H2 client's cost
  scales with **stream count**, not bytes.
- **Where** (client-only; **no worker changes**, see
  §5 "Server-side impact"):
  - Preferred: the external
    `opendal_service_goosefs::core::GoosefsCore::open_range_reader`
    (in the `opendal-service-goosefs` crate) — this is the entry
    frame in the flame graph.
  - Or in this repo: add
    `GoosefsFileReader::read_ranges(&[(off, len)]) -> Vec<Bytes>`
    on top of the existing `open_range_with_context`, with internal
    merge + slice mapping. Then have the opendal service call the
    new API.
- **Design**:
  1. Sort input ranges by `offset`.
  2. Merge two adjacent ranges when `next.start - cur.end ≤ gap`,
     where `gap` is a config
     (`goosefs.client.range.coalesce.gap.bytes`, default `65536`).
  3. Cap merged range size
     (`goosefs.client.range.coalesce.max.bytes`, default `4 MiB`)
     to avoid pathological blow-ups.
  4. Issue **one** `open_range_reader(merged_start, merged_len)`
     call, buffer the response, then splice into per-caller
     `Bytes` slices at `[off - merged_start, off - merged_start + len)`.
  5. Reuse the existing `coalesce_ranges` helper from
     [`src/block/short_circuit`](../src/block/short_circuit/) — the
     algorithm is identical; only the "return-slice mapping" is new.
- **Risk**: medium.
  - **Over-read**: the gap bytes between merged sub-ranges are
    fetched but not consumed by the caller. Lance / DuckDB tolerate
    this (they already round to page boundaries). Behaviour must be
    off by default until validated.
  - **Semantic**: caller must observe *exactly* the ranges it asked
    for; the split step must be byte-accurate.
- **Rollout**:
  - Feature flag on: `goosefs.client.range.coalesce.enabled=false`
    by default.
  - Enable in the Lance workload benchmark first.
- **Verification**:
  - Property test: 10⁴ random `(off, len)` sets, assert
    `read_ranges(input) == input.map(read_range)` byte-for-byte.
  - Metric: emit `range_coalesce_input_count`,
    `range_coalesce_output_count`,
    `range_coalesce_wasted_bytes`.
  - Flame graph rerun: `hyper H2ClientFuture::poll` and
    `PipeToSendStream::poll` self% should shrink roughly in
    proportion to `1 - output/input` ratio.
- **Est. saving**: **5–15 %** on-CPU (workload-dependent).

### B3. Bump `worker_connection_pool_size`

- **Goal**: Split H2 flow-control across N connections instead of 1
  to reduce `h2 StreamRef::reserve_capacity` (3.5 %) +
  `send_data` (2.8 %) contention.
- **Where**:
  - [`src/config.rs`](../src/config.rs) (or wherever
    `worker_connection_pool_size` is defined)
  - Wherever the H2 client is constructed (e.g.
    `WorkerClient` factory).
- **Design**: raise default from `1` to `min(cores, 4)`. Expose
  config `goosefs.client.worker.connection.pool.size`. Round-robin
  requests across connections.
- **Risk**: low. Slightly higher memory (per-connection buffers) and
  more sockets on the worker; both are tiny compared to the block
  cache.
- **Rollout**: bump the default in a minor version. Include the new
  key in [`docs/CLIENT_CONFIGURATION.md`](CLIENT_CONFIGURATION.md).
- **Verification**: A/B benchmark; expect `h2::` self% to roughly
  halve.
- **Est. saving**: **3–8 %** on-CPU.

### B4. Cap tokio `worker_threads`

- **Goal**: Reduce `tokio worker::run` (39.6 % self) inflation caused
  by an oversized runtime worker pool.
- **Where**: runtime construction site — the SDK crate itself does
  not build a Tokio runtime (embedders own the `tokio::Builder`); the
  only in-tree runtime the project ships is the Python binding one
  in [`bindings/python/src/runtime.rs`](../bindings/python/src/runtime.rs).
- **Design**:
  - Recommended cap: `worker_threads = min(available_cores, 8)`.
  - Consider moving the metadata (master) path to the blocking
    pool: it is bursty, low-fanout, and does not benefit from being
    on the shared multi-thread scheduler.
  - Expose an env-var override so deployments can cap without
    forking the crate.
- **Risk**: low, but must be perf-tested — under-sizing hurts
  throughput on IO-heavy workloads.
- **Status (2026-07-06)**: **landed as opt-in override + benchmark
  harness; default flip deferred pending workload-specific data.**
  - Python binding runtime honours
    `GOOSEFS_TOKIO_WORKER_THREADS` (and
    `GOOSEFS_TOKIO_MAX_BLOCKING_THREADS`) at module init, clamped to
    `>=1`, with the current `cpus.max(16)` default preserved when
    unset. See
    [`bindings/python/src/runtime.rs::init_custom_runtime`](../bindings/python/src/runtime.rs).
    Deployments that want the §B4 cap set
    `GOOSEFS_TOKIO_WORKER_THREADS=8` (or the value picked by the
    knee-finder below) before importing the wheel.
  - Knee-finder harness lives at
    [`benchmarks/tokio_worker_ab.rs`](../benchmarks/tokio_worker_ab.rs)
    — sweeps `{4, 8, cpus, cpus.max(16)}` (overridable via
    `GFS_WORKERS`) over the same PR `read_at` workload as
    `pr_runtime_ab`, prints per-row MiB/s + p50/p99, and highlights
    the smallest `workers` within 3 % of the best row (the value to
    export as `GOOSEFS_TOKIO_WORKER_THREADS`).
  - Harness has been exercised on 2026-07-06 (14-core loopback
    host, `wpool=1`) across two workloads — PR 1 MiB / conc=16 and
    PR 64 KiB / conc=64. Both produced a **null result**: 4 / 8 / 16
    workers all deliver ~1.6 GiB/s within ±1 %, with `p50 = 0 µs`
    (below the harness's µs sampling floor) and no visible knee.
    Full evidence, raw logs, and the "why this host cannot answer
    §B4" analysis are archived at
    [`docs/perf/2026-07-06-b4-tokio-workers/`](perf/2026-07-06-b4-tokio-workers/README.md).
    Root causes: no cross-host RTT (loopback masks worker parking),
    single H2 channel (`wpool=1` bottleneck is B3, not B4), and the
    PR `read_at` shape does not reproduce the Lance/DuckDB flame
    graph that motivated §B4.
  - The default flip is intentionally *not* landed: the current
    `cpus.max(16)` default is what recent throughput regression
    fixes were validated against (see
    [`docs/RUST_PYTHON_SDK_OPTIMIZATION.md`](RUST_PYTHON_SDK_OPTIMIZATION.md)
    §V.5), and flipping it without site-specific numbers can
    silently cost throughput on IO-heavy workloads. Operators run
    the harness once, pick their knee, and pin it via env var.
    A future default flip requires a run on a host that exhibits
    the ~40 % `tokio worker::run` self time from
    [`docs/perf/2026-07-06-oncpu-goose-vs-local/`](perf/2026-07-06-oncpu-goose-vs-local/README.md).
- **Verification**:
  - `cargo run --release --example tokio_worker_ab` on the target
    host / workload, pick the knee, then A/B via
    `bindings/python/benchmarks/run_ab_compare.sh` with and without
    `GOOSEFS_TOKIO_WORKER_THREADS` exported.
  - Regression guard: env-var parsing has unit tests in
    [`bindings/python/src/runtime.rs`](../bindings/python/src/runtime.rs)
    (`env_usize_missing_and_empty_return_none`,
    `env_usize_parses_valid_values`).
- **Est. saving**: **3–5 %** on-CPU (situational; realised only when
  the knee value is exported).

### C1. Coalesce `ArcSwap` loads inside `select_worker`  (P0)

- **Goal**: Remove the ~25 % CPU (self) burned by
  `wait_for_readers → Debt::pay_all` under 32-way concurrent
  `WorkerRouter::select_worker` calls. Root cause is 4 independent
  `ArcSwap::load` invocations per call plus a `store` on the
  local-worker probe path fired **every scoped snapshot**.
- **Where**:
  - [`src/block/router.rs`](../src/block/router.rs) —
    `WorkerRouter::select_worker`
- **Design**: snapshot each shared field (`workers`, `hash_ring`,
  `local_worker_id`) **exactly once** per call, drive the rest of
  the control flow off the local variables, and reduce the probe
  path to at most one `store()` per router lifetime (paired with C2).
- **Risk**: very low. Semantics unchanged; only the number of atomic
  loads is reduced.
- **Rollout**: single PR; no feature flag.
- **Verification**:
  - Existing `test_select_worker_*` tests must keep passing.
  - Add a test asserting that under 1000 sequential calls no
    additional `local_worker_id.store()` happens after the first.
  - Rerun the flame graph: expect `pay_all` self% ≤ 2 % and
    `select_worker` inclusive ≤ 3 %.
- **Est. saving**: **~15–25 %** on-CPU.

### C2. Inherit `local_worker_id` in `WorkerRouter::snapshot_from`  (P0)

- **Goal**: Kill the ~12.7 % self time spent in `arc_swap::pay_all`
  triggered by every scoped router snapshot re-running
  `detect_local_worker` and issuing a fresh `ArcSwap::store` on its
  first read. The demo binary (oncpu_4) has 0 % here.
- **Where**:
  - [`src/block/router.rs`](../src/block/router.rs) —
    `WorkerRouter::snapshot_from`
- **Design**: replace `local_worker_id: ArcSwap::from_pointee(None)`
  in the snapshot constructor with an inherited value taken from
  the shared parent router:
  `local_worker_id: ArcSwap::new(shared.local_worker_id.load_full())`.
  `local_worker_id` describes the host, not the reader, so sharing
  is semantically correct.
- **Risk**: essentially zero. If the parent has not been probed
  yet, the child inherits `None` and one probe eventually runs for
  the whole router chain.
- **Rollout**: 1-line change; ship with C1 in the same PR.
- **Verification**:
  - `test_snapshot_from_shares_hash_ring_arc` already asserts
    sharing for `hash_ring`. Add
    `test_snapshot_from_shares_local_worker_id` using `Arc::ptr_eq`.
  - Flame graph rerun: `pay_all` self% drops from ~12.7 % to <2 %.
- **Est. saving**: **~12 %** on-CPU.

### C3. Fast-path `cleanup_expired_failures` on empty map  (P0)

- **Goal**: Remove the ~1.5 % self time in `DashMap::_retain`
  triggered by `cleanup_expired_failures` acquiring every shard's
  write lock **on every** `select_worker` call, even when
  `failed_workers` is empty (the common case on a healthy cluster).
- **Where**:
  - [`src/block/router.rs`](../src/block/router.rs) —
    `WorkerRouter::cleanup_expired_failures`
- **Design**:
  ```rust
  fn cleanup_expired_failures(&self) {
      if self.failed_workers.is_empty() { return; }
      self.failed_workers.retain(|_, v| v.elapsed() < self.failure_ttl);
  }
  ```
- **Risk**: none.
- **Rollout**: 3-line change; can ship together with C1/C2.
- **Verification**: unit test asserting no shard lock is taken when
  the map is empty (or a counter-based test); rerun flame graph
  and expect `DashMap::_retain` to fall out of the top-30 self
  frames.
- **Est. saving**: **~1.5 %** on-CPU.

### C4. Reduce `CompleteReader<GoosefsReader>` drop frequency  (P1)

- **Goal**: The oncpu_3 baseline shows a 1:1 ratio between
  `CompleteReader::read` (9.41 % inc) and `drop_in_place` of the
  same reader (9.38 % inc), meaning every range spins up and tears
  down a full opendal reader. Lance vector search reads many
  adjacent ranges on the same file, so the drop should be amortised.
- **Where** (external crate; separate PR):
  - `opendal-service-goosefs` — `GoosefsReader` lifecycle and the
    entry frame `GoosefsCore::open_range_reader`.
  - `object_store_opendal::OpendalStore::get_ranges` — audit
    whether it opens one reader per range or reuses one.
- **Design**:
  1. Confirm whether `get_ranges` in the opendal object-store
     adapter creates a fresh reader per range; if yes, reuse one.
  2. If `GoosefsReader::drop` performs synchronous work (e.g.
     "close block" notifications), consider fire-and-forget via
     `tokio::spawn`.
  3. Consider range coalescing at the opendal service layer for
     adjacent ranges of the same file (aligns with §B2).
- **Risk**: medium — reader-lifecycle refactor across two crates;
  needs careful review.
- **Rollout**: separate PR against the opendal service crate,
  gated by a feature flag until benched.
- **Verification**:
  - Metric: `drop_in_place<CompleteReader<GoosefsReader>>` self%
    ≤ 50 % of the corresponding `read` self%.
  - A/B via `bindings/python/benchmarks/run_ab_compare.sh`.
- **Est. saving**: **3–5 %** on-CPU.

### C5. Trim `open_range_with_context` / `init_with_context`  (P1)

- **Goal**: Reduce per-range initialization overhead. oncpu_5
  still shows `open_range_with_context` at 5.18 % inc and
  `init_with_context` at 4.75 % inc **per range**, even with the
  FileInfoCache and shared router snapshots landed.
- **Where**:
  - [`src/io/file_reader.rs`](../src/io/file_reader.rs) —
    `GoosefsFileReader::init_with_context`,
    `open_range_with_context`
- **Design**:
  1. Cache `block_id` / offset math when a reader opens a file, so
     subsequent ranges compute deltas rather than re-deriving from
     scratch.
  2. Return `Arc<FileInfo>` views out of the cache instead of
     cloning the value each time (avoids the `RawVec` growth path
     that also shows up in oncpu_3).
  3. Once C1/C2/C3 land, revisit whether the per-reader
     `WorkerRouter` snapshot is still needed at all — a shared
     `Arc<WorkerRouter>` reference plus a per-reader failure
     override set (see A1's Design bullet 1) may suffice.
- **Risk**: medium — touches the read hot path; needs unit tests
  and integration benchmarks.
- **Rollout**: separate PR after C1/C2/C3 are validated in prod.
- **Verification**:
  - Micro-benchmark opens/s on the same host.
  - Flame graph rerun: expect both `open_range_with_context` and
    `init_with_context` inclusive time to drop by ≥ 40 %.
- **Est. saving**: **3–5 %** on-CPU.

### C6. Flip `short_circuit_enabled` default to `false`  (P0, config-only)

- **Goal**: Align the SDK's shipped default with the configuration
  that actually performs best on the Lance/DuckDB workload. Empirical
  evidence: exporting `GOOSEFS_SHORT_CIRCUIT_ENABLED=false` on the
  current branch moved QPS from **~600 → ~900** (a **~50 %**
  improvement) without any code change. The demo binary flame graph
  (oncpu_4, 1200 QPS) contains **no** short-circuit related frames
  either, confirming that short-circuit is not on the fast path for
  this class of workload.
- **Where**:
  - [`src/config.rs`](../src/config.rs) —
    `Default for GoosefsConfig` initialiser (line ~1885):
    `short_circuit_enabled: true,` → `short_circuit_enabled: false,`
  - [`docs/CLIENT_CONFIGURATION.md`](CLIENT_CONFIGURATION.md) —
    update the documented default and the migration note.
  - Any existing tests that assert the default value
    (grep `assert!(cfg.short_circuit_enabled)` — currently only
    negative assertions exist at `src/config.rs` lines 4046 / 4076 /
    4105, so the flip is compatible).
- **Design**:
  1. `client_cache_enabled` is **already `false` by default**
     (`src/config.rs` line 1870); no change needed there. The
     documentation in `src/cache/mod.rs` already says so.
  2. Only `short_circuit_enabled` needs to flip from `true` to
     `false`. All plumbing (env var
     `GOOSEFS_SHORT_CIRCUIT_ENABLED`, storage option
     `goosefs_short_circuit_enabled`, builder
     `with_short_circuit_enabled(...)`) is already in place, so
     deployments that rely on short-circuit today can opt back in
     with a single env var — the mechanism is fully backwards
     compatible.
  3. Log a one-line `tracing::info!` in `WorkerRouter::new` (or
     wherever the SC factory is instantiated) recording the
     effective SC flag, so operators can spot a stale env-var
     override without reading the config dump.
- **Risk**: low, but **not zero** — deployments that today rely on
  short-circuit for low-latency single-host reads will see the fast
  path disabled unless they explicitly re-enable it. That is exactly
  why the mechanism to re-enable it (env var + storage option +
  builder) is unchanged. The migration story is one env var per
  affected deployment.
- **Rollout**:
  - Ship in a **minor** version bump (default change is
    user-visible).
  - Include a `CHANGELOG.md` entry listing the exact opt-in flag
    for anyone who wants the old behaviour back.
  - Update the FAQ / `CLIENT_CONFIGURATION.md` table so the
    documented default matches the code default.
- **Verification**:
  - Unit test in `src/config.rs`:
    `assert!(!GoosefsConfig::default().short_circuit_enabled);`
    (mirrors the existing `client_cache_enabled` default test).
  - Rerun the Lance/DuckDB A/B: expect the unmodified default
    build to match the previously-required
    `GOOSEFS_SHORT_CIRCUIT_ENABLED=false` QPS.
  - Confirm the SC test suite
    ([`tests/short_circuit_e2e.rs`](../tests/short_circuit_e2e.rs))
    explicitly opts in via `with_short_circuit_enabled(true)` — do
    **not** allow it to rely on the default.
- **Est. saving**: **~30 %** end-to-end throughput on the profiling
  workload (measured, not projected).

### C7. Eliminate per-reader `ArcSwap` allocations via `WorkerRouterView`  (P0)

> **Status (2026-07-08): Steps 0 / 1 / 2 ✅ landed; Step 3 (delete
> `snapshot_from`) pending.** See
> [`perf/2026-07-07-hotspot-optimizations/README.md`](perf/2026-07-07-hotspot-optimizations/README.md)
> §3.4 for the per-step landing notes and parity test names. After
> Step 2, **all three** production hot paths
> (`file_reader.rs` / `file_writer.rs` / `file_in_stream.rs`) route
> through `WorkerRouterView::from_shared` (context path) or
> `WorkerRouterView::from_workers` (legacy `open()` path); zero
> production call sites reference `WorkerRouter::snapshot_from`.
> Full lib suite: **424 / 424**.
>
> **Post-`oncpu_8` validation (2026-07-08)**: capture on the
> post-P0-D/E branch came in at **~1000 QPS** (up from 900 on
> oncpu_7, target 1200). `arc_swap::debt::list::LocalNode::with` and
> `arc_swap::Debt::pay_all` are both out of the top-30 self frames,
> confirming C7's measured wins. The remaining ~200 QPS gap now
> lives in three smaller sources (per-view `DashMap::new()`,
> opendal-side Reader lifecycle, residual `format!` / `core::fmt`)
> — see the new **P0-F** items in §3.6 of the per-day README and
> §6.3 below for the post-`oncpu_8` execution plan.

> Full analysis + evidence:
> [`docs/perf/2026-07-07-hotspot-optimizations/README.md`](perf/2026-07-07-hotspot-optimizations/README.md)
> §3.4 (P0-D).

- **Goal**: After C1/C2/C3 landed (`oncpu_7`), `select_worker`
  inclusive dropped from 11.24 % to 1.13 %, but QPS stayed ~900. A
  **second, larger** `arc_swap` hotspot surfaced on the reader **Drop**
  path: each per-range `WorkerRouter::snapshot_from` constructs three
  fresh `ArcSwap` fields whose teardown runs through
  `arc_swap::debt::list::LocalNode::with` (**19.55 % self** on the Drop
  side), plus ~2.8 % on the construction side allocating inside
  `snapshot_from`. Eliminate the `ArcSwap`-ness from the per-reader
  path entirely.
- **Where**:
  - [`src/block/router.rs`](../src/block/router.rs) — introduce
    `WorkerRouterView { workers: Arc<Vec<WorkerInfo>>, hash_ring:
    Arc<Vec<(u64, usize)>>, local_worker_id: Option<i64>,
    failed_workers: DashMap<..>, failure_ttl, failed_count }` with
    `from_shared`, `from_workers`, `select_worker`, `mark_failed`,
    `pick_any_worker`.
  - [`src/io/file_reader.rs`](../src/io/file_reader.rs),
    [`src/io/file_in_stream.rs`](../src/io/file_in_stream.rs),
    [`src/io/file_writer.rs`](../src/io/file_writer.rs) — switch the
    per-reader/writer `router` field off `snapshot_from`.
  - `WorkerRouter` (owned by `FileSystemContext`) keeps its `ArcSwap`
    fields; it is the only writer and the background refresh target.
    `ShortCircuitFactory`
    ([`src/block/short_circuit/factory.rs`](../src/block/short_circuit/factory.rs)
    L206) holds the **shared** `Arc<WorkerRouter>` and keeps using
    `is_block_source_local` — the view does not need that method.
- **Design**: the view clones the shared router's `workers` +
  `hash_ring` `Arc`s (wait-free, no rebuild) and captures
  `local_worker_id` as a resolved `Option<i64>` value. No `ArcSwap`
  is constructed or dropped per range, so both the `LocalNode::with`
  Drop cost and the construction-side allocation vanish.
- **Implementation caveats** (must address all three, else the
  migration fails to compile or silently regresses — see README §3.4.3.1):
  1. **Legacy `open()` needs a second constructor.**
     `file_in_stream.rs` L241–242 builds its router from a raw
     `Vec<WorkerInfo>` (`WorkerRouter::new` + `update_workers`), not
     from a shared router. Provide `WorkerRouterView::from_workers(
     workers, failure_ttl)` (builds the ring in-line) so the shared
     `router` field type switch compiles on this path too.
  2. **`local_worker_id` probe must follow every `update_workers`,
     not just run once at init.** `WorkerRouter::update_workers`
     ([`src/block/router.rs`](../src/block/router.rs) L218) resets
     `local_worker_id` to `None` on every worker-**set change** (the
     fingerprint fast-path L200–206 does not). Since the view has no
     `ArcSwap` to write the probe result back, an unprobed shared
     router makes **every** subsequently-minted view skip local-first.
     Re-run `detect_local_worker` wherever the cache is reset (inside
     `update_workers`' slow path + once at context init), or add a
     `WorkerRouter::probe_local_worker` helper called from both.
     **Topology note**: this only matters when the client is
     co-located on a worker node; a remote Lance client detects no
     local worker anyway, so the collapse is harmless — treat it as
     correctness-completeness, not a QPS blocker.
  3. **Test ptr-eq is not portable.** `local_worker_id` is now a
     plain `Option<i64>`; port `test_snapshot_from_shares_local_worker_id`
     as a **value-equality** assertion, not `Arc::ptr_eq`.
- **Risk**: **medium.** New type; three call sites plus the legacy
  `open()` path and `WorkerClientPool` interactions need auditing.
  Failure isolation is unchanged (each view owns its own
  `failed_workers` `DashMap` + `failed_count`).
- **Rollout**: dedicated PR, three-step migration —
  (1) introduce the type + tests without removing `snapshot_from`;
  (2) migrate `file_reader.rs` → `file_in_stream.rs` → `file_writer.rs`,
  running the full suite + the 32-way concurrent Lance workload after
  **each** file; (3) delete `snapshot_from` once no callers remain.
- **Verification**:
  - `test_view_from_shared_shares_hash_ring_arc` (ptr-eq on `workers`
    + `hash_ring`), `test_view_from_shared_inherits_local_worker_id`
    (value equality, all four probe states),
    `test_view_from_workers_builds_hash_ring`,
    `test_context_eager_probes_local_worker`.
  - 32-tasks × 200-ranges stress test; optional `#[cfg(feature =
    "flamegraph")]` regression asserting `arc_swap::debt` frames < 3 %.
  - Rerun `oncpu_8`: `LocalNode::with` self < 2 %,
    `drop_in_place<CompleteReader<…GoosefsReader>>` inc ≤ 5 %,
    QPS ≥ 1100 (target 1200).
- **Est. saving**: **~18–23 %** on-CPU (Drop ~19 % + construction
  ~2.8 %); expected to close the ~900 → 1100–1200 QPS gap.

### C8. `AtomicUsize` short-circuit for `cleanup_expired_failures`  (P0)

> Full analysis: README §3.5 (P0-E).

- **Goal**: After C3's empty-map fast path landed, the `is_empty()`
  check itself still shows up at **0.98 % self** in `oncpu_7` because
  `DashMap::is_empty()` walks every shard with a `try_read`, once per
  `select_worker`. Replace the shard walk with a single `Relaxed`
  atomic load.
- **Where**:
  - [`src/block/router.rs`](../src/block/router.rs) —
    `WorkerRouter::mark_failed`, `cleanup_expired_failures`, plus a new
    `failed_count: AtomicUsize` field. (Same pattern in
    `WorkerRouterView` from C7.)
- **Design**: `mark_failed` does `fetch_add(1, Relaxed)` only when
  `DashMap::insert` returns `None` (new key). `cleanup_expired_failures`
  returns early when `failed_count.load(Relaxed) == 0`; otherwise it
  `retain`s and `fetch_sub`s the number removed.
- **Implementation caveats** (README §3.5.5):
  - Footprint is **~15 lines, not 5–10**: the `use`, the field, and
    **all four** constructors (`new`, `with_failure_ttl`, `with_ttls`,
    `snapshot_from`) must initialise the counter.
  - `pick_any_worker` also calls `cleanup_expired_failures`, so it
    benefits too — no extra work.
  - Correctness invariant: re-inserting an existing key must **not**
    touch the counter; concurrent inserts of the same new key are
    serialised by the per-shard write lock, so exactly one observes
    `None`.
- **Risk**: **very low.** `failed_count` is only a Relaxed fast-path
  gate; a stale `+1` at worst causes one spurious `retain` walk (what
  C3 already tolerates), never a routing error (`is_failed`
  independently checks `elapsed() < failure_ttl`).
- **Rollout**: stand-alone 5–10-line PR. Land it **before** C7 as a
  tool-chain sanity check — it touches only `router.rs`, is
  semantically identical to the current `is_empty()` fast path, and
  produces a clean isolated `oncpu_8` delta
  (`cleanup_expired_failures` self: 0.98 % → ~0 %), validating the
  flame-graph capture + bench harness before the larger C7 refactor.
- **Verification**:
  - `test_cleanup_expired_failures_counter_stays_in_sync`: insert N
    failures, wait past the TTL, call `cleanup_expired_failures`,
    assert `failed_count == 0` and `failed_workers.is_empty()`.
  - Rerun `oncpu_8`: `cleanup_expired_failures` self < 0.1 %.
- **Est. saving**: **~1 %** on-CPU.

## 4. Suggested landing order

Ranked by *(expected on-CPU saving) / (behavioural risk)*:

| # | Item | Est. saving | Risk    | PR shape |
|---|------|------------:|---------|----------|
| 1 | **C6 — flip `short_circuit_enabled` default to `false`**     | **~30 % throughput (measured)** | **low**   | ✅ landed — 1-line default flip + docs |
| 2 | A1 — reuse `WorkerRouter` in `init_with_context`             | ~13 %                   | very low  | pure refactor (landed) |
| 3 | A2 — remove `format!` on hash / router hot paths             | ~5 %                    | very low  | pure refactor (landed) |
| 4 | **C2 — inherit `local_worker_id` in `snapshot_from`**        | **~12 %**               | **very low** | ✅ landed — 1-line fix |
| 5 | **C1 — coalesce `ArcSwap` loads in `select_worker`**         | **~15–25 %**            | **low**   | ✅ landed — single-file refactor |
| 6 | **C3 — fast-path `cleanup_expired_failures` on empty map**   | **~1.5 %**              | **none**  | ✅ landed — 3-line fix |
| 7 | **C8 — `AtomicUsize` short-circuit for `cleanup_expired_failures`** | **~1 %**         | **very low** | ✅ landed — 15-line fix + 2 regression tests (tool-chain sanity check) |
| 8 | **C7 — eliminate per-reader `ArcSwap` via `WorkerRouterView`** | **~18–23 %**          | **medium** | ✅ Steps 0/1/2 landed — new type + 3-file migration; Step 3 (delete `snapshot_from`) pending, awaiting `oncpu_8` capture |
| 9 | B1 — audit short-circuit hit rate + per-cause fixes          | 10–20 % (situational)   | medium    | metrics + N small PRs |
| 10 | B2 — coalesce adjacent ranges before `get_ranges`           | 5–15 % (situational)    | medium    | feature-flagged |
| 11 | B3 — bump `worker_connection_pool_size`                     | 3–8 %                   | low       | config default bump (landed) |
| 12 | C4 — reduce `CompleteReader<GoosefsReader>` drop frequency  | 3–5 %                   | medium    | opendal-service PR |
| 13 | C5 — trim `open_range_with_context` / `init_with_context`   | 3–5 %                   | medium    | reader hot-path PR |
| 14 | B4 — cap tokio `worker_threads`                             | 3–5 %                   | low       | ✅ opt-in override + bench harness landed; default flip deferred |
| 15 | A3 — short-TTL `FileInfo` cache in context                  | 1–2 %                   | needs flag| opt-in cache (landed as FileInfoCache) |

**Post-`oncpu_8` next round (QPS 1000 → 1080-1150 target)** —
documented in detail at
[`docs/perf/2026-07-07-hotspot-optimizations/README.md`](perf/2026-07-07-hotspot-optimizations/README.md)
§3.6 (P0-F) and §6.3 (execution plan):

| # | Item | Est. saving | Risk | PR shape |
|---|------|------------:|------|----------|
| P0-F.1 | lazy-init `failed_workers: OnceLock<DashMap>` | ~3–5 %   | very low | ~30 lines, 4 ctors + mark_failed/is_failed/cleanup |
| P0-F.2 | `itoa` in `worker_addr_key`                          | ~0.5–1 % | very low | 1-line swap + dep |
| P0-F.3 | `format!` / `core::fmt` cleanup in IO files          | ~1–2 %   | low      | pure refactor, 3 files |
| P0-F.4 | `ShortCircuitFactory` lazy construction             | ~0.5–1 % | low      | 1 module + `acquire_short_circuit` accessor |

**Recommended immediate action (2026-07-07, post-`oncpu_7`)**: C6 +
C1/C2/C3 have **landed**, which brought `select_worker` inclusive from
11.24 % → 1.13 % but left QPS at ~900 because a **second** `arc_swap`
hotspot emerged on the reader **Drop** path. Next round:

1. Land **C8** first as a stand-alone 5–10-line PR — zero-risk, ~1 %,
   and it doubles as a validation that the flame-graph capture + bench
   harness can resolve sub-percent deltas before the bigger C7 refactor.
2. Land **C7** (`WorkerRouterView`) as a dedicated PR using the
   three-step migration (introduce type + tests → migrate the three
   IO files one at a time, running the 32-way Lance workload after
   each → delete `snapshot_from`). This is expected to close the last
   gap from ~900 QPS to the ~1200 QPS demo baseline.
3. Re-capture `oncpu_8` and check the C7/C8 success criteria before
   moving on to B1/B2.

**Recommended immediate action (2026-07-08, post-`oncpu_8`)**: C7
(`WorkerRouterView`) and C8 (`AtomicUsize` short-circuit) have
**landed**; the post-`oncpu_8` capture shows QPS at ~1000 with the
`arc_swap` family completely absent from the top-30 self frames
(closure of the originally-stated goal). The remaining ~200 QPS
gap to the 1200 QPS demo binary now lives in three smaller sources.
Next round (full detail in the per-day README §3.6 / §6.3):

1. Land **P0-F.1** (`OnceLock<DashMap>` lazy-init) first — the
   single biggest win (~3-5 %), 4-ctor touch + `mark_failed` /
   `is_failed` / `cleanup_expired_failures` plumbing, very low risk.
2. Land **P0-F.3** (`format!` cleanup in IO files) next — pure
   refactor across `file_reader.rs` / `file_in_stream.rs` /
   `file_writer.rs` (~1-2 %).
3. Land **P0-F.2** (`itoa` in `worker_addr_key`) — 1-line swap once
   `itoa` is in `Cargo.toml`; mechanical (~0.5-1 %).
4. Land **P0-F.4** (`ShortCircuitFactory` lazy construction) last
   — touches a separate module but is the smallest win (~0.5-1 %).
5. Re-capture `oncpu_9` and check the P0-F success criteria
   (per-day README §7.1) before opening any cross-crate PR.
6. If QPS is still below the demo after P0-F, proceed with
   **P1-A** (opendal-side `Reader` reuse) — cross-crate work
   against `opendal-service-goosefs`, gated by a feature flag.

Items 1 and 2 are pure implementation cleanups (behaviour-preserving)
and should ship first; the rest need matching benchmarks
(`bindings/python/benchmarks/run_ab_compare.sh`,
`benchmarks/master_hotpath.rs`) to confirm the savings and rule out
tail-latency regressions.

## 5. Server-side impact

**None of these items require any change to GooseFS master or worker.**

- A1 / A2 / A3 are purely client-internal (router bookkeeping,
  hashing, metadata cache).
- B1 changes *how* the client picks the SC path; the worker's
  short-circuit protocol is unchanged.
- **B2** merges N small range requests into one larger range request
  on the client. The worker sees the same `ReadBlock` RPC shape; it
  just receives fewer (larger) requests. No protocol change, no new
  fields, no rolling upgrade required. Old and new workers both
  respond correctly.
- B3 / B4 are runtime / connection-pool tuning on the client only.
- **C1 / C2 / C3 / C7 / C8** are pure client-side router bookkeeping;
  the master and workers see identical wire traffic. C7 only changes
  the in-process type that holds the routing snapshot; C8 only changes
  how the client gates a local cleanup pass.
- **C4 / C5** are client-side lifecycle / hot-path cleanups; no
  RPC-level change.
- **C6** flips a client-side default. The short-circuit protocol on
  the worker is untouched; when a deployment opts back in via
  env var / storage option / builder, the on-wire behaviour is
  identical to today.

This makes the plan safe to roll out **independently of any GooseFS
server release cadence**.

## 6. Verification harness

Consolidated per-item, so we can rerun after every batch:

1. **Unit / property tests** local to the touched module (see each
   item's *Verification* section).
2. **Micro-benchmarks** in `benchmarks/`:
   - `master_hotpath.rs` (existing) — validates A1/A2/A3.
   - Add `range_coalesce_bench.rs` for B2.
   - Add `router_open_bench.rs` for A1/A2 (10⁵ opens/s).
   - Add `router_select_bench.rs` for **C1/C2/C3/C7/C8** — measures
     `select_worker` throughput under 32/64/128 concurrent tasks
     with and without the ArcSwap-coalescing patch. For **C7**, extend
     it to measure per-range router **construct + Drop** cost
     (`WorkerRouterView::from_shared` vs. `WorkerRouter::snapshot_from`)
     so the `LocalNode::with` Drop win is captured, not just the
     select throughput.
   - `tokio_worker_ab.rs` (landed) — knee-finder for B4; run once
     per host / workload before pinning
     `GOOSEFS_TOKIO_WORKER_THREADS`.
3. **Macro A/B**: `bindings/python/benchmarks/run_ab_compare.sh` with
   the same Lance/DuckDB workload used to produce the flame graph.
4. **Flame graph regression**:
   - Regenerate on-CPU flame graphs following the "Reproducing"
     section of the source report.
   - Diff against
     [`docs/perf/2026-07-06-oncpu-goose-vs-local/README.md`](perf/2026-07-06-oncpu-goose-vs-local/README.md).
   - Success criterion per item: named self% shrinks by at least
     the item's *Est. saving* lower bound.

## 7. Non-goals / explicitly deferred

- **Custom transport (io_uring / QUIC / SPDY-like)**: covered by a
  separate feasibility document
  ([`docs/SHORT_CIRCUIT_IO_URING_FEASIBILITY.md`](SHORT_CIRCUIT_IO_URING_FEASIBILITY.md)).
  Not part of this plan.
- **Master-side changes** (RPC batching, streaming metadata):
  addressed in
  [`docs/RUST_PYTHON_SDK_OPTIMIZATION.md`](RUST_PYTHON_SDK_OPTIMIZATION.md);
  not revisited here.
- **Page-cache re-work**: covered by
  [`docs/CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md).

## 8. Open questions

1. Does `WorkerInfo.id` (used as the router key candidate for A2)
   remain stable across worker restarts? If not, keep the `(host,
   port)` compact key.
2. What is the SC hit rate today on the profiling host? Needed to
   size B1 before we invest in per-cause fixes.
3. For B2, does Lance ever call `get_ranges` with **overlapping**
   ranges? The merge algorithm assumes sorted, non-overlapping
   inputs; if overlaps exist we need an explicit dedup step.
4. ~~Once C1/C2/C3 land, is the per-reader `WorkerRouter` snapshot
   still needed at all?~~ **Answered by C7**: `oncpu_7` showed the
   per-reader `ArcSwap` fields dominate the Drop path (~19 %), so the
   snapshot *is* the problem. C7 replaces it with a `WorkerRouterView`
   that clones two `Arc`s + a resolved `Option<i64>` (no `ArcSwap`),
   keeps per-reader failure isolation via a per-view `failed_workers`
   `DashMap`, and deletes `snapshot_from` in its step 3. The
   `HashSet<worker_id>` override idea from A1 is superseded by the view.
5. After C6, is there any remaining workload class that genuinely
   benefits from short-circuit-on by default? Small-object reads
   from a co-located worker with a warm block cache **could**
   still favour SC — if such a workload exists in our benchmark
   matrix, C6 should ship together with a documented opt-in
   recipe (env var snippet + guidance on when to use it).
6. `client_cache_enabled` is already `false` by default; the
   flame graph shows no page-cache frames on the hot path. If a
   future workload ever motivates flipping it on, the same
   audit that motivated C6 (measure end-to-end QPS under both
   defaults on a representative workload) must be repeated first.

## 9. References

- Raw analysis:
  [`docs/perf/2026-07-06-oncpu-goose-vs-local/README.md`](perf/2026-07-06-oncpu-goose-vs-local/README.md)
- Related design docs in this folder:
  [`SHORT_CIRCUIT_DESIGN.md`](SHORT_CIRCUIT_DESIGN.md),
  [`SHORT_CIRCUIT_IO_URING_FEASIBILITY.md`](SHORT_CIRCUIT_IO_URING_FEASIBILITY.md),
  [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md),
  [`RUST_PYTHON_SDK_OPTIMIZATION.md`](RUST_PYTHON_SDK_OPTIMIZATION.md),
  [`CLIENT_CONFIGURATION.md`](CLIENT_CONFIGURATION.md),
  [`METRICS.md`](METRICS.md).
