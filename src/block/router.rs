// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Worker router: maps block IDs to workers using consistent hashing.
//!
//! Goosefs uses consistent hashing to decide which worker should serve
//! a particular block. This module implements the routing logic with:
//! - Consistent hash ring based on worker IDs
//! - Failed-worker filtering with configurable TTL
//! - Thread-safe worker list updates via `RwLock<Arc<...>>`
//! - Worker list TTL — auto-refresh after `worker_refresh_ttl` (default 30 s)
//! - Local worker preference — detect the local worker by hostname/IP
//!   and route block reads there first (mirrors Java `LocalFirstPolicy`)

use std::hash::Hasher;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::{debug, warn};
use xxhash_rust::xxh3::Xxh3Default;

use arc_swap::ArcSwap;

use crate::error::{Error, Result};
use crate::proto::grpc::block::WorkerInfo;
use crate::proto::grpc::WorkerNetAddress;

/// How long a worker is considered "failed" before being retried.
const DEFAULT_FAILURE_TTL: Duration = Duration::from_secs(60);

/// Default TTL for the worker list before a background refresh is triggered.
/// Matches Go SDK's `WorkerRefreshPeriod = 30s`.
const DEFAULT_WORKER_REFRESH_TTL: Duration = Duration::from_secs(30);

/// Number of virtual nodes per worker in the hash ring.
const VIRTUAL_NODES_PER_WORKER: u32 = 100;

/// P0-F.1:
/// allocate the `failed_workers` `DashMap` with **2 shards** instead of the
/// default 4. The failure map is written only by `mark_failed` (rare,
/// error-only path) and read by `is_failed` / `cleanup_expired_failures`
/// (both already gated by the `failed_count` atomic fast path). Using the
/// minimum allowed shard count (DashMap 6.x asserts `shard_amount > 1`)
/// halves the `RwLock` + `HashMap` + `RawVec` allocation cost on the rare
/// path where the map is actually constructed, while the `OnceLock`
/// lazy-init (see `failed_workers` field docs) avoids allocating it at all
/// on the happy path.
fn new_failed_workers_map() -> DashMap<String, Instant> {
    DashMap::with_capacity_and_shard_amount(0, 2)
}

/// Thread-safe worker router with consistent hashing, failure tracking,
/// TTL-based refresh, and local-worker preference.
pub struct WorkerRouter {
    /// Current snapshot of workers — published wait-free via [`ArcSwap`].
    ///
    /// The hot `select_worker` / `pick_any_worker` path does a single atomic
    /// `load` (no `tokio::sync::RwLock` round-trip per op); `update_workers`
    /// publishes a fresh `Arc<Vec<WorkerInfo>>` via `store`. Readers see either
    /// the old or new snapshot, never a torn mix.
    workers: ArcSwap<Vec<WorkerInfo>>,
    /// Tracks recently failed worker addresses with the failure timestamp.
    ///
    /// **P0-F.1**    /// §3.6.1): wrapped in `OnceLock` so the `DashMap` is only allocated on
    /// the first `mark_failed` call. On the happy path (healthy cluster,
    /// no failures) — which is the overwhelming majority of readers —
    /// **zero** heap allocation occurs for failure tracking. `OnceLock::get()`
    /// is a single `Acquire` atomic load (~1 ns), cheaper than even the
    /// `failed_count.load(Relaxed)` gate from P0-E. The `DashMap` itself is
    /// constructed via [`new_failed_workers_map`] (1 shard, not the default
    /// 4) to further reduce the allocation cost on the rare error path.
    failed_workers: OnceLock<DashMap<String, Instant>>,
    /// Approximate size of `failed_workers`, used as a wait-free fast-path
    /// gate in `cleanup_expired_failures` (P0-E).
    ///
    /// `DashMap::is_empty()` walks every shard and takes a `try_read` on
    /// each, which showed up as ~0.98 % CPU self time on `oncpu_7.svg`
    /// even on a healthy cluster where the map stays empty for the entire
    /// process lifetime. Replacing that shard walk with a single `Relaxed`
    /// atomic load is exact when the counter is in sync and, in the worst
    /// case of a transient over-count, only causes one spurious `retain`
    /// walk — the same cost the previous `is_empty()` fast path already
    /// tolerated.
    ///
    /// Invariants:
    /// - Incremented by `mark_failed` **only** when `DashMap::insert`
    ///   returns `None` (new key). Re-inserts of an existing key must
    ///   not touch the counter, otherwise it drifts monotonically
    ///   upward on repeated failures of the same worker.
    /// - Decremented by `cleanup_expired_failures` by exactly the number
    ///   of entries `retain` removed.
    /// - The counter is **not** required to be perfectly in sync with
    ///   `failed_workers.len()` for correctness of routing (`is_failed`
    ///   still gates on `elapsed() < failure_ttl` and never trusts the
    ///   counter). It is only a fast-path hint.
    failed_count: AtomicUsize,
    /// Duration after which a failed worker is eligible again.
    failure_ttl: Duration,

    // ── Worker list TTL ─────────────────────────────────────────────────────────
    /// Timestamp of the last worker-list update.
    ///
    /// **H3**):
    /// changed from `tokio::sync::RwLock<Instant>` to `std::sync::Mutex<Instant>`
    /// — the critical section is a single `*ptr = Instant::now()` /
    /// `instant.elapsed()` (nanoseconds), so a synchronous `std::sync::Mutex`
    /// is strictly cheaper than an async `RwLock` (no tokio task scheduling,
    /// no `await` round-trip). This is only called on the shared router's
    /// background refresh path (not on per-range-read), so the win is small
    /// (~0.3-0.5 %) but the code is simpler.
    last_refresh: Mutex<Instant>,
    /// Duration after which the worker list is considered stale.
    worker_refresh_ttl: Duration,

    // ── Local worker cache ──────────────────────────────────────────────────
    /// Cached result of local-worker detection.
    ///
    /// - `None`              → not probed yet (next `select_worker` will probe)
    /// - `Some(None)`        → probed and confirmed there is **no** local worker
    /// - `Some(Some(id))`    → probed and found local worker with this ID
    ///
    /// The previous design used `i64 = 0` to mean both "not probed" and
    /// "no local worker found", which forced `hostname::get()` and a write
    /// lock acquisition on every `select_worker()` call when no local
    /// worker was present. Distinguishing the two states eliminates that
    /// hot-path overhead.
    local_worker_id: ArcSwap<Option<Option<i64>>>,

    // ── Pre-built consistent-hash ring ──────────────────────────────────────
    /// Sorted `(hash, worker_index)` pairs across all virtual nodes.
    ///
    /// Built once in `update_workers` and published wait-free via [`ArcSwap`]
    /// alongside `workers`, so the hot `select_worker` path does only an
    /// O(log N) `binary_search`.
    /// Previously the ring was rebuilt + sorted on every call (O(N log N)
    /// with N typically `VIRTUAL_NODES_PER_WORKER * worker_count`).
    hash_ring: ArcSwap<Vec<(u64, usize)>>,
}

impl WorkerRouter {
    /// Create a new router with an empty worker list and default TTLs.
    pub fn new() -> Self {
        Self {
            workers: ArcSwap::from_pointee(Vec::new()),
            failed_workers: OnceLock::new(),
            failed_count: AtomicUsize::new(0),
            failure_ttl: DEFAULT_FAILURE_TTL,
            last_refresh: Mutex::new(Instant::now()),
            worker_refresh_ttl: DEFAULT_WORKER_REFRESH_TTL,
            local_worker_id: ArcSwap::from_pointee(None),
            hash_ring: ArcSwap::from_pointee(Vec::new()),
        }
    }

    /// Create a router with a custom failure TTL.
    pub fn with_failure_ttl(failure_ttl: Duration) -> Self {
        Self {
            workers: ArcSwap::from_pointee(Vec::new()),
            failed_workers: OnceLock::new(),
            failed_count: AtomicUsize::new(0),
            failure_ttl,
            last_refresh: Mutex::new(Instant::now()),
            worker_refresh_ttl: DEFAULT_WORKER_REFRESH_TTL,
            local_worker_id: ArcSwap::from_pointee(None),
            hash_ring: ArcSwap::from_pointee(Vec::new()),
        }
    }

    /// Create a router with custom failure TTL and worker refresh TTL.
    pub fn with_ttls(failure_ttl: Duration, worker_refresh_ttl: Duration) -> Self {
        Self {
            workers: ArcSwap::from_pointee(Vec::new()),
            failed_workers: OnceLock::new(),
            failed_count: AtomicUsize::new(0),
            failure_ttl,
            last_refresh: Mutex::new(Instant::now()),
            worker_refresh_ttl,
            local_worker_id: ArcSwap::from_pointee(None),
            hash_ring: ArcSwap::from_pointee(Vec::new()),
        }
    }

    /// Build a lightweight per-caller router that **shares** the current
    /// worker snapshot and consistent-hash ring of `shared` — without
    /// rebuilding the ring.
    ///
    /// This is the hot-path constructor used by
    /// `GoosefsFileReader::init_with_context` /
    /// `GoosefsFileInStream::open_with_context` /
    /// `GoosefsFileWriter::ensure_router_init` (see optimisation A1 in
    /// 
    ///
    /// Semantics guaranteed:
    /// - `select_worker` observes the **exact** ring the shared router had at
    ///   snapshot time (both `workers` and `hash_ring` `Arc`s are cloned via
    ///   `ArcSwap::load_full`, not rebuilt).
    /// - `mark_failed` writes into a **fresh, per-caller** `failed_workers`
    ///   set, so failure state stays local to the reader/writer instance —
    ///   byte-for-byte the same isolation the previous
    ///   `WorkerRouter::new() + update_workers()` pattern provided.
    /// - The local-worker probe cache is fresh (`None`) so the caller
    ///   independently probes on first use, matching the previous behaviour
    ///   exactly.
    /// - The refresh TTL clock is left at "now" so a fresh scoped router does
    ///   NOT trigger an unrelated background refresh right after creation.
    pub fn snapshot_from(shared: &WorkerRouter) -> Self {
        Self {
            // Wait-free snapshot of the shared ring's current pointee — no
            // rebuild, no re-sort, no re-hash. This is the whole point of A1.
            workers: ArcSwap::new(shared.workers.load_full()),
            hash_ring: ArcSwap::new(shared.hash_ring.load_full()),
            failed_workers: OnceLock::new(),
            // P0-E: fresh, per-snapshot counter mirrors the fresh
            // `failed_workers` map above (both start empty). Must NOT
            // inherit the parent's count — snapshot failure state is
            // isolated by design (see `test_snapshot_mark_failed_is_isolated_from_parent`).
            failed_count: AtomicUsize::new(0),
            failure_ttl: shared.failure_ttl,
            last_refresh: Mutex::new(Instant::now()),
            worker_refresh_ttl: shared.worker_refresh_ttl,
            // C2 §3.2):
            // inherit the parent's already-probed `local_worker_id` instead
            // of resetting it to `None`. `local_worker_id` describes the
            // host ("which registered worker, if any, is co-located with
            // this process"), not the reader — it's the same answer on
            // every scoped snapshot.
            //
            // The old `ArcSwap::from_pointee(None)` forced every new
            // `GoosefsFileReader` to (a) re-run `detect_local_worker`
            // (including the syscall in `hostname::get()`) and (b) issue
            // a fresh `ArcSwap::store` on the first `select_worker` call,
            // which was worth ~12.7 % CPU self time in `arc_swap::Debt::
            // pay_all` on `oncpu_3.svg`. The demo binary (`oncpu_4.svg`,
            // 1200 QPS) has zero samples in that stack — inheriting the
            // Arc closes that gap for free.
            //
            // If the parent has never been probed, the child inherits
            // `Arc::new(None)` and the first snapshot to call
            // `select_worker` performs one probe for the whole chain.
            local_worker_id: ArcSwap::new(shared.local_worker_id.load_full()),
        }
    }

    /// Update the full worker list (snapshot replace pattern).
    ///
    /// Also resets the TTL clock so the list won't be considered stale
    /// immediately after an explicit update, and **rebuilds the consistent
    /// hash ring** so the hot `select_worker` path can do an O(log N)
    /// lookup instead of rebuilding+sorting on every call.
    ///
    /// The rebuild is **skipped** when the new sorted `(id, host, port)`
    /// fingerprint matches the currently published ring (defensive add-on
    ///  repeated background
    /// refreshes that observe the same worker set no longer pay the
    /// N·V·xxh3 + O(N·V·log(N·V)) sort cost.
    pub async fn update_workers(&self, workers: Vec<WorkerInfo>) {
        // Fast-path: skip the rebuild if the new set is identical to the
        // currently-published one. The fingerprint is order-independent and
        // covers everything the ring depends on (id, host, port,
        // virtual_node_num).
        let new_fp = workers_fingerprint(&workers);
        let cur_workers = self.workers.load_full();
        let cur_fp = workers_fingerprint(&cur_workers);

        if new_fp == cur_fp && !cur_workers.is_empty() {
            // Ring is unchanged — do NOT rebuild, do NOT invalidate the
            // local-worker probe cache (it is still valid). Only bump the
            // refresh clock so the caller's TTL semantics are preserved.
            *self
                .last_refresh
                .lock()
                .expect("last_refresh mutex poisoned") = Instant::now();
            return;
        }

        // Slow-path: worker set actually changed → rebuild the ring.
        let new_ring = Arc::new(build_hash_ring(&workers));
        let new_snapshot = Arc::new(workers);

        // Atomic wait-free publication (readers never block / never tear).
        self.workers.store(new_snapshot.clone());
        self.hash_ring.store(new_ring);
        // Reset refresh clock
        *self
            .last_refresh
            .lock()
            .expect("last_refresh mutex poisoned") = Instant::now();

        // D-Step0        // §3.4.3.1 Caveat 2): re-run the local-worker probe **synchronously
        // right here** instead of leaving `local_worker_id` in the
        // "unprobed" state (`Arc::new(None)`) for the next `select_worker`
        // to lazily populate.
        //
        // Rationale: P0-D is going to replace the per-reader `ArcSwap`
        // fields with an immutable `WorkerRouterView`, which no longer has
        // a writer for `local_worker_id`. If the shared router is left
        // unprobed at any point, every view minted afterwards would
        // permanently collapse `local_worker_id` to `None` and silently
        // skip local-first routing. Probing here — inside the only place
        // the cache is reset — guarantees the shared router is
        // always-probed, so every `WorkerRouterView::from_shared` (to be
        // introduced in P0-D Step 1) inherits a resolved
        // `Some(_)` regardless of whether short-circuit is on.
        //
        // `detect_local_worker` is off the hot path: `update_workers` is
        // only called by the background refresh task in
        // `FileSystemContext` (default `worker_refresh_ttl = 30 s`), and
        // then only on the slow path where the worker set actually
        // changed. The syscall in `hostname::get()` plus the O(N) scan
        // over `new_snapshot` is negligible at that frequency.
        let detected = Self::detect_local_worker(&new_snapshot).await;
        let cached = if detected > 0 { Some(detected) } else { None };
        self.local_worker_id.store(Arc::new(Some(cached)));
    }

    /// Get a snapshot of the current worker list.
    pub async fn get_workers(&self) -> Arc<Vec<WorkerInfo>> {
        self.workers.load_full()
    }

    /// Whether the worker list is currently empty.
    ///
    /// **H4**):
    /// the old `init_with_context` called `get_workers().await.len()` just to
    /// check non-empty — that does a full `Arc::clone` (inc + dec on the
    /// `Arc<Vec<WorkerInfo>>`) for no reason. This method borrows the
    /// `Guard` and checks `is_empty()` directly, saving one atomic
    /// inc + dec per range read.
    pub fn workers_is_empty(&self) -> bool {
        self.workers.load().is_empty()
    }

    // ── TTL helpers ─────────────────────────────────────────────────────────────

    /// Returns `true` if the worker list is older than `worker_refresh_ttl`.
    ///
    /// **H3**: `std::sync::Mutex` lock + `elapsed()` — synchronous, no tokio
    /// scheduling overhead. Still `async fn` for API compatibility (callers
    /// `await` it; the body is synchronous so the future resolves immediately).
    pub async fn needs_refresh(&self) -> bool {
        self.last_refresh
            .lock()
            .expect("last_refresh mutex poisoned")
            .elapsed()
            >= self.worker_refresh_ttl
    }

    /// Refresh the worker list by calling `get_worker_info_list()` on the
    /// given `WorkerManagerClient`.
    ///
    /// This is a **blocking** refresh (awaited inline).  For a non-blocking
    /// background refresh, the caller should `tokio::spawn` this call.
    ///
    /// Returns `Ok(())` on success; logs a warning and returns `Ok(())` on
    /// failure so the stale list keeps serving traffic (stale-while-revalidate).
    pub async fn refresh_workers(&self, wm: &crate::client::WorkerManagerClient) -> Result<()> {
        match wm.get_worker_info_list().await {
            Ok(workers) => {
                debug!(count = workers.len(), "worker list refreshed");
                self.update_workers(workers).await;
                Ok(())
            }
            Err(e) => {
                warn!("worker list refresh failed, keeping stale list: {}", e);
                // Reset the clock anyway to avoid hammering the master on each call
                *self
                    .last_refresh
                    .lock()
                    .expect("last_refresh mutex poisoned") = Instant::now();
                Ok(())
            }
        }
    }

    // ── Local worker detection ────────────────────────────────────────────────

    /// Detect and cache the ID of the local worker, if any.
    ///
    /// A worker is "local" when its registered `host` is either a known local
    /// name (`localhost` / loopback / this machine's hostname) **or** a local
    /// interface address. The latter is decisive in practice: Goosefs workers
    /// usually register with their LAN IP (e.g. `10.x.x.x`), not loopback or
    /// hostname (SHORT_CIRCUIT_DESIGN §3.7).
    ///
    /// Returns the worker ID of the local worker, or `0` if none found.
    async fn detect_local_worker(workers: &[WorkerInfo]) -> i64 {
        let local_names = Self::local_hostnames();

        for w in workers {
            if let Some(addr) = &w.address {
                let host = addr.host.as_deref().unwrap_or("");
                if host.is_empty() {
                    continue;
                }
                if local_names.iter().any(|n| n == host) || Self::is_local_address(host) {
                    let id = w.id.unwrap_or(0);
                    debug!(host = %host, worker_id = id, "detected local worker");
                    return id;
                }
            }
        }
        0
    }

    /// Collect the set of names that statically identify the local machine:
    /// `localhost` / `127.0.0.1` / `::1` and the system hostname (full + short).
    ///
    /// Interface IPs are matched separately by [`is_local_address`] (binding),
    /// which is more reliable than precomputing the outbound IP on multi-homed
    /// hosts.
    ///
    /// [`is_local_address`]: Self::is_local_address
    fn local_hostnames() -> Vec<String> {
        let mut names = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
        ];

        // Try to get the system hostname
        if let Ok(h) = hostname::get() {
            if let Ok(s) = h.into_string() {
                names.push(s.clone());
                // Also push the short form (before the first '.')
                if let Some(short) = s.split('.').next() {
                    names.push(short.to_string());
                }
            }
        }

        names
    }

    /// Whether `host` is an address of a **local** network interface.
    ///
    /// Implemented by attempting to bind a UDP socket to `(host, 0)`: the OS
    /// only allows binding to an address that belongs to a local interface, so
    /// a successful bind means the host is local. Port `0` is ephemeral and no
    /// packets are sent — this is purely a route/interface check. Works for
    /// both IPv4/IPv6 literals and resolvable local names, with no extra
    /// dependency or `unsafe` interface enumeration.
    fn is_local_address(host: &str) -> bool {
        use std::net::UdpSocket;
        UdpSocket::bind((host, 0u16)).is_ok()
    }

    /// Select the best worker for the given block ID using consistent hashing.
    ///
    /// # Stale-while-revalidate
    ///
    /// If the worker list is older than `worker_refresh_ttl`, a background
    /// refresh task is spawned *without* blocking the current read.  The
    /// stale list continues to serve traffic until the refresh completes.
    ///
    /// **Note**: The background refresh requires access to a
    /// `WorkerManagerClient`. Since `WorkerRouter` does not hold a reference
    /// to one, callers that want automatic refresh should call
    /// `needs_refresh` + `refresh_workers` themselves (e.g. in
    /// `GoosefsFileInStream::open`).
    ///
    /// # Local-first routing
    ///
    /// If a local worker is detected, it is returned for *all* block IDs
    /// (regardless of consistent-hash position), matching Java's
    /// `LocalFirstPolicy`.  The local worker is only bypassed if it is in the
    /// failed set.
    pub async fn select_worker(&self, block_id: i64) -> Result<WorkerInfo> {
        // C1 §3.1):
        // snapshot every shared `ArcSwap` field **exactly once** per call.
        //
        // Before this refactor `select_worker` performed 3–4 independent
        // `ArcSwap::load`/`load_full` invocations plus a rogue `store` on
        // the local-worker probe path. Under 32+ concurrent readers the
        // `arc_swap` hybrid strategy spent up to ~25% CPU in
        // `wait_for_readers → Debt::pay_all` (see `oncpu_5.svg`). Loading
        // each field once and driving the rest of the control flow off
        // local variables coalesces all those debt slots into a single
        // strategy interaction per call.
        let workers = self.workers.load_full();
        let ring = self.hash_ring.load_full();
        let local = self.local_worker_id.load_full();

        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers registered".to_string(),
            });
        }

        // Clean up expired failures. Cheap on healthy clusters thanks to
        // the empty-map fast path in `cleanup_expired_failures` (C3).
        self.cleanup_expired_failures();

        // Detect local worker on first call after list update.
        //
        // After a probe, the result is cached as `Some(Option<i64>)` so that
        // a "no local worker" result no longer forces a re-probe (with the
        // expensive `hostname::get()` call) on every subsequent
        // `select_worker()`. The `store()` — which is what actually costs
        // CPU on the ArcSwap writer side — now happens **at most once per
        // router lifetime**, and never at all on scoped snapshots that
        // inherit their parent's cached value (see `snapshot_from`, C2).
        let local_id_opt: Option<i64> = match *local {
            Some(cached) => cached,
            None => {
                let detected = Self::detect_local_worker(&workers).await;
                let cached_value = if detected > 0 { Some(detected) } else { None };
                self.local_worker_id.store(Arc::new(Some(cached_value)));
                cached_value
            }
        };

        // Prefer local worker if available and not failed.
        if let Some(local_id) = local_id_opt {
            if let Some(local_w) = workers.iter().find(|w| w.id == Some(local_id)) {
                if let Some(addr) = &local_w.address {
                    if !self.is_failed(&worker_addr_key(addr)) {
                        return Ok(local_w.clone());
                    }
                }
            }
        }

        // Use the pre-built consistent-hash ring (rebuilt only on
        // `update_workers`). The hot path is now O(log N) `binary_search`
        // plus a small forward-walk to skip failed workers, instead of the
        // previous O(N log N) rebuild-and-sort per request.
        if let Some(w) = self.consistent_hash_select_with_ring(block_id, &workers, &ring, true) {
            return Ok(w);
        }

        // All eligible workers exhausted (every virtual node points to a
        // failed worker). Fall back to ignoring the failure state — at
        // worst we'll re-fail the same address and surface the error.
        self.consistent_hash_select_with_ring(block_id, &workers, &ring, false)
            .ok_or_else(|| Error::NoWorkerAvailable {
                message: format!("no suitable worker for block_id={}", block_id),
            })
    }

    /// Mark a worker as failed (e.g., after a connection error).
    ///
    /// P0-E: increments `failed_count` iff `insert` returns `None` (a new
    /// key). Re-inserting an existing key only refreshes its timestamp and
    /// must not touch the counter, otherwise repeated failures of the same
    /// worker would drift the counter upward and defeat the fast-path gate
    /// in `cleanup_expired_failures`.
    ///
    /// Concurrent inserts of the same *new* key are serialised by the
    /// per-shard write lock inside `DashMap`, so exactly one of them
    /// observes `None` — the counter cannot double-count.
    pub fn mark_failed(&self, addr: &WorkerNetAddress) {
        let key = worker_addr_key(addr);
        // P0-F.1: lazily initialise the DashMap on first failure.
        // On the happy path this closure never runs.
        let map = self.failed_workers.get_or_init(new_failed_workers_map);
        if map.insert(key, Instant::now()).is_none() {
            self.failed_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `source_is_local` pre-filter for short-circuit reads
    /// (SHORT_CIRCUIT_DESIGN §3.7).
    ///
    /// Returns `true` iff the worker that would serve `block_id` is the
    /// detected local worker. This composes the existing local-first routing:
    /// [`select_worker`](Self::select_worker) already returns the local worker
    /// (when present & healthy) for *every* block, so a match here means the
    /// block would be served locally.
    ///
    /// **Note (design §3.7):** "worker local" ≠ "block physically local". This
    /// is only a pre-filter to avoid issuing a pointless `OpenLocalBlock` RPC
    /// to a remote worker; the final authority on whether the block can be
    /// mmap'd locally is the `OpenLocalBlock` RPC itself.
    pub async fn is_block_source_local(&self, block_id: i64) -> bool {
        let Ok(selected) = self.select_worker(block_id).await else {
            return false;
        };
        // `select_worker` has now probed & cached the local worker id.
        match **self.local_worker_id.load() {
            Some(Some(local_id)) => selected.id == Some(local_id),
            _ => false,
        }
    }

    /// Pick any eligible worker (random selection, not tied to a block ID).
    ///
    /// Matches Java `UnderFileSystemFileOutStream`:
    /// ```java
    /// Collections.shuffle(workerNetAddresses);
    /// WorkerNetAddress address = workerNetAddresses.get(0);
    /// ```
    ///
    /// Used for opening the single long UFS stream in `CACHE_THROUGH` / `THROUGH`
    /// mode, where the UFS stream must be independent of the per-block cache
    /// routing.
    ///
    /// Excludes recently-failed workers; falls back to all workers if every
    /// worker is marked failed.
    pub async fn pick_any_worker(&self) -> Result<WorkerInfo> {
        let workers = self.workers.load_full();

        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers registered".to_string(),
            });
        }

        self.cleanup_expired_failures();

        let eligible: Vec<WorkerInfo> = workers
            .iter()
            .filter(|w| {
                if let Some(addr) = w.address.as_ref() {
                    let key = worker_addr_key(addr);
                    !self.is_failed(&key)
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        let pool = if eligible.is_empty() {
            (*workers).clone()
        } else {
            eligible
        };

        if pool.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no eligible workers".to_string(),
            });
        }

        // Random pick — `rand::rng()` is the project-standard RNG (already
        // a dependency for retry jitter). Using `subsec_nanos() % len` is
        // biased on the low bits and tends to repeat under tight loops,
        // which defeated the load-spreading purpose of this function.
        let idx = rand::Rng::random_range(&mut rand::rng(), 0..pool.len());
        Ok(pool[idx].clone())
    }

    /// Check if a worker address is currently in the failed set.
    fn is_failed(&self, key: &str) -> bool {
        // P0-F.1: if the map was never initialised, there are no failures.
        let Some(map) = self.failed_workers.get() else {
            return false;
        };
        if let Some(entry) = map.get(key) {
            entry.value().elapsed() < self.failure_ttl
        } else {
            false
        }
    }

    /// Remove expired failure entries.
    fn cleanup_expired_failures(&self) {
        // P0-E §3.5):
        // wait-free fast-path gate via an external counter.
        //
        // The previous C3 fix used `DashMap::is_empty()` as the fast path,
        // which still walks every shard and takes a `try_read` on each —
        // ~0.98 % CPU self time on `oncpu_7.svg`. A single `Relaxed`
        // atomic load is O(1) and never touches the map at all.
        //
        // A stale over-count only forces one spurious `retain` walk (same
        // worst case the old `is_empty()` fast path tolerated), so the
        // Relaxed ordering is sufficient — we do NOT need to observe the
        // exact `failed_workers` state, only a hint that it is worth
        // scanning.
        if self.failed_count.load(Ordering::Relaxed) == 0 {
            return;
        }

        // P0-F.1: the counter is non-zero, so the map must have been
        // initialised by a prior `mark_failed`. Defensive `get()` in
        // case of a transient over-count.
        let Some(map) = self.failed_workers.get() else {
            return;
        };

        // Slow path: map is (approximately) non-empty. Retain live entries
        // and update the counter by exactly the number of removals so it
        // stays in sync across multiple cleanup rounds.
        let ttl = self.failure_ttl;
        let mut removed: usize = 0;
        map.retain(|_, v| {
            if v.elapsed() < ttl {
                true
            } else {
                removed += 1;
                false
            }
        });
        if removed > 0 {
            self.failed_count.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    /// Test-only: returns `true` if the `failed_workers` `DashMap` has not
    /// been allocated yet (P0-F.1 lazy-init check). On the happy path this
    /// must remain `true` — the map is only constructed by `mark_failed`.
    #[cfg(test)]
    fn failed_workers_is_uninitialised(&self) -> bool {
        self.failed_workers.get().is_none()
    }

    /// Test-only: returns the number of entries in the `failed_workers` map,
    /// or `0` if the map was never initialised (P0-F.1).
    #[cfg(test)]
    fn failed_workers_len(&self) -> usize {
        self.failed_workers.get().map_or(0, |m| m.len())
    }

    /// Consistent hash selection over a pre-built ring.
    ///
    /// `ring` is `(hash, worker_index)` sorted by `hash`, where `worker_index`
    /// indexes into `workers`. We binary-search for the first virtual node
    /// at or above `hash(block_id)`, then walk forward until we find an
    /// eligible (or — when `skip_failed` is `false` — any) worker.
    /// Ring-based worker selection with optional failed-worker filtering.
    ///
    /// Called by `select_worker` (both `skip_failed=true` primary attempt
    /// and `skip_failed=false` fallback). Delegates the actual walk to the
    /// free-standing [`consistent_hash_select_from_ring`] so that
    /// [`WorkerRouterView`] can reuse the exact same algorithm without
    /// duplicating the code or being forced to share `self`. The only
    /// per-router state involved is the failure predicate, which we pass
    /// in as a closure.
    fn consistent_hash_select_with_ring(
        &self,
        block_id: i64,
        workers: &[WorkerInfo],
        ring: &[(u64, usize)],
        skip_failed: bool,
    ) -> Option<WorkerInfo> {
        consistent_hash_select_from_ring(block_id, workers, ring, skip_failed, |key| {
            self.is_failed(key)
        })
    }
}

/// Free-standing consistent-hash ring walk shared by [`WorkerRouter`] and
/// [`WorkerRouterView`].
///
/// Extracted from `WorkerRouter::consistent_hash_select_with_ring` in P0-D
/// Step 1 §3.4):
/// the algorithm is identical for the shared router and every per-reader
/// snapshot / view, and the only piece of per-router state involved is
/// the failed-worker predicate. Passing that in as a closure lets both
/// types delegate here without duplicating the ~30 lines of ring
/// arithmetic — critical to keep A/B behaviour bit-exact during the
/// coexistence phase (Step 2).
fn consistent_hash_select_from_ring<F>(
    block_id: i64,
    workers: &[WorkerInfo],
    ring: &[(u64, usize)],
    skip_failed: bool,
    is_failed_fn: F,
) -> Option<WorkerInfo>
where
    F: Fn(&str) -> bool,
{
    if ring.is_empty() || workers.is_empty() {
        return None;
    }

    // A2: hash the raw i64 bytes instead of formatting the id to a
    // decimal string. Same hasher (xxh3), same ring — the byte encoding
    // matches `build_hash_ring`'s virtual-node encoding domain (both
    // consumed by the same client process), so this is self-consistent.
    let target = hash_block_id(block_id);
    let start = ring
        .binary_search_by_key(&target, |(h, _)| *h)
        .unwrap_or_else(|p| p)
        % ring.len();

    // Walk forward at most `ring.len()` positions so we eventually
    // probe every distinct virtual node before giving up.
    for offset in 0..ring.len() {
        let pos = (start + offset) % ring.len();
        let worker_idx = ring[pos].1;
        // Defensive: ring may reference an index outside `workers` if
        // somehow stale (should not happen — ring is rebuilt with the
        // same vector — but guard anyway).
        let Some(w) = workers.get(worker_idx) else {
            continue;
        };
        if !skip_failed {
            return Some(w.clone());
        }
        if let Some(addr) = w.address.as_ref() {
            if !is_failed_fn(&worker_addr_key(addr)) {
                return Some(w.clone());
            }
        }
    }
    None
}

/// Wait-free, immutable snapshot of the routing state for a single
/// reader/writer's lifetime.
///
/// **Motivation**/// §3.4): every per-range `WorkerRouter::snapshot_from` today allocates
/// **three fresh `ArcSwap` fields**, each of which triggers
/// `Box<[T]>::from_iter → posix_memalign` on construction (~2.81 % CPU on
/// `oncpu_7.svg`) and `arc_swap::debt::list::LocalNode::with` on Drop
/// (~19 % CPU). Neither the construction nor the destruction cost is
/// justified for a per-reader snapshot: the `workers` / `hash_ring`
/// fields are never mutated after the snapshot is minted, and the
/// `local_worker_id` is now guaranteed to be already-probed by the
/// shared router (P0-D Step 0 — probe follows `update_workers`).
///
/// `WorkerRouterView` captures the routing state as plain `Arc` pointers
/// and a value-typed `Option<i64>`, so `from_shared` is two `Arc::clone`s
/// plus a value copy — no `ArcSwap`, no `Box<[T]>::from_iter`, no
/// `LocalNode::with`. Drop is symmetric.
///
/// # Coexistence with `WorkerRouter::snapshot_from`
///
/// Step 1 (this commit) introduces the type but **does not remove**
/// `snapshot_from`. Both APIs exist side-by-side so Step 2 can migrate
/// call sites (`file_reader.rs`, `file_in_stream.rs`, `file_writer.rs`)
/// one at a time, running the full 32-way concurrent Lance workload
/// after each file to validate A/B behavioural parity before the
/// snapshot API is retired in Step 3.
///
/// # Semantic equivalence to `snapshot_from` (see §3.4.3.2)
///
/// | Field           | Shape                         | Equivalent to snapshot? |
/// |-----------------|-------------------------------|-------------------------|
/// | `workers`       | `Arc<Vec<WorkerInfo>>`        | ✅ same `Arc` pointer   |
/// | `hash_ring`     | `Arc<Vec<(u64, usize)>>`      | ✅ same `Arc` pointer   |
/// | `failed_workers`| `DashMap<String, Instant>`    | ✅ fresh, per-view      |
/// | `failed_count`  | `AtomicUsize` (P0-E)          | ✅ fresh, per-view      |
/// | `failure_ttl`   | `Duration`                    | ✅ value copy           |
/// | `local_worker_id` | `Option<i64>` (value)       | ✅ *iff* the shared     |
/// |                 |                               |    router is probed —   |
/// |                 |                               |    guaranteed by Step 0 |
///
/// # Deliberately **not** exposed
///
/// `WorkerRouterView` intentionally does **not** provide:
/// - `update_workers` — mutation belongs on the shared router only
/// - `refresh_workers` / `needs_refresh` — TTL-driven refresh path
/// - `is_block_source_local` — short-circuit query (already only called
///   on the shared router at
///   [`src/block/short_circuit/factory.rs`](../short_circuit/factory.rs))
///
/// The type system therefore statically prevents a future contributor
/// from calling shared-router-only APIs on a per-reader view (§3.4.3.3),
/// which the current `snapshot_from` allows at compile time but silently
/// mishandles.
pub struct WorkerRouterView {
    /// Immutable snapshot of the worker list captured at construction.
    workers: Arc<Vec<WorkerInfo>>,
    /// Immutable snapshot of the consistent-hash ring captured at
    /// construction. Points to the same `Arc` as the shared router when
    /// built via [`WorkerRouterView::from_shared`], so a shared-router
    /// `update_workers` afterwards cannot tear existing views.
    hash_ring: Arc<Vec<(u64, usize)>>,
    /// Local-worker id captured at construction. `None` means either
    /// "no local worker" or "shared router was not probed yet"; the
    /// latter case is prevented in practice by P0-D Step 0 (the shared
    /// router always probes synchronously inside `update_workers`).
    local_worker_id: Option<i64>,
    /// Per-view failed-worker set. Failure state is intentionally
    /// isolated from the shared router and from every other view — a
    /// transient block failure observed by one reader does not leak into
    /// other readers or into the shared router (matches
    /// `WorkerRouter::snapshot_from` behaviour, see
    /// `test_snapshot_mark_failed_is_isolated_from_parent`).
    ///
    /// **P0-F.1**: wrapped in `OnceLock` — same lazy-init strategy as
    /// `WorkerRouter::failed_workers`. See that field's doc comment
    /// for the full rationale.
    failed_workers: OnceLock<DashMap<String, Instant>>,
    /// P0-E fast-path counter for [`Self::cleanup_expired_failures`].
    /// Invariants are identical to `WorkerRouter::failed_count`; see the
    /// doc comment there.
    failed_count: AtomicUsize,
    /// Copied from the shared router at construction (config, read-only).
    failure_ttl: Duration,
}

impl WorkerRouterView {
    /// Build a view from the shared [`WorkerRouter`].
    ///
    /// Cost is two `Arc::clone`s + a value copy — no `ArcSwap`, no
    /// heap allocation, no `arc_swap::debt` traffic. This is the
    /// **primary** constructor for the context path (used by
    /// `GoosefsFileReader::init_with_context`,
    /// `GoosefsFileInStream::open_with_context`,
    /// `GoosefsFileWriter::create_with_context` after Step 2).
    ///
    /// # Precondition (§3.4.3.1 Caveat 2)
    ///
    /// `shared.local_worker_id` should be in the "probed" state
    /// (`Some(_)`) when this is called; P0-D Step 0 guarantees this by
    /// running `detect_local_worker` synchronously inside
    /// `WorkerRouter::update_workers`. If the shared router has never
    /// had `update_workers` called (fresh `WorkerRouter::new()` with an
    /// empty worker list), the view captures `local_worker_id = None`
    /// and skips local-first routing — same as an empty snapshot today.
    pub fn from_shared(shared: &WorkerRouter) -> Self {
        // Capture `local_worker_id` as a value. `Some(None)` (probed,
        // no local worker) and `None` (unprobed) both collapse to
        // `Option<i64> = None` here — this is safe because Step 0
        // guarantees the shared router is always in the probed state
        // after any `update_workers` call, so the only way to observe
        // `None` here is on a router that was never populated.
        //
        // `Option::flatten` expresses exactly this collapse:
        //   Some(Some(id)) → Some(id)
        //   Some(None)     → None
        //   None           → None
        // …which is idiomatic and clippy-clean, whereas the equivalent
        // `match` triggered `clippy::manual_unwrap_or_default`.
        let local_worker_id: Option<i64> = (*shared.local_worker_id.load_full()).flatten();
        Self {
            workers: shared.workers.load_full(),
            hash_ring: shared.hash_ring.load_full(),
            local_worker_id,
            failed_workers: OnceLock::new(),
            failed_count: AtomicUsize::new(0),
            failure_ttl: shared.failure_ttl,
        }
    }

    /// Build a view directly from a raw worker list (legacy `open()`
    /// path in `file_in_stream.rs`).
    ///
    /// This is the escape hatch for call sites that today do
    ///
    /// ```ignore
    /// let router = WorkerRouter::new();
    /// router.update_workers(workers).await;
    /// ```
    ///
    /// without ever going through a shared [`WorkerRouter`]. The view
    /// takes ownership of `workers`, builds the ring in-line, and
    /// captures no `local_worker_id` (see the second caveat below —
    /// this constructor makes it explicit that local-first is not
    /// available on this path).
    ///
    /// # Semantics vs `from_shared`
    ///
    /// - The ring is built here rather than borrowed from a shared
    ///   router. This is O(N · virtual_nodes) once per construction —
    ///   negligible for the legacy path, which is only used by tests
    ///   and the direct (non-context) `GoosefsFileInStream::open`.
    /// - `local_worker_id` is always `None`: the legacy path does not
    ///   have access to a probed shared router and running
    ///   `detect_local_worker` synchronously here would drag the
    ///   `hostname::get()` syscall onto the caller. Local-first is a
    ///   context-path optimisation only; the legacy path was never a
    ///   hot code path anyway.
    pub fn from_workers(workers: Vec<WorkerInfo>, failure_ttl: Duration) -> Self {
        let ring = Arc::new(build_hash_ring(&workers));
        Self {
            workers: Arc::new(workers),
            hash_ring: ring,
            local_worker_id: None,
            failed_workers: OnceLock::new(),
            failed_count: AtomicUsize::new(0),
            failure_ttl,
        }
    }

    /// Placeholder view with no workers.
    ///
    /// Used by call sites that construct a router-typed field eagerly
    /// but populate it lazily (e.g. `GoosefsFileWriter::create_with_context`
    /// stores a placeholder here so zero-byte writes never touch the
    /// hash ring; the first `write()` swaps in a `from_shared` view).
    ///
    /// Also used by unit tests that need a router-typed field but
    /// never issue routing calls.
    ///
    /// Semantic equivalent of `WorkerRouter::new()` in the pre-Step-2
    /// call sites: empty `workers` / `hash_ring`, no local worker,
    /// default failure TTL. `select_worker` and `pick_any_worker` on
    /// an empty view return `Err(NoWorkerAvailable)` — identical to
    /// the old `WorkerRouter::new()` behaviour.
    pub fn empty() -> Self {
        Self {
            workers: Arc::new(Vec::new()),
            hash_ring: Arc::new(Vec::new()),
            local_worker_id: None,
            failed_workers: OnceLock::new(),
            failed_count: AtomicUsize::new(0),
            failure_ttl: DEFAULT_FAILURE_TTL,
        }
    }

    /// Default failure TTL used by [`Self::empty`] and by the legacy
    /// `from_workers` call sites that want to match the `WorkerRouter`
    /// defaults without depending on the `router` module's private
    /// `DEFAULT_FAILURE_TTL` constant.
    ///
    /// Exposed as an associated `fn` (not a public `const`) so a future
    /// change to the default value flows automatically to all call
    /// sites without them re-importing anything.
    pub fn default_failure_ttl() -> Duration {
        DEFAULT_FAILURE_TTL
    }

    /// Select a worker for the given block id.
    ///
    /// Mirrors [`WorkerRouter::select_worker`] step-for-step so that
    /// behavioural parity is bit-exact for every `(workers, hash_ring,
    /// local_worker_id, failed_workers)` tuple. Any divergence here is
    /// a correctness regression — covered by
    /// `test_view_select_worker_matches_shared_for_all_block_ids`.
    pub async fn select_worker(&self, block_id: i64) -> Result<WorkerInfo> {
        if self.workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers registered".to_string(),
            });
        }

        // Fast-path cleanup gate (P0-E). Identical semantics to the
        // shared router.
        self.cleanup_expired_failures();

        // Local-first routing: unlike `WorkerRouter::select_worker`,
        // the view has no `ArcSwap` writer to lazily probe into. The
        // value was captured at construction and either
        //   (a) inherited from a probed shared router (§3.4.3.1
        //       Caveat 2 fix, guaranteed by P0-D Step 0), or
        //   (b) `None` on the legacy `from_workers` path, which
        //       intentionally skips local-first (see `from_workers`
        //       doc comment).
        if let Some(local_id) = self.local_worker_id {
            if let Some(local_w) = self.workers.iter().find(|w| w.id == Some(local_id)) {
                if let Some(addr) = &local_w.address {
                    if !self.is_failed(&worker_addr_key(addr)) {
                        return Ok(local_w.clone());
                    }
                }
            }
        }

        // Primary attempt: skip failed workers.
        if let Some(w) =
            consistent_hash_select_from_ring(block_id, &self.workers, &self.hash_ring, true, |k| {
                self.is_failed(k)
            })
        {
            return Ok(w);
        }

        // Fallback: ignore the failure state (same last-resort escape
        // as the shared router — at worst we'll re-fail the same
        // address and surface the error to the caller).
        consistent_hash_select_from_ring(block_id, &self.workers, &self.hash_ring, false, |k| {
            self.is_failed(k)
        })
        .ok_or_else(|| Error::NoWorkerAvailable {
            message: format!("no suitable worker for block_id={}", block_id),
        })
    }

    /// Pick any eligible worker at random. Mirrors
    /// [`WorkerRouter::pick_any_worker`] step-for-step (same random
    /// source, same eligible/pool fallback logic).
    pub async fn pick_any_worker(&self) -> Result<WorkerInfo> {
        if self.workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers registered".to_string(),
            });
        }

        self.cleanup_expired_failures();

        let eligible: Vec<WorkerInfo> = self
            .workers
            .iter()
            .filter(|w| {
                if let Some(addr) = w.address.as_ref() {
                    let key = worker_addr_key(addr);
                    !self.is_failed(&key)
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        let pool = if eligible.is_empty() {
            (*self.workers).clone()
        } else {
            eligible
        };

        if pool.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no eligible workers".to_string(),
            });
        }

        let idx = rand::Rng::random_range(&mut rand::rng(), 0..pool.len());
        Ok(pool[idx].clone())
    }

    /// Mark a worker as failed on this view only.
    ///
    /// Failure isolation semantics are identical to
    /// [`WorkerRouter::mark_failed`] on a `snapshot_from`: writes go
    /// only to this view's `failed_workers` map, so a transient block
    /// failure observed by one reader never leaks into other readers
    /// or into the shared router.
    pub fn mark_failed(&self, addr: &WorkerNetAddress) {
        let key = worker_addr_key(addr);
        // P0-F.1: lazily initialise the DashMap on first failure.
        let map = self.failed_workers.get_or_init(new_failed_workers_map);
        if map.insert(key, Instant::now()).is_none() {
            self.failed_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Check whether a worker key is currently marked failed.
    fn is_failed(&self, key: &str) -> bool {
        // P0-F.1: if the map was never initialised, there are no failures.
        let Some(map) = self.failed_workers.get() else {
            return false;
        };
        if let Some(entry) = map.get(key) {
            entry.value().elapsed() < self.failure_ttl
        } else {
            false
        }
    }

    /// Fast-path cleanup gate identical to
    /// [`WorkerRouter::cleanup_expired_failures`] (P0-E).
    fn cleanup_expired_failures(&self) {
        if self.failed_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        // P0-F.1: defensive — counter non-zero implies map is initialised.
        let Some(map) = self.failed_workers.get() else {
            return;
        };
        let ttl = self.failure_ttl;
        let mut removed: usize = 0;
        map.retain(|_, v| {
            if v.elapsed() < ttl {
                true
            } else {
                removed += 1;
                false
            }
        });
        if removed > 0 {
            self.failed_count.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    /// Test-only accessor for `local_worker_id` (used by parity tests
    /// in `#[cfg(test)]`; not part of the public API).
    #[cfg(test)]
    fn local_worker_id(&self) -> Option<i64> {
        self.local_worker_id
    }

    /// Test-only accessor for the captured `workers` `Arc` (parity tests).
    #[cfg(test)]
    fn workers_arc(&self) -> &Arc<Vec<WorkerInfo>> {
        &self.workers
    }

    /// Test-only: returns `true` if the `failed_workers` `DashMap` has not
    /// been allocated yet (P0-F.1 lazy-init check). On the happy path this
    /// must remain `true` — the map is only constructed by `mark_failed`.
    #[cfg(test)]
    fn failed_workers_is_uninitialised(&self) -> bool {
        self.failed_workers.get().is_none()
    }

    /// Test-only: returns the number of entries in the `failed_workers` map,
    /// or `0` if the map was never initialised (P0-F.1).
    #[cfg(test)]
    fn failed_workers_len(&self) -> usize {
        self.failed_workers.get().map_or(0, |m| m.len())
    }

    /// Test-only accessor for the captured `hash_ring` `Arc` (parity tests).
    #[cfg(test)]
    fn hash_ring_arc(&self) -> &Arc<Vec<(u64, usize)>> {
        &self.hash_ring
    }
}

/// Build a sorted `(hash, worker_index)` ring for consistent hashing.
///
/// Called from [`WorkerRouter::update_workers`] so the hot `select_worker`
/// path can do an O(log N) binary search instead of rebuilding+sorting on
/// every request.
///
/// **A2**: virtual-node hashes are computed by feeding the raw `i64` /
/// `u32` bytes to xxh3 directly (no `format!` / no allocation). The ring
/// is client-local (never exchanged across processes), so intra-process
/// self-consistency with [`hash_block_id`] is the only requirement.
fn build_hash_ring(workers: &[WorkerInfo]) -> Vec<(u64, usize)> {
    let mut ring: Vec<(u64, usize)> =
        Vec::with_capacity(workers.len() * VIRTUAL_NODES_PER_WORKER as usize);
    for (idx, worker) in workers.iter().enumerate() {
        let worker_id = worker.id.unwrap_or(idx as i64);
        let virtual_nodes = worker
            .virtual_node_num
            .unwrap_or(VIRTUAL_NODES_PER_WORKER as i32) as u32;
        for vn in 0..virtual_nodes {
            let hash = hash_virtual_node(worker_id, vn);
            ring.push((hash, idx));
        }
    }
    ring.sort_by_key(|(h, _)| *h);
    ring
}

/// Compute a fingerprint of a worker set for `update_workers` fast-path.
///
/// Order-independent: sorts the `(id, host, rpc_port, virtual_node_num)`
/// tuples before hashing so a re-ordered but otherwise identical response
/// from the master does not force an unnecessary ring rebuild.
fn workers_fingerprint(workers: &[WorkerInfo]) -> u64 {
    if workers.is_empty() {
        return 0;
    }
    let mut tuples: Vec<(i64, &str, i32, i32)> = workers
        .iter()
        .map(|w| {
            let addr = w.address.as_ref();
            let host = addr.and_then(|a| a.host.as_deref()).unwrap_or("");
            let port = addr.and_then(|a| a.rpc_port).unwrap_or(0);
            let vn = w
                .virtual_node_num
                .unwrap_or(VIRTUAL_NODES_PER_WORKER as i32);
            (w.id.unwrap_or(0), host, port, vn)
        })
        .collect();
    tuples.sort_unstable();

    let mut h = Xxh3Default::default();
    for (id, host, port, vn) in tuples {
        h.write(&id.to_le_bytes());
        h.write(&(port as i32).to_le_bytes());
        h.write(&(vn as i32).to_le_bytes());
        h.write(&(host.len() as u32).to_le_bytes());
        h.write(host.as_bytes());
        // Explicit tuple separator so `(1, "a", 2, 3) + (4, ...)` cannot
        // collide with `(1, "a"+encoded(2, 3, 4), ...)` after concatenation.
        h.write(&[0u8]);
    }
    h.finish()
}

impl Default for WorkerRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Produce a unique key for a `WorkerNetAddress`.
///
/// **A2 / P0-F.2**: hand-rolled string join (no `format!` / no fmt machinery /
/// no intermediate `String` for the port) — this is called on every
/// `mark_failed` / `is_failed` and on every gRPC worker acquisition
/// (`file_reader.rs`, `file_in_stream.rs`, `file_writer.rs`, the SC factory's
/// `acquire_worker`) and shows up on flame graphs.
///
/// `pub(crate)` so the IO modules share the same implementation rather than
/// each re-rolling their own `format!("{}:{}", host, port)`.
pub(crate) fn worker_addr_key(addr: &WorkerNetAddress) -> String {
    let host = addr.host.as_deref().unwrap_or("unknown");
    let port = addr.rpc_port.unwrap_or(0);
    // Reserve exactly `host + ':' + up-to-11-digit i32` up-front to avoid
    // the growth-and-copy chain we saw as `RawVec…::finish_grow` in the
    // flame graph. Then `itoa::Buffer` formats the port straight into a
    // stack `&str` (P0-F.2): the integer-to-string conversion skips the
    // `core::fmt::Formatter` machinery entirely, the only remaining
    // copy is the final `push_str` into the pre-sized `String`.
    let mut s = String::with_capacity(host.len() + 12);
    s.push_str(host);
    s.push(':');
    let mut buf = itoa::Buffer::new();
    s.push_str(buf.format(port));
    s
}

/// Build the gRPC endpoint (`host:port`) used by clients to actually dial a
/// worker. P0-F.2 / table row #2 in the post-`oncpu_8` plan: every
/// `file_reader.rs` / `file_in_stream.rs` / `file_writer.rs` /
/// `short_circuit::factory::acquire_worker` call site used to roll its own
/// `format!("{}:{}", host, port)`; share this one instead so the
/// pre-sized `String` + `itoa` win applies everywhere.
///
/// Defaults differ from [`worker_addr_key`] (GooseFS RPC defaults
/// `127.0.0.1` / `9203` vs. the "unknown" / `0` hash-key defaults used
/// purely for identity comparison).
pub(crate) fn rpc_endpoint(addr: &WorkerNetAddress) -> String {
    let host = addr.host.as_deref().unwrap_or("127.0.0.1");
    let port = addr.rpc_port.unwrap_or(9203);
    let mut s = String::with_capacity(host.len() + 12);
    s.push_str(host);
    s.push(':');
    let mut buf = itoa::Buffer::new();
    s.push_str(buf.format(port));
    s
}

/// Hash a virtual-node identifier `(worker_id, vn)` into the ring.
///
/// **A2**: feeds the raw `i64` + separator + `u32` bytes to xxh3 with no
/// allocation. Domain-separated from [`hash_block_id`] by construction
/// (different byte layout: 8-byte id + 1-byte separator + 4-byte vn vs.
/// bare 8-byte block id) so the two never collide within one ring.
#[inline]
fn hash_virtual_node(worker_id: i64, vn: u32) -> u64 {
    let mut h = Xxh3Default::default();
    h.write(&worker_id.to_le_bytes());
    h.write(b":");
    h.write(&vn.to_le_bytes());
    h.finish()
}

/// Hash a `block_id` for consistent-hash ring lookup.
///
/// **A2**: replaces the old `hash_key(&block_id.to_string())` (fmt
/// machinery + `String` allocation on every `select_worker`) with a
/// direct `to_le_bytes()` feed to the xxh3 hasher.
#[inline]
fn hash_block_id(block_id: i64) -> u64 {
    let mut h = Xxh3Default::default();
    h.write(&block_id.to_le_bytes());
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_worker(id: i64, host: &str, port: i32) -> WorkerInfo {
        WorkerInfo {
            id: Some(id),
            address: Some(WorkerNetAddress {
                host: Some(host.to_string()),
                rpc_port: Some(port),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_select_worker_empty() {
        let router = WorkerRouter::new();
        assert!(router.select_worker(123).await.is_err());
    }

    #[tokio::test]
    async fn test_select_worker_deterministic() {
        let router = WorkerRouter::new();
        let workers = vec![
            make_worker(1, "w1", 9203),
            make_worker(2, "w2", 9203),
            make_worker(3, "w3", 9203),
        ];
        router.update_workers(workers).await;

        // Same block_id should map to same worker
        let w1 = router.select_worker(42).await.unwrap();
        let w2 = router.select_worker(42).await.unwrap();
        assert_eq!(w1.id, w2.id);
    }

    #[tokio::test]
    async fn test_failed_worker_filtered() {
        let router = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let workers = vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)];
        router.update_workers(workers.clone()).await;

        // Mark w1 as failed
        router.mark_failed(workers[0].address.as_ref().unwrap());

        // Should select w2
        let selected = router.select_worker(42).await.unwrap();
        assert_eq!(selected.id, Some(2));
    }

    #[tokio::test]
    async fn test_pick_any_worker_empty() {
        let router = WorkerRouter::new();
        assert!(router.pick_any_worker().await.is_err());
    }

    #[tokio::test]
    async fn test_pick_any_worker_returns_eligible() {
        let router = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let workers = vec![
            make_worker(1, "w1", 9203),
            make_worker(2, "w2", 9203),
            make_worker(3, "w3", 9203),
        ];
        router.update_workers(workers.clone()).await;
        // Mark w1 + w2 as failed, only w3 eligible.
        router.mark_failed(workers[0].address.as_ref().unwrap());
        router.mark_failed(workers[1].address.as_ref().unwrap());

        for _ in 0..10 {
            let picked = router.pick_any_worker().await.unwrap();
            assert_eq!(picked.id, Some(3));
        }
    }

    #[tokio::test]
    async fn test_pick_any_worker_fallback_when_all_failed() {
        let router = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let workers = vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)];
        router.update_workers(workers.clone()).await;
        router.mark_failed(workers[0].address.as_ref().unwrap());
        router.mark_failed(workers[1].address.as_ref().unwrap());

        // Should fall back to all workers instead of erroring out.
        let picked = router.pick_any_worker().await.unwrap();
        assert!(picked.id == Some(1) || picked.id == Some(2));
    }

    // ── TTL tests ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_needs_refresh_false_after_new() {
        // A freshly constructed router should NOT need refresh yet.
        let router = WorkerRouter::new();
        assert!(!router.needs_refresh().await);
    }

    #[tokio::test]
    async fn test_needs_refresh_true_with_zero_ttl() {
        // With a 0 s TTL, the list is immediately stale.
        let router = WorkerRouter::with_ttls(DEFAULT_FAILURE_TTL, Duration::ZERO);
        // needs_refresh checks elapsed >= ttl; with ZERO the condition is always true
        // after at least 1 ns has passed — sleep a tiny bit to be safe.
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(router.needs_refresh().await);
    }

    #[tokio::test]
    async fn test_with_ttls_stores_values() {
        let failure = Duration::from_secs(10);
        let refresh = Duration::from_secs(5);
        let router = WorkerRouter::with_ttls(failure, refresh);
        assert_eq!(router.failure_ttl, failure);
        assert_eq!(router.worker_refresh_ttl, refresh);
    }

    #[tokio::test]
    async fn test_update_workers_resets_refresh_clock() {
        // Use an instantaneous TTL so needs_refresh() starts true.
        let router = WorkerRouter::with_ttls(DEFAULT_FAILURE_TTL, Duration::ZERO);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(
            router.needs_refresh().await,
            "should need refresh before update"
        );

        // update_workers resets the clock — now the default 30 s TTL from
        // with_ttls(ZERO) is still 0, so it will still be stale.
        // To test the *reset* we need a non-zero TTL.
        let router2 = WorkerRouter::with_ttls(DEFAULT_FAILURE_TTL, Duration::from_secs(60));
        router2
            .update_workers(vec![make_worker(1, "w1", 9203)])
            .await;
        // Clock reset — should not need refresh yet.
        assert!(!router2.needs_refresh().await);
    }

    // ── Local worker tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_local_worker_preferred() {
        // Register a "localhost" worker and two remote workers.
        let router = WorkerRouter::new();
        let workers = vec![
            make_worker(1, "remote1", 9203),
            make_worker(2, "localhost", 9203), // local
            make_worker(3, "remote2", 9203),
        ];
        router.update_workers(workers).await;

        // Every block ID should be routed to the local worker.
        for block_id in [1i64, 42, 100, 999, 10_000] {
            let selected = router.select_worker(block_id).await.unwrap();
            assert_eq!(
                selected.id,
                Some(2),
                "block_id={} should route to local worker",
                block_id
            );
        }
    }

    #[tokio::test]
    async fn test_local_worker_skipped_when_failed() {
        let router = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let local_worker = make_worker(2, "localhost", 9203);
        let workers = vec![
            make_worker(1, "remote1", 9203),
            local_worker.clone(),
            make_worker(3, "remote2", 9203),
        ];
        router.update_workers(workers).await;

        // Mark the local worker as failed.
        router.mark_failed(local_worker.address.as_ref().unwrap());

        // Routing should fall back to consistent-hash selection of remote workers.
        let selected = router.select_worker(42).await.unwrap();
        assert_ne!(
            selected.id,
            Some(2),
            "failed local worker should not be selected"
        );
    }

    #[tokio::test]
    async fn test_detect_local_worker_none() {
        // No worker has a local hostname → returns 0.
        let workers = vec![
            make_worker(1, "remote-host-a.example.com", 9203),
            make_worker(2, "remote-host-b.example.com", 9203),
        ];
        let id = WorkerRouter::detect_local_worker(&workers).await;
        assert_eq!(id, 0);
    }

    #[tokio::test]
    async fn test_detect_local_worker_loopback() {
        // A worker with "127.0.0.1" must be detected as local.
        let workers = vec![
            make_worker(1, "10.0.0.1", 9203),
            make_worker(2, "127.0.0.1", 9203),
        ];
        let id = WorkerRouter::detect_local_worker(&workers).await;
        assert_eq!(id, 2);
    }

    #[tokio::test]
    async fn test_local_worker_cache_invalidated_on_update() {
        let router = WorkerRouter::new();
        // First update — no local worker.
        //
        // D-Step0: `update_workers` now probes synchronously on the slow
        // path, so after this call the cache is already populated with the
        // probe result (`Some(None)` = probed, no local worker), NOT with
        // the "unprobed" sentinel (`None`) as before.
        router
            .update_workers(vec![make_worker(1, "remote1", 9203)])
            .await;
        assert_eq!(**router.local_worker_id.load(), Some(None));
        // A follow-up `select_worker` must observe the same cached value
        // without triggering another probe (same guarantee as before, just
        // now the probe happened one step earlier).
        let _ = router.select_worker(1).await;
        assert_eq!(**router.local_worker_id.load(), Some(None));

        // Second update — local worker arrives.
        router
            .update_workers(vec![
                make_worker(1, "remote1", 9203),
                make_worker(2, "127.0.0.1", 9203),
            ])
            .await;
        // Cache is re-probed synchronously inside `update_workers` and
        // must directly reflect the newly detected local worker — no lazy
        // re-detection required.
        assert_eq!(**router.local_worker_id.load(), Some(Some(2)));
        let selected = router.select_worker(1).await.unwrap();
        assert_eq!(selected.id, Some(2), "new local worker should be preferred");
    }

    /// D-Step0    /// §3.4.3.1 Caveat 2): after **every** slow-path `update_workers`
    /// the shared router must be in the "probed" state
    /// (`Some(_)`), never the "unprobed" state (`None`). This is the
    /// invariant that lets a future `WorkerRouterView::from_shared`
    /// inherit a resolved `Option<i64>` without needing an `ArcSwap`
    /// writer of its own.
    #[tokio::test]
    async fn test_update_workers_leaves_local_worker_id_probed() {
        let router = WorkerRouter::new();

        // Case A: no local worker in the set → probed as `Some(None)`.
        router
            .update_workers(vec![
                make_worker(1, "remote-a.example.com", 9203),
                make_worker(2, "remote-b.example.com", 9203),
            ])
            .await;
        match **router.local_worker_id.load() {
            Some(_) => {}
            None => panic!("update_workers must leave local_worker_id in the probed state"),
        }
        assert_eq!(
            **router.local_worker_id.load(),
            Some(None),
            "no local worker → cache must be Some(None), not None (unprobed)"
        );

        // Case B: local worker present → probed as `Some(Some(id))`.
        router
            .update_workers(vec![
                make_worker(1, "remote-a.example.com", 9203),
                make_worker(2, "127.0.0.1", 9203),
            ])
            .await;
        assert_eq!(
            **router.local_worker_id.load(),
            Some(Some(2)),
            "local worker present → cache must be Some(Some(id))"
        );

        // Case C: subsequent slow-path update re-probes (does not linger
        // in the "unprobed" state even for a moment observable to a
        // reader that could mint a view between store calls).
        router
            .update_workers(vec![make_worker(3, "remote-c.example.com", 9203)])
            .await;
        assert!(
            (**router.local_worker_id.load()).is_some(),
            "post-update state must always be probed"
        );
    }

    // ── A1 / A2: snapshot + fingerprint fast-path tests ──────────────────

    /// A1: `snapshot_from` produces a router that shares the exact same
    /// hash-ring `Arc` as the parent — verified by pointer identity — and
    /// therefore never rebuilds the ring.
    #[tokio::test]
    async fn test_snapshot_from_shares_hash_ring_arc() {
        let shared = WorkerRouter::new();
        shared
            .update_workers(vec![
                make_worker(1, "w1", 9203),
                make_worker(2, "w2", 9203),
                make_worker(3, "w3", 9203),
            ])
            .await;

        let snap = WorkerRouter::snapshot_from(&shared);

        // Same ring (Arc pointer identity).
        let a = shared.hash_ring.load_full();
        let b = snap.hash_ring.load_full();
        assert!(
            Arc::ptr_eq(&a, &b),
            "snapshot must reuse the shared hash_ring Arc (no rebuild)"
        );
        // Same worker vec (Arc pointer identity).
        let wa = shared.workers.load_full();
        let wb = snap.workers.load_full();
        assert!(
            Arc::ptr_eq(&wa, &wb),
            "snapshot must reuse the shared workers Arc"
        );
    }

    /// C2: `snapshot_from` must inherit the parent's `local_worker_id`
    /// `Arc` instead of resetting it to `None`. This is what avoids the
    /// `wait_for_readers → arc_swap::Debt::pay_all` hot path observed on
    /// `oncpu_3.svg` (~12.7 % CPU self time) — every scoped snapshot was
    /// re-probing and re-storing.
    ///
    /// Verified two ways:
    ///   1. Fresh (unprobed) parent → child's `local_worker_id` Arc must
    ///      point at the same allocation as the parent's (pointer identity).
    ///   2. Probed parent → child inherits the same `Some(_)` value without
    ///      running its own probe (no `store` on the child).
    #[tokio::test]
    async fn test_snapshot_from_shares_local_worker_id() {
        let shared = WorkerRouter::new();
        shared
            .update_workers(vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)])
            .await;

        // Case 1: parent never probed. Both sides must observe the same
        // "unprobed" pointee (i.e. `None`) via a shared Arc.
        let snap_unprobed = WorkerRouter::snapshot_from(&shared);
        let a = shared.local_worker_id.load_full();
        let b = snap_unprobed.local_worker_id.load_full();
        assert!(
            Arc::ptr_eq(&a, &b),
            "snapshot must reuse the shared local_worker_id Arc (unprobed case)"
        );

        // Case 2: force the parent into the "probed, no local worker"
        // state without going through `select_worker` (which would
        // require a real local hostname match). The child snapshot
        // taken *after* the store must observe the probed value
        // through the same Arc.
        shared.local_worker_id.store(Arc::new(Some(None)));
        let snap_probed = WorkerRouter::snapshot_from(&shared);
        let pa = shared.local_worker_id.load_full();
        let pb = snap_probed.local_worker_id.load_full();
        assert!(
            Arc::ptr_eq(&pa, &pb),
            "snapshot must reuse the shared local_worker_id Arc (probed case)"
        );
        assert_eq!(
            *pb,
            Some(None),
            "snapshot must observe the probed value, not re-probe"
        );
    }

    /// C3: on a healthy cluster `failed_workers` is empty for the entire
    /// process lifetime, so `cleanup_expired_failures` must not acquire
    /// any DashMap shard lock — the empty-map check has to short-circuit.
    ///
    /// This is a semantic (not micro-benchmark) test: after the call the
    /// map must still be empty *and* still be reusable by a subsequent
    /// `mark_failed`. The perf win itself is measured by the router
    /// select bench
    #[tokio::test]
    async fn test_cleanup_expired_failures_empty_is_noop() {
        let router = WorkerRouter::new();
        router
            .update_workers(vec![make_worker(1, "w1", 9203)])
            .await;

        // Precondition: map starts empty (P0-F.1: not yet allocated).
        assert!(router.failed_workers_is_uninitialised());

        // Cleanup on empty map must not panic and must leave the map empty.
        router.cleanup_expired_failures();
        assert!(
            router.failed_workers_is_uninitialised(),
            "cleanup on empty map must be a no-op (map not allocated)"
        );

        // Map remains usable: a subsequent `mark_failed` inserts one entry
        // and the next cleanup keeps it (TTL not elapsed).
        let worker = make_worker(1, "w1", 9203);
        router.mark_failed(worker.address.as_ref().unwrap());
        assert_eq!(router.failed_workers_len(), 1);
        router.cleanup_expired_failures();
        assert_eq!(
            router.failed_workers_len(),
            1,
            "cleanup must not evict non-expired entries"
        );
    }

    /// A1: `select_worker` on a snapshot returns the same worker as on the
    /// parent for every block id — same ring, deterministic mapping.
    #[tokio::test]
    async fn test_snapshot_from_select_worker_matches_parent() {
        let shared = WorkerRouter::new();
        shared
            .update_workers(vec![
                make_worker(1, "w1", 9203),
                make_worker(2, "w2", 9203),
                make_worker(3, "w3", 9203),
            ])
            .await;
        let snap = WorkerRouter::snapshot_from(&shared);

        for block_id in [0i64, 1, 42, 999, 1 << 30, i64::MAX] {
            let a = shared.select_worker(block_id).await.unwrap();
            let b = snap.select_worker(block_id).await.unwrap();
            assert_eq!(
                a.id, b.id,
                "snapshot and parent must select the same worker for block_id={}",
                block_id
            );
        }
    }

    /// A1 semantics: `mark_failed` on the snapshot must NOT affect the
    /// parent's failure state. This is the isolation guarantee that lets us
    /// safely swap `WorkerRouter::new + update_workers` for `snapshot_from`.
    #[tokio::test]
    async fn test_snapshot_mark_failed_is_isolated_from_parent() {
        let shared = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let workers = vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)];
        shared.update_workers(workers.clone()).await;

        let snap = WorkerRouter::snapshot_from(&shared);
        // Fail w1 on the snapshot only.
        snap.mark_failed(workers[0].address.as_ref().unwrap());

        // Parent must still see w1 as healthy.
        assert!(!shared.is_failed(&worker_addr_key(workers[0].address.as_ref().unwrap())));
        // Snapshot must see it as failed.
        assert!(snap.is_failed(&worker_addr_key(workers[0].address.as_ref().unwrap())));
    }

    /// A2 defensive add-on: `update_workers` skips the ring rebuild when the
    /// new worker set fingerprint matches the currently-published one.
    /// Verified by pointer identity of the `hash_ring` `Arc`.
    #[tokio::test]
    async fn test_update_workers_fingerprint_skip_rebuild() {
        let router = WorkerRouter::new();
        let workers = vec![
            make_worker(1, "w1", 9203),
            make_worker(2, "w2", 9203),
            make_worker(3, "w3", 9203),
        ];
        router.update_workers(workers.clone()).await;
        let ring_before = router.hash_ring.load_full();

        // Same set → must skip rebuild → same Arc.
        router.update_workers(workers.clone()).await;
        let ring_same = router.hash_ring.load_full();
        assert!(
            Arc::ptr_eq(&ring_before, &ring_same),
            "identical worker set must not rebuild the ring"
        );

        // Same set in different order → still same fingerprint → same Arc.
        let mut reordered = workers.clone();
        reordered.reverse();
        router.update_workers(reordered).await;
        let ring_reordered = router.hash_ring.load_full();
        assert!(
            Arc::ptr_eq(&ring_before, &ring_reordered),
            "reordered-but-identical worker set must not rebuild the ring"
        );

        // Now change the set → must rebuild → different Arc.
        let changed = vec![
            make_worker(1, "w1", 9203),
            make_worker(2, "w2", 9203),
            make_worker(4, "w4", 9203), // <-- new worker id
        ];
        router.update_workers(changed).await;
        let ring_after = router.hash_ring.load_full();
        assert!(
            !Arc::ptr_eq(&ring_before, &ring_after),
            "changed worker set must rebuild the ring"
        );
    }

    /// P0-E: `failed_count` must stay in sync with `failed_workers` across
    /// insert / re-insert / cleanup rounds — otherwise the fast-path gate
    /// in `cleanup_expired_failures` would either skip a needed walk (stale
    /// zero) or pay a permanent spurious walk (permanent over-count).
    #[tokio::test]
    async fn test_cleanup_expired_failures_counter_stays_in_sync() {
        // Very short TTL so we can wait it out in the test.
        let ttl = Duration::from_millis(30);
        let router = WorkerRouter::with_failure_ttl(ttl);

        // Insert 3 distinct failures → counter must go to 3.
        let a = make_worker(1, "w1", 9203);
        let b = make_worker(2, "w2", 9203);
        let c = make_worker(3, "w3", 9203);
        router.mark_failed(a.address.as_ref().unwrap());
        router.mark_failed(b.address.as_ref().unwrap());
        router.mark_failed(c.address.as_ref().unwrap());
        assert_eq!(router.failed_count.load(Ordering::Relaxed), 3);
        assert_eq!(router.failed_workers_len(), 3);

        // Re-insert an existing key (same address) must NOT bump the counter.
        // Otherwise repeated failures of one flaky worker would drift the
        // counter upward and permanently defeat the fast-path gate.
        router.mark_failed(a.address.as_ref().unwrap());
        router.mark_failed(a.address.as_ref().unwrap());
        assert_eq!(
            router.failed_count.load(Ordering::Relaxed),
            3,
            "re-insert must not touch the counter"
        );

        // Fast path: on a healthy cluster (empty map) cleanup must not
        // walk the map — we can only assert the observable outcome (map
        // stays empty, counter stays 0).
        let healthy = WorkerRouter::new();
        healthy.cleanup_expired_failures();
        assert_eq!(healthy.failed_count.load(Ordering::Relaxed), 0);
        assert!(healthy.failed_workers_is_uninitialised());

        // Wait past the TTL so every entry expires.
        tokio::time::sleep(ttl + Duration::from_millis(20)).await;
        router.cleanup_expired_failures();

        // After cleanup: map fully drained AND counter follows down.
        assert_eq!(
            router.failed_workers_len(),
            0,
            "expired entries must be removed"
        );
        assert_eq!(
            router.failed_count.load(Ordering::Relaxed),
            0,
            "counter must be decremented by exactly the number of removals"
        );

        // Idempotent: a second cleanup on an empty map takes the fast path
        // and leaves the counter at 0.
        router.cleanup_expired_failures();
        assert_eq!(router.failed_count.load(Ordering::Relaxed), 0);
    }

    /// P0-E: `snapshot_from` must NOT inherit the parent's `failed_count`.
    /// Snapshot failure state is isolated (fresh `DashMap`), so its counter
    /// starts fresh too — otherwise it would gate `cleanup_expired_failures`
    /// on a phantom set that is not in the snapshot's own map.
    #[tokio::test]
    async fn test_snapshot_failed_count_starts_fresh() {
        let shared = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        shared
            .update_workers(vec![make_worker(1, "w1", 9203)])
            .await;
        // Poison the parent counter with a real failure.
        shared.mark_failed(&WorkerNetAddress {
            host: Some("w1".to_string()),
            rpc_port: Some(9203),
            ..Default::default()
        });
        assert_eq!(shared.failed_count.load(Ordering::Relaxed), 1);

        let snap = WorkerRouter::snapshot_from(&shared);
        assert_eq!(
            snap.failed_count.load(Ordering::Relaxed),
            0,
            "snapshot must not inherit parent's failed_count"
        );
        assert!(snap.failed_workers_is_uninitialised());
    }

    /// A2: hashing the same virtual node / block id twice must produce the
    /// same value (self-consistency of the byte-encoding scheme).
    #[test]
    fn test_hash_functions_are_stable() {
        assert_eq!(hash_virtual_node(42, 7), hash_virtual_node(42, 7));
        assert_eq!(hash_block_id(1234567890), hash_block_id(1234567890));
        // Domain separation: (worker_id, vn) must not collide with a bare
        // block_id under the same numerical values.
        assert_ne!(hash_virtual_node(42, 0), hash_block_id(42));
    }

    /// P0-F.1: `failed_workers` `DashMap` must NOT be allocated on the
    /// happy path — i.e. when `mark_failed` is never called. `select_worker`
    /// on a healthy cluster should leave the `OnceLock` uninitialised.
    #[tokio::test]
    async fn test_view_failed_workers_is_lazy_initialised() {
        let shared = WorkerRouter::new();
        shared
            .update_workers(vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)])
            .await;

        let view = WorkerRouterView::from_shared(&shared);

        // 1000 select_worker calls without any mark_failed → no allocation.
        for i in 0..1000 {
            let _ = view.select_worker(i).await;
        }
        assert!(
            view.failed_workers_is_uninitialised(),
            "OnceLock must stay uninitialised on the happy path (no mark_failed calls)"
        );

        // Same for the shared router.
        assert!(
            shared.failed_workers_is_uninitialised(),
            "shared router must also stay uninitialised on the happy path"
        );
    }

    /// P0-F.1: the first `mark_failed` lazily initialises the `DashMap`,
    /// and subsequent `is_failed` / `cleanup_expired_failures` calls operate
    /// on the initialised map. Counter must stay in sync.
    #[tokio::test]
    async fn test_view_mark_failed_init_dashmap_lazily() {
        let shared = WorkerRouter::new();
        let workers = vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)];
        shared.update_workers(workers.clone()).await;

        let view = WorkerRouterView::from_shared(&shared);

        // Precondition: not yet allocated.
        assert!(view.failed_workers_is_uninitialised());

        // First mark_failed triggers lazy init.
        view.mark_failed(workers[0].address.as_ref().unwrap());
        assert!(!view.failed_workers_is_uninitialised());
        assert_eq!(view.failed_workers_len(), 1);
        assert_eq!(view.failed_count.load(Ordering::Relaxed), 1);

        // is_failed works on the lazily-initialised map.
        let key = worker_addr_key(workers[0].address.as_ref().unwrap());
        assert!(view.is_failed(&key));

        // Second mark_failed does not re-init (already allocated).
        view.mark_failed(workers[1].address.as_ref().unwrap());
        assert_eq!(view.failed_workers_len(), 2);
        assert_eq!(view.failed_count.load(Ordering::Relaxed), 2);
    }

    // ------------------------------------------------------------------
    // P0-D Step 1: `WorkerRouterView` parity tests
    //
    // These live alongside the `WorkerRouter::snapshot_from` tests
    // during the coexistence phase (Steps 1–3). They assert that a
    // View minted from the same shared router as a snapshot produces
    // byte-exact routing decisions across:
    //   - the `workers` / `hash_ring` `Arc` identity,
    //   - the inherited `local_worker_id` in all four parent states,
    //   - the legacy `from_workers` path,
    //   - `select_worker` for a wide spread of block ids,
    //   - failure-isolation semantics.
    // ------------------------------------------------------------------

    /// §3.4.6: `from_shared` must borrow the same `workers` and
    /// `hash_ring` `Arc`s as the shared router — this is what makes
    /// construction ~2 ns (`Arc::clone`) instead of ~250 ns
    /// (`Box<[T]>::from_iter → posix_memalign`).
    #[tokio::test]
    async fn test_view_from_shared_shares_hash_ring_arc() {
        let shared = WorkerRouter::new();
        shared
            .update_workers(vec![
                make_worker(1, "w1", 9203),
                make_worker(2, "w2", 9203),
                make_worker(3, "w3", 9203),
            ])
            .await;
        let workers_arc_before = shared.workers.load_full();
        let ring_arc_before = shared.hash_ring.load_full();

        let view = WorkerRouterView::from_shared(&shared);

        assert!(
            Arc::ptr_eq(&workers_arc_before, view.workers_arc()),
            "view must share the shared router's `workers` Arc (no re-allocation)"
        );
        assert!(
            Arc::ptr_eq(&ring_arc_before, view.hash_ring_arc()),
            "view must share the shared router's `hash_ring` Arc (no re-build)"
        );
    }

    /// §3.4.6: `local_worker_id` inheritance across all four parent
    /// states. Value-equality (not `Arc::ptr_eq`), because the view
    /// stores `Option<i64>` directly, not an `Arc<Option<Option<i64>>>`.
    #[tokio::test]
    async fn test_view_from_shared_inherits_local_worker_id() {
        // Case A: parent = None (unprobed).
        //
        // This state is only reachable on a brand-new `WorkerRouter::new()`
        // that has never had `update_workers` called (P0-D Step 0 fills
        // the cache synchronously). Verify the collapse rule explicitly.
        let shared_a = WorkerRouter::new();
        assert!((**shared_a.local_worker_id.load()).is_none());
        let view_a = WorkerRouterView::from_shared(&shared_a);
        assert_eq!(
            view_a.local_worker_id(),
            None,
            "unprobed parent → view captures None"
        );

        // Case B: parent = Some(None) (probed, no local worker).
        let shared_b = WorkerRouter::new();
        shared_b
            .update_workers(vec![
                make_worker(1, "remote-a.example.com", 9203),
                make_worker(2, "remote-b.example.com", 9203),
            ])
            .await;
        assert_eq!(**shared_b.local_worker_id.load(), Some(None));
        let view_b = WorkerRouterView::from_shared(&shared_b);
        assert_eq!(
            view_b.local_worker_id(),
            None,
            "probed-no-local parent → view captures None"
        );

        // Case C: parent = Some(Some(id)) (probed, local worker present).
        let shared_c = WorkerRouter::new();
        shared_c
            .update_workers(vec![
                make_worker(1, "remote-a.example.com", 9203),
                make_worker(2, "127.0.0.1", 9203),
            ])
            .await;
        assert_eq!(**shared_c.local_worker_id.load(), Some(Some(2)));
        let view_c = WorkerRouterView::from_shared(&shared_c);
        assert_eq!(
            view_c.local_worker_id(),
            Some(2),
            "probed-local-present parent → view captures Some(id)"
        );

        // Case D: parent probed AFTER the view was minted — the view
        // must keep its captured value (this is the entire point of
        // the "captured at construction" semantics).
        let shared_d = WorkerRouter::new();
        let view_d = WorkerRouterView::from_shared(&shared_d);
        // Force a probe on the parent after the view exists.
        shared_d
            .update_workers(vec![make_worker(1, "127.0.0.1", 9203)])
            .await;
        assert!(matches!(
            **shared_d.local_worker_id.load(),
            Some(Some(1)) | Some(None)
        ));
        assert_eq!(
            view_d.local_worker_id(),
            None,
            "view minted before probe must keep its captured None — parent's later probe does NOT retroactively update the view"
        );
    }

    /// §3.4.6: `from_workers` must build a functional hash ring
    /// without going through a shared `WorkerRouter`. Verifies the
    /// legacy `file_in_stream.rs::open` escape hatch.
    #[tokio::test]
    async fn test_view_from_workers_builds_hash_ring() {
        let workers = vec![
            make_worker(1, "w1", 9203),
            make_worker(2, "w2", 9203),
            make_worker(3, "w3", 9203),
        ];
        let view = WorkerRouterView::from_workers(workers, Duration::from_secs(60));

        // Ring size must match the standard virtual-node count.
        assert_eq!(
            view.hash_ring_arc().len(),
            3 * VIRTUAL_NODES_PER_WORKER as usize
        );

        // `select_worker` must be deterministic on the same block_id.
        let a = view.select_worker(42).await.unwrap();
        let b = view.select_worker(42).await.unwrap();
        assert_eq!(a.id, b.id, "same block_id must select the same worker");

        // No local worker on the legacy path — by design.
        assert_eq!(
            view.local_worker_id(),
            None,
            "from_workers must NOT run detect_local_worker (blocking syscall on legacy path)"
        );
    }

    /// §3.4.6 (the key A/B parity test): for a wide spread of block ids,
    /// `WorkerRouterView::select_worker` must return the **exact same**
    /// worker as `WorkerRouter::select_worker` on the same shared
    /// router (and as `snapshot_from` — the coexistence contract).
    ///
    /// This is the invariant that unblocks Step 2 (call-site migration):
    /// as long as this test passes, migrating a file from
    /// `snapshot_from` to `WorkerRouterView::from_shared` cannot change
    /// which worker any block gets routed to.
    #[tokio::test]
    async fn test_view_select_worker_matches_shared_for_all_block_ids() {
        let shared = WorkerRouter::new();
        shared
            .update_workers(vec![
                make_worker(1, "w1", 9203),
                make_worker(2, "w2", 9203),
                make_worker(3, "w3", 9203),
                make_worker(4, "w4", 9203),
                make_worker(5, "w5", 9203),
            ])
            .await;
        let snap = WorkerRouter::snapshot_from(&shared);
        let view = WorkerRouterView::from_shared(&shared);

        for block_id in [
            0i64,
            1,
            -1,
            42,
            999,
            1 << 20,
            1 << 30,
            i64::MAX,
            i64::MIN,
            0x7fff_ffff_ffff_ffff,
        ] {
            let a = shared.select_worker(block_id).await.unwrap();
            let b = snap.select_worker(block_id).await.unwrap();
            let c = view.select_worker(block_id).await.unwrap();
            assert_eq!(
                a.id, b.id,
                "snapshot must match shared for block_id={}",
                block_id
            );
            assert_eq!(
                a.id, c.id,
                "view must match shared for block_id={} (A/B parity)",
                block_id
            );
        }
    }

    /// §3.4.6: failure state on a view must be isolated from the
    /// shared router (and from every other view) — identical to
    /// `snapshot_from`'s isolation contract.
    #[tokio::test]
    async fn test_view_mark_failed_is_isolated_from_shared() {
        let shared = WorkerRouter::with_failure_ttl(Duration::from_secs(3600));
        let workers = vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)];
        shared.update_workers(workers.clone()).await;

        let view = WorkerRouterView::from_shared(&shared);
        view.mark_failed(workers[0].address.as_ref().unwrap());

        // Shared router must still see w1 as healthy.
        assert!(!shared.is_failed(&worker_addr_key(workers[0].address.as_ref().unwrap())));
        // View must see it as failed.
        assert!(view.is_failed(&worker_addr_key(workers[0].address.as_ref().unwrap())));

        // A second view minted from the same shared router must NOT
        // inherit the first view's failure — per-view isolation.
        let view2 = WorkerRouterView::from_shared(&shared);
        assert!(!view2.is_failed(&worker_addr_key(workers[0].address.as_ref().unwrap())));
    }

    /// §3.4.6: `pick_any_worker` on a view must match the shared
    /// router's semantics — returns Ok on non-empty pools, Err on
    /// empty ones, respects the eligible/pool fallback.
    #[tokio::test]
    async fn test_view_pick_any_worker_semantics() {
        // Empty router → Err.
        let shared_empty = WorkerRouter::new();
        let view_empty = WorkerRouterView::from_shared(&shared_empty);
        assert!(view_empty.pick_any_worker().await.is_err());

        // Non-empty → Ok.
        let shared = WorkerRouter::new();
        shared
            .update_workers(vec![make_worker(1, "w1", 9203), make_worker(2, "w2", 9203)])
            .await;
        let view = WorkerRouterView::from_shared(&shared);
        let picked = view.pick_any_worker().await.unwrap();
        assert!(matches!(picked.id, Some(1) | Some(2)));

        // All failed → falls back to the full pool (matches shared behaviour).
        let w1_addr = shared.workers.load_full()[0].address.clone().unwrap();
        let w2_addr = shared.workers.load_full()[1].address.clone().unwrap();
        view.mark_failed(&w1_addr);
        view.mark_failed(&w2_addr);
        let picked_after_fail = view.pick_any_worker().await.unwrap();
        assert!(matches!(picked_after_fail.id, Some(1) | Some(2)));
    }

    /// §3.4.6 (documented behaviour): `from_workers` intentionally
    /// captures `local_worker_id = None`, so local-first routing is
    /// **disabled** on the legacy path even when a local worker is
    /// present in the list. This is by design (avoid the
    /// `hostname::get()` syscall on a non-hot code path) and this test
    /// pins it explicitly so a future change can't silently regress
    /// the invariant.
    #[tokio::test]
    async fn test_view_from_workers_no_local_first_when_not_probed() {
        let workers = vec![
            make_worker(1, "remote-a.example.com", 9203),
            make_worker(2, "127.0.0.1", 9203), // would be local on a probed shared router
        ];
        let view = WorkerRouterView::from_workers(workers, Duration::from_secs(60));

        assert_eq!(view.local_worker_id(), None);

        // Even for block_id = 0, selection must go through the ring —
        // NOT unconditionally return the "local-looking" worker 2.
        // (The ring position is deterministic; asserting the id is
        // stable across calls is enough to prove the local-first
        // shortcut was NOT taken.)
        let a = view.select_worker(0).await.unwrap();
        let b = view.select_worker(0).await.unwrap();
        assert_eq!(a.id, b.id);
    }

    /// P0-D Step 2.0: `WorkerRouterView::empty()` must match the
    /// pre-Step-2 `WorkerRouter::new()` semantics that the call-site
    /// migration relies on. Specifically:
    ///   - `select_worker` returns `NoWorkerAvailable` (empty ring).
    ///   - `pick_any_worker` returns `NoWorkerAvailable`.
    ///   - `local_worker_id` is `None` (matches the "unprobed" collapse).
    ///   - `mark_failed` on an empty view is a no-op that doesn't panic.
    /// `default_failure_ttl` must return the same `DEFAULT_FAILURE_TTL`
    /// the shared router uses, so a call site can pass it into
    /// `from_workers` without importing the private constant.
    #[tokio::test]
    async fn test_view_empty_matches_worker_router_new_semantics() {
        let view = WorkerRouterView::empty();

        // Routing on an empty view must fail with NoWorkerAvailable.
        assert!(matches!(
            view.select_worker(0).await,
            Err(Error::NoWorkerAvailable { .. })
        ));
        assert!(matches!(
            view.select_worker(i64::MAX).await,
            Err(Error::NoWorkerAvailable { .. })
        ));
        assert!(matches!(
            view.pick_any_worker().await,
            Err(Error::NoWorkerAvailable { .. })
        ));

        // No local worker.
        assert_eq!(view.local_worker_id(), None);

        // `mark_failed` on an unknown address must not panic and must
        // still bump the counter (the address is a valid key even if
        // no worker in the empty list matches it).
        view.mark_failed(&WorkerNetAddress {
            host: Some("unknown".to_string()),
            rpc_port: Some(9203),
            ..Default::default()
        });
        assert_eq!(view.failed_count.load(Ordering::Relaxed), 1);

        // Public default TTL matches the private constant used by the
        // shared router — this is the guarantee that lets a call site
        // pass `WorkerRouterView::default_failure_ttl()` to
        // `from_workers` and get the exact same failure-recovery window
        // as `WorkerRouter::new()` used to.
        assert_eq!(
            WorkerRouterView::default_failure_ttl(),
            DEFAULT_FAILURE_TTL,
            "public default TTL must equal the shared router's private DEFAULT_FAILURE_TTL"
        );
    }
}
