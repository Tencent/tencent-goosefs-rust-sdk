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

//! [`ShortCircuitFactory`] — per-task hot-block reader cache + the
//! [`should_use_short_circuit`] decision (design  /  / ).
//!
//! The factory owns:
//! - a **bounded** LRU of live [`LocalBlockReader`]s keyed by `block_id`, so a
//!   re-read of a hot block reuses the existing mmap (no new `OpenLocalBlock`),
//! - a **bounded** negative cache of recently-failed `block_id`s so the SC path
//!   is not retried for them until the entry expires (avoids repeated RTT),
//! - a sticky process-level "SC disabled" flag set on a permanent failure
//!   (e.g. `File::open` EACCES — uid mismatch, design ).
//!
//! Both caches are bounded `LruCache`s (never a naked `HashMap`) so a workload
//! touching many distinct block ids cannot grow them without bound (design
//! ). TTLs are enforced lazily on lookup.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use lru::LruCache;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::block::router::{rpc_endpoint, WorkerRouter};
use crate::client::WorkerClientPool;
use crate::config::GoosefsConfig;
use crate::error::{Error, Result};
use crate::metrics::{self, name};
use crate::proto::proto::security::Capability;

use super::{AccessHint, LocalBlockReader, ShortCircuitError};

/// Supplies the `Capability` to attach to an `OpenLocalBlock` request for a
/// given block (design  / INV-S3).
///
/// On capability-enabled clusters the Worker rejects `OpenLocalBlock` requests
/// without a valid capability; an implementation of this trait is how the SC
/// path obtains one. The credential **source** does not yet exist in the dev
/// client read path (`InStreamOptions` has no `capability_fetcher`), so by
/// default no provider is set and `None` (no capability) is sent — which on a
/// capability-enabled cluster simply triggers a transparent gRPC fallback
/// (INV-S1). Wiring a real provider (from `FileSystemContext` / auth config) is
/// the remaining external step.
pub trait CapabilityProvider: Send + Sync {
    /// Return the capability for `block_id`, or `None` if unavailable.
    fn capability_for(&self, block_id: i64) -> Option<Capability>;
}

/// Resolved short-circuit tuning, derived from [`GoosefsConfig`] (design ).
#[derive(Debug, Clone)]
pub struct ShortCircuitConfig {
    /// Master kill switch (`goosefs.user.short.circuit.enabled`).
    pub enabled: bool,
    /// Per-task LRU reader-cache capacity (`...cache.capacity`).
    pub cache_capacity: usize,
    /// Idle TTL after which a cached reader is dropped (`...cache.ttl`).
    pub cache_ttl: Duration,
    /// Negative-cache TTL (`...neg.cache.ttl`).
    pub neg_cache_ttl: Duration,
    /// L1 kernel-readahead hint (`...advise`).
    pub advise: AccessHint,
    /// L2 application prefetch master switch (`...prefetch.enabled`).
    pub prefetch_enabled: bool,
    /// Max gap merged by `prefetch_many` (`...prefetch.coalesce.gap`).
    pub prefetch_coalesce_gap: usize,
    /// Max `madvise` calls per `prefetch_many` (`...prefetch.max.batch`).
    pub prefetch_max_batch: usize,
    /// Minimum block size to attempt SC (`...min.block.size`).
    pub min_block_size: i64,
    /// Install the SIGBUS diagnostic handler (`...sigbus.handler`).
    pub sigbus_handler: bool,
    /// Request THP for the mapping via `MADV_HUGEPAGE` (`...thp`, experimental).
    pub thp: bool,
}

impl ShortCircuitConfig {
    /// Derive the SC tuning from the SDK config, applying safe clamps.
    pub fn from_config(cfg: &GoosefsConfig) -> Self {
        Self {
            enabled: cfg.short_circuit_enabled,
            cache_capacity: cfg.short_circuit_cache_capacity.max(1),
            cache_ttl: cfg.short_circuit_cache_ttl,
            neg_cache_ttl: cfg.short_circuit_neg_cache_ttl,
            advise: AccessHint::from_advise_str(&cfg.short_circuit_advise),
            prefetch_enabled: cfg.short_circuit_prefetch_enabled,
            prefetch_coalesce_gap: cfg.short_circuit_prefetch_coalesce_gap,
            prefetch_max_batch: cfg.short_circuit_prefetch_max_batch.max(1),
            min_block_size: cfg.short_circuit_min_block_size.max(0),
            sigbus_handler: cfg.short_circuit_sigbus_handler,
            thp: cfg.short_circuit_thp,
        }
    }
}

/// Inputs to the [`should_use_short_circuit`] decision (design ).
///
/// Kept as a plain data struct so the decision is a pure, unit-testable
/// function independent of any live cluster state.
#[derive(Debug, Clone, Copy)]
pub struct ScDecisionCtx {
    /// `source_is_local`: the block would be served by the local worker.
    pub source_is_local: bool,
    /// Sticky per-process "SC permanently disabled" flag (past EACCES).
    pub process_sc_disabled: bool,
    /// The block id is in the (unexpired) negative cache.
    pub negative_cached: bool,
    /// Logical block size in bytes.
    pub block_size: i64,
}

/// Pure SC gating decision (design  decision matrix).
///
/// ```text
/// should_use_short_circuit(cfg, ctx):
///   if !cfg.enabled                 -> false   # kill switch
///   if !ctx.source_is_local         -> false   # pre-filter
///   if ctx.process_sc_disabled      -> false   # past EACCES (sticky)
///   if ctx.negative_cached          -> false   # recent failure
///   if ctx.block_size < min_block   -> false   # tuning
///   return true
/// ```
pub fn should_use_short_circuit(cfg: &ShortCircuitConfig, ctx: &ScDecisionCtx) -> bool {
    cfg.enabled
        && ctx.source_is_local
        && !ctx.process_sc_disabled
        && !ctx.negative_cached
        && ctx.block_size >= cfg.min_block_size
}

/// A cached reader plus its insertion time (for idle-TTL eviction).
struct CachedReader {
    reader: Arc<LocalBlockReader>,
    inserted: Instant,
}

/// Per-task factory that opens, caches and reuses [`LocalBlockReader`]s, and
/// decides whether a given block should use the short-circuit path.
pub struct ShortCircuitFactory {
    /// Shared worker connection pool (control-plane `OpenLocalBlock` RPC).
    worker_pool: Arc<WorkerClientPool>,
    /// Shared router (local-worker detection + addressing).
    router: Arc<WorkerRouter>,
    /// Hot-block reader LRU (bounded + idle TTL). Lazily initialised in
    /// [`Self::cache`] / [`Self::neg_cache`](): a `FileSystemContext`
    /// created with SC enabled but used purely for non-local reads (or
    /// never read from at all) never pays for the `Mutex<LruCache>` pair.
    /// Mirrors the `OnceLock<DashMap>` pattern used for `WorkerRouter`'s
    /// `failed_workers`().
    cache: OnceLock<Mutex<LruCache<i64, CachedReader>>>,
    /// Bounded negative cache: `block_id -> last failure time`. See
    /// [`Self::cache`] for the lazy-init rationale.
    neg_cache: OnceLock<Mutex<LruCache<i64, Instant>>>,
    /// Sticky process-level disable (set on a permanent failure like EACCES).
    process_sc_disabled: AtomicBool,
    /// Optional capability provider for `OpenLocalBlock` (design ). `None`
    /// → send no capability (works on capability-disabled / NOSASL clusters).
    capability_provider: Option<Arc<dyn CapabilityProvider>>,
    cfg: ShortCircuitConfig,
}

impl ShortCircuitFactory {
    /// Create a factory from the shared pool + router and resolved SC config.
    ///
    /// **Lazy construction ( / table row #4)**: the LRU caches are NOT
    /// allocated here — they are wrapped in `OnceLock<Mutex<LruCache<…>>>`
    /// and only created on the first [`Self::get_or_open`] / [`Self::should_use`]
    /// call that actually needs them. A factory created but never used (e.g.
    /// a context that only does non-local reads) costs ~one `AtomicBool` and
    /// a couple of `Option<Arc>` slots instead of two `Mutex<LruCache>`s.
    pub fn new(
        worker_pool: Arc<WorkerClientPool>,
        router: Arc<WorkerRouter>,
        cfg: ShortCircuitConfig,
    ) -> Self {
        // Install the process-global SIGBUS diagnostic handler (idempotent).
        super::sigbus::install_if_enabled(cfg.sigbus_handler);
        Self {
            worker_pool,
            router,
            cache: OnceLock::new(),
            neg_cache: OnceLock::new(),
            process_sc_disabled: AtomicBool::new(false),
            capability_provider: None,
            cfg,
        }
    }

    /// Construct the hot-block reader LRU on first use.
    ///
    /// `OnceLock::get_or_init` is the standard once-initialised
    /// pattern; the first caller pays the `Mutex::new` + `LruCache::new`
    /// cost, every subsequent caller gets a `&Mutex<LruCache>` with no
    /// extra atomic. The closure is `FnOnce` so we move the capacity in.
    fn cache(&self) -> &Mutex<LruCache<i64, CachedReader>> {
        self.cache.get_or_init(|| {
            let cap = NonZeroUsize::new(self.cfg.cache_capacity.max(1)).unwrap();
            Mutex::new(LruCache::new(cap))
        })
    }

    /// Construct the negative cache on first use (see [`Self::cache`] for
    /// the lazy-init rationale).
    fn neg_cache_cell(&self) -> &Mutex<LruCache<i64, Instant>> {
        self.neg_cache.get_or_init(|| {
            // The negative cache is independently bounded; reuse the
            // reader-cache capacity as a sensible upper bound on distinct
            // recently-failed ids, with a 64-entry floor.
            let neg_cap = NonZeroUsize::new(self.cfg.cache_capacity.max(1).max(64)).unwrap();
            Mutex::new(LruCache::new(neg_cap))
        })
    }

    /// Test-only accessor: whether the hot-block LRU has been allocated.
    #[cfg(test)]
    fn cache_is_uninitialised(&self) -> bool {
        self.cache.get().is_none()
    }

    /// Test-only accessor: whether the negative cache has been allocated.
    #[cfg(test)]
    fn neg_cache_is_uninitialised(&self) -> bool {
        self.neg_cache.get().is_none()
    }

    /// Attach a [`CapabilityProvider`] used to populate `OpenLocalBlock`
    /// requests (design ). Builder style; defaults to no provider.
    pub fn with_capability_provider(mut self, provider: Arc<dyn CapabilityProvider>) -> Self {
        self.capability_provider = Some(provider);
        self
    }

    /// The resolved SC config.
    pub fn config(&self) -> &ShortCircuitConfig {
        &self.cfg
    }

    /// Whether SC has been permanently disabled for this process.
    pub fn is_process_disabled(&self) -> bool {
        self.process_sc_disabled.load(Ordering::Relaxed)
    }

    /// Decide whether `block_id` (of `block_size` bytes) should use SC.
    ///
    /// Gathers the live inputs (local-source pre-filter, sticky disable,
    /// negative cache) and applies [`should_use_short_circuit`].
    pub async fn should_use(&self, block_id: i64, block_size: i64) -> bool {
        if !self.cfg.enabled || self.is_process_disabled() {
            return false;
        }
        let ctx = ScDecisionCtx {
            source_is_local: self.router.is_block_source_local(block_id).await,
            process_sc_disabled: self.is_process_disabled(),
            negative_cached: self.is_negative_cached(block_id).await,
            block_size,
        };
        should_use_short_circuit(&self.cfg, &ctx)
    }

    /// Get a cached reader for `block_id`, or open one via `OpenLocalBlock`.
    ///
    /// On success the reader is cached (bounded LRU) for reuse. On a
    /// *recoverable* failure the `block_id` is added to the negative cache and
    /// the error is returned so the caller falls back to gRPC (INV-S1). A
    /// *semantic* error is never produced here (it can only arise from
    /// per-read bounds checks).
    pub async fn get_or_open(
        &self,
        block_id: i64,
        block_size: i64,
    ) -> std::result::Result<Arc<LocalBlockReader>, ShortCircuitError> {
        // 1) Fast path: fresh cached reader.
        if let Some(reader) = self.cache_get_fresh(block_id).await {
            metrics::counter(name::CLIENT_SC_CACHE_HITS).inc(1);
            return Ok(reader);
        }

        // 2) Resolve + acquire the (local) worker for the control-plane RPC.
        let worker = self
            .acquire_worker(block_id)
            .await
            .map_err(|e| ShortCircuitError::OpenLocalBlock(Box::new(e)))?;

        // 3) Open the local block. capability comes from the provider when one
        //    is configured (design ); otherwise `None` (no capability),
        //    which on a capability-enabled cluster triggers a transparent gRPC
        //    fallback (INV-S1/S3).
        let capability = self
            .capability_provider
            .as_ref()
            .and_then(|p| p.capability_for(block_id));
        let open_result = LocalBlockReader::open(
            &worker,
            block_id,
            block_size,
            capability,
            self.cfg.advise,
            self.cfg.thp,
        )
        .await;

        match open_result {
            Ok(reader) => {
                let reader = Arc::new(reader);
                self.cache_put(block_id, reader.clone()).await;
                Ok(reader)
            }
            Err(e) => {
                // Permanent failures (EACCES) sticky-disable SC for the whole
                // process (design ); transient ones only negative-cache the
                // block.
                if let ShortCircuitError::FileOpen(io) = &e {
                    if io.kind() == std::io::ErrorKind::PermissionDenied {
                        warn!(
                            block_id = block_id,
                            "short-circuit File::open EACCES — disabling SC for this process"
                        );
                        self.process_sc_disabled.store(true, Ordering::Relaxed);
                    }
                }
                self.mark_failure(block_id).await;
                Err(e)
            }
        }
    }

    /// Record a failed SC attempt for `block_id` so it is skipped until the
    /// negative-cache TTL expires.
    pub async fn mark_failure(&self, block_id: i64) {
        let mut neg = self.neg_cache_cell().lock().await;
        neg.put(block_id, Instant::now());
    }

    /// Drop any cached reader for `block_id` (e.g. on a detected
    /// inconsistency). The underlying mmap is released once the last
    /// outstanding `Bytes`/`Arc` reference is gone (INV-D3).
    pub async fn invalidate(&self, block_id: i64) {
        let mut cache = self.cache().lock().await;
        cache.pop(&block_id);
    }

    // ── internal helpers ────────────────────────────────────────────────

    /// Look up a fresh (non-expired) cached reader, dropping it if its idle
    /// TTL has elapsed.
    async fn cache_get_fresh(&self, block_id: i64) -> Option<Arc<LocalBlockReader>> {
        let mut cache = self.cache().lock().await;
        if let Some(entry) = cache.get(&block_id) {
            if entry.inserted.elapsed() < self.cfg.cache_ttl {
                return Some(entry.reader.clone());
            }
            // Expired — evict.
            cache.pop(&block_id);
            metrics::counter(name::CLIENT_SC_CACHE_EVICTIONS).inc(1);
        }
        None
    }

    /// Insert `reader`, accounting for LRU capacity eviction.
    async fn cache_put(&self, block_id: i64, reader: Arc<LocalBlockReader>) {
        let mut cache = self.cache().lock().await;
        // `put` returns the previous value for the same key (not an eviction);
        // a capacity eviction is detected by the cache being full before insert.
        let was_full = cache.len() == cache.cap().get() && cache.peek(&block_id).is_none();
        cache.put(
            block_id,
            CachedReader {
                reader,
                inserted: Instant::now(),
            },
        );
        if was_full {
            metrics::counter(name::CLIENT_SC_CACHE_EVICTIONS).inc(1);
        }
    }

    /// Whether `block_id` has an unexpired negative-cache entry.
    async fn is_negative_cached(&self, block_id: i64) -> bool {
        let mut neg = self.neg_cache_cell().lock().await;
        if let Some(t) = neg.get(&block_id) {
            if t.elapsed() < self.cfg.neg_cache_ttl {
                metrics::counter(name::CLIENT_SC_NEG_CACHE_HITS).inc(1);
                return true;
            }
            // Expired — drop it so the block can be retried.
            neg.pop(&block_id);
        }
        false
    }

    /// Resolve the worker serving `block_id` and acquire a pooled client.
    async fn acquire_worker(&self, block_id: i64) -> Result<crate::client::WorkerClient> {
        let worker_info = self.router.select_worker(block_id).await?;
        let addr = worker_info
            .address
            .as_ref()
            .ok_or_else(|| Error::Internal {
                message: "short-circuit: worker has no address".to_string(),
                source: None,
            })?;
        let worker_addr = rpc_endpoint(addr);
        debug!(block_id = block_id, worker = %worker_addr, "short-circuit acquiring local worker");
        self.worker_pool.acquire(&worker_addr).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg() -> ShortCircuitConfig {
        ShortCircuitConfig {
            enabled: true,
            cache_capacity: 64,
            cache_ttl: Duration::from_secs(30),
            neg_cache_ttl: Duration::from_secs(5),
            advise: AccessHint::Random,
            prefetch_enabled: true,
            prefetch_coalesce_gap: 64 * 1024,
            prefetch_max_batch: 1024,
            min_block_size: 0,
            sigbus_handler: false,
            thp: false,
        }
    }

    #[test]
    fn decision_kill_switch() {
        let mut cfg = test_cfg();
        cfg.enabled = false;
        let ctx = ScDecisionCtx {
            source_is_local: true,
            process_sc_disabled: false,
            negative_cached: false,
            block_size: 1 << 20,
        };
        assert!(!should_use_short_circuit(&cfg, &ctx));
    }

    #[test]
    fn decision_requires_local_source() {
        let cfg = test_cfg();
        let ctx = ScDecisionCtx {
            source_is_local: false,
            process_sc_disabled: false,
            negative_cached: false,
            block_size: 1 << 20,
        };
        assert!(!should_use_short_circuit(&cfg, &ctx));
    }

    #[test]
    fn decision_sticky_disable_and_neg_cache() {
        let cfg = test_cfg();
        let base = ScDecisionCtx {
            source_is_local: true,
            process_sc_disabled: false,
            negative_cached: false,
            block_size: 1 << 20,
        };
        assert!(should_use_short_circuit(&cfg, &base));

        let disabled = ScDecisionCtx {
            process_sc_disabled: true,
            ..base
        };
        assert!(!should_use_short_circuit(&cfg, &disabled));

        let neg = ScDecisionCtx {
            negative_cached: true,
            ..base
        };
        assert!(!should_use_short_circuit(&cfg, &neg));
    }

    #[test]
    fn decision_min_block_size() {
        let mut cfg = test_cfg();
        cfg.min_block_size = 2 * 1024 * 1024;
        let small = ScDecisionCtx {
            source_is_local: true,
            process_sc_disabled: false,
            negative_cached: false,
            block_size: 1024,
        };
        assert!(!should_use_short_circuit(&cfg, &small));

        let big = ScDecisionCtx {
            block_size: 4 * 1024 * 1024,
            ..small
        };
        assert!(should_use_short_circuit(&cfg, &big));
    }

    #[test]
    fn config_from_goosefs_config_defaults() {
        // `GoosefsConfig::default()` now emits
        // `short_circuit_enabled: false` (see
        //
        // `config::tests::test_short_circuit_enabled_default_is_false`).
        // `ShortCircuitConfig::from_config` faithfully mirrors that, so
        // the default `enabled` is `false`, not `true`. Flip it on
        // explicitly and re-derive to check the rest of the mapping.
        let cfg = GoosefsConfig::new("127.0.0.1:9200");
        let sc = ShortCircuitConfig::from_config(&cfg);
        assert!(
            !sc.enabled,
            "SC must be OFF by default (P2-B) — flip on via env/storage-options/API to opt in"
        );

        let cfg_on = GoosefsConfig::new("127.0.0.1:9200").with_short_circuit_enabled(true);
        let sc_on = ShortCircuitConfig::from_config(&cfg_on);
        assert!(sc_on.enabled);
        assert_eq!(sc_on.cache_capacity, 64);
        assert_eq!(sc_on.cache_ttl, Duration::from_secs(30));
        assert_eq!(sc_on.neg_cache_ttl, Duration::from_secs(5));
        assert_eq!(sc_on.advise, AccessHint::Random);
        assert!(sc_on.prefetch_enabled);
    }

    /// `ShortCircuitFactory::new` must NOT allocate either
    /// `Mutex<LruCache>`. A factory created but never used (e.g. a
    /// `FileSystemContext` that only does non-local reads) should keep
    /// both `OnceLock`s uninitialised until the first `should_use` /
    /// `get_or_open` / `invalidate` / `mark_failure` / `is_process_disabled`
    /// call. Mirrors the `WorkerRouter::failed_workers`
    /// lazy-init test.
    #[tokio::test]
    async fn test_factory_caches_are_lazy_initialised() {
        use crate::block::router::WorkerRouter;
        use crate::client::WorkerClientPool;

        let pool = WorkerClientPool::new_shared(GoosefsConfig::new("127.0.0.1:9200"));
        let router = Arc::new(WorkerRouter::new());
        //  needs SC actually enabled — otherwise `should_use` would
        // short-circuit on `!self.cfg.enabled` (default) and never
        // touch the negative cache, masking the lazy-init we want to test.
        let cfg = GoosefsConfig::new("127.0.0.1:9200").with_short_circuit_enabled(true);
        let sc_cfg = ShortCircuitConfig::from_config(&cfg);

        let factory = ShortCircuitFactory::new(pool, router, sc_cfg);
        assert!(
            factory.cache_is_uninitialised(),
            "hot-block LRU must stay uninitialised on the happy path (no reads)"
        );
        assert!(
            factory.neg_cache_is_uninitialised(),
            "negative cache must stay uninitialised on the happy path (no reads)"
        );

        // The decision path is the only thing that hits the negative
        // cache eagerly (via `is_negative_cached`).
        let _ = factory.should_use(1, 1024).await;
        // `should_use` always allocates the neg cache (even if the
        // decision turns out to be false) — confirm the OnceLock is now
        // initialised, then the other side stays lazy.
        assert!(!factory.neg_cache_is_uninitialised());
        // The hot-block LRU must still be untouched because `should_use`
        // never reads or writes to it.
        assert!(
            factory.cache_is_uninitialised(),
            "hot-block LRU must stay uninitialised — should_use does not touch it"
        );
    }
}
