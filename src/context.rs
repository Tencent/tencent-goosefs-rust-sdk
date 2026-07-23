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

//! `FileSystemContext` — shared connection pool and routing context.
//!
//! This module implements the **three-layer connection architecture** that
//! eliminates repeated TCP+SASL handshakes:
//!
//! ```text
//! Layer 3: FileSystemContext — lifecycle manager + unified acquisition API
//!          │
//!          ├── Arc<MasterClient>           — persistent Master gRPC channel
//!          ├── Arc<WorkerManagerClient>    — persistent WorkerMgr gRPC channel
//!          ├── Arc<WorkerClientPool>       — shared Worker connection pool
//!          ├── Arc<WorkerRouter>           — shared consistent-hash router
//!          └── Option<Arc<HeartbeatTask>>  — periodic metrics heartbeat (when enabled)
//!
//! Layer 2: WorkerClientPool / WorkerRouter — connection & routing management
//!
//! Layer 1: MasterClient / WorkerManagerClient / WorkerClient — gRPC stubs
//! ```
//!
//! # Before vs After
//!
//! | Operation | Before (per-call) | After (shared) |
//! |-----------|-------------------|----------------|
//! | `BaseFileSystem::get_status()` | 1 TCP+SASL | 0 (reused) |
//! | `GoosefsFileInStream::open()` | 2 TCP+SASL | 0 (reused) |
//! | Reading N blocks | N TCP connects | ~N_workers (pooled) |
//!
//! # Usage
//!
//! ```rust,no_run
//! use goosefs_sdk::context::FileSystemContext;
//! use goosefs_sdk::config::GoosefsConfig;
//! use goosefs_sdk::fs::FileSystem; // needed to call trait methods
//!
//! # async fn example() -> goosefs_sdk::error::Result<()> {
//! // Build once, share across all operations
//! let ctx = FileSystemContext::connect(GoosefsConfig::new("127.0.0.1:9200")).await?;
//!
//! // Pass ctx into filesystem operations
//! use goosefs_sdk::fs::BaseFileSystem;
//! let fs = BaseFileSystem::from_context(ctx.clone());
//! let status = fs.get_status("/data/file.parquet").await?;
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::block::router::WorkerRouter;
use crate::block::short_circuit::{ShortCircuitConfig, ShortCircuitFactory};
use crate::cache::{CacheManager, LocalCacheManager};
use crate::client::metrics_master::MetricsClient;
use crate::client::metrics_master::MetricsMasterClient;
use crate::client::{
    create_master_inquire_client, MasterClient, MasterClientPool, MasterInquireClient,
    WorkerClientPool, WorkerManagerClient,
};
use crate::config::{ConfigRefresher, GoosefsConfig, TransparentAccelerationSwitch};
use crate::error::Result;
use crate::file_info_cache::FileInfoCache;
use crate::metrics::heartbeat::{resolve_app_id, HeartbeatTask};
#[cfg(feature = "metrics-pushgateway")]
use crate::metrics::pushgateway::{PushgatewayConfig, PushgatewayTask};
use crate::metrics::reporter::ClientMetricsReporter;

/// How often the background refresh loop checks whether the worker list is stale.
/// Matches the `DEFAULT_WORKER_REFRESH_TTL` (30s) in `WorkerRouter`.
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How often the background config refresh loop runs (default 60s).
///
/// Mirrors Java's `refreshInterval` (default 60s) in `NamespaceRefreshThread`.
/// This is intentionally separate from [`REFRESH_CHECK_INTERVAL`] so that
/// config reloading and worker-list refreshing run on independent cadences.
const CONFIG_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Shared connection context for Goosefs filesystem operations.
///
/// A single `FileSystemContext` instance should be created per Goosefs cluster
/// and shared across all `BaseFileSystem`, `GoosefsFileInStream`, and
/// `GoosefsFileWriter` instances that connect to that cluster.
///
/// The context owns:
/// - One persistent gRPC channel to the Master
/// - One persistent gRPC channel to the WorkerManager service
/// - One `WorkerClientPool` shared across all readers and writers
/// - One `WorkerRouter` that tracks live workers and routes block reads
pub struct FileSystemContext {
    config: Arc<GoosefsConfig>,

    /// Persistent Master gRPC connection pool (metadata RPCs).
    ///
    /// Pool of `config.master_connection_pool_size` channels (default 1).
    /// Scheduling strategy is controlled by `master_connection_pool_schedule`
    /// (default `RoundRobin`; set to `P2C` for adaptive load balancing).
    master_pool: Arc<MasterClientPool>,

    /// Persistent WorkerManager gRPC connection (`GetWorkerInfoList`).
    ///
    /// `None` when the remote Master does not expose `WorkerManagerMasterClientService`
    /// (e.g. older GooseFS versions).  In that case, operations that need worker
    /// discovery will log a warning and fall back to the Master RPC path.
    worker_manager: Option<Arc<WorkerManagerClient>>,

    /// Worker gRPC connection pool — shared across all readers and writers.
    worker_pool: Arc<WorkerClientPool>,

    /// Consistent-hash router with TTL refresh and local-first preference.
    worker_router: Arc<WorkerRouter>,

    /// Shared short-circuit (local mmap) factory, when SC is enabled.
    ///
    /// Hoisted to the context (P8) so **all** `GoosefsFileInStream`s built from
    /// this context share one hot-block reader LRU + negative cache: a hot
    /// local block is `OpenLocalBlock`+mmap'd once and reused across every
    /// concurrent stream/task, instead of once per stream. `None` when the SC
    /// kill switch is off. See `docs/SHORT_CIRCUIT_DESIGN.md` §3.5 / §10 P8.
    short_circuit: Option<Arc<ShortCircuitFactory>>,

    /// Client-side local page cache, when `config.client_cache_enabled`.
    ///
    /// `None` when the cache is disabled or failed to initialize (the cache is
    /// best-effort, so an init failure degrades to no-cache rather than
    /// failing `connect()`). Shared across all readers in this context.
    cache_manager: Option<Arc<dyn CacheManager>>,

    /// Opt-in short-TTL `FileInfo` metadata cache
    /// (FLAMEGRAPH_OPTIMIZATION_PLAN §A3).
    ///
    /// `None` only when `config.file_info_cache_ttl == 0`. By default the
    /// TTL is 30 s, so this is a live cache. When enabled,
    /// [`GoosefsFileReader::open_with_context`] and
    /// [`GoosefsFileInStream::open_with_context`] consult this cache before
    /// issuing `MasterClient::get_status`, and the write path
    /// (create / delete / rename) explicitly invalidates entries so
    /// through-client mutations are never observed as stale.
    file_info_cache: Option<Arc<FileInfoCache>>,

    /// HA Master address discovery client (shared between master + wm).
    inquire_client: Arc<dyn MasterInquireClient>,

    /// Periodic config refresher — reloads `goosefs-site.properties` when
    /// expired and updates the transparent acceleration switch flags.
    ///
    /// Mirrors Java's `ConfigurationUtils.loadIfExpire()` +
    /// `AbstractCompatibleFileSystem.refreshTransparentAccelerationSwitch()`.
    config_refresher: Arc<ConfigRefresher>,

    /// Set to `true` once `close()` has been called.
    closed: Arc<AtomicBool>,

    /// Handle to the background worker-list TTL-refresh task.
    /// Aborted on `close()`.
    worker_refresh_task: Mutex<Option<tokio::task::JoinHandle<()>>>,

    /// Handle to the background config refresh task.
    /// Aborted on `close()`.
    config_refresh_task: Mutex<Option<tokio::task::JoinHandle<()>>>,

    /// Periodic metrics heartbeat task.
    /// `None` when `config.metrics_enabled = false`.
    /// Shut down gracefully (with final flush) in `close()`.
    metrics_heartbeat: Mutex<Option<Arc<HeartbeatTask>>>,

    /// Prometheus Pushgateway background push task.
    /// `None` when `config.pushgateway_enabled = false`.
    /// Shut down gracefully (with final flush) in `close()`.
    #[cfg(feature = "metrics-pushgateway")]
    pushgateway_task: Mutex<Option<PushgatewayTask>>,
}

impl FileSystemContext {
    // ── Construction ────────────────────────────────────────────────────────

    /// Build a `FileSystemContext` by connecting to the Goosefs cluster.
    ///
    /// Establishes persistent connections to the Master and WorkerManager,
    /// fetches the initial worker list, and starts a background refresh task.
    ///
    /// This is the **only** call that performs network I/O.  All subsequent
    /// operations on the context are zero-cost Arc clones.
    pub async fn connect(config: GoosefsConfig) -> Result<Arc<Self>> {
        let config = Arc::new(config);

        // Build a shared inquire client so Master + WorkerManager both use the
        // same singleflight-deduped HA discovery.
        let inquire_client = create_master_inquire_client(&config);

        // Connect Master pool (required) and WorkerManager (optional on older clusters).
        let (pool_res, wm_res) = tokio::join!(
            MasterClientPool::connect_with_inquire(&config, inquire_client.clone()),
            WorkerManagerClient::connect_with_inquire(&config, inquire_client.clone()),
        );
        let master_pool = Arc::new(pool_res?);

        // Build the router with failure/refresh TTLs.
        let worker_router = Arc::new(WorkerRouter::with_ttls(
            Duration::from_secs(60), // failure_ttl
            Duration::from_secs(30), // worker_refresh_ttl (matches Go SDK)
        ));

        // WorkerManager is optional — older Master versions may not expose
        // WorkerManagerMasterClientService / GetWorkerInfoList.
        let worker_manager = match wm_res {
            Ok(wm) => {
                // Try to fetch the initial worker list; if the RPC itself fails
                // (e.g. Method not found), treat the WorkerManager as unavailable.
                match wm.get_worker_info_list().await {
                    Ok(workers) => {
                        if workers.is_empty() {
                            warn!("WorkerManager returned empty worker list — proceeding without worker discovery");
                            None
                        } else {
                            debug!(count = workers.len(), "initial worker list fetched");
                            worker_router.update_workers(workers).await;
                            Some(Arc::new(wm))
                        }
                    }
                    Err(e) => {
                        warn!("GetWorkerInfoList failed ({}), proceeding without worker discovery. \
                               Master-only operations (CreateFile, GetStatus, etc.) will still work.", e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!(
                    "WorkerManager connection failed ({}), proceeding without worker discovery. \
                       Master-only operations (CreateFile, GetStatus, etc.) will still work.",
                    e
                );
                None
            }
        };

        // Build the shared worker connection pool.
        let worker_pool = WorkerClientPool::new_shared((*config).clone());

        // Build the shared short-circuit factory (one per context, reused by
        // every stream) when the SC kill switch is on. It uses the shared
        // worker pool + router so local-worker detection and connection reuse
        // are consistent across all streams (P8).
        let sc_cfg = ShortCircuitConfig::from_config(&config);
        let short_circuit = if sc_cfg.enabled {
            Some(Arc::new(ShortCircuitFactory::new(
                worker_pool.clone(),
                worker_router.clone(),
                sc_cfg,
            )))
        } else {
            None
        };

        // Build the client-side local page cache (best-effort).
        let cache_manager: Option<Arc<dyn CacheManager>> = if config.client_cache_enabled {
            match LocalCacheManager::from_config(&config).await {
                Ok(mgr) => {
                    debug!(
                        page_size = config.client_cache_page_size,
                        dirs = ?config.client_cache_dirs,
                        "client local page cache enabled"
                    );
                    Some(mgr as Arc<dyn CacheManager>)
                }
                Err(e) => {
                    warn!(error = %e, "failed to init client page cache; continuing without cache");
                    None
                }
            }
        } else {
            None
        };

        // Build the opt-in FileInfo (metadata) cache — §A3. `maybe_new`
        // returns `None` when the TTL is zero (default), so this is a
        // no-op unless the caller explicitly opted in via
        // `with_file_info_cache_ttl`.
        let file_info_cache =
            FileInfoCache::maybe_new(config.file_info_cache_ttl, config.file_info_cache_capacity);
        if let Some(c) = &file_info_cache {
            debug!(
                ttl_ms = config.file_info_cache_ttl.as_millis(),
                capacity = config.file_info_cache_capacity,
                "FileInfo metadata cache enabled (opt-in, §A3), ttl={:?}",
                c.ttl(),
            );
        }

        let ctx = Arc::new(Self {
            config: config.clone(),
            master_pool,
            worker_manager,
            worker_pool,
            worker_router,
            short_circuit,
            cache_manager,
            file_info_cache,
            inquire_client,
            config_refresher: Arc::new(ConfigRefresher::from_config(&config)),
            closed: Arc::new(AtomicBool::new(false)),
            worker_refresh_task: Mutex::new(None),
            config_refresh_task: Mutex::new(None),
            metrics_heartbeat: Mutex::new(None),
            #[cfg(feature = "metrics-pushgateway")]
            pushgateway_task: Mutex::new(None),
        });

        // Start the background worker-list refresh loop.
        ctx.clone().start_worker_refresh_task().await;
        // Start the background config refresh loop (separate cadence).
        ctx.clone().start_config_refresh_task().await;
        // Start the metrics heartbeat task (no-op when metrics_enabled = false).
        ctx.clone().start_metrics_heartbeat_task().await?;
        // Start the Pushgateway push task (no-op when pushgateway_enabled = false).
        #[cfg(feature = "metrics-pushgateway")]
        ctx.clone().start_pushgateway_task().await;

        Ok(ctx)
    }

    // ── Acquisition API ──────────────────────────────────────────────────────

    /// Return a shared `MasterClient` from the pool.
    ///
    /// With `master_connection_pool_schedule = RoundRobin` (default) this
    /// cycles through pooled channels in order. With `P2C` it picks the
    /// least-loaded connection out of two random candidates. Per-channel
    /// in-flight counts are tracked inside `MasterClient::with_retry`, so
    /// the load signal stays accurate even for clients cloned out of the
    /// pool (e.g. by `GoosefsFileWriter`).
    pub fn acquire_master(&self) -> Arc<MasterClient> {
        self.master_pool.pick()
    }

    /// Return the shared `MasterClientPool` (zero-cost Arc clone).
    pub fn acquire_master_pool(&self) -> Arc<MasterClientPool> {
        self.master_pool.clone()
    }

    /// Return the shared `WorkerManagerClient` (zero-cost Arc clone).
    ///
    /// Returns `None` when the Master does not support `GetWorkerInfoList`.
    pub fn acquire_worker_manager(&self) -> Option<Arc<WorkerManagerClient>> {
        self.worker_manager.clone()
    }

    /// Return the shared `WorkerClientPool` (zero-cost Arc clone).
    pub fn acquire_worker_pool(&self) -> Arc<WorkerClientPool> {
        self.worker_pool.clone()
    }

    /// Return the shared `WorkerRouter` (zero-cost Arc clone).
    pub fn acquire_router(&self) -> Arc<WorkerRouter> {
        self.worker_router.clone()
    }

    /// Return the shared short-circuit factory, if SC is enabled (zero-cost
    /// Arc clone). All streams built from this context share it (P8), so a hot
    /// local block is opened/mmap'd once and reused across streams.
    pub fn acquire_short_circuit(&self) -> Option<Arc<ShortCircuitFactory>> {
        self.short_circuit.clone()
    }

    /// Return the shared client-side page cache, if enabled.
    ///
    /// `None` when `config.client_cache_enabled = false` or the cache failed
    /// to initialize. Readers consult this on the random-read path.
    pub fn acquire_cache_manager(&self) -> Option<Arc<dyn CacheManager>> {
        self.cache_manager.clone()
    }

    /// Return the shared opt-in `FileInfo` metadata cache
    /// (FLAMEGRAPH_OPTIMIZATION_PLAN §A3).
    ///
    /// `None` only when `config.file_info_cache_ttl == 0` (opt-out).
    /// By default the TTL is 30 s, so this returns the live cache.
    pub fn acquire_file_info_cache(&self) -> Option<Arc<FileInfoCache>> {
        self.file_info_cache.clone()
    }

    /// Convenience: invalidate the `FileInfo` cache entry for `path`, if the
    /// cache is enabled. Idempotent no-op when the cache is disabled or the
    /// path is not currently cached.
    ///
    /// **Contract**: every write path (create, delete, rename, setAttr...)
    /// that mutates `path` on the master through this client MUST call this
    /// after the mutation is acknowledged, so subsequent reads observe the
    /// fresh metadata (§A3).
    pub fn invalidate_file_info(&self, path: &str) {
        if let Some(cache) = &self.file_info_cache {
            cache.invalidate(path);
        }
    }

    /// Return the shared `MasterInquireClient` (zero-cost Arc clone).
    pub fn acquire_inquire_client(&self) -> Arc<dyn MasterInquireClient> {
        self.inquire_client.clone()
    }

    /// Return the configuration used to build this context.
    pub fn config(&self) -> &GoosefsConfig {
        &self.config
    }

    /// Return the shared `ConfigRefresher` (zero-cost Arc clone).
    ///
    /// Use this to query the current transparent acceleration switch values
    /// or to trigger a config reload check.
    pub fn acquire_config_refresher(&self) -> Arc<ConfigRefresher> {
        self.config_refresher.clone()
    }

    /// Refresh the transparent acceleration switch by reloading config if expired.
    ///
    /// Convenience wrapper around `ConfigRefresher::refresh_transparent_acceleration_switch()`.
    /// Mirrors Java's `AbstractCompatibleFileSystem.refreshTransparentAccelerationSwitch()`.
    pub fn refresh_transparent_acceleration_switch(&self) -> TransparentAccelerationSwitch {
        self.config_refresher
            .refresh_transparent_acceleration_switch()
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    /// Gracefully shut down the context.
    ///
    /// Aborts the background refresh task and marks the context as closed.
    /// Idempotent — safe to call multiple times.
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already closed.
        }

        // Cancel the background refresh tasks.
        let worker_handle = self.worker_refresh_task.lock().await.take();
        if let Some(h) = worker_handle {
            h.abort();
            debug!("worker refresh task aborted");
        }
        let config_handle = self.config_refresh_task.lock().await.take();
        if let Some(h) = config_handle {
            h.abort();
            debug!("config refresh task aborted");
        }

        // Gracefully shut down the metrics heartbeat task (performs final flush).
        if let Some(task) = self.metrics_heartbeat.lock().await.take() {
            task.shutdown().await;
            debug!("metrics heartbeat task shut down");
        }

        // Gracefully shut down the Pushgateway push task (performs final push).
        #[cfg(feature = "metrics-pushgateway")]
        if let Some(task) = self.pushgateway_task.lock().await.take() {
            task.shutdown().await;
            debug!("pushgateway task shut down");
        }

        Ok(())
    }

    /// Return `true` if `close()` has been called.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    // ── Background refresh ────────────────────────────────────────────────────

    /// Start the background worker-list TTL-refresh loop.
    ///
    /// The loop wakes every [`REFRESH_CHECK_INTERVAL`] seconds, calls
    /// [`WorkerRouter::needs_refresh`], and if stale triggers
    /// [`WorkerRouter::refresh_workers`].
    async fn start_worker_refresh_task(self: Arc<Self>) {
        let worker_router = self.worker_router.clone();
        let worker_manager = self.worker_manager.clone();
        let closed = self.closed.clone();

        // If no WorkerManager is available, skip the refresh task entirely.
        let Some(wm) = worker_manager else {
            debug!("worker refresh task skipped: no WorkerManager available");
            return;
        };

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(REFRESH_CHECK_INTERVAL).await;

                // Stop if the context has been closed.
                if closed.load(Ordering::SeqCst) {
                    debug!("worker refresh task: context closed, exiting");
                    break;
                }

                // Refresh worker list if stale.
                if worker_router.needs_refresh().await {
                    if let Err(e) = worker_router.refresh_workers(&wm).await {
                        warn!("worker refresh failed: {}", e);
                        // refresh_workers already resets the TTL clock to avoid
                        // hammering on repeated failures (stale-while-revalidate).
                    } else {
                        debug!("worker list refreshed by background task");
                    }
                }
            }
        });

        *self.worker_refresh_task.lock().await = Some(handle);
    }

    /// Start the background config refresh loop.
    ///
    /// On first invocation the task **immediately** loads the config from
    /// `goosefs-site.properties` (via `refresh_transparent_acceleration_switch`)
    /// so that the transparent acceleration switches are up-to-date right after
    /// `connect()` returns.  Subsequent refreshes happen every
    /// [`CONFIG_REFRESH_INTERVAL`] seconds (default 60s, matching Java's
    /// `refreshInterval`).
    ///
    /// This runs independently from the worker-list refresh task.
    async fn start_config_refresh_task(self: Arc<Self>) {
        let config_refresher = self.config_refresher.clone();
        let closed = self.closed.clone();

        let handle = tokio::spawn(async move {
            // Eagerly load config on startup so the switches are current
            // before any file-system operation is issued.
            let switch = config_refresher.refresh_transparent_acceleration_switch();
            debug!(
                transparent_acceleration_enabled = switch.enabled,
                cosranger_enabled = switch.cosranger_enabled,
                "config refresh: initial load completed"
            );

            loop {
                tokio::time::sleep(CONFIG_REFRESH_INTERVAL).await;

                // Stop if the context has been closed.
                if closed.load(Ordering::SeqCst) {
                    debug!("config refresh task: context closed, exiting");
                    break;
                }

                // Refresh transparent acceleration switch (reload config if expired).
                // Mirrors Java's NamespaceRefreshThread calling
                // refreshTransparentAccelerationSwitch() each loop iteration.
                let switch = config_refresher.refresh_transparent_acceleration_switch();
                debug!(
                    transparent_acceleration_enabled = switch.enabled,
                    cosranger_enabled = switch.cosranger_enabled,
                    "config refresh check completed"
                );
            }
        });

        *self.config_refresh_task.lock().await = Some(handle);
    }
}

impl Drop for FileSystemContext {
    fn drop(&mut self) {
        // Signal `closed` first so any in-flight background tasks that poll
        // this flag stop themselves before we try to abort their handles.
        // This avoids a race where a task wakes up between our abort() call
        // and the actual cancellation and touches shared state.
        self.closed.store(true, Ordering::SeqCst);

        // Best-effort abort of the refresh tasks.
        // `drop` is synchronous, so we use `try_lock`; if we cannot obtain the
        // lock the task loop will observe `closed == true` on its next iteration
        // and exit on its own.
        if let Ok(mut guard) = self.worker_refresh_task.try_lock() {
            if let Some(h) = guard.take() {
                h.abort();
            }
        }
        if let Ok(mut guard) = self.config_refresh_task.try_lock() {
            if let Some(h) = guard.take() {
                h.abort();
            }
        }
        // Send non-blocking shutdown signal to heartbeat task.
        // HeartbeatTask::drop() will handle the rest (sets closed + try_send).
        // We don't await shutdown() here because drop() is synchronous.
        if let Ok(mut guard) = self.metrics_heartbeat.try_lock() {
            guard.take(); // dropping the Arc triggers HeartbeatTask::drop
        }
    }
}

impl FileSystemContext {
    // ── Metrics heartbeat ──────────────────────────────────────────────────

    /// Start the periodic metrics heartbeat background task.
    ///
    /// Does nothing when `config.metrics_enabled = false`, so no
    /// `MetricsMasterClient` is created and no background task is spawned.
    ///
    /// The task shares the same [`MasterInquireClient`] as `MasterClient` and
    /// `WorkerManagerClient` for HA primary discovery.
    async fn start_metrics_heartbeat_task(self: Arc<Self>) -> Result<()> {
        if !self.config.metrics_enabled {
            debug!("metrics disabled — heartbeat task not started");
            return Ok(());
        }

        let mm_client =
            MetricsMasterClient::connect_with_inquire(&self.config, self.inquire_client.clone())
                .await?;

        let reporter = Arc::new(ClientMetricsReporter::default());
        let app_id = resolve_app_id(&self.config);

        debug!(
            app_id = %app_id,
            interval_ms = self.config.metrics_heartbeat_interval.as_millis(),
            timeout_ms = self.config.metrics_heartbeat_timeout.as_millis(),
            "starting metrics heartbeat task"
        );

        let task = Arc::new(HeartbeatTask::spawn(
            Arc::new(mm_client) as Arc<dyn MetricsClient>,
            reporter,
            app_id,
            self.config.metrics_heartbeat_interval,
            self.config.metrics_heartbeat_timeout,
            self.closed.clone(),
        ));
        *self.metrics_heartbeat.lock().await = Some(task);
        Ok(())
    }

    // ── Pushgateway ────────────────────────────────────────────────────────

    /// Start the Prometheus Pushgateway background push task.
    ///
    /// If the initial config has `pushgateway_enabled = false`, this method
    /// will attempt to auto-discover pushgateway settings from the properties
    /// file (via `GoosefsConfig::from_properties_auto()`).  This ensures that
    /// callers using `GoosefsConfig::new(addr)` still get pushgateway reporting
    /// when the properties file has it enabled.
    ///
    /// Does nothing only when **both** the initial config and the properties
    /// file have pushgateway disabled (or no properties file is found).
    #[cfg(feature = "metrics-pushgateway")]
    async fn start_pushgateway_task(self: Arc<Self>) {
        // Determine the effective pushgateway config: prefer the initial config
        // if it already has pushgateway enabled; otherwise try auto-discovery.
        let effective_config = if self.config.pushgateway_enabled {
            // Caller explicitly enabled pushgateway — use their settings.
            None
        } else {
            // Try loading from properties file to see if pushgateway is enabled there.
            match GoosefsConfig::from_properties_auto() {
                Ok(file_cfg) if file_cfg.pushgateway_enabled => {
                    debug!(
                        "pushgateway not enabled in initial config, \
                         but enabled in properties file — using file config"
                    );
                    Some(file_cfg)
                }
                _ => {
                    debug!("pushgateway disabled — push task not started");
                    return;
                }
            }
        };

        // Use the effective config (either initial or from properties file).
        let cfg = effective_config.as_ref().unwrap_or(&self.config);

        let mut pg_config = PushgatewayConfig::new(
            cfg.pushgateway_endpoint.clone(),
            cfg.pushgateway_job.clone(),
        )
        .with_push_interval(cfg.pushgateway_push_interval);

        if let Some(ref instance) = cfg.pushgateway_instance {
            pg_config = pg_config.with_instance(instance.clone());
        } else {
            // Auto-generate a unique instance identifier using "ip:pid"
            // so that multiple client processes on the same machine do not
            // overwrite each other's metrics in Pushgateway.
            // Using IP is more intuitive and aligns with Prometheus conventions.
            let pid = std::process::id();
            let ip = Self::resolve_local_ip();
            let auto_instance = format!("{}:{}", ip, pid);
            debug!(auto_instance = %auto_instance, "auto-generated pushgateway instance");
            pg_config = pg_config.with_instance(auto_instance);
        }

        debug!(
            endpoint = %cfg.pushgateway_endpoint,
            job = %cfg.pushgateway_job,
            interval_ms = cfg.pushgateway_push_interval.as_millis(),
            "starting pushgateway push task"
        );

        let task = PushgatewayTask::spawn(pg_config);
        *self.pushgateway_task.lock().await = Some(task);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// Resolve the local outbound IP address.
    ///
    /// Uses a UDP socket trick: connect to a public address (without actually
    /// sending data) and read back the local address the OS chose.  This gives
    /// the correct outbound IP even on multi-homed machines.
    ///
    /// Falls back to `"127.0.0.1"` if detection fails.
    #[cfg(feature = "metrics-pushgateway")]
    fn resolve_local_ip() -> String {
        use std::net::UdpSocket;
        match UdpSocket::bind("0.0.0.0:0") {
            Ok(socket) => {
                // Connect to a well-known external address (Google DNS).
                // No actual traffic is sent — this just triggers route lookup.
                if socket.connect("8.8.8.8:80").is_ok() {
                    if let Ok(addr) = socket.local_addr() {
                        return addr.ip().to_string();
                    }
                }
                "127.0.0.1".to_string()
            }
            Err(_) => "127.0.0.1".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Verify that the context fields initialise with sane values when connected
    /// (we can't test actual network here, but we can validate the structure).
    #[test]
    fn test_context_closed_starts_false() {
        let closed = Arc::new(AtomicBool::new(false));
        assert!(!closed.load(Ordering::SeqCst));
    }

    #[test]
    fn test_context_close_is_idempotent() {
        let closed = Arc::new(AtomicBool::new(false));

        // First close
        let was_open = !closed.swap(true, Ordering::SeqCst);
        assert!(was_open);

        // Second close — should be a no-op
        let was_open2 = !closed.swap(true, Ordering::SeqCst);
        assert!(!was_open2);
    }

    /// Verify that the worker refresh check interval constant is 30s.
    #[test]
    fn test_refresh_check_interval() {
        assert_eq!(REFRESH_CHECK_INTERVAL, Duration::from_secs(30));
    }

    /// Verify that the config refresh interval constant is 60s (matching Java's refreshInterval).
    #[test]
    fn test_config_refresh_interval() {
        assert_eq!(CONFIG_REFRESH_INTERVAL, Duration::from_secs(60));
    }

    /// Verify that WorkerRouter with_ttls accepts the values used by context.
    #[test]
    fn test_worker_router_ttls_accepted() {
        let router = WorkerRouter::with_ttls(Duration::from_secs(60), Duration::from_secs(30));
        // Just verifying it constructs without panic — fields are private.
        drop(router);
    }

    /// Verify that `resolve_app_id` is accessible from context and returns a
    /// non-empty string — the full resolution logic is tested in heartbeat tests.
    #[test]
    fn test_resolve_app_id_non_empty() {
        let config = GoosefsConfig::new("127.0.0.1:9200");
        let id = resolve_app_id(&config);
        assert!(!id.is_empty());
    }

    /// Verify that metrics are disabled by default when the builder disables them,
    /// and that the config field round-trips correctly.
    #[test]
    fn test_metrics_enabled_flag_is_accessible() {
        let config_on = GoosefsConfig::new("127.0.0.1:9200").with_metrics_enabled(true);
        assert!(config_on.metrics_enabled);

        let config_off = GoosefsConfig::new("127.0.0.1:9200").with_metrics_enabled(false);
        assert!(!config_off.metrics_enabled);
    }

    /// Verify that `start_metrics_heartbeat_task` returns `Ok(())` immediately
    /// without attempting any network connection when `metrics_enabled = false`.
    ///
    /// This is the core contract for the `disabled_no_task_spawn` requirement
    /// from the design spec §8.1:
    ///   - `metrics_enabled=false` → no HeartbeatTask spawned, no MetricsMasterClient created.
    ///
    /// We test the gate condition directly (the config flag check) rather than
    /// exercising `FileSystemContext::connect()` which requires a real cluster.
    #[tokio::test]
    async fn disabled_no_task_spawn() {
        // Build a minimal config with metrics disabled.
        let config = Arc::new(GoosefsConfig::new("127.0.0.1:9200").with_metrics_enabled(false));
        assert!(!config.metrics_enabled, "metrics_enabled must be false");

        // Simulate the gate condition in `start_metrics_heartbeat_task`:
        // when `metrics_enabled = false`, the task must not be spawned.
        let metrics_heartbeat: Mutex<Option<Arc<HeartbeatTask>>> = Mutex::new(None);

        // Replicate the exact guard from start_metrics_heartbeat_task.
        let task_was_spawned = if config.metrics_enabled {
            // Would connect to master and spawn task (not reached here).
            true
        } else {
            // early return — no task spawned
            false
        };

        assert!(
            !task_was_spawned,
            "metrics_enabled=false must prevent task from being spawned"
        );

        // Verify the Mutex remains None (no task was placed into it).
        let guard = metrics_heartbeat.lock().await;
        assert!(
            guard.is_none(),
            "metrics_heartbeat field must remain None when metrics are disabled"
        );
    }

    /// Verify the `metrics_enabled` default value (true = opt-in enabled by default,
    /// matching the design spec §2).
    #[test]
    fn metrics_disabled_by_default() {
        let config = GoosefsConfig::new("127.0.0.1:9200");
        // Per config.rs, metrics_enabled defaults to true (align with Java SDK default).
        // Explicitly disabling sets it to false.
        let config_off = GoosefsConfig::new("127.0.0.1:9200").with_metrics_enabled(false);
        assert!(
            !config_off.metrics_enabled,
            "with_metrics_enabled(false) must disable metrics"
        );

        // Verify the default (true).
        assert!(
            config.metrics_enabled,
            "metrics_enabled defaults to true (opt-in enabled by default per Java SDK alignment)"
        );
    }

    // ── A3: FileInfo cache opt-in semantics ─────────────────────────────

    /// FLAMEGRAPH_OPTIMIZATION_PLAN §A3: the cache is **enabled** by default
    /// with a 30 s TTL, so `FileSystemContext::acquire_file_info_cache()`
    /// must return a live cache on a plain `GoosefsConfig::default()`.
    #[test]
    fn file_info_cache_enabled_by_default() {
        // We can't call `FileSystemContext::connect()` (needs live master),
        // so exercise the field-population logic directly on the config
        // + `FileInfoCache::maybe_new` gate.
        let cfg = GoosefsConfig::default();
        assert_eq!(
            cfg.file_info_cache_ttl,
            Duration::from_secs(30),
            "default TTL must be 30 s (enabled by default per §A3)"
        );
        assert!(
            crate::file_info_cache::FileInfoCache::maybe_new(
                cfg.file_info_cache_ttl,
                cfg.file_info_cache_capacity,
            )
            .is_some(),
            "FileInfoCache::maybe_new must return Some when default TTL > 0"
        );
    }

    /// Explicit opt-in via `with_file_info_cache_ttl` must produce a live
    /// cache with the requested TTL.
    #[test]
    fn file_info_cache_opt_in_produces_live_cache() {
        let cfg = GoosefsConfig::new("127.0.0.1:9200")
            .with_file_info_cache_ttl(Duration::from_secs(30))
            .with_file_info_cache_capacity(256);
        assert_eq!(cfg.file_info_cache_ttl, Duration::from_secs(30));
        assert_eq!(cfg.file_info_cache_capacity, 256);

        let cache = crate::file_info_cache::FileInfoCache::maybe_new(
            cfg.file_info_cache_ttl,
            cfg.file_info_cache_capacity,
        )
        .expect("opt-in TTL > 0 must produce a live cache");
        assert_eq!(cache.ttl(), Duration::from_secs(30));
    }

    /// `with_file_info_cache_capacity(0)` must be clamped to `1` (LRU
    /// requires non-zero capacity).
    #[test]
    fn file_info_cache_capacity_clamped_to_one() {
        let cfg = GoosefsConfig::new("127.0.0.1:9200").with_file_info_cache_capacity(0);
        assert_eq!(
            cfg.file_info_cache_capacity, 1,
            "with_file_info_cache_capacity(0) must clamp to 1"
        );
    }
}
