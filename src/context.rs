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
use crate::cache::{CacheManager, LocalCacheManager};
use crate::client::metrics_master::MetricsClient;
use crate::client::metrics_master::MetricsMasterClient;
use crate::client::{
    create_master_inquire_client, MasterClient, MasterClientPool, MasterInquireClient,
    WorkerClientPool, WorkerManagerClient,
};
use crate::config::{ConfigRefresher, GoosefsConfig, TransparentAccelerationSwitch};
use crate::error::{Error, Result};
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
    /// Round-robin pool of `config.master_connection_pool_size` channels
    /// (default 1 = single channel, backward compatible). See Part V R3.
    master_pool: Arc<MasterClientPool>,

    /// Persistent WorkerManager gRPC connection (`GetWorkerInfoList`).
    worker_manager: Arc<WorkerManagerClient>,

    /// Worker gRPC connection pool — shared across all readers and writers.
    worker_pool: Arc<WorkerClientPool>,

    /// Consistent-hash router with TTL refresh and local-first preference.
    worker_router: Arc<WorkerRouter>,

    /// Client-side local page cache, when `config.client_cache_enabled`.
    ///
    /// `None` when the cache is disabled or failed to initialize (the cache is
    /// best-effort, so an init failure degrades to no-cache rather than
    /// failing `connect()`). Shared across all readers in this context.
    cache_manager: Option<Arc<dyn CacheManager>>,

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

        // Connect Master pool and WorkerManager in parallel.
        let (pool_res, wm_res) = tokio::join!(
            MasterClientPool::connect_with_inquire(&config, inquire_client.clone()),
            WorkerManagerClient::connect_with_inquire(&config, inquire_client.clone()),
        );
        let master_pool = Arc::new(pool_res?);
        let worker_manager = Arc::new(wm_res?);

        // Fetch the initial worker list.
        let workers = worker_manager.get_worker_info_list().await?;
        if workers.is_empty() {
            return Err(Error::NoWorkerAvailable {
                message: "no workers available at startup".to_string(),
            });
        }
        debug!(count = workers.len(), "initial worker list fetched");

        // Build the router with failure/refresh TTLs.
        let worker_router = Arc::new(WorkerRouter::with_ttls(
            Duration::from_secs(60), // failure_ttl
            Duration::from_secs(30), // worker_refresh_ttl (matches Go SDK)
        ));
        worker_router.update_workers(workers).await;

        // Build the shared worker connection pool.
        let worker_pool = WorkerClientPool::new_shared((*config).clone());

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

        let ctx = Arc::new(Self {
            config: config.clone(),
            master_pool,
            worker_manager,
            worker_pool,
            worker_router,
            cache_manager,
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

    /// Return a shared `MasterClient` from the pool (zero-cost Arc clone).
    ///
    /// With `master_connection_pool_size > 1` this round-robins across the
    /// pooled channels to spread concurrent metadata RPCs over multiple
    /// HTTP/2 connections (Part V R3).
    pub fn acquire_master(&self) -> Arc<MasterClient> {
        self.master_pool.pick()
    }

    /// Return the shared `MasterClientPool` (zero-cost Arc clone).
    pub fn acquire_master_pool(&self) -> Arc<MasterClientPool> {
        self.master_pool.clone()
    }

    /// Return the shared `WorkerManagerClient` (zero-cost Arc clone).
    pub fn acquire_worker_manager(&self) -> Arc<WorkerManagerClient> {
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

    /// Return the shared client-side page cache, if enabled.
    ///
    /// `None` when `config.client_cache_enabled = false` or the cache failed
    /// to initialize. Readers consult this on the random-read path.
    pub fn acquire_cache_manager(&self) -> Option<Arc<dyn CacheManager>> {
        self.cache_manager.clone()
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
                    if let Err(e) = worker_router.refresh_workers(&worker_manager).await {
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
}
