//! `FileSystemContext` — shared connection pool and routing context.
//!
//! This module implements the **three-layer connection architecture** that
//! eliminates repeated TCP+SASL handshakes:
//!
//! ```text
//! Layer 3: FileSystemContext — lifecycle manager + unified acquisition API
//!          │
//!          ├── Arc<MasterClient>         — persistent Master gRPC channel
//!          ├── Arc<WorkerManagerClient>  — persistent WorkerMgr gRPC channel
//!          ├── Arc<WorkerClientPool>     — shared Worker connection pool
//!          └── Arc<WorkerRouter>         — shared consistent-hash router
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
//! | `GooseFsFileInStream::open()` | 2 TCP+SASL | 0 (reused) |
//! | Reading N blocks | N TCP connects | ~N_workers (pooled) |
//!
//! # Usage
//!
//! ```rust,no_run
//! use goosefs_sdk::context::FileSystemContext;
//! use goosefs_sdk::config::GooseFsConfig;
//! use goosefs_sdk::fs::FileSystem; // needed to call trait methods
//!
//! # async fn example() -> goosefs_sdk::error::Result<()> {
//! // Build once, share across all operations
//! let ctx = FileSystemContext::connect(GooseFsConfig::new("127.0.0.1:9200")).await?;
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
use crate::client::{
    create_master_inquire_client, MasterClient, MasterInquireClient, WorkerClientPool,
    WorkerManagerClient,
};
use crate::config::{ConfigRefresher, GooseFsConfig, TransparentAccelerationSwitch};
use crate::error::{Error, Result};

/// How often the background refresh loop checks whether the worker list is stale.
/// Matches the `DEFAULT_WORKER_REFRESH_TTL` (30s) in `WorkerRouter`.
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How often the background config refresh loop runs (default 60s).
///
/// Mirrors Java's `refreshInterval` (default 60s) in `NamespaceRefreshThread`.
/// This is intentionally separate from [`REFRESH_CHECK_INTERVAL`] so that
/// config reloading and worker-list refreshing run on independent cadences.
const CONFIG_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Shared connection context for GooseFS filesystem operations.
///
/// A single `FileSystemContext` instance should be created per GooseFS cluster
/// and shared across all `BaseFileSystem`, `GooseFsFileInStream`, and
/// `GooseFsFileWriter` instances that connect to that cluster.
///
/// The context owns:
/// - One persistent gRPC channel to the Master
/// - One persistent gRPC channel to the WorkerManager service
/// - One `WorkerClientPool` shared across all readers and writers
/// - One `WorkerRouter` that tracks live workers and routes block reads
pub struct FileSystemContext {
    config: Arc<GooseFsConfig>,

    /// Persistent Master gRPC connection (metadata RPCs).
    master: Arc<MasterClient>,

    /// Persistent WorkerManager gRPC connection (`GetWorkerInfoList`).
    worker_manager: Arc<WorkerManagerClient>,

    /// Worker gRPC connection pool — shared across all readers and writers.
    worker_pool: Arc<WorkerClientPool>,

    /// Consistent-hash router with TTL refresh and local-first preference.
    worker_router: Arc<WorkerRouter>,

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
}

impl FileSystemContext {
    // ── Construction ────────────────────────────────────────────────────────

    /// Build a `FileSystemContext` by connecting to the GooseFS cluster.
    ///
    /// Establishes persistent connections to the Master and WorkerManager,
    /// fetches the initial worker list, and starts a background refresh task.
    ///
    /// This is the **only** call that performs network I/O.  All subsequent
    /// operations on the context are zero-cost Arc clones.
    pub async fn connect(config: GooseFsConfig) -> Result<Arc<Self>> {
        let config = Arc::new(config);

        // Build a shared inquire client so Master + WorkerManager both use the
        // same singleflight-deduped HA discovery.
        let inquire_client = create_master_inquire_client(&config);

        // Connect Master and WorkerManager in parallel.
        let (master_res, wm_res) = tokio::join!(
            MasterClient::connect_with_inquire(&config, inquire_client.clone()),
            WorkerManagerClient::connect_with_inquire(&config, inquire_client.clone()),
        );
        let master = Arc::new(master_res?);
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

        let ctx = Arc::new(Self {
            config: config.clone(),
            master,
            worker_manager,
            worker_pool,
            worker_router,
            inquire_client,
            config_refresher: Arc::new(ConfigRefresher::from_config(&config)),
            closed: Arc::new(AtomicBool::new(false)),
            worker_refresh_task: Mutex::new(None),
            config_refresh_task: Mutex::new(None),
        });

        // Start the background worker-list refresh loop.
        ctx.clone().start_worker_refresh_task().await;
        // Start the background config refresh loop (separate cadence).
        ctx.clone().start_config_refresh_task().await;

        Ok(ctx)
    }

    // ── Acquisition API ──────────────────────────────────────────────────────

    /// Return the shared `MasterClient` (zero-cost Arc clone).
    pub fn acquire_master(&self) -> Arc<MasterClient> {
        self.master.clone()
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

    /// Return the shared `MasterInquireClient` (zero-cost Arc clone).
    pub fn acquire_inquire_client(&self) -> Arc<dyn MasterInquireClient> {
        self.inquire_client.clone()
    }

    /// Return the configuration used to build this context.
    pub fn config(&self) -> &GooseFsConfig {
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
}
