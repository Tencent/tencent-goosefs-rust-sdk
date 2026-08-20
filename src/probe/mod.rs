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

//! Client-side GooseFS probe trace.
//!
//! When enabled, every Master/Worker RPC carries `probe-enabled: true` so a
//! Java GooseFS server records sub-phase timings and returns them in the
//! `probe-timing-bin` trailer. This module collects those trailers, adds
//! client-local phases (connect / pool), and prints a tree report matching
//! `goosefs fs probe` / `copyFromLocal --probe`.
//!
//! Enable via environment or `goosefs-site.properties` — OpenDAL / Lance /
//! DuckDB pick this up with no code changes:
//!
//! ```text
//! export GOOSEFS_PROBE_ENABLED=true
//! export GOOSEFS_PROBE_OUTPUT=./probe.log
//! ```
//!
//! ```properties
//! goosefs.user.client.probe.enabled=true
//! goosefs.user.client.probe.output=/tmp/probe.log
//! ```
//!
//! Disabled (the default) is a single `AtomicBool` load on the interceptor
//! hot path and no trailer parsing.

pub mod collector;
pub mod phase;
pub mod report;

use std::collections::HashMap;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use tonic::metadata::MetadataMap;

use crate::config::{parse_bool_loose, GoosefsConfig, ENV_PROBE_ENABLED, ENV_PROBE_OUTPUT};

use collector::{timing_from_trailers, ProbeCollector};
use report::{BlockProbeResult, ProbeKind, ProbeResult};

pub use collector::{
    ProbeCollector as ProbeTimingCollector, PROBE_ENABLED_HEADER as PROBE_ENABLED,
};
pub use report::{format_aggregate, ProbeResult as ProbeReport};

tokio::task_local! {
    static CURRENT: Arc<ProbeCollector>;
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static INIT: AtomicBool = AtomicBool::new(false);
static OUTPUT: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
static FILE_SEQ: AtomicU64 = AtomicU64::new(0);
static AGGREGATE: Mutex<Vec<ProbeResult>> = Mutex::new(Vec::new());

fn output_slot() -> &'static Mutex<Option<PathBuf>> {
    OUTPUT.get_or_init(|| Mutex::new(None))
}

fn env_flag() -> bool {
    std::env::var(ENV_PROBE_ENABLED)
        .ok()
        .and_then(|v| parse_bool_loose(&v))
        .unwrap_or(false)
}

fn env_output() -> Option<PathBuf> {
    std::env::var(ENV_PROBE_OUTPUT)
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}

/// Ensure the process-level flag is initialized from the environment.
///
/// Called from the interceptor before `FileSystemContext::connect` so a
/// `GOOSEFS_PROBE_ENABLED=true` process activates header injection even when
/// the integrator constructed a bare `GoosefsConfig::new(addr)`.
fn ensure_init() {
    if INIT.load(Ordering::Acquire) {
        return;
    }
    let on = env_flag();
    ENABLED.store(on, Ordering::Relaxed);
    if let Some(path) = env_output() {
        *output_slot().lock().unwrap_or_else(|e| e.into_inner()) = Some(path);
    }
    INIT.store(true, Ordering::Release);
}

/// Apply probe flags from a constructed config, OR-ing with env and the
/// properties file so OpenDAL's partial config cannot mask an env/file enable.
pub fn apply_config(config: &GoosefsConfig) {
    let file_cfg = GoosefsConfig::from_properties_auto().unwrap_or_default();
    let on = config.probe_enabled || env_flag() || file_cfg.probe_enabled;
    ENABLED.store(on, Ordering::Relaxed);
    let path = config
        .probe_output
        .clone()
        .or_else(|| env_output().map(|p| p.to_string_lossy().into_owned()))
        .or(file_cfg.probe_output);
    if let Some(path) = path {
        *output_slot().lock().unwrap_or_else(|e| e.into_inner()) = Some(PathBuf::from(path));
    }
    INIT.store(true, Ordering::Release);
}

/// Whether probe mode is active. Hot path: one `Relaxed` atomic load after init.
#[inline]
pub fn is_enabled() -> bool {
    ensure_init();
    ENABLED.load(Ordering::Relaxed)
}

/// Current output path, if any. `None` means print reports to stderr.
pub fn output_path() -> Option<PathBuf> {
    output_slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// gRPC method names for trailer classification.
pub use collector::{
    METHOD_COMPLETE_FILE as RPC_COMPLETE_FILE, METHOD_CREATE_DIRECTORY as RPC_CREATE_DIRECTORY,
    METHOD_CREATE_FILE as RPC_CREATE_FILE, METHOD_GET_STATUS as RPC_GET_STATUS,
    METHOD_READ_BLOCK as RPC_READ_BLOCK, METHOD_REMOVE as RPC_REMOVE, METHOD_RENAME as RPC_RENAME,
    METHOD_WRITE_BLOCK as RPC_WRITE_BLOCK, PROBE_TIMING_TRAILER,
};

/// Start an `Instant` only when probe is on, so the disabled path is one load.
#[inline]
pub fn rpc_start() -> Option<Instant> {
    if is_enabled() {
        Some(Instant::now())
    } else {
        None
    }
}

/// Record a unary RPC from trailing metadata. No-op when probe is off or `started` is `None`.
///
/// Prefer attaching timings to the active [`ProbeSession`] collector. When there
/// is no session (OpenDAL put prep / finalize: GetStatus, Remove, CreateDirectory,
/// Rename), still emit a standalone report for methods in
/// [`standalone_emit_method`] so Master server time is not lost.
pub fn record_unary(method: &'static str, started: Option<Instant>, trailers: &MetadataMap) {
    let Some(started) = started else {
        return;
    };
    let timing = timing_from_trailers(method, started, trailers);
    if let Some(collector) = current_collector() {
        collector.record_rpc(timing);
        return;
    }
    if standalone_emit_method(method) {
        emit_standalone_rpc(&timing);
    }
}

/// OpenDAL-style put metadata RPCs that often run outside a write session.
fn standalone_emit_method(method: &str) -> bool {
    matches!(
        method,
        collector::METHOD_GET_STATUS
            | collector::METHOD_REMOVE
            | collector::METHOD_CREATE_DIRECTORY
            | collector::METHOD_RENAME
    )
}

fn emit_standalone_rpc(timing: &collector::ProbeRpcTiming) {
    let n = FILE_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
    let short = method_leaf_name(&timing.method_name);
    let header = format!("\n########## Probe RPC #{n}: {short} ##########\n");
    let body = report::format_standalone_rpc(timing);
    let text = format!("{header}{body}");
    if let Some(path) = output_path() {
        if let Err(e) = append_file(&path, &text) {
            tracing::warn!(path = %path.display(), error = %e, "failed to write probe report");
            eprint!("{text}");
        }
    } else {
        eprint!("{text}");
    }
}

fn method_leaf_name(method: &str) -> &str {
    method.rsplit('/').next().unwrap_or(method)
}

/// Record a finished streaming RPC from trailers. No-op when probe is off.
pub fn record_stream(method: &'static str, started: Instant, trailers: &MetadataMap) {
    let Some(collector) = current_collector() else {
        return;
    };
    collector.record_rpc(timing_from_trailers(method, started, trailers));
}

/// Snapshot the current session collector so a spawned task (WriteBlock) can
/// record trailers after `task_local` no longer applies.
pub fn current_collector() -> Option<Arc<ProbeCollector>> {
    if !is_enabled() {
        return None;
    }
    CURRENT.try_with(Clone::clone).ok()
}

/// Run `fut` with `collector` installed as the task-local session.
pub async fn scoped<T>(collector: Option<&Arc<ProbeCollector>>, fut: impl Future<Output = T>) -> T {
    match collector {
        Some(c) => CURRENT.scope(c.clone(), fut).await,
        None => fut.await,
    }
}

/// RAII client-local phase. Disabled probe returns a no-op with no `Instant`.
pub struct PhaseScope {
    name: Option<&'static str>,
    start: Option<Instant>,
}

/// Begin a named client-local phase (pool acquire, connect, …).
#[inline]
pub fn phase(name: &'static str) -> PhaseScope {
    if !is_enabled() {
        return PhaseScope {
            name: None,
            start: None,
        };
    }
    PhaseScope {
        name: Some(name),
        start: Some(Instant::now()),
    }
}

impl Drop for PhaseScope {
    fn drop(&mut self) {
        let (Some(name), Some(start)) = (self.name, self.start) else {
            return;
        };
        let us = start.elapsed().as_micros() as i64;
        if let Some(c) = current_collector() {
            c.record_local(name, us);
        }
    }
}

/// Per-file probe session owned by `GoosefsFileWriter` / `GoosefsFileReader`.
pub struct ProbeSession {
    collector: Arc<ProbeCollector>,
    kind: ProbeKind,
    path: String,
    master_address: String,
    block_size: u64,
    write_type: Option<String>,
    read_type: Option<String>,
    total_start: Instant,
    create_or_open_us: AtomicU64,
    /// Client-local phases that finished during CreateFile / OpenFile.
    /// Drained from the collector at [`record_create_or_open`] so later
    /// worker connect / stream-open times are not dumped under this RPC.
    create_or_open_local: Mutex<HashMap<String, i64>>,
    /// Write-path locals drained at `close()` so CompleteFile/Close do not
    /// steal worker_connect / open_stream from Data Write. Leftover after
    /// per-block captures (e.g. THROUGH UFS-only).
    data_local: Mutex<HashMap<String, i64>>,
    complete_file_local: Mutex<HashMap<String, i64>>,
    close_local: Mutex<HashMap<String, i64>>,
    data_us: AtomicU64,
    complete_us: AtomicU64,
    close_us: AtomicU64,
    bytes: AtomicU64,
    emitted: AtomicBool,
}

impl ProbeSession {
    fn new(
        kind: ProbeKind,
        path: impl Into<String>,
        master_address: impl Into<String>,
        block_size: u64,
        write_type: Option<String>,
        read_type: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            collector: Arc::new(ProbeCollector::new()),
            kind,
            path: path.into(),
            master_address: master_address.into(),
            block_size,
            write_type,
            read_type,
            total_start: Instant::now(),
            create_or_open_us: AtomicU64::new(0),
            create_or_open_local: Mutex::new(HashMap::new()),
            data_local: Mutex::new(HashMap::new()),
            complete_file_local: Mutex::new(HashMap::new()),
            close_local: Mutex::new(HashMap::new()),
            data_us: AtomicU64::new(0),
            complete_us: AtomicU64::new(0),
            close_us: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            emitted: AtomicBool::new(false),
        })
    }

    /// Start a write session when probe is enabled.
    pub fn begin_write(
        path: impl Into<String>,
        master_address: impl Into<String>,
        block_size: u64,
        write_type: Option<String>,
    ) -> Option<Arc<Self>> {
        Self::begin_write_if(is_enabled(), path, master_address, block_size, write_type)
    }

    /// Like [`begin_write`] but uses an explicit enable flag (e.g. this
    /// `FileSystemContext`'s `probe_enabled`, not only the process atomic).
    pub fn begin_write_if(
        enabled: bool,
        path: impl Into<String>,
        master_address: impl Into<String>,
        block_size: u64,
        write_type: Option<String>,
    ) -> Option<Arc<Self>> {
        if !enabled {
            return None;
        }
        ENABLED.store(true, Ordering::Relaxed);
        INIT.store(true, Ordering::Release);
        Some(Self::new(
            ProbeKind::Write,
            path,
            master_address,
            block_size,
            write_type,
            None,
        ))
    }

    /// Start a read session when probe is enabled.
    pub fn begin_read(
        path: impl Into<String>,
        master_address: impl Into<String>,
        block_size: u64,
        read_type: Option<String>,
    ) -> Option<Arc<Self>> {
        Self::begin_read_if(is_enabled(), path, master_address, block_size, read_type)
    }

    /// Like [`begin_read`] with an explicit enable flag.
    pub fn begin_read_if(
        enabled: bool,
        path: impl Into<String>,
        master_address: impl Into<String>,
        block_size: u64,
        read_type: Option<String>,
    ) -> Option<Arc<Self>> {
        if !enabled {
            return None;
        }
        ENABLED.store(true, Ordering::Relaxed);
        INIT.store(true, Ordering::Release);
        Some(Self::new(
            ProbeKind::Read,
            path,
            master_address,
            block_size,
            None,
            read_type,
        ))
    }

    pub fn collector(&self) -> &Arc<ProbeCollector> {
        &self.collector
    }

    pub fn record_create_or_open(&self, us: u64) {
        self.create_or_open_us.store(us, Ordering::Relaxed);
        let taken = self.collector.take_local();
        *self
            .create_or_open_local
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = taken;
    }

    pub fn add_data(&self, us: u64) {
        self.data_us.fetch_add(us, Ordering::Relaxed);
    }

    pub fn add_bytes(&self, n: u64) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_complete(&self, us: u64) {
        self.complete_us.store(us, Ordering::Relaxed);
        let taken = self.collector.take_local();
        *self
            .complete_file_local
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = taken;
    }

    /// Start a cache WriteBlock so subsequent `chunk_copy` / `flush_ack`
    /// land on this block instead of Block #0.
    pub fn begin_block(&self) {
        self.collector.begin_write_block();
    }

    /// Drain locals accumulated during Data Write so they stay on the block
    /// tree instead of CompleteFile / Close.
    pub fn capture_data_local(&self) {
        let taken = self.collector.take_local();
        *self.data_local.lock().unwrap_or_else(|e| e.into_inner()) = taken;
    }

    /// Stop attributing client-local phases to the WriteBlock that just closed.
    pub fn capture_block_local(&self) {
        self.collector.end_write_block();
    }

    pub fn record_close(&self, us: u64) {
        self.close_us.store(us, Ordering::Relaxed);
    }

    /// Client-local phases that belong to the Close section (not a WriteBlock).
    pub fn add_close_local(&self, phase: &str, duration_us: i64) {
        if duration_us <= 0 {
            return;
        }
        let mut map = self.close_local.lock().unwrap_or_else(|e| e.into_inner());
        *map.entry(phase.to_string()).or_insert(0) += duration_us;
    }

    pub fn complete_micros(&self) -> u64 {
        self.complete_us.load(Ordering::Relaxed)
    }

    /// Build and emit the report. Idempotent.
    ///
    /// A vacuous result (no bytes, no phase timings, no RPC trailers) is
    /// discarded **without** setting `emitted`, so a later `close()` /
    /// `read_all()` can still publish the real report.
    pub fn finish(&self, file_size: u64, block_count: usize) -> Option<ProbeResult> {
        let result = self.build_result(
            file_size.max(self.bytes.load(Ordering::Relaxed)),
            block_count,
        );
        if result.is_vacuous() {
            return None;
        }
        if self
            .emitted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        emit_report(&result);
        Some(result)
    }

    fn build_result(&self, file_size: u64, block_count: usize) -> ProbeResult {
        // `latest_matching` already lowercases the method name.
        let create_file_timing = self.collector.latest_matching(|n| n.contains("createfile"));
        let complete_file_timing = self
            .collector
            .latest_matching(|n| n.contains("completefile"));
        let open_file_timing = self.collector.latest_matching(|n| {
            n.contains("getstatus") || n.contains("getfileinfo") || n.contains("getfilestatus")
        });

        let leftover_local = match self.kind {
            ProbeKind::Write => self
                .data_local
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            ProbeKind::Read => self.collector.snapshot_local(),
        };
        let mut per_block_local = match self.kind {
            ProbeKind::Write => self.collector.take_write_block_locals(),
            ProbeKind::Read => Vec::new(),
        };
        if !leftover_local.is_empty() {
            if let Some(last) = per_block_local.last_mut() {
                merge_local_maps(last, leftover_local);
            } else {
                per_block_local.push(leftover_local);
            }
        }
        let blocks = match self.kind {
            ProbeKind::Write => {
                let (primary, ufs) = self.collector.write_block_timings();
                let n = block_count.max(primary.len()).max(1);
                (0..n)
                    .map(|i| {
                        let mut b = BlockProbeResult {
                            index: i,
                            ..Default::default()
                        };
                        if let Some(t) = primary.get(i) {
                            b.client_total_us = t.client_total_us;
                            b.network_us = t.network_us;
                            b.worker_processing_us = t.server_total_us;
                            b.worker_sub_timings = t.sub_timings_us.clone();
                        }
                        if let Some(t) = ufs.get(i) {
                            b.ufs_network_us = t.network_us;
                            b.ufs_worker_processing_us = t.server_total_us;
                            b.ufs_worker_sub_timings = t.sub_timings_us.clone();
                        }
                        if let Some(local) = per_block_local.get(i) {
                            b.client_local = local.clone();
                        }
                        b
                    })
                    .collect()
            }
            ProbeKind::Read => {
                let reads = self.collector.read_block_timings();
                let n = block_count.max(reads.len()).max(1);
                (0..n)
                    .map(|i| {
                        let mut b = BlockProbeResult {
                            index: i,
                            ..Default::default()
                        };
                        if let Some(t) = reads.get(i) {
                            b.client_total_us = t.client_total_us;
                            b.network_us = t.network_us;
                            b.worker_processing_us = t.server_total_us;
                            b.worker_sub_timings = t.sub_timings_us.clone();
                        }
                        if i == 0 {
                            if let Some(local) = per_block_local.first() {
                                b.client_local = local.clone();
                            }
                        }
                        b
                    })
                    .collect()
            }
        };

        let create_or_open = self.create_or_open_us.load(Ordering::Relaxed) as i64;
        let create_or_open_local = self
            .create_or_open_local
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        ProbeResult {
            kind: self.kind,
            path: self.path.clone(),
            file_size,
            block_size: self.block_size,
            block_count: block_count.max(1),
            write_type: self.write_type.clone(),
            read_type: self.read_type.clone(),
            master_address: self.master_address.clone(),
            total_time_us: self.total_start.elapsed().as_micros() as i64,
            create_file_client_us: if self.kind == ProbeKind::Write {
                create_or_open
            } else {
                0
            },
            open_file_client_us: if self.kind == ProbeKind::Read {
                create_or_open
            } else {
                0
            },
            data_phase_us: self.data_us.load(Ordering::Relaxed) as i64,
            complete_file_client_us: self.complete_us.load(Ordering::Relaxed) as i64,
            close_us: self.close_us.load(Ordering::Relaxed) as i64,
            create_file_timing,
            open_file_timing,
            complete_file_timing,
            create_file_local: if self.kind == ProbeKind::Write {
                create_or_open_local.clone()
            } else {
                Default::default()
            },
            open_file_local: if self.kind == ProbeKind::Read {
                create_or_open_local
            } else {
                Default::default()
            },
            complete_file_local: self
                .complete_file_local
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            close_local: self
                .close_local
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            blocks,
        }
    }
}

fn merge_local_maps(dst: &mut HashMap<String, i64>, src: HashMap<String, i64>) {
    for (k, v) in src {
        if v > 0 {
            *dst.entry(k).or_insert(0) += v;
        }
    }
}

fn emit_report(result: &ProbeResult) {
    let header = {
        let n = FILE_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
        format!("\n########## Probe File #{n}: {} ##########\n", result.path)
    };
    let body = result.to_string();
    let text = format!("{header}{body}");

    if let Ok(mut agg) = AGGREGATE.lock() {
        agg.push(result.clone());
    }

    if let Some(path) = output_path() {
        if let Err(e) = append_file(&path, &text) {
            tracing::warn!(path = %path.display(), error = %e, "failed to write probe report");
            eprint!("{text}");
        }
    } else {
        eprint!("{text}");
    }
}

fn append_file(path: &Path, text: &str) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    f.write_all(text.as_bytes())?;
    Ok(())
}

/// Flush a multi-file aggregate summary (call at process shutdown if useful).
pub fn emit_aggregate_summary() {
    let reports = AGGREGATE.lock().unwrap_or_else(|e| e.into_inner());
    if reports.len() < 2 {
        return;
    }
    let text = format_aggregate(&reports);
    if let Some(path) = output_path() {
        let _ = append_file(&path, &text);
    } else {
        eprint!("{text}");
    }
}

/// Helper used by the interceptor: ASCII `true` value for `probe-enabled`.
pub fn probe_enabled_header_value() -> tonic::metadata::MetadataValue<tonic::metadata::Ascii> {
    tonic::metadata::MetadataValue::from_static("true")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_phase_is_noop() {
        // Default in unit tests: env unset → disabled (unless the parent
        // process exported GOOSEFS_PROBE_ENABLED).
        if env_flag() {
            return;
        }
        let scope = phase(phase::client::POOL_ACQUIRE_US);
        drop(scope);
        assert!(current_collector().is_none());
    }

    #[test]
    fn header_constant_matches_java() {
        assert_eq!(PROBE_ENABLED, "probe-enabled");
        assert_eq!(collector::PROBE_TIMING_TRAILER, "probe-timing-bin");
        assert!(collector::METHOD_RENAME.ends_with("/Rename"));
        assert!(collector::METHOD_REMOVE.ends_with("/Remove"));
        assert!(collector::METHOD_CREATE_DIRECTORY.ends_with("/CreateDirectory"));
        assert!(standalone_emit_method(collector::METHOD_GET_STATUS));
        assert!(standalone_emit_method(collector::METHOD_REMOVE));
        assert!(standalone_emit_method(collector::METHOD_CREATE_DIRECTORY));
        assert!(standalone_emit_method(collector::METHOD_RENAME));
        assert!(!standalone_emit_method(collector::METHOD_CREATE_FILE));
    }

    #[test]
    fn vacuous_finish_does_not_consume_emit_slot() {
        let session = ProbeSession::new(
            ProbeKind::Write,
            "/vacuous",
            "127.0.0.1:9200",
            64 * 1024 * 1024,
            Some("CACHE_THROUGH".into()),
            None,
        );
        assert!(session.finish(0, 1).is_none());
        session.record_create_or_open(12_256);
        session.add_data(3_387);
        let result = session
            .finish(3791, 1)
            .expect("real finish after vacuous skip");
        assert_eq!(result.file_size, 3791);
        assert_eq!(result.create_file_client_us, 12_256);
        assert_eq!(result.data_phase_us, 3_387);
    }

    #[test]
    fn local_phases_stay_on_the_section_that_captured_them() {
        let session = ProbeSession::new(
            ProbeKind::Write,
            "/probe.dat",
            "127.0.0.1:9200",
            64 * 1024 * 1024,
            Some("CACHE_THROUGH".into()),
            None,
        );
        session
            .collector()
            .record_local(phase::client::POOL_ACQUIRE_US, 50);
        session.record_create_or_open(1_620);
        session
            .collector()
            .record_local(phase::client::WORKER_CONNECT_US, 1_930);
        session
            .collector()
            .record_local(phase::client::OPEN_STREAM_US, 36);
        session.add_data(4_150);
        session.capture_data_local();
        let result = session.finish(32, 1).expect("report");
        assert_eq!(
            result.create_file_local.get(phase::client::POOL_ACQUIRE_US),
            Some(&50)
        );
        assert!(
            !result
                .create_file_local
                .contains_key(phase::client::WORKER_CONNECT_US),
            "worker_connect after CreateFile must not appear under CreateFile"
        );
        assert!(!result
            .create_file_local
            .contains_key(phase::client::OPEN_STREAM_US));
        assert_eq!(
            result.blocks[0]
                .client_local
                .get(phase::client::WORKER_CONNECT_US),
            Some(&1_930)
        );
        assert_eq!(
            result.blocks[0]
                .client_local
                .get(phase::client::OPEN_STREAM_US),
            Some(&36)
        );
        assert!(!result.blocks[0]
            .client_local
            .contains_key(phase::client::POOL_ACQUIRE_US));
    }

    #[test]
    fn write_block_locals_are_captured_per_block() {
        let session = ProbeSession::new(
            ProbeKind::Write,
            "/probe.dat",
            "127.0.0.1:9200",
            64 * 1024 * 1024,
            Some("ASYNC_THROUGH".into()),
            None,
        );
        session.begin_block();
        session
            .collector()
            .record_local(phase::client::CHUNK_COPY_US, 10_000);
        session.capture_block_local();
        session.begin_block();
        session
            .collector()
            .record_local(phase::client::CHUNK_COPY_US, 20_000);
        session
            .collector()
            .record_local(phase::client::FLUSH_ACK_US, 5_000);
        session.capture_block_local();
        session.add_data(1);
        let result = session.finish(1, 2).expect("report");
        assert_eq!(
            result.blocks[0]
                .client_local
                .get(phase::client::CHUNK_COPY_US),
            Some(&10_000)
        );
        assert!(!result.blocks[0]
            .client_local
            .contains_key(phase::client::FLUSH_ACK_US));
        assert_eq!(
            result.blocks[1]
                .client_local
                .get(phase::client::CHUNK_COPY_US),
            Some(&20_000)
        );
        assert_eq!(
            result.blocks[1]
                .client_local
                .get(phase::client::FLUSH_ACK_US),
            Some(&5_000)
        );
    }

    #[test]
    fn close_locals_are_kept_off_the_block_tree() {
        let session = ProbeSession::new(
            ProbeKind::Write,
            "/probe.dat",
            "127.0.0.1:9200",
            64 * 1024 * 1024,
            Some("ASYNC_THROUGH".into()),
            None,
        );
        session.add_close_local(phase::client::LAST_BLOCK_CLOSE_US, 9_200);
        session.add_close_local(phase::client::ASYNC_PERSIST_US, 1_400);
        session.record_close(10_940);
        session.add_data(1);
        let result = session.finish(1, 1).expect("report");
        assert_eq!(
            result.close_local.get(phase::client::LAST_BLOCK_CLOSE_US),
            Some(&9_200)
        );
        assert_eq!(
            result.close_local.get(phase::client::ASYNC_PERSIST_US),
            Some(&1_400)
        );
        assert!(!result.blocks[0]
            .client_local
            .contains_key(phase::client::LAST_BLOCK_CLOSE_US));
    }
}
