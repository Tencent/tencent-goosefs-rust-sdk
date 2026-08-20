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

//! Tree-shaped probe report, aligned with Java `ProbeReportFormatter`.

use std::collections::HashMap;
use std::fmt::{self, Write as _};

use super::collector::ProbeRpcTiming;
use super::phase::{self, DisplayType};

const SEPARATOR: &str = "==============================================================";

/// Kind of probe session used to pick the report header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeKind {
    Write,
    Read,
}

/// One block's worker timings (cache stream + optional UFS stream).
#[derive(Debug, Clone, Default)]
pub struct BlockProbeResult {
    pub index: usize,
    pub client_total_us: i64,
    pub network_us: i64,
    pub worker_processing_us: i64,
    pub worker_sub_timings: HashMap<String, i64>,
    pub client_local: HashMap<String, i64>,
    pub ufs_network_us: i64,
    pub ufs_worker_processing_us: i64,
    pub ufs_worker_sub_timings: HashMap<String, i64>,
}

impl BlockProbeResult {
    fn has_ufs(&self) -> bool {
        self.ufs_worker_processing_us > 0 || !self.ufs_worker_sub_timings.is_empty()
    }
}

/// Structured probe result for a single file, matching Java `ProbeResult`.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub kind: ProbeKind,
    pub path: String,
    pub file_size: u64,
    pub block_size: u64,
    pub block_count: usize,
    pub write_type: Option<String>,
    pub read_type: Option<String>,
    pub master_address: String,
    pub total_time_us: i64,
    pub create_file_client_us: i64,
    pub open_file_client_us: i64,
    pub data_phase_us: i64,
    pub complete_file_client_us: i64,
    pub close_us: i64,
    pub create_file_timing: Option<ProbeRpcTiming>,
    pub open_file_timing: Option<ProbeRpcTiming>,
    pub complete_file_timing: Option<ProbeRpcTiming>,
    pub create_file_local: HashMap<String, i64>,
    pub open_file_local: HashMap<String, i64>,
    pub complete_file_local: HashMap<String, i64>,
    pub close_local: HashMap<String, i64>,
    pub blocks: Vec<BlockProbeResult>,
}

impl ProbeResult {
    /// True when this result has no recorded IO or RPC timings.
    ///
    /// Used so a premature `finish(0, 1)` (e.g. `ProbeSession` Drop) cannot
    /// occupy the emit slot and hide the real `close()` / `read_all()` report.
    pub(crate) fn is_vacuous(&self) -> bool {
        self.file_size == 0
            && self.create_file_client_us == 0
            && self.open_file_client_us == 0
            && self.data_phase_us == 0
            && self.complete_file_client_us == 0
            && self.close_us == 0
            && self.create_file_timing.is_none()
            && self.open_file_timing.is_none()
            && self.complete_file_timing.is_none()
            && self
                .blocks
                .iter()
                .all(|b| b.client_total_us == 0 && b.worker_processing_us == 0)
    }

    pub fn format_write(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out);
        let _ = writeln!(out, "{SEPARATOR}");
        let _ = writeln!(out, "           GooseFS Write Probe Report");
        let _ = writeln!(out, "{SEPARATOR}");
        self.write_basic_info(&mut out);
        let _ = writeln!(
            out,
            "  Write Type:      {}",
            self.write_type.as_deref().unwrap_or("UNKNOWN")
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "--- [1] CreateFile (Master create file metadata) ---");
        print_rpc_timing(
            &mut out,
            self.create_file_timing.as_ref(),
            self.create_file_client_us,
            &self.create_file_local,
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "--- [2] Data Write (Worker data write) ---");
        let _ = writeln!(
            out,
            "  Total Duration:  {}",
            format_duration_us(self.data_phase_us)
        );
        print_block_results(&mut out, &self.blocks);
        let _ = writeln!(out);

        let _ = writeln!(out, "--- [3] CompleteFile (Master complete file) ---");
        print_rpc_timing(
            &mut out,
            self.complete_file_timing.as_ref(),
            self.complete_file_client_us,
            &self.complete_file_local,
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "--- [4] Close ---");
        let named = top_level_local_us(&self.close_local);
        let duration = self.close_us.max(named);
        let _ = writeln!(out, "  Duration:        {}", format_duration_us(duration));
        if !self.close_local.is_empty() {
            print_named_locals(
                &mut out,
                &self.close_local,
                duration,
                "    ├── ",
                "    └── ",
                "    │     ├── ",
                "          └── ",
            );
        }
        let _ = writeln!(out);

        print_summary(&mut out, self);
        out
    }

    pub fn format_read(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out);
        let _ = writeln!(out, "{SEPARATOR}");
        let _ = writeln!(out, "           GooseFS Read Probe Report");
        let _ = writeln!(out, "{SEPARATOR}");
        self.write_basic_info(&mut out);
        let _ = writeln!(
            out,
            "  Read Type:       {}",
            self.read_type.as_deref().unwrap_or("CACHE")
        );
        let _ = writeln!(out);

        let _ = writeln!(
            out,
            "--- [1] OpenFile (Master metadata query + client stream init) ---"
        );
        print_rpc_timing(
            &mut out,
            self.open_file_timing.as_ref(),
            self.open_file_client_us,
            &self.open_file_local,
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "--- [2] Data Read (Worker data read) ---");
        let _ = writeln!(
            out,
            "  Total Duration:  {}",
            format_duration_us(self.data_phase_us)
        );
        print_block_results(&mut out, &self.blocks);
        let _ = writeln!(out);

        let _ = writeln!(out, "--- [3] Close ---");
        let named = top_level_local_us(&self.close_local);
        let duration = self.close_us.max(named);
        let _ = writeln!(out, "  Duration:        {}", format_duration_us(duration));
        if !self.close_local.is_empty() {
            print_named_locals(
                &mut out,
                &self.close_local,
                duration,
                "    ├── ",
                "    └── ",
                "    │     ├── ",
                "          └── ",
            );
        }
        let _ = writeln!(out);

        print_summary(&mut out, self);
        out
    }

    fn write_basic_info(&self, out: &mut String) {
        let _ = writeln!(out, "  File Path:       {}", self.path);
        let _ = writeln!(
            out,
            "  File Size:       {} ({} bytes)",
            format_size(self.file_size),
            self.file_size
        );
        let _ = writeln!(out, "  Block Size:      {}", format_size(self.block_size));
        let _ = writeln!(out, "  Block Count:     {}", self.block_count);
        if !self.master_address.is_empty() {
            let _ = writeln!(out, "  Master Address:  {}", self.master_address);
        }
    }

    fn master_processing_us(&self) -> i64 {
        let a = self
            .create_file_timing
            .as_ref()
            .map(|t| t.server_total_us)
            .unwrap_or(0);
        let b = self
            .open_file_timing
            .as_ref()
            .map(|t| t.server_total_us)
            .unwrap_or(0);
        let c = self
            .complete_file_timing
            .as_ref()
            .map(|t| t.server_total_us)
            .unwrap_or(0);
        a + b + c
    }

    fn worker_processing_us(&self) -> i64 {
        self.blocks
            .iter()
            .map(|b| b.worker_processing_us + b.ufs_worker_processing_us)
            .sum()
    }

    fn network_master_us(&self) -> i64 {
        let a = self
            .create_file_timing
            .as_ref()
            .map(|t| t.network_us)
            .unwrap_or(0);
        let b = self
            .open_file_timing
            .as_ref()
            .map(|t| t.network_us)
            .unwrap_or(0);
        let c = self
            .complete_file_timing
            .as_ref()
            .map(|t| t.network_us)
            .unwrap_or(0);
        a + b + c
    }

    fn network_worker_us(&self) -> i64 {
        self.blocks
            .iter()
            .map(|b| b.network_us + b.ufs_network_us)
            .sum()
    }

    fn client_local_us(&self) -> i64 {
        (self.total_time_us
            - self.master_processing_us()
            - self.worker_processing_us()
            - self.network_master_us()
            - self.network_worker_us())
        .max(0)
    }
}

impl fmt::Display for ProbeResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ProbeKind::Write => f.write_str(&self.format_write()),
            ProbeKind::Read => f.write_str(&self.format_read()),
        }
    }
}

/// Multi-file aggregate, matching Java `CpCommand.printProbeAggregateSummary`.
pub fn format_aggregate(results: &[ProbeResult]) -> String {
    if results.is_empty() {
        return String::new();
    }
    let files = results.len();
    let total_us: i64 = results.iter().map(|r| r.total_time_us).sum();
    let total_bytes: u64 = results.iter().map(|r| r.file_size).sum();
    let create_us: i64 = results.iter().map(|r| r.create_file_client_us).sum();
    let data_us: i64 = results.iter().map(|r| r.data_phase_us).sum();
    let close_us: i64 = results.iter().map(|r| r.close_us).sum();
    let master_us: i64 = results.iter().map(|r| r.master_processing_us()).sum();
    let worker_us: i64 = results.iter().map(|r| r.worker_processing_us()).sum();
    let mut out = String::new();
    let _ = writeln!(out);
    let _ = writeln!(out, "{SEPARATOR}");
    let _ = writeln!(out, "              Probe Aggregate Summary (all files)");
    let _ = writeln!(out, "{SEPARATOR}");
    let _ = writeln!(out, "  Files Copied:        {files}");
    let _ = writeln!(out, "  Total Bytes:         {total_bytes}");
    let _ = writeln!(
        out,
        "  Total Time:          {}",
        format_duration_us(total_us)
    );
    let avg = if files > 0 {
        total_us / files as i64
    } else {
        0
    };
    let _ = writeln!(out, "  Avg per File:        {}", format_duration_us(avg));
    let _ = writeln!(
        out,
        "    ├── CreateFile:    {} ({})",
        format_duration_us(create_us),
        pct(create_us, total_us)
    );
    let _ = writeln!(
        out,
        "    ├── Data Write:    {} ({})",
        format_duration_us(data_us),
        pct(data_us, total_us)
    );
    let _ = writeln!(
        out,
        "    └── Close:         {} ({})",
        format_duration_us(close_us),
        pct(close_us, total_us)
    );
    let _ = writeln!(
        out,
        "  Master Processing:   {} ({})",
        format_duration_us(master_us),
        pct(master_us, total_us)
    );
    let _ = writeln!(
        out,
        "  Worker Processing:   {} ({})",
        format_duration_us(worker_us),
        pct(worker_us, total_us)
    );
    let throughput = if total_us > 0 {
        (total_bytes as f64 / 1024.0 / 1024.0) / (total_us as f64 / 1_000_000.0)
    } else {
        0.0
    };
    let _ = writeln!(out, "  Effective Throughput: {throughput:.1} MB/s");
    let _ = writeln!(out, "{SEPARATOR}");
    out
}

pub fn format_duration_us(us: i64) -> String {
    if us < 1_000 {
        format!("{us} us")
    } else if us < 1_000_000 {
        let ms = us as f64 / 1_000.0;
        if ms == (ms as i64) as f64 {
            format!("{} ms", ms as i64)
        } else {
            format!("{ms:.2} ms")
        }
    } else {
        format!("{:.3} s", us as f64 / 1_000_000.0)
    }
}

fn format_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.1} GB", b / GB)
    } else if b >= MB {
        format!("{:.1} MB", b / MB)
    } else if b >= KB {
        format!("{:.1} KB", b / KB)
    } else {
        format!("{bytes} B")
    }
}

fn pct(part: i64, total: i64) -> String {
    if total <= 0 {
        "0.0%".to_string()
    } else {
        format!("{:.1}%", 100.0 * part as f64 / total as f64)
    }
}

fn pool_acquire_present(client_local: &HashMap<String, i64>) -> bool {
    client_local
        .get(phase::client::POOL_ACQUIRE_US)
        .copied()
        .unwrap_or(0)
        > 0
}

fn is_nested_client_local(key: &str, client_local: &HashMap<String, i64>) -> bool {
    phase::nested_in_pool_acquire(key) && pool_acquire_present(client_local)
}

/// Sum top-level client-local phases. `worker_connect` / `master_connect`
/// sit inside `pool_acquire` and must not be added again.
fn top_level_local_us(client_local: &HashMap<String, i64>) -> i64 {
    client_local
        .iter()
        .filter(|(k, v)| **v > 0 && !is_nested_client_local(k, client_local))
        .map(|(_, v)| *v)
        .sum()
}

/// Fold pre-RPC setup into the tree so Client Total always covers
/// Client Local + Network + Processing (children never exceed the parent).
fn partition_client_tree(
    client_total_us: i64,
    network_us: i64,
    processing_us: i64,
    named_us: i64,
) -> (i64, i64) {
    let residual = (client_total_us - network_us - processing_us).max(0);
    let local = residual.max(named_us);
    let total = client_total_us.max(local + network_us + processing_us);
    (total, local)
}

fn print_named_locals(
    out: &mut String,
    client_local: &HashMap<String, i64>,
    parent_us: i64,
    prefix_mid: &str,
    prefix_last: &str,
    nest_mid: &str,
    nest_last: &str,
) {
    let mut top: Vec<(&str, i64)> = client_local
        .iter()
        .filter(|(k, v)| **v > 0 && !is_nested_client_local(k, client_local))
        .map(|(k, v)| (k.as_str(), *v))
        .collect();
    top.sort_by(|a, b| b.1.cmp(&a.1));
    let accounted: i64 = top.iter().map(|(_, v)| *v).sum();
    let gap = parent_us - accounted;
    let has_gap = gap > 0;
    let total_items = top.len() + usize::from(has_gap);

    for (i, (key, value)) in top.iter().enumerate() {
        let is_last_top = i + 1 >= total_items;
        let prefix = if is_last_top { prefix_last } else { prefix_mid };
        let _ = writeln!(
            out,
            "{prefix}{}: {}",
            phase::display_label(key),
            format_duration_us(*value)
        );
        if *key == phase::client::POOL_ACQUIRE_US {
            let mut children: Vec<(&str, i64)> = client_local
                .iter()
                .filter(|(k, v)| **v > 0 && phase::nested_in_pool_acquire(k))
                .map(|(k, v)| (k.as_str(), *v))
                .collect();
            children.sort_by(|a, b| b.1.cmp(&a.1));
            for (ci, (ck, cv)) in children.iter().enumerate() {
                let last_child = ci + 1 >= children.len();
                let cprefix = match (is_last_top, last_child) {
                    (true, true) => nest_last.to_string(),
                    (true, false) => nest_last.replace("└── ", "├── "),
                    (false, true) => nest_mid.replace("├── ", "└── "),
                    (false, false) => nest_mid.to_string(),
                };
                let _ = writeln!(
                    out,
                    "{cprefix}{}: {}",
                    phase::display_label(ck),
                    format_duration_us(*cv)
                );
            }
        }
    }
    if has_gap {
        let _ = writeln!(
            out,
            "{prefix_last}other (grpc_framework + retry_logic): {}",
            format_duration_us(gap)
        );
    }
}

fn format_sub_value(key: &str, value: i64) -> String {
    match phase::display_type(key) {
        DisplayType::TypeMarker => {
            if key == phase::worker::STORE_TYPE {
                if value == 1 {
                    "PAGE".to_string()
                } else {
                    "BLOCK".to_string()
                }
            } else if value == 1 {
                "UFS".to_string()
            } else {
                "CACHE".to_string()
            }
        }
        DisplayType::Count => value.to_string(),
        DisplayType::Duration => format_duration_us(value),
    }
}

fn print_rpc_timing(
    out: &mut String,
    timing: Option<&ProbeRpcTiming>,
    client_us: i64,
    client_local: &HashMap<String, i64>,
) {
    let Some(timing) = timing else {
        let _ = writeln!(out, "  Client Total:    {}", format_duration_us(client_us));
        let _ = writeln!(out, "  (Server timing info not available)");
        return;
    };
    let named_us = top_level_local_us(client_local);
    // Section Instant minus RPC wall-clock is the true Client Local residual;
    // named phases (connect/pool) may sit outside that Instant.
    let residual = (client_us - timing.client_total_us).max(0);
    let client_local_us = residual.max(named_us);
    let client_total_us =
        client_us.max(client_local_us + timing.network_us + timing.server_total_us);
    let _ = writeln!(
        out,
        "  Client Total:    {}",
        format_duration_us(client_total_us)
    );
    let _ = writeln!(
        out,
        "    ├── Client Local:         {}",
        format_duration_us(client_local_us)
    );

    print_named_locals(
        out,
        client_local,
        client_local_us,
        "    │   ├── ",
        "    │   └── ",
        "    │   │     ├── ",
        "    │         └── ",
    );

    let _ = writeln!(
        out,
        "    ├── Network (RTT):        {}",
        format_duration_us(timing.network_us)
    );
    let _ = writeln!(
        out,
        "    └── Server Processing:    {}",
        format_duration_us(timing.server_total_us)
    );
    if !timing.sub_timings_us.is_empty() {
        print_hierarchical_server(&timing.sub_timings_us, timing.server_total_us, out);
    }
}

/// Format a Master/Worker RPC that ran outside a file read/write
/// [`super::ProbeSession`] (e.g. OpenDAL finalize `Rename`).
pub fn format_standalone_rpc(timing: &ProbeRpcTiming) -> String {
    let short = timing
        .method_name
        .rsplit('/')
        .next()
        .unwrap_or(timing.method_name.as_str());
    let mut out = String::new();
    let _ = writeln!(out);
    let _ = writeln!(out, "{SEPARATOR}");
    let _ = writeln!(out, "           GooseFS RPC Probe Report");
    let _ = writeln!(out, "{SEPARATOR}");
    let _ = writeln!(out, "  Method:          {short}");
    let _ = writeln!(out, "  Full Path:       {}", timing.method_name);
    let _ = writeln!(out);
    let _ = writeln!(out, "--- {short} (Master RPC) ---");
    print_rpc_timing(
        &mut out,
        Some(timing),
        timing.client_total_us,
        &HashMap::new(),
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "{SEPARATOR}");
    out
}

fn print_hierarchical_server(sub: &HashMap<String, i64>, server_total_us: i64, out: &mut String) {
    let mut level1: Vec<(&str, i64)> = phase::master_level1()
        .iter()
        .filter_map(|k| sub.get(*k).map(|v| (*k, *v)))
        .filter(|(_, v)| *v > 0)
        .collect();
    level1.sort_by(|a, b| b.1.cmp(&a.1));
    let level1_sum: i64 = level1.iter().map(|(_, v)| *v).sum();
    let has_gap = server_total_us - level1_sum > 0;
    let total_items = level1.len() + usize::from(has_gap);

    for (i, (key, value)) in level1.iter().enumerate() {
        let prefix = if i + 1 < total_items {
            "        ├── "
        } else {
            "        └── "
        };
        let _ = writeln!(
            out,
            "{prefix}{}: {}",
            phase::display_label(key),
            format_duration_us(*value)
        );
        if *key == phase::master::RPC_CALLABLE_US {
            let mut children: Vec<(&str, i64)> = phase::master_callable_children()
                .iter()
                .filter_map(|k| sub.get(*k).map(|v| (*k, *v)))
                .filter(|(_, v)| *v > 0)
                .collect();
            children.sort_by(|a, b| b.1.cmp(&a.1));
            let child_sum: i64 = children.iter().map(|(_, v)| *v).sum();
            let child_gap = value - child_sum;
            let show_child_gap = child_gap > 0;
            let child_total = children.len() + usize::from(show_child_gap);
            let vert = if i + 1 < total_items {
                "        │"
            } else {
                "         "
            };
            for (ci, (ck, cv)) in children.iter().enumerate() {
                let cprefix = if ci + 1 < child_total {
                    format!("{vert}   ├── ")
                } else {
                    format!("{vert}   └── ")
                };
                let _ = writeln!(
                    out,
                    "{cprefix}{}: {}",
                    phase::display_label(ck),
                    format_duration_us(*cv)
                );
            }
            if show_child_gap {
                let _ = writeln!(
                    out,
                    "{vert}   └── {}: {}",
                    phase::inner_gap_master_callable(),
                    format_duration_us(child_gap)
                );
            }
        }
    }
    if has_gap {
        let _ = writeln!(
            out,
            "        └── {}: {}",
            phase::inner_gap_master_top(),
            format_duration_us(server_total_us - level1_sum)
        );
    }
}

fn print_block_results(out: &mut String, blocks: &[BlockProbeResult]) {
    const MAX: usize = 3;
    let display = blocks.len().min(MAX);
    for block in blocks.iter().take(display) {
        print_single_block(out, block);
    }
    if blocks.len() > display {
        let avg = if blocks.is_empty() {
            0
        } else {
            blocks.iter().map(|b| b.client_total_us).sum::<i64>() / blocks.len() as i64
        };
        let _ = writeln!(
            out,
            "  ... ({} more blocks, avg: {})",
            blocks.len() - display,
            format_duration_us(avg)
        );
    }
}

fn print_single_block(out: &mut String, block: &BlockProbeResult) {
    let _ = writeln!(out);
    let _ = writeln!(out, "  Block #{}", block.index);
    let (client_total_us, block_local) = partition_client_tree(
        block.client_total_us,
        block.network_us,
        block.worker_processing_us,
        top_level_local_us(&block.client_local),
    );
    let _ = writeln!(
        out,
        "    Client Total:           {}",
        format_duration_us(client_total_us)
    );
    let _ = writeln!(
        out,
        "      ├── Client Local:       {}",
        format_duration_us(block_local)
    );
    print_named_locals(
        out,
        &block.client_local,
        block_local,
        "      │     ├── ",
        "      │     └── ",
        "      │     │     ├── ",
        "      │           └── ",
    );
    let _ = writeln!(
        out,
        "      ├── Network Overhead:   {}",
        format_duration_us(block.network_us)
    );
    let _ = writeln!(
        out,
        "      └── Worker Processing:  {}",
        format_duration_us(block.worker_processing_us)
    );
    print_worker_subs(out, &block.worker_sub_timings, block.worker_processing_us);
    if block.has_ufs() {
        let _ = writeln!(out, "    --- UFS Write Stream ---");
        let _ = writeln!(
            out,
            "      ├── Network Overhead (UFS): {}",
            format_duration_us(block.ufs_network_us)
        );
        let _ = writeln!(
            out,
            "      └── Worker Processing:      {}",
            format_duration_us(block.ufs_worker_processing_us)
        );
        print_worker_subs(
            out,
            &block.ufs_worker_sub_timings,
            block.ufs_worker_processing_us,
        );
    }
}

fn print_worker_subs(out: &mut String, sub: &HashMap<String, i64>, worker_processing_us: i64) {
    let mut duration: Vec<(&str, i64)> = sub
        .iter()
        .filter(|(k, v)| {
            **v > 0
                && matches!(phase::display_type(k), DisplayType::Duration)
                && !phase::nested_in_data_write_local(k)
        })
        .map(|(k, v)| (k.as_str(), *v))
        .collect();
    duration.sort_by(|a, b| b.1.cmp(&a.1));
    let named_us: i64 = duration.iter().map(|(_, v)| *v).sum();
    let gap = (worker_processing_us - named_us).max(0);
    let show_gap = gap > 0;

    let meta: Vec<(&str, i64)> = sub
        .iter()
        .filter(|(k, v)| match phase::display_type(k) {
            DisplayType::TypeMarker => true,
            DisplayType::Count => **v != 0,
            DisplayType::Duration => false,
        })
        .map(|(k, v)| (k.as_str(), *v))
        .collect();

    let total_items = duration.len() + meta.len() + usize::from(show_gap);
    if total_items == 0 {
        return;
    }

    let mut i = 0;
    let write_item = |out: &mut String, i: usize, label: &str, rendered: String| {
        let prefix = if i + 1 < total_items {
            "            ├── "
        } else {
            "            └── "
        };
        let _ = writeln!(out, "{prefix}{label}: {rendered}");
    };

    for (key, value) in &duration {
        let parent_is_last = i + 1 >= total_items;
        write_item(
            out,
            i,
            worker_duration_label(key, sub),
            format_sub_value(key, *value),
        );
        if *key == phase::worker::DATA_WRITE_LOCAL_US {
            print_data_write_local_children(out, sub, *value, parent_is_last);
        }
        i += 1;
    }
    for (key, value) in &meta {
        write_item(
            out,
            i,
            phase::display_label(key),
            format_sub_value(key, *value),
        );
        i += 1;
    }
    if show_gap {
        write_item(out, i, worker_inner_gap_label(sub), format_duration_us(gap));
    }
}

fn worker_subs_are_page(sub: &HashMap<String, i64>) -> bool {
    sub.get(phase::worker::STORE_TYPE).copied() == Some(1)
}

fn worker_has_block_append_children(sub: &HashMap<String, i64>) -> bool {
    sub.iter()
        .any(|(k, v)| *v > 0 && phase::nested_in_data_write_local(k))
}

fn worker_duration_label<'a>(key: &'a str, sub: &HashMap<String, i64>) -> &'a str {
    if key == phase::worker::DATA_WRITE_LOCAL_US {
        phase::data_write_local_label(
            sub.get(phase::worker::STORE_TYPE).copied(),
            worker_has_block_append_children(sub),
        )
    } else {
        phase::display_label(key)
    }
}

fn worker_pipeline_split(sub: &HashMap<String, i64>) -> bool {
    [
        phase::worker::AWAIT_CHUNK_US,
        phase::worker::EXECUTOR_WAIT_US,
        phase::worker::FLUSH_US,
        phase::worker::CHUNK_BUFFER_US,
        phase::worker::CLOSE_WRITER_US,
        phase::worker::RATE_LIMIT_US,
    ]
    .iter()
    .any(|k| sub.get(*k).copied().unwrap_or(0) > 0)
}

fn worker_subs_are_ufs(sub: &HashMap<String, i64>) -> bool {
    sub.get(phase::worker::WRITE_TYPE).copied() == Some(1)
        || [
            phase::worker::DATA_WRITE_UFS_US,
            phase::worker::CREATE_UFS_FILE_US,
            phase::worker::COMPLETE_UFS_FILE_US,
        ]
        .iter()
        .any(|k| sub.get(*k).copied().unwrap_or(0) > 0)
}

fn worker_inner_gap_label(sub: &HashMap<String, i64>) -> &'static str {
    phase::inner_gap_worker_write(worker_subs_are_ufs(sub), worker_pipeline_split(sub))
}

fn print_data_write_local_children(
    out: &mut String,
    sub: &HashMap<String, i64>,
    parent_us: i64,
    parent_is_last: bool,
) {
    let mut kids: Vec<(&str, i64)> = sub
        .iter()
        .filter(|(k, v)| **v > 0 && phase::nested_in_data_write_local(k))
        .map(|(k, v)| (k.as_str(), *v))
        .collect();
    if kids.is_empty() {
        return;
    }
    kids.sort_by(|a, b| b.1.cmp(&a.1));
    let child_sum: i64 = kids.iter().map(|(_, v)| *v).sum();
    let child_gap = parent_us - child_sum;
    let show_gap = child_gap > 0;
    let n = kids.len() + usize::from(show_gap);
    let (mid, last) = if parent_is_last {
        ("                  ├── ", "                  └── ")
    } else {
        ("            │     ├── ", "            │     └── ")
    };
    for (i, (key, value)) in kids.iter().enumerate() {
        let prefix = if i + 1 >= n { last } else { mid };
        let _ = writeln!(
            out,
            "{prefix}{}: {}",
            phase::display_label(key),
            format_duration_us(*value)
        );
    }
    if show_gap {
        let _ = writeln!(
            out,
            "{last}{}: {}",
            phase::inner_gap_data_write_local(worker_subs_are_page(sub)),
            format_duration_us(child_gap)
        );
    }
}

fn print_summary(out: &mut String, result: &ProbeResult) {
    let total = result.total_time_us;
    let _ = writeln!(out, "{SEPARATOR}");
    let _ = writeln!(out, "                     Summary");
    let _ = writeln!(out, "{SEPARATOR}");
    let _ = writeln!(
        out,
        "  Total Time:                          {}",
        format_duration_us(total)
    );
    let _ = writeln!(
        out,
        "    ├── Master Processing:             {} ({})",
        format_duration_us(result.master_processing_us()),
        pct(result.master_processing_us(), total)
    );
    let _ = writeln!(
        out,
        "    ├── Worker Processing:             {} ({})",
        format_duration_us(result.worker_processing_us()),
        pct(result.worker_processing_us(), total)
    );
    let _ = writeln!(
        out,
        "    ├── Network (Client↔Master):       {} ({})",
        format_duration_us(result.network_master_us()),
        pct(result.network_master_us(), total)
    );
    let _ = writeln!(
        out,
        "    ├── Network Overhead (Client↔Worker): {} ({})",
        format_duration_us(result.network_worker_us()),
        pct(result.network_worker_us(), total)
    );
    let _ = writeln!(
        out,
        "    └── Client Local:                  {} ({})",
        format_duration_us(result.client_local_us()),
        pct(result.client_local_us(), total)
    );
    let throughput = if total > 0 {
        (result.file_size as f64 / 1024.0 / 1024.0) / (total as f64 / 1_000_000.0)
    } else {
        0.0
    };
    let _ = writeln!(
        out,
        "  Effective Throughput:                 {throughput:.1} MB/s"
    );
    let _ = writeln!(out, "{SEPARATOR}");
    let _ = writeln!(out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_formatting_matches_java_buckets() {
        assert_eq!(format_duration_us(8), "8 us");
        assert_eq!(format_duration_us(1_000), "1 ms");
        assert_eq!(format_duration_us(122_560), "122.56 ms");
        assert_eq!(format_duration_us(1_420_000), "1.420 s");
    }

    #[test]
    fn write_report_contains_sections() {
        let result = ProbeResult {
            kind: ProbeKind::Write,
            path: "/test/file".into(),
            file_size: 3791,
            block_size: 128 * 1024 * 1024,
            block_count: 1,
            write_type: Some("CACHE_THROUGH".into()),
            read_type: None,
            master_address: "172.16.16.23:9200".into(),
            total_time_us: 438_600,
            create_file_client_us: 122_560,
            open_file_client_us: 0,
            data_phase_us: 33_870,
            complete_file_client_us: 0,
            close_us: 278_170,
            create_file_timing: None,
            open_file_timing: None,
            complete_file_timing: None,
            create_file_local: HashMap::new(),
            open_file_local: HashMap::new(),
            complete_file_local: HashMap::new(),
            close_local: HashMap::new(),
            blocks: vec![BlockProbeResult {
                index: 0,
                ..Default::default()
            }],
        };
        let text = result.format_write();
        assert!(text.contains("GooseFS Write Probe Report"));
        assert!(text.contains("--- [1] CreateFile"));
        assert!(text.contains("--- [2] Data Write"));
        assert!(text.contains("CACHE_THROUGH"));
        assert!(text.contains("172.16.16.23:9200"));
        assert!(!result.is_vacuous());
    }

    #[test]
    fn close_section_prints_named_phases() {
        let result = ProbeResult {
            kind: ProbeKind::Write,
            path: "/test/file".into(),
            file_size: 512 * 1024 * 1024,
            block_size: 64 * 1024 * 1024,
            block_count: 9,
            write_type: Some("ASYNC_THROUGH".into()),
            read_type: None,
            master_address: "172.16.16.23:9200".into(),
            total_time_us: 763_840,
            create_file_client_us: 2_280,
            open_file_client_us: 0,
            data_phase_us: 674_030,
            complete_file_client_us: 2_060,
            close_us: 10_940,
            create_file_timing: None,
            open_file_timing: None,
            complete_file_timing: None,
            create_file_local: HashMap::new(),
            open_file_local: HashMap::new(),
            complete_file_local: HashMap::new(),
            close_local: HashMap::from([
                (phase::client::LAST_BLOCK_CLOSE_US.to_string(), 9_200),
                (phase::client::ASYNC_PERSIST_US.to_string(), 1_400),
                (phase::client::INVALIDATE_META_US.to_string(), 80),
            ]),
            blocks: vec![BlockProbeResult::default()],
        };
        let text = result.format_write();
        assert!(text.contains("--- [4] Close ---"));
        assert!(text.contains("last_block_close: 9.20 ms"));
        assert!(text.contains("async_persist: 1.40 ms"));
        assert!(text.contains("invalidate_meta: 80 us"));
        let empty = ProbeResult {
            kind: ProbeKind::Write,
            path: "/x".into(),
            file_size: 0,
            block_size: 1,
            block_count: 1,
            write_type: None,
            read_type: None,
            master_address: String::new(),
            total_time_us: 3160,
            create_file_client_us: 0,
            open_file_client_us: 0,
            data_phase_us: 0,
            complete_file_client_us: 0,
            close_us: 0,
            create_file_timing: None,
            open_file_timing: None,
            complete_file_timing: None,
            create_file_local: HashMap::new(),
            open_file_local: HashMap::new(),
            complete_file_local: HashMap::new(),
            close_local: HashMap::new(),
            blocks: vec![BlockProbeResult::default()],
        };
        assert!(empty.is_vacuous());
    }

    #[test]
    fn client_local_parent_covers_overlapping_named_phases() {
        let timing = ProbeRpcTiming {
            method_name: "WriteBlock".into(),
            client_total_us: 5_100,
            server_total_us: 4_000,
            network_us: 1_100,
            sub_timings_us: HashMap::new(),
        };
        let block = BlockProbeResult {
            index: 0,
            client_total_us: 5_100,
            network_us: 1_100,
            worker_processing_us: 4_000,
            client_local: HashMap::from([(phase::client::WORKER_CONNECT_US.to_string(), 1_930)]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        // Setup (1.93 ms) sits outside the RPC Instant; fold it into Client Total
        // so Local + Network + Worker partition the parent (7.03 ms).
        assert!(out.contains("Client Total:           7.03 ms"));
        assert!(out.contains("Client Local:       1.93 ms"));
        assert!(out.contains("worker_connect: 1.93 ms"));
        assert!(!out.contains("Client Local:       0 us"));
        assert!(!out.contains("Client Total:           5.10 ms"));

        let mut rpc_out = String::new();
        print_rpc_timing(
            &mut rpc_out,
            Some(&timing),
            1_620,
            &HashMap::from([(phase::client::POOL_ACQUIRE_US.to_string(), 50)]),
        );
        // Residual is 0 (RPC wall-clock 5.10 ms > section 1.62 ms); named
        // child still fits under a raised Client Local parent.
        assert!(rpc_out.contains("Client Local:         50 us"));
        assert!(rpc_out.contains("pool_acquire: 50 us"));
    }

    #[test]
    fn block_tree_client_total_covers_local_and_worker() {
        // Read report: RPC Instant 81 us, server 294 us, pool/connect 2.42 ms.
        let block = BlockProbeResult {
            index: 0,
            client_total_us: 81,
            network_us: 0,
            worker_processing_us: 294,
            client_local: HashMap::from([
                (phase::client::POOL_ACQUIRE_US.to_string(), 2_420),
                (phase::client::WORKER_CONNECT_US.to_string(), 1_380),
                (phase::client::OPEN_STREAM_US.to_string(), 1_140),
            ]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        // pool_acquire (2.42 ms, includes connect) + open_stream (1.14 ms) + worker 294 us
        assert!(out.contains("Client Total:           3.85 ms"));
        assert!(out.contains("Client Local:       3.56 ms"));
        assert!(out.contains("pool_acquire: 2.42 ms"));
        assert!(out.contains("worker_connect: 1.38 ms"));
        assert!(out.contains("open_stream: 1.14 ms"));
        assert!(out.contains("Worker Processing:  294 us"));
        assert!(!out.contains("Client Total:           81 us"));
        assert!(!out.contains("Client Local:       2.42 ms"));
    }

    #[test]
    fn pool_acquire_nests_connect_and_sums_with_open_stream() {
        let block = BlockProbeResult {
            index: 0,
            client_total_us: 0,
            network_us: 0,
            worker_processing_us: 0,
            client_local: HashMap::from([
                (phase::client::POOL_ACQUIRE_US.to_string(), 2_620),
                (phase::client::WORKER_CONNECT_US.to_string(), 1_100),
                (phase::client::OPEN_STREAM_US.to_string(), 981),
            ]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        assert!(out.contains("Client Total:           3.60 ms"));
        assert!(out.contains("Client Local:       3.60 ms"));
        assert!(out.contains("pool_acquire: 2.62 ms"));
        assert!(out.contains("worker_connect: 1.10 ms"));
        assert!(out.contains("open_stream: 981 us"));
        assert!(!out.contains("Client Total:           2.62 ms"));
    }

    #[test]
    fn worker_tree_shows_inner_gap_when_sub_phases_under_account() {
        // 512MB ASYNC block: server_total 240.69ms, named local write ~17.5ms.
        let block = BlockProbeResult {
            index: 0,
            client_total_us: 259_080,
            network_us: 437,
            worker_processing_us: 240_690,
            worker_sub_timings: HashMap::from([
                (phase::worker::DATA_WRITE_LOCAL_US.to_string(), 16_490),
                (phase::worker::CREATE_BLOCK_REMOTE_US.to_string(), 596),
                (phase::worker::COMMIT_BLOCK_US.to_string(), 302),
                (phase::worker::GET_TEMP_WRITER_US.to_string(), 121),
                (phase::worker::WRITE_TYPE.to_string(), 0),
            ]),
            ufs_worker_processing_us: 249_750,
            ufs_worker_sub_timings: HashMap::from([
                (phase::worker::CREATE_UFS_FILE_US.to_string(), 147_780),
                (phase::worker::DATA_WRITE_UFS_US.to_string(), 350),
                (phase::worker::WRITE_TYPE.to_string(), 1),
            ]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        assert!(out.contains("Worker Processing:  240.69 ms"));
        assert!(out.contains("data_write_local (BlockWriter.append): 16.49 ms"));
        assert!(out.contains(
            "(inner_gap) (await client chunk + executor + unscoped pipeline): 223.18 ms"
        ));
        assert!(out.contains("write_type: CACHE"));
        assert!(out.contains("UFS Write Stream"));
        assert!(out.contains("create_ufs_file: 147.78 ms"));
        assert!(out.contains("write_type: UFS"));
        assert!(out.contains("(inner_gap) (await client chunk + unscoped UFS pipeline): 101.62 ms"));
    }

    #[test]
    fn worker_tree_splits_write_inner_gap_into_named_phases() {
        let block = BlockProbeResult {
            index: 1,
            client_total_us: 79_190,
            network_us: 366,
            worker_processing_us: 78_830,
            worker_sub_timings: HashMap::from([
                (phase::worker::DATA_WRITE_LOCAL_US.to_string(), 20_340),
                (phase::worker::AWAIT_CHUNK_US.to_string(), 31_200),
                (phase::worker::FLUSH_US.to_string(), 18_400),
                (phase::worker::CHUNK_BUFFER_US.to_string(), 4_100),
                (phase::worker::EXECUTOR_WAIT_US.to_string(), 2_200),
                (phase::worker::CLOSE_WRITER_US.to_string(), 80),
                (phase::worker::CREATE_BLOCK_REMOTE_US.to_string(), 237),
                (phase::worker::COMMIT_BLOCK_US.to_string(), 198),
                (phase::worker::GET_TEMP_WRITER_US.to_string(), 25),
                (phase::worker::WRITE_TYPE.to_string(), 0),
            ]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        assert!(out.contains("await_chunk (writer idle for next client chunk): 31.20 ms"));
        assert!(out.contains("flush (FileChannel.force / UFS flush): 18.40 ms"));
        assert!(out.contains("chunk_buffer (ByteString → DataBuffer): 4.10 ms"));
        assert!(out.contains("executor_wait (serializing executor hop): 2.20 ms"));
        assert!(out.contains("close_writer (BlockWriter.close): 80 us"));
        assert!(out.contains("data_write_local (BlockWriter.append): 20.34 ms"));
        assert!(!out.contains("(inner_gap): 58.03 ms"));
    }

    #[test]
    fn data_write_local_nests_mmap_copy_unmap_without_double_counting_gap() {
        let block = BlockProbeResult {
            index: 0,
            client_total_us: 100_000,
            network_us: 400,
            worker_processing_us: 100_000,
            worker_sub_timings: HashMap::from([
                (phase::worker::DATA_WRITE_LOCAL_US.to_string(), 20_000),
                (phase::worker::DATA_WRITE_MMAP_US.to_string(), 7_000),
                (phase::worker::DATA_WRITE_COPY_US.to_string(), 8_000),
                (phase::worker::DATA_WRITE_UNMAP_US.to_string(), 5_000),
                (phase::worker::WRITE_TYPE.to_string(), 0),
            ]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        assert!(out.contains(
            "data_write_local (block store: mmap + copy into local file, no fsync): 20 ms"
        ));
        assert!(out.contains("data_write_copy (memcpy into mapped pages): 8 ms"));
        assert!(out.contains("data_write_mmap (FileChannel.map): 7 ms"));
        assert!(out.contains("data_write_unmap (unmap / cleanDirectBuffer): 5 ms"));
        // Children must not be added into the Worker inner_gap remainder (80 ms).
        assert!(
            out.contains("(inner_gap) (await client chunk + executor + unscoped pipeline): 80 ms")
        );
        assert!(
            !out.contains("(inner_gap) (await client chunk + executor + unscoped pipeline): 60 ms")
        );
    }

    #[test]
    fn data_write_local_page_store_does_not_claim_mmap() {
        let block = BlockProbeResult {
            index: 0,
            client_total_us: 50_000,
            network_us: 400,
            worker_processing_us: 40_000,
            worker_sub_timings: HashMap::from([
                (phase::worker::DATA_WRITE_LOCAL_US.to_string(), 18_000),
                (phase::worker::STORE_TYPE.to_string(), 1),
                (phase::worker::WRITE_TYPE.to_string(), 0),
            ]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        assert!(
            out.contains("data_write_local (page store: CacheManager.append temp pages): 18 ms")
        );
        assert!(out.contains("store_type: PAGE"));
        assert!(!out.contains("mmap + copy into local file"));
    }

    #[test]
    fn data_write_local_block_store_type_uses_mmap_label() {
        let block = BlockProbeResult {
            index: 0,
            client_total_us: 50_000,
            network_us: 400,
            worker_processing_us: 40_000,
            worker_sub_timings: HashMap::from([
                (phase::worker::DATA_WRITE_LOCAL_US.to_string(), 18_000),
                (phase::worker::STORE_TYPE.to_string(), 0),
                (phase::worker::WRITE_TYPE.to_string(), 0),
            ]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        assert!(out.contains(
            "data_write_local (block store: mmap + copy into local file, no fsync): 18 ms"
        ));
        assert!(out.contains("store_type: BLOCK"));
    }

    #[test]
    fn later_blocks_print_their_own_client_local() {
        let blocks = vec![
            BlockProbeResult {
                index: 0,
                client_total_us: 243_970,
                network_us: 373,
                worker_processing_us: 120_650,
                client_local: HashMap::from([(phase::client::CHUNK_COPY_US.to_string(), 102_950)]),
                ..Default::default()
            },
            BlockProbeResult {
                index: 1,
                client_total_us: 79_190,
                network_us: 366,
                worker_processing_us: 78_830,
                client_local: HashMap::from([
                    (phase::client::CHUNK_COPY_US.to_string(), 8_500),
                    (phase::client::CHUNK_SEND_US.to_string(), 3_100),
                ]),
                ..Default::default()
            },
        ];
        let mut out = String::new();
        print_block_results(&mut out, &blocks);
        assert!(out.contains("Block #1"));
        assert!(out.contains("chunk_copy: 8.50 ms"));
        assert!(out.contains("chunk_send (gRPC send / Worker backpressure): 3.10 ms"));
        assert!(!out.contains("Client Local:       0 us"));
    }

    #[test]
    fn worker_tree_shows_chunk_copy_and_pending_chunk_client_locals() {
        let block = BlockProbeResult {
            index: 0,
            client_total_us: 250_000,
            network_us: 400,
            worker_processing_us: 30_000,
            client_local: HashMap::from([
                (phase::client::CHUNK_COPY_US.to_string(), 78_900),
                (phase::client::CHUNK_SEND_US.to_string(), 3_100),
                (phase::client::FLUSH_ACK_US.to_string(), 29_500),
                (phase::client::PENDING_CHUNK_US.to_string(), 1_200),
                (phase::client::POOL_ACQUIRE_US.to_string(), 14_300),
                (phase::client::SELECT_WORKER_US.to_string(), 200),
                (phase::client::BLOCK_CLOSE_US.to_string(), 600),
            ]),
            ..Default::default()
        };
        let mut out = String::new();
        print_single_block(&mut out, &block);
        assert!(out.contains("chunk_copy: 78.90 ms"));
        assert!(out.contains("chunk_send (gRPC send / Worker backpressure): 3.10 ms"));
        assert!(out.contains("flush_ack: 29.50 ms"));
        assert!(out.contains("pending_chunk: 1.20 ms"));
        assert!(out.contains("pool_acquire: 14.30 ms"));
        assert!(out.contains("select_worker: 200 us"));
        assert!(out.contains("block_close: 600 us"));
    }

    #[test]
    fn standalone_rename_report_splits_client_network_server() {
        let timing = ProbeRpcTiming {
            method_name: super::super::collector::METHOD_RENAME.to_string(),
            client_total_us: 580_084,
            server_total_us: 579_500,
            network_us: 584,
            sub_timings_us: HashMap::from([
                (phase::master::RPC_CALLABLE_US.to_string(), 579_000),
                (phase::master::RENAME_INTERNAL_US.to_string(), 578_000),
            ]),
        };
        let out = format_standalone_rpc(&timing);
        assert!(out.contains("GooseFS RPC Probe Report"));
        assert!(out.contains("Method:          Rename"));
        assert!(out.contains("Client Total:"));
        assert!(out.contains("Network (RTT):"));
        assert!(out.contains("Server Processing:"));
        assert!(out.contains("rename_internal:"));
    }
}
