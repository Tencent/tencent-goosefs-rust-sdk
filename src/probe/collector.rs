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

//! Per-session probe timing collector.
//!
//! One collector is attached to a [`super::ProbeSession`] (one file write or
//! read). RPC timings are recorded from unary trailing metadata and streaming
//! trailers; client-local phases are accumulated by name.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use prost::Message;
use tonic::metadata::MetadataMap;

use crate::proto::grpc::ProbeTimingInfo;

use super::phase;

/// gRPC ASCII header that activates server-side `ProbeTimingServerInterceptor`.
pub const PROBE_ENABLED_HEADER: &str = "probe-enabled";

/// gRPC binary trailer carrying a serialized [`ProbeTimingInfo`].
pub const PROBE_TIMING_TRAILER: &str = "probe-timing-bin";

/// Full gRPC method names used to classify collected RPCs (tonic path form).
pub const METHOD_CREATE_FILE: &str =
    "/com.qcloud.cos.goosefs.grpc.file.FileSystemMasterClientService/CreateFile";
pub const METHOD_GET_STATUS: &str =
    "/com.qcloud.cos.goosefs.grpc.file.FileSystemMasterClientService/GetStatus";
pub const METHOD_COMPLETE_FILE: &str =
    "/com.qcloud.cos.goosefs.grpc.file.FileSystemMasterClientService/CompleteFile";
pub const METHOD_RENAME: &str =
    "/com.qcloud.cos.goosefs.grpc.file.FileSystemMasterClientService/Rename";
/// Master delete RPC (proto method name is `Remove`).
pub const METHOD_REMOVE: &str =
    "/com.qcloud.cos.goosefs.grpc.file.FileSystemMasterClientService/Remove";
pub const METHOD_CREATE_DIRECTORY: &str =
    "/com.qcloud.cos.goosefs.grpc.file.FileSystemMasterClientService/CreateDirectory";
pub const METHOD_WRITE_BLOCK: &str = "/com.qcloud.cos.goosefs.grpc.block.BlockWorker/WriteBlock";
pub const METHOD_READ_BLOCK: &str = "/com.qcloud.cos.goosefs.grpc.block.BlockWorker/ReadBlock";

/// Timing for a single RPC, matching Java `ProbeClientInterceptor.ProbeRpcTiming`.
#[derive(Debug, Clone)]
pub struct ProbeRpcTiming {
    pub method_name: String,
    pub client_total_us: i64,
    pub server_total_us: i64,
    pub network_us: i64,
    pub sub_timings_us: HashMap<String, i64>,
}

impl ProbeRpcTiming {
    pub fn from_parts(
        method_name: impl Into<String>,
        client_total_us: i64,
        server: ProbeTimingInfo,
    ) -> Self {
        let server_total_us = server.server_total_us.unwrap_or(0);
        let network_us = (client_total_us - server_total_us).max(0);
        Self {
            method_name: method_name.into(),
            client_total_us,
            server_total_us,
            network_us,
            sub_timings_us: server.sub_timings_us,
        }
    }

    pub fn is_write_ufs(&self) -> bool {
        self.sub_timings_us.get(phase::worker::WRITE_TYPE).copied() == Some(1)
    }
}

/// Parse `ProbeTimingInfo` from a gRPC metadata map (trailers or status).
pub fn parse_timing_from_metadata(meta: &MetadataMap) -> ProbeTimingInfo {
    let Some(value) = meta.get_bin(PROBE_TIMING_TRAILER) else {
        return ProbeTimingInfo::default();
    };
    // Binary gRPC metadata is base64 on the wire; `as_ref()` is encoded.
    let bytes = match value.to_bytes() {
        Ok(b) => b,
        Err(e) => {
            tracing::debug!(error = %e, "invalid probe-timing-bin trailer encoding");
            return ProbeTimingInfo::default();
        }
    };
    match ProbeTimingInfo::decode(bytes.as_ref()) {
        Ok(info) => info,
        Err(e) => {
            tracing::debug!(error = %e, "failed to decode probe-timing-bin trailer");
            ProbeTimingInfo::default()
        }
    }
}

/// Build an RPC timing from client elapsed time and trailing metadata.
pub fn timing_from_trailers(
    method: &str,
    started: Instant,
    trailers: &MetadataMap,
) -> ProbeRpcTiming {
    let client_total_us = started.elapsed().as_micros() as i64;
    ProbeRpcTiming::from_parts(
        method,
        client_total_us,
        parse_timing_from_metadata(trailers),
    )
}

/// Per-file collector. Safe to share with a WriteBlock background task.
#[derive(Debug)]
pub struct ProbeCollector {
    rpcs: Mutex<Vec<ProbeRpcTiming>>,
    local: Mutex<HashMap<String, i64>>,
    /// 0 = not inside a WriteBlock; 1-based index into [`block_locals`].
    current_block: AtomicUsize,
    /// Client-local phases keyed by cache WriteBlock (open order).
    block_locals: Mutex<Vec<HashMap<String, i64>>>,
}

impl ProbeCollector {
    pub fn new() -> Self {
        Self {
            rpcs: Mutex::new(Vec::new()),
            local: Mutex::new(HashMap::new()),
            current_block: AtomicUsize::new(0),
            block_locals: Mutex::new(Vec::new()),
        }
    }

    /// Start attributing [`record_local`] to a new cache WriteBlock.
    pub fn begin_write_block(&self) {
        let mut blocks = self.block_locals.lock().unwrap_or_else(|e| e.into_inner());
        blocks.push(HashMap::new());
        self.current_block.store(blocks.len(), Ordering::Release);
    }

    /// Stop attributing to the current WriteBlock (CompleteFile / Close follow).
    pub fn end_write_block(&self) {
        self.current_block.store(0, Ordering::Release);
    }

    pub fn take_write_block_locals(&self) -> Vec<HashMap<String, i64>> {
        self.current_block.store(0, Ordering::Release);
        let mut blocks = self.block_locals.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *blocks)
    }

    pub fn record_rpc(&self, timing: ProbeRpcTiming) {
        self.rpcs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(timing);
    }

    pub fn record_local(&self, phase: &str, duration_us: i64) {
        if duration_us <= 0 {
            return;
        }
        let idx = self.current_block.load(Ordering::Acquire);
        if idx > 0 {
            let mut blocks = self.block_locals.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(map) = blocks.get_mut(idx.saturating_sub(1)) {
                *map.entry(phase.to_string()).or_insert(0) += duration_us;
                return;
            }
        }
        let mut map = self.local.lock().unwrap_or_else(|e| e.into_inner());
        *map.entry(phase.to_string()).or_insert(0) += duration_us;
    }

    pub fn snapshot_local(&self) -> HashMap<String, i64> {
        self.local.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Drain client-local phases so the next section (data write/read) starts empty.
    pub fn take_local(&self) -> HashMap<String, i64> {
        let mut map = self.local.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *map)
    }

    pub fn latest_matching(&self, pred: impl Fn(&str) -> bool) -> Option<ProbeRpcTiming> {
        let rpcs = self.rpcs.lock().unwrap_or_else(|e| e.into_inner());
        rpcs.iter()
            .rev()
            .find(|t| pred(&t.method_name.to_ascii_lowercase()))
            .cloned()
    }

    pub fn write_block_timings(&self) -> (Vec<ProbeRpcTiming>, Vec<ProbeRpcTiming>) {
        let rpcs = self.rpcs.lock().unwrap_or_else(|e| e.into_inner());
        let mut primary = Vec::new();
        let mut ufs = Vec::new();
        for t in rpcs.iter() {
            if !t.method_name.to_ascii_lowercase().contains("writeblock") {
                continue;
            }
            if t.is_write_ufs() {
                ufs.push(t.clone());
            } else {
                primary.push(t.clone());
            }
        }
        (primary, ufs)
    }

    pub fn read_block_timings(&self) -> Vec<ProbeRpcTiming> {
        let rpcs = self.rpcs.lock().unwrap_or_else(|e| e.into_inner());
        rpcs.iter()
            .filter(|t| t.method_name.to_ascii_lowercase().contains("readblock"))
            .cloned()
            .collect()
    }
}

impl Default for ProbeCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_metadata_returns_default() {
        let info = parse_timing_from_metadata(&MetadataMap::new());
        assert_eq!(info.server_total_us, None);
        assert!(info.sub_timings_us.is_empty());
    }

    #[test]
    fn parse_binary_trailer_from_metadata_map() {
        use tonic::metadata::MetadataValue;
        let original = ProbeTimingInfo {
            server_total_us: Some(1234),
            sub_timings_us: HashMap::from([(phase::master::RPC_CALLABLE_US.to_string(), 1000)]),
        };
        let mut meta = MetadataMap::new();
        meta.insert_bin(
            PROBE_TIMING_TRAILER,
            MetadataValue::from_bytes(&original.encode_to_vec()),
        );
        let decoded = parse_timing_from_metadata(&meta);
        assert_eq!(decoded.server_total_us, Some(1234));
        assert_eq!(decoded.sub_timings_us.get("rpc_callable_us"), Some(&1000));
    }

    #[test]
    fn roundtrip_probe_timing_info_bytes() {
        let original = ProbeTimingInfo {
            server_total_us: Some(1234),
            sub_timings_us: HashMap::from([
                (phase::master::RPC_CALLABLE_US.to_string(), 1000),
                (phase::master::CREATE_FILE_INTERNAL_US.to_string(), 300),
            ]),
        };
        let bytes = original.encode_to_vec();
        let decoded = ProbeTimingInfo::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.server_total_us, Some(1234));
        assert_eq!(decoded.sub_timings_us.get("rpc_callable_us"), Some(&1000));
    }

    #[test]
    fn ufs_write_type_classifies_stream() {
        let mut t = ProbeRpcTiming::from_parts("WriteBlock", 10, ProbeTimingInfo::default());
        assert!(!t.is_write_ufs());
        t.sub_timings_us
            .insert(phase::worker::WRITE_TYPE.to_string(), 1);
        assert!(t.is_write_ufs());
    }

    #[test]
    fn take_local_drains_the_map() {
        let c = ProbeCollector::new();
        c.record_local(phase::client::POOL_ACQUIRE_US, 100);
        c.record_local(phase::client::WORKER_CONNECT_US, 200);
        let taken = c.take_local();
        assert_eq!(taken.get(phase::client::POOL_ACQUIRE_US), Some(&100));
        assert_eq!(taken.get(phase::client::WORKER_CONNECT_US), Some(&200));
        assert!(c.snapshot_local().is_empty());
        c.record_local(phase::client::OPEN_STREAM_US, 30);
        assert_eq!(c.snapshot_local().len(), 1);
        assert!(!c
            .snapshot_local()
            .contains_key(phase::client::POOL_ACQUIRE_US));
    }

    #[test]
    fn write_block_locals_are_isolated_from_take_local() {
        let c = ProbeCollector::new();
        c.begin_write_block();
        c.record_local(phase::client::CHUNK_COPY_US, 10_000);
        c.end_write_block();
        c.begin_write_block();
        c.record_local(phase::client::CHUNK_COPY_US, 20_000);
        c.record_local(phase::client::FLUSH_ACK_US, 5_000);
        c.end_write_block();

        assert!(c.take_local().is_empty());
        let blocks = c.take_write_block_locals();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].get(phase::client::CHUNK_COPY_US), Some(&10_000));
        assert_eq!(blocks[1].get(phase::client::CHUNK_COPY_US), Some(&20_000));
        assert_eq!(blocks[1].get(phase::client::FLUSH_ACK_US), Some(&5_000));
        assert!(!blocks[0].contains_key(phase::client::FLUSH_ACK_US));
    }
}
