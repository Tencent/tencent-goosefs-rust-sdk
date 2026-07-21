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

//! Client metrics collection and heartbeat infrastructure.
//!
//! This module provides:
//! - **Registry**: Thread-safe global counters and gauges
//! - **Reporter**: Snapshot and diff calculation for heartbeat
//! - **Heartbeat**: Periodic background task for master reporting
//! - **Master Client**: gRPC client for metrics heartbeat RPC
//!
//! ## Quick Start
//!
//! ```ignore
//! use goosefs_sdk::metrics;
//!
//! // Increment a counter (gets or creates on first call)
//! let bytes_read = metrics::counter(metrics::name::CLIENT_BYTES_READ_LOCAL);
//! bytes_read.inc(1024);
//!
//! // The metrics are automatically reported to Master via heartbeat
//! // (if metrics_enabled=true in GoosefsConfig)
//! ```

pub(crate) mod heartbeat;
#[cfg(feature = "metrics-pushgateway")]
pub mod pushgateway;
pub mod registry;
pub mod reporter;

pub use registry::{counter, gauge, name, Counter, Gauge};

// Re-export internal types needed by integration tests.
// These are lower-level APIs; prefer the high-level `FileSystemContext` for
// production use.
#[doc(hidden)]
pub use heartbeat::{resolve_app_id, HeartbeatTask};
#[doc(hidden)]
pub use reporter::ClientMetricsReporter;
