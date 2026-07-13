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

//! gRPC client modules for Goosefs Master and Worker services.

pub mod master;
pub mod master_inquire;
pub mod metrics_master;
pub mod worker;
pub mod worker_manager;

pub use master::MasterClient;
pub use master::MasterClientPool;
pub use master::PooledClient;
pub use master_inquire::{
    create_master_inquire_client, MasterInquireClient, PollingMasterInquireClient,
    SingleMasterInquireClient,
};
pub use metrics_master::MetricsClient;
pub use worker::OpenLocalBlockGuard;
pub use worker::WorkerClient;
pub use worker::WorkerClientPool;
pub use worker::WriteBlockHandle;
pub use worker::WriteBlockOptions;
pub use worker_manager::WorkerManagerClient;
