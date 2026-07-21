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

//! Block mapping and worker routing modules.

pub mod mapper;
pub mod router;
pub mod short_circuit;

pub use mapper::{BlockMapper, BlockReadPlan, BlockWritePlan};
pub use router::WorkerRouter;
pub use short_circuit::{
    should_use_short_circuit, AccessHint, LocalBlockReader, ScDecisionCtx, ShortCircuitConfig,
    ShortCircuitError, ShortCircuitFactory,
};
