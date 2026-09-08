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

//! JNI binding for `goosefs-sdk`.
//!
//! Thin wrapper: protocol, cache, routing, and retry stay in the SDK crate.

mod async_fs;
mod batch;
mod config;
mod convert;
mod error;
mod executor;
mod handle;
mod options;
mod positioned_read;
mod status;
mod streaming;
mod sync_fs;
mod tracing_bridge;
mod worker;

pub(crate) use error::{Error, Result};
