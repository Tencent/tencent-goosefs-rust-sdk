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

//! Pass-through metadata helpers used when `metadata-cache` is disabled.

use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::proto::grpc::file::{FileInfo, LoadMetadataPType};

/// Uninhabited cache handle for the feature-disabled build.
pub struct MetadataCache;

impl MetadataCache {
    pub fn maybe_new(_ttl: Duration, _capacity: usize) -> Option<Arc<Self>> {
        None
    }

    pub fn invalidate(&self, _path: &str) {}

    pub fn invalidate_with_parent(&self, _path: &str) {}

    pub fn ttl(&self) -> Duration {
        Duration::ZERO
    }
}

pub fn normalize_path(path: &str) -> Arc<str> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return Arc::from("/");
    }
    let stripped = trimmed.trim_end_matches('/');
    if stripped.is_empty() {
        return Arc::from("/");
    }
    if stripped.starts_with('/') {
        Arc::from(stripped)
    } else {
        Arc::from(format!("/{stripped}"))
    }
}

pub fn parent_path(path: &str) -> Option<String> {
    let normalized = normalize_path(path);
    if &*normalized == "/" {
        return None;
    }
    let last_slash = normalized.rfind('/')?;
    if last_slash == 0 {
        Some("/".to_string())
    } else {
        Some(normalized[..last_slash].to_string())
    }
}

pub fn should_skip_listing_cache(
    recursive: bool,
    load_type: LoadMetadataPType,
    load_metadata_only: bool,
    sync_interval_ms: i64,
) -> bool {
    recursive
        || load_type == LoadMetadataPType::Always
        || load_metadata_only
        || sync_interval_ms == 0
}

pub fn invalidate_on_success<T>(
    _cache: Option<&MetadataCache>,
    _path: &str,
    result: Result<T>,
) -> Result<T> {
    result
}

pub fn invalidate_rename_on_success<T>(
    _cache: Option<&MetadataCache>,
    _src: &str,
    _dst: &str,
    result: Result<T>,
) -> Result<T> {
    result
}

pub async fn get_status_through_cache<F, Fut>(
    _cache: Option<&MetadataCache>,
    _path: &str,
    _sync_interval_ms: i64,
    fetch: F,
) -> Result<FileInfo>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<FileInfo>>,
{
    fetch().await
}

pub async fn list_status_through_cache<F, Fut>(
    _cache: Option<&MetadataCache>,
    _path: &str,
    _skip: bool,
    fetch: F,
) -> Result<Vec<FileInfo>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<FileInfo>>>,
{
    fetch().await
}
