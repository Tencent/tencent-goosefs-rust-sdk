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

//! Live-cluster checks for the Java-aligned metadata cache.
//!
//! RPC counts come from `Client.GetStatusOps` / `Client.ListStatusOps`, which
//! only increment on real Master RPCs (cache hits are not counted).
//!
//! Ignored by default — needs a live master. Run:
//! ```bash
//! GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=simple \
//!   cargo test --test metadata_cache_e2e -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::config::{GoosefsConfig, WriteType};
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::{
    CreateFileOptions, DeleteOptions, GetStatusOptions, ListStatusOptions, OpenFileOptions,
};
use goosefs_sdk::fs::{BaseFileSystem, FileSystem};
use goosefs_sdk::io::GoosefsFileInStream;
use goosefs_sdk::metrics;
use goosefs_sdk::proto::grpc::file::LoadMetadataPType;

fn master_addr() -> String {
    std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
}

fn auth_type() -> AuthType {
    match std::env::var("GOOSEFS_AUTH_TYPE") {
        Ok(s) => s.parse::<AuthType>().unwrap_or(AuthType::Simple),
        Err(_) => AuthType::Simple,
    }
}

fn unique_root() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/sdk-metadata-cache-e2e/{}_{ts}", std::process::id())
}

fn config(cache_enabled: bool) -> GoosefsConfig {
    let mut config = GoosefsConfig::new(master_addr())
        .with_metadata_cache_enabled(cache_enabled)
        .with_metrics_enabled(false);
    config.auth_type = auth_type();
    if let Ok(user) = std::env::var("GOOSEFS_AUTH_USERNAME") {
        config.auth_username = user;
    } else if let Ok(user) = std::env::var("USER") {
        config.auth_username = user;
    }
    config
}

async fn connect(cache_enabled: bool) -> Result<Arc<BaseFileSystem>> {
    let ctx = FileSystemContext::connect(config(cache_enabled)).await?;
    Ok(BaseFileSystem::from_context(ctx))
}

fn get_status_ops() -> i64 {
    metrics::counter(metrics::name::CLIENT_GET_STATUS_OPS).get()
}

fn list_status_ops() -> i64 {
    metrics::counter(metrics::name::CLIENT_LIST_STATUS_OPS).get()
}

fn write_opts() -> CreateFileOptions {
    let mut opts = CreateFileOptions::with_write_type(WriteType::MustCache);
    opts.recursive = true;
    opts
}

async fn cleanup(fs: &BaseFileSystem, root: &str) {
    let _ = fs.delete(root, DeleteOptions::recursive()).await;
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn cache_disabled_get_status_always_rpcs() -> Result<()> {
    let fs = connect(false).await?;
    let root = unique_root();
    let path = format!("{root}/file.bin");
    fs.write_file(&path, b"hello-disabled", write_opts())
        .await?;

    let before = get_status_ops();
    let a = fs.get_status(&path).await?;
    let b = fs.get_status(&path).await?;
    let delta = get_status_ops() - before;
    eprintln!("[disabled] get_status x2 → RPC={delta} length={}", a.length);
    assert_eq!(a.length, b.length);
    assert_eq!(delta, 2, "INV-MC-S8: cache off must RPC every get_status");

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn cache_enabled_get_status_second_is_hit() -> Result<()> {
    let fs = connect(true).await?;
    assert!(
        fs.context().acquire_metadata_cache().is_some(),
        "enabled=true must construct MetadataCache"
    );
    let root = unique_root();
    let path = format!("{root}/file.bin");
    fs.write_file(&path, b"hello-enabled", write_opts()).await?;

    let before = get_status_ops();
    let a = fs.get_status(&path).await?;
    let b = fs.get_status(&path).await?;
    let delta = get_status_ops() - before;
    eprintln!("[enabled] get_status x2 → RPC={delta} length={}", a.length);
    assert_eq!(a.length, b.length);
    assert_eq!(a.length, b"hello-enabled".len() as i64);
    assert_eq!(delta, 1, "second get_status must be served from cache");

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn negative_cache_then_create() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    fs.mkdir(&root, true).await?;
    let missing = format!("{root}/not-there.bin");

    let before = get_status_ops();
    let first = fs.get_status(&missing).await;
    let second = fs.get_status(&missing).await;
    let delta = get_status_ops() - before;
    eprintln!("[neg-cache] missing get x2 → RPC={delta} first={first:?} second={second:?}");
    assert!(first.unwrap_err().is_not_found());
    assert!(second.unwrap_err().is_not_found());
    assert_eq!(delta, 1, "NotFound must be negatively cached");

    fs.write_file(&missing, b"now-exists", write_opts()).await?;
    let after_create = get_status_ops();
    let got = fs.get_status(&missing).await?;
    let create_delta = get_status_ops() - after_create;
    eprintln!(
        "[neg-cache] after create get_status → RPC={create_delta} length={}",
        got.length
    );
    assert_eq!(got.length, b"now-exists".len() as i64);
    assert_eq!(
        create_delta, 1,
        "create must invalidate NotFound so the next get RPCs"
    );

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn list_status_cached_unless_recursive_or_always() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    fs.write_file(&format!("{root}/a.bin"), b"a", write_opts())
        .await?;
    fs.write_file(&format!("{root}/b.bin"), b"bb", write_opts())
        .await?;

    let before = list_status_ops();
    let first = fs.list_status(&root, false).await?;
    let second = fs.list_status(&root, false).await?;
    let delta = list_status_ops() - before;
    eprintln!("[list] non-recursive x2 → RPC={delta} n={}", first.len());
    assert_eq!(first.len(), second.len());
    assert!(first.len() >= 2);
    assert_eq!(delta, 1, "non-recursive list_status must cache");

    let child = &first[0].path;
    let gs_before = get_status_ops();
    let _ = fs.get_status(child).await?;
    let child_delta = get_status_ops() - gs_before;
    eprintln!("[list] get_status(child after list) → RPC={child_delta}");
    assert_eq!(
        child_delta, 1,
        "INV-MC-S6: listing must not populate child status slots"
    );

    let rec_before = list_status_ops();
    let _ = fs.list_status(&root, true).await?;
    let _ = fs.list_status(&root, true).await?;
    let rec_delta = list_status_ops() - rec_before;
    eprintln!("[list] recursive x2 → RPC={rec_delta}");
    assert!(
        rec_delta >= 2,
        "recursive listing must skip cache, got RPC={rec_delta}"
    );

    let always_before = list_status_ops();
    let always = ListStatusOptions {
        load_metadata_type: Some(LoadMetadataPType::Always),
        ..Default::default()
    };
    let _ = fs.list_status_with_options(&root, always.clone()).await?;
    let _ = fs.list_status_with_options(&root, always).await?;
    let always_delta = list_status_ops() - always_before;
    eprintln!("[list] ALWAYS x2 → RPC={always_delta}");
    assert_eq!(always_delta, 2, "ALWAYS must skip listing cache");

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn sync_interval_zero_skips_cache() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    let path = format!("{root}/file.bin");
    fs.write_file(&path, b"sync0", write_opts()).await?;

    let opts = GetStatusOptions::always_sync();
    let before = get_status_ops();
    let _ = fs.get_status_with_options(&path, opts.clone()).await?;
    let _ = fs.get_status_with_options(&path, opts).await?;
    let delta = get_status_ops() - before;
    eprintln!("[sync=0] get_status x2 → RPC={delta}");
    assert_eq!(delta, 2);

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn mkdir_invalidates_parent_listing() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    fs.mkdir(&root, true).await?;

    let before = list_status_ops();
    let first = fs.list_status(&root, false).await?;
    let _ = fs.list_status(&root, false).await?;
    assert_eq!(list_status_ops() - before, 1);
    let n0 = first.len();

    fs.mkdir(&format!("{root}/child"), false).await?;
    let after = fs.list_status(&root, false).await?;
    eprintln!(
        "[mkdir] parent listing before={} after={} (must miss cache)",
        n0,
        after.len()
    );
    assert_eq!(after.len(), n0 + 1, "parent listing must see the new dir");

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn open_reuses_get_status_cache() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    let path = format!("{root}/file.bin");
    fs.write_file(&path, b"open-cache", write_opts()).await?;

    let _ = fs.get_status(&path).await?;
    let before = get_status_ops();
    let mut stream = GoosefsFileInStream::open_with_context(
        fs.context().clone(),
        &path,
        OpenFileOptions::default(),
    )
    .await?;
    let bytes = stream.read_all().await?;
    let delta = get_status_ops() - before;
    eprintln!(
        "[open] after get_status, open+read → extra getStatus RPC={delta} bytes={}",
        bytes.len()
    );
    assert_eq!(&bytes[..], b"open-cache");
    assert_eq!(delta, 0, "open must reuse the cached get_status");

    cleanup(&fs, &root).await;
    Ok(())
}
