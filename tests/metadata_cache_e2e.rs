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

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn list_status_sync_zero_skips_cache() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    fs.write_file(&format!("{root}/a.bin"), b"a", write_opts())
        .await?;

    let opts = ListStatusOptions {
        sync_interval_ms: Some(0),
        ..Default::default()
    };
    let before = list_status_ops();
    let _ = fs.list_status_with_options(&root, opts.clone()).await?;
    let _ = fs.list_status_with_options(&root, opts).await?;
    let delta = list_status_ops() - before;
    eprintln!("[list sync=0] list_status x2 → RPC={delta}");
    assert_eq!(delta, 2, "sync=0 must skip listing cache");

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn exists_reuses_get_status_cache() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    let path = format!("{root}/file.bin");
    fs.write_file(&path, b"exists-cache", write_opts()).await?;

    let before = get_status_ops();
    assert!(fs.exists(&path).await?);
    assert!(fs.exists(&path).await?);
    let delta = get_status_ops() - before;
    eprintln!("[exists] present x2 → RPC={delta}");
    assert_eq!(delta, 1, "second exists must reuse the cached get_status");

    let missing = format!("{root}/missing.bin");
    let miss_before = get_status_ops();
    assert!(!fs.exists(&missing).await?);
    assert!(!fs.exists(&missing).await?);
    let miss_delta = get_status_ops() - miss_before;
    eprintln!("[exists] missing x2 → RPC={miss_delta}");
    assert_eq!(
        miss_delta, 1,
        "NotFound must be negatively cached for exists"
    );

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn delete_invalidates_parent_listing() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    let child = format!("{root}/gone.bin");
    fs.write_file(&format!("{root}/keep.bin"), b"keep", write_opts())
        .await?;
    fs.write_file(&child, b"gone", write_opts()).await?;

    let before = list_status_ops();
    let first = fs.list_status(&root, false).await?;
    let _ = fs.list_status(&root, false).await?;
    assert_eq!(list_status_ops() - before, 1);
    assert!(first.len() >= 2);

    fs.delete(&child, DeleteOptions::default()).await?;
    let after = fs.list_status(&root, false).await?;
    eprintln!(
        "[delete] parent listing before={} after={}",
        first.len(),
        after.len()
    );
    assert_eq!(
        after.len(),
        first.len() - 1,
        "delete must invalidate the parent listing"
    );

    let gs_before = get_status_ops();
    let gone = fs.get_status(&child).await;
    let gs_delta = get_status_ops() - gs_before;
    assert!(gone.unwrap_err().is_not_found());
    assert_eq!(gs_delta, 1, "deleted path must not stay as Present");

    cleanup(&fs, &root).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master (metadata cache e2e)"]
async fn rename_invalidates_src_dst_and_parents() -> Result<()> {
    let fs = connect(true).await?;
    let root = unique_root();
    let src_dir = format!("{root}/src");
    let dst_dir = format!("{root}/dst");
    fs.mkdir(&src_dir, true).await?;
    fs.mkdir(&dst_dir, true).await?;
    let src = format!("{src_dir}/file.bin");
    let dst = format!("{dst_dir}/file.bin");
    fs.write_file(&src, b"renamed", write_opts()).await?;

    let src_before = list_status_ops();
    let src_list = fs.list_status(&src_dir, false).await?;
    let _ = fs.list_status(&src_dir, false).await?;
    assert_eq!(list_status_ops() - src_before, 1);
    assert_eq!(src_list.len(), 1);

    let dst_before = list_status_ops();
    let dst_list = fs.list_status(&dst_dir, false).await?;
    let _ = fs.list_status(&dst_dir, false).await?;
    assert_eq!(list_status_ops() - dst_before, 1);
    assert_eq!(dst_list.len(), 0);

    fs.rename(&src, &dst).await?;

    let after_src = fs.list_status(&src_dir, false).await?;
    let after_dst = fs.list_status(&dst_dir, false).await?;
    eprintln!(
        "[rename] src listing {} → {}, dst listing {} → {}",
        src_list.len(),
        after_src.len(),
        dst_list.len(),
        after_dst.len()
    );
    assert_eq!(
        after_src.len(),
        0,
        "src parent listing must miss after rename"
    );
    assert_eq!(
        after_dst.len(),
        1,
        "dst parent listing must miss after rename"
    );

    let gs_before = get_status_ops();
    assert!(fs.get_status(&src).await.unwrap_err().is_not_found());
    let moved = fs.get_status(&dst).await?;
    let gs_delta = get_status_ops() - gs_before;
    assert_eq!(moved.length, b"renamed".len() as i64);
    assert_eq!(
        gs_delta, 2,
        "rename must invalidate src and dst status slots"
    );

    cleanup(&fs, &root).await;
    Ok(())
}
