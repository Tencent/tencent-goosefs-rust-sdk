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

//! CI coverage for every goosefs-sdk API that Apache OpenDAL's GooseFS
//! service (`opendal-src/core/services/goosefs`, GooseFS **v2.1.0.1**) calls.
//!
//! OpenDAL never talks to Master/Worker gRPC itself — it only uses:
//!
//! | Layer | SDK API |
//! |-------|---------|
//! | Config | `from_properties_auto`, `with_auth_type_str`, `with_auth_username`, `validate`, field overlay (`master_addr` / `master_addrs` / `block_size` / `chunk_size` / `write_type` / `root`) |
//! | Context | `FileSystemContext::connect`, `acquire_master`, `invalidate_file_info` |
//! | Master | `create_directory(path, true)`, `get_status`, `list_status(path, false)`, `delete(path, false)`, `rename` |
//! | Writer | `GoosefsFileWriter::create_with_context`, `write`, `close`, `cancel`, `file_info().file_id` |
//! | Reader | `open_with_context`, `open_range_with_context`, `read_next_block`, `read_file_with_context`, `read_range_with_context` |
//! | Error / FileInfo | `NotFound`, `AlreadyExists`, `is_authentication_failed`, `folder` / `length` / `last_modification_time_ms` / `file_id` / `path` |
//!
//! Cluster tests are `#[ignore]` (need GooseFS v2.1.0.1). Run:
//! ```bash
//! GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=simple \
//!   cargo test --test opendal_sdk_api -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::{Error, Result};
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::proto::grpc::file::FileInfo;

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique(tag: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/sdk-opendal-api/{tag}_{}_{ts}", std::process::id())
}

fn sibling_tmp(path: &str) -> String {
    let (dir, base) = match path.rfind('/') {
        Some(idx) => (&path[..idx], &path[idx + 1..]),
        None => ("", path),
    };
    let pid = std::process::id();
    let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    if dir.is_empty() {
        format!(".opendal.tmp.{pid}.{counter}.{nanos}.{base}")
    } else {
        format!("{dir}/.opendal.tmp.{pid}.{counter}.{nanos}.{base}")
    }
}

/// Same config construction OpenDAL `GoosefsBuilder::build` uses.
fn opendal_style_config() -> GoosefsConfig {
    let mut cfg = GoosefsConfig::from_properties_auto().expect("from_properties_auto");
    if let Ok(addr) = std::env::var("GOOSEFS_MASTER_ADDR") {
        let addrs: Vec<String> = addr
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if addrs.len() == 1 {
            cfg.master_addr = addrs[0].clone();
            cfg.master_addrs.clear();
        } else if addrs.len() > 1 {
            cfg.master_addr = addrs[0].clone();
            cfg.master_addrs = addrs;
        }
    }
    if let Ok(auth) = std::env::var("GOOSEFS_AUTH_TYPE") {
        cfg = cfg.with_auth_type_str(&auth).expect("with_auth_type_str");
    } else {
        cfg = cfg
            .with_auth_type_str("simple")
            .expect("with_auth_type_str");
    }
    if let Ok(user) = std::env::var("GOOSEFS_AUTH_USERNAME") {
        cfg = cfg.with_auth_username(user);
    } else if let Ok(user) = std::env::var("USER") {
        cfg = cfg.with_auth_username(user);
    }
    cfg.root = "/".to_string();
    cfg.write_type = Some(1); // MUST_CACHE — OpenDAL default
    cfg.validate().expect("validate");
    cfg
}

async fn connect() -> Result<Arc<FileSystemContext>> {
    FileSystemContext::connect(opendal_style_config()).await
}

async fn mkdir_p(ctx: &Arc<FileSystemContext>, path: &str) -> Result<()> {
    ctx.acquire_master().create_directory(path, true).await
}

/// OpenDAL `GoosefsCore::delete`: NotFound is success (idempotent).
async fn delete_idempotent(ctx: &Arc<FileSystemContext>, path: &str) -> Result<()> {
    ctx.invalidate_file_info(path);
    match ctx.acquire_master().delete(path, false).await {
        Ok(()) => Ok(()),
        Err(Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn write_via_sdk(
    ctx: &Arc<FileSystemContext>,
    path: &str,
    payload: &[u8],
) -> Result<FileInfo> {
    let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
    if !payload.is_empty() {
        w.write(payload).await?;
    }
    w.close().await?;
    Ok(w.file_info().clone())
}

/// OpenDAL `GoosefsReadStream`: loop `read_next_block` until `None`.
async fn read_streaming(ctx: &Arc<FileSystemContext>, path: &str) -> Result<Vec<u8>> {
    let mut reader = GoosefsFileReader::open_with_context(ctx.clone(), path).await?;
    let mut out = Vec::new();
    while let Some(block) = reader.read_next_block().await? {
        out.extend_from_slice(&block);
    }
    Ok(out)
}

async fn read_range_streaming(
    ctx: &Arc<FileSystemContext>,
    path: &str,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>> {
    let mut reader =
        GoosefsFileReader::open_range_with_context(ctx.clone(), path, offset, length).await?;
    let mut out = Vec::new();
    while let Some(block) = reader.read_next_block().await? {
        out.extend_from_slice(&block);
    }
    Ok(out)
}

fn assert_file_info_fields(info: &FileInfo, expect_folder: bool) {
    assert_eq!(
        info.folder.unwrap_or(false),
        expect_folder,
        "FileInfo.folder (OpenDAL EntryMode)"
    );
    if !expect_folder {
        assert!(info.length.is_some(), "FileInfo.length (content_length)");
        assert!(info.file_id.is_some(), "FileInfo.file_id (OpenDAL etag)");
    }
    assert!(
        info.path.as_deref().is_some_and(|p| !p.is_empty()),
        "FileInfo.path (list relative path)"
    );
    assert!(
        info.last_modification_time_ms.is_some(),
        "FileInfo.last_modification_time_ms (last_modified)"
    );
}

// ── Hermetic (unit CI, no cluster) ───────────────────────────────────────────

#[test]
fn opendal_config_from_properties_auto_validate() {
    let cfg = GoosefsConfig::from_properties_auto().expect("from_properties_auto");
    // Defaults always include a master address, so validate succeeds even
    // without GOOSEFS_MASTER_ADDR (OpenDAL then overlays / fails if blank).
    cfg.validate().expect("default+env config validates");
}

#[test]
fn opendal_config_ha_overlay_matches_builder() {
    let mut cfg = GoosefsConfig::from_properties_auto().expect("from_properties_auto");
    let addrs = vec![
        "10.0.0.1:9200".to_string(),
        "10.0.0.2:9200".to_string(),
        "10.0.0.3:9200".to_string(),
    ];
    cfg.master_addr = addrs[0].clone();
    cfg.master_addrs = addrs;
    cfg.block_size = 4 * 1024 * 1024;
    cfg.chunk_size = 256 * 1024;
    cfg.write_type = Some(3); // CACHE_THROUGH
    cfg.validate().expect("HA overlay validates");
    assert!(cfg.is_multi_master());
}

#[test]
fn opendal_error_is_authentication_failed_is_the_retry_discriminant() {
    let auth = Error::AuthenticationFailed {
        message: "sasl expired".into(),
    };
    assert!(
        auth.is_authentication_failed(),
        "OpenDAL GoosefsCore::open_reader retries only on is_authentication_failed"
    );
    assert!(!Error::NotFound { path: "/x".into() }.is_authentication_failed());
    assert!(!Error::AlreadyExists { path: "/x".into() }.is_authentication_failed());
}

// ── Cluster: context + metadata (create_dir / stat / list / delete) ──────────

#[tokio::test]
#[ignore = "Requires GooseFS v2.1.0.1 (OpenDAL metadata APIs)"]
async fn opendal_connect_mkdir_stat_list_delete() -> Result<()> {
    let ctx = connect().await?;
    let master = ctx.acquire_master();
    let same = ctx.acquire_master();
    assert!(
        Arc::ptr_eq(&master, &same),
        "acquire_master must reuse the shared Master client"
    );

    let root = unique("meta");
    mkdir_p(&ctx, &root).await?;
    mkdir_p(&ctx, &root).await?; // OpenDAL create_dir_existing: allow_exists

    let dir_info = master.get_status(&root).await?;
    assert_file_info_fields(&dir_info, true);

    let child_dir = format!("{root}/nested");
    mkdir_p(&ctx, &child_dir).await?;
    let file = format!("{root}/nested/hello.bin");
    let payload = b"opendal-stat-list";
    let _ = write_via_sdk(&ctx, &file, payload).await?;

    let file_info = master.get_status(&file).await?;
    assert_file_info_fields(&file_info, false);
    assert_eq!(file_info.length.unwrap() as usize, payload.len());

    let missing = format!("{root}/no-such");
    let err = master.get_status(&missing).await.expect_err("stat missing");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "get_status missing → NotFound, got {err:?}"
    );

    // list_status(path, false) — OpenDAL lister; GooseFS returns children only.
    let children = master.list_status(&root, false).await?;
    let names: Vec<String> = children.iter().filter_map(|e| e.path.clone()).collect();
    assert!(
        names
            .iter()
            .any(|p| p.ends_with("/nested") || p.ends_with("nested")),
        "list_status must include nested dir, got {names:?}"
    );
    assert!(
        !names.iter().any(|p| p == &root),
        "list_status must not include the directory itself (OpenDAL synthesizes it)"
    );
    for info in &children {
        assert_file_info_fields(info, info.folder.unwrap_or(false));
    }

    let empty = format!("{root}/empty");
    mkdir_p(&ctx, &empty).await?;
    let empty_kids = master.list_status(&empty, false).await?;
    assert!(
        empty_kids.is_empty(),
        "empty dir children must be empty, got {empty_kids:?}"
    );

    let list_missing = master.list_status(&missing, false).await;
    assert!(
        matches!(list_missing, Err(Error::NotFound { .. })),
        "list_status missing dir → NotFound (OpenDAL maps this to empty page), got {list_missing:?}"
    );

    // GooseFS 2.0 dropped ListStatusPOptions.recursive; MasterClient BFS
    // must still return the full subtree (nested file + dirs).
    let rec = master.list_status(&root, true).await?;
    let rec_names: Vec<String> = rec.iter().filter_map(|e| e.path.clone()).collect();
    assert!(
        rec_names
            .iter()
            .any(|p| p.ends_with("hello.bin") || p.ends_with("/hello.bin")),
        "recursive list_status must include nested file, got {rec_names:?}"
    );
    assert!(
        rec_names
            .iter()
            .any(|p| p.ends_with("/nested") || p.ends_with("nested")),
        "recursive list_status must include nested dir, got {rec_names:?}"
    );
    assert!(
        !rec_names.iter().any(|p| p == &root),
        "recursive list must not re-emit the starting directory (BFS self-loop), got {rec_names:?}"
    );

    delete_idempotent(&ctx, &file).await?;
    let gone = master.get_status(&file).await;
    assert!(matches!(gone, Err(Error::NotFound { .. })));
    delete_idempotent(&ctx, &file).await?; // OpenDAL idempotent delete

    let _ = master.delete(&root, true).await;
    Ok(())
}

// ── Cluster: write / read / range / empty / multi-chunk ─────────────────────

#[tokio::test]
#[ignore = "Requires GooseFS v2.1.0.1 (OpenDAL I/O APIs)"]
async fn opendal_write_read_range_and_empty() -> Result<()> {
    let ctx = connect().await?;
    let root = unique("io");
    mkdir_p(&ctx, &root).await?;

    let path = format!("{root}/full.bin");
    let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let info = write_via_sdk(&ctx, &path, &payload).await?;
    assert!(info.file_id.is_some(), "etag file_id after close");

    let streamed = read_streaming(&ctx, &path).await?;
    assert_eq!(streamed, payload, "open_with_context + read_next_block");

    let oneshot = GoosefsFileReader::read_file_with_context(ctx.clone(), &path).await?;
    assert_eq!(&oneshot[..], payload.as_slice(), "read_file_with_context");

    let range = read_range_streaming(&ctx, &path, 100, 50).await?;
    assert_eq!(range, &payload[100..150], "open_range_with_context");

    let oneshot_range =
        GoosefsFileReader::read_range_with_context(ctx.clone(), &path, 100, 50).await?;
    assert_eq!(&oneshot_range[..], &payload[100..150]);

    // Unbounded-offset tail: OpenDAL get_status then open_range(off, len).
    let file_len = ctx
        .acquire_master()
        .get_status(&path)
        .await?
        .length
        .unwrap() as u64;
    let off = 4000u64;
    let tail_len = file_len.saturating_sub(off);
    let tail = read_range_streaming(&ctx, &path, off, tail_len).await?;
    assert_eq!(tail, &payload[off as usize..]);

    // Empty tail (offset == length, length 0) — OpenDAL short-circuit.
    let empty_tail = read_range_streaming(&ctx, &path, file_len, 0).await?;
    assert!(empty_tail.is_empty());

    // Empty object: OpenDAL close() with no write().
    let empty_path = format!("{root}/empty.bin");
    let empty_info = write_via_sdk(&ctx, &empty_path, b"").await?;
    assert!(empty_info.file_id.is_some());
    let empty_body = read_streaming(&ctx, &empty_path).await?;
    assert!(empty_body.is_empty());
    let empty_stat = ctx.acquire_master().get_status(&empty_path).await?;
    assert_eq!(empty_stat.length.unwrap_or(0), 0);

    // Multi-chunk write (OpenDAL iterates Buffer segments).
    let multi_path = format!("{root}/multi.bin");
    let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), &multi_path, None).await?;
    w.write(b"aaa").await?;
    w.write(b"bbb").await?;
    w.write(b"ccc").await?;
    w.close().await?;
    let multi = read_streaming(&ctx, &multi_path).await?;
    assert_eq!(multi, b"aaabbbccc");

    let _ = ctx.acquire_master().delete(&root, true).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS v2.1.0.1 (multi-block streaming)"]
async fn opendal_multi_block_read_next_block() -> Result<()> {
    let mut cfg = opendal_style_config();
    cfg.block_size = 64 * 1024;
    cfg.chunk_size = 16 * 1024;
    cfg.validate().expect("small block config");
    let ctx = FileSystemContext::connect(cfg).await?;

    let root = unique("multiblock");
    mkdir_p(&ctx, &root).await?;
    let path = format!("{root}/two-blocks.bin");
    let payload: Vec<u8> = (0..(64 * 1024 + 1024)).map(|i| (i % 251) as u8).collect();

    let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), &path, None).await?;
    w.write(&payload).await?;
    w.close().await?;

    let mut reader = GoosefsFileReader::open_with_context(ctx.clone(), &path).await?;
    let mut blocks = 0usize;
    let mut out = Vec::new();
    while let Some(block) = reader.read_next_block().await? {
        blocks += 1;
        out.extend_from_slice(&block);
    }
    assert!(
        blocks >= 2,
        "OpenDAL streams per-block; expected >= 2 blocks, got {blocks}"
    );
    assert_eq!(out, payload);

    let _ = ctx.acquire_master().delete(&root, true).await;
    Ok(())
}

// ── Cluster: rename (no-replace, overwrite-via-delete, nested parent) ────────

#[tokio::test]
#[ignore = "Requires GooseFS v2.1.0.1 (OpenDAL rename APIs)"]
async fn opendal_rename_no_replace_overwrite_and_nested() -> Result<()> {
    let ctx = connect().await?;
    let master = ctx.acquire_master();
    let root = unique("rename");
    mkdir_p(&ctx, &root).await?;

    // Success when dst is absent.
    let src_ok = format!("{root}/ok-src.bin");
    let dst_ok = format!("{root}/ok-dst.bin");
    write_via_sdk(&ctx, &src_ok, b"rename-me").await?;
    ctx.invalidate_file_info(&src_ok);
    ctx.invalidate_file_info(&dst_ok);
    master.rename(&src_ok, &dst_ok).await?;
    assert!(matches!(
        master.get_status(&src_ok).await,
        Err(Error::NotFound { .. })
    ));
    assert_eq!(read_streaming(&ctx, &dst_ok).await?, b"rename-me");

    // file_id is preserved across Master rename (OpenDAL etag).
    let src_id = format!("{root}/id-src.bin");
    let dst_id = format!("{root}/id-dst.bin");
    let before = write_via_sdk(&ctx, &src_id, b"etag").await?;
    master.rename(&src_id, &dst_id).await?;
    let after = master.get_status(&dst_id).await?;
    assert_eq!(
        before.file_id, after.file_id,
        "Master rename keeps inode id (OpenDAL etag)"
    );

    // No-replace: dst exists → AlreadyExists, dst bytes unchanged.
    let src_nr = format!("{root}/nr-src.bin");
    let dst_nr = format!("{root}/nr-dst.bin");
    write_via_sdk(&ctx, &src_nr, b"from-src").await?;
    write_via_sdk(&ctx, &dst_nr, b"dst-original").await?;
    let err = master
        .rename(&src_nr, &dst_nr)
        .await
        .expect_err("no-replace");
    assert!(
        matches!(err, Error::AlreadyExists { .. }),
        "expected AlreadyExists, got {err:?}"
    );
    assert_eq!(read_streaming(&ctx, &dst_nr).await?, b"dst-original");
    assert_eq!(read_streaming(&ctx, &src_nr).await?, b"from-src");

    // OpenDAL overwrite path: delete dst then rename. The reads above cached
    // both entries, and raw Master calls bypass the client's write-path
    // invalidation, so drop them the way `GoosefsCore::rename` does after a
    // successful rename — otherwise the open below serves pre-rename metadata.
    master.delete(&dst_nr, false).await?;
    master.rename(&src_nr, &dst_nr).await?;
    ctx.invalidate_file_info(&src_nr);
    ctx.invalidate_file_info(&dst_nr);
    assert_eq!(read_streaming(&ctx, &dst_nr).await?, b"from-src");

    // Nested dst: create_directory(parent, true) then rename.
    let src_nest = format!("{root}/nest-src.bin");
    let nest_parent = format!("{root}/a/b");
    let dst_nest = format!("{root}/a/b/c.bin");
    write_via_sdk(&ctx, &src_nest, b"nested").await?;
    mkdir_p(&ctx, &nest_parent).await?;
    master.rename(&src_nest, &dst_nest).await?;
    assert_eq!(read_streaming(&ctx, &dst_nest).await?, b"nested");

    // Destination is a directory — OpenDAL get_status + folder check.
    let src_dir = format!("{root}/onto-dir.bin");
    let dst_dir = format!("{root}/existing-dir");
    write_via_sdk(&ctx, &src_dir, b"x").await?;
    mkdir_p(&ctx, &dst_dir).await?;
    let dst_stat = master.get_status(&dst_dir).await?;
    assert!(
        dst_stat.folder.unwrap_or(false),
        "OpenDAL maps this to IsADirectory and must not rename"
    );

    // Missing source → NotFound.
    let missing_src = format!("{root}/no-src.bin");
    let missing_dst = format!("{root}/no-dst.bin");
    let err = master
        .rename(&missing_src, &missing_dst)
        .await
        .expect_err("rename missing src");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "rename missing src → NotFound, got {err:?}"
    );

    let _ = master.delete(&root, true).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS v2.1.0.1 (concurrent no-replace rename)"]
async fn opendal_concurrent_rename_exactly_one_wins() -> Result<()> {
    let ctx = connect().await?;
    let master = ctx.acquire_master();
    let root = unique("race");
    mkdir_p(&ctx, &root).await?;
    let dst = format!("{root}/final.bin");

    let src_a = format!("{root}/a.bin");
    let src_b = format!("{root}/b.bin");
    write_via_sdk(&ctx, &src_a, b"A").await?;
    write_via_sdk(&ctx, &src_b, b"B").await?;

    let master_a = master.clone();
    let master_b = master.clone();
    let src_a_c = src_a.clone();
    let src_b_c = src_b.clone();
    let dst_a = dst.clone();
    let dst_b = dst.clone();

    let (ra, rb) = tokio::join!(
        async move { master_a.rename(&src_a_c, &dst_a).await },
        async move { master_b.rename(&src_b_c, &dst_b).await },
    );

    match (&ra, &rb) {
        (Ok(()), Err(Error::AlreadyExists { .. })) | (Err(Error::AlreadyExists { .. }), Ok(())) => {
        }
        (Ok(()), Ok(())) => panic!("both concurrent renames succeeded"),
        other => panic!("unexpected concurrent rename outcome: {other:?}"),
    }

    let body = read_streaming(&ctx, &dst).await?;
    assert!(body == b"A" || body == b"B", "winner content, got {body:?}");

    let _ = master.delete(&root, true).await;
    Ok(())
}

// ── Cluster: OpenDAL write-via-temp + abort ──────────────────────────────────

#[tokio::test]
#[ignore = "Requires GooseFS v2.1.0.1 (OpenDAL write-via-temp / abort)"]
async fn opendal_write_via_temp_rename_and_abort() -> Result<()> {
    let ctx = connect().await?;
    let master = ctx.acquire_master();
    let root = unique("tmpwrite");
    mkdir_p(&ctx, &root).await?;

    // Publish: write tmp → close → rename(tmp, final). file_id is etag.
    let final_path = format!("{root}/published.bin");
    let tmp = sibling_tmp(&final_path);
    let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), &tmp, None).await?;
    w.write(b"published-body").await?;
    w.close().await?;
    let etag = w.file_info().file_id;
    assert!(etag.is_some());
    ctx.invalidate_file_info(&tmp);
    ctx.invalidate_file_info(&final_path);
    master.rename(&tmp, &final_path).await?;
    assert_eq!(read_streaming(&ctx, &final_path).await?, b"published-body");
    let published = master.get_status(&final_path).await?;
    assert_eq!(published.file_id, etag);

    // if_not_exists publish against existing dst → AlreadyExists; dst unchanged.
    let tmp2 = sibling_tmp(&final_path);
    write_via_sdk(&ctx, &tmp2, b"loser").await?;
    let err = master
        .rename(&tmp2, &final_path)
        .await
        .expect_err("Create must fail");
    assert!(matches!(err, Error::AlreadyExists { .. }));
    assert_eq!(read_streaming(&ctx, &final_path).await?, b"published-body");
    delete_idempotent(&ctx, &tmp2).await?;

    // abort: cancel writer + delete tmp; final path never touched.
    let abort_final = format!("{root}/never-published.bin");
    let abort_tmp = sibling_tmp(&abort_final);
    let mut aw = GoosefsFileWriter::create_with_context(ctx.clone(), &abort_tmp, None).await?;
    aw.write(b"should-not-publish").await?;
    aw.cancel().await?;
    delete_idempotent(&ctx, &abort_tmp).await?;
    let abort_stat = master.get_status(&abort_final).await;
    assert!(
        matches!(abort_stat, Err(Error::NotFound { .. })),
        "abort must not create the final path, got {abort_stat:?}"
    );
    let tmp_stat = master.get_status(&abort_tmp).await;
    assert!(
        matches!(tmp_stat, Err(Error::NotFound { .. })),
        "cancelled tmp inode must be gone, got {tmp_stat:?}"
    );

    let _ = master.delete(&root, true).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS v2.1.0.1 (invalidate_file_info)"]
async fn opendal_invalidate_file_info_after_mutation() -> Result<()> {
    let mut cfg = opendal_style_config();
    cfg.metadata_cache_enabled = true;
    let ctx = FileSystemContext::connect(cfg).await?;
    let master = ctx.acquire_master();
    let root = unique("inv");
    mkdir_p(&ctx, &root).await?;
    let path = format!("{root}/cached.bin");
    write_via_sdk(&ctx, &path, b"v1").await?;

    // Warm the context metadata cache (OpenDAL readers go through
    // `open_with_context` → `get_file_info_cached`).
    let warm = read_streaming(&ctx, &path).await?;
    assert_eq!(warm, b"v1");

    // OpenDAL invalidates before delete so a later open does not see a stale
    // FileInfo / try to read deleted blocks.
    ctx.invalidate_file_info(&path);
    master.delete(&path, false).await?;
    ctx.invalidate_file_info(&path);

    let after = GoosefsFileReader::open_with_context(ctx.clone(), &path).await;
    match after {
        Err(Error::NotFound { .. }) => {}
        Err(e) => panic!("after invalidate+delete, open_with_context must be NotFound, got {e}"),
        Ok(_) => panic!("after invalidate+delete, open_with_context must be NotFound, got Ok"),
    }

    let _ = master.delete(&root, true).await;
    Ok(())
}
