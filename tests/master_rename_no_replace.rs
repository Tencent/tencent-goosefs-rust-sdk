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

//! T0' — Master no-replace rename regression via pure SDK `MasterClient::rename`.
//!
//! Documents Lance/OpenDAL GooseFS safe Create (§8.8 T0'): when `dst` already
//! exists, client RPC rename must return [`Error::AlreadyExists`] and **must
//! not** overwrite `dst` contents. This path does **not** go through OpenDAL
//! `GoosefsCore::rename` (which used to delete-then-rename).
//!
//! Authority: GooseFS Master `DefaultFileSystemMaster.renameInternal`
//! (`FileAlreadyExistsException` when `dst.fullPathExists`).
//!
//! Ignored by default — needs a live master. Run:
//! ```bash
//! GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=simple \
//!   cargo test --test master_rename_no_replace -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::{Error, Result};
use goosefs_sdk::fs::options::OpenFileOptions;
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};

fn master_addr() -> String {
    std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
}

fn auth_type() -> AuthType {
    match std::env::var("GOOSEFS_AUTH_TYPE") {
        Ok(s) => s.parse::<AuthType>().unwrap_or(AuthType::Simple),
        Err(_) => AuthType::Simple,
    }
}

fn base_config() -> GoosefsConfig {
    let mut config = GoosefsConfig::new(master_addr());
    config.auth_type = auth_type();
    if let Ok(user) = std::env::var("GOOSEFS_AUTH_USERNAME") {
        config.auth_username = user;
    } else if let Ok(user) = std::env::var("USER") {
        config.auth_username = user;
    }
    config
}

fn unique_path(tag: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "/sdk-t0-rename-no-replace/{tag}_{}_{ts}.bin",
        std::process::id()
    )
}

async fn write_blob(ctx: &Arc<FileSystemContext>, path: &str, payload: &[u8]) -> Result<()> {
    let master = ctx.acquire_master();
    let _ = master
        .create_directory("/sdk-t0-rename-no-replace", true)
        .await;
    let _ = master.delete(path, false).await;
    let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
    w.write(payload).await?;
    w.close().await?;
    Ok(())
}

async fn read_blob(ctx: &Arc<FileSystemContext>, path: &str) -> Result<Vec<u8>> {
    let mut s =
        GoosefsFileInStream::open_with_context(ctx.clone(), path, OpenFileOptions::default())
            .await?;
    let bytes = s.read_all().await?;
    Ok(bytes.to_vec())
}

/// dst already exists → `AlreadyExists`; dst bytes unchanged (Master no-replace).
#[tokio::test]
#[ignore = "Requires GooseFS master (MasterClient::rename no-replace)"]
async fn master_rename_rejects_existing_dst_without_overwrite() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();

    let src = unique_path("src");
    let dst = unique_path("dst");
    let src_payload = b"from-src-should-not-land-on-dst";
    let dst_payload = b"dst-original-must-survive";

    write_blob(&ctx, &src, src_payload).await?;
    write_blob(&ctx, &dst, dst_payload).await?;

    eprintln!("[T0'] rename {src} -> {dst} (dst exists; expect AlreadyExists)");
    let err = master
        .rename(&src, &dst)
        .await
        .expect_err("rename onto existing dst must fail");

    assert!(
        matches!(err, Error::AlreadyExists { .. }),
        "expected AlreadyExists from Master no-replace rename, got: {err:?}"
    );
    eprintln!("[T0'] got AlreadyExists ✅: {err}");

    let dst_after = read_blob(&ctx, &dst).await?;
    assert_eq!(
        dst_after, dst_payload,
        "dst content must be unchanged after failed rename"
    );
    eprintln!("[T0'] dst content unchanged ✅");

    // Source should still exist (rename did not consume it).
    let src_after = read_blob(&ctx, &src).await?;
    assert_eq!(src_after, src_payload);
    eprintln!("[T0'] src still present ✅");

    let _ = master.delete(&src, false).await;
    let _ = master.delete(&dst, false).await;
    eprintln!("[T0'] MasterClient::rename no-replace regression ok ✅");
    Ok(())
}

/// Successful rename when dst is absent still works (smoke for same API).
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn master_rename_succeeds_when_dst_absent() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();

    let src = unique_path("ok-src");
    let dst = unique_path("ok-dst");
    let payload = b"rename-me-ok";

    write_blob(&ctx, &src, payload).await?;
    let _ = master.delete(&dst, false).await;

    master.rename(&src, &dst).await?;
    let dst_after = read_blob(&ctx, &dst).await?;
    assert_eq!(dst_after, payload);

    // Source path should be gone after successful rename.
    let src_stat = master.get_status(&src).await;
    assert!(
        matches!(src_stat, Err(Error::NotFound { .. })),
        "src should be NotFound after rename, got: {src_stat:?}"
    );

    let _ = master.delete(&dst, false).await;
    eprintln!("[T0'] rename to absent dst ok ✅");
    Ok(())
}
