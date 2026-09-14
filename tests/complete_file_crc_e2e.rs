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

//! Live-cluster checks that `CompleteFile` stamps inode CRC32C xattr.
//!
//! Java `GooseFSFileOutStream.close()` always sends `crcType` / `crcValue` and
//! `inodeId`. Master only writes `CrcType` / `CrcValue` xattr when both CRC
//! fields are present; otherwise persist logs `inode crc missing, skip ufs check`
//! and skips HybridPersistenceManager UFS verification.
//!
//! These tests talk to a real Master (Docker fixture or a local cluster). They
//! cannot grep Master logs from CI, so they assert the xattr Master would use
//! for that check. `scripts/ci/run_rust_integration.sh` discovers this file
//! automatically and runs it on both FILE and PAGE workers.
//!
//! Multi-block coverage is FILE-only: PAGE's `PagedBlockWriter.flush()` still
//! throws on a mid-file block switch.
//!
//! Ignored by default. Run:
//! ```bash
//! GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=simple \
//!   cargo test --test complete_file_crc_e2e -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::config::{GoosefsConfig, WriteType};
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::{CreateFileOptions, DeleteOptions, OpenFileOptions};
use goosefs_sdk::fs::{BaseFileSystem, FileSystem, URIStatus};
use goosefs_sdk::io::GoosefsFileInStream;
use goosefs_sdk::proto::grpc::ChecksumTypeProto;

/// ITU-T V.42 / Castagnoli check vector. Same value Java `PureJavaCrc32C`
/// sends as `CompleteFilePOptions.crc_value`.
const CHECK_VECTOR: &[u8] = b"123456789";
const CHECK_VECTOR_CRC32C: u32 = 0xe3069283;

fn master_addr() -> String {
    std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
}

fn auth_type() -> AuthType {
    match std::env::var("GOOSEFS_AUTH_TYPE") {
        Ok(s) => s.parse::<AuthType>().unwrap_or(AuthType::Simple),
        Err(_) => AuthType::Simple,
    }
}

fn worker_store_type() -> String {
    std::env::var("GOOSEFS_WORKER_BLOCK_STORE_TYPE")
        .unwrap_or_else(|_| "FILE".to_string())
        .to_ascii_uppercase()
}

/// A unique path directly under the Goosefs root.
///
/// Flat on purpose: a persist job that reaches UFS needs the parent to exist
/// there, and Rust `mkdir` does not set directory `write_type`. Same constraint
/// as `ufs_stream_lifecycle_e2e` / `write_degrade_e2e`.
fn unique_path(name: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/sdk-crc-e2e-{}_{ts}-{name}", std::process::id())
}

fn base_config() -> GoosefsConfig {
    let mut config = GoosefsConfig::new(master_addr())
        .with_metrics_enabled(false)
        .with_file_replication_durable(1)
        .with_file_replication_durable_min(1);
    config.auth_type = auth_type();
    if let Ok(user) = std::env::var("GOOSEFS_AUTH_USERNAME") {
        config.auth_username = user;
    } else if let Ok(user) = std::env::var("USER") {
        config.auth_username = user;
    }
    config
}

async fn connect() -> Result<Arc<BaseFileSystem>> {
    Ok(BaseFileSystem::from_context(
        FileSystemContext::connect(base_config()).await?,
    ))
}

fn write_opts(write_type: WriteType) -> CreateFileOptions {
    CreateFileOptions::with_write_type(write_type)
}

async fn cleanup(fs: &BaseFileSystem, path: &str) {
    let _ = fs.delete(path, DeleteOptions::default()).await;
}

fn assert_crc32c_xattr(status: &URIStatus, expect: u32) {
    assert!(
        status.file_id > 0,
        "CompleteFile must leave a real inode id (P1 inode_id)"
    );
    assert!(status.completed, "file must be completed");
    let crc_type = status
        .xattr
        .get("CrcType")
        .unwrap_or_else(|| panic!("CrcType xattr missing — CompleteFile omitted crc_type"));
    assert_eq!(
        crc_type.as_slice(),
        [ChecksumTypeProto::ChecksumCrc32c as i32 as u8],
        "CrcType must be CRC32C"
    );
    let crc_value = status
        .xattr
        .get("CrcValue")
        .unwrap_or_else(|| panic!("CrcValue xattr missing — CompleteFile omitted crc_value"));
    assert_eq!(
        crc_value.as_slice(),
        expect.to_be_bytes(),
        "CrcValue 0x{expect:08x} mismatch"
    );
}

async fn wait_persisted(fs: &BaseFileSystem, path: &str) -> Result<URIStatus> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = fs.get_status(path).await?;
        if status.persisted || status.persistence_state == "PERSISTED" {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for async persist of {path}: persisted={} state={}",
                status.persisted, status.persistence_state
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// P0: ASYNC_THROUGH CompleteFile stamps Castagnoli CRC32C on the inode.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn async_through_stamps_crc32c_xattr() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("check.bin");

    fs.write_file(&path, CHECK_VECTOR, write_opts(WriteType::AsyncThrough))
        .await?;

    let status = fs.get_status(&path).await?;
    assert_eq!(status.mode & 0o777, 0o644, "CreateFile default mode");
    assert_crc32c_xattr(&status, CHECK_VECTOR_CRC32C);
    assert!(
        status.persisted
            || status.persistence_state == "TO_BE_PERSISTED"
            || status.persistence_state == "PERSISTED",
        "ASYNC_THROUGH must schedule persist, got {}",
        status.persistence_state
    );

    let got = GoosefsFileInStream::open_with_context(
        fs.context().clone(),
        &path,
        OpenFileOptions::default(),
    )
    .await?
    .read_all()
    .await?;
    assert_eq!(got.as_ref(), CHECK_VECTOR);

    cleanup(&fs, &path).await;
    Ok(())
}

/// MUST_CACHE also sends CRC — Java always does, not only persisting writes.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn must_cache_stamps_crc32c_xattr() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("must-cache.bin");

    fs.write_file(&path, CHECK_VECTOR, write_opts(WriteType::MustCache))
        .await?;

    let status = fs.get_status(&path).await?;
    assert_crc32c_xattr(&status, CHECK_VECTOR_CRC32C);

    cleanup(&fs, &path).await;
    Ok(())
}

/// Empty file still sends CRC32C `0`, matching Java `DataChecksum` of no bytes.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn empty_file_crc32c_is_zero() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("empty.bin");

    fs.write_file(&path, b"", write_opts(WriteType::MustCache))
        .await?;

    let status = fs.get_status(&path).await?;
    assert_eq!(status.length, 0);
    assert_crc32c_xattr(&status, 0);

    cleanup(&fs, &path).await;
    Ok(())
}

/// Docker / local UFS persist job must succeed and keep the same inode CRC.
#[tokio::test]
#[ignore = "Requires GooseFS master with a mounted UFS"]
async fn async_through_persist_keeps_crc_xattr() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("persist.bin");

    fs.write_file(&path, CHECK_VECTOR, write_opts(WriteType::AsyncThrough))
        .await?;
    let status = wait_persisted(&fs, &path).await?;
    assert_crc32c_xattr(&status, CHECK_VECTOR_CRC32C);

    cleanup(&fs, &path).await;
    Ok(())
}

/// Multi-block running checksum. PAGE still cannot flush on a mid-file switch.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn multi_block_async_through_crc_on_file_store() -> Result<()> {
    if worker_store_type() == "PAGE" {
        eprintln!("skipping multi-block CRC on PAGE (PagedBlockWriter.flush unsupported)");
        return Ok(());
    }

    let fs = connect().await?;
    let path = unique_path("multiblock.bin");
    let block = 64 * 1024;
    let payload: Vec<u8> = (0..block * 3 + 123).map(|i| (i % 251) as u8).collect();
    let expect = crc32c::crc32c(&payload);

    let mut opts = write_opts(WriteType::AsyncThrough);
    opts.block_size_bytes = Some(block as i64);
    fs.write_file(&path, &payload, opts).await?;

    let status = fs.get_status(&path).await?;
    assert!(
        status.block_ids.len() >= 2 || status.length > block as i64,
        "expected more than one block, length={} blocks={:?}",
        status.length,
        status.block_ids
    );
    assert_crc32c_xattr(&status, expect);

    cleanup(&fs, &path).await;
    Ok(())
}
