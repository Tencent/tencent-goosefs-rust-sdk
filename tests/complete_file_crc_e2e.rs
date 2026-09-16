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
//! `inodeId`. The default checksum is **CRC32C** (Castagnoli /
//! `java.util.zip.CRC32C` / `PureJavaCrc32C`), not IEEE CRC32
//! (`java.util.zip.CRC32`). Master only writes `CrcType` / `CrcValue` xattr
//! when both CRC fields are present; otherwise persist logs
//! `inode crc missing, skip ufs check` and skips HybridPersistenceManager UFS
//! verification.
//!
//! These tests talk to a real Master (Docker fixture or a local cluster). They
//! cannot grep Master logs from CI, so they assert the xattr Master would use
//! for that check — including the ITU-T V.42 vector `"123456789"` →
//! `0xe3069283` (IEEE CRC32 of the same bytes is `0xcbf43926`).
//! `scripts/ci/run_rust_integration.sh` discovers this file automatically and
//! runs it on both FILE and PAGE workers.
//!
//! Do **not** wait for `PERSISTED`. The CI fixture's `start-default.sh`
//! skips `job_master` / `job_worker` (~1.1 GiB), so async persist jobs never
//! run and the inode stays `TO_BE_PERSISTED`. CRC xattr is written at
//! `CompleteFile`, which is the SDK contract under test.
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
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::config::{GoosefsConfig, WriteType, WriterChecksumType};
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::options::{CreateFileOptions, DeleteOptions, OpenFileOptions};
use goosefs_sdk::fs::{BaseFileSystem, FileSystem, URIStatus};
use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use goosefs_sdk::proto::grpc::ChecksumTypeProto;
use goosefs_sdk::WritePType;

/// ITU-T V.42 / Castagnoli check vector. Same value Java `java.util.zip.CRC32C`
/// / `PureJavaCrc32C` sends as `CompleteFilePOptions.crc_value` for
/// `"123456789"`. IEEE CRC32 (`java.util.zip.CRC32`) of the same bytes is
/// `0xcbf43926` — these tests must fail if the writer ever switches polynomials.
const CHECK_VECTOR: &[u8] = b"123456789";
const CHECK_VECTOR_CRC32C: u32 = 0xe3069283;
const CHECK_VECTOR_IEEE_CRC32: u32 = 0xcbf4_3926;

/// `java.util.zip.CRC32` (IEEE) of the multi-block e2e payload below.
/// Live xattr must be Castagnoli `0x9fa363fa`, not this.
const MULTIBLOCK_IEEE_CRC32: u32 = 0x9c20_a08f;

/// Independent Castagnoli CRC32C so this integration test does not take a
/// crates.io `crc32c` dep. Must match `src/io/crc32c.rs` / Java `PureJavaCrc32C`.
fn expected_crc32c(data: &[u8]) -> u32 {
    const POLY: u32 = 0x82f6_3b78;
    let mut crc = 0xffff_ffffu32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

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
    connect_with(base_config()).await
}

async fn connect_with(config: GoosefsConfig) -> Result<Arc<BaseFileSystem>> {
    Ok(BaseFileSystem::from_context(
        FileSystemContext::connect(config).await?,
    ))
}

fn write_opts(write_type: WriteType) -> CreateFileOptions {
    CreateFileOptions::with_write_type(write_type)
}

fn create_proto_opts(write_type: WriteType) -> CreateFilePOptions {
    CreateFilePOptions {
        write_type: Some(WritePType::from(write_type) as i32),
        recursive: Some(false),
        ..Default::default()
    }
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
        "CrcValue 0x{expect:08x} mismatch (Java CRC32C Castagnoli; IEEE CRC32 of \"123456789\" is 0x{CHECK_VECTOR_IEEE_CRC32:08x})"
    );
}

fn assert_java_check_vector_xattr(status: &URIStatus) {
    assert_crc32c_xattr(status, CHECK_VECTOR_CRC32C);
    assert_ne!(
        status.xattr.get("CrcValue").unwrap().as_slice(),
        CHECK_VECTOR_IEEE_CRC32.to_be_bytes(),
        "CrcValue must be Java CRC32C, not IEEE CRC32"
    );
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
    assert_java_check_vector_xattr(&status);
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
    assert_java_check_vector_xattr(&status);

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
    let expect = expected_crc32c(&payload);
    assert_eq!(
        expect, 0x9fa3_63fa,
        "multi-block payload must stay the Java CRC32C golden"
    );
    assert_ne!(expect, MULTIBLOCK_IEEE_CRC32);

    let mut opts = write_opts(WriteType::AsyncThrough);
    opts.block_size_bytes = Some(block as i64);
    fs.write_file(&path, &payload, opts).await?;

    let status = fs.get_status(&path).await?;
    assert_eq!(status.block_size_bytes, block as i64);
    assert!(
        status.block_ids.len() >= 2,
        "expected more than one block, length={} blocks={:?}",
        status.length,
        status.block_ids
    );
    assert_crc32c_xattr(&status, expect);

    cleanup(&fs, &path).await;
    Ok(())
}

/// Java `GooseFSFileOutStream.writeInternal` calls `Checksum.update` on every
/// write. Two `write()` calls of `"12345"` then `"6789"` must complete as
/// CRC32C `0xe3069283`, not IEEE CRC32 `0xcbf43926`.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn incremental_writes_match_java_crc32c_not_ieee() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("incremental.bin");

    let mut writer = GoosefsFileWriter::create_with_context(
        fs.context().clone(),
        &path,
        Some(create_proto_opts(WriteType::MustCache)),
    )
    .await?;
    writer.write(b"12345").await?;
    writer.write(b"6789").await?;
    writer.close().await?;

    let status = fs.get_status(&path).await?;
    assert_java_check_vector_xattr(&status);

    cleanup(&fs, &path).await;
    Ok(())
}

/// CACHE_THROUGH also stamps CRC32C — Java always sends it, not only ASYNC_THROUGH.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn cache_through_stamps_java_crc32c_xattr() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("cache-through.bin");

    fs.write_file(&path, CHECK_VECTOR, write_opts(WriteType::CacheThrough))
        .await?;

    let status = fs.get_status(&path).await?;
    assert_java_check_vector_xattr(&status);

    cleanup(&fs, &path).await;
    Ok(())
}

#[test]
fn e2e_helper_matches_java_crc32c_not_ieee() {
    assert_eq!(expected_crc32c(CHECK_VECTOR), CHECK_VECTOR_CRC32C);
    assert_ne!(expected_crc32c(CHECK_VECTOR), CHECK_VECTOR_IEEE_CRC32);
    assert_eq!(expected_crc32c(b""), 0);
    assert_eq!(expected_crc32c(b"Hello world!"), 0x7b98_e751);
}

/// `goosefs.user.streaming.writer.checksum.type=CRC32` must send IEEE CRC32
/// (`ChecksumTypeProto.CHECKSUM_CRC32` / `0xcbf43926`), not Castagnoli.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn ieee_crc32_checksum_type_stamps_xattr() -> Result<()> {
    let fs =
        connect_with(base_config().with_writer_checksum_type(WriterChecksumType::Crc32)).await?;
    let path = unique_path("ieee.bin");

    fs.write_file(&path, CHECK_VECTOR, write_opts(WriteType::MustCache))
        .await?;

    let status = fs.get_status(&path).await?;
    assert!(status.completed);
    let crc_type = status
        .xattr
        .get("CrcType")
        .unwrap_or_else(|| panic!("CrcType xattr missing"));
    assert_eq!(
        crc_type.as_slice(),
        [ChecksumTypeProto::ChecksumCrc32 as i32 as u8],
        "CrcType must be IEEE CRC32"
    );
    let crc_value = status
        .xattr
        .get("CrcValue")
        .unwrap_or_else(|| panic!("CrcValue xattr missing"));
    assert_eq!(crc_value.as_slice(), CHECK_VECTOR_IEEE_CRC32.to_be_bytes());
    assert_ne!(crc_value.as_slice(), CHECK_VECTOR_CRC32C.to_be_bytes());

    cleanup(&fs, &path).await;
    Ok(())
}

/// `checksum.type=NULL` still sends both CompleteFile fields; Master stamps
/// type 0 and value 0 (Java `DataChecksum.newDataChecksum(NULL)`).
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn null_checksum_type_stamps_zero_xattr() -> Result<()> {
    let fs =
        connect_with(base_config().with_writer_checksum_type(WriterChecksumType::Null)).await?;
    let path = unique_path("null.bin");

    fs.write_file(&path, CHECK_VECTOR, write_opts(WriteType::MustCache))
        .await?;

    let status = fs.get_status(&path).await?;
    assert!(status.completed);
    let crc_type = status
        .xattr
        .get("CrcType")
        .unwrap_or_else(|| panic!("CrcType xattr missing"));
    assert_eq!(
        crc_type.as_slice(),
        [ChecksumTypeProto::ChecksumNull as i32 as u8]
    );
    let crc_value = status
        .xattr
        .get("CrcValue")
        .unwrap_or_else(|| panic!("CrcValue xattr missing"));
    assert_eq!(crc_value.as_slice(), 0u32.to_be_bytes());

    cleanup(&fs, &path).await;
    Ok(())
}
