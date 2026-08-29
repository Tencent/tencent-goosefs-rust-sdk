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

//! Live-cluster checks for the cache-write degradation path.
//!
//! When a cache write fails, Java's `handleCacheWriteException` either aborts
//! the write or falls back to a UFS-only one. The decision matrix itself is
//! unit-tested in `file_writer.rs`; what needs a real cluster is the wiring
//! around it — that a degraded write actually reaches the UFS, carries the
//! whole buffer, and reports the file as already persisted.
//!
//! Both cases are driven from client config alone, no fixture hooks needed:
//!
//! - **Degrade**: ASYNC_THROUGH with an unsatisfiable persist watermark. The
//!   first block finds no worker with space, and since no block has opened
//!   yet the writer is allowed to fall back to the UFS.
//! - **Abort**: ASYNC_THROUGH with `durable.min` above the achievable replica
//!   count. That breaks the replication contract the caller asked for, so
//!   falling back to a single UFS copy is forbidden and the write must fail.
//!
//! Paths are flat, directly under the Goosefs root, on purpose. A write that
//! reaches the UFS needs its parent directory to exist *on the UFS*, and
//! `mkdir` does not put it there — see the TODO on
//! [`unique_path`]. Nesting the fixtures would test that gap rather than the
//! degrade path.
//!
//! TODO(coverage): both cases here fail *before* any block opens, which is the
//! only kind of failure client config can provoke. The rules that need a
//! failure to land mid-stream are still uncovered:
//!
//! - rule 3, ASYNC_THROUGH with a block already opened must abort rather than
//!   degrade (degrading there would strand committed cache blocks and produce
//!   a truncated UFS file — the worst outcome in the whole matrix, and the one
//!   with no test);
//! - rule 4, rejected credentials must abort without blacklisting the worker;
//! - the CACHE_THROUGH degrade, where a worker dies with the UFS stream
//!   already open.
//!
//! Reaching those means killing or partitioning a worker part-way through a
//! write, which the Docker fixture cannot currently do.
//! `cache_write_failure_is_fatal` is unit-tested over the full matrix, so the
//! *decisions* are covered; what is missing is proof the writer survives the
//! failure arriving mid-stream. Needs a fixture that can drop a worker on
//! command.
//!
//! Ignored by default — needs a live master with a mounted UFS. Run:
//! ```bash
//! GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=simple \
//!   cargo test --test write_degrade_e2e -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::config::{GoosefsConfig, WriteType};
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::{Error, Result};
use goosefs_sdk::fs::options::{CreateFileOptions, DeleteOptions, OpenFileOptions};
use goosefs_sdk::fs::{BaseFileSystem, FileSystem};
use goosefs_sdk::io::GoosefsFileInStream;
use goosefs_sdk::proto::grpc::file::LoadMetadataPType;

/// Larger than any worker's persist capacity, so the watermark filter rejects
/// every candidate for the first block.
const UNSATISFIABLE_REMAIN_BYTES: u64 = u64::MAX / 2;

fn master_addr() -> String {
    std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
}

fn auth_type() -> AuthType {
    match std::env::var("GOOSEFS_AUTH_TYPE") {
        Ok(s) => s.parse::<AuthType>().unwrap_or(AuthType::Simple),
        Err(_) => AuthType::Simple,
    }
}

/// A unique path directly under the Goosefs root.
///
/// TODO(java-parity): this is flat because a nested one would not work.
/// `MasterClient::create_directory` never sets `CreateDirectoryPOptions`
/// `write_type` (the field exists, tag 4), so directories are created with the
/// Master's default — cache-only. Java takes the write type from
/// `alluxio.user.file.writetype.default` and a CACHE_THROUGH/THROUGH `mkdir`
/// creates the directory on the UFS too. The consequence is that a Rust client
/// writing a CACHE_THROUGH file into a freshly-created subdirectory gets
/// `NOT_FOUND: <ufs path> (No such file or directory)` from the worker, because
/// the parent exists in Goosefs but not on the UFS. Fixing it means plumbing
/// the write type through `mkdir`, which changes directory-creation behaviour
/// for every caller and so wants its own change and its own tests.
fn unique_path(name: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/sdk-write-degrade-e2e-{}_{ts}-{name}", std::process::id())
}

fn base_config() -> GoosefsConfig {
    let mut config = GoosefsConfig::new(master_addr()).with_metrics_enabled(false);
    config.auth_type = auth_type();
    if let Ok(user) = std::env::var("GOOSEFS_AUTH_USERNAME") {
        config.auth_username = user;
    } else if let Ok(user) = std::env::var("USER") {
        config.auth_username = user;
    }
    config
}

async fn connect(config: GoosefsConfig) -> Result<Arc<BaseFileSystem>> {
    Ok(BaseFileSystem::from_context(
        FileSystemContext::connect(config).await?,
    ))
}

fn write_opts(write_type: WriteType) -> CreateFileOptions {
    CreateFileOptions::with_write_type(write_type)
}

/// Ask the UFS directly whether it holds the file: drop the Goosefs inode and
/// force the Master to re-import. `None` means the UFS never got it.
///
/// The Goosefs-only delete is best-effort — a write that failed may have left
/// no inode to delete, and that is not what this is measuring.
async fn reimport_length_from_ufs(fs: &BaseFileSystem, path: &str) -> Result<Option<i64>> {
    let _ = fs
        .delete(path, DeleteOptions::goosefs_only_unchecked())
        .await;

    match fs
        .context()
        .acquire_master()
        .get_status_with_load_type(path, Some(LoadMetadataPType::Always), Some(0))
        .await
    {
        Ok(info) => Ok(Some(info.length.unwrap_or_default())),
        Err(Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

async fn cleanup(fs: &BaseFileSystem, path: &str) {
    let _ = fs.delete(path, DeleteOptions::default()).await;
}

/// The degradation main line. Before this path existed the write simply
/// failed; now the client falls back to a UFS-only write and tells the Master
/// the file is already persisted.
///
/// The UFS copy has to be there the moment `close()` returns. If it only
/// showed up later, that would mean the Master scheduled an async-persist job
/// instead of honouring `forcePersisted` — the exact confusion this alignment
/// was meant to remove.
#[tokio::test]
#[ignore = "Requires GooseFS master with a mounted UFS"]
async fn async_through_degrades_to_ufs_when_no_worker_has_space() -> Result<()> {
    let mut config = base_config();
    config.block_worker_available_min_remain_bytes = UNSATISFIABLE_REMAIN_BYTES;
    // Pin replication to 1 so the watermark is the *only* reason the candidate
    // pool empties. On a single-worker cluster the default `durable.min = 2`
    // would also come up short, and that failure is ResourceExhausted — fatal
    // by rule 1, not a degrade. The test would then pass or fail depending on
    // how many workers the cluster happens to have.
    config.file_replication_durable = 1;
    config.file_replication_durable_min = 1;
    let fs = connect(config).await?;

    let path = unique_path("degraded.bin");
    let payload: Vec<u8> = (0..48 * 1024).map(|i| (i % 241) as u8).collect();

    if let Err(e) = fs
        .write_file(&path, &payload, write_opts(WriteType::AsyncThrough))
        .await
    {
        panic!("ASYNC_THROUGH must degrade to UFS rather than fail: {e}");
    }

    let status = fs.get_status(&path).await?;
    assert_eq!(
        status.length,
        payload.len() as i64,
        "the degraded write must report the full length to the Master"
    );

    let mut stream = GoosefsFileInStream::open_with_context(
        fs.context().clone(),
        &path,
        OpenFileOptions::default(),
    )
    .await?;
    let got = stream.read_all().await?;
    assert_eq!(
        got, payload,
        "the whole buffer must reach the UFS, including the prefix the cache \
         stream had already accepted before it failed"
    );

    let length = reimport_length_from_ufs(&fs, &path).await?;
    assert_eq!(
        length,
        Some(payload.len() as i64),
        "the UFS copy must exist as soon as close() returns (forcePersisted), \
         not after an async-persist job"
    );

    cleanup(&fs, &path).await;
    Ok(())
}

/// The control: degradation must not be a blanket fallback. When the block
/// store cannot satisfy `durable.min`, quietly writing a single UFS copy would
/// break the replication contract, so the write has to fail instead.
#[tokio::test]
#[ignore = "Requires GooseFS master with a mounted UFS"]
async fn async_through_does_not_degrade_when_replication_contract_broken() -> Result<()> {
    let mut config = base_config();
    // `durable.min` above the achievable replica count — `replica_write_plan`
    // rejects this outright as InvalidArgument.
    config.file_replication_number = 1;
    config.file_replication_durable = 2;
    config.file_replication_durable_min = 9;
    let fs = connect(config).await?;

    let path = unique_path("must-not-degrade.bin");

    let err = fs
        .write_file(&path, b"payload", write_opts(WriteType::AsyncThrough))
        .await
        .expect_err("a broken replication contract must not silently degrade to UFS");
    assert!(
        matches!(err, Error::InvalidArgument { .. }),
        "expected InvalidArgument, got {err:?}"
    );

    let length = reimport_length_from_ufs(&fs, &path).await?;
    assert_eq!(
        length, None,
        "an aborted write must leave nothing behind on the UFS"
    );

    cleanup(&fs, &path).await;
    Ok(())
}
