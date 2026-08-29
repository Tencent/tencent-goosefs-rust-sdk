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

//! Live-cluster checks that the UFS stream is opened when Java opens it.
//!
//! Java's `GooseFSFileOutStream` constructor opens the UFS stream for every
//! `isSyncPersist()` write type, which is why a zero-byte CACHE_THROUGH or
//! THROUGH write still leaves an empty file on the UFS. Opening it lazily on
//! the first `write()` skips the RPC entirely for such files, and the UFS ends
//! up with nothing.
//!
//! Proving the UFS copy exists needs a signal that does not read through the
//! Goosefs inode, so each test drops the Goosefs-side metadata
//! (`goosefs_only = true`) and then forces a re-import with
//! `LoadMetadataPType::ALWAYS`. The file can only come back if the UFS has it.
//!
//! Paths are flat, directly under the Goosefs root, on purpose. A write that
//! reaches the UFS needs its parent directory to exist *on the UFS*, and
//! `mkdir` does not put it there — see the TODO on [`unique_path`]. Nesting
//! the fixtures would test that gap rather than the UFS stream lifecycle.
//!
//! Ignored by default — needs a live master with a mounted UFS. Run:
//! ```bash
//! GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=simple \
//!   cargo test --test ufs_stream_lifecycle_e2e -- --ignored --nocapture --test-threads=1
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
    format!("/sdk-ufs-stream-e2e-{}_{ts}-{name}", std::process::id())
}

async fn connect() -> Result<Arc<BaseFileSystem>> {
    let mut config = GoosefsConfig::new(master_addr()).with_metrics_enabled(false);
    config.auth_type = auth_type();
    if let Ok(user) = std::env::var("GOOSEFS_AUTH_USERNAME") {
        config.auth_username = user;
    } else if let Ok(user) = std::env::var("USER") {
        config.auth_username = user;
    }
    Ok(BaseFileSystem::from_context(
        FileSystemContext::connect(config).await?,
    ))
}

fn write_opts(write_type: WriteType) -> CreateFileOptions {
    CreateFileOptions::with_write_type(write_type)
}

async fn cleanup(fs: &BaseFileSystem, path: &str) {
    let _ = fs.delete(path, DeleteOptions::default()).await;
}

/// Drop the Goosefs inode without touching the UFS, then force a re-import.
/// Returns the length the file came back with, or `None` if it did not come
/// back at all — which means the UFS never had it.
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

/// The regression this whole change is about: a CACHE_THROUGH file that is
/// created and closed without a single `write()` must still exist on the UFS.
#[tokio::test]
#[ignore = "Requires GooseFS master with a mounted UFS"]
async fn zero_byte_cache_through_creates_the_ufs_file() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("empty-cache-through.bin");

    fs.write_file(&path, b"", write_opts(WriteType::CacheThrough))
        .await?;

    let length = reimport_length_from_ufs(&fs, &path).await?;
    assert_eq!(
        length,
        Some(0),
        "a zero-byte CACHE_THROUGH write must leave an empty file on the UFS; \
         `None` means the UFS stream was never opened"
    );

    cleanup(&fs, &path).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master with a mounted UFS"]
async fn zero_byte_through_creates_the_ufs_file() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("empty-through.bin");

    fs.write_file(&path, b"", write_opts(WriteType::Through))
        .await?;

    let length = reimport_length_from_ufs(&fs, &path).await?;
    assert_eq!(length, Some(0));

    cleanup(&fs, &path).await;
    Ok(())
}

/// ASYNC_THROUGH must keep the opposite behaviour: no UFS stream is opened at
/// create time, so nothing reaches the UFS until the Master's persist job runs.
/// Opening one eagerly here would turn every async write into a synchronous
/// one.
#[tokio::test]
#[ignore = "Requires GooseFS master with a mounted UFS"]
async fn zero_byte_async_through_does_not_create_the_ufs_file() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("empty-async-through.bin");

    fs.write_file(&path, b"", write_opts(WriteType::AsyncThrough))
        .await?;

    let length = reimport_length_from_ufs(&fs, &path).await?;
    assert_eq!(
        length, None,
        "ASYNC_THROUGH must not write to the UFS from the client"
    );

    cleanup(&fs, &path).await;
    Ok(())
}

/// Opening the stream at create time must not disturb the normal path: the
/// bytes still have to arrive, and exactly once.
#[tokio::test]
#[ignore = "Requires GooseFS master with a mounted UFS"]
async fn non_empty_cache_through_still_round_trips() -> Result<()> {
    let fs = connect().await?;
    let path = unique_path("payload.bin");
    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();

    fs.write_file(&path, &payload, write_opts(WriteType::CacheThrough))
        .await?;

    let status = fs.get_status(&path).await?;
    assert_eq!(status.length, payload.len() as i64);

    let mut stream = GoosefsFileInStream::open_with_context(
        fs.context().clone(),
        &path,
        OpenFileOptions::default(),
    )
    .await?;
    let got = stream.read_all().await?;
    assert_eq!(got, payload, "eager UFS open must not corrupt the payload");

    let length = reimport_length_from_ufs(&fs, &path).await?;
    assert_eq!(
        length,
        Some(payload.len() as i64),
        "the UFS copy must hold every byte exactly once"
    );

    cleanup(&fs, &path).await;
    Ok(())
}
