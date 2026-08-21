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

//! Acceptance tests for exclusive-create support on GooseFS: inode fencing on
//! `complete_file`, and reclamation of the INCOMPLETE inodes that a dead writer
//! leaves behind.
//!
//! A writer that dies between `createFile` and `completeFile` leaves an
//! INCOMPLETE inode that occupies the path forever — GooseFS puts no lease on
//! those inodes. `MasterClient::try_reclaim_stale_incomplete` removes such an
//! inode with a compare-and-swap on its mtime, and the `inode_id` passed to
//! `complete_file` guarantees the writer that lost its inode fails loudly
//! instead of completing over somebody else's file.
//!
//! Authority:
//! - `DefaultFileSystemMaster.delete()` — the `ttl` / `ttlExpectMtime` check.
//! - `DefaultFileSystemMaster.checkClientMismatch()` — inode fencing.
//! - `DefaultFileSystemMaster.completeFileInternal()` — mtime is stamped here
//!   and nowhere else during a write, which is why age is the only staleness
//!   signal available.
//!
//! Ignored by default — needs a live master. Run:
//! ```bash
//! GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=simple \
//!   cargo test --test stale_incomplete_reclaim -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::client::{MasterClient, ReclaimOutcome};
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::{Error, Result};
use goosefs_sdk::fs::options::DeleteOptions;
use goosefs_sdk::io::GoosefsFileWriter;
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;

const TEST_DIR: &str = "/sdk-stale-incomplete-reclaim";

/// Long enough that no test write is ever mistaken for abandoned.
const NEVER_STALE: Duration = Duration::from_secs(3600);

/// Treat anything as stale — the tests create the leftover deliberately.
const ALWAYS_STALE: Duration = Duration::ZERO;

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
    format!("{TEST_DIR}/{tag}_{}_{ts}.bin", std::process::id())
}

/// Create the inode and stop there, exactly like a writer killed mid-write.
/// Returns the inode id the Master assigned.
async fn leave_incomplete_inode(master: &MasterClient, path: &str) -> Result<i64> {
    let _ = master.create_directory(TEST_DIR, true).await;
    let info = master
        .create_file(
            path,
            CreateFilePOptions {
                recursive: Some(true),
                block_size_bytes: Some(64 * 1024 * 1024),
                mode: Some(goosefs_sdk::client::master::default_file_mode()),
                ..Default::default()
            },
        )
        .await?;
    let inode_id = info.file_id.expect("Master must return a file id");
    let status = master.get_status(path).await?;
    assert_eq!(
        status.completed,
        Some(false),
        "a create without completeFile must leave the inode INCOMPLETE"
    );
    Ok(inode_id)
}

/// Whether this Master honours `DeleteOptions::ttl_expect_mtime`.
///
/// The guard landed on GooseFS `branch-2.0` on 2026-07-13 and is not in any
/// release tag yet, so a fixture pinned to an older image ignores the field —
/// which turns every conditional delete into an unconditional one. Probe the
/// behaviour instead of guessing from a version string: issue a conditional
/// delete carrying a deliberately wrong mtime against a throwaway inode. A
/// Master that honours the field refuses; one that ignores it deletes the inode.
async fn honours_mtime_guard(master: &MasterClient) -> Result<bool> {
    let probe = unique_path("probe-mtime-guard");
    leave_incomplete_inode(master, &probe).await?;
    let mtime = master
        .get_status(&probe)
        .await?
        .last_modification_time_ms
        .expect("Master must report an mtime");

    let refused = match master
        .delete_with_options(
            &probe,
            // Deliberately not the observed mtime.
            DeleteOptions::for_reclaim_stale_incomplete(mtime + 1_000_000),
        )
        .await
    {
        Err(e) if e.is_ttl_expect_mtime_mismatch() => true,
        Err(e) => return Err(e),
        Ok(()) => false,
    };

    // The inode survives only if the guard refused the delete.
    if refused {
        let _ = master
            .delete_with_options(&probe, DeleteOptions::for_cancel())
            .await;
    }
    Ok(refused)
}

async fn write_blob(ctx: &Arc<FileSystemContext>, path: &str, payload: &[u8]) -> Result<()> {
    let master = ctx.acquire_master();
    let _ = master.create_directory(TEST_DIR, true).await;
    let mut w = GoosefsFileWriter::create_with_context(ctx.clone(), path, None).await?;
    w.write(payload).await?;
    w.close().await?;
    Ok(())
}

/// The core case: an abandoned INCOMPLETE inode is removed and the path becomes
/// creatable again.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn abandoned_incomplete_inode_is_reclaimed_and_path_reusable() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();
    let path = unique_path("abandoned");

    leave_incomplete_inode(&master, &path).await?;

    let outcome = master
        .try_reclaim_stale_incomplete(&path, ALWAYS_STALE)
        .await?;
    assert_eq!(outcome, ReclaimOutcome::Reclaimed, "expected a reclaim");
    assert!(outcome.is_path_free());

    let after = master.get_status(&path).await;
    assert!(
        matches!(after, Err(Error::NotFound { .. })),
        "inode must be gone after reclaim, got: {after:?}"
    );

    // The whole point: an exclusive create can now proceed.
    write_blob(&ctx, &path, b"reclaimed-then-written").await?;
    let status = master.get_status(&path).await?;
    assert_eq!(status.completed, Some(true));

    let _ = master.delete(&path, false).await;
    eprintln!("[reclaim] abandoned inode reclaimed and path reused ✅");
    Ok(())
}

/// A fresh INCOMPLETE inode must be left alone — this is the guard that keeps
/// the reclaim path from shooting live writers.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn young_incomplete_inode_is_left_alone() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();
    let path = unique_path("young");

    leave_incomplete_inode(&master, &path).await?;

    let outcome = master
        .try_reclaim_stale_incomplete(&path, NEVER_STALE)
        .await?;
    assert!(
        matches!(outcome, ReclaimOutcome::WriteInProgress { .. }),
        "a young incomplete inode must be treated as an in-flight write, got: {outcome:?}"
    );
    assert!(!outcome.is_path_free());

    master
        .get_status(&path)
        .await
        .expect("inode must survive an unsuccessful reclaim");

    let _ = master
        .delete_with_options(&path, DeleteOptions::for_cancel())
        .await;
    eprintln!("[reclaim] young incomplete inode preserved ✅");
    Ok(())
}

/// A completed file is an ordinary conflict, not something to reclaim.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn completed_file_is_reported_as_conflict_not_reclaimed() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();
    let path = unique_path("completed");

    write_blob(&ctx, &path, b"must-survive-any-reclaim-attempt").await?;

    let outcome = master
        .try_reclaim_stale_incomplete(&path, ALWAYS_STALE)
        .await?;
    assert_eq!(
        outcome,
        ReclaimOutcome::Completed,
        "a completed file must never be reclaimed even when old"
    );
    assert!(!outcome.is_path_free());

    let status = master.get_status(&path).await?;
    assert_eq!(status.completed, Some(true), "file must be untouched");

    let _ = master.delete(&path, false).await;
    eprintln!("[reclaim] completed file untouched ✅");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn missing_path_reports_vanished() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();
    let path = unique_path("never-created");

    let outcome = master
        .try_reclaim_stale_incomplete(&path, ALWAYS_STALE)
        .await?;
    assert_eq!(outcome, ReclaimOutcome::Vanished);
    assert!(outcome.is_path_free());
    eprintln!("[reclaim] missing path reports Vanished ✅");
    Ok(())
}

/// End-to-end proof of the compare-and-swap: the writer completes inside the
/// race window, so its mtime moves and the delete built from the stale mtime is
/// refused. Without this guard the reclaim would destroy a finished file.
///
/// Skipped — loudly, so the CI log always states which mode it ran in — against
/// a Master that predates the `ttlExpectMtime` guard. Point the fixture at a
/// newer build via `GOOSEFS_IMAGE` to exercise it.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn conditional_delete_is_refused_once_mtime_moves() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();

    if !honours_mtime_guard(&master).await? {
        eprintln!(
            "[reclaim] SKIP conditional-delete assertion: this Master ignores \
             DeletePOptions.ttlExpectMtime, so every conditional delete is \
             unconditional. Reclamation is NOT race-safe here — use a GooseFS \
             build that contains the 2026-07-13 mtime-guard change."
        );
        return Ok(());
    }
    eprintln!("[reclaim] Master honours ttlExpectMtime — asserting the CAS");

    let path = unique_path("cas");
    let inode_id = leave_incomplete_inode(&master, &path).await?;
    let stale_mtime = master
        .get_status(&path)
        .await?
        .last_modification_time_ms
        .expect("Master must report an mtime");

    // Stand in for "the writer finished while we were deciding": completeFile is
    // the one operation that advances mtime during a write.
    master
        .complete_file(&path, Some(0), None, Some(inode_id))
        .await?;
    let fresh_mtime = master
        .get_status(&path)
        .await?
        .last_modification_time_ms
        .expect("Master must report an mtime");
    assert_ne!(
        stale_mtime, fresh_mtime,
        "completeFile is expected to advance mtime; without that the CAS has \
         nothing to detect"
    );

    let err = master
        .delete_with_options(
            &path,
            DeleteOptions::for_reclaim_stale_incomplete(stale_mtime),
        )
        .await
        .expect_err("a delete carrying a stale mtime must be refused");
    assert!(
        err.is_ttl_expect_mtime_mismatch(),
        "expected TtlExpectMtimeMismatch, got: {err:?}"
    );

    master
        .get_status(&path)
        .await
        .expect("file must survive the refused conditional delete");

    let _ = master.delete(&path, false).await;
    eprintln!("[reclaim] stale-mtime conditional delete refused ✅");
    Ok(())
}

/// The fencing half: after a reclaim replaces the inode, the original writer's
/// `complete_file` must fail rather than complete over the new file.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn complete_file_with_superseded_inode_id_is_rejected() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();
    let path = unique_path("fencing");

    let old_inode_id = leave_incomplete_inode(&master, &path).await?;

    // Somebody reclaims our inode and takes the path.
    let outcome = master
        .try_reclaim_stale_incomplete(&path, ALWAYS_STALE)
        .await?;
    assert_eq!(outcome, ReclaimOutcome::Reclaimed);
    let new_inode_id = leave_incomplete_inode(&master, &path).await?;
    assert_ne!(
        old_inode_id, new_inode_id,
        "the recreated file must be a different inode for this test to mean anything"
    );

    let err = master
        .complete_file(&path, Some(0), None, Some(old_inode_id))
        .await
        .expect_err("completing a superseded inode must fail");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "checkClientMismatch raises FileDoesNotExistException, expected NotFound, got: {err:?}"
    );

    let status = master.get_status(&path).await?;
    assert_eq!(
        status.completed,
        Some(false),
        "the new writer's inode must not have been completed by the loser"
    );
    assert_eq!(status.file_id, Some(new_inode_id));

    let _ = master
        .delete_with_options(&path, DeleteOptions::for_cancel())
        .await;
    eprintln!("[fencing] superseded inode id rejected ✅");
    Ok(())
}

/// Without fencing the same call succeeds — this is what makes passing
/// `inode_id` load-bearing rather than cosmetic.
#[tokio::test]
#[ignore = "Requires GooseFS master"]
async fn complete_file_without_inode_id_skips_the_fence() -> Result<()> {
    let ctx = FileSystemContext::connect(base_config()).await?;
    let master = ctx.acquire_master();
    let path = unique_path("unfenced");

    leave_incomplete_inode(&master, &path).await?;

    // `None` maps to UNKNOWN_INODE_ID, which makes the Master skip
    // checkClientMismatch entirely.
    master
        .complete_file(&path, Some(0), None, None)
        .await
        .expect("an unfenced completion is accepted regardless of inode identity");

    let status = master.get_status(&path).await?;
    assert_eq!(status.completed, Some(true));

    let _ = master.delete(&path, false).await;
    eprintln!("[fencing] unfenced completion documented ✅");
    Ok(())
}
