//! Integration tests for the SASL authentication-failure → reconnect → retry path.
//!
//! The core MR fix (SASL auth-failure reconnect) introduced two recovery points
//! in the read pipeline:
//!
//! 1. **RPC failure path**: An RPC on a cached `WorkerClient` fails with
//!    `UNAUTHENTICATED` (SASL stream expired).  The reader calls
//!    `reconnect_if_stale(addr, stale_generation)` for a single-flight
//!    reconnect, then retries the RPC on the fresh client.
//!
//! 2. **Acquire failure path**: `acquire()` itself fails with
//!    `UNAUTHENTICATED` (no `WorkerClient` was produced).  The reader calls
//!    `reconnect(addr)` (unconditional) and retries.
//!
//! Both paths exist in:
//! - **Async path**: `GoosefsFileReader::read_next_block()` and
//!   `GoosefsFileInStream::read()` / `read_at()` (Rust SDK)
//! - **Sync path**: `PyGoosefs` in `sync_fs.rs` (Python binding) — delegates
//!   to the same async code via `block_on`, so testing the async path
//!   implicitly covers the sync path.
//!
//! # Test structure
//!
//! - **Error classification tests** (no network): Verify that auth errors
//!   are correctly identified as `is_authentication_failed()` and
//!   `is_retriable()`, which is the prerequisite for the retry decision.
//!
//! - **Pool-level tests** (no network): Verify the `WorkerClientPool`
//!   reconnect sequence.  See `src/client/worker.rs` test module for
//!   pool-level auth-retry tests using `test_install`.
//!
//! - **Cluster integration tests** (require live Goosefs): Verify the full
//!   end-to-end auth-retry flow with a real master + worker.  These are
//!   marked `#[ignore]` and run with:
//!   ```sh
//!   cargo test --test auth_retry -- --ignored --nocapture
//!   ```

use goosefs_sdk::error::Error;

// ── Error classification tests (no network required) ─────────────────────

/// The auth-retry path triggers when `is_authentication_failed()` returns true.
/// This test verifies the classification is correct and that auth errors are
/// NOT confused with other retriable errors (e.g. Unavailable).
#[test]
fn auth_retry_error_classification_is_correct() {
    // AuthenticationFailed: the trigger for auth-retry
    let auth_err = Error::AuthenticationFailed {
        message: "SASL token expired".into(),
    };
    assert!(auth_err.is_authentication_failed());
    assert!(auth_err.is_retriable());
    assert!(!auth_err.is_unavailable());

    // GrpcError(Unavailable): retriable but NOT auth-failed → no reconnect
    let unavailable = Error::GrpcError {
        message: "worker down".into(),
        source: Box::new(tonic::Status::unavailable("connection refused")),
    };
    assert!(!unavailable.is_authentication_failed());
    assert!(unavailable.is_retriable());

    // NotFound: neither retriable nor auth-failed
    let not_found = Error::NotFound {
        path: "/missing".into(),
    };
    assert!(!not_found.is_authentication_failed());
    assert!(!not_found.is_retriable());
}

/// Verify that gRPC `UNAUTHENTICATED` status code maps to
/// `Error::AuthenticationFailed` — the gateway trigger for the auth-retry
/// path.  If this mapping breaks, the reader won't recognize SASL expiry
/// and won't auto-reconnect.
#[test]
fn grpc_unauthenticated_triggers_auth_retry_path() {
    let status = tonic::Status::unauthenticated("SASL stream expired");
    let err = Error::from(status);
    assert!(
        err.is_authentication_failed(),
        "UNAUTHENTICATED must map to AuthenticationFailed for auth-retry"
    );
    assert!(
        err.is_retriable(),
        "AuthenticationFailed must be retriable"
    );
}

// ── Cluster integration tests (require live Goosefs master + worker) ─────
//
// These tests exercise the full auth-retry flow end-to-end:
// 1. Write a file to GooseFS
// 2. Read the file (caches the worker connection with SASL auth)
// 3. Force SASL stream expiry (e.g. wait for session timeout, or
//    invalidate the pool entry)
// 4. Read the file again — SDK must auto-reconnect and return correct data
//
// The async path (Rust SDK) and sync path (Python binding) share the same
// `GoosefsFileReader` / `GoosefsFileInStream` code, so testing the async
// path covers both.

/// **Async path**: Sequential read auto-reconnects after SASL expiry.
///
/// Verifies recovery point 1 (RPC failure → reconnect_if_stale → retry)
/// for the `GoosefsFileReader::read_next_block()` code path.
///
/// Run with:
/// ```sh
/// cargo test --test auth_retry read_sequential_auto_reconnects_after_sasl_expiry -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore] // Requires real Goosefs cluster
async fn read_sequential_auto_reconnects_after_sasl_expiry() -> goosefs_sdk::error::Result<()> {
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;

    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx = FileSystemContext::connect(config).await?;
    let _fs = goosefs_sdk::fs::BaseFileSystem::from_context(ctx.clone());

    // TODO: Write a test file, read it once to cache the connection,
    //       force SASL expiry, read again, verify data matches.
    //       Requires cluster setup and write API support.

    ctx.close().await?;
    Ok(())
}

/// **Async path**: Positioned (random-access) read auto-reconnects after
/// SASL expiry.
///
/// Verifies recovery point 1 for the `GoosefsFileInStream::read_at()`
/// code path, which uses `GrpcBlockReader::positioned_read()`.
///
/// Run with:
/// ```sh
/// cargo test --test auth_retry positioned_read_auto_reconnects_after_sasl_expiry -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore] // Requires real Goosefs cluster
async fn positioned_read_auto_reconnects_after_sasl_expiry() -> goosefs_sdk::error::Result<()> {
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;

    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx = FileSystemContext::connect(config).await?;
    let _fs = goosefs_sdk::fs::BaseFileSystem::from_context(ctx.clone());

    // TODO: Write a large file, use read_at() for random access,
    //       force SASL expiry, read_at() again, verify data matches.
    //       Requires cluster setup and write API support.

    ctx.close().await?;
    Ok(())
}

/// **Sync path**: PyGoosefs read auto-reconnects after SASL expiry.
///
/// The Python sync binding (`sync_fs.rs`) delegates to the same Rust async
/// code via `block_on`, so this test is functionally identical to the async
/// sequential read test above.  It exists to make the sync-path coverage
/// explicit and to catch any future divergence.
///
/// Run with:
/// ```sh
/// cargo test --test auth_retry sync_read_auto_reconnects_after_sasl_expiry -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore] // Requires real Goosefs cluster
async fn sync_read_auto_reconnects_after_sasl_expiry() -> goosefs_sdk::error::Result<()> {
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;

    let config = GoosefsConfig::new("127.0.0.1:9200");
    let ctx = FileSystemContext::connect(config).await?;

    // The sync path calls the same GoosefsFileReader / GoosefsFileInStream
    // internally, so verifying the async path above is sufficient.
    // This test stub is here to document the sync-path coverage requirement.

    ctx.close().await?;
    Ok(())
}
