//! Error types for the Goosefs Rust client.
//!
//! Follows the pattern from fluss-rust: a single `Error` enum with `thiserror`
//! derive, a `Result<T>` alias, and `From` conversions for common upstream errors.

use thiserror::Error;

/// Convenience type alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type for goosefs-sdk.
#[derive(Debug, Error)]
pub enum Error {
    /// gRPC transport or protocol error (from tonic).
    ///
    /// The `source` is boxed to keep the enum variant small; `tonic::Status`
    /// is ~200 bytes which triggers `clippy::result_large_err` otherwise.
    #[error("gRPC error: {message} — {source}")]
    GrpcError {
        message: String,
        #[source]
        source: Box<tonic::Status>,
    },

    /// gRPC transport / connection-level error.
    ///
    /// Boxed for the same reason as `GrpcError`.
    #[error("gRPC transport error: {message} — {source}")]
    TransportError {
        message: String,
        #[source]
        source: Box<tonic::transport::Error>,
    },

    /// The file or directory was not found on Goosefs.
    #[error("not found: {path}")]
    NotFound { path: String },

    /// The file or directory already exists.
    #[error("already exists: {path}")]
    AlreadyExists { path: String },

    /// Permission denied for the requested operation.
    #[error("permission denied: {message}")]
    PermissionDenied { message: String },

    /// Invalid argument supplied by the caller.
    #[error("invalid argument: {message}")]
    InvalidArgument { message: String },

    /// A required field was missing in the protobuf response.
    #[error("missing field in response: {field}")]
    MissingField { field: String },

    /// Block read/write I/O error.
    #[error("block IO error: {message}")]
    BlockIoError { message: String },

    /// No worker available to serve the request.
    #[error("no worker available: {message}")]
    NoWorkerAvailable { message: String },

    /// No primary Master could be discovered (HA polling failed).
    #[error("master unavailable: {message}")]
    MasterUnavailable { message: String },

    /// Configuration error.
    #[error("config error: {message}")]
    ConfigError { message: String },

    /// Generic internal error with an optional boxed source.
    #[error("internal error: {message}")]
    Internal {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    // -----------------------------------------------------------------------
    // Domain-specific errors mapped from Java server exceptions.
    //
    // Java server throws strongly-typed exceptions which arrive at the gRPC
    // boundary as FAILED_PRECONDITION status codes. The message text carries
    // the discriminating keyword so we parse it here to produce Rust domain
    // errors instead of the opaque GrpcError catch-all.
    // -----------------------------------------------------------------------
    /// The file exists but has not been completed yet (INCOMPLETE state).
    ///
    /// Java: `FileIncompleteException` → gRPC `FAILED_PRECONDITION` with
    /// message containing `"is incomplete"`.
    /// This can happen when reading a file that another writer has not yet
    /// called `close()` on.
    #[error("file is incomplete: {message}")]
    FileIncomplete { message: String },

    /// Attempted to delete a non-empty directory without `recursive = true`.
    ///
    /// Java: `DirectoryNotEmptyException` → gRPC `FAILED_PRECONDITION` with
    /// message containing `"is not empty"`.
    #[error("directory is not empty: {message}")]
    DirectoryNotEmpty { message: String },

    /// Attempted to open a path that refers to a directory (not a file).
    ///
    /// Java: `IsDirectoryException` → gRPC `FAILED_PRECONDITION` with
    /// message containing `"Is a directory"`.
    #[error("path is a directory: {path}")]
    OpenDirectory { path: String },

    /// The supplied path string is syntactically invalid.
    ///
    /// Java: `InvalidPathException` → gRPC `INVALID_ARGUMENT` with the path
    /// in the message.  Kept separate from `InvalidArgument` so callers can
    /// match on path problems specifically.
    #[error("invalid path: {path}")]
    InvalidPath { path: String },

    // -----------------------------------------------------------------------
    // Authentication errors — must NOT trigger worker blacklisting.
    //
    // When a worker rejects a request with UNAUTHENTICATED the caller should
    // surface this error and let the higher-level retry policy decide whether
    // to re-authenticate rather than permanently removing the worker from the
    // routing table.
    // -----------------------------------------------------------------------
    /// Authentication with the Master or Worker failed.
    ///
    /// Mapped from tonic `UNAUTHENTICATED` status.  The router **must not**
    /// mark the worker as failed when this error is returned — the worker is
    /// healthy; only the credentials are wrong.
    #[error("authentication failed: {message}")]
    AuthenticationFailed { message: String },
}

// ---------------------------------------------------------------------------
// From conversions
// ---------------------------------------------------------------------------

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => Error::NotFound {
                path: status.message().to_string(),
            },
            tonic::Code::AlreadyExists => Error::AlreadyExists {
                path: status.message().to_string(),
            },
            tonic::Code::PermissionDenied => Error::PermissionDenied {
                message: status.message().to_string(),
            },
            tonic::Code::InvalidArgument => Error::InvalidArgument {
                message: status.message().to_string(),
            },
            // Authentication failures — surface as a dedicated variant so
            // the WorkerRouter is NOT instructed to blacklist the worker.
            tonic::Code::Unauthenticated => Error::AuthenticationFailed {
                message: status.message().to_string(),
            },
            // FAILED_PRECONDITION carries several distinct Java exceptions;
            // disambiguate by inspecting the message text.
            //
            // Java exception → gRPC message keyword mapping (verified against
            // DefaultFileSystemMaster.java and GoosefsStatusException.java):
            //   FileIncompleteException      → "is incomplete"
            //   DirectoryNotEmptyException   → "is not empty"
            //   IsDirectoryException         → "Is a directory"
            tonic::Code::FailedPrecondition => {
                let msg = status.message();
                if msg.contains("is not empty") {
                    Error::DirectoryNotEmpty {
                        message: msg.to_string(),
                    }
                } else if msg.contains("is incomplete") {
                    Error::FileIncomplete {
                        message: msg.to_string(),
                    }
                } else if msg.contains("Is a directory") {
                    Error::OpenDirectory {
                        path: msg.to_string(),
                    }
                } else {
                    Error::GrpcError {
                        message: format!("[{}] {}", status.code(), msg),
                        source: Box::new(status),
                    }
                }
            }
            _ => Error::GrpcError {
                message: format!("[{}] {}", status.code(), status.message()),
                source: Box::new(status),
            },
        }
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Error::TransportError {
            message: err.to_string(),
            source: Box::new(err),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

impl Error {
    /// Returns `true` if this error is retriable (transient network / unavailable).
    pub fn is_retriable(&self) -> bool {
        match self {
            Error::GrpcError { source, .. } => matches!(
                source.as_ref().code(),
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Aborted
            ),
            Error::TransportError { .. } => true,
            // Authentication failures are retriable — the SASL stream may have
            // expired (e.g. after process fork or long idle).  The caller should
            // invalidate the cached channel and re-authenticate before retrying.
            Error::AuthenticationFailed { .. } => true,
            _ => false,
        }
    }

    /// Returns `true` if the file was not found.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound { .. })
    }

    /// Returns `true` if the path already exists.
    pub fn is_already_exists(&self) -> bool {
        matches!(self, Error::AlreadyExists { .. })
    }

    /// Returns `true` if the file exists but is in INCOMPLETE (not yet closed) state.
    pub fn is_file_incomplete(&self) -> bool {
        matches!(self, Error::FileIncomplete { .. })
    }

    /// Returns `true` if the directory is not empty.
    pub fn is_directory_not_empty(&self) -> bool {
        matches!(self, Error::DirectoryNotEmpty { .. })
    }

    /// Returns `true` if the authentication credentials were rejected.
    ///
    /// When this returns `true` the caller should **not** mark the worker as
    /// failed — the worker itself is healthy.
    pub fn is_authentication_failed(&self) -> bool {
        matches!(self, Error::AuthenticationFailed { .. })
    }

    /// Returns `true` if the error is a permission / authentication problem.
    ///
    /// This covers both `PermissionDenied` (authorisation) and
    /// `AuthenticationFailed` (authentication).
    pub fn is_access_denied(&self) -> bool {
        matches!(
            self,
            Error::PermissionDenied { .. } | Error::AuthenticationFailed { .. }
        )
    }

    /// Convenience constructor for missing-field errors.
    pub fn missing_field(field: impl Into<String>) -> Self {
        Error::MissingField {
            field: field.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unauthenticated_maps_to_authentication_failed() {
        let status = tonic::Status::unauthenticated("token expired");
        let err = Error::from(status);
        assert!(err.is_authentication_failed());
        // AuthenticationFailed is now retriable — the caller should invalidate
        // the cached channel and re-authenticate before retrying.
        assert!(err.is_retriable());
    }

    #[test]
    fn test_failed_precondition_directory_not_empty() {
        let status =
            tonic::Status::failed_precondition("/foo/bar is not empty, cannot delete recursively");
        let err = Error::from(status);
        assert!(
            err.is_directory_not_empty(),
            "expected DirectoryNotEmpty, got {:?}",
            err
        );
    }

    #[test]
    fn test_failed_precondition_file_incomplete() {
        let status = tonic::Status::failed_precondition("/tmp/partial.parquet is incomplete");
        let err = Error::from(status);
        assert!(
            err.is_file_incomplete(),
            "expected FileIncomplete, got {:?}",
            err
        );
    }

    #[test]
    fn test_failed_precondition_is_directory() {
        let status = tonic::Status::failed_precondition("/data/dir Is a directory");
        let err = Error::from(status);
        assert!(
            matches!(err, Error::OpenDirectory { .. }),
            "expected OpenDirectory, got {:?}",
            err
        );
    }

    #[test]
    fn test_failed_precondition_unknown_falls_through_to_grpc_error() {
        let status = tonic::Status::failed_precondition("some other precondition failure");
        let err = Error::from(status);
        assert!(
            matches!(err, Error::GrpcError { .. }),
            "expected GrpcError fallthrough, got {:?}",
            err
        );
    }

    #[test]
    fn test_not_found_helper() {
        let status = tonic::Status::not_found("/missing");
        let err = Error::from(status);
        assert!(err.is_not_found());
    }

    #[test]
    fn test_is_access_denied_covers_both_variants() {
        let perm = Error::PermissionDenied {
            message: "no".to_string(),
        };
        let auth = Error::AuthenticationFailed {
            message: "expired".to_string(),
        };
        assert!(perm.is_access_denied());
        assert!(auth.is_access_denied());
    }
}
