//! Error types for the GooseFS Rust client.
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
    #[error("gRPC error: {message} — {source}")]
    GrpcError {
        message: String,
        source: tonic::Status,
    },

    /// gRPC transport / connection-level error.
    #[error("gRPC transport error: {message} — {source}")]
    TransportError {
        message: String,
        source: tonic::transport::Error,
    },

    /// The file or directory was not found on GooseFS.
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
}

// ---------------------------------------------------------------------------
// From conversions
// ---------------------------------------------------------------------------

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        // Map well-known gRPC codes to specific error variants
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
            _ => Error::GrpcError {
                message: format!("[{}] {}", status.code(), status.message()),
                source: status,
            },
        }
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Error::TransportError {
            message: err.to_string(),
            source: err,
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
                source.code(),
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Aborted
            ),
            Error::TransportError { .. } => true,
            _ => false,
        }
    }

    /// Convenience constructor for missing-field errors.
    pub fn missing_field(field: impl Into<String>) -> Self {
        Error::MissingField {
            field: field.into(),
        }
    }
}
