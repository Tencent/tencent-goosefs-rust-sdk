//! GooseFS Rust gRPC Client
//!
//! A Rust client library that communicates with GooseFS Master/Worker
//! via gRPC (tonic/protobuf). This is the **Layer 3** crate in the
//! Lance → OpenDAL → GooseFS architecture.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │  ★ High-Level API (recommended)                     │
//! │  GooseFsFileWriter — end-to-end file write pipeline │
//! │  GooseFsFileReader — end-to-end file read pipeline  │
//! ├─────────────────────────────────────────────────────┤
//! │  MasterClient    — File metadata CRUD (Master:9200) │
//! │  WorkerMgrClient — Worker discovery  (Master:9200)  │
//! │  VersionClient   — Service handshake (Master:9200)  │
//! │  WorkerClient    — Block streaming   (Worker:9203)  │
//! ├─────────────────────────────────────────────────────┤
//! │  BlockMapper     — file range → block read plans    │
//! │  WorkerRouter    — consistent hash block→worker     │
//! ├─────────────────────────────────────────────────────┤
//! │  GrpcBlockReader — bidirectional streaming read     │
//! │  GrpcBlockWriter — bidirectional streaming write    │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Quick Start — High-Level API
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use goosefs_sdk::context::FileSystemContext;
//! use goosefs_sdk::io::{GooseFsFileWriter, GooseFsFileReader};
//! use goosefs_sdk::config::GooseFsConfig;
//!
//! #[tokio::main]
//! async fn main() -> goosefs_sdk::error::Result<()> {
//!     // Build once — the only call that performs TCP+SASL.
//!     let ctx = FileSystemContext::connect(GooseFsConfig::new("127.0.0.1:9200")).await?;
//!
//!     // Write a file (zero new connections — reuses ctx).
//!     GooseFsFileWriter::write_file_with_context(ctx.clone(), "/my-file.txt", b"Hello!").await?;
//!
//!     // Read it back.
//!     let data = GooseFsFileReader::read_file_with_context(ctx.clone(), "/my-file.txt").await?;
//!     println!("read {} bytes", data.len());
//!
//!     Ok(())
//! }
//! ```
//!
//! # Low-Level API
//!
//! ```rust,no_run
//! use goosefs_sdk::client::MasterClient;
//! use goosefs_sdk::config::GooseFsConfig;
//!
//! #[tokio::main]
//! async fn main() -> goosefs_sdk::error::Result<()> {
//!     let config = GooseFsConfig::default();
//!     let master = MasterClient::connect(&config).await?;
//!     let file_info = master.get_status("/my-file.txt").await?;
//!     println!("file length: {:?}", file_info.length);
//!     Ok(())
//! }
//! ```

pub mod auth;
pub mod block;
pub mod client;
pub mod config;
pub mod context;
pub mod error;
pub mod fs;
pub mod io;
pub mod retry;

// Re-export commonly used types for convenience.
pub use crate::config::{ConfigRefresher, TransparentAccelerationSwitch, WriteType};
pub use crate::config::{
    ENV_AUTHORIZATION_PERMISSION_ENABLED, ENV_AUTH_TYPE, ENV_AUTH_USERNAME, ENV_BLOCK_SIZE,
    ENV_CHUNK_SIZE, ENV_CONFIG_MANAGER_RPC_ADDRESSES, ENV_CONFIG_RPC_PORT,
    ENV_LOGIN_IMPERSONATION_USERNAME, ENV_MASTER_ADDR,
    ENV_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED, ENV_TRANSPARENT_ACCELERATION_ENABLED,
    ENV_WRITE_TYPE, IMPERSONATION_NONE, STORAGE_OPT_AUTHORIZATION_PERMISSION_ENABLED,
    STORAGE_OPT_AUTH_TYPE, STORAGE_OPT_AUTH_USERNAME, STORAGE_OPT_BLOCK_SIZE,
    STORAGE_OPT_CHUNK_SIZE, STORAGE_OPT_CONFIG_MANAGER_RPC_ADDRESSES, STORAGE_OPT_CONFIG_RPC_PORT,
    STORAGE_OPT_LOGIN_IMPERSONATION_USERNAME, STORAGE_OPT_MASTER_ADDR,
    STORAGE_OPT_TRANSPARENT_ACCELERATION_COSRANGER_ENABLED,
    STORAGE_OPT_TRANSPARENT_ACCELERATION_ENABLED, STORAGE_OPT_WRITE_TYPE,
};
pub use crate::context::FileSystemContext;
pub use crate::proto::grpc::file::WritePType;

/// Generated protobuf / gRPC types from GooseFS `.proto` definitions.
///
/// The module layout must mirror the proto package hierarchy exactly so that
/// prost-generated `super::` relative paths resolve correctly:
///
/// ```text
/// proto (root)
/// ├── grpc           — com.qcloud.cos.goosefs.grpc  (WorkerNetAddress, BlockInfo …)
/// │   ├── file       — com.qcloud.cos.goosefs.grpc.file  (FileSystemMasterClientService)
/// │   ├── block      — com.qcloud.cos.goosefs.grpc.block (BlockWorker, WorkerManagerMaster…)
/// │   ├── version    — com.qcloud.cos.goosefs.grpc.version (ServiceVersionClientService)
/// │   └── fscommon   — com.qcloud.cos.goosefs.grpc.fscommon (LoadDescendantPType)
/// └── proto          — (intermediate)
///     ├── dataserver — com.qcloud.cos.goosefs.proto.dataserver
///     ├── security   — com.qcloud.cos.goosefs.proto.security (Capability, DelegationToken)
///     ├── shared     — com.qcloud.cos.goosefs.proto.shared   (AccessControlList)
///     └── status     — com.qcloud.cos.goosefs.proto.status   (PStatus)
/// ```
///
/// Key path resolutions from generated code:
/// - `grpc::file` uses `super::Bits`              → `grpc::Bits`   ✓
/// - `grpc::file` uses `super::super::proto::security::Capability`
///   → `proto(root)::proto::security::Capability` ✓
/// - `proto::dataserver` uses `super::shared::*`  → `proto(inner)::shared::*` ✓
pub mod proto {
    pub mod grpc {
        include!("generated/com.qcloud.cos.goosefs.grpc.rs");

        pub mod file {
            include!("generated/com.qcloud.cos.goosefs.grpc.file.rs");
        }

        pub mod block {
            include!("generated/com.qcloud.cos.goosefs.grpc.block.rs");
        }

        pub mod version {
            include!("generated/com.qcloud.cos.goosefs.grpc.version.rs");
        }

        pub mod fscommon {
            include!("generated/com.qcloud.cos.goosefs.grpc.fscommon.rs");
        }

        pub mod sasl {
            include!("generated/com.qcloud.cos.goosefs.grpc.sasl.rs");
        }
    }

    pub mod proto {
        pub mod dataserver {
            include!("generated/com.qcloud.cos.goosefs.proto.dataserver.rs");
        }

        pub mod security {
            include!("generated/com.qcloud.cos.goosefs.proto.security.rs");
        }

        pub mod shared {
            include!("generated/com.qcloud.cos.goosefs.proto.shared.rs");
        }

        pub mod status {
            include!("generated/com.qcloud.cos.goosefs.proto.status.rs");
        }
    }
}
