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
//! use goosefs_client::io::{GooseFsFileWriter, GooseFsFileReader};
//! use goosefs_client::config::GooseFsConfig;
//!
//! #[tokio::main]
//! async fn main() -> goosefs_client::error::Result<()> {
//!     let config = GooseFsConfig::new("127.0.0.1:9200");
//!
//!     // Write a file
//!     GooseFsFileWriter::write_file(&config, "/my-file.txt", b"Hello!").await?;
//!
//!     // Read it back
//!     let data = GooseFsFileReader::read_file(&config, "/my-file.txt").await?;
//!     println!("read {} bytes", data.len());
//!
//!     Ok(())
//! }
//! ```
//!
//! # Low-Level API
//!
//! ```rust,no_run
//! use goosefs_client::client::MasterClient;
//! use goosefs_client::config::GooseFsConfig;
//!
//! #[tokio::main]
//! async fn main() -> goosefs_client::error::Result<()> {
//!     let config = GooseFsConfig::default();
//!     let master = MasterClient::connect(&config).await?;
//!     let file_info = master.get_status("/my-file.txt").await?;
//!     println!("file length: {:?}", file_info.length);
//!     Ok(())
//! }
//! ```

pub mod block;
pub mod client;
pub mod config;
pub mod error;
pub mod io;
pub mod retry;

// Re-export commonly used types for convenience.
pub use crate::config::WriteType;
pub use crate::config::{
    ENV_BLOCK_SIZE, ENV_CHUNK_SIZE, ENV_MASTER_ADDR, ENV_WRITE_TYPE, STORAGE_OPT_BLOCK_SIZE,
    STORAGE_OPT_CHUNK_SIZE, STORAGE_OPT_MASTER_ADDR, STORAGE_OPT_WRITE_TYPE,
};
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
///                                                → `proto(root)::proto::security::Capability` ✓
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
