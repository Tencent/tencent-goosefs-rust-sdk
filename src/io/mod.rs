//! I/O modules: block-level streaming and high-level file read/write over gRPC.
//!
//! This module provides two layers of abstraction:
//!
//! ## Low-level: Block-level streaming I/O
//!
//! - [`GrpcBlockReader`] — Bidirectional streaming reader for a single block
//! - [`GrpcBlockWriter`] — Bidirectional streaming writer for a single block
//!
//! These are the building blocks that directly wrap the gRPC `ReadBlock` /
//! `WriteBlock` RPCs.
//!
//! ## High-level: File-level I/O (recommended)
//!
//! - [`GooseFsFileReader`] — Orchestrates the full read pipeline:
//!   `GetStatus` → `BlockMapper` → `WorkerRouter` → `GrpcBlockReader` per block
//! - [`GooseFsFileWriter`] — Orchestrates the full write pipeline:
//!   `CreateFile` → `BlockMapper` → `WorkerRouter` → `GrpcBlockWriter` per block → `CompleteFile`
//!
//! The high-level APIs are the recommended entry point for most users.

pub mod file_reader;
pub mod file_writer;
pub mod reader;
pub mod writer;

pub use file_reader::GooseFsFileReader;
pub use file_writer::GooseFsFileWriter;
pub use reader::GrpcBlockReader;
pub use writer::GrpcBlockWriter;
