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
//! - [`GoosefsFileReader`] — Orchestrates the full read pipeline (sequential,
//!   block-by-block).
//! - [`GoosefsFileInStream`] — Seekable dual-path file stream: supports
//!   sequential reads, `seek`, and positioned random reads (`read_at`).
//! - [`GoosefsFileWriter`] — Orchestrates the full write pipeline.
//!
//! The high-level APIs are the recommended entry point for most users.

pub mod file_in_stream;
pub mod file_reader;
pub mod file_writer;
pub mod reader;
pub mod writer;

pub use file_in_stream::GoosefsFileInStream;
pub use file_reader::GoosefsFileReader;
pub use file_writer::GoosefsFileWriter;
pub use reader::GrpcBlockReader;
pub use writer::GrpcBlockWriter;
