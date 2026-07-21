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
//!
//! ## Trait-style adapter
//!
//! - [`GoosefsAsyncReader`] — `tokio::io::AsyncRead + AsyncSeek` adapter
//!   over [`GoosefsFileInStream`]. Use it when you want to plug a Goosefs
//!   stream into ecosystem tools that take any `AsyncRead` (e.g.
//!   `tokio::io::copy`, `tokio::io::BufReader`, `tokio_util::io::ReaderStream`,
//!   the future opendal `goosefs` adapter, JNI / C bindings).

pub mod async_reader;
pub mod file_in_stream;
pub mod file_reader;
pub mod file_writer;
pub(crate) mod range_coalesce;
pub mod reader;
pub mod writer;

pub use async_reader::GoosefsAsyncReader;
pub use file_in_stream::GoosefsFileInStream;
pub use file_reader::GoosefsFileReader;
pub use file_writer::GoosefsFileWriter;
pub use reader::GrpcBlockReader;
pub use writer::GrpcBlockWriter;
