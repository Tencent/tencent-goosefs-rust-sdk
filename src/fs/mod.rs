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

//! Goosefs filesystem abstractions.
//!
//! This module houses all high-level filesystem types:
//! - [`options`]          — options structs for file-system operations.
//! - [`uri_status`]       — immutable file/directory metadata snapshot.
//! - [`write_type`]       — `WriteType` xattr helpers.
//! - [`filesystem`]       — `FileSystem` trait.
//! - [`base_filesystem`]  — `BaseFileSystem` implementation.

pub mod base_filesystem;
pub mod filesystem;
pub mod options;
pub mod uri_status;
pub mod write_type;

pub use base_filesystem::BaseFileSystem;
pub use filesystem::FileSystem;
pub use options::{CreateFileOptions, DeleteOptions, InStreamOptions, OpenFileOptions, ReadType};
pub use uri_status::URIStatus;
pub use write_type::{get_write_type_from_xattr, WriteTypeXAttr, WRITE_TYPE_XATTR_KEY};
