//! GooseFS filesystem abstractions.
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
