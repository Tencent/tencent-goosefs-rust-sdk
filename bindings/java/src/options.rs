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

//! Parse Java option objects into SDK / proto types.

use goosefs_sdk::config::WriteType;
use goosefs_sdk::fs::options::{
    CreateFileOptions, DeleteOptions, InStreamOptions, OpenFileOptions, ReadType,
};
use goosefs_sdk::fs::write_type::WriteTypeXAttr;
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JObject;
use jni::Env;

use crate::error::Error;
use crate::Result;

pub(crate) fn parse_read_range(offset: i64, length: i64) -> Result<(u64, u64)> {
    if offset < 0 || length < 0 {
        return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: format!(
                "read_range offset and length must be non-negative (offset={offset}, length={length})"
            ),
        }));
    }
    Ok((offset as u64, length as u64))
}

pub(crate) fn parse_delete_options(env: &mut Env, options: &JObject) -> Result<DeleteOptions> {
    if options.is_null() {
        return Ok(DeleteOptions::default());
    }
    let recursive = env
        .call_method(options, jni_str!("isRecursive"), jni_sig!("()Z"), &[])?
        .z()?;
    let unchecked = env
        .call_method(options, jni_str!("isUnchecked"), jni_sig!("()Z"), &[])?
        .z()?;
    let goosefs_only = env
        .call_method(options, jni_str!("isGoosefsOnly"), jni_sig!("()Z"), &[])?
        .z()?;
    Ok(DeleteOptions {
        recursive,
        unchecked,
        goosefs_only,
    })
}

/// `writeType == null` omits proto `write_type` so the SDK inherits parent xattr.
pub(crate) fn parse_create_file_options(
    env: &mut Env,
    options: &JObject,
) -> Result<CreateFilePOptions> {
    if options.is_null() {
        return Ok(CreateFilePOptions {
            recursive: Some(false),
            ..Default::default()
        });
    }

    let recursive = env
        .call_method(options, jni_str!("isRecursive"), jni_sig!("()Z"), &[])?
        .z()?;
    let write_type_obj = env
        .call_method(
            options,
            jni_str!("getWriteType"),
            jni_sig!("()Lcom/tencent/goosefs/WriteType;"),
            &[],
        )?
        .l()?;
    let block_size_obj = env
        .call_method(
            options,
            jni_str!("getBlockSizeBytes"),
            jni_sig!("()Ljava/lang/Long;"),
            &[],
        )?
        .l()?;

    let write_type = if write_type_obj.is_null() {
        None
    } else {
        let proto = env
            .call_method(&write_type_obj, jni_str!("getProto"), jni_sig!("()I"), &[])?
            .i()?;
        if !(1..=5).contains(&proto) {
            return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
                message: format!("WriteType proto value {proto} is not in 1..=5"),
            }));
        }
        Some(proto)
    };

    let block_size_bytes = if block_size_obj.is_null() {
        None
    } else {
        Some(
            env.call_method(&block_size_obj, jni_str!("longValue"), jni_sig!("()J"), &[])?
                .j()?,
        )
    };

    Ok(CreateFilePOptions {
        block_size_bytes,
        recursive: Some(recursive),
        write_type,
        ..Default::default()
    })
}

fn parse_write_type_field(env: &mut Env, options: &JObject) -> Result<Option<WriteType>> {
    let write_type_obj = env
        .call_method(
            options,
            jni_str!("getWriteType"),
            jni_sig!("()Lcom/tencent/goosefs/WriteType;"),
            &[],
        )?
        .l()?;
    if write_type_obj.is_null() {
        return Ok(None);
    }
    let proto = env
        .call_method(&write_type_obj, jni_str!("getProto"), jni_sig!("()I"), &[])?
        .i()?;
    let wt = match proto {
        1 => WriteType::MustCache,
        2 => WriteType::TryCache,
        3 => WriteType::CacheThrough,
        4 => WriteType::Through,
        5 => WriteType::AsyncThrough,
        other => {
            return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
                message: format!("WriteType proto value {other} is not in 1..=5"),
            }));
        }
    };
    Ok(Some(wt))
}

/// SDK `CreateFileOptions` for streaming `createFile` (xattr inherit when writeType is null).
pub(crate) fn parse_sdk_create_file_options(
    env: &mut Env,
    options: &JObject,
) -> Result<CreateFileOptions> {
    if options.is_null() {
        return Ok(CreateFileOptions::default());
    }
    let recursive = env
        .call_method(options, jni_str!("isRecursive"), jni_sig!("()Z"), &[])?
        .z()?;
    let write_type = parse_write_type_field(env, options)?;
    let block_size_obj = env
        .call_method(
            options,
            jni_str!("getBlockSizeBytes"),
            jni_sig!("()Ljava/lang/Long;"),
            &[],
        )?
        .l()?;
    let replication_obj = env
        .call_method(
            options,
            jni_str!("getReplicationMax"),
            jni_sig!("()Ljava/lang/Integer;"),
            &[],
        )?
        .l()?;
    let block_size_bytes = if block_size_obj.is_null() {
        None
    } else {
        Some(
            env.call_method(&block_size_obj, jni_str!("longValue"), jni_sig!("()J"), &[])?
                .j()?,
        )
    };
    let replication_max = if replication_obj.is_null() {
        None
    } else {
        Some(
            env.call_method(&replication_obj, jni_str!("intValue"), jni_sig!("()I"), &[])?
                .i()?,
        )
    };
    Ok(CreateFileOptions {
        write_type: match write_type {
            Some(wt) => WriteTypeXAttr::Explicit(wt),
            None => WriteTypeXAttr::Inherit,
        },
        block_size_bytes,
        replication_max,
        recursive,
    })
}

pub(crate) fn parse_open_file_options(env: &mut Env, options: &JObject) -> Result<OpenFileOptions> {
    if options.is_null() {
        return Ok(OpenFileOptions::default());
    }
    let read_type_obj = env
        .call_method(
            options,
            jni_str!("getReadType"),
            jni_sig!("()Lcom/tencent/goosefs/ReadType;"),
            &[],
        )?
        .l()?;
    let read_type = if read_type_obj.is_null() {
        ReadType::Cache
    } else {
        let proto = env
            .call_method(&read_type_obj, jni_str!("getProto"), jni_sig!("()I"), &[])?
            .i()?;
        match proto {
            1 => ReadType::NoCache,
            2 => ReadType::Cache,
            other => {
                return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
                    message: format!("ReadType proto value {other} is not 1 or 2"),
                }));
            }
        }
    };
    Ok(OpenFileOptions {
        in_stream_options: InStreamOptions {
            read_type,
            ..Default::default()
        },
    })
}

pub(crate) fn resolve_seek(whence: i32, offset: i64, pos: i64, file_length: i64) -> Result<i64> {
    match whence {
        0 => {
            if offset < 0 {
                return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
                    message: "negative seek offset is invalid for SEEK_SET".into(),
                }));
            }
            Ok(offset)
        }
        1 => Ok(pos + offset),
        2 => Ok(file_length + offset),
        other => Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: format!("invalid whence value: {other} (expected 0, 1, or 2)"),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_range_rejects_negatives() {
        assert!(parse_read_range(-1, 1).is_err());
        assert!(parse_read_range(0, -1).is_err());
        assert_eq!(parse_read_range(0, 0).unwrap(), (0, 0));
        assert_eq!(parse_read_range(10, 5).unwrap(), (10, 5));
    }

    #[test]
    fn seek_set_cur_end() {
        assert_eq!(resolve_seek(0, 50, 10, 100).unwrap(), 50);
        assert_eq!(resolve_seek(1, 10, 55, 100).unwrap(), 65);
        assert_eq!(resolve_seek(2, -5, 0, 100).unwrap(), 95);
        assert!(resolve_seek(0, -1, 0, 100).is_err());
        assert!(resolve_seek(99, 0, 0, 100).is_err());
    }
}
