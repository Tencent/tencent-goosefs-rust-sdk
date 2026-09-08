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

//! Copy `URIStatus` into a Java POJO (no native lifetime).

use goosefs_sdk::fs::URIStatus;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::sys::jboolean;
use jni::sys::jint;
use jni::sys::jlong;
use jni::Env;
use jni::EnvUnowned;

use crate::convert::{array_list_add, new_array_list};
use crate::error::{Error, ThrowException};
use crate::Result;

pub(crate) fn make_uri_status<'local>(
    env: &mut Env<'local>,
    status: URIStatus,
) -> Result<JObject<'local>> {
    let name = env.new_string(&status.name)?;
    let path = env.new_string(&status.path)?;
    let ufs_path = env.new_string(&status.ufs_path)?;
    let owner = env.new_string(&status.owner)?;
    let group = env.new_string(&status.group)?;
    let persistence_state = env.new_string(&status.persistence_state)?;
    let ufs_fingerprint = env.new_string(&status.ufs_fingerprint)?;

    let block_ids = env.new_long_array(status.block_ids.len())?;
    let ids = status.block_ids.clone();
    block_ids.set_region(env, 0, &ids)?;

    let xattr = env.new_object(jni_str!("java/util/HashMap"), jni_sig!("()V"), &[])?;
    for (k, v) in &status.xattr {
        let key = env.new_string(k)?;
        let val = env.byte_array_from_slice(v)?;
        env.call_method(
            &xattr,
            jni_str!("put"),
            jni_sig!("(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;"),
            &[JValue::Object(&key), JValue::Object(&val)],
        )?;
    }

    let symlink = match &status.symlink {
        Some(s) => env.new_string(s)?.into(),
        None => JObject::null(),
    };

    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/URIStatus"),
        jni_sig!(
            "(JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;JJ[JJJJZZZZZIILjava/lang/String;Ljava/lang/String;ILjava/lang/String;JLjava/lang/String;Ljava/util/Map;Ljava/lang/String;)V"
        ),
        &[
            JValue::Long(status.file_id),
            JValue::Object(&name),
            JValue::Object(&path),
            JValue::Object(&ufs_path),
            JValue::Long(status.length),
            JValue::Long(status.block_size_bytes),
            JValue::Object(&block_ids),
            JValue::Long(status.creation_time_ms),
            JValue::Long(status.last_modification_time_ms),
            JValue::Long(status.last_access_time_ms),
            JValue::Bool(status.completed as jboolean),
            JValue::Bool(status.folder as jboolean),
            JValue::Bool(status.cacheable as jboolean),
            JValue::Bool(status.persisted as jboolean),
            JValue::Bool(status.mount_point as jboolean),
            JValue::Int(status.in_goose_fs_percentage),
            JValue::Int(status.in_memory_percentage),
            JValue::Object(&owner),
            JValue::Object(&group),
            JValue::Int(status.mode),
            JValue::Object(&persistence_state),
            JValue::Long(status.mount_id),
            JValue::Object(&ufs_fingerprint),
            JValue::Object(&xattr),
            JValue::Object(&symlink),
        ],
    )?)
}

pub(crate) fn make_status_array_list<'local>(
    env: &mut Env<'local>,
    items: Vec<URIStatus>,
) -> Result<JObject<'local>> {
    let cap = i32::try_from(items.len()).unwrap_or(i32::MAX);
    let list = env.new_object(
        jni_str!("java/util/ArrayList"),
        jni_sig!("(I)V"),
        &[JValue::Int(cap)],
    )?;
    for status in items {
        let obj = make_uri_status(env, status)?;
        env.call_method(
            &list,
            jni_str!("add"),
            jni_sig!("(Ljava/lang/Object;)Z"),
            &[JValue::Object(&obj)],
        )?;
        env.delete_local_ref(obj);
    }
    Ok(list)
}

pub(crate) fn make_nested_status_lists<'local>(
    env: &mut Env<'local>,
    items: Vec<Vec<URIStatus>>,
) -> Result<JObject<'local>> {
    let outer = new_array_list(env, items.len())?;
    for v in items {
        let inner = make_status_array_list(env, v)?;
        array_list_add(env, &outer, &inner)?;
        env.delete_local_ref(inner);
    }
    Ok(outer)
}

pub(crate) fn make_grouped_lists<'local>(
    env: &mut Env<'local>,
    items: Vec<Vec<URIStatus>>,
) -> Result<JObject<'local>> {
    let outer = new_array_list(env, items.len())?;
    for v in items {
        let inner = make_uri_status_list(env, v)?;
        array_list_add(env, &outer, &inner)?;
        env.delete_local_ref(inner);
    }
    Ok(outer)
}

pub(crate) struct JavaUriStatusList {
    items: Vec<URIStatus>,
}

pub(crate) fn make_uri_status_list<'local>(
    env: &mut Env<'local>,
    items: Vec<URIStatus>,
) -> Result<JObject<'local>> {
    let handle = Box::into_raw(Box::new(JavaUriStatusList { items })) as jlong;
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/URIStatusList"),
        jni_sig!("(J)V"),
        &[JValue::Long(handle)],
    )?)
}

fn list_ptr(op: *mut JavaUriStatusList) -> Result<&'static JavaUriStatusList> {
    if op.is_null() {
        return Err(Error::IllegalState("URIStatusList is closed".into()));
    }
    // SAFETY: Java `NativeObject` owns this pointer until `disposeInternal`.
    Ok(unsafe { &*op })
}

/// # Safety
///
/// `op` must be the pointer from `make_uri_status_list` and not yet disposed.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_URIStatusList_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    op: *mut JavaUriStatusList,
) {
    if !op.is_null() {
        unsafe {
            drop(Box::from_raw(op));
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_URIStatusList_nativeSize<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaUriStatusList,
) -> jint {
    env.with_env(|_env| intern_size(op))
        .resolve::<ThrowException>()
}

fn intern_size(op: *mut JavaUriStatusList) -> Result<jint> {
    let list = list_ptr(op)?;
    i32::try_from(list.items.len()).map_err(|_| {
        Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: format!(
                "URIStatusList length {} does not fit in int",
                list.items.len()
            ),
        })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_URIStatusList_nativeGet<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaUriStatusList,
    index: jint,
) -> JObject<'local> {
    env.with_env(|env| intern_get(env, op, index))
        .resolve::<ThrowException>()
}

fn intern_get<'local>(
    env: &mut Env<'local>,
    op: *mut JavaUriStatusList,
    index: jint,
) -> Result<JObject<'local>> {
    let list = list_ptr(op)?;
    let len = list.items.len();
    if index < 0 || (index as usize) >= len {
        return Err(Error::IndexOutOfBounds(format!(
            "index {index} out of range for length {len}"
        )));
    }
    make_uri_status(env, list.items[index as usize].clone())
}
