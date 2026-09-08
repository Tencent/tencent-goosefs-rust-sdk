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

//! JNI conversions for String / Map / byte arrays.

use std::collections::HashMap;

use jni::jni_sig;
use jni::jni_str;
use jni::objects::JByteArray;
use jni::objects::JList;
use jni::objects::JMap;
use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::JValue;
use jni::sys::jboolean;
use jni::sys::jint;
use jni::sys::jlong;
use jni::Env;

use crate::error::Error;
use crate::Result;

pub(crate) fn jmap_to_hashmap(env: &mut Env, params: &JObject) -> Result<HashMap<String, String>> {
    if params.is_null() {
        return Ok(HashMap::new());
    }
    let map = env.new_cast_local_ref::<JMap>(params)?;
    let mut iter = map.iter(env)?;
    let mut result: HashMap<String, String> = HashMap::new();
    while let Some(entry) = iter.next(env)? {
        let k = entry.key(env)?;
        let v = entry.value(env)?;
        // SAFETY: callers pass `Map<String, String>`.
        let k = unsafe { JString::from_raw(env, k.into_raw()) };
        let v = unsafe { JString::from_raw(env, v.into_raw()) };
        result.insert(jstring_to_string(env, &k)?, jstring_to_string(env, &v)?);
    }
    Ok(result)
}

pub(crate) fn jstring_to_string(env: &mut Env, s: &JString) -> Result<String> {
    Ok(s.mutf8_chars(env)?.into())
}

pub(crate) fn optional_jstring_to_string(env: &mut Env, s: &JObject) -> Result<Option<String>> {
    if s.is_null() {
        return Ok(None);
    }
    // SAFETY: callers pass `String` or null.
    let s = unsafe { JString::from_raw(env, s.as_raw()) };
    Ok(Some(jstring_to_string(env, &s)?))
}

pub(crate) fn millis_as_jlong(millis: u128) -> jlong {
    jlong::try_from(millis).unwrap_or(jlong::MAX)
}

pub(crate) fn string_list<'a>(env: &mut Env<'a>, items: &[String]) -> Result<JObject<'a>> {
    let list = env.new_object(jni_str!("java/util/ArrayList"), jni_sig!("()V"), &[])?;
    for item in items {
        let s = env.new_string(item)?;
        env.call_method(
            &list,
            jni_str!("add"),
            jni_sig!("(Ljava/lang/Object;)Z"),
            &[JValue::Object(&s)],
        )?;
    }
    Ok(list)
}

pub(crate) fn boxed_integer<'a>(env: &mut Env<'a>, value: Option<i32>) -> Result<JObject<'a>> {
    match value {
        None => Ok(JObject::null()),
        Some(v) => Ok(env
            .call_static_method(
                jni_str!("java/lang/Integer"),
                jni_str!("valueOf"),
                jni_sig!("(I)Ljava/lang/Integer;"),
                &[JValue::Int(v)],
            )?
            .l()?),
    }
}

pub(crate) fn boxed_boolean<'a>(env: &mut Env<'a>, value: bool) -> Result<JObject<'a>> {
    Ok(env
        .call_static_method(
            jni_str!("java/lang/Boolean"),
            jni_str!("valueOf"),
            jni_sig!("(Z)Ljava/lang/Boolean;"),
            &[JValue::Bool(value as jboolean)],
        )?
        .l()?)
}

pub(crate) fn boxed_long<'a>(env: &mut Env<'a>, value: i64) -> Result<JObject<'a>> {
    Ok(env
        .call_static_method(
            jni_str!("java/lang/Long"),
            jni_str!("valueOf"),
            jni_sig!("(J)Ljava/lang/Long;"),
            &[JValue::Long(value)],
        )?
        .l()?)
}

pub(crate) fn jbyte_array_to_vec(env: &mut Env, array: &JByteArray) -> Result<Vec<u8>> {
    if array.is_null() {
        return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: "data must not be null".into(),
        }));
    }
    let len = array.len(env)?;
    if len > i32::MAX as usize {
        return Err(payload_too_large(len));
    }
    let mut buf = vec![0i8; len];
    array.get_region(env, 0, &mut buf)?;
    Ok(buf.into_iter().map(|b| b as u8).collect())
}

pub(crate) fn bytes_to_jbyte_array<'a>(env: &mut Env<'a>, data: &[u8]) -> Result<JObject<'a>> {
    if data.len() > i32::MAX as usize {
        return Err(payload_too_large(data.len()));
    }
    Ok(env.byte_array_from_slice(data)?.into())
}

pub(crate) fn u64_as_jlong(n: u64) -> Result<jlong> {
    i64::try_from(n).map_err(|_| {
        Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: format!("byte count {n} does not fit in Java long"),
        })
    })
}

fn payload_too_large(len: usize) -> Error {
    Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
        message: format!("payload length {len} exceeds Integer.MAX_VALUE; use streaming I/O"),
    })
}

pub(crate) fn new_array_list<'a>(env: &mut Env<'a>, cap: usize) -> Result<JObject<'a>> {
    let cap = i32::try_from(cap).unwrap_or(i32::MAX);
    Ok(env.new_object(
        jni_str!("java/util/ArrayList"),
        jni_sig!("(I)V"),
        &[JValue::Int(cap)],
    )?)
}

pub(crate) fn array_list_add(env: &mut Env, list: &JObject, item: &JObject) -> Result<()> {
    env.call_method(
        list,
        jni_str!("add"),
        jni_sig!("(Ljava/lang/Object;)Z"),
        &[JValue::Object(item)],
    )?;
    Ok(())
}

pub(crate) fn jlist_to_strings(env: &mut Env, list: &JObject) -> Result<Vec<String>> {
    if list.is_null() {
        return Ok(Vec::new());
    }
    let list = env.new_cast_local_ref::<JList>(list)?;
    let size = list.size(env)?;
    let mut out = Vec::with_capacity(size as usize);
    for i in 0..size {
        let item = list.get(env, i)?;
        if item.is_null() {
            return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
                message: format!("paths[{i}] must not be null"),
            }));
        }
        // SAFETY: List<String> at the JNI boundary.
        let s = unsafe { JString::from_raw(env, item.as_raw()) };
        out.push(jstring_to_string(env, &s)?);
    }
    Ok(out)
}

pub(crate) fn bools_to_jlist<'a>(env: &mut Env<'a>, items: Vec<bool>) -> Result<JObject<'a>> {
    let list = new_array_list(env, items.len())?;
    for b in items {
        let boxed = boxed_boolean(env, b)?;
        array_list_add(env, &list, &boxed)?;
        env.delete_local_ref(boxed);
    }
    Ok(list)
}

pub(crate) fn i64s_to_jlist<'a>(env: &mut Env<'a>, items: Vec<i64>) -> Result<JObject<'a>> {
    let list = new_array_list(env, items.len())?;
    for n in items {
        let boxed = boxed_long(env, n)?;
        array_list_add(env, &list, &boxed)?;
        env.delete_local_ref(boxed);
    }
    Ok(list)
}

#[allow(dead_code)]
pub(crate) fn jint_as_usize(v: jint) -> Result<usize> {
    usize::try_from(v).map_err(|_| {
        Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: format!("negative length {v}"),
        })
    })
}
