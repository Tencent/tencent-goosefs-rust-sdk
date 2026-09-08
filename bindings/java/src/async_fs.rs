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

//! Async JNI methods for `AsyncGoosefs`.

use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::FileSystem;
use goosefs_sdk::io::GoosefsFileReader;
use goosefs_sdk::io::GoosefsFileWriter;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JByteArray;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::JValue;
use jni::sys::jboolean;
use jni::sys::jint;
use jni::sys::jlong;
use jni::Env;
use jni::EnvUnowned;
use jni::JavaVM;

use crate::config::parse_config_from_java;
use crate::convert::{
    bools_to_jlist, boxed_boolean, boxed_long, bytes_to_jbyte_array, i64s_to_jlist,
    jbyte_array_to_vec, jlist_to_strings, jstring_to_string, optional_jstring_to_string,
    u64_as_jlong,
};
use crate::error::{Error, ThrowException};
use crate::executor::{executor_or_default, Executor};
use crate::handle::JavaFsHandle;
use crate::options::{
    parse_create_file_options, parse_delete_options, parse_open_file_options, parse_read_range,
    parse_sdk_create_file_options,
};
use crate::status::{
    make_grouped_lists, make_nested_status_lists, make_status_array_list, make_uri_status,
    make_uri_status_list,
};
use crate::streaming::{make_async_file_reader, make_async_file_writer, make_reader_list};
use crate::worker::make_async_worker_client;
use crate::Result;

pub(crate) fn request_id(env: &mut Env) -> Result<jlong> {
    Ok(env
        .call_static_method(
            jni_str!("com/tencent/goosefs/AsyncRegistry"),
            jni_str!("requestId"),
            jni_sig!("()J"),
            &[],
        )?
        .j()?)
}

fn get_future<'local>(env: &mut Env<'local>, id: jlong) -> Result<JObject<'local>> {
    Ok(env
        .call_static_method(
            jni_str!("com/tencent/goosefs/AsyncRegistry"),
            jni_str!("get"),
            jni_sig!("(J)Ljava/util/concurrent/CompletableFuture;"),
            &[JValue::Long(id)],
        )?
        .l()?)
}

pub(crate) fn complete_future<F>(id: jlong, build: F)
where
    F: for<'a> FnOnce(&mut Env<'a>) -> Result<JObject<'a>>,
{
    let vm = JavaVM::singleton().expect("JavaVM singleton must be initialized");
    vm.attach_current_thread(|env| -> Result<()> {
        let future = get_future(env, id)?;
        match build(env) {
            Ok(object) => {
                env.call_method(
                    &future,
                    jni_str!("complete"),
                    jni_sig!("(Ljava/lang/Object;)Z"),
                    &[JValue::Object(&object)],
                )?;
            }
            Err(err) => {
                let exception = err.to_exception(env)?;
                env.call_method(
                    &future,
                    jni_str!("completeExceptionally"),
                    jni_sig!("(Ljava/lang/Throwable;)Z"),
                    &[JValue::Object(&exception)],
                )?;
            }
        }
        Ok(())
    })
    .expect("complete future must succeed");
}

fn make_async_goosefs<'local>(
    env: &mut Env<'local>,
    handle: jlong,
    executor_handle: jlong,
) -> Result<JObject<'local>> {
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/AsyncGoosefs"),
        jni_sig!("(JJ)V"),
        &[JValue::Long(handle), JValue::Long(executor_handle)],
    )?)
}

fn handle_ptr(op: *mut JavaFsHandle) -> Result<&'static JavaFsHandle> {
    if op.is_null() {
        return Err(Error::IllegalState("client is closed".into()));
    }
    // SAFETY: Java `NativeObject` owns this pointer until `disposeInternal`.
    Ok(unsafe { &*op })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeConnect<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    config: JObject<'local>,
    executor: *const Executor,
) -> jlong {
    env.with_env(|env| intern_connect(env, config, executor))
        .resolve::<ThrowException>()
}

fn intern_connect(env: &mut Env, config: JObject, executor: *const Executor) -> Result<jlong> {
    let cfg = parse_config_from_java(env, &config)?;
    let id = request_id(env)?;
    let executor_handle = executor as jlong;
    executor_or_default(executor)?.spawn(async move {
        let result = FileSystemContext::connect(cfg).await;
        complete_future(id, move |env| {
            let ctx = result?;
            let handle = Box::into_raw(Box::new(JavaFsHandle::new(ctx))) as jlong;
            make_async_goosefs(env, handle, executor_handle)
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeGetStatus<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
) -> jlong {
    env.with_env(|env| intern_get_status(env, op, executor, path))
        .resolve::<ThrowException>()
}

fn intern_get_status(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = fs.get_status(&path).await;
        complete_future(id, move |env| make_uri_status(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeCloseAsync<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
) -> jlong {
    env.with_env(|env| intern_close_async(env, op, executor))
        .resolve::<ThrowException>()
}

fn intern_close_async(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let id = request_id(env)?;
    let ctx = handle.ctx.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = ctx.close().await;
        complete_future(id, move |_env| {
            result?;
            Ok(JObject::null())
        });
    });
    Ok(id)
}

/// # Safety
///
/// `op` must be the pointer from `nativeConnect` and not yet disposed.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    op: *mut JavaFsHandle,
) {
    if !op.is_null() {
        unsafe {
            drop(Box::from_raw(op));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeThrowIfOnTokio<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) {
    env.with_env(|_env| crate::executor::throw_if_in_runtime())
        .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeExists<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
) -> jlong {
    env.with_env(|env| intern_exists(env, op, executor, path))
        .resolve::<ThrowException>()
}

fn intern_exists(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = fs.exists(&path).await;
        complete_future(id, move |env| boxed_boolean(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeListStatus<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    recursive: jboolean,
) -> jlong {
    env.with_env(|env| intern_list_status(env, op, executor, path, recursive))
        .resolve::<ThrowException>()
}

fn intern_list_status(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    recursive: bool,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = fs.list_status(&path, recursive).await;
        complete_future(id, move |env| make_status_array_list(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeListStatusGrouped<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    recursive: jboolean,
) -> jlong {
    env.with_env(|env| intern_list_status_grouped(env, op, executor, path, recursive))
        .resolve::<ThrowException>()
}

fn intern_list_status_grouped(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    recursive: bool,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = fs.list_status(&path, recursive).await;
        complete_future(id, move |env| make_uri_status_list(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeMkdir<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    recursive: jboolean,
) -> jlong {
    env.with_env(|env| intern_mkdir(env, op, executor, path, recursive))
        .resolve::<ThrowException>()
}

fn intern_mkdir(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    recursive: bool,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = fs.mkdir(&path, recursive).await;
        complete_future(id, move |_env| {
            result?;
            Ok(JObject::null())
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeDelete<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    options: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_delete(env, op, executor, path, options))
        .resolve::<ThrowException>()
}

fn intern_delete(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let opts = parse_delete_options(env, &options)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = fs.delete(&path, opts).await;
        complete_future(id, move |_env| {
            result?;
            Ok(JObject::null())
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeRename<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    src: JString<'local>,
    dst: JString<'local>,
) -> jlong {
    env.with_env(|env| intern_rename(env, op, executor, src, dst))
        .resolve::<ThrowException>()
}

fn intern_rename(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    src: JString,
    dst: JString,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let src = jstring_to_string(env, &src)?;
    let dst = jstring_to_string(env, &dst)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = fs.rename(&src, &dst).await;
        complete_future(id, move |_env| {
            result?;
            Ok(JObject::null())
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeReadFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
) -> jlong {
    env.with_env(|env| intern_read_file(env, op, executor, path))
        .resolve::<ThrowException>()
}

fn intern_read_file(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    let ctx = handle.ctx.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = GoosefsFileReader::read_file_with_context(ctx, &path).await;
        complete_future(id, move |env| bytes_to_jbyte_array(env, result?.as_ref()));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeReadRange<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    offset: jlong,
    length: jlong,
) -> jlong {
    env.with_env(|env| intern_read_range(env, op, executor, path, offset, length))
        .resolve::<ThrowException>()
}

fn intern_read_range(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    offset: jlong,
    length: jlong,
) -> Result<jlong> {
    let (offset, length) = parse_read_range(offset, length)?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    let ctx = handle.ctx.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = GoosefsFileReader::read_range_with_context(ctx, &path, offset, length).await;
        complete_future(id, move |env| bytes_to_jbyte_array(env, result?.as_ref()));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeWriteFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    data: JByteArray<'local>,
    options: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_write_file(env, op, executor, path, data, options))
        .resolve::<ThrowException>()
}

fn intern_write_file(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    data: JByteArray,
    options: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let payload = jbyte_array_to_vec(env, &data)?;
    let proto_opts = parse_create_file_options(env, &options)?;
    let id = request_id(env)?;
    let ctx = handle.ctx.clone();
    executor_or_default(executor)?.spawn(async move {
        let result = GoosefsFileWriter::write_file_with_context_and_options(
            ctx,
            &path,
            &payload,
            Some(proto_opts),
        )
        .await;
        complete_future(id, move |env| boxed_long(env, u64_as_jlong(result?)?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeOpenFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    options: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_open_file(env, op, executor, path, options))
        .resolve::<ThrowException>()
}

fn intern_open_file(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let opts = parse_open_file_options(env, &options)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    let executor_handle = executor as jlong;
    executor_or_default(executor)?.spawn(async move {
        let result = fs.open_file(&path, opts).await;
        complete_future(id, move |env| {
            make_async_file_reader(env, result?, executor_handle)
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeCreateFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    options: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_create_file(env, op, executor, path, options))
        .resolve::<ThrowException>()
}

fn intern_create_file(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let opts = parse_sdk_create_file_options(env, &options)?;
    let id = request_id(env)?;
    let fs = handle.fs.clone();
    let executor_handle = executor as jlong;
    executor_or_default(executor)?.spawn(async move {
        let result = fs.create_file(&path, opts).await;
        complete_future(id, move |env| {
            make_async_file_writer(env, result?, executor_handle)
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchGetStatus<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_batch_get_status(env, op, executor, paths))
        .resolve::<ThrowException>()
}

fn intern_batch_get_status(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_get_status(handle, paths).await;
        complete_future(id, move |env| make_status_array_list(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchExists<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_batch_exists(env, op, executor, paths))
        .resolve::<ThrowException>()
}

fn intern_batch_exists(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_exists(handle, paths).await;
        complete_future(id, move |env| bools_to_jlist(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchOpenFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_batch_open_file(env, op, executor, paths))
        .resolve::<ThrowException>()
}

fn intern_batch_open_file(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let id = request_id(env)?;
    let executor_handle = executor as jlong;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_open_file(
            handle,
            paths,
            goosefs_sdk::fs::options::OpenFileOptions::default(),
        )
        .await;
        complete_future(id, move |env| {
            make_reader_list(env, result?, executor_handle, true)
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchCreateFile<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    options: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_batch_create_file(env, op, executor, paths, options))
        .resolve::<ThrowException>()
}

fn intern_batch_create_file(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    options: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let opts = parse_sdk_create_file_options(env, &options)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_create_file(handle, paths, opts).await;
        complete_future(id, move |env| i64s_to_jlist(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchCreateDir<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    recursive: jboolean,
) -> jlong {
    env.with_env(|env| intern_batch_create_dir(env, op, executor, paths, recursive))
        .resolve::<ThrowException>()
}

fn intern_batch_create_dir(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    recursive: jboolean,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_create_dir(handle, paths, recursive).await;
        complete_future(id, move |_env| {
            result?;
            Ok(JObject::null())
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchRename<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    pairs: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_batch_rename(env, op, executor, pairs))
        .resolve::<ThrowException>()
}

fn intern_batch_rename(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    pairs: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let pairs = crate::batch::even_pairs(jlist_to_strings(env, &pairs)?)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_rename(handle, pairs).await;
        complete_future(id, move |_env| {
            result?;
            Ok(JObject::null())
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchDelete<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    options: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_batch_delete(env, op, executor, paths, options))
        .resolve::<ThrowException>()
}

fn intern_batch_delete(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    options: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let opts = parse_delete_options(env, &options)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_delete(handle, paths, opts).await;
        complete_future(id, move |_env| {
            result?;
            Ok(JObject::null())
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchListStatus<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    recursive: jboolean,
) -> jlong {
    env.with_env(|env| intern_batch_list_status(env, op, executor, paths, recursive))
        .resolve::<ThrowException>()
}

fn intern_batch_list_status(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    recursive: jboolean,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_list_status(handle, paths, recursive).await;
        complete_future(id, move |env| make_nested_status_lists(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeBatchListStatusGrouped<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    recursive: jboolean,
) -> jlong {
    env.with_env(|env| intern_batch_list_status_grouped(env, op, executor, paths, recursive))
        .resolve::<ThrowException>()
}

fn intern_batch_list_status_grouped(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    recursive: jboolean,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::batch::batch_list_status(handle, paths, recursive).await;
        complete_future(id, move |env| make_grouped_lists(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativePositionedRead<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    block_index: jint,
    offset: jlong,
    length: jlong,
    chunk_size: jlong,
) -> jlong {
    env.with_env(|env| {
        intern_positioned_read(
            env,
            op,
            executor,
            path,
            block_index,
            offset,
            length,
            chunk_size,
        )
    })
    .resolve::<ThrowException>()
}

#[allow(clippy::too_many_arguments)]
fn intern_positioned_read(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    block_index: jint,
    offset: jlong,
    length: jlong,
    chunk_size: jlong,
) -> Result<jlong> {
    let idx =
        crate::positioned_read::validate_positioned_read(block_index, offset, length, chunk_size)?;
    let handle = handle_ptr(op)?.clone();
    let path = jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result =
            crate::positioned_read::positioned_read(handle, path, idx, offset, length, chunk_size)
                .await;
        complete_future(id, move |env| bytes_to_jbyte_array(env, &result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncGoosefs_nativeAcquireWorkerForBlock<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    block_id: jlong,
    path: JObject<'local>,
) -> jlong {
    env.with_env(|env| intern_acquire_worker(env, op, executor, block_id, path))
        .resolve::<ThrowException>()
}

fn intern_acquire_worker(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    block_id: jlong,
    path: JObject,
) -> Result<jlong> {
    let handle = handle_ptr(op)?.clone();
    let path = optional_jstring_to_string(env, &path)?;
    let id = request_id(env)?;
    let executor_handle = executor as jlong;
    executor_or_default(executor)?.spawn(async move {
        let result = crate::positioned_read::acquire_worker_for_block(handle, block_id, path).await;
        complete_future(id, move |env| {
            let (client, ufs_opts) = result?;
            make_async_worker_client(env, client, ufs_opts, executor_handle)
        });
    });
    Ok(id)
}
