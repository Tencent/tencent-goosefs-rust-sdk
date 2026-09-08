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

//! Blocking JNI methods for `Goosefs`.

use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::FileSystem;
use goosefs_sdk::io::GoosefsFileReader;
use goosefs_sdk::io::GoosefsFileWriter;
use jni::objects::JByteArray;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JString;
use jni::sys::jboolean;
use jni::sys::jint;
use jni::sys::jlong;
use jni::Env;
use jni::EnvUnowned;

use crate::config::parse_config_from_java;
use crate::convert::{
    bools_to_jlist, bytes_to_jbyte_array, i64s_to_jlist, jbyte_array_to_vec, jlist_to_strings,
    jstring_to_string, optional_jstring_to_string, u64_as_jlong,
};
use crate::error::{Error, ThrowException};
use crate::executor::{executor_or_default, throw_if_in_runtime, Executor};
use crate::handle::JavaFsHandle;
use crate::options::{
    parse_create_file_options, parse_delete_options, parse_open_file_options, parse_read_range,
    parse_sdk_create_file_options,
};
use crate::status::{
    make_grouped_lists, make_nested_status_lists, make_status_array_list, make_uri_status,
    make_uri_status_list,
};
use crate::streaming::{make_file_reader, make_file_writer, make_reader_list};
use crate::worker::make_worker_client;
use crate::Result;

fn handle_ptr(op: *mut JavaFsHandle) -> Result<&'static JavaFsHandle> {
    if op.is_null() {
        return Err(Error::IllegalState("client is closed".into()));
    }
    // SAFETY: Java `NativeObject` owns this pointer until `disposeInternal`.
    Ok(unsafe { &*op })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeConnect<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    config: JObject<'local>,
    executor: *const Executor,
) -> jlong {
    env.with_env(|env| intern_connect(env, config, executor))
        .resolve::<ThrowException>()
}

fn intern_connect(env: &mut Env, config: JObject, executor: *const Executor) -> Result<jlong> {
    throw_if_in_runtime()?;
    let cfg = parse_config_from_java(env, &config)?;
    let ctx = executor_or_default(executor)?.block_on(FileSystemContext::connect(cfg))?;
    Ok(Box::into_raw(Box::new(JavaFsHandle::new(ctx))) as jlong)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeGetStatus<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_get_status(env, op, executor, path))
        .resolve::<ThrowException>()
}

fn intern_get_status<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let fs = handle.fs.clone();
    let status = executor_or_default(executor)?.block_on(fs.get_status(&path))?;
    make_uri_status(env, status)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeExists<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
) -> jboolean {
    env.with_env(|env| intern_exists(env, op, executor, path))
        .resolve::<ThrowException>()
}

fn intern_exists(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
) -> Result<jboolean> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let fs = handle.fs.clone();
    let exists = executor_or_default(executor)?.block_on(fs.exists(&path))?;
    Ok(exists as jboolean)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeListStatus<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    recursive: jboolean,
) -> JObject<'local> {
    env.with_env(|env| intern_list_status(env, op, executor, path, recursive))
        .resolve::<ThrowException>()
}

fn intern_list_status<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    recursive: bool,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let fs = handle.fs.clone();
    let items = executor_or_default(executor)?.block_on(fs.list_status(&path, recursive))?;
    make_status_array_list(env, items)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeListStatusGrouped<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    recursive: jboolean,
) -> JObject<'local> {
    env.with_env(|env| intern_list_status_grouped(env, op, executor, path, recursive))
        .resolve::<ThrowException>()
}

fn intern_list_status_grouped<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    recursive: bool,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let fs = handle.fs.clone();
    let items = executor_or_default(executor)?.block_on(fs.list_status(&path, recursive))?;
    make_uri_status_list(env, items)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeMkdir<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    recursive: jboolean,
) {
    env.with_env(|env| intern_mkdir(env, op, executor, path, recursive))
        .resolve::<ThrowException>()
}

fn intern_mkdir(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    recursive: bool,
) -> Result<()> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.block_on(fs.mkdir(&path, recursive))?;
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeDelete<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    options: JObject<'local>,
) {
    env.with_env(|env| intern_delete(env, op, executor, path, options))
        .resolve::<ThrowException>()
}

fn intern_delete(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<()> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let opts = parse_delete_options(env, &options)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.block_on(fs.delete(&path, opts))?;
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeRename<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    src: JString<'local>,
    dst: JString<'local>,
) {
    env.with_env(|env| intern_rename(env, op, executor, src, dst))
        .resolve::<ThrowException>()
}

fn intern_rename(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    src: JString,
    dst: JString,
) -> Result<()> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let src = jstring_to_string(env, &src)?;
    let dst = jstring_to_string(env, &dst)?;
    let fs = handle.fs.clone();
    executor_or_default(executor)?.block_on(fs.rename(&src, &dst))?;
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeReadFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_read_file(env, op, executor, path))
        .resolve::<ThrowException>()
}

fn intern_read_file<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let ctx = handle.ctx.clone();
    let bytes = executor_or_default(executor)?
        .block_on(GoosefsFileReader::read_file_with_context(ctx, &path))?;
    bytes_to_jbyte_array(env, bytes.as_ref())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeReadRange<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    offset: jlong,
    length: jlong,
) -> JObject<'local> {
    env.with_env(|env| intern_read_range(env, op, executor, path, offset, length))
        .resolve::<ThrowException>()
}

fn intern_read_range<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    offset: jlong,
    length: jlong,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let (offset, length) = parse_read_range(offset, length)?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let ctx = handle.ctx.clone();
    let bytes = executor_or_default(executor)?.block_on(
        GoosefsFileReader::read_range_with_context(ctx, &path, offset, length),
    )?;
    bytes_to_jbyte_array(env, bytes.as_ref())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeWriteFile<'local>(
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
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let payload = jbyte_array_to_vec(env, &data)?;
    let proto_opts = parse_create_file_options(env, &options)?;
    let ctx = handle.ctx.clone();
    let n = executor_or_default(executor)?.block_on(
        GoosefsFileWriter::write_file_with_context_and_options(
            ctx,
            &path,
            &payload,
            Some(proto_opts),
        ),
    )?;
    u64_as_jlong(n)
}

/// # Safety
///
/// `op` must be the pointer from `nativeConnect` and not yet disposed.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeDispose<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
) {
    env.with_env(|_env| intern_dispose(op, executor))
        .resolve::<ThrowException>()
}

fn intern_dispose(op: *mut JavaFsHandle, executor: *const Executor) -> Result<()> {
    if op.is_null() {
        return Ok(());
    }
    throw_if_in_runtime()?;
    // SAFETY: exclusive ownership from Java `close()`.
    let handle = unsafe { Box::from_raw(op) };
    executor_or_default(executor)?.block_on(handle.ctx.close())?;
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeOpenFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    options: JObject<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_open_file(env, op, executor, path, options))
        .resolve::<ThrowException>()
}

fn intern_open_file<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let opts = parse_open_file_options(env, &options)?;
    let executor_handle = executor as jlong;
    let stream = executor_or_default(executor)?.block_on(handle.fs.open_file(&path, opts))?;
    make_file_reader(env, stream, executor_handle)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeCreateFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    options: JObject<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_create_file(env, op, executor, path, options))
        .resolve::<ThrowException>()
}

fn intern_create_file<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?;
    let path = jstring_to_string(env, &path)?;
    let opts = parse_sdk_create_file_options(env, &options)?;
    let executor_handle = executor as jlong;
    let writer = executor_or_default(executor)?.block_on(handle.fs.create_file(&path, opts))?;
    make_file_writer(env, writer, executor_handle)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchGetStatus<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_batch_get_status(env, op, executor, paths))
        .resolve::<ThrowException>()
}

fn intern_batch_get_status<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let items =
        executor_or_default(executor)?.block_on(crate::batch::batch_get_status(handle, paths))?;
    make_status_array_list(env, items)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchExists<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_batch_exists(env, op, executor, paths))
        .resolve::<ThrowException>()
}

fn intern_batch_exists<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let items =
        executor_or_default(executor)?.block_on(crate::batch::batch_exists(handle, paths))?;
    bools_to_jlist(env, items)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchOpenFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_batch_open_file(env, op, executor, paths))
        .resolve::<ThrowException>()
}

fn intern_batch_open_file<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let executor_handle = executor as jlong;
    let streams = executor_or_default(executor)?.block_on(crate::batch::batch_open_file(
        handle,
        paths,
        goosefs_sdk::fs::options::OpenFileOptions::default(),
    ))?;
    make_reader_list(env, streams, executor_handle, false)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchCreateFile<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    options: JObject<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_batch_create_file(env, op, executor, paths, options))
        .resolve::<ThrowException>()
}

fn intern_batch_create_file<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    options: JObject,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let opts = parse_sdk_create_file_options(env, &options)?;
    let items = executor_or_default(executor)?
        .block_on(crate::batch::batch_create_file(handle, paths, opts))?;
    i64s_to_jlist(env, items)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchCreateDir<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    recursive: jboolean,
) {
    env.with_env(|env| intern_batch_create_dir(env, op, executor, paths, recursive))
        .resolve::<ThrowException>()
}

fn intern_batch_create_dir(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    recursive: jboolean,
) -> Result<()> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    executor_or_default(executor)?
        .block_on(crate::batch::batch_create_dir(handle, paths, recursive))?;
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchRename<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    pairs: JObject<'local>,
) {
    env.with_env(|env| intern_batch_rename(env, op, executor, pairs))
        .resolve::<ThrowException>()
}

fn intern_batch_rename(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    pairs: JObject,
) -> Result<()> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let pairs = crate::batch::even_pairs(jlist_to_strings(env, &pairs)?)?;
    executor_or_default(executor)?.block_on(crate::batch::batch_rename(handle, pairs))?;
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchDelete<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    options: JObject<'local>,
) {
    env.with_env(|env| intern_batch_delete(env, op, executor, paths, options))
        .resolve::<ThrowException>()
}

fn intern_batch_delete(
    env: &mut Env,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    options: JObject,
) -> Result<()> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let opts = parse_delete_options(env, &options)?;
    executor_or_default(executor)?.block_on(crate::batch::batch_delete(handle, paths, opts))?;
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchListStatus<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    recursive: jboolean,
) -> JObject<'local> {
    env.with_env(|env| intern_batch_list_status(env, op, executor, paths, recursive))
        .resolve::<ThrowException>()
}

fn intern_batch_list_status<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    recursive: jboolean,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let items = executor_or_default(executor)?
        .block_on(crate::batch::batch_list_status(handle, paths, recursive))?;
    make_nested_status_lists(env, items)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeBatchListStatusGrouped<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject<'local>,
    recursive: jboolean,
) -> JObject<'local> {
    env.with_env(|env| intern_batch_list_status_grouped(env, op, executor, paths, recursive))
        .resolve::<ThrowException>()
}

fn intern_batch_list_status_grouped<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    paths: JObject,
    recursive: jboolean,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let paths = jlist_to_strings(env, &paths)?;
    let items = executor_or_default(executor)?
        .block_on(crate::batch::batch_list_status(handle, paths, recursive))?;
    make_grouped_lists(env, items)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativePositionedRead<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString<'local>,
    block_index: jint,
    offset: jlong,
    length: jlong,
    chunk_size: jlong,
) -> JObject<'local> {
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
fn intern_positioned_read<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    path: JString,
    block_index: jint,
    offset: jlong,
    length: jlong,
    chunk_size: jlong,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let idx =
        crate::positioned_read::validate_positioned_read(block_index, offset, length, chunk_size)?;
    let handle = handle_ptr(op)?.clone();
    let path = jstring_to_string(env, &path)?;
    let data = executor_or_default(executor)?.block_on(crate::positioned_read::positioned_read(
        handle, path, idx, offset, length, chunk_size,
    ))?;
    bytes_to_jbyte_array(env, &data)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_Goosefs_nativeAcquireWorkerForBlock<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    block_id: jlong,
    path: JObject<'local>,
) -> JObject<'local> {
    env.with_env(|env| intern_acquire_worker(env, op, executor, block_id, path))
        .resolve::<ThrowException>()
}

fn intern_acquire_worker<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFsHandle,
    executor: *const Executor,
    block_id: jlong,
    path: JObject,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let handle = handle_ptr(op)?.clone();
    let path = optional_jstring_to_string(env, &path)?;
    let executor_handle = executor as jlong;
    let (client, ufs_opts) = executor_or_default(executor)?.block_on(
        crate::positioned_read::acquire_worker_for_block(handle, block_id, path),
    )?;
    make_worker_client(env, client, ufs_opts, executor_handle)
}
