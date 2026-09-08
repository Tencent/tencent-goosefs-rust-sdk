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

//! JNI for `WorkerClient` / `AsyncWorkerClient`. No `connect_simple`.

use std::sync::Arc;

use goosefs_sdk::client::WorkerClient;
use goosefs_sdk::io::GrpcBlockReader;
use goosefs_sdk::proto::proto::dataserver::OpenUfsBlockOptions;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::JValue;
use jni::sys::jlong;
use jni::Env;
use jni::EnvUnowned;
use tokio::sync::Mutex;

use crate::async_fs::{complete_future, request_id};
use crate::config::parse_config_from_java;
use crate::convert::{bytes_to_jbyte_array, jstring_to_string};
use crate::error::{Error, ThrowException};
use crate::executor::{executor_or_default, throw_if_in_runtime, Executor};
use crate::positioned_read::validate_block_read;
use crate::Result;

pub(crate) struct JavaWorkerClient {
    inner: Arc<Mutex<Option<WorkerClient>>>,
    addr: String,
    ufs_opts_for_block: Option<(i64, OpenUfsBlockOptions)>,
}

impl JavaWorkerClient {
    pub(crate) fn new(
        client: WorkerClient,
        ufs_opts_for_block: Option<(i64, OpenUfsBlockOptions)>,
    ) -> Self {
        let addr = client.addr().to_string();
        Self {
            inner: Arc::new(Mutex::new(Some(client))),
            addr,
            ufs_opts_for_block,
        }
    }
}

pub(crate) fn make_worker_client<'local>(
    env: &mut Env<'local>,
    client: WorkerClient,
    ufs_opts: Option<(i64, OpenUfsBlockOptions)>,
    executor: jlong,
) -> Result<JObject<'local>> {
    let handle = Box::into_raw(Box::new(JavaWorkerClient::new(client, ufs_opts))) as jlong;
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/WorkerClient"),
        jni_sig!("(JJ)V"),
        &[JValue::Long(handle), JValue::Long(executor)],
    )?)
}

pub(crate) fn make_async_worker_client<'local>(
    env: &mut Env<'local>,
    client: WorkerClient,
    ufs_opts: Option<(i64, OpenUfsBlockOptions)>,
    executor: jlong,
) -> Result<JObject<'local>> {
    let handle = Box::into_raw(Box::new(JavaWorkerClient::new(client, ufs_opts))) as jlong;
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/AsyncWorkerClient"),
        jni_sig!("(JJ)V"),
        &[JValue::Long(handle), JValue::Long(executor)],
    )?)
}

fn worker_ptr(op: *mut JavaWorkerClient) -> Result<&'static JavaWorkerClient> {
    if op.is_null() {
        return Err(Error::IllegalState("WorkerClient is closed".into()));
    }
    Ok(unsafe { &*op })
}

fn drop_worker(op: *mut JavaWorkerClient) {
    if !op.is_null() {
        unsafe {
            drop(Box::from_raw(op));
        }
    }
}

async fn clone_worker(inner: &Arc<Mutex<Option<WorkerClient>>>) -> Result<WorkerClient> {
    let guard = inner.lock().await;
    guard
        .as_ref()
        .ok_or_else(|| Error::IllegalState("WorkerClient is closed".into()))
        .cloned()
}

async fn do_read_block(
    worker: &JavaWorkerClient,
    block_id: i64,
    offset: i64,
    length: i64,
    chunk_size: i64,
) -> Result<Vec<u8>> {
    if length == 0 {
        return Ok(Vec::new());
    }
    let client = clone_worker(&worker.inner).await?;
    let ufs_opts = worker
        .ufs_opts_for_block
        .as_ref()
        .filter(|(id, _)| *id == block_id)
        .map(|(_, opts)| opts.clone());
    let bytes =
        GrpcBlockReader::positioned_read(&client, block_id, offset, length, chunk_size, ufs_opts)
            .await?;
    Ok(bytes.to_vec())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_WorkerClient_nativeConnect<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    addr: JString<'local>,
    config: JObject<'local>,
    executor: *const Executor,
) -> JObject<'local> {
    env.with_env(|env| intern_connect_sync(env, addr, config, executor))
        .resolve::<ThrowException>()
}

fn intern_connect_sync<'local>(
    env: &mut Env<'local>,
    addr: JString,
    config: JObject,
    executor: *const Executor,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let addr = jstring_to_string(env, &addr)?;
    if addr.is_empty() {
        return Err(crate::positioned_read::invalid_arg(
            "WorkerClient.connect: addr must not be empty",
        ));
    }
    let cfg = parse_config_from_java(env, &config)?;
    let executor_handle = executor as jlong;
    let client = executor_or_default(executor)?.block_on(WorkerClient::connect(&addr, &cfg))?;
    make_worker_client(env, client, None, executor_handle)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_WorkerClient_nativeReadBlockPositioned<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaWorkerClient,
    executor: *const Executor,
    block_id: jlong,
    offset: jlong,
    length: jlong,
    chunk_size: jlong,
) -> JObject<'local> {
    env.with_env(|env| intern_read_sync(env, op, executor, block_id, offset, length, chunk_size))
        .resolve::<ThrowException>()
}

fn intern_read_sync<'local>(
    env: &mut Env<'local>,
    op: *mut JavaWorkerClient,
    executor: *const Executor,
    block_id: jlong,
    offset: jlong,
    length: jlong,
    chunk_size: jlong,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    validate_block_read(offset, length, chunk_size)?;
    let worker = worker_ptr(op)?;
    let data = executor_or_default(executor)?
        .block_on(do_read_block(worker, block_id, offset, length, chunk_size))?;
    bytes_to_jbyte_array(env, &data)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_WorkerClient_nativeGetAddr<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaWorkerClient,
) -> JObject<'local> {
    env.with_env(|env| intern_get_addr(env, op))
        .resolve::<ThrowException>()
}

fn intern_get_addr<'local>(
    env: &mut Env<'local>,
    op: *mut JavaWorkerClient,
) -> Result<JObject<'local>> {
    let worker = worker_ptr(op)?;
    Ok(env.new_string(&worker.addr)?.into())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_WorkerClient_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    op: *mut JavaWorkerClient,
) {
    drop_worker(op);
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncWorkerClient_nativeConnect<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    addr: JString<'local>,
    config: JObject<'local>,
    executor: *const Executor,
) -> jlong {
    env.with_env(|env| intern_connect_async(env, addr, config, executor))
        .resolve::<ThrowException>()
}

fn intern_connect_async(
    env: &mut Env,
    addr: JString,
    config: JObject,
    executor: *const Executor,
) -> Result<jlong> {
    let addr = jstring_to_string(env, &addr)?;
    if addr.is_empty() {
        return Err(crate::positioned_read::invalid_arg(
            "AsyncWorkerClient.connect: addr must not be empty",
        ));
    }
    let cfg = parse_config_from_java(env, &config)?;
    let id = request_id(env)?;
    let executor_handle = executor as jlong;
    executor_or_default(executor)?.spawn(async move {
        let result = WorkerClient::connect(&addr, &cfg).await;
        complete_future(id, move |env| {
            make_async_worker_client(env, result?, None, executor_handle)
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncWorkerClient_nativeReadBlockPositioned<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaWorkerClient,
    executor: *const Executor,
    block_id: jlong,
    offset: jlong,
    length: jlong,
    chunk_size: jlong,
) -> jlong {
    env.with_env(|env| intern_read_async(env, op, executor, block_id, offset, length, chunk_size))
        .resolve::<ThrowException>()
}

fn intern_read_async(
    env: &mut Env,
    op: *mut JavaWorkerClient,
    executor: *const Executor,
    block_id: jlong,
    offset: jlong,
    length: jlong,
    chunk_size: jlong,
) -> Result<jlong> {
    validate_block_read(offset, length, chunk_size)?;
    let worker = worker_ptr(op)?;
    let inner = worker.inner.clone();
    let ufs_opts = worker.ufs_opts_for_block.clone();
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = async {
            if length == 0 {
                return Ok::<_, Error>(Vec::new());
            }
            let client = clone_worker(&inner).await?;
            let opts = ufs_opts
                .as_ref()
                .filter(|(bid, _)| *bid == block_id)
                .map(|(_, o)| o.clone());
            let bytes = GrpcBlockReader::positioned_read(
                &client, block_id, offset, length, chunk_size, opts,
            )
            .await?;
            Ok(bytes.to_vec())
        }
        .await;
        complete_future(id, move |env| bytes_to_jbyte_array(env, &result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncWorkerClient_nativeGetAddr<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaWorkerClient,
) -> JObject<'local> {
    env.with_env(|env| intern_get_addr(env, op))
        .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncWorkerClient_nativeCloseAsync<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaWorkerClient,
    executor: *const Executor,
) -> jlong {
    env.with_env(|env| intern_close_async(env, op, executor))
        .resolve::<ThrowException>()
}

fn intern_close_async(
    env: &mut Env,
    op: *mut JavaWorkerClient,
    executor: *const Executor,
) -> Result<jlong> {
    let worker = worker_ptr(op)?;
    let inner = worker.inner.clone();
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = async {
            let mut guard = inner.lock().await;
            let _ = guard.take();
            Ok::<_, Error>(())
        }
        .await;
        complete_future(id, move |_env| {
            result?;
            Ok(JObject::null())
        });
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncWorkerClient_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    op: *mut JavaWorkerClient,
) {
    drop_worker(op);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_AsyncWorkerClient_nativeThrowIfOnTokio<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) {
    env.with_env(|_env| throw_if_in_runtime())
        .resolve::<ThrowException>()
}
