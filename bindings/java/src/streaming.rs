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

//! Streaming `FileReader` / `FileWriter` and async twins.

use std::sync::Arc;

use goosefs_sdk::io::{GoosefsFileInStream, GoosefsFileWriter};
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JByteArray;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::sys::{jint, jlong};
use jni::Env;
use jni::EnvUnowned;
use tokio::sync::Mutex;

use crate::async_fs::{complete_future, request_id};
use crate::convert::{bytes_to_jbyte_array, jbyte_array_to_vec, u64_as_jlong};
use crate::error::{Error, ThrowException};
use crate::executor::{executor_or_default, throw_if_in_runtime, Executor};
use crate::options::resolve_seek;
use crate::Result;

pub(crate) struct JavaFileReader {
    inner: Arc<Mutex<Option<GoosefsFileInStream>>>,
    file_length: i64,
}

pub(crate) enum WriterSlot {
    Open(Box<GoosefsFileWriter>),
    Done,
}

pub(crate) struct JavaFileWriter {
    inner: Arc<Mutex<WriterSlot>>,
}

impl JavaFileReader {
    pub(crate) fn new(stream: GoosefsFileInStream) -> Self {
        let file_length = stream.len();
        Self {
            inner: Arc::new(Mutex::new(Some(stream))),
            file_length,
        }
    }
}

impl JavaFileWriter {
    pub(crate) fn new(writer: GoosefsFileWriter) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WriterSlot::Open(Box::new(writer)))),
        }
    }
}

pub(crate) fn make_file_reader<'local>(
    env: &mut Env<'local>,
    stream: GoosefsFileInStream,
    executor: jlong,
) -> Result<JObject<'local>> {
    let handle = Box::into_raw(Box::new(JavaFileReader::new(stream))) as jlong;
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/FileReader"),
        jni_sig!("(JJ)V"),
        &[JValue::Long(handle), JValue::Long(executor)],
    )?)
}

pub(crate) fn make_async_file_reader<'local>(
    env: &mut Env<'local>,
    stream: GoosefsFileInStream,
    executor: jlong,
) -> Result<JObject<'local>> {
    let handle = Box::into_raw(Box::new(JavaFileReader::new(stream))) as jlong;
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/AsyncFileReader"),
        jni_sig!("(JJ)V"),
        &[JValue::Long(handle), JValue::Long(executor)],
    )?)
}

pub(crate) fn make_file_writer<'local>(
    env: &mut Env<'local>,
    writer: GoosefsFileWriter,
    executor: jlong,
) -> Result<JObject<'local>> {
    let handle = Box::into_raw(Box::new(JavaFileWriter::new(writer))) as jlong;
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/FileWriter"),
        jni_sig!("(JJ)V"),
        &[JValue::Long(handle), JValue::Long(executor)],
    )?)
}

pub(crate) fn make_async_file_writer<'local>(
    env: &mut Env<'local>,
    writer: GoosefsFileWriter,
    executor: jlong,
) -> Result<JObject<'local>> {
    let handle = Box::into_raw(Box::new(JavaFileWriter::new(writer))) as jlong;
    Ok(env.new_object(
        jni_str!("com/tencent/goosefs/AsyncFileWriter"),
        jni_sig!("(JJ)V"),
        &[JValue::Long(handle), JValue::Long(executor)],
    )?)
}

pub(crate) fn make_reader_list<'local>(
    env: &mut Env<'local>,
    streams: Vec<GoosefsFileInStream>,
    executor: jlong,
    async_readers: bool,
) -> Result<JObject<'local>> {
    let list = crate::convert::new_array_list(env, streams.len())?;
    for stream in streams {
        let obj = if async_readers {
            make_async_file_reader(env, stream, executor)?
        } else {
            make_file_reader(env, stream, executor)?
        };
        crate::convert::array_list_add(env, &list, &obj)?;
        env.delete_local_ref(obj);
    }
    Ok(list)
}

fn reader_ptr(op: *mut JavaFileReader) -> Result<&'static JavaFileReader> {
    if op.is_null() {
        return Err(Error::IllegalState("FileReader is closed".into()));
    }
    Ok(unsafe { &*op })
}

fn writer_ptr(op: *mut JavaFileWriter) -> Result<&'static JavaFileWriter> {
    if op.is_null() {
        return Err(Error::IllegalState("FileWriter is closed".into()));
    }
    Ok(unsafe { &*op })
}

fn busy(kind: &str) -> Error {
    Error::IllegalState(format!(
        "{kind} is in use by another operation; handles are not shareable across threads"
    ))
}

async fn pull_n(stream: &mut GoosefsFileInStream, want: usize) -> Result<Vec<u8>> {
    if want == 0 {
        return Ok(Vec::new());
    }
    let mut out = vec![0u8; want];
    let mut filled = 0;
    while filled < want {
        let n = stream.read(&mut out[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    out.truncate(filled);
    Ok(out)
}

async fn do_read(stream: &mut GoosefsFileInStream, size: i32) -> Result<Vec<u8>> {
    if size == 0 {
        return Ok(Vec::new());
    }
    if size < 0 {
        let bytes = stream.read_all().await?;
        if bytes.len() > i32::MAX as usize {
            return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
                message: format!(
                    "payload length {} exceeds Integer.MAX_VALUE; use streaming I/O",
                    bytes.len()
                ),
            }));
        }
        return Ok(bytes.to_vec());
    }
    pull_n(stream, size as usize).await
}

fn drop_reader(op: *mut JavaFileReader) {
    if !op.is_null() {
        unsafe {
            drop(Box::from_raw(op));
        }
    }
}

fn drop_writer(op: *mut JavaFileWriter) {
    if !op.is_null() {
        unsafe {
            drop(Box::from_raw(op));
        }
    }
}

// ── FileReader (sync) ────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileReader_nativeRead<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
    size: jint,
) -> JObject<'local> {
    env.with_env(|env| intern_read_sync(env, op, executor, size))
        .resolve::<ThrowException>()
}

fn intern_read_sync<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
    size: jint,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    let reader = reader_ptr(op)?;
    let inner = reader.inner.clone();
    let data = executor_or_default(executor)?.block_on(async move {
        let mut guard = inner.try_lock().map_err(|_| busy("FileReader"))?;
        let stream = guard
            .as_mut()
            .ok_or_else(|| Error::IllegalState("FileReader is closed".into()))?;
        do_read(stream, size).await
    })?;
    bytes_to_jbyte_array(env, &data)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileReader_nativeReadAt<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
    offset: jlong,
    length: jint,
) -> JObject<'local> {
    env.with_env(|env| intern_read_at_sync(env, op, executor, offset, length))
        .resolve::<ThrowException>()
}

fn intern_read_at_sync<'local>(
    env: &mut Env<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
    offset: jlong,
    length: jint,
) -> Result<JObject<'local>> {
    throw_if_in_runtime()?;
    if offset < 0 {
        return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: "offset must be non-negative".into(),
        }));
    }
    if length < 0 {
        return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: "readAt length must be non-negative".into(),
        }));
    }
    let reader = reader_ptr(op)?;
    let inner = reader.inner.clone();
    let data = executor_or_default(executor)?.block_on(async move {
        let mut guard = inner.try_lock().map_err(|_| busy("FileReader"))?;
        let stream = guard
            .as_mut()
            .ok_or_else(|| Error::IllegalState("FileReader is closed".into()))?;
        stream
            .read_at(offset, length as usize)
            .await
            .map_err(Error::from)
    })?;
    bytes_to_jbyte_array(env, data.as_ref())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileReader_nativeSeek<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
    offset: jlong,
    whence: jint,
) -> jlong {
    env.with_env(|env| intern_seek_sync(env, op, executor, offset, whence))
        .resolve::<ThrowException>()
}

fn intern_seek_sync(
    _env: &mut Env,
    op: *mut JavaFileReader,
    executor: *const Executor,
    offset: jlong,
    whence: jint,
) -> Result<jlong> {
    throw_if_in_runtime()?;
    let reader = reader_ptr(op)?;
    let inner = reader.inner.clone();
    let file_length = reader.file_length;
    executor_or_default(executor)?.block_on(async move {
        let mut guard = inner.try_lock().map_err(|_| busy("FileReader"))?;
        let stream = guard
            .as_mut()
            .ok_or_else(|| Error::IllegalState("FileReader is closed".into()))?;
        let target = resolve_seek(whence, offset, stream.pos(), file_length)?;
        stream.seek(target).await.map_err(Error::from)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileReader_nativeTell<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
) -> jlong {
    env.with_env(|_env| intern_tell(op))
        .resolve::<ThrowException>()
}

fn intern_tell(op: *mut JavaFileReader) -> Result<jlong> {
    let reader = reader_ptr(op)?;
    let guard = reader.inner.try_lock().map_err(|_| busy("FileReader"))?;
    let stream = guard
        .as_ref()
        .ok_or_else(|| Error::IllegalState("FileReader is closed".into()))?;
    Ok(stream.pos())
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileReader_nativeLength<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
) -> jlong {
    env.with_env(|_env| intern_length(op))
        .resolve::<ThrowException>()
}

fn intern_length(op: *mut JavaFileReader) -> Result<jlong> {
    Ok(reader_ptr(op)?.file_length)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileReader_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    op: *mut JavaFileReader,
) {
    drop_reader(op);
}

// ── AsyncFileReader ──────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileReader_nativeRead<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
    size: jint,
) -> jlong {
    env.with_env(|env| intern_read_async(env, op, executor, size))
        .resolve::<ThrowException>()
}

fn intern_read_async(
    env: &mut Env,
    op: *mut JavaFileReader,
    executor: *const Executor,
    size: jint,
) -> Result<jlong> {
    let reader = reader_ptr(op)?;
    let inner = reader.inner.clone();
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = async {
            let mut guard = inner.try_lock().map_err(|_| busy("FileReader"))?;
            let stream = guard
                .as_mut()
                .ok_or_else(|| Error::IllegalState("FileReader is closed".into()))?;
            do_read(stream, size).await
        }
        .await;
        complete_future(id, move |env| bytes_to_jbyte_array(env, &result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileReader_nativeReadAt<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
    offset: jlong,
    length: jint,
) -> jlong {
    env.with_env(|env| intern_read_at_async(env, op, executor, offset, length))
        .resolve::<ThrowException>()
}

fn intern_read_at_async(
    env: &mut Env,
    op: *mut JavaFileReader,
    executor: *const Executor,
    offset: jlong,
    length: jint,
) -> Result<jlong> {
    if offset < 0 {
        return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: "offset must be non-negative".into(),
        }));
    }
    if length < 0 {
        return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: "readAt length must be non-negative".into(),
        }));
    }
    let reader = reader_ptr(op)?;
    let inner = reader.inner.clone();
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = async {
            let mut guard = inner.try_lock().map_err(|_| busy("FileReader"))?;
            let stream = guard
                .as_mut()
                .ok_or_else(|| Error::IllegalState("FileReader is closed".into()))?;
            stream
                .read_at(offset, length as usize)
                .await
                .map_err(Error::from)
        }
        .await;
        complete_future(id, move |env| bytes_to_jbyte_array(env, result?.as_ref()));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileReader_nativeSeek<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
    offset: jlong,
    whence: jint,
) -> jlong {
    env.with_env(|env| intern_seek_async(env, op, executor, offset, whence))
        .resolve::<ThrowException>()
}

fn intern_seek_async(
    env: &mut Env,
    op: *mut JavaFileReader,
    executor: *const Executor,
    offset: jlong,
    whence: jint,
) -> Result<jlong> {
    let reader = reader_ptr(op)?;
    let inner = reader.inner.clone();
    let file_length = reader.file_length;
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = async {
            let mut guard = inner.try_lock().map_err(|_| busy("FileReader"))?;
            let stream = guard
                .as_mut()
                .ok_or_else(|| Error::IllegalState("FileReader is closed".into()))?;
            let target = resolve_seek(whence, offset, stream.pos(), file_length)?;
            stream.seek(target).await.map_err(Error::from)
        }
        .await;
        complete_future(id, move |env| crate::convert::boxed_long(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileReader_nativeTell<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
) -> jlong {
    env.with_env(|_env| intern_tell(op))
        .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileReader_nativeLength<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
) -> jlong {
    env.with_env(|_env| intern_length(op))
        .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileReader_nativeCloseAsync<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileReader,
    executor: *const Executor,
) -> jlong {
    env.with_env(|env| intern_reader_close_async(env, op, executor))
        .resolve::<ThrowException>()
}

fn intern_reader_close_async(
    env: &mut Env,
    op: *mut JavaFileReader,
    executor: *const Executor,
) -> Result<jlong> {
    let reader = reader_ptr(op)?;
    let inner = reader.inner.clone();
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = async {
            let mut guard = inner.try_lock().map_err(|_| busy("FileReader"))?;
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
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileReader_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    op: *mut JavaFileReader,
) {
    drop_reader(op);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_AsyncFileReader_nativeThrowIfOnTokio<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) {
    env.with_env(|_env| throw_if_in_runtime())
        .resolve::<ThrowException>()
}

// ── FileWriter (sync) ────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileWriter_nativeWrite<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileWriter,
    executor: *const Executor,
    data: JByteArray<'local>,
) -> jlong {
    env.with_env(|env| intern_write_sync(env, op, executor, data))
        .resolve::<ThrowException>()
}

fn intern_write_sync(
    env: &mut Env,
    op: *mut JavaFileWriter,
    executor: *const Executor,
    data: JByteArray,
) -> Result<jlong> {
    throw_if_in_runtime()?;
    let payload = jbyte_array_to_vec(env, &data)?;
    let n = payload.len();
    let writer = writer_ptr(op)?;
    let inner = writer.inner.clone();
    executor_or_default(executor)?.block_on(async move {
        let mut guard = inner.try_lock().map_err(|_| busy("FileWriter"))?;
        match &mut *guard {
            WriterSlot::Open(w) => w.write(&payload).await.map_err(Error::from),
            WriterSlot::Done => Err(Error::IllegalState("FileWriter is closed".into())),
        }
    })?;
    u64_as_jlong(n as u64)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileWriter_nativeCommit<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileWriter,
    executor: *const Executor,
) {
    env.with_env(|env| intern_commit_sync(env, op, executor, false))
        .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileWriter_nativeCancel<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileWriter,
    executor: *const Executor,
) {
    env.with_env(|env| intern_commit_sync(env, op, executor, true))
        .resolve::<ThrowException>()
}

fn intern_commit_sync(
    _env: &mut Env,
    op: *mut JavaFileWriter,
    executor: *const Executor,
    cancel: bool,
) -> Result<()> {
    throw_if_in_runtime()?;
    let writer = writer_ptr(op)?;
    let inner = writer.inner.clone();
    executor_or_default(executor)?.block_on(async move {
        let mut guard = inner.try_lock().map_err(|_| busy("FileWriter"))?;
        match std::mem::replace(&mut *guard, WriterSlot::Done) {
            WriterSlot::Open(mut w) => {
                if cancel {
                    w.cancel().await.map_err(Error::from)
                } else {
                    w.close().await.map_err(Error::from)
                }
            }
            WriterSlot::Done => Ok(()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_FileWriter_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    op: *mut JavaFileWriter,
) {
    drop_writer(op);
}

// ── AsyncFileWriter ──────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileWriter_nativeWrite<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileWriter,
    executor: *const Executor,
    data: JByteArray<'local>,
) -> jlong {
    env.with_env(|env| intern_write_async(env, op, executor, data))
        .resolve::<ThrowException>()
}

fn intern_write_async(
    env: &mut Env,
    op: *mut JavaFileWriter,
    executor: *const Executor,
    data: JByteArray,
) -> Result<jlong> {
    let payload = jbyte_array_to_vec(env, &data)?;
    let n = payload.len() as i64;
    let writer = writer_ptr(op)?;
    let inner = writer.inner.clone();
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = async {
            let mut guard = inner.try_lock().map_err(|_| busy("FileWriter"))?;
            match &mut *guard {
                WriterSlot::Open(w) => {
                    w.write(&payload).await?;
                    Ok(n)
                }
                WriterSlot::Done => Err(Error::IllegalState("FileWriter is closed".into())),
            }
        }
        .await;
        complete_future(id, move |env| crate::convert::boxed_long(env, result?));
    });
    Ok(id)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileWriter_nativeCommit<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileWriter,
    executor: *const Executor,
) -> jlong {
    env.with_env(|env| intern_finalize_async(env, op, executor, false))
        .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileWriter_nativeCancel<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut JavaFileWriter,
    executor: *const Executor,
) -> jlong {
    env.with_env(|env| intern_finalize_async(env, op, executor, true))
        .resolve::<ThrowException>()
}

fn intern_finalize_async(
    env: &mut Env,
    op: *mut JavaFileWriter,
    executor: *const Executor,
    cancel: bool,
) -> Result<jlong> {
    let writer = writer_ptr(op)?;
    let inner = writer.inner.clone();
    let id = request_id(env)?;
    executor_or_default(executor)?.spawn(async move {
        let result = async {
            let mut guard = inner.try_lock().map_err(|_| busy("FileWriter"))?;
            match std::mem::replace(&mut *guard, WriterSlot::Done) {
                WriterSlot::Open(mut w) => {
                    if cancel {
                        w.cancel().await.map_err(Error::from)
                    } else {
                        w.close().await.map_err(Error::from)
                    }
                }
                WriterSlot::Done => Ok(()),
            }
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
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncFileWriter_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    op: *mut JavaFileWriter,
) {
    drop_writer(op);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_AsyncFileWriter_nativeThrowIfOnTokio<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) {
    env.with_env(|_env| throw_if_in_runtime())
        .resolve::<ThrowException>()
}
