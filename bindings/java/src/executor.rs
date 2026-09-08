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

//! Tokio executor for the Java binding.
//!
//! Runtime sizing matches the Python binding (`max(16, cpus)`, 64 blocking
//! threads, `GOOSEFS_TOKIO_*` overrides) — not OpenDAL's `available_parallelism()`
//! alone. Worker threads permanently attach to the JVM; they detach on stop
//! to avoid the Windows deadlock in OpenDAL #6869 / jni-rs #701.

use std::ffi::c_void;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use jni::jni_sig;
use jni::jni_str;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::sys::{jint, jlong};
use jni::Env;
use jni::EnvUnowned;
use jni::JavaVM;
use tokio::task::JoinHandle;

use crate::error::ThrowException;
use crate::Result;

static mut RUNTIME: OnceLock<Executor> = OnceLock::new();

const RUNTIME_MAX_BLOCKING_THREADS: usize = 64;

#[unsafe(no_mangle)]
pub unsafe extern "system" fn JNI_OnLoad(vm: *mut jni::sys::JavaVM, _: *mut c_void) -> jint {
    // Register the JavaVM singleton so worker threads can attach later.
    let _ = unsafe { JavaVM::from_raw(vm) };
    jni::sys::JNI_VERSION_1_8
}

/// # Safety
///
/// Called by the JVM when unloading this library. Live clients after unload
/// are use-after-free; do not reload / GC the classloader while clients exist.
#[allow(static_mut_refs)]
#[unsafe(no_mangle)]
pub unsafe extern "system" fn JNI_OnUnload(_: *mut jni::sys::JavaVM, _: *mut c_void) {
    unsafe {
        RUNTIME.take();
    }
}

pub enum Executor {
    Tokio(tokio::runtime::Runtime),
}

impl Executor {
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self {
            Executor::Tokio(e) => e.spawn(future),
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        match self {
            Executor::Tokio(e) => e.block_on(future),
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_AsyncExecutor_makeTokioExecutor<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    cores: usize,
) -> jlong {
    env.with_env(|_env| -> Result<jlong> {
        let executor = make_tokio_executor(Some(cores.max(1)))?;
        Ok(Box::into_raw(Box::new(executor)) as jlong)
    })
    .resolve::<ThrowException>()
}

/// # Safety
///
/// `executor` must be a pointer previously returned by `makeTokioExecutor`
/// and not yet disposed.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_tencent_goosefs_AsyncExecutor_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    executor: *mut Executor,
) {
    if !executor.is_null() {
        unsafe {
            drop(Box::from_raw(executor));
        }
    }
}

pub(crate) fn make_tokio_executor(cores_override: Option<usize>) -> Result<Executor> {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(16);
    let default_worker_threads = cpus.max(16);
    let worker_threads = cores_override
        .or_else(|| env_usize("GOOSEFS_TOKIO_WORKER_THREADS"))
        .map(|n| n.max(1))
        .unwrap_or(default_worker_threads);
    let max_blocking_threads = env_usize("GOOSEFS_TOKIO_MAX_BLOCKING_THREADS")
        .map(|n| n.max(1))
        .unwrap_or(RUNTIME_MAX_BLOCKING_THREADS);

    let counter = AtomicUsize::new(0);
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .thread_name_fn(move || {
            let id = counter.fetch_add(1, Ordering::SeqCst);
            format!("goosefs-tokio-worker-{id}")
        })
        .on_thread_start(|| {
            let vm = JavaVM::singleton().expect("JavaVM singleton must be initialized");
            vm.attach_current_thread(set_current_thread_name)
                .expect("attach current thread must succeed");
        })
        .on_thread_stop(|| {
            if let Ok(vm) = JavaVM::singleton() {
                let _ = vm.detach_current_thread();
            }
        })
        .enable_all()
        .build()
        .map_err(|e| crate::Error::Config(format!("failed to create tokio runtime: {e}")))?;
    Ok(Executor::Tokio(executor))
}

fn env_usize(key: &str) -> Option<usize> {
    match std::env::var(key) {
        Ok(v) => {
            let trimmed = v.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<usize>().ok()
            }
        }
        Err(_) => None,
    }
}

fn set_current_thread_name(env: &mut Env) -> Result<()> {
    let current_thread = env
        .call_static_method(
            jni_str!("java/lang/Thread"),
            jni_str!("currentThread"),
            jni_sig!("()Ljava/lang/Thread;"),
            &[],
        )?
        .l()?;
    let thread_name = match std::thread::current().name() {
        Some(thread_name) => env.new_string(thread_name)?,
        None => env.new_string("goosefs-tokio-worker")?,
    };
    env.call_method(
        &current_thread,
        jni_str!("setName"),
        jni_sig!("(Ljava/lang/String;)V"),
        &[JValue::Object(&thread_name)],
    )?;
    Ok(())
}

/// Crash if the executor is disposed.
#[inline]
pub(crate) fn executor_or_default(executor: *const Executor) -> Result<&'static Executor> {
    if executor.is_null() {
        default_executor()
    } else {
        // SAFETY: Java `AsyncExecutor` keeps this pointer alive until `close()`.
        Ok(unsafe { &*executor })
    }
}

#[allow(static_mut_refs)]
fn default_executor() -> Result<&'static Executor> {
    unsafe {
        if let Some(runtime) = RUNTIME.get() {
            return Ok(runtime);
        }
        let executor = make_tokio_executor(None)?;
        Ok(RUNTIME.get_or_init(|| executor))
    }
}

/// Throw `IllegalStateException` if the caller is already on a Tokio worker.
pub(crate) fn throw_if_in_runtime() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(crate::Error::IllegalState(
            "Goosefs sync methods cannot be invoked from inside a Tokio runtime; \
             use AsyncGoosefs instead"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_usize_missing_and_empty_return_none() {
        let key = "GOOSEFS_TEST_JAVA_ENV_USIZE_MISSING_KEY_XYZ_1";
        std::env::remove_var(key);
        assert_eq!(env_usize(key), None);
        std::env::set_var(key, "");
        assert_eq!(env_usize(key), None);
        std::env::set_var(key, "   ");
        assert_eq!(env_usize(key), None);
        std::env::set_var(key, "not-a-number");
        assert_eq!(env_usize(key), None);
        std::env::remove_var(key);
    }

    #[test]
    fn env_usize_parses_valid_values() {
        let key = "GOOSEFS_TEST_JAVA_ENV_USIZE_VALID_KEY_XYZ_2";
        std::env::set_var(key, "8");
        assert_eq!(env_usize(key), Some(8));
        std::env::set_var(key, "  16  ");
        assert_eq!(env_usize(key), Some(16));
        std::env::remove_var(key);
    }
}
