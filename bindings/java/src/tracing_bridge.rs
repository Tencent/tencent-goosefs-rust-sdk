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

//! `Tracing.enable` — opt-in stderr subscriber (Python `enable_tracing`).

use std::sync::OnceLock;

use jni::objects::JClass;
use jni::objects::JObject;
use jni::Env;
use jni::EnvUnowned;
use tracing_subscriber::fmt;
use tracing_subscriber::EnvFilter;

use crate::convert::optional_jstring_to_string;
use crate::error::{Error, ThrowException};
use crate::Result;

static TRACING_INSTALLED: OnceLock<bool> = OnceLock::new();

fn intern_enable(env: &mut Env, level: JObject, target: JObject) -> Result<()> {
    let level = optional_jstring_to_string(env, &level)?.unwrap_or_else(|| "info".into());
    let target = optional_jstring_to_string(env, &target)?.unwrap_or_else(|| "stderr".into());
    enable_tracing(&level, &target)
}

pub(crate) fn enable_tracing(level: &str, target: &str) -> Result<()> {
    match target.to_ascii_lowercase().as_str() {
        "stderr" => {}
        "logging" | "stdout" => {
            return Err(Error::IllegalArgument(format!(
                "target={target:?} is reserved for a future release; only \"stderr\" is supported today"
            )));
        }
        other => {
            return Err(Error::IllegalArgument(format!(
                "target must be \"stderr\"; got {other:?}"
            )));
        }
    }

    let normalized = level.to_ascii_lowercase();
    if !matches!(
        normalized.as_str(),
        "trace" | "debug" | "info" | "warn" | "error"
    ) {
        return Err(Error::IllegalArgument(format!(
            "level must be one of trace|debug|info|warn|error (case-insensitive); got {level:?}"
        )));
    }

    if TRACING_INSTALLED.get().is_some() {
        return Ok(());
    }

    let filter = if std::env::var_os("RUST_LOG").is_some() {
        EnvFilter::try_from_default_env()
            .map_err(|e| Error::IllegalArgument(format!("invalid RUST_LOG value: {e}")))?
    } else {
        let directive = format!("warn,goosefs_sdk={normalized},goosefs_java={normalized}");
        EnvFilter::try_new(&directive).map_err(|e| {
            Error::IllegalArgument(format!("failed to build EnvFilter from {directive:?}: {e}"))
        })?
    };

    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_ansi(false)
        .try_init()
        .map_err(|e| {
            Error::IllegalState(format!(
                "could not install goosefs tracing subscriber (another subscriber is already active?): {e}"
            ))
        })?;

    let _ = TRACING_INSTALLED.set(true);
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_tencent_goosefs_Tracing_nativeEnable<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    level: JObject<'local>,
    target: JObject<'local>,
) {
    env.with_env(|env| intern_enable(env, level, target))
        .resolve::<ThrowException>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_level_and_target() {
        assert!(enable_tracing("verbose", "stderr").is_err());
        assert!(enable_tracing("info", "stdout").is_err());
        assert!(enable_tracing("info", "logging").is_err());
    }
}
