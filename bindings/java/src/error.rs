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

//! JNI error mapping: exhaustive SDK `Error` → `GoosefsException.Code`.

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use jni::errors::ErrorPolicy;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JThrowable;
use jni::objects::JValue;
use jni::strings::JNIString;
use jni::Env;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub(crate) enum Error {
    Sdk(goosefs_sdk::error::Error),
    Jni(jni::errors::Error),
    IllegalState(String),
    Config(String),
    IndexOutOfBounds(String),
    IllegalArgument(String),
}

impl Error {
    pub(crate) fn throw(&self, env: &mut Env) {
        if let Err(err) = self.do_throw(env) {
            match err {
                jni::errors::Error::JavaException => {}
                _ => env.fatal_error(JNIString::new(err.to_string()).as_ref()),
            }
        }
    }

    pub(crate) fn to_exception<'local>(
        &self,
        env: &mut Env<'local>,
    ) -> jni::errors::Result<JThrowable<'local>> {
        if let Error::IllegalState(message)
        | Error::IndexOutOfBounds(message)
        | Error::IllegalArgument(message) = self
        {
            let class = match self {
                Error::IndexOutOfBounds(_) => jni_str!("java/lang/IndexOutOfBoundsException"),
                Error::IllegalArgument(_) => jni_str!("java/lang/IllegalArgumentException"),
                _ => jni_str!("java/lang/IllegalStateException"),
            };
            let message = env.new_string(message)?;
            let exception = env.new_object(
                class,
                jni_sig!("(Ljava/lang/String;)V"),
                &[JValue::Object(&message)],
            )?;
            // SAFETY: just constructed as a Throwable subclass.
            return Ok(unsafe { JThrowable::from_raw(env, exception.into_raw()) });
        }

        let code = env.new_string(self.exception_code())?;
        let message = env.new_string(self.to_string())?;
        let exception = env.new_object(
            jni_str!("com/tencent/goosefs/GoosefsException"),
            jni_sig!("(Ljava/lang/String;Ljava/lang/String;)V"),
            &[JValue::Object(&code), JValue::Object(&message)],
        )?;
        // SAFETY: just constructed as GoosefsException, a Throwable.
        Ok(unsafe { JThrowable::from_raw(env, exception.into_raw()) })
    }

    fn do_throw(&self, env: &mut Env) -> jni::errors::Result<()> {
        let exception = self.to_exception(env)?;
        env.throw(exception)
    }

    /// Java `GoosefsException.Code` name. Exhaustive over the SDK enum.
    pub(crate) fn exception_code(&self) -> &'static str {
        match self {
            Error::IllegalState(_) | Error::IndexOutOfBounds(_) | Error::IllegalArgument(_) => {
                "Unexpected"
            }
            Error::Config(_) => "ConfigError",
            Error::Jni(_) => "Unexpected",
            Error::Sdk(err) => sdk_exception_code(err),
        }
    }
}

/// Map every `goosefs_sdk::error::Error` variant. No `_` arm: a new SDK
/// variant fails the build, matching the Python binding's `map_err`.
pub(crate) fn sdk_exception_code(err: &goosefs_sdk::error::Error) -> &'static str {
    use goosefs_sdk::error::Error as E;
    match err {
        E::NotFound { .. } => "NotFound",
        E::AlreadyExists { .. } => "AlreadyExists",
        E::PermissionDenied { .. } => "PermissionDenied",
        E::InvalidArgument { .. } | E::InvalidPath { .. } => "InvalidArgument",
        E::FileIncomplete { .. } => "FileIncomplete",
        E::DirectoryNotEmpty { .. } => "DirectoryNotEmpty",
        E::OpenDirectory { .. } => "IsADirectory",
        E::AuthenticationFailed { .. } => "AuthenticationFailed",
        E::NoWorkerAvailable { .. } => "NoWorkerAvailable",
        E::MasterUnavailable { .. } => "MasterUnavailable",
        E::ConfigError { .. } => "ConfigError",
        E::GrpcError { .. } | E::TransportError { .. } => "RpcError",
        E::BlockIoError { .. } | E::ResourceExhausted { .. } => "IoError",
        E::MissingField { .. } | E::Internal { .. } => "Unexpected",
    }
}

/// Native-method [`ErrorPolicy`]: SDK/JNI errors become Java exceptions;
/// panics become `fatal_error` (never unwind across FFI).
pub(crate) enum ThrowException {}

impl<T: Default> ErrorPolicy<T, Error> for ThrowException {
    type Captures<'unowned_env_local: 'native_method, 'native_method> = ();

    fn on_error<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _cap: &mut Self::Captures<'unowned_env_local, 'native_method>,
        err: Error,
    ) -> jni::errors::Result<T> {
        if !env.exception_check() {
            err.throw(env);
        }
        Ok(T::default())
    }

    fn on_panic<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _cap: &mut Self::Captures<'unowned_env_local, 'native_method>,
        payload: Box<dyn std::any::Any + Send + 'static>,
    ) -> jni::errors::Result<T> {
        if !env.exception_check() {
            let payload = downcast_payload(payload);
            env.fatal_error(JNIString::new(payload).as_ref());
        }
        Ok(T::default())
    }
}

fn downcast_payload(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    let payload = match payload.downcast::<&'static str>() {
        Ok(payload) => return payload.to_string(),
        Err(payload) => payload,
    };
    match payload.downcast::<String>() {
        Ok(payload) => *payload,
        Err(_) => "native method panicked".to_string(),
    }
}

impl From<goosefs_sdk::error::Error> for Error {
    fn from(err: goosefs_sdk::error::Error) -> Self {
        Self::Sdk(err)
    }
}

impl From<jni::errors::Error> for Error {
    fn from(err: jni::errors::Error) -> Self {
        Self::Jni(err)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Sdk(err) => Display::fmt(err, f),
            Error::Jni(err) => Display::fmt(err, f),
            Error::IllegalState(msg)
            | Error::Config(msg)
            | Error::IndexOutOfBounds(msg)
            | Error::IllegalArgument(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Sdk(err) => Some(err),
            Error::Jni(err) => Some(err),
            Error::IllegalState(_)
            | Error::Config(_)
            | Error::IndexOutOfBounds(_)
            | Error::IllegalArgument(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use goosefs_sdk::error::Error as E;

    #[test]
    fn sdk_codes_are_exhaustive_and_stable() {
        let cases: Vec<(E, &str)> = vec![
            (E::NotFound { path: "/x".into() }, "NotFound"),
            (E::AlreadyExists { path: "/x".into() }, "AlreadyExists"),
            (
                E::PermissionDenied {
                    message: "p".into(),
                },
                "PermissionDenied",
            ),
            (
                E::InvalidArgument {
                    message: "i".into(),
                },
                "InvalidArgument",
            ),
            (
                E::InvalidPath {
                    path: "/bad".into(),
                },
                "InvalidArgument",
            ),
            (
                E::FileIncomplete {
                    message: "f".into(),
                },
                "FileIncomplete",
            ),
            (
                E::DirectoryNotEmpty {
                    message: "d".into(),
                },
                "DirectoryNotEmpty",
            ),
            (E::OpenDirectory { path: "/d".into() }, "IsADirectory"),
            (
                E::AuthenticationFailed {
                    message: "a".into(),
                },
                "AuthenticationFailed",
            ),
            (
                E::NoWorkerAvailable {
                    message: "w".into(),
                },
                "NoWorkerAvailable",
            ),
            (
                E::ResourceExhausted {
                    message: "replicas".into(),
                },
                "IoError",
            ),
            (
                E::MasterUnavailable {
                    message: "m".into(),
                },
                "MasterUnavailable",
            ),
            (
                E::ConfigError {
                    message: "c".into(),
                },
                "ConfigError",
            ),
            (
                E::MissingField {
                    field: "block_id".into(),
                },
                "Unexpected",
            ),
            (
                E::BlockIoError {
                    message: "io".into(),
                },
                "IoError",
            ),
            (
                E::Internal {
                    message: "boom".into(),
                    source: None,
                },
                "Unexpected",
            ),
            (
                E::GrpcError {
                    message: "g".into(),
                    source: Box::new(tonic::Status::unknown("g")),
                },
                "RpcError",
            ),
        ];
        for (err, code) in cases {
            assert_eq!(sdk_exception_code(&err), code, "{err}");
        }
    }
}
