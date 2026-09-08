/*
 * Copyright (C) 2026 Tencent. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.goosefs;

/** Unchecked exception wrapping a GooseFS SDK error category. */
public class GoosefsException extends RuntimeException {
    public enum Code {
        Unexpected,
        NotFound,
        AlreadyExists,
        PermissionDenied,
        InvalidArgument,
        FileIncomplete,
        DirectoryNotEmpty,
        IsADirectory,
        AuthenticationFailed,
        NoWorkerAvailable,
        MasterUnavailable,
        RpcError,
        IoError,
        ConfigError
    }

    private final Code code;

    public GoosefsException(String code, String message) {
        super(message);
        this.code = Code.valueOf(code);
    }

    public GoosefsException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code getCode() {
        return code;
    }
}
