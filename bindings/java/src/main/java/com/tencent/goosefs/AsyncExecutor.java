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

/**
 * Isolated Tokio runtime. Must outlive clients that received this handle.
 * Pass {@code null} to {@link AsyncGoosefs#connect(Config, AsyncExecutor)} to
 * use the process default runtime.
 */
public class AsyncExecutor extends NativeObject {
    private AsyncExecutor(long nativeHandle) {
        super(nativeHandle);
    }

    public static AsyncExecutor createTokioExecutor(int cores) {
        if (cores < 1) {
            throw new IllegalArgumentException("cores must be >= 1");
        }
        return new AsyncExecutor(makeTokioExecutor(cores));
    }

    private static native long makeTokioExecutor(int cores);

    @Override
    protected native void disposeInternal(long handle);
}
