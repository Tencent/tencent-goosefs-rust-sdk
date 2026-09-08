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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Async streaming writer. {@link #commit()} finalises; {@link #close()} cancels if still open.
 */
public class AsyncFileWriter extends NativeObject {
    private final long executorHandle;
    private final AtomicBoolean finalized = new AtomicBoolean(false);

    AsyncFileWriter(long nativeHandle, long executorHandle) {
        super(nativeHandle);
        this.executorHandle = executorHandle;
    }

    public CompletableFuture<Long> write(byte[] data) {
        ensureOpen();
        Objects.requireNonNull(data, "data");
        if (finalized.get()) {
            throw new IllegalStateException("FileWriter is closed");
        }
        return AsyncRegistry.take(nativeWrite(nativeHandle, executorHandle, data));
    }

    public CompletableFuture<Void> commit() {
        ensureOpen();
        if (!finalized.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        return AsyncRegistry.take(nativeCommit(nativeHandle, executorHandle));
    }

    public CompletableFuture<Void> cancel() {
        ensureOpen();
        if (!finalized.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        return AsyncRegistry.take(nativeCancel(nativeHandle, executorHandle));
    }

    /**
     * If neither commit nor cancel has run, cancel (blocking), then dispose. Does not commit.
     */
    @Override
    public void close() {
        nativeThrowIfOnTokio();
        if (isDisposed()) {
            return;
        }
        try {
            if (finalized.compareAndSet(false, true)) {
                joinQuiet(AsyncRegistry.<Void>take(nativeCancel(nativeHandle, executorHandle)));
            }
        } finally {
            super.close();
        }
    }

    private static void joinQuiet(CompletableFuture<Void> f) {
        try {
            f.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long nativeWrite(long nativeHandle, long executorHandle, byte[] data);

    private static native long nativeCommit(long nativeHandle, long executorHandle);

    private static native long nativeCancel(long nativeHandle, long executorHandle);

    private static native void nativeThrowIfOnTokio();
}
