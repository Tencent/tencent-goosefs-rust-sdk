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

import java.util.concurrent.CompletableFuture;

/** Async seekable streaming reader. {@link #tell()} / {@link #length()} stay synchronous. */
public class AsyncFileReader extends NativeObject {
    public static final int SEEK_SET = FileReader.SEEK_SET;
    public static final int SEEK_CUR = FileReader.SEEK_CUR;
    public static final int SEEK_END = FileReader.SEEK_END;

    private final long executorHandle;

    AsyncFileReader(long nativeHandle, long executorHandle) {
        super(nativeHandle);
        this.executorHandle = executorHandle;
    }

    public CompletableFuture<byte[]> read() {
        ensureOpen();
        return AsyncRegistry.take(nativeRead(nativeHandle, executorHandle, -1));
    }

    public CompletableFuture<byte[]> read(int size) {
        ensureOpen();
        if (size < 0) {
            throw new GoosefsException(GoosefsException.Code.InvalidArgument, "read size must be non-negative");
        }
        return AsyncRegistry.take(nativeRead(nativeHandle, executorHandle, size));
    }

    public CompletableFuture<byte[]> readAt(long offset, int length) {
        ensureOpen();
        if (length < 0) {
            throw new GoosefsException(GoosefsException.Code.InvalidArgument, "readAt length must be non-negative");
        }
        return AsyncRegistry.take(nativeReadAt(nativeHandle, executorHandle, offset, length));
    }

    public CompletableFuture<Long> seek(long offset, int whence) {
        ensureOpen();
        return AsyncRegistry.take(nativeSeek(nativeHandle, executorHandle, offset, whence));
    }

    public long tell() {
        ensureOpen();
        return nativeTell(nativeHandle);
    }

    public long length() {
        ensureOpen();
        return nativeLength(nativeHandle);
    }

    public CompletableFuture<Void> closeAsync() {
        if (isDisposed()) {
            return CompletableFuture.completedFuture(null);
        }
        return AsyncRegistry.<Void>take(nativeCloseAsync(nativeHandle, executorHandle))
                .whenComplete((v, t) -> super.close());
    }

    @Override
    public void close() {
        nativeThrowIfOnTokio();
        closeAsync().join();
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long nativeRead(long nativeHandle, long executorHandle, int size);

    private static native long nativeReadAt(long nativeHandle, long executorHandle, long offset, int length);

    private static native long nativeSeek(long nativeHandle, long executorHandle, long offset, int whence);

    private static native long nativeTell(long nativeHandle);

    private static native long nativeLength(long nativeHandle);

    private static native long nativeCloseAsync(long nativeHandle, long executorHandle);

    private static native void nativeThrowIfOnTokio();
}
