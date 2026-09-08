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

/**
 * Async Worker block client. {@link #getAddr()} stays synchronous. There is no {@code
 * connectSimple}.
 */
public class AsyncWorkerClient extends NativeObject {
    public static final long DEFAULT_CHUNK_SIZE = WorkerClient.DEFAULT_CHUNK_SIZE;

    private final long executorHandle;

    AsyncWorkerClient(long nativeHandle, long executorHandle) {
        super(nativeHandle);
        this.executorHandle = executorHandle;
    }

    public static CompletableFuture<AsyncWorkerClient> connect(String addr, Config config) {
        return connect(addr, config, null);
    }

    public static CompletableFuture<AsyncWorkerClient> connect(String addr, Config config, AsyncExecutor executor) {
        Objects.requireNonNull(addr, "addr");
        Objects.requireNonNull(config, "config");
        final long executorHandle = executor != null ? executor.nativeHandle : 0L;
        return AsyncRegistry.take(nativeConnect(addr, config, executorHandle));
    }

    public CompletableFuture<byte[]> readBlockPositioned(long blockId, long offset, long length) {
        return readBlockPositioned(blockId, offset, length, DEFAULT_CHUNK_SIZE);
    }

    public CompletableFuture<byte[]> readBlockPositioned(long blockId, long offset, long length, long chunkSize) {
        ensureOpen();
        return AsyncRegistry.take(
                nativeReadBlockPositioned(nativeHandle, executorHandle, blockId, offset, length, chunkSize));
    }

    public String getAddr() {
        ensureOpen();
        return nativeGetAddr(nativeHandle);
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

    private static native long nativeConnect(String addr, Config config, long executorHandle);

    private static native long nativeReadBlockPositioned(
            long nativeHandle, long executorHandle, long blockId, long offset, long length, long chunkSize);

    private static native String nativeGetAddr(long nativeHandle);

    private static native long nativeCloseAsync(long nativeHandle, long executorHandle);

    private static native void nativeThrowIfOnTokio();
}
