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

/**
 * Blocking Worker block client. Closing a pool-acquired handle only drops the wrapper; the pooled
 * channel stays in {@code FileSystemContext}.
 */
public class WorkerClient extends NativeObject {
    public static final long DEFAULT_CHUNK_SIZE = 1L << 20;

    private final long executorHandle;

    WorkerClient(long nativeHandle, long executorHandle) {
        super(nativeHandle);
        this.executorHandle = executorHandle;
    }

    public static WorkerClient connect(String addr, Config config) {
        return connect(addr, config, null);
    }

    public static WorkerClient connect(String addr, Config config, AsyncExecutor executor) {
        Objects.requireNonNull(addr, "addr");
        Objects.requireNonNull(config, "config");
        final long executorHandle = executor != null ? executor.nativeHandle : 0L;
        return nativeConnect(addr, config, executorHandle);
    }

    public byte[] readBlockPositioned(long blockId, long offset, long length) {
        return readBlockPositioned(blockId, offset, length, DEFAULT_CHUNK_SIZE);
    }

    public byte[] readBlockPositioned(long blockId, long offset, long length, long chunkSize) {
        ensureOpen();
        return nativeReadBlockPositioned(nativeHandle, executorHandle, blockId, offset, length, chunkSize);
    }

    public String getAddr() {
        ensureOpen();
        return nativeGetAddr(nativeHandle);
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native WorkerClient nativeConnect(String addr, Config config, long executorHandle);

    private static native byte[] nativeReadBlockPositioned(
            long nativeHandle, long executorHandle, long blockId, long offset, long length, long chunkSize);

    private static native String nativeGetAddr(long nativeHandle);
}
