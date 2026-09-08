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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Streaming writer. {@link #commit()} finalises the inode; {@link #close()} cancels if neither
 * commit nor cancel has run.
 */
public class FileWriter extends NativeObject {
    private final long executorHandle;
    private final AtomicBoolean finalized = new AtomicBoolean(false);

    FileWriter(long nativeHandle, long executorHandle) {
        super(nativeHandle);
        this.executorHandle = executorHandle;
    }

    public long write(byte[] data) {
        ensureOpen();
        Objects.requireNonNull(data, "data");
        if (finalized.get()) {
            throw new IllegalStateException("FileWriter is closed");
        }
        return nativeWrite(nativeHandle, executorHandle, data);
    }

    /** Finalise and persist. Idempotent. */
    public void commit() {
        ensureOpen();
        if (finalized.compareAndSet(false, true)) {
            nativeCommit(nativeHandle, executorHandle);
        }
    }

    /** Abandon uncommitted state. Idempotent. */
    public void cancel() {
        ensureOpen();
        if (finalized.compareAndSet(false, true)) {
            nativeCancel(nativeHandle, executorHandle);
        }
    }

    /**
     * If neither {@link #commit()} nor {@link #cancel()} has run, cancel, then dispose. Does
     * <strong>not</strong> commit.
     */
    @Override
    public void close() {
        if (isDisposed()) {
            return;
        }
        try {
            if (finalized.compareAndSet(false, true)) {
                nativeCancel(nativeHandle, executorHandle);
            }
        } finally {
            super.close();
        }
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long nativeWrite(long nativeHandle, long executorHandle, byte[] data);

    private static native void nativeCommit(long nativeHandle, long executorHandle);

    private static native void nativeCancel(long nativeHandle, long executorHandle);
}
