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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for types that own a native (Rust) pointer.
 *
 * <p>Call {@link #close()} or use try-with-resources. There is no {@code finalize()};
 * the GC cannot see through the {@code long} handle (JEP 421).
 */
public abstract class NativeObject implements AutoCloseable {
    static {
        NativeLibrary.loadLibrary();
    }

    private final AtomicBoolean disposed = new AtomicBoolean(false);

    protected final long nativeHandle;

    protected NativeObject(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    @Override
    public void close() {
        if (disposed.compareAndSet(false, true)) {
            disposeInternal(nativeHandle);
        }
    }

    public boolean isDisposed() {
        return disposed.get();
    }

    protected void ensureOpen() {
        if (isDisposed()) {
            throw new IllegalStateException("native object is closed");
        }
    }

    protected abstract void disposeInternal(long handle);
}
