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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Native-backed listing. {@link #size()} creates zero {@link URIStatus} objects; {@link #get(int)}
 * materialises one.
 */
public final class URIStatusList extends NativeObject implements Iterable<URIStatus> {
    URIStatusList(long nativeHandle) {
        super(nativeHandle);
    }

    public int size() {
        ensureOpen();
        return nativeSize(nativeHandle);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public URIStatus get(int index) {
        ensureOpen();
        return nativeGet(nativeHandle, index);
    }

    @Override
    public Iterator<URIStatus> iterator() {
        ensureOpen();
        return new Iterator<URIStatus>() {
            private int i;
            private final int n = size();

            @Override
            public boolean hasNext() {
                return i < n;
            }

            @Override
            public URIStatus next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return get(i++);
            }
        };
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native int nativeSize(long handle);

    private static native URIStatus nativeGet(long handle, int index);
}
