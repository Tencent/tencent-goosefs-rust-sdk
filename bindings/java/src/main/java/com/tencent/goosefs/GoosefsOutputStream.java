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

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * {@link OutputStream} over a {@link FileWriter}. {@link #close()} commits (Java IO contract);
 * {@link #cancel()} aborts.
 */
public final class GoosefsOutputStream extends OutputStream {
    private final FileWriter writer;
    private boolean cancelled;

    public GoosefsOutputStream(FileWriter writer) {
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    @Override
    public void write(int b) {
        writer.write(new byte[] {(byte) b});
    }

    @Override
    public void write(byte[] b, int off, int len) {
        Objects.requireNonNull(b, "b");
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        if (off == 0 && len == b.length) {
            writer.write(b);
        } else {
            writer.write(Arrays.copyOfRange(b, off, off + len));
        }
    }

    /** Abandon the file. Subsequent {@link #close()} will not commit. */
    public void cancel() {
        cancelled = true;
        writer.cancel();
    }

    @Override
    public void close() {
        if (!cancelled) {
            writer.commit();
        }
        writer.close();
    }
}
