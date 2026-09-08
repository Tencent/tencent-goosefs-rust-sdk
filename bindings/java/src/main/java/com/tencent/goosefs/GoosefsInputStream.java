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

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/** {@link InputStream} over a {@link FileReader}. {@code close()} releases the reader. */
public final class GoosefsInputStream extends InputStream {
    private final FileReader reader;
    private byte[] buf = new byte[0];
    private int pos;
    private boolean eof;

    public GoosefsInputStream(FileReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : (one[0] & 0xff);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.requireNonNull(b, "b");
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        if (eof) {
            return -1;
        }
        if (pos >= buf.length) {
            buf = reader.read(FileReader.CHUNK_SIZE);
            pos = 0;
            if (buf.length == 0) {
                eof = true;
                return -1;
            }
        }
        int n = Math.min(len, buf.length - pos);
        System.arraycopy(buf, pos, b, off, n);
        pos += n;
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }
        int remainingInBuf = buf.length - pos;
        if (remainingInBuf > 0 && n <= remainingInBuf) {
            pos += (int) n;
            return n;
        }
        long skipped = remainingInBuf;
        buf = new byte[0];
        pos = 0;
        eof = false;
        long rest = n - remainingInBuf;
        long from = reader.tell();
        reader.seek(rest, FileReader.SEEK_CUR);
        return skipped + (reader.tell() - from);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void close() {
        reader.close();
    }
}
