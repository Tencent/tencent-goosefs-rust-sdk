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
package com.tencent.goosefs.behavior;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tencent.goosefs.AsyncFileReader;
import com.tencent.goosefs.AsyncFileWriter;
import com.tencent.goosefs.AsyncGoosefs;
import com.tencent.goosefs.FileReader;
import com.tencent.goosefs.FileWriter;
import com.tencent.goosefs.Goosefs;
import com.tencent.goosefs.GoosefsException;
import com.tencent.goosefs.GoosefsInputStream;
import com.tencent.goosefs.GoosefsOutputStream;
import com.tencent.goosefs.OpenFileOptions;
import com.tencent.goosefs.ReadType;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "GOOSEFS_MASTER_ADDR", matches = ".+")
class StreamingTest {
    private Goosefs fs;
    private String tmp;

    @BeforeEach
    void setUp() {
        fs = Goosefs.connect(ClusterSupport.config());
        tmp = ClusterSupport.uniqueDir(fs);
    }

    @AfterEach
    void tearDown() {
        if (fs != null) {
            ClusterSupport.deleteQuietly(fs, tmp);
            fs.close();
        }
    }

    @Test
    void writerThenReaderRoundTrip() {
        String path = tmp + "/hello.bin";
        byte[] payload = "sync streaming hello\n".getBytes(StandardCharsets.UTF_8);
        try (FileWriter w = fs.createFile(path)) {
            assertThat(w.write(payload)).isEqualTo(payload.length);
            w.commit();
        }
        try (FileReader r = fs.openFile(path)) {
            assertThat(r.length()).isEqualTo(payload.length);
            assertThat(r.read()).isEqualTo(payload);
        }
    }

    @Test
    void forgottenCommitCancels() {
        String path = tmp + "/forgotten.bin";
        try (FileWriter w = fs.createFile(path)) {
            w.write("will be discarded".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(fs.exists(path)).isFalse();
    }

    @Test
    void explicitCancelIsIdempotent() {
        String path = tmp + "/cancel.bin";
        FileWriter w = fs.createFile(path);
        w.write("discarded".getBytes(StandardCharsets.UTF_8));
        w.cancel();
        w.cancel();
        w.close();
        assertThat(fs.exists(path)).isFalse();
    }

    @Test
    void readerReadInChunks() {
        String path = tmp + "/chunks.bin";
        byte[] body = ClusterSupport.payload("chunks", 256 * 1024);
        try (FileWriter w = fs.createFile(path)) {
            w.write(body);
            w.commit();
        }
        try (FileReader r = fs.openFile(path)) {
            byte[] out = new byte[0];
            while (true) {
                byte[] piece = r.read(33 * 1024);
                if (piece.length == 0) {
                    break;
                }
                out = concat(out, piece);
            }
            assertThat(out).isEqualTo(body);
        }
    }

    @Test
    void readAtDoesNotMoveCursor() {
        String path = tmp + "/pread.bin";
        byte[] body = ClusterSupport.payload("pread", 4096);
        try (FileWriter w = fs.createFile(path)) {
            w.write(body);
            w.commit();
        }
        try (FileReader r = fs.openFile(path)) {
            assertThat(r.tell()).isZero();
            byte[] chunk = r.readAt(100, 16);
            byte[] expected = new byte[16];
            System.arraycopy(body, 100, expected, 0, 16);
            assertThat(chunk).isEqualTo(expected);
            assertThat(r.tell()).isZero();
            byte[] head = r.read(4);
            byte[] headExpected = new byte[4];
            System.arraycopy(body, 0, headExpected, 0, 4);
            assertThat(head).isEqualTo(headExpected);
            assertThat(r.tell()).isEqualTo(4);
        }
    }

    @Test
    void seekSetCurEnd() {
        String path = tmp + "/seek.bin";
        byte[] body = "abcdefghij".repeat(100).getBytes(StandardCharsets.UTF_8);
        try (FileWriter w = fs.createFile(path)) {
            w.write(body);
            w.commit();
        }
        try (FileReader r = fs.openFile(path)) {
            assertThat(r.seek(50, FileReader.SEEK_SET)).isEqualTo(50);
            assertThat(r.read(5)).isEqualTo(slice(body, 50, 55));
            assertThat(r.seek(10, FileReader.SEEK_CUR)).isEqualTo(65);
            assertThat(r.read(5)).isEqualTo(slice(body, 65, 70));
            assertThat(r.seek(-5, FileReader.SEEK_END)).isEqualTo(body.length - 5);
            assertThat(r.read()).isEqualTo(slice(body, body.length - 5, body.length));
        }
    }

    @Test
    void invalidWhenceAndNegativeSeekSet() {
        String path = tmp + "/badseek.bin";
        try (FileWriter w = fs.createFile(path)) {
            w.write("abc".getBytes(StandardCharsets.UTF_8));
            w.commit();
        }
        try (FileReader r = fs.openFile(path)) {
            assertThatThrownBy(() -> r.seek(0, 99))
                    .isInstanceOf(GoosefsException.class)
                    .extracting(ex -> ((GoosefsException) ex).getCode())
                    .isEqualTo(GoosefsException.Code.InvalidArgument);
            assertThatThrownBy(() -> r.seek(-1, FileReader.SEEK_SET))
                    .isInstanceOf(GoosefsException.class)
                    .extracting(ex -> ((GoosefsException) ex).getCode())
                    .isEqualTo(GoosefsException.Code.InvalidArgument);
        }
    }

    @Test
    void writeAfterCloseRaises() {
        String path = tmp + "/write-after.bin";
        FileWriter w = fs.createFile(path);
        w.write("hi".getBytes(StandardCharsets.UTF_8));
        w.commit();
        w.close();
        assertThatThrownBy(() -> w.write("too late".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void openFileNoCacheReads() {
        String path = tmp + "/nocache.bin";
        byte[] payload = ClusterSupport.payload("nocache", 128);
        fs.writeFile(path, payload);
        try (FileReader r = fs.openFile(
                path, OpenFileOptions.builder().readType(ReadType.NO_CACHE).build())) {
            assertThat(r.read()).isEqualTo(payload);
        }
    }

    @Test
    void jdkStreamsRoundTrip() throws Exception {
        String path = tmp + "/jdk-io.bin";
        byte[] payload = ClusterSupport.payload("jdk", 64);
        try (GoosefsOutputStream out = fs.createOutputStream(path)) {
            out.write(payload);
        }
        try (GoosefsInputStream in = fs.createInputStream(path)) {
            byte[] got = in.readAllBytes();
            assertThat(got).isEqualTo(payload);
        }
    }

    @Test
    void outputStreamCancelAbandonsFile() {
        String path = tmp + "/jdk-cancel.bin";
        GoosefsOutputStream out = fs.createOutputStream(path);
        out.write(1);
        out.cancel();
        out.close();
        assertThat(fs.exists(path)).isFalse();
    }

    @Test
    void inputStreamSkipHonoursBuffer() throws Exception {
        String path = tmp + "/skip.bin";
        byte[] body = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        try (GoosefsOutputStream out = fs.createOutputStream(path)) {
            out.write(body);
        }
        try (GoosefsInputStream in = fs.createInputStream(path)) {
            assertThat(in.read()).isEqualTo('a');
            assertThat(in.read()).isEqualTo('b');
            assertThat(in.skip(3)).isEqualTo(3);
            byte[] rest = in.readAllBytes();
            assertThat(rest).isEqualTo("fghij".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void asyncWriterThenReader() {
        try (AsyncGoosefs async = ClusterSupport.join(AsyncGoosefs.connect(ClusterSupport.config()))) {
            String path = tmp + "/async-stream.bin";
            byte[] payload = ClusterSupport.payload("async-stream", 64);
            AsyncFileWriter w = ClusterSupport.join(async.createFile(path));
            try {
                assertThat(ClusterSupport.join(w.write(payload))).isEqualTo((long) payload.length);
                ClusterSupport.join(w.commit());
            } finally {
                w.close();
            }
            AsyncFileReader r = ClusterSupport.join(async.openFile(path));
            try {
                assertThat(r.length()).isEqualTo(payload.length);
                assertThat(ClusterSupport.join(r.read())).isEqualTo(payload);
            } finally {
                ClusterSupport.join(r.closeAsync());
            }
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static byte[] slice(byte[] src, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(src, from, out, 0, out.length);
        return out;
    }
}
