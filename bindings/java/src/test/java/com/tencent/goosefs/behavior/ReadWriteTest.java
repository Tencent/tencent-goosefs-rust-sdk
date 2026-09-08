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

import com.tencent.goosefs.AsyncGoosefs;
import com.tencent.goosefs.CreateFileOptions;
import com.tencent.goosefs.Goosefs;
import com.tencent.goosefs.GoosefsException;
import com.tencent.goosefs.URIStatus;
import com.tencent.goosefs.WriteType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@EnabledIfEnvironmentVariable(named = "GOOSEFS_MASTER_ADDR", matches = ".+")
class ReadWriteTest {
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

    @ParameterizedTest
    @EnumSource(
            value = WriteType.class,
            names = {"MUST_CACHE", "CACHE_THROUGH", "THROUGH", "ASYNC_THROUGH"})
    void roundTrip64b(WriteType writeType) {
        String path = tmp + "/" + writeType.name() + "-64.bin";
        byte[] payload = ClusterSupport.payload(writeType.name(), 64);
        long n = fs.writeFile(
                path, payload, CreateFileOptions.builder().writeType(writeType).build());
        assertThat(n).isEqualTo(64);
        assertThat(fs.readFile(path)).isEqualTo(payload);
        URIStatus st = fs.getStatus(path);
        assertThat(st.getLength()).isEqualTo(64);
        assertThat(st.isCompleted()).isTrue();
    }

    @Test
    void roundTrip64kibMustCache() {
        String path = tmp + "/must-cache-64k.bin";
        byte[] payload = ClusterSupport.payload("64k", 64 * 1024);
        long n = fs.writeFile(
                path,
                payload,
                CreateFileOptions.builder().writeType(WriteType.MUST_CACHE).build());
        assertThat(n).isEqualTo(payload.length);
        assertThat(fs.readFile(path)).isEqualTo(payload);
    }

    @Test
    void mustCacheReportsInGoosefsPercentage() {
        String path = tmp + "/must-cache-pct.bin";
        fs.writeFile(
                path,
                ClusterSupport.payload("pct", 4096),
                CreateFileOptions.builder().writeType(WriteType.MUST_CACHE).build());
        URIStatus st = fs.getStatus(path);
        assertThat(st.isCacheable()).isTrue();
        assertThat(st.isPersisted()).isFalse();
        assertThat(st.getInGooseFsPercentage()).isGreaterThan(0);
    }

    @Test
    void readRangeOffsetsAndShortRead() {
        String path = tmp + "/read-range.bin";
        byte[] payload = ClusterSupport.payload("read-range", 4096);
        fs.writeFile(
                path,
                payload,
                CreateFileOptions.builder().writeType(WriteType.MUST_CACHE).build());
        assertThat(fs.readRange(path, 1024, 512)).isEqualTo(java.util.Arrays.copyOfRange(payload, 1024, 1536));
        assertThat(fs.readRange(path, 4000, 96)).isEqualTo(java.util.Arrays.copyOfRange(payload, 4000, 4096));
        assertThat(fs.readRange(path, 4000, 1024)).isEqualTo(java.util.Arrays.copyOfRange(payload, 4000, 4096));
    }

    @Test
    void readRangeRejectsNegatives() {
        String path = tmp + "/read-range-neg.bin";
        fs.writeFile(path, new byte[10]);
        assertThatThrownBy(() -> fs.readRange(path, 0, -1))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.InvalidArgument);
        assertThatThrownBy(() -> fs.readRange(path, -1, 1))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.InvalidArgument);
    }

    @Test
    void writeFileInheritWriteType() {
        String path = tmp + "/inherit.bin";
        byte[] payload = ClusterSupport.payload("inherit", 256);
        assertThat(fs.writeFile(path, payload)).isEqualTo(256);
        assertThat(fs.readFile(path)).isEqualTo(payload);
    }

    @Test
    void writeFileMissingParentIsNotFound() {
        assertThatThrownBy(() -> fs.writeFile(tmp + "/missing-parent/file.bin", new byte[] {1}))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.NotFound);
    }

    @Test
    void readFileOnDirectoryIsADirectory() {
        assertThatThrownBy(() -> fs.readFile(tmp))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.IsADirectory);
    }

    @Test
    void asyncRoundTrip() {
        try (AsyncGoosefs async = ClusterSupport.join(AsyncGoosefs.connect(ClusterSupport.config()))) {
            String path = tmp + "/async.bin";
            byte[] payload = ClusterSupport.payload("async", 128);
            Long n = ClusterSupport.join(async.writeFile(path, payload));
            assertThat(n).isEqualTo(128L);
            assertThat(ClusterSupport.join(async.readFile(path))).isEqualTo(payload);
        }
    }
}
