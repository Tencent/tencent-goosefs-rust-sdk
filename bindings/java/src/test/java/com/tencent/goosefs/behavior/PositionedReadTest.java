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
import com.tencent.goosefs.AsyncWorkerClient;
import com.tencent.goosefs.Goosefs;
import com.tencent.goosefs.GoosefsException;
import com.tencent.goosefs.URIStatus;
import com.tencent.goosefs.WorkerClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "GOOSEFS_MASTER_ADDR", matches = ".+")
class PositionedReadTest {
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
    void roundTripAndOffset() {
        String path = tmp + "/pread.bin";
        byte[] payload = ClusterSupport.payload("pread", 4096);
        fs.writeFile(path, payload);
        assertThat(fs.positionedRead(path, 0, 0, payload.length)).isEqualTo(payload);
        assertThat(fs.positionedRead(path, 0, 16, 32)).isEqualTo(java.util.Arrays.copyOfRange(payload, 16, 48));
        assertThat(fs.positionedRead(path, 0, 0, -1)).isEqualTo(payload);
        assertThat(fs.positionedRead(path, 0, 0, 0)).isEmpty();
    }

    @Test
    void rejectsLengthBelowMinusOne() {
        String path = tmp + "/neg.bin";
        fs.writeFile(path, new byte[64]);
        assertThatThrownBy(() -> fs.positionedRead(path, 0, 0, -2))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.InvalidArgument);
    }

    @Test
    void offsetPastBlockRaises() {
        String path = tmp + "/oob.bin";
        fs.writeFile(path, new byte[64]);
        assertThatThrownBy(() -> fs.positionedRead(path, 0, 1000, 10))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.InvalidArgument);
    }

    @Test
    void acquireWorkerForBlockReads() {
        String path = tmp + "/acq.bin";
        byte[] payload = ClusterSupport.payload("acq", 128);
        fs.writeFile(path, payload);
        URIStatus st = fs.getStatus(path);
        assertThat(st.getBlockIds()).isNotEmpty();
        long blockId = st.getBlockIds()[0];
        try (WorkerClient wc = fs.acquireWorkerForBlock(blockId, path)) {
            assertThat(wc.getAddr()).isNotBlank();
            assertThat(wc.readBlockPositioned(blockId, 0, payload.length)).isEqualTo(payload);
        }
    }

    @Test
    void asyncPositionedRead() {
        try (AsyncGoosefs async = ClusterSupport.join(AsyncGoosefs.connect(ClusterSupport.config()))) {
            String path = tmp + "/async-pread.bin";
            byte[] payload = ClusterSupport.payload("async-pread", 64);
            ClusterSupport.join(async.writeFile(path, payload));
            assertThat(ClusterSupport.join(async.positionedRead(path, 0, 0, -1)))
                    .isEqualTo(payload);
            URIStatus st = ClusterSupport.join(async.getStatus(path));
            AsyncWorkerClient wc = ClusterSupport.join(async.acquireWorkerForBlock(st.getBlockIds()[0], path));
            try {
                assertThat(ClusterSupport.join(wc.readBlockPositioned(st.getBlockIds()[0], 0, payload.length)))
                        .isEqualTo(payload);
            } finally {
                ClusterSupport.join(wc.closeAsync());
            }
        }
    }
}
