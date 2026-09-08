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
import com.tencent.goosefs.AsyncGoosefs;
import com.tencent.goosefs.DeleteOptions;
import com.tencent.goosefs.FileReader;
import com.tencent.goosefs.Goosefs;
import com.tencent.goosefs.GoosefsException;
import com.tencent.goosefs.URIStatus;
import com.tencent.goosefs.URIStatusList;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "GOOSEFS_MASTER_ADDR", matches = ".+")
class BatchTest {
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
    void batchGetStatusReturnsInOrder() {
        String a = tmp + "/a";
        String b = tmp + "/b";
        fs.mkdir(a);
        fs.mkdir(b);
        List<URIStatus> statuses = fs.batchGetStatus(List.of(a, b, tmp));
        assertThat(statuses).hasSize(3);
        assertThat(statuses.get(0).getPath()).isEqualTo(a);
        assertThat(statuses.get(1).getPath()).isEqualTo(b);
        assertThat(statuses.get(2).getPath()).isEqualTo(tmp);
    }

    @Test
    void batchGetStatusFailsWholeBatchOnMissing() {
        assertThatThrownBy(() -> fs.batchGetStatus(List.of(tmp, tmp + "/missing")))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.NotFound);
    }

    @Test
    void batchExistsAndCreateDir() {
        List<String> dirs = List.of(tmp + "/bd0", tmp + "/bd1", tmp + "/bd2");
        fs.batchCreateDir(dirs);
        assertThat(fs.batchExists(dirs)).containsExactly(true, true, true);
    }

    @Test
    void batchCreateFileCreatesEmptyFiles() {
        List<String> files = List.of(tmp + "/bf0", tmp + "/bf1", tmp + "/bf2");
        assertThat(fs.batchCreateFile(files)).containsExactly(0L, 0L, 0L);
        for (URIStatus s : fs.batchGetStatus(files)) {
            assertThat(s.isFolder()).isFalse();
            assertThat(s.getLength()).isZero();
        }
    }

    @Test
    void batchRenameMovesAll() {
        List<String> src = List.of(tmp + "/src0", tmp + "/src1");
        List<String> dst = List.of(tmp + "/dst0", tmp + "/dst1");
        fs.batchCreateFile(src);
        List<String> pairs = new ArrayList<>();
        for (int i = 0; i < src.size(); i++) {
            pairs.add(src.get(i));
            pairs.add(dst.get(i));
        }
        fs.batchRename(pairs);
        assertThat(fs.batchExists(src)).containsExactly(false, false);
        assertThat(fs.batchExists(dst)).containsExactly(true, true);
    }

    @Test
    void batchRenameOddLengthRaises() {
        assertThatThrownBy(() -> fs.batchRename(List.of(tmp + "/a", tmp + "/b", tmp + "/c")))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.InvalidArgument);
    }

    @Test
    void batchDeleteRemovesAll() {
        List<String> paths = List.of(tmp + "/del0", tmp + "/del1", tmp + "/del2");
        fs.batchCreateFile(paths);
        fs.batchDelete(paths);
        assertThat(fs.batchExists(paths)).containsExactly(false, false, false);
    }

    @Test
    void batchDeleteRecursive() {
        String parent = tmp + "/tree";
        fs.mkdir(parent);
        fs.mkdir(parent + "/sub");
        fs.writeFile(parent + "/root.txt", new byte[] {'x'});
        fs.writeFile(parent + "/sub/child.txt", new byte[] {'x'});
        fs.batchDelete(List.of(parent), DeleteOptions.builder().recursive(true).build());
        assertThat(fs.exists(parent)).isFalse();
    }

    @Test
    void batchOpenFileReadsInOrder() {
        List<String> paths = List.of(tmp + "/bof-0.bin", tmp + "/bof-1.bin", tmp + "/bof-2.bin");
        List<byte[]> payloads = List.of("payload-0".getBytes(), "payload-1".getBytes(), "payload-2".getBytes());
        for (int i = 0; i < paths.size(); i++) {
            fs.writeFile(paths.get(i), payloads.get(i));
        }
        List<FileReader> readers = fs.batchOpenFile(paths);
        try {
            assertThat(readers).hasSize(3);
            for (int i = 0; i < readers.size(); i++) {
                assertThat(readers.get(i).read()).isEqualTo(payloads.get(i));
            }
        } finally {
            for (FileReader r : readers) {
                r.close();
            }
        }
    }

    @Test
    void batchOpenFileEmptyAndSingle() {
        assertThat(fs.batchOpenFile(List.of())).isEmpty();
        String path = tmp + "/bof-solo.bin";
        fs.writeFile(path, "solo".getBytes());
        List<FileReader> readers = fs.batchOpenFile(List.of(path));
        try {
            assertThat(readers).hasSize(1);
            assertThat(readers.get(0).read()).isEqualTo("solo".getBytes());
        } finally {
            readers.get(0).close();
        }
    }

    @Test
    void batchOpenFileMissingFailsAndDoesNotLeak() {
        String good = tmp + "/bof-exists.bin";
        String missing = tmp + "/bof-missing.bin";
        fs.writeFile(good, new byte[] {'x'});
        assertThatThrownBy(() -> fs.batchOpenFile(List.of(good, missing)))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.NotFound);
        assertThat(fs.readFile(good)).isEqualTo(new byte[] {'x'});
        List<FileReader> readers = fs.batchOpenFile(List.of(good));
        try {
            assertThat(readers.get(0).read()).isEqualTo(new byte[] {'x'});
        } finally {
            readers.get(0).close();
        }
    }

    @Test
    void batchListStatusAndGrouped() {
        fs.mkdir(tmp + "/l0");
        fs.mkdir(tmp + "/l1");
        List<List<URIStatus>> nested = fs.batchListStatus(List.of(tmp));
        assertThat(nested).hasSize(1);
        assertThat(nested.get(0)).hasSize(2);
        List<URIStatusList> grouped = fs.batchListStatusGrouped(List.of(tmp));
        try {
            assertThat(grouped).hasSize(1);
            assertThat(grouped.get(0).size()).isEqualTo(2);
        } finally {
            for (URIStatusList list : grouped) {
                list.close();
            }
        }
    }

    @Test
    void asyncBatchOpenFile() {
        try (AsyncGoosefs async = ClusterSupport.join(AsyncGoosefs.connect(ClusterSupport.config()))) {
            String path = tmp + "/async-bof.bin";
            fs.writeFile(path, "async".getBytes());
            List<AsyncFileReader> readers = ClusterSupport.join(async.batchOpenFile(List.of(path)));
            try {
                assertThat(ClusterSupport.join(readers.get(0).read())).isEqualTo("async".getBytes());
            } finally {
                ClusterSupport.join(readers.get(0).closeAsync());
            }
        }
    }
}
