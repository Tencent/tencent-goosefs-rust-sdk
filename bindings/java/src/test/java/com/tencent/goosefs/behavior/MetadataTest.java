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
import com.tencent.goosefs.DeleteOptions;
import com.tencent.goosefs.Goosefs;
import com.tencent.goosefs.GoosefsException;
import com.tencent.goosefs.URIStatus;
import com.tencent.goosefs.URIStatusList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "GOOSEFS_MASTER_ADDR", matches = ".+")
class MetadataTest {
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
    void existsMissingAndDirectory() {
        assertThat(fs.exists(tmp + "/missing")).isFalse();
        assertThat(fs.exists(tmp)).isTrue();
    }

    @Test
    void mkdirCreatesAndIsIdempotent() {
        String sub = tmp + "/sub";
        fs.mkdir(sub);
        assertThat(fs.exists(sub)).isTrue();
        fs.mkdir(sub);
        assertThat(fs.exists(sub)).isTrue();
    }

    @Test
    void mkdirRecursiveCreatesParents() {
        String deep = tmp + "/a/b/c";
        fs.mkdir(deep, true);
        assertThat(fs.exists(tmp + "/a")).isTrue();
        assertThat(fs.exists(tmp + "/a/b")).isTrue();
        assertThat(fs.exists(deep)).isTrue();
    }

    @Test
    void listStatusEmptyAndChildren() {
        assertThat(fs.listStatus(tmp)).isEmpty();
        fs.mkdir(tmp + "/a");
        fs.mkdir(tmp + "/b");
        fs.mkdir(tmp + "/c");
        List<String> names =
                fs.listStatus(tmp).stream().map(URIStatus::getName).sorted().collect(Collectors.toList());
        assertThat(names).containsExactly("a", "b", "c");
    }

    @Test
    void listStatusRecursiveWalksTree() {
        fs.mkdir(tmp + "/x/y/z", true);
        List<String> paths =
                fs.listStatus(tmp, true).stream().map(URIStatus::getPath).collect(Collectors.toList());
        assertThat(paths).contains(tmp + "/x", tmp + "/x/y", tmp + "/x/y/z");
    }

    @Test
    void listStatusGroupedIsLazy() {
        fs.mkdir(tmp + "/a");
        fs.mkdir(tmp + "/b");
        try (URIStatusList grouped = fs.listStatusGrouped(tmp)) {
            assertThat(grouped.size()).isEqualTo(2);
            assertThat(grouped.isEmpty()).isFalse();
            URIStatus first = grouped.get(0);
            assertThat(first.getName()).isIn("a", "b");
            List<String> names = new java.util.ArrayList<>();
            for (URIStatus s : grouped) {
                names.add(s.getName());
            }
            assertThat(names).containsExactlyInAnyOrder("a", "b");
            assertThatThrownBy(() -> grouped.get(5)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> grouped.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void renameMovesDirectory() {
        String src = tmp + "/src";
        String dst = tmp + "/dst";
        fs.mkdir(src);
        fs.rename(src, dst);
        assertThat(fs.exists(src)).isFalse();
        assertThat(fs.exists(dst)).isTrue();
    }

    @Test
    void deleteEmptyDirectory() {
        String sub = tmp + "/sub";
        fs.mkdir(sub);
        fs.delete(sub);
        assertThat(fs.exists(sub)).isFalse();
    }

    @Test
    void deleteNonEmptyWithoutRecursiveRaises() {
        String parent = tmp + "/parent";
        fs.mkdir(parent);
        fs.mkdir(parent + "/child");
        assertThatThrownBy(() -> fs.delete(parent)).isInstanceOf(GoosefsException.class);
    }

    @Test
    void deleteRecursiveRemovesTree() {
        String parent = tmp + "/parent";
        fs.mkdir(parent + "/a/b", true);
        fs.delete(parent, DeleteOptions.builder().recursive(true).build());
        assertThat(fs.exists(parent)).isFalse();
    }

    @Test
    void asyncExistsAndMkdir() {
        try (AsyncGoosefs async = ClusterSupport.join(AsyncGoosefs.connect(ClusterSupport.config()))) {
            String sub = tmp + "/async-sub";
            ClusterSupport.join(async.mkdir(sub));
            assertThat(ClusterSupport.join(async.exists(sub))).isTrue();
            ClusterSupport.join(async.delete(sub));
        }
    }
}
