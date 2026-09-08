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

import com.tencent.goosefs.Config;
import com.tencent.goosefs.FileReader;
import com.tencent.goosefs.Goosefs;
import com.tencent.goosefs.WriteType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

@EnabledIfEnvironmentVariable(named = "GOOSEFS_MASTER_ADDR", matches = ".+")
class PageCacheTest {
    private static final int PAGE_SIZE = 64 * 1024;

    @Test
    void cacheOnAndOffByteIdentity(@TempDir Path cacheDir) throws Exception {
        byte[] payload = ClusterSupport.payload("page-cache", 4 * PAGE_SIZE);
        Config on = cacheConfig(cacheDir, true);
        Config off = cacheConfig(cacheDir.resolve("off"), false);

        String path;
        try (Goosefs fs = Goosefs.connect(on)) {
            String dir = ClusterSupport.uniqueDir(fs);
            path = dir + "/cached.bin";
            fs.writeFile(
                    path,
                    payload,
                    com.tencent.goosefs.CreateFileOptions.builder()
                            .writeType(WriteType.CACHE_THROUGH)
                            .build());
            assertThat(readAllViaStream(fs, path)).isEqualTo(payload);
            assertThat(countFiles(cacheDir)).isGreaterThan(0);
            assertThat(readAllViaStream(fs, path)).isEqualTo(payload);
            try (FileReader r = fs.openFile(path)) {
                assertThat(r.readAt(PAGE_SIZE, 4096))
                        .isEqualTo(java.util.Arrays.copyOfRange(payload, PAGE_SIZE, PAGE_SIZE + 4096));
            }
        }

        try (Goosefs fs = Goosefs.connect(off)) {
            assertThat(readAllViaStream(fs, path)).isEqualTo(payload);
            assertThat(fs.readFile(path)).isEqualTo(payload);
        }
    }

    @Test
    void readFileBypassesPageCache(@TempDir Path cacheDir) throws Exception {
        Config on = cacheConfig(cacheDir, true);
        try (Goosefs fs = Goosefs.connect(on)) {
            String dir = ClusterSupport.uniqueDir(fs);
            String path = dir + "/oneshot.bin";
            byte[] payload = ClusterSupport.payload("oneshot", 4 * PAGE_SIZE);
            fs.writeFile(
                    path,
                    payload,
                    com.tencent.goosefs.CreateFileOptions.builder()
                            .writeType(WriteType.THROUGH)
                            .build());
            assertThat(fs.readFile(path)).isEqualTo(payload);
            assertThat(fs.readFile(path)).isEqualTo(payload);
            assertThat(countFiles(cacheDir)).isZero();
        }
    }

    private static Config cacheConfig(Path cacheDir, boolean enabled) {
        Map<String, String> props = new HashMap<>();
        props.put("goosefs.user.file.replication.durable", "1");
        props.put("goosefs.user.file.replication.durable.min", "1");
        props.put("goosefs.user.client.cache.enabled", enabled ? "true" : "false");
        props.put("goosefs.user.client.cache.page.size", String.valueOf(PAGE_SIZE));
        props.put("goosefs.user.client.cache.size", "64MB");
        props.put("goosefs.user.client.cache.dirs", cacheDir.toAbsolutePath().toString());
        props.put("goosefs.user.client.cache.eviction.policy", "LRU");
        props.put("goosefs.user.client.cache.async.write.enabled", "false");
        props.put("goosefs.user.client.cache.sequential.read.enabled", "true");
        return new Config(System.getenv("GOOSEFS_MASTER_ADDR"), props);
    }

    private static byte[] readAllViaStream(Goosefs fs, String path) {
        byte[] out = new byte[0];
        try (FileReader r = fs.openFile(path)) {
            while (true) {
                byte[] chunk = r.read(PAGE_SIZE);
                if (chunk.length == 0) {
                    break;
                }
                byte[] next = new byte[out.length + chunk.length];
                System.arraycopy(out, 0, next, 0, out.length);
                System.arraycopy(chunk, 0, next, out.length, chunk.length);
                out = next;
            }
        }
        return out;
    }

    private static long countFiles(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).count();
        }
    }
}
