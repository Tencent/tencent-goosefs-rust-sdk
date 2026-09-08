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

import com.tencent.goosefs.Config;
import com.tencent.goosefs.DeleteOptions;
import com.tencent.goosefs.Goosefs;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

final class ClusterSupport {
    private ClusterSupport() {}

    static Config config() {
        String addr = System.getenv("GOOSEFS_MASTER_ADDR");
        return new Config(
                addr,
                Map.of(
                        "goosefs.user.file.replication.durable", "1",
                        "goosefs.user.file.replication.durable.min", "1"));
    }

    static String uniqueDir(Goosefs fs) {
        String base = "/tmp/javagoosefs-tests";
        try {
            fs.mkdir(base, true);
        } catch (RuntimeException ignored) {
            // Already exists from a previous run.
        }
        String path = base + "/" + System.currentTimeMillis() + "-"
                + UUID.randomUUID().toString().substring(0, 8);
        fs.mkdir(path, true);
        return path;
    }

    static void deleteQuietly(Goosefs fs, String path) {
        try {
            fs.delete(path, DeleteOptions.builder().recursive(true).build());
        } catch (RuntimeException ignored) {
            // Test already deleted the tree, or the cluster went away.
        }
    }

    static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    static byte[] payload(String seed, int size) {
        int state = Math.abs(seed.hashCode());
        byte[] out = new byte[size];
        for (int i = 0; i < size; i++) {
            state = (state * 1103515245 + 12345) & 0x7fffffff;
            out[i] = (byte) (state & 0xff);
        }
        return out;
    }
}
