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
package com.tencent.goosefs.examples;

import com.tencent.goosefs.Config;
import com.tencent.goosefs.FileReader;
import com.tencent.goosefs.FileWriter;
import com.tencent.goosefs.Goosefs;
import com.tencent.goosefs.Tracing;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Guide example. Skipped unless {@code GOOSEFS_MASTER_ADDR} is set. */
@EnabledIfEnvironmentVariable(named = "GOOSEFS_MASTER_ADDR", matches = ".+")
class Main {
    @Test
    void guide() {
        Tracing.enable();
        String addr = System.getenv("GOOSEFS_MASTER_ADDR");
        try (Goosefs fs = Goosefs.connect(new Config(addr))) {
            String dir = "/tmp/javagoosefs-example-" + UUID.randomUUID();
            fs.mkdir(dir, true);
            String path = dir + "/hello.txt";
            byte[] payload = "hello from java".getBytes(StandardCharsets.UTF_8);
            try (FileWriter w = fs.createFile(path)) {
                w.write(payload);
                w.commit();
            }
            try (FileReader r = fs.openFile(path)) {
                if (r.read().length != payload.length) {
                    throw new AssertionError("round-trip length mismatch");
                }
            }
            fs.delete(
                    dir,
                    com.tencent.goosefs.DeleteOptions.builder().recursive(true).build());
        }
    }
}
