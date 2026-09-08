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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigTest {
    @Test
    void singleMasterAddr() {
        assumeTrue(System.getenv("GOOSEFS_MASTER_ADDR") == null);
        Config cfg = new Config("127.0.0.1:9200");
        assertThat(cfg.getMasterAddr()).isEqualTo("127.0.0.1:9200");
        assertThat(cfg.getMasterAddrs()).containsExactly("127.0.0.1:9200");
        assertThat(cfg.getRoot()).isEmpty();
    }

    @Test
    void commaSeparatedHaList() {
        assumeTrue(System.getenv("GOOSEFS_MASTER_ADDR") == null);
        Config cfg = new Config("m1:9200,m2:9200,m3:9200");
        assertThat(cfg.getMasterAddr()).isEqualTo("m1:9200");
        assertThat(cfg.getMasterAddrs()).containsExactly("m1:9200", "m2:9200", "m3:9200");
    }

    @Test
    void uriFormSetsRoot() {
        assumeTrue(System.getenv("GOOSEFS_MASTER_ADDR") == null);
        Config cfg = Config.fromUri("gfs://10.0.0.1:9200/data");
        assertThat(cfg.getMasterAddr()).isEqualTo("10.0.0.1:9200");
        assertThat(cfg.getRoot()).isEqualTo("/data");
    }

    @Test
    void emptyMasterAddrRejected() {
        assertThatThrownBy(() -> new Config("  , ,"))
                .isInstanceOf(GoosefsException.class)
                .extracting(ex -> ((GoosefsException) ex).getCode())
                .isEqualTo(GoosefsException.Code.ConfigError);
    }

    @Test
    void propertiesOverlayBlockSize() {
        Config cfg = new Config("127.0.0.1:9200", Map.of("goosefs.user.block.size.bytes.default", "8MB"));
        assertThat(cfg.getBlockSize()).isEqualTo(8L * 1024 * 1024);
    }

    @Test
    void exceptionCodesRoundTrip() {
        GoosefsException ex = new GoosefsException("NotFound", "missing");
        assertThat(ex.getCode()).isEqualTo(GoosefsException.Code.NotFound);
        assertThat(ex.getMessage()).isEqualTo("missing");
    }
}
