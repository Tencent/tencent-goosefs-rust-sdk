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

import org.junit.jupiter.api.Test;

class OptionsTest {
    @Test
    void writeTypeProtoValues() {
        assertThat(WriteType.MUST_CACHE.getProto()).isEqualTo(1);
        assertThat(WriteType.TRY_CACHE.getProto()).isEqualTo(2);
        assertThat(WriteType.CACHE_THROUGH.getProto()).isEqualTo(3);
        assertThat(WriteType.THROUGH.getProto()).isEqualTo(4);
        assertThat(WriteType.ASYNC_THROUGH.getProto()).isEqualTo(5);
    }

    @Test
    void readTypeProtoValues() {
        assertThat(ReadType.NO_CACHE.getProto()).isEqualTo(1);
        assertThat(ReadType.CACHE.getProto()).isEqualTo(2);
    }

    @Test
    void createFileOptionsDefaultsInheritWriteType() {
        CreateFileOptions opts = CreateFileOptions.builder().build();
        assertThat(opts.getWriteType()).isNull();
        assertThat(opts.isRecursive()).isFalse();
        assertThat(opts.getBlockSizeBytes()).isNull();
        assertThat(opts.getReplicationMax()).isNull();
    }

    @Test
    void deleteOptionsDefaults() {
        DeleteOptions opts = DeleteOptions.defaults();
        assertThat(opts.isRecursive()).isFalse();
        assertThat(opts.isUnchecked()).isTrue();
        assertThat(opts.isGoosefsOnly()).isFalse();
    }

    @Test
    void openFileOptionsDefaultIsCache() {
        assertThat(OpenFileOptions.builder().build().getReadType()).isEqualTo(ReadType.CACHE);
    }
}
