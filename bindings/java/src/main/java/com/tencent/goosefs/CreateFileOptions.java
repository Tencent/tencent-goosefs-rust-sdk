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

/**
 * Options for creating / one-shot-writing a file.
 *
 * <p>{@code writeType == null} inherits the parent directory {@code innerWriteType} xattr.
 */
public final class CreateFileOptions {
    private final WriteType writeType;
    private final Long blockSizeBytes;
    private final Integer replicationMax;
    private final boolean recursive;

    private CreateFileOptions(WriteType writeType, Long blockSizeBytes, Integer replicationMax, boolean recursive) {
        this.writeType = writeType;
        this.blockSizeBytes = blockSizeBytes;
        this.replicationMax = replicationMax;
        this.recursive = recursive;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** {@code null} means inherit parent xattr. */
    public WriteType getWriteType() {
        return writeType;
    }

    public Long getBlockSizeBytes() {
        return blockSizeBytes;
    }

    public Integer getReplicationMax() {
        return replicationMax;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public static final class Builder {
        private WriteType writeType;
        private Long blockSizeBytes;
        private Integer replicationMax;
        private boolean recursive;

        public Builder writeType(WriteType writeType) {
            this.writeType = writeType;
            return this;
        }

        public Builder blockSizeBytes(long blockSizeBytes) {
            this.blockSizeBytes = blockSizeBytes;
            return this;
        }

        public Builder replicationMax(int replicationMax) {
            this.replicationMax = replicationMax;
            return this;
        }

        public Builder recursive(boolean recursive) {
            this.recursive = recursive;
            return this;
        }

        public CreateFileOptions build() {
            return new CreateFileOptions(writeType, blockSizeBytes, replicationMax, recursive);
        }
    }
}
