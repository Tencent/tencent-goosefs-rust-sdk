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

import java.util.Objects;

/** Options for opening a file for reading. {@code openFile} honours {@link ReadType}. */
public final class OpenFileOptions {
    private final ReadType readType;

    private OpenFileOptions(ReadType readType) {
        this.readType = Objects.requireNonNull(readType, "readType");
    }

    public static Builder builder() {
        return new Builder();
    }

    public ReadType getReadType() {
        return readType;
    }

    public static final class Builder {
        private ReadType readType = ReadType.CACHE;

        public Builder readType(ReadType readType) {
            this.readType = Objects.requireNonNull(readType, "readType");
            return this;
        }

        public OpenFileOptions build() {
            return new OpenFileOptions(readType);
        }
    }
}
