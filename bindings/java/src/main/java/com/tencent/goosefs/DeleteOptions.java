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
 * Options for {@code delete}. Defaults match Java {@code deleteDefaults}: non-recursive,
 * {@code unchecked=true}, propagate to UFS.
 */
public final class DeleteOptions {
    private final boolean recursive;
    private final boolean unchecked;
    private final boolean goosefsOnly;

    private DeleteOptions(boolean recursive, boolean unchecked, boolean goosefsOnly) {
        this.recursive = recursive;
        this.unchecked = unchecked;
        this.goosefsOnly = goosefsOnly;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DeleteOptions defaults() {
        return builder().build();
    }

    public boolean isRecursive() {
        return recursive;
    }

    public boolean isUnchecked() {
        return unchecked;
    }

    public boolean isGoosefsOnly() {
        return goosefsOnly;
    }

    public static final class Builder {
        private boolean recursive;
        private boolean unchecked = true;
        private boolean goosefsOnly;

        public Builder recursive(boolean recursive) {
            this.recursive = recursive;
            return this;
        }

        public Builder unchecked(boolean unchecked) {
            this.unchecked = unchecked;
            return this;
        }

        public Builder goosefsOnly(boolean goosefsOnly) {
            this.goosefsOnly = goosefsOnly;
            return this;
        }

        public DeleteOptions build() {
            return new DeleteOptions(recursive, unchecked, goosefsOnly);
        }
    }
}
