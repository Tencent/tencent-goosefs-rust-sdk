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
 * Process-wide tracing. Off by default. {@code RUST_LOG} overrides {@code level}. Second call is a
 * no-op. Unknown level/target → {@link IllegalArgumentException}.
 */
public final class Tracing {
    static {
        NativeLibrary.loadLibrary();
    }

    private Tracing() {}

    public static void enable() {
        enable("info", "stderr");
    }

    public static void enable(String level) {
        enable(level, "stderr");
    }

    public static void enable(String level, String target) {
        if (level == null || target == null) {
            throw new IllegalArgumentException("level and target must be non-null");
        }
        nativeEnable(level, target);
    }

    private static native void nativeEnable(String level, String target);
}
