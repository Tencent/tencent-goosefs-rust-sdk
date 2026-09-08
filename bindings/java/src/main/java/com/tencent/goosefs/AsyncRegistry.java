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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** JNI {@code CompletableFuture} registry (OpenDAL pattern; no global refs). */
final class AsyncRegistry {
    private static final AsyncRegistry INSTANCE = new AsyncRegistry();

    private final Map<Long, CompletableFuture<?>> registry = new ConcurrentHashMap<>();

    private AsyncRegistry() {}

    @SuppressWarnings("unused")
    static long requestId() {
        final CompletableFuture<?> f = new CompletableFuture<>();
        while (true) {
            final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
            if (requestId != 0 && INSTANCE.registry.putIfAbsent(requestId, f) == null) {
                return requestId;
            }
        }
    }

    static CompletableFuture<?> get(long requestId) {
        return INSTANCE.registry.get(requestId);
    }

    @SuppressWarnings("unchecked")
    static <T> CompletableFuture<T> take(long requestId) {
        final CompletableFuture<?> f = get(requestId);
        if (f != null) {
            f.whenComplete((r, e) -> INSTANCE.registry.remove(requestId));
        }
        return (CompletableFuture<T>) f;
    }
}
