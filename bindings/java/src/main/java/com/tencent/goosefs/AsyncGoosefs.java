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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Asynchronous GooseFS filesystem client. */
public class AsyncGoosefs extends NativeObject {
    public static final int MAX_BATCH_RPC_IN_FLIGHT = 64;

    private final long executorHandle;

    AsyncGoosefs(long nativeHandle, long executorHandle) {
        super(nativeHandle);
        this.executorHandle = executorHandle;
    }

    public static CompletableFuture<AsyncGoosefs> connect(Config config) {
        return connect(config, null);
    }

    public static CompletableFuture<AsyncGoosefs> connect(Config config, AsyncExecutor executor) {
        Objects.requireNonNull(config, "config");
        final long executorHandle = executor != null ? executor.nativeHandle : 0L;
        final long requestId = nativeConnect(config, executorHandle);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<URIStatus> getStatus(String path) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        final long requestId = nativeGetStatus(nativeHandle, executorHandle, path);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Boolean> exists(String path) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return AsyncRegistry.take(nativeExists(nativeHandle, executorHandle, path));
    }

    public CompletableFuture<List<URIStatus>> listStatus(String path) {
        return listStatus(path, false);
    }

    public CompletableFuture<List<URIStatus>> listStatus(String path, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return AsyncRegistry.take(nativeListStatus(nativeHandle, executorHandle, path, recursive));
    }

    public CompletableFuture<URIStatusList> listStatusGrouped(String path) {
        return listStatusGrouped(path, false);
    }

    public CompletableFuture<URIStatusList> listStatusGrouped(String path, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return AsyncRegistry.take(nativeListStatusGrouped(nativeHandle, executorHandle, path, recursive));
    }

    public CompletableFuture<Void> mkdir(String path) {
        return mkdir(path, false);
    }

    public CompletableFuture<Void> mkdir(String path, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return AsyncRegistry.take(nativeMkdir(nativeHandle, executorHandle, path, recursive));
    }

    public CompletableFuture<Void> delete(String path) {
        return delete(path, DeleteOptions.defaults());
    }

    public CompletableFuture<Void> delete(String path, DeleteOptions options) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(options, "options");
        return AsyncRegistry.take(nativeDelete(nativeHandle, executorHandle, path, options));
    }

    public CompletableFuture<Void> rename(String src, String dst) {
        ensureOpen();
        Objects.requireNonNull(src, "src");
        Objects.requireNonNull(dst, "dst");
        return AsyncRegistry.take(nativeRename(nativeHandle, executorHandle, src, dst));
    }

    public CompletableFuture<byte[]> readFile(String path) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return AsyncRegistry.take(nativeReadFile(nativeHandle, executorHandle, path));
    }

    public CompletableFuture<byte[]> readRange(String path, long offset, long length) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return AsyncRegistry.take(nativeReadRange(nativeHandle, executorHandle, path, offset, length));
    }

    public CompletableFuture<Long> writeFile(String path, byte[] data) {
        return writeFile(path, data, null);
    }

    public CompletableFuture<Long> writeFile(String path, byte[] data, CreateFileOptions options) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(data, "data");
        return AsyncRegistry.take(nativeWriteFile(nativeHandle, executorHandle, path, data, options));
    }

    public CompletableFuture<AsyncFileReader> openFile(String path) {
        return openFile(path, OpenFileOptions.builder().build());
    }

    public CompletableFuture<AsyncFileReader> openFile(String path, OpenFileOptions options) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(options, "options");
        return AsyncRegistry.take(nativeOpenFile(nativeHandle, executorHandle, path, options));
    }

    public CompletableFuture<AsyncFileWriter> createFile(String path) {
        return createFile(path, null);
    }

    public CompletableFuture<AsyncFileWriter> createFile(String path, CreateFileOptions options) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return AsyncRegistry.take(nativeCreateFile(nativeHandle, executorHandle, path, options));
    }

    public CompletableFuture<byte[]> positionedRead(String path) {
        return positionedRead(path, 0, 0, -1, WorkerClient.DEFAULT_CHUNK_SIZE);
    }

    public CompletableFuture<byte[]> positionedRead(String path, int blockIndex, long offset, long length) {
        return positionedRead(path, blockIndex, offset, length, WorkerClient.DEFAULT_CHUNK_SIZE);
    }

    public CompletableFuture<byte[]> positionedRead(
            String path, int blockIndex, long offset, long length, long chunkSize) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return AsyncRegistry.take(
                nativePositionedRead(nativeHandle, executorHandle, path, blockIndex, offset, length, chunkSize));
    }

    public CompletableFuture<AsyncWorkerClient> acquireWorkerForBlock(long blockId) {
        return acquireWorkerForBlock(blockId, null);
    }

    public CompletableFuture<AsyncWorkerClient> acquireWorkerForBlock(long blockId, String path) {
        ensureOpen();
        return AsyncRegistry.take(nativeAcquireWorkerForBlock(nativeHandle, executorHandle, blockId, path));
    }

    public CompletableFuture<List<URIStatus>> batchGetStatus(List<String> paths) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return AsyncRegistry.take(nativeBatchGetStatus(nativeHandle, executorHandle, paths));
    }

    public CompletableFuture<List<Boolean>> batchExists(List<String> paths) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return AsyncRegistry.take(nativeBatchExists(nativeHandle, executorHandle, paths));
    }

    public CompletableFuture<List<AsyncFileReader>> batchOpenFile(List<String> paths) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return AsyncRegistry.take(nativeBatchOpenFile(nativeHandle, executorHandle, paths));
    }

    public CompletableFuture<List<Long>> batchCreateFile(List<String> paths) {
        return batchCreateFile(paths, null);
    }

    public CompletableFuture<List<Long>> batchCreateFile(List<String> paths, CreateFileOptions options) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return AsyncRegistry.take(nativeBatchCreateFile(nativeHandle, executorHandle, paths, options));
    }

    public CompletableFuture<Void> batchCreateDir(List<String> paths) {
        return batchCreateDir(paths, false);
    }

    public CompletableFuture<Void> batchCreateDir(List<String> paths, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return AsyncRegistry.take(nativeBatchCreateDir(nativeHandle, executorHandle, paths, recursive));
    }

    public CompletableFuture<Void> batchRename(List<String> pairs) {
        ensureOpen();
        Objects.requireNonNull(pairs, "pairs");
        return AsyncRegistry.take(nativeBatchRename(nativeHandle, executorHandle, pairs));
    }

    public CompletableFuture<Void> batchDelete(List<String> paths) {
        return batchDelete(paths, DeleteOptions.defaults());
    }

    public CompletableFuture<Void> batchDelete(List<String> paths, DeleteOptions options) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        Objects.requireNonNull(options, "options");
        return AsyncRegistry.take(nativeBatchDelete(nativeHandle, executorHandle, paths, options));
    }

    public CompletableFuture<List<List<URIStatus>>> batchListStatus(List<String> paths) {
        return batchListStatus(paths, false);
    }

    public CompletableFuture<List<List<URIStatus>>> batchListStatus(List<String> paths, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return AsyncRegistry.take(nativeBatchListStatus(nativeHandle, executorHandle, paths, recursive));
    }

    public CompletableFuture<List<URIStatusList>> batchListStatusGrouped(List<String> paths) {
        return batchListStatusGrouped(paths, false);
    }

    public CompletableFuture<List<URIStatusList>> batchListStatusGrouped(List<String> paths, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return AsyncRegistry.take(nativeBatchListStatusGrouped(nativeHandle, executorHandle, paths, recursive));
    }

    /**
     * Graceful close. Async call sites should prefer this over {@link #close()}.
     */
    public CompletableFuture<Void> closeAsync() {
        if (isDisposed()) {
            return CompletableFuture.completedFuture(null);
        }
        final long requestId = nativeCloseAsync(nativeHandle, executorHandle);
        return AsyncRegistry.<Void>take(requestId).whenComplete((v, t) -> super.close());
    }

    /**
     * {@code closeAsync().join()} with a Tokio-worker deadlock guard. Safe from
     * the caller thread of {@code connect().join()} (not a Tokio worker).
     */
    @Override
    public void close() {
        nativeThrowIfOnTokio();
        try {
            closeAsync().join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long nativeConnect(Config config, long executorHandle);

    private static native long nativeGetStatus(long nativeHandle, long executorHandle, String path);

    private static native long nativeExists(long nativeHandle, long executorHandle, String path);

    private static native long nativeListStatus(long nativeHandle, long executorHandle, String path, boolean recursive);

    private static native long nativeListStatusGrouped(
            long nativeHandle, long executorHandle, String path, boolean recursive);

    private static native long nativeMkdir(long nativeHandle, long executorHandle, String path, boolean recursive);

    private static native long nativeDelete(long nativeHandle, long executorHandle, String path, DeleteOptions options);

    private static native long nativeRename(long nativeHandle, long executorHandle, String src, String dst);

    private static native long nativeReadFile(long nativeHandle, long executorHandle, String path);

    private static native long nativeReadRange(
            long nativeHandle, long executorHandle, String path, long offset, long length);

    private static native long nativeWriteFile(
            long nativeHandle, long executorHandle, String path, byte[] data, CreateFileOptions options);

    private static native long nativeOpenFile(
            long nativeHandle, long executorHandle, String path, OpenFileOptions options);

    private static native long nativeCreateFile(
            long nativeHandle, long executorHandle, String path, CreateFileOptions options);

    private static native long nativePositionedRead(
            long nativeHandle,
            long executorHandle,
            String path,
            int blockIndex,
            long offset,
            long length,
            long chunkSize);

    private static native long nativeAcquireWorkerForBlock(
            long nativeHandle, long executorHandle, long blockId, String path);

    private static native long nativeBatchGetStatus(long nativeHandle, long executorHandle, List<String> paths);

    private static native long nativeBatchExists(long nativeHandle, long executorHandle, List<String> paths);

    private static native long nativeBatchOpenFile(long nativeHandle, long executorHandle, List<String> paths);

    private static native long nativeBatchCreateFile(
            long nativeHandle, long executorHandle, List<String> paths, CreateFileOptions options);

    private static native long nativeBatchCreateDir(
            long nativeHandle, long executorHandle, List<String> paths, boolean recursive);

    private static native long nativeBatchRename(long nativeHandle, long executorHandle, List<String> pairs);

    private static native long nativeBatchDelete(
            long nativeHandle, long executorHandle, List<String> paths, DeleteOptions options);

    private static native long nativeBatchListStatus(
            long nativeHandle, long executorHandle, List<String> paths, boolean recursive);

    private static native long nativeBatchListStatusGrouped(
            long nativeHandle, long executorHandle, List<String> paths, boolean recursive);

    private static native long nativeCloseAsync(long nativeHandle, long executorHandle);

    private static native void nativeThrowIfOnTokio();
}
