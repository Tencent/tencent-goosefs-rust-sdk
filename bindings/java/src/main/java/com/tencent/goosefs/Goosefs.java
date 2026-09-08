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

/** Synchronous GooseFS filesystem client ({@code block_on} on the shared runtime). */
public class Goosefs extends NativeObject {
    public static final int MAX_BATCH_RPC_IN_FLIGHT = AsyncGoosefs.MAX_BATCH_RPC_IN_FLIGHT;

    private final long executorHandle;

    Goosefs(long nativeHandle, long executorHandle) {
        super(nativeHandle);
        this.executorHandle = executorHandle;
    }

    public static Goosefs connect(Config config) {
        return connect(config, null);
    }

    public static Goosefs connect(Config config, AsyncExecutor executor) {
        Objects.requireNonNull(config, "config");
        final long executorHandle = executor != null ? executor.nativeHandle : 0L;
        final long handle = nativeConnect(config, executorHandle);
        return new Goosefs(handle, executorHandle);
    }

    public URIStatus getStatus(String path) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return nativeGetStatus(nativeHandle, executorHandle, path);
    }

    public boolean exists(String path) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return nativeExists(nativeHandle, executorHandle, path);
    }

    public List<URIStatus> listStatus(String path) {
        return listStatus(path, false);
    }

    public List<URIStatus> listStatus(String path, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return nativeListStatus(nativeHandle, executorHandle, path, recursive);
    }

    public URIStatusList listStatusGrouped(String path) {
        return listStatusGrouped(path, false);
    }

    public URIStatusList listStatusGrouped(String path, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return nativeListStatusGrouped(nativeHandle, executorHandle, path, recursive);
    }

    public void mkdir(String path) {
        mkdir(path, false);
    }

    public void mkdir(String path, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        nativeMkdir(nativeHandle, executorHandle, path, recursive);
    }

    public void delete(String path) {
        delete(path, DeleteOptions.defaults());
    }

    public void delete(String path, DeleteOptions options) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(options, "options");
        nativeDelete(nativeHandle, executorHandle, path, options);
    }

    public void rename(String src, String dst) {
        ensureOpen();
        Objects.requireNonNull(src, "src");
        Objects.requireNonNull(dst, "dst");
        nativeRename(nativeHandle, executorHandle, src, dst);
    }

    public byte[] readFile(String path) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return nativeReadFile(nativeHandle, executorHandle, path);
    }

    public byte[] readRange(String path, long offset, long length) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return nativeReadRange(nativeHandle, executorHandle, path, offset, length);
    }

    public long writeFile(String path, byte[] data) {
        return writeFile(path, data, null);
    }

    public long writeFile(String path, byte[] data, CreateFileOptions options) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(data, "data");
        return nativeWriteFile(nativeHandle, executorHandle, path, data, options);
    }

    public FileReader openFile(String path) {
        return openFile(path, OpenFileOptions.builder().build());
    }

    public FileReader openFile(String path, OpenFileOptions options) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(options, "options");
        return nativeOpenFile(nativeHandle, executorHandle, path, options);
    }

    public FileWriter createFile(String path) {
        return createFile(path, null);
    }

    public FileWriter createFile(String path, CreateFileOptions options) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return nativeCreateFile(nativeHandle, executorHandle, path, options);
    }

    public GoosefsInputStream createInputStream(String path) {
        return new GoosefsInputStream(openFile(path));
    }

    public GoosefsOutputStream createOutputStream(String path) {
        return new GoosefsOutputStream(createFile(path));
    }

    public byte[] positionedRead(String path) {
        return positionedRead(path, 0, 0, -1, WorkerClient.DEFAULT_CHUNK_SIZE);
    }

    public byte[] positionedRead(String path, int blockIndex, long offset, long length) {
        return positionedRead(path, blockIndex, offset, length, WorkerClient.DEFAULT_CHUNK_SIZE);
    }

    public byte[] positionedRead(String path, int blockIndex, long offset, long length, long chunkSize) {
        ensureOpen();
        Objects.requireNonNull(path, "path");
        return nativePositionedRead(nativeHandle, executorHandle, path, blockIndex, offset, length, chunkSize);
    }

    public WorkerClient acquireWorkerForBlock(long blockId) {
        return acquireWorkerForBlock(blockId, null);
    }

    public WorkerClient acquireWorkerForBlock(long blockId, String path) {
        ensureOpen();
        return nativeAcquireWorkerForBlock(nativeHandle, executorHandle, blockId, path);
    }

    public List<URIStatus> batchGetStatus(List<String> paths) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return nativeBatchGetStatus(nativeHandle, executorHandle, paths);
    }

    public List<Boolean> batchExists(List<String> paths) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return nativeBatchExists(nativeHandle, executorHandle, paths);
    }

    public List<FileReader> batchOpenFile(List<String> paths) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return nativeBatchOpenFile(nativeHandle, executorHandle, paths);
    }

    public List<Long> batchCreateFile(List<String> paths) {
        return batchCreateFile(paths, null);
    }

    public List<Long> batchCreateFile(List<String> paths, CreateFileOptions options) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return nativeBatchCreateFile(nativeHandle, executorHandle, paths, options);
    }

    public void batchCreateDir(List<String> paths) {
        batchCreateDir(paths, false);
    }

    public void batchCreateDir(List<String> paths, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        nativeBatchCreateDir(nativeHandle, executorHandle, paths, recursive);
    }

    public void batchRename(List<String> pairs) {
        ensureOpen();
        Objects.requireNonNull(pairs, "pairs");
        nativeBatchRename(nativeHandle, executorHandle, pairs);
    }

    public void batchDelete(List<String> paths) {
        batchDelete(paths, DeleteOptions.defaults());
    }

    public void batchDelete(List<String> paths, DeleteOptions options) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        Objects.requireNonNull(options, "options");
        nativeBatchDelete(nativeHandle, executorHandle, paths, options);
    }

    public List<List<URIStatus>> batchListStatus(List<String> paths) {
        return batchListStatus(paths, false);
    }

    public List<List<URIStatus>> batchListStatus(List<String> paths, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return nativeBatchListStatus(nativeHandle, executorHandle, paths, recursive);
    }

    public List<URIStatusList> batchListStatusGrouped(List<String> paths) {
        return batchListStatusGrouped(paths, false);
    }

    public List<URIStatusList> batchListStatusGrouped(List<String> paths, boolean recursive) {
        ensureOpen();
        Objects.requireNonNull(paths, "paths");
        return nativeBatchListStatusGrouped(nativeHandle, executorHandle, paths, recursive);
    }

    @Override
    protected void disposeInternal(long handle) {
        nativeDispose(handle, executorHandle);
    }

    private static native long nativeConnect(Config config, long executorHandle);

    private static native URIStatus nativeGetStatus(long nativeHandle, long executorHandle, String path);

    private static native boolean nativeExists(long nativeHandle, long executorHandle, String path);

    private static native List<URIStatus> nativeListStatus(
            long nativeHandle, long executorHandle, String path, boolean recursive);

    private static native URIStatusList nativeListStatusGrouped(
            long nativeHandle, long executorHandle, String path, boolean recursive);

    private static native void nativeMkdir(long nativeHandle, long executorHandle, String path, boolean recursive);

    private static native void nativeDelete(long nativeHandle, long executorHandle, String path, DeleteOptions options);

    private static native void nativeRename(long nativeHandle, long executorHandle, String src, String dst);

    private static native byte[] nativeReadFile(long nativeHandle, long executorHandle, String path);

    private static native byte[] nativeReadRange(
            long nativeHandle, long executorHandle, String path, long offset, long length);

    private static native long nativeWriteFile(
            long nativeHandle, long executorHandle, String path, byte[] data, CreateFileOptions options);

    private static native FileReader nativeOpenFile(
            long nativeHandle, long executorHandle, String path, OpenFileOptions options);

    private static native FileWriter nativeCreateFile(
            long nativeHandle, long executorHandle, String path, CreateFileOptions options);

    private static native byte[] nativePositionedRead(
            long nativeHandle,
            long executorHandle,
            String path,
            int blockIndex,
            long offset,
            long length,
            long chunkSize);

    private static native WorkerClient nativeAcquireWorkerForBlock(
            long nativeHandle, long executorHandle, long blockId, String path);

    private static native List<URIStatus> nativeBatchGetStatus(
            long nativeHandle, long executorHandle, List<String> paths);

    private static native List<Boolean> nativeBatchExists(long nativeHandle, long executorHandle, List<String> paths);

    private static native List<FileReader> nativeBatchOpenFile(
            long nativeHandle, long executorHandle, List<String> paths);

    private static native List<Long> nativeBatchCreateFile(
            long nativeHandle, long executorHandle, List<String> paths, CreateFileOptions options);

    private static native void nativeBatchCreateDir(
            long nativeHandle, long executorHandle, List<String> paths, boolean recursive);

    private static native void nativeBatchRename(long nativeHandle, long executorHandle, List<String> pairs);

    private static native void nativeBatchDelete(
            long nativeHandle, long executorHandle, List<String> paths, DeleteOptions options);

    private static native List<List<URIStatus>> nativeBatchListStatus(
            long nativeHandle, long executorHandle, List<String> paths, boolean recursive);

    private static native List<URIStatusList> nativeBatchListStatusGrouped(
            long nativeHandle, long executorHandle, List<String> paths, boolean recursive);

    private static native void nativeDispose(long nativeHandle, long executorHandle);
}
