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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** Immutable metadata snapshot copied out of Rust at the JNI boundary. */
public final class URIStatus {
    private final long fileId;
    private final String name;
    private final String path;
    private final String ufsPath;
    private final long length;
    private final long blockSizeBytes;
    private final long[] blockIds;
    private final long creationTimeMs;
    private final long lastModificationTimeMs;
    private final long lastAccessTimeMs;
    private final boolean completed;
    private final boolean folder;
    private final boolean cacheable;
    private final boolean persisted;
    private final boolean mountPoint;
    private final int inGooseFsPercentage;
    private final int inMemoryPercentage;
    private final String owner;
    private final String group;
    private final int mode;
    private final String persistenceState;
    private final long mountId;
    private final String ufsFingerprint;
    private final Map<String, byte[]> xattr;
    private final String symlink;

    URIStatus(
            long fileId,
            String name,
            String path,
            String ufsPath,
            long length,
            long blockSizeBytes,
            long[] blockIds,
            long creationTimeMs,
            long lastModificationTimeMs,
            long lastAccessTimeMs,
            boolean completed,
            boolean folder,
            boolean cacheable,
            boolean persisted,
            boolean mountPoint,
            int inGooseFsPercentage,
            int inMemoryPercentage,
            String owner,
            String group,
            int mode,
            String persistenceState,
            long mountId,
            String ufsFingerprint,
            Map<String, byte[]> xattr,
            String symlink) {
        this.fileId = fileId;
        this.name = name;
        this.path = path;
        this.ufsPath = ufsPath;
        this.length = length;
        this.blockSizeBytes = blockSizeBytes;
        this.blockIds = blockIds == null ? new long[0] : blockIds.clone();
        this.creationTimeMs = creationTimeMs;
        this.lastModificationTimeMs = lastModificationTimeMs;
        this.lastAccessTimeMs = lastAccessTimeMs;
        this.completed = completed;
        this.folder = folder;
        this.cacheable = cacheable;
        this.persisted = persisted;
        this.mountPoint = mountPoint;
        this.inGooseFsPercentage = inGooseFsPercentage;
        this.inMemoryPercentage = inMemoryPercentage;
        this.owner = owner;
        this.group = group;
        this.mode = mode;
        this.persistenceState = persistenceState;
        this.mountId = mountId;
        this.ufsFingerprint = ufsFingerprint;
        this.xattr = xattr == null ? Map.of() : Map.copyOf(xattr);
        this.symlink = symlink;
    }

    public long getFileId() {
        return fileId;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public String getUfsPath() {
        return ufsPath;
    }

    public long getLength() {
        return length;
    }

    public long getBlockSizeBytes() {
        return blockSizeBytes;
    }

    public long[] getBlockIds() {
        return blockIds.clone();
    }

    public long getCreationTimeMs() {
        return creationTimeMs;
    }

    public long getLastModificationTimeMs() {
        return lastModificationTimeMs;
    }

    public long getLastAccessTimeMs() {
        return lastAccessTimeMs;
    }

    public boolean isCompleted() {
        return completed;
    }

    public boolean isFolder() {
        return folder;
    }

    public boolean isCacheable() {
        return cacheable;
    }

    public boolean isPersisted() {
        return persisted;
    }

    public boolean isMountPoint() {
        return mountPoint;
    }

    public int getInGooseFsPercentage() {
        return inGooseFsPercentage;
    }

    public int getInMemoryPercentage() {
        return inMemoryPercentage;
    }

    public String getOwner() {
        return owner;
    }

    public String getGroup() {
        return group;
    }

    public int getMode() {
        return mode;
    }

    public String getPersistenceState() {
        return persistenceState;
    }

    public long getMountId() {
        return mountId;
    }

    public String getUfsFingerprint() {
        return ufsFingerprint;
    }

    public Map<String, byte[]> getXattr() {
        return Collections.unmodifiableMap(xattr);
    }

    public String getSymlink() {
        return symlink;
    }

    public boolean isReadable() {
        return folder || completed;
    }

    public int blockCount() {
        return blockIds.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof URIStatus)) {
            return false;
        }
        URIStatus that = (URIStatus) o;
        return lastModificationTimeMs == that.lastModificationTimeMs && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, lastModificationTimeMs);
    }

    @Override
    public String toString() {
        return "URIStatus{path=" + path + ", length=" + length + ", folder=" + folder + ", completed=" + completed
                + ", owner=" + owner + ", blockIds=" + Arrays.toString(blockIds) + "}";
    }
}
