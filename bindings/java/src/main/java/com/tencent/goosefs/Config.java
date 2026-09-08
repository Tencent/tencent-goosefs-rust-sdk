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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable client configuration. Parsed natively so {@code GOOSEFS_*} env vars
 * apply (same precedence as the Python binding). Not a {@link NativeObject}.
 */
public final class Config {
    static {
        NativeLibrary.loadLibrary();
    }

    private final String seedMasterAddr;
    private final Map<String, String> seedProperties;
    private final String seedFileContent;

    private final String masterAddr;
    private final List<String> masterAddrs;
    private final long blockSize;
    private final long chunkSize;
    private final String root;
    private final boolean vpcMappingEnabled;
    private final String authType;
    private final String authUsername;
    private final boolean metricsEnabled;
    private final long connectTimeoutMs;
    private final long requestTimeoutMs;
    private final Integer writeType;
    private final int fileReplicationNumber;
    private final int fileReplicationDurable;
    private final int fileReplicationDurableMin;

    public Config(String masterAddr) {
        this(masterAddr, Map.of());
    }

    public Config(String masterAddr, Map<String, String> properties) {
        this(
                nativeResolve(Objects.requireNonNull(masterAddr, "masterAddr"), copy(properties), null),
                masterAddr,
                copy(properties),
                null);
    }

    public static Config fromPropertiesFile(String path) {
        final String content;
        try {
            content = Files.readString(Path.of(path));
        } catch (IOException e) {
            throw new GoosefsException(
                    GoosefsException.Code.ConfigError, "failed to read config file '" + path + "': " + e.getMessage());
        }
        return new Config(nativeResolve(null, Map.of(), content), null, Map.of(), content);
    }

    public static Config fromUri(String uri) {
        return fromUri(uri, Map.of());
    }

    public static Config fromUri(String uri, Map<String, String> properties) {
        return new Config(
                nativeResolve(Objects.requireNonNull(uri, "uri"), copy(properties), null), uri, copy(properties), null);
    }

    private Config(
            Resolved resolved, String seedMasterAddr, Map<String, String> seedProperties, String seedFileContent) {
        this.seedMasterAddr = seedMasterAddr;
        this.seedProperties = seedProperties;
        this.seedFileContent = seedFileContent;
        this.masterAddr = resolved.masterAddr;
        this.masterAddrs = List.copyOf(resolved.masterAddrs);
        this.blockSize = resolved.blockSize;
        this.chunkSize = resolved.chunkSize;
        this.root = resolved.root;
        this.vpcMappingEnabled = resolved.vpcMappingEnabled;
        this.authType = resolved.authType;
        this.authUsername = resolved.authUsername;
        this.metricsEnabled = resolved.metricsEnabled;
        this.connectTimeoutMs = resolved.connectTimeoutMs;
        this.requestTimeoutMs = resolved.requestTimeoutMs;
        this.writeType = resolved.writeType;
        this.fileReplicationNumber = resolved.fileReplicationNumber;
        this.fileReplicationDurable = resolved.fileReplicationDurable;
        this.fileReplicationDurableMin = resolved.fileReplicationDurableMin;
    }

    String seedMasterAddr() {
        return seedMasterAddr;
    }

    Map<String, String> seedProperties() {
        return seedProperties;
    }

    String seedFileContent() {
        return seedFileContent;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public List<String> getMasterAddrs() {
        return masterAddrs;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public String getRoot() {
        return root;
    }

    public boolean isVpcMappingEnabled() {
        return vpcMappingEnabled;
    }

    public String getAuthType() {
        return authType;
    }

    public String getAuthUsername() {
        return authUsername;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public long getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public Integer getWriteType() {
        return writeType;
    }

    public int getFileReplicationNumber() {
        return fileReplicationNumber;
    }

    public int getFileReplicationDurable() {
        return fileReplicationDurable;
    }

    public int getFileReplicationDurableMin() {
        return fileReplicationDurableMin;
    }

    private static Map<String, String> copy(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(Map.copyOf(properties));
    }

    private static native Resolved nativeResolve(String masterAddr, Map<String, String> properties, String fileContent);

    static final class Resolved {
        final String masterAddr;
        final List<String> masterAddrs;
        final long blockSize;
        final long chunkSize;
        final String root;
        final boolean vpcMappingEnabled;
        final String authType;
        final String authUsername;
        final boolean metricsEnabled;
        final long connectTimeoutMs;
        final long requestTimeoutMs;
        final Integer writeType;
        final int fileReplicationNumber;
        final int fileReplicationDurable;
        final int fileReplicationDurableMin;

        Resolved(
                String masterAddr,
                List<String> masterAddrs,
                long blockSize,
                long chunkSize,
                String root,
                boolean vpcMappingEnabled,
                String authType,
                String authUsername,
                boolean metricsEnabled,
                long connectTimeoutMs,
                long requestTimeoutMs,
                Integer writeType,
                int fileReplicationNumber,
                int fileReplicationDurable,
                int fileReplicationDurableMin) {
            this.masterAddr = masterAddr;
            this.masterAddrs = masterAddrs;
            this.blockSize = blockSize;
            this.chunkSize = chunkSize;
            this.root = root;
            this.vpcMappingEnabled = vpcMappingEnabled;
            this.authType = authType;
            this.authUsername = authUsername;
            this.metricsEnabled = metricsEnabled;
            this.connectTimeoutMs = connectTimeoutMs;
            this.requestTimeoutMs = requestTimeoutMs;
            this.writeType = writeType;
            this.fileReplicationNumber = fileReplicationNumber;
            this.fileReplicationDurable = fileReplicationDurable;
            this.fileReplicationDurableMin = fileReplicationDurableMin;
        }
    }
}
