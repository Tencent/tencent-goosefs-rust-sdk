---
slug: /
sidebar_position: 1
title: Introduction
---

# Introduction

[GooseFS](https://cloud.tencent.com/document/product/1424) is a high-performance distributed caching file system built on top of COS (Cloud Object Storage). It accelerates data access for big data and AI/ML workloads with a unified namespace and an intelligent caching layer between compute engines and cloud storage.

This site documents the **GooseFS client libraries** for Rust, Python, and Java, developed in the [tencent-goosefs-rust-sdk](https://github.com/Tencent/tencent-goosefs-rust-sdk) repository. These clients talk directly to GooseFS Master/Worker over gRPC and let you:

- **Manage** files and directories (create, list, rename, delete, get status)
- **Read and write** data with high-level streaming APIs
- **Accelerate** hot reads with client local page cache and metadata cache
- **Observe** client behavior via Master heartbeat and Prometheus Pushgateway metrics

## Architecture

The Rust crate (`goosefs-sdk`) is Layer 3 in the **Lance → OpenDAL → GooseFS** stack:

```text
┌────────────────────────────────────────────────────────────────┐
│  Layer 1 — Lance Provider (lance-io / ObjectStore)             │
├────────────────────────────────────────────────────────────────┤
│  Layer 2 — OpenDAL GooseFS Service (opendal::services)         │
├────────────────────────────────────────────────────────────────┤
│  Layer 3 — GooseFS Rust gRPC Client  ← this project            │
│                                                                │
│  FileSystem / BaseFileSystem / FileSystemContext               │
│  GoosefsFileInStream / GoosefsFileWriter / GoosefsFileReader   │
│  MasterClient / WorkerClient / WorkerRouter                    │
│  Page cache · Metadata cache · Metrics                         │
└────────────────────────────────────────────────────────────────┘
```

## Client Overview

|                   | Rust                                                               | Python                                                 | Java                                                          |
| ----------------- | ------------------------------------------------------------------ | ------------------------------------------------------ | ------------------------------------------------------------- |
| **Package**       | [`goosefs-sdk`](https://crates.io/crates/goosefs-sdk) on crates.io | [`goosefs`](https://pypi.org/project/goosefs/) on PyPI | `com.tencent.goosefs:goosefs` (classified JARs; publish TBD) |
| **Async runtime** | Tokio                                                              | Sync (`Goosefs`) + Async (`AsyncGoosefs`)              | Blocking (`Goosefs`) + `CompletableFuture` (`AsyncGoosefs`)   |
| **API style**     | `FileSystem` trait + high-level I/O helpers                        | Blocking + coroutine APIs over the Rust SDK            | JNI over the same Rust SDK                                    |
| **Bridge**        | —                                                                  | PyO3 (abi3, CPython 3.9+)                              | JNI (`jni` 0.22.4), JDK 11+                                   |
| **Status**        | Experimental (v0.2.x)                                              | Alpha (tracks Rust SDK version)                        | Alpha (tracks Rust SDK version)                               |

## Prerequisites

You need a running GooseFS cluster (Master RPC default `9200`, Worker data port default `9203`).

For local development, this repository ships a Docker fixture:

```bash
bash scripts/ci/goosefs-up.sh
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
export GOOSEFS_AUTH_TYPE=simple
```

## How This Guide Is Organised

- **Rust** — installation, FileSystem API, configuration, page cache, metadata cache, metrics, and examples
- **Python** — installation, sync/async quickstart, batch APIs, caching, and binding examples
- **Java** — installation (dual JAR), quickstart, filesystem, streaming (`commit()` vs `close()`), errors, configuration
- **Contributing** — build, test, and PR conventions
- **Release** — publishing `goosefs-sdk`, the Python wheel, and the Java classified-JAR dry-run
