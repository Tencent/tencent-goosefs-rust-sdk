---
slug: /
sidebar_position: 1
title: Introduction
---

# Introduction

[GooseFS](https://cloud.tencent.com/document/product/1424) is a high-performance distributed caching file system built on top of COS (Cloud Object Storage). It accelerates data access for big data and AI/ML workloads with a unified namespace and an intelligent caching layer between compute engines and cloud storage.

This site documents the **GooseFS client libraries** for Rust and Python, developed in the [tencent-goosefs-rust-sdk](https://github.com/Tencent/tencent-goosefs-rust-sdk) repository. These clients talk directly to GooseFS Master/Worker over gRPC and let you:

- **Manage** files and directories (create, list, rename, delete, get status)
- **Read and write** data with high-level streaming APIs
- **Accelerate** hot reads with client local page cache and short-circuit mmap
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
│  Page cache · Short-circuit · Metrics                          │
└────────────────────────────────────────────────────────────────┘
```

## Client Overview

|                   | Rust                                                               | Python                                                 |
| ----------------- | ------------------------------------------------------------------ | ------------------------------------------------------ |
| **Package**       | [`goosefs-sdk`](https://crates.io/crates/goosefs-sdk) on crates.io | [`goosefs`](https://pypi.org/project/goosefs/) on PyPI |
| **Async runtime** | Tokio                                                              | Sync (`Goosefs`) + Async (`AsyncGoosefs`)              |
| **API style**     | `FileSystem` trait + high-level I/O helpers                        | Blocking + coroutine APIs over the Rust SDK            |
| **Python bridge** | —                                                                  | PyO3 (abi3, CPython 3.9+)                              |
| **Status**        | Experimental (v0.1.x)                                              | Alpha (tracks Rust SDK version)                        |

## Prerequisites

You need a running GooseFS cluster (Master RPC default `9200`, Worker data port default `9203`).

For local development, this repository ships a Docker fixture:

```bash
bash scripts/ci/goosefs-up.sh
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
export GOOSEFS_AUTH_TYPE=simple
```

## How This Guide Is Organised

- **Rust** — installation, FileSystem API, configuration, page cache, short-circuit, metrics, and examples
- **Python** — installation, sync/async quickstart, and binding examples
- **Contributing** — build, test, and PR conventions
- **Release** — publishing `goosefs-sdk` and the Python wheel
