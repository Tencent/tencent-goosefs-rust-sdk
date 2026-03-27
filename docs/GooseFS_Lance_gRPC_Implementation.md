# GooseFS gRPC 集成实现方案

> **技术路线**：Lance ObjectStore → OpenDAL → GooseFS Rust Client (gRPC) → GooseFS gRPC Service  
> **版本**: v1.1 | **日期**: 2026-03-27

---

## 目录

1. [方案概述](#1-方案概述)
2. [整体架构](#2-整体架构)
3. [GooseFS gRPC 协议技术参考](#3-goosefs-grpc-协议技术参考)
4. [WriteType 完整支持设计](#4-writetype-完整支持设计) ← **v1.1 新增**
5. [详细实现设计](#5-详细实现设计)
6. [ObjectStore 操作 → gRPC 调用映射](#6-objectstore-操作--grpc-调用映射)
7. [代码变更清单与工作量](#7-代码变更清单与工作量)
8. [配置与使用指南](#8-配置与使用指南)
9. [测试计划](#9-测试计划)
10. [性能优化与风险](#10-性能优化与风险)
11. [实施路线图](#11-实施路线图)

---

## 1. 方案概述

### 1.1 背景

- **Lance**：面向 ML/AI 的列式数据格式，通过可插拔 `ObjectStoreProvider` 支持多种存储后端
- **GooseFS**：腾讯基于 Alluxio 的分布式缓存文件系统，当前（2.1.0）**不包含 Proxy 模块**，仅支持 gRPC / S3 API / FUSE 三种访问方式
- **OpenDAL**：Apache 统一数据访问层，现有 `services-alluxio` 基于 REST API 但 `read: false` 不可用

### 1.2 核心思路

在 OpenDAL 中新增 `services-goosefs` service，通过独立的 **GooseFS Rust gRPC Client** 直接对接 GooseFS Master/Worker 的 gRPC 接口。

**关键优势**：
- 不依赖 Proxy：直接走 gRPC，解决 GooseFS 无 Proxy 模块的问题
- gRPC 二进制传输，性能优于 REST API
- 符合 PR #5740 标准模式（OpenDAL + OpendalStore），与 COS provider 架构一致
- GooseFS Rust Client 作为独立 crate 可复用，可贡献回 Apache OpenDAL 社区

---

## 2. 整体架构

### 2.1 完整链路

```
Lance ObjectStore API
        │
        ▼
Lance ObjectStoreProvider (GooseFsDalStoreProvider)        ← 层级 1：Lance Provider（~100 行）
        │
        ▼
OpenDAL GooseFS Service (services-goosefs)                 ← 层级 2：OpenDAL Service（impl Access）
        │  impl opendal::Access trait
        ▼
GooseFS Rust Client (gRPC)                                 ← 层级 3：独立 crate：goosefs-client-rs
        │  tonic gRPC (protobuf)
        ▼
GooseFS gRPC Service
├── Master:9200                                            → 元数据 + 管理
│   ├── FileSystemMasterClientServiceHandler               → ★ 文件系统元数据（核心）
│   ├── WorkerManagerMasterClientServiceHandler             → Worker 管理/容量/限流
│   ├── MetaMasterClientServiceHandler                      → 集群元信息/备份
│   ├── MetricsMasterClientServiceHandler                   → 监控指标
│   ├── JournalMasterClientServiceHandler                   → Journal Raft 管理
│   ├── TableMasterClientServiceHandler                     → 表格/Lance Namespace
│   ├── JobMasterClientServiceHandler                       → 分布式任务管理
│   └── ServiceVersionClientServiceHandler                  → 服务版本协商
├── Worker:9203                                            → 数据读写
│   └── BlockWorkerImpl                                     → ★ Block 流式读写（核心）
│       ├── readBlock / writeBlock                          → 双向流式
│       ├── openLocalBlock / createLocalBlock               → 短路读写
│       └── asyncCache / syncCache / removeBlock / ...       → 管理操作
        │
        ▼
UFS (COS / S3 / HDFS)
```

### 2.2 三层架构

```
┌─────────────────────────────────────────────────────────────────────┐
│ 层级 1: Lance Provider（轻量适配层，~100 行）                         │
│   GooseFsDalStoreProvider                                           │
│   - 接受 goosefs-dal:// URL                                         │
│   - 构建 OpenDAL Operator → OpendalStore → Lance ObjectStore         │
├─────────────────────────────────────────────────────────────────────┤
│ 层级 2: OpenDAL GooseFS Service（impl Access trait）                 │
│   opendal/services-goosefs/                                         │
│   - GooseFsBackend: impl Access (read/write/stat/delete/list/rename)│
│   - GooseFsReader / GooseFsWriter / GooseFsLister                   │
├─────────────────────────────────────────────────────────────────────┤
│ 层级 3: GooseFS Rust Client (gRPC)（独立 crate）                     │
│   goosefs-client-rs/                                                │
│   - MasterClient / WorkerClient / BlockMapper / WorkerRouter         │
│   - Proto 定义: GooseFS gRPC protobuf (tonic-build)                  │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.3 与现有 `services-alluxio` 对比

| 对比维度 | `services-alluxio` (现有) | `services-goosefs` (新增) |
|----------|--------------------------|---------------------------|
| 传输协议 | HTTP REST API | gRPC (tonic/protobuf) |
| 读取支持 | ❌ 不支持 | ✅ 流式读取 + range read |
| 依赖组件 | Alluxio Proxy | GooseFS Master + Worker（无需 Proxy） |
| GooseFS 兼容 | ❌ 需要 Proxy | ✅ 直接对接 |
| 性能 | 较低（HTTP 开销） | 较高（gRPC 二进制传输） |

---

## 3. GooseFS gRPC 协议技术参考

> 基于 GooseFS 源码深度分析，拆解 `goosefs-client-rs` 需实现的 **6 个核心模块**。

### 3.1 GooseFS gRPC 服务入口总览

GooseFS 共有 **8 个 gRPC ServiceHandler**（7 个 Master 端 + 1 个 Worker 端），注册在不同端口：

#### Master 端（端口 9200）

| # | Handler 类 | 继承自 (gRPC ImplBase) | 注册模块 | 主要职责 | Lance 需要? |
|---|-----------|----------------------|---------|---------|------------|
| 1 | **`FileSystemMasterClientServiceHandler`** | `FileSystemMasterClientServiceGrpc.ImplBase` | `DefaultFileSystemMaster` | 文件系统 CRUD、挂载、ACL、Namespace | **★ 核心** |
| 2 | **`WorkerManagerMasterClientServiceHandler`** | `WorkerManagerMasterClientServiceGrpc.ImplBase` | `DefaultWorkerManagerMaster` | Worker 列表/容量/限流/管理 | **★ 需要**（Worker 发现） |
| 3 | `MetaMasterClientServiceHandler` | `MetaMasterClientServiceGrpc.ImplBase` | `DefaultMetaMaster` | 集群元信息、备份/恢复、checkpoint | 可选 |
| 4 | `MetricsMasterClientServiceHandler` | `MetricsMasterClientServiceGrpc.ImplBase` | `DefaultMetricsMaster` | 指标上报/查询 | 可选 |
| 5 | `JournalMasterClientServiceHandler` | `JournalMasterClientServiceGrpc.ImplBase` | `DefaultJournalMaster` | Raft quorum 管理 | 否 |
| 6 | `TableMasterClientServiceHandler` | `TableMasterClientServiceGrpc.ImplBase` | `DefaultTableMaster` | 表格元数据 / Lance Namespace | Phase 1 已完成 |
| 7 | `JobMasterClientServiceHandler` | `JobMasterClientServiceGrpc.ImplBase` | `JobMaster` | 分布式任务调度（Job Server） | 否 |
| 8 | `ServiceVersionClientServiceHandler` | `ServiceVersionClientServiceGrpc.ImplBase` | `GrpcServerBuilder` | 服务版本协商 | **需要**（连接握手） |

> **注**：除 `ClientServiceHandler` 外，Master 还注册了 `*WorkerServiceHandler`（Master 与 Worker 内部通信）和 `*MasterServiceHandler`（Master HA 选举），这些属于服务端内部通信，Rust Client 无需实现。

#### Worker 端（端口 9203）

| # | Handler 类 | 继承自 | 主要方法 | Lance 需要? |
|---|-----------|--------|---------|------------|
| 1 | **`BlockWorkerImpl`** | `BlockWorkerGrpc.BlockWorkerImplBase` | `readBlock`(流式读)、`writeBlock`(流式写)、`openLocalBlock`(短路读)、`createLocalBlock`(短路写)、`asyncCache`、`syncCache`、`removeBlock`、`moveBlock`、`checkBlocks` | **★ 核心** |

#### Lance 集成所需的最小 gRPC Service 子集

```
Lance Rust Client 需要对接的 3 个核心 Service:
┌─────────────────────────────────────────────────────────────────────┐
│ Master:9200                                                         │
│   ① FileSystemMasterClientService  → 文件元数据 CRUD               │
│   ② WorkerManagerMasterClientService → Worker 列表发现              │
│   ③ ServiceVersionClientService     → 版本握手                     │
├─────────────────────────────────────────────────────────────────────┤
│ Worker:9203                                                         │
│   ④ BlockWorker Service             → Block 流式读写（核心数据路径）│
└─────────────────────────────────────────────────────────────────────┘
```

#### 各 Handler 的 gRPC 方法清单

**① FileSystemMasterClientServiceHandler** (`DefaultFileSystemMaster.getServices()`注册，38+ 方法):

| 方法 | 功能 | Lance 使用 |
|------|------|-----------|
| `getStatus` | 获取文件/目录状态 | ★ head/stat |
| `listStatus` | 列出子路径（server-streaming） | ★ list |
| `createFile` | 创建文件 | ★ put |
| `completeFile` | 标记文件写完 | ★ put 收尾 |
| `remove` | 删除文件/目录 | ★ delete |
| `rename` | 重命名 | ★ rename/manifest |
| `createDirectory` | 创建目录 | ★ 隐式创建 |
| `checkAccess` | 检查访问权限 | 可选 |
| `createSymlink` / `getLinkTarget` | 符号链接 | 否 |
| `free` | 释放缓存 | 可选 |
| `mount` / `unmount` / `updateMount` | UFS 挂载管理 | 否 |
| `setAttribute` / `setAcl` | 属性/权限设置 | 可选 |
| `startSync` / `stopSync` | UFS 同步 | 否 |
| `createNamespace` / `deleteNamespace` / `listNamespace` / `statNamespace` / `updateNamespace` / `setNamespaceAttribute` | Namespace 管理 | Phase 1 |
| `getDelegationToken` / `cancelDelegationToken` / `renewDelegationToken` | Kerberos Token | 可选 |
| `checkConsistency` / `scheduleAsyncPersistence` / `reverseResolve` / `getFilePath` / `removeBlocks` / ... | 其他管理 | 否 |

**② WorkerManagerMasterClientServiceHandler** (`DefaultWorkerManagerMaster.getServices()`注册):

| 方法 | 功能 | Lance 使用 |
|------|------|-----------|
| `getWorkerInfoList` | **获取所有 Worker 列表** | ★ Worker 路由 |
| `getWorkerReport` | 获取 Worker 详情 | 可选 |
| `getCapacityBytes` | 集群总容量 | 否 |
| `getUsedBytes` | 已用容量 | 否 |
| `getWorkerManagerMasterInfo` | Block Master 信息 | 否 |
| `getWorkerLostStorage` | 丢失存储 | 否 |
| `manageWorker` | Worker 管理（上下线） | 否 |
| `updateClusterRateLimit` / `getClusterRateLimit` | 集群限流 | 否 |

**③ BlockWorkerImpl** (`GrpcDataServer`注册):

| 方法 | 类型 | 功能 | Lance 使用 |
|------|------|------|-----------|
| `readBlock` | **双向流式** | Block 数据读取 | ★ 核心 |
| `writeBlock` | **双向流式** | Block 数据写入 | ★ 核心 |
| `openLocalBlock` | 双向流式 | 短路读（同节点零拷贝） | 可选优化 |
| `createLocalBlock` | 双向流式 | 短路写（同节点零拷贝） | 可选优化 |
| `asyncCache` | 一元 RPC | 异步缓存预热 | 可选 |
| `syncCache` | 一元 RPC | 同步缓存 | 可选 |
| `removeBlock` | 一元 RPC | 删除 Block | 否 |
| `moveBlock` | 一元 RPC | 移动 Block | 否 |
| `checkBlocks` | 一元 RPC | 校验 Block | 否 |
| `clearMetrics` | 一元 RPC | 清除指标 | 否 |
| `avoidBlockDeadLock` | 一元 RPC | 死锁避免 | 否 |

### 3.2 核心实现模块总览

| # | 模块 | Java 对应源码 | Rust 需实现 | 复杂度 | 状态 |
|---|------|-------------|------------|--------|------|
| 1 | **Master 元数据客户端** | `RetryHandlingFileSystemMasterClient` | `MasterClient` | 中 | ✅ 已完成 |
| 2 | **Worker 管理客户端** | `RetryHandlingWorkerManagerMasterClient` | `WorkerManagerClient` | 低 | ✅ 已完成 |
| 3 | **Worker 数据客户端** | `DefaultBlockWorkerClient` | `WorkerClient` | 高 | ✅ 已完成 |
| 4 | **Block 映射计算** | `GooseFSFileInStream.updateStream()` | `BlockMapper` | 中 | ✅ 已完成 |
| 5 | **Worker 路由选择** | `ClientWorkerManager` / `GooseFSBlockStore` | `WorkerRouter` | 中 | ✅ 已完成 |
| 6 | **gRPC 流式读** | `GrpcDataReader` + `GrpcBlockingStream` | `GrpcBlockReader` | 极高 | ✅ 已完成 |
| 7 | **gRPC 流式写** | `GrpcDataWriter` + `BlockOutStream` | `GrpcBlockWriter` | 极高 | ✅ 已完成 |
| 8 | **★ 高层文件读取器** | `GooseFSFileInStream` | `GooseFsFileReader` | 中 | ✅ 已完成 |
| 9 | **★ 高层文件写入器** | `GooseFSFileOutStream` | `GooseFsFileWriter` | 中 | ✅ 已完成 |

### 3.3 gRPC 协议详解

**ReadRequest 关键字段**：

```protobuf
message ReadRequest {
  optional int64 block_id = 1;
  optional int64 offset = 2;          // range read 起始偏移
  optional int64 length = 3;          // range read 长度
  optional int64 chunk_size = 4;
  optional OpenUfsBlockOptions open_ufs_block_options = 5;
  optional int64 offset_received = 6; // 流控 ACK
  optional int32 prefetch_window = 11;
}
```

**WriteRequest 关键字段**：

```protobuf
message WriteRequest {
  oneof value {
    WriteRequestCommand command = 1;  // 首条消息
    Chunk chunk = 2;                  // 后续数据
  }
}
message WriteRequestCommand {
  optional RequestType type = 1;      // GOOSEFS_BLOCK / UFS_FILE / UFS_FALLBACK_BLOCK
  optional int64 id = 2;             // Block ID
  optional int64 offset = 3;
  optional bool flush = 4;
  optional CreateUfsFileOptions create_ufs_file_options = 5;  // THROUGH 模式必填
  optional int64 space_to_reserve = 6;
}
// RequestType 决定 Worker 端数据写入目标：
//   GOOSEFS_BLOCK(0) — 写入 GooseFS 缓存块
//   UFS_FILE(1)      — 直接写入 UFS 文件（THROUGH 模式）
//   UFS_FALLBACK_BLOCK(2) — 缓存满降级写 UFS
```

### 3.4 模块 1：Master 元数据客户端

```rust
pub struct GooseFsMasterClient {
    channel: FileSystemMasterClientServiceClient<Channel>,
    master_addr: String,
    retry_policy: RetryPolicy,
}

impl GooseFsMasterClient {
    pub async fn get_status(&self, path: &str) -> Result<FileInfo> { /* GetStatus RPC */ }
    pub async fn list_status(&self, path: &str, recursive: bool) -> Result<Vec<FileInfo>> { /* server-streaming */ }
    pub async fn create_file(&self, path: &str, options: CreateFileOptions) -> Result<FileInfo> { /* ... */ }
    pub async fn complete_file(&self, path: &str, inode_id: i64) -> Result<()> { /* ... */ }
    pub async fn delete(&self, path: &str, recursive: bool) -> Result<()> { /* ... */ }
    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> { /* ... */ }
}
```

**关键难点**：
- Master HA：✅ 已实现 `PollingMasterInquireClient` Leader 发现（`from_addresses()` 统一入口）
- `ListStatus` 是 server-streaming RPC，用 tonic `Streaming<T>` 处理
- 认证：Kerberos/LDAP 需实现 gRPC 拦截器

### 3.5 模块 2：Worker 数据客户端

Java 端维护**两个独立 gRPC Channel**：
- **Streaming Channel**：`ReadBlock`/`WriteBlock`，禁用连接池，追求吞吐
- **RPC Channel**：`RemoveBlock`/`CheckBlocks` 等管理操作，使用连接池

```rust
pub struct GooseFsWorkerClient {
    streaming_client: BlockWorkerClient<Channel>,  // 高吞吐
    rpc_client: BlockWorkerClient<Channel>,        // 管理操作
    worker_addr: WorkerNetAddress,
}
```

### 3.6 模块 3：Block 映射计算

**映射规则**（从 `GooseFSFileInStream` 提取）：

```
给定文件偏移 file_offset:
  block_index  = file_offset / blockSizeBytes
  block_id     = blockIds[block_index]
  block_offset = file_offset % blockSizeBytes
  read_length  = min(requested_length, blockSizeBytes - block_offset)

跨 Block 读取: 拆分为多次 ReadBlock RPC
```

```rust
pub struct BlockMapper {
    block_size: i64,
    block_ids: Vec<i64>,
    file_length: i64,
    block_locations: Vec<FileBlockInfo>,
}

impl BlockMapper {
    pub fn plan_read(&self, file_offset: i64, length: i64) -> Vec<BlockReadSegment> {
        // 将文件级 range read 拆分为 Block 级读取计划
    }
}
```

### 3.7 模块 4：Worker 路由选择

GooseFS 使用**一致性哈希**映射 Block ID → Worker：

```rust
pub struct WorkerRouter {
    workers: Arc<RwLock<Vec<WorkerInfo>>>,
    hash_ring: ConsistentHashRing,
    failed_workers: DashMap<String, Instant>,
}

impl WorkerRouter {
    pub fn select_worker(&self, block_id: i64) -> Result<WorkerInfo> {
        // 一致性哈希 + 失败节点过滤
    }
}
```

### 3.8 模块 5：gRPC 流式读（最复杂）

**读取协议（5 步）**：
```
Client                                    Worker
  │  1. ReadRequest(block_id, offset,        │
  │     length, chunk_size)                  │
  ├─────────────────────────────────────────→│
  │  2. ReadResponse(chunk.data)             │
  │←─────────────────────────────────────────┤
  │  3. ReadRequest(offset_received=N)       │ ← 流控 ACK
  ├─────────────────────────────────────────→│
  │  4. ReadResponse(chunk.data) ...         │
  │←─────────────────────────────────────────┤
  │  5. close/cancel                         │
  ├──────────────────────────────────────────→│
```

**Java 流控机制**（`GrpcDataReader.readChunk()`）：
- 首次：发送完整 ReadRequest（含 block_id, offset, length, chunk_size）
- 后续：发送 `offset_received` 确认已接收偏移（流控 ACK）
- 接收 ReadResponse 中的 Chunk 数据

**读取策略分层**（`BlockInStream.create()`）：
1. 短路读（同节点直接读本地文件）
2. SharedGrpcDataReader（共享读，减少 seek 开销）
3. ChunkCachingGrpcDataReader（Chunk 缓存读）
4. GrpcDataReader（标准 gRPC 读）

```rust
pub struct GrpcBlockReader {
    block_id: i64,
    offset: i64,
    length: i64,
    pos_to_read: i64,
    request_tx: mpsc::Sender<ReadRequest>,
    response_rx: Streaming<ReadResponse>,
}

impl GrpcBlockReader {
    pub async fn open(client: &GooseFsWorkerClient, block_id: i64, 
                      offset: i64, length: i64, chunk_size: i64) -> Result<Self> {
        // 建立双向流式 RPC，发送初始 ReadRequest
    }
    
    pub async fn read_chunk(&mut self) -> Result<Option<Bytes>> {
        // 发送 offset_received ACK → 接收 ReadResponse
    }
    
    pub async fn read_all(&mut self) -> Result<Bytes> {
        // 循环 read_chunk 直到读完
    }
}
```

### 3.9 模块 6：gRPC 流式写

**完整写入流程**：
```
1. Master: CreateFile(path, blockSizeBytes, writeType) → FileInfo
2. 对于每个 Block:
   a. Worker 选择: ConsistentHash(blockId)
   b. WriteBlock 流式写入:
      - 首条消息: WriteRequestCommand { id, type, spaceToReserve }
      - 后续消息: Chunk { data }
      - flush: WriteRequestCommand { flush=true } → 等待 WriteResponse 确认
3. Master: CompleteFile(path, inodeId)
```

### 3.10 ★ 高层封装：端到端文件读写 API（已实现）

低层模块（MasterClient、WorkerClient、BlockMapper、WorkerRouter、GrpcBlockReader/Writer）各自独立，
使用时需要开发者手动编排。**高层封装** 将完整管道包装为两个简单 API，类似 Java 端的
`GooseFSFileInStream` / `GooseFSFileOutStream`。

#### 3.10.1 GooseFsFileWriter — 端到端写入管道

```text
写入流程:
GooseFsFileWriter::create(path)
  → MasterClient.create_file()           创建文件元数据（含 writeType）
  → resolve_write_strategy()             根据 writeType + FileInfo 推导写入策略
  → WorkerManagerClient.get_worker_info_list()  发现 Worker
  → WorkerRouter.update_workers()        构建一致性哈希环

GooseFsFileWriter::write(data)           可多次调用
  → BlockMapper.plan_write()             拆分数据为 Block 段
  → for each block:
      → compute_block_id(file_id, block_index)  计算 Block ID
      → WorkerRouter.select_worker()     一致性哈希路由
      → WorkerClient.connect()           连接 Worker
      → GrpcBlockWriter.open(WriteBlockOptions)  按策略启动双向流
        ├─ MUST_CACHE/CACHE_THROUGH/ASYNC_THROUGH: GoosefsBlock
        └─ THROUGH: UfsFile + CreateUfsFileOptions
      → GrpcBlockWriter.write_all()      分 chunk 发送数据
      → GrpcBlockWriter.flush()          发送 flush 并等待 ACK
      → GrpcBlockWriter.close()          关闭写入流

GooseFsFileWriter::close()
  → MasterClient.complete_file()         标记文件写入完成
  → if ASYNC_THROUGH:
      → MasterClient.schedule_async_persistence()  调度异步持久化
```

**使用方式**：

```rust
use goosefs_client::io::GooseFsFileWriter;
use goosefs_client::config::GooseFsConfig;

let config = GooseFsConfig::new("127.0.0.1:9200");

// 方式 1: 一行搞定
GooseFsFileWriter::write_file(&config, "/data/file.txt", b"Hello!").await?;

// 方式 2: 流式多段写入
let mut writer = GooseFsFileWriter::create(&config, "/data/file.txt").await?;
writer.write(b"part 1 ").await?;
writer.write(b"part 2 ").await?;
writer.close().await?;
```

#### 3.10.2 GooseFsFileReader — 端到端读取管道

```text
读取流程:
GooseFsFileReader::open(path) / open_range(path, offset, length)
  → MasterClient.get_status()            获取文件元数据（含 blockIds, blockSize）
  → WorkerManagerClient.get_worker_info_list()  发现 Worker
  → WorkerRouter.update_workers()        构建一致性哈希环
  → BlockMapper.plan_read()              文件范围 → Block 级读取计划

GooseFsFileReader::read_next_block()     逐块流式读取
  → resolve_block_id()                   优先使用 FileBlockInfo 中的实际 Block ID
  → WorkerRouter.select_worker()         一致性哈希路由
  → WorkerClient.connect()              连接 Worker（失败自动标记）
  → GrpcBlockReader.open()              启动双向流
  → GrpcBlockReader.read_all()          读取整个 Block 段数据

GooseFsFileReader::read_all()            读取所有 Block 并拼接
```

**使用方式**：

```rust
use goosefs_client::io::GooseFsFileReader;
use goosefs_client::config::GooseFsConfig;

let config = GooseFsConfig::new("127.0.0.1:9200");

// 方式 1: 一行读取整个文件
let data = GooseFsFileReader::read_file(&config, "/data/file.txt").await?;

// 方式 2: 范围读取
let range = GooseFsFileReader::read_range(&config, "/data/file.txt", 100, 500).await?;

// 方式 3: 逐块流式读取
let mut reader = GooseFsFileReader::open(&config, "/data/file.txt").await?;
while let Some(chunk) = reader.read_next_block().await? {
    process(chunk);
}
```

#### 3.10.3 高层 API 与低层 API 的关系

```text
┌─────────────────────────────────────────────────────────────────────┐
│  高层 API（推荐使用，适合大多数场景）                                   │
│                                                                     │
│  GooseFsFileWriter   ← 一行写文件，自动编排全流程                     │
│  GooseFsFileReader   ← 一行读文件/范围读，自动编排全流程               │
├─────────────────────────────────────────────────────────────────────┤
│  低层 API（适合需要精细控制的场景）                                     │
│                                                                     │
│  MasterClient        ← 文件元数据 CRUD                              │
│  WorkerManagerClient ← Worker 发现                                  │
│  WorkerClient        ← Block 级双向流 RPC                           │
│  BlockMapper         ← 文件范围 → Block 计划                        │
│  WorkerRouter        ← 一致性哈希路由                                │
│  GrpcBlockReader     ← 单 Block 流式读（带流控 ACK）                 │
│  GrpcBlockWriter     ← 单 Block 流式写（带 flush/close）             │
└─────────────────────────────────────────────────────────────────────┘
```

高层 API 在内部调用所有低层组件，用户无需关心：
- Worker 发现与路由
- Block 拆分与映射
- gRPC 双向流管理
- 文件完成（CompleteFile）收尾

### 3.11 gRPC 调用组装示例

以下展示各模块如何协同完成 Range Read（Lance 最核心操作）：

```rust
async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
    // 1. 获取文件元数据（含 blockIds, blockSizeBytes）
    let info = self.master_client.get_status(location.as_ref()).await?;
    
    // 2. Block 映射：文件级 range → Block 级读取计划
    let mapper = BlockMapper::new(&info);
    let segments = mapper.plan_read(range.start as i64, (range.end - range.start) as i64);
    
    // 3. 对每个 Block segment 执行 gRPC ReadBlock
    let mut result = BytesMut::with_capacity(range.end - range.start);
    for seg in segments {
        let worker = self.worker_router.select_worker(seg.block_id)?;
        let client = self.get_worker_client(&worker).await?;
        let mut reader = GrpcBlockReader::open(
            &client, seg.block_id, seg.block_offset, seg.length, self.config.chunk_size,
        ).await?;
        result.extend_from_slice(&reader.read_all().await?);
    }
    Ok(result.freeze())
}
```

---

## 4. WriteType 完整支持设计

> **v1.1 新增**：线上除了 MUST_CACHE 以外，THROUGH、CACHE_THROUGH、ASYNC_THROUGH 都有客户在使用，4 种 WriteType 全部需要支持。

### 4.1 背景与需求

GooseFS 支持 6 种 `WritePType`，其中 4 种在线上活跃使用：

| WriteType | 枚举值 | 数据流向 | 线上使用 |
|-----------|--------|---------|---------|
| **MUST_CACHE** | 1 | 仅写 GooseFS 缓存，不持久化到 UFS | ✅ |
| **TRY_CACHE** | 2 | 尝试缓存，缓存满时降级为 THROUGH | ⚠️ 少量 |
| **CACHE_THROUGH** | 3 | 写缓存 + 同步持久化到 UFS | ✅ |
| **THROUGH** | 4 | 直写 UFS，跳过缓存 | ✅ |
| **ASYNC_THROUGH** | 5 | 写缓存，异步调度持久化到 UFS | ✅ |
| **NONE** | 6 | 使用服务端默认 | — |

### 4.2 现状分析 vs 目标状态

| WriteType | v1.0 现状 | v1.1 目标 | 所需改动 |
|-----------|----------|----------|---------|
| **MUST_CACHE** | ✅ 可用（当前默认行为） | ✅ 保持不变 | 无 |
| **THROUGH** | ❌ 不可用 | ✅ 完整支持 | 🔴 **重大改动** |
| **CACHE_THROUGH** | ⚠️ 部分可用（config 已传 writeType） | ✅ 完整支持 | 🟡 中度改动 |
| **ASYNC_THROUGH** | ⚠️ 部分可用 | ✅ 完整支持 | 🟡 中度改动 |

**核心差异**：
- **MUST_CACHE / CACHE_THROUGH**：Worker 端使用 `RequestType::GoosefsBlock`，数据写入缓存
- **THROUGH**：Worker 端使用 `RequestType::UfsFile`，需传入 `CreateUfsFileOptions`（含 `ufs_path`、`owner`、`group`、`mode`、`mount_id`），数据直接写入 UFS
- **ASYNC_THROUGH**：数据写入缓存（同 MUST_CACHE），但 `close()` 后需调用 `scheduleAsyncPersistence` RPC

### 4.3 修改架构全景图

```text
修改涉及 4 层代码（从底到上）:

Layer 1 — WorkerClient::write_block()      ← 新增 WriteBlockOptions（RequestType + CreateUfsFileOptions）
  ↑
Layer 2 — GrpcBlockWriter::open()           ← 透传 WriteBlockOptions
  ↑
Layer 3 — GooseFsFileWriter::write_block()  ← 根据 WriteStrategy 决策 RequestType
  ↑                         ::close()       ← ASYNC_THROUGH 时调用 schedule_async_persistence
Layer 4 — MasterClient::complete_file()     ← 增加可选 async_persist_options 参数
```

### 4.4 WriteBlockOptions — 写入请求参数封装

```rust
use crate::proto::grpc::block::RequestType;
use crate::proto::proto::dataserver::CreateUfsFileOptions;

/// Block 写入请求参数，封装 RequestType 和可选的 UFS 文件创建选项。
pub struct WriteBlockOptions {
    /// 请求类型:
    /// - GoosefsBlock(0) — 写入 GooseFS 缓存块（MUST_CACHE/CACHE_THROUGH/ASYNC_THROUGH）
    /// - UfsFile(1) — 直接写入 UFS 文件（THROUGH）
    /// - UfsFallbackBlock(2) — 缓存满时降级写 UFS（TRY_CACHE fallback）
    pub request_type: RequestType,

    /// THROUGH 模式下需要传入 UFS 文件创建参数。
    /// 包含：ufs_path, owner, group, mode, mount_id, acl。
    /// 从 Master.CreateFile 返回的 FileInfo 中提取。
    pub create_ufs_file_options: Option<CreateUfsFileOptions>,
}

impl Default for WriteBlockOptions {
    fn default() -> Self {
        Self {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
        }
    }
}
```

### 4.5 WriteStrategy — 写入策略决策

根据 `WritePType` 推导每种模式下的 Worker 行为和 close 后处理：

```rust
/// 写入策略：根据 WritePType 决定 Worker 端行为和后处理。
struct WriteStrategy {
    /// Worker 写入的 RequestType
    request_type: RequestType,
    /// THROUGH 模式下需要 UFS 文件创建选项（从 FileInfo 提取）
    create_ufs_file_options: Option<CreateUfsFileOptions>,
    /// 是否需要在 close() 后调用 schedule_async_persistence
    need_async_persist: bool,
}
```

**决策逻辑**：

```rust
fn resolve_write_strategy(
    write_type: Option<i32>,
    file_info: &FileInfo,
) -> WriteStrategy {
    match write_type {
        // MUST_CACHE / TRY_CACHE / 未设置: 只写缓存，不涉及 UFS
        Some(1) | Some(2) | None => WriteStrategy {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
            need_async_persist: false,
        },

        // CACHE_THROUGH: 写缓存，Master 端 CompleteFile 时自动同步持久化
        Some(3) => WriteStrategy {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
            need_async_persist: false,  // Master 端自动处理
        },

        // THROUGH: 直接写 UFS，跳过缓存
        Some(4) => WriteStrategy {
            request_type: RequestType::UfsFile,
            create_ufs_file_options: Some(CreateUfsFileOptions {
                ufs_path: file_info.ufs_path.clone(),
                owner: file_info.owner.clone(),
                group: file_info.group.clone(),
                mode: file_info.mode,
                mount_id: file_info.mount_id,
                acl: None,
            }),
            need_async_persist: false,
        },

        // ASYNC_THROUGH: 写缓存，close() 后异步调度持久化
        Some(5) => WriteStrategy {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
            need_async_persist: true,
        },

        _ => WriteStrategy {
            request_type: RequestType::GoosefsBlock,
            create_ufs_file_options: None,
            need_async_persist: false,
        },
    }
}
```

### 4.6 四种 WriteType 完整数据流对比

```text
┌─────────────────────────────────────────────────────────────────────────┐
│ MUST_CACHE (1)                                                          │
│                                                                         │
│ CreateFile(writeType=1) → Worker[GoosefsBlock] → CompleteFile           │
│                                     ↓                                   │
│                              Worker 缓存层 ✅                           │
│                              UFS ❌                                      │
├─────────────────────────────────────────────────────────────────────────┤
│ CACHE_THROUGH (3)                                                        │
│                                                                         │
│ CreateFile(writeType=3) → Worker[GoosefsBlock] → CompleteFile           │
│                                     ↓                                   │
│                              Worker 缓存层 ✅                           │
│                              ↓ Master 在 CompleteFile 时同步持久化       │
│                              UFS(COS/S3/HDFS) ✅                        │
├─────────────────────────────────────────────────────────────────────────┤
│ THROUGH (4)                                                              │
│                                                                         │
│ CreateFile(writeType=4) → Worker[UfsFile + CreateUfsFileOptions]        │
│                                     ↓                                   │
│                              Worker 缓存层 ❌                           │
│                              UFS(COS/S3/HDFS) ✅ (直接写入)             │
│                          → CompleteFile                                  │
├─────────────────────────────────────────────────────────────────────────┤
│ ASYNC_THROUGH (5)                                                        │
│                                                                         │
│ CreateFile(writeType=5) → Worker[GoosefsBlock] → CompleteFile           │
│                                     ↓              → scheduleAsyncPersistence
│                              Worker 缓存层 ✅                           │
│                              ↓ 后台异步                                 │
│                              UFS(COS/S3/HDFS) ✅ (eventually)           │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.7 各层改动详情

#### 4.7.1 Layer 1: `WorkerClient::write_block()` — 支持 WriteBlockOptions

**改动前**（硬编码 `GoosefsBlock`）:

```rust
pub async fn write_block(
    &self,
    block_id: i64,
    space_to_reserve: i64,
) -> Result<(mpsc::Sender<WriteRequest>, Streaming<WriteResponse>)> {
    // ...
    WriteRequestCommand {
        r#type: Some(RequestType::GoosefsBlock as i32),  // ← 硬编码
        create_ufs_file_options: None,                    // ← 永远 None
        // ...
    }
}
```

**改动后**（接受 `WriteBlockOptions`）:

```rust
pub async fn write_block(
    &self,
    block_id: i64,
    space_to_reserve: i64,
    options: WriteBlockOptions,  // ← 新增参数
) -> Result<(mpsc::Sender<WriteRequest>, Streaming<WriteResponse>)> {
    // ...
    WriteRequestCommand {
        r#type: Some(options.request_type as i32),            // ← 由调用方决定
        create_ufs_file_options: options.create_ufs_file_options, // ← 透传
        // ...
    }
}
```

#### 4.7.2 Layer 2: `GrpcBlockWriter::open()` — 透传 WriteBlockOptions

```rust
pub async fn open(
    worker: &WorkerClient,
    block_id: i64,
    space_to_reserve: i64,
    options: WriteBlockOptions,  // ← 新增
) -> Result<Self> {
    let (request_tx, response_rx) = worker
        .write_block(block_id, space_to_reserve, options)
        .await?;
    // ...
}
```

#### 4.7.3 Layer 3: `GooseFsFileWriter` — 核心决策逻辑

**新增字段**：

```rust
pub struct GooseFsFileWriter {
    // ... 现有字段 ...
    /// 从 config.write_type + CreateFilePOptions 推导的写入策略
    write_strategy: WriteStrategy,
}
```

**`create_with_options` 中初始化策略**：

```rust
// 推导生效的 write_type: 优先使用 CreateFilePOptions 中的，否则用 config 的
let effective_write_type = create_options.write_type.or(config.write_type);
let write_strategy = resolve_write_strategy(effective_write_type, &file_info);
```

**`write_block` 中使用策略**：

```rust
let write_opts = WriteBlockOptions {
    request_type: self.write_strategy.request_type,
    create_ufs_file_options: self.write_strategy.create_ufs_file_options.clone(),
};
let mut block_writer =
    GrpcBlockWriter::open(&worker, block_id, block_size as i64, write_opts).await?;
```

**`close()` 中处理 ASYNC_THROUGH**：

```rust
pub async fn close(&mut self) -> Result<()> {
    // ... CompleteFile ...
    self.master.complete_file(&self.path, ufs_length).await?;
    self.completed = true;

    // ASYNC_THROUGH: 调度异步持久化
    if self.write_strategy.need_async_persist {
        debug!(path = %self.path, "scheduling async persistence for ASYNC_THROUGH");
        self.master.schedule_async_persistence(&self.path, None).await?;
    }
    // ...
}
```

#### 4.7.4 Layer 4: MasterClient — 已有 `schedule_async_persistence`

`MasterClient::schedule_async_persistence()` 已在 v1.0 中实现，无需改动。
`CompleteFilePOptions` 的 `async_persist_options` 字段已在 proto 中定义，可选使用。

### 4.8 涉及修改的文件清单

| # | 文件 | 改动类型 | 改动量 | 说明 |
|---|------|---------|--------|------|
| 1 | `src/client/worker.rs` | **重大修改** | ~30 行 | `write_block()` 新增 `WriteBlockOptions` 参数 |
| 2 | `src/io/writer.rs` | **中度修改** | ~10 行 | `GrpcBlockWriter::open()` 透传 `WriteBlockOptions` |
| 3 | `src/io/file_writer.rs` | **重大修改** | ~80 行 | 新增 `WriteStrategy` + `resolve_write_strategy()`，修改 `write_block()` 和 `close()` |
| 4 | `src/config.rs` | ✅ **已完成** | — | `write_type` 字段已添加 |
| 5 | `src/lib.rs` | ✅ **已完成** | — | `WritePType` 已重新导出 |
| **总计** | | | **~120 行** | |

### 4.9 关键设计决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| THROUGH 时 `ufs_path` 从哪获取？ | 从 `FileInfo.ufs_path` | Master `CreateFile` 返回的 `FileInfo` 中包含 UFS 映射路径 |
| CACHE_THROUGH 客户端需额外操作？ | **不需要** | Master 在 `CompleteFile` 时根据 `writeType=3` 自动同步持久化 |
| ASYNC_THROUGH 何时调度持久化？ | `close()` 中 `CompleteFile` 之后 | 遵循 Java `GooseFSFileOutStream.close()` 行为 |
| `WriteBlockOptions` 用结构体还是参数展开？ | **结构体** | 避免参数过多，便于未来扩展 |
| 向后兼容性 | `WriteBlockOptions::default()` = 当前行为 | 不设置 `write_type` 时完全等价于现有 MUST_CACHE |

### 4.10 使用示例

```rust
use goosefs_client::config::GooseFsConfig;
use goosefs_client::WritePType;
use goosefs_client::io::GooseFsFileWriter;

// MUST_CACHE（默认，不变）
let config = GooseFsConfig::new("127.0.0.1:9200");
GooseFsFileWriter::write_file(&config, "/data/file.txt", data).await?;

// CACHE_THROUGH — 写缓存 + 同步持久化
let config = GooseFsConfig::new("127.0.0.1:9200")
    .with_write_type(WritePType::CacheThrough);
GooseFsFileWriter::write_file(&config, "/data/file.txt", data).await?;

// THROUGH — 直写 UFS，跳过缓存
let config = GooseFsConfig::new("127.0.0.1:9200")
    .with_write_type(WritePType::Through);
GooseFsFileWriter::write_file(&config, "/data/file.txt", data).await?;

// ASYNC_THROUGH — 写缓存，异步持久化
let config = GooseFsConfig::new("127.0.0.1:9200")
    .with_write_type(WritePType::AsyncThrough);
GooseFsFileWriter::write_file(&config, "/data/file.txt", data).await?;
// close() 内部自动调用 schedule_async_persistence
```

---

## 5. 详细实现设计

### 5.1 层级 3：`goosefs-client-rs`

**项目结构**：

```
goosefs-client-rs/
├── Cargo.toml
├── build.rs                     # tonic-build 编译 proto
├── proto/grpc/
│   ├── file_system_master.proto
│   ├── block_worker.proto
│   ├── block_master.proto
│   └── common.proto
├── src/
│   ├── lib.rs
│   ├── client/
│   │   ├── master.rs            # MasterClient (~200行)
│   │   ├── worker.rs            # WorkerClient (~250行)
│   │   ├── worker_manager.rs    # WorkerManagerClient (~60行)
│   │   └── config.rs            # 连接配置 (~50行)
│   ├── block/
│   │   ├── mapper.rs            # Block 映射 (~100行)
│   │   └── router.rs            # Worker 路由 (~80行)
│   ├── io/
│   │   ├── file_reader.rs       # ★ 高层文件读取器 (~300行) — 端到端读取管道
│   │   ├── file_writer.rs       # ★ 高层文件写入器 (~300行) — 端到端写入管道
│   │   ├── reader.rs            # gRPC 流式读 (~150行)
│   │   └── writer.rs            # gRPC 流式写 (~150行)
│   └── error.rs
├── examples/
│   ├── highlevel_file_rw.rs      # ★ 高层文件读写（推荐）
│   ├── lowlevel_block_read.rs    # 低层块级流式读取
│   ├── lowlevel_create_file.rs   # 低层文件创建（仅元数据）
│   ├── metadata_crud.rs          # 文件/目录元数据 CRUD
│   └── async_persistence.rs      # 异步持久化调度
└── tests/
```

**Cargo.toml**：

```toml
[package]
name = "goosefs-client-rs"
version = "0.1.0"
edition = "2021"

[dependencies]
tonic = { version = "0.12", features = ["tls"] }
prost = "0.13"
prost-types = "0.13"
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"
bytes = "1"
thiserror = "2"
tracing = "0.1"

[build-dependencies]
tonic-build = "0.12"
```

**MasterClient 完整实现**：

```rust
// src/client/master.rs
use tonic::transport::Channel;

pub struct MasterClient {
    inner: FileSystemMasterClientServiceClient<Channel>,
}

impl MasterClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let channel = Channel::from_shared(format!("http://{}", addr))?
            .connect().await?;
        Ok(Self { inner: FileSystemMasterClientServiceClient::new(channel) })
    }

    pub async fn get_status(&self, path: &str) -> Result<FileInfo> {
        let req = GetStatusPRequest {
            path: Some(path.to_string()),
            options: Some(GetStatusPOptions::default()),
        };
        Ok(self.inner.clone().get_status(req).await?.into_inner().file_info.unwrap())
    }

    pub async fn list_status(&self, path: &str) -> Result<Vec<FileInfo>> {
        let req = ListStatusPRequest {
            path: Some(path.to_string()),
            options: Some(ListStatusPOptions::default()),
        };
        Ok(self.inner.clone().list_status(req).await?.into_inner().file_infos)
    }

    pub async fn create_file(&self, path: &str, options: CreateFilePOptions) -> Result<FileInfo> {
        let req = CreateFilePRequest { path: Some(path.to_string()), options: Some(options) };
        Ok(self.inner.clone().create_file(req).await?.into_inner().file_info.unwrap())
    }

    pub async fn complete_file(&self, path: &str) -> Result<()> {
        let req = CompleteFilePRequest {
            path: Some(path.to_string()), options: Some(CompleteFilePOptions::default()),
        };
        self.inner.clone().complete_file(req).await?;
        Ok(())
    }

    pub async fn delete(&self, path: &str, recursive: bool) -> Result<()> {
        let req = DeletePRequest {
            path: Some(path.to_string()),
            options: Some(DeletePOptions { recursive: Some(recursive), ..Default::default() }),
        };
        self.inner.clone().delete(req).await?;
        Ok(())
    }

    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let req = RenamePRequest {
            path: Some(src.to_string()), dst_path: Some(dst.to_string()),
            options: Some(RenamePOptions::default()),
        };
        self.inner.clone().rename(req).await?;
        Ok(())
    }

    pub async fn create_directory(&self, path: &str) -> Result<()> {
        let req = CreateDirectoryPRequest {
            path: Some(path.to_string()),
            options: Some(CreateDirectoryPOptions { recursive: Some(true), ..Default::default() }),
        };
        self.inner.clone().create_directory(req).await?;
        Ok(())
    }
}
```

**WorkerClient 完整实现**：

```rust
// src/client/worker.rs
pub struct WorkerClient {
    inner: BlockWorkerClient<Channel>,
}

impl WorkerClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let channel = Channel::from_shared(format!("http://{}", addr))?
            .connect().await?;
        Ok(Self { inner: BlockWorkerClient::new(channel) })
    }

    pub async fn read_block(
        &self, block_id: i64, offset: i64, length: i64,
        open_ufs_options: Option<OpenFilePOptions>,
    ) -> Result<impl Stream<Item = Result<Bytes>>> {
        let req = ReadRequest {
            block_id: Some(block_id), offset: Some(offset), length: Some(length),
            open_ufs_block_options: open_ufs_options.map(|o| OpenUfsBlockOptions {
                ufs_path: o.ufs_path, offset_in_file: Some(offset),
                block_size: Some(length), ..Default::default()
            }),
            ..Default::default()
        };
        let response = self.inner.clone().read_block(req).await?;
        Ok(response.into_inner().map(|r| r.map(|c| c.chunk.unwrap_or_default().into()).map_err(Into::into)))
    }

    pub async fn write_block(
        &self, block_id: i64, data_stream: impl Stream<Item = WriteRequest>,
    ) -> Result<()> {
        self.inner.clone().write_block(data_stream).await?;
        Ok(())
    }
}
```

**BlockMapper 完整实现**：

```rust
// src/block/mapper.rs
pub struct BlockMapper;

impl BlockMapper {
    pub fn map_range(file_info: &FileInfo, offset: u64, length: u64) -> Vec<BlockReadPlan> {
        let block_size = file_info.block_size_bytes.unwrap_or(64 * 1024 * 1024) as u64;
        let mut plans = Vec::new();
        let (mut remaining, mut current) = (length, offset);

        while remaining > 0 {
            let idx = current / block_size;
            let off = current % block_size;
            let len = std::cmp::min(remaining, block_size - off);
            let bid = file_info.block_ids.get(idx as usize).copied().unwrap_or(-1);

            plans.push(BlockReadPlan {
                block_id: bid, block_index: idx, offset_in_block: off, length: len,
                worker_locations: file_info.file_block_infos.get(idx as usize)
                    .and_then(|bi| bi.block_info.as_ref().map(|b| b.locations.clone()))
                    .unwrap_or_default(),
            });
            current += len;
            remaining -= len;
        }
        plans
    }
}

pub struct BlockReadPlan {
    pub block_id: i64, pub block_index: u64,
    pub offset_in_block: u64, pub length: u64,
    pub worker_locations: Vec<BlockLocation>,
}
```

### 5.2 层级 2：OpenDAL `services-goosefs`

**项目结构**：

```
opendal/core/services/goosefs/src/
├── lib.rs       # scheme = "goosefs"
├── backend.rs   # GooseFsBackend (impl Access)
├── config.rs    # GooseFsConfig
├── reader.rs    # GooseFsReader (impl oio::Read)
├── writer.rs    # GooseFsWriter (impl oio::Write)
├── lister.rs    # GooseFsLister (impl oio::List)
├── deleter.rs   # GooseFsDeleter (impl oio::Delete)
└── error.rs     # gRPC 错误码 → OpenDAL 错误映射
```

**GooseFsBackend 核心**：

```rust
use goosefs_client_rs::{MasterClient, WorkerClient, BlockMapper, WorkerRouter};

pub struct GooseFsBackend {
    master: Arc<MasterClient>,
    worker_router: Arc<WorkerRouter>,
    root: String,
}

impl Access for GooseFsBackend {
    type Reader = GooseFsReader;
    type Writer = GooseFsWriter;
    type Lister = GooseFsLister;
    type Deleter = GooseFsDeleter;

    fn info(&self) -> Arc<AccessorInfo> {
        // Capability: stat=true, read=true, write=true, delete=true, list=true, rename=true, copy=false
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let info = self.master.get_status(&format!("{}/{}", self.root, path)).await?;
        Ok(RpStat::new(parse_file_info_to_metadata(&info)))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let info = self.master.get_status(&format!("{}/{}", self.root, path)).await?;
        let (offset, length) = args.range().into_offset_length(info.length.unwrap_or(0) as u64);
        Ok((RpRead::new(), GooseFsReader::new(info, offset, length, self.worker_router.clone())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let full = format!("{}/{}", self.root, path);
        let info = self.master.create_file(&full, CreateFilePOptions {
            write_type: Some(WritePType::CacheThrough as i32), ..Default::default()
        }).await?;
        Ok((RpWrite::new(), GooseFsWriter::new(full, info, self.master.clone(), self.worker_router.clone())))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        self.master.delete(&format!("{}/{}", self.root, path), false).await?;
        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        Ok((RpList::default(), GooseFsLister::new(format!("{}/{}", self.root, path), self.master.clone())))
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        self.master.rename(&format!("{}/{}", self.root, from), &format!("{}/{}", self.root, to)).await?;
        Ok(RpRename::default())
    }
}
```

**GooseFsReader**：

```rust
pub struct GooseFsReader {
    file_info: FileInfo, offset: u64, length: u64,
    worker_router: Arc<WorkerRouter>,
    current_block_idx: usize, block_plans: Vec<BlockReadPlan>,
}

impl oio::Read for GooseFsReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.block_plans.is_empty() {
            self.block_plans = BlockMapper::map_range(&self.file_info, self.offset, self.length);
        }
        if self.current_block_idx >= self.block_plans.len() { return Ok(Buffer::new()); }

        let plan = &self.block_plans[self.current_block_idx];
        let worker = self.worker_router.select_worker(&plan.worker_locations).await?;
        let data = worker.read_block(plan.block_id, plan.offset_in_block as i64, plan.length as i64, None).await?;
        self.current_block_idx += 1;

        let mut buf = Vec::with_capacity(plan.length as usize);
        pin_mut!(data);
        while let Some(chunk) = data.next().await { buf.extend_from_slice(&chunk?); }
        Ok(Buffer::from(buf))
    }
}
```

### 5.3 层级 1：Lance Provider

```rust
// rust/lance-io/src/object_store/providers/goosefs_dal.rs
#[derive(Default, Debug)]
pub struct GooseFsDalStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for GooseFsDalStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let storage_options = &params.storage_options.as_ref()
            .map(|s| s.0.clone()).unwrap_or_default();

        let master_addr = storage_options.get("goosefs_master_addr")
            .or_else(|| std::env::var("GOOSEFS_MASTER_ADDR").ok().as_ref())
            .unwrap_or(&"localhost:9200".to_string()).clone();

        let root = base_path.path().to_string();

        let mut config_map = HashMap::new();
        config_map.insert("master_addr".to_string(), master_addr);
        config_map.insert("root".to_string(), root);

        let operator = Operator::from_iter::<services::GooseFs>(config_map)?.finish();
        let opendal_store = Arc::new(OpendalStore::new(operator));

        Ok(ObjectStore {
            scheme: "goosefs-dal".to_string(),
            inner: opendal_store,
            block_size: DEFAULT_CLOUD_BLOCK_SIZE,
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            ..Default::default()
        })
    }
}
```

---

## 6. ObjectStore 操作 → gRPC 调用映射

| ObjectStore 操作 | OpenDAL Access 方法 | GooseFS gRPC 调用 |
|-----------------|--------------------|--------------------|
| `head(path)` | `stat()` | `MasterClient.get_status(path)` |
| `get(path)` | `read()` | `GetStatus` → `BlockMapper` → `WorkerClient.read_block` |
| `get_range(path, range)` | `read(OpRead+range)` | 同上，带 offset/length (**核心路径**) |
| `put(path, data)` | `write()` | `create_file` → `write_block` → `complete_file` |
| `delete(path)` | `delete()` | `MasterClient.delete(path)` |
| `list(prefix)` | `list()` | `MasterClient.list_status(prefix)` |
| `rename(from, to)` | `rename()` | `MasterClient.rename(src, dst)` |
| `put_opts(Create)` | 自定义扩展 | `create_file(overwrite=false)` |

---

## 7. 代码变更清单与工作量

### 7.1 变更量汇总

| 组件 | 新增行数 | 说明 |
|------|----------|------|
| `goosefs-client-rs` | ~1,800 | 独立 crate，gRPC 客户端 + 高层 API |
| ↳ 低层模块 | ~1,200 | MasterClient, WorkerClient, BlockMapper, WorkerRouter, GrpcBlockReader/Writer |
| ↳ **高层封装** | **~600** | **GooseFsFileReader (~300行) + GooseFsFileWriter (~300行)** |
| OpenDAL `services-goosefs` | ~710 | OpenDAL service 适配层 |
| Lance `goosefs_dal.rs` | ~100 | Lance provider 适配 |
| Proto 定义 | ~500 | GooseFS gRPC protobuf |
| 测试代码 | ~300 | 单元 + 集成测试 |
| **总计** | **~3,410** | 跨三个仓库 |

### 7.2 工作量评估

| 模块 | 预估 | 风险 |
|------|------|------|
| Proto 编译 + tonic 代码生成 | 1 周 | 低 |
| Master 元数据客户端 | 2 周 | 中（HA、streaming） |
| Worker 数据客户端框架 | 1 周 | 低 |
| gRPC 流式读 + 流控 | 3 周 | **高** |
| gRPC 流式写 + 分 Block | 3 周 | **高** |
| Block 映射 + Worker 路由 | 1 周 | 中 |
| OpenDAL Access 适配 | 1 周 | 低 |
| 认证（Kerberos 等） | 2 周 | **高** |
| 测试 + 集成调试 | 3 周 | **高** |
| **总计** | **~17 周（4+ 人月）** | |

---

## 8. 配置与使用指南

### 8.1 Rust API

```rust
use lance::Dataset;

// 环境变量
std::env::set_var("GOOSEFS_MASTER_ADDR", "goosefs-master:9200");
let dataset = Dataset::open("goosefs-dal://goosefs-master:9200/lance-datasets/embeddings").await?;

// storage_options
let mut params = ObjectStoreParams::default();
params.set_storage_option("goosefs_master_addr", "goosefs-master:9200");
let dataset = DatasetBuilder::from_uri("goosefs-dal://goosefs-master:9200/datasets/v1")
    .with_params(params).load().await?;
```

### 8.2 Python API

```python
import lance, os
os.environ["GOOSEFS_MASTER_ADDR"] = "goosefs-master:9200"
ds = lance.dataset("goosefs-dal://goosefs-master:9200/lance-datasets/embeddings")

# 或 storage_options
ds = lance.dataset("goosefs-dal://...", storage_options={"goosefs_master_addr": "goosefs-master:9200"})
```

---

## 9. 测试计划

### 9.1 Docker Compose 测试环境

```yaml
version: "3.8"
services:
  goosefs-master:
    image: ccr.ccs.tencentyun.com/goosefs/goosefs:latest
    command: master
    ports: ["9200:9200", "9201:9201"]
    environment:
      ALLUXIO_JAVA_OPTS: "-Dalluxio.master.hostname=goosefs-master"

  goosefs-worker:
    image: ccr.ccs.tencentyun.com/goosefs/goosefs:latest
    command: worker
    depends_on: [goosefs-master]
    ports: ["9203:9203", "9204:9204"]
    environment:
      ALLUXIO_JAVA_OPTS: >
        -Dalluxio.master.hostname=goosefs-master
        -Dalluxio.worker.ramdisk.size=1GB
```

### 9.2 测试矩阵

| 测试类型 | 覆盖内容 |
|----------|----------|
| **单元测试** | BlockMapper（单 Block / 跨 Block / 边界）、WorkerRouter（failover）、URL 解析 |
| **集成测试** | MasterClient CRUD、WorkerClient 流式读写、端到端 Block 读写流程 |
| **Lance E2E** | Dataset 创建/读取/追加/版本管理/向量搜索 via gRPC 链路 |
| **性能基准** | Lance + GooseFS gRPC vs COS 直连（冷/热缓存） |

---

## 10. 性能优化与风险

### 10.1 性能优化

| 优化项 | 描述 |
|--------|------|
| Block Size 对齐 | Lance block_size = GooseFS page_size 整数倍 |
| 元数据缓存 | GooseFsBackend 缓存 FileInfo（含 blockIds） |
| gRPC Channel 复用 | 连接池管理 Worker 连接 |
| 客户端侧缓存 | 可选叠加 OpenDAL FoyerLayer |

### 10.2 缓存一致性

```properties
# 对 Lance manifest 禁用缓存（关键！）
alluxio.user.file.metadata.sync.interval=0s
# goosefs fs setTtl --action free /lance-datasets/**/_versions/ 0
```

### 10.3 性能预期

| 场景 | COS 直连 | GooseFS gRPC（热） |
|------|---------|-------------------|
| 单次 4MB 读取 | ~200ms | ~3ms (-98.5%) |
| 向量搜索 Top-K | ~2s | ~80ms |
| 批量训练加载 | ~60s/epoch | ~6s/epoch |

### 10.4 风险应对

| 风险 | 概率 | 应对 |
|------|------|------|
| gRPC 流控复杂度高 | 高 | 先实现简化版（无流控 ACK），再迭代优化 |
| GooseFS 协议变更 | 中 | Proto 版本锁定 + 兼容性测试 |
| Kerberos 认证 | 中 | 初期跳过，使用无认证模式 |
| 缓存一致性 | 中 | manifest 路径 TTL=0 |

---

## 11. 实施路线图

```
Phase 2a — GooseFS Rust Client (gRPC) [核心]
Week 1-3:
├── Day 1-2:  整理 GooseFS proto 定义，配置 tonic-build
├── Day 3-5:  实现 MasterClient
├── Day 6-7:  实现 BlockMapper
├── Day 8:    实现 WorkerRouter
├── Day 9-11: 实现 WorkerClient + GrpcBlockReader
├── Day 12-13: 实现 GrpcBlockWriter
└── Day 14:   单元 + 集成测试

Phase 2b — OpenDAL GooseFS Service
Week 4:
├── Day 1-2: GooseFsBackend (impl Access)
├── Day 3:   GooseFsReader / GooseFsWriter / GooseFsLister
├── Day 4:   集成测试
└── Day 5:   提交 PR 到 apache/opendal

Phase 2c — Lance GooseFS DAL Provider
Week 5:
├── Day 1: GooseFsDalStoreProvider (~100 行)
├── Day 2: 注册 scheme + Commit Handler
├── Day 3: 端到端测试
├── Day 4: Python/Java bindings 适配
└── Day 5: 提交 PR 到 lance-format/lance
```

---

> **文档结束**
