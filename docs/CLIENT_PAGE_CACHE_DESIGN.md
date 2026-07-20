# Rust Client SDK 本地 Page Cache 开发设计文档

> 状态：**已实现（P0–P3）** · 分支：`feature/local-page-cache`
> 作者：（待补充）· 最近更新：2026-06-16
> 参考实现：GooseFS Java Client `com.qcloud.cos.goosefs.client.file.cache.*`
> 目标仓库：`goosefs-client-rust`（crate `goosefs-sdk`）

> **实现状态摘要**：本设计的 P0–P3 已在 `src/cache/` 落地并通过单元测试。
> 与设计相比的主要差异：（1）evictor 与 meta 为**自研实现**（未引入 `moka`/`lru`），
> 仅依赖 `async-trait`；（2）并发模型采用「单 `Mutex<Inner>` 守护索引/反查索引/
> 每目录计费 + 1024 条带 page 锁」，磁盘 IO 始终在 `Inner` 锁外执行（详见 §5.9 / 
> `manager.rs` 模块文档）；（3）`file_id` 直接取 `URIStatus.file_id`（服务端 inode）
> 的字符串形式；覆盖写通过 `on_file_open` 比对 `(length, last_modification_time_ms)` 
> 感知并失效。各模块实现状态见 §13。

---

## 1. 背景与目标

### 1.1 背景

GooseFS Java 客户端内置了一套**客户端本地 page cache**（client-side local cache），它把远端读取的数据按固定大小的"页（page）"为单位缓存到本地磁盘，后续重复读取直接命中本地缓存，避免重复走 Worker / UFS。该机制对以下场景收益显著：

- AI 训练 / 数据分析的重复 epoch 读取；
- Parquet / ORC 等列存的随机小 IO；
- 热点小文件的高并发读取。

目前 Rust 客户端 SDK（`goosefs-sdk`）**完全没有客户端本地缓存层**：每次读取都经过

```text
GoosefsFileReader / GoosefsFileInStream
  → MasterClient.get_status(path)          // 元数据 + block_ids
  → WorkerRouter.select_worker(block_id)   // 一致性哈希
  → WorkerClientPool.acquire(addr)
  → GrpcBlockReader (gRPC ReadBlock 双向流)
```

本设计的目标是在 Rust SDK 中实现与 Java 客户端**功能对齐**的本地 page cache，并对齐其配置项（`goosefs.user.client.cache.*`）与 metrics（`Client.Cache*`）语义。

### 1.2 目标（Goals）

1. 在 Rust SDK 中实现一个可插拔的本地 page cache 层，缓存单位为固定大小 page。
2. 提供 `CacheManager` 抽象 + `LocalCacheManager` 默认实现（本地磁盘后端）。
3. 支持 LRU / LFU 淘汰策略、多缓存目录、容量配额、异步回填（async cache）。
4. 与现有读路径（`GoosefsFileInStream` / `GoosefsFileReader`）无侵入式集成，可通过配置开关启停。
5. 对齐 Java 的配置项命名与默认值，对齐 `Client.Cache*` metrics。
6. 通过 Python binding 暴露开关与配置。
7. 进程重启后可从磁盘恢复（restore）已有缓存（可作为 P2 阶段）。

### 1.3 非目标（Non-Goals）

- 不实现服务端（Worker）的 page cache（Worker 已有独立的 `BlockPageMetaStore`）。
- 首版不实现 RocksDB / 内存后端，只实现 `LocalPageStore`（本地文件），但抽象需为后续扩展预留。
- 不实现写缓存（write-back / write-through cache），首版聚焦**读缓存**。
- 不实现跨进程共享缓存（多进程共享磁盘目录）的强一致协调，首版按单进程独占目录处理。

### 1.4 Data-Consistency Invariants (INV-PC-*)

Page cache is best-effort by design (see §9): any internal failure is
swallowed and the read falls back to the external source. "Best-effort"
however does **not** weaken the byte-level contract — the reader must
always observe the exact bytes the worker / UFS would have served. The
following hard invariants make that contract testable and gate every
release.

They mirror the structure used by `SHORT_CIRCUIT_DESIGN.md` §1.3
(`INV-D*` data-plane / `INV-S*` semantic). Every invariant maps to a
gating-grade test case in `tests/page_cache_consistency.rs` (§12.5).

| ID | Invariant | Test case |
|---|---|---|
| **INV-PC-D1** | Cache-on and cache-off paths return byte-for-byte identical data on every page / chunk / block / tail boundary, on both cold-miss and warm-hit reads. | `inv_pc_d1_cache_vs_direct_byte_diff` |
| **INV-PC-D2** | The three public read APIs on `GoosefsFileInStream` (`read` sequential, `read_at` positioned, `read_all` whole-file) return identical bytes for the same logical input under cache-on. | `inv_pc_d2_read_apis_are_equivalent` |
| **INV-PC-S1** | When the cache layer fails — unwritable cache directory, store-write rejection, async-fill queue exhaustion — the next `get` either misses cleanly or serves correct bytes; it must never return stale or torn data. | `inv_pc_s1_failed_fill_does_not_poison_cache` |
| **INV-PC-S2** | Cached pages survive process restart only when `(file_id, length, last_modification_time_ms)` is unchanged; on overwrite, `on_file_open` invalidates them before the first read so no stale bytes are served. | `inv_pc_s2_restart_byte_parity` |

Lower-level invariants (page-store atomic rename, evictor ordering, TTL
lazy expiry, benign racing) are exercised by the in-tree unit tests
listed in §12 item 1 and are not duplicated at the gating tier.

---

## 2. Java 端实现参考

> 源码根目录：`/opt/sourcecode/cos/goosefs/core/client/fs/src/main/java/com/qcloud/cos/goosefs/client/file/cache/`

### 2.1 组件总览

```text
LocalCacheFileSystem (FileSystem 装饰器)
   └── openFile() → LocalCacheFileInStream (集成缓存的读流)
                       └── mCacheManager.get()/put()
                              │
                    CacheManager.Factory (单例)
                       └── NoExceptionCacheManager  (异常吞噬包装层)
                              └── LocalCacheManager  (核心协调者)
                                     ├── PageMetaStore       (元数据 + 索引 + 淘汰协调)
                                     │      ├── IndexedSet<PageInfo>  (pageId/fileId 双索引)
                                     │      └── List<PageStoreDir>
                                     │             ├── PageStore (LocalPageStore: 实际磁盘 IO)
                                     │             ├── CacheEvictor (LRU/LFU)
                                     │             └── 字节计费 (QuotaManagedPageStoreDir)
                                     ├── Allocator (多 dir 分配, HashAllocator)
                                     ├── ReadWriteLock[1024]   (页级条带锁)
                                     └── 线程池 (async write / async restore / TTL)
```

### 2.2 关键类与职责

| 类 | 职责 |
|---|---|
| `CacheManager`（接口） | 顶层接口：`put/get/delete/append/invalidate`，含 `Factory` 单例创建 + `State` 枚举（`NOT_IN_USE`/`READ_ONLY`/`READ_WRITE`） |
| `LocalCacheManager` | 核心实现：协调 metaStore + pageStore + evictor + 页级锁 + 异步线程池 |
| `NoExceptionCacheManager` | 包装层，吞掉所有异常使缓存"尽力而为"（best-effort），缓存故障绝不影响正确性 |
| `CacheManagerOptions` | 从配置读取所有缓存参数 |
| `PageId` | 页标识 `(fileId: String, pageIndex: long)` |
| `PageInfo` | 页元数据：pageId、page 大小、scope、所属 dir、创建时间 |
| `PageStore`（接口） | 存储后端抽象：`put/get/delete`；`open()` 工厂方法 |
| `LocalPageStore` | 本地磁盘实现（唯一实际后端） |
| `PageStoreDir` / `LocalPageStoreDir` | 单个缓存目录抽象，持有 pageStore + evictor + 容量计费、目录扫描/恢复 |
| `QuotaManagedPageStoreDir` | 字节计费、reserve/release、临时文件管理 |
| `PageMetaStore` / `DefaultPageMetaStore` | 页元数据与索引（`PageId → PageInfo`），与淘汰器协调 |
| `CacheEvictor` / `LRUCacheEvictor` / `LFUCacheEvictor` | 淘汰策略 |
| `Allocator` / `HashAllocator` | 多目录时按 fileId hash 选 dir |
| `LocalCacheFileInStream` | 集成缓存的读流：命中读 cache，未命中走 external 并回填 |

### 2.3 关键方法签名（Java）

```java
// CacheManager / LocalCacheManager
boolean put(PageId pageId, ByteBuffer page, CacheContext cacheContext);
int     get(PageId pageId, int pageOffset, int bytesToRead,
            PageReadTargetBuffer buffer, CacheContext cacheContext);
boolean delete(PageId pageId, CacheContext cacheContext);
void    invalidate(Predicate<PageInfo> predicate);
```

### 2.4 读路径核心逻辑（`LocalCacheFileInStream`）

读 `[position, position+length)` 时：

1. 计算覆盖的 page 区间：`startPage = position / pageSize` … `endPage`。
2. 对每个 page：
   - 计算 `pageId = (fileId, pageIndex)`，`pageOffset = position % pageSize`；
   - 调用 `mCacheManager.get(pageId, pageOffset, bytesToRead, buffer)`：
     - **命中**：直接从本地 page store 拷贝到目标 buffer，累加 `BytesReadCache`；
     - **未命中**：从 external（Worker/UFS）读取整页，拷贝需要的片段给调用方，并**异步/同步**调用 `put()` 回填整页，累加 `BytesReadExternal`。
3. `mPageSize`、`mBufferSize` 来自配置。

### 2.5 锁层级（重要）

`LocalCacheManager` 的所有页操作严格遵循以下顺序，Rust 实现需对齐：

```text
1. 获取对应 page 的条带锁（page lock，按 pageId hash 到 1024 个 RwLock 之一）
2. 获取 metastore 锁（mMetaLock）
3. 更新 metastore（索引 / evictor 状态）
4. 释放 metastore 锁
5. 更新 page store（实际磁盘 IO）与 evictor
6. 释放 page lock
```

### 2.6 配置项（Java `PropertyKey.USER_CLIENT_CACHE_*`）

| 配置 key | 含义 | 默认值（参考） |
|---|---|---|
| `goosefs.user.client.cache.enabled` | 是否启用本地缓存 | `false` |
| `goosefs.user.client.cache.page.size` | page 大小 | `1MB` |
| `goosefs.user.client.cache.size` | 单目录缓存容量 | `512MB` |
| `goosefs.user.client.cache.dirs` | 缓存目录列表 | `/tmp/goosefs_cache` |
| `goosefs.user.client.cache.store.type` | 后端类型 | `LOCAL` |
| `goosefs.user.client.cache.eviction.policy`（evictor class） | 淘汰策略 | `LRU` |
| `goosefs.user.client.cache.async.write.enabled` | 是否异步回填 | `true` |
| `goosefs.user.client.cache.async.write.threads` | 异步回填线程数 | `16` |
| `goosefs.user.client.cache.in.stream.buffer.size` | 读流缓冲 | `0`（关闭） |
| `goosefs.user.client.cache.quota.enabled` | 是否启用配额 | `false` |
| `goosefs.user.client.cache.ttl.enabled` / `.ttl` | 页 TTL | `false` |

> 注：以上默认值以仓库实际 `PropertyKey.java` 为准，落地前需逐项核对。

### 2.7 Metrics（`MetricKey.Client.Cache*`）

用户给出的完整 metrics 清单见附录 A，核心几类：

- 命中/穿透字节：`CacheBytesReadCache`、`CacheBytesReadExternal`、`CacheBytesRequestedExternal`、`CacheBytesReadInStreamBuffer`；
- 命中率：`CacheHitRate`；
- 容量：`CacheSpaceAvailable`、`CacheSpaceUsed`、`CacheSpaceUsedCount`、`CachePages`；
- 淘汰：`CacheBytesEvicted`、`CachePagesEvicted`、`CacheBytesDiscarded`、`CachePagesDiscarded`；
- 写入：`CacheBytesWrittenCache`；
- 时延：`CachePageReadCacheTimeNanos`、`CachePageReadExternalTimeNanos`；
- 各类错误计数：`CacheGetErrors`、`CachePutErrors`、`CacheDeleteErrors`、`CachePut*Errors`、`CacheStore*Timeout` 等；
- 状态：`CacheState`、`FallbackState`。

---

## 3. Rust Client 现状与集成点

### 3.1 现有读路径

| 模块 | 文件 | 说明 |
|---|---|---|
| 高层读编排 | `src/io/file_reader.rs` (`GoosefsFileReader`) | 端到端读管线，`open_with_context` / `read_all` / `read_next_block` / `read_range_*` |
| 可 seek 流 | `src/io/file_in_stream.rs` (`GoosefsFileInStream`) | 双路径：顺序读 `read()`（block_in_stream）+ 随机读 `read_at(offset, n)`（positioned_read） |
| 单块流读 | `src/io/reader.rs` (`GrpcBlockReader`) | gRPC `ReadBlock` 双向流、prefetch、ACK 合流 |
| Worker 客户端 | `src/client/worker.rs` (`WorkerClient` / `WorkerClientPool`) | 读数据最终源头 |
| 上下文 | `src/context.rs` (`FileSystemContext`) | 连接池 / Worker 管理生命周期，**page cache 的最佳挂载点** |
| 配置 | `src/config.rs` (`GoosefsConfig`) | 配置 struct + `ENV_*` / `STORAGE_OPT_*` 常量 |
| metrics | `src/metrics/registry.rs` | 全局 `Counter`/`Gauge`（`AtomicI64` + `DashMap`），`name` 子模块定义常量 |
| 错误 | `src/error.rs` (`Error` / `Result`) | 统一 `thiserror` 错误枚举 |

关键读方法签名：

```rust
// src/io/file_in_stream.rs
impl GoosefsFileInStream {
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;       // 顺序
    pub async fn read_at(&mut self, offset: i64, n: usize) -> Result<Bytes>; // 随机
    pub async fn seek(&mut self, pos: i64) -> Result<i64>;
}
```

### 3.2 集成点选择（已落地）

**集成点：`GoosefsFileInStream` 的 `read_at`（随机读）与 `read`（顺序读）均已接入缓存。**

1. 随机读语义天然按 offset 切页，最贴合 page cache 模型，`read_at` 是首要集成点；
2. 顺序读 `read()` 在 P2 已复用同一缓存查询：每次调用经 `read_at_cached(self.pos, end)` 满足一段并推进 `pos`（对齐 Java `LocalCacheFileInStream` 让所有读都过缓存）。

集成策略：`GoosefsFileInStream` 持有可选的 `Arc<dyn CacheManager>`（由 `FileSystemContext::acquire_cache_manager` 注入），并对集成层实现 `ExternalRangeReader`（回源走现有 worker/UFS positioned-read）。读取时：

```text
read_at(offset, n) / read(buf):
    if cache 存在:
        read_through_cache(cache, self /*ExternalRangeReader*/, file_id,
                           page_size, file_length, offset, end, fill_mode):
            按 page 拆分 [offset, end)
            for each page:
                cache.get(page) ──命中──> 拷贝片段
                                └─未命中─> ext.read_range 整页 → 返回片段
                                           + 按 FillMode 回填(Sync/Async/None)
    else:
        走原有 read_external_range / positioned_read 路径
```

> `fill_mode` 由 `cache_fill`（是否回填）与 `cache_async_write`（同步/异步）决定，映射为
> `FillMode::{None, Sync, Async}`（见 §6.2）。`ReadType` 语义的精细化（仅特定读类型才回填）
> 列为后续增强（§14.1）。

---

## 4. 整体架构设计

```text
                 FileSystemContext
                       │ (持有 Option<Arc<dyn CacheManager>>; acquire_cache_manager)
                       ▼
   GoosefsFileReader / GoosefsFileInStream
                       │ read_at / read  (impl ExternalRangeReader)
                       ▼
        ┌──────────────────────────────────┐
        │  read_through_cache() + FillMode  │   ← 集成层(无状态函数)：page 拆分 + 命中判定 + 回填
        └──────────────────────────────────┘
                       │
            ┌──────────┴───────────┐
       cache.get()              ext.read_range()（未命中整页回源）
            │                        │  (复用现有 GrpcBlockReader)
            ▼                        ▼
   ┌─────────────────┐      WorkerClientPool → ReadBlock
   │  CacheManager   │ (trait; DisabledCacheManager 关闭态)
   │  └ LocalCache   │ (默认实现)
   │     Manager     │
   └─────────────────┘
            │
   ┌────────┴───────────────────┬──────────────────┐
   ▼                            ▼                  ▼
 Mutex<Inner>               Vec<LocalPageStore>   Allocator
 ├ meta (索引)              (磁盘 IO: 临时文件      (HashAllocator)
 ├ by_file (反查)            + 原子 rename)
 ├ versions (覆盖写检测)
 └ dirs: Vec<DirState>
      └ evictor (Lru/Lfu) + used_bytes/capacity 计费
   + page_locks: [RwLock; 1024]   (页级条带锁)
   + async_write_sem              (异步回填限流)
```

> 说明：`Mutex<Inner>` 仅守护内存元数据/计费/evictor（短临界区）；`LocalPageStore` 的磁盘 IO
> 与淘汰文件删除均在锁外执行。详见 §5.3 / §5.9。

### 4.1 模块布局（实际落地）

```text
src/cache/
  ├── mod.rs              # 导出 + CacheManager trait + CacheState 枚举 + DisabledCacheManager
  ├── page_id.rs          # PageId / PageInfo / CacheScope
  ├── manager.rs          # LocalCacheManager（核心协调：索引 + 计费 + 锁 + TTL + restore）
  ├── options.rs          # CacheManagerOptions（从 GoosefsConfig 读取，5% overhead）
  ├── evictor/
  │     ├── mod.rs        # CacheEvictor trait + build_evictor()
  │     ├── lru.rs        # LruCacheEvictor
  │     └── lfu.rs        # LfuCacheEvictor
  ├── store/
  │     ├── mod.rs        # PageStore trait
  │     └── local.rs      # LocalPageStore（本地文件：临时文件 + 原子 rename）
  ├── allocator.rs        # Allocator trait + HashAllocator
  ├── metrics.rs          # cache 专用 metrics 名称常量（name 子模块）
  └── caching_reader.rs   # read_through_cache / FillMode / ExternalRangeReader：与 file_in_stream 的集成层
```

> **与初版设计的差异**（实现时简化/合并）：
> - **`noop.rs` → `mod.rs::DisabledCacheManager`**：best-effort「吞异常」语义不再需要独立包装层，
>   `CacheManager` 各方法本身即返回 `bool`/`usize`（不返回 `Result`），错误在 `LocalCacheManager`
>   内部就地吞掉并计 `*Errors` metric；缓存关闭时用 `DisabledCacheManager`（always-miss）。
> - **`meta_store.rs`（`PageMetaStore`/`DefaultPageMetaStore`）→ `manager.rs::Inner`**：索引、
>   `file_id` 反查索引、每目录 evictor + 字节计费、文件版本表统一收敛进单个 `Mutex<Inner>`，
>   避免多层锁嵌套（详见 §5.9 / §10.1）。
> - **`store/dir.rs`（`LocalPageStoreDir`/`QuotaManagedPageStoreDir`）→ `manager.rs::DirState`**：
>   每目录容量/计费/淘汰协调内联到 `Inner.dirs`，`PageStore` 仅保留纯磁盘 IO 抽象。
> - **集成层非结构体**：未引入 `CachingPositionReader` 结构体，改为无状态函数
>   `caching_reader::read_through_cache(...)` + `FillMode` 枚举，便于离线单测（见 §6.1）。

---

## 5. 核心模块详细设计

### 5.1 `PageId` / `PageInfo`

```rust
// src/cache/page_id.rs

/// 页标识：等价 Java PageId(fileId, pageIndex)。
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PageId {
    /// 文件唯一标识。Java 用 String（通常是 fileId 或 path hash）。
    /// Rust 侧建议用 file_id(i64) 的字符串形式或路径派生的稳定 hash。
    pub file_id: Arc<str>,
    /// 页索引 = offset / page_size。
    pub page_index: u64,
}

/// 页元数据。
#[derive(Clone, Debug)]
pub struct PageInfo {
    pub page_id: PageId,
    /// 页内实际字节数（最后一页可能 < page_size）。
    pub page_size: u64,
    /// 所属缓存目录索引。
    pub dir_index: usize,
    /// 创建时间（用于 TTL）。
    pub created_at: std::time::Instant,
    /// 配额 scope（首版可为 Global）。
    pub scope: CacheScope,
}
```

> `file_id` 来源：优先使用 `URIStatus` 中的稳定标识（如 `file_id` / `mount_id + ufs_path`）。需保证同一文件多次打开得到相同 `file_id`，否则缓存无法跨流命中。

### 5.2 `CacheManager` trait

```rust
// src/cache/mod.rs

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CacheState { NotInUse, ReadOnly, ReadWrite }

#[async_trait::async_trait]
pub trait CacheManager: Send + Sync {
    /// 写入（回填）一整页。返回是否成功缓存。
    async fn put(&self, page_id: &PageId, page: Bytes) -> bool;

    /// 调度一次不阻塞调用方的 best-effort 回填（默认 spawn；
    /// LocalCacheManager 覆写以走 Semaphore 限流的异步写回池）。
    fn schedule_fill(self: Arc<Self>, page_id: PageId, page: Bytes) where Self: 'static;

    /// 读取页内 [offset, offset+len) 到 dst，返回实际读取字节数。
    /// 未命中返回 0（不报错，调用方据此回源）。
    async fn get(&self, page_id: &PageId, page_offset: usize, dst: &mut [u8]) -> usize;

    /// 删除一页。
    async fn delete(&self, page_id: &PageId) -> bool;

    /// 失效某文件全部页（如文件被覆盖/删除时）。
    async fn invalidate(&self, file_id: &str);

    /// 通知缓存某文件被 (重新) 打开；比对 (length, mtime) 检测覆盖写，
    /// 不一致则失效该文件全部页。默认 no-op。
    async fn on_file_open(&self, _file_id: &str, _length: i64, _last_modification_time_ms: i64) {}

    fn state(&self) -> CacheState;
}
```

设计要点：
- `get` **不返回 `Result`**：缓存是 best-effort，未命中即返回 0，错误内部吞掉并记 metric（对齐 Java `NoExceptionCacheManager`；缓存关闭时用 `DisabledCacheManager`）。
- 用 `bytes::Bytes` 传整页，零拷贝友好。
- 依赖 `async-trait`（已在 `Cargo.toml`）。

### 5.3 `LocalCacheManager`

核心字段（已落地）：

```rust
pub struct LocalCacheManager {
    options: CacheManagerOptions,
    /// 每个缓存目录一个 page store（不可变；IO 在 inner 锁外执行）。
    stores: Vec<LocalPageStore>,
    allocator: Box<dyn Allocator>,
    /// 单一元数据锁：索引 + 反查索引 + 版本表 + 每目录 evictor/计费。
    inner: Mutex<Inner>,
    /// 页级条带锁：LOCK_SIZE = 1024 个 RwLock，按 PageId hash 选择。
    page_locks: Vec<RwLock<()>>,
    /// 异步写回许可（容量 = async_write_threads）。
    async_write_sem: Arc<Semaphore>,
    state: CacheState,
}
```

**`get` 流程**（对齐 §2.5 锁层级，含 TTL 惰性过期）：

```text
1. rl = page_locks[hash(page_id) % 1024].read()
2. 进入 inner 锁：
     - 若 is_expired(page_id) → 移除元数据 + evictor.on_remove + 扣计费，
       记 CachePagesDiscarded/CacheBytesDiscarded，返回 0（miss）
     - 否则取 page_info.dir_index；不存在 → 返回 0（miss）
   释放 inner 锁
3. n = stores[dir_index].get(page_id, page_offset, dst).await   // 锁外磁盘 IO
       └─ 读失败 → 记 GetStoreReadErrors，视为 miss；n==0 → 视为 racy eviction miss
4. 进入 inner 锁：evictor.on_access(page_id)；释放
5. metrics: BytesReadCache += n; PageReadCacheTimeNanos += 耗时
6. 返回 n
```

**`put` 流程**：

```text
1. wl = page_locks[hash(page_id)].write()
2. dir_index = allocator.allocate(page_id, stores.len())
3. 进入 inner 锁：
     - 已存在同页 → benign racing，记 PutBenignRacingErrors，返回 false
     - while used_bytes + page_len > capacity：pop_victim（取候选→移元数据→记 evicted），
         无候选 → 记 PutInsufficientSpaceErrors，返回 false
     - 试预留 used_bytes += page_len
   释放 inner 锁
4. 锁外删除被淘汰受害者的盘文件（best-effort）
5. stores[dir_index].put(page_id, &page).await   // 锁外磁盘 IO
       失败 → 进 inner 回滚预留，记 PutStoreWriteErrors，返回 false
6. 进 inner 锁：插入 meta + by_file + evictor.on_add；BytesWrittenCache += page_len；刷新占用 gauge
7. 返回 true
```

**异步回填**：`schedule_fill` 用 `Semaphore::try_acquire_owned` 限流（容量 = `async_write_threads`），获得许可则 `tokio::spawn` 调 `put`，许可耗尽则拒绝并记 `CachePutAsyncRejectionErrors`（对齐 Java `SynchronousQueue` + 拒绝策略）。

### 5.4 `PageStore` / `LocalPageStore`

```rust
// src/cache/store/mod.rs
#[async_trait::async_trait]
pub trait PageStore: Send + Sync {
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()>;
    /// 读取 [offset, offset+dst.len())，返回读取字节数。
    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize>;
    async fn delete(&self, page_id: &PageId) -> Result<()>;
}
```

`LocalPageStore` 磁盘布局（对齐 Java）：

```text
<cache_dir>/<page_size>/<bucket>/<file_id>/<page_index>
                          │
                          └── bucket = hash(file_id) % NUM_BUCKETS  (默认 1000，避免单目录文件过多)
```

- 写：先写临时文件 `*.tmp`，`fsync` 后原子 `rename`（对齐 Java commit/abort 语义）。
- 读：`File::open` + `seek(offset)` + `read`。建议用 `tokio::fs` 或 `spawn_blocking` 包裹同步 IO（磁盘 IO 不应阻塞 tokio worker 线程）。
- 删：`remove_file`，忽略 `NotFound`（记 `CacheDeleteNonExistingPageErrors`）。

### 5.5 元数据与索引（`manager.rs::Inner`）

职责：维护 `PageId → PageInfo` 索引，支持按 `file_id` 反查（用于 `invalidate`），与 evictor 协调。

> **实现说明**：初版设计的独立 `PageMetaStore`/`DefaultPageMetaStore` 抽象在落地时被收敛进
> `LocalCacheManager` 内部的单 `Mutex<Inner>`，避免「meta 锁 + dir 锁 + evictor 锁」的多层
> 嵌套与一致性负担（见 §10.1）。`Inner` 字段如下：

```rust
struct Inner {
    /// PageId → PageInfo 主索引。
    meta: HashMap<PageId, PageInfo>,
    /// file_id → set(page_index) 反查索引（用于 invalidate）。
    by_file: HashMap<Arc<str>, HashSet<u64>>,
    /// file_id → (length, last_modification_time_ms) 已知版本，
    /// 用于在 (重新) 打开时检测覆盖写并失效陈旧页。
    versions: HashMap<Arc<str>, (i64, i64)>,
    /// 每目录 evictor + 字节计费。
    dirs: Vec<DirState>,   // DirState { evictor, used_bytes, capacity }
}
```

- 主索引：`HashMap<PageId, PageInfo>`（在 `Mutex` 内，非 `DashMap`——因索引/计费/淘汰需在同一临界区原子更新）。
- 反查索引：`HashMap<Arc<str>, HashSet<u64>>`，等价 Java `IndexedSet` 的双索引。
- 占用量由 `Inner.dirs[*].used_bytes` 求和驱动 `CachePages` / `CacheSpaceUsed` / `CacheSpaceAvailable` gauge（`publish_occupancy`）。

### 5.6 `CacheEvictor`（LRU / LFU）

```rust
// src/cache/evictor/mod.rs
pub trait CacheEvictor: Send + Sync {
    fn on_add(&self, id: &PageId);
    fn on_access(&self, id: &PageId);     // get 命中时触摸
    fn on_remove(&self, id: &PageId);
    /// 返回下一个应被淘汰的页。
    fn evict_candidate(&self) -> Option<PageId>;
}

pub fn build_evictor(kind: CacheEvictorType) -> Box<dyn CacheEvictor>;
```

> **实现说明（已落地，自研）**：未引入 `moka`/`lru`。每个缓存目录持有一个独立的 `Box<dyn CacheEvictor>`
> （存于 `DirState.evictor`），其内部状态变更均在 `Inner` 锁的临界区内发生，无需 evictor 自带锁。
> - `LruCacheEvictor`（`evictor/lru.rs`）：访问顺序队列，`on_access` 移到队尾，`evict_candidate` 取队首。
> - `LfuCacheEvictor`（`evictor/lfu.rs`）：频率计数，淘汰最低频页。

### 5.7 `Allocator`（多目录）

```rust
// src/cache/allocator.rs
pub trait Allocator: Send + Sync {
    fn allocate(&self, page_id: &PageId, num_dirs: usize) -> usize;
}
```

`HashAllocator`：`hash(file_id) % num_dirs`，保证同一文件的页集中在同一目录（对齐 Java `AffinityHashAllocator`，利于按文件失效/恢复）。

### 5.8 容量配额与计费（`DirState`）

> **实现说明**：每目录的容量与计费内联在 `Inner.dirs[i]: DirState`，而非独立 `PageStoreDir` 结构。

每个 dir 维护：
- `capacity`（来自 `cache.size`，减去 5% overhead，对齐 `PageStoreType.LOCAL` overhead，见 `options.rs`）；
- `used_bytes: u64`（在 `Inner` 锁内更新，非原子型——临界区已串行化）；
- `put` 时若 `used_bytes + page_len > capacity`，循环 `pop_victim`（取 evictor 候选 → 移除元数据 → 锁外删盘文件）直到腾出空间；腾不出则记 `CachePutInsufficientSpaceErrors` 并返回 false。
- 写盘前先「试预留」`used_bytes += page_len`（让并发 put 看到空间已占用），写盘失败再回滚。

### 5.9 并发与锁（已落地）

- **页级条带锁**：`Vec<tokio::sync::RwLock<()>>`，长度 `LOCK_SIZE = 1024`，按 `hash(page_id) % 1024` 选择；`get` 取读锁，`put`/`delete` 取写锁。同页操作串行，异页并发。
- **元数据锁**：单个 `Mutex<Inner>`，仅守护内存索引/反查索引/版本表/每目录 evictor + 计费；**绝不跨磁盘 IO 持有**——所有 `PageStore` 读写、淘汰文件删除均在 `Inner` 锁释放后执行，保证读写可扩展。
- 加锁顺序：**page lock → Inner 锁（短临界区）→ 释放 Inner → 磁盘 IO**，避免死锁；淘汰受害者的元数据先在 `Inner` 内移除，其磁盘文件在锁外删除（Unix 下 inode 在 fd 关闭前存活，并发 `get` 仍可完成）。

---

## 6. 读路径集成

### 6.1 `read_through_cache`（集成层）

集成层（`src/cache/caching_reader.rs`）封装"page 拆分 + 命中判定 + 回源 + 回填"。

> **实现说明**：未引入 `CachingPositionReader` 结构体，改为**无状态函数** + `ExternalRangeReader`
> trait + `FillMode` 枚举，便于离线单测（用 `FakeExternal` 注入回源）。`GoosefsFileInStream`
> 实现 `ExternalRangeReader`，把回源委托给现有 worker/UFS positioned-read 路径。

```rust
// src/cache/caching_reader.rs

/// 回源抽象：由 file_in_stream 在其 worker/UFS positioned-read 路径上实现。
#[async_trait::async_trait]
pub trait ExternalRangeReader {
    /// 读取 [offset, end)，仅在 EOF 时可返回更少字节。
    async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes>;
}

/// 未命中页的回填方式。
pub enum FillMode { None, Sync, Async }

/// 通过 page cache 服务 [offset, end)。
/// 逐页 cache.get；未命中则整页 read_range 回源并按 fill_mode 回填。
/// best-effort：缓存错误降级为外部读，绝不变成失败。
pub async fn read_through_cache<R: ExternalRangeReader + ?Sized>(
    cache: &Arc<dyn CacheManager>,
    ext: &mut R,
    file_id: &Arc<str>,
    page_size: u64,
    file_length: i64,
    offset: i64,
    end: i64,
    fill_mode: FillMode,
) -> Result<Bytes>;
```

回源命中/未命中分支同时记 `CacheBytesReadExternal` / `CacheBytesRequestedExternal`；`FillMode::Async`
走 `schedule_fill`（受 `Semaphore` 限流，满则记 `CachePutAsyncRejectionErrors`），`Sync` 则 `await put`。

### 6.2 `GoosefsFileInStream` 改动（已落地）

- 新增字段：`cache: Option<Arc<dyn CacheManager>>`、`cache_page_size`、`cache_file_id: Arc<str>`、`cache_fill: bool`、`cache_async_write: bool`（构造时由 `FileSystemContext` 注入；legacy `open()` 全部为 `None`/默认）。
- **随机读** `read_at`：`cache.is_some()` 时走 `read_at_cached`（内部调用 `read_through_cache`），否则走 `read_external_range`（即原 cache-less 实现，被抽出作为 miss 回源源）。
- **顺序读** `read()`：已接缓存（P2）——`cache.is_some()` 时每次调用经 `read_at_cached(self.pos, end)` 满足一段并推进 `pos`。
- `cache_fill_mode()` 依据 `cache_fill` + `cache_async_write` 映射为 `FillMode::{None,Sync,Async}`。
- `ExternalRangeReader for GoosefsFileInStream::read_range` 即回源入口。

### 6.3 `FileSystemContext` 改动（已落地）

- `connect()` 时若 `config.client_cache_enabled == true`，构造 `LocalCacheManager::from_config(...)` 并以 `Option<Arc<dyn CacheManager>>` 持有（best-effort：init 失败降级为 no-cache，不影响 `connect()`）；通过 `acquire_cache_manager()` 暴露给各 reader 共享。
- 打开文件时（`open_with_context`）注入 cache + `cache_file_id`（来自 `URIStatus.file_id`），并调用 `cache.on_file_open(file_id, length, last_modification_time_ms)` 感知覆盖写。
- 重启恢复由 `LocalCacheManager::create` 内的 `restore()` 完成；TTL sweeper 在 `from_config` 时按需 spawn（持 `Weak<Self>`，manager drop 后自动退出）。

---

## 7. 配置项设计

在 `src/config.rs` 的 `GoosefsConfig` 中新增字段（对齐 Java `USER_CLIENT_CACHE_*`，并补 `ENV_*` / `STORAGE_OPT_*` 常量）：

```rust
pub struct GoosefsConfig {
    // ... existing ...

    // ── Client local page cache ──────────────────────────────
    /// 是否启用客户端本地 page cache（默认 false）。
    #[serde(default)]
    pub client_cache_enabled: bool,
    /// page 大小（字节），默认 1 MiB。
    #[serde(default = "default_cache_page_size")]
    pub client_cache_page_size: u64,
    /// 每个缓存目录容量（字节），默认 1 GiB。与 dirs 一一对应或统一值。
    #[serde(default = "default_cache_size")]
    pub client_cache_size: u64,
    /// 缓存目录列表，默认 ["/tmp/goosefs_cache"]。
    #[serde(default = "default_cache_dirs")]
    pub client_cache_dirs: Vec<String>,
    /// 淘汰策略：LRU / LFU，默认 LRU。
    #[serde(default = "default_cache_evictor")]
    pub client_cache_evictor: CacheEvictorType,
    /// 是否异步回填，默认 true。
    #[serde(default = "default_true")]
    pub client_cache_async_write_enabled: bool,
    /// 异步回填并发数，默认 16。
    #[serde(default = "default_cache_async_write_threads")]
    pub client_cache_async_write_threads: usize,
    /// 是否启用配额，默认 false。
    #[serde(default)]
    pub client_cache_quota_enabled: bool,
    /// 页 TTL（秒），0 表示不过期。
    #[serde(default)]
    pub client_cache_ttl_secs: u64,
}
```

对应常量（命名沿用现有风格）：

```rust
// ENV
pub const ENV_CLIENT_CACHE_ENABLED: &str   = "GOOSEFS_USER_CLIENT_CACHE_ENABLED";
pub const ENV_CLIENT_CACHE_PAGE_SIZE: &str = "GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE";
pub const ENV_CLIENT_CACHE_SIZE: &str      = "GOOSEFS_USER_CLIENT_CACHE_SIZE";
pub const ENV_CLIENT_CACHE_DIRS: &str      = "GOOSEFS_USER_CLIENT_CACHE_DIRS";
// ... evictor / async / quota / ttl

// storage option (用于 OpenDAL / Python kwargs)
pub const STORAGE_OPT_CLIENT_CACHE_ENABLED: &str   = "goosefs_client_cache_enabled";
pub const STORAGE_OPT_CLIENT_CACHE_PAGE_SIZE: &str = "goosefs_client_cache_page_size";
// ...
```

> 默认值需与 Java `PropertyKey.java` 实际值核对后定稿（见 §2.6 待核对项）。

---

## 8. Metrics 设计

在 `src/metrics/registry.rs` 的 `name` 模块新增 cache metrics 常量（对齐用户给出的 `Client.Cache*`，完整清单见附录 A）。优先实现以下高价值子集，其余按需补齐：

| Rust 常量（建议） | metric 名 | 类型 |
|---|---|---|
| `CLIENT_CACHE_BYTES_READ_CACHE` | `Client.CacheBytesReadCache` | counter |
| `CLIENT_CACHE_BYTES_READ_EXTERNAL` | `Client.CacheBytesReadExternal` | counter |
| `CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL` | `Client.CacheBytesRequestedExternal` | counter |
| `CLIENT_CACHE_BYTES_WRITTEN_CACHE` | `Client.CacheBytesWrittenCache` | counter |
| `CLIENT_CACHE_BYTES_EVICTED` | `Client.CacheBytesEvicted` | counter |
| `CLIENT_CACHE_PAGES` | `Client.CachePages` | gauge |
| `CLIENT_CACHE_PAGES_EVICTED` | `Client.CachePagesEvicted` | counter |
| `CLIENT_CACHE_SPACE_USED` | `Client.CacheSpaceUsed` | gauge |
| `CLIENT_CACHE_SPACE_AVAILABLE` | `Client.CacheSpaceAvailable` | gauge |
| `CLIENT_CACHE_HIT_RATE` | `Client.CacheHitRate` | gauge |
| `CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS` | `Client.CachePageReadCacheTimeNanos` | counter |
| `CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS` | `Client.CachePageReadExternalTimeNanos` | counter |
| `CLIENT_CACHE_GET_ERRORS` / `CLIENT_CACHE_PUT_ERRORS` / ... | `Client.Cache*Errors` | counter |
| `CLIENT_CACHE_STATE` | `Client.CacheState` | gauge |

- 命中率 `CacheHitRate`（已实现）：由 `BytesReadCache / (BytesReadCache + BytesReadExternal)` 计算（`metrics::publish_hit_rate`），在命中与回源两条读路径上实时刷新 gauge（非周期任务，避免常驻后台线程）。
- 现有 metrics 上报链路（`ClientMetricsReporter` → `HeartbeatTask` → Master，及可选 Pushgateway）天然复用，无需新增上报通道。

埋点位置（已落地）：
- `BytesReadCache` / `PageReadCacheTimeNanos`：`LocalCacheManager::get` 命中分支；
- `BytesReadExternal` / `BytesRequestedExternal` / `PageReadExternalTimeNanos`：`read_through_cache` 回源分支；
- `BytesWrittenCache` / `CachePages` / `CacheSpaceUsed` / `CacheSpaceUsedCount`：`put` 成功 + `publish_occupancy`；
- `BytesEvicted` / `PagesEvicted`：evictor 淘汰；
- 各 `*Errors`：对应错误分支。

---

## 9. 错误处理

- 缓存层内部错误**不向上抛**，统一吞掉并记对应 `Client.Cache*Errors` metric，回退到回源读取（对齐 Java `NoExceptionCacheManager`）。
- `src/error.rs` 可新增内部用 `Error::Cache(String)` 变体，仅用于 `PageStore` 等内部 `Result`，**不会逃逸到 SDK 公共 API**。
- 关键保证：**缓存任何故障都不得影响读取正确性**——未命中/错误一律回源。

---

## 10. 关键取舍与风险

### 10.1 自研 vs `moka`

| 方案 | 优点 | 缺点 |
|---|---|---|
| 自研 evictor + meta（贴合 Java） | 与 Java 行为 1:1 对齐，可控 | 工作量大，需自己保证并发正确性 |
| 复用 `moka`（值存 key 元数据，listener 删磁盘） | 成熟的 TinyLFU/LRU、容量/TTL 驱逐、async 友好 | 行为与 Java 略有差异；磁盘与内存元数据一致性需小心处理驱逐回调 |

**建议**：P1 用 `moka` 管理 key 元数据 + eviction listener 删磁盘文件，快速可用；若后续需要与 Java 严格对齐再切自研 evictor。需先确认 `Cargo.toml` 是否已含 `moka`，否则新增依赖。

> **最终决定（已落地）**：采用**自研** evictor（`evictor/lru.rs`、`evictor/lfu.rs`）+
> 自研内存索引（`manager.rs::Inner`），未引入 `moka`/`lru`。原因：缓存值在磁盘、元数据在
> 内存，需精确控制「淘汰元数据 → 锁外删盘文件」的顺序与一致性，自研更易与 Java 行为对齐，
> 且仅多依赖一个 `async-trait`。`PageMetaStore`/`DefaultPageMetaStore` 抽象被简化为
> `manager.rs` 内的单 `Mutex<Inner>`（主索引 + `file_id` 反查索引 + 每目录计费），磁盘 IO
> 一律在该锁外执行。

### 10.2 `file_id` 稳定性

缓存跨流命中依赖 `file_id` 稳定。需确认 `URIStatus` 是否提供稳定 `file_id`；若仅有路径，用 `path` 的稳定 hash，但要注意文件被覆盖（mtime/length 变化）时必须 `invalidate`，否则读到脏数据。**这是正确性关键风险点**，需在 `get_status` 时校验 length/mtime 与缓存元数据一致。

### 10.3 阻塞 IO

磁盘读写不能阻塞 tokio runtime；统一用 `tokio::fs` 或 `spawn_blocking`。

### 10.4 多进程共享目录

首版假设单进程独占缓存目录。多进程共享需文件锁 + 目录隔离，列为后续工作。

---

## 11. Python Binding 暴露（已落地）

Python `Config` 接受 `properties` 字典，序列化为 Java-properties 格式后交由
`GoosefsConfig::from_properties_str` 解析（见 `bindings/python/src/config.rs`）。由于该解析器
**已识别** `goosefs.user.client.cache.*` 全部键（`config.rs` 中 `from_properties_str`），缓存配置
**无需任何 binding 改动**即可从 Python 传入：

```python
from goosefs import Config

cfg = Config("m1:9200", properties={
    "goosefs.user.client.cache.enabled": "true",
    "goosefs.user.client.cache.page.size": "1MB",
    "goosefs.user.client.cache.size": "512MB",
    "goosefs.user.client.cache.dirs": "/data/gfs_cache",
    "goosefs.user.client.cache.eviction.policy": "LRU",   # or LFU
    "goosefs.user.client.cache.async.write.enabled": "true",
    "goosefs.user.client.cache.async.write.threads": "16",
    "goosefs.user.client.cache.ttl.seconds": "0",          # 0 = no expiry
})
```

可识别的属性键：`enabled` / `page.size` / `size` / `dirs` / `eviction.policy` /
`async.write.enabled` / `async.write.threads` / `quota.enabled` / `ttl.seconds`。

> 说明：`config.rs` 中另有 `ENV_CLIENT_CACHE_*`（环境变量覆盖）与
> `STORAGE_OPT_CLIENT_CACHE_*`（OpenDAL / storage-option 风格 `goosefs_client_cache_*`）两套入口；
> Python 走 properties 路径即可，无需逐键透传。⏳ 待补：Python e2e 用例验证开关透传与命中行为。

---

## 12. 测试方案

> 状态标注：✅ 已实现 · ⏳ 待补。

1. **单元测试**（随各模块就地 `#[cfg(test)]`）
   - ✅ `LocalPageStore` put/get 往返、按 offset 读、缺页返回 0、页尾短读、delete 后 miss（`store/local.rs`；写路径经临时文件 + 原子 rename，由往返用例覆盖）。
   - ✅ `LruCacheEvictor` / `LfuCacheEvictor` 淘汰顺序（`evictor/lru.rs`、`evictor/lfu.rs`）。
   - ✅ `HashAllocator` 同文件落同目录（`allocator.rs`）。
   - ✅ `LocalCacheManager`：put/get 命中、多目录往返与亲和、每目录 LRU/LFU 淘汰、`invalidate`、`schedule_fill` 异步回填、并发 put/get、benign racing（`manager.rs`）。
   - ✅ **TTL 惰性过期 + sweeper**：`get_lazily_expires_page`、`no_ttl_never_expires`、`sweep_expired_removes_all_stale_pages`（`manager.rs`）。
   - ✅ **覆盖写失效 `on_file_open`**：首次记录、length/mtime 变更失效、相同身份 no-op（`manager.rs`）。
   - ✅ `read_through_cache` 命中/未命中/回填 page 拆分（`caching_reader.rs`，用 `FakeExternal`）。
   - ✅ `CacheManagerOptions` 解析（5% overhead、TTL=0→None、清洗）（`options.rs`）。
2. **集成测试**（`tests/page_cache_e2e.rs`，`#[ignore]`，连真实集群）
   - ✅ 冷读未命中 → 回填 → 热读命中（断言 `BytesReadCache` 增长、`BytesReadExternal` 不增长、`HitRate` 已发布）：`cold_miss_then_warm_hit`。
   - ✅ 容量打满触发淘汰（断言 `PagesEvicted` 增长且内容正确）：`capacity_full_triggers_eviction`。
   - ✅ 文件覆盖后 `on_file_open` 失效不读脏数据：`overwrite_invalidates_stale_pages`。
   - ✅ 缓存目录不可写时回退回源不报错：`unwritable_cache_dir_falls_back`。
   - 运行：`GOOSEFS_AUTH_TYPE=nosasl cargo test --test page_cache_e2e -- --ignored`。
3. **基准测试**（`benchmarks/page_cache_ab.rs`，example 目标）
   - ✅ 重复读吞吐 cache on/off 对比（本机实测 ≈2.7× 加速，warm 后仅 1 page 走外部、HitRate ~97%）。
     运行：`GOOSEFS_AUTH_TYPE=nosasl cargo run --release --example page_cache_ab`。
4. **Python e2e**（`bindings/python/tests/test_page_cache.py`）
   - ✅ 缓存开关经 `Config(properties=…)` 透传、读往返、重复读、range 读、覆盖写不读脏数据、关闭缓存基线。
     运行：`GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=nosasl uv run --group test pytest tests/test_page_cache.py`。
5. **Gating-grade consistency suite**（`tests/page_cache_consistency.rs`，`#[ignore]`，连真实集群） — see §12.5.

### 12.5 Gating-grade consistency suite (`page_cache_consistency`)

This is the page-cache analogue of `tests/sc_consistency.rs`. Every
invariant from §1.4 maps to exactly one `#[tokio::test] #[ignore]` case
that asserts a hard byte-equality contract (not a perf metric); a
failure here is a release blocker. Run them explicitly:

```bash
GOOSEFS_AUTH_TYPE=nosasl \
  cargo test --test page_cache_consistency -- --ignored --nocapture --test-threads=1
```

Coverage map:

| Test case | Invariant | What it asserts |
|---|---|---|
| `inv_pc_d1_cache_vs_direct_byte_diff` | INV-PC-D1 | Two contexts — one with cache enabled, one disabled — open the same blob and read at a curated set of boundaries (page 4 KiB, chunk 1 MiB, block 4 MiB, tail). Each pair plus the source payload are asserted three-way equal, on both cold-miss and warm-hit passes. |
| `inv_pc_d2_read_apis_are_equivalent` | INV-PC-D2 | A single cache-on context drains the same file three ways — `read_all`, sequential `read` with heterogeneous chunk sizes, positioned `read_at` with a 257 KiB step — and asserts the three results plus the source are byte-equal. |
| `inv_pc_s1_failed_fill_does_not_poison_cache` | INV-PC-S1 | Cache directory is pointed at an unwritable path, so every fill fails. The reader must still return bytes equal to the source for both whole-file and boundary-spanning ranges, and the `Client.CacheBytesReadCache` counter must stay flat (no torn data is ever served from the cache). |
| `inv_pc_s2_restart_byte_parity` | INV-PC-S2 | Two phases. Phase A: cache-on context, write payload v1, read it warm, drop the context. Phase B: a fresh context backed by the same on-disk cache directory reads the file again and must return v1 byte-for-byte. Then the file is overwritten as v2 (different length); a third context reading after the overwrite must observe v2 bytes (no stale v1 from disk). |

Design notes (parity with `sc_consistency.rs`):

- `block_size = 4 MiB` and a 10 MiB payload force every test to cross at
  least two block boundaries on a single-worker dev cluster.
- A position-dependent payload (Knuth multiplicative hash) is used so
  any wrong offset / length surfaces as a byte mismatch instead of
  `0 == 0` luck.
- `client_cache_async_write_enabled = false` makes fills deterministic;
  the warm pass therefore truly exercises the cache rather than racing
  with an in-flight async fill.
- All cases are `#[ignore]`d so plain `cargo test` stays hermetic and
  CI's gating job opts in via `--ignored`.

Not covered by this suite (intentional, lower-tier coverage suffices):

- INV-PC-S1 sub-case for async-fill queue exhaustion under load — covered by the unit test `concurrent_puts_and_gets_same_and_distinct_pages` in `manager.rs` and the `CachePutAsyncRejectionErrors` counter wiring; reproducing it deterministically at e2e tier needs a synthetic slow `PageStore`.
- INV-PC-S2 sub-case for sidecar drift — covered by `restore_drops_pages_without_identity_sidecar` and `restore_reclaims_empty_shell_dir_with_only_sidecar` in `manager.rs`.

---

## 13. 分阶段实施计划

| 阶段 | 内容 | 产出 | 状态 |
|---|---|---|---|
| **P0 脚手架** | `src/cache/` 模块骨架、`CacheManager` trait、`PageId`、config 字段与常量、metrics 常量 | 编译通过，开关默认关闭 | ✅ 已完成 |
| **P1 最小可用** | `LocalPageStore` + 内存索引 + LRU + 单目录 + `CachingPositionReader`（`read_through_cache`）接 `read_at` + 同步回填 + 核心 metrics | 随机读命中可用 | ✅ 已完成 |
| **P2 完善** | 异步回填（`schedule_fill` + `Semaphore` 限流）+ 多目录 + `HashAllocator` + 每目录容量计费/淘汰 + LFU + 顺序读 `read()` 接缓存 + 完整 metrics | 功能对齐 Java | ✅ 已完成 |
| **P3 持久化与健壮性** | 进程重启 `restore`、TTL 惰性过期（`get`）+ 后台 TTL sweeper、覆盖写一致性校验（`on_file_open` 比对 length/mtime）、Python e2e | 生产可用 | ✅ 已完成 |

### 13.1 关键实现落点（速查）

| 能力 | 位置 |
|---|---|
| `CacheManager` trait / `DisabledCacheManager` | `src/cache/mod.rs` |
| `LocalCacheManager`（put/get/delete/invalidate/on_file_open/schedule_fill） | `src/cache/manager.rs` |
| TTL 惰性过期（`is_expired` → `get` 命中前丢弃过期页，记 `*Discarded`） | `src/cache/manager.rs::get` |
| 后台 TTL sweeper（`maybe_spawn_ttl_sweeper` + `sweep_expired`，持 `Weak<Self>` 随 drop 退出） | `src/cache/manager.rs` |
| 覆盖写失效（`on_file_open` 比对 `(length, mtime)`） | `src/cache/manager.rs` + 调用点 `src/io/file_in_stream.rs::open_with_context` |
| 进程重启 `restore`（扫描 `<dir>/<page_size>/<bucket>/<file_id>/<page_index>`，清理 `.tmp-`） | `src/cache/manager.rs::restore` |
| 页拆分 + 命中/回源/回填编排 | `src/cache/caching_reader.rs`（`read_through_cache` / `FillMode`） |
| 本地磁盘后端（临时文件 + 原子 rename） | `src/cache/store/` |
| LRU / LFU evictor | `src/cache/evictor/` |
| 多目录分配 | `src/cache/allocator.rs`（`HashAllocator`） |
| 选项解析（含 5% overhead、TTL=0→None） | `src/cache/options.rs` |
| metrics 名称常量 | `src/cache/metrics.rs` |
| 配置字段 / `ENV_*` / `STORAGE_OPT_*` | `src/config.rs` |
| context 挂载（`acquire_cache_manager`） | `src/context.rs` |

---

## 14. 待确认事项（Open Questions）

> 落地后逐项结论如下。

1. **`URIStatus` 是否提供稳定 `file_id`？覆盖写如何感知？**
   ✅ 已解决。`URIStatus.file_id`（服务端 inode，`i64`）作为缓存 key 命名空间，其字符串形式即 `cache_file_id`，保证同一文件多次打开跨流命中。覆盖写通过
   `on_file_open(file_id, length, last_modification_time_ms)` 在打开时比对已记录版本：
   length 或 mtime 变化即判定为覆盖，`invalidate` 该文件全部页后更新版本。调用点在
   `GoosefsFileInStream::open_with_context`。
2. **`Cargo.toml` 是否已有 `moka` / `lru` / `async-trait`？**
   ✅ 已定稿。仅引入 `async-trait`；evictor（LRU/LFU）与 meta 索引为**自研**，未引入
   `moka`/`lru`，以与 Java 行为对齐并精确控制「值在磁盘、元数据在内存」的一致性。
3. **缓存默认目录与权限策略？**
   ✅ 默认 `/tmp/goosefs_cache`（`DEFAULT_CLIENT_CACHE_DIR`），可经
   `goosefs.user.client.cache.dirs` / `GOOSEFS_USER_CLIENT_CACHE_DIRS` /
   `goosefs_client_cache_dirs` 覆盖。容器场景建议显式指定挂载盘。单进程独占目录假设见 §10.4。
4. **Java `PropertyKey.USER_CLIENT_CACHE_*` 默认值核对？**
✅ 已对齐：page size `1 MiB`、单目录容量 `1 GiB`（使用前预留 5% overhead）、
   async write threads `16`、async write enabled `true`、quota/ttl 默认关闭、enabled 默认
   `false`、evictor 默认 `LRU`。详见 `src/config.rs` 默认值常量。
5. **是否需要与 Java/Go 客户端共享同一磁盘缓存目录格式（跨语言互通）？**
   ⏳ 暂不支持。磁盘布局
   `<dir>/<page_size>/<bucket>/<file_id>/<page_index>` 与 Java 对齐其形态，但 `file_id`
   语义（Rust 用服务端 inode）与跨进程共享一致性尚未验证，列为后续工作（见 §10.4）。

### 14.1 后续工作（Future Work）

已完成（本轮）：
- ✅ `CacheHitRate` gauge：在命中（`manager.get`）与回源（`caching_reader`）两条读路径上按字节计数器实时计算并发布（`metrics::publish_hit_rate`）。
- ✅ `CacheSpaceUsedCount` gauge：随占用刷新（`publish_occupancy`，等于缓存页数）。
- ✅ `CachePageReadExternalTimeNanos` 埋点：`caching_reader` 回源分支记录外部读耗时。
- ✅ 回填按 `ReadType` 门控：`ReadType::NoCache` 打开的流只服务命中、不回填（不污染缓存），见 §3.2。
- ✅ 端到端集成测试 / 基准 / Python e2e（见 §12）。

仍未做（明确的后续/非目标，均不影响主功能正确性）：
- ⏳ 持久化元数据快照以加速重启 `restore`（当前为目录全量扫描重建；属优化项）。
- ⏳ in-stream buffer（`cache.in.stream.buffer.size`）与 `CacheBytesReadInStreamBuffer` 指标。
  说明：本实现按整页读取并缓存，已基本覆盖该 buffer 的收益，优先级低。
- ⏳ 配额（per-scope quota）完整实现（当前 `quota_enabled` 预留，按 `CacheScope::Global` 处理）。
- ⏳ 多进程共享缓存目录的文件锁 + 目录隔离（§1.3 非目标 / §10.4）。
- ⏳ 跨语言（Java/Go）共享磁盘缓存目录格式互通（§14 OQ#5；需验证 `file_id` 语义）。
- ⏳ 让 `GoosefsFileReader`（`read_file`/`read_range`）与 `positioned_read` 也走 page cache。
  当前缓存仅集成在 `GoosefsFileInStream`（含 Python `fs.open_file`），上述 one-shot/worker-直连
  路径绕过缓存；如需对它们生效，需将其读编排改为复用 `read_through_cache`。

---

## 附录 A：完整 Metrics 清单（对齐 Java `MetricKey`）

```text
Client.CacheBytesReadCache
Client.CacheBytesReadInStreamBuffer
Client.CacheBytesReadExternal
Client.CacheBytesRequestedExternal
Client.CachePageReadCacheTimeNanos
Client.CacheBytesEvicted
Client.CachePageReadExternalTimeNanos
Client.CacheBytesDiscarded
Client.CachePagesDiscarded
Client.CachePages
Client.CachePagesEvicted
Client.CacheBytesWrittenCache
Client.CacheHitRate
Client.CacheSpaceAvailable
Client.CacheSpaceUsed
Client.CacheSpaceUsedCount
Client.CacheCleanErrors
Client.CacheCleanupGetErrors
Client.CacheCleanupPutErrors
Client.CacheCreateErrors
Client.CacheDeleteErrors
Client.CacheDeleteNonExistingPageErrors
Client.CacheDeleteNotReadyErrors
Client.CacheDeleteFromStoreErrors
Client.CacheDeleteStoreDeleteErrors
Client.CacheGetErrors
Client.CacheGetNotReadyErrors
Client.CacheGetStoreReadErrors
Client.CachePutErrors
Client.CachePutAsyncRejectionErrors
Client.CachePutEvictionErrors
Client.CachePutBenignRacingErrors
Client.CachePutInsufficientSpaceErrors
Client.CachePutNotReadyErrors
Client.CachePutStoreDeleteErrors
Client.CachePutStoreWriteErrors
Client.CachePutStoreWriteNoSpaceErrors
Client.CacheStoreDeleteTimeout
Client.CacheStoreGetTimeout
Client.CacheStorePutTimeout
Client.CacheStoreThreadsRejected
Client.CacheState
Client.FallbackState
Client.ReadStreamFallBackCount
Client.AsyncThroughThreadsPoolSize
Client.AsyncThroughQueueLength
Client.AsyncThroughThreadsActive
```

## 附录 B：关键源码路径速查

**Java 参考**（`/opt/sourcecode/cos/goosefs/core/client/fs/.../client/file/cache/`）：
`LocalCacheManager.java`、`CacheManager.java`、`NoExceptionCacheManager.java`、`CacheManagerOptions.java`、`PageId.java`、`PageInfo.java`、`PageStore.java`、`store/LocalPageStore.java`、`store/LocalPageStoreDir.java`、`store/QuotaManagedPageStoreDir.java`、`PageMetaStore.java`、`DefaultPageMetaStore.java`、`evictor/LRUCacheEvictor.java`、`evictor/LFUCacheEvictor.java`、`allocator/HashAllocator.java`、`LocalCacheFileInStream.java`、`LocalCacheFileSystem.java`。

**Rust 集成点**（`/opt/sourcecode/cos/goosefs-client-rust/src/`）：
`io/file_in_stream.rs`（`read_at` / `read`）、`io/file_reader.rs`、`io/reader.rs`、`context.rs`、`config.rs`、`metrics/registry.rs`、`fs/options.rs`（`ReadType`）、`fs/uri_status.rs`、`error.rs`。
