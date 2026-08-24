# 用 foyer 替换 moka 做 SSD Page Cache 的开发设计方案

> Status: **Draft** · Last updated: 2026-08-24
> Target crate: `goosefs-sdk` (`src/cache/`)
> 前置设计: [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md)
> 候选库: [foyer-rs/foyer](https://github.com/foyer-rs/foyer)（Hybrid in-memory and disk cache in Rust）

---

## 0. 一句话结论

当前 SSD page cache 把 `moka` 当成「并发 HashMap + 手写 O(N) 选 victim」用，**没有用上 moka 自己的 O(1) 驱逐**。稳态满容时，每次 miss 回填都要对全量页做一次扫描；在 **100 GB / 约 10 万页** 的规模下，即便 **命中率 99%**，这 1% 的 fill 路径也会把 P99/并发吞吐量打穿。

推荐分两阶段落地：

1. **P0（先换 evictor）**：用 foyer-memory 的侵入式 LRU / w-TinyLFU 替换 `MokaCacheEvictor::evict_candidate` 的 O(N) 扫描，磁盘布局、`PageStore`、identity sidecar、metrics **全部不动**。
2. **P1（可选，整库替换）**：用 foyer `HybridCache` 接管「内存元数据 + SSD 数据 + io_uring」，淘汰自研 `UringPageStore` / 页文件布局。代价是 on-disk format 不兼容、MSRV 可能要升到 1.91。

---

## 1. 现状（Why we must change）

### 1.1 今天的 SSD page cache 长什么样

客户端本地 page cache 已经落地（`src/cache/`），语义对齐 Java `goosefs.user.client.cache.*`：

```text
GoosefsFileInStream::read / read_at
  → caching_reader::read_through_cache          // 按 page_size 切页
       ├── LocalCacheManager::get               // hit：本地盘
       └── miss → Worker/UFS + schedule_fill    // 异步回填
              │
              ▼
       LocalCacheManager
         ├── DashMap<PageId, PageInfo>          // 内存元数据（热路径无全局锁）
         ├── DirState.evictor  = MokaCacheEvictor   // ★ 本方案要换的
         ├── DirState.used_bytes / capacity     // 按字节配额
         ├── PageStore                          // 页数据在 SSD 上
         │     ├── LocalPageStore  (tokio::fs)
         │     └── UringPageStore  (io_uring + moka fd cache)
         ├── HashAllocator                      // 多目录
         ├── by_file / versions                 // invalidate + overwrite 检测
         └── 1024 striped page locks
```

关键不变量（**本方案不得破坏**）：

| ID | 不变量 | 含义 |
|---|---|---|
| INV-PC-D1 | cache-on / cache-off 字节级一致 | 缓存层永远不能返回脏页或撕页 |
| INV-PC-S1 | fill 失败不得污染缓存 | miss 干净回退到 Worker |
| INV-PC-S2 | restart + overwrite | `(file_id, length, mtime)` 变了必须 invalidate |
| Best-effort | `CacheManager` 对外 `bool` / `usize` | 缓存故障不得变成读错误 |

页数据 **全部在盘上**，内存只持有 `PageId` / `PageInfo` / evictor 索引。这是典型的 SSD cache，不是内存 cache。`moka` 被塞进这条路径后，能力模型和实际职责是错位的。

### 1.2 `moka` 在工程里的两处用法

`Cargo.toml` 里 `moka = { version = "0.12", features = ["future", "sync"] }`，服务两个完全不同的角色：

| 位置 | 用途 | 容量模型 | 是否本方案主战场 |
|---|---|---|---|
| `src/cache/evictor/moka_evictor.rs` | 给 `LocalCacheManager` 选 SSD 驱逐 victim | `max_capacity = u64::MAX`，**关闭** moka 自动驱逐 | **是** |
| `src/cache/store/uring/store.rs` `PAGE_FD_CACHE` | 缓存已打开的 page fd（Lance `HANDLE_CACHE`） | 1 万条 + 60s TTL，走 moka 自己的 LRU | 否（P0 不动；P1 可顺手换掉） |

P0 只动第一处。第二处是「内存里的 fd 句柄 cache」，规模固定 10k，moka 用得正常，不是 10 万页 O(N) 的来源。

### 1.3 根因：把 moka 用成了「能扫的 HashMap」

`MokaCacheEvictor` 的设计注释写得很清楚（`moka_evictor.rs`）：

```text
max_capacity = u64::MAX     → 禁用 moka 自动驱逐
on_access                   → O(1) insert（LRU tick / LFU count）
evict_candidate             → iter().min_by_key(value)   ★ O(N) 全表扫描
```

真正选 victim 的代码：

```rust
fn evict_candidate(&self) -> Option<PageId> {
    self.cache.run_pending_tasks();
    self.cache
        .iter()
        .min_by_key(|(_, v)| *v)
        .map(|(k, _)| k.as_ref().clone())
}
```

`LocalCacheManager::put` 在目录 `used_bytes + page_len > capacity` 时循环调用 `pop_victim` → `evict_candidate`。稳态满容（SSD cache 的正常工作点）下，**每一次 miss 回填都要扫一遍当前目录里的全部页**。

这等于：

- 花了 moka 的依赖和 per-segment 锁，只换掉了当年全局 `Mutex<LruState>` 的并发问题；
- **完全没用上** moka 内置的 LRU 链表 / TinyLFU 的 O(1) victim 选择；
- 还额外付了 `run_pending_tasks()` + `iter()` 的成本。

moka 是给「value 在内存里、cache 自己管生命周期」设计的。我们的 value 在 SSD 上、删除必须和 `DashMap` / `by_file` / identity sidecar 协同，所以当时把 `max_capacity` 拉到 `u64::MAX`、改成 manager 手动 `pop_victim`。这个折中在「几千页」时能过；到 **10 万页 SSD cache** 就是结构性瓶颈。

### 1.4 规模账：100 GB ≈ 10 万页，99% 命中也扛不住

SDK 默认 `page_size = 1 MiB`。产品侧给出的量级：

| 量 | 值 |
|---|---|
| SSD cache 容量 | 100 GB |
| page size | 1 MiB（默认） |
| 页数 N | 100 GB / 1 MiB = **102,400 ≈ 10 万页** |
| 工作点 | 满容 + 持续回填 |
| 命中率 | 99%（乐观） |

代码里对 1 万页的经验值是 `evict_candidate ≈ 100–200 µs`。线性外推：

| N（页） | 对应容量（1 MiB/页） | 单次 `evict_candidate`（估） |
|---|---|---|
| 1,000 | 1 GB | ~10–20 µs |
| 10,000 | 10 GB | **100–200 µs**（代码注释实测） |
| 100,000 | **100 GB** | **1–2 ms**（CPU 扫描，持有 per-dir `StdMutex`） |
| 1,000,000 | 1 TB / 或 128 KiB 页的 128 GB | 10–20 ms |

**为什么 99% 命中率仍然很痛：**

命中路径 `get` → `on_access` 是 O(1)，99% 的读本身不扫表。贵的是剩下那 **1% miss → `put` → `pop_victim`**：

1. **稳态满容时，每次回填必驱逐。** 99% 命中 ⇒ 1% 请求走 fill；fill 每次至少一次 O(N) 扫描。
2. **Miss 延迟被 CPU 扫描主导。** NVMe 读一页是 10–100 µs 量级；1–2 ms 的 `iter().min` 比盘 IO 还贵 10–100 倍。用户感知的是「偶发卡一下」，不是平均 QPS。
3. **锁持有时间随 N 线性涨。** `pop_victim` 在 `dir_locks[i]` 里调用 `evict_candidate`。同一 cache 目录的并发 `put`（冷启动、顺序扫大文件、多文件同时回填）全部串行在这把锁上。N=10 万时临界区 1–2 ms，fill 吞吐被锁死在大约 500–1000 evict/s/dir，和 SSD 带宽无关。
4. **一次 `put` 可能多次扫描。** 配额按字节、尾页小于 `page_size`、CAS 失败重试时，`put` 循环里会连续 `pop_victim`。每次都是完整 O(N)。
5. **平均延迟会被 1% 的慢路径拉歪。** 粗算：`0.99 * hit + 0.01 * (hit_path + 1.5 ms scan + 盘删)`。扫描这一项单独贡献约 **15 µs 到平均值**，但对 miss 延迟是 **+1.5 ms**，对 fill 并发是灾难。分析型 workload（Parquet / Lance 小 IO）对尾延迟极敏感。
6. **N 还会因小页放大。** 若把 page 调到 128 KiB，同样 100 GB 变成 **80 万页**，单次扫描到 10 ms 量级。O(N) 在产品上不可调参缓解。

结论：这不是「再优化一下 moka 配置」能修的。只要 victim 选择是 `iter().min`，复杂度就是 Θ(N)，和命中率解耦——命中率只决定扫描发生的频率，不决定单次扫描的成本。

### 1.5 并发与正确性上的连带伤害

- **Hit 与 fill 不对称：** `on_access` 走 moka per-segment 锁，不拿 `dir_locks`；`on_add` / `on_remove` / `evict_candidate` 拿 `dir_locks`。N 变大后 fill 侧锁时间主导，看起来像「命中很快、写入/回填突然变慢」。
- **扫描中的脏视图：** `run_pending_tasks()` 之后立刻 `iter()`，仍可能扫到刚 `invalidate` 的 key；`pop_victim` 还要再对 `meta` 做 remove，存在「扫到的 victim 已不在 meta」的空转，最坏再扫一轮。
- **LFU 还不是原子的：** `on_access` 是 `get` + `insert(count+1)`，高并发下频率被低估。这在 O(N) 面前是次要问题，但说明 moka 在这里只是 concurrent map，不是 eviction engine。
- **自研 io_uring 页文件布局仍然在。** `UringPageStore` 一页一个文件（`<root>/<bucket>/<file_id>/<page_index>`），open/close/fd cache/VFS 锁是另一条复杂度线。P0 不碰它；P1 才用 foyer 的 block engine + `UringIoEngine` 把它吃掉。

### 1.6 现状小结（给决策用）

```text
今天的 SSD cache
  数据面：自研 PageStore（tokio::fs / io_uring），一页一文件
  索引面：DashMap + by_file + versions + 配额
  驱逐面：moka::sync::Cache，但 max_capacity=∞，victim = O(N) min scan
  fd 面：moka::future::Cache（10k，正常 LRU）

痛点优先级
  P0  驱逐 O(N) —— 100 GB / 10 万页 / 99% 命中仍不可接受
  P1  自研 io_uring + 页文件 + fd cache 的运维/性能天花板
  P2  依赖裁剪（moka 仅因 evictor + fd cache 留下）
```

**成功标准（P0 就必须达到）：** `evict_candidate` / 等价驱逐从 Θ(N) 降到 O(1) 或 O(log N)；在 N=10 万、满容、99% 命中的 A/B 里，miss 回填的 P99 不再被 CPU 扫描主导，同目录并发 fill 不再随 N 线性掉速。

---

## 2. 目标与非目标

### 2.1 目标

1. 把 SSD page cache 的 victim 选择从 **O(页数)** 降到 **O(1)**（或分片后的 O(1)）。
2. 对外 `CacheManager` / 配置项 / `Client.Cache*` metrics / Python binding **保持兼容**。
3. 不破坏 INV-PC-D1/S1/S2：overwrite 检测、TTL、restart restore、best-effort 回退。
4. 分阶段交付：P0 可单独合入、可回滚；P1 整库替换是明确的可选增量。
5. 用 [foyer](https://github.com/foyer-rs/foyer) 作为驱逐算法与（P1）混合缓存的实现来源，而不是再手写一套 LRU 链表。

### 2.2 非目标

- 不在本方案里重做 Java Worker 侧 page cache。
- P0 不改 on-disk page 文件布局，不做 cache 目录跨版本迁移。
- 不引入跨进程共享 cache 目录。
- 不把 foyer 直接暴露给 SDK 用户；它是 `LocalCacheManager` 的内部实现。
- 不把 metadata cache（`src/metadata_cache.rs`，自研 LRU）纳入本次替换。

---

## 3. 为什么是 foyer，而不是「修好 moka」或再手写 LRU

### 3.1 三条路

| 方案 | 做法 | 优点 | 缺点 | 结论 |
|---|---|---|---|---|
| A. 继续用 moka，打开 `max_capacity` + eviction listener 删盘 | 让 moka 自己 O(1) 驱逐 | 改动面小 | listener 与 `DashMap`/`by_file`/sidecar 时序难；moka 仍是内存 cache，不管 SSD 生命周期；LFU 是 TinyLFU 不是 Java LFU | 不推荐作终态 |
| B. 自研侵入式 LRU/LFU 链表 | 回到 `Mutex<LruState>` 之前的方向，但按 shard 做 | 零新依赖、行为可控 | 已经用全局 Mutex 踩过 32 并发 38x 退化；正确的 concurrent LRU 工作量 ≈ 再造 foyer-memory | P0 备选，不作为首选 |
| C. foyer | P0 用 `foyer::Cache` 作 evictor；P1 用 `HybridCache` 接管盘 | 为 hybrid/SSD 而生；驱逐可插拔（LRU / w-TinyLFU / S3-FIFO / SIEVE / FIFO）；磁盘引擎自带 io_uring；生产案例多（RisingWave、Chroma、SlateDB、Percas） | MSRV、on-disk 格式、`invalidate(file_id)` 无前缀扫描 | **推荐** |

### 3.2 foyer 和我们需求的对应关系

来源：[foyer 架构](https://foyer-rs.github.io/foyer/docs/design/architecture)、[GitHub](https://github.com/foyer-rs/foyer)。

| 我们要的 | foyer 提供的 |
|---|---|
| O(1) victim（侵入式链表，不是扫 HashMap） | `foyer-memory`：分片 indexer + 可插拔 eviction container |
| LRU / LFU（对齐 `CacheEvictorType`） | `LruConfig` / `LfuConfig`（w-TinyLFU）；额外还有 FIFO / S3-FIFO / SIEVE |
| 驱逐时删 SSD 上的页 | `EventListener::on_leave`（P0 钩到现有 `PageStore::delete`） |
| 数据在盘、热数据可在内存 | `HybridCache` + `HybridCachePolicy::WriteOnInsertion` 或 `WriteOnEviction` |
| io_uring | `UringIoEngine` / `UringIoEngineConfig`（Linux）；非 Linux 用 `PsyncIoEngine` |
| 大容量 SSD（数十 GB～TB） | Block engine：按 block 追加写，内存只留 indexer |
| 可观测 | 自带 Prometheus/OTel；我们仍以 `Client.Cache*` 为准，内部可加 foyer 指标作诊断 |

foyer 不是「又一个 moka」。moka 解决内存并发 cache；foyer 解决 **hybrid（内存 + 盘）cache**，驱逐、盘上 GC、IO engine 是一等公民。我们今天的痛点正好落在它的主场。

### 3.3 已知约束（必须写进排期）

| 约束 | 详情 | 处理 |
|---|---|---|
| ~~MSRV~~ | ~~foyer 0.22 文档写 1.91.0~~ | **已验证解除**，见 §3.4 |
| 平台 | foyer 面向 Linux；macOS/Windows 可编但 disk engine / io_uring 不完整 | 与现网一致：Linux 走 uring/psync，其它平台 P0 仍用内存 evictor + `LocalPageStore` |
| 磁盘格式 | Block engine ≠ 一页一文件 | **P0 不改盘格式**；P1 升级等于 cache 冷启动（可接受，best-effort） |
| 无前缀查询 | indexer 是 hash 表，不能 `invalidate(file_id*)` | P0 仍用现有 `by_file`；P1 继续保留反向索引或在 key 里带 `file_id` 后自行枚举 |
| 仍在快速迭代 | 官方 roadmap 标明 heavy development | 锁版本（例如 `0.22`），不跟 `*` |

### 3.4 实测结论（2026-08-24，分支 `feature/foyer-ssd-cache-evictor`）

开工第一步按 §7.2 做了 MSRV 门禁和 API 钉死，两条结论都推翻了本文档初稿的假设。

**（1）MSRV 不是问题，无需升级工具链。**

README 上的 “minimum supported version 1.91.0” 是 foyer **开发工作区**的口径。实际发布到 crates.io 的包声明的是 1.85：

```text
foyer-0.22.3/Cargo.toml         rust-version = "1.85.0"
foyer-memory-0.22.3             rust-version = "1.85.0"
foyer-common-0.22.3             rust-version = "1.85.0"
foyer-storage-0.22.3            rust-version = "1.85.0"
传递依赖最高： mea 1.85.0 / hashbrown 1.65.0 / cmsketch 1.81.0
```

`cargo +1.88 check --lib` 在 `foyer-memory` + `foyer-common` 进入依赖树后编译通过。**本仓库 `rust-version = "1.88"` 保持不变**，README / CONTRIBUTING / Python wheel 都不用动。

**（2）依赖只取 `foyer-memory`，不取伞crate `foyer`。**

P0 只要内存侧的驱逐容器，不要磁盘引擎。`foyer-memory` + `foyer-common` 共引入 12 个包，且不含 `foyer-storage` / io_uring / 设备栈——SSD 数据面仍是在库的 `PageStore`。等 P1 真的上 `HybridCache` 时再换成伞 crate。

**（3）§5.3 的“方式 2”在 foyer 0.22.3 上不可实现。**

方式 2 依赖“O(1) 拿到淘汰队头”。核对 `foyer-memory` 0.22.3 公开 API 后，这个能力**没有对外暴露**：

| 想要的 | 实际情况 |
|---|---|
| `Cache::pop()` / peek 淘汰队头 | **不存在**。公开面只有 `insert` / `get` / `touch` / `remove` / `contains` / `clear` / `capacity` / `usage` / `entries` / `resize` / `evict_all` / `shards` / `with_pipe` |
| 自己驱动 `Eviction` trait（它有 `fn pop()`） | `Eviction` 虽在 prelude 导出，但其签名依赖 `Record<E>`，而 `mod record;` 是私有模块且未 re-export ⇒ **外部无法构造**，trait 实际不可用 |
| 用 `resize(n-1)` 逼出一条 | `resize` 对**每个 shard `std::thread::spawn` 再 join**（`raw.rs:497`）。放在驱逐热路径上比原来的 O(N) 扫描还贵 |
| 插入哨兵 probe 挤出一条 | LRU 下可行（新记录进链表尾，`pop` 取头）；但 **LFU/w-TinyLFU 下 probe 进 `window` 尾部，`pop` 比较 `window.front()` 与 `probation.front()`**（`lfu.rs:287`），window 很小时 probe 可能把自己挤掉，返回的不是真实页 |
| 全局 victim | `Cache` 是**分片**的，容量按 shard 均分，**驱逐只在被插入的那个 shard 触发**。“给我全局最该走的一条”在公开 API 上不可表达 |

其中最硬的一条是最后一条：manager 的配额是**按目录全局记字节**，foyer 的配额是**按 shard 记权重**。即便用 probe 逼出一条，只要 probe 落到的 shard 尚未超过自己那份容量，就**驱逐不出任何东西**，`evict_candidate` 返回 `None` → `put` 记 `CacheInsufficientSpaceErrors` 直接失败。而 `put` 失败又不会让 foyer 增长，manager 的 `used_bytes` 会一直卡在 capacity —— 形成**回填永久失败的活锁**。这不是调参能绕开的。

foyer 支持的观测淘汰的方式只有一条：`EventListener::on_leave(Event::Evict, key, value)`（`CacheBuilder::with_event_listener`），即**由 foyer 自己决定何时驱逐、事后通知**。这正好是本文档 §4.1 的**方式 1**。

**结论：按本文档 §4.1 “若 foyer 公开 API 不便『只踢一条』，则退回方式 1” 的预案，P0 落到方式 1。** 具体影响见 §5.7。

---

## 4. 总体架构

### 4.1 P0：只换 evictor（推荐先做）

```text
                    不变
                    ────
GoosefsFileInStream / caching_reader / CacheManager trait
LocalCacheManager (DashMap meta, by_file, versions, page locks, quotas)
PageStore (LocalPageStore / UringPageStore) + identity sidecar
Client.Cache* metrics

                    变
                    ──
DirState.evictor: MokaCacheEvictor
        ↓
DirState.evictor: FoyerCacheEvictor
   └── foyer::Cache<PageId, EvictMeta>
         ├── EvictionConfig::Lru / Lfu
         ├── capacity = 该目录最大页数（或 weighter=page_size）
         ├── EventListener： Evicted → 现有 pop_victim 删除盘文件
         └── victim 选择：侵入式链表 O(1)，不再 iter().min
```

两种接入方式。初稿推荐方式 2，但 §3.4 实测证伪，**最终落方式 1**：

**方式 1 — 把驱逐控制权交给 foyer（listener 驱动删盘）✅ 已实现**

- `CacheBuilder::new(pages_per_dir)` + `EventListener::on_leave`
- `put` 不再手写 `pop_victim` 循环，insert 超容时 foyer 自己踢人
- 风险：`on_leave` 在 foyer 内部上下文触发，必须异步删盘、不能重入 `CacheManager`；和 `dir_locks` / page lock 的锁顺序要重新证明

**方式 2 — 保持 `CacheEvictor` trait，foyer 只当 O(1) 的「谁该走」❌ 不可实现，见 §3.4**

- 继续 `on_add` / `on_access` / `on_remove` / `evict_candidate`
- `evict_candidate` 改为：对 foyer cache 做一次 **受控的单条驱逐**（peek + remove 队头，或 insert 一个哨兵触发一次 eviction 并在 listener 里记下 victim）
- `LocalCacheManager::pop_victim` 的锁顺序、删盘、sidecar、metrics **一行业务逻辑都不改**
- 实现细节在 §5；若 foyer 公开 API 不便「只踢一条」，则退回方式 1，并在 P0 设计评审时定稿

无论哪种，**禁止**再对 foyer/moka 做 `iter().min_by_key`。

### 4.2 P1：HybridCache 接管 SSD（可选）

```text
LocalCacheManager
  ├── 仍负责：page 切分、best-effort、on_file_open、TTL 语义、metrics、多目录路由
  └── 每个 cache dir 一个 foyer HybridCache<PageKey, Vec<u8>>
        ├── memory(): 热页缓冲（数十～数百 MB，不是 100 GB）
        ├── storage(): FsDeviceBuilder + BlockEngine
        │     └── IO: Linux → UringIoEngine；其它 → PsyncIoEngine
        ├── policy: WriteOnInsertion（对齐今天「put 立刻落盘」）
        └── recover: RecoverMode，替代自研 restore() 扫目录
```

P1 可以删掉或大幅收缩：

- `UringPageStore` + 自研 io_uring driver + `PAGE_FD_CACHE`
- 一页一文件的目录树
- `moka` 依赖（若 fd cache 也切走）

保留：`CacheManager` 门面、`by_file`（或等价的文件级失效）、identity/`on_file_open`、striped page lock（若 foyer 已覆盖并发，可评估去掉）。

---

## 5. P0 详细设计（FoyerCacheEvictor）

### 5.1 模块与文件

| 文件 | 动作 |
|---|---|
| `src/cache/evictor/foyer_evictor.rs` | **新增** `FoyerCacheEvictor` |
| `src/cache/evictor/moka_evictor.rs` | 保留一个版本作 A/B；feature 或 `CacheEvictorType` 可切回 |
| `src/cache/evictor/mod.rs` | `build_evictor` 默认指向 foyer |
| `Cargo.toml` | 加 `foyer`（先 `default-features` 足够用 memory cache）；评估是否立刻删 moka（fd cache 仍需要则留） |
| `src/config.rs` `CacheEvictorType` | 文档字符串从 “backed by moka” 改为 foyer；枚举值 `Lru`/`Lfu` 不变 |
| `benchmarks/cache_evictor_bench.rs` | 增加 foyer vs moka 对照，**强制 N∈{1e3,1e4,1e5}** |

### 5.2 类型与容量

```rust
pub struct FoyerCacheEvictor {
    cache: foyer::Cache<PageId, EvictMeta>,
    mode: EvictMode, // Lru | Lfu，仅用于测试断言 / 日志
}

struct EvictMeta {
    page_size: u64, // weighter 用；若按页计数可忽略
}
```

- `PageId` 已 `Clone + Eq + Hash`，可直接作 foyer `Key`。
- 容量：`CacheBuilder::new(pages_per_dir)` 或 `with_weighter(|_, m| m.page_size as usize)` 对齐目录字节配额。
- 分片：`with_shards(64)` 量级，避免再出现当年单 Mutex 的 38x。
- 算法：
  - `CacheEvictorType::Lru` → `EvictionConfig::Lru(LruConfig { high_priority_pool_ratio: 0.0 })`
  - `CacheEvictorType::Lfu` → `EvictionConfig::Lfu(LfuConfig::default())`（w-TinyLFU，与当前 moka TinyLFU 同族，比「纯计数 min」更抗扫描）

### 5.3 `CacheEvictor` 映射（方式 2）

| trait 方法 | foyer 操作 | 复杂度目标 |
|---|---|---|
| `on_add(id)` | `cache.insert(id, meta)` | O(1) 分片 |
| `on_access(id)` | `cache.get(id)` / `touch`（以 foyer API 为准，禁止再手写 tick） | O(1) |
| `on_remove(id)` | `cache.remove(id)` | O(1) |
| `evict_candidate()` | 取该分片 eviction 容器的队头并 `remove`，或一次性踢出并返回 key | **O(1)** |
| `len()` | `cache.usage()` / entry count | O(1) |

`evict_candidate` 是本阶段唯一允许的新代码。若 foyer 0.22 的公开 API **没有**「peek LRU head」：

- 实现一个极小的 `VictimListener: EventListener`，`on_leave` 把 key 推进 `Mutex<VecDeque<PageId>>`；
- `evict_candidate` 先看队列；队列空则 `insert` 一个不会被查询的 **probe 条目** 挤出一条真实页（probe 随后立刻 remove）。这是权宜之计，只为保住现有 `pop_victim` 时序；P0 评审时若认为太 hack，就改走方式 1。

无论哪种，**复杂度不得退回 O(N)**。单测里用 N=50_000 断言 `evict_candidate` 在固定时间预算内（例如 < 50 µs p99）。

### 5.4 锁顺序（必须保持）

现有顺序（`manager.rs`）：

```text
page_locks[hash(page)]   // 外层，跨 await
  dir_locks[dir]         // 仅保护 evictor 写 + used_bytes 预订，不跨 await
    evictor.evict_candidate / on_add / on_remove
  by_file / versions     // 独立 RwLock，cold path
盘 IO                    // 锁外
```

P0 规则：

- foyer 内部已有分片锁。`dir_locks` 暂时保留，避免 `used_bytes` CAS 与 evictor 插入乱序。
- **禁止**在 `EventListener::on_leave` 里再抢 `page_locks` / `dir_locks`（死锁）。listener 只记录 victim 或 `spawn` 删盘。
- `on_access` 继续在 `get` 热路径、`page_locks` 读锁内调用；必须是 foyer 的 O(1) touch，不能 insert 整条新记录。

### 5.5 配置与兼容

| 项 | P0 行为 |
|---|---|
| `goosefs.user.client.cache.eviction.policy=LRU\|LFU` | 原样解析，后端换 foyer |
| 默认 `Lfu` | 保持 |
| 新算法 S3-FIFO / SIEVE | **不在 P0 暴露**，避免 Java 配置矩阵膨胀；P1 可加 `CacheEvictorType` 变体 |
| `client_cache_size` / dirs / ttl / uring / sync pread | 不动 |

可选逃生开关（建议加，默认 foyer）：

```text
goosefs.user.client.cache.eviction.backend = foyer | moka   # 仅内部/压测
```

合入一个版本后的下一个版本再删 moka evictor。

### 5.6 指标

现有 `Client.CacheBytesEvicted` / `CachePagesEvicted` 继续打，位置从 `pop_victim` 移到 `reclaim_victims`（方式 1 下驱逐是事后清理，见 §5.7）。

P0 增加内部诊断（不必进 Java 兼容名）—— **已完成**：

- `Client.CacheEvictCandidateNanos`（cumulative counter）—— 用来证明不再随 N 线性涨。

  埋点在 `LocalCacheManager::put` 里**只包住 `evictor.on_add`**：方式 1 下选 victim 发生在 `on_add` 内部，而写盘、索引更新、删 victim 文件都在计时之外，因此磁盘延迟不会掩盖策略本身的退化。

  用法：除以填充次数得到每次 put 的策略开销，该值必须**不随缓存页数增长**。目录填满后每次 put 都会驱逐，此时它就是「选一条 victim 的成本」——旧 moka evictor 在 10 万页时是 ~23 ms，foyer 是 ~0.84 ms（§5.9）。

- 可选：foyer 自带 metrics 挂到现有 registry，仅 debug —— 未做。

### 5.7 改走方式 1 后的实际改动面（据 §3.4 定稿）

方式 1 = **foyer 持有容量并自行驱逐，manager 事后清理**。相对方式 2，多出来的改动集中在 `LocalCacheManager::put`：

**今天（evict-then-write）**

```text
loop { 若 used + page_len > capacity → pop_victim() 取一条并删盘 }
CAS 预订字节
写盘
on_add
```

**方式 1（write-then-drain）**

```text
CAS 预订字节
写盘
on_add            → foyer 超容时自行驱逐，EventListener 把 victim 塞进队列
drain_victims()   → manager 逐条：meta.remove + used_bytes -= size + 删盘 + by_file 更新
```

对应的 trait 变化：`evict_candidate() -> Option<PageId>` 换成 `drain_victims() -> Vec<PageId>`（`on_add` / `on_access` / `on_remove` / `len` 不变）。

**需要评审确认的语义变化：**

| 项 | 影响 | 评估 |
|---|---|---|
| 配额瞬时超出 | 从「先腾地方再写」变成「先写再腾」，目录可能瞬时超出约 1 页 × 超容 shard 数 | `CacheManagerOptions` 已预留 `LOCAL_STORE_OVERHEAD = 5%`，1 MiB 页 / 100 GB 下超出量 ≪ 5%，可接受 |
| 驱逐粒度 | 全局 LRU → 分片 LRU | 今天 moka 也是 per-segment，不是回退 |
| `EventListener` 上下文 | `on_leave` 在 foyer 持有 shard 写锁时同步回调（`raw.rs:507`） | **listener 内只允许 push 到无锁队列**，禁止抢 `page_locks` / `dir_locks`、禁止删盘、禁止 `await`。删盘仍在 manager 的锁外路径 |
| INV-PC-* | `put` 是 INV-PC-D1 / S1 的关键路径 | `tests/page_cache_consistency.rs` 必须全绿；另加「满容连续 put 后 used_bytes 与盘上页数一致」的用例 |

**这是本文档 §11 决策清单第 1 项的定稿点**：方式 2 已被证伪，方式 1 需要接受上表第一行的配额语义变化。

### 5.8 分片带来的有效容量折损（实现时新发现）

foyer 把容量**按 shard 均分并按 shard 驱逐**，所以目录实际填到的是「最忙的那个 shard 满了」，略低于额定配额。缺口就是哈希不均衡，量级 `1/sqrt(每 shard 页数)`。

实现里用 `MIN_PAGES_PER_SHARD = 256` 控制：页数少于 256 的目录只给 1 个 shard（精确填满），大目录在 `MAX_SHARDS = 64` 封顶，100k 页工作点下每 shard ≈ 1.5k 页，折损 ~2-3%。取 16 会让折损到 ~25%，取 256 是容量与并发的折中。

这不是 foyer 独有的：老的 moka evictor 同样是 per-segment，只是它的 `max_capacity = u64::MAX` 让配额根本没生效，所以看不出来。单测 `sharded_directories_fill_close_to_but_below_quota` 把这个性质钉住。

### 5.9 实测结果（`benchmarks/cache_evictor_bench.rs`，macOS / tokio::fs / 1 KiB 页）

新增的 Phase 2 直接测「目录已满时一次 put 的耗时」，即每次 put 都必然触发一次驱逐，横扫 N ∈ {1e3, 1e4, 1e5}：

| 后端 | N=1e3 | N=1e4 | N=1e5 | 1e3→1e5 |
|---|---|---|---|---|
| **foyer / LRU** | 937 µs | 956 µs | **838 µs** | **0.9×（平）** |
| **foyer / LFU** | 878 µs | 775 µs | **856 µs** | **1.0×（平）** |
| moka / LRU | 1.01 ms | 2.03 ms | **22.87 ms** | 22.7×（线性） |
| moka / LFU | 1.23 ms | 2.06 ms | **22.30 ms** | 18.1×（线性） |

**在 100k 页工作点上，一次驱逐从 22.9 ms 降到 0.84 ms，约 27×。** 绝对值里包含了写新页 + 删旧页的真实盘 IO，两者共有；差异全部来自 victim 选择。moka 的 22 ms 比初稿估的 1-2 ms 还差一个量级——初稿只算了 `min_by_key` 的比较，没算 moka 迭代器为每条 entry 做的 `Arc` 克隆和 `run_pending_tasks`。

复杂度性质另有单测 `eviction_cost_does_not_scale_with_page_count` 把守（10k vs 100k 页，断言比值 < 5× 且绝对值 < 50 µs），不依赖跑 benchmark。

### 5.10 内存占用实测（`benchmarks/evictor_memory.rs`）

评审提出的疑问：「foyer 里面有自带的内存 cache，有可能这个数据量全部换存在内存里面了」。

**没有。** `FoyerCacheEvictor` 持有的是 `Cache<PageId, u64>`，value 是页大小（同时充当 weight），页数据一个字节都不进 foyer——它仍然只在 SSD 上，由 `PageStore` 管。传给 `CacheBuilder::new()` 的 100 GB 是**权重单位**（= 盘上字节），不是内存预算；foyer 不会为此分配内存，只是在权重累计到配额时开始驱逐，从而让驱逐点和目录溢出点重合。

因此 evictor 的内存是 `O(页数)` 的元数据，与缓存总量无关。用计数 allocator 实测每页常驻堆字节（比 RSS 精确，无分配器 slack）：

| 后端 | 10 万页 | 100 万页 | 每页 |
|---|---|---|---|
| **foyer / LRU** | 11.8 MiB | 124.8 MiB | **131 B** |
| **foyer / LFU** | 12.8 MiB | 125.8 MiB | **132 B** |
| moka / LRU | 24.2 MiB | 237.3 MiB | 249 B |
| moka / LFU | 24.2 MiB | 237.3 MiB | 249 B |

两个量级上每页开销一致，确认线性。**foyer 比它替掉的 moka evictor 省一半内存**（131 vs 249 B/页），所以这次替换在内存维度是净收益而不是新风险。100 GB / 1 MiB 页 = 10 万页 ≈ 12 MiB。

要盯的是**页数**而不是缓存容量：`page_size` 配小会等比放大元数据（100 GB 的 64 KiB 页 = 160 万页 ≈ 200 MiB）。这个放大系数对新旧两种 evictor 相同，不是 foyer 引入的。

另一个评审疑问：「如果关了 foyer 也没法用 os page cache，它的读盘好像是 O_DIRECT」。不适用——P0 只依赖 `foyer-memory` / `foyer-common`，**没有引入 `foyer-storage`**（见 Cargo.lock：只解析出 `foyer-common`、`foyer-memory`、`foyer-intrusive-collections`、`foyer-tokio`）。O_DIRECT 是 foyer storage engine 的行为，P0 不在那条路径上；读盘仍走自研 `PageStore`，OS page cache 照常生效。这一条要到 §6 的 P1 才需要重新评估。

遗留（未做，非阻塞）：页身份目前在内存里有**两份**——manager 的 `meta` 和 evictor 的索引。moka 版本同样如此，P0 未改变。若要压内存下限，可以合并成一份，代价是 evictor 需要能反查页大小。

---

## 6. P1 详细设计（HybridCache，可选）

仅当 P0 证明驱逐不再是瓶颈、且自研 `UringPageStore` 仍有明确 ROI 时启动。

### 6.1 职责切分

| 职责 | 仍由 SDK | 交给 foyer |
|---|---|---|
| 按 `page_size` 切页、`ReadType::NoCache` | ✓ | |
| best-effort、错误吞掉 | ✓ | |
| `on_file_open` overwrite | ✓（identity 仍要） | |
| `invalidate(file_id)` | ✓（`by_file` 枚举后 `hybrid.remove`） | 单 key remove |
| 多目录 | ✓（N 个 `HybridCache`） | 单设备 |
| 页字节的 get/put/delete、恢复、GC、io_uring | | ✓ |
| LRU/LFU | | ✓ |

### 6.2 建议配置骨架

```rust
let device = FsDeviceBuilder::new(dir)
    .with_capacity(dir_capacity)
    .build()?;

let hybrid: HybridCache<PageKey, Vec<u8>> = HybridCacheBuilder::new()
    .with_name("goosefs-page-cache")
    .with_policy(HybridCachePolicy::WriteOnInsertion) // 对齐今日立刻落盘
    .memory(memory_cap)                               // 热页，不是 100 GB
    .with_eviction_config(lru_or_lfu)
    .with_weighter(|_, v: &Vec<u8>| v.len())
    .storage()
    .with_io_engine_config(uring_or_psync)
    .with_engine_config(
        BlockEngineConfig::new(device)
            .with_block_size(16 * 1024 * 1024)
            .with_indexer_shards(64)
            .with_flushers(2)
            .with_reclaimers(2)
            .with_eviction_pickers(vec![Box::<FifoPicker>::default()]),
    )
    .with_recover_mode(RecoverMode::Quiet)
    .build()
    .await?;
```

`PageKey`：推荐 `(file_id: String, page_index: u64)` 并 `impl foyer::Code`（或 `serde` feature）。`Vec<u8>` 已实现 `Code`。

Linux：`UringIoEngineConfig::new().with_io_depth(...).with_threads(...)`，把现有 `client_cache_uring_queue_depth` / `thread_count` 映射过去。非 Linux：`PsyncIoEngineConfig`。`client_cache_sync_read_enabled` 可映射为强制 psync。

### 6.3 P1 必须单独设计的点（本文件只点名，开工前开子文档）

1. **冷升级：** 旧页文件目录与 foyer block 文件不能混读；升级 = 清空 cache dir 或换路径。文档和 release note 必须写明。
2. **`invalidate(file_id)`：** 继续用 `by_file` 列出 page_index，逐个 `remove`。不要幻想 foyer 做 prefix delete。
3. **TTL：** foyer 是否支持 TTL 以当时 API 为准；没有则保留现有 sweeper，sweeper 只 `hybrid.remove`。
4. **部分页读取：** 今日 `get(page_offset, len)` 只读页内切片。HybridCache 返回整 value 后再 slice（1 MiB 页可接受）。若以后 page 很大，再评估 zero-copy / `Location`。
5. **双 IO 栈：** P1 上线后删除自研 uring driver，避免进程内两套 io_uring。
6. **压缩：** foyer 默认可 Lz4；page cache 是已压缩列存页时建议 `Compression::None`，避免 CPU 白烧。

---

## 7. 依赖、MSRV、构建

### 7.1 Cargo

```toml
# P0（已落地在分支上）
foyer-memory = "0.22"   # 驱逐容器；不引 foyer-storage / io_uring
foyer-common = "0.22"   # Event / EventListener
moka = { version = "0.12", features = ["future", "sync"] }  # P0 仍给 PAGE_FD_CACHE

# P1 之后
# 换成伞 crate foyer = "0.22"；删除 moka；io-uring 是否仍直接依赖取决于是否完全交给 foyer
```

### 7.2 MSRV 决策（P0 门禁）—— 已完成

结论：**无需改动 MSRV**。详见 §3.4：发布包声明 1.85，`cargo +1.88 check --lib` 通过，`rust-version = "1.88"` 保持。README / CONTRIBUTING / Python wheel 均不动。

---

## 8. 测试与验收

### 8.1 正确性（P0 必须绿）

沿用现有网关，不得降级：

- ✅ `src/cache/evictor/*` 单测：LRU 顺序、LFU 频率、空 cache、并发 `on_access` 无死锁
- ✅ `manager.rs`：`eviction_per_dir_*`、`moka_evictor_concurrent_gets_no_deadlock` 改编为 foyer
- ⬜ `tests/page_cache_consistency.rs`：INV-PC-D1/D2/S1/S2
- ⬜ `tests/page_cache_e2e.rs`（有集群时）

后两项**尚未跑通**，卡在环境而非代码。两点排查结论值得留档：

1. **认证**：模块文档写的 `GOOSEFS_AUTH_TYPE=nosasl` 会被 master 拒（`AuthenticationFailed ... not authenticated for call ... CreateFile`），实际要用 `simple`。测试里 `auth_type()` 的 fallback 也是 `NoSasl`，即不设环境变量同样会失败。**跑之前必须显式 `GOOSEFS_AUTH_TYPE=simple`**，否则 9 个用例全红且报错指向认证，容易误判成代码问题。

2. **worker 地址过期**：过了认证后卡在 `TransportError ... tcp connect error, 10.64.80.50:9203, Connection refused`。master 的 `fsadmin report capacity` 显示 worker 注册名是 `10.64.80.50:9203` 且 `IsAlive=true` 心跳正常，但宿主机 IP 已变为 `10.64.80.53`。原因是 `conf/goosefs-site.properties` 未设 `goosefs.worker.hostname`，worker 启动时自动探测 IP 并注册，之后 DHCP 换了地址，注册名就失效了——**心跳仍然正常，所以 report 看起来是健康的，这一点有迷惑性**。

   修法：重启 worker 让它重新探测；或先设 `goosefs.worker.hostname=localhost` 再重启，避免复发。判据是 `fsadmin report capacity` 里的地址变成当前 IP。

新增（均已落地）：

- **N=100_000 的驱逐预算测试** —— `foyer_evictor::tests::eviction_cost_does_not_scale_with_page_count`。两条断言：
  - 比值：100k 页的耗时 < 10k 页的 5×（真正的 O(N) 扫描会是 ~10×）
  - 绝对值：100k 页时 < 50 µs

  两条都要，因为比值单独看不住「两端都慢」的退化。测量用「重复 N 轮取最小值」而不是平均，避免并行跑测试时的调度噪声让墙钟断言变 flaky。
- 满容后连续 `put` 超出配额 —— `manager::tests::sustained_puts_past_quota_keep_accounting_and_disk_in_sync`，校验 `used_bytes`、`meta`、盘上页文件三者一致。注意方式 1 允许**瞬时**超配额（§5.7），断言的是收敛后的稳态。

### 8.2 性能（P0 的存在理由）

扩展 `benchmarks/cache_evictor_bench.rs`：

| 场景 | N | 命中率 | 并发 | 看什么 |
|---|---|---|---|---|
| 热命中 | 1e5 | 100% | 1, 32 | foyer 不比 moka 的 `on_access` 差 |
| **满容回填（主指标）** | **1e5（100 GB 缩比也可用 1 MiB×1e5 在内存盘/小页模拟）** | **99%** | 8, 32 | miss 的 P99、evict 耗时、同 dir 填入吞吐 |
| 冷启动填满 | 1e5 | 0% | 8 | 锁串行是否消失 |

bench 分 4 个 phase，用 `BENCH_PHASES` 选跑（跑满 N=1e5 全矩阵约 45 分钟，每个 (后端, 策略) 组合都要灌 10 万页）：

| Phase | 场景 | 关键环境变量 |
|---|---|---|
| 1 | 热命中（100% 命中，有容量余量） | `BENCH_NUM_PAGES` `BENCH_CONCURRENCY` |
| 2 | 满容驱逐，单并发扫 N | `BENCH_EVICT_SCALE` `BENCH_EVICT_OPS` |
| 3 | **满容回填（主指标）**，99% 命中 + 并发 | `BENCH_REFILL_PAGES` `BENCH_REFILL_CONCURRENCY` `BENCH_HIT_PERCENT` |
| 4 | 冷启动填满 | `BENCH_COLD_PAGES` `BENCH_COLD_CONCURRENCY` |

Phase 3 的读取只落在**实际驻留**的页上（populate 后先扫一遍拿到驻留集），否则命中率会被 §5.8 的分片折损决定而不是被 workload 决定——初版没做这一步，实测命中率只有 48%。

#### 实测结果（macOS / tokio::fs / 1 KiB 页 / N=1e5）

**Phase 1 — 命中路径（验收线：不回退超过 10%）**

| 策略 | 并发 | moka | foyer | 判定 |
|---|---|---|---|---|
| LFU | 1 | 355.9 µs | 357.1 µs | +0.3% ✅ |
| LFU | 32 | 1.22 ms | 1.28 ms | +4.9% ✅ |
| LRU | 1 | 355.3 µs | 372.3 µs | +4.8% ✅ |
| LRU | 32 | 1.27 ms | 1.17 ms | −7.9% ✅ |

四个点全部落在 ±10% 内，**命中路径不回退**这条验收通过。

**Phase 3 — 满容 + 99% 命中 + 并发回填（主指标）**

| 策略 | 并发 | fill avg (moka→foyer) | fill p99 (moka→foyer) |
|---|---|---|---|
| LFU | 8 | 34.82 ms → **4.99 ms** | 84.96 ms → **15.64 ms**（5.4×） |
| LFU | 32 | 68.08 ms → **20.74 ms** | 192.78 ms → **33.99 ms**（5.7×） |
| LRU | 8 | 38.37 ms → **5.99 ms** | 117.93 ms → **80.01 ms**（1.5×） |
| LRU | 32 | 61.50 ms → **20.99 ms** | 160.98 ms → **35.31 ms**（4.6×） |

实测命中率 99.6%–100%，符合设定。

**一个必须说明的读数**：同一张表里 foyer 在 conc=8 的 hit avg 比 moka 高 16–19%（1.95 vs 1.64 ms），单看像是踩了验收线。但这不是 evictor 变慢——两个后端的**总吞吐几乎相同**（41 vs 39 fills/s，总墙钟时间基本一致）。原因是 foyer 的 fill 快 7×，任务不再被堵在驱逐上，于是把更多并发压力转移到了读盘上，延迟在 fill 和 read 之间重新分配而已。干净的命中路径对照是 Phase 1（无驱逐干扰），那里 N=1e5 两个并发点都通过。conc=32 时该效应消失（−1.1% / +2.4%）。

**Phase 4 — 冷启动填满（N=1e5，1 KiB 页，并发 8）**

| 轮次 | foyer/LFU | foyer/LRU | moka/LFU | moka/LRU | 轮内离散 |
|---|---|---|---|---|---|
| 1（丢弃） | 2040 | 1995 | 2037 | **4811** | 2.4× |
| 2 | 6315 | 5767 | 6487 | 5882 | 12% |
| 3 | 6612 | 6799 | 6766 | 6734 | 3% |

单位 ops/s。第 1 轮整体比后两轮慢 3×且组内发散，那个 moka/LRU = 4811 的离群值在复跑中**不可复现**，是首轮预热效应（紧接在被中止的 40 分钟矩阵之后跑，临时目录清理和文件系统状态未稳定），故整轮丢弃。

以第 2、3 轮为准：**四个配置在冷启动路径上无可测差异，并发 8 没有出现串行化坍塌**。这符合预期——冷启动目录从空填到额定容量，moka 全程不触发扫描，foyer 只因分片不均衡（§5.8）产生个位数百分比的额外驱逐，两者都被盘写主导。这个 phase 验的是「不退化」，不是「有收益」。

方法论上留一条：**这个 benchmark 的第一轮不可信**，比较类结论至少要跑到第二轮。

#### 字节维度：真实 1 MiB 页（N=1000，即 1 GB 目录）

上面那轮是**页数维度**（1 KiB 小页堆出 10 万页，不真占 100 GB）。字节维度另跑一轮验证真实页大小下的 IO 路径：

| Phase | 结果 |
|---|---|
| 1 命中 | conc=1: 145–164 µs；conc=32: 0.92–1.20 ms。四个配置互相在 0.89×–1.30× 内 |
| 2 驱逐 | 250→1000 页全部判定 flat（0.5×–1.3×），foyer 408 µs vs moka 471–568 µs |
| 3 回填 | fill_avg 0.89–3.38 ms，命中率 95.8%–100% |
| 4 冷启动 | 7748–8608 ops/s，四个配置挤在 10% 带宽内 |

**结论：在 1000 页这个量级上两个后端没有可测差异**，这符合预期而不是反例——moka 扫 1000 页本来就便宜（页数维度那张表里 moka 在 N=1e3 也只要 1.0–1.2 ms）。这一轮要验的是真实 1 MiB 页下 IO 路径、记账、驱逐后删盘都正常，这点通过了。**驱逐收益只在页数维度体现，字节维度不放大它。**

两个维度合起来的意思是：决定 evictor 成本的是**页数**，决定 IO 成本的是**字节数**，100 GB 的收益要靠 `100GB / page_size` 得到的页数去推，不能靠总容量。

#### 验收结论（相对 moka O(N) evictor）

- ✅ N=1e5 驱逐耗时 22.9 ms → 0.84 ms，**约 27×**（要求 ≥ 20×）
- ✅ 99% 命中 + 满容 + 并发：fill p99 改善 4.6–5.7×（LRU/conc=8 那个 1.5× 是 80 ms 的 p99 尾巴，样本量偏小）
- ✅ 命中路径不回退超过 10%（Phase 1，四个点全过）
- ✅ Phase 4 冷启动无串行化坍塌（复跑 3 轮，第 2/3 轮四配置一致）
- ✅ 真实 1 MiB 页的 IO 路径、记账、驱逐删盘正常（字节维度）
- ⬜ 真机 100 GB 介质、集群门禁（`page_cache_consistency` / `page_cache_e2e`）：未跑，见下

可用 `BENCH_NUM_PAGES=100000 BENCH_PAGE_SIZE=1024` 在不真占 100 GB 的情况下模拟 **页数**；另用较小 N + 真实 1 MiB 页验证 IO 路径。**页数维度和字节维度都要测**——上面这轮只做了页数维度。

### 8.3 P1 额外

- restore：进程重启后 hit（`RecoverMode`）
- `on_file_open` 改 mtime 后旧 key 全部 miss
- Linux uring vs psync A/B（可复用 `cache_uring_bench` 思路）
- 升级：旧目录文件被忽略或需手动清空，不得读出错误字节

---

## 9. 迁移步骤与回滚

```text
Week 0  MSRV 调查 + foyer 0.22 API 钉死 evict_candidate 的接法（方式 1 vs 2）
Week 1  FoyerCacheEvictor + 单测 + evict 微基准（N=1e3/1e4/1e5）
Week 2  接入 build_evictor，默认 foyer，保留 moka backend 开关
        cache_evictor_bench 99% 命中满容对照
        consistency / e2e
Week 3  内部 workload 复现 100 GB 级（或等页数缩比）后合入
P1      另开设计/排期：HybridCache、删 UringPageStore、删 moka
```

回滚：

- P0：配置 `eviction.backend=moka` 或 revert evictor 文件；盘格式不变，**无需清 cache 目录**。
- P1：回滚代码后 cache 目录格式不兼容，必须换目录或清空。Release note 强制写。

---

## 10. 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| foyer 无稳定的「踢一条」API | 方式 2 落不成 | 改方式 1；listener 只投递 victim，删盘仍走现有 `pop_victim` 后半段 |
| MSRV 1.91 卡住下游 | 编译失败 | 作为 P0 门禁；bump 或 pin 旧版，禁止含糊合入 |
| w-TinyLFU ≠ Java 纯 LFU | 驱逐选择不完全对齐 | 可接受（今天 moka TinyLFU 已经不是纯 LFU）；文档说明 |
| listener 死锁 | 卡住读路径 | 禁止在 `on_leave` 抢 SDK 锁；单测 concurrent put/get/invalidate |
| P1 双 io_uring | 资源争用 | P1 必须下线自研 driver |
| foyer 快速迭代破坏 API | 编译/行为漂 | 锁 `0.22`，升级单独 PR |
| 100 GB 真机与缩比不一致 | 误判 O(1) | 合入前至少一次真 10 万页（可用小页堆出 N，不强制 100 GB 介质） |

---

## 11. 决策清单（评审时勾选）

- [x] ~~确认 P0 采用方式 2 还是方式 1~~ → **方式 2 在 foyer 0.22.3 上不可实现（§3.4），落方式 1**；待确认的是 §5.7 的配额瞬时超出语义
- [x] ~~确认 MSRV：升 1.91 / pin 旧 foyer~~ → **无需变更，1.88 实测通过（§3.4）**
- [ ] 确认默认后端切 foyer 后，moka evictor 保留几个版本
- [ ] 确认 P1 是否立项（可只做 P0）
- [ ] 确认 100 GB / 10 万页 / 99% 命中 的验收环境（真机 vs 等页数缩比）

---

## 12. 参考

- [foyer-rs/foyer](https://github.com/foyer-rs/foyer)
- [foyer Architecture](https://foyer-rs.github.io/foyer/docs/design/architecture)
- [foyer Hybrid Cache setup](https://foyer-rs.github.io/foyer/docs/getting-started/hybrid-cache)
- [foyer EventListener 示例](https://github.com/foyer-rs/foyer/blob/main/examples/event_listener.rs)
- 本仓库：`src/cache/evictor/moka_evictor.rs`、`src/cache/manager.rs`（`pop_victim`）、`src/cache/store/uring/store.rs`（`PAGE_FD_CACHE`）
- [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md) §10.1（当初自研 evictor vs moka 的取舍；现状已偏离该文档「未引入 moka」的表述）
