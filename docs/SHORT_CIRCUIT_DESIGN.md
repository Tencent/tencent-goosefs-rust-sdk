# Rust ShortCircuit 设计与实现文档（Rust SC 短路版本）

> 项目：`goosefs-client-rust`
> 目标：在功能等价于 Java `LocalFileDataReader` / `LocalFileBlockReader` 的前提下，**性能严格优于** Java SC 实现（尤其在高并发 PositionedRead 场景下达到 5×~50× 吞吐提升）。
> 适用版本：feature/short-circuit 分支起。
> 修订日期：2026-06-24

---

## 0. TL;DR（一页能讲完的核心结论）

| 维度 | Java SC | Rust SC（本设计） | 收益方向 |
|---|---|---|---|
| 控制面（OpenLocalBlock bidi） | gRPC | gRPC（一致） | 持平 |
| 块文件打开 | RandomAccessFile + FileChannel | File → mmap 后立即 drop(File) | 省 1 个 fd / block |
| mmap 粒度 | per-chunk: FileChannel.map(off,len) | once-per-block: Mmap::map(&file) | 省 N-1 次 mmap syscall |
| 内核预读（L1） | 系统默认（PR 时浪费） | MADV_RANDOM / SEQUENTIAL / NORMAL 按场景切换 | 节省内存带宽 |
| 应用层预读（L2） | **无**（SC 路径忽略 prefetchWindow） | `prefetch` / `prefetch_many` → MADV_WILLNEED | 冷数据 p99 数十× 改善 |
| 数据出借 | MappedByteBuffer → NioDataBuffer 包装 | &[u8] 借用 + Bytes 零拷贝 | 真正零拷贝 |
| 线程模型 | 阻塞 IO（GrpcBlockingStream） | tonic async + mmap 同步直调（无 spawn_blocking） | 省线程切换 |
| 锁生命周期 | Factory.close 关流 | OpenLocalBlockGuard Drop 关流 | RAII，泄漏豁免 |
| 容错 | NotFoundException → fallback gRPC | Err → fallback gRPC + 缓存负向条目 | 失败也快 |
| 复用 | per-BlockInStream reader | per-task block-id LRU 缓存 | 重读无新建 |
| 可观测性 | Java metrics | tracing + Prometheus + 计数器 | 同等或更细 |
| 大 page | 不支持 | THP via `MADV_HUGEPAGE` opt-in（>=2MB block，收益视内核/FS 而定） | 可能减 TLB miss |

预期端到端收益（256 并发 × 64KB PositionedRead，1GB block）：

- mmap syscall：Java ≈ 16k/s，Rust ≈ 0
- 用户态拷贝：Java 每读 1 次 + 64KB；Rust 0 次（caller 直接消费 &[u8]）
- p99 延迟：Java 800µs~3ms（mmap 抖动）→ Rust < 50µs（纯 page-fault + memcpy）
- 吞吐上限：受限于内存带宽 / page cache 命中率，Rust 可打满 NUMA 单节点带宽

---

## 1. 设计目标与非目标

### 1.1 设计目标（Must）

0. **一致性优先（最高优先级，先于性能与所有其他目标）**

   - **0a. 数据一致性（Data Consistency）**：SC 路径在任意 `(offset, len)` 上返回的字节序列，必须与同一时刻经 gRPC 路径读取同一 block 得到的字节序列**逐字节相同**；mmap、`MADV_WILLNEED`、LRU 缓存、`Bytes` 零拷贝、HugeTLB、SIGBUS 兜底等任何优化路径都**不得**引入：撕裂读（torn read）、过期读（stale read）、越界读、跨 block 串读。
   - **0b. 语义一致性（Semantic Consistency）**：SC 路径对上层（`BlockInStream` / `FileInStream`）暴露的所有可观察行为必须与 gRPC 路径**语义等价**，包括但不限于：相同 `(block_id, offset, len)` 输入产生相同的成功/错误分类（NotFound、OutOfRange、Permission）、EOF 判定、短读语义、零长度读处理、reader Drop 之前 Worker 侧锁始终持有、capability 鉴权效果、SC→gRPC fallback 对调用方透明且不改变 `read` 返回值序列。

   一致性是**硬约束**：第 3 章的任何性能优化，若与 0a / 0b 冲突，必须以一致性为准、放弃或降级该优化；§1.3 的不变式表是 0a / 0b 的可验证细化。

1. **协议兼容**：与 GooseFS Worker 现有 OpenLocalBlock bidi gRPC 协议完全兼容，无须 Worker 改动。
2. **功能等价**：覆盖 Java SC 的所有合法路径——本地决策、open-lock、读、unlock、失败回退、capability 鉴权。
3. **性能严格超越 Java**：在以下三个基准上必须不劣于、目标超越：
   - 顺序读（chunk_size=8MB）：吞吐 ≥ Java × 1.2
   - PositionedRead（offset random，buf=64KB）：吞吐 ≥ Java × 5
   - 高并发（256 threads × 同 block）：p99 延迟 ≤ Java / 10
4. **零额外不安全面**：所有 unsafe 必须有显式 SAFETY 注释，覆盖 SIGBUS、TOCTOU、生命周期三类风险。
5. **优雅降级**：任何 SC 失败必须无缝 fallback 到 gRPC，且失败原因可观测；fallback 切换必须满足 0b 语义一致性。

### 1.2 非目标（Won't）

- 不支持 Unix Domain Socket 数据面（tonic 限制，Java 用 DS 走 netty，Rust 不引入额外传输）。
- 不实现 mmap 写路径（block 文件 CommitBlock 后即不可变，写仅经 Worker）。
- 不替代 Worker 端逻辑（pin、evict、commit 仍归 Worker）。
- 不维护跨进程共享 mmap 表（每个客户端进程独立映射）。

### 1.3 全局一致性不变式（Invariants）

下列不变式在任意代码路径上必须成立，违反即视为正确性 bug，优先级高于性能回退；它们是 §1.1 中目标 0a / 0b 的可验证细化，并在 §3 / §5.3 / §8.4 中被引用与论证。

| ID | 不变式 | 类别 | 验证手段 |
|---|---|---|---|
| INV-D1 | block 文件在 reader 生命周期内内容不可变（Worker 持锁不 truncate / replace / 改写已落盘 block） | 数据 | 协议约束 + SIGBUS handler 兜底 |
| INV-D2 | mmap 切片 `&mmap[off..off+len]` 的字节内容 = Worker 端同 block 同范围 pread 的字节内容 | 数据 | `sc_consistency` 测试：SC vs gRPC 双读 diff |
| INV-D3 | `read_bytes` 返回的 `Bytes` 在其生命周期内对应内存不会被 munmap | 数据 | `Arc<Mmap>` 作为 owner，结构体字段 Drop 顺序保证 |
| INV-D4 | `prefetch` / `prefetch_many` 不修改任何字节，仅是 readahead hint | 数据 | `madvise(MADV_WILLNEED)` 语义保证 + 单测 |
| INV-S1 | SC 失败 fallback 到 gRPC 后，上层读到的字节序列与"始终走 gRPC"完全相同 | 语义 | 故障注入测试 |
| INV-S2 | reader 存活期间 Worker 侧 OpenLocalBlock 锁始终持有；reader Drop 触发异步解锁，最坏由 reaper / Worker 会话超时回收，不无限泄漏 | 语义 | 字段声明顺序 + Drop 顺序审查 + reaper 超时（§8.2.1） |
| INV-S3 | capability 鉴权语义与 gRPC 路径完全一致（开启的集群上未带 capability 必拒） | 语义 | 集成测试覆盖 |
| INV-S4 | 错误分类稳定：`OutOfRange` 等语义错误不被 fallback 吞掉，必须上抛 | 语义 | §3.6 决策矩阵 + 单测 |
| INV-S5 | `read` / `read_bytes` / `read_to_slice` 三 API 在相同 `(offset, len)` 输入下返回的字节内容一致 | 语义 | 单测三路径 diff |

---

## 2. 架构总览

```
┌──────────────────────── Rust Client Process ────────────────────────┐
│                                                                       │
│  ┌────────────────┐   should_use_sc()   ┌──────────────────────────┐ │
│  │ BlockInStream  │ ───────────────────▶│  WorkerRouter            │ │
│  │  ::create()    │                     │   .is_local_worker(id)   │ │
│  └────────────────┘                     └──────────────────────────┘ │
│         │ yes                                                         │
│         ▼                                                             │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ ShortCircuitFactory (per task / per FileInStream)               │ │
│  │  ┌────────────────────────┐   ┌──────────────────────────────┐ │ │
│  │  │ LRU<block_id, Reader>  │   │ NegativeCache<block_id, t>   │ │ │
│  │  │  (hot block reuse)     │   │  (avoid re-trying SC for     │ │ │
│  │  └────────────────────────┘   │   recently-failed blocks)    │ │ │
│  │                               └──────────────────────────────┘ │ │
│  │                  open() if miss                                  │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│         │                                                             │
│         ▼                                                             │
│  ┌──────────────────────────────────────────────────────────────────┐│
│  │ LocalBlockReader  (single block, lifetime = read session)        ││
│  │   ┌─────────────────────────────┐  ┌───────────────────────────┐ ││
│  │   │ OpenLocalBlockGuard         │  │ Arc<Mmap>  (whole block)  │ ││
│  │   │  (bidi stream Sender alive) │  │  + advise(MADV_RANDOM)    │ ││
│  │   └─────────────────────────────┘  └───────────────────────────┘ ││
│  │                                                                   ││
│  │   read(off,len) -> &[u8]      (no syscall, pure slice)           ││
│  │   read_bytes(off,len) -> Bytes (zero-copy, ref-counted)           ││
│  └──────────────────────────────────────────────────────────────────┘│
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
              │ gRPC (control plane only)
              ▼
        GooseFS Worker  (locks block, returns path, unlocks on stream close)
```

关键不变式：

- 一个 block 在 Reader 生命周期内**至多一次 mmap**。
- `OpenLocalBlockGuard` 的 Drop 顺序**晚于** `Mmap` 的 Drop（结构体字段顺序保证）——保证 mmap 生效期间锁始终持有（对应 INV-S2）。
- `Arc<Mmap>` 可被多个并发 `read_bytes` 共享，但 guard 只此一份（对应 INV-D3）。
- **一致性硬约束**：所有数据/语义路径必须满足 §1.3 的 INV-D1~D4 与 INV-S1~S5；本章后续的所有性能优化在论述时都以这些不变式为前提。

---

## 3. 与 Java 的逐项对比与改进

### 3.1 控制面（OpenLocalBlock）

| 项目 | Java | Rust SC |
|---|---|---|
| 协议 | bidi gRPC | bidi gRPC（一致） |
| 客户端 | GrpcBlockingStream（阻塞） | tonic async + mpsc::Sender 维持发送端 |
| 请求字段 | block_id, block_size, capability? | 全部字段一致；capability 必须支持（Rust SC 补齐） |
| 锁释放 | Factory.close() → mStream.close() + waitForComplete（**同步等待**） | OpenLocalBlockGuard Drop → sender 关闭 → Worker onCompleted（**异步/最终释放**，由 reaper + Worker 会话超时兜底，见 §8.2.1） |
| 超时 | USER_STREAMING_DATA_TIMEOUT | 同名配置，默认 30s，async tokio::time::timeout |

**Rust SC 改进点**：

1. **capability 注入**：早期原型与现有读路径都标 `capability: None`（事实核对：`worker.rs` 的 `read_block`/`write_block` 构造 `ReadRequest`/`WriteRequest` 时 `capability: None` 写死，L383/L425/L477）。SC 路径必须在 capability-enabled 集群上带上有效 capability，否则会被 Worker 拒绝并 fallback 到 gRPC，浪费一次 RTT。
   > **⚠️ capability 来源尚不存在，是 P3 待补项**：dev 当前 `InStreamOptions`（`fs/options.rs` L79）仅有 `read_type` / `position_short` / `max_ufs_read_concurrency` / `prefetch_window`，**没有 `capability_fetcher` 字段**，整个客户端读路径也尚未接入 `Capability`。因此"从某处取 capability 填入"是**需要新增的能力**，不能照抄一个不存在的 API。落地时（§10 P3，随 `worker.rs` 的 `open_local_block` 封装一并做）需先确定来源——候选是 `FileSystemContext` / 鉴权配置（参考 `auth/mod.rs` 的 `CAPABILITY_TOKEN` 与 `config.rs` 的 capability 相关 TODO），来源确定前不得在接口上假定其已存在。
2. **锁可见性**：guard 内嵌 Drop 时间戳到 tracing span，便于排查"锁未释放"。
3. **多 RPC 并行**：当上层一次性需要多个 block（向量化批读）时，提供 `open_local_blocks_batched(Vec<id>) -> Vec<Result<Reader>>`，并发 N 路 bidi，把 N 次 RTT 压到 1 次（Java 没有此能力）。

### 3.2 数据面（mmap 策略）—— 核心优化

#### Java 行为（事实核对）

```java
// LocalFileBlockReader.java:97
public ByteBuffer read(long offset, long length) {
    return mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
}
```

```java
// LocalFileDataReader.java:67
public DataBuffer readChunk(int prefetchWindow) {
    ByteBuffer buffer = mReader.read(mPos, Math.min(mChunkSize, mEnd - mPos));
    return new NioDataBuffer(buffer, buffer.remaining());
}
```

→ 每个 chunk 一次 mmap syscall（chunk 默认 8MB）。
→ 1GB block 顺序读 ≈ 128 次 mmap；
→ 但 PositionedRead 模式下 chunk 与 buf 等大（如 64KB），1GB ≈ **16k 次 mmap**；
→ 256 并发热 block 共享场景：**百万级 mmap/s**，page-table lock + VMA 红黑树成为瓶颈。

#### Rust SC 行为

```rust
// open() 一次：
let mmap = unsafe { Mmap::map(&file) }?;       // 1 次 mmap
mmap.advise(Advice::Random)?;                  // 1 次 madvise（L1：关闭内核预读）
drop(file);                                    // fd 立即释放，inode 由 VMA 持有

// 可选：上层拿到 PR offset 列表后立刻批量预读（L2，异步）
reader.prefetch_many(&[(off1,len1), (off2,len2), ...])?;  // 1 次 madvise/邻接段

// read() N 次：
&self.mmap[offset..offset+len]                 // 0 次 syscall，纯指针运算
```

| 指标 | Java | Rust SC | 倍率 |
|---|---|---|---|
| mmap syscall / 1GB 顺序读 | 128 | 1 | 128× |
| mmap syscall / 1GB PR @ 64KB | 16,384 | 1 | 16,384× |
| VMA 数量 / reader | 等于 read 次数 | 1 | N× |
| fd 占用 / reader | 1 | 0（drop 后） | 省 1 |
| page-table lock 竞争 | 高 | 几乎为零 | 显著 |
| 预读策略 | 系统默认（顺序友好，PR 时浪费） | MADV_RANDOM 关预读 | 节省内存带宽 |

#### 风险与缓解

| 风险 | 描述 | Rust SC 缓解 | 一致性影响 |
|---|---|---|---|
| **SIGBUS** | Worker 在客户端 mmap 持有期间 truncate/unlink 被替换的 inode | 数据一致性的真正根基是 INV-D1（Worker 持锁期间不改写/不截断已落盘 block），正常路径下 SIGBUS **不应发生**。<br>**不依赖** "信号转 panic + catch_unwind"——SIGBUS 发生在 libc `memcpy` 等任意故障指令处，从异步信号 handler 穿过信号 trampoline / libc 栈帧做 Rust unwind 是 UB，`catch_unwind` 也捕不到信号。<br>缓解分层：1) 注册 SIGBUS handler **仅做诊断（记录 block_id/addr/tracing）后 `abort`**，把"协议被破坏"暴露为致命错误而非静默错读；2) 需要在不可信 FS 上换鲁棒性的部署，用 `io.mode=pread`（§11.4）走 `pread64` 数据面，从源头避免 mmap 缺页 SIGBUS；3)（可选、高级）若必须在 mmap 模式下软恢复，只能用 per-thread `sigsetjmp/siglongjmp` 包住"纯 memcpy"那一小段，且无法保护调用方直接消费 `&[u8]` 的场景——成本高、收益有限，默认不启用 | 关联 INV-D1：一致性由"Worker 不改写已锁 block"保证，而非靠捕获 SIGBUS。`io.mode=pread` 鲁棒路径仍满足 INV-S1（与全程 gRPC 等价）；mmap 模式下 SIGBUS=致命，宁可 abort 也不返回撕裂/过期字节 |
| **虚拟地址耗尽** | 64-bit Linux 128TB VA，理论无忧；32-bit 不支持 | 文档显式声明仅支持 64-bit Linux/macOS | 无 |
| **RSS 膨胀** | 大量 hot block 全部触达页面 | LRU 缓存上限 + idle TTL；Drop 触发 munmap | LRU 淘汰必须在所有引用（含 `Bytes` clone）释放后才真正 munmap，由 `Arc<Mmap>` 保证（INV-D3） |
| **NFS / 慢盘** | 缺页时阻塞工作线程 | 仅在 is_local_worker == true 时启用；NFS 远程 block 不会走此路径 | 无 |

### 3.2.1 三层预读模型（Java SC 缺失，Rust SC 完整覆盖）

Rust SC 路径上的预读分三层，必须分别归位、按场景调度。Java SC 在 `LocalFileDataReader` 路径上**全部缺失**——传入的 `prefetchWindow` 参数被直接忽略，仅在 `GrpcDataReader` 远程路径才生效。

| 层 | 名称 | 触发主体 | 机制 | Rust SC 表达 | Java SC | 适用场景 |
|---|---|---|---|---|---|---|
| L1 | 内核 readahead | Linux page cache | `mmap` 默认行为 + `madvise` hint | `AccessHint::{Sequential, Random, Default}` → `MADV_*` | 无（FileChannel.map 不暴露 madvise） | 顺序读放大预读窗口；PR 关闭预读 |
| L2 | 应用层 prefetch | 客户端调用方 | `madvise(MADV_WILLNEED)` 异步触发 readahead | `prefetch(off,len)` / `prefetch_many(&[ranges])` | **无**（prefetchWindow 被忽略） | 已知 offset 列表的冷数据 PR；流式 reader 提前拉取下一 chunk |
| L3 | 上层 IO 调度 prefetch | Lance ReadBatch / `take()` | 业务侧批并发 | 不在 SC 范围，由调用方实现 | 同 | 跨多 block 的并发预取 |

**L1 决策矩阵**：

```
hint = match (workload, cfg.advise) {
    (PositionedRead, _)         => MADV_RANDOM,      // 关预读，避免内存带宽浪费
    (Sequential, _)             => MADV_SEQUENTIAL,  // 预读窗口 ×2
    (Unknown, "none")           => no madvise,
    (Unknown, _)                => MADV_NORMAL,
}
```

**L2 价值矩阵**：

| 场景 | 是否调用 prefetch | 预期收益 |
|---|---|---|
| 冷数据 + 顺序 chunk 流 | 在消费当前 chunk 时 prefetch 下一 chunk | 掩盖磁盘缺页延迟 ≈ 单盘延迟 |
| 冷数据 + Lance `take(rows[])` 已知 N 个 offset | 一次性 `prefetch_many(ranges)` | p99 改善 10× ~ 50×（实测随磁盘类型） |
| 热数据（page cache 命中） | 调用即可，零开销 | no-op（内核短路） |
| PR 单点未知后续 | 不调用 | 避免误读浪费 |

**L2 实现要点**：

- `MADV_WILLNEED` 是**异步**的：内核登记 readahead 任务后立即返回，不阻塞调用线程。
- `prefetch_many` 在内部对邻近的 `(offset, len)` 做合并（merge & sort），减少 `madvise` 次数。
- 对热页是 no-op：内核检查 page 已 present 直接跳过。
- 调用成本：单次 `madvise` 典型 < 5µs。
- **一致性边界（INV-D4）**：prefetch 仅是 readahead hint，不读取、不修改、不返回任何字节；其成功与失败都不得改变后续 `read` 的返回内容。`prefetch` / `prefetch_many` 在禁用或 FS 不支持 `MADV_WILLNEED` 时静默降级为 no-op，结果数据一致性不受影响。

**跨平台差异（madvise 矩阵）**：上表以 Linux 为准。macOS 上 `MADV_RANDOM` / `MADV_SEQUENTIAL` / `MADV_WILLNEED` 存在但语义更弱（`WILLNEED` 的异步 readahead 效果不保证），且**没有 `MADV_HUGEPAGE`、也没有 `MAP_HUGETLB`**。因此：`AccessHint::Default`（不调用 madvise）是各平台都安全的默认；THP（§11.1）与冷数据 prefetch 收益仅在 Linux 上有意义，macOS 上 `prefetch` 退化为尽力而为/可能 no-op，但仍满足 INV-D4。

### 3.3 数据出借 / 拷贝

Java：

```java
new NioDataBuffer(buffer, buffer.remaining());   // 包装，但下游 readBytes 会 copy 到 byte[]
```

Rust SC：

- `read(off,len) -> &[u8]`：纯借用，零拷贝，生命周期绑定 reader。
- `read_bytes(off,len) -> Bytes`：用 `bytes::Bytes::from_owner(Arc<Mmap>)` 把 mmap 切片包装为引用计数 Bytes，**真正零拷贝**且可跨 await 边界传递。
- `read_to_slice(off, dst)`：一次 `copy_from_slice`，对应"上层一定要拥有 buffer"的场景。

**一致性约束（INV-D3 / INV-S5）**：三个 API 在相同 `(offset, len)` 输入下必须返回字节内容相同的视图；`read_bytes` 通过 `Arc<Mmap>` 把 mapping 生命周期延长到最后一个 `Bytes` 被 Drop，保证零拷贝路径的 owner 永远活到引用结束。

```rust
/// Newtype so that `Arc<Mmap>` can be handed to `Bytes::from_owner`
/// (which requires `AsRef<[u8]> + Send + 'static`). `Arc<Mmap>` itself
/// does NOT implement `AsRef<[u8]>`, hence the wrapper.
struct MmapChunk(Arc<Mmap>);
impl AsRef<[u8]> for MmapChunk {
    fn as_ref(&self) -> &[u8] { &self.0[..] }
}

pub fn read_bytes(&self, offset: usize, len: usize) -> Result<Bytes> {
    self.bounds_check(offset, len)?;
    // No unsafe: `Bytes::from_owner` (bytes >= 1.9) keeps the owner
    // (Arc<Mmap>) alive for as long as the returned Bytes (and any
    // clone / sub-slice) lives. We map the whole block once and then
    // narrow to the requested window with `.slice()`.
    let full = Bytes::from_owner(MmapChunk(Arc::clone(&self.mmap)));
    Ok(full.slice(offset..offset + len))
}
```

> 实现说明：本仓库已锁定 `bytes = "1.11.1"`，其 `Bytes::from_owner`（1.9 起）原生支持把任意 `AsRef<[u8]> + Send + 'static` 的 owner 包成零拷贝 `Bytes`，因此**无需 unsafe**。`from_owner` 返回覆盖整段映射的 `Bytes`，再用 `.slice(off..off+len)` 收窄到请求窗口（`slice` 仅调整指针/长度，不拷贝）。注意 `Arc<Mmap>` 本身不实现 `AsRef<[u8]>`，必须用上面的 `MmapChunk` newtype 包一层。若未来降级到不支持 `from_owner` 的 bytes 版本，可用 `Bytes::from(Vec)` + 一次 copy 退化（仍不劣于 Java）。

### 3.4 线程模型

| 项目 | Java | Rust SC |
|---|---|---|
| 控制面 | GrpcBlockingStream 阻塞 | tonic async；await 自然让出 |
| File::open + mmap | netty IO 线程外做（隐式） | 直接 sync 调用，注释说明 "mmap 不阻塞数据 IO，仅元数据" |
| 数据读 | netty pipeline → 用户线程 | 调用线程直接 slice / memcpy |

**Rust SC 决定**：取消早期原型的 `tokio::task::spawn_blocking(File::open + mmap)`。理由：

- mmap **syscall** 在 Linux 上不进行数据 IO，只触发 VMA 分配 + 页表占位；典型耗时 < 50µs。
- spawn_blocking 跨线程切换 + scheduler 唤醒 ≈ 5-20µs，对 `open` 这种短任务**可能比直接调用还慢**。
- 实测 NFS 极端场景再加回 spawn_blocking，由配置 `goosefs.client.short.circuit.open.blocking = true` 控制。

> **关键约束（mmap + async 的经典坑）**：`mmap` syscall 本身不做 IO，但**随后读字节会触发缺页**。page cache 命中时是 minor fault（微秒级，可在 async 线程直接读）；**冷数据的 major fault 会做同步磁盘 IO 并阻塞当前 tokio worker 线程**，进而饿死该线程上的其它 task。因此本设计的低延迟保证（§5.2 的 p99 < 50~80µs）**仅在 page cache 命中（热数据）时成立**。冷数据路径必须二选一：
> 1. 先 `prefetch` / `prefetch_many` 触发异步 readahead，并在数据**驻留后**再把 `&[u8]` / `Bytes` 交给 async 消费者；或
> 2. 把真正 touch 字节的读操作放到 `spawn_blocking`（由 `open.blocking` 同族开关或调用方决定）。
>
> 换言之，"数据面在调用线程直接 slice/memcpy" 的优化前提是**热 cache**；冷场景下不得在 async runtime 线程上同步触达未驻留页面。这条与 §5.2、§11.5（FS 探测）联动。

### 3.5 复用与缓存

Java：每个 BlockInStream 一个 LocalFileBlockReader，结束即丢。

Rust SC：

- **Per-task LRU 缓存**：`HashMap<block_id, Arc<LocalBlockReader>>`，容量默认 64，TTL 默认 30s。
- **Negative cache**：最近 N 秒 SC 失败的 block 不重试 SC，直接走 gRPC，避免反复 OpenLocalBlock 失败。
- **跨任务共享**：可选 SharedLocalReaderPool（进程级），权衡引用计数原子开销 vs 命中率。

### 3.6 失败回退

| 阶段 | 失败原因 | Rust SC 行为 | 一致性 |
|---|---|---|---|
| source_is_local 预筛 | 服务该 block 的 worker 非本地 | 直接 gRPC，不调用 OpenLocalBlock（省 1 RTT） | INV-S1：与全程 gRPC 等价 |
| OpenLocalBlock RPC | NotFound（block 不在本地）/ IO error | warn + negative cache + gRPC fallback | INV-S1 |
| File::open | EACCES（uid 不一致） | 首次记录 hint，永久禁用 SC（per-process flag） | INV-S1 |
| Mmap::map | ENOMEM / EINVAL | 缓存条目失败计数 + gRPC fallback | INV-S1 |
| 读切片越界 | 上层 bug | Err，**不** fallback（语义错误必须暴露） | INV-S4：错误分类稳定 |
| capability 拒绝 | 集群启用 capability，请求未带或过期 | 与 gRPC 路径相同的错误分类上抛 | INV-S3 |

**fallback 透明性总则**：除"语义错误"外，所有 SC 失败都必须保证调用方观察到的字节序列与"始终走 gRPC"完全等价（INV-S1）。fallback 不得发生在已经向上层返回部分字节之后，即一次 `read` 调用的成功路径必须是单一来源（要么 SC 完整成功，要么完整走 gRPC）。

### 3.7 决策矩阵

```
should_use_short_circuit(cfg, ctx):
  if !cfg.short_circuit_enabled            -> false       # kill switch
  if !ctx.source_is_local                  -> false       # 预筛：服务该 block 的 worker 是否本地
  if ctx.process_sc_disabled (sticky)      -> false       # past EACCES
  if ctx.negative_cached(block_id)         -> false       # recent failure
  if cfg.huge_block_only && size < 2MB     -> false       # tuning
  return true
```

> **`source_is_local` 的真实来源与语义边界（对照 dev 分支代码）**：
> 客户端没有独立的 `is_local_worker(block_id)`。本地性由 `WorkerRouter` 现有能力派生——`select_worker(block_id)` 已实现 local-first 路由（本地 worker 存在且未失败时所有 block 路由到它），故 `source_is_local` 应组合判定为 `select_worker(block_id).id == local_worker_id`（`local_worker_id` 由 `detect_local_worker` 经 hostname/本地 IP 匹配并 ArcSwap 缓存）。§2 架构图里的 `WorkerRouter.is_local_worker(id)` 是对该组合语义的抽象命名。
>
> **关键：worker 本地 ≠ block 可本地 mmap**。local-first 只保证"由本地 worker 服务"，**不保证该 block 物理就在本地盘**（本地 worker 可能仍需从 UFS/peer 拉取）。因此 `source_is_local` 仅是"避免对远端 worker 发无谓 OpenLocalBlock RPC"的**预筛优化**；block 是否真正可本地读，**最终裁决权在 OpenLocalBlock RPC**——Worker 仅在 block 确实落在本地时回 `path`，否则报错并由下一行（OpenLocalBlock NotFound/IO error → fallback）兜底。实现者**不得**因 `source_is_local == true` 就假定可以 mmap。

---

## 4. 关键 API 设计

### 4.1 LocalBlockReader

```rust
pub struct LocalBlockReader {
    block_id: i64,
    /// 逻辑块大小（来自 OpenLocalBlock 响应 / `block_size`），**不是**磁盘文件
    /// 的物理长度。若 block 文件被预分配/稀疏到比逻辑长度更大，按物理长度
    /// mmap 会把尾部暴露成 0，违反 INV-D2。mmap 长度与所有 bounds_check 都
    /// 必须以此逻辑大小为准。
    file_size: usize,
    /// Whole-block read-only mapping. Created exactly once in `open`.
    mmap: Arc<Mmap>,
    /// Worker-side block lock. Field declared AFTER `mmap` so Drop
    /// order is: mmap first (munmap), then guard (close stream → unlock).
    /// In practice the order is irrelevant for correctness because
    /// the kernel keeps the inode alive via the VMA, but the ordering
    /// makes intent explicit.
    _guard: OpenLocalBlockGuard,
}

impl LocalBlockReader {
    /// `open` 流程（对照 dev 分支 proto `OpenLocalBlockRequest/Response`）：
    ///   1. 发起 OpenLocalBlock bidi 流，请求字段 = { block_id, capability, block_size }
    ///      （三字段与生成的 `OpenLocalBlockRequest` 完全一致）。
    ///      capability 形参类型 `Option<Capability>` 与生成代码精确对齐：proto 字段
    ///      本就是 `Option<Capability>`（block.rs L167），`None` → 不带 capability。
    ///      ⚠️ capability 的**来源**在 dev 尚不存在（`InStreamOptions` 无 `capability_fetcher`，
    ///      读路径未接入 Capability），是 §10 P3 待补项；落地前须确认来源，详见 §3.1 改点1。
    ///      落地时需确认 Worker 对"空 capability"的拒绝逻辑与 gRPC 路径一致（INV-S3）。
    ///   2. 从 `OpenLocalBlockResponse` 取：
    ///        - `path`        → 要 mmap 的本地 block 文件路径（mmap 的唯一目标）
    ///        - `block_size`  → 逻辑块大小 → 即 `file_size`（**不要**用磁盘文件物理长度，见上文字段注释）
    ///      响应只回这两项；mmap 长度与全部 bounds_check 均以 `block_size` 为准。
    ///   3. 持有发送端的 guard，确保会话锁在 reader 存活期内不释放（§8.2.1）。
    /// 入参 `block_size` 是上层已知的期望大小，用于请求；最终以响应回传的 `block_size` 为准。
    pub async fn open(client: &WorkerClient,
                      block_id: i64, block_size: i64,
                      capability: Option<Capability>,
                      hint: AccessHint) -> Result<Self>;

    pub fn read(&self, offset: usize, len: usize) -> Result<&[u8]>;
    pub fn read_bytes(&self, offset: usize, len: usize) -> Result<Bytes>;
    pub fn read_to_slice(&self, offset: usize, dst: &mut [u8]) -> Result<usize>;

    /// L2 应用层预读：通知内核异步把 [offset, offset+len) 拉入 page cache。
    ///
    /// 语义：触发 `madvise(MADV_WILLNEED)`，立即返回（异步 readahead）；
    /// 对已在 page cache 的范围是 no-op；调用本身不阻塞，典型耗时 < 5µs。
    ///
    /// 用法：
    ///   - 流式 reader 在消费当前 chunk 时，prefetch 下一 chunk
    ///   - Lance `take(rows[])` 拿到 offset 列表瞬间一次性预读
    ///
    /// 注意：未越界检查时返回 `OutOfRange`，与 `read` 一致。
    pub fn prefetch(&self, offset: usize, len: usize) -> Result<()>;

    /// L2 批量预读：合并/排序后一次或少量 `madvise` 覆盖所有 range。
    ///
    /// 内部对相邻 `(offset, len)` 做 coalesce，最少化 syscall。
    /// 典型场景：Lance PR 一次拿到 N 个 row offset，构成 N 个 range。
    pub fn prefetch_many(&self, ranges: &[(usize, usize)]) -> Result<()>;

    pub fn block_id(&self) -> i64;
    pub fn file_size(&self) -> usize;
}

pub enum AccessHint {
    Sequential,   // → MADV_SEQUENTIAL
    Random,       // → MADV_RANDOM
    Default,      // no madvise
}
```

#### 与早期原型的差异

| 项目 | 早期原型（goosefs-lance-tests/short_circuit.rs） | Rust SC |
|---|---|---|
| 持有 _file 字段 | 是 | **否**（drop 后省 1 fd） |
| spawn_blocking 包 open + mmap | 是 | 否（直接 sync） |
| MADV_RANDOM | 无 | 有（PR 场景默认） |
| capability | 无 | 有 |
| read_bytes 零拷贝 API | 无 | 有 |
| SIGBUS 注释 | 部分 | 完整（含恢复路径） |

### 4.2 ShortCircuitFactory

```rust
pub struct ShortCircuitFactory {
    client: Arc<FileSystemContext>,
    cache: Mutex<LruCache<i64, Arc<LocalBlockReader>>>,
    /// 负缓存必须**有界**：用 `LruCache`（而非裸 `HashMap`）+ 容量上限 + 每条 TTL。
    /// 裸 HashMap 只在 lookup 时判 TTL、从不主动清扫，面对大量不同的失败
    /// block_id 会无界增长；容量受限的 LRU 可自动淘汰最旧的负向条目。
    neg_cache: Mutex<LruCache<i64, Instant>>,
    cfg: ShortCircuitConfig,
}

impl ShortCircuitFactory {
    pub async fn get_or_open(&self, ctx: BlockReadCtx) -> Result<Arc<LocalBlockReader>>;
    pub fn invalidate(&self, block_id: i64);
}
```

### 4.3 上层集成（BlockInStream::create）

```rust
pub async fn create(...) -> Result<Box<dyn BlockInStream>> {
    if should_use_short_circuit(&cfg, &ctx) {
        match factory.get_or_open(ctx.clone()).await {
            Ok(reader) => return Ok(Box::new(LocalShortCircuitInStream::new(reader, ctx))),
            Err(e) => {
                tracing::warn!(block_id = ctx.block_id, error = %e,
                               "short-circuit failed, falling back to gRPC");
                factory.mark_failure(ctx.block_id);
            }
        }
    }
    create_grpc_block_in_stream(ctx).await
}
```

---

## 5. 性能模型与基准

### 5.1 单次读路径成本拆解

| 步骤 | Java | Rust SC |
|---|---|---|
| Open（一次性，摊销） | TCP RTT + bidi 握手 + RandomAccessFile open | TCP RTT + bidi 握手 + mmap |
| Per read 系统调用 | 1× mmap + 1× munmap（隐式 GC） | 0 |
| Per read 用户态 | MappedByteBuffer 包装 + 下游 readBytes 拷贝 | &[u8] 借用，可选 memcpy |
| Per read 锁竞争 | mmap 触发 mmap_sem 写锁 | 无 |
| 缺页处理 | 同 Rust，等价 | 同 Java |

### 5.2 预期 Benchmark（指标，非实测）

环境：单机本地 Worker，1GB block，page cache 全热。

| 场景 | Java SC 吞吐 | Rust SC 吞吐 | 比值 |
|---|---|---|---|
| 顺序读 64KB×N，1 thread | 8 GB/s | 12 GB/s | 1.5× |
| 顺序读 64KB×N，8 threads | 18 GB/s | 35 GB/s | 1.9× |
| PR 64KB×N 随机，1 thread | 1.2 GB/s | 8 GB/s | **6.7×** |
| PR 64KB×N 随机，256 threads | 3 GB/s（mmap 锁瓶颈） | 25 GB/s | **8.3×** |
| PR p99 latency, 256 threads | 3 ms | 80 µs | **37× 改善** |
| **冷数据** PR 64KB×N，1 thread，无 prefetch | ≈ 单盘 IOPS × 64KB | ≈ 单盘 IOPS × 64KB | ≈1× |
| **冷数据** PR 64KB×N，1 thread，`prefetch_many`（L2） | 不可用 | **盘带宽上限** | **10×~50×**（取决于盘） |
| **冷数据** PR p99 latency，无 prefetch | 受单次缺页延迟主导 | 同 Java | 持平 |
| **冷数据** PR p99 latency，`prefetch_many`（L2） | 不可用 | 接近热数据延迟 | **数十× 改善** |

> 实测时需补充火焰图，定位是否被 page-fault / Arc 原子操作主导。
>
> **前提声明**：上表"热数据"各行的低延迟（p99 < 50~80µs）成立的**充要前提是 page cache 命中**；此时缺页为 minor fault，可在调用线程直接 slice/memcpy。冷数据行的延迟由磁盘缺页主导，且若在 async runtime 线程上同步触达未驻留页面会阻塞该 worker 线程（见 §3.4），因此冷场景必须经 `prefetch` 预热或 `spawn_blocking` 隔离，否则不仅延迟退化，还会拖累同线程其它 task。

### 5.2.1 火焰图采集与查看（命令级 SOP）

火焰图（Flame Graph）是验证本设计每条性能假设的**强制产出**：是否真的没有 mmap syscall？热点是否落在 page-fault？`Arc::clone` 是否成为高并发瓶颈？以下给出从环境准备到差分对比的逐条命令。

#### A. 环境准备（一次性）

**A.1 通用 Rust 端配置**：火焰图必须有完整符号，发布构建默认 strip，需要在 `Cargo.toml` 顶层加：

```toml
[profile.release]
debug = "line-tables-only"   # 保留行号，体积膨胀有限
strip = false                # 不 strip 符号

[profile.bench]
debug = "line-tables-only"
strip = false
```

> **⚠️ 务必与 dev 现有 `[profile.release]` 合并，不要整体覆盖**：dev `Cargo.toml` 的 `[profile.release]` 已含 `lto = "fat"`、`codegen-units = 1`，且**刻意不启用 `panic = "abort"`**（保留 unwind/Drop 语义，正是 §8.2.1 异步 unlock reaper 与 §8.3 panic-safety 论证的依赖）。新增 `debug`/`strip` 时只追加这两个键，**保留现有 `lto`/`codegen-units`、严禁加 `panic = "abort"`**——否则会破坏 SC 的解锁与 panic 安全保证。`[profile.bench]` 在 dev 当前没有显式声明，新增一段无冲突。

或临时用环境变量（不改 Cargo.toml）：

```bash
RUSTFLAGS="-C debuginfo=1 -C strip=none" cargo build --release
```

**A.2 Linux 环境（腾讯云 CVM / TKE 节点，TencentOS Server / OpenCloudOS）**：

腾讯云生产机型当前主要为 **TencentOS Server 2.4 / 3.x**（兼容 CentOS 7 / RHEL 8 包管理）和 **OpenCloudOS 8 / 9**，少量场景使用 Ubuntu 镜像。下面命令以前两者为主，Ubuntu 在末尾给出。

```bash
# 0) 先确认发行版与内核
cat /etc/os-release
uname -r       # 例：5.4.119-19-0009.11（TencentOS 内核命名带 -tlinux/-tencent 后缀）
uname -m       # x86_64 / aarch64（腾讯云 ARM 实例如 SR1/SR2 是 aarch64）
```

**A.2.1 安装 perf（按发行版分支）**

```bash
# —— TencentOS Server 2.4 / CentOS 7 系（yum） ——
sudo yum install -y perf
# 若提示找不到包，换成对应内核的 kernel-tools：
sudo yum install -y "kernel-tools-$(uname -r)" || sudo yum install -y kernel-tools

# —— TencentOS Server 3.x / OpenCloudOS 8+ / RHEL 8 系（dnf） ——
sudo dnf install -y perf
# 同样可能需要：
sudo dnf install -y "kernel-tools-$(uname -r)"

# —— Ubuntu 镜像 ——
sudo apt-get update
sudo apt-get install -y linux-tools-common linux-tools-generic "linux-tools-$(uname -r)"
# 腾讯云 Ubuntu 镜像内核常带 -tlinux 后缀，可能没有完全匹配的 linux-tools-<ver> 包，
# 这种情况下 fallback 用通用包：
sudo apt-get install -y linux-tools-generic
# 然后用 /usr/lib/linux-tools/<ver>/perf 直接调用，或软链：
sudo ln -sf /usr/lib/linux-tools/*/perf /usr/local/bin/perf

# 验证
perf --version
perf list | head    # 能列出事件即 OK
```

> **腾讯云常见坑 1：内核 tools 版本不匹配** —— 自定义镜像或灰度内核（5.4.119-19-0009 等）可能在仓库里没有完全对应的 `kernel-tools` 包。处理顺序：① `yum/dnf install kernel-tools-$(uname -r)` ② 失败则装通用 `kernel-tools`，运行 `perf` 时若提示 `WARNING: perf not found for kernel ...` 但仍可工作即可接受 ③ 仍不行就装 `samply`（见 A.2.4）绕过 perf。

**A.2.2 perf 采样权限（CVM 物理实例 / 虚机 OK，容器内见 A.2.3）**

```bash
# 一次性（重启失效）
sudo sysctl -w kernel.perf_event_paranoid=-1
sudo sysctl -w kernel.kptr_restrict=0

# 持久化
sudo tee /etc/sysctl.d/99-perf.conf >/dev/null <<'EOF'
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
EOF
sudo sysctl --system

# TencentOS Server 默认开启 SELinux=enforcing 的镜像较少，但若开启可能拦截 perf：
getenforce
# 若是 Enforcing 且 perf 报 "Permission denied"，临时：
sudo setenforce 0
```

> **腾讯云常见坑 2：`perf_event_paranoid` 在部分加固镜像里被设为 2 或 3** —— 直接 `cat /proc/sys/kernel/perf_event_paranoid` 看当前值；本设计需要 ≤ 1 才能采到内核栈（page-fault / mmap 才看得见），≤ -1 才能采所有事件。

**A.2.3 在 TKE / 容器（containerd / Docker）里跑 perf**

腾讯云 TKE 工作节点上跑 bench 时，绝大多数 GooseFS Worker 部署在容器中，而 perf 必须能看到**宿主机内核符号**才有意义。两种推荐姿势：

```bash
# 方式 A：直接在宿主机（CVM）上对容器进程采样（推荐，符号最完整）
# 1) 找到容器进程 PID
PID=$(crictl inspect $(crictl ps -q --name goosefs-worker) | jq -r '.info.pid')
# 或 docker：PID=$(docker inspect -f '{{.State.Pid}}' goosefs-worker)

# 2) 在宿主机上对该 PID 采样
sudo perf record -F 999 -g --call-graph dwarf -p "$PID" -o perf_worker.data -- sleep 30

# 3) 解析时需要容器的 rootfs 提供符号（perf 自动通过 /proc/<PID>/root 读取）
sudo perf script -i perf_worker.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl > flamegraph_worker.svg

# 方式 B：在容器内部跑 perf（需要给容器加权限，bench 临时容器才推荐）
# 启动 bench 容器时加：
#   --privileged  或  --cap-add=SYS_ADMIN --cap-add=PERFMON --cap-add=SYS_PTRACE
#   --security-opt seccomp=unconfined
#   -v /sys/kernel/debug:/sys/kernel/debug:ro
#   -v /lib/modules:/lib/modules:ro
#   -v /usr/src:/usr/src:ro
# 然后容器内安装 perf 同 A.2.1，并执行 sysctl 调整（需 --privileged）
```

> **腾讯云常见坑 3：TKE 默认 PodSecurityPolicy / 节点 seccomp profile 会拦截 `perf_event_open(2)`**。如果 bench Pod 启动后 `perf record` 立刻报 `Operation not permitted`，先确认是否带了 `SYS_ADMIN`+`PERFMON`，或直接用方式 A 在宿主机采样。

**A.2.4 ARM 实例（SR1/SR2/标准型 SA 系列 aarch64）注意点**

- `--call-graph dwarf` 在 aarch64 上同样可用，但 `--call-graph fp` 需要 `RUSTFLAGS="-C force-frame-pointers=yes"` **且** 内核 ≥ 5.10 才能正确解析；建议优先用 dwarf。
- 部分 ARM 实例 `perf list` 中硬件 PMU 事件较少，可能只能用软件事件（`cpu-clock`、`task-clock`）；对火焰图采样足够。

**A.2.5 安装 FlameGraph 与 Rust 采样工具**

```bash
# Brendan Gregg 的 FlameGraph 脚本（仅 perl 脚本，无编译依赖）
git clone https://github.com/brendangregg/FlameGraph.git ~/FlameGraph
export PATH=$PATH:~/FlameGraph
echo 'export PATH=$PATH:~/FlameGraph' >> ~/.bashrc

# Rust 一键采样工具（推荐，封装 perf record + stackcollapse + flamegraph.pl）
cargo install flamegraph        # 走 perf，需要 A.2.1 + A.2.2 完成
cargo install samply            # 不依赖 perf，腾讯云加固镜像 / TKE 容器内首选 fallback
```

> **腾讯云常见坑 4：内网 cargo install 慢** —— 推荐配置腾讯云 crates 镜像（`~/.cargo/config.toml`）：
> ```toml
> [source.crates-io]
> replace-with = "tencent"
> [source.tencent]
> registry = "https://mirrors.tencent.com/crates.io-index"
> ```

**A.3 macOS 环境**（无 perf，用 `samply` 或 `dtrace`）：

```bash
brew install samply
# 或使用 cargo-instruments（需要 Xcode）
cargo install cargo-instruments
```

#### B. 采集火焰图（按场景给命令）

**场景 B.1：bench 顺序读 SC 路径（最常用）**

```bash
# 直接对 criterion bench 二进制做采样
cargo flamegraph --bench sc_seq -o flamegraph_sc_seq.svg -- --bench

# 或精细控制 perf 参数（采样频率 999Hz，避免与定时器谐振）
RUSTFLAGS="-C debuginfo=1" cargo bench --bench sc_seq --no-run
BENCH_BIN=$(ls -t target/release/deps/sc_seq-* | grep -v '\.d$' | head -n1)

sudo perf record -F 999 -g --call-graph dwarf -o perf_sc_seq.data \
    -- "$BENCH_BIN" --bench

sudo perf script -i perf_sc_seq.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl --title "Rust SC seq read" \
    > flamegraph_sc_seq.svg
```

> 注：`--call-graph dwarf` 适合 Rust（基于调试信息回溯栈）；若内核太老或 dwarf 太慢，可换 `--call-graph fp`，但需要 `RUSTFLAGS="-C force-frame-pointers=yes"`。

**场景 B.2：bench 高并发 PR（256 线程，重点抓 `Arc` / 锁竞争）**

```bash
cargo flamegraph --bench sc_pr -o flamegraph_sc_pr_256t.svg \
    -- --bench --measurement-time 30 high_concurrency_256

# 抓 off-CPU（看是否被 madvise / mmap_sem 阻塞）
sudo perf record -F 999 -e sched:sched_switch -e sched:sched_stat_sleep \
    --call-graph dwarf -o perf_offcpu.data \
    -- "$BENCH_BIN" --bench
sudo perf inject -s -i perf_offcpu.data -o perf_offcpu.inject.data
sudo perf script -i perf_offcpu.inject.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl --color=io --title "Rust SC off-CPU" \
    > flamegraph_offcpu.svg
```

**场景 B.3：仅抓 page-fault（验证\"是否被 page-fault 主导\"）**

```bash
# 只采 page-fault 事件，过滤其他噪音
sudo perf record -e page-faults -c 1 --call-graph dwarf -o perf_pf.data \
    -- "$BENCH_BIN" --bench positioned_read_cold

sudo perf script -i perf_pf.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl --color=mem --title "Page-fault flamegraph" \
    > flamegraph_pagefault.svg

# 数值统计：缺页次数 / minor / major
sudo perf stat -e page-faults,minor-faults,major-faults \
    -- "$BENCH_BIN" --bench positioned_read_cold
```

**场景 B.4：抓系统调用（验证\"mmap 次数是否真的是 1\"）**

```bash
# 只统计 mmap / munmap / madvise 的调用次数与耗时
sudo perf trace -e 'syscalls:sys_enter_mmap,syscalls:sys_enter_munmap,syscalls:sys_enter_madvise' \
    -- "$BENCH_BIN" --bench seq_read_1gb 2>perf_trace.log

grep -c sys_enter_mmap perf_trace.log    # 期望：1（per reader）
grep -c sys_enter_madvise perf_trace.log # 期望：1 + N（N = prefetch 调用次数）
```

或更轻量地用 `strace -c`（仅在小规模 bench 用，会显著拖慢）：

```bash
strace -c -e trace=mmap,munmap,madvise,pread64 "$BENCH_BIN" --bench seq_read_1gb
```

**场景 B.5：macOS（无 perf，用 samply）**

```bash
cargo bench --bench sc_seq --no-run
BENCH_BIN=$(ls -t target/release/deps/sc_seq-* | grep -v '\.d$' | head -n1)

samply record -- "$BENCH_BIN" --bench
# 命令结束后 samply 自动打开浏览器，含火焰图 + Sandwich 视图 + 时间轴
```

#### C. 查看火焰图

**C.1 浏览器打开 SVG**：火焰图本身就是 SVG，任意浏览器直接打开。Linux 远程服务器场景：

```bash
# 把 SVG 拖回本地
scp user@server:/path/to/flamegraph_sc_seq.svg ./
open flamegraph_sc_seq.svg     # macOS
xdg-open flamegraph_sc_seq.svg # Linux 桌面

# 或在远程起 HTTP 服务（端口转发后本地访问）
python3 -m http.server 8000 -d /path/to/svg/dir
# 本地：ssh -L 8000:localhost:8000 user@server
# 然后浏览器开 http://localhost:8000/flamegraph_sc_seq.svg
```

**C.2 阅读要点**：

- **横轴是采样数量**（不是时间序列），宽 = 该函数在 CPU 上的相对耗时占比。
- **纵轴是调用栈**，下方是被调用者（最底是当前 CPU 上正在执行的函数）。
- **颜色无含义**（默认是暖色随机），仅为视觉区分；用 `--color=mem`（绿）/ `--color=io`（蓝）可表达分类。
- **点击任意函数** = 放大该子树（"zoom in"），右键复位。
- **顶部搜索框**支持正则（如 `mmap|munmap|madvise|pthread_mutex|alloc::sync`）。

#### D. 关键热点判读清单（与本设计直接相关）

按"该热点在火焰图里的可见特征 → 应当采取的设计动作"列出：

| 观察到的栈帧 | 含义 | 期望占比 | 超阈值动作 |
|---|---|---|---|
| `sys_mmap` / `sys_munmap` 顶端宽条 | 仍在频繁 mmap | < 0.5%（应仅 open/close 时各 1 次） | 检查是否退化成 per-chunk mmap，违反 §3.2 |
| `do_page_fault` / `handle_mm_fault` | 缺页处理（首次触达页面） | 冷数据合理；热数据 < 5% | 热数据高占比说明 LRU/驻留不足，调大 cache 或加 prefetch |
| `__memmove_avx_unaligned` / `memcpy` | 用户态拷贝 | `read_to_slice` 路径合理；`read_bytes` 路径应几乎为零 | `read_bytes` 路径出现 memcpy = 零拷贝失效，回查 `Bytes::from_owner` 实现 |
| `alloc::sync::Arc::clone` / `__atomic_fetch_add` | Arc 引用计数原子操作 | < 2% | > 5% 说明 hot path 上有不必要的 `Arc::clone`，改成借用或 `&Arc` |
| `parking_lot::Mutex::lock` / `futex_wait` | 锁竞争 | 几乎为零（设计上无共享可变状态） | 出现即 bug，定位是哪一把锁（多半是 LRU 或 neg cache） |
| `__madvise` 高频 | prefetch_many 没合并好 | < 0.1% | 检查 §3.2.1 的 coalesce 逻辑，看 `prefetch.coalesce.gap` 是否生效 |
| `tonic` / `h2` / `prost` | gRPC 路径 | 仅 open / close 阶段可见 | 数据 read 路径出现 = SC 退化到 gRPC，查 fallback 原因 |
| `tokio::runtime::*` / `park` | 异步调度 | 控制面合理，数据面应不出现 | 数据面出现 = 误用 async / `spawn_blocking`，违反 §3.4 |

#### E. 差分火焰图（Java vs Rust SC，或优化前后）

差分火焰图直观展示"哪些栈变快了、哪些变慢了"——红色为变慢，蓝色为变快。

```bash
# 1) 采集 baseline（Rust SC 优化前 / Java SC 等价路径）
sudo perf record -F 999 -g -o perf_before.data -- "$BENCH_BEFORE" --bench
sudo perf script -i perf_before.data | ~/FlameGraph/stackcollapse-perf.pl > before.folded

# 2) 采集 after
sudo perf record -F 999 -g -o perf_after.data -- "$BENCH_AFTER" --bench
sudo perf script -i perf_after.data | ~/FlameGraph/stackcollapse-perf.pl > after.folded

# 3) 生成差分图
~/FlameGraph/difffolded.pl before.folded after.folded \
    | ~/FlameGraph/flamegraph.pl --title "before -> after diff" \
    > flamegraph_diff.svg
```

#### F. 归档约定

每次 P5 阶段 bench 必须随 PR 提交以下文件，路径约定 `docs/perf/<date>-<scenario>/`：

```
docs/perf/2026-06-24-sc-pr-256t/
├── env.txt                        # uname -a / lscpu / free -h / kernel cmdline
├── cargo_bench.log                # criterion 完整输出
├── perf_stat.txt                  # perf stat -e cycles,instructions,page-faults,...
├── flamegraph_oncpu.svg
├── flamegraph_offcpu.svg
├── flamegraph_pagefault.svg
└── README.md                      # 一段话总结：是否符合 §5.2 预期，热点偏离点
```

> **强制要求**：火焰图与对应的 `perf stat` 数值必须互相印证；只有 SVG 不附数值的报告视为不合格，PR 评审驳回。

### 5.3 验收基准

文档落地时，下列基准与测试必须运行并归档结果：

1. `cargo bench --bench sc_seq` — 顺序读吞吐对比 gRPC 路径
2. `cargo bench --bench sc_pr` — PR 模式 1/8/64/256 线程 × {4KB, 64KB, 1MB} buf
3. `cargo bench --bench sc_lat` — p50/p99/p999 延迟分布
4. `cargo bench --bench sc_prefetch` — 冷数据 PR 在 {无 prefetch, prefetch, prefetch_many} 三种模式下的吞吐与 p99，必须证明 prefetch 路径相对无 prefetch 至少 10× p99 改善
5. `cargo test --test sc_consistency` — **门禁级一致性回归测试**（与 bench 不同，必须 100% 通过；任何 PR 触发，失败直接阻塞合入）。覆盖 §1.3 的全部不变式：
   - **INV-D2**：同一 block 用 SC 路径与 gRPC 路径双读，逐字节 diff（覆盖顺序读、PR、跨 chunk、跨 page 边界）
   - **INV-D3**：`read_bytes` 返回的 `Bytes` 在 reader Drop 之后仍能被安全访问，内容不变（验证 owner 生命周期）
   - **INV-D4**：`prefetch` / `prefetch_many` 调用前后同范围 `read` 字节内容完全一致
   - **INV-S1**：故障注入（强制 OpenLocalBlock RPC 失败 / `File::open` EACCES / `Mmap::map` 失败）后，`BlockInStream::read` 返回的字节序列与全程 gRPC 完全相同
   - **INV-S2**：构造异常路径（read 中 panic / `?` 提前返回），验证 reader Drop 后 Worker 侧锁释放
   - **INV-S3**：capability 启用 / 关闭两组场景下错误分类与 gRPC 一致
   - **INV-S4**：`OutOfRange` 不被 fallback 吞掉，调用方收到与 gRPC 相同的错误类型
   - **INV-S5**：`read` / `read_bytes` / `read_to_slice` 三 API 在相同输入下结果一致

---

## 6. 配置项

| 配置 | 默认 | 说明 |
|---|---|---|
| goosefs.user.short.circuit.enabled | true | 总开关 |
| goosefs.user.short.circuit.preferred | true | 与 Java 同名；Rust 因无 DS，恒视为 true |
| goosefs.client.short.circuit.cache.capacity | 64 | per-task LRU 大小 |
| goosefs.client.short.circuit.cache.ttl | 30s | reader 空闲过期 |
| goosefs.client.short.circuit.neg.cache.ttl | 5s | 失败缓存 |
| goosefs.client.short.circuit.advise | random | L1 内核预读：sequential / random / normal / none |
| goosefs.client.short.circuit.prefetch.enabled | true | L2 应用层预读总开关（关闭后 prefetch/prefetch_many 退化为 no-op） |
| goosefs.client.short.circuit.prefetch.coalesce.gap | 64KB | prefetch_many 中相邻 range 合并的最大 gap |
| goosefs.client.short.circuit.prefetch.max.batch | 1024 | 单次 prefetch_many 最多 madvise 调用数（防 syscall 风暴） |
| goosefs.client.short.circuit.open.blocking | false | 是否 spawn_blocking 包 open（NFS 场景设 true） |
| goosefs.client.short.circuit.thp | false | THP via `madvise(MADV_HUGEPAGE)`（**非** MAP_HUGETLB；file-backed THP 视内核/FS 而定，实验项） |
| goosefs.client.short.circuit.min.block.size | 0 | 小于此值不走 SC |
| goosefs.client.short.circuit.sigbus.recover | true | 注册 SIGBUS handler |

---

## 7. 错误处理与可观测性

### 7.1 错误分类

```rust
pub enum ShortCircuitError {
    NotLocal,
    OpenLocalBlock(tonic::Status),
    FileOpen(std::io::Error),
    Mmap(std::io::Error),
    Madvise(std::io::Error),
    OutOfRange { off: usize, len: usize, file_size: usize },
    SigBus,
}
```

所有 ShortCircuitError 都可被 BlockInStream::create 透明转 fallback；只有 OutOfRange 直接上抛（语义错误）。

### 7.2 Tracing 字段

每个 LocalBlockReader::open span 包含：

- block_id, block_size, capability_present
- path, file_size, mmap_addr (debug 级)
- open_duration_us, mmap_duration_us
- cache_hit（factory 层）

### 7.3 Prometheus metrics

| metric | 类型 | 说明 |
|---|---|---|
| goosefs_sc_open_total{result} | counter | open 次数，result=success/openlocal_fail/file_open_fail/mmap_fail |
| goosefs_sc_read_bytes_total | counter | SC 路径读字节 |
| goosefs_sc_read_calls_total | counter | SC 路径 read() 调用次数 |
| goosefs_sc_cache_hits_total | counter | LRU 命中 |
| goosefs_sc_cache_evictions_total | counter | LRU 淘汰 |
| goosefs_sc_neg_cache_hits_total | counter | 负缓存命中（避免重试） |
| goosefs_sc_active_readers | gauge | 当前活跃 reader |
| goosefs_sc_mmap_bytes | gauge | 累计 mmap 字节数（虚拟内存） |
| goosefs_sc_open_duration_seconds | histogram | open 延迟 |
| goosefs_sc_prefetch_calls_total | counter | prefetch / prefetch_many 调用次数 |
| goosefs_sc_prefetch_bytes_total | counter | 累计请求预读字节 |
| goosefs_sc_prefetch_madvise_total | counter | 实际 madvise(WILLNEED) syscall 次数（合并后） |

---

## 8. 安全性论证

### 8.1 unsafe 清单

仅 1 处 unsafe：

1. **Mmap::map(&file)**（memmap2 crate 内部）
   - SAFETY 前提：在 mmap 生命周期内，文件内容不被截断/替换（INV-D1）。
   - 缓解：Worker 持锁不 truncate（协议根基）；SIGBUS handler 仅做诊断后 `abort`（见 §3.2）；需要鲁棒性的部署改用 `io.mode=pread` 数据面而非 mmap。

> 零拷贝 `read_bytes` 路径**不引入 unsafe**：改用 `Bytes::from_owner`（§3.3），由 bytes crate 在安全代码内维持 owner 生命周期。早期原型的 `Bytes::from_raw_parts` 是不存在的 API，已废弃。

### 8.2 Drop 顺序

```rust
struct LocalBlockReader {
    block_id: i64,
    file_size: usize,
    mmap: Arc<Mmap>,         // dropped before guard
    _guard: OpenLocalBlockGuard,
}
```

Rust struct field drop 顺序为声明顺序自上而下。mmap 先 Drop（munmap → VMA 释放），再 _guard Drop（关 bidi → Worker unlock）。即使顺序反过来也安全（munmap 不依赖 lock），但当前顺序最贴合"先释放资源再释放许可"的语义。

#### 8.2.1 异步 unlock 在同步 Drop 中的语义（与 Java 的差异）

Java 的 `LocalFileDataReader.close()` 是 `mStream.close() + waitForComplete()`，**同步等待** Worker 确认 unlock 完成。Rust 的 `Drop` 是同步的、**不能 `await`**，所以 `OpenLocalBlockGuard::drop` 只能"关闭 bidi 发送端"，真正的 `onCompleted` → Worker unlock 发生在 runtime 后台，是**最终释放（eventually released）**而非同步释放。须明确处理以下风险：

- **释放滞后**：`Drop` 返回后到 Worker 真正解锁之间存在窗口；正常负载可忽略，但需在 INV-S2 的语义里写清"reader Drop 之前锁必持有；Drop 之后锁在有限时间内异步释放"。
- **runtime shutdown / task 不再被 poll**：若发送端所在 task 永不再被调度（如 runtime 正在销毁），unlock RPC 可能**永不 flush → 锁泄漏**。
- **应对策略**：guard 不直接依赖"被 Drop 的 task 恰好还会被 poll"，而是把 close 信号 `try_send` 到一个**进程级 reaper task**（独立常驻 task），由它统一 `await` 各 bidi 的完成并做超时兜底；reaper 在 runtime shutdown 前做 best-effort flush。Worker 侧本身也应有 OpenLocalBlock 会话的空闲超时作为最终兜底（与 Java 一致）。
- INV-S2 据此细化为：**"reader 存活期间锁必持有；reader Drop 触发异步解锁，最坏情况由 reaper 超时或 Worker 会话超时回收，不会无限泄漏。"**

### 8.3 panic-safety

- LocalBlockReader::open 中任意 ? 提前返回时，已构造出的 guard 会被 Drop，自动解锁。
- 缓存 LRU 在 Drop 时遍历释放，不依赖外部 close。

### 8.4 一致性论证（对应 §1.1 目标 0a / 0b 与 §1.3 不变式）

本节给出每条不变式的论证链路，回答"为什么 SC 路径在所有实现优化下仍与 gRPC 路径数据/语义等价"。

**数据一致性（INV-D1 ~ INV-D4）**：

- INV-D1（block 文件不可变）：协议约束——Worker 在 OpenLocalBlock 锁持有期间不会 truncate / 改写已落盘 block；commit 后的 block 全局只读。这是 SC 路径所有数据一致性论证的根基。
- INV-D2（mmap 切片 ≡ pread 字节）：mmap 是文件的内存映射，Linux 页缓存与 pread 共享同一组 page frame，因此 `&mmap[off..off+len]` 与 Worker 端 `pread(fd, off, len)` 返回相同字节。当 INV-D1 成立时，无论何时读都得到一致内容。**前提**：映射长度与 bounds_check 必须以 OpenLocalBlock 响应里的**逻辑块大小**为准（见 §4.1 `file_size` 注释），否则物理文件被预分配/稀疏到更大时，按物理长度映射会把尾部暴露成 0，破坏该不变式。
- INV-D3（`Bytes` owner 生命周期）：`read_bytes` 通过把 `Arc<Mmap>` 装入 `Bytes` 的 owner，使 mapping 的引用计数在最后一个 `Bytes`（含跨 await / 跨 task clone）Drop 之后才归零；只有归零后才会真正 munmap。`LocalBlockReader` 自身的 Drop 不会立即释放底层 mapping，避免了"reader Drop 后老 Bytes 读到悬挂指针"。
- INV-D4（prefetch 不改字节）：`madvise(MADV_WILLNEED)` 是内核 readahead hint，文档明确不修改 page 内容；在 tmpfs / NFS 等场景退化为 no-op，对字节内容同样无影响。

**语义一致性（INV-S1 ~ INV-S5）**：

- INV-S1（fallback 透明）：§3.6 决策矩阵把所有可恢复错误统一转 gRPC，且 fallback 切换发生在"未向调用方返回任何字节"之前，因此调用方观察到的最终字节序列与"始终 gRPC"等价。
- INV-S2（锁生命周期）：`LocalBlockReader` 字段声明顺序确定 Drop 顺序：`mmap: Arc<Mmap>` 在前、`_guard: OpenLocalBlockGuard` 在后；guard Drop 时关闭 bidi stream → Worker `onCompleted` → 解锁。任意 panic / `?` 早退路径都依赖 Rust 自动 Drop，guard 永远在最后释放。注意 `Drop` 是同步的、不能 `await`，因此解锁是**异步最终释放**（§8.2.1）：reader 存活期间锁必持有，Drop 后由后台 reaper `await` 完成、并以 reaper 超时 + Worker 会话空闲超时双重兜底，保证不会无限泄漏。
- INV-S3（capability 等价）：capability 字段在 OpenLocalBlock 请求里被显式注入；集群拒绝时与 gRPC 路径返回相同的 `tonic::Status`，错误分类不丢失。
- INV-S4（错误分类稳定）：§3.6 决策矩阵把错误显式分组——可恢复错误 → fallback；语义错误（`OutOfRange`、bounds_check 失败）→ 直接上抛；fallback 不会吞掉语义错误。
- INV-S5（三 API 等价）：`read` 返回 `&mmap[off..off+len]`；`read_bytes` 返回包装同一切片的 `Bytes`；`read_to_slice` 内部 `copy_from_slice` 同一切片。三者源都是同一段 mapping，字节内容必然一致。

**撕裂读防护**：单次 `read(off, len)` 在 Rust 层是一次切片创建 + 调用方 memcpy。Worker 协议禁止在锁持有期间改写 block（INV-D1），因此在 mmap 持有期内底层字节不变，不会出现"读一半被改"。撕裂/过期读的防护**不依赖运行期捕获 SIGBUS**，而是依赖 INV-D1 这个协议根基；万一协议被破坏触发 SIGBUS，handler 选择 `abort`（§3.2）而非返回半截字节，从而不破坏数据一致性。需要在不可信 FS 上避免该风险的部署应改用 `io.mode=pread`。

---

## 9. 与 Java SC 的迁移兼容矩阵

| 客户端 | Worker | 行为 |
|---|---|---|
| Java SC | Java Worker | 维持现状 |
| Rust SC | Java Worker | 完全兼容（协议无改动） |
| Rust SC + capability | Java Worker（capability on） | 兼容 |
| Rust SC（DS preferred） | Java Worker（DS on） | DS 不走 SC，自动回退 gRPC（与 Java 一致） |
| Java SC | 未来 Rust Worker | 兼容（需 Worker 实现 OpenLocalBlock RPC） |

---

## 10. 实施路线图

| 阶段 | 内容 | 依赖 | 状态 |
|---|---|---|---|
| P0 | 文档评审通过 | 本文档 | ✅ 完成 |
| P1 | 重构 LocalBlockReader：去 _file，去 spawn_blocking，加 MADV_RANDOM | crate memmap2 ≥ 0.9 | ✅ 完成（`src/block/short_circuit/reader.rs`，零拷贝 read/read_bytes/read_to_slice + L2 prefetch） |
| P2 | 实现 ShortCircuitFactory（LRU + neg cache） | lru crate | ✅ 完成（`factory.rs`，有界 LRU + 有界负缓存 + 决策矩阵 + EACCES 粘性禁用） |
| P3 | 接入 BlockInStream::create，capability 注入 | worker.rs 改造 | ✅ 完成：随机/定位读路径（`read_external_range`）与顺序 `read()` 路径均接入 SC，透明回退 gRPC；`WorkerClient::open_local_block` + RAII guard 实现。capability **插桩已完成**——`CapabilityProvider` trait + 工厂 `with_capability_provider`，按 block 注入；默认无 provider 发 `None`（NOSASL/禁用集群正常，开启集群自动回退）。仅「真实凭据来源」属外部待补（dev 读路径尚无 `capability_fetcher`） |
| P4 | metrics + tracing 全量打通 | metrics/tracing | ✅ 完成（`Client.ShortCircuit*` 13 项计数/Gauge + tracing span） |
| — | **端到端验证**（本地 NOSASL 集群） | 运行中的 Worker | ✅ 完成：`examples/short_circuit_demo.rs` + `tests/short_circuit_e2e.rs`（4 用例：SC 命中、SC vs gRPC 字节一致 INV-S1、顺序 read_all、reader 复用）。附带修复：`WorkerRouter` 改用「绑定本机接口地址」判定本地 worker |
| P5 | bench：sc_seq / sc_pr / sc_lat 三套 | criterion | ✅ 完成（以可运行 A/B 形式）：`benchmarks/sc_pr_ab.rs`（SC vs gRPC 随机读吞吐+p50/p99/p999）。实测见 `docs/perf/2026-06-24-sc-pr-ab/`：热 cache 下吞吐 ×307、p99 ×261 |
| P6 | SIGBUS handler + safe_read 兜底 | signal-hook | ✅ 完成（`sigbus.rs`，SA_SIGINFO 异步信号安全诊断 + abort，unix；用 libc 非 signal-hook） |
| P7 | 大页（THP via MADV_HUGEPAGE）opt-in + 实测 | 内核 THP 支持 | ◑ opt-in 已实现（`short.circuit.thp`，Linux `MADV_HUGEPAGE`，默认关）；实测留待 Linux 节点 |
| P8 | 跨任务共享 pool（可选） | 评估后决定 | ✅ 完成：`ShortCircuitFactory` 上提到 `FileSystemContext`（`acquire_short_circuit`），同一 context 的所有流共享一份热块 reader LRU + 负缓存——热块仅 `OpenLocalBlock`+mmap 一次，跨流/跨任务复用。前置：将 guard 的 tonic `Streaming` 包入 `Mutex` 使 `LocalBlockReader: Send+Sync`（编译期断言 + E2E `short_circuit_reader_shared_across_streams` 验证） |

每阶段需附 PR + bench 报告 + 火焰图。

> **实现说明（截至本次提交）**：P1–P6、P8 完成；P3 含 capability 插桩（`CapabilityProvider`），随机+顺序读路径均落地并经真实集群验证字节一致 + 性能基准 + 跨流共享。剩余外部依赖：P3 真实 capability 凭据来源、P7 Linux THP 实测。代码位于 `src/block/short_circuit/`（`reader.rs`/`factory.rs`/`sigbus.rs`）、`src/client/worker.rs`、`src/block/router.rs`、`src/io/file_in_stream.rs`、`src/context.rs`（共享工厂）。基准 `benchmarks/sc_pr_ab.rs`，E2E `tests/short_circuit_e2e.rs`（5 用例），示例 `examples/short_circuit_demo.rs`。

---

## 11. 已知 Trade-off 与开放问题

1. **大页（THP via `MADV_HUGEPAGE`）**：理论上可减 TLB miss，但**不能用 `MAP_HUGETLB`**——后者仅支持匿名映射或 hugetlbfs 后端，对 ext4/xfs 上的常规 block 文件 `mmap` 会 `EINVAL`。文件页只能走透明大页（THP），通过 `madvise(MADV_HUGEPAGE)` 申请；而 **file-backed/page-cache 的 THP 支持高度依赖内核版本与 FS**（多数老内核仅对匿名页生效），收益不稳定。默认关，仅作 opt-in 实验项，须以 §5.2.1 火焰图（`do_page_fault`/TLB 计数）实测验证收益后再决定保留。
2. **跨任务共享 pool**：跨 task 复用可降 open RTT，但引入 Arc 原子开销和锁竞争；待 P5 实测后决策。
3. **SIGBUS handler 全局性**：进程级唯一，需协调宿主程序（如 Python 嵌入场景）。
4. **mmap vs pread**：在小块（< 4KB）随机读极端场景，pread 可能更快（无缺页 + 内核 page cache 直接 copy）。考虑提供 `goosefs.client.short.circuit.io.mode = mmap | pread` 切换。
5. **`MADV_WILLNEED` 在不同文件系统上的行为不一致**：L2 应用层预读依赖内核异步 readahead，但实际效果与 block 文件所在的 FS 强相关，须分类对待。

   | 文件系统 / 介质 | `MADV_WILLNEED` 行为 | Rust SC 应对 |
   |---|---|---|
   | ext4 / xfs（GooseFS Worker 默认） | 标准异步 readahead，效果最佳 | 默认启用，符合预期 |
   | tmpfs / ramfs | **直接 no-op**（数据已在内存，无 readahead 概念） | 自动跳过即可，零开销但也零收益 |
   | NFS | 行为依 mount 选项与服务端实现而异，部分仅触发预取，部分无效 | 通过 `prefetch.enabled=false` 关闭，避免误判收益 |
   | FUSE / 用户态 FS | 取决于具体 FS 实现，多数无效或退化为同步 IO | 默认按"无效"处理，不依赖其加速 |
   | 直接挂载的块设备（无 FS） | 不适用 | SC 路径不会触达 |
   | 内核版本 < 3.x | 历史上 `MADV_WILLNEED` 曾**同步**触发 readahead，可能阻塞 | 文档声明最低支持内核 ≥ 4.x；老内核走 fallback |

   **运行时探测策略**（P5 阶段实现）：

   - 启动期对 Worker 数据目录做一次 `statfs`，识别 FS 类型；tmpfs / 未知 FS 自动降级 `prefetch.enabled=false`。
   - Bench `sc_prefetch` 必须在 ext4 与 tmpfs 上分别跑，验证"ext4 上 ≥10× p99 改善 / tmpfs 上无回归"两条结论。
   - 若用户显式配置 `prefetch.enabled=true` 强制开启，记录一条 warn 日志说明"当前 FS 上 MADV_WILLNEED 可能无效"。

---

## 12. 参考

- [Java] core/common/.../LocalFileBlockReader.java
- [Java] core/client/fs/.../LocalFileDataReader.java
- [Java] core/client/fs/.../BlockInStream.java
- [Rust SC 原型] goosefs-lance-tests/short_circuit.rs
- [Rust，待新增] `goosefs-client-rust/src/client/worker.rs` 的 OpenLocalBlock 封装（目前 `open_local_block` 仅作为生成的 gRPC stub 存在于 `src/generated/com.qcloud.cos.goosefs.grpc.block.rs`，客户端侧封装尚未实现）
- [Rust，现状] `goosefs-client-rust/src/block/router.rs`：本地 Worker 判定现以 `detect_local_worker` / `local_worker_id`（hostname 匹配）实现；§2/§3.7 文中的 `is_local_worker(id)` 为对其语义的抽象命名，落地时对齐到该实现
- [对比文档] goosefs-lance-tests/docs/stress-testing/Java_vs_Rust_ShortCircuit_PositionedRead对比.md
- Linux mmap(2), madvise(2), pread(2) man pages
- "What every programmer should know about memory" — Drepper, 2007
- Linux Kernel mm/mmap.c: VMA red-black tree & mmap_sem contention notes

