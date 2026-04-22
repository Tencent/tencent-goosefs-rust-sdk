# CACHE_THROUGH 写入 UFS 数据被覆盖的根因与修复方案

**文档版本**: v1.0
**撰写日期**: 2026-04-22
**影响版本**: goosefs-client-rust 当前 feature 分支
**严重级别**: Critical（UFS 上数据最终只剩最后一个 block，用户数据丢失）
**方向**: 方向 A —— 客户端侧修复

---

## 1. 问题现象

在 3-node GooseFS 集群上运行 Lance / VectorDBBench 基准测试时发现：

1. **UFS 侧数据严重缺失**：一个通过 Rust 客户端以 `CACHE_THROUGH` 写入的 Lance 数据文件，在 UFS（LocalUnderFileSystem / COSUnderFileSystem）上落盘后大小为 **53,839,600 字节**，远小于客户端实际写入的 **~485MB** 总字节数。
2. **数值上吻合"只剩最后一个 block"**：文件逻辑大小 ≈ 7 × 64MB + 53MB，UFS 上仅剩约 53MB，正好等于**最后一个 block 的大小**。
3. **Cache 侧数据完整**：短时间内 read 能正常返回全部数据；但一旦 worker 触发缓存淘汰，回源到 UFS 时就开始报 `skipping N bytes`、数据错位、文件校验失败。

现象**完全符合"每写一个 block 就把 UFS 文件从头覆盖写一遍"**的假设。

---

## 2. 排查过程

### 2.1 Rust 客户端侧代码审阅

关键文件：

- `src/io/file_writer.rs` —— `GooseFsFileWriter` 主入口
- `src/client/worker.rs` —— `GrpcBlockWriter` 与 Worker 的 gRPC 交互
- `proto/grpc/block_worker.proto` —— `WriteRequestCommand` 协议定义
- `src/generated/*` —— protobuf 生成的 `RequestType` 枚举

在 `file_writer.rs::resolve_write_strategy` 中观察到如下错误映射：

```rust
// 当前错误实现（节选）
Some(3) | Some(4) => WriteStrategy {
    request_type: RequestType::UfsFile,   // ← CACHE_THROUGH 也用了 UFS_FILE
    create_ufs_file_options: Some(ufs_opts),
    need_async_persist: false,
},
```

配套注释甚至写明了**错误的理解**：

> "This is why CACHE_THROUGH uses `UfsFile` mode — the same as THROUGH — rather than
>  `GoosefsBlock`. Without this, data reaches the cache but never gets persisted to UFS"

在 `worker.rs` 的 `GrpcBlockWriter::open` 中，每个 block 初始 command 都是：

```rust
let initial_command = WriteRequest {
    value: Some(write_request::Value::Command(WriteRequestCommand {
        r#type: Some(options.request_type as i32),      // UFS_FILE
        id: Some(block_id),                              // 每个 block 一个新 id
        offset: Some(0),                                 // ← 恒为 0
        create_ufs_file_options: options.create_ufs_file_options.clone(),
        ...
    })),
    ...
};
```

也就是说，Rust 客户端**按 block 切分写入**时，每切一个新 block 就对 Worker 新起一次
`WriteBlock(UFS_FILE, block_id=new, offset=0, create_ufs_file_options=same_path)` 的 gRPC 流。

### 2.2 Worker 端行为验证（Java 实现）

关键文件：

- `core/server/worker/.../worker/grpc/UfsFileWriteHandler.java`

该 handler 的 `createUfsFile` 路径会执行：

```java
// 简化
context.setOutputStream(ufs.createNonexistingFile(request.getUfsPath(), createOptions));
```

即：**每次收到 `RequestType.UFS_FILE` 的 WriteBlock 初始 command，就会对同一个 UFS path
调 `createNonexistingFile`**。对绝大多数 UFS 实现（Local / COS / S3 等）这等价于
`O_CREAT | O_TRUNC | O_WRONLY`，**覆盖写**。

于是客户端每切一个 block，Worker 就把 UFS 文件**截断并重写**：

```
block 0 (64MB) → open(path) → truncate → write 64MB → close
block 1 (64MB) → open(path) → truncate → write 64MB → close   ← block 0 丢失
...
block 7 (53MB) → open(path) → truncate → write 53MB → close   ← 前 7 个 block 全丢
最终 UFS = 53MB = 最后一个 block 大小 ✅ 与现象吻合
```

### 2.3 对照 Java 客户端的正确做法

关键文件：

- `core/client/fs/.../client/block/stream/UnderFileSystemFileOutStream.java`
- `core/client/fs/.../client/file/GooseFSFileOutStream.java`

核心结论：**Java 客户端对 CACHE_THROUGH 维护两条独立流**。

#### 2.3.1 UFS 流：整个文件**一条**连续 gRPC 流

`UnderFileSystemFileOutStream` 构造时只开**一个** `GrpcDataWriter`：

```java
BlockOutStream blockOutStream = new BlockOutStream(
    GrpcDataWriter.create(context, address,
        ID_UNUSED,          // block_id = -1
        Long.MAX_VALUE,     // length = Long.MAX_VALUE（整个文件当作一个"无限 block"）
        RequestType.UFS_FILE,
        options),
    Long.MAX_VALUE, address);
```

即：
- `block_id = -1`（`ID_UNUSED`）
- `length = Long.MAX_VALUE`
- **整个文件作为一条连续流**从头 append 到尾，Worker 端 `UfsFileWriteHandler.writeBuf` 复用
  同一个 `OutputStream`，仅在首次进入时调用 `createNonexistingFile`（`if (context.getOutputStream() == null)`）。

#### 2.3.2 Cache 流：按 block 切分

同时 `GooseFSFileOutStream.mCurrentBlockOutStream` 独立按 block 切分，`RequestType.GOOSEFS_BLOCK`，
每个 block 可能调度到不同 Worker。

#### 2.3.3 写入时**双流并行**

`GooseFSFileOutStream.writeInternal`：

```java
// 伪代码
if (mShouldCacheCurrentBlock) {
    mCurrentBlockOutStream.write(...);   // cache 流，按 block 切
}
if (mUnderStorageType.isSyncPersist()) {
    mUnderStorageOutputStream.write(...); // ufs 流，连续 append
}
```

关闭顺序：**先关 UFS 流，再关最后一个 cache block，最后 `completeFile(ufsLength)`**。

### 2.4 各 write_type 的正确对照表

| write_type | cache 流 | ufs 流 | complete_file 动作 |
|---|---|---|---|
| MUST_CACHE (1) | ✅ `GOOSEFS_BLOCK`，按 block 切 | ❌ | `completeFile()` |
| TRY_CACHE (2) | ✅ `GOOSEFS_BLOCK`，按 block 切 | ❌ | `completeFile()` |
| **CACHE_THROUGH (3)** | ✅ `GOOSEFS_BLOCK`，按 block 切 | ✅ **`UFS_FILE`, `block_id=-1`, `length=i64::MAX`，整文件一条流** | `completeFile(ufsLength)` |
| THROUGH (4) | ❌ | ✅ `UFS_FILE`, `block_id=-1`, `length=i64::MAX` | `completeFile(ufsLength)` |
| ASYNC_THROUGH (5) | ✅ `GOOSEFS_BLOCK`，按 block 切 | ❌ | `completeFile()` + `scheduleAsyncPersistence` |

---

## 3. 根因（Root Cause）

**Rust 客户端错误地把 `CACHE_THROUGH` 简化为"对每个 block 用 `RequestType::UFS_FILE`"**，导致：

1. 每写一个新 block，都会对 Worker 发起一次独立的 `WriteBlock(UFS_FILE)` RPC；
2. Worker 端 `UfsFileWriteHandler` 在每次新 RPC 初始化时都会调用 `ufs.createNonexistingFile(path)`；
3. 该调用在 Local / COS / S3 等 UFS 实现上等价于**覆盖写（O_TRUNC）**；
4. 结果：前面所有 block 的 UFS 数据都被后一个 block 覆盖，UFS 上最终只保留**最后一个 block**。

同时，Cache 层确实写进去了（因为 Worker 在 `UFS_FILE` 模式下会额外缓存 block 数据），所以在
缓存未淘汰前读取正常；一旦淘汰回源到 UFS，就会读到**错位/截断**的数据，出现 `skipping N bytes`。

---

## 4. 修复方案

参照 Java `GooseFSFileOutStream` 的"双流并行"架构，在 Rust 客户端实现同等语义。

### 4.1 `WriteStrategy` 重新定义

替换 `src/io/file_writer.rs` 里原有的 `WriteStrategy`：

```rust
#[derive(Clone, Debug)]
struct WriteStrategy {
    /// 是否开 cache 流（按 block 切，RequestType = GOOSEFS_BLOCK）
    cache_stream: bool,
    /// 是否开 ufs 流（block_id=-1, length=i64::MAX, RequestType = UFS_FILE）
    ufs_stream: bool,
    /// ufs 流所需的 CreateUfsFileOptions（仅 ufs_stream=true 时有效）
    create_ufs_file_options: Option<CreateUfsFileOptions>,
    /// 关闭时是否需要调度异步持久化
    need_async_persist: bool,
}

fn resolve_write_strategy(write_type: Option<i32>, file_info: &FileInfo) -> WriteStrategy {
    let ufs_opts = || CreateUfsFileOptions {
        ufs_path: file_info.ufs_path.clone(),
        owner: file_info.owner.clone(),
        group: file_info.group.clone(),
        mode: file_info.mode,
        mount_id: file_info.mount_id,
        acl: None,
    };
    match write_type {
        Some(3) => WriteStrategy {           // CACHE_THROUGH: 双流
            cache_stream: true,
            ufs_stream: true,
            create_ufs_file_options: Some(ufs_opts()),
            need_async_persist: false,
        },
        Some(4) => WriteStrategy {           // THROUGH: 只 ufs
            cache_stream: false,
            ufs_stream: true,
            create_ufs_file_options: Some(ufs_opts()),
            need_async_persist: false,
        },
        Some(5) => WriteStrategy {           // ASYNC_THROUGH: 只 cache
            cache_stream: true,
            ufs_stream: false,
            create_ufs_file_options: None,
            need_async_persist: true,
        },
        _ => WriteStrategy {                 // MUST_CACHE / TRY_CACHE / 默认
            cache_stream: true,
            ufs_stream: false,
            create_ufs_file_options: None,
            need_async_persist: false,
        },
    }
}
```

### 4.2 `GooseFsFileWriter` 新增 UFS 单流字段

```rust
pub struct GooseFsFileWriter {
    // ... 既有字段 ...

    /// CACHE_THROUGH / THROUGH 场景下：跨整个文件生命周期的单个 UFS 流
    ufs_stream: Option<GrpcBlockWriter>,
    /// 累计写入字节数（close 时传给 completeFile 作为 ufsLength）
    total_bytes_written: u64,
    /// 策略
    strategy: WriteStrategy,
}
```

### 4.3 `open_ufs_stream()`：挑 worker + 建单条长流

参考 Java `UnderFileSystemFileOutStream` 的 worker 选择策略（`Collections.shuffle(workers); workers.get(0)`）：

```rust
async fn open_ufs_stream(&mut self) -> Result<()> {
    // 独立挑一个 worker（与 cache block 的 worker 可以不同）
    let worker_info = self.router.pick_any_worker().await?;
    let addr = worker_info.data_rpc_addr();
    let worker = self.worker_pool.acquire(&addr).await?;

    let opts = WriteBlockOptions {
        request_type: RequestType::UfsFile,
        create_ufs_file_options: self.strategy.create_ufs_file_options.clone(),
    };
    // block_id = -1 (ID_UNUSED), space = i64::MAX（Long.MAX_VALUE）
    let writer = GrpcBlockWriter::open(&worker, -1, i64::MAX, opts).await?;
    self.ufs_stream = Some(writer);
    Ok(())
}
```

### 4.4 `write()`：双流并行推入

```rust
pub async fn write(&mut self, data: &[u8]) -> Result<()> {
    // 1) cache 流：按 block 切分（复用现有逻辑）
    if self.strategy.cache_stream {
        self.write_to_cache_stream(data).await?;
    }
    // 2) ufs 流：整条长流 append，只按 chunk_size 切 chunk，不按 block 切
    if self.strategy.ufs_stream {
        if self.ufs_stream.is_none() {
            self.open_ufs_stream().await?;
        }
        if let Some(ufs) = self.ufs_stream.as_mut() {
            ufs.write_all(data, self.config.chunk_size).await?;
        }
    }
    self.total_bytes_written += data.len() as u64;
    Ok(())
}
```

注意：**不要在 ufs 流上按 block 边界 flush/close/reopen**，这就是本次 bug 的根源。

### 4.5 `close()`：严格顺序

参考 Java 实现顺序（`GooseFSFileOutStream.close` line ~212-250）：

```rust
pub async fn close(mut self) -> Result<()> {
    // 1) 先 flush + close UFS 流
    if let Some(mut ufs) = self.ufs_stream.take() {
        ufs.flush().await?;
        ufs.close().await?;
    }
    // 2) 关最后一个 cache block（复用现有逻辑）
    if self.strategy.cache_stream {
        self.close_current_block().await?;
    }
    // 3) completeFile：CACHE_THROUGH/THROUGH 必须带上 ufsLength，否则 master 端对不上
    let ufs_length = if self.strategy.ufs_stream {
        Some(self.total_bytes_written as i64)
    } else {
        None
    };
    self.master.complete_file(&self.path, ufs_length).await?;

    // 4) ASYNC_THROUGH：额外触发异步持久化
    if self.strategy.need_async_persist {
        self.master.schedule_async_persistence(&self.path).await?;
    }
    Ok(())
}
```

### 4.6 `cancel()`：保证两条流都能安全中断

```rust
pub async fn cancel(mut self) -> Result<()> {
    if let Some(mut ufs) = self.ufs_stream.take() {
        let _ = ufs.cancel().await;     // 尽力取消，不因失败阻塞
    }
    if self.strategy.cache_stream {
        let _ = self.cancel_current_block().await;
    }
    self.master.delete_file(&self.path, /*recursive=*/false).await?;
    Ok(())
}
```

---

## 5. 附带需要一起改的点

1. **`WorkerRouter::pick_any_worker()`**：新增接口（或 `pick_worker_for_ufs()`），
   行为等价于 Java 的 `Collections.shuffle(workers); workers.get(0)`。

2. **`worker.rs` 中 `offset: Some(0)` 不需要改**：对 cache block 来说 offset=0 表示 block 内
   从头开始，是正确的；对 UFS 流来说，初始 command 的 `offset=0` 表示"从文件头开始连续推"，
   **后续 chunk 不再携带 offset**，Worker 端靠同一个 `OutputStream` 维护 position。所以
   **只要不按 block 切 UFS 流，worker.rs 可以不动**。

3. **Cache 流失败降级**（可作为后续 TODO，不在本次修复范围）：
   Java 有 `handleCacheWriteException`——cache 流失败且 UFS 流正常时，把当前及后续 block
   的 `mShouldCacheCurrentBlock` 置 false，仅继续写 UFS。本次修复只要保证：
   - cache 流失败 **不牵连** UFS 流；
   - UFS 流失败 **不牵连** cache 流（可以降级为 MUST_CACHE 语义）。
   后续再跟进完整降级逻辑。

4. **`create_ufs_file_options` 的 owner / group / mode / mount_id**：必须从 master 返回的
   `FileInfo` 里如实透传，不要默认填 0 / 空串，否则 Worker 端 `createNonexistingFile` 可能
   因权限不足失败（尤其是 Local UFS）。

---

## 6. 测试与验证计划

### 6.1 单元测试（新增）

在 `tests/` 或 `src/io/file_writer.rs` 的 `#[cfg(test)]` 中补充：

1. `test_resolve_write_strategy_cache_through`：确认返回 `cache_stream=true, ufs_stream=true`。
2. `test_resolve_write_strategy_through`：确认返回 `cache_stream=false, ufs_stream=true`。
3. `test_resolve_write_strategy_must_cache`：确认返回 `cache_stream=true, ufs_stream=false`。
4. `test_resolve_write_strategy_async_through`：确认 `need_async_persist=true`。

### 6.2 Mock Worker 集成测试

用 mock gRPC server 记录收到的 `WriteRequest` 序列，验证：

1. **CACHE_THROUGH 写 3 个 block**：
   - UFS 流：**恰好 1 个** `WriteRequestCommand(type=UFS_FILE, id=-1)` + N 个 chunk。
   - Cache 流：**恰好 3 个** `WriteRequestCommand(type=GOOSEFS_BLOCK, id=<blk_id>)`，每个独立 block。
2. **THROUGH 写 3 个 block 体量的数据**：只有 1 个 UFS 流，无 cache 流。
3. **MUST_CACHE 写 3 个 block**：无 UFS 流，3 个 cache block。

### 6.3 真机端到端验证（3-node 集群）

在 Lance / VectorDBBench 基准测试中：

1. 写入 ~485MB Lance 数据文件后，对比：
   - `goosefs fs ls -h <path>` 的逻辑大小；
   - 直接登陆 UFS（本地 ext4 / COS bucket）查看物理文件大小。
   - **预期**：两者一致，不再是 ~53MB。
2. 写入后立即 `goosefs fs free <path>` 强制驱逐缓存，再做 read/search 校验：
   - 不应再出现 `skipping N bytes` 警告；
   - 数据内容与写入前一致（md5sum 比对）。
3. THROUGH 模式对照测试：确认 UFS 大小完整、cache 为空。
4. MUST_CACHE 模式对照测试：确认 UFS 为空（或不存在该文件）、cache 完整。

---

## 7. 风险评估

| 风险 | 评估 | 缓解措施 |
|---|---|---|
| UFS 流与 cache 流的 worker 不同，网络分区时单边失败 | 中 | 保证两条流错误互不传染；UFS 失败直接返回错误；cache 失败暂时走 fail-fast（后续再加降级） |
| `block_id = -1` 与既有代码假设冲突 | 低 | `worker.rs::GrpcBlockWriter::open` 已支持任意 block_id，只要传负数即可；确认 ID_UNUSED 的常量值与 Java 对齐（-1） |
| `i64::MAX` 作为 length 传给 Worker 溢出 | 低 | Java 也是 `Long.MAX_VALUE`，Worker 端 handler 已兼容 |
| `completeFile` 携带 ufsLength 字段语义变化 | 低 | master API 已支持该参数；仅在 `ufs_stream=true` 场景传；向后兼容 |
| chunk_size 选择影响 UFS 吞吐 | 低 | 沿用现有 `config.chunk_size`（通常 1-4MB），不做新配置 |

---

## 8. 变更清单（预计 Commit 划分）

建议拆分为 3 个 commit：

1. **commit 1**: `fix(writer): rewrite CACHE_THROUGH to dual-stream architecture`
   - `src/io/file_writer.rs`：新 `WriteStrategy`、`GooseFsFileWriter` 双流字段、`write/close/cancel`。
   - `src/client/router.rs`：`pick_any_worker()`。

2. **commit 2**: `test(writer): add unit & mock tests for write strategies`
   - `tests/write_strategy_test.rs`（新建）。

3. **commit 3**: `docs: add CACHE_THROUGH write bug root cause analysis`
   - 即本文件。

---

## 9. 参考

- Java 对照实现：
  - `core/client/fs/src/main/java/com/qcloud/cos/goosefs/client/block/stream/UnderFileSystemFileOutStream.java`
  - `core/client/fs/src/main/java/com/qcloud/cos/goosefs/client/file/GooseFSFileOutStream.java`
  - `core/server/worker/src/main/java/com/qcloud/cos/goosefs/worker/grpc/UfsFileWriteHandler.java`
- 协议：
  - `proto/grpc/block_worker.proto` —— `WriteRequestCommand`, `CreateUfsFileOptions`
- 当前 Rust 错误实现：
  - `src/io/file_writer.rs::resolve_write_strategy`（line ~88-99）
  - `src/client/worker.rs::GrpcBlockWriter::open`（line ~290-310）
