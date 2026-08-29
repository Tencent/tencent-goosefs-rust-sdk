# 异步写 / 写降级 Java 对齐实现方案（代码行级）

> 基准（authority）：`/opt/sourcecode/cos/goosefs`（Java SDK）
> 待改造：`/opt/sourcecode/cos/tencent-goosefs-rust-sdk`（Rust SDK）
> 核心文件：`src/io/file_writer.rs`、`src/client/master.rs`、`src/config.rs`
> 状态：设计定稿，待实现

---

## 0. 摘要

本文覆盖 8 个已确认的差异项。其中 **G3（cache 写失败降级到 UFS）是主线缺口**，G1/G2 依附于它；G4 复核后确认**已经对齐**，无需改动。

| ID | 差异项 | 现状 | 结论 | 优先级 |
|:--|:--|:--|:--|:--|
| G1 | `CompleteFilePOptions.forcePersisted` | Rust 从不设置 | 缺失，需补 | P0（依赖 G3） |
| G2 | `asyncPersistOptions.persistenceWaitTime` / `commonOptions` | 恒为 `None`，且忽略 `NO_AUTO_PERSIST` 语义 | 缺失，需补 | P1 |
| G3 | cache 写失败降级到 UFS（含"仅首块可降级"约束） | 完全缺失，一失败即整体失败 | 缺失，需补 | **P0** |
| G4 | UFS 流 worker pick 未过滤 `failed_workers` | **复核结论：已过滤** | 已对齐，仅需补一条注释 | — |
| G5 | `completeFile` 失败后的 UFS 恢复 | 有 `delete(goosefs_only)`，缺 `loadMetadata(ALWAYS)`，且恢复成功后仍抛错 | 部分缺失，需补 | P1 |
| G6 | worker 池为空时 `failedWorkers.clear()` | 无重置，靠 `use_all_workers` 兜底 | 部分缺失，需补 | P2 |
| G7 | `total_bytes_written` 记账口径 | 分散在 cache / UFS 两条路径，降级后易错 | 重构前置项 | P0（G3 前置） |
| G8 | `flush()` 语义 | 不 flush UFS 流；cache flush 条件过宽；失败走降级 | 三处不一致，需补 | P0（正确性） |

---

## 1. 基准语义还原

### 1.1 WriteType → UnderStorageType → Rust WriteStrategy

Java `WriteType.getUnderStorageType()`（`core/client/fs/src/main/java/com/qcloud/cos/goosefs/client/WriteType.java:88-95`）：

```java
public UnderStorageType getUnderStorageType() {
  if (isThrough()) {          // CACHE_THROUGH | THROUGH
    return UnderStorageType.SYNC_PERSIST;
  } else if (isAsync()) {     // ASYNC_THROUGH
    return UnderStorageType.ASYNC_PERSIST;
  }
  return UnderStorageType.NO_PERSIST;
}
```

| WriteType | UnderStorageType | GooseFSStorageType | Rust `WriteStrategy` 现状 | 是否允许降级 |
|:--|:--|:--|:--|:--|
| MUST_CACHE (1) | NO_PERSIST | STORE | `cache=T, ufs=F, async=F` | ❌ |
| TRY_CACHE (2) | NO_PERSIST | STORE | `cache=T, ufs=F, async=F` | ❌ |
| CACHE_THROUGH (3) | SYNC_PERSIST | STORE | `cache=T, ufs=T, async=F` | ✅ 任意时刻 |
| THROUGH (4) | SYNC_PERSIST | NO_STORE | `cache=F, ufs=T, async=F` | n/a（本就无 cache） |
| ASYNC_THROUGH (5) | ASYNC_PERSIST | STORE | `cache=T, ufs=F, async=T` | ✅ **仅首块未开成功时** |
| NONE (6) | NO_PERSIST | NO_STORE | `cache=T, ufs=F, async=F` ⚠️ | ❌ |

> ⚠️ **附带发现（不在本次范围）**：Java `NONE` 的 `GooseFSStorageType` 是 `NO_STORE`（`WriteType.java:78-83`，`isCache()` 不含 `NONE`），即数据既不入 cache 也不入 UFS。Rust `resolve_write_strategy` 的兜底分支（`src/io/file_writer.rs:148-153`）把 `NONE` 与 `MUST_CACHE` 一视同仁，仍然写 cache。`NONE` 官方定位是"仅供测试"，影响面极小，单独开 issue 跟进即可。

### 1.2 Java 的两种 UFS 输出流

`UnderFileSystemFileOutStream`（`core/client/fs/.../block/stream/UnderFileSystemFileOutStream.java:35-61`）有两种形态：

| 形态 | 构造方式 | 说明 |
|:--|:--|:--|
| `CLIENT_UFS` | `create(UnderFSDataOutputStream)` | 客户端进程内直连 Hadoop FS 写 UFS |
| `WORKER_UFS` | `create(context, worker, options)` → `GrpcDataWriter.create(ctx, addr, ID_UNUSED(-1), Long.MAX_VALUE, RequestType.UFS_FILE, options)` | 经 worker 转发写 UFS |

`GooseFSFileOutStream` 构造函数（`GooseFSFileOutStream.java:175-221`）的分支：

```java
if (!mUnderStorageType.isSyncPersist()) {
  mUnderStorageOutputStream = null;                       // ASYNC_THROUGH / MUST_CACHE 走这里
} else if (conf.getBoolean(USER_LOCAL_WRITE_UFS_CLIENT_ENABLED)) {   // 默认 true
  mUnderStorageOutputStream = createUnderStorageOutputStream();       // CLIENT_UFS
} else {
  ... UnderFileSystemFileOutStream.create(mContext, worker, mOptions) // WORKER_UFS
}
```

**对 Rust 的影响**：Rust SDK 没有 Hadoop FS 客户端，`CLIENT_UFS` 形态无法实现。因此**降级路径统一走 `WORKER_UFS`**，也就是复用现有的 `GoosefsFileWriter::open_ufs_stream()`（`src/io/file_writer.rs:875-925`），它已经是 `RequestType::UfsFile` + `block_id=-1` + `length=i64::MAX`，与 Java `WORKER_UFS` 逐字段一致。

> 注：`USER_LOCAL_WRITE_UFS_CLIENT_ENABLED` 默认 `true`（`ClientPropertyKey.java:36-42`），所以生产上 Java 走的是 CLIENT_UFS。Rust 走 WORKER_UFS 在**数据落盘结果**上等价（同一个 UFS 文件、同样的 `CreateUfsFileOptions`），差异只在链路多一跳 worker、以及 Hadoop delegation token 鉴权路径不同。这一点需在 release note 中说明。

### 1.3 降级决策表（`handleCacheWriteException` 完整还原）

Java 源码 `GooseFSFileOutStream.java:532-568`。这个方法**正常返回 = 降级**，**抛异常 = 致命**：

```java
private void handleCacheWriteException(Exception e) throws IOException {
  LOG.warn("Failed to write into GooseFSStore, canceling write attempt.", e);

  // ① 副本约束类异常，不允许静默降级
  if (e instanceof ResourceExhaustedException || e instanceof InvalidArgumentException) {
    mCanceled = true;
    throw new IOException(ExceptionMessage.FAILED_CACHE.getMessage(e.getMessage()), e);
  }
  // ② NO_PERSIST（MUST_CACHE / TRY_CACHE / NONE）：无处可降
  if (!mUnderStorageType.isSyncPersist() && !mUnderStorageType.isAsyncPersist()) {
    mCanceled = true;
    throw new IOException(...);
  }
  // ③ 异步写：openBlock 成功后遇到写数据异常，则不应该降级
  if (mUnderStorageType.isAsyncPersist() && openBlock) {
    mCanceled = true;
    throw new IOException(...);
  }

  mShouldCacheCurrentBlock = false;                 // ④ 关闭 cache 分支
  if (mCurrentBlockOutStream != null) {
    mCurrentBlockOutStream.cancel();
  }

  // ⑤ 鉴权异常直接抛（并同时 cancel UFS 流）
  if (e instanceof UnauthenticatedException || e instanceof PermissionDeniedException) {
    handleUnderStorageWriteException(new IOException(...));
  }
  // ⑥ 首块都没开成功且不允许忽略未知状态 → 抛（默认 IGNORE=true，此分支默认不触发）
  if (!openBlock && !conf.getBoolean(USER_LOCAL_UFS_CLIENT_IGNORE_BLOCK_STREAM_UNKNOWN_STATUS)) {
    handleUnderStorageWriteException(new IOException(...));
  }
  // 走到这里 = 降级
}
```

判定矩阵（`IGNORE_BLOCK_STREAM_UNKNOWN_STATUS` 默认 `true`，见 `ClientPropertyKey.java:1255-1261`）：

| # | 条件 | 结果 |
|:--|:--|:--|
| ① | `ResourceExhausted` / `InvalidArgument` | 致命，`canceled=true` |
| ② | UnderStorageType == NO_PERSIST | 致命，`canceled=true` |
| ③ | ASYNC_PERSIST **且** `openBlock == true` | 致命，`canceled=true` |
| ⑤ | `Unauthenticated` / `PermissionDenied` | 致命（并 cancel UFS 流） |
| ⑥ | `!openBlock && !IGNORE_UNKNOWN`（默认不成立） | 致命 |
| — | 其余 | **降级**：`shouldCacheCurrentBlock=false`，cancel 当前块，后续只写 UFS |

### 1.4 `openBlock` 语义与"仅首块可降级"的数据正确性证明

`openBlock` 在 `getNextBlock()` 里、`mBlockStore.getOutStream(...)` 成功之后置位（`GooseFSFileOutStream.java:497`），**且全生命周期不复位**：

```java
mCurrentBlockOutStream = mBlockStore.getOutStream(...);   // 471-495
mShouldCacheCurrentBlock = true;
openBlock = true;                                          // 497
```

因此 `openBlock == false` 等价于「**从未成功打开过任何 block**」。

这条约束不是保守策略，而是**数据正确性的必要条件**：

ASYNC_THROUGH 在构造时 `mUnderStorageOutputStream == null`（1.2 节第一个分支），UFS 流只在降级那一刻才创建。若允许在第 N 个 block（N>0）失败时降级，则前 N 个 block 的数据只进了 cache、从未进入 UFS 流，而降级后新建的 UFS 流是从**文件偏移 0** 开始写的 —— UFS 上会得到一个缺失前 N 个 block 的残缺文件。

反之，`openBlock == false` 意味着首次 `getNextBlock()` 就失败，此时 `mCurrentBlockOutStream.write(...)` 一次都没执行过，`mBytesWritten == 0`。降级后 `writeInternal` 尾部把**整个当前缓冲区** `b[off..off+len]` 交给 UFS 流（`GooseFSFileOutStream.java:446-449`），UFS 文件从字节 0 开始完整覆盖。**无数据丢失。**

> Rust 实现必须原样保留这条不变量，并在测试中固化（见 §6 T-G3-4）。

### 1.5 降级触发点在 `writeInternal` 中的位置

`GooseFSFileOutStream.java:417-449`：

```java
if (mShouldCacheCurrentBlock) {
  try {
    int tLen = len, tOff = off;
    while (tLen > 0) {
      if (mCurrentBlockOutStream == null || mCurrentBlockOutStream.remaining() == 0) {
        getNextBlock();                       // ← 首块失败在这里抛
      }
      long left = mCurrentBlockOutStream.remaining();
      if (left >= tLen) { mCurrentBlockOutStream.write(b, tOff, tLen); tLen = 0; }
      else { mCurrentBlockOutStream.write(b, tOff, (int) left); tOff += left; tLen -= left; }
    }
  } catch (Exception e) {
    handleCacheWriteException(e);             // ← 正常返回 = 降级
    if (mUnderStorageType.isAsyncPersist()) { // ← 仅异步写需要临时建流
      if (mUnderStorageOutputStream == null) {
        mUnderStorageOutputStream = createUnderStorageOutputStream();
      }
    }
  }
}

if (mUnderStorageOutputStream != null) {      // ← 降级后天然接管
  mUnderStorageOutputStream.write(b, off, len);
  Metrics.BYTES_WRITTEN_UFS.inc(len);
}
mBytesWritten += len;                          // ← 记账只在这一处
```

三个关键点：
1. `try` 包住**整个 while 循环**，所以 `getNextBlock()` 与 `write()` 的失败共用一条 catch；
2. catch 后**不 rethrow、不 return**，直接落到 UFS 分支；
3. UFS 分支写的是**完整的 `b[off..off+len]`**，不是 cache 剩下的部分。

### 1.6 `close()` 中的 `forcePersisted` / `asyncPersistOptions` 分支

`GooseFSFileOutStream.java:284-301`：

```java
if (!mCanceled && mUnderStorageType.isAsyncPersist()) {
  if (mUnderStorageOutputStreamCompleted) {          // 降级成功 → UFS 上已有完整文件
    optionsBuilder.setForcePersisted(true);
  } else if (mOptions.getPersistenceWaitTime() != Constants.NO_AUTO_PERSIST) {
    optionsBuilder.setAsyncPersistOptions(
        FileSystemOptions.scheduleAsyncPersistDefaults(mContext.getPathConf(mUri)).toBuilder()
            .setCommonOptions(mOptions.getCommonOptions())
            .setPersistenceWaitTime(mOptions.getPersistenceWaitTime()));
  }
  // ... addLocations(最后一个 block)
}
```

三分支互斥，且**仅在 ASYNC_PERSIST 下生效**：

| 分支 | 触发条件 | Master 行为 |
|:--|:--|:--|
| `forcePersisted=true` | 已降级并成功写完 UFS | `isPersisted=true` → 拉 UFS fingerprint → `PersistenceState.PERSISTED`，**不排异步 persist** |
| `asyncPersistOptions=...` | 未降级 且 `waitTime != -1` | `scheduleAsyncPersistenceInternal(..., isPersistCmd=false)` → `mHybridPersistenceManager.addPersistRequest` |
| 都不设 | 未降级 且 `waitTime == -1`（`NO_AUTO_PERSIST`） | **完全不排 persist**，等 rename 或 persist CLI |

Master 侧证据：
- `DefaultFileSystemMaster.java:1649` — `boolean isPersisted = fileInode.isPersisted() || context.getOptions().getForcePersisted();`
- `DefaultFileSystemMaster.java:1726-1728` — `if (getForcePersisted()) builder.setPersistenceState(PersistenceState.PERSISTED.name());`
- `DefaultFileSystemMaster.java:1575-1577` — `if (hasAsyncPersistOptions()) scheduleAsyncPersistenceInternal(...)`

相关常量：
- `Constants.NO_AUTO_PERSIST = -1`（`core/base/src/main/java/com/qcloud/cos/goosefs/Constants.java:207-209`）
- `USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME` 默认 `"0"`（`ClientPropertyKey.java:387-389`）
- `FileSystemOptions.scheduleAsyncPersistDefaults` 只设 `commonOptions`（`FileSystemOptions.java:291-296`）

### 1.7 `close()` catch 块的 UFS 恢复

`GooseFSFileOutStream.java:315-337`：

```java
} catch (Throwable e) {
  if ((mUnderStorageType.isSyncPersist() || mUnderStorageType.isAsyncPersist())
      && mUnderStorageOutputStreamCompleted) {
    try (CloseableResource<FileSystemMasterClient> masterClient = ...) {
      masterClient.get().delete(mUri, DeletePOptions.getDefaultInstance()
          .toBuilder().setGoosefsOnly(true).build());
      GetStatusPOptions option = FileSystemOptions.getStatusDefaults(...).toBuilder()
          .setLoadMetadataType(LoadMetadataPType.ALWAYS)
          .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0))
          .build();
      masterClient.get().getStatus(mUri, option);
      LOG.warn("GFS completeFile failed but file [{}] was successfully recovered from UFS.", mUri, e);
      return;                       // ← 恢复成功 = 整体视为写入成功，吞掉异常
    } catch (Throwable reloadException) {
      LOG.warn("Failed to recover file [{}] from UFS ...", mUri, reloadException);
    }
  }
  throw mCloser.rethrow(e);
}
```

三个必须复刻的点：
1. 触发条件是 `(SYNC_PERSIST || ASYNC_PERSIST) && ufsCompleted` —— **包含降级后的 ASYNC_THROUGH**，不只是 CACHE_THROUGH；
2. `delete` 之后必须 `getStatus(LoadMetadataPType.ALWAYS, syncIntervalMs=0)` 把 UFS 元数据拉回来；
3. 两步都成功 → **`close()` 返回 Ok**，不向调用方抛错。

### 1.8 worker 池为空时的失败列表重置

`GooseFSBlockStore.java:333-338`：

```java
if (workerPool.isEmpty()) {
  LOG.debug("No available GooseFS worker found, will retry to pick workers");
  failedWorkers.clear();
  currentWorkers.set(null);
  throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
}
```

语义：候选池被过滤空之后，**清空失败列表**并让 `currentWorkers` 失效，使外层 `getNextBlock()` 的 catch 重试（`GooseFSFileOutStream.java:489-495`）能在**全新的、未被失败标记污染的**池上重新 pick。

---

## 2. Rust 现状定位

### 2.1 关键代码位置

| 关注点 | 位置 |
|:--|:--|
| `WriteStrategy` 结构体 | `src/io/file_writer.rs:90-101` |
| `resolve_write_strategy` | `src/io/file_writer.rs:114-155`（ASYNC_THROUGH 分支 `141-146`） |
| `GoosefsFileWriter` 字段 | `src/io/file_writer.rs:199-273` |
| `write()` | `src/io/file_writer.rs:431-459`（串行喂两条流 `447-456`） |
| `flush()` | `src/io/file_writer.rs:476-499` |
| `write_to_cache_stream()` | `src/io/file_writer.rs:519-561` |
| `write_to_ufs_stream()` | `src/io/file_writer.rs:566-588` |
| `open_next_block()` | `src/io/file_writer.rs:599-637` |
| `open_replica_writers()` | `src/io/file_writer.rs:641-750`（空池分支 `668-672`） |
| `close_current_block()` | `src/io/file_writer.rs:789-865` |
| `open_ufs_stream()` | `src/io/file_writer.rs:875-925` |
| `handle_cache_write_exception()` | `src/io/file_writer.rs:933-950` |
| `handle_complete_file_error()` | `src/io/file_writer.rs:1117-1142`（TODO `1137-1139`） |
| `close()` | `src/io/file_writer.rs:1160-1277`（persist 选项 `1235-1242`） |
| `complete_file_locations()` | `src/io/file_writer.rs:1713-1722` |
| `MasterClient::complete_file_with_options` | `src/client/master.rs:693-726` |
| `MasterClient::get_status` | `src/client/master.rs:475-...`（写死 `GetStatusPOptions::default()` 于 `494`） |
| `filter_no_space_workers` | `src/io/replica_write.rs:131-158` |
| `degrade_replicas` | `src/io/replica_write.rs:167-180` |
| `WorkerRouterView::pick_any_worker` | `src/block/router.rs:1488-1525` |
| `WorkerRouterView::filter_not_failed` | `src/block/router.rs:1266-1276` |

### 2.2 proto 侧无需改动

`src/generated/com.qcloud.cos.goosefs.grpc.file.rs:70-85` 已包含全部字段：

```rust
pub struct CompleteFilePOptions {
    pub ufs_length: Option<i64>,                                        // tag 1
    pub async_persist_options: Option<ScheduleAsyncPersistencePOptions>,// tag 2
    pub common_options: Option<FileSystemMasterCommonPOptions>,         // tag 3
    pub crc_type: Option<i32>,                                          // tag 4
    pub crc_value: Option<i64>,                                         // tag 5
    pub locations: Vec<FileLocation>,                                   // tag 6
    pub force_persisted: Option<bool>,                                  // tag 7  ← 已存在，从未被赋值
}
```

`ScheduleAsyncPersistencePOptions`（同文件 `735-740`）也已有 `common_options` / `persistence_wait_time`。

### 2.3 G4 复核：UFS 流 worker pick —— **已对齐，不需改动**

早前分析认为 `open_ufs_stream()` 用的 `pick_any_worker()` 不过滤失败 worker。复核 `src/block/router.rs:1488-1525` 后确认**该结论有误**：

```rust
pub async fn pick_any_worker(&self) -> Result<WorkerInfo> {
    if self.workers.is_empty() { return Err(...); }
    self.cleanup_expired_failures();
    let eligible: Vec<WorkerInfo> = self.workers.iter()
        .filter(|w| match w.address.as_ref() {
            Some(addr) => !self.is_failed(&worker_addr_key(addr)),   // ← 已过滤
            None => false,
        })
        .cloned().collect();
    let pool = if eligible.is_empty() { (*self.workers).clone() } else { eligible };  // ← 全失败时兜底
    ...
    let idx = rand::Rng::random_range(&mut rand::rng(), 0..pool.len());
    Ok(pool[idx].clone())
}
```

这与 Java `handleFailedWorkers`（`GooseFSFileOutStream.java:583-596`）语义一致：优先未失败者，全失败时回退到全量池。

唯一细微差异：全失败时 Java 挑**失败时间最早**的那一个，Rust 随机挑。影响可忽略（Rust 侧 `failure_ttl` 已提供等价的时间衰减）。

**动作**：仅在 `open_ufs_stream()` 上方补一条注释指明该对齐关系，避免后续 review 重复提问。

---

## 3. 改造方案（代码级）

### 3.1 G7（前置）：统一 `total_bytes_written` 记账口径

**问题**：当前记账分散在两处，且互斥依赖 `write_strategy.ufs_stream`：

- `src/io/file_writer.rs:580` —— UFS 路径 `self.total_bytes_written += total as u64;`
- `src/io/file_writer.rs:857-859` —— cache 路径 `if !self.write_strategy.ufs_stream { self.total_bytes_written += bytes_written; }`

引入降级后，ASYNC_THROUGH 会在运行时从「cache 记账」切到「UFS 记账」，`write_strategy.ufs_stream` 这个**静态**标志不再能表达真实状态，会导致 `ufs_length` 记账错误。

**方案**：改为 Java 口径 —— 在 `write()` 里按接受字节数记一次（`GooseFSFileOutStream.java:450`）。

**改动 1** — `src/io/file_writer.rs:576-587`，删除 UFS 路径记账：

```rust
        let total = data.len();
        match ufs.write_all(data, chunk_size).await {
            Ok(()) => {
-               // Track total UFS bytes written (for completeFile's ufsLength).
-               self.total_bytes_written += total as u64;
                // Instrument: record UFS-path bytes written.
                crate::metrics::counter(crate::metrics::name::CLIENT_BYTES_WRITTEN_UFS)
                    .inc(total as i64);
                Ok(())
            }
            Err(e) => self.handle_ufs_write_exception(e).await,
        }
```

**改动 2** — `src/io/file_writer.rs:856-859`，删除 cache 路径记账：

```rust
            self.committed_block_ids.push(block_id);
-           if !self.write_strategy.ufs_stream {
-               self.total_bytes_written += bytes_written;
-           }
            Ok(loc)
```

**改动 3** — `src/io/file_writer.rs:431-459` `write()` 尾部统一记账（与 G3 的改动合并落地，见下）。

**影响面**：`bytes_written()`（`1337-1339`）与 `write_file_with_context_and_options`（`1324-1334`）的返回值语义从「已提交字节」变为「已接受字节」。二者在 `close()` 之后取值相同，无外部行为变化。

---

### 3.2 G3（主线）：cache 写失败降级到 UFS

#### 3.2.1 `WriteStrategy` 补 UFS 建流参数

ASYNC_THROUGH 降级时需要 `CreateUfsFileOptions`，但当前该分支为 `None`。`build_ufs_opts()` 只依赖 `FileInfo`，无副作用，可安全提前构造。

`src/io/file_writer.rs:141-146`：

```rust
        // ASYNC_THROUGH: cache only, schedule async persist after close.
        Some(5) => WriteStrategy {
            cache_stream: true,
            ufs_stream: false,
-           create_ufs_file_options: None,
+           // Not opened eagerly (`ufs_stream: false`), but pre-resolved so the
+           // degrade path in `write()` can open a WORKER_UFS stream without
+           // re-deriving them. Java builds these in
+           // `createUnderStorageOutputStream()` at degrade time.
+           create_ufs_file_options: Some(build_ufs_opts()),
            need_async_persist: true,
        },
```

MUST_CACHE / TRY_CACHE / NONE 分支（`148-153`）**保持 `None`** —— 它们是 NO_PERSIST，不允许降级，`None` 同时充当一道保险。

同步更新单测 `test_strategy_async_through`（`1909-1917`）：

```rust
-       assert!(s.create_ufs_file_options.is_none());
+       assert!(s.create_ufs_file_options.is_some());
```

#### 3.2.2 新增两个状态字段

对应 Java 的 `openBlock`（`GooseFSFileOutStream.java:118`）与 `mShouldCacheCurrentBlock`（`109`）。

`src/io/file_writer.rs`，在 `_router_needs_init`（`272`）之后插入：

```rust
    /// Whether any cache block was ever opened successfully.
    ///
    /// Mirrors Java `GoosefsFileOutStream.openBlock` — set once in
    /// `open_next_block` and never reset. `false` means no block writer has
    /// ever been established, which is the only state where ASYNC_THROUGH may
    /// degrade to a UFS-only write without losing already-cached bytes.
    block_opened: bool,
    /// Whether the cache branch is still live.
    ///
    /// Mirrors Java `mShouldCacheCurrentBlock`. Cleared by
    /// `handle_cache_write_exception` when the writer degrades to UFS-only.
    should_cache: bool,
```

三处构造点补字段：
- `create_with_context` 的 `Ok(Self { ... })`（`356-375`）：`block_opened: false, should_cache: true,`
- 测试夹具 `make_drop_test_writer`（`2003-2022`）：同上
- 其余 `Self { ... }` 字面量（如有）同步

#### 3.2.3 `open_next_block` 置位 `block_opened`

`src/io/file_writer.rs:616-636`，两个成功分支各加一行：

```rust
        match self.open_replica_writers(block_id, block_size, &plan, false).await {
            Ok(active) => {
                self.current_block_writer = Some(active);
+               // Java `GoosefsFileOutStream.getNextBlock()` sets `openBlock = true`
+               // right after `getOutStream` returns. Never reset.
+               self.block_opened = true;
                Ok(())
            }
            Err(e) => {
                warn!(...);
                let active = self.open_replica_writers(block_id, block_size, &plan, true).await?;
                self.current_block_writer = Some(active);
+               self.block_opened = true;
                Ok(())
            }
        }
```

#### 3.2.4 重写 `handle_cache_write_exception`

**契约变更**：`Ok(())` 从「不可能发生」变为「**已降级**」；`Err(e)` 表示致命。这与 Java「正常返回 = 降级」逐字对应。

替换 `src/io/file_writer.rs:927-950` 整段：

```rust
    /// Decide whether a cache-write failure is fatal or can degrade to UFS.
    ///
    /// # Java authority
    ///
    /// `GoosefsFileOutStream.handleCacheWriteException`
    /// (`GooseFSFileOutStream.java:532-568`). Java signals "degrade" by
    /// returning normally and "fatal" by throwing; this method mirrors that
    /// with `Ok(())` / `Err(e)`.
    ///
    /// Returns `Ok(())` only when the caller may keep going on a UFS-only
    /// stream. Every `Err` path leaves the writer effectively cancelled.
    async fn handle_cache_write_exception(&mut self, err: Error) -> Result<()> {
        warn!(
            path = %self.path,
            error = %err,
            "failed to write to Goosefs cache, evaluating degrade to UFS"
        );

        // ① Replica-contract violations must never silently degrade.
        //    Java: `e instanceof ResourceExhaustedException || InvalidArgumentException`.
        let contract_violation = matches!(
            err,
            Error::ResourceExhausted { .. } | Error::InvalidArgument { .. }
        );
        // ② NO_PERSIST write types have nowhere to degrade to.
        //    Java: `!isSyncPersist() && !isAsyncPersist()`.
        let persistable = self.write_strategy.ufs_stream || self.write_strategy.need_async_persist;
        // ③ ASYNC_THROUGH may only degrade before any block was opened —
        //    otherwise the already-cached blocks would be missing from the
        //    UFS file (see docs/ASYNC_WRITE_JAVA_PARITY_IMPL.md §1.4).
        //    Java: `isAsyncPersist() && openBlock`.
        let async_after_open = self.write_strategy.need_async_persist && self.block_opened;

        if contract_violation || !persistable || async_after_open {
            self.cancelled.store(true, Ordering::SeqCst);
            self.tear_down_cache_block().await;
            return Err(err);
        }

        // Java clears the cache branch *before* the auth checks below, so the
        // subsequent `close()` does not try to commit a dead block stream.
        self.should_cache = false;
        self.tear_down_cache_block().await;

        // ⑤ Auth failures are never recoverable by switching storage tier.
        //    Java routes them through `handleUnderStorageWriteException`,
        //    which cancels the UFS stream and rethrows.
        if err.is_access_denied() {
            self.cancelled.store(true, Ordering::SeqCst);
            if let Some(writer) = self.ufs_stream.take() {
                writer.cancel().await;
            }
            self.ufs_worker_addr = None;
            return Err(err);
        }

        // ⑥ Java's `!openBlock && !IGNORE_BLOCK_STREAM_UNKNOWN_STATUS` guard.
        //    `goosefs.user.local.ufs.client.ignore.block.stream.unknown.status`
        //    defaults to `true`, so this branch is inert unless an operator
        //    explicitly opts out. Not ported: the Rust SDK has no equivalent
        //    knob and adding one would only ever make writes fail harder.

        info!(
            path = %self.path,
            block_opened = self.block_opened,
            "degrading cache write to UFS-only (Java handleCacheWriteException fall-through)"
        );
        Ok(())
    }

    /// Cancel and drop the in-flight cache block, marking its workers failed.
    async fn tear_down_cache_block(&mut self) {
        if let Some(active) = self.current_block_writer.take() {
            for r in &active.replicas {
                self.router.mark_failed(&r.net_address);
                self.worker_pool.invalidate(&r.worker_addr).await;
            }
            active.cancel_replicas().await;
        }
    }
```

> **设计说明（⑥ 不移植）**：Java 该分支由 `USER_LOCAL_UFS_CLIENT_IGNORE_BLOCK_STREAM_UNKNOWN_STATUS`（默认 `true`）控制，存在的原因是 CLIENT_UFS 模式绕过了 worker 的 capability 鉴权，运维需要一个开关来强制"首块状态不明时不降级"。Rust 走 WORKER_UFS，鉴权仍由 worker 执行，该风险不存在。若后续引入客户端直写 UFS，需回补此开关。

#### 3.2.5 `write()` 重构为「cache 失败可降级 + UFS 兜底」

替换 `src/io/file_writer.rs:431-459`：

```rust
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.cancelled.load(Ordering::SeqCst) || self.closed.load(Ordering::SeqCst) {
            return Err(Error::BlockIoError {
                message: "cannot write to a completed or cancelled file".to_string(),
            });
        }

        if data.is_empty() {
            return Ok(());
        }

        self.ensure_router_init().await?;

        // 1) Cache branch. On failure `handle_cache_write_exception` either
        //    returns `Err` (fatal) or `Ok` after clearing `should_cache`,
        //    in which case we fall through to the UFS branch with the *whole*
        //    buffer — Java `writeInternal` lines 417-444.
        if self.write_strategy.cache_stream && self.should_cache {
            if let Err(e) = self.write_to_cache_stream(data).await {
                self.handle_cache_write_exception(e).await?;
                // Java opens the fallback stream only for ASYNC_THROUGH;
                // CACHE_THROUGH/THROUGH already hold one from construction.
                if self.write_strategy.need_async_persist && self.ufs_stream.is_none() {
                    self.open_ufs_stream().await?;
                }
            }
        }

        // 2) UFS branch. Java gates on `mUnderStorageOutputStream != null`,
        //    which is true both for the eager SYNC_PERSIST stream and for a
        //    stream created by the degrade path above.
        if self.write_strategy.ufs_stream || self.ufs_stream.is_some() {
            self.write_to_ufs_stream(data).await?;
        }

        // 3) Single accounting point, matching Java `mBytesWritten += len`.
        self.total_bytes_written += data.len() as u64;

        Ok(())
    }
```

**关键点**：
- `write_to_cache_stream` 失败后**不再直接返回**，而是交给决策函数；
- 降级后 UFS 拿到的是**完整的 `data`**，不是 cache 未消费的尾部 —— 与 Java `write(b, off, len)` 一致；
- `ufs_stream.is_some()` 让降级后建立的流自动接管后续 `write()`；
- 若降级后 `open_ufs_stream()` 本身失败，`?` 直接上抛（Java 里 `createUnderStorageOutputStream()` 抛 IOException 同样上抛）。

#### 3.2.6 `write_to_cache_stream` 改为透传原始错误

当前它在 `552-554` 自己调用了 `handle_cache_write_exception`，会与 `write()` 的新逻辑重复。

`src/io/file_writer.rs:539-557`：

```rust
            let block_full;
            let emit_result;
            {
                let writer = self.current_block_writer.as_mut().unwrap();
                ...
                emit_result = emit_aligned_chunks(writer, slice, chunk_size).await;
            }
-           if let Err(e) = emit_result {
-               return self.handle_cache_write_exception(e).await;
-           }
+           // Raw error propagates to `write()`, which owns the
+           // degrade-or-fail decision (Java wraps the whole while-loop in
+           // one try/catch — `GooseFSFileOutStream.java:421-443`).
+           emit_result?;
            if block_full {
                self.close_current_block(true).await?;
            }
```

`open_next_block` 的 `?`（`536`）与 `close_current_block` 的 `?`（`556`）无需改动 —— 它们本来就向上透传，正好落进 `write()` 的 `if let Err(e)`，与 Java 把 `getNextBlock()` 放在同一个 try 内一致。

#### 3.2.7 `close()` 消费 `should_cache`

`close()` 在 `1205` 无条件调用 `close_current_block(false)`。降级后 `current_block_writer` 已被 `tear_down_cache_block` 取空，该调用返回 `Ok(None)`，行为正确，**无需改动**。但为可读性建议加注释说明降级后此处必然为 `None`。

---

### 3.3 G8：`flush()` 对齐

Java `flush()` 全文（`GooseFSFileOutStream.java:344-359`）：

```java
public void flush() throws IOException {
  try {
    if (mUnderStorageOutputStream != null) {
      mUnderStorageOutputStream.flush();                    // ① UFS 流优先
    }
    // 异步写场景下同步刷 block 到 worker 盘
    if (mUnderStorageType.isAsyncPersist() && mCurrentBlockOutStream != null
        && conf.getBoolean(USER_FILE_ASYNC_PERSIST_FLUSH_ENABLED)) {
      mCurrentBlockOutStream.flush();                       // ② 仅 ASYNC_PERSIST
    }
  } catch (IOException e) {
    handleUnderStorageWriteException(e);                    // ③ 一律致命，不降级
  }
}
```

与 Rust 现状（`src/io/file_writer.rs:476-499`）比对，有三处不一致：

| 点 | Java | Rust 现状 | 处置 |
|:--|:--|:--|:--|
| ① UFS 流 flush | 有 | **完全没有** | 补上 |
| ② cache 块 flush 条件 | 仅 `isAsyncPersist() && flushEnabled` | `!need_async_persist \|\| flush_enabled` —— MUST_CACHE / CACHE_THROUGH 也会 flush | 对齐到 Java |
| ③ 异常处理 | 一律 `handleUnderStorageWriteException`（`canceled=true` + cancel UFS 流 + rethrow），**flush 路径不存在降级** | 走 `handle_cache_write_exception` | 改为不降级 |

③ 是正确性问题：`flush()` 发生在 cache 块已经开过之后，此时降级会导致 UFS 文件缺失前序 block（§1.4 的同一条论证）。Java 明确不在 flush 路径降级，Rust 必须跟随。

替换整个 `flush()`（`476-499`）：

```rust
    pub async fn flush(&mut self) -> Result<()> {
        if self.cancelled.load(Ordering::SeqCst) || self.closed.load(Ordering::SeqCst) {
            return Err(Error::BlockIoError {
                message: "cannot flush a completed or cancelled file".to_string(),
            });
        }

        // ① Java flushes the UFS stream first — it is the durability
        //    boundary that actually matters for SYNC_PERSIST.
        if let Some(ufs) = self.ufs_stream.as_mut() {
            if let Err(e) = ufs.flush().await {
                return self.handle_ufs_write_exception(e).await;
            }
        }

        // ② Java only pushes the cache block under ASYNC_PERSIST, gated by
        //    `goosefs.user.file.async.persist.flush.enabled`
        //    (`GooseFSFileOutStream.java:352-355`). For MUST_CACHE /
        //    CACHE_THROUGH the block is left to `close_current_block`.
        if self.write_strategy.need_async_persist
            && self.config.file_async_persist_flush_enabled
        {
            if let Some(active) = self.current_block_writer.as_mut() {
                if active.bytes_written > 0 {
                    let tail = std::mem::take(&mut active.pending_chunk);
                    if !tail.is_empty() {
                        if let Err(e) = active.write_chunk(tail).await {
                            // ③ Java routes every flush failure through
                            //    `handleUnderStorageWriteException`: fatal,
                            //    never a degrade. Degrading here would strand
                            //    the already-written blocks outside the UFS file.
                            return self.fail_flush(e).await;
                        }
                    }
                    if let Err(e) = active.flush_replicas().await {
                        return self.fail_flush(e).await;
                    }
                }
            }
        }
        Ok(())
    }

    /// Java `handleUnderStorageWriteException`
    /// (`GooseFSFileOutStream.java:570-577`): mark cancelled, tear down the
    /// UFS stream, and rethrow. Never degrades.
    async fn fail_flush(&mut self, err: Error) -> Result<()> {
        warn!(path = %self.path, error = %err, "flush failed; cancelling write");
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(writer) = self.ufs_stream.take() {
            writer.cancel().await;
        }
        self.ufs_worker_addr = None;
        Err(err)
    }
```

> **行为变更**：MUST_CACHE / CACHE_THROUGH 下 `flush()` 不再推送 cache 块。数据仍会在块写满或 `close()` 时落盘，不丢；但显式 `flush()` 的「立即可见」预期变弱。这是向 Java 对齐的有意变更，需写入 CHANGELOG。若判断风险过高，可保留现有更强的 flush 行为，仅落地 ① 和 ③ —— ③ 是必须的，① 是补缺，② 可延后。

### 3.4 G1：`force_persisted`

#### 3.4.1 `MasterClient` 参数收敛为结构体

`complete_file_with_options` 已有 5 个位置参数，再加一个可读性太差。引入选项结构体。

`src/client/master.rs`，在 `complete_file` 之前插入：

```rust
/// Optional fields of `CompleteFilePOptions` driven by the write path.
///
/// Mirrors the builder calls in Java `GoosefsFileOutStream.close()`
/// (`GooseFSFileOutStream.java:239-305`).
#[derive(Debug, Default, Clone)]
pub struct CompleteFileOptions {
    /// Total file length (`setUfsLength`). Java sets this unconditionally.
    pub ufs_length: Option<i64>,
    /// Idempotency token carried in `commonOptions.operationId`.
    pub operation_id: Option<FsOpPId>,
    /// Last block's replica locations. ASYNC_THROUGH only.
    pub locations: Vec<FileLocation>,
    /// Schedule an async persist job. Mutually exclusive with
    /// `force_persisted` — see `GooseFSFileOutStream.java:284-291`.
    pub async_persist_options: Option<ScheduleAsyncPersistencePOptions>,
    /// Mark the file as already persisted because the client degraded to a
    /// UFS-only write and that write completed. Master then skips async
    /// persist entirely and stamps `PersistenceState.PERSISTED`
    /// (`DefaultFileSystemMaster.java:1649`, `1726-1728`).
    pub force_persisted: Option<bool>,
}
```

改写 `src/client/master.rs:693-726`：

```rust
    pub async fn complete_file_with_options(
        &self,
        path: &str,
        opts: CompleteFileOptions,
    ) -> Result<()> {
        let path = path.to_string();
        self.with_retry("complete_file", |mut client| {
            let path = path.clone();
            let opts = opts.clone();
            async move {
                let common_options = opts.operation_id.map(|op_id| FileSystemMasterCommonPOptions {
                    operation_id: Some(op_id),
                    ..Default::default()
                });
                let req = CompleteFilePRequest {
                    path: Some(path),
                    options: Some(CompleteFilePOptions {
                        ufs_length: opts.ufs_length,
                        common_options,
                        locations: opts.locations,
                        async_persist_options: opts.async_persist_options,
                        force_persisted: opts.force_persisted,
                        ..Default::default()
                    }),
                    inode_id: None,
                };
                client.complete_file(req).await?;
                Ok(())
            }
        })
        .await
    }
```

`complete_file`（`674-682`）转发：

```rust
        self.complete_file_with_options(
            path,
            CompleteFileOptions { ufs_length, operation_id, ..Default::default() },
        )
        .await
```

同步在 `src/client/mod.rs`（或 `master` 的 re-export 处）导出 `CompleteFileOptions`。

#### 3.4.2 `close()` 组装三分支

替换 `src/io/file_writer.rs:1233-1257`：

```rust
        let locations =
            complete_file_locations(self.write_strategy.need_async_persist, last_location);

        // Java `GoosefsFileOutStream.close()` lines 284-301: under ASYNC_PERSIST
        // the three outcomes are mutually exclusive.
        //   a) degraded + UFS closed  → forcePersisted = true, no persist job
        //   b) waitTime != NO_AUTO_PERSIST → schedule async persist
        //   c) waitTime == NO_AUTO_PERSIST → neither; wait for rename/CLI
        let mut force_persisted = None;
        let mut async_persist_options = None;
        if self.write_strategy.need_async_persist {
            if self.ufs_stream_completed.load(Ordering::SeqCst) {
                force_persisted = Some(true);
            } else if self.config.file_persistence_initial_wait_time_ms != NO_AUTO_PERSIST {
                async_persist_options = Some(ScheduleAsyncPersistencePOptions {
                    common_options: None,
                    persistence_wait_time: Some(
                        self.config.file_persistence_initial_wait_time_ms,
                    ),
                });
            }
        }

        if let Err(e) = self
            .master
            .complete_file_with_options(
                &self.path,
                CompleteFileOptions {
                    ufs_length,
                    operation_id: Some(op_id),
                    locations,
                    async_persist_options,
                    force_persisted,
                },
            )
            .await
        {
            return self.handle_complete_file_error(e).await;
        }
```

> `handle_complete_file_error` 的返回类型在 G5 中从 `Error` 改为 `Result<()>`，此处已按新签名书写。

#### 3.4.3 `ufs_stream_completed` 在降级路径的置位

`close()` 的 `1178-1201` 已经在 UFS 流 flush+close 成功后置位 `ufs_stream_completed`。由于降级建立的流同样存放在 `self.ufs_stream`，**该逻辑自动覆盖降级场景，无需改动**。

需要注意 `1221-1225` 的 `ufs_length` 计算：

```rust
let ufs_length = if self.write_strategy.ufs_stream || self.total_bytes_written > 0 {
    Some(self.total_bytes_written as i64)
} else {
    None
};
```

Java 是无条件 `setUfsLength(mBytesWritten)`（`252`）。建议同步简化为 `Some(self.total_bytes_written as i64)`，消除零字节文件时 `ufs_length` 缺省导致 Master 记 `UNKNOWN_SIZE` 的风险。

---

### 3.5 G2：`persistence_wait_time` 与 `NO_AUTO_PERSIST`

#### 3.5.1 新增配置项

`src/config.rs`，在 `file_async_persist_flush_enabled`（`1728-1729`）之后：

```rust
    /// Initial delay before the Master schedules the async persist job for an
    /// ASYNC_THROUGH file, in milliseconds
    /// (`goosefs.user.file.persistence.initial.wait.time`, Java default `0`).
    ///
    /// [`NO_AUTO_PERSIST`] (`-1`) means "never auto-persist" — the client then
    /// omits `asyncPersistOptions` entirely and the file is only persisted by
    /// a later rename or an explicit persist command
    /// (Java `Constants.NO_AUTO_PERSIST`, `GooseFSFileOutStream.java:287`).
    #[serde(default = "default_file_persistence_initial_wait_time_ms")]
    pub file_persistence_initial_wait_time_ms: i64,
```

常量与默认值：

```rust
/// Sentinel for `file_persistence_initial_wait_time_ms` meaning "no automatic
/// persistence". Matches Java `Constants.NO_AUTO_PERSIST`
/// (`core/base/.../Constants.java:207-209`).
pub const NO_AUTO_PERSIST: i64 = -1;

fn default_file_persistence_initial_wait_time_ms() -> i64 {
    // Matches Java ClientPropertyKey.USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME.
    0
}
```

四处接线，与 `file_async_persist_flush_enabled` 完全平行：

| 位置 | 改动 |
|:--|:--|
| `src/config.rs:413` 附近（properties 解析） | `if let Some(n) = self.get_parsed::<i64>("goosefs.user.file.persistence.initial.wait.time") { cfg.file_persistence_initial_wait_time_ms = n; }` —— **注意不能加 `n > 0` 守卫**，`-1` 与 `0` 都是合法值 |
| `src/config.rs:1090` 附近（env 常量） | `pub const ENV_FILE_PERSISTENCE_INITIAL_WAIT_TIME: &str = "GOOSEFS_USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME";` |
| `src/config.rs:2504` 附近（`Default`） | `file_persistence_initial_wait_time_ms: default_file_persistence_initial_wait_time_ms(),` |
| `src/config.rs:3269` 附近（env 覆盖） | 解析 `i64`，同样不加正数守卫 |

再补一个 builder：

```rust
    /// Set [`file_persistence_initial_wait_time_ms`](Self::file_persistence_initial_wait_time_ms).
    ///
    /// Pass [`NO_AUTO_PERSIST`] to disable automatic persistence.
    pub fn with_file_persistence_initial_wait_time_ms(mut self, ms: i64) -> Self {
        self.file_persistence_initial_wait_time_ms = ms;
        self
    }
```

#### 3.5.2 `common_options` 的取舍

Java 会带上 `scheduleAsyncPersistDefaults(conf).commonOptions`，其内容为 `commonDefaults(conf)`（`syncIntervalMs` / `ttl` / `ttlAction`）。Rust 侧目前没有对应的 `FileSystemMasterCommonPOptions` 默认值装配器，而 Master 的 `ScheduleAsyncPersistenceContext` 只消费 `persistenceWaitTime`（`DefaultFileSystemMaster.java:4005-4007`）。

**决定：`common_options` 保持 `None`。** 在 §3.4.2 的代码中已加注释说明，避免被误认为遗漏。若后续 Master 开始消费该字段，再补装配器。

---

### 3.6 G5：`completeFile` 失败后的 UFS 恢复

#### 3.6.1 `MasterClient` 补带选项的 `get_status`

`src/client/master.rs:475-...` 的 `get_status` 把 `GetStatusPOptions::default()` 写死在 `494`。新增一个变体（保留原方法转发）：

```rust
    /// `GetStatus` with an explicit `loadMetadataType` / `syncIntervalMs`.
    ///
    /// # Java authority
    ///
    /// `GoosefsFileOutStream.close()` recovery path
    /// (`GooseFSFileOutStream.java:324-328`) uses
    /// `LoadMetadataPType.ALWAYS` + `syncIntervalMs = 0` to force the Master
    /// to re-read the file from UFS after a `completeFile` failure.
    #[instrument(skip(self), fields(path = %path, ?load_metadata_type, sync_interval_ms))]
    pub async fn get_status_with_load_type(
        &self,
        path: &str,
        load_metadata_type: Option<LoadMetadataPType>,
        sync_interval_ms: Option<i64>,
    ) -> Result<FileInfo> {
        let path = path.to_string();
        let load = load_metadata_type.map(|t| t as i32);
        self.with_retry("get_status", |mut client| {
            let path = path.clone();
            async move {
                let req = GetStatusPRequest {
                    path: Some(path),
                    options: Some(GetStatusPOptions {
                        load_metadata_type: load,
                        common_options: sync_interval_ms.map(|ms| {
                            FileSystemMasterCommonPOptions {
                                sync_interval_ms: Some(ms),
                                ..Default::default()
                            }
                        }),
                        ..Default::default()
                    }),
                    request_id: None,
                };
                client
                    .get_status(req)
                    .await?
                    .into_inner()
                    .file_info
                    .ok_or_else(|| Error::missing_field("file_info"))
            }
        })
        .await
    }
```

#### 3.6.2 重写 `handle_complete_file_error`

**签名变更**：`async fn handle_complete_file_error(&mut self, err: Error) -> Error` → `-> Result<()>`。恢复成功返回 `Ok(())`（Java `return;`），失败返回 `Err(err)`。

替换 `src/io/file_writer.rs:1086-1142`：

```rust
    /// Recover from a `completeFile` failure when the UFS write already
    /// succeeded.
    ///
    /// # Java authority
    ///
    /// `GoosefsFileOutStream.close()` catch block
    /// (`GooseFSFileOutStream.java:315-337`):
    ///
    /// 1. `delete(goosefsOnly = true)` — drop the INCOMPLETE inode but keep
    ///    the UFS file, which is now the source of truth.
    /// 2. `getStatus(LoadMetadataPType.ALWAYS, syncIntervalMs = 0)` — force
    ///    the Master to re-import the file from UFS.
    /// 3. If both succeed, the write is considered **successful** and the
    ///    original error is swallowed.
    ///
    /// Applies to both SYNC_PERSIST (CACHE_THROUGH / THROUGH) and a degraded
    /// ASYNC_THROUGH, matching Java's
    /// `(isSyncPersist() || isAsyncPersist()) && mUnderStorageOutputStreamCompleted`.
    async fn handle_complete_file_error(&mut self, err: Error) -> Result<()> {
        let persistable = self.write_strategy.ufs_stream || self.write_strategy.need_async_persist;
        if !persistable || !self.ufs_stream_completed.load(Ordering::SeqCst) {
            return Err(err);
        }

        warn!(
            path = %self.path,
            error = %err,
            "completeFile failed after UFS close succeeded; attempting UFS metadata recovery"
        );

        if let Err(del_err) = self
            .master
            .delete_with_options(&self.path, DeleteOptions::goosefs_only_unchecked())
            .await
        {
            warn!(
                path = %self.path,
                error = %del_err,
                "recovery step 1/2 (delete goosefs-only) failed; surfacing the original error"
            );
            return Err(err);
        }

        if let Err(reload_err) = self
            .master
            .get_status_with_load_type(
                &self.path,
                Some(crate::proto::grpc::file::LoadMetadataPType::Always),
                Some(0),
            )
            .await
        {
            warn!(
                path = %self.path,
                error = %reload_err,
                "recovery step 2/2 (loadMetadata ALWAYS) failed; surfacing the original error"
            );
            return Err(err);
        }

        warn!(
            path = %self.path,
            error = %err,
            "completeFile failed but the file was recovered from UFS; treating write as successful"
        );
        if let Some(ctx) = &self._context {
            ctx.invalidate_file_info(&self.path);
        }
        Ok(())
    }
```

`close()` 的调用点（`1243-1257`）已在 §3.4.2 改为 `return self.handle_complete_file_error(e).await;`。

> **行为变更提醒**：恢复成功后 `close()` 从「返回错误」变为「返回 Ok」。这与 Java 一致，但对现有调用方是可观察的语义变化，须写入 CHANGELOG。

---

### 3.7 G6：worker 池被过滤空时重置失败列表

#### 3.7.1 `WorkerRouterView` 增加 `clear_failed`

`src/block/router.rs`，在 `mark_failed`（`1534-1541`）之后：

```rust
    /// Drop every entry from this view's failed-worker set.
    ///
    /// # Java authority
    ///
    /// `GooseFSBlockStore.getOutStream` calls `failedWorkers.clear()` when the
    /// candidate pool filters down to empty, so the outer `getNextBlock`
    /// retry re-picks from a pool that is not poisoned by stale failures
    /// (`GooseFSBlockStore.java:333-338`).
    pub fn clear_failed(&self) {
        if let Some(map) = self.failed_workers.get() {
            map.clear();
        }
        self.failed_count.store(0, Ordering::Relaxed);
    }
```

`WorkerRouter` 上同样补一个（保持两个类型的 API 对称）。

#### 3.7.2 `open_replica_writers` 空池分支调用

`src/io/file_writer.rs:668-672`：

```rust
        if pool.is_empty() {
+           // Java resets the failure list here so the caller's retry re-picks
+           // from a clean pool (`GooseFSBlockStore.java:333-338`).
+           debug!(
+               block_id = block_id,
+               "no available GooseFS worker after filtering; clearing failed-worker set for retry"
+           );
+           self.router.clear_failed();
            return Err(Error::NoWorkerAvailable {
                message: format!("no available GooseFS worker for block_id={block_id}"),
            });
        }
```

`open_next_block`（`599-637`）的 `use_all_workers = true` 重试已对应 Java 的 `currentWorkers.set(getAllWorkers())`，无需额外改动。

> **注意**：只在**过滤后为空**这一个分支调用 `clear_failed`。不要在 `worker_count < min_needed`（`711-731`）分支调用 —— Java 那里也没有清，因为那是「打开成功数不足」而非「候选池被过滤空」，语义不同。

---

## 4. 改动清单汇总

| # | 文件 | 位置 | 改动 | 关联 |
|:--|:--|:--|:--|:--|
| 1 | `src/io/file_writer.rs` | `576-587` | 删除 UFS 路径 `total_bytes_written` 累加 | G7 |
| 2 | `src/io/file_writer.rs` | `856-859` | 删除 cache 路径 `total_bytes_written` 累加 | G7 |
| 3 | `src/io/file_writer.rs` | `141-146` | ASYNC_THROUGH 分支预置 `create_ufs_file_options` | G3 |
| 4 | `src/io/file_writer.rs` | `272` 后 | 新增 `block_opened` / `should_cache` 字段 | G3 |
| 5 | `src/io/file_writer.rs` | `356-375`、`2003-2022` | 构造点补两个新字段 | G3 |
| 6 | `src/io/file_writer.rs` | `616-636` | `open_next_block` 成功后置 `block_opened = true` | G3 |
| 7 | `src/io/file_writer.rs` | `927-950` | 重写 `handle_cache_write_exception` + 抽出 `tear_down_cache_block` | G3 |
| 8 | `src/io/file_writer.rs` | `431-459` | 重写 `write()`：降级分支 + 统一记账 | G3/G7 |
| 9 | `src/io/file_writer.rs` | `552-554` | `write_to_cache_stream` 透传原始错误 | G3 |
| 10 | `src/io/file_writer.rs` | `476-499` | 重写 `flush()`：补 UFS flush、收窄 cache flush 条件、失败一律致命（新增 `fail_flush`） | G8 |
| 11 | `src/io/file_writer.rs` | `1221-1225` | `ufs_length` 无条件下发 | G1 |
| 12 | `src/io/file_writer.rs` | `1233-1257` | `close()` 组装 `force_persisted` / `async_persist_options` 三分支 | G1/G2 |
| 13 | `src/io/file_writer.rs` | `1086-1142` | 重写 `handle_complete_file_error`，返回 `Result<()>` | G5 |
| 14 | `src/io/file_writer.rs` | `668-672` | 空池时 `clear_failed()` | G6 |
| 15 | `src/io/file_writer.rs` | `875` 上方 | 补 `pick_any_worker` 已过滤失败 worker 的注释 | G4 |
| 16 | `src/client/master.rs` | `693` 前 | 新增 `CompleteFileOptions` 结构体 | G1 |
| 17 | `src/client/master.rs` | `674-726` | `complete_file{,_with_options}` 改签名 + 下发 `force_persisted` | G1 |
| 18 | `src/client/master.rs` | `475` 后 | 新增 `get_status_with_load_type` | G5 |
| 19 | `src/block/router.rs` | `1541` 后 | `WorkerRouterView::clear_failed`（`WorkerRouter` 同步） | G6 |
| 20 | `src/config.rs` | 多处 | 新增 `file_persistence_initial_wait_time_ms` + `NO_AUTO_PERSIST` | G2 |
| 21 | `src/io/file_writer.rs` | `1909-1917` | 修正 `test_strategy_async_through` 断言 | G3 |

---

## 5. 落地顺序（建议按 PR 拆分）

| PR | 内容 | 依赖 | 可独立合入 |
|:--|:--|:--|:--|
| **PR-1** | G7 记账重构（#1、#2 + `write()` 记账行） | — | ✅ |
| **PR-2** | G2 配置项 `file_persistence_initial_wait_time_ms`（#20） | — | ✅ |
| **PR-3** | G1 `CompleteFileOptions` + `force_persisted` 下发（#16、#17、#11、#12） | PR-2 | ✅（`force_persisted` 暂恒为 `None`） |
| **PR-4** | G6 `clear_failed`（#19、#14） | — | ✅ |
| **PR-5** | G5 恢复路径（#18、#13） | PR-3（签名） | ✅ |
| **PR-6** | G8 `flush()` 对齐（#10） | — | ✅ |
| **PR-7** | **G3 降级主线**（#3~#9、#15、#21） | PR-1、PR-3、PR-6 | ✅ |

PR-7 最后合入：它是唯一会让 `force_persisted` 真正取到 `Some(true)` 的改动，前置 PR 先把管道铺好，能显著降低单个 PR 的 review 面积。

---

## 6. 测试计划

### 6.1 单元测试（`src/io/file_writer.rs` 内 `mod tests`）

| ID | 用例 | 断言 |
|:--|:--|:--|
| T-G3-1 | `resolve_write_strategy(Some(5))` | `create_ufs_file_options.is_some()` 且 `ufs_stream == false` |
| T-G3-2 | `resolve_write_strategy(Some(1))` / `(None)` | `create_ufs_file_options.is_none()`（NO_PERSIST 不可降级） |
| T-G3-3 | `handle_cache_write_exception(ResourceExhausted)`，ASYNC_THROUGH，`block_opened=false` | `Err`，`cancelled == true`，`should_cache` 未被清 |
| T-G3-4 | `handle_cache_write_exception(BlockIoError)`，ASYNC_THROUGH，**`block_opened=true`** | `Err`，`cancelled == true` —— 固化「仅首块可降级」 |
| T-G3-5 | `handle_cache_write_exception(BlockIoError)`，ASYNC_THROUGH，`block_opened=false` | `Ok`，`should_cache == false`，`cancelled == false` |
| T-G3-6 | `handle_cache_write_exception(BlockIoError)`，MUST_CACHE | `Err`，`cancelled == true` |
| T-G3-7 | `handle_cache_write_exception(BlockIoError)`，CACHE_THROUGH，`block_opened=true` | `Ok` —— SYNC_PERSIST 不受 `openBlock` 约束 |
| T-G3-8 | `handle_cache_write_exception(PermissionDenied)`，CACHE_THROUGH | `Err`，`ufs_stream` 被 take 并 cancel |
| T-G1-1 | 纯函数化后的 persist 分支：`need_async_persist=true, ufs_completed=true` | `force_persisted == Some(true)` 且 `async_persist_options.is_none()` |
| T-G1-2 | 同上但 `ufs_completed=false`，`wait_time=0` | `async_persist_options == Some(_ { persistence_wait_time: Some(0) })`，`force_persisted.is_none()` |
| T-G1-3 | 同上但 `wait_time = NO_AUTO_PERSIST` | 两者皆 `None` |
| T-G1-4 | `need_async_persist=false`（CACHE_THROUGH） | 两者皆 `None`（即使 `ufs_completed=true`） |
| T-G6-1 | `WorkerRouterView::clear_failed` | `mark_failed` ×3 后 `clear_failed`，`filter_not_failed` 返回全量 |
| T-G2-1 | 配置解析 | `goosefs.user.file.persistence.initial.wait.time=-1` → `-1`；`=5000` → `5000`；缺省 → `0` |
| T-G8-1 | `flush()` 在 MUST_CACHE / CACHE_THROUGH 下 | 不触碰 `current_block_writer`（`pending_chunk` 保持不变） |
| T-G8-2 | `flush()` 在 ASYNC_THROUGH + `flush_enabled=false` | 同上，不 flush |
| T-G8-3 | `fail_flush` | `cancelled == true`，`ufs_stream` 被 take，返回原始 `Err` |

> T-G1-1~4 需要把 §3.4.2 的三分支逻辑抽成一个纯函数（例如 `fn resolve_persist_options(need_async: bool, ufs_completed: bool, wait_ms: i64) -> (Option<bool>, Option<ScheduleAsyncPersistencePOptions>)`），否则无法脱离真实集群测试。**建议在 PR-3 就抽出来。**

### 6.2 集成测试（`tests/`，需真实集群）

| ID | 场景 | 期望 |
|:--|:--|:--|
| IT-1 | ASYNC_THROUGH，全部 worker `forbid_write=true` | `write()` 成功；`close()` 后 `getStatus` 显示 `persisted == true`；UFS 上文件内容完整 |
| IT-2 | ASYNC_THROUGH，写完第 1 个 block 后手动下线所有 worker | 第 2 个 block 写入失败并抛错，**不产生残缺 UFS 文件** |
| IT-3 | ASYNC_THROUGH，正常路径 | `force_persisted` 未设置；Master 侧异步 persist 任务被排入 |
| IT-4 | `wait_time = -1` 的 ASYNC_THROUGH | Master 不排 persist；`rename` 后才 persist |
| IT-5 | CACHE_THROUGH，UFS 关流成功后注入 `completeFile` 失败 | `close()` 返回 `Ok`；`getStatus` 能读到从 UFS 恢复的文件 |
| IT-6 | 多 block ASYNC_THROUGH 降级后 `ufs_length` | Master 记录的 length == 实际写入字节数 |

### 6.3 回归

- `cargo test --all-features`
- `cargo clippy --all-targets -- -D warnings`
- 现有 `drop_without_close_marks_cancelled` 等 Drop 测试须因新增字段而更新夹具

---

## 7. 风险与缓解

| 风险 | 影响 | 缓解 |
|:--|:--|:--|
| 降级走 WORKER_UFS 而非 Java 的 CLIENT_UFS | 链路多一跳 worker；delegation token 鉴权路径不同 | 在 CHANGELOG 与 README 写明；Rust 侧鉴权仍由 worker 执行，安全性不降反升 |
| `close()` 在恢复成功后不再抛错 | 依赖「close 抛错 = 写失败」的上层逻辑会变 | 属于向 Java 对齐的**有意**变更，写入 CHANGELOG breaking-change 段 |
| `handle_cache_write_exception` 契约反转（`Ok` = 降级） | 误用会静默吞掉致命错误 | 方法文档首段明写契约；调用点仅 2 处，全部在本 PR 内改完；加 `#[must_use]` 无效（返回 `Result` 已被 `must_use` 覆盖） |
| `total_bytes_written` 语义变化 | `bytes_written()` 在 `close()` 前的取值变大 | 该 getter 本就无「仅计已提交」的文档承诺；补充文档说明 |
| 降级后 `open_ufs_stream` 再失败 | 写入整体失败 | 与 Java 一致（`createUnderStorageOutputStream()` 抛 IOException 同样上抛） |
| `clear_failed` 清空过猛 | 刚失败的 worker 立刻被重选 | 仅在候选池被过滤空这一个分支触发，与 Java 完全一致；此时不清空则必然失败，清空至少给一次机会 |
| G8 收窄 `flush()` 的 cache 推送条件 | MUST_CACHE / CACHE_THROUGH 显式 `flush()` 后数据不再立即落 worker 盘 | 数据不丢（块满或 `close()` 时落盘）；若上层强依赖，可只落地 ①③ 保留现有 ② 行为，见 §3.3 末段 |

---

## 8. 附：Java 源码索引

| 主题 | 路径 | 行 |
|:--|:--|:--|
| `GooseFSFileOutStream` 构造（UFS 流建立） | `core/client/fs/src/main/java/com/qcloud/cos/goosefs/client/file/GooseFSFileOutStream.java` | 175-221 |
| `close()` 全文 | 同上 | 234-342 |
| `close()` persist 三分支 | 同上 | 284-301 |
| `close()` UFS 恢复 catch | 同上 | 315-337 |
| `flush()` | 同上 | 344-359 |
| `writeInternal(byte[],int,int)` | 同上 | 409-451 |
| 降级触发点 | 同上 | 435-443 |
| `createUnderStorageOutputStream()` | 同上 | 453-469 |
| `getNextBlock()` / `openBlock = true` | 同上 | 471-500 / 497 |
| `commitCurrentBlock()` | 同上 | 509-530 |
| `handleCacheWriteException()` | 同上 | 532-568 |
| `handleUnderStorageWriteException()` | 同上 | 570-577 |
| `handleFailedWorkers()` | 同上 | 583-596 |
| `getOutStream()` | `core/client/fs/.../client/block/GooseFSBlockStore.java` | 304-405 |
| 空池 `failedWorkers.clear()` | 同上 | 333-338 |
| `filterNoSpaceWorkers()` | 同上 | 221-250 |
| `UnderStorageType` | `core/client/fs/.../client/UnderStorageType.java` | 22-52 |
| `WriteType.getUnderStorageType()` | `core/client/fs/.../client/WriteType.java` | 88-95 |
| `UnderFileSystemFileOutStream` | `core/client/fs/.../client/block/stream/UnderFileSystemFileOutStream.java` | 35-145 |
| `BlockOutStream.executeWithReplication()` | `core/client/fs/.../client/block/stream/BlockOutStream.java` | 167-212 |
| `Constants.NO_AUTO_PERSIST` | `core/base/src/main/java/com/qcloud/cos/goosefs/Constants.java` | 207-209 |
| `OutStreamOptions.getPersistenceWaitTime()` | `core/client/fs/.../client/file/options/OutStreamOptions.java` | 139 / 221-223 |
| `scheduleAsyncPersistDefaults()` | `core/client/fs/.../util/FileSystemOptions.java` | 291-296 |
| `USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME` | `core/common/.../conf/ClientPropertyKey.java` | 387-389 |
| `USER_LOCAL_WRITE_UFS_CLIENT_ENABLED` | 同上 | 36-42 |
| `USER_LOCAL_UFS_CLIENT_IGNORE_BLOCK_STREAM_UNKNOWN_STATUS` | 同上 | 1255-1261 |
| Master 消费 `forcePersisted` | `core/server/master/.../master/file/DefaultFileSystemMaster.java` | 1649 / 1726-1728 |
| Master 消费 `asyncPersistOptions` | 同上 | 1575-1577 / 3992-4010 |
