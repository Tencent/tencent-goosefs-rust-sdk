# GooseFS Rust Client 多 Master 地址支持 — 实现计划

## 一、背景分析

### Java 端当前实现

GooseFS Java 客户端处理多 Master 地址的核心架构：

```java
// 截图中的关键代码片段
InetSocketAddress address = mFsContext.getMasterAddress();
List<InetSocketAddress> addresses = Arrays.asList(address);
PollingMasterInquireClient inquireClient = new PollingMasterInquireClient(
    addresses,
    () -> new ExponentialBackoffRetry(50, 100, 2),
    mFsContext.getClusterConf()
);
InetSocketAddress primaryAddr = inquireClient.getPrimaryRpcAddress();
```

| 组件 | 功能 |
|------|------|
| `MasterInquireClient` 接口 | 统一的 Master 发现抽象（`getPrimaryRpcAddress()`, `getMasterRpcAddresses()`） |
| `SingleMasterInquireClient` | 单地址直连，`getPrimaryRpcAddress()` 直接返回固定地址 |
| `PollingMasterInquireClient` | 多地址轮询，遍历所有地址 ping `getServiceVersion` RPC 找到 Primary |
| `ExponentialTimeBoundedRetry` | 时间限制的指数退避重试（默认最长 2 分钟，sleep 50ms→3s） |
| `MasterSelectionPolicy` | Primary 地址缓存 + 重置机制，支持 failover |

**核心原理**：只有 Primary Master 会响应 `getServiceVersion` RPC，逐个 ping 所有地址，成功的就是 Leader。

### Rust 端当前状态

| 特性 | 状态 |
|------|------|
| Master 地址配置 | ⚠️ 仅支持单地址 `String` (`master_addr`) |
| Leader 发现 | ❌ 未实现 |
| 连接重试 | ❌ 未实现（仅定义了 `is_retriable()` 但未使用） |
| Master 故障转移 | ❌ 未实现 |
| `getServiceVersion` proto | ✅ 已有 `version.proto` 定义 |

---

## 二、实现计划

### Phase 1: 配置层改造 — 支持多地址

**修改文件**: `src/config.rs`

1. **新增字段** `master_addrs: Vec<String>` — 多 Master 地址列表
2. **保持向后兼容**：`master_addr` 保留为首选单地址（内部转为 `master_addrs` 的第一个元素）
3. **新增构造方法** `GooseFsConfig::new_ha(addrs: Vec<String>)` — HA 模式创建
4. **新增重试配置字段**：
   - `master_inquire_retry_max_duration: Duration` — 最大重试总时长（默认 2 分钟）
   - `master_inquire_initial_sleep: Duration` — 初始退避时间（默认 50ms）
   - `master_inquire_max_sleep: Duration` — 最大退避时间（默认 3s）
   - `master_polling_timeout: Duration` — 单次 ping 超时（默认 30s）
5. **新增辅助方法** `is_ha_mode() -> bool` — 判断是否 HA 模式

### Phase 2: 重试策略 — 指数退避

**新增文件**: `src/retry.rs`

1. **`RetryPolicy` trait**：
   ```rust
   pub trait RetryPolicy {
       fn attempt(&mut self) -> bool;  // 是否可继续重试
       fn attempt_count(&self) -> u32;
   }
   ```

2. **`ExponentialTimeBoundedRetry`** — 对标 Java `ExponentialTimeBoundedRetry`：
   - 参数：`max_duration`、`initial_sleep`、`max_sleep`
   - 退避公式：`next_sleep = min(next_sleep * 2, max_sleep) + jitter(0~10%)`
   - 到达 `max_duration` 前做最后一次尝试

3. **`ExponentialBackoffRetry`** — 对标 Java 基于次数的重试：
   - 参数：`base_sleep_ms`、`max_sleep_ms`、`max_retries`

### Phase 3: Master 发现客户端

**新增文件**: `src/client/master_inquire.rs`

1. **`MasterInquireClient` trait**：
   ```rust
   #[async_trait]
   pub trait MasterInquireClient: Send + Sync {
       async fn get_primary_rpc_address(&self) -> Result<String>;
       fn get_master_rpc_addresses(&self) -> Vec<String>;
   }
   ```

2. **`SingleMasterInquireClient`**：
   - 单地址直连，`get_primary_rpc_address()` 直接返回固定地址
   - 零开销（无网络调用）

3. **`PollingMasterInquireClient`** — 核心实现：
   - 持有 `Vec<String>` 地址列表 + `RetryPolicy` 供应器
   - `get_primary_rpc_address()` 流程：
     ```
     创建 ExponentialTimeBoundedRetry
     while retry.attempt():
         for addr in addresses:
             ping_meta_service(addr)
             if 成功 → 返回 addr (Primary)
             if NotFound → standby, 跳过
             if Unavailable / Timeout → 跳过
             if 其他错误 → break 本轮
         sleep 退避时间
     抛出 UnavailableException
     ```
   - `ping_meta_service(addr)` — 通过已有 `ServiceVersionClientService` gRPC 调用 `getServiceVersion(META_MASTER_CLIENT_SERVICE)` 验证 Primary

4. **工厂方法** `MasterInquireClient::create(config) -> Box<dyn MasterInquireClient>`：
   - `addrs.len() == 1` → `SingleMasterInquireClient`
   - `addrs.len() > 1` → `PollingMasterInquireClient`

### Phase 4: 改造现有 Client 使用 MasterInquireClient

**修改文件**: `src/client/master.rs`, `src/client/worker_manager.rs`

1. **`MasterClient::connect`** 改造：
   - 不再直接使用 `config.master_endpoint()`
   - 调用 `MasterInquireClient::get_primary_rpc_address()` 获取 Leader 地址
   - 用 Leader 地址建立 gRPC Channel

2. **`WorkerManagerClient::connect`** 同样改造

3. **新增** Primary 地址缓存（对标 `AbstractMasterSelectionPolicy`）：
   - 缓存上次获取的 Primary 地址，避免每次操作都轮询
   - 当 RPC 失败且 `is_retriable()` 时，重置缓存并重新发现 Leader

### Phase 5: RPC 级别重试 + Failover

**修改文件**: `src/client/master.rs`, `src/io/file_reader.rs`, `src/io/file_writer.rs`

1. **MasterClient RPC 重试**：
   - 包装每个 RPC 方法（`get_status`、`list_status` 等）添加重试逻辑
   - 当遇到 `Unavailable` / `DeadlineExceeded` 时：
     - 重置 Primary 缓存
     - 重新获取 Leader 地址
     - 重建 gRPC Channel
     - 重试 RPC

2. **Worker 连接 failover**（已有 `mark_failed` 基础，增强为自动重试）：
   - `file_reader.rs` 的 `read_next_block()`：Worker 连接失败时，mark_failed + 重新 select_worker + 重试
   - `file_writer.rs` 同理

### Phase 6: 依赖与模块注册

**修改文件**: `Cargo.toml`, `src/lib.rs`

1. **新增依赖**:
   - `async-trait` — 异步 trait 支持（用于 `MasterInquireClient` trait）
   - `rand` — 随机抖动（jitter）
2. **注册模块**: `src/lib.rs` 中新增 `pub mod retry;`
3. **`src/client/mod.rs`** 新增导出

### Phase 7: 测试与示例

1. **单元测试**（`src/retry.rs`）：
   - `ExponentialTimeBoundedRetry` 退避时间正确性
   - `ExponentialBackoffRetry` 次数限制正确性
   - Jitter 在预期范围内

2. **单元测试**（`src/client/master_inquire.rs`）：
   - `SingleMasterInquireClient` 直接返回
   - `PollingMasterInquireClient` 多地址场景（mock gRPC）

3. **集成测试/示例**：
   - 新增 `examples/ha_multi_master.rs` — 演示多 Master 地址配置和自动 failover

---

## 三、文件变更清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `Cargo.toml` | 修改 | 添加 `async-trait`、`rand` 依赖 |
| `src/lib.rs` | 修改 | 新增 `pub mod retry;` |
| `src/config.rs` | 修改 | 新增 `master_addrs`、重试参数、`is_ha_mode()` |
| `src/retry.rs` | **新增** | `RetryPolicy` trait + 两种指数退避实现 |
| `src/client/master_inquire.rs` | **新增** | `MasterInquireClient` trait + Single/Polling 实现 |
| `src/client/mod.rs` | 修改 | 导出 `master_inquire` 模块 |
| `src/client/master.rs` | 修改 | 使用 `MasterInquireClient` 获取 Leader + RPC 重试 |
| `src/client/worker_manager.rs` | 修改 | 使用 `MasterInquireClient` 获取 Leader |
| `src/io/file_reader.rs` | 修改 | Worker 连接失败自动重试 |
| `src/io/file_writer.rs` | 修改 | Worker 连接失败自动重试 |
| `src/error.rs` | 修改 | 可能新增 `MasterUnavailable` 变体 |
| `examples/ha_multi_master.rs` | **新增** | HA 多 Master 示例 |

---

## 四、Java ↔ Rust 对照关系

| Java 类 | Rust 对应 |
|---------|-----------|
| `MasterInquireClient` 接口 | `MasterInquireClient` trait |
| `SingleMasterInquireClient` | `SingleMasterInquireClient` struct |
| `PollingMasterInquireClient` | `PollingMasterInquireClient` struct |
| `ExponentialTimeBoundedRetry` | `retry::ExponentialTimeBoundedRetry` |
| `ExponentialBackoffRetry` | `retry::ExponentialBackoffRetry` |
| `RetryPolicy` 接口 | `retry::RetryPolicy` trait |
| `FileSystemContext.getMasterAddress()` | `MasterClient::connect()` 内部调用 inquire |
| `AbstractMasterSelectionPolicy.mPrimaryMasterAddress` | 在 `MasterClient` 中缓存 Primary 地址 |

---

## 五、实现优先级建议

| 优先级 | Phase | 预计工作量 | 说明 |
|--------|-------|-----------|------|
| **P0** | Phase 1 (配置) | 小 | 基础设施，后续都依赖 |
| **P0** | Phase 2 (重试策略) | 小 | 通用基础设施 |
| **P0** | Phase 3 (Master 发现) | 中 | 核心功能 |
| **P1** | Phase 4 (Client 改造) | 中 | 集成到现有代码 |
| **P1** | Phase 5 (RPC 重试) | 中 | 提升鲁棒性 |
| **P2** | Phase 6 (依赖/模块) | 小 | 工程化 |
| **P2** | Phase 7 (测试) | 中 | 质量保障 |

---

## 六、备注

- 本计划**不包含 ZooKeeper 模式**（`ZkMasterInquireClient`），Rust 客户端暂不需要 ZK 支持。如后续需要可单独扩展。
- 所有超时和重试参数均提供合理默认值，同时支持通过 `GooseFsConfig` 自定义。
