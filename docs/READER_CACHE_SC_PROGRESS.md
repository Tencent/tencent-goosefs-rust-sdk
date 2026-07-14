# GoosefsFileReader: page-cache + short-circuit 接入进度

> 依据设计文档 `goosefs-lance-tests/docs/design/GOOSEFS_SDK_0.1.6_UPGRADE_AND_CACHE_DESIGN.md`
> §7（page cache 内置到 `read_next_block`）、§8.2（short-circuit 同构接入）、§9（一致性硬门）。
> 分支：`feature/reader-page-cache-short-circuit`（从 `0.1.6` 切出）。

## 目标 / Sprint 契约（硬 pass/fail）

1. `GoosefsFileReader` 的 `read_next_block` 在 `client_cache_enabled=true` 时经
   `read_through_cache` 取数；`false` 时走 `read_file_range` 直读，**字节级等价**于旧实现。
2. 公开 API 签名一字不改（`open_with_context` / `open_range_with_context` /
   `read_next_block` / `read_all` / `read_*_with_context` 返回类型不变）。
3. HR-1：`file_id <= 0` 在 `GoosefsFileReader::attach_cache` **与**
   `GoosefsFileInStream::open_with_context` 两处均禁用缓存（置于 `on_file_open` 之前）。
4. HR-3：miss 回源复用旧 `try_read_block`（`GrpcBlockReader::open + read_all`），
   与非 cache 路径同一 server verb；`read_segment` 完整保留双层容错。
5. §8.2：short-circuit 注入 `GoosefsFileReader` 的按块读取收口点（`read_segment`），
   失败透明 fallback 到 gRPC。
6. `cargo build` / `cargo clippy` / `cargo test`（离线单测）全绿。

## TODO

- [x] T1 分支创建 + 进度文件
- [x] T2 HR-1：`GoosefsFileInStream::open_with_context` 加 `file_id<=0` 降级
- [x] T3 `file_reader.rs`：imports + 结构体加 cache/SC 字段
- [x] T4 `file_reader.rs`：`build()` 填默认值 + `attach_cache()` + 两个 opener 调用
- [x] T5 `file_reader.rs`：`worker_addr` / `block_logical_size` / `read_segment`（双层容错 + SC）/ `read_file_range` / 重写 `read_next_block`
- [x] T6 `file_reader.rs`：`impl ExternalRangeReader`
- [x] T7 单测：`block_logical_size` / `worker_addr` / build 默认值；`cargo build`+`clippy`+`test`（359 passed）
- [x] T8 门禁回归测试：`tests/reader_page_cache_consistency.rs`（§9.4 HR-3 `disabled==cold==warm`，`#[ignore]` + 集群环境变量，与 FileInStream 套件同构）
- [x] T9 复核 + 文档收尾
- [x] T10 HR-4 并发冷读回归（同页并发填充安全）→ 集群实测通过
- [x] T11 示例 `examples/reader_page_cache_demo.rs`（演示 opendal 热路径命中缓存）→ 集群实测通过
- [x] T12 HR-2 立场文档化（见下）
- [x] T13 reader short-circuit 端到端测试（§8.2 SC 经 reader 命中 + SC==gRPC 字节等价）→ 集群实测通过
- [x] T14 测试整合：reader 用例并入既有文件（`page_cache_consistency.rs::reader_consistency` / `short_circuit_e2e.rs::reader_sc`），删除独立文件

## 验证结果

- `cargo check --lib` / `cargo clippy --lib --tests`：通过，修改文件零警告
- `cargo test --lib`：**359 passed, 0 failed**（新增 4 个 file_reader 纯逻辑单测）
- **真实集群实测（127.0.0.1:9200, SIMPLE 认证）**：
  - `page_cache_consistency`（`consistency::` FileInStream 4 + `reader_consistency::` reader 4，含 HR-3/HR-4）：**8/8 PASS**
  - `short_circuit_e2e`（`e2e::` FileInStream 5 + `reader_sc::` reader 2，§8.2 + SC==gRPC）：**7/7 PASS**
  - 既有回归 `sc_consistency` 5/5：全绿（证明 HR-1 改动无回归）
  - `examples/reader_page_cache_demo`：冷读 ΔreadExternal=+512KiB/ΔwrittenCache=+512KiB；**热读 ΔreadCache=+512KiB / ΔreadExternal=0（100% 命中）**

## 测试文件布局（整合后）

- `tests/page_cache_consistency.rs`：`mod consistency`（FileInStream）+ `mod reader_consistency`（GoosefsFileReader）
- `tests/short_circuit_e2e.rs`：`mod e2e`（FileInStream）+ `mod reader_sc`（GoosefsFileReader）
- 运行 reader 用例：
  ```bash
  GOOSEFS_AUTH_TYPE=simple cargo test --test page_cache_consistency reader_consistency:: -- --ignored --test-threads=1
  GOOSEFS_AUTH_TYPE=simple cargo test --test short_circuit_e2e reader_sc:: -- --ignored --test-threads=1
  ```

## 门禁测试运行方式（需真实 GooseFS 集群）

```bash
# page cache（含 reader 用例）
GOOSEFS_AUTH_TYPE=simple cargo test --test page_cache_consistency -- --ignored --nocapture --test-threads=1
# short-circuit（含 reader 用例）
GOOSEFS_AUTH_TYPE=simple cargo test --test short_circuit_e2e -- --ignored --nocapture --test-threads=1
# 示例：
GOOSEFS_AUTH_TYPE=simple cargo run --example reader_page_cache_demo
```

覆盖：`disabled==cold==warm` 字节等价（page/chunk/block/EOF 边界）、whole-file 冷热等价、多段错位 range 重组等价、warm 命中 canary、**并发冷读同页填充安全（HR-4）**、§8.2 SC 经 reader 命中 + SC==gRPC 等价。

## HR-2（overwrite / 旁路写 stale-read）立场

按设计 §9.3，HR-2 采分层缓解，且**不在本次 reader 接入的代码范围内**，理由：

- **L1（已具备）**：每次 open 经 `on_file_open(file_id, length, mtime)` 做版本校验失效；`GoosefsFileReader::attach_cache` 已复用此机制，overwrite（length/mtime 变化）自动失效旧页。集群 `page_cache_e2e::overwrite_invalidates_stale_pages` 已覆盖并通过。
- **L2（ufs_fingerprint 身份）**：需把 `CacheManager::on_file_open` 的身份从 `(length, mtime)` 扩展为 `(length, mtime, ufs_fingerprint)`，属 `cache/manager.rs` 的 trait 级改动，跨越"reader 接入"范畴，另行立项。
- **L3（保守 TTL）**：`client_cache_ttl_secs` 已是全局 config 且 manager 已实现懒过期；reader 侧不应擅自覆盖全局 TTL（会造成意外行为），故作为**运营配置建议**：旁路写场景下部署方应设有限 TTL 收敛 stale 窗口。
- **L4（manifest NoCache）**：见 §9.8 OW-1，依赖 opendal 透传读意图，当前无实现路径，不阻断本次合并。

因此 HR-2 在 reader 侧**无需额外代码**，以文档形式明确边界与运营建议。

## 说明

- 端到端 cache 命中/字节等价验收依赖真实 GooseFS 集群（在 `goosefs-lance-tests` /
  lance-io fork 侧执行），本仓库内以离线单测 + 编译 + clippy 为门禁。
- opendal-service-goosefs 侧 **0 改动**（本仓库不含该代码）。
