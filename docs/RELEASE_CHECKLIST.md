# 发布 Checklist

`goosefs-sdk` 发布前需要完成的事项清单。

## 一、Crate 元数据

| 事项 | 当前状态 | 操作 |
|------|---------|------|
| 包名 | `goosefs-sdk` | ✅ 最终包名已确认为 `goosefs-sdk` |
| 版本号 | `0.1.0` | ✅ 首次发布保持 `0.1.0` |
| description | 已有 | ✅ |
| license | `Apache-2.0` | ✅ |
| authors | `Goosefs Team` | ✅ |
| repository | 缺失 | TODO: 填写仓库地址（如需公开） |
| homepage | 缺失 | TODO: 填写项目主页 |
| keywords | 缺失 | TODO: 建议 `["goosefs", "grpc", "storage", "distributed-filesystem", "cache"]` |
| categories | 缺失 | TODO: 建议 `["network-programming", "filesystem"]` |
| readme | 缺失 | TODO: 添加 `readme = "README.md"` |

## 二、文件检查

- [ ] `README.md` 内容完整，包含基本用法示例
- [ ] `LICENSE` 文件存在（Apache-2.0）
- [ ] `.gitignore` 中排除了不需要的文件
- [ ] 检查 `cargo package --list` 输出，确认打包文件合理

## 三、API 审查

| 模块 | 当前可见性 | 建议 | 决策 |
|------|-----------|------|------|
| `auth` | `pub` | 保持公开 | TODO |
| `block` | `pub` | 考虑是否需要公开（底层 API） | TODO |
| `client` | `pub` | 保持公开（Low-Level gRPC 客户端） | TODO |
| `config` | `pub` | 保持公开 | TODO |
| `error` | `pub` | 保持公开 | TODO |
| `io` | `pub` | 保持公开（推荐的高层 API 入口） | TODO |
| `retry` | `pub` | 考虑是否需要公开（内部实现） | TODO |
| `proto` | `pub` | 考虑标注为不稳定 / `#[doc(hidden)]` | TODO |

## 四、Proto 生成代码

- [ ] 确认 `src/generated/` 下的 protobuf 代码是否适合直接发布
- [ ] 决定 `proto` 模块的公开策略：
  - **方案 A**：保持 `pub mod proto`，文档标注为"高级用法 / 不保证稳定"
  - **方案 B**：改为 `pub(crate) mod proto`，仅通过高层 API 暴露
- [ ] 确认 `.proto` 源文件是否需要包含在 crate 包中（通过 `Cargo.toml` 的 `include`/`exclude` 控制）

## 五、文档

- [ ] `lib.rs` 顶层文档（crate-level doc）
- [ ] 核心类型的 doc comment
- [ ] 运行 `cargo doc --no-deps` 确认无警告
- [ ] 示例代码可编译通过（`cargo test --doc`）

## 六、质量保证

- [ ] `cargo test` 全部通过
- [ ] `cargo clippy` 无警告
- [ ] `cargo fmt --check` 格式检查通过
- [ ] `cargo publish --dry-run` 模拟发布成功

## 七、CI/CD

- [ ] TODO: 配置 CI 流水线（GitHub Actions / 工蜂流水线）
- [ ] TODO: 配置 `CARGO_REGISTRY_TOKEN` Secret
- [ ] TODO: 配置 release tag 触发自动发布（可选）

## 八、发布执行

```bash
# 1. 最终确认
cargo test
cargo publish --dry-run

# 2. 创建 Git Tag
git tag v0.1.0
git push origin v0.1.0

# 3. 发布
cargo publish --token <token>

# 4. 验证
# 等待几分钟后
cargo add goosefs-sdk  # 或最终确定的包名
```

## 九、发布后

- [ ] 验证 crate 可以正常安装和使用
- [ ] TODO: 通知相关团队/用户
- [ ] 更新内部文档中的依赖说明
