# Release Guide

本文档描述如何打包并发布 `goosefs-client-rs` 到 Cargo 仓库（crates.io 或腾讯内部 Cargo Registry）。

## 前置条件

确保已安装以下工具：

```bash
# Rust 工具链
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 确认 cargo 可用
cargo --version
```

## 发布前检查

在发布之前，请确保完成以下检查：

```bash
# 1. 运行测试
cargo test

# 2. 检查文档是否有警告
cargo doc --no-deps

# 3. 检查打包内容（不实际上传）
cargo publish --dry-run

# 4. 查看将要打包的文件列表
cargo package --list
```

---

## 方案一：发布到官方 crates.io

### 上传命令

```bash
cargo publish --token <your-crates-io-token>
```

### 安装验证

```bash
cargo add goosefs-client-rs
```

### 项目地址

- https://crates.io/crates/goosefs-client-rs

---

## 方案二：发布到腾讯内部 Cargo Registry

### 配置 Registry

在 `~/.cargo/config.toml` 中添加：

```toml
[registries.tencent]
index = "TODO: 腾讯内部 Cargo Registry 地址"
```

### 上传命令

```bash
cargo publish --registry tencent --token <your-token>
```

### 安装验证

在项目的 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
goosefs-client-rs = { version = "0.1", registry = "tencent" }
```

或通过 `.cargo/config.toml` 全局配置默认 registry 后直接使用：

```toml
[dependencies]
goosefs-client-rs = "0.1"
```

---

## 参数说明

| 参数 | 说明 |
|------|------|
| `--token` | 访问令牌（crates.io 在 https://crates.io/settings/tokens 创建） |
| `--registry` | 目标 registry 名称（省略则默认为 crates.io） |
| `--dry-run` | 仅模拟发布，不实际上传 |
| `--allow-dirty` | 允许在有未提交更改时发布（不推荐） |

## 完整发布流程

```bash
# 1. 确认版本号已更新（Cargo.toml 中的 version 字段）
grep '^version' Cargo.toml

# 2. 确认所有测试通过
cargo test

# 3. 确认文档无警告
cargo doc --no-deps

# 4. 模拟发布，检查打包内容
cargo publish --dry-run

# 5a. 发布到官方 crates.io
cargo publish --token <your-crates-io-token>

# 5b. 或发布到腾讯内部 registry
cargo publish --registry tencent --token <your-token>

# 6. 创建 Git Tag
git tag v0.1.0
git push origin v0.1.0
```

## 注意事项

1. 发布新版本前，务必更新 `Cargo.toml` 中的 `version` 字段
2. crates.io **不允许删除或覆盖已发布的版本**，只能 yank（标记为不推荐）
3. 发布到 crates.io 后，crate 包内的源码将公开可见（即使 Git 仓库是私有的）
4. 建议在发布前运行 `cargo publish --dry-run` 验证
5. 妥善保管 Token，切勿提交到代码仓库
6. crates.io Token 可在 https://crates.io/settings/tokens 创建和管理
7. 如果不希望源码公开，请使用腾讯内部 Cargo Registry（方案二）
