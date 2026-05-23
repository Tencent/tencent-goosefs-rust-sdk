# 开发指引

`goosefs` Python binding 的本地构建、测试与发布流程。

## 前置工具

| 工具 | 推荐版本 | 用途 |
| --- | --- | --- |
| **Rust** | 1.86+ stable | 通过 `rustup` 安装，cargo 即随附 |
| **Python** | 3.9+ | 推荐 3.10/3.11/3.12，与 CI 矩阵保持一致 |
| **uv** | 0.5+ | 项目锁定的 Python 包管理器；不要混用 pip / poetry |
| **maturin** | 1.5+ | 通过 `dev` 依赖组拉取，无需手动安装 |

仓库根目录已有 `rust-toolchain.toml` 锁定 Rust 版本；本目录通过 `pyproject.toml` 的 `[dependency-groups]` 锁定 Python 工具链。

## 一次性环境准备

```bash
cd bindings/python
uv sync --all-extras --group dev --group test
```

这一步会：
* 创建 `.venv/`
* 安装运行时依赖
* 安装 dev 工具（`maturin` / `ruff` / `mypy`）
* 安装测试依赖（`pytest` / `pytest-asyncio` / `pytest-timeout` / `mypy.stubtest` 用的 `mypy>=1.19.1`）
* 安装 examples 依赖（`pyarrow` / `pandas`，因为 `--all-extras` 包含 `[examples]`）

## 本地开发循环

### 1. 编辑 + 构建

```bash
# 改完 Rust 后
uv run maturin develop --uv
```

`maturin develop` 会编译 cdylib、把 abi3 wheel 安装为 editable，5–10 秒一轮。

### 2. 跑全量测试（需要活集群）

```bash
# 启动 GooseFS 集群（参考仓库根 README）
# 然后：
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
uv run --group test pytest -q
```

预期结果：**125 passed**（截至 P7）。

如果 `GOOSEFS_MASTER_ADDR` 未设置，conftest 会在 collection 阶段跳过依赖集群的用例，只跑 `test_errors.py`（异常层级）+ `test_tracing.py`（参数校验），约 13 个用例。

### 3. 跑 lint / 类型检查 / stub 一致性

四件套，CI 会强制：

```bash
# Rust
cargo clippy --all-targets -- -D warnings
cargo test --lib

# Python
uv run ruff check python/goosefs tests examples
uv run ruff format --check python/goosefs tests examples
uv run --group test mypy python/goosefs
uv run --group test python -m mypy.stubtest goosefs
```

### 4. 手动跑 5 个示例（活集群）

```bash
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
for f in examples/0*.py; do echo "==> $f"; uv run python "$f" || break; done
```

## 项目结构速览

```
bindings/python/
├── Cargo.toml          # cdylib + rlib，pyo3 0.27，依赖 ../../goosefs-sdk
├── pyproject.toml      # maturin 配置 + uv 依赖组 + ruff/mypy/pytest 配置
├── PYPI_README.md      # PyPI 长描述（用户视角）
├── README.md           # 本目录入口（开发者 + 文档导航）
├── DEVELOPMENT.md      # 本文档
├── CHANGELOG.md        # 版本变更记录
│
├── src/                # PyO3 wrapper Rust 代码
│   ├── lib.rs          # #[pymodule] 入口，注册所有 pyclass
│   ├── config.rs       # Config
│   ├── context.rs      # 内部 PyFsHandle
│   ├── errors.rs       # 14 个 Exception 子类 + map_err
│   ├── filesystem.rs   # AsyncGoosefs
│   ├── sync_fs.rs      # Goosefs（同步包装）
│   ├── streaming.rs    # 4 个文件句柄类
│   ├── status.rs       # URIStatus
│   ├── options.rs      # Open/Create/Delete Options
│   ├── types.rs        # WriteType / ReadType
│   ├── runtime.rs      # 共享 tokio runtime
│   └── tracing.rs      # enable_tracing
│
├── python/goosefs/
│   ├── __init__.py     # 重新导出 + atexit 兜底 + sys.modules 注入
│   ├── __init__.pyi    # 类型存根（541 行，stubtest 校验）
│   ├── exceptions.pyi  # 14 个异常子类的存根
│   └── py.typed        # PEP 561 marker
│
├── examples/           # 5 个可运行示例
└── tests/              # pytest 套件（含 conftest.py 控制集群门控）
```

## Stub 维护

* 修了 Rust 端 pyclass / pyfunction → **必须**手动同步 `python/goosefs/__init__.pyi`
* 修了运行时 Exception 子类 → 同步 `python/goosefs/exceptions.pyi`
* `mypy.stubtest goosefs` 会强校验 stub 与实际签名一致
* 不引入 `pyo3-stub-gen`：当前 PyO3 0.27 的生成器对我们用的 `#[pyclass(eq, eq_int, frozen, hash)]` / `#[pyo3(get)]` 支持不完整；手写让我们能精确表达 "Awaitable[T]" 与 "不跨任务/线程共享" 等约束。

## 调试技巧

```python
import goosefs
goosefs.enable_tracing(level="debug")  # SDK + binding 走 stderr
```

也可以走 `RUST_LOG`，会覆盖 `enable_tracing` 的 `level` 参数：

```bash
RUST_LOG="debug,h2=warn,hyper=warn" python my_script.py
```

## 性能基线

性能 benchmark 留在 P9（灰度 + 回归）阶段，预计放在 `bench/` 目录：read/write 时延、吞吐与 Java SDK 对照，以及元数据 RPS。

## 发布流程（P8 落地后补）

详细发布流程将在 P8 引入 GitHub Actions / 公司流水线后补充。届时会：
1. 由 `goosefs-sdk` 与 `goosefs-python` 的 `Cargo.toml` 版本对齐 check
2. 5 个 target wheel（manylinux x86_64/aarch64、macOS x86_64/arm64、Windows x86_64）
3. PyPI Trusted Publisher（OIDC）发布
4. 内部 PyPI 镜像同步

在那之前，本地手工发布请先咨询 GooseFS 团队。
