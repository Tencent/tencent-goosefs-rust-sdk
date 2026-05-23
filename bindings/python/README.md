# GooseFS Python Client

[![PyPI](https://img.shields.io/pypi/v/goosefs.svg)](https://pypi.org/project/goosefs/)
[![Python Versions](https://img.shields.io/pypi/pyversions/goosefs.svg)](https://pypi.org/project/goosefs/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

`goosefs` 是 [腾讯云 GooseFS](https://cloud.tencent.com/product/goosefs) 的官方 Python 客户端，底层由 Rust 原生实现（[`goosefs-sdk`](https://crates.io/crates/goosefs-sdk) crate）通过 [PyO3](https://pyo3.rs/) 桥接到 Python。

| | |
| --- | --- |
| **包名 / Import 名** | `goosefs`（PyPI 包名与 import 名完全一致） |
| **Python 支持** | abi3 wheel，运行时下限 CPython 3.9+ |
| **平台** | Linux x86_64 / aarch64（manylinux_2_28），macOS x86_64 / arm64；Windows best-effort |
| **API 风格** | 同步阻塞（`Goosefs`）+ 协程异步（`AsyncGoosefs`） |
| **当前状态** | alpha — 详见 [开发路线图](../../docs/PYTHON_BINDING_PROGRESS.md) |

## 文档导航

- **PyPI 用户首屏文档**：[`PYPI_README.md`](./PYPI_README.md) — 安装、quickstart、线程/fork 安全、类型存根
- **可运行的 5 个示例**：[`examples/`](./examples/)
  - [`01_quickstart.py`](./examples/01_quickstart.py) — 同步一页流：connect → mkdir → write → read → delete
  - [`02_async.py`](./examples/02_async.py) — `AsyncGoosefs` + `asyncio.gather` 并发扇出
  - [`03_streaming.py`](./examples/03_streaming.py) — 流式 reader/writer：分块读写 + seek + read_at
  - [`04_with_pyarrow.py`](./examples/04_with_pyarrow.py) — Arrow Table → Parquet → GooseFS 往返
  - [`05_pandas_csv.py`](./examples/05_pandas_csv.py) — pandas DataFrame ↔ CSV 往返
- **开发指引（构建、测试、release）**：[`DEVELOPMENT.md`](./DEVELOPMENT.md)
- **变更日志**：[`CHANGELOG.md`](./CHANGELOG.md)
- **完整开发跟踪**：[`docs/PYTHON_BINDING_PROGRESS.md`](../../docs/PYTHON_BINDING_PROGRESS.md)
- **完整 API 类型存根（含 docstring）**：[`python/goosefs/__init__.pyi`](./python/goosefs/__init__.pyi)

## 一分钟试用

```bash
pip install goosefs
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
```

```python
from goosefs import Config, Goosefs

with Goosefs(Config("127.0.0.1:9200")) as fs:
    fs.mkdir("/hello", recursive=True)
    fs.write_file("/hello/world.txt", b"hi")
    assert fs.read_file("/hello/world.txt") == b"hi"
```

更多场景请直接跑 `examples/` 里的脚本，每个文件都自包含、可独立运行。

## 安装变体

| 安装命令 | 拉取的额外依赖 | 适用场景 |
| --- | --- | --- |
| `pip install goosefs` | 仅 binding 本身 | 只需要 GooseFS 核心 API |
| `pip install 'goosefs[arrow]'` | + `pyarrow` | 配合 Arrow / Parquet |
| `pip install 'goosefs[pandas]'` | + `pandas` + `pyarrow` | DataFrame 工作流 |
| `pip install 'goosefs[examples]'` | + `pyarrow` + `pandas` | 跑全部 5 个示例脚本 |

## 启用日志

binding 默认**不**安装任何 `tracing` 订阅器，把日志控制权完全交给宿主程序。需要排查问题时显式启用：

```python
import goosefs
goosefs.enable_tracing(level="debug")
```

详见 [`PYPI_README.md`](./PYPI_README.md) 与 [`__init__.pyi`](./python/goosefs/__init__.pyi) 中的 `enable_tracing` docstring。`RUST_LOG` 环境变量（如果已设置）会覆盖 `level` 参数。

## 反馈

* Bug / 功能请求：请在仓库 issue 列表中提单（标签 `python-binding`）
* 内部用户请联系 GooseFS 团队，我们会优先处理
* 如果你愿意贡献代码，请先读 [`DEVELOPMENT.md`](./DEVELOPMENT.md)

## License

Apache License 2.0。详见仓库根目录 `LICENSE` 文件。
