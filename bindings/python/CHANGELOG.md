# Changelog

本文档记录 `goosefs` Python binding 的所有重要变更。格式遵循 [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)，版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

> 注意：版本号与底层 `goosefs-sdk` crate 保持一致。在 P8（CI/Wheel）落地前，本目录的 wheel 仅在内部发布；公开 PyPI 发布从 0.1.0 起算。

## [Unreleased]

### Added

- 暂无

### Changed

- 暂无

### Fixed

- 暂无

---

## [0.1.2] — 2026-05-23

首个内部可用 alpha 版本。覆盖 P0–P7 全部里程碑。

### Added

#### 核心 API

- **`Config`**：构造客户端配置，支持单点 / HA 多 master、`properties` 字典、`from_properties_file`，以及 12 个常用字段 getter（`master_addr` / `master_addrs` / `block_size` / `chunk_size` / `root` / `use_vpc_mapping` / `auth_type` / `auth_username` / `metrics_enabled` / `connect_timeout_ms` / `request_timeout_ms` / `write_type`）。
- **`AsyncGoosefs`** —— 协程异步客户端：
  - 元数据：`get_status` / `list_status` / `exists` / `mkdir` / `delete` / `delete_with_options` / `rename`
  - 一次性读写：`read_file` / `read_range` / `write_file`
  - 流式工厂：`open_file` / `create_file`
  - 生命周期：`close()` / `__aenter__` / `__aexit__` / 静态工厂 `connect(config)`
- **`Goosefs`** —— 阻塞同步客户端，与 `AsyncGoosefs` API 等价，外加：
  - tokio runtime / asyncio loop **死锁防护**（Review §17.1）：在 tokio worker 或 asyncio 事件循环内调用同步方法将抛 `RuntimeError`，而不是死锁
  - **fork 安全护栏**（Review §17.4）：记录 `creator_pid`，子进程调用拒绝复用，避免共享 socket
- **流式文件句柄**：
  - `AsyncFileReader` / `FileReader`：`read(n=-1)` / `read_at(offset, length)` / `seek(offset, whence=0)` / `tell()` / `close()` / `__len__`（文件长度）
  - `AsyncFileWriter` / `FileWriter`：`write(data)` / `close()` / `cancel()`（with 块异常时自动 cancel 而非 close）
  - 全部支持 `with` / `async with` 上下文管理
- **类型 / 选项**：
  - `WriteType` 枚举：`MustCache` / `CacheThrough` / `Through` / `AsyncThrough` / `TryCache`，`from_str()` 大小写不敏感
  - `ReadType` 枚举：`Cache` / `NoCache`
  - `OpenFileOptions` / `CreateFileOptions` / `DeleteOptions`：构造可重用配置传给底层 SDK
  - `URIStatus`：25 个字段的元数据快照，含 `is_readable()` / `is_completed()` / `is_folder()` / `is_persisted()` / `block_count()` 谓词
- **异常体系**：14 个具名异常子类（`GoosefsError` 基类 + `NotFound` / `AlreadyExists` / `PermissionDenied` / `Unauthorized` / `InvalidArgument` / `IoError` / `Network` / `Timeout` / `Cancelled` / `Unavailable` / `ConfigError` / `Unimplemented` / `Internal`），与 SDK `error::Error` 16 个变体的全量映射（无 `_` 兜底分支）。
- **模块级辅助**：
  - `goosefs.enable_tracing(level="info", *, target="stderr")` ——（Review §17.7）opt-in `tracing` 桥接到 stderr，幂等、`RUST_LOG` 优先；reserve `target="logging"` / `"stdout"` 给未来 minor。
  - `goosefs.__version__` —— 与底层 SDK 版本对齐
  - `goosefs.exceptions` 子模块 —— 自动注入 `sys.modules`，支持 `from goosefs.exceptions import NotFound`

#### 包装与约束

- **`bytes-like` 输入校验**（P4）：`write_file` / `FileWriter.write` 接受 `bytes` / `bytearray` / `memoryview` / `array.array("B", …)` / NumPy `uint8` 等所有 buffer-protocol 输入；**显式拒绝 `str`** 抛 `TypeError`，避免 Latin-1 隐式解码。
- **atexit 兜底清理**（Review §17.4）：`Goosefs` 与 `AsyncGoosefs` 通过 `weakref.WeakSet` 自动跟踪；解释器退出时
  - 同步未关闭句柄 → 静默 `close()`（幂等，已关闭即 no-op）
  - 异步未关闭句柄 → `ResourceWarning`（atexit 阶段无法 `await close()`，让用户感知到泄漏）

#### 文档与示例

- 5 个可运行示例：`01_quickstart.py` / `02_async.py` / `03_streaming.py` / `04_with_pyarrow.py` / `05_pandas_csv.py`
- 完整类型存根（PEP 561）：`python/goosefs/__init__.pyi`（541 行）+ `python/goosefs/exceptions.pyi`（105 行）+ `py.typed` marker
- `mypy.stubtest` 在 CI 强校验存根与运行时签名一致
- 文档：`README.md` / `PYPI_README.md` / `DEVELOPMENT.md` / `CHANGELOG.md`（本文件）

#### 包装 / 安装

- abi3 wheel，运行时下限 CPython 3.9
- 平台支持：Linux x86_64 / aarch64（manylinux_2_28），macOS x86_64 / arm64；Windows best-effort
- 可选依赖：`goosefs[arrow]`（pyarrow）、`goosefs[pandas]`（pandas + pyarrow）、`goosefs[examples]`（pyarrow + pandas）

### Changed

- **底层 SDK 升级**：`goosefs-sdk` 0.1.2 同步上游 `proto` 文件，修复 `WorkerInfo.sync_cache_rate_limit` wire-type 不匹配问题。
- **SDK 短读丢字节根治**（P5.5-A）：`GoosefsFileInStream::read` 引入 `carry_over: BytesMut`，oversized chunk 溢出字节 park 入 `carry_over`，`pos()` / `remaining()` / `is_eof()` 改为"用户视角"。binding 端不再需要 `ReaderState` workaround（P5.5-B 已移除）。
- **SDK `tokio::io::AsyncRead + AsyncSeek` 适配器**（P5.5-C）：新增 `GoosefsAsyncReader`，下游（opendal / JNI / C bindings）可直接用 `tokio::io::copy` / `BufReader` 等生态工具。

### Fixed

- 修复异步 `connect` 在 PyO3 0.27 上 `Python::detach`/`allow_threads` 切换造成的 spurious panic（P3）。
- 修复 `enable_tracing` 在 second-call 路径上跳过参数校验导致错误参数被静默吞掉的问题（test 捕获）。

### Security

- 暂无相关变更。

---

## 注

- 0.1.0 / 0.1.1 在 P0–P5 开发期间作为内部里程碑使用，未公开发布；本文档从 0.1.2（首个 P7 完成的可发版本）开始记录。
- 完整的开发节奏与每个阶段的交付产出请见 [`docs/PYTHON_BINDING_PROGRESS.md`](../../docs/PYTHON_BINDING_PROGRESS.md)。
- 与 PyPI 发布相关的版本号变化（包括 0.1.0 公开发布）将在 P9（灰度 + 回归）阶段补登。
