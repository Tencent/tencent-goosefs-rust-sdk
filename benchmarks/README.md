# Benchmarks 使用说明

本目录是 GooseFS Rust SDK 的**基准 / 压测 / 复现**用例。可运行型基准以 `example` 目标声明（见 `Cargo.toml`），默认连本机 Master `127.0.0.1:9200`，运行前请确保本地集群已启动（`lsof -i:9200` 能看到 Master/Worker 在监听）。

> 入门用的功能性 API 示例（`highlevel_file_rw`、`context_file_rw`、`streaming_file_read`、`metadata_crud`、`auth_demo` 等）在 [`../examples`](../examples) 目录。

```bash
# 可运行基准 / 复现（example 目标）
cargo run --release --example <name>

# criterion 统计型微基准（master_hotpath）
cargo bench --bench master_hotpath
```

| 基准 | 类型 | 说明 |
|------|------|------|
| `partv_perf_verify` | `--example` | **Part V 优化端到端验证 + 压测**（随机读 / 顺序读 / Master 池 / Worker 池），含逐字节正确性校验。**本文档重点。** |
| `pr_runtime_ab` | `--example` | **B1 根因 A/B**：同一 `read_at`、同文件、同 offset，仅"驱动方式"不同（spawn / block_on / current_thread / per-call block_on），定位单线程随机读吞吐差异 |
| `repro_writer_write_with_concurrent` | `--example` | 并发写复现用例 |
| `master_hotpath` | `--bench`（criterion）| GetFileStatus 热路径统计型微基准（`ArcSwap` / counter 缓存 / path move 优化对照）|

---

## `partv_perf_verify` — 性能验证 / 压测

一次运行覆盖四块，并对**随机读 + 全文件顺序读做逐字节校验**（守一致性红线 C1 字节精确 / C2 绝不静默短读）：

| 步骤 | 验证的优化点 | 输出行 |
|------|-------------|-------|
| `[1]` 并发随机读（PR） | **R2** `read_at` 单 block 快路径 + **Worker 多 channel 池** | `... reads, ... MiB in ...s → ... MiB/s` + `✅ byte-for-byte verification passed` |
| `[2]` 顺序读（SR） | **R1-B** prefetch / 后台 drain / ACK | `64KiB-buf scan: ...` 和 `read_all : ...` |
| `[3]` Master 元数据吞吐 | **R3** Master 多 channel 池 | `pool=1 ...` vs `pool=N ...` + `Δ ...%` |

### 参数（全部用环境变量覆盖）

| 环境变量 | 默认 | 含义 |
|----------|------|------|
| `GFS_ADDR` | `127.0.0.1:9200` | Master 地址（远程集群改这里） |
| `GFS_SIZE_MB` | `128` | 测试文件大小（MiB）。**注意全缓存**：MUST_CACHE 不落 UFS，文件须 ≤ Worker 缓存容量，否则 block 被淘汰会读失败 |
| `GFS_IO_KB` | `1024` | 单次 `read_at` / `read` 的 IO 大小（KiB）。1MB=1024、8MB=8192、16MB=16384 |
| `GFS_CONC` | `16` | 并发 reader 数（PR 与 SR 共用） |
| `GFS_READS` | `8` | 每个 PR reader 发起的随机读次数（总随机读 = `CONC × READS`） |
| `GFS_POOL` | `8` | **Master** 连接池大小（`master_connection_pool_size`，对应 R3） |
| `GFS_WPOOL` | `1` | **每个 Worker** 的 channel 池大小（`worker_connection_pool_size`，Worker 侧多 channel） |
| `GFS_META_OPS` | `20000` | `[3]` 步 `get_status` 总次数 |
| `GFS_META_CONC` | `256` | `[3]` 步元数据并发度 |
| `GFS_TAG` | （空） | 测试文件路径后缀，多进程并行各用独立文件（如 `GFS_TAG=p1` → `/partv-bench/data-p1.bin`） |

> 务必用 `--release` 跑（LTO=fat 已开），否则数字不具代表性。

---

## 配方：验证 Worker 多 channel 池（`GFS_WPOOL` sweep）

这正是「单进程加 worker channel 能否把随机读吞吐从 ~1.6 GiB/s 抬到 ~4 GiB/s」的验证场景。
思路：**固定文件全缓存、固定并发，只扫 `GFS_WPOOL = 1/2/4/8`**，对比 PR 随机读吞吐。

```bash
cd /opt/sourcecode/cos/goosefs-client-rust

for W in 1 2 4 8; do
  ( GFS_TAG=w$W \
    GFS_SIZE_MB=256 GFS_IO_KB=8192 GFS_CONC=32 GFS_READS=16 \
    GFS_POOL=4 GFS_WPOOL=$W \
    GFS_META_OPS=1 GFS_META_CONC=4 \
    ./target/release/examples/partv_perf_verify > /tmp/wp_$W.log 2>&1 &
    echo $! > /tmp/wp.pid )
  # watchdog：最多等 90s，挂死则杀
  P=$(cat /tmp/wp.pid)
  for i in $(seq 1 90); do kill -0 $P 2>/dev/null || break; sleep 1; done
  kill -9 $P 2>/dev/null
  printf 'WPOOL=%s\n' "$W"
  grep -E 'reads,|read_all|64KiB-buf' /tmp/wp_$W.log
done
```

> 先 `cargo build --release --example partv_perf_verify` 一次，循环里直接跑二进制，避免每轮重复编译。

参数为什么这么设：

- `GFS_IO_KB=8192`（8MB 大 IO）+ `GFS_CONC=32`：制造足够并发把单 channel 打满，差异才明显。
- `GFS_SIZE_MB=256`：单文件全缓存，避免淘汰干扰（多进程并行时每进程再调小）。
- `GFS_META_OPS=1`：本配方只看读吞吐，元数据步走个过场即可。
- `GFS_TAG=w$W`：每个 WPOOL 用独立文件，互不干扰。

预期结论：**PR 随机读随 WPOOL 增大显著上升**（单 channel → 多 channel 解除单连接瓶颈）；**SR 顺序读基本持平**（单流任一时刻只在一个 block 上推进，天然串行，跨不了 channel）。

---

## 配方：不同 IO 大小（8MB / 16MB）

```bash
GFS_SIZE_MB=256 GFS_IO_KB=8192  cargo run --release --example partv_perf_verify   # 8MB IO
GFS_SIZE_MB=256 GFS_IO_KB=16384 cargo run --release --example partv_perf_verify   # 16MB IO
```

## 配方：Master 池对比（R3）

```bash
# [3] 步会自动跑 pool=1 vs pool=GFS_POOL 并打印 Δ%
GFS_POOL=8 GFS_META_OPS=20000 GFS_META_CONC=256 cargo run --release --example partv_perf_verify
```

## 配方：多进程聚合极限（探本机天花板）

单进程受单 worker channel 限制时，用多进程各自独立 channel 叠加，找聚合上限：

```bash
for i in 1 2 3 4; do
  ( GFS_TAG=q$i GFS_SIZE_MB=128 GFS_IO_KB=8192 GFS_CONC=24 GFS_READS=20 \
    GFS_WPOOL=2 GFS_META_OPS=1 \
    ./target/release/examples/partv_perf_verify > /tmp/agg_$i.log 2>&1 ) &
done
wait
grep -h 'reads,' /tmp/agg_*.log | sed -E 's/.*→ ([0-9]+) MiB.*/\1/' \
  | awk '{s+=$1} END{printf "聚合 PR ≈ %d MiB/s = %.2f GiB/s\n", s, s/1024}'
```

---

## 常见问题

- **读时报 block 不存在 / 缓存淘汰**：文件太大超过 Worker 缓存（MUST_CACHE 默认不落 UFS）。调小 `GFS_SIZE_MB`，或多进程并行时减小每进程文件。
- **进程挂住不退**：用上面 watchdog 循环兜底；正常多 chunk 顺序读不会挂（早期 ACK 合并死锁已修复，默认每 chunk ACK）。
- **localhost 数字偏低**：本机 loopback 走完整 gRPC 数据面（编码→TCP→解码→拷贝），不是 FUSE short-circuit；远程集群 R3/Worker 池在 RTT 下收益更大。
- **远程压测**：`GFS_ADDR=<remote-master>:9200` 即可，其余参数照旧。

---

## `pr_runtime_ab` — B1 调用方驱动粒度 A/B

定位"单线程随机读 Python 比 Rust 快"的根因。同一份 SDK `read_at`、同一文件、同一 XorShift offset 序列，**只改驱动方式**，对每种打印 per-op 延迟分布：

| 模式 | 含义 |
|------|------|
| `multi_thread + spawn` | 把读循环 spawn 到多线程池（等价 `partv_perf_verify` CONC=1）|
| `multi_thread + block_on` | 多线程 runtime，整循环一个 `block_on` 驱动 |
| `current_thread + block_on` | 当前线程 runtime，整循环一个 `block_on` |
| `py-style mt + block_on` | 照搬 Python 绑定的 runtime 配置（worker=max(cpu,16)+max_blocking=64）|
| `per-call block_on` | **每次 `read_at` 一个独立 `block_on`**（持久 stream）——复刻 Python sync 模型 |
| `buffer_unordered(16) [REC]` | **推荐写法**：有界并发 + 每 reader 持久 stream 复用（验证调用方正确姿势）|

```bash
GFS_SIZE_MB=128 GFS_IO_KB=1024 GFS_READS=1000 GFS_TAG=shared \
  cargo run --release --example pr_runtime_ab
```

结论（已验证）：前 4 种 ~1200µs/op（~520 MiB/s），`per-call block_on` ~580µs/op（与 Python sync 一致），`buffer_unordered(16)` **1901 MiB/s 聚合**（推荐写法，达并超单 stream floor）→ 差异来自**调用方驱动粒度**，非 SDK。详见 [`docs/RUST_PYTHON_SDK_OPTIMIZATION.md`](../docs/RUST_PYTHON_SDK_OPTIMIZATION.md) §V.5「B1 验证结果」。环境变量：`GFS_ADDR` / `GFS_SIZE_MB` / `GFS_IO_KB` / `GFS_READS` / `GFS_TAG`。

---

## `master_hotpath` — GetFileStatus 热路径微基准（criterion）

统计型微基准，对照 Master 元数据热路径优化（`ArcSwap<AuthedState>` / counter 缓存为字段 / path move）。用 criterion harness（`harness = false`）：

```bash
cargo bench --bench master_hotpath
```

---

## `repro_writer_write_with_concurrent` — 并发写复现

```bash
cargo run --release --example repro_writer_write_with_concurrent
```
