# Client Page Cache — io_uring 客户端开发设计文档

> 状态：**实现中** · 分支：`feature/reader-page-cache-short-circuit`
> 日期：2026-07-08
> 前置文档：
> - [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md) — 现有 `tokio::fs` 后端的完整设计
> - [`SHORT_CIRCUIT_IO_URING_FEASIBILITY.md`](SHORT_CIRCUIT_IO_URING_FEASIBILITY.md) — SC 路径的 io_uring 可行性分析
> - [`perf/2026-07-08-oncpu3-cache-hotspots/CACHE_VS_NOCACHE_ANALYSIS.md`](perf/2026-07-08-oncpu3-cache-hotspots/CACHE_VS_NOCACHE_ANALYSIS.md) — 火焰图根因分析
> 参考实现：
> - `/opt/sourcecode/lance/rust/lance-io/src/uring/` — Lance 的 io_uring 实现 (thread pool + Future waker 模式)

---

## 1. 背景与动机

### 1.1 问题：`tokio::fs` 的 `spawn_blocking` 把 cache 命中路径打成 300 QPS

当前 `LocalPageStore`（`src/cache/store/local.rs:212-242`）使用 `tokio::fs::File` 实现文件读写。`tokio::fs` 的每个操作（`open` / `seek` / `read`）都内部调用 `spawn_blocking`，把同步 syscall 丢到 tokio 的 blocking pool。

火焰图证据（`clientcache_oncpu_3.svg`，300 QPS）：

| 函数 | 占比 |
|---|---|
| `tokio::runtime::blocking::pool::Inner::run` | **22.44%** |
| `LocalCacheManager::get` | 4.77% |
| `LocalPageStore::get` | 3.64% |
| `tokio::fs::File::poll_read` | 1.00% |
| `tokio::fs::File::start_seek` | 0.60% |
| `spawn_blocking`（3 处） | ~2.6% |

每次 cache 命中 = 3 次 `spawn_blocking`（open + seek + read），调度开销 ~50-100 µs/次，是 NVMe 实际 IO 时间（~10 µs）的 5-10 倍。

### 1.2 为什么不用 D1（合并 `spawn_blocking`）/ D6（专用 IO 线程池）

D1 把 3 次合并为 1 次，仍残留 1 次 `spawn_blocking`（~50-100 µs）。D6 自建专用 OS 线程池，本质仍是"同步 IO + 线程池"——每次 IO 仍需线程切换 + channel 通信。

### 1.3 io_uring 的优势

| 维度 | `spawn_blocking` / D1 / D6 | `io_uring` |
|---|---|---|
| IO 模型 | 同步 syscall + 线程池 | 真正异步 SQE/CQE |
| 线程切换 | 每次操作 1-3 次 | **0 次**（waker 直接唤醒） |
| syscall 次数/cache hit | 2-3 次 (open + pread) | **1 次** (batch 提交) |
| 调度开销 | ~50-300 µs | **~1-5 µs** |
| 批处理 | 不支持 | **支持** (一次 `submit()` 提交多个 SQE) |

### 1.4 Lance io_uring 参考实现

Lance 在 `rust/lance-io/src/uring/` 实现了成熟的 io_uring 文件读取，核心设计：

| Lance 文件 | 职责 | GooseFS 对标 |
|---|---|---|
| `uring.rs` | 模块入口 + 配置常量 | `src/cache/store/uring/mod.rs` |
| `requests.rs` | `IoRequest` + `RequestState`（共享状态 + waker） | `src/cache/store/uring/requests.rs` |
| `thread.rs` | 后台线程池 + 主循环（SQE 提交 + CQE 收割） | `src/cache/store/uring/driver.rs` |
| `future.rs` | `UringReadFuture`（实现 `Future` trait，poll 查 `RequestState`） | `src/cache/store/uring/future.rs` |
| `reader.rs` | `UringReader`（实现 `Reader` trait，open + fd 缓存 + submit_read） | `src/cache/store/uring/store.rs` |

**Lance 的关键设计决策**（我们采纳）：
1. **后台线程池模式**：N 个专用 OS 线程，每个持有一个 `IoUring` 实例，通过 `std::sync::mpsc::sync_channel` 接收请求
2. **`Arc<IoRequest>` + `Mutex<RequestState>` 共享状态**：提交者构造 request → channel 发送 → 后台线程提交 SQE → CQE 完成时设置 `completed = true` + `waker.wake()`
3. **自定义 `Future`**：`UringReadFuture` 实现 `Future` trait，`poll` 时检查 `RequestState.completed`，未完成则存 `waker`
4. **fd 缓存**：`UringFileHandle` 用 `moka::future::Cache` 按 `(path, block_size)` 缓存已打开的文件句柄，避免重复 `open`
5. **short read 重试**：CQE 返回部分读时，调整 `offset` + `bytes_read` 后重新 push SQE
6. **攒批提交**：从 channel 非阻塞收取多个请求，攒到 `submit_batch_size` 或 channel 空时统一 `ring.submit()`

**Lance 的局限性**（我们改进）：
- Lance 只实现了**读**（`get_range` / `get_all`），没有写路径。我们需要 `put`（tmp + rename）和 `delete`
- Lance 的 fd 缓存用 `moka`，我们首版简化为每次 open（io_uring 的 `OP_OPENAT` 也是异步的，开销远小于 `spawn_blocking`）
- Lance 用 `std::sync::mpsc::sync_channel`（单消费者），我们需要多线程 round-robin 选择

---

## 2. 整体架构

```text
                    LocalCacheManager (src/cache/manager.rs, 改动最小)
                          │
                   ┌──────┴──────┐
                   │             │
            PageStore trait   (meta/evictor/lock 不变)
            (src/cache/store/mod.rs)
                   │
          ┌────────┴─────────────────────┐
          │                              │
   LocalPageStore                  UringPageStore     ← 新增
   (src/cache/store/local.rs)      (src/cache/store/uring/store.rs)
   - tokio::fs 后端                 - io_uring 后端
   - 保留作为 fallback             - 后台线程池 (driver.rs)
   - 非 Linux 使用                 - 自定义 Future (future.rs)
   - config off 时使用             - 攒批提交
```

### 2.1 模块布局

```text
src/cache/store/
  ├── mod.rs                    # PageStore trait (不变)
  ├── local.rs                  # LocalPageStore (tokio::fs 后端, 保留)
  └── uring/                    # 新增 io_uring 后端
      ├── mod.rs                # UringPageStore + 模块声明
      ├── store.rs              # UringPageStore 实现 PageStore trait
      ├── requests.rs           # IoRequest + RequestState (共享状态 + waker)
      ├── driver.rs              # UringDriver — 后台线程池 + 主循环
      ├── future.rs             # UringReadFuture / UringWriteFuture
      └── sys.rs                # 平台检测 + io_uring 可用性探测 + 降级
```

---

## 3. 核心组件设计

### 3.1 `RequestState` / `IoRequest` — 共享状态与 waker

参考 Lance `requests.rs:13-54`，但扩展支持写操作。

```rust
// src/cache/store/uring/requests.rs

use bytes::BytesMut;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::Mutex;
use std::task::Waker;

/// IO 操作完成后的共享状态。
///
/// 提交者（async 线程）构造 `IoRequest`，通过 channel 发送到后台线程；
/// 后台线程提交 SQE，CQE 完成时更新此状态并 `waker.wake()`；
/// 提交者通过 `UringReadFuture::poll` 检查 `completed`。
///
/// 参考: Lance `requests.rs:13-20` 的 `RequestState`
pub struct RequestState {
    /// 操作是否完成（CQE 已收割）
    pub completed: bool,
    /// tokio 的 waker，CQE 完成时调用 `wake()` 唤醒等待的 async 任务
    pub waker: Option<Waker>,
    /// 错误（如果有）。CQE result < 0 时设置
    pub err: Option<io::Error>,
    /// 读操作: 返回的缓冲区（写入操作为空）
    pub buffer: BytesMut,
    /// 累积已读字节数（处理 short read 重试）
    pub bytes_read: usize,
}

/// 单个 IO 操作的描述，在提交者 → 后台线程 → Future 之间共享。
///
/// 参考: Lance `requests.rs:24-38` 的 `IoRequest`
pub struct IoRequest {
    /// 文件描述符（调用者负责 open + close）
    pub fd: RawFd,
    /// 读/写偏移
    pub offset: u64,
    /// 读/写长度
    pub length: usize,
    /// 操作类型（读/写/open/close/unlink/rename）
    pub op_type: UringOpType,
    /// 共享状态
    pub state: Mutex<RequestState>,
}

/// io_uring 操作类型
pub enum UringOpType {
    Read,
    Write,
    OpenAt,
    Close,
    UnlinkAt,
    RenameAt,
}

impl IoRequest {
    /// 标记失败并唤醒等待者。
    /// 参考: Lance `requests.rs:45-53` 的 `fail()`
    pub fn fail(&self, err: io::Error) {
        let mut state = self.state.lock().unwrap();
        state.err = Some(err);
        state.completed = true;
        if let Some(waker) = state.waker.take() {
            drop(state);
            waker.wake();
        }
    }
}
```

### 3.2 `UringDriver` — 后台线程池 + 主循环

参考 Lance `thread.rs:30-250`，保留多线程 + round-robin + 攒批提交的核心设计。

```rust
// src/cache/store/uring/driver.rs

use super::requests::{IoRequest, RequestState, UringOpType};
use io_uring::{IoUring, opcode, types};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

/// 后台线程 handle — 持有 channel sender 用于提交请求。
///
/// 参考: Lance `thread.rs:23-25` 的 `UringThreadHandle`
struct UringThreadHandle {
    request_tx: SyncSender<Arc<IoRequest>>,
}

/// 全局 io_uring 线程池 — 进程级单例，首次访问时懒初始化。
///
/// 参考: Lance `thread.rs:30-54` 的 `URING_THREADS: LazyLock<Vec<UringThreadHandle>>`
pub static URING_THREADS: LazyLock<Vec<UringThreadHandle>> = LazyLock::new(|| {
    let queue_depth = get_queue_depth();       // 默认 16384
    let thread_count = get_thread_count();      // 默认 2

    let mut threads = Vec::with_capacity(thread_count);
    for i in 0..thread_count {
        let (tx, rx) = sync_channel(queue_depth);
        std::thread::Builder::new()
            .name(format!("gfs-uring-{}", i))
            .spawn(move || run_uring_thread(rx, queue_depth as u32, i))
            .expect("Failed to spawn io_uring thread");
        threads.push(UringThreadHandle { request_tx: tx });
    }
    tracing::info!(
        thread_count,
        queue_depth,
        "io_uring thread pool initialized for page cache"
    );
    threads
});

/// Round-robin 线程选择计数器。
/// 参考: Lance `thread.rs:57` 的 `THREAD_SELECTOR: AtomicU64`
static THREAD_SELECTOR: AtomicU64 = AtomicU64::new(0);

/// user_data 生成器 — 每个 SQE 分配唯一 ID 用于 CQE 匹配。
/// 参考: Lance `thread.rs:63` 的 `USER_DATA_COUNTER: AtomicU64`
static USER_DATA_COUNTER: AtomicU64 = AtomicU64::new(1);

/// 提交一个 IO 请求到后台线程池，返回 `Arc<IoRequest>` 供 Future 持有。
///
/// 参考: Lance `reader.rs:183-238` 的 `submit_read()`
pub fn submit_request(request: Arc<IoRequest>) {
    let thread_idx = (THREAD_SELECTOR.fetch_add(1, Ordering::Relaxed) as usize)
        % URING_THREADS.len();
    match URING_THREADS[thread_idx].request_tx.send(Arc::clone(&request)) {
        Ok(()) => {}
        Err(_) => {
            request.fail(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring thread died",
            ));
        }
    }
}

/// 后台线程主循环。
///
/// 参考: Lance `thread.rs:117-250` 的 `run_uring_thread()`
///
/// 循环逻辑:
/// 1. 先收割所有可用 CQE（process_completions）
/// 2. 从 channel 攒批收取请求（try_recv + recv_timeout）
/// 3. 为每个请求构造 SQE push 到 SQ ring（push_to_sq）
/// 4. 统一 ring.submit() 提交到内核
fn run_uring_thread(request_rx: Receiver<Arc<IoRequest>>, queue_depth: u32, thread_id: usize) {
    let mut ring = IoUring::builder()
        .build(queue_depth)
        .expect("Failed to create io_uring");

    // user_data → IoRequest 映射表
    let mut pending: HashMap<u64, Arc<IoRequest>> = HashMap::with_capacity(queue_depth as usize);
    let poll_timeout = Duration::from_millis(10);
    let submit_batch_size = 128usize;
    let mut last_log = Instant::now();

    loop {
        // 1) 收割 CQE — 设置 completed + wake
        process_completions(&mut ring, &mut pending);

        // 2) 攒批从 channel 收取请求
        let mut batch_count = 0usize;
        loop {
            let recv_result = if pending.is_empty() && batch_count == 0 {
                // 无在途请求且无批次 — 可等待
                request_rx.recv_timeout(poll_timeout).map_err(|e| match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => {
                        std::sync::mpsc::TryRecvError::Empty
                    }
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        std::sync::mpsc::TryRecvError::Disconnected
                    }
                })
            } else {
                request_rx.try_recv()
            };

            match recv_result {
                Ok(request) => {
                    if let Err(e) = push_to_sq(&mut ring, &mut pending, request) {
                        tracing::error!(error = %e, "Failed to push to io_uring SQ");
                    } else {
                        batch_count += 1;
                    }
                    if batch_count >= submit_batch_size {
                        break;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    if batch_count > 0 {
                        let _ = ring.submit();
                    }
                    return;
                }
            }
        }

        // 3) 提交批次到内核
        if batch_count > 0 {
            if let Err(e) = ring.submit() {
                tracing::error!(error = %e, batch_count, "Failed to submit io_uring batch");
            }
        }
    }
}

/// 构造 SQE 并 push 到 submission queue（不提交）。
///
/// 根据 `request.op_type` 构造不同的 opcode:
/// - Read → `opcode::Read` (pread, 一次 syscall 定位+读取)
/// - Write → `opcode::Write`
/// - OpenAt → `opcode::OpenAt`
/// - Close → `opcode::Close`
/// - UnlinkAt → `opcode::UnlinkAt`
/// - RenameAt → `opcode::RenameAt`
///
/// 参考: Lance `thread.rs:256-309` 的 `push_to_sq()` (Lance 只处理 Read)
fn push_to_sq(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, Arc<IoRequest>>,
    request: Arc<IoRequest>,
) -> io::Result<()> {
    let user_data = USER_DATA_COUNTER.fetch_add(1, Ordering::Relaxed);

    // 根据操作类型构造 SQE
    let sqe = match request.op_type {
        UringOpType::Read => {
            // 参考: Lance thread.rs:276-277
            let (buf_ptr, read_offset, read_len) = {
                let state = request.state.lock().unwrap();
                let br = state.bytes_read;
                (
                    unsafe { state.buffer.as_ptr().add(br) as *mut u8 },
                    request.offset + br as u64,
                    (request.length - br) as u32,
                )
            };
            opcode::Read::new(types::Fd(request.fd), buf_ptr, read_len)
                .offset(read_offset)
                .build()
        }
        UringOpType::Write => {
            // 写操作: 数据在 buffer 中
            let state = request.state.lock().unwrap();
            opcode::Write::new(
                types::Fd(request.fd),
                state.buffer.as_ptr(),
                state.buffer.len() as u32,
            )
            .offset(request.offset as i64)
            .build()
        }
        UringOpType::OpenAt => {
            // OpenAt 需要 path — 存在 buffer 中 (as bytes)
            let state = request.state.lock().unwrap();
            let path_ptr = state.buffer.as_ptr() as *const i8;
            opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_ptr)
                .flags(libc::O_RDONLY | libc::O_CLOEXEC)
                .build()
        }
        UringOpType::Close => {
            opcode::Close::new(types::Fd(request.fd)).build()
        }
        UringOpType::UnlinkAt => {
            let state = request.state.lock().unwrap();
            let path_ptr = state.buffer.as_ptr() as *const i8;
            opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path_ptr).build()
        }
        UringOpType::RenameAt => {
            // RenameAt 需要两个 path — 在 buffer 中用 \0 分隔
            // 简化: 用 RequestState 额外字段存储
            // 实际实现需要扩展 IoRequest 结构
            unimplemented!("RenameAt needs special handling")
        }
    }
    .user_data(user_data);

    let mut sq = ring.submission();
    if sq.is_full() {
        // 参考: Lance thread.rs:283-293 — SQ 满时返回错误
        request.fail(io::Error::new(
            io::ErrorKind::WouldBlock,
            "io_uring submission queue full",
        ));
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "io_uring submission queue full",
        ));
    }

    unsafe {
        if sq.push(&sqe).is_err() {
            request.fail(io::Error::other("Failed to push to SQ"));
            return Err(io::Error::other("Failed to push to SQ"));
        }
    }
    drop(sq);

    pending.insert(user_data, request);
    Ok(())
}

/// 收割所有可用 CQE，更新 RequestState 并唤醒 waker。
///
/// 参考: Lance `thread.rs:324-396` 的 `process_completions()`
fn process_completions(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, Arc<IoRequest>>,
) {
    for cqe in ring.completion() {
        let user_data = cqe.user_data();
        let result = cqe.result();

        if let Some(request) = pending.remove(&user_data) {
            let mut state = request.state.lock().unwrap();

            if result < 0 {
                // 内核错误
                state.err = Some(io::Error::from_raw_os_error(-result));
                state.completed = true;
            } else if result == 0 && request.op_type == UringOpType::Read {
                // EOF — 读到 0 字节但请求了非零长度
                let br = state.bytes_read;
                if br == 0 {
                    // 完全 miss (文件被删除/racy eviction)
                    state.completed = true;
                } else {
                    // partial read 完成
                    state.buffer.truncate(br);
                    state.completed = true;
                }
            } else {
                // 正常完成: result > 0 (读) 或 result >= 0 (写/open/close/unlink)
                match request.op_type {
                    UringOpType::Read => {
                        let n = result as usize;
                        state.bytes_read += n;
                        if state.bytes_read >= request.length {
                            // 完整读完成
                            state.buffer.truncate(state.bytes_read);
                            state.completed = true;
                        } else {
                            // Short read — 需要重试
                            // 参考: Lance thread.rs:371-376
                            drop(state);
                            // 重新 push (调整 offset + bytes_read)
                            let _ = push_to_sq(ring, pending, request);
                            continue;
                        }
                    }
                    UringOpType::Write | UringOpType::OpenAt
                    | UringOpType::Close | UringOpType::UnlinkAt => {
                        // 写/open/close/unlink: result 是 fd 或 0
                        state.completed = true;
                    }
                    UringOpType::RenameAt => {
                        state.completed = true;
                    }
                }
            }

            // 唤醒等待的 Future
            // 参考: Lance thread.rs:380-383
            if let Some(waker) = state.waker.take() {
                drop(state);
                waker.wake();
            }
        }
    }
}

// ── 配置读取 ──────────────────────────────────────────────

fn get_queue_depth() -> usize {
    std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16384)
}

fn get_thread_count() -> usize {
    std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
}
```

### 3.3 `UringReadFuture` — 自定义 Future

参考 Lance `future.rs:16-46`。

```rust
// src/cache/store/uring/future.rs

use super::requests::IoRequest;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// 等待 io_uring 读操作完成的 Future。
///
/// `poll` 时检查 `RequestState.completed`:
/// - true → 取出 buffer/errors 返回 `Poll::Ready`
/// - false → 存 waker 返回 `Poll::Pending`，CQE 完成时后台线程调 `waker.wake()`
///
/// 参考: Lance `future.rs:16-46` 的 `UringReadFuture`
pub struct UringOpFuture {
    pub request: Arc<IoRequest>,
}

impl Future for UringOpFuture {
    /// 返回 (result_code, Bytes) — result_code 是 CQE result，Bytes 是读到的数据（写操作为空）
    type Output = (i32, Bytes);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.request.state.lock().unwrap();

        if state.completed {
            // 参考: Lance future.rs:26-39
            match state.err.take() {
                Some(err) => {
                    // 返回负数 errno
                    let raw_err = err.raw_os_error().unwrap_or(-1);
                    Poll::Ready((raw_err, Bytes::new()))
                }
                None => {
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    // 对读操作返回 bytes_read 作为 result_code
                    // 对其他操作返回 0
                    Poll::Ready((state.bytes_read as i32, bytes))
                }
            }
        } else {
            // 未完成 — 存 waker 等待唤醒
            // 参考: Lance future.rs:41-43
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
```

### 3.4 `UringPageStore` — 实现 `PageStore` trait

参考 Lance `reader.rs:97-292` 的 `UringReader`，但扩展为完整的 `PageStore`（get + put + delete）。

```rust
// src/cache/store/uring/store.rs

use super::driver::submit_request;
use super::future::UringOpFuture;
use super::requests::{IoRequest, RequestState, UringOpType};
use super::NUM_BUCKETS;
use crate::cache::page_id::PageId;
use crate::cache::store::PageStore;
use crate::error::{Error, Result};
use bytes::BytesMut;
use std::ffi::CString;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// io_uring 后端的 PageStore 实现。
///
/// 与 `LocalPageStore`（tokio::fs 后端）实现相同的 `PageStore` trait，
/// 上层 `LocalCacheManager` 透明切换。
///
/// 参考: Lance `reader.rs:97-109` 的 `UringReader`
pub struct UringPageStore {
    root: PathBuf,
    page_size: u64,
}

impl UringPageStore {
    /// 创建 store + 目录。
    /// 参考: Lance `reader.rs:124-180` 的 `open()`
    pub async fn create(dir: &Path, page_size: u64) -> Result<Self> {
        let root = dir.join(page_size.to_string());
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self { root, page_size })
    }

    /// page 文件路径: <root>/<bucket>/<file_id>/<page_index>
    /// 与 LocalPageStore 完全一致 (local.rs:82-88)
    fn page_path(&self, page_id: &PageId) -> PathBuf {
        let bucket = hash_file_id(&page_id.file_id) % NUM_BUCKETS;
        self.root
            .join(bucket.to_string())
            .join(page_id.file_id.as_ref())
            .join(page_id.page_index.to_string())
    }

    /// 异步 open 文件 (OP_OPENAT) — 返回 fd
    ///
    /// 用 io_uring 的 OpenAt opcode, 零 spawn_blocking
    async fn open_fd(&self, path: &Path, flags: i32) -> std::io::Result<RawFd> {
        let path_cstring = CString::new(path.to_string_lossy().into_owned())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        let request = Arc::new(IoRequest {
            fd: libc::AT_FDCWD,
            offset: 0,
            length: 0,
            op_type: UringOpType::OpenAt,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::from(path_cstring.to_bytes()),
                bytes_read: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, _bytes) = UringOpFuture { request }.await;

        if result < 0 {
            Err(std::io::Error::from_raw_os_error(-result))
        } else {
            Ok(result as RawFd)
        }
    }

    /// 异步 close fd (OP_CLOSE)
    async fn close_fd(&self, fd: RawFd) {
        let request = Arc::new(IoRequest {
            fd,
            offset: 0,
            length: 0,
            op_type: UringOpType::Close,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::new(),
                bytes_read: 0,
            }),
        });
        submit_request(Arc::clone(&request));
        let _ = UringOpFuture { request }.await; // best-effort
    }

    /// 异步 unlink (OP_UNLINKAT)
    async fn unlink_fd(&self, path: &Path) -> std::io::Result<()> {
        let path_cstring = CString::new(path.to_string_lossy().into_owned())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        let request = Arc::new(IoRequest {
            fd: libc::AT_FDCWD,
            offset: 0,
            length: 0,
            op_type: UringOpType::UnlinkAt,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::from(path_cstring.to_bytes()),
                bytes_read: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, _) = UringOpFuture { request }.await;

        if result < 0 {
            let e = std::io::Error::from_raw_os_error(-result);
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(()); // idempotent
            }
            return Err(e);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PageStore for UringPageStore {
    /// 读 page — OP_OPENAT + OP_READ + OP_CLOSE
    ///
    /// 对比 LocalPageStore::get (local.rs:212-242):
    /// - LocalPageStore: 3 次 spawn_blocking (open + seek + read)
    /// - UringPageStore: 3 次 io_uring SQE (open + read + close), 零 spawn_blocking
    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let path = self.page_path(page_id);

        // 1) OP_OPENAT — 异步打开
        let fd = match self.open_fd(&path, libc::O_RDONLY).await {
            Ok(fd) => fd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(io_error("uring open", e)),
        };

        // 2) OP_READ — 异步 pread (offset + length 一次 syscall)
        //    参考 Lance reader.rs:188-191 的 buffer 分配
        let request = Arc::new(IoRequest {
            fd,
            offset: offset as u64,
            length: dst.len(),
            op_type: UringOpType::Read,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::from(unsafe {
                    std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len())
                }),
                bytes_read: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, read_bytes) = UringOpFuture { request }.await;

        // 3) OP_CLOSE — 异步关闭 (fire-and-forget)
        self.close_fd(fd).await;

        // 4) 处理结果
        if result < 0 {
            let e = std::io::Error::from_raw_os_error(-result);
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(0); // racy eviction → miss
            }
            return Err(io_error("uring read", e));
        }

        // 将读到的数据拷贝到 dst
        let n = result as usize;
        if n > 0 {
            dst[..n].copy_from_slice(&read_bytes[..n]);
        }
        Ok(n)
    }

    /// 写 page — OP_OPENAT + OP_WRITE + OP_CLOSE + OP_RENAMEAT
    ///
    /// 对比 LocalPageStore::put (local.rs:170-210):
    /// - LocalPageStore: 4 次 spawn_blocking (create + write_all + flush + rename)
    /// - UringPageStore: 4 次 io_uring SQE, 零 spawn_blocking
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()> {
        let final_path = self.page_path(page_id);
        let parent = final_path.parent().unwrap().to_path_buf();

        // 确保目录存在 (这一步不在热路径上, 用 tokio::fs)
        tokio::fs::create_dir_all(&parent).await
            .map_err(|e| io_error("create page dir", e))?;

        let tmp_path = parent.join(format!(
            "{}.tmp-{}",
            page_id.page_index,
            uuid::Uuid::new_v4()
        ));

        let tmp_cstring = CString::new(tmp_path.to_string_lossy().into_owned())
            .map_err(|e| io_error("cstring", e))?;

        // 1) OP_OPENAT (O_WRONLY | O_CREAT | O_TRUNC)
        let fd = {
            let request = Arc::new(IoRequest {
                fd: libc::AT_FDCWD,
                offset: 0,
                length: 0,
                op_type: UringOpType::OpenAt,
                state: std::sync::Mutex::new(RequestState {
                    completed: false,
                    waker: None,
                    err: None,
                    buffer: BytesMut::from(tmp_cstring.to_bytes()),
                    bytes_read: 0,
                }),
            });
            submit_request(Arc::clone(&request));
            let (result, _) = UringOpFuture { request }.await;
            if result < 0 {
                return Err(io_error("uring open tmp",
                    std::io::Error::from_raw_os_error(-result)));
            }
            result as RawFd
        };

        // 2) OP_WRITE (整页)
        {
            let request = Arc::new(IoRequest {
                fd,
                offset: 0,
                length: page.len(),
                op_type: UringOpType::Write,
                state: std::sync::Mutex::new(RequestState {
                    completed: false,
                    waker: None,
                    err: None,
                    buffer: BytesMut::from(page),
                    bytes_read: 0,
                }),
            });
            submit_request(Arc::clone(&request));
            let (result, _) = UringOpFuture { request }.await;
            if result < 0 {
                self.close_fd(fd).await;
                return Err(io_error("uring write",
                    std::io::Error::from_raw_os_error(-result)));
            }
        }

        // 3) OP_CLOSE
        self.close_fd(fd).await;

        // 4) rename (用 std::fs::rename — rename 不在热路径上, 且需要跨路径)
        //    TODO: 后续可改用 OP_RENAMEAT
        std::fs::rename(&tmp_path, &final_path)
            .map_err(|e| io_error("rename temp page file", e))?;

        Ok(())
    }

    /// 删 page — OP_UNLINKAT
    ///
    /// 对比 LocalPageStore::delete (local.rs:244-251):
    /// - LocalPageStore: 1 次 spawn_blocking (remove_file)
    /// - UringPageStore: 1 次 io_uring SQE, 零 spawn_blocking
    async fn delete(&self, page_id: &PageId) -> Result<()> {
        let path = self.page_path(page_id);
        self.unlink_fd(&path).await
            .map_err(|e| io_error("uring unlink", e))?;
        Ok(())
    }
}
```

### 3.5 `sys.rs` — 平台检测与降级

```rust
// src/cache/store/uring/sys.rs

/// 检测 io_uring 是否可用。
/// 1. target_os == "linux" (编译时)
/// 2. 运行时尝试初始化一个 io_uring 实例 (探测内核版本)
/// 3. 失败则返回 None → 降级到 LocalPageStore
///
/// 参考: Lance `uring.rs:32-35` — "only available on Linux and requires kernel 5.1"
pub fn is_uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // 尝试创建一个最小 io_uring 实例探测内核支持
        match io_uring::IoUring::new(4) {
            Ok(_) => {
                tracing::info!("io_uring is available on this platform");
                true
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "io_uring not available; falling back to tokio::fs backend"
                );
                false
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}
```

### 3.6 `mod.rs` — 模块声明与工具函数

```rust
// src/cache/store/uring/mod.rs

#[cfg(target_os = "linux")]
mod driver;
#[cfg(target_os = "linux")]
mod future;
#[cfg(target_os = "linux")]
mod requests;
#[cfg(target_os = "linux")]
mod store;
mod sys;

#[cfg(target_os = "linux")]
pub use store::UringPageStore;

pub use sys::is_uring_available;

/// 与 LocalPageStore 一致的 hash (local.rs:61-63)
/// xxHash3 64-bit, 用于 bucket 分配
fn hash_file_id(file_id: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(file_id.as_bytes())
}

/// 与 LocalPageStore 一致的 bucket 数 (local.rs:24)
const NUM_BUCKETS: u64 = 1000;

fn io_error(message: impl Into<String>, e: std::io::Error) -> Error {
    Error::Internal {
        message: message.into(),
        source: Some(Box::new(e)),
    }
}
```

---

## 4. 集成点改动

### 4.1 `PageStore` trait — 无需改动

```rust
// src/cache/store/mod.rs (现有, 不变)
// L19-33
pub trait PageStore: Send + Sync {
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()>;
    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize>;
    async fn delete(&self, page_id: &PageId) -> Result<()>;
}
```

### 4.2 `LocalCacheManager` — 改 `Vec<LocalPageStore>` → `Vec<Arc<dyn PageStore>>`

**文件**: `src/cache/manager.rs`

**改动 1** — struct 字段 (L82-92):

```rust
// 改动前 (L83):
//   stores: Vec<LocalPageStore>,
// 改动后:
stores: Vec<Arc<dyn PageStore>>,
```

**改动 2** — `create()` 工厂方法 (L109-160):

```rust
pub async fn create(options: CacheManagerOptions) -> Result<Self> {
    let dir_paths: Vec<&Path> = if options.dirs.is_empty() {
        vec![Path::new("/tmp/goosefs_cache")]
    } else {
        options.dirs.iter().map(|p| p.as_path()).collect()
    };

    // 检测 io_uring 可用性
    let use_uring = options.uring_enabled && uring::is_uring_available();

    let mut stores: Vec<Arc<dyn PageStore>> = Vec::with_capacity(dir_paths.len());
    let mut dirs = Vec::with_capacity(dir_paths.len());

    for dir in &dir_paths {
        let store: Arc<dyn PageStore> = if use_uring {
            // 使用 io_uring 后端
            match UringPageStore::create(dir, options.page_size).await {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    tracing::warn!(error = %e, "UringPageStore creation failed; fallback to LocalPageStore");
                    Arc::new(LocalPageStore::create(dir, options.page_size).await?)
                }
            }
        } else {
            // 使用 tokio::fs 后端 (现有)
            Arc::new(LocalPageStore::create(dir, options.page_size).await?)
        };
        stores.push(store);

        dirs.push(DirState {
            evictor: build_evictor(options.evictor),
            used_bytes: 0,
            capacity: options.dir_capacity,
        });
    }

    // ... 其余初始化不变 (L127-159)
    let page_locks = (0..LOCK_SIZE).map(|_| RwLock::new(())).collect();
    let async_write_sem = Arc::new(Semaphore::new(options.async_write_threads.max(1)));

    let mgr = Self {
        options,
        stores,
        allocator: Box::new(HashAllocator::new()),
        inner: Mutex::new(Inner {
            meta: HashMap::new(),
            by_file: HashMap::new(),
            versions: HashMap::new(),
            dirs,
        }),
        page_locks,
        async_write_sem,
        state: CacheState::ReadWrite,
    };

    if let Err(e) = mgr.restore().await {
        warn!(error = %e, "cache restore failed; starting with empty cache");
    }
    mgr.publish_capacity_gauges_initial();
    Ok(mgr)
}
```

**改动 3** — `get()` / `put()` / `delete()` 中的 store 调用:

```rust
// get() L582:
// 改动前: let n = match self.stores[dir_index].get(page_id, page_offset, dst).await
// 改动后: let n = match self.stores[dir_index].get(page_id, page_offset, dst).await  // 不变! trait object

// put() L480:
// 改动前: if let Err(e) = self.stores[dir_index].put(page_id, &page).await
// 改动后: 同上, 不变

// delete() L634:
// 改动前: if let Err(e) = self.stores[dir_index].delete(page_id).await
// 改动后: 同上, 不变
```

### 4.3 `CacheManagerOptions` — 新增 io_uring 配置

**文件**: `src/cache/options.rs`

```rust
// 在 CacheManagerOptions struct 中新增字段
pub struct CacheManagerOptions {
    // ... 现有字段 ...

    /// 是否启用 io_uring 后端 (仅 Linux 5.1+)
    pub uring_enabled: bool,
    /// io_uring 队列深度 (默认 16384)
    pub uring_queue_depth: usize,
    /// io_uring 后台线程数 (默认 2)
    pub uring_thread_count: usize,
}

impl CacheManagerOptions {
    pub fn from_config(config: &GoosefsConfig) -> Self {
        Self {
            // ... 现有字段 ...

            uring_enabled: config.client_cache_uring_enabled,
            uring_queue_depth: config.client_cache_uring_queue_depth,
            uring_thread_count: config.client_cache_uring_thread_count,
        }
    }
}
```

### 4.4 `GoosefsConfig` — 新增配置字段

**文件**: `src/config.rs`

```rust
// 在 GoosefsConfig struct 中新增 (约 L1889 附近, client_cache_ttl_secs 后面)

/// 是否启用 io_uring 后端 (仅 Linux, 默认 true on Linux)
#[serde(default = "default_cache_uring_enabled")]
pub client_cache_uring_enabled: bool,

/// io_uring 队列深度 (默认 16384)
#[serde(default = "default_cache_uring_queue_depth")]
pub client_cache_uring_queue_depth: usize,

/// io_uring 后台线程数 (默认 2)
#[serde(default = "default_cache_uring_thread_count")]
pub client_cache_uring_thread_count: usize,

fn default_cache_uring_enabled() -> bool {
    cfg!(target_os = "linux")
}

fn default_cache_uring_queue_depth() -> usize { 16384 }
fn default_cache_uring_thread_count() -> usize { 2 }
```

对应环境变量:

```rust
// src/config.rs — ENV 常量 (约 L680 附近)
pub const ENV_CLIENT_CACHE_URING_ENABLED: &str = "GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED";
pub const ENV_CLIENT_CACHE_URING_QUEUE_DEPTH: &str = "GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH";
pub const ENV_CLIENT_CACHE_URING_THREAD_COUNT: &str = "GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT";

// storage option
pub const STORAGE_OPT_CLIENT_CACHE_URING_ENABLED: &str = "goosefs_client_cache_uring_enabled";
pub const STORAGE_OPT_CLIENT_CACHE_URING_QUEUE_DEPTH: &str = "goosefs_client_cache_uring_queue_depth";
pub const STORAGE_OPT_CLIENT_CACHE_URING_THREAD_COUNT: &str = "goosefs_client_cache_uring_thread_count";
```

### 4.5 `Cargo.toml` — 新增依赖

```toml
# Cargo.toml — 新增 (约 L68 后)

# io_uring 后端 (仅 Linux)
[target.'cfg(target_os = "linux")'.dependencies]
io-uring = "0.7"
libc = "0.2"
```

参考 Lance `Cargo.toml:52-53`:
```toml
# Lance 的写法
[target.'cfg(target_os = "linux")'.dependencies]
io-uring = { workspace = true }
```

---

## 5. 读路径完整流程对比

### 5.1 cache 命中路径（`get`）

```text
read_at(offset, n)
  → read_at_cached(offset, end)                    // file_in_stream.rs:873
    → read_through_cache(...)                       // caching_reader.rs:55
      → cache.get(page_id, in_page_off, &mut dst)  // manager.rs:540
        → page_locks[idx].read().await             // 条带读锁 (不变)
        → inner.lock().await → 查 meta             // 元数据锁 (不变)
        → stores[dir_index].get(page_id, off, dst) // ← UringPageStore::get
          │
          │  UringPageStore::get:
          │  1. OP_OPENAT → submit_request → UringOpFuture.await
          │     ↓ 后台线程: push SQE → ring.submit()
          │     ↓ 内核: 异步 open → CQE
          │     ↓ 后台线程: process_completions → waker.wake()
          │     ↓ tokio reactor: 唤醒 async 任务
          │
          │  2. OP_READ → submit_request → UringOpFuture.await  (同上)
          │
          │  3. OP_CLOSE → submit_request → fire-and-forget
          │
          │  全程零 spawn_blocking, 零线程切换
          │
        → inner.lock().await → evictor.on_access   // LRU 更新 (不变)
        → 返回 n
```

### 5.2 时序对比

**tokio::fs (当前, 300 QPS)**:
```
async线程          blocking-pool-worker    syscall
  │                    │                     │
  ├─ spawn_blocking ──→│                     │
  │                    ├─ open() ──────────→│ open syscall
  │                    │←───────────────────│
  │←───────────────────│                     │
  ├─ spawn_blocking ──→│                     │
  │                    ├─ lseek() ─────────→│ lseek syscall
  │                    │←───────────────────│
  │←───────────────────│                     │
  ├─ spawn_blocking ──→│                     │
  │                    ├─ read() ──────────→│ read syscall
  │                    │←───────────────────│
  │←───────────────────│                     │
  │                                              总计 ~150-300 µs
```

**io_uring (目标, 900+ QPS)**:
```
async线程          uring-driver-thread    kernel
  │                    │                     │
  ├─ submit_request ──→│                     │
  │  (channel send)    │                     │
  │                    ├─ push SQE (open)    │
  │                    ├─ push SQE (read)    │
  │                    ├─ ring.submit() ────→│ io_uring_enter
  │  UringOpFuture      │                     │  (内核并行处理)
  │  .await (Pending)  │                     │
  │                    │←──── CQE (open) ────│
  │                    │  waker.wake()       │
  │←───────────────────│                     │
  │                    │←──── CQE (read) ────│
  │                    │  waker.wake()       │
  │←───────────────────│                     │
  │                                              总计 ~5-20 µs
```

---

## 6. fd 缓存（P2 优化）

参考 Lance `reader.rs:57-63` 的 `HANDLE_CACHE: LazyLock<moka::future::Cache>`。

```rust
// src/cache/store/uring/store.rs — P2 优化

use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;

pub struct UringPageStore {
    root: PathBuf,
    page_size: u64,
    /// fd 缓存: PageId → RawFd, 避免每次 open/close
    /// 参考: Lance reader.rs:57-63 的 moka cache
    fd_cache: Mutex<LruCache<PageId, RawFd>>,
}

impl UringPageStore {
    pub async fn create(dir: &Path, page_size: u64) -> Result<Self> {
        // ...
        let fd_cache = Mutex::new(LruCache::new(
            NonZeroUsize::new(1024).unwrap()
        ));
        Ok(Self { root, page_size, fd_cache })
    }

    async fn get_fd(&self, page_id: &PageId) -> std::io::Result<RawFd> {
        // 1) 查 fd cache
        {
            let mut cache = self.fd_cache.lock().unwrap();
            if let Some(fd) = cache.get(page_id) {
                return Ok(*fd);  // 命中, 零 open syscall
            }
        }
        // 2) miss → OP_OPENAT
        let fd = self.open_fd(&self.page_path(page_id), libc::O_RDONLY).await?;
        // 3) 存入 cache
        let mut cache = self.fd_cache.lock().unwrap();
        if let Some((_, evicted_fd)) = cache.push(page_id.clone(), fd) {
            // LRU 淘汰的 fd 需要关闭
            self.close_fd(evicted_fd).await;
        }
        Ok(fd)
    }
}
```

**注意**: fd cache 需要在 `delete()` 时同步清理对应条目，否则读到已删文件的 stale fd。

---

## 7. 配置项

| 配置 key | 含义 | 默认值 |
|---|---|---|
| `goosefs.user.client.cache.uring.enabled` | 是否启用 io_uring 后端 | `true` (Linux), 忽略 (其他平台) |
| `goosefs.user.client.cache.uring.queue.depth` | SQ/CQ 队列深度 | `16384` |
| `goosefs.user.client.cache.uring.thread.count` | 后台线程数 | `2` |

环境变量:

```bash
# 关闭 io_uring, 回退到 tokio::fs
export GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED=false

# 调大队列深度 (高并发场景)
export GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH=32768

# 调大后台线程数
export GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT=4
```

参考 Lance 的环境变量 (`uring.rs:19-27`):
```bash
LANCE_URING_QUEUE_DEPTH=16384
LANCE_URING_THREAD_COUNT=2
LANCE_URING_SUBMIT_BATCH_SIZE=128
LANCE_URING_POLL_TIMEOUT_MS=10
```

---

## 8. Metrics

| Rust 常量 | metric 名 | 类型 | 说明 |
|---|---|---|---|
| `CLIENT_CACHE_URING_BACKEND_ACTIVE` | `Client.CacheUringBackendActive` | gauge | 1=io_uring, 0=tokio::fs |
| `CLIENT_CACHE_URING_QUEUE_DEPTH` | `Client.CacheUringQueueDepth` | gauge | SQ/CQ 队列深度 |
| `CLIENT_CACHE_URING_THREAD_COUNT` | `Client.CacheUringThreadCount` | gauge | 后台线程数 |
| `CLIENT_CACHE_URING_SUBMITTED_TOTAL` | `Client.CacheUringSubmittedTotal` | counter | 累计提交 SQE 数 |
| `CLIENT_CACHE_URING_COMPLETED_TOTAL` | `Client.CacheUringCompletedTotal` | counter | 累计完成 CQE 数 |
| `CLIENT_CACHE_URING_ERRORS_TOTAL` | `Client.CacheUringErrorsTotal` | counter | io_uring 操作错误数 |
| `CLIENT_CACHE_URING_IN_FLIGHT` | `Client.CacheUringInFlight` | gauge | 当前在途请求数 |

---

## 9. 服务端影响

### 9.1 零服务端改动

**本设计不涉及任何 GooseFS 服务端（Master / Worker）改动。**

| 关注点 | 为什么是纯客户端改动 |
|---|---|
| 磁盘布局 | `<dir>/<page_size>/<bucket>/<file_id>/<page_index>` — 与 `LocalPageStore` 完全一致, 服务端无感知 |
| 缓存文件内容 | page cache 存的是客户端从 Worker/UFS 读回的整页数据, 不是服务端 block 文件 |
| `PageStore` trait 契约 | `put` / `get` / `delete` 全部操作的是**客户端本地磁盘上的缓存文件**, 不涉及任何 RPC |
| io_uring 操作对象 | `OP_OPENAT` / `OP_READ` / `OP_WRITE` / `OP_CLOSE` / `OP_UNLINKAT` 全部作用于**客户端本地的 page cache 文件**, 不是 GooseFS block 文件 |
| 缓存 key | `file_id` = `URIStatus.file_id` (服务端 inode 字符串), 但仅作为本地文件路径组件使用, 不回传服务端 |
| 覆盖写检测 | `on_file_open(file_id, length, mtime)` 在客户端本地比对 `(length, last_modification_time_ms)`, 不调用任何 Master RPC |
| 进程重启恢复 | `restore()` 扫描本地缓存目录重建索引, 不涉及服务端 |
| 回源路径 | cache miss → `read_external_range` → `positioned_read_with_retry` → gRPC `ReadBlock` — 这是**现有路径, 不变** |
| 滚动升级 | io_uring 后端与 tokio::fs 后端磁盘格式完全一致, 客户端可自由切换; 服务端版本无关 |
| 协议兼容 | 零 proto 改动; 零 Master/Worker 代码改动; 零配置改动 |

**与 SC io_uring 可行性分析对比**: [`SHORT_CIRCUIT_IO_URING_FEASIBILITY.md`](SHORT_CIRCUIT_IO_URING_FEASIBILITY.md) §5 也确认了 SC 路径零服务端改动。本设计的 page cache 路径同理 — `PageStore` 是纯本地文件操作抽象, 不涉及任何网络协议。

### 9.2 改动范围

| 改动层 | 文件 | 性质 |
|---|---|---|
| 新增代码 | `src/cache/store/uring/{mod,store,driver,future,requests,sys}.rs` | 纯新增, 不修改现有逻辑 |
| 类型替换 | `src/cache/manager.rs:83` `Vec<LocalPageStore>` → `Vec<Arc<dyn PageStore>>` | trait object 化, 上层调用不变 |
| 配置新增 | `src/cache/options.rs`, `src/config.rs` | 新增字段 + 默认值, 不影响现有配置 |
| 依赖新增 | `Cargo.toml` | `io-uring` + `libc`, 仅 Linux, target-gated |
| **服务端** | **无** | **零改动** |

---

## 10. 数据一致性与语义一致性

### 10.1 数据一致性 — INV-PC-* 不变量逐项验证

io_uring 后端必须满足与 `tokio::fs` 后端完全相同的正确性契约。以下逐项验证 [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md) §1.4 定义的不变量:

| 不变量 | io_uring 后端如何保证 |
|---|---|
| **INV-PC-D1** (cache vs direct byte diff) | `UringPageStore::get` 用 `OP_READ` (pread) 读取的磁盘字节与 `LocalPageStore::get` 用 `tokio::fs::File::read` 读取的是**同一文件同一偏移**。`pread` 是 POSIX 标准的原子定位+读取, 语义与 `seek + read` 完全等价。字节一致性由磁盘文件内容保证 — 两后端读写的是**同一组磁盘文件** (`<dir>/<page_size>/<bucket>/<file_id>/<page_index>`)。 |
| **INV-PC-D2** (read APIs equivalent) | `read` / `read_at` / `read_all` 都经过 `read_through_cache` → `cache.get` → `PageStore::get`。后端切换对上层完全透明 — `LocalCacheManager` 持有 `Vec<Arc<dyn PageStore>>`, trait object 分发, 调用方不感知后端类型。 |
| **INV-PC-S1** (failed fill doesn't poison) | (1) `OP_WRITE` 失败 → tmp 文件残留 → `put` 返回 `false` → meta 不更新 → 下次 `get` miss → 回源正确读取。(2) `OP_RENAMEAT` / `std::fs::rename` 失败 → tmp 文件残留 → `restore` 时清理 `.tmp-*` 文件。(3) SQ 满 → `request.fail(WouldBlock)` → `put` 返回 `false` → meta 不更新。三种失败路径都保证: **缓存失败降级为 miss, 绝不返回脏数据**。 |
| **INV-PC-S2** (restart byte parity) | io_uring 后端写 page 用 `tmp + rename` 原子模式 — 与 `LocalPageStore` 完全一致。`rename` 在 POSIX 语义下是原子的: 要么旧文件不存在新文件可见, 要么旧文件存在新文件不可见。进程重启后 `restore()` 扫描同一目录格式 (`<dir>/<page_size>/<bucket>/<file_id>/<page_index>`), 不区分文件由哪个后端写入。`.identity` sidecar 的读写也走 `tokio::fs` (不在热路径), 两后端共享。 |

### 10.2 语义一致性 — PageStore trait 契约对齐

`PageStore` trait (`src/cache/store/mod.rs:19-33`) 定义了三个方法的语义契约:

```rust
async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()>;
async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize>;
async fn delete(&self, page_id: &PageId) -> Result<()>;
```

逐方法验证 io_uring 后端的语义对齐:

#### `get` 语义

| 契约 | LocalPageStore (tokio::fs) | UringPageStore (io_uring) | 一致? |
|---|---|---|---|
| 命中: 返回读取字节数 (>0) | `File::open` + `seek` + `read` | `OP_OPENAT` + `OP_READ` + `OP_CLOSE` | ✅ |
| 未命中: 返回 `Ok(0)` | `File::open` 返回 `NotFound` → `Ok(0)` | `OP_OPENAT` 返回 `-ENOENT` → `Ok(0)` | ✅ |
| 读失败: 返回 `Err` | `read` 返回 `Err` | `OP_READ` CQE result < 0 → `Err` | ✅ |
| Short read: 循环读完 `dst.len()` | `while filled < dst.len() { read() }` | CQE short read → `push_to_sq` 调整 offset 重试 (参考 Lance `thread.rs:371-376`) | ✅ |
| Racy eviction: `get` 返回 0 | `open` 成功但文件已被 delete → `read` 返回 0 | `OP_OPENAT` 成功但 `OP_READ` 返回 `-ENOENT` → `Ok(0)` | ✅ |
| `offset` 语义: page 内偏移 | `f.seek(Start(offset))` | `OP_READ.offset(offset)` (pread) | ✅ |

#### `put` 语义

| 契约 | LocalPageStore (tokio::fs) | UringPageStore (io_uring) | 一致? |
|---|---|---|---|
| 原子写: tmp + rename | `File::create(tmp)` + `write_all` + `flush` + `rename` | `OP_OPENAT(O_CREAT\|O_TRUNC)` + `OP_WRITE` + `OP_CLOSE` + `rename` | ✅ |
| 写失败: tmp 清理 | `remove_file(tmp)` | `close_fd` + `remove_file(tmp)` (best-effort) | ✅ |
| 并发写同一 page: 不互相覆盖 | tmp 文件名含 UUID | tmp 文件名含 UUID (同策略) | ✅ |
| `rename` 原子性 | `tokio::fs::rename` (POSIX rename) | `std::fs::rename` (POSIX rename) | ✅ |

**注意**: `put` 路径中的 `std::fs::rename` 是同步调用。这是有意的 — rename 不在 cache 命中热路径上（只在 cache miss 回填时执行），且 POSIX rename 在 NVMe 上 ~5 µs，不影响整体性能。如果后续需要进一步优化，可改用 `OP_RENAMEAT`，但首版不引入额外复杂度。

#### `delete` 语义

| 契约 | LocalPageStore (tokio::fs) | UringPageStore (io_uring) | 一致? |
|---|---|---|---|
| 文件不存在: 返回 `Ok(())` | `remove_file` 返回 `NotFound` → `Ok(())` | `OP_UNLINKAT` 返回 `-ENOENT` → `Ok(())` | ✅ |
| 删除成功: 返回 `Ok(())` | `remove_file` 成功 | `OP_UNLINKAT` CQE result == 0 | ✅ |
| 删除失败: 返回 `Err` | `remove_file` 返回其他错误 | `OP_UNLINKAT` CQE result < 0 → `Err` | ✅ |

### 10.3 并发语义 — 与 LocalPageStore 一致

io_uring 后端不改变 `LocalCacheManager` 的并发模型:

| 并发机制 | 现有 (tokio::fs) | io_uring 后端 | 一致? |
|---|---|---|---|
| 页级条带锁 `page_locks[1024]` | `RwLock` — get 取读锁, put/delete 取写锁 | **不变** — 锁在 `LocalCacheManager` 层, 不在 `PageStore` 层 | ✅ |
| 元数据锁 `Mutex<Inner>` | 守护 meta/by_file/versions/dirs | **不变** | ✅ |
| 磁盘 IO 在锁外 | `inner.lock()` 释放后才调 `store.get/put` | **不变** — `UringPageStore::get` 在 `inner.lock()` 释放后调用 | ✅ |
| 同页串行 | `page_locks[hash(page_id)].write()` 保证同页 put 串行 | **不变** | ✅ |
| 异页并发 | 不同 stripe 的 `RwLock` 不互斥 | **不变** | ✅ |
| 异步回填限流 | `Semaphore(async_write_threads)` | **不变** | ✅ |

### 10.4 io_uring 特有风险与缓解

| 风险 | 语义影响 | 缓解措施 | 对齐现有行为 |
|---|---|---|---|
| `OP_OPENAT` + `OP_READ` 之间文件被 delete | `OP_READ` 返回 `-ENOENT` | 视为 miss → `Ok(0)` → 上层回源 | ✅ 与 `LocalPageStore` 的 `open` 成功但 `read` 返回 0 一致 |
| Short read (CQE result < length) | 部分数据读到 | `push_to_sq` 调整 `offset + bytes_read` 重试 (参考 Lance `thread.rs:371-376`) | ✅ 与 `LocalPageStore` 的 `while filled < dst.len()` 循环语义一致 |
| SQ 满 → push 失败 | 请求无法提交 | `request.fail(WouldBlock)` → `put` 返回 `false` → meta 不更新 | ✅ 与 `LocalPageStore` 写盘失败返回 `false` 一致 |
| 后台线程 panic | channel disconnect | `submit_request` 返回 `BrokenPipe` → `fail()` → 上层回源 | ✅ 降级为 miss, 不影响正确性 |
| `O_DIRECT` 对齐错误 | `OP_READ` 返回 `-EINVAL` | 首版不用 `O_DIRECT` (走内核 page cache), 无对齐问题 | ✅ 与 `tokio::fs` 走内核 page cache 一致 |
| io_uring 初始化失败 | 无法使用 io_uring | `sys::is_uring_available()` 返回 `false` → 降级到 `LocalPageStore` | ✅ 透明降级, 上层无感知 |

### 10.5 降级安全性

当 io_uring 不可用时（非 Linux / 内核版本不够 / 初始化失败），自动降级到 `LocalPageStore`:

```text
LocalCacheManager::create()
  ├── uring_enabled && is_uring_available()?
  │     ├── YES → UringPageStore::create()
  │     │         ├── 成功 → 使用 io_uring 后端
  │     │         └── 失败 → 降级 ↓
  │     └── NO  → ↓
  └── LocalPageStore::create() → 使用 tokio::fs 后端 (现有行为)
```

降级后:
- 磁盘文件格式完全一致 — 同一目录可被两后端交叉使用
- 元数据索引/淘汰/计费完全一致 — `LocalCacheManager` 层不感知后端
- 已缓存的 page 可被任一后端读取
- 进程重启后 `restore()` 不区分后端

### 10.6 测试验证

| 测试层级 | 验证内容 | 文件 |
|---|---|---|
| 单元测试 | `UringPageStore` put/get/delete 基本功能 + short read + 并发 + 降级 | `src/cache/store/uring/store.rs` `#[cfg(test)]` |
| 集成测试 | INV-PC-D1/D2/S1/S2 全部在 io_uring 后端通过 | `tests/page_cache_consistency.rs` (复用, 无改动) |
| 交叉后端 | tokio::fs 写入 → io_uring 读取 (反向亦然) | 新增 `test_cross_backend_compatibility` |
| 性能基准 | io_uring vs tokio::fs 对比 | `benchmarks/cache_uring_bench.rs` |

---

## 10. 测试方案

### 10.1 单元测试

```rust
// src/cache/store/uring/store.rs — #[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn uring_put_get_roundtrip() {
        let store = UringPageStore::create(
            Path::new("/tmp/gfs_uring_test"), 1024
        ).await.unwrap();
        let id = PageId::new("file-a", 0);
        let data = b"hello uring page cache";
        store.put(&id, data).await.unwrap();
        let mut dst = vec![0u8; data.len()];
        let n = store.get(&id, 0, &mut dst).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&dst, data);
    }

    #[tokio::test]
    async fn uring_get_missing_returns_zero() { /* ... */ }

    #[tokio::test]
    async fn uring_concurrent_get_same_page() { /* 32 并发 */ }

    #[tokio::test]
    async fn uring_short_read_retry() { /* 模拟 short read */ }

    #[tokio::test]
    async fn uring_delete_then_miss() { /* delete 后 get 返回 0 */ }
}
```

### 10.2 集成测试

复用现有测试（后端切换对上层透明）:
- `tests/page_cache_e2e.rs`
- `tests/page_cache_consistency.rs`
- `benchmarks/page_cache_ab.rs`

### 10.3 性能基准

```rust
// benchmarks/cache_uring_bench.rs
async fn bench_cache_hit_uring() { /* UringPageStore, 10^5 cache hit */ }
async fn bench_cache_hit_tokio_fs() { /* LocalPageStore 对比 */ }
async fn bench_cache_hit_concurrent_uring() { /* 32 并发 */ }
```

**预期**:

| 后端 | 单线程 ops/s | 32 并发 ops/s | p99 延迟 |
|---|---|---|---|
| `tokio::fs` (当前) | ~3,000 | ~10,000 | ~2 ms |
| `io_uring` (预期) | ~50,000 | ~200,000 | ~0.1 ms |

---

## 11. 实施计划

| 阶段 | 内容 | 改动文件 | 预计工时 |
|---|---|---|---|
| **P0** | `requests.rs` + `future.rs` + `driver.rs` + 单测 | `src/cache/store/uring/{requests,future,driver}.rs` | 2 天 |
| **P1** | `store.rs` 实现 `PageStore` trait (get/put/delete) + `sys.rs` | `src/cache/store/uring/{store,sys,mod}.rs` | 2 天 |
| **P2** | `manager.rs` 改 `Vec<Arc<dyn PageStore>>` + `options.rs` + `config.rs` + `Cargo.toml` | `src/cache/{manager,options}.rs`, `src/config.rs`, `Cargo.toml` | 1 天 |
| **P3** | 性能基准 + 火焰图验证 + 调优 | `benchmarks/cache_uring_bench.rs` | 1 天 |
| **P4** | fd 缓存 (参考 Lance `HANDLE_CACHE`) + 批量提交优化 | `src/cache/store/uring/store.rs` | 2 天 |
| **P5** | 一致性回归 + CI + 文档 | `tests/`, `docs/` | 1 天 |

---

## 12. 预期效果

| 阶段 | QPS | 对比 Java |
|---|---|---|
| 当前 (tokio::fs) | 300 | Java 无 cache 开销 |
| P0-P3 完成 (io_uring) | 900-1100 | **追平 Java** |
| P4 完成 (fd cache + batch) | 1000-1200 | **超越 Java** (io_uring 优势) |

---

## 13. 交叉引用

- [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md) — 现有 cache 设计 (P0-P3 已实现)
- [`SHORT_CIRCUIT_IO_URING_FEASIBILITY.md`](SHORT_CIRCUIT_IO_URING_FEASIBILITY.md) — SC 路径的 io_uring 分析
- [`FLAMEGRAPH_OPTIMIZATION_PLAN.md`](FLAMEGRAPH_OPTIMIZATION_PLAN.md) — A/B/C 系列优化 (router/transport 层)
- [`perf/2026-07-08-oncpu3-cache-hotspots/CACHE_VS_NOCACHE_ANALYSIS.md`](perf/2026-07-08-oncpu3-cache-hotspots/CACHE_VS_NOCACHE_ANALYSIS.md) — D 系列优化项
- Lance 参考: `/opt/sourcecode/lance/rust/lance-io/src/uring/` — `thread.rs`, `reader.rs`, `future.rs`, `requests.rs`

---

## 14. 实现进度

> 最后更新：2026-07-08

### P0 — io_uring 核心组件 ✅

| 文件 | 状态 | 说明 |
|---|---|---|
| `src/cache/store/uring/requests.rs` | ✅ 完成 | `IoRequest` + `RequestState` + `UringOpType`（Read/Write/OpenAt/Close/UnlinkAt） |
| `src/cache/store/uring/future.rs` | ✅ 完成 | `UringOpFuture` — 通用 Future，返回 `(result_code, Bytes)` |
| `src/cache/store/uring/driver.rs` | ✅ 完成 | 后台线程池 + 主循环 + 攒批提交 + short read/write 重试 + round-robin |

**与设计文档的差异（改进）：**
- `RequestState.bytes_read` → `bytes_transferred`（同时用于 read 和 write 的 short 重试）
- 新增 `RequestState.result_code: i32`（存储 CQE result，解决 OpenAt 返回 fd 的问题）
- 新增 `IoRequest.open_flags: i32`（OpenAt 的 flags 参数，支持 `O_RDONLY` / `O_WRONLY|O_CREAT|O_TRUNC`）
- `process_completions` 收集 short read/write 重试到 `Vec` 后统一处理（参考 Lance），避免在 completion 循环中递归调用 `push_to_sq`
- EOF（result == 0 on Read）视为正常完成（返回已读字节），不视为错误——匹配 `LocalPageStore::get` 的 page tail 语义

### P1 — UringPageStore + 平台检测 ✅

| 文件 | 状态 | 说明 |
|---|---|---|
| `src/cache/store/uring/store.rs` | ✅ 完成 | `UringPageStore` 实现 `PageStore` trait（get/put/delete + identity） |
| `src/cache/store/uring/sys.rs` | ✅ 完成 | `is_uring_available()` — 编译时 + 运行时双重检测 |
| `src/cache/store/uring/mod.rs` | ✅ 完成 | 模块声明 + 共享工具（`hash_file_id`/`NUM_BUCKETS`/`io_error`） |

**关键设计决策：**
- `UringPageStore::get` 分配独立 `BytesMut` 缓冲区（参考 Lance），完成后 copy 到 `dst`（设计文档中 `BytesMut::from(dst)` 的方式会导致多余拷贝）
- `UringPageStore::put` 的 `rename` 使用 `std::fs::rename`（同步），因为 rename 不在 cache 命中热路径上
- Identity sidecar 操作使用 `tokio::fs`（不在热路径上），与 `LocalPageStore` 共享磁盘格式

### P2 — 集成 + 配置 ✅

| 文件 | 状态 | 说明 |
|---|---|---|
| `src/cache/store/mod.rs` | ✅ 完成 | `PageStore` trait 扩展（新增 `root_dir`/`write_identity`/`read_identity`/`delete_identity`） |
| `src/cache/store/local.rs` | ✅ 完成 | identity 方法从 inherent impl 移到 trait impl |
| `src/cache/manager.rs` | ✅ 完成 | `Vec<LocalPageStore>` → `Vec<Arc<dyn PageStore>>` + io_uring 降级逻辑 |
| `src/cache/options.rs` | ✅ 完成 | 新增 `uring_enabled`/`uring_queue_depth`/`uring_thread_count` |
| `src/config.rs` | ✅ 完成 | 新增 `client_cache_uring_*` 配置字段 + env vars + storage option keys + defaults |
| `Cargo.toml` | ✅ 完成 | `io-uring = "0.7"` (Linux-only, target-gated) |
| `src/cache/metrics.rs` | ✅ 完成 | 新增 7 个 io_uring metric 常量 |

**编译验证：**
- ✅ macOS (`cargo check`) — 零 warning，io_uring 代码被 `#[cfg(target_os = "linux")]` 隔离
- ✅ 全部 76 个 cache 单元测试通过（macOS 上只运行 `LocalPageStore` 路径）

### P3 — 性能基准 ✅

| 文件 | 状态 | 说明 |
|---|---|---|
| `benchmarks/cache_uring_bench.rs` | ✅ 完成 | io_uring vs tokio::fs 单线程 + 32 并发对比，local-only（无需集群） |

**基准设计：**
- 直接对 `PageStore` trait 调用 `get()`，无需 GooseFS 集群
- warm-up 预热 fd cache 后测量纯 cache-hit 路径
- 单线程：10^5 次 `get()`，记录 per-op 延迟 → ops/s + p50/p99
- 并发：32 个 tokio task 各 10^4 次 `get()` → 聚合 ops/s + p99
- macOS baseline（tokio::fs only）：单线程 ~32K ops/s，32 并发 ~61K ops/s
- Linux 预期（io_uring）：单线程 ~50K+ ops/s，32 并发 ~200K+ ops/s

### P4 — fd 缓存 + 批量提交优化 ✅

| 文件 | 状态 | 说明 |
|---|---|---|
| `src/cache/store/uring/store.rs` (fd cache) | ✅ 完成 | `LruCache<PageId, Arc<File>>` — cache-hit 读路径从 3 SQE 降为 1 SQE |

**fd 缓存设计要点：**
- `fd_cache: Mutex<LruCache<PageId, Arc<File>>>` — 容量 1024，LRU 淘汰
- `get_fd()` 方法：cache hit → 返回 `Arc::clone(&file)`（零 open）；miss → OP_OPENAT → `File::from_raw_fd` → 存入 cache
- `Arc<File>` 保证 fd 在并发读期间不会被关闭：即使 LRU 淘汰了 cache 条目，只要还有 `Arc<File>` 引用存在，`File::drop` 就不会关闭 fd
- `LruCache::put` 淘汰旧条目时，`Arc<File>` 的 `Drop` 自动关闭 fd（如果无其他引用）
- `delete()` 调用 `invalidate_fd()` 先从 cache 移除，避免读到 stale fd
- Unix 语义保证：unlink 一个有打开 fd 的文件是安全的——inode 在所有 fd 关闭后才释放

**cache-hit 读路径对比（P0-P3 vs P4）：**
- P0-P3: `OP_OPENAT` + `OP_READ` + `OP_CLOSE` = 3 SQE
- P4: `OP_READ`（使用 cached fd）= **1 SQE**

**新增测试：**
- `uring_fd_cache_repeated_reads` — 重复读同一 page 验证 fd 复用
- `uring_fd_cache_invalidation_on_delete` — delete 后 fd cache 失效，get 返回 0
- `uring_fd_cache_lru_eviction` — 超容量插入触发 LRU 淘汰，淘汰后重读仍正确

### P5 — 测试 + 文档 ✅ (部分)

| 文件 | 状态 | 说明 |
|---|---|---|
| `src/cache/store/uring/store.rs` 单测 | ✅ 完成 | 7 个测试（put/get roundtrip, offset, missing, short read, delete, concurrent, identity）— Linux-only |
| 交叉后端兼容性测试 | ⏳ | `test_cross_backend_compatibility` — 需要 Linux |
| 集成测试复用 | ✅ | 现有 `tests/page_cache_*.rs` 无需改动（trait object 透明切换） |

### 平台支持矩阵

| 平台 | 编译 | io_uring 后端 | 降级行为 |
|---|---|---|---|
| Linux 5.1+ | ✅ | ✅ 可用 | 默认启用，config 可关闭 |
| Linux < 5.1 | ✅ | ❌ 不可用 | 运行时检测 → `LocalPageStore` |
| macOS | ✅ | ❌ 不可用 | 编译时检测 → `LocalPageStore` |
| Windows | ✅ | ❌ 不可用 | 编译时检测 → `LocalPageStore` |

### P6 — `Bytes` 返回模型：完全消除 tmp 中间 buffer ✅

> 状态：**已实现**
>
> 日期：2026-07-10（设计） · 2026-07-11（实现 commit `0e9d67a`） · 2026-07-13（文档同步）
>
> 背景：128 并发性能分析（`docs/perf/2026-07-10-oncpu-concurrent-uring-analysis/README.md`）发现 `dst` 写入模型在 `read_through_cache` 层仍有 `tmp -> out` 拷贝和 per-page `Vec<u8>` 分配开销。
>
> 实现 commit：`0e9d67a` — "optimize page cache read path for io_uring"
> - 新增 Bytes-returning cache read APIs（`PageStore::get_bytes` / `CacheManager::get_bytes` + `get_batch_bytes`）
> - `read_through_cache` 批量 cache-hit 探测用 `join_all`，消除 per-page `JoinSet::spawn`
> - cache miss 正确性加固：拒绝 short external read，防止部分页被返回或持久化（§6.4）

#### 6.1 问题分析：原 `dst` 写入模型的数据流

P6 之前，`read_through_cache` 的数据流：

```
单 page 命中路径：
  io_uring OP_READ
    → kernel 写入 caller 提供的 tmp: Vec<u8>       （零拷贝：kernel → user）
    → out.extend_from_slice(&tmp)                    （1 次拷贝：tmp → out）
    → out.freeze() -> Bytes                          （零拷贝：wrap）
    → 返回给 Lance

多 page 命中路径（N pages）：
  for each page:
    JoinSet::spawn                                    （1 次 task spawn）
    tmp = vec![0u8; want]                             （1 次 Vec 分配）
    cache.get(page_id, offset, &mut tmp)              （kernel → tmp）
    out.extend_from_slice(&tmp)                       （1 次拷贝：tmp → out）
  out.freeze() -> Bytes
```

**开销**：
- 每 page 1 次 `Vec<u8>` 分配（~100-500ns，含 heap alloc + zero-fill）
- 每 page 1 次 `extend_from_slice` 拷贝（~want 字节的 memcpy）
- 多 page 时每 page 1 次 `JoinSet::spawn`（~1-5µs tokio 调度）

#### 6.2 目标：`Bytes` 返回模型

```
单 page 命中路径（优化后）：
  io_uring OP_READ
    → kernel 写入 BytesMut（store 内部分配）          （零拷贝：kernel → user）
    → freeze() -> Bytes                               （零拷贝：wrap）
    → 直接返回给 read_through_cache                    （零拷贝：单 page 无需组装）

多 page 命中路径（优化后）：
  cache.get_batch_bytes(page_requests)                 （join_all 并发）
    → Vec<Bytes>                                      （每个 Bytes 持有 io_uring buffer）
  for each page:
    chunks.push(cached_bytes)                          （零拷贝：直接 push）
  out.extend_from_slice(&chunk)                        （1 次拷贝：chunk → out）
  out.freeze() -> Bytes
```

**消除的开销**：
- 消除 per-page `Vec<u8>` 分配（`Bytes` 直接持有 io_uring 的 `BytesMut` buffer）
- 消除 per-page `JoinSet::spawn` 调度（批量接口下沉到 `CacheManager::get_batch_bytes`）
- 单 page 命中时消除 `extend_from_slice` 拷贝（直接返回 `Bytes` chunk，无组装）
- 多 page 仍保留 1 次 `extend_from_slice` 拷贝（chunk -> out），但这是组装最终 `Bytes` 的必要拷贝

#### 6.3 API 设计（已实现）

> **与原始设计的差异**：原设计提议 `get_bytes(&PageId) -> Option<Bytes>` 返回整页 + `PageStore::get_bytes_many` 做 io_uring 批量提交。实际实现采用更务实的签名 `get_bytes(&PageId, offset, len) -> Bytes`（空 = miss），并将批量并发放在 `CacheManager::get_batch_bytes` 层用 `join_all` 实现——避免在 `PageStore` 层引入未使用的批量 API（store.rs:502-505 注释说明了此决策）。

##### 6.3.1 `PageStore` trait 新增方法

```rust
// src/cache/store/mod.rs:49-66 (实际实现)

#[async_trait::async_trait]
pub trait PageStore: Send + Sync {
    // ... put / get / delete 等现有方法保持不变 ...

    /// Read bytes from a page and return them directly.
    ///
    /// Backends that naturally allocate their own read buffer (notably
    /// io_uring) should override this to avoid copying into a temporary caller
    /// buffer before returning to the cache layer.
    async fn get_bytes(&self, page_id: &PageId, offset: usize, len: usize) -> Result<Bytes> {
        // 默认实现：fallback 到 get() + Vec 分配
        if len == 0 {
            return Ok(Bytes::new());
        }
        let mut dst = vec![0u8; len];
        let n = self.get(page_id, offset, &mut dst).await?;
        if n == 0 {
            Ok(Bytes::new())
        } else {
            dst.truncate(n);
            Ok(Bytes::from(dst))
        }
    }

    // ... identity 方法 ...
}
```

**签名说明**：
- 返回 `Result<Bytes>`（不是 `Option<Bytes>`）：错误用 `Result` 表达，miss 用空 `Bytes` 表达（`bytes.is_empty() == true`）
- 接收 `offset` + `len`：支持 page 内子范围读取，调用方无需读取整页再切片
- 默认实现 fallback 到 `get()`：`LocalPageStore` 不覆写，走默认 `Vec` 路径（非热路径后端）

##### 6.3.2 `CacheManager` trait 新增方法

```rust
// src/cache/mod.rs:139-167 (实际实现)

#[async_trait::async_trait]
pub trait CacheManager: Send + Sync {
    // ... put / get / delete 等现有方法保持不变 ...

    /// Read bytes from a cached page and return the owned `Bytes` directly.
    ///
    /// The default implementation preserves the legacy `get` contract by
    /// reading into a caller-owned buffer. io_uring-backed implementations
    /// override this to return the kernel-filled buffer directly, avoiding one
    /// extra copy on cache hits.
    async fn get_bytes(&self, page_id: &PageId, page_offset: usize, len: usize) -> Bytes {
        // 默认实现：fallback 到 get() + Vec 分配
        if len == 0 {
            return Bytes::new();
        }
        let mut dst = vec![0u8; len];
        let n = self.get(page_id, page_offset, &mut dst).await;
        if n == 0 {
            Bytes::new()
        } else {
            dst.truncate(n);
            Bytes::from(dst)
        }
    }

    /// Read multiple cached pages. Each output corresponds to the request at
    /// the same index; an empty `Bytes` means miss or cache error.
    async fn get_batch_bytes(&self, requests: &[PageReadRequest]) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(requests.len());
        for req in requests {
            out.push(self.get_bytes(&req.page_id, req.page_offset, req.len).await);
        }
        out
    }

    // ... 其余方法 ...
}
```

**`PageReadRequest` 结构**（`src/cache/mod.rs:65-70`）：
```rust
#[derive(Debug, Clone)]
pub struct PageReadRequest {
    pub page_id: PageId,
    pub page_offset: usize,
    pub len: usize,
}
```

**签名说明**：
- `get_bytes` 返回 `Bytes`（不是 `Option<Bytes>`）：空 `Bytes` = miss，与 `get()` 返回 `0` 的语义对齐
- `get_batch_bytes` 接收 `&[PageReadRequest]`（含 offset + len），返回 `Vec<Bytes>`
- 默认 `get_batch_bytes` 是串行循环；`LocalCacheManager` 覆写为 `join_all` 并发

##### 6.3.3 `UringPageStore::get_bytes` 实现

```rust
// src/cache/store/uring/store.rs:584-645 (实际实现)

async fn get_bytes(&self, page_id: &PageId, offset: usize, len: usize) -> Result<Bytes> {
    if len == 0 {
        return Ok(Bytes::new());
    }

    // ── 热路径：page fd cache hit → 1 SQE (OP_READ only) ───────
    if let Some(entry) = PAGE_FD_CACHE.get(page_id).await {
        let fd = entry.fd;
        // `entry: Arc<PageFdEntry>` keeps the underlying `Arc<File>` alive
        // for the duration of the read, so the fd is guaranteed valid.
        let _entry = entry;

        return match self.read_with_fd(fd, offset, len).await {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                PAGE_FD_CACHE.invalidate(page_id).await;
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(Bytes::new())
                } else {
                    Err(io_error("uring read (page fd cache hit)", e))
                }
            }
        };
    }

    // ── 冷路径：page fd cache miss → dir fd cache + openat + read ─
    let dirfd = match self.get_dir_fd(&page_id.file_id).await {
        Ok(fd) => fd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Bytes::new()),
        Err(e) => return Err(io_error("uring open dir", e)),
    };

    let page_name = page_id.page_index.to_string();
    let fd = match self.openat_relative(dirfd, &page_name, libc::O_RDONLY).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Bytes::new()),
        Err(e) => return Err(io_error("uring open page", e)),
    };

    let read_bytes = match self.read_with_fd(fd, offset, len).await {
        Ok(bytes) => bytes,
        Err(e) => {
            self.close_fd_background(fd);
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(Bytes::new());
            }
            return Err(io_error("uring read", e));
        }
    };

    // SAFETY: `fd` was just successfully opened by io_uring and ownership
    // is transferred into `File`; moka closes it when the cache entry drops.
    let file = unsafe { std::fs::File::from_raw_fd(fd) };
    PAGE_FD_CACHE
        .insert(page_id.clone(), Arc::new(PageFdEntry::new(file)))
        .await;

    Ok(read_bytes)
}
```

**零拷贝核心**：`read_with_fd` → `new_read_request`（`store.rs:457-481`）分配 `BytesMut::with_capacity(len)` + `unsafe set_len(len)`，kernel 的 `OP_READ` 直接写入此 buffer，CQE 完成后 `UringOpFuture::poll`（`future.rs:52-54`）执行 `std::mem::take(&mut state.buffer).freeze()` 将 `BytesMut` 转为 `Bytes`——全程无额外拷贝。

```rust
// src/cache/store/uring/store.rs:457-481 — new_read_request（零拷贝 buffer 分配）
fn new_read_request(fd: RawFd, offset: usize, len: usize) -> Arc<IoRequest> {
    let mut buffer = BytesMut::with_capacity(len);
    // SAFETY: buffer has capacity for `len` bytes; io_uring writes into it
    // before the future exposes it as `Bytes`.
    unsafe {
        buffer.set_len(len);
    }

    Arc::new(IoRequest {
        fd,
        offset: offset as u64,
        length: len,
        op_type: UringOpType::Read,
        open_flags: 0,
        state: std::sync::Mutex::new(RequestState {
            completed: false,
            waker: None,
            err: None,
            buffer,                    // ← kernel 直接写入此 BytesMut
            bytes_transferred: 0,
            consumed: false,
            result_code: 0,
        }),
    })
}
```

**`UringPageStore::get` 改为委托 `get_bytes`**（`store.rs:575-582`）：
```rust
async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize> {
    let bytes = self.get_bytes(page_id, offset, dst.len()).await?;
    let n = bytes.len().min(dst.len());
    if n > 0 {
        dst[..n].copy_from_slice(&bytes[..n]);
    }
    Ok(n)
}
```

##### 6.3.4 `LocalCacheManager::get_bytes` + `get_batch_bytes` 实现

```rust
// src/cache/manager.rs:714-776 — get_bytes（实际实现）

async fn get_bytes(&self, page_id: &PageId, page_offset: usize, len: usize) -> Bytes {
    if self.state == CacheState::NotInUse {
        counter(mn::CLIENT_CACHE_GET_NOT_READY_ERRORS).inc(1);
        return Bytes::new();
    }
    if len == 0 {
        return Bytes::new();
    }

    let _rl = self.page_locks[page_lock_index(page_id)].read().await;

    // Phase C: lock-free DashMap read for dir_index + evictor.on_access
    let dir_index = match self.meta.get(page_id) {
        Some(info) => {
            // Check TTL (no-op when TTL is None).
            if let Some(ttl) = self.options.ttl {
                if info.created_at.elapsed() > ttl {
                    drop(info);
                    let _ = self.get_expired_path(page_id).await;
                    return Bytes::new();
                }
            }
            let di = info.dir_index;
            self.dirs[di].evictor.on_access(page_id);
            di
        }
        None => return Bytes::new(), // miss
    };

    // Disk IO — completely lock-free, delegates to PageStore::get_bytes
    let start = Instant::now();
    let bytes = match self.stores[dir_index]
        .get_bytes(page_id, page_offset, len)
        .await
    {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(error = %e, "get: failed to read page from store");
            counter(mn::CLIENT_CACHE_GET_STORE_READ_ERRORS).inc(1);
            counter(mn::CLIENT_CACHE_GET_ERRORS).inc(1);
            return Bytes::new();
        }
    };
    if bytes.is_empty() {
        return Bytes::new(); // racy eviction → miss
    }

    counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).inc(bytes.len() as i64);
    counter(mn::CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS).inc(start.elapsed().as_nanos() as i64);
    crate::cache::metrics::publish_hit_rate();
    bytes
}
```

```rust
// src/cache/manager.rs:778-785 — get_batch_bytes（实际实现，join_all 并发）

async fn get_batch_bytes(&self, requests: &[PageReadRequest]) -> Vec<Bytes> {
    join_all(
        requests
            .iter()
            .map(|req| self.get_bytes(&req.page_id, req.page_offset, req.len)),
    )
    .await
}
```

**设计决策**：`get_batch_bytes` 用 `join_all`（tokio 并发）而非下沉到 `PageStore::get_bytes_many` 做 io_uring 批量提交。原因（`store.rs:502-505` 注释）：
- `join_all` 已提供足够并发度，io_uring SQE 本身就是异步攒批提交（driver.rs 主循环）
- 避免在 `PageStore` trait 增加未使用的 `get_bytes_many` 抽象层
- 每个 `get_bytes` 独立持有 `Arc<PageFdEntry>`，fd 生命周期管理更简单

**`LocalCacheManager::get` 改为委托 `get_bytes`**（`manager.rs:705-712`）：
```rust
async fn get(&self, page_id: &PageId, page_offset: usize, dst: &mut [u8]) -> usize {
    let bytes = self.get_bytes(page_id, page_offset, dst.len()).await;
    let n = bytes.len().min(dst.len());
    if n > 0 {
        dst[..n].copy_from_slice(&bytes[..n]);
    }
    n
}
```

##### 6.3.5 `read_through_cache` 改造（实际实现）

```rust
// src/cache/caching_reader.rs:55-184 (实际实现)

pub async fn read_through_cache<R: ExternalRangeReader + ?Sized>(
    cache: &Arc<dyn CacheManager>,
    ext: &mut R,
    file_id: &Arc<str>,
    page_size: u64,
    file_length: i64,
    offset: i64,
    end: i64,
    fill_mode: FillMode,
) -> Result<Bytes> {
    let page_size = page_size.max(1);
    let requested_len = (end - offset).max(0) as usize;
    let mut cur = offset;
    let mut pages = Vec::new();

    // Phase 1: Compute page requests
    while cur < end {
        let page_index = (cur as u64) / page_size;
        let page_start = (page_index * page_size) as i64;
        let page_end = (((page_index + 1) * page_size) as i64).min(file_length);
        let in_page_off = (cur - page_start) as usize;
        let want = (end.min(page_end) - cur) as usize;
        pages.push((
            PageId::new(file_id.clone(), page_index),
            page_index, page_start, page_end, in_page_off, want,
        ));
        cur += want as i64;
    }

    // Phase 2: Batch cache read via get_batch_bytes (eliminates JoinSet)
    let cache_requests: Vec<PageReadRequest> = pages
        .iter()
        .map(|(page_id, _, _, _, in_page_off, want)| PageReadRequest {
            page_id: page_id.clone(),
            page_offset: *in_page_off,
            len: *want,
        })
        .collect();
    let mut cached = cache.get_batch_bytes(&cache_requests).await;
    if cached.len() != pages.len() {
        cached = vec![Bytes::new(); pages.len()];
    }

    // Phase 3: Assemble output — collect chunks first
    let mut chunks: Vec<Bytes> = Vec::with_capacity(pages.len());
    for ((page_id, page_index, page_start, page_end, in_page_off, want), cached_bytes) in
        pages.into_iter().zip(cached.into_iter())
    {
        // 1) Cache hit: keep the returned Bytes directly. For the io_uring
        // backend this is the kernel-filled buffer, so single-page reads avoid
        // the old tmp-buffer copy entirely.
        if cached_bytes.len() == want {
            chunks.push(cached_bytes);   // ← 零拷贝：直接 push
            continue;
        }

        // 2) Miss → read the whole page from the external source.
        let ext_start = Instant::now();
        let page_bytes = ext.read_range(page_start, page_end).await?;
        counter(metric_name::CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS)
            .inc(ext_start.elapsed().as_nanos() as i64);
        counter(metric_name::CLIENT_CACHE_BYTES_READ_EXTERNAL).inc(page_bytes.len() as i64);
        counter(metric_name::CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL).inc(page_end - page_start);
        crate::cache::metrics::publish_hit_rate();

        // ── cache miss 正确性加固（commit 0e9d67a）──
        // 拒绝 short external read：如果 worker/UFS 返回的字节数少于
        // 预期的 page 范围 (page_end - page_start)，直接返回错误，
        // 防止部分页被返回给调用方或被回填到缓存中。
        let expected_page_len = (page_end - page_start) as usize;
        if page_bytes.len() < expected_page_len {
            return Err(Error::Internal {
                message: format!(
                    "read_through_cache: short external read for page {}: got {} of {} bytes",
                    page_index,
                    page_bytes.len(),
                    expected_page_len
                ),
                source: None,
            });
        }
        // 如果 external 返回的字节多于预期（如对齐读取），截断到预期长度。
        let page_bytes = if page_bytes.len() > expected_page_len {
            page_bytes.slice(0..expected_page_len)
        } else {
            page_bytes
        };

        // 3) Back-fill per the fill mode (best-effort).
        if !page_bytes.is_empty() {
            match fill_mode {
                FillMode::None => {}
                FillMode::Sync => {
                    cache.put(&page_id, page_bytes.clone()).await;
                }
                FillMode::Async => {
                    Arc::clone(cache).schedule_fill(page_id.clone(), page_bytes.clone());
                }
            }
        }

        // 4) Return the requested slice from the freshly read page.
        let avail = page_bytes.len();
        let s = in_page_off.min(avail);
        let e = (in_page_off + want).min(avail);
        let advanced = (e - s) as i64;
        if advanced == 0 {
            return Err(Error::Internal {
                message: format!(
                    "read_through_cache: 0 bytes for page {} (cur={}, end={})",
                    page_index,
                    page_start + in_page_off as i64,
                    end
                ),
                source: None,
            });
        }
        chunks.push(page_bytes.slice(s..e));
    }

    // Phase 4: Return — single chunk = zero-copy, multi-chunk = one assemble copy
    if chunks.is_empty() {
        return Ok(Bytes::new());
    }
    if chunks.len() == 1 {
        return Ok(chunks.pop().unwrap());   // ← 单 page hit：零拷贝返回
    }

    let mut out = BytesMut::with_capacity(requested_len);
    for chunk in chunks {
        out.extend_from_slice(&chunk);      // ← 多 page：1 次组装拷贝
    }
    Ok(out.freeze())
}
```

**关键优化点**：
1. **单 page hit 零拷贝**：`chunks.len() == 1` 时直接 `chunks.pop().unwrap()` 返回，无 `extend_from_slice`
2. **多 page hit 零 `Vec` 分配**：每个 chunk 是 `Bytes`（持有 io_uring buffer），无 per-page `Vec<u8>` 分配
3. **零 `JoinSet::spawn`**：`get_batch_bytes` 内部用 `join_all`，无 per-page task spawn

#### 6.4 cache miss 正确性加固：拒绝 short external read

> 来源：commit `0e9d67a` — "Harden cache miss correctness by rejecting short external page reads before slicing or filling the cache, preventing partial pages from being returned or persisted."

**问题**：P6 之前，`read_through_cache` 在 cache miss 时调用 `ext.read_range(page_start, page_end)` 读取整页。如果 worker/UFS 返回的字�数少于预期的 page 范围（`page_end - page_start`），旧代码会静默地用部分数据组装返回值，并可能将**不完整的页**回填到缓存中——后续命中该页的读取会得到截断的数据，违反 INV-PC-D1（cache vs direct byte diff）。

**修复**（`caching_reader.rs:122-138`）：在 external read 返回后、切片和回填之前，显式检查长度：

```rust
// src/cache/caching_reader.rs:122-138
let expected_page_len = (page_end - page_start) as usize;
if page_bytes.len() < expected_page_len {
    return Err(Error::Internal {
        message: format!(
            "read_through_cache: short external read for page {}: got {} of {} bytes",
            page_index,
            page_bytes.len(),
            expected_page_len
        ),
        source: None,
    });
}
// 如果 external 返回的字节多于预期（如对齐读取），截断到预期长度。
let page_bytes = if page_bytes.len() > expected_page_len {
    page_bytes.slice(0..expected_page_len)
} else {
    page_bytes
};
```

**三道防线**：

| 检查 | 触发条件 | 行为 | 防护目标 |
|------|---------|------|---------|
| `page_bytes.len() < expected_page_len` | worker/UFS 返回不足 | 返回 `Error::Internal` | 防止部分页返回给调用方 |
| 同上 | 同上 | 不执行 `cache.put` / `schedule_fill` | 防止部分页被持久化到缓存 |
| `page_bytes.len() > expected_page_len` | worker/UFS 返回过多（对齐读取） | `page_bytes.slice(0..expected_page_len)` | 防止多余字节污染回填 |

**与 INV-PC-S1 的关系**：`CLIENT_PAGE_CACHE_DESIGN.md` §1.4 的 INV-PC-S1 要求"failed fill doesn't poison cache"。此修复将 short external read 视为 fill 失败——错误返回，不回填缓存，下次 `get` miss → 重新回源正确读取。这与 `OP_WRITE` 失败、`OP_RENAMEAT` 失败、SQ 满三种已有失败路径的语义一致。

**测试**（`caching_reader.rs:361-389`）：

```rust
struct ShortExternal { data: Vec<u8> }

#[async_trait::async_trait]
impl ExternalRangeReader for ShortExternal {
    async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes> {
        let s = offset as usize;
        let e = (end as usize).min(self.data.len()).saturating_sub(1); // ← 少读 1 字节
        Ok(Bytes::copy_from_slice(&self.data[s..e]))
    }
}

#[tokio::test]
async fn short_external_page_read_errors_and_does_not_fill_cache() {
    // ShortExternal 返回的 page_bytes.len() < expected_page_len
    // → read_through_cache 返回 "short external read" 错误
    // → cache 中不残留部分页（后续 get 返回 0 = miss）
    let err = read_through_cache(...).await.unwrap_err();
    assert!(format!("{}", err).contains("short external read"));

    let mut dst = vec![0u8; 8];
    assert_eq!(cache.get(&PageId::new(file_id.clone(), 0), 0, &mut dst).await, 0);
}
```

#### 6.5 消除的拷贝/分配对比

| 场景 | P6 之前（dst 模型） | P6 之后（Bytes 模型） | 消除 |
|------|---------------------|----------------------|------|
| 单 page hit | `kernel->tmp` + `tmp->out` + `Vec alloc` | `kernel->BytesMut` + `freeze` + 直接返回 | `Vec alloc` + `extend_from_slice` 拷贝 |
| N page hit | N x (`kernel->tmp` + `tmp->out` + `Vec alloc` + `spawn`) | N x (`kernel->BytesMut` + `freeze`) + `join_all` + 1 x assemble | `N x Vec alloc` + `N x spawn` + N-1 x `extend_from_slice` |
| 单 page miss | 不变 | 不变 | — |

**每 page 节省**：
- `Vec<u8>` 分配：~100-500ns（heap alloc + zero-fill `want` bytes）
- `JoinSet::spawn`：~1-5µs（tokio task 创建 + 调度）

**128 并发 x 多 page 场景累计节省**：每 query 若跨 4 pages，节省 4 x (500ns + 3µs) = 14µs/query。128 并发下每秒 ~10K queries，累计 ~140ms/s CPU 节省。

#### 6.6 兼容性设计（已实现）

| 维度 | 实现 |
|------|------|
| `PageStore` trait | 新增 `get_bytes(page_id, offset, len) -> Result<Bytes>`，默认实现 fallback 到 `get()` + `Vec` |
| `CacheManager` trait | 新增 `get_bytes(page_id, offset, len) -> Bytes` + `get_batch_bytes(requests) -> Vec<Bytes>`，默认实现 fallback 到 `get()` |
| `LocalPageStore` | **不覆写** `get_bytes`——走默认 `Vec` 路径（非热路径后端，macOS/降级场景） |
| `UringPageStore` | 覆写 `get_bytes`——用 `read_with_fd` 返回 `Bytes`（零拷贝，kernel 直写 `BytesMut`） |
| `LocalCacheManager` | 覆写 `get_bytes`（委托 `PageStore::get_bytes`）+ `get_batch_bytes`（`join_all` 并发） |
| `DisabledCacheManager` | 不覆写——默认 `get_bytes` 调 `get()` 返回 0 → 空 `Bytes`；`get_batch_bytes` 默认串行循环返回全空 |
| `read_through_cache` | 改用 `get_batch_bytes` + `chunks` 组装；单 page hit 零拷贝返回；short external read 拒绝（§6.4） |
| `UringPageStore::get` | 改为委托 `get_bytes` + `copy_to_slice`（保持 `dst` 接口兼容） |
| `LocalCacheManager::get` | 改为委托 `get_bytes` + `copy_to_slice`（保持 `dst` 接口兼容） |
| 现有测试 | 不受影响（`get()` / `get_bytes()` 接口均保留，`get` 内部委托 `get_bytes`） |

#### 6.7 实现清单

| # | 改动 | 文件 | 状态 |
|---|------|------|------|
| 1 | `PageStore::get_bytes` trait 方法 + 默认实现 | `src/cache/store/mod.rs:49-66` | ✅ |
| 2 | `CacheManager::get_bytes` + `get_batch_bytes` trait 方法 + 默认实现 | `src/cache/mod.rs:139-167` | ✅ |
| 3 | `PageReadRequest` 结构 | `src/cache/mod.rs:65-70` | ✅ |
| 4 | `UringPageStore::get_bytes` 实现（page fd cache 热路径 + dir fd cache 冷路径） | `src/cache/store/uring/store.rs:584-645` | ✅ |
| 5 | `UringPageStore::get` 改为委托 `get_bytes` | `src/cache/store/uring/store.rs:575-582` | ✅ |
| 6 | `new_read_request` + `read_with_fd` + `wait_read_request` 零拷贝 buffer 分配 | `src/cache/store/uring/store.rs:457-500` | ✅ |
| 7 | `LocalCacheManager::get_bytes` 实现（lock-free meta + 委托 store） | `src/cache/manager.rs:714-776` | ✅ |
| 8 | `LocalCacheManager::get_batch_bytes` 实现（`join_all` 并发） | `src/cache/manager.rs:778-785` | ✅ |
| 9 | `LocalCacheManager::get` 改为委托 `get_bytes` | `src/cache/manager.rs:705-712` | ✅ |
| 10 | `read_through_cache` 改造（`get_batch_bytes` + `chunks` + 单 page 零拷贝） | `src/cache/caching_reader.rs:55-184` | ✅ |
| 11 | short external read 拒绝 + over-read 截断（cache miss 正确性加固） | `src/cache/caching_reader.rs:122-138` | ✅ |
| 12 | `futures = "0.3"` 从 dev-dependencies 移到 dependencies（`join_all` 热路径依赖） | `Cargo.toml` | ✅ |

> 实现来源：commit `0e9d67a` — "optimize page cache read path for io_uring"

#### 6.8 测试覆盖

| 测试 | 文件 | 验证内容 |
|------|------|---------|
| `uring_put_get_roundtrip` | `src/cache/store/uring/store.rs` | put + get 基本往返（`get` 内部走 `get_bytes`） |
| `uring_get_short_read_at_tail` | `src/cache/store/uring/store.rs` | page tail short read（`get_bytes` 返回部分字节） |
| `uring_page_fd_cache_hit_after_first_read` | `src/cache/store/uring/store.rs` | 首次 get 后 page fd cache 命中 |
| `uring_get_batch_concurrent` | `src/cache/store/uring/store.rs` | `get_batch` 批量读 8 pages（内部调 `get_bytes`） |
| `cold_read_misses_then_warm_read_hits` | `src/cache/caching_reader.rs` | `read_through_cache` 冷 miss + 热 hit（走 `get_batch_bytes`） |
| `spans_multiple_pages_and_partial_offsets` | `src/cache/caching_reader.rs` | 跨 page 边界 + 非对齐 offset（多 chunk 组装） |
| `short_external_page_read_errors_and_does_not_fill_cache` | `src/cache/caching_reader.rs` | short external read 返回错误 + 缓存不残留部分页（§6.4 加固） |
| `inv_pc_d1_cache_vs_direct_byte_diff` | `tests/page_cache_consistency.rs` | cache vs direct 字节一致性（P6 路径） |

### 下一步

1. **在 Linux 环境编译验证**：`cargo test --lib cache::store::uring` 运行 io_uring 单测（含 fd cache + get_bytes 测试）
2. **P3 性能基准**：`benchmarks/cache_uring_bench.rs` 对比 io_uring vs tokio::fs（含 fd cache hit/miss 场景）
3. **交叉后端测试**：验证 tokio::fs 写入 → io_uring 读取（反向亦然）
4. **P6 性能验证**：128 并发火焰图对比，确认 per-page `Vec` 分配和 `JoinSet` 调度已消除
