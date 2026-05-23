//! 复现 opendal `test_writer_write_with_concurrent` 失败：
//!
//!   "WriteBlock server error for block_id=...: status: Internal,
//!    message: \"The file length is inconsistent with the amount of
//!    data that has been written\""
//!
//! ## 关键复现条件（从 1.log 分析得出）
//!
//! libtest_mimic 默认并行跑 behavior tests，从日志可以看到：
//!   15:40:19.713 path=A: write started
//!   15:40:20.116 path=B: write started   <-- 在 A 还未 close 时
//!   15:40:20.235 path=A: write close failed
//!
//! 也就是说：**多个 writer 共用同一个 FileSystemContext，并发写不同
//! 文件**，才能触发服务端的 block 长度统计错乱。单 writer 串行跑无
//! 法复现。
//!
//! 复现条件：
//!   * 同一个 FileSystemContext
//!   * write_type = MUST_CACHE
//!   * N 个并发任务，每个任务执行 3 次 write（5..6 MiB 随机长度），
//!     最后 close
//!   * 路径放在根目录（与 opendal `op.write(uuid, ...)` 一致）
//!
//! 用法：
//!   cargo run --example repro_writer_write_with_concurrent
//!   cargo run --example repro_writer_write_with_concurrent -- 20 8
//!         # 20 轮，每轮 8 并发
//!
//! 环境变量（对照实验）：
//!   * SHARED=0|1   每个 task 是否共享 FileSystemContext（默认 1）
//!   * ALIGN=0|1    数据是否严格对齐到 1 MiB（默认 0；ALIGN=1 时触发
//!                  条件 2 失效，预期 0 失败）
//!   * WP=must_cache|cache_through|through|async_through
//!                  WritePType 模式对照（默认 must_cache）：
//!                    - must_cache    仅 cache 流（GOOSEFS_BLOCK），命中 bug
//!                    - cache_through cache + UFS 双流，命中 bug 且压力翻倍
//!                    - through       仅 UFS 流（UFS_FILE），完全绕过 bug
//!                    - async_through cache 流 + close 后异步 persist
//!
//! 前置：本机已启动 GooseFS 集群，Master 监听 127.0.0.1:9200。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::GoosefsFileWriter;
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use goosefs_sdk::WritePType;
use rand::Rng;

const MIN_SIZE: usize = 5 * 1024 * 1024;
const MAX_SIZE: usize = 6 * 1024 * 1024;
// 通过环境变量 ALIGN=1 启用：写入总字节数严格对齐到 CHUNK_SIZE。
// 默认（ALIGN=0）模拟 opendal 测试随机大小、最后一个 chunk 部分写入。
const CHUNK_SIZE: usize = 1024 * 1024;

fn gen_bytes_with_range(min: usize, max: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut size = rng.gen_range(min..max);
    let align = std::env::var("ALIGN").ok().as_deref() == Some("1");
    if align {
        size -= size % CHUNK_SIZE;
        if size < CHUNK_SIZE {
            size = CHUNK_SIZE;
        }
    }
    let mut buf = vec![0u8; size];
    rng.fill(&mut buf[..]);
    buf
}

fn make_root_path() -> String {
    // 与 opendal TEST_FIXTURE.new_file_path() 等价：纯 UUID（根目录）
    // 这里不引入 uuid 依赖，用 nanos+counter+pid 凑一个唯一名。
    static C: AtomicUsize = AtomicUsize::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    format!("/repro-{}-{}-{}.bin", std::process::id(), n, nanos)
}

/// 解析 WP 环境变量到 WritePType。默认 MUST_CACHE。
fn resolve_write_ptype() -> (WritePType, &'static str) {
    match std::env::var("WP")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        Some("cache_through") | Some("cache-through") => {
            (WritePType::CacheThrough, "CACHE_THROUGH")
        }
        Some("through") => (WritePType::Through, "THROUGH"),
        Some("async_through") | Some("async-through") => {
            (WritePType::AsyncThrough, "ASYNC_THROUGH")
        }
        Some("must_cache") | Some("must-cache") | None => (WritePType::MustCache, "MUST_CACHE"),
        Some(other) => {
            panic!("unknown WP={other}; expected must_cache|cache_through|through|async_through")
        }
    }
}

/// 模拟 opendal goosefs writer 的 tmp_path
fn make_tmp_path(path: &str) -> String {
    let (dir, base) = match path.rfind('/') {
        Some(idx) => (&path[..idx], &path[idx + 1..]),
        None => ("", path),
    };
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    if dir.is_empty() {
        // path 形如 "/foo"，rfind 命中开头的 '/'，dir=="" base=="foo"
        format!("/.opendal.tmp.{pid}.{nanos}.{base}")
    } else {
        format!("{dir}/.opendal.tmp.{pid}.{nanos}.{base}")
    }
}

/// 单个 worker：模拟 opendal `test_writer_write_with_concurrent` 一次完整流程：
///   - tmp_path 上写 3 段
///   - close
///   - rename 到最终 path
///
/// 如果 `shared_ctx` 为 None，则任务自己建 ctx（不与他人共享）。
async fn one_writer_round(
    shared_ctx: Option<Arc<FileSystemContext>>,
    idx: usize,
    write_ptype: WritePType,
) -> std::result::Result<(usize, usize), (usize, String)> {
    let ctx = match shared_ctx {
        Some(c) => c,
        None => {
            let cfg = GoosefsConfig::new("127.0.0.1:9200");
            match FileSystemContext::connect(cfg).await {
                Ok(c) => c,
                Err(e) => return Err((idx, format!("private connect failed: {e}"))),
            }
        }
    };
    let path = make_root_path();
    let tmp = make_tmp_path(&path);

    let options = CreateFilePOptions {
        write_type: Some(write_ptype as i32),
        recursive: Some(true),
        ..Default::default()
    };

    let mut writer =
        match GoosefsFileWriter::create_with_context(ctx.clone(), &tmp, Some(options)).await {
            Ok(w) => w,
            Err(e) => return Err((idx, format!("create_with_context failed: {e}"))),
        };

    let a = gen_bytes_with_range(MIN_SIZE, MAX_SIZE);
    let b = gen_bytes_with_range(MIN_SIZE, MAX_SIZE);
    let c = gen_bytes_with_range(MIN_SIZE, MAX_SIZE);
    let total = a.len() + b.len() + c.len();

    if let Err(e) = writer.write(&a).await {
        return Err((idx, format!("write A failed: {e}")));
    }
    if let Err(e) = writer.write(&b).await {
        return Err((idx, format!("write B failed: {e}")));
    }
    if let Err(e) = writer.write(&c).await {
        return Err((idx, format!("write C failed: {e}")));
    }

    if let Err(e) = writer.close().await {
        // 模拟 opendal 失败时清理 tmp
        let master = ctx.acquire_master();
        let _ = master.delete(&tmp, false).await;
        return Err((idx, format!("close failed (total={total}): {e}")));
    }

    // 成功：rename tmp -> path（与 opendal finalize_rename 一致）
    let master = ctx.acquire_master();
    if let Err(e) = master.rename(&tmp, &path).await {
        return Err((idx, format!("rename failed: {e}")));
    }
    let _ = master.delete(&path, false).await; // cleanup
    Ok((idx, total))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let rounds: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(20);
    let concurrency: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    // SHARED=0 则每个任务自建 ctx；SHARED=1（默认）则共享一个 ctx。
    let shared = std::env::var("SHARED")
        .ok()
        .map(|v| v != "0")
        .unwrap_or(true);
    let align = std::env::var("ALIGN").ok().as_deref() == Some("1");
    let (write_ptype, wp_label) = resolve_write_ptype();

    println!("===============================================================");
    println!(" Reproducer: opendal test_writer_write_with_concurrent failure");
    println!(
        " rounds = {}, concurrency-per-round = {}, shared_ctx = {}",
        rounds, concurrency, shared
    );
    println!(" write_type = {}, align_to_chunk = {}", wp_label, align);
    println!("===============================================================");

    println!("\n[0] Connecting to GooseFS master at 127.0.0.1:9200 ...");
    let shared_ctx: Option<Arc<FileSystemContext>> = if shared {
        let config = GoosefsConfig::new("127.0.0.1:9200");
        let ctx = FileSystemContext::connect(config).await?;
        println!("    ✅ FileSystemContext connected (shared across all tasks)");
        Some(ctx)
    } else {
        println!("    (each task will build its own FileSystemContext)");
        None
    };

    let mut total_runs = 0usize;
    let mut total_fails = 0usize;
    let mut first_err: Option<String> = None;

    for r in 0..rounds {
        let mut handles = Vec::with_capacity(concurrency);
        for i in 0..concurrency {
            let ctx_clone = shared_ctx.clone();
            let task_idx = r * concurrency + i;
            handles.push(tokio::spawn(one_writer_round(
                ctx_clone,
                task_idx,
                write_ptype,
            )));
        }

        let mut round_fails = 0;
        for h in handles {
            total_runs += 1;
            match h.await {
                Ok(Ok((_idx, _bytes))) => {}
                Ok(Err((idx, msg))) => {
                    total_fails += 1;
                    round_fails += 1;
                    if first_err.is_none() {
                        first_err = Some(format!("task {idx}: {msg}"));
                    }
                    if round_fails <= 3 {
                        println!("    ❌ task {idx}: {msg}");
                    }
                }
                Err(join_err) => {
                    total_fails += 1;
                    round_fails += 1;
                    println!("    ❌ join error: {join_err}");
                }
            }
        }
        println!(
            "[round {:>3}] tasks={}, failures={}",
            r + 1,
            concurrency,
            round_fails
        );

        // 提前结束以加快诊断
        if total_fails >= 3 {
            println!("    >>> reached 3 failures, stopping early");
            break;
        }
    }

    if let Some(ctx) = shared_ctx {
        ctx.close().await?;
    }

    println!("\n===============================================================");
    println!(
        " total_runs = {}, total_failures = {}",
        total_runs, total_fails
    );
    if let Some(e) = &first_err {
        println!(" first error: {e}");
    }
    println!("===============================================================");

    if total_fails > 0 {
        std::process::exit(1);
    }
    Ok(())
}
