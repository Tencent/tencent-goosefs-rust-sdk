//! Async persistence scheduling example.
//!
//! Demonstrates two approaches to async persistence:
//!
//! 1. **ASYNC_THROUGH mode**: Uses `GooseFsFileWriter` with `WritePType::AsyncThrough`.
//!    Data is written to Worker cache first, then `close()` automatically triggers
//!    async persistence — no manual scheduling needed.
//!
//! 2. **MUST_CACHE + manual schedule**: Uses `GooseFsFileWriter` with the default
//!    `WritePType::MustCache` to write data to cache, then manually calls
//!    `MasterClient::schedule_async_persistence()` to trigger persistence.
//!
//! WriteType controls where data is physically persisted:
//!
//! | WriteType        | Worker cache | UFS (COS/S3/HDFS) |
//! |------------------|--------------|--------------------|
//! | MUST_CACHE       | ✅ (default)  | ❌                 |
//! | CACHE_THROUGH    | ✅            | ✅ (sync on close)  |
//! | THROUGH          | ❌            | ✅ (direct)         |
//! | ASYNC_THROUGH    | ✅            | ✅ (async after close) |
//!
//! Usage:
//!   cargo run --example async_persistence

use goosefs_client::client::MasterClient;
use goosefs_client::config::GooseFsConfig;
use goosefs_client::error::Result;
use goosefs_client::io::GooseFsFileWriter;
use goosefs_client::WritePType;

#[tokio::main]
async fn main() -> Result<()> {
    println!("GooseFS 异步持久化演示");
    println!("======================");

    let base_config = GooseFsConfig::new("127.0.0.1:9200");

    // ── Step 0: Cleanup & create directory ────────────────────────
    println!("\n0. 清理已存在的测试目录...");
    let master = MasterClient::connect(&base_config).await?;
    match master.delete("/persisted-demo", true).await {
        Ok(_) => println!("  已删除旧的测试目录"),
        Err(_) => println!("  旧目录不存在，跳过"),
    }
    master.create_directory("/persisted-demo", true).await?;
    println!("  目录 /persisted-demo 创建成功");

    // ── Step 1: ASYNC_THROUGH mode ───────────────────────────────
    println!("\n━━━ 1. ASYNC_THROUGH 模式（自动异步持久化）━━━");
    println!("  数据先写入 Worker 缓存，close() 时自动调度异步持久化到 UFS。");
    {
        let config = GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::AsyncThrough);

        let content = b"This file is written with ASYNC_THROUGH mode.\n\
                        Data goes to Worker cache first, then persisted to UFS asynchronously.\n\
                        GooseFS Rust Client async persistence demo.";

        let bytes_written =
            GooseFsFileWriter::write_file(&config, "/persisted-demo/async_through.txt", content)
                .await?;
        println!("  ✅ 写入完成: {} 字节", bytes_written);

        // Check initial status
        let info = master
            .get_status("/persisted-demo/async_through.txt")
            .await?;
        println!("  初始状态:");
        println!("    持久化: {:?}", info.persisted.unwrap_or(false));
        println!("    持久化状态: {:?}", info.persistence_state);
    }

    // ── Step 2: MUST_CACHE + manual schedule ─────────────────────
    println!("\n━━━ 2. MUST_CACHE + 手动调度持久化 ━━━");
    println!("  数据仅写入 Worker 缓存，然后手动调度异步持久化。");
    {
        let config = GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::MustCache);

        let content = b"This file is written with MUST_CACHE mode.\n\
                        After writing, we manually schedule async persistence.\n\
                        GooseFS Rust Client manual persistence demo.";

        let bytes_written =
            GooseFsFileWriter::write_file(&config, "/persisted-demo/manual_persist.txt", content)
                .await?;
        println!("  ✅ 写入完成: {} 字节", bytes_written);

        // Check status before scheduling
        let info_before = master
            .get_status("/persisted-demo/manual_persist.txt")
            .await?;
        println!("  持久化前状态:");
        println!("    持久化: {:?}", info_before.persisted.unwrap_or(false));
        println!("    持久化状态: {:?}", info_before.persistence_state);

        // Manually schedule async persistence
        println!("  调度异步持久化...");
        master
            .schedule_async_persistence("/persisted-demo/manual_persist.txt", None)
            .await?;
        println!("  ✅ 持久化已调度");
    }

    // ── Step 3: Wait and check final status ──────────────────────
    println!("\n━━━ 3. 等待持久化完成 ━━━");
    println!("  等待 15 秒让 JobWorker 完成持久化...");
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    // Check final status for both files
    let files = [
        "/persisted-demo/async_through.txt",
        "/persisted-demo/manual_persist.txt",
    ];

    let mut all_persisted = true;
    for path in &files {
        let info = master.get_status(path).await?;
        let persisted = info.persisted.unwrap_or(false);
        let state = info.persistence_state.as_deref().unwrap_or("unknown");
        println!("  {} — persisted={}, state={}", path, persisted, state);
        if !persisted {
            all_persisted = false;
        }
    }

    if all_persisted {
        println!("\n✅ 所有文件已成功持久化到 UFS!");
    } else {
        println!("\n⚠️  部分文件尚未持久化，可能需要更多时间。");
        println!("  请使用以下命令检查状态:");
        println!("  ./bin/goosefs fs ls /persisted-demo");
    }

    // ── Step 4: List directory contents ──────────────────────────
    println!("\n━━━ 4. 目录内容 ━━━");
    let entries = master.list_status("/persisted-demo", false).await?;
    println!("  /persisted-demo 包含 {} 个条目:", entries.len());
    for entry in &entries {
        println!(
            "  - {} ({} 字节, persisted={})",
            entry.path.as_deref().unwrap_or("unknown"),
            entry.length.unwrap_or(0),
            entry.persisted.unwrap_or(false),
        );
    }

    println!("\n✅ 异步持久化演示完成!");
    Ok(())
}
