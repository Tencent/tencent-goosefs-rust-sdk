//! Demonstrates the **4 write types** supported by GooseFS.
//!
//! Each write type controls where data is physically persisted:
//!
//! | WriteType        | Worker cache | UFS (COS/S3/HDFS) |
//! |------------------|--------------|--------------------|
//! | MUST_CACHE       | ✅ (default)  | ❌                 |
//! | CACHE_THROUGH    | ✅            | ✅ (sync on close)  |
//! | THROUGH          | ❌            | ✅ (direct)         |
//! | ASYNC_THROUGH    | ✅            | ✅ (async after close) |
//!
//! Usage:
//!   cargo run --example write_types

use goosefs_client::client::MasterClient;
use goosefs_client::config::GooseFsConfig;
use goosefs_client::error::Result;
use goosefs_client::io::{GooseFsFileReader, GooseFsFileWriter};
use goosefs_client::WritePType;

#[tokio::main]
async fn main() -> Result<()> {
    println!("GooseFS WriteType 写入模式演示");
    println!("================================");

    let base_config = GooseFsConfig::new("127.0.0.1:9200");

    // 初始化：清理 & 创建测试目录
    let master = MasterClient::connect(&base_config).await?;
    match master.delete("/write-type-demo", true).await {
        Ok(_) => println!("已清理旧的测试目录"),
        Err(_) => {}
    }
    master.create_directory("/write-type-demo", true).await?;
    println!("测试目录 /write-type-demo 已创建\n");

    // ────────────────────────────────────────────────────────
    // 1. MUST_CACHE — 仅写入 Worker 缓存，不持久化到 UFS
    // ────────────────────────────────────────────────────────
    println!("━━━ 1. MUST_CACHE（默认）━━━");
    println!("  数据仅写入 Worker 缓存，不持久化到底层存储。");
    {
        let config = GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::MustCache);

        let data = b"MUST_CACHE: data lives only in GooseFS cache.";
        let bytes =
            GooseFsFileWriter::write_file(&config, "/write-type-demo/must_cache.txt", data).await?;
        println!("  ✅ 写入完成: {} 字节", bytes);

        // 回读验证
        let read = GooseFsFileReader::read_file(&config, "/write-type-demo/must_cache.txt").await?;
        assert_eq!(read.as_ref(), data.as_slice());
        println!("  ✅ 读取验证通过");
    }

    // ────────────────────────────────────────────────────────
    // 2. CACHE_THROUGH — 写缓存 + Master 在 CompleteFile 时同步持久化
    // ────────────────────────────────────────────────────────
    println!("\n━━━ 2. CACHE_THROUGH ━━━");
    println!("  数据写入缓存，Master 在 CompleteFile 时自动同步持久化到 UFS。");
    {
        let config = GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::CacheThrough);

        let data = b"CACHE_THROUGH: written to cache, Master syncs to UFS on CompleteFile.";
        let bytes =
            GooseFsFileWriter::write_file(&config, "/write-type-demo/cache_through.txt", data)
                .await?;
        println!("  ✅ 写入完成: {} 字节", bytes);

        let read =
            GooseFsFileReader::read_file(&config, "/write-type-demo/cache_through.txt").await?;
        assert_eq!(read.as_ref(), data.as_slice());
        println!("  ✅ 读取验证通过");
    }

    // ────────────────────────────────────────────────────────
    // 3. THROUGH — 直写 UFS，跳过缓存
    // ────────────────────────────────────────────────────────
    println!("\n━━━ 3. THROUGH ━━━");
    println!("  数据直接写入 UFS（COS/S3/HDFS），不经过缓存。");
    println!("  Worker 使用 UfsFile + CreateUfsFileOptions 完成写入。");
    {
        let config = GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::Through);

        let data = b"THROUGH: data written directly to UFS, bypassing cache.";
        let bytes =
            GooseFsFileWriter::write_file(&config, "/write-type-demo/through.txt", data).await?;
        println!("  ✅ 写入完成: {} 字节", bytes);

        let read = GooseFsFileReader::read_file(&config, "/write-type-demo/through.txt").await?;
        assert_eq!(read.as_ref(), data.as_slice());
        println!("  ✅ 读取验证通过");
    }

    // ────────────────────────────────────────────────────────
    // 4. ASYNC_THROUGH — 写缓存，close() 后自动调度异步持久化
    // ────────────────────────────────────────────────────────
    println!("\n━━━ 4. ASYNC_THROUGH ━━━");
    println!("  数据写入缓存，close() 后自动调用 scheduleAsyncPersistence。");
    println!("  数据最终会异步持久化到 UFS。");
    {
        let config = GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::AsyncThrough);

        let data = b"ASYNC_THROUGH: written to cache, async persistence scheduled after close.";
        let bytes =
            GooseFsFileWriter::write_file(&config, "/write-type-demo/async_through.txt", data)
                .await?;
        println!("  ✅ 写入完成: {} 字节", bytes);
        println!("  ℹ️  close() 内部已自动调用 scheduleAsyncPersistence");

        let read =
            GooseFsFileReader::read_file(&config, "/write-type-demo/async_through.txt").await?;
        assert_eq!(read.as_ref(), data.as_slice());
        println!("  ✅ 读取验证通过");

        // 检查持久化状态
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let file_info = master
            .get_status("/write-type-demo/async_through.txt")
            .await?;
        println!(
            "  持久化状态: persisted={:?}, state={:?}",
            file_info.persisted, file_info.persistence_state
        );
    }

    // ────────────────────────────────────────────────────────
    // 汇总
    // ────────────────────────────────────────────────────────
    println!("\n━━━ 汇总 ━━━");
    let entries = master.list_status("/write-type-demo", false).await?;
    println!("目录 /write-type-demo 包含 {} 个文件:", entries.len());
    for entry in &entries {
        println!(
            "  {} — {} 字节, 持久化: {:?}",
            entry.path.as_deref().unwrap_or("?"),
            entry.length.unwrap_or(0),
            entry.persisted.unwrap_or(false),
        );
    }

    println!("\n================================");
    println!("✅ 所有 WriteType 演示完成!");
    println!("\n提示: 使用以下命令验证文件:");
    println!("  ./bin/goosefs fs ls /write-type-demo");
    println!("  ./bin/goosefs fs stat /write-type-demo/through.txt");

    Ok(())
}
