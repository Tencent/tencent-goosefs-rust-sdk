//! High-level file write and read example using `GooseFsFileWriter` / `GooseFsFileReader`.
//!
//! This example demonstrates the **recommended** high-level API:
//! 1. One-shot write via `GooseFsFileWriter::write_file()`
//! 2. Full-file read via `GooseFsFileReader::read_file()`
//! 3. Range read via `GooseFsFileReader::read_range()`
//! 4. Streaming block-by-block read via `GooseFsFileReader::open()` + `read_next_block()`
//! 5. Builder-pattern multi-chunk write via `GooseFsFileWriter::create()` + `write()` + `close()`
//! 6. Write with `CACHE_THROUGH` mode for durable persistence
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
//!   cargo run --example highlevel_file_rw

use goosefs_client::client::MasterClient;
use goosefs_client::config::GooseFsConfig;
use goosefs_client::error::Result;
use goosefs_client::io::{GooseFsFileReader, GooseFsFileWriter};
use goosefs_client::WritePType;

#[tokio::main]
async fn main() -> Result<()> {
    println!("GooseFS 端到端文件读写演示 (高层 API)");
    println!("======================================");

    let config = GooseFsConfig::new("127.0.0.1:9200");

    // ── Step 0: Cleanup ──────────────────────────────────────────
    println!("\n0. 清理已有的测试文件...");
    let master = MasterClient::connect(&config).await?;
    match master.delete("/e2e-test/hello.txt", false).await {
        Ok(_) => println!("  已删除旧文件"),
        Err(_) => println!("  旧文件不存在，跳过"),
    }
    match master.create_directory("/e2e-test", true).await {
        Ok(_) => println!("  目录 /e2e-test 已创建"),
        Err(_) => println!("  目录已存在，跳过"),
    }

    // ── Step 1: Write a file ─────────────────────────────────────
    println!("\n1. 写入文件 /e2e-test/hello.txt ...");
    let content = "Hello, GooseFS! 这是通过高层 API 写入的文件内容。\n\
                   Line 2: GooseFS Rust Client 端到端测试。\n\
                   Line 3: 支持自动分块、一致性哈希路由、gRPC 流式写入。\n\
                   Line 4: 写入完成后自动调用 CompleteFile 收尾。";

    let bytes_written =
        GooseFsFileWriter::write_file(&config, "/e2e-test/hello.txt", content.as_bytes()).await?;
    println!("  ✅ 写入完成: {} 字节", bytes_written);

    // ── Step 2: Read the file back ───────────────────────────────
    println!("\n2. 读取文件 /e2e-test/hello.txt ...");
    let data = GooseFsFileReader::read_file(&config, "/e2e-test/hello.txt").await?;
    let read_content = String::from_utf8_lossy(&data);
    println!("  ✅ 读取完成: {} 字节", data.len());
    println!("  内容:\n  ---");
    for line in read_content.lines() {
        println!("  {}", line);
    }
    println!("  ---");

    // Verify content matches
    if read_content == content {
        println!("  ✅ 内容验证通过: 写入与读取一致!");
    } else {
        println!("  ❌ 内容不一致!");
        println!("  写入长度: {}, 读取长度: {}", content.len(), data.len());
    }

    // ── Step 3: Range read ───────────────────────────────────────
    println!("\n3. 范围读取 (offset=0, length=20) ...");
    let range_data = GooseFsFileReader::read_range(&config, "/e2e-test/hello.txt", 0, 20).await?;
    println!("  ✅ 范围读取完成: {} 字节", range_data.len());
    println!("  内容: {:?}", String::from_utf8_lossy(&range_data));

    // ── Step 4: Streaming read ───────────────────────────────────
    println!("\n4. 流式逐块读取...");
    let mut reader = GooseFsFileReader::open(&config, "/e2e-test/hello.txt").await?;
    println!(
        "  文件长度: {} 字节, 块数: {}",
        reader.file_length(),
        reader.block_count()
    );

    let mut block_idx = 0;
    while let Some(chunk) = reader.read_next_block().await? {
        println!("  Block {}: {} 字节", block_idx, chunk.len());
        block_idx += 1;
    }
    println!(
        "  ✅ 流式读取完成: 共 {} 块, {} 字节",
        block_idx,
        reader.bytes_read()
    );

    // ── Step 5: Write with builder pattern ───────────────────────
    println!("\n5. 使用 builder 模式写入多段数据...");
    match master.delete("/e2e-test/multi.txt", false).await {
        Ok(_) => {}
        Err(_) => {}
    }

    let mut writer = GooseFsFileWriter::create(&config, "/e2e-test/multi.txt").await?;
    writer.write(b"First chunk of data. ").await?;
    writer.write(b"Second chunk of data. ").await?;
    writer.write(b"Third and final chunk.").await?;
    writer.close().await?;
    println!("  ✅ 多段写入完成: {} 字节", writer.bytes_written());

    // Verify
    let multi_data = GooseFsFileReader::read_file(&config, "/e2e-test/multi.txt").await?;
    println!("  验证: {:?}", String::from_utf8_lossy(&multi_data));

    // ── Step 6: Write with CACHE_THROUGH mode ────────────────
    println!("\n6. 使用 CACHE_THROUGH 模式写入（缓存 + 同步持久化到 UFS）...");
    match master.delete("/e2e-test/durable.txt", false).await {
        Ok(_) => {}
        Err(_) => {}
    }
    let durable_config =
        GooseFsConfig::new("127.0.0.1:9200").with_write_type(WritePType::CacheThrough);
    let durable_content = b"This data is written to cache AND persisted to UFS synchronously.";
    let durable_bytes =
        GooseFsFileWriter::write_file(&durable_config, "/e2e-test/durable.txt", durable_content)
            .await?;
    println!("  ✅ CACHE_THROUGH 写入完成: {} 字节", durable_bytes);

    // 验证持久化状态
    let durable_info = master.get_status("/e2e-test/durable.txt").await?;
    println!(
        "  持久化状态: persisted={:?}",
        durable_info.persisted.unwrap_or(false)
    );

    println!("\n======================================");
    println!("✅ 高级api测试完成!");
    Ok(())
}
