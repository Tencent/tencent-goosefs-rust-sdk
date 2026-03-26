//! File and directory metadata CRUD example.
//!
//! Demonstrates `MasterClient` metadata operations:
//! - `create_directory` — create directories (with recursive option)
//! - `create_file` / `complete_file` — create file entries
//! - `get_status` — get file/directory info (size, persistence state, etc.)
//! - `list_status` — list directory contents
//! - `rename` — move/rename files and directories
//! - `delete` — delete files and directories (with recursive option)
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
//!   cargo run --example metadata_crud

use goosefs_client::client::MasterClient;
use goosefs_client::config::GooseFsConfig;
use goosefs_client::error::Result;
use goosefs_client::proto::grpc::file::CreateFilePOptions;

#[tokio::main]
async fn main() -> Result<()> {
    // 连接到GooseFS Master
    println!("正在连接到GooseFS Master...");
    let config = GooseFsConfig::new("127.0.0.1:9200");
    let master = MasterClient::connect(&config).await?;
    println!("连接成功!");

    // 先清理已存在的测试目录
    println!("\n1. 清理已存在的测试目录...");
    match master.delete("/test-demo", true).await {
        Ok(_) => println!("已删除已存在的测试目录"),
        Err(e) => println!("删除测试目录失败或目录不存在: {:?}", e),
    }

    // 创建测试目录
    println!("\n2. 创建测试目录...");
    master.create_directory("/test-demo", true).await?;
    println!("目录 /test-demo 创建成功");

    // 创建测试文件
    println!("\n2. 创建测试文件...");
    let mut create_options = CreateFilePOptions::default();
    create_options.block_size_bytes = Some(64 * 1024 * 1024); // 64MB block size
    master
        .create_file("/test-demo/hello.txt", create_options)
        .await?;
    println!("文件 /test-demo/hello.txt 创建成功");

    // 写入文件内容
    println!("\n3. 写入文件内容...");
    let content = "Hello, GooseFS! 这是一个测试文件内容。\n欢迎使用GooseFS Rust客户端！\n当前时间: 2026-03-26 18:06:32";
    // 这里需要实现文件写入逻辑，但当前API可能不支持直接写入
    println!("写入内容: {}", content);
    println!("注意: 当前API可能需要通过其他方式写入内容，比如使用worker客户端");

    // 标记文件完成（模拟写入完成）
    println!("\n4. 标记文件完成...");
    master
        .complete_file("/test-demo/hello.txt", Some(content.len() as i64))
        .await?;
    println!("文件标记完成，内容长度: {} 字节", content.len());

    // 获取文件状态
    println!("\n4. 获取文件状态...");
    let file_info = master.get_status("/test-demo/hello.txt").await?;
    println!("文件信息: {:?}", file_info);
    println!("路径: {:?}", file_info.path);
    println!("长度: {:?} 字节", file_info.length);
    println!("是否为目录: {:?}", file_info.folder);

    // 列出目录内容
    println!("\n5. 列出目录内容...");
    let entries = master.list_status("/test-demo", false).await?;
    println!("目录 /test-demo 包含 {} 个条目:", entries.len());
    for entry in &entries {
        println!(
            "  - {} ({}, {} 字节)",
            entry.path.as_deref().unwrap_or("unknown"),
            if entry.folder.unwrap_or(false) {
                "目录"
            } else {
                "文件"
            },
            entry.length.unwrap_or(0)
        );
    }

    // 先删除已存在的world.txt文件
    println!("\n6. 删除已存在的world.txt文件...");
    match master.delete("/test-demo/world.txt", false).await {
        Ok(_) => println!("已删除已存在的world.txt文件"),
        Err(e) => println!("删除world.txt文件失败或文件不存在: {:?}", e),
    }

    // 重命名文件
    println!("\n7. 重命名文件...");
    master
        .rename("/test-demo/hello.txt", "/test-demo/world.txt")
        .await?;
    println!("文件重命名为 /test-demo/world.txt");

    // 验证重命名
    println!("\n7. 验证重命名...");
    let entries = master.list_status("/test-demo", false).await?;
    for entry in &entries {
        println!("  - {}", entry.path.as_deref().unwrap_or("unknown"));
    }

    // // 删除文件
    // println!("\n8. 删除文件...");
    // master.delete("/test-demo/world.txt", false).await?;
    // println!("文件删除成功");
    //
    // // 删除目录
    // println!("\n9. 删除目录...");
    // master.delete("/test-demo", true).await?;
    // println!("目录删除成功");

    println!("\n✅ 所有文件操作测试完成!");
    Ok(())
}
