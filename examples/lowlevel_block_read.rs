//! Low-level block streaming read example.
//!
//! Demonstrates the low-level block read pipeline:
//! `MasterClient::get_status()` → `WorkerRouter` → `GrpcBlockReader`
//!
//! This example reads a file block-by-block using the underlying gRPC streaming API,
//! which gives full control over flow-control ACK and chunk-level processing.
//!
//! ⚠️ 前置条件：需要先运行 `highlevel_file_rw` 创建一个真正有数据块的文件：
//!   cargo run --example highlevel_file_rw
//!
//! Usage:
//!   cargo run --example lowlevel_block_read

use goosefs_client::block::WorkerRouter;
use goosefs_client::client::{MasterClient, WorkerClient, WorkerManagerClient};
use goosefs_client::config::GooseFsConfig;
use goosefs_client::error::Result;
use goosefs_client::io::GrpcBlockReader;

#[tokio::main]
async fn main() -> Result<()> {
    println!("GooseFS 块级别流式读取演示");
    println!("==========================");

    // 连接到GooseFS Master
    println!("\n1. 连接到GooseFS Master...");
    let config = GooseFsConfig::new("127.0.0.1:9200");
    let master = MasterClient::connect(&config).await?;
    println!("连接成功!");

    // 检查测试文件是否存在
    // ⚠️ 读取的文件必须是通过 Worker 真正写入过数据块的文件
    // （例如 highlevel_file_rw 示例创建的 /e2e-test/hello.txt）
    // lowlevel_create_file 创建的文件只有元数据，没有数据块，读取会失败/卡住
    println!("\n2. 检查测试文件...");
    let test_file_path = "/e2e-test/hello.txt";

    let file_info = match master.get_status(test_file_path).await {
        Ok(info) => info,
        Err(_) => {
            println!(
                "测试文件 {} 不存在，请先运行 highlevel_file_rw 示例创建文件",
                test_file_path
            );
            println!("运行命令: cargo run --example highlevel_file_rw");
            return Ok(());
        }
    };

    println!("文件信息:");
    println!("  路径: {:?}", file_info.path);
    println!("  长度: {:?} 字节", file_info.length);
    println!("  块大小: {:?}", file_info.block_size_bytes);
    println!("  文件ID: {:?}", file_info.file_id);
    println!("  块ID列表: {:?}", file_info.block_ids);
    println!("  文件块信息: {:?}", file_info.file_block_infos);

    // 发现workers并构建路由器
    println!("\n3. 发现workers并构建路由器...");
    let wm = WorkerManagerClient::connect(&config).await?;
    let workers = wm.get_worker_info_list().await?;
    println!("发现 {} 个workers", workers.len());

    let router = WorkerRouter::new();
    router.update_workers(workers).await;

    // 映射文件范围到块级别读取计划
    println!("\n4. 映射文件范围到块级别读取计划...");
    let file_length = file_info.length.unwrap_or(0) as u64;

    // 使用文件信息中的实际块ID
    let block_id = if let Some(first_block) = file_info.file_block_infos.first() {
        if let Some(block_info) = &first_block.block_info {
            block_info.block_id.unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    println!("使用块ID: {}", block_id);
    println!("文件长度: {} 字节", file_length);

    // 流式读取块
    println!("\n5. 流式读取块...");
    let mut total_bytes_read = 0;

    if block_id > 0 {
        println!("\n读取块 (ID: {})...", block_id);

        // 选择worker
        let worker_info = match router.select_worker(block_id).await {
            Ok(info) => info,
            Err(e) => {
                println!("选择worker失败: {:?}", e);
                return Ok(());
            }
        };

        let addr = worker_info.address.as_ref().unwrap();
        let worker_addr = format!(
            "{}:{}",
            addr.host.as_deref().unwrap_or("127.0.0.1"),
            addr.rpc_port.unwrap_or(9203)
        );

        println!("  连接到worker: {}", worker_addr);

        // 连接到worker
        let worker = match WorkerClient::connect(&worker_addr, config.connect_timeout).await {
            Ok(client) => client,
            Err(e) => {
                println!("连接worker失败: {:?}", e);
                return Ok(());
            }
        };

        // 创建块读取器
        let mut reader = match GrpcBlockReader::open(
            &worker,
            block_id,
            0, // 从块的开头开始读取
            file_length as i64,
            config.chunk_size as i64,
            None, // 缓存读取无需 UFS 选项
        )
        .await
        {
            Ok(reader) => reader,
            Err(e) => {
                println!("创建块读取器失败: {:?}", e);
                return Ok(());
            }
        };

        // 读取块数据
        let data = match reader.read_all().await {
            Ok(data) => data,
            Err(e) => {
                println!("读取块数据失败: {:?}", e);
                return Ok(());
            }
        };

        total_bytes_read += data.len();

        println!(
            "  读取 {} 字节 (完成: {})",
            data.len(),
            reader.is_complete()
        );

        // 显示读取的内容（如果是文本文件）
        if let Ok(content) = String::from_utf8(data.to_vec()) {
            println!(
                "  内容预览: {:?}",
                if content.len() > 50 {
                    format!("{}...", &content[..50])
                } else {
                    content
                }
            );
        }
    } else {
        println!("无效的块ID，无法读取");
    }

    println!("\n6. 读取完成");
    println!("总读取字节数: {} 字节", total_bytes_read);
    println!("文件总长度: {} 字节", file_length);

    if total_bytes_read == file_length as usize {
        println!("✅ 成功读取完整文件内容!");
    } else {
        println!("⚠️  读取字节数与文件长度不匹配");
    }

    Ok(())
}
