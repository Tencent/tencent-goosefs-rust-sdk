//! Low-level file creation example (metadata only, no data written).
//!
//! Demonstrates the low-level `MasterClient` API for creating file entries
//! in the GooseFS namespace via `CreateFile` + `CompleteFile`.
//! This does NOT write any data blocks — it only creates metadata.
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
//!   cargo run --example lowlevel_create_file

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

    // 先创建test-demo目录
    println!("\n创建test-demo目录...");
    match master.create_directory("/test-demo", true).await {
        Ok(_) => println!("目录 /test-demo 创建成功"),
        Err(e) => println!("创建目录失败或目录已存在: {:?}", e),
    }

    // 清理已存在的文件
    println!("\n清理已存在的文件...");
    match master.delete("/test-demo/world.txt", false).await {
        Ok(_) => println!("已删除已存在的world.txt文件"),
        Err(e) => println!("删除world.txt文件失败或文件不存在: {:?}", e),
    }

    // 创建文件
    println!("\n创建world.txt文件...");
    let mut create_options = CreateFilePOptions::default();
    create_options.block_size_bytes = Some(64 * 1024 * 1024); // 64MB block size
    master
        .create_file("/test-demo/world.txt", create_options)
        .await?;
    println!("文件 /test-demo/world.txt 创建成功");

    // ⚠️ 注意：这里只是设置元数据长度，并没有真正写入数据块到 Worker。
    // 真正的写入流程需要：
    //   1. 通过一致性哈希选择目标 Worker
    //   2. 与 Worker 建立 gRPC 双向流连接
    //   3. 将数据块通过流式传输写入 Worker 缓存
    //   4. Worker 侧 commitBlock
    // 如需真正写入数据，请参考 highlevel_file_rw 示例。
    let fake_length = 169i64; // 模拟一个文件长度（实际 Worker 上不存在对应的数据块）

    // 标记文件完成（仅在 Master 侧设置 completed=true + length）
    println!("\n标记文件完成（仅元数据）...");
    master
        .complete_file("/test-demo/world.txt", Some(fake_length))
        .await?;
    println!("文件元数据标记完成，设置长度为 {} 字节", fake_length);

    // 获取文件状态
    println!("\n获取文件状态...");
    let file_info = master.get_status("/test-demo/world.txt").await?;
    println!(
        "文件长度: {:?} 字节（注意：这只是元数据长度，Worker 上没有实际数据块）",
        file_info.length
    );

    println!("\n✅ world.txt 文件元数据已创建!");
    println!("⚠️  注意：此示例仅演示 CreateFile + CompleteFile 元数据操作。");
    println!("   文件在 Master 命名空间中存在，但 Worker 上没有实际数据块。");
    println!("   执行 `goosefs fs cat /test-demo/world.txt` 会卡住，因为无法读取到数据块。");
    println!("   如需创建可读写的文件，请使用 highlevel_file_rw 示例：");
    println!("   cargo run --example highlevel_file_rw");

    Ok(())
}
