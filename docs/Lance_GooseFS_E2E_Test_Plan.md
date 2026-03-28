# Lance + GooseFS 端到端读写测试验证计划

## 一、前置条件

### 1.1 环境要求

| 组件 | 要求 | 当前状态 |
|------|------|---------|
| GooseFS Master | 运行在 `127.0.0.1:9200` | 需确认（之前 goosefs-client-rs examples 中已验证过） |
| GooseFS Worker | 至少 1 个 Worker 节点 | 需确认 |
| Rust 工具链 | nightly/stable（支持 edition 2024） | ✅ 已有 |
| Lance 仓库 | `feature/goosefs-provider` 分支 | ✅ `/opt/sourcecode/lance` |
| OpenDAL patch | `[patch.crates-io]` 指向本地 opendal | ✅ 已配置 |

### 1.2 GooseFS 集群管理

#### 格式化（首次部署或需要清空元数据时执行）

```bash
cd $GOOSEFS_HOME 或
cd /opt/sourcecode/cos/goosefs

# 格式化 Master（清空元数据，慎用！）
./bin/goosefs formatMaster

# 格式化 Worker（清空缓存数据，慎用！）
./bin/goosefs formatWorker
```

#### 启动集群

```bash
cd $GOOSEFS_HOME

# 方式一：一键启动本地全部组件（Master + Worker + Job Master + Job Worker）
./bin/goosefs-start.sh local

# 方式二：分组件启动（推荐用于调试 / 生产环境）
./bin/goosefs-start.sh master        # 启动 Master
./bin/goosefs-start.sh worker        # 启动 Worker
./bin/goosefs-start.sh job_master    # 启动 Job Master（异步任务调度）
./bin/goosefs-start.sh job_worker    # 启动 Job Worker（异步任务执行）
```

#### 停止集群

```bash
cd $GOOSEFS_HOME

# 停止本地全部组件
./bin/goosefs-stop.sh local
```

#### 状态检查

```bash
# 确认 GooseFS Master 已启动（打印 leader 地址和所有 master 地址列表）
goosefs fs masterInfo

# 确认当前 leader master 主机名
goosefs fs leader

# 确认 Worker 存活（查看 Live Workers / Lost Workers 数量、容量等）
goosefs fsadmin report summary

# 查看所有 Worker 的详细容量信息（名称、最后心跳时间、IsAlive、存储容量等）
goosefs fsadmin report capacity

# 仅查看存活 Worker
goosefs fsadmin report capacity -live

# 也可用 monitor 脚本检查健康状态
./bin/goosefs-monitor.sh all
```

### 1.3 预清理 GooseFS 测试目录

```bash
# 清理旧数据，避免残留文件干扰测试
goosefs fs rm -R /lance-test/ 2>/dev/null || true
goosefs fs mkdir /lance-test/
```

---

## 二、测试分层策略

按 **4 个阶段**，从底层到上层逐层验证：

```
Stage 4: Lance Dataset E2E （完整 Dataset 操作）
    ↑
Stage 3: Lance ObjectStore I/O （底层存储读写）
    ↑
Stage 2: OpenDAL GooseFs Service （OpenDAL 层读写）
    ↑
Stage 1: GooseFS Rust Client（gRPC 直连验证）← 已通过
```

---

## 三、Stage 1：GooseFS 连通性验证（冒烟测试）

> **目的**：确认 GooseFS 集群可达，gRPC 链路正常。

```bash
# 运行已有的 goosefs-client-rs example 验证连通性
cd /opt/sourcecode/cos/goosefs-client-rust
cargo run --example highlevel_file_rw
```

**预期结果**：6 步全部 ✅ 通过（写入、读取、范围读、流式读、多段写、CACHE_THROUGH 写）。

**如果失败**：先排查 GooseFS 集群问题，不要进入后续阶段。

---

## 四、Stage 2：OpenDAL GooseFs Service 验证

> **目的**：通过 OpenDAL Operator 直接测试文件 CRUD，确认 OpenDAL → GooseFS 链路正常。

### 测试文件

在 `/opt/sourcecode/lance` 仓库中创建独立测试文件：

**文件路径**：`rust/lance-io/tests/goosefs_integration.rs`

```rust
//! GooseFS integration tests via OpenDAL.
//! Run: cargo test -p lance-io --features goosefs --test goosefs_integration -- --ignored
#![cfg(feature = "goosefs")]

use opendal::{Operator, services::GooseFs};
use std::collections::HashMap;

fn get_operator() -> Operator {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let mut cfg = HashMap::new();
    cfg.insert("master_addr".to_string(), addr);
    cfg.insert("root".to_string(), "/lance-test/opendal".to_string());
    Operator::from_iter::<GooseFs>(cfg).unwrap().finish()
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_opendal_write_read() {
    let op = get_operator();
    op.write("hello.txt", "Hello from OpenDAL").await.unwrap();
    let data = op.read("hello.txt").await.unwrap();
    assert_eq!(data.to_vec(), b"Hello from OpenDAL");
    op.delete("hello.txt").await.unwrap();
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_opendal_list() {
    let op = get_operator();
    op.write("dir/a.txt", "aaa").await.unwrap();
    op.write("dir/b.txt", "bbb").await.unwrap();
    let entries: Vec<_> = op.list("dir/").await.unwrap();
    assert!(entries.len() >= 2);
    op.delete("dir/a.txt").await.unwrap();
    op.delete("dir/b.txt").await.unwrap();
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_opendal_stat() {
    let op = get_operator();
    op.write("stat_test.txt", "12345").await.unwrap();
    let meta = op.stat("stat_test.txt").await.unwrap();
    assert_eq!(meta.content_length(), 5);
    op.delete("stat_test.txt").await.unwrap();
}
```

### 运行方式

```bash
cd /opt/sourcecode/lance
cargo test -p lance-io --features goosefs --test goosefs_integration -- --ignored --nocapture 2>&1 | head -50
```

**预期结果**：3 个测试全部 PASS。

---

## 五、Stage 3：Lance ObjectStore I/O 验证

> **目的**：测试 `GooseFsStoreProvider` 创建的 `ObjectStore` 能否正确执行底层文件操作（put / get / list / delete）。

### 测试文件

追加到 `rust/lance-io/tests/goosefs_integration.rs`：

```rust
use std::sync::Arc;
use lance_io::object_store::{ObjectStore, ObjectStoreParams};

async fn get_lance_store() -> Arc<ObjectStore> {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let uri = format!("goosefs://{}/lance-test/lance-io", addr);
    ObjectStore::from_uri(&uri).await.unwrap().0
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_objectstore_put_get() {
    let store = get_lance_store().await;
    let path = object_store::path::Path::from("test_put_get.bin");

    // Cleanup
    let _ = store.inner.delete(&path).await;

    // Write
    store.inner.put(&path, b"lance-goosefs-test".into()).await.unwrap();

    // Read
    let result = store.inner.get(&path).await.unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(&bytes[..], b"lance-goosefs-test");

    // Cleanup
    store.inner.delete(&path).await.unwrap();
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_objectstore_list() {
    let store = get_lance_store().await;
    let dir = object_store::path::Path::from("list_test");

    store.inner.put(&dir.child("a.bin"), b"aaa".into()).await.unwrap();
    store.inner.put(&dir.child("b.bin"), b"bbb".into()).await.unwrap();

    let entries: Vec<_> = store.inner
        .list(Some(&dir))
        .try_collect()
        .await
        .unwrap();
    assert!(entries.len() >= 2);

    store.inner.delete(&dir.child("a.bin")).await.unwrap();
    store.inner.delete(&dir.child("b.bin")).await.unwrap();
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_objectstore_large_file() {
    let store = get_lance_store().await;
    let path = object_store::path::Path::from("large_file.bin");
    let _ = store.inner.delete(&path).await;

    // Write 5MB file
    let data = vec![42u8; 5 * 1024 * 1024];
    store.inner.put(&path, data.clone().into()).await.unwrap();

    let result = store.inner.get(&path).await.unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.len(), 5 * 1024 * 1024);
    assert_eq!(&bytes[..10], &[42u8; 10]);

    store.inner.delete(&path).await.unwrap();
}
```

### 运行方式

```bash
cd /opt/sourcecode/lance
cargo test -p lance-io --features goosefs --test goosefs_integration -- --ignored --nocapture 2>&1 | head -80
```

**预期结果**：6 个测试全部 PASS（Stage 2 的 3 个 + Stage 3 的 3 个）。

---

## 六、Stage 4：Lance Dataset E2E 验证 ⭐（核心）

> **目的**：验证完整的 Lance Dataset 生命周期（Create → Write → Open → Scan → Append → Count → Delete）通过 `goosefs://` URL 工作。

### 测试 4.1：基础读写

```rust
// 文件：rust/lance-io/tests/goosefs_integration.rs（或单独的 binary）
use arrow::array::{Int32Array, Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_write_read() {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let uri = format!("goosefs://{}/lance-test/datasets/basic.lance", addr);

    // 1. 创建 Schema + RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie", "david", "eve"])),
            Arc::new(Float32Array::from(vec![95.5, 87.3, 91.0, 78.8, 99.1])),
        ],
    ).unwrap();

    // 2. Write Dataset
    let write_params = WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    };
    let batches = RecordBatchIterator::new([Ok(batch.clone())], schema.clone());
    Dataset::write(batches, &uri, Some(write_params)).await.unwrap();

    // 3. Open + Verify row count
    let dataset = Dataset::open(&uri).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 5);

    // 4. Scan + Verify data
    let mut stream = dataset.scan().try_into_stream().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 5);

    // 5. Schema verification
    assert_eq!(dataset.schema().fields.len(), 3);
}
```

### 测试 4.2：Append 追加写入

```rust
#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_append() {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let uri = format!("goosefs://{}/lance-test/datasets/append.lance", addr);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float32, false),
    ]));

    // Initial write
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
        ],
    ).unwrap();

    let batches = RecordBatchIterator::new([Ok(batch1)], schema.clone());
    let mut dataset = Dataset::write(batches, &uri, Some(WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    })).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);

    // Append
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(Float32Array::from(vec![4.0, 5.0])),
        ],
    ).unwrap();

    let batches2 = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    dataset.append(batches2, None).await.unwrap();

    // Verify
    let dataset = Dataset::open(&uri).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 5);
}
```

### 测试 4.3：列投影 + 过滤

```rust
#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_scan_with_filter() {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let uri = format!("goosefs://{}/lance-test/datasets/filter.lance", addr);

    // Write 100 rows
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let ids: Vec<i32> = (0..100).collect();
    let categories: Vec<String> = (0..100).map(|i| {
        if i % 3 == 0 { "A".into() } else if i % 3 == 1 { "B".into() } else { "C".into() }
    }).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(categories)),
        ],
    ).unwrap();

    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(batches, &uri, Some(WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    })).await.unwrap();

    // count_rows with filter
    let dataset = Dataset::open(&uri).await.unwrap();
    let count_a = dataset.count_rows(Some("category = 'A'".into())).await.unwrap();
    assert_eq!(count_a, 34); // 0,3,6,...,99 → ceil(100/3)=34

    // Scan with projection
    let mut scanner = dataset.scan();
    scanner.project(&["id"]).unwrap();
    let batches: Vec<_> = scanner.try_into_stream().await.unwrap()
        .try_collect().await.unwrap();
    assert_eq!(batches[0].num_columns(), 1);
}
```

### 测试 4.4：大批量数据写入（稳定性 + 性能）

```rust
#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_large_write() {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let uri = format!("goosefs://{}/lance-test/datasets/large.lance", addr);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("embedding", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), 128
        ), false),
    ]));

    let num_rows = 10_000;
    // 构建 10000 行 * 128 维向量数据（~5MB）
    let ids: Vec<i32> = (0..num_rows).collect();
    let embeddings: Vec<f32> = (0..num_rows * 128).map(|i| (i as f32) * 0.001).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(FixedSizeListArray::try_new_from_values(
                Float32Array::from(embeddings), 128,
            ).unwrap()),
        ],
    ).unwrap();

    let start = std::time::Instant::now();
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(batches, &uri, Some(WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    })).await.unwrap();
    let write_time = start.elapsed();

    let start = std::time::Instant::now();
    let dataset = Dataset::open(&uri).await.unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    let read_time = start.elapsed();

    println!("Large write: {} rows, write={:?}, open+count={:?}", count, write_time, read_time);
    assert_eq!(count, num_rows as usize);
}
```

### 测试 4.5：版本管理（Overwrite 后重新读取）

```rust
#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_versioning() {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let uri = format!("goosefs://{}/lance-test/datasets/versioned.lance", addr);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));

    // Version 1: write 3 rows
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();
    let batches = RecordBatchIterator::new([Ok(batch1)], schema.clone());
    Dataset::write(batches, &uri, Some(WriteParams {
        mode: WriteMode::Overwrite, ..Default::default()
    })).await.unwrap();

    let ds1 = Dataset::open(&uri).await.unwrap();
    assert_eq!(ds1.count_rows(None).await.unwrap(), 3);
    let v1 = ds1.version().version;

    // Version 2: append 2 rows
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![4, 5]))],
    ).unwrap();
    let batches2 = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    let mut ds_append = Dataset::open(&uri).await.unwrap();
    ds_append.append(batches2, None).await.unwrap();

    let ds2 = Dataset::open(&uri).await.unwrap();
    assert_eq!(ds2.count_rows(None).await.unwrap(), 5);
    assert!(ds2.version().version > v1);

    // Checkout v1
    let ds_v1 = ds2.checkout_version(v1).await.unwrap();
    assert_eq!(ds_v1.count_rows(None).await.unwrap(), 3);
}
```

### 测试 4.6：storage_options 方式连接

```rust
#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_with_storage_options() {
    use lance::dataset::builder::DatasetBuilder;
    use goosefs_client::STORAGE_OPT_MASTER_ADDR;  // 使用常量避免魔法字符串

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/opts.lance";

    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
    ).unwrap();

    // Write using DatasetBuilder with storage_options
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(batches, uri, Some(WriteParams {
        mode: WriteMode::Overwrite, ..Default::default()
    })).await.unwrap();

    // Read with explicit storage_options（使用常量）
    let dataset = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_MASTER_ADDR, "127.0.0.1:9200")
        .load()
        .await
        .unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
}
```

### 测试 4.7：持久化写入 — CACHE_THROUGH ⭐

> **目的**：验证通过 `goosefs_write_type=cache_through` 写入的数据同时存在于缓存和 UFS，
> 文件状态为 `PERSISTED`。

```rust
#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_write_cache_through() {
    use lance::dataset::builder::DatasetBuilder;
    use goosefs_client::{STORAGE_OPT_WRITE_TYPE};
    use goosefs_client::config::WriteType;

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/cache_through.lance";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie", "david", "eve"])),
            Arc::new(Float32Array::from(vec![95.5, 87.3, 91.0, 78.8, 99.1])),
        ],
    ).unwrap();

    // 使用 WriteType 枚举 + 常量构建 storage_options
    let mut options = HashMap::new();
    options.insert(
        STORAGE_OPT_WRITE_TYPE.to_string(),
        WriteType::CacheThrough.to_string(),  // "cache_through"
    );

    // ... 通过 ObjectStoreParams 传入 storage_options，写入 Dataset ...

    // 验证
    let dataset = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WriteType::CacheThrough.as_str())
        .load().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 5);
    // GooseFS 中该文件状态应为 PERSISTED
}
```

### 测试 4.8：持久化写入 — THROUGH（直写 UFS）

> **目的**：验证 `goosefs_write_type=through` 直写 UFS，跳过缓存。

```rust
#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_write_through() {
    use lance::dataset::builder::DatasetBuilder;
    use goosefs_client::STORAGE_OPT_WRITE_TYPE;
    use goosefs_client::config::WriteType;

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/through.lance";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Float32Array::from(vec![1.1, 2.2, 3.3])),
        ],
    ).unwrap();

    // 使用 WriteType::Through 枚举
    let mut options = HashMap::new();
    options.insert(
        STORAGE_OPT_WRITE_TYPE.to_string(),
        WriteType::Through.to_string(),  // "through"
    );

    // ... 写入 + 验证 ...
    let dataset = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WriteType::Through.as_str())
        .load().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
    // GooseFS 中该文件状态应为 PERSISTED
}
```

### 测试 4.9：持久化 + Append + 版本管理

> **目的**：验证 `CACHE_THROUGH` 模式下的 append 和版本回退仍然正确工作。

```rust
#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_persisted_append_versioning() {
    use lance::dataset::builder::DatasetBuilder;
    use goosefs_client::STORAGE_OPT_WRITE_TYPE;
    use goosefs_client::config::WriteType;

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/persist_append.lance";

    // Version 1: 写 3 行（CACHE_THROUGH）
    // ... 写入 ...
    let ds1 = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WriteType::CacheThrough.as_str())
        .load().await.unwrap();
    assert_eq!(ds1.count_rows(None).await.unwrap(), 3);
    let v1 = ds1.version().version;

    // Version 2: append 2 行（CACHE_THROUGH）
    // ... append ...
    let ds2 = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WriteType::CacheThrough.as_str())
        .load().await.unwrap();
    assert_eq!(ds2.count_rows(None).await.unwrap(), 5);
    assert!(ds2.version().version > v1);

    // Checkout v1 验证版本回退
    let ds_v1 = ds2.checkout_version(v1).await.unwrap();
    assert_eq!(ds_v1.count_rows(None).await.unwrap(), 3);
}
```

### WriteType 与 Storage Option 常量参考

测试中使用的常量和枚举来自 `goosefs-client-rs` crate 的 `config` 模块：

| Rust 常量 / 枚举 | 值 | 说明 |
|------------------|---|------|
| `STORAGE_OPT_MASTER_ADDR` | `"goosefs_master_addr"` | Master 地址 key |
| `STORAGE_OPT_WRITE_TYPE` | `"goosefs_write_type"` | 写入类型 key |
| `STORAGE_OPT_BLOCK_SIZE` | `"goosefs_block_size"` | Block 大小 key |
| `STORAGE_OPT_CHUNK_SIZE` | `"goosefs_chunk_size"` | Chunk 大小 key |
| `WriteType::MustCache` | `"must_cache"` (i32=1) | 仅缓存，`NOT_PERSISTED` |
| `WriteType::CacheThrough` | `"cache_through"` (i32=3) | 缓存+同步持久化，`PERSISTED` |
| `WriteType::Through` | `"through"` (i32=4) | 直写 UFS，`PERSISTED` |
| `WriteType::AsyncThrough` | `"async_through"` (i32=5) | 缓存+异步持久化 |

---

## 七、运行汇总

### 推荐的运行方式

所有集成测试写到一个文件，按阶段标记：

```bash
# Stage 1: 连通性
cd /opt/sourcecode/cos/goosefs-client-rust && cargo run --example highlevel_file_rw

# Stage 2+3: OpenDAL + ObjectStore 层
cd /opt/sourcecode/lance && cargo test -p lance-io --features goosefs --test goosefs_integration -- --ignored --nocapture

# Stage 4: Dataset E2E（需要在 lance crate 层测试）
cd /opt/sourcecode/lance && cargo test -p lance --features goosefs --test goosefs_e2e -- --ignored --nocapture
```

### 测试结果记录表

| Stage | 测试 | 预期 | 实际 | 耗时 |
|-------|------|------|------|------|
| 1 | GooseFS 连通性 | ✅ 6/6 通过 | | |
| 2 | OpenDAL write/read | ✅ | | |
| 2 | OpenDAL list | ✅ | | |
| 2 | OpenDAL stat | ✅ | | |
| 3 | ObjectStore put/get | ✅ | | |
| 3 | ObjectStore list | ✅ | | |
| 3 | ObjectStore large file (5MB) | ✅ | | |
| 4.1 | Dataset basic write/read | ✅ 5 rows | | |
| 4.2 | Dataset append | ✅ 3→5 rows | | |
| 4.3 | Dataset filter + projection | ✅ 34 rows | | |
| 4.4 | Dataset large (10K×128d) | ✅ 10000 rows | | |
| 4.5 | Dataset versioning | ✅ v1=3, v2=5, checkout v1=3 | | |
| 4.6 | Dataset storage_options | ✅ 3 rows | | |
| 4.7 | Dataset CACHE_THROUGH 持久化 | ✅ 5 rows, PERSISTED | | |
| 4.8 | Dataset THROUGH 直写 UFS | ✅ 3 rows, PERSISTED | | |
| 4.9 | 持久化 + append + 版本管理 | ✅ v1=3, v2=5, PERSISTED | | |

---

## 八、已知风险与排查

| 风险 | 可能表现 | 排查方向 |
|------|---------|---------|
| GooseFS 集群未启动 | Stage 1 连接超时 | 检查 `goosefs-start.sh local` |
| OpenDAL list 排序问题 | Stage 3 list 测试 assertion 失败 | 确认 GooseFS `listStatus` 返回有序 |
| ConditionalPut 不支持 | Stage 4 写入版本冲突 | GooseFS `copy_if_not_exists` 是否正确实现 |
| 大文件分块写入 | Stage 4.4 超时或数据不一致 | 检查 `chunk_size` 和 GooseFS block_size 配置 |
| Lance manifest 写入 | Stage 4 写入后无法打开 | 检查 GooseFS 是否正确处理 `.lance` 目录下的 manifest 文件 |
| Rust edition 2024 兼容性 | 编译错误 | 确认 rust-toolchain 版本 |

---

## 九、下一步

1. 从 **Stage 1**（连通性确认）开始，确认 GooseFS 集群正常
2. 逐层推进到 Stage 2 → Stage 3 → Stage 4
3. 每个阶段通过后再进入下一阶段
4. 记录每个测试的实际结果和耗时到上方结果记录表
