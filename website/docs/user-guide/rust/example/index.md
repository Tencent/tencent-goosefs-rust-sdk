---
sidebar_position: 1
---

# Examples

Runnable examples live under [`examples/`](https://github.com/Tencent/tencent-goosefs-rust-sdk/tree/main/examples) in the repository. Point them at a live cluster:

```bash
bash scripts/ci/goosefs-up.sh
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
export GOOSEFS_AUTH_TYPE=simple   # or nosasl

cargo run --example metadata_crud
cargo run --example context_file_rw
cargo run --example highlevel_file_rw
cargo run --example page_cache_demo
cargo run --example short_circuit_demo
```

| Example               | Topic                                           |
| --------------------- | ----------------------------------------------- |
| `metadata_crud`       | Master metadata create / list / rename / delete |
| `context_file_rw`     | Shared `FileSystemContext` read/write           |
| `highlevel_file_rw`   | `GoosefsFileWriter` / `GoosefsFileReader`       |
| `seekable_file_read`  | Seek + `read_at` on `GoosefsFileInStream`       |
| `streaming_file_read` | Block-by-block streaming read                   |
| `write_types`         | `WriteType` variants                            |
| `ha_multi_master`     | Multi-master HA discovery                       |
| `page_cache_demo`     | Local page cache cold/warm hit                  |
| `short_circuit_demo`  | Local mmap short-circuit path                   |
| `metrics_heartbeat`   | Master metrics heartbeat                        |
| `metrics_pushgateway` | Prometheus Pushgateway export                   |
| `auth_demo`           | SASL / SIMPLE auth                              |

Or run the CI helper that executes the curated example set:

```bash
bash scripts/ci/run_rust_examples.sh
```
