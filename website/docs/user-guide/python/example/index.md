---
sidebar_position: 1
---

# Examples

Runnable scripts live under [`bindings/python/examples/`](https://github.com/Tencent/tencent-goosefs-rust-sdk/tree/main/bindings/python/examples):

| Script               | Topic                                                                     |
| -------------------- | ------------------------------------------------------------------------- |
| `quickstart.py`      | Sync connect → mkdir → write → read → delete                              |
| `async_demo.py`      | `AsyncGoosefs` + `asyncio.gather` fan-out                                 |
| `streaming.py`       | Chunked read/write, seek, `read_at`                                       |
| `positioned_read.py` | High-level `positioned_read` + low-level `AsyncWorkerClient`              |
| `batch_status.py`    | `batch_get_status` / `batch_exists` / `list_status_grouped`               |
| `batch_files.py`     | `batch_create_file` / `batch_open_file` / `batch_rename` / `batch_delete` |
| `with_pyarrow.py`    | Arrow Table → Parquet → GooseFS round-trip                                |
| `pandas_csv.py`      | pandas DataFrame ↔ CSV round-trip                                         |
| `page_cache.py`      | Client local page cache via `open_file`                                   |
| `diagnose_pread.py`  | Debug: trace the Python `read_at` path (`RUST_LOG=goosefs_sdk=debug`)     |

```bash
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
pip install 'goosefs[examples]'

cd bindings/python
python examples/quickstart.py
python examples/batch_status.py
python examples/batch_files.py
python examples/positioned_read.py
```
