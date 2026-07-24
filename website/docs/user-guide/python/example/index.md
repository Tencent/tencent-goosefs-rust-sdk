---
sidebar_position: 1
---

# Examples

Runnable scripts live under [`bindings/python/examples/`](https://github.com/Tencent/tencent-goosefs-rust-sdk/tree/main/bindings/python/examples):

| Script            | Topic                                        |
| ----------------- | -------------------------------------------- |
| `quickstart.py`   | Sync connect → mkdir → write → read → delete |
| `async_demo.py`   | `AsyncGoosefs` + `asyncio.gather` fan-out    |
| `streaming.py`    | Chunked read/write, seek, `read_at`          |
| `with_pyarrow.py` | Arrow Table → Parquet → GooseFS round-trip   |
| `pandas_csv.py`   | pandas DataFrame ↔ CSV round-trip            |
| `page_cache.py`   | Client local page cache                      |

```bash
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
pip install 'goosefs[examples]'
python bindings/python/examples/quickstart.py
```
