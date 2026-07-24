---
sidebar_position: 5
---

# Batch APIs

The Python binding provides nine batch APIs that fan out multiple RPCs with **bounded concurrency** (at most `MAX_BATCH_RPC_IN_FLIGHT = 64` in flight). Each batch completes in a **single PyO3 boundary crossing**, eliminating per-call GIL acquisition and making them dramatically faster than N individual calls under GIL contention.

## When to Use Batch APIs

| Scenario                          | Use batch? | Why                                    |
| --------------------------------- | ---------- | -------------------------------------- |
| Check existence of 100+ paths     | `batch_exists` | One GIL crossing instead of 100       |
| Fetch metadata for a file list    | `batch_get_status` | Results in input order, bounded fan-out |
| Create / delete / rename a batch  | `batch_create_*` / `batch_delete` / `batch_rename` | Concurrent ops; partial changes may remain on failure |
| Open 50 files for parallel read   | `batch_open_file` | Streams cleaned up on partial failure  |
| List multiple directories         | `batch_list_status` / `batch_list_status_grouped` | Concurrent listing |

:::warning
All batch APIs **fail the whole batch on the first error**, but operations that already completed are **not rolled back**. Use individual calls if you need per-path error isolation or atomicity.
:::

## Status APIs

### `batch_exists(paths)` → `list[bool]`

```python
paths = ["/data/a", "/data/b", "/data/missing"]
results = await fs.batch_exists(paths)
# [True, True, False] — in input order
```

### `batch_get_status(paths)` → `list[URIStatus]`

```python
statuses = await fs.batch_get_status(["/data/a", "/data/b"])
for s in statuses:
    print(f"{s.path}: {s.length} bytes")
```

Raises `NotFound` if any path is missing — the whole batch fails.

### `list_status_grouped(path)` → `URIStatusList` (lazy)

```python
grouped = await fs.list_status_grouped("/data", recursive=False)
print(len(grouped))     # O(1) — no URIStatus objects created
first = grouped[0]      # materialises one URIStatus on demand
for entry in grouped:   # iteration materialises one at a time
    print(entry.name)
```

`URIStatusList` is a **lazy** container: `len()` is O(1) with zero object creation, and `__getitem__` / `__iter__` materialise `URIStatus` objects on demand. This reduces GIL occupancy by ~99% for N=100 entries.

### `batch_list_status_grouped(dirs)` → `list[URIStatusList]`

```python
dirs = ["/data/d1", "/data/d2", "/data/d3"]
groups = await fs.batch_list_status_grouped(dirs, recursive=False)
for i, g in enumerate(groups):
    print(f"dir {i}: {len(g)} entries")
```

Lazy counterpart to `batch_list_status`. Each directory's entries are returned as a `URIStatusList` (1 Python object per directory) instead of `list[URIStatus]` (N objects per directory).

### `batch_list_status(dirs)` → `list[list[URIStatus]]`

Eager variant — materialises all entries immediately. Use when you need a plain `list[URIStatus]` for slicing or library interop.

## File-Lifecycle APIs

### `batch_create_file(paths)` → `list[int]`

Creates and closes an empty file at every path. Returns bytes written per file (always 0 for empty files) in input order.

```python
files = ["/data/f1", "/data/f2", "/data/f3"]
written = await fs.batch_create_file(files)
# [0, 0, 0]
```

### `batch_create_dir(paths)` → `None`

```python
dirs = ["/data/d1", "/data/d2"]
await fs.batch_create_dir(dirs, recursive=True)
```

### `batch_rename(pairs)` → `None`

`pairs` is a **flat** list of alternating source and destination: `[src_0, dst_0, src_1, dst_1, ...]`. Length must be even.

```python
await fs.batch_rename(["/data/old1", "/data/new1", "/data/old2", "/data/new2"])
```

Raises `ValueError` if the list length is odd.

### `batch_delete(paths)` → `None`

```python
await fs.batch_delete(["/data/d1", "/data/d2"], recursive=True)
```

Options: `recursive`, `unchecked` (skip empty-check), `goosefs_only` (don't propagate to UFS).

:::tip
When batch-deleting a tree, only include the **parent directories** (with `recursive=True`). Including child files in the same batch can race: if a parent finishes first, the child delete sees a missing path and fails the whole batch.
:::

## `batch_open_file(paths)` → `list[AsyncFileReader]`

Opens N read streams concurrently. Returns readers in input order.

```python
readers = await fs.batch_open_file(["/data/a", "/data/b", "/data/c"])
for r in readers:
    data = await r.read()
    print(len(data))
```

**Resource cleanup on partial failure**: after all dispatched open attempts complete, if any path failed, all successfully opened streams are dropped (their `Drop` releases worker resources) and the batch raises an error.

## Sync API

All batch APIs are also available on the synchronous `Goosefs` wrapper:

```python
with Goosefs(cfg) as fs:
    results = fs.batch_exists(["/data/a", "/data/b"])
    fs.batch_create_dir(["/data/d1", "/data/d2"])
```

:::note
`batch_open_file` is **async-only** — the synchronous `Goosefs` wrapper does not expose it because it returns `AsyncFileReader` objects that require an asyncio runtime. All other batch APIs are available on both `AsyncGoosefs` and `Goosefs`.
:::

## Performance Characteristics

Batch APIs complete in a **single PyO3 boundary crossing** instead of N. For a directory with N=100 entries, `list_status_grouped` creates 1 Python object (~0.3 µs GIL occupancy) instead of 100 `URIStatus` objects (~33.4 µs total), reducing GIL occupancy by ~99%. The speedup comes from eliminating per-call PyO3 boundary crossings (each briefly acquires the GIL and allocates Python objects).
