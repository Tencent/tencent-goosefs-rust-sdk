## Summary

Add Python examples and integration tests for the nine batch APIs (`batch_get_status`, `batch_exists`, `batch_create_file`, `batch_create_dir`, `batch_rename`, `batch_delete`, `batch_open_file`, `list_status_grouped`, `batch_list_status_grouped`) that previously had no runnable example or IT coverage on `AsyncGoosefs` / `Goosefs`.

## Why

The batch APIs were implemented in the Python binding but sat invisible: no example script demonstrates how a user wires them together, and no integration test guards the fan-out, resource-cleanup, or lazy-materialisation paths. Without coverage, a regression in the bounded-concurrency pipeline or the `batch_open_file` stream-drop-on-failure logic would not be caught until a downstream consumer hits it.

## What

### Batch status APIs (examples)

`batch_status.py` walks through `list_status_grouped` (single path, lazy `URIStatusList`), `batch_list_status_grouped` (multi-directory lazy), `batch_get_status` (eager `list[URIStatus]` in input order), and `batch_exists` (`list[bool]` in input order). Each call is followed by inline assertions so the example doubles as a smoke test.

### Batch file-lifecycle APIs (examples)

`batch_files.py` exercises `batch_create_dir`, `batch_create_file`, `batch_open_file` (with per-stream content verification via `read()`), `batch_rename` (flat `[src,dst,...]` pairs), and `batch_delete` (recursive teardown). The example creates a tree, mutates it in bulk, verifies every step with `batch_exists` / `batch_get_status`, and cleans up.

### Lazy URIStatusList tests

`test_list_status_grouped.py` covers both the async and sync entry points for the `*_grouped` return type, asserting `len()` is correct without materialising `URIStatus` objects, `__getitem__` returns a real `URIStatus` on positive and negative indices, `IndexError` on out-of-range, `__iter__` lists all entries in order, `__bool__` reflects emptiness, and `recursive=True` expands the tree.

### Batch mutation / resource tests

`test_metadata.py` gains 7 tests: `batch_create_dir` (flat + recursive), `batch_create_file` (empty-file byte-count assertion), `batch_rename` (happy path + odd-length `ValueError`), and `batch_delete` (plain + recursive). `test_read_write.py` gains 3 `batch_open_file` tests: parallel read with in-order content verification, single-path batch, and missing-path fails-whole-batch with guaranteed stream cleanup.

## Files changed

| File | Description |
|------|-------------|
| `bindings/python/examples/batch_status.py` | Example for `list_status_grouped`, `batch_list_status_grouped`, `batch_get_status`, `batch_exists` |
| `bindings/python/examples/batch_files.py` | Example for `batch_create_file`, `batch_create_dir`, `batch_open_file`, `batch_rename`, `batch_delete` |
| `bindings/python/tests/test_list_status_grouped.py` | 11 IT tests for lazy `URIStatusList` semantics (async + sync) |
| `bindings/python/tests/test_metadata.py` | 7 IT tests for `batch_create_dir`, `batch_create_file`, `batch_rename`, `batch_delete` |
| `bindings/python/tests/test_read_write.py` | 3 IT tests for `batch_open_file` |

## Test plan

- All 5 files pass `python3 -m py_compile` syntax check.
- All 21 new test cases are wired through the existing `conftest.py` fixtures (`async_fs` / `sync_fs` / `tmp_dir` / `sync_tmp_dir`) and skip cleanly when `$GOOSEFS_MASTER_ADDR` is unset.
- CI coverage is automatic: `ci_integration.yml` runs the `python` matrix with the GooseFS Docker fixture on any PR touching `bindings/python/**` — no workflow change required.
