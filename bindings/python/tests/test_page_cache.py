"""Integration tests for the client-side local page cache (config passthrough).

These verify that enabling the local page cache via ``Config(properties=...)``
is transparent end-to-end and actually caches data on disk.

The page cache is integrated into the seekable streaming reader
(``fs.open_file(...)`` → ``read`` / ``read_at``), which wraps the SDK's
``GoosefsFileInStream``. The one-shot ``read_file`` / ``read_range`` helpers use
the worker-direct path and do **not** consult the cache, so these tests read
through ``open_file`` to exercise it. The page-cache properties are parsed by
the same Rust ``from_properties_str`` path the SDK uses, so no binding change is
needed — this test guards that contract.

Requires a running cluster at ``$GOOSEFS_MASTER_ADDR``; skipped otherwise.
Set ``GOOSEFS_AUTH_TYPE`` (``nosasl`` / ``simple``) to match your cluster.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
import uuid

import pytest
from goosefs import Config, Goosefs, WriteType

GOOSEFS_MASTER_ADDR = os.environ.get("GOOSEFS_MASTER_ADDR")
GOOSEFS_AUTH_TYPE = os.environ.get("GOOSEFS_AUTH_TYPE", "NOSASL")

pytestmark = pytest.mark.skipif(
    not GOOSEFS_MASTER_ADDR,
    reason="GOOSEFS_MASTER_ADDR is not set; skipping page-cache integration tests",
)

PAGE_SIZE = 64 * 1024


def _cache_config(cache_dir: str, *, enabled: bool = True, sequential_read: bool = True) -> Config:
    """Build a Config with the local page cache configured via properties."""
    return Config(
        GOOSEFS_MASTER_ADDR,
        properties={
            "goosefs.security.authentication.type": GOOSEFS_AUTH_TYPE,
            "goosefs.user.client.cache.enabled": "true" if enabled else "false",
            "goosefs.user.client.cache.page.size": str(PAGE_SIZE),
            "goosefs.user.client.cache.size": "64MB",
            "goosefs.user.client.cache.dirs": cache_dir,
            "goosefs.user.client.cache.eviction.policy": "LRU",
            # Inline fill so a repeat read is guaranteed to hit the cache.
            "goosefs.user.client.cache.async.write.enabled": "false",
            # These tests exercise the cache through sequential ``read`` (see
            # ``_read_all_via_stream``); sequential reads bypass the cache by
            # default, so opt them in for the test to observe back-fill/hits.
            "goosefs.user.client.cache.sequential.read.enabled": (
                "true" if sequential_read else "false"
            ),
        },
    )


def _make_payload(size: int) -> bytes:
    return bytes((i % 251) for i in range(size))


def _scratch_path() -> str:
    name = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    return f"/tmp/pygoosefs-cache-tests/{name}.bin"


def _read_all_via_stream(fs: Goosefs, path: str) -> bytes:
    """Read the whole file through the cache-aware streaming reader."""
    buf = bytearray()
    with fs.open_file(path) as r:
        while True:
            chunk = r.read(PAGE_SIZE)
            if not chunk:
                break
            buf.extend(chunk)
    return bytes(buf)


def _count_files(root: str) -> int:
    return sum(len(files) for _, _, files in os.walk(root))


def test_cache_enabled_reads_and_persists_pages() -> None:
    """Cold→warm reads via open_file round-trip AND persist pages on disk."""
    cache_dir = tempfile.mkdtemp(prefix="pygfs_cache_")
    fs = Goosefs(_cache_config(cache_dir, enabled=True))
    path = _scratch_path()
    try:
        fs.mkdir("/tmp/pygoosefs-cache-tests", recursive=True)
        payload = _make_payload(256 * 1024)  # 4 pages of 64 KiB
        fs.write_file(path, payload, write_type=WriteType.CacheThrough)

        # Cold read fills the cache.
        assert _read_all_via_stream(fs, path) == payload, "cold read mismatch"
        assert _count_files(cache_dir) > 0, "cold read must back-fill pages to disk"

        # Warm read still correct (served from cache).
        assert _read_all_via_stream(fs, path) == payload, "warm read mismatch"

        # Positioned read on the cached stream.
        with fs.open_file(path) as r:
            chunk = r.read_at(PAGE_SIZE, 4096)
        assert chunk == payload[PAGE_SIZE : PAGE_SIZE + 4096], "read_at mismatch"
    finally:
        try:
            fs.delete(path)
        except Exception:  # noqa: BLE001
            pass
        fs.close()
        shutil.rmtree(cache_dir, ignore_errors=True)


def test_cache_overwrite_does_not_serve_stale() -> None:
    """Overwriting a file must not serve stale cached pages on reopen."""
    cache_dir = tempfile.mkdtemp(prefix="pygfs_cache_")
    fs = Goosefs(_cache_config(cache_dir, enabled=True))
    path = _scratch_path()
    try:
        fs.mkdir("/tmp/pygoosefs-cache-tests", recursive=True)

        v1 = b"\xaa" * (128 * 1024)
        fs.write_file(path, v1, write_type=WriteType.CacheThrough)
        assert _read_all_via_stream(fs, path) == v1  # populates cache

        # Overwrite with different content AND different length.
        fs.delete(path)
        v2 = b"\xbb" * (200 * 1024)
        fs.write_file(path, v2, write_type=WriteType.CacheThrough)

        # Reopen → overwrite detected → stale pages invalidated.
        assert _read_all_via_stream(fs, path) == v2, "must not serve stale cached bytes"
    finally:
        try:
            fs.delete(path)
        except Exception:  # noqa: BLE001
            pass
        fs.close()
        shutil.rmtree(cache_dir, ignore_errors=True)


def test_cache_disabled_still_reads() -> None:
    """Baseline: cache-disabled config reads correctly (toggle passthrough)."""
    cache_dir = tempfile.mkdtemp(prefix="pygfs_cache_")
    fs = Goosefs(_cache_config(cache_dir, enabled=False))
    path = _scratch_path()
    try:
        fs.mkdir("/tmp/pygoosefs-cache-tests", recursive=True)
        payload = _make_payload(64 * 1024)
        fs.write_file(path, payload, write_type=WriteType.CacheThrough)
        assert _read_all_via_stream(fs, path) == payload
        # Disabled cache must not create page files.
        assert _count_files(cache_dir) == 0, "disabled cache must not persist pages"
    finally:
        try:
            fs.delete(path)
        except Exception:  # noqa: BLE001
            pass
        fs.close()
        shutil.rmtree(cache_dir, ignore_errors=True)


def test_sequential_read_bypasses_cache_by_default() -> None:
    """With the cache enabled but sequential-read caching off (the default),
    sequential ``read`` must not back-fill pages, while random ``read_at``
    still populates the cache."""
    cache_dir = tempfile.mkdtemp(prefix="pygfs_cache_")
    fs = Goosefs(_cache_config(cache_dir, enabled=True, sequential_read=False))
    path = _scratch_path()
    try:
        fs.mkdir("/tmp/pygoosefs-cache-tests", recursive=True)
        payload = _make_payload(256 * 1024)  # 4 pages of 64 KiB
        fs.write_file(path, payload, write_type=WriteType.CacheThrough)

        # Sequential read is correct but bypasses the cache → no pages on disk.
        assert _read_all_via_stream(fs, path) == payload, "sequential read mismatch"
        assert _count_files(cache_dir) == 0, "sequential read must not back-fill by default"

        # Random read_at still uses the cache → pages are persisted.
        with fs.open_file(path) as r:
            chunk = r.read_at(PAGE_SIZE, 4096)
        assert chunk == payload[PAGE_SIZE : PAGE_SIZE + 4096], "read_at mismatch"
        assert _count_files(cache_dir) > 0, "random read_at must still back-fill pages"
    finally:
        try:
            fs.delete(path)
        except Exception:  # noqa: BLE001
            pass
        fs.close()
        shutil.rmtree(cache_dir, ignore_errors=True)
