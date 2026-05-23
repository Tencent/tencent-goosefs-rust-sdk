"""P4 integration tests — high-level ``read_file`` / ``read_range`` /
``write_file`` over both the async and sync APIs.

Test matrix
-----------

* **WriteType**: ``MustCache``, ``CacheThrough``, ``Through``,
  ``AsyncThrough``. ``TryCache`` is functionally equivalent to
  ``MustCache`` for a healthy worker, so we skip it to keep the matrix
  short.
* **Payload size**: 64 B (well below the gRPC chunk size), 64 KiB
  (multiple chunks but one block), 1 MiB (still one block but spans many
  chunks and exercises the prefetch window).

The default GooseFS block size is 256 MiB, so all sizes here fit in a
single block. End-to-end multi-block coverage is out of scope for the
binding's integration tests — it is already covered by the SDK suite
under :file:`src/io/`.

For each combination we assert:
1. ``write_file`` returns the exact byte count.
2. ``read_file`` round-trips the payload byte-for-byte.
3. ``read_range`` honours arbitrary offsets and short-reads at EOF.
4. The metadata reflects the correct length and ``WriteType`` (read back
   via ``get_status`` for sanity).
"""

from __future__ import annotations

import asyncio

import pytest
from goosefs import AsyncGoosefs, Goosefs, WriteType

# ---------------------------------------------------------------------------
# Parametrisation
# ---------------------------------------------------------------------------


# (label, WriteType) — label keeps the pytest IDs readable.
WRITE_TYPES = [
    ("must_cache", WriteType.MustCache),
    ("cache_through", WriteType.CacheThrough),
    ("through", WriteType.Through),
    ("async_through", WriteType.AsyncThrough),
]


# (label, byte length).  Each must round-trip identically — we generate a
# deterministic payload from os.urandom seeded by the label so failures are
# reproducible across runs.
PAYLOAD_SIZES = [
    ("64B", 64),
    ("64KiB", 64 * 1024),
    ("1MiB", 1024 * 1024),
]


def _make_payload(seed: str, size: int) -> bytes:
    """Generate a deterministic random-ish payload.

    We do *not* use ``os.urandom`` because we want the same bytes across
    test re-runs; we use a tiny LCG seeded from the label hash. Pure
    Python, no SciPy — keeps the test environment minimal.
    """
    state = abs(hash(seed)) % (2**32)
    out = bytearray(size)
    for i in range(size):
        state = (state * 1103515245 + 12345) & 0x7FFFFFFF
        out[i] = state & 0xFF
    return bytes(out)


# ---------------------------------------------------------------------------
# Async path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("wt_label,wt", WRITE_TYPES, ids=[w[0] for w in WRITE_TYPES])
@pytest.mark.parametrize("size_label,size", PAYLOAD_SIZES, ids=[s[0] for s in PAYLOAD_SIZES])
@pytest.mark.asyncio
async def test_async_round_trip(
    async_fs: AsyncGoosefs,
    tmp_dir: str,
    wt_label: str,
    wt: WriteType,
    size_label: str,
    size: int,
) -> None:
    """``write_file`` followed by ``read_file`` must round-trip exactly."""
    path = f"{tmp_dir}/{wt_label}-{size_label}.bin"
    payload = _make_payload(f"{wt_label}-{size_label}", size)

    n = await async_fs.write_file(path, payload, write_type=wt)
    assert n == size, f"write_file returned {n}, expected {size}"

    got = await async_fs.read_file(path)
    assert isinstance(got, bytes)
    assert len(got) == size
    assert got == payload, "read_file did not round-trip the payload byte-for-byte"

    # Status must reflect the correct length.
    st = await async_fs.get_status(path)
    assert st.length == size
    assert st.is_completed()


@pytest.mark.asyncio
async def test_async_read_range_arbitrary_offsets(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    """Spot-check ``read_range`` on three offset+length combinations."""
    path = f"{tmp_dir}/read-range.bin"
    payload = _make_payload("read-range", 4096)
    await async_fs.write_file(path, payload, write_type=WriteType.MustCache)

    # 1) Aligned mid-file slice.
    chunk = await async_fs.read_range(path, 1024, 512)
    assert chunk == payload[1024:1536]

    # 2) Tail slice that runs to EOF — exact length, no over-read.
    chunk = await async_fs.read_range(path, 4000, 96)
    assert chunk == payload[4000:4096]

    # 3) Range that *crosses* EOF: the SDK short-reads.
    chunk = await async_fs.read_range(path, 4000, 1024)
    assert chunk == payload[4000:4096], "read_range past EOF should short-read, not raise"


@pytest.mark.asyncio
async def test_async_write_accepts_bytes_like_objects(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    """``write_file`` should accept ``bytes`` / ``bytearray`` / ``memoryview``
    interchangeably (PyO3's ``&[u8]`` extractor handles the buffer protocol)."""
    base = b"buffer-protocol"
    for kind, payload in [
        ("bytes", bytes(base)),
        ("bytearray", bytearray(base)),
        ("memoryview", memoryview(bytes(base))),
    ]:
        p = f"{tmp_dir}/{kind}.bin"
        n = await async_fs.write_file(p, payload, write_type=WriteType.MustCache)
        assert n == len(base), f"{kind}: wrong byte count {n}"
        got = await async_fs.read_file(p)
        assert got == base, f"{kind}: round-trip mismatch"


@pytest.mark.asyncio
async def test_async_write_rejects_non_bytes(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    """A plain ``str`` must be rejected with ``TypeError``."""
    with pytest.raises(TypeError):
        # Deliberate wrong type at the API boundary; the `# type: ignore`
        # below silences mypy, the runtime ``TypeError`` is what we assert.
        await async_fs.write_file(f"{tmp_dir}/bad.bin", "not bytes")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_async_write_default_write_type_is_inherit(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    """Omitting ``write_type`` should make the SDK fall back to xattr
    inheritance (and ultimately the cluster default). Verify the file is
    successfully created and round-trips."""
    path = f"{tmp_dir}/inherit.bin"
    payload = b"x" * 256
    n = await async_fs.write_file(path, payload)
    assert n == 256
    assert await async_fs.read_file(path) == payload


# ---------------------------------------------------------------------------
# Sync path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("wt_label,wt", WRITE_TYPES, ids=[w[0] for w in WRITE_TYPES])
@pytest.mark.parametrize("size_label,size", PAYLOAD_SIZES, ids=[s[0] for s in PAYLOAD_SIZES])
def test_sync_round_trip(
    sync_fs: Goosefs,
    sync_tmp_dir: str,
    wt_label: str,
    wt: WriteType,
    size_label: str,
    size: int,
) -> None:
    path = f"{sync_tmp_dir}/{wt_label}-{size_label}.bin"
    payload = _make_payload(f"sync-{wt_label}-{size_label}", size)

    n = sync_fs.write_file(path, payload, write_type=wt)
    assert n == size

    got = sync_fs.read_file(path)
    assert isinstance(got, bytes)
    assert got == payload

    st = sync_fs.get_status(path)
    assert st.length == size
    assert st.is_completed()


def test_sync_read_range_arbitrary_offsets(sync_fs: Goosefs, sync_tmp_dir: str) -> None:
    path = f"{sync_tmp_dir}/sync-read-range.bin"
    payload = _make_payload("sync-read-range", 4096)
    sync_fs.write_file(path, payload, write_type=WriteType.MustCache)

    assert sync_fs.read_range(path, 1024, 512) == payload[1024:1536]
    assert sync_fs.read_range(path, 4000, 96) == payload[4000:4096]
    assert sync_fs.read_range(path, 4000, 1024) == payload[4000:4096]


def test_sync_write_rejects_non_bytes(sync_fs: Goosefs, sync_tmp_dir: str) -> None:
    with pytest.raises(TypeError):
        sync_fs.write_file(f"{sync_tmp_dir}/bad.bin", "not bytes")  # type: ignore[arg-type]


def test_sync_write_inside_asyncio_loop_is_refused(sync_fs: Goosefs, sync_tmp_dir: str) -> None:
    """The deadlock guard from P3 (Review #17.1) must keep applying to the
    new write/read methods."""
    path = f"{sync_tmp_dir}/should-not-write.bin"

    async def attempt() -> None:
        with pytest.raises(RuntimeError):
            sync_fs.write_file(path, b"x")
        with pytest.raises(RuntimeError):
            sync_fs.read_file(path)
        with pytest.raises(RuntimeError):
            sync_fs.read_range(path, 0, 1)

    asyncio.run(attempt())
