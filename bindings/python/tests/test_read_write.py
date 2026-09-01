# Copyright (C) 2026 Tencent. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""P4 integration tests — high-level ``read_file`` / ``read_range`` /
``write_file`` over both the async and sync APIs.

Test matrix
-----------

* **WriteType**: ``MustCache``, ``CacheThrough``, ``Through``,
  ``AsyncThrough``. ``TryCache`` is functionally equivalent to
  ``MustCache`` for a healthy worker, so we skip it to keep the matrix
  short. The shared :func:`config` fixture pins
  ``replication.durable`` / ``durable.min`` to ``1`` because the Docker
  cluster has a single worker.
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
import concurrent.futures

import pytest
from goosefs import AsyncGoosefs, Config, Goosefs, WriteType
from goosefs.exceptions import GoosefsError, InvalidArgument, IsADirectory

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
async def test_async_must_cache_get_status_reports_in_goosefs_percentage(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    """Same contract as the sync test: MustCache → ``in_goose_fs_percentage > 0``."""
    path = f"{tmp_dir}/must-cache-pct.bin"
    await async_fs.write_file(path, b"x" * 4096, write_type=WriteType.MustCache)

    st = await async_fs.get_status(path)
    assert st.cacheable is True
    assert st.is_persisted() is False
    assert st.in_goose_fs_percentage > 0


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
async def test_async_read_range_rejects_negative_offset_and_length(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    """Negatives must be ``InvalidArgument``, not PyO3 ``OverflowError``
    from extracting into ``u64``.
    """
    path = f"{tmp_dir}/read-range-neg.bin"
    await async_fs.write_file(path, b"x" * 10)

    with pytest.raises(InvalidArgument, match="non-negative"):
        await async_fs.read_range(path, 0, -1)
    with pytest.raises(InvalidArgument, match="non-negative"):
        await async_fs.read_range(path, -1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [-2, -100, -(1 << 31)])
async def test_async_positioned_read_rejects_length_below_minus_one(
    async_fs: AsyncGoosefs, tmp_dir: str, length: int
) -> None:
    """Only ``-1`` means "read to end"; other negatives are caller bugs.

    Treating e.g. ``-2`` (a miscomputed ``end - start``) as "read to end"
    silently returns the whole block — potentially tens of MiB the caller
    never asked for. ``ValueError`` matches the sibling checks on this same
    method (``offset``, ``chunk_size``) and the lower-level
    ``read_block_positioned`` it wraps; note ``read_range`` reports its own
    negatives as ``InvalidArgument``, which is *not* a ``ValueError``.
    """
    path = f"{tmp_dir}/pread-neg-len.bin"
    await async_fs.write_file(path, b"x" * 4096)

    with pytest.raises(ValueError, match=r"length must be -1 .* or non-negative"):
        await async_fs.positioned_read(path, offset=0, length=length)


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


def test_sync_must_cache_get_status_reports_in_goosefs_percentage(
    sync_fs: Goosefs, sync_tmp_dir: str
) -> None:
    """MustCache writes live entirely in GooseFS; ``in_goose_fs_percentage``
    must not stay at Master's default of 0 (Python ``get_status`` has no
    ``checkBlockReplicas`` argument to trigger the Java CheckBlocks path).
    """
    path = f"{sync_tmp_dir}/must-cache-pct.bin"
    sync_fs.write_file(path, b"x" * 4096, write_type=WriteType.MustCache)

    st = sync_fs.get_status(path)
    assert st.cacheable is True
    assert st.is_persisted() is False
    assert st.in_goose_fs_percentage > 0


# --------------------------------------------------------------------------
# Repeated reads of one path
#
# The worker admits a UFS block read only while that block's session count is
# below ``maxUfsReadConcurrency``. ``read_file`` / ``read_range`` used to leave
# that option unset, which decodes as 0, so the first read of a block worked
# (no session entry yet) and every later one blocked forever waiting for a
# permit. The trigger is the path, not the verb: reads that did send the option
# (``positioned_read``, ``open_file``) still left the entry behind and wedged
# the next ``read_file``, in the same process or a fresh one.
# --------------------------------------------------------------------------

_ONESHOT_READS = {
    "read_file": lambda fs, path, n: fs.read_file(path),
    "read_range": lambda fs, path, n: fs.read_range(path, 0, n),
    "read_range_small": lambda fs, path, n: fs.read_range(path, 0, min(n, 4096)),
    "positioned_read": lambda fs, path, n: fs.positioned_read(
        path, block_index=0, offset=0, length=-1
    ),
}

# Ordered pairs covering the reported hangs plus their mirror images. The
# positioned→positioned pair is the one combination that always worked (both
# sides sent the option), kept here as the control.
_SECOND_READ_PAIRS = [
    ("read_file", "read_file"),
    ("read_file", "read_range"),
    ("read_file", "positioned_read"),
    ("read_range", "read_range"),
    ("read_range", "read_file"),
    ("read_range_small", "read_range"),
    ("positioned_read", "read_file"),
    ("positioned_read", "read_range"),
    ("positioned_read", "positioned_read"),
]


@pytest.mark.parametrize(
    "first,second",
    _SECOND_READ_PAIRS,
    ids=[f"{first}-then-{second}" for first, second in _SECOND_READ_PAIRS],
)
def test_sync_second_oneshot_read_of_same_path_does_not_hang(
    sync_fs: Goosefs, sync_tmp_dir: str, first: str, second: str
) -> None:
    """Reading one path twice must work for every pair of one-shot verbs.

    ``Through`` is the write type that reproduced on the test cluster. The
    reads run on a worker thread so a native hang surfaces as a timeout
    instead of wedging the whole session.
    """
    path = f"{sync_tmp_dir}/twice-{first}-then-{second}.bin"
    payload = _make_payload(f"{first}-{second}", 64 * 1024)
    n = len(payload)
    sync_fs.write_file(path, payload, write_type=WriteType.Through)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        got_first = pool.submit(_ONESHOT_READS[first], sync_fs, path, n).result(timeout=20)
        got_second = pool.submit(_ONESHOT_READS[second], sync_fs, path, n).result(timeout=20)

    assert got_first == payload[: len(got_first)]
    assert got_second == payload[: len(got_second)]


def test_sync_second_reader_instance_can_read_an_already_read_path(
    sync_fs: Goosefs, sync_tmp_dir: str, config: Config
) -> None:
    """The stuck state lives on the worker, so a fresh instance must work too.

    Reading a brand-new path from a new instance always worked; reading one
    that another instance had already read is what hung, which is what pins
    this to worker-side state rather than anything in the client.
    """
    path = f"{sync_tmp_dir}/read-by-two-instances.bin"
    payload = _make_payload("read-by-two-instances", 64 * 1024)
    sync_fs.write_file(path, payload, write_type=WriteType.Through)
    assert sync_fs.read_file(path) == payload

    other = Goosefs(config)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(other.read_file, path).result(timeout=20) == payload
    finally:
        other.close()


# Cycled by the repeated-read tests so each round trips a different verb.
_READ_CYCLE = ["read_file", "read_range", "positioned_read", "read_range_small"]


@pytest.mark.parametrize("label,write_type", WRITE_TYPES, ids=[label for label, _ in WRITE_TYPES])
def test_sync_many_reads_of_same_path_stay_healthy(
    sync_fs: Goosefs, sync_tmp_dir: str, label: str, write_type: WriteType
) -> None:
    """One path, many reads, every write type.

    Reading twice is not enough to prove this fixed. The worker allows
    ``maxUfsReadConcurrency`` (8) sessions per block, so a client that sends
    the option but still leaked one session per read would pass every 2-read
    test here and only wedge on the 9th. This walks well past the limit.

    The payload is 4 x 64 KiB, the multi-page shape from the report, and the
    verbs rotate so the leak would show up whichever one is responsible.

    Only ``Through`` reproduces the original hang: the other write types leave
    the block in the worker's cache, so reads never open a UFS block session
    and never take a permit. They are kept as coverage of the ordinary
    repeated-read path (and they would start reproducing it once a cached
    block is evicted and has to be re-read from UFS).
    """
    path = f"{sync_tmp_dir}/many-reads-{label}.bin"
    payload = _make_payload(f"many-reads-{label}", 4 * 64 * 1024)
    n = len(payload)
    sync_fs.write_file(path, payload, write_type=write_type)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        for i in range(12):
            verb = _READ_CYCLE[i % len(_READ_CYCLE)]
            got = pool.submit(_ONESHOT_READS[verb], sync_fs, path, n).result(timeout=20)
            assert got == payload[: len(got)], f"read #{i + 1} via {verb} returned wrong bytes"


def test_sync_many_reads_of_multi_chunk_file(sync_fs: Goosefs, sync_tmp_dir: str) -> None:
    """Same, for a payload spanning several gRPC chunks.

    The default chunk size is 1 MiB, so 4 MiB exercises the chunked streaming
    path instead of the single-frame one the smaller payloads take.
    """
    path = f"{sync_tmp_dir}/many-reads-multi-chunk.bin"
    payload = _make_payload("many-reads-multi-chunk", 4 * 1024 * 1024)
    sync_fs.write_file(path, payload, write_type=WriteType.Through)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        for i in range(10):
            got = pool.submit(sync_fs.read_file, path).result(timeout=30)
            assert got == payload, f"read #{i + 1} returned wrong bytes"


def test_sync_streaming_and_oneshot_reads_interleave(sync_fs: Goosefs, sync_tmp_dir: str) -> None:
    """``open_file`` repeatedly, interleaved with one-shot reads.

    ``open_file`` with an explicit close is the one path that kept working
    throughout the report, so it doubles as the control: it must keep working,
    and it must not wedge the one-shot reads that follow it.
    """
    path = f"{sync_tmp_dir}/streaming-interleave.bin"
    payload = _make_payload("streaming-interleave", 64 * 1024)
    n = len(payload)
    sync_fs.write_file(path, payload, write_type=WriteType.Through)

    def _stream() -> bytes:
        with sync_fs.open_file(path) as reader:
            return reader.read()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(_stream).result(timeout=20) == payload
        assert pool.submit(_stream).result(timeout=20) == payload
        assert pool.submit(sync_fs.read_file, path).result(timeout=20) == payload
        assert pool.submit(_stream).result(timeout=20) == payload
        assert pool.submit(sync_fs.read_range, path, 0, n).result(timeout=20) == payload
        assert pool.submit(_stream).result(timeout=20) == payload


@pytest.mark.asyncio
async def test_async_second_oneshot_read_of_same_path_does_not_hang(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    """Async counterpart, including the cross-verb ``read_file`` → ``read_range``."""
    path = f"{tmp_dir}/same-path-twice-async.bin"
    payload = _make_payload("same-path-twice-async", 64 * 1024)
    await async_fs.write_file(path, payload, write_type=WriteType.Through)

    first = await asyncio.wait_for(async_fs.read_file(path), timeout=20)
    second = await asyncio.wait_for(async_fs.read_file(path), timeout=20)
    ranged = await asyncio.wait_for(async_fs.read_range(path, 0, len(payload)), timeout=20)
    assert first == payload
    assert second == payload
    assert ranged == payload


@pytest.mark.asyncio
async def test_async_streaming_read_then_oneshot_read_same_path(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    """``open_file`` and the one-shot verbs must not wedge each other.

    Both take the same worker-side block, so whichever reads it first decides
    whether the other can get in.
    """
    path = f"{tmp_dir}/streaming-then-oneshot.bin"
    payload = _make_payload("streaming-then-oneshot", 64 * 1024)
    await async_fs.write_file(path, payload, write_type=WriteType.Through)

    reader = await async_fs.open_file(path)
    try:
        assert await asyncio.wait_for(reader.read(), timeout=20) == payload
    finally:
        await reader.close()

    assert await asyncio.wait_for(async_fs.read_file(path), timeout=20) == payload

    reader = await async_fs.open_file(path)
    try:
        assert await asyncio.wait_for(reader.read(), timeout=20) == payload
    finally:
        await reader.close()


def test_sync_read_range_arbitrary_offsets(sync_fs: Goosefs, sync_tmp_dir: str) -> None:
    path = f"{sync_tmp_dir}/sync-read-range.bin"
    payload = _make_payload("sync-read-range", 4096)
    sync_fs.write_file(path, payload, write_type=WriteType.MustCache)

    assert sync_fs.read_range(path, 1024, 512) == payload[1024:1536]
    assert sync_fs.read_range(path, 4000, 96) == payload[4000:4096]
    assert sync_fs.read_range(path, 4000, 1024) == payload[4000:4096]


def test_sync_read_range_rejects_negative_offset_and_length(
    sync_fs: Goosefs, sync_tmp_dir: str
) -> None:
    """Negatives must be ``InvalidArgument``, not PyO3 ``OverflowError``."""
    path = f"{sync_tmp_dir}/sync-read-range-neg.bin"
    sync_fs.write_file(path, b"x" * 10)

    with pytest.raises(InvalidArgument, match="non-negative"):
        sync_fs.read_range(path, 0, -1)
    with pytest.raises(InvalidArgument, match="non-negative"):
        sync_fs.read_range(path, -1, 1)


@pytest.mark.parametrize("length", [-2, -100, -(1 << 31)])
def test_sync_positioned_read_rejects_length_below_minus_one(
    sync_fs: Goosefs, sync_tmp_dir: str, length: int
) -> None:
    """Sync counterpart — see the async test for the rationale."""
    path = f"{sync_tmp_dir}/sync-pread-neg-len.bin"
    sync_fs.write_file(path, b"x" * 4096)

    with pytest.raises(ValueError, match=r"length must be -1 .* or non-negative"):
        sync_fs.positioned_read(path, offset=0, length=length)


def test_sync_positioned_read_keeps_minus_one_and_zero_semantics(
    sync_fs: Goosefs, sync_tmp_dir: str
) -> None:
    """Rejecting ``length < -1`` must not disturb the two legal edge values."""
    path = f"{sync_tmp_dir}/sync-pread-len-edges.bin"
    payload = _make_payload("sync-pread-len-edges", 4096)
    sync_fs.write_file(path, payload)

    assert sync_fs.positioned_read(path, offset=0, length=-1) == payload
    assert sync_fs.positioned_read(path, offset=0, length=0) == b""
    assert sync_fs.positioned_read(path, offset=0, length=16) == payload[:16]


def test_sync_read_file_on_directory_raises_is_a_directory(
    sync_fs: Goosefs, sync_tmp_dir: str
) -> None:
    """``read_file`` / ``read_range`` on a directory must raise, not return ``b''``."""
    path = f"{sync_tmp_dir}/is-a-dir"
    sync_fs.mkdir(path)
    with pytest.raises(IsADirectory):
        sync_fs.read_file(path)
    with pytest.raises(IsADirectory):
        sync_fs.read_range(path, 0, 1)


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


# ---------------------------------------------------------------------------
# batch_open_file — fan out N read streams with bounded concurrency.
# ---------------------------------------------------------------------------


async def test_batch_open_file_reads_all_in_order(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    """Open N files in parallel and verify contents match in input order."""
    paths = [f"{tmp_dir}/bof-{i}.bin" for i in range(3)]
    payloads = [f"payload-{i}".encode() for i in range(3)]
    for p, data in zip(paths, payloads):
        await async_fs.write_file(p, data, write_type=WriteType.MustCache)

    readers = await async_fs.batch_open_file(paths)
    assert len(readers) == len(paths)

    contents = []
    for r in readers:
        data = await r.read()
        contents.append(data)
    assert contents == payloads


async def test_batch_open_file_single_path(async_fs: AsyncGoosefs, tmp_dir: str) -> None:
    """A one-element batch should still return a list with one reader."""
    path = f"{tmp_dir}/single.bin"
    await async_fs.write_file(path, b"solo", write_type=WriteType.MustCache)

    readers = await async_fs.batch_open_file([path])
    assert len(readers) == 1
    data = await readers[0].read()
    assert data == b"solo"


async def test_batch_open_file_missing_path_fails_whole_batch(
    async_fs: AsyncGoosefs, tmp_dir: str
) -> None:
    """If any path is missing the whole batch fails; already-opened
    streams are dropped to avoid worker-connection leaks."""
    good = f"{tmp_dir}/exists.bin"
    missing = f"{tmp_dir}/missing.bin"
    await async_fs.write_file(good, b"x", write_type=WriteType.MustCache)

    with pytest.raises(GoosefsError):
        await async_fs.batch_open_file([good, missing])
