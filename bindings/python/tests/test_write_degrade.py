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

"""Integration tests for the cache-write degradation path via the binding.

Scope
-----

The UFS-level semantics of degradation — that the bytes really land on the
under-store, and that ``forcePersisted`` is sent so the Master does not
schedule a redundant persist job — are covered by the SDK suite in
:file:`tests/write_degrade_e2e.rs`. That check needs to drop the GooseFS inode
and force a re-import with ``LoadMetadataPType.ALWAYS``, which the binding does
not expose.

What these tests cover instead is the part the Rust suite cannot: that the
behaviour survives the PyO3 layer. The binding delegates to
``GoosefsFileWriter`` rather than reimplementing write logic, so degradation
*should* be transparent — these tests are what turns that into something
checked rather than assumed. A binding that started intercepting errors, or
pinned its own write options, would break here while the Rust suite stayed
green.

Both cases are driven from client config alone, no fault injection needed:

* **Degrade** — an unsatisfiable persist watermark leaves no worker eligible
  for the first block. No block has opened yet, so the writer may fall back to
  a UFS-only write.
* **Abort** — ``durable.min`` above the achievable replica count breaks the
  replication contract, so falling back to a single UFS copy is forbidden.

Together they pin both directions: degradation happens when it should, and
does not happen when it must not.
"""

from __future__ import annotations

import uuid

import pytest
from goosefs import Config, Goosefs, WriteType
from goosefs.exceptions import GoosefsError

# Larger than any worker's persist capacity, so the watermark filter rejects
# every candidate for the first block.
UNSATISFIABLE_REMAIN_BYTES = str(2**63 - 1)


def _scratch_path(name: str) -> str:
    """A unique path directly under the Goosefs root.

    Flat, unlike the ``tmp_dir`` fixture the rest of the suite uses, because a
    write that reaches the UFS needs its parent directory to exist *on the
    UFS*. ``mkdir`` does not put it there — ``create_directory`` never sets the
    ``write_type`` field, so directories are cache-only — and neither does
    ``recursive=True`` when creating the file. A nested path therefore fails
    with ``NOT_FOUND: <ufs path> (No such file or directory)`` from the worker.
    The same constraint shapes the SDK suites in :file:`tests/write_degrade_e2e.rs`.
    """
    return f"/sdk-py-degrade-{uuid.uuid4().hex[:8]}-{name}"


def _connect(master_addr: str, **properties: str) -> Goosefs:
    return Goosefs(Config(master_addr, properties=properties))


def test_async_through_degrades_to_ufs(master_addr: str) -> None:
    """A first-block failure with no eligible worker must fall back to the UFS
    rather than surface as an error to the Python caller.

    Replication is pinned to 1 so the watermark is the only reason the
    candidate pool empties; otherwise on a single-worker cluster the default
    ``durable.min = 2`` would come up short too, and that is a *fatal*
    ResourceExhausted rather than a degrade.
    """
    fs = _connect(
        master_addr,
        **{
            "goosefs.user.block.worker.available.min.remain.bytes": UNSATISFIABLE_REMAIN_BYTES,
            "goosefs.user.file.replication.durable": "1",
            "goosefs.user.file.replication.durable.min": "1",
        },
    )
    path = _scratch_path("degraded.bin")
    try:
        payload = bytes(i % 241 for i in range(48 * 1024))

        written = fs.write_file(path, payload, write_type=WriteType.AsyncThrough, recursive=True)
        assert written == len(payload)

        assert fs.get_status(path).length == len(payload)
        assert fs.read_file(path) == payload, (
            "the whole buffer must survive the degrade, including the prefix "
            "the cache stream had already accepted"
        )
    finally:
        try:
            fs.delete(path, recursive=True)
        except Exception:  # noqa: BLE001 — cleanup only
            pass
        fs.close()


def test_broken_replica_contract_does_not_degrade(master_addr: str) -> None:
    """Degradation must not be a blanket fallback.

    When ``durable.min`` cannot be met, quietly writing a single UFS copy
    would break the durability the caller asked for, so the write has to fail.
    """
    fs = _connect(
        master_addr,
        **{
            "goosefs.user.file.replication.number": "1",
            "goosefs.user.file.replication.durable": "2",
            "goosefs.user.file.replication.durable.min": "9",
        },
    )
    try:
        with pytest.raises(GoosefsError):
            fs.write_file(
                _scratch_path("must-not-degrade.bin"),
                b"payload",
                write_type=WriteType.AsyncThrough,
                recursive=True,
            )
    finally:
        fs.close()


def test_new_persistence_config_keys_are_accepted(master_addr: str) -> None:
    """The async-persist keys added for Java parity must reach the SDK parser.

    The binding serialises ``properties`` back into ``key=value`` form and
    hands it to ``GoosefsConfig::from_properties_str``, so a key the SDK does
    not recognise is silently dropped rather than rejected. This asserts on
    behaviour instead: ``NO_AUTO_PERSIST`` (``-1``) tells the Master not to
    schedule a persist job, and a write under it must still succeed.
    """
    fs = _connect(
        master_addr,
        **{
            "goosefs.user.file.persistence.initial.wait.time": "-1",
            "goosefs.user.local.ufs.client.ignore.block.stream.unknown.status": "true",
            "goosefs.user.file.replication.durable": "1",
            "goosefs.user.file.replication.durable.min": "1",
        },
    )
    path = _scratch_path("no-auto-persist.bin")
    try:
        payload = b"x" * 4096
        fs.write_file(path, payload, write_type=WriteType.AsyncThrough, recursive=True)
        assert fs.read_file(path) == payload
    finally:
        try:
            fs.delete(path, recursive=True)
        except Exception:  # noqa: BLE001 — cleanup only
            pass
        fs.close()
