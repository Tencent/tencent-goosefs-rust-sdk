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

"""Binding-level check that CompleteFile stamps Java CRC32C xattr.

Locks the default GooseFS Java checksum: Castagnoli CRC32C
(``java.util.zip.CRC32C`` / ``PureJavaCrc32C``), not IEEE CRC32
(``java.util.zip.CRC32``). The SDK suite in ``tests/complete_file_crc_e2e.rs``
covers persist polling and multi-block FILE writes; this file proves the PyO3
``URIStatus.xattr`` surface and incremental ``FileWriter.write`` match Java.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping

from goosefs import Config, Goosefs, WriterChecksumType, WriteType

# ITU-T V.42 / Castagnoli. Java CRC32C of b"123456789".
_CHECK_VECTOR = b"123456789"
_CHECK_CRC32C = (0xE3069283).to_bytes(4, "big")
# Same bytes under java.util.zip.CRC32 — must not appear as CrcValue.
_CHECK_IEEE_CRC32 = (0xCBF43926).to_bytes(4, "big")
_CRC32C_TYPE = (2).to_bytes(1, "big")  # ChecksumTypeProto.CHECKSUM_CRC32C


def _assert_java_crc32c(xattr: Mapping[str, bytes]) -> None:
    assert xattr.get("CrcType") == _CRC32C_TYPE, xattr
    assert xattr.get("CrcValue") == _CHECK_CRC32C, xattr
    assert xattr.get("CrcValue") != _CHECK_IEEE_CRC32, (
        "CrcValue is IEEE CRC32; Java default is CRC32C Castagnoli"
    )


def test_async_through_stamps_crc32c_xattr(sync_fs: Goosefs) -> None:
    path = f"/sdk-py-crc-{uuid.uuid4().hex[:8]}.bin"
    try:
        n = sync_fs.write_file(path, _CHECK_VECTOR, write_type=WriteType.AsyncThrough)
        assert n == len(_CHECK_VECTOR)
        status = sync_fs.get_status(path)
        assert status.file_id > 0
        assert status.completed
        assert status.mode & 0o777 == 0o644
        _assert_java_crc32c(status.xattr)
        assert sync_fs.read_file(path) == _CHECK_VECTOR
    finally:
        try:
            sync_fs.delete(path)
        except Exception:  # noqa: BLE001
            pass


def test_must_cache_stamps_crc32c_xattr(sync_fs: Goosefs) -> None:
    path = f"/sdk-py-crc-mc-{uuid.uuid4().hex[:8]}.bin"
    try:
        sync_fs.write_file(path, _CHECK_VECTOR, write_type=WriteType.MustCache)
        _assert_java_crc32c(sync_fs.get_status(path).xattr)
    finally:
        try:
            sync_fs.delete(path)
        except Exception:  # noqa: BLE001
            pass


def test_cache_through_stamps_crc32c_xattr(sync_fs: Goosefs) -> None:
    path = f"/sdk-py-crc-ct-{uuid.uuid4().hex[:8]}.bin"
    try:
        sync_fs.write_file(path, _CHECK_VECTOR, write_type=WriteType.CacheThrough)
        _assert_java_crc32c(sync_fs.get_status(path).xattr)
    finally:
        try:
            sync_fs.delete(path)
        except Exception:  # noqa: BLE001
            pass


def test_empty_file_crc32c_is_zero(sync_fs: Goosefs) -> None:
    path = f"/sdk-py-crc-empty-{uuid.uuid4().hex[:8]}.bin"
    try:
        sync_fs.write_file(path, b"", write_type=WriteType.MustCache)
        status = sync_fs.get_status(path)
        assert status.length == 0
        xattr = status.xattr
        assert xattr.get("CrcType") == _CRC32C_TYPE, xattr
        assert xattr.get("CrcValue") == (0).to_bytes(4, "big"), xattr
    finally:
        try:
            sync_fs.delete(path)
        except Exception:  # noqa: BLE001
            pass


def test_incremental_writes_match_java_crc32c(sync_fs: Goosefs) -> None:
    """Two FileWriter.write calls must match Java Checksum.update running CRC32C."""
    path = f"/sdk-py-crc-inc-{uuid.uuid4().hex[:8]}.bin"
    try:
        with sync_fs.create_file(path, write_type=WriteType.MustCache) as w:
            w.write(b"12345")
            w.write(b"6789")
        _assert_java_crc32c(sync_fs.get_status(path).xattr)
        assert sync_fs.read_file(path) == _CHECK_VECTOR
    finally:
        try:
            sync_fs.delete(path)
        except Exception:  # noqa: BLE001
            pass


def test_config_default_checksum_type_is_crc32c() -> None:
    cfg = Config("127.0.0.1:9200")
    assert cfg.writer_checksum_type == WriterChecksumType.Crc32c
    assert cfg.writer_checksum_type.as_str() == "CRC32C"


def test_config_parses_java_checksum_type() -> None:
    crc32 = Config(
        "127.0.0.1:9200",
        properties={"goosefs.user.streaming.writer.checksum.type": "CRC32"},
    )
    assert crc32.writer_checksum_type == WriterChecksumType.Crc32

    null = Config(
        "127.0.0.1:9200",
        properties={"goosefs.user.streaming.writer.checksum.type": "NULL"},
    )
    assert null.writer_checksum_type == WriterChecksumType.Null

    invalid = Config(
        "127.0.0.1:9200",
        properties={"goosefs.user.streaming.writer.checksum.type": "MD5"},
    )
    assert invalid.writer_checksum_type == WriterChecksumType.Crc32c


def _fs_with_checksum(master_addr: str, checksum_type: str) -> Goosefs:
    return Goosefs(
        Config(
            master_addr,
            properties={
                "goosefs.user.file.replication.durable": "1",
                "goosefs.user.file.replication.durable.min": "1",
                "goosefs.user.streaming.writer.checksum.type": checksum_type,
            },
        )
    )


def test_ieee_crc32_via_checksum_type(master_addr: str) -> None:
    fs = _fs_with_checksum(master_addr, "CRC32")
    path = f"/sdk-py-crc-ieee-{uuid.uuid4().hex[:8]}.bin"
    try:
        fs.write_file(path, _CHECK_VECTOR, write_type=WriteType.MustCache)
        xattr = fs.get_status(path).xattr
        assert xattr.get("CrcType") == (1).to_bytes(1, "big"), xattr
        assert xattr.get("CrcValue") == _CHECK_IEEE_CRC32, xattr
        assert xattr.get("CrcValue") != _CHECK_CRC32C
    finally:
        try:
            fs.delete(path)
        except Exception:  # noqa: BLE001
            pass
        fs.close()


def test_null_checksum_type_stamps_zero_xattr(master_addr: str) -> None:
    fs = _fs_with_checksum(master_addr, "NULL")
    path = f"/sdk-py-crc-null-{uuid.uuid4().hex[:8]}.bin"
    try:
        fs.write_file(path, _CHECK_VECTOR, write_type=WriteType.MustCache)
        xattr = fs.get_status(path).xattr
        assert xattr.get("CrcType") == (0).to_bytes(1, "big"), xattr
        assert xattr.get("CrcValue") == (0).to_bytes(4, "big"), xattr
    finally:
        try:
            fs.delete(path)
        except Exception:  # noqa: BLE001
            pass
        fs.close()
