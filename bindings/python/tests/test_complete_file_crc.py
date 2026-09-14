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

"""Binding-level check that CompleteFile stamps inode CRC32C xattr.

The SDK suite in ``tests/complete_file_crc_e2e.rs`` covers persist polling and
multi-block FILE writes. This file only proves the PyO3 ``URIStatus.xattr``
surface carries ``CrcType`` / ``CrcValue`` after ``write_file``.
"""

from __future__ import annotations

import uuid

from goosefs import Goosefs, WriteType

# ITU-T V.42 / Castagnoli check vector. Java PureJavaCrc32C of "123456789".
_CHECK_VECTOR = b"123456789"
_CHECK_CRC32C = (0xE3069283).to_bytes(4, "big")
_CRC32C_TYPE = (2).to_bytes(1, "big")  # ChecksumTypeProto.CHECKSUM_CRC32C


def test_async_through_stamps_crc32c_xattr(sync_fs: Goosefs) -> None:
    path = f"/sdk-py-crc-{uuid.uuid4().hex[:8]}.bin"
    try:
        n = sync_fs.write_file(path, _CHECK_VECTOR, write_type=WriteType.AsyncThrough)
        assert n == len(_CHECK_VECTOR)
        status = sync_fs.get_status(path)
        assert status.file_id > 0
        assert status.completed
        assert status.mode & 0o777 == 0o644
        xattr = status.xattr
        assert xattr.get("CrcType") == _CRC32C_TYPE, xattr
        assert xattr.get("CrcValue") == _CHECK_CRC32C, xattr
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
        xattr = sync_fs.get_status(path).xattr
        assert xattr.get("CrcType") == _CRC32C_TYPE, xattr
        assert xattr.get("CrcValue") == _CHECK_CRC32C, xattr
    finally:
        try:
            sync_fs.delete(path)
        except Exception:  # noqa: BLE001
            pass
