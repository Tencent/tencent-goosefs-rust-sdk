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

"""Hermetic guards for Java-client-parity Python surface.

These tests never talk to a cluster. They only pin the constructors and
methods added for ``feat/java-client-parity``.
"""

from __future__ import annotations

from goosefs import AsyncGoosefs, CreateFileOptions, Goosefs, OpenFileOptions, ReadType


def test_create_file_options_replication_max() -> None:
    opts = CreateFileOptions(replication_max=3)
    assert opts.replication_max == 3
    assert CreateFileOptions().replication_max is None


def test_open_file_options_default_is_cache() -> None:
    assert OpenFileOptions().read_type == ReadType.Cache
    assert OpenFileOptions(read_type=ReadType.NoCache).read_type == ReadType.NoCache


def test_filesystem_exposes_persist_and_set_attribute() -> None:
    for cls in (Goosefs, AsyncGoosefs):
        assert callable(cls.persist)
        assert callable(cls.set_attribute)
        assert callable(cls.rename)
