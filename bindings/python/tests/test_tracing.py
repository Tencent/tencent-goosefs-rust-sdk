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

"""Smoke tests for ``goosefs.enable_tracing``.

These tests run **without** a live GooseFS cluster — they only exercise
the parameter validation and idempotency contract documented on the
function. The fact that the subscriber is actually wired into
``tracing-subscriber`` is covered by clippy + cargo build; here we only
care that the Python signature behaves as advertised.
"""

from __future__ import annotations

import goosefs
import pytest


def test_enable_tracing_is_exported() -> None:
    """``enable_tracing`` must be importable as a top-level attribute."""
    assert callable(goosefs.enable_tracing)
    assert "enable_tracing" in goosefs.__all__


def test_enable_tracing_default_args_succeed() -> None:
    """First call with default args should succeed (or be a no-op if a
    previous test in this session already installed a subscriber).

    We don't assert installation order — pytest may have run another
    test that already called it. Both outcomes are valid because
    ``enable_tracing`` is idempotent.
    """
    # Either the first install (returns None) or a subsequent silent
    # no-op (also returns None) — both must not raise.
    assert goosefs.enable_tracing() is None


def test_enable_tracing_is_idempotent() -> None:
    """Calling it twice in a row must not raise."""
    goosefs.enable_tracing(level="info")
    # Second call: silent no-op.
    goosefs.enable_tracing(level="debug")


@pytest.mark.parametrize("level", ["TRACE", "Debug", "info", "WARN", "error"])
def test_enable_tracing_accepts_case_insensitive_levels(level: str) -> None:
    """All five severity names should be accepted regardless of case."""
    goosefs.enable_tracing(level=level)


def test_enable_tracing_rejects_unknown_level() -> None:
    """Bad ``level`` must raise ``ValueError`` (PyValueError on the
    Rust side)."""
    with pytest.raises(ValueError, match="level must be one of"):
        goosefs.enable_tracing(level="verbose")


def test_enable_tracing_rejects_unknown_target() -> None:
    """Anything other than ``stderr`` must raise — even the reserved
    names that we explicitly call out as future extensions."""
    with pytest.raises(ValueError, match="reserved for a future release"):
        goosefs.enable_tracing(target="logging")
    with pytest.raises(ValueError, match="reserved for a future release"):
        goosefs.enable_tracing(target="stdout")
    with pytest.raises(ValueError, match="target must be"):
        goosefs.enable_tracing(target="syslog")


def test_enable_tracing_target_is_keyword_only() -> None:
    """``target`` is keyword-only; passing it positionally should fail."""
    with pytest.raises(TypeError):
        goosefs.enable_tracing("info", "stderr")  # type: ignore[misc]
