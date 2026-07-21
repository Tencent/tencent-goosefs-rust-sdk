#!/usr/bin/env python3
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

"""Check that source files carry an Apache-2.0 license header.

Mirrors OpenDAL's hawkeye license check, but validates the Tencent
Apache-2.0 header used in this repository (see ``license-header.txt``).
Third-party files that retain an ASF header are also accepted.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

EXTS = {".rs", ".py", ".sh", ".proto", ".toml", ".yml", ".yaml"}
SKIP_DIRS = {
    ".git",
    "target",
    ".venv",
    "venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "node_modules",
    "dist",
    "build",
}
SKIP_FILES = {
    "Cargo.lock",
    "uv.lock",
    "LICENSE",
    "license-header.txt",
}
# Copied from OpenDAL; keep ASF header as-is.
ALLOW_ASF = {
    "fixtures/goosefs/bin/start-default.sh",
}

HEAD_BYTES = 2048

REQUIRED_TENCENT = (
    "Copyright (C) 2026 Tencent. All rights reserved.",
    'Licensed under the Apache License, Version 2.0 (the "License");',
    "http://www.apache.org/licenses/LICENSE-2.0",
)
REQUIRED_ASF = (
    "Licensed to the Apache Software Foundation (ASF)",
    "http://www.apache.org/licenses/LICENSE-2.0",
)


def iter_files() -> list[Path]:
    out: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name in SKIP_FILES:
                continue
            path = Path(dirpath) / name
            if path.suffix.lower() not in EXTS:
                continue
            out.append(path)
    return sorted(out)


def ok_header(rel: str, text: str) -> bool:
    head = text[:HEAD_BYTES]
    if rel in ALLOW_ASF:
        return all(m in head for m in REQUIRED_ASF)
    if all(m in head for m in REQUIRED_ASF):
        # Extra ASF-marked third-party files, if any.
        return True
    return all(m in head for m in REQUIRED_TENCENT)


def main() -> int:
    missing: list[str] = []
    for path in iter_files():
        rel = path.relative_to(ROOT).as_posix()
        try:
            raw = path.read_bytes()
        except OSError as exc:
            print(f"ERROR reading {rel}: {exc}", file=sys.stderr)
            return 2
        if b"\0" in raw[:1024]:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if not ok_header(rel, text):
            missing.append(rel)

    if missing:
        print(
            f"Missing Apache-2.0 license header in {len(missing)} file(s):",
            file=sys.stderr,
        )
        for rel in missing:
            print(f"  {rel}", file=sys.stderr)
        print(
            "\nExpected Tencent header (see license-header.txt), "
            "or ASF header for allowlisted third-party files.",
            file=sys.stderr,
        )
        return 1

    print(f"OK: Apache-2.0 license headers present ({len(list(iter_files()))} files checked).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
