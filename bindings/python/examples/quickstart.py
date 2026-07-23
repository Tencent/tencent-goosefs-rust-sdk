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

"""Quickstart — synchronous Goosefs client.

This is the shortest end-to-end example: connect to a master, create a
directory, write a small file, read it back, and clean up. It uses the
blocking ``Goosefs`` API and is the right starting point for scripts,
data-engineering jobs, and the REPL.

Prerequisites
-------------

1. A reachable GooseFS cluster. Set the master address via env var::

       export GOOSEFS_MASTER_ADDR=127.0.0.1:9200

2. Install the binding (PyPI release will be ``goosefs``)::

       pip install goosefs

Run
---

    python examples/quickstart.py
"""

from __future__ import annotations

import os
import sys

from goosefs import Config, Goosefs


def main() -> None:
    master = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master:
        print(
            "GOOSEFS_MASTER_ADDR is not set. Example: export GOOSEFS_MASTER_ADDR=127.0.0.1:9200",
            file=sys.stderr,
        )
        raise SystemExit(2)

    cfg = Config(master)

    # Use Goosefs as a context manager so the connection is always closed
    # cleanly, even if the body raises. The atexit safety-net will catch
    # forgotten handles too — see Review  — but explicit ``with`` is
    # always preferable.
    with Goosefs(cfg) as fs:
        path_dir = "/quickstart"
        path_file = f"{path_dir}/hello.txt"

        # ``recursive=True`` is idempotent: re-running the script does not
        # raise even though the directory already exists.
        fs.mkdir(path_dir, recursive=True)
        print(f"[mkdir]  {path_dir}")

        payload = b"Hello, GooseFS!\n"
        n = fs.write_file(path_file, payload)
        print(f"[write]  {path_file}  ({n} bytes)")

        roundtrip = fs.read_file(path_file)
        print(f"[read]   {path_file}  -> {roundtrip!r}")
        assert roundtrip == payload, "round-trip mismatch"

        status = fs.get_status(path_file)
        print(
            f"[stat]   length={status.length}  "
            f"completed={status.is_completed()}  "
            f"persisted={status.is_persisted()}"
        )

        # Cleanup so the example is re-runnable.
        fs.delete(path_file)
        print(f"[delete] {path_file}")


if __name__ == "__main__":
    main()
