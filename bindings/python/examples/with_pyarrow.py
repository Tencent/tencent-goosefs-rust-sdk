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

"""Pyarrow Parquet round-trip on top of GooseFS.

Builds a small Arrow Table, writes it to a Parquet file living on
GooseFS, then reads it back and verifies content equality. The Parquet
codec runs in pure Python land — GooseFS only sees opaque bytes — so
there's no special integration to set up.

Prerequisites
-------------

::

    export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
    pip install 'goosefs[arrow]'   # pulls pyarrow

Run::

    python examples/with_pyarrow.py
"""

from __future__ import annotations

import io
import os
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover — example-time guidance only
    print(
        "pyarrow is required for this example. Install with: pip install 'goosefs[arrow]'",
        file=sys.stderr,
    )
    raise SystemExit(2) from exc

from goosefs import Config, Goosefs, WriteType


def _build_table() -> pa.Table:
    """A toy table with three columns and 5 rows."""
    return pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(
                ["alpha", "bravo", "charlie", "delta", "echo"],
                type=pa.string(),
            ),
            "score": pa.array([0.5, 1.5, 2.5, 3.5, 4.5], type=pa.float64()),
        }
    )


def main() -> None:
    master = os.environ.get("GOOSEFS_MASTER_ADDR")
    if not master:
        print(
            "GOOSEFS_MASTER_ADDR is not set. Example: export GOOSEFS_MASTER_ADDR=127.0.0.1:9200",
            file=sys.stderr,
        )
        raise SystemExit(2)

    cfg = Config(master)
    table = _build_table()
    print(f"[arrow]  built table with {table.num_rows} rows, columns={table.column_names}")

    with Goosefs(cfg) as fs:
        path_dir = "/pyarrow_demo"
        path_file = f"{path_dir}/sample.parquet"

        fs.mkdir(path_dir, recursive=True)
        if fs.exists(path_file):
            fs.delete(path_file)

        # ─── Encode locally, ship to GooseFS as opaque bytes ──────────────
        # ``pyarrow.parquet.write_table`` writes to a ``pyarrow.NativeFile``
        # or any Python file-like object. We use an ``io.BytesIO`` so we
        # can hand the resulting buffer to ``write_file`` in one shot.
        # For very large tables you'd want to stream chunks via
        # ``Goosefs.create_file`` instead — see ``streaming.py``.
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        encoded = buf.getvalue()
        print(f"[encode] parquet bytes = {len(encoded)}")

        n = fs.write_file(path_file, encoded, write_type=WriteType.CacheThrough)
        print(f"[write]  {path_file}  ({n} bytes)")

        # ─── Read back and decode ─────────────────────────────────────────
        roundtrip_bytes = fs.read_file(path_file)
        decoded = pq.read_table(io.BytesIO(roundtrip_bytes))
        print(f"[read]   decoded {decoded.num_rows} rows")

        assert decoded.equals(table), "round-trip table mismatch"
        print("[verify] arrow Table equality OK")

        # Cleanup
        fs.delete(path_file)
        fs.delete(path_dir)
        print(f"[delete] {path_dir}")


if __name__ == "__main__":
    main()
