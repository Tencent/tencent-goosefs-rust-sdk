"""Pandas DataFrame ↔ CSV on GooseFS.

Demonstrates the cleanest way to mix pandas with GooseFS today: encode
to text/binary in pandas, ship the bytes via ``write_file``, fetch with
``read_file``, decode in pandas. This matches the pattern most data-eng
notebooks use and avoids any custom fsspec adapter.

Prerequisites
-------------

::

    export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
    pip install 'goosefs[pandas]'   # pulls pandas + pyarrow

Run::

    python examples/05_pandas_csv.py
"""

from __future__ import annotations

import io
import os
import sys

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover — example-time guidance only
    print(
        "pandas is required for this example. Install with: pip install 'goosefs[pandas]'",
        file=sys.stderr,
    )
    raise SystemExit(2) from exc

from goosefs import Config, Goosefs, WriteType


def _build_dataframe() -> pd.DataFrame:
    """A toy DataFrame: city populations across a few decades."""
    return pd.DataFrame(
        {
            "city": ["Beijing", "Shanghai", "Shenzhen", "Guangzhou"],
            "year": [2020, 2020, 2020, 2020],
            "population_millions": [21.5, 24.9, 17.6, 15.3],
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
    df = _build_dataframe()
    print(f"[pandas] built DataFrame with shape={df.shape}")

    with Goosefs(cfg) as fs:
        path_dir = "/pandas_demo"
        path_file = f"{path_dir}/cities.csv"

        fs.mkdir(path_dir, recursive=True)
        if fs.exists(path_file):
            fs.delete(path_file)

        # ─── Encode → bytes → GooseFS ─────────────────────────────────────
        # ``DataFrame.to_csv`` writes to any file-like object. We use
        # StringIO + ``encode("utf-8")`` because GooseFS speaks bytes only;
        # passing a ``str`` directly to ``write_file`` would (correctly)
        # raise TypeError.
        sio = io.StringIO()
        df.to_csv(sio, index=False)
        encoded = sio.getvalue().encode("utf-8")
        print(f"[encode] CSV bytes = {len(encoded)}")

        n = fs.write_file(path_file, encoded, write_type=WriteType.CacheThrough)
        print(f"[write]  {path_file}  ({n} bytes)")

        # ─── Round-trip back into pandas ──────────────────────────────────
        raw = fs.read_file(path_file)
        decoded = pd.read_csv(io.BytesIO(raw))
        print(f"[read]   decoded DataFrame shape={decoded.shape}")
        print(decoded.to_string(index=False))

        # Equality check (CSV round-trip preserves dtypes for our toy data;
        # use ``check_exact=True`` to be strict about float comparisons).
        pd.testing.assert_frame_equal(decoded, df, check_exact=True)
        print("[verify] pandas DataFrame equality OK")

        # Cleanup
        fs.delete(path_file)
        fs.delete(path_dir)
        print(f"[delete] {path_dir}")


if __name__ == "__main__":
    main()
