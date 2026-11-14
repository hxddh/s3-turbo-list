#!/usr/bin/env python3
"""Read an s3tl Parquet dataset (an -o directory) and print a summary.

Usage:
    python examples/read-parquet.py path/to/dataset-dir

Prints row count, column names, dtypes, and the first 20 rows.
"""
import glob
import os
import sys

def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} path/to/dataset-dir", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]

    try:
        import pandas as pd
    except ImportError:
        print("Error: pandas is required.  Install with: pip install pandas pyarrow", file=sys.stderr)
        sys.exit(1)

    try:
        # Only the committed parts: the directory also holds _result.json.
        parts = sorted(glob.glob(os.path.join(path, "part-*.parquet")))
        if not parts:
            print(f"Error: no part-*.parquet files in {path}", file=sys.stderr)
            sys.exit(1)
        df = pd.concat((pd.read_parquet(p) for p in parts), ignore_index=True)
    except Exception as e:
        print(f"Error reading Parquet file: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Dataset: {path} ({len(parts)} part file(s))")
    print(f"Rows:    {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print()
    print("Dtypes:")
    print(df.dtypes.to_string())
    print()
    print(f"First {min(20, len(df))} rows:")
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
