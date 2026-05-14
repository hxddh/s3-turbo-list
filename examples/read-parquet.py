#!/usr/bin/env python3
"""Read an s3-turbo-list Parquet file and print a summary.

Usage:
    python examples/read-parquet.py path/to/output.parquet

Prints row count, column names, dtypes, and the first 20 rows.
"""
import sys

def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} path/to/output.parquet", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]

    try:
        import pandas as pd
    except ImportError:
        print("Error: pandas is required.  Install with: pip install pandas pyarrow", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_parquet(path)
    except FileNotFoundError:
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading Parquet file: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"File:    {path}")
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
