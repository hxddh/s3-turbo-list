#!/usr/bin/env python3
"""Print a compact summary from a s3-turbo-list run manifest."""

import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {Path(sys.argv[0]).name} RUN_MANIFEST.json", file=sys.stderr)
        return 2

    path = Path(sys.argv[1])
    with path.open("r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    print(f"status: {manifest.get('status')}")
    print(f"exit_code: {manifest.get('exit_code')}")
    print(f"elapsed_secs: {manifest.get('elapsed_secs')}")

    metrics = manifest.get("metrics", {})
    print(f"parquet_rows: {metrics.get('parquet_rows')}")
    print(f"ks_entries: {metrics.get('ks_entries')}")

    print("artifacts:")
    for artifact in manifest.get("artifacts", []):
        path = artifact.get("path")
        kind = artifact.get("kind")
        exists = artifact.get("exists")
        size = artifact.get("size_bytes")
        sha = artifact.get("sha256")
        print(f"  - {kind}: exists={exists} size={size} sha256={sha}")
        parquet = artifact.get("parquet")
        if parquet:
            print(
                "    "
                f"rows={parquet.get('row_count')} "
                f"row_groups={parquet.get('row_group_count')} "
                f"schema={','.join(parquet.get('schema_fields', []))}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
