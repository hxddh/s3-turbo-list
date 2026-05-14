#!/usr/bin/env python3
"""Summarize an s3-turbo-list trace JSONL file.

Usage:
    python examples/inspect-trace.py path/to/trace.jsonl

Prints operation counts, HTTP status distribution, S3 error codes,
addressing style counts, start_after / continuation_token statistics,
max latency, and sample events with non-empty pagination parameters.
"""
import json
import sys
from collections import Counter
from typing import Any


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} path/to/trace.jsonl", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]

    try:
        with open(path) as fh:
            lines = fh.readlines()
    except FileNotFoundError:
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    events: list[dict[str, Any]] = []
    malformed: list[tuple[int, str]] = []

    for i, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            events.append(json.loads(stripped))
        except json.JSONDecodeError:
            malformed.append((i, stripped[:120]))
            if len(malformed) <= 5:
                print(f"Warning: malformed JSON at line {i}: {stripped[:80]}", file=sys.stderr)

    if not events:
        print("No valid trace events found.", file=sys.stderr)
        sys.exit(1)

    # ── Counters ───────────────────────────────────────────
    op_counts: Counter[str] = Counter()
    http_counts: Counter[int] = Counter()
    s3_error_counts: Counter[str] = Counter()
    style_counts: Counter[str] = Counter()
    with_start_after = 0
    with_continuation_token = 0
    with_next_continuation_token = 0
    max_latency = 0
    sample_pagination: list[dict[str, Any]] = []

    for ev in events:
        op_counts[ev.get("operation", "?")] += 1
        http_counts[ev.get("http_status", 0)] += 1
        if ec := ev.get("s3_error_code"):
            s3_error_counts[ec] += 1
        style_counts[ev.get("addressing_style", "?")] += 1
        if ev.get("start_after"):
            with_start_after += 1
        if ev.get("continuation_token"):
            with_continuation_token += 1
        if ev.get("next_continuation_token"):
            with_next_continuation_token += 1
        lat = ev.get("latency_ms", 0)
        if lat > max_latency:
            max_latency = lat
        # Collect sample events with interesting pagination params.
        if (ev.get("start_after") or ev.get("continuation_token") or ev.get("next_continuation_token")) and len(sample_pagination) < 5:
            sample_pagination.append(ev)

    # ── Report ─────────────────────────────────────────────
    print(f"File:      {path}")
    print(f"Events:    {len(events)}")
    if malformed:
        print(f"Malformed: {len(malformed)} (first 5 reported on stderr)")
    print()

    print("Operation counts:")
    for op, cnt in op_counts.most_common():
        print(f"  {op:30s} {cnt}")
    print()

    print("HTTP status counts:")
    for code, cnt in sorted(http_counts.items()):
        print(f"  {code:>3d}  {cnt}")
    print()

    if s3_error_counts:
        print("S3 error code counts:")
        for code, cnt in s3_error_counts.most_common():
            print(f"  {code:30s} {cnt}")
        print()

    print("Addressing style counts:")
    for style, cnt in style_counts.most_common():
        print(f"  {style:10s} {cnt}")
    print()

    print("Pagination parameters:")
    print(f"  Events with start_after:           {with_start_after}")
    print(f"  Events with continuation_token:    {with_continuation_token}")
    print(f"  Events with next_continuation_token: {with_next_continuation_token}")
    print()

    print(f"Max latency_ms: {max_latency}")
    print()

    if sample_pagination:
        print(f"Sample events with pagination params ({len(sample_pagination)} shown):")
        for ev in sample_pagination:
            fields = {
                k: ev.get(k)
                for k in (
                    "operation",
                    "start_after",
                    "continuation_token",
                    "next_continuation_token",
                    "is_truncated",
                    "key_count",
                    "contents_count",
                    "latency_ms",
                )
                if ev.get(k) is not None
            }
            print(f"  {json.dumps(fields)}")
    else:
        print("No events with start_after, continuation_token, or next_continuation_token.")


if __name__ == "__main__":
    main()
