# Trace Reference

Trace output (`--trace-compat trace.jsonl` or `--debug-s3`) records every S3
API call as one JSON object per line (JSONL). Each line is one
`S3CompatEvent`.

## Inspecting traces

```bash
# Human-readable
cat trace.jsonl | jq '.'

# Count by operation
cat trace.jsonl | jq -r .operation | sort | uniq -c

# Find errors
cat trace.jsonl | jq 'select(.s3_error_code != null)'

# Local summary tooling (no S3 access)
s3-turbo-list trace-summary trace.jsonl --output-format json
```

## Event fields

| Field | Type | Description |
|---|---|---|
| `timestamp` | string | ISO 8601 wall-clock time of the call. |
| `operation` | string | S3 operation name (e.g. `"ListObjectsV2"`, `"HeadBucket"`). |
| `profile` | string? | Vendor/profile name (e.g. `"bos"`, `"minio"`, `null`). |
| `endpoint_url` | string | Full endpoint URL used for the request. |
| `region` | string? | AWS region or vendor region. |
| `addressing_style` | string | `"path"`, `"virtual"`, or `"auto"`. |
| `bucket` | string | Target bucket name. |
| `prefix` | string | Listing prefix. |
| `delimiter` | string? | S3 delimiter (default `"/"`). |
| `start_after` | string? | `start-after` parameter if set. |
| `max_keys` | int? | `max-keys` parameter if set. |
| `continuation_token` | string? | Continuation token sent in the request. |
| `http_status` | uint16 | HTTP response status code. |
| `s3_error_code` | string? | S3 error code (e.g. `"NoSuchBucket"`). |
| `s3_error_message` | string? | Error message body. |
| `request_id` | string? | `x-amz-request-id` or equivalent header. |
| `request_id_2` | string? | `x-amz-id-2` (AWS extended request ID). |
| `retry_attempt` | uint32 | Zero-indexed retry count. |
| `latency_ms` | uint64 | Round-trip latency in milliseconds. |
| `retryable` | bool | Whether the error is classified as retryable. |
| `fatal` | bool | Whether the error is classified as fatal. |
| `is_truncated` | bool | Whether the ListObjectsV2 response was truncated. |
| `next_continuation_token` | string? | Continuation token for the next page. |
| `key_count` | int? | `KeyCount` from the ListObjectsV2 response. |
| `contents_count` | int? | Number of `Contents` entries in the response. |
| `common_prefixes_count` | int? | Number of `CommonPrefixes` entries. |
| `next_continuation_token_present` | bool? | Whether the response explicitly included a next continuation token. |
| `segment_index` | uint? | Segment index for `ListObjectsV2SegmentSummary` events. |
| `end_before` | string? | Upper segment boundary, when present. |
| `segment_pages` | uint32? | Pages read by a completed segment summary. |
| `segment_objects` | uint? | Objects emitted by a completed segment summary. |
| `segment_common_prefixes` | uint? | CommonPrefixes seen by a completed segment summary. |
| `ended_by` | string? | Segment completion reason, such as `"pagination"` or `"boundary"`. |
| `truncated_raw_body` | string? | First 512 bytes of the error response body. |
