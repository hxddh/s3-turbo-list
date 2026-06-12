# Compat Probe Report

`compat-probe` validates an S3-compatible endpoint with a small sequence of
real S3 API calls.  It is intended for endpoint diagnostics before full-scale
listing.  Do not run it against real cloud endpoints unless that endpoint test
is intentional.

## Command Shape

```bash
s3-turbo-list compat-probe \
  --endpoint https://example.invalid \
  --region us-east-1 \
  --bucket my-bucket \
  --addressing-style path \
  --output compat-probe.json
```

`compat-probe` reads the endpoint from the global `--endpoint-url` flag
(or its subcommand-local `--endpoint` override).
Template placeholders such as `<account-id>` are rejected locally with provider
setup exit code `3` before network setup.  Literal but unreachable endpoints are
left to the probe so transport failures remain part of the diagnostic report.

## Top-Level Fields

| Field | Type | Meaning |
|---|---|---|
| `endpoint_url` | string | Endpoint passed via `--endpoint`. |
| `region` | string | Region passed via `--region`. |
| `bucket` | string | Bucket passed via `--bucket`. |
| `addressing_style` | string | `path`, `virtual`, or `auto`. |
| `tests` | array | Per-operation probe results. |
| `overall_status` | string | `compatible`, `partial`, or `incompatible`. |

`overall_status` is derived from the test statuses:

| Value | Meaning |
|---|---|
| `compatible` | No test reported `error`. |
| `partial` | Some tests reported `error`, while at least one did not. |
| `incompatible` | Every test reported `error`. |

## Test Fields

| Field | Type | Stability | Meaning |
|---|---|---|---|
| `test` | string | stable | Human-readable probe test name. |
| `status` | string | stable | `ok`, `error`, or `skipped`. |
| `latency_ms` | integer | stable | Wall-clock latency for the probe step. |
| `http_status` | integer, optional | stable | HTTP status when the SDK exposes one. |
| `s3_error_code` | string, optional | stable | Modeled S3 error code, such as `AccessDenied` or `NotImplemented`. |
| `error_kind` | string, optional | stable | SDK failure category. |
| `diagnostic_code` | string, optional | stable | s3-turbo-list diagnostic category for the failure. |
| `recommendation` | string, optional | stable intent | Human-readable next step associated with `diagnostic_code`. |
| `error_message` | string, optional | unstable text | Debug/fallback error detail for humans. |
| `request_id` | string, optional | stable | `x-amz-request-id` or equivalent. |
| `request_id_2` | string, optional | stable | Extended S3 request ID, usually `x-amz-id-2`. |
| `is_truncated` | boolean, optional | stable | Pagination result flag. |
| `key_count` | integer, optional | stable | Key count reported or observed during pagination tests. |
| `contents_count` | integer, optional | stable | Number of object entries observed during pagination tests. |
| `next_continuation_token_present` | boolean, optional | stable | Whether a continuation token was present. |

Minimal report shape:

```json
{
  "endpoint_url": "https://example.invalid",
  "region": "us-east-1",
  "bucket": "my-bucket",
  "addressing_style": "path",
  "tests": [
    {
      "test": "HeadBucket",
      "status": "ok",
      "latency_ms": 42,
      "http_status": 200,
      "request_id": "REQ123",
      "request_id_2": "EXTENDED123"
    },
    {
      "test": "ListObjectsV2 basic",
      "status": "error",
      "latency_ms": 35,
      "http_status": 501,
      "s3_error_code": "NotImplemented",
      "error_kind": "service",
      "diagnostic_code": "operation_not_supported",
      "recommendation": "Endpoint does not implement this S3 operation or option; inspect which probe test failed before full listing",
      "error_message": "service-specific debug text",
      "request_id": "REQ456",
      "request_id_2": "EXTENDED456"
    }
  ],
  "overall_status": "partial"
}
```

`error_kind` values are SDK-level categories:

| Value | Meaning |
|---|---|
| `service` | The endpoint returned a modeled service error response. |
| `response` | The endpoint responded, but the SDK could not parse it as expected. |
| `dispatch` | Transport failed before an HTTP response was available. |
| `timeout` | The SDK timed out. |
| `construction` | Request construction or local SDK setup failed. |
| `pagination` | The endpoint returned inconsistent pagination metadata. |
| `unknown` | Future SDK error variant not classified by this release. |

`diagnostic_code` values are s3-turbo-list categories intended for automation
and operator triage:

| Value | Typical cause |
|---|---|
| `signature_mismatch` | Credentials, region, endpoint, clock skew, or addressing style do not match the provider's signing expectations. |
| `access_denied` | Credentials are invalid or lack bucket/list permissions. |
| `bucket_not_found` | Bucket name, account/project scope, region, or addressing style is wrong. |
| `region_or_endpoint_mismatch` | Provider redirected the request or rejected the signing region. |
| `operation_not_supported` | Endpoint does not support the S3 operation or option used by that probe step. |
| `redirect` | Endpoint responded with an HTTP redirect. |
| `bad_request` | Endpoint rejected the request shape before a more specific S3 code was available. |
| `not_found` | Endpoint returned HTTP 404 without a modeled S3 error code. |
| `server_error` | Endpoint returned HTTP 5xx. |
| `timeout` | SDK timeout before a complete response. |
| `transport_failure` | DNS, TCP, TLS, proxy, firewall, or other transport failure before HTTP metadata was available. |
| `invalid_response` | Endpoint responded with data the S3 SDK could not parse as the expected S3 response. |
| `request_construction` | Local SDK request construction failed before dispatch. |
| `pagination_token_missing` | Endpoint reported truncation without a continuation token. |
| `unknown_error` | No stable category matched. |

## Stability Guidance

- Existing report fields are intended to remain backward compatible through the
  `0.2.x` series.
- New diagnostic fields may be added as optional fields.
- Missing optional fields mean the SDK or endpoint did not provide that
  metadata; missing is not the same as an empty string.
- Automation should prefer `status`, `http_status`, `s3_error_code`,
  `error_kind`, and `diagnostic_code` over parsing `error_message`.
- `error_message` is a fallback debug string and is not stable for scripts.
