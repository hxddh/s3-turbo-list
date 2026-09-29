# Providers, Config File, and compat-probe

s3-turbo-list works against any S3-compatible endpoint.  This page covers the
built-in provider presets, the TOML config file, and `compat-probe`, the
command that checks an endpoint before a full run.

## Provider presets

`--provider` (config key `s3.provider`) selects an optional preset for a
known S3-compatible service.  A preset is local metadata plus conservative
defaults for the endpoint and addressing style: it never touches
credentials, probes nothing, changes no output schema, and adds no
provider-specific pagination behavior.  Credentials always come from the
AWS SDK chain (`AWS_PROFILE`, environment variables, instance roles).
`--provider` replaces the pre-0.37 `--profile`, which still works as a
hidden alias.

`s3-turbo-list guide <provider>` prints a quickstart and the preset's facts
locally (no S3 access).

| Provider | Service | Endpoint | Addressing | Default region | Project status |
|---|---|---|---|---|---|
| `aws` | AWS S3 | SDK-derived from the region | virtual | — | validated baseline |
| `minio` | MinIO | deployment-specific: pass `--endpoint-url` | path | — | validated |
| `bos` | Baidu BOS S3-compatible API | `https://s3.{region}.bcebos.com` | virtual | `bj` | validated |
| `r2` | Cloudflare R2 | account-specific: pass `--endpoint-url` | path | pass `--region auto` | documented preset |
| `b2` | Backblaze B2 S3-compatible API | `https://s3.{region}.backblazeb2.com` | path | — | documented preset |
| `oss` | Alibaba Cloud OSS S3-compatible API | `https://{region}.aliyuncs.com` | virtual | — | documented preset |

Presets with a `{region}` endpoint derive the endpoint from `--region`, so
everyday commands need no `--endpoint-url`.  When `--region` is omitted,
`bos` uses its default region `bj` (endpoint and signing) rather than the
ambient `AWS_REGION`; `oss` and `b2` have no default region and need
`--region` (or an explicit endpoint).  R2 signs for region `auto`: pass
`--region auto`.

```bash
s3-turbo-list --provider oss list --bucket my-bucket --region oss-cn-beijing --output-dir out
s3-turbo-list --provider bos list --bucket my-bos-bucket --region gz --output-dir out
s3-turbo-list --provider b2 list --bucket my-b2-bucket --region us-west-004 --output-dir out

# Deployment- or account-specific endpoints stay explicit:
s3-turbo-list --provider minio --endpoint-url http://localhost:9000 \
  list --bucket my-bucket --region us-east-1 --output-dir out
s3-turbo-list --provider r2 --endpoint-url https://<account-id>.r2.cloudflarestorage.com \
  list --bucket my-bucket --region auto --output-dir out
```

The provider and endpoint options are global, so they may also follow the
command name (`s3-turbo-list list --provider oss …`).

`validated` means the project has run its endpoint validation flow for that
provider.  `documented preset` means the preset encodes known provider
defaults without a claim of full validation: treat the OSS, R2, and B2
presets as starting points until `compat-probe` and a representative listing
run pass in your environment.  BOS is fully ListObjectsV2-compatible and is
treated like any other endpoint (hinted multi-segment listing, startup
discovery, and runtime splitting all apply).

### Precedence

A preset only fills values nobody chose.  An explicit `--endpoint-url` /
`s3.endpoint_url` wins over the preset's endpoint, and an explicit
`--addressing-style` / `s3.addressing_style` — including `auto` — wins over
the preset's addressing style.

A preset with a deployment- or account-specific endpoint (`minio`, `r2`)
and no endpoint URL configured, or an endpoint still holding a template
placeholder such as `<account-id>` or `<region>`, is a provider setup error:
a real run, `doctor`, and a dry run (plan `status: blocked`) all exit `3`
before any request.  A region-derived preset without a default region
(`oss`, `b2`) and neither `--region` nor an endpoint is a `doctor` warning
(doctor takes no region) and a run or dry-run error, exit `3`.

For `diff` with a region-templated preset and no explicit endpoint, each side
uses the preset endpoint for its own region (`--region` and
`--target-region`); an explicit endpoint applies to both sides.

## Config file

A config file is only needed for settings the command-line options do not
cover, or to pin a custom endpoint for repeated runs.  It is read from
`--config <path>`, else `./s3-turbo-list.toml`, else `~/.s3-turbo-list.toml`.
Command-line options win over the file.

Minimal config for a custom endpoint:

```toml
[s3]
endpoint_url = "https://s3.example.internal:9000"
provider = "minio"
addressing_style = "path"
```

```bash
s3-turbo-list doctor --config s3-turbo-list.toml
s3-turbo-list list --config s3-turbo-list.toml --bucket my-bucket --region us-east-1 --dry-run
```

Valid keys, by section (defaults in [`tuning.md`](tuning.md#core-defaults)):

| Section | Keys |
|---|---|
| `[s3]` | `max_attempts`, `initial_backoff_secs`, `connect_timeout_secs`, `operation_timeout_secs`, `endpoint_url`, `addressing_style` (`path`, `virtual`, `auto`), `provider` |
| `[runtime]` | `worker_threads`, `max_concurrency` |
| `[output]` | `row_group_size`, `compression`, `compression_level` |
| `[channel]` | `capacity` |

Keys are checked: an unknown section or key — a typo, or a pre-0.37 per-run
key such as `s3.start_after`, `s3.debug_s3`, `s3.trace_compat`,
`output.parquet_file`, `output.ks_file`, or `output.log_file` — fails with
exit code `2` and names the expected keys.  The deprecated `s3.profile` and
`s3.force_path_style` keys are still read for one release; use `provider`
and `addressing_style = "path"`.  An explicit `--config` path that does not
exist is also exit `2`.

## compat-probe

`compat-probe` checks an S3-compatible endpoint with a few real S3 requests
before a full-scale listing.  It contacts the endpoint, so run it only
against endpoints you intend to test.

```bash
s3-turbo-list compat-probe --endpoint-url https://s3.example.internal:9000 \
  --addressing-style path --bucket my-bucket --output compat-probe.json
s3-turbo-list --provider bos compat-probe --bucket my-bos-bucket --region gz
```

The probe resolves the endpoint and addressing style the way a listing run
does — global `--endpoint-url` / `--addressing-style`, then the config file,
then the `--provider` preset — so it exercises what the run would use.  It
needs an endpoint from one of those (for AWS, pass the regional endpoint,
e.g. `https://s3.us-east-1.amazonaws.com`); without one it exits `3` before
any request.  `--region` is optional: it defaults to the preset's region,
else the SDK's.  `-p/--prefix` probes under a prefix.  The pre-0.37
subcommand-local `--endpoint` still works as a hidden alias.

The probe sends `HeadBucket`, `ListObjectsV2 (max-keys=1)`,
`ListObjectsV2 with delimiter`, and a `ListObjectsV2 pagination check`.
`--trace-compat <file>` writes the requests' trace as JSONL (`-` for
stderr); without it, the trace goes to stderr.  `--dry-run` prints the plan
without contacting the endpoint.  Template placeholders such as
`<account-id>` are rejected locally with exit `3`; a literal but unreachable
endpoint is left to the probe, so transport failures are part of the report.

### Report

| Field | Type | Meaning |
|---|---|---|
| `endpoint_url` | string | Endpoint probed. |
| `region` | string | Region the probe signed for. |
| `bucket` | string | Bucket probed. |
| `addressing_style` | string | `path`, `virtual`, or `auto`. |
| `tests` | array | Per-operation results. |
| `overall_status` | string | `compatible`, `partial`, or `incompatible`. |

| `overall_status` | Meaning | Exit code |
|---|---|---|
| `compatible` | No test reported `error`.  `skipped` tests were not exercised: a bucket with fewer than three objects skips the pagination check, so `compatible` on a near-empty bucket says nothing about continuation-token behavior. | `0` |
| `partial` | Some tests reported `error`, at least one did not.  `tests[]` names the failing operations. | `0` |
| `incompatible` | Every test reported `error`. | `3` when every failure is a setup error (`AccessDenied`, `NoSuchBucket`, bad signature, redirect, or a codeless 401/403/404 such as `HeadBucket`'s), otherwise `4` |

The report is written (stdout or `--output`) before a non-zero exit.

Per-test fields:

| Field | Type | Stability | Meaning |
|---|---|---|---|
| `test` | string | stable | Probe test name. |
| `status` | string | stable | `ok`, `error`, or `skipped`. |
| `latency_ms` | integer | stable | Wall-clock latency of the step. |
| `http_status` | integer, optional | stable | HTTP status when the SDK exposes one. |
| `s3_error_code` | string, optional | stable | Modeled S3 error code, e.g. `AccessDenied`, `NotImplemented`. |
| `error_kind` | string, optional | stable | SDK failure category (below). |
| `diagnostic_code` | string, optional | stable | s3-turbo-list diagnostic category (below). |
| `recommendation` | string, optional | stable intent | Human-readable next step for `diagnostic_code`. |
| `error_message` | string, optional | unstable text | Debug/fallback detail for humans. |
| `request_id` | string, optional | stable | `x-amz-request-id` or equivalent. |
| `request_id_2` | string, optional | stable | Extended request ID, usually `x-amz-id-2`. |
| `is_truncated` | boolean, optional | stable | Pagination result flag. |
| `key_count` | integer, optional | stable | Key count reported or observed during pagination tests. |
| `contents_count` | integer, optional | stable | Object entries observed during pagination tests. |
| `next_continuation_token_present` | boolean, optional | stable | Whether a continuation token was present. |

Minimal report:

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
      "test": "ListObjectsV2 (max-keys=1)",
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

`error_kind` values (SDK-level):

| Value | Meaning |
|---|---|
| `service` | The endpoint returned a modeled service error. |
| `response` | The endpoint responded, but the SDK could not parse it as expected. |
| `dispatch` | Transport failed before an HTTP response was available. |
| `timeout` | The SDK timed out. |
| `construction` | Request construction or local SDK setup failed. |
| `pagination` | The endpoint returned inconsistent pagination metadata. |
| `unknown` | An SDK error variant this release does not classify. |

`diagnostic_code` values (for automation and triage):

| Value | Typical cause |
|---|---|
| `signature_mismatch` | Credentials, region, endpoint, clock skew, or addressing style do not match the provider's signing expectations. |
| `access_denied` | Credentials are invalid or lack bucket/list permissions. |
| `bucket_not_found` | Bucket name, account/project scope, region, or addressing style is wrong. |
| `region_or_endpoint_mismatch` | The provider redirected the request or rejected the signing region. |
| `operation_not_supported` | The endpoint does not support the operation or option used by that step. |
| `redirect` | HTTP redirect. |
| `bad_request` | The request shape was rejected before a more specific S3 code was available. |
| `not_found` | HTTP 404 without a modeled S3 error code. |
| `server_error` | HTTP 5xx. |
| `timeout` | SDK timeout before a complete response. |
| `transport_failure` | DNS, TCP, TLS, proxy, firewall, or other transport failure before HTTP metadata. |
| `invalid_response` | Data the SDK could not parse as the expected S3 response. |
| `request_construction` | Local SDK request construction failed before dispatch. |
| `pagination_token_missing` | Truncation reported without a continuation token. |
| `unknown_error` | No stable category matched. |

Existing report fields stay backward compatible, and new diagnostic fields
are added as optional fields.  A missing optional field means the SDK or
endpoint did not provide that metadata (not an empty string).  Automation
should branch on `status`, `http_status`, `s3_error_code`, `error_kind`, and
`diagnostic_code`, never on `error_message`.
