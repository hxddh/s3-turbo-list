# Endpoint Profiles

Endpoint profiles are optional presets for S3-compatible providers.  They are
local metadata plus conservative config defaults.  They do not probe the
network, change output schema, or add provider-specific pagination workarounds.

Use profiles explicitly:

```bash
s3-turbo-list profiles list
s3-turbo-list profiles show r2 --json

s3-turbo-list list \
  --bucket my-bucket \
  --endpoint-url https://<account-id>.r2.cloudflarestorage.com \
  --region auto \
  --profile r2
```

## Built-in Profiles

| Profile | Provider | Endpoint | Addressing | Project status |
|---|---|---|---|---|
| `aws` | AWS S3 | SDK-derived from region | virtual | validated baseline |
| `minio` | MinIO | deployment-specific, explicit | path | validated |
| `bos` | Baidu BOS S3-compatible API | `https://s3.{region}.bcebos.com` (default region `bj`) | virtual | validated with documented caveat |
| `r2` | Cloudflare R2 | account-specific, explicit | path | documented preset |
| `b2` | Backblaze B2 S3-compatible API | `https://s3.{region}.backblazeb2.com` | path | documented preset |
| `oss` | Alibaba Cloud OSS S3-compatible API | `https://{region}.aliyuncs.com` | virtual | documented preset |

Profiles with a `{region}` endpoint pattern derive the endpoint from the
run's `--region`, so everyday commands need no `--endpoint-url`:

```bash
s3-turbo-list --profile oss list --region oss-cn-beijing --bucket my-bucket
s3-turbo-list --profile bos list --region gz --bucket my-bos-bucket
s3-turbo-list --profile b2 list --region us-west-004 --bucket my-b2-bucket
```

`validated` means the project has run its endpoint validation flow for that
provider or local server.  `documented preset` means the profile encodes known
provider defaults, but the project has not claimed full validation for that
endpoint.  In particular, OSS, R2, and B2 profiles should be treated as starting
points until `compat-probe` and a representative listing run pass in the target
environment.

## Precedence

Profiles only fill defaults when the user did not provide a value.  Explicit
CLI/config values win over profile defaults for endpoint URL and addressing
style.

For example, `--profile bos` fills the standard BOS endpoint and virtual-hosted
addressing when they are otherwise unset.  Passing `--endpoint-url` or
`--addressing-style path` keeps the explicit value.

Profiles with deployment- or account-specific endpoints (`minio`, `r2`) warn
during local `doctor` and dry-run preflight until an endpoint URL is
configured; region-derived profiles (`oss`, `b2`) warn when neither an
endpoint nor a region is available.  Starter config placeholders such as
`<account-id>` or `<region>` are also reported locally so agents do not
launch a real run with an unedited template endpoint.

`compat-probe` takes its endpoint from the global `--endpoint-url` flag (or
its subcommand-local `--endpoint` override).  It applies the same placeholder guard to that value and exits with
provider setup code `3` before network setup when the endpoint still contains
template markers such as `<account-id>`.  Literal but unreachable endpoints are
left to the probe itself so connectivity failures remain part of the diagnostic
report.

## Caveats

- `profiles show` and `profiles list` are local-only commands.
- `compat-probe` is still the explicit command for real endpoint validation.
- `compat-probe` reports structured HTTP status, S3 error code, and request ID
  metadata when the AWS SDK exposes those values for service errors.  See
  [`compat-probe`](compat-probe.md) for the report field contract.
- R2, B2, and OSS presets are documented defaults, not a claim of full project
  validation.
- The BOS profile does not enable a default pagination workaround.
- `diff` is authoritative single-segment by design.  It ignores conventional
  hints caches, and `diff --hints-file` / `diff --resume` are rejected before
  any S3 request to avoid incomplete left-only or right-only results from
  mismatched segment boundaries.
