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

| Profile | Provider | Default region | Endpoint default | Addressing | Project status |
|---|---|---:|---|---|---|
| `aws` | AWS S3 | user/SDK supplied | none | virtual | validated baseline |
| `minio` | MinIO | user supplied | none | path | validated |
| `bos` | Baidu BOS S3-compatible API | `bj` | `https://s3.bj.bcebos.com` | virtual | validated with documented caveat |
| `r2` | Cloudflare R2 | `auto` | account-specific | path | documented preset |
| `b2` | Backblaze B2 S3-compatible API | provider-specific | account/bucket-specific | path | documented preset |
| `oss` | Alibaba Cloud OSS S3-compatible API | provider-specific | region-specific | virtual | documented preset |

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

## Caveats

- `profiles show` and `profiles list` are local-only commands.
- `compat-probe` is still the explicit command for real endpoint validation.
- R2, B2, and OSS presets are documented defaults, not a claim of full project
  validation.
- The BOS profile does not enable a default pagination workaround.
- Hinted multi-segment diff coordination is still deferred.
