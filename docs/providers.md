# Endpoints

`s3tl` works against any endpoint that implements ListObjectsV2. There are
no presets: an endpoint is an `--endpoint-url`, plus `--path-style` and
`--region` where the provider needs them. Credentials always come from the
AWS SDK chain (`AWS_PROFILE`, `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`,
web identity, instance and container roles).

## Resolution

| | Explicit | Otherwise |
|---|---|---|
| Endpoint | `--endpoint-url` (`--target-endpoint-url` for a diff's right side) | `AWS_ENDPOINT_URL_S3`, then `AWS_ENDPOINT_URL`, else AWS S3 |
| Region | `--region` | AWS S3: the bucket's region, from the `x-amz-bucket-region` header of one HeadBucket. Another endpoint: `AWS_REGION` (or the profile's region), else `us-east-1` |
| Addressing | `--path-style` | path-style for IP and `localhost` endpoints, virtual-hosted otherwise |

The result's `source` / `target` show what was used. Because the endpoint is
resolved by `s3tl` itself (not left to the SDK), it is always the one
recorded in results, traces and checkpoints.

## Examples

```bash
# AWS S3: nothing to configure; the bucket's region is detected.
s3tl list s3://my-bucket/ -o out/

# MinIO (localhost is path-style automatically).
s3tl list s3://my-bucket/ -o out/ --endpoint-url http://localhost:9000
s3tl list s3://my-bucket/ -o out/ --endpoint-url https://minio.internal:9000 --path-style

# Cloudflare R2: account endpoint, region "auto".
s3tl list s3://my-bucket/ -o out/ \
  --endpoint-url https://<account-id>.r2.cloudflarestorage.com --region auto

# Backblaze B2.
s3tl list s3://my-bucket/ -o out/ \
  --endpoint-url https://s3.us-west-004.backblazeb2.com --region us-west-004 --path-style

# Alibaba Cloud OSS (virtual-hosted).
s3tl list s3://my-bucket/ -o out/ \
  --endpoint-url https://oss-cn-beijing.aliyuncs.com --region oss-cn-beijing

# Baidu BOS (virtual-hosted).
s3tl list s3://my-bucket/ -o out/ --endpoint-url https://s3.bj.bcebos.com --region bj

# Cross-provider diff: AWS on the left, MinIO on the right.
s3tl diff s3://src/data/ s3://dst/data/ -o d/ --target-endpoint-url http://10.0.0.5:9000
```

For an endpoint you use every day, set `AWS_ENDPOINT_URL_S3` (and
`AWS_REGION`) in the environment or the AWS profile instead of repeating the
flags.

| Endpoint | Status |
|---|---|
| AWS S3 | Validated (path and virtual-hosted) |
| MinIO | Validated (path and virtual-hosted) |
| Baidu BOS | Validated; fully ListObjectsV2-compatible (see `docs/validation-results/`) |
| Cloudflare R2, Backblaze B2, Alibaba OSS | Run `s3tl check` first |

## `s3tl check`

```bash
s3tl check                                    # credentials, region, endpoint, proxy (no requests)
s3tl check s3://my-bucket/logs/ --endpoint-url https://endpoint
```

With an S3 URL, `check` resolves the region like a run and sends a handful
of requests — HeadBucket, a one-key ListObjectsV2, a delimiter listing, and a
two-page pagination check — and reports each under `check.probe.tests`, with
the HTTP status, S3 error code, request IDs and a recommendation on failure.
`check.probe.overall_status` is `compatible`, `partial` (some requests
failed; the listing may still work) or `incompatible` (every request failed:
exit 3 for a setup problem, 4 otherwise). `--trace FILE` records the probe
requests.

`check` also reports the proxy the request would go through
(`HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` / `NO_PROXY`, matched the way the
SDK's HTTP client matches them): a local endpoint goes through `HTTP_PROXY`
too unless its host is in `NO_PROXY`.

## Compatibility position

`s3tl` targets the ListObjectsV2 contract as AWS S3 implements it and does
not work around provider deviations. It guards against them instead: a
listing whose keys arrive out of order or repeat fails with
`endpoint.inconsistent_listing`; a truncated page without a usable
continuation token is retried with `start-after` rather than read as the
end of the listing. If a provider deviates, record it with `check` and
`--trace`, and report it to the provider.
