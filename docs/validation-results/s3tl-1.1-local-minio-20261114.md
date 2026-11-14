# s3tl 1.1: local MinIO validation (2026-11-14)

End-to-end check of the 1.1 features against a real S3 implementation,
run entirely on the local machine: a MinIO server built from source
(`DEVELOPMENT.GOGET`, single node, single drive) listening on
`127.0.0.1:9100`, path-style, static local credentials. No cloud endpoint
was contacted. Binary: `s3tl` 1.1.0 release build.

## Fixture

Seeded with boto3:

- `src`: 20,007 small objects (`data/NN/obj-NNNNNN.bin` over 37 prefixes,
  plus keys with a space, `+`, `%`, `&<>`, `?`, `~` and non-ASCII
  characters), each with a CRC32 checksum, and `mp/same-bytes.bin` (6 MiB,
  single PUT, CRC32 full-object checksum). 20,008 objects.
- `dst`: the same keys except every 1,000th from index 1 (21 missing), every
  1,000th from index 2 rewritten with the same size and different bytes (21),
  15 extra keys, and `mp/same-bytes.bin` re-uploaded as a 2-part multipart
  upload of the same bytes with a CRC32 `FULL_OBJECT` checksum: ETag
  `5c40…-2` against `2446…`, checksum `EyNCRQ==` on both.
- `ver` (versioning enabled): `a.txt` with 3 versions, `b.txt` with 1 version
  then a delete marker, `c d.txt` with 2 versions, 500 single-version keys.
  507 entries.
- `out`: empty, for S3 output.

## Results

| Run | Result |
|---|---|
| `check s3://src` | `compatible` |
| `list s3://src -o DIR` | 20,008 objects, 1.1 s |
| `list s3://src -o -` | 20,008 NDJSON rows; every special key round-trips exactly |
| `list s3://src/data/ --delimiter /` | 37 folder rows |
| `diff s3://src s3://dst` (`etag`) | left_only 21, right_only 15, changed 22, equal 19,965 |
| `diff … --compare size` | changed 0, equal 19,987 |
| `diff … --compare checksum` | changed 21, equal 19,966, verified_equal 1, unverified 0: the multipart re-upload is proven equal, the 21 same-size rewrites stay changed (their full-object CRC32s differ) |
| `list s3://ver --versions -o -` | 507 rows; per key newest first; the delete marker has size 0, null `etag`/`storage_class`, `is_latest` true |
| `list s3://ver --versions --max-keys 7 -c 8 -o DIR` | 507 rows, schema `s3tl.list-versions/1`, 1 delete marker |
| `list s3://ver --versions --max-keys 1 -c 1`, SIGINT, then `--resume` | 18 rows + 489 rows = 507, no duplicate `(key, version_id)` |
| an interrupted `--versions` run resumed without `--versions` | `usage.checkpoint_mismatch` (`differs in: versions`) |
| `list s3://src -o s3://out/run1/` | `part-00000.parquet` + `_result.json`; rows identical to the local dataset (key, etag, size) |
| the same command again | `usage.output_not_empty`, nothing sent but the emptiness check |
| `diff s3://src s3://dst -o s3://out/diff1` | 58 rows, schema `s3tl.diff/1`, same as the local diff dataset |
| `list … -o s3://out/int1/`, SIGINT mid-run | exit 130, `interrupted; nothing was written to the S3 output`; 0 objects under the prefix |
| open multipart uploads in `out` after all runs | 0 |

The 16 MiB upload-part splitting is not reached by outputs this small; it
is covered by the local mock (`local_mock_s3_output_assembles_multi_part_uploads_in_order`,
1 KiB parts through the hidden `S3TL_UPLOAD_PART_BYTES` override), which
reassembles the parts and checks every key.

## Not covered

- Real cloud providers: no endpoint other than this local MinIO was used.
  Behaviour of `--versions`, `-o s3://` and `--compare checksum` on AWS,
  R2, OSS, BOS and others is expected from the S3 API contract and remains
  to be confirmed when a run against those endpoints is authorized.
- Composite (per-part) checksums: MinIO stored full-object checksums here;
  the composite case is covered by unit tests (`checksum::tests`).
