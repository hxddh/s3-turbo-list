# Endpoint Validation Plan — s3-turbo-list

**Date**: 2026-05-14
**Scope**: MinIO (local/test), AWS S3 (us-east-1 or chosen region), Baidu BOS (`s3.bj.bcebos.com`)
**Prerequisites**: 85 tests passing, `cargo check` 0 warnings, working tree clean.

---

## 0. Required Tools

Install these before running the validation plan:

| Tool | Min version | Purpose | Install |
|---|---|---|---|
| `cargo` + Rust toolchain | 1.80+ | Build s3-turbo-list | `rustup` |
| `mc` (MinIO Client) | any recent | Populate MinIO test data | `wget https://dl.min.io/client/mc/release/linux-arm64/mc && chmod +x mc` |
| `aws` CLI | v2 | Populate/verify AWS S3 buckets | `pip install awscli` or `apt install awscli` |
| `python3` | 3.8+ | Trace analysis, Parquet validation | system package |
| `pyarrow` | 10+ | Read Parquet files | `pip install pyarrow` |
| `toml` (Python) | any | Read checkpoint TOML | `pip install toml` |
| `curl` | any | Connectivity checks | system package |
| `bcecmd` (optional) | any | BOS bucket management | BOS console download |

Verify with:
```bash
cargo --version && mc --version && aws --version && python3 -c "import pyarrow, toml; print('pyarrow+toml OK')"
```

---

## 0. Environment Setup (pre-flight)

### 0.1 Build release binary

```bash
cd /home/ubuntu/s3-turbo-list
cargo build --release 2>&1 | tail -3
# Expected: "Finished release [optimized] target(s)"
BIN="$(pwd)/target/release/s3-turbo-list"
$BIN --help | head -5
```

### 0.2 Create a validation directory

```bash
VALDIR="/tmp/s3tl-validation-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$VALDIR"/{minio,aws,bos,artifacts}
echo "Validation dir: $VALDIR"
```

### 0.3 Prepare minimal config files (reference)

These config files document the recommended settings per endpoint. The validation
commands below use CLI flags instead of `--config` so each test is self-contained.
Save these as fallback if you want to shorten later commands.

**MinIO** (`$VALDIR/minio/s3-turbo-list.toml`):
```toml
[s3]
endpoint_url = "http://localhost:9000"
addressing_style = "path"
profile = "minio"
force_path_style = true
max_attempts = 3
initial_backoff_secs = 1
connect_timeout_secs = 5
operation_timeout_secs = 5

[runtime]
worker_threads = 4
max_concurrency = 4
```

**AWS S3** (`$VALDIR/aws/s3-turbo-list.toml`):
```toml
[s3]
max_attempts = 5
initial_backoff_secs = 5
connect_timeout_secs = 10
operation_timeout_secs = 5

[runtime]
worker_threads = 10
max_concurrency = 50
```

**BOS** (`$VALDIR/bos/s3-turbo-list.toml`):
```toml
[s3]
endpoint_url = "https://s3.bj.bcebos.com"
addressing_style = "path"
profile = "bos"
force_path_style = true
max_attempts = 5
initial_backoff_secs = 5
connect_timeout_secs = 10
operation_timeout_secs = 5

[runtime]
worker_threads = 6
max_concurrency = 20
```

### 0.4 Verify credentials are available

```bash
# MinIO: assumes local MinIO with default creds
curl -s http://localhost:9000 | head -3

# AWS S3
aws sts get-caller-identity 2>&1
# Expected: JSON with Account/Arn

# BOS: check ~/.aws/credentials for [bos] profile
grep -A2 '\[bos\]' ~/.aws/credentials 2>/dev/null || echo "WARNING: [bos] profile not found — will skip BOS tests"
```

---

## Part A — MinIO Local Endpoint

**Prerequisites**: MinIO running on `localhost:9000` with a test bucket and test objects.

### A.1 Create test data in MinIO

```bash
# Create a bucket with varied-prefix objects
BUCKET="s3tl-validation"

# Use aws cli (or mc) to populate — adapt as needed
mc mb local/$BUCKET 2>/dev/null || true

# Create objects in a structured hierarchy
for d in a b c; do
  for f in 01 02 03 04 05; do
    echo "data $d/$f" | mc pipe local/$BUCKET/$d/file-$f.txt
  done
done

# Also some top-level objects and a deep path
echo "top" | mc pipe local/$BUCKET/top.txt
echo "deep" | mc pipe local/$BUCKET/x/y/z/deep.txt

# Add an object with a special character in the name (encoding-type=url)
echo "special" | mc pipe "local/$BUCKET/logs/2025/file with spaces.log"
echo "plus" | mc pipe "local/$BUCKET/logs/2025/file+plus.log"

# Verify
mc ls local/$BUCKET/ --recursive | wc -l
# Expected: ≥18 objects
```

### A.2 Compat-probe

```bash
cd "$VALDIR/minio"

$BIN compat-probe \
  --endpoint http://localhost:9000 \
  --region us-east-1 \
  --bucket "$BUCKET" \
  --addressing-style path \
  --output "$VALDIR/artifacts/minio-compat-probe.json"

cat "$VALDIR/artifacts/minio-compat-probe.json" | python3 -m json.tool
```

**Expect**:
- `overall_status: "compatible"` (6 tests all `"ok"`)
- `HeadBucket` latency < 200ms
- `ListObjectsV2 (max-keys=1)` returns `contents_count: 1`
- `ListObjectsV2 (encoding-type=url)` is `"ok"` (MinIO supports it)
- `ListObjectsV2 pagination check` is `"ok"` or `"skipped"` (if < 3 objects)

**Failure interpretation**:
- `HeadBucket error 403` → credential issue, check `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
- `ListObjectsV2 error` → bucket doesn't exist or path-style misconfiguration
- `encoding-type=url error` → MinIO version too old (< RELEASE.2022-10-20T00-55-09Z; upgrade)

### A.3 Basic list (prefix/delimiter/max_keys/continuation_token)

#### A.3.1 Default listing (no delimiter, default prefix)

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --threads 4 --concurrency 4 \
  --output-parquet-file "$VALDIR/artifacts/minio-basic.parquet" \
  --output-ks-file "$VALDIR/artifacts/minio-basic.ks" \
  --log
```

**Expect**:
- Exit code 0
- `minio-basic.parquet` created, non-zero size
- `minio-basic.ks` created (KeySpace file listing all prefixes observed)
- Log file created (`turbo_list_*.log`)
- Log contains `"Flat List S3 Task — $BUCKET — completed"`

**Parquet verification**:
```bash
python3 -c "
import pyarrow.parquet as pq
t = pq.read_table('$VALDIR/artifacts/minio-basic.parquet')
print('Rows:', t.num_rows)
print('Schema:', t.schema)
print(t.to_pandas().head())
"
```

**Expect**:
- `Rows` ≥ 18 (or however many objects created)
- Schema contains: `Key`, `Size`, `LastModified`, `ETag`, `DiffFlag`
- All `DiffFlag` = 1 (list mode — all objects are left-side present; 0 used only in diff mode)

**Failure interpretation**:
- 0 rows → prefix mismatch, check prefix syntax
- Empty parquet but log says "completed" → objects existed but filter screened them; check prefix and bucket
- No `.parquet` file → data_map task crashed; check log for errors

#### A.3.2 Prefix filtering

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "a/" \
  --threads 4 --concurrency 4 \
  --output-parquet-file "$VALDIR/artifacts/minio-prefix-a.parquet"
```

**Parquet verification**:
```bash
python3 -c "
import pyarrow.parquet as pq
t = pq.read_table('$VALDIR/artifacts/minio-prefix-a.parquet')
print('Rows:', t.num_rows)
# All keys should start with 'a/'
for k in t.column('Key').to_pylist():
    assert k.startswith('a/'), f'Key {k} does not start with a/'
print('All keys OK — start with a/')
"
```

**Expect**: Only `a/`-prefixed objects. `Rows` should be exactly 5.

#### A.3.3 Delimiter (hierarchical)

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --delimiter "/" \
  --max-keys 5 \
  --threads 4 --concurrency 4 \
  --output-parquet-file "$VALDIR/artifacts/minio-delim.parquet"
```

**Trace observation**:
```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --delimiter "/" \
  --max-keys 5 \
  --threads 4 --concurrency 4 \
  --debug-s3 2>&1 | head -20
```

**Expect**:
- Trace events show `"delimiter":"/"` in each `ListObjectsV2` event
- `"max_keys":5` in events
- If bucket has a hierarchy, `"common_prefixes_count"` > 0 on some pages
- Parquet rows match expected count (top-level + all nested)

#### A.3.4 max_keys variation

```bash
# Test with max_keys=2 (many small pages — stress pagination)
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --max-keys 2 \
  --threads 4 --concurrency 4 \
  --debug-s3 2>&1 | grep '"max_keys"' | sort | uniq -c

# Expected: many events all with "max_keys":2
```

**Expect**: All trace events show `"max_keys":2`. Pagination works correctly — all objects listed despite small page size.

#### A.3.5 start_after

`--start-after` is wired into the S3 ListObjectsV2 `start-after` parameter.
When set, listing begins after the specified key (lexicographic).

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --start-after "b/" \
  --threads 4 --concurrency 4 \
  --output-parquet-file "$VALDIR/artifacts/minio-start-after.parquet"
```

**Parquet check**:
```bash
python3 -c "
import pyarrow.parquet as pq
t = pq.read_table('$VALDIR/artifacts/minio-start-after.parquet')
keys = sorted(t.column('Key').to_pylist())
# All keys should be >= 'b/' lexicographically
for k in keys:
    assert k >= 'b/', f'Key {k} should be >= b/'
print(f'Rows: {t.num_rows} — all keys >= b/')
"
```

#### A.3.6 encoding-type=url (implicit through --debug-s3)

Check that response keys with spaces/plus signs are correctly decoded by the AWS SDK:

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "logs/" \
  --debug-s3 2>&1 | grep -o '"key_count":[0-9]*' | head -5
```

**Expect**: `key_count > 0` in trace events. The special-character objects (`file with spaces.log`, `file+plus.log`) appear in the output without URL-encoding.

**Manual verification**:
```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "logs/" \
  --output-parquet-file "$VALDIR/artifacts/minio-encode.parquet"

python3 -c "
import pyarrow.parquet as pq
t = pq.read_table('$VALDIR/artifacts/minio-encode.parquet')
keys = t.column('Key').to_pylist()
print('Keys found:', keys)
assert any(' ' in k for k in keys), 'Should have a key with spaces'
print('Special chars OK')
"
```

### A.4 Trace compatibility (--debug-s3 and --trace-compat)

#### A.4.1 --debug-s3 (stderr)

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --max-keys 5 \
  --threads 2 --concurrency 2 \
  --debug-s3 2>"$VALDIR/artifacts/minio-trace-stderr.jsonl" \
  --output-parquet-file "$VALDIR/artifacts/scratch-trace.parquet"
```

**Verify JSONL structure**:
```bash
# Count events
wc -l < "$VALDIR/artifacts/minio-trace-stderr.jsonl"

# Every line should be valid JSON
python3 -c "
import json
with open('$VALDIR/artifacts/minio-trace-stderr.jsonl') as f:
    for i, line in enumerate(f, 1):
        line = line.strip()
        if not line: continue
        obj = json.loads(line)
        # Required fields must be present
        for k in ['timestamp', 'operation', 'endpoint_url', 'bucket', 'prefix',
                   'addressing_style', 'http_status', 'retry_attempt',
                   'latency_ms', 'is_truncated']:
            assert k in obj, f'Line {i}: missing field {k}'
print(f'All lines valid JSON with required fields')
"
```

**Expect**:
- At least 1 event per segment (usually many more for paginated responses)
- `"operation": "ListObjectsV2"` on every line
- `"addressing_style": "path"` on every line
- `"http_status": 200` on success lines
- `"is_truncated"` is `true` for intermediate pages, `false` for last page
- `"contents_count"` reflects actual objects per page
- `"next_continuation_token_present": true` when `is_truncated` is true

**Failure interpretation**:
- No trace output → `--debug-s3` flag not propagated; check `apply_cli_overrides` in main.rs
- Duplicate line content → trace writer not flushing correctly
- Missing `is_truncated` or `contents_count` → event construction bug in `emit_trace_compat`

#### A.4.2 --trace-compat (file)

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --max-keys 5 \
  --threads 2 --concurrency 2 \
  --trace-compat "$VALDIR/artifacts/minio-trace-file.jsonl" \
  --output-parquet-file "$VALDIR/artifacts/scratch-trace2.parquet"

# Compare with stderr output — structure should be identical
diff <(head -5 "$VALDIR/artifacts/minio-trace-stderr.jsonl" | python3 -c "
import json, sys
for l in sys.stdin:
    obj = json.loads(l.strip())
    print(sorted(obj.keys()))
") <(head -5 "$VALDIR/artifacts/minio-trace-file.jsonl" | python3 -c "
import json, sys
for l in sys.stdin:
    obj = json.loads(l.strip())
    print(sorted(obj.keys()))
")
# Expected: no diff — same field sets
```

**Expect**: Both outputs have the same schema. File output contains all events that stderr does.

### A.5 Path-style vs virtual-hosted-style

MinIO has `force_path_style: true` by default with `--profile minio` / `--addressing-style path`. Test that the alternate style is respected.

#### A.5.1 Virtual-hosted-style (auto — MinIO should detect failure or work via DNS)

```bash
# Test with explicit virtual-hosted (may fail if no DNS)
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --addressing-style virtual \
  --prefix "/" \
  --max-keys 2 \
  --threads 2 --concurrency 2 \
  --debug-s3 2>&1 | head -5
```

**Expect**:
- If MinIO is configured for virtual-host style: works, trace shows `"addressing_style":"virtual"`
- If not: DNS error or connection refused — trace shows error event with `fatal: true`
- **Note**: virtual-hosted style may succeed against MinIO depending on version and
  server configuration. This is environment-dependent and does not imply virtual-hosted
  will work against BOS.

#### A.5.2 Path style (explicit)

Already tested above; confirm trace shows `"addressing_style":"path"` consistently.

```bash
grep -o '"addressing_style":"[^"]*"' "$VALDIR/artifacts/minio-trace-file.jsonl" | sort | uniq -c
# Expected: all "addressing_style":"path"
```

### A.6 Continuation token behavior

With `--max-keys 2` and many objects, verify that pagination with continuation tokens works:

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --max-keys 2 \
  --threads 4 --concurrency 4 \
  --trace-compat "$VALDIR/artifacts/minio-paginate.jsonl" \
  --output-parquet-file "$VALDIR/artifacts/minio-paginate.parquet"

# Check that we have multiple pages per segment
python3 -c "
import json
pages_per_segment = {}
with open('$VALDIR/artifacts/minio-paginate.jsonl') as f:
    for line in f:
        obj = json.loads(line.strip())
        sa = obj.get('start_after', 'NONE')
        pages_per_segment[sa] = pages_per_segment.get(sa, 0) + 1

for sa, count in pages_per_segment.items():
    print(f'start_after={sa}: {count} pages')

# With max_keys=2 and 5+ objects per segment, expect > 1 page per segment
assert any(c > 1 for c in pages_per_segment.values()), \
    'No segment had multiple pages — pagination not exercised'
print('Pagination verified: multiple pages detected')
"
```

### A.7 Parquet output validity

```bash
python3 -c "
import pyarrow.parquet as pq
import os

for f in ['minio-basic.parquet', 'minio-delim.parquet', 'minio-paginate.parquet']:
    path = os.path.join('$VALDIR/artifacts', f)
    if not os.path.exists(path):
        print(f'SKIP {f} — not found')
        continue
    t = pq.read_table(path)
    # Check required columns
    for col in ['Key', 'Size', 'LastModified', 'ETag']:
        assert col in t.column_names, f'{f}: missing column {col}'
    # Row count sanity
    assert t.num_rows > 0, f'{f}: zero rows'
    # Schema types
    assert t.schema.field('Key').type == pyarrow.string(), f'{f}: Key not string'
    assert t.schema.field('Size').type == pyarrow.int64(), f'{f}: Size not int64'
    print(f'{f}: {t.num_rows} rows, schema OK')
print('All Parquet files valid')
"
```

### A.8 Object filter

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --filter "SOURCE.size > 0" \
  --output-parquet-file "$VALDIR/artifacts/minio-filter.parquet"

python3 -c "
import pyarrow.parquet as pq
t = pq.read_table('$VALDIR/artifacts/minio-filter.parquet')
print(f'Filtered rows: {t.num_rows}')
# All should have Size > 0 (our test objects are small but > 0)
sizes = t.column('Size').to_pylist()
assert all(s > 0 for s in sizes), 'Filter failed: some objects have Size=0'
print('Filter OK: all sizes > 0')
"
```

### A.9 Checkpoint / resume

#### A.9.1 Initial run (create checkpoint state)

```bash
cd "$VALDIR/minio"

# First run with --resume (no checkpoint exists yet — starts fresh)
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --max-keys 1 \
  --threads 2 --concurrency 2 \
  --resume \
  --log \
  --output-parquet-file "$VALDIR/artifacts/minio-resume.parquet"

# Check if checkpoint was saved (it's saved every 30s during run)
CHECKPOINT_FILE="us-east-1_${BUCKET}_checkpoint.toml"
ls -la "$CHECKPOINT_FILE" 2>/dev/null && echo "Checkpoint exists" || echo "Checkpoint not saved (run too fast)"

# If it exists, inspect
cat "$CHECKPOINT_FILE" 2>/dev/null
```

**Expect**:
- Run completes normally
- If run was long enough (>30s with many small segments), a checkpoint is saved
- Checkpoint TOML contains: `bucket`, `prefix`, `total_segments`, `completed_indices`, `last_updated`, `[identity]` block

**Checkpoint not saved?** Force a longer run:
```bash
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "x/" \
  --max-keys 1 \
  --threads 2 --concurrency 2 \
  --no-auto-hints \
  --resume \
  --log
sleep 35  # Wait for potential checkpoint save
ls -la "us-east-1_${BUCKET}_checkpoint.toml"
```

#### A.9.2 Resume from checkpoint

If a checkpoint file exists:

```bash
# Run again with --resume
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --max-keys 1 \
  --threads 2 --concurrency 2 \
  --resume \
  --log 2>&1 | grep -i "checkpoint\|resum"

# Expected log lines:
# "Resuming checkpoint: N of M segments completed"
# "Resume: X segments filtered, Y remaining"
```

**Expect**:
- Log mentions checkpoint loading and identity verification
- If all segments were completed previously, log shows `"all N segments already completed"` and run exits quickly
- If some segments remain, only those are processed

#### A.9.3 Checkpoint identity mismatch

```bash
# First run: create checkpoint with delimiter="/"
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --delimiter "/" \
  --max-keys 5 \
  --resume \
  --log \
  --output-parquet-file "$VALDIR/artifacts/scratch-ckpt.parquet"

# Second run: try to resume with different delimiter — should reject checkpoint
$BIN list \
  --bucket "$BUCKET" \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio \
  --force-path-style \
  --prefix "/" \
  --delimiter "#" \
  --max-keys 5 \
  --resume \
  --log 2>&1 | grep -i "checkpoint\|identity\|mismatch"
```

**Expected log output** (from the second run):
```
WARN ... Checkpoint ... identity mismatch on field(s): delimiter — discarding checkpoint and starting fresh
```

**Expect**: The run starts from scratch (no "Resuming checkpoint" message, no "segments filtered"). All segments are processed.

**Additional mismatch tests** (run each independently):
```bash
# Mismatch: max_keys changed
# First: --max-keys 5 --resume ...
# Then:  --max-keys 10 --resume ...
# Expect: "identity mismatch on field(s): max_keys"

# Mismatch: profile changed
# First: --profile minio --resume ...
# Then:  --profile other --resume ...
# Expect: "identity mismatch on field(s): profile"

# Mismatch: mode changed (list vs diff)
# First:  list --resume ...
# Then:   diff ... (creates different checkpoint path, no direct mismatch)
```

---

## Part B — AWS S3

**Prerequisites**: Valid AWS credentials with `s3:ListBucket` on test bucket.

### B.1 Create / identify test bucket

```bash
BUCKET="s3tl-validation-$(date +%s)"
REGION="us-east-1"

# Create if it doesn't exist
aws s3 mb "s3://$BUCKET" --region "$REGION" 2>/dev/null || echo "Bucket may already exist"

# Populate with structured test objects
for d in alpha beta gamma; do
  for f in 01 02 03 04 05; do
    echo "data $d $f" | aws s3 cp - "s3://$BUCKET/$d/file-$f.txt" --region "$REGION" --no-sign-request 2>/dev/null || \
    echo "data $d $f" | aws s3 cp - "s3://$BUCKET/$d/file-$f.txt" --region "$REGION"
  done
done

# Top-level objects
echo "top" | aws s3 cp - "s3://$BUCKET/top-level.txt" --region "$REGION"
echo "test" | aws s3 cp - "s3://$BUCKET/another.txt" --region "$REGION"

# Special chars
echo "special" | aws s3 cp - "s3://$BUCKET/logs/file with spaces.log" --region "$REGION"
echo "plus" | aws s3 cp - "s3://$BUCKET/logs/file+plus.log" --region "$REGION"

# Verify
aws s3 ls "s3://$BUCKET/" --recursive --region "$REGION" | wc -l
# Expected: ≥18
```

### B.2 Compat-probe (AWS)

```bash
cd "$VALDIR/aws"

$BIN compat-probe \
  --endpoint https://s3.amazonaws.com \
  --region "$REGION" \
  --bucket "$BUCKET" \
  --addressing-style virtual \
  --output "$VALDIR/artifacts/aws-compat-probe.json"

cat "$VALDIR/artifacts/aws-compat-probe.json" | python3 -m json.tool
```

**Expect**:
- `overall_status: "compatible"`
- All 6 tests `"ok"`
- `HeadBucket` HTTP status 200
- `ListObjectsV2 pagination check` is `"ok"` (not skipped) if ≥ 3 objects
- `request_id` populated on at least one test result

**Failure interpretation**:
- `AccessDenied` / 403 → IAM permissions missing; run `aws sts get-caller-identity` and verify policy
- `NoSuchBucket` / 404 → bucket doesn't exist in specified region
- `SignatureDoesNotMatch` / 403 → credentials or clock skew; check `date` command

### B.3 Standard AWS listing (virtual-hosted style)

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "/" \
  --threads 10 --concurrency 50 \
  --output-parquet-file "$VALDIR/artifacts/aws-basic.parquet" \
  --output-ks-file "$VALDIR/artifacts/aws-basic.ks" \
  --log
```

**Expect**:
- Exit code 0
- Parquet with ≥18 rows
- Log mentions `"Flat List S3 Task — $BUCKET — completed"`

**Failure interpretation**:
- `DispatchFailure` → network/VPN issue or region not set correctly
- `region must be set` → pass `--region` explicitly
- Slow listing (many minutes) → concurrency too low; increase `--threads` / `--concurrency`

### B.4 --debug-s3 and --trace-compat (AWS)

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "/" \
  --max-keys 5 \
  --threads 5 --concurrency 10 \
  --debug-s3 \
  --trace-compat "$VALDIR/artifacts/aws-trace.jsonl" \
  2>"$VALDIR/artifacts/aws-trace-stderr.jsonl" \
  --output-parquet-file "$VALDIR/artifacts/scratch-aws-trace.parquet"
```

**Verify trace JSONL**:
```bash
python3 -c "
import json

# Check --trace-compat file
with open('$VALDIR/artifacts/aws-trace.jsonl') as f:
    lines = [json.loads(l.strip()) for l in f if l.strip()]
    print(f'--trace-compat: {len(lines)} events')

    # Verify addressing_style
    styles = {e['addressing_style'] for e in lines}
    print(f'Addressing styles: {styles}')  # Expect: {'virtual'}

    # Verify request_id present
    ids = [e.get('request_id') for e in lines if e.get('request_id')]
    print(f'Events with x-amz-request-id: {len(ids)}/{len(lines)}')

    # Check for continuation token chain
    with_token = [e for e in lines if e.get('next_continuation_token_present')]
    print(f'Events with continuation token: {len(with_token)}')

    # Check truncated_raw_body absent on success
    bodies = [e.get('truncated_raw_body') for e in lines if e.get('truncated_raw_body')]
    assert not bodies, f'truncated_raw_body present on {len(bodies)} success events!'
    print('No truncated_raw_body on success events (correct)')

print('AWS trace validation complete')
"
```

**Expect**:
- `addressing_style` is `"virtual"` for all events (AWS default)
- `request_id` populated on nearly all events (AWS always returns x-amz-request-id)
- `next_continuation_token_present: true` for paginated pages
- No `truncated_raw_body` on 200 responses

### B.5 Path-style vs virtual-hosted (AWS)

```bash
# AWS with explicit path-style
$BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "/" \
  --addressing-style path \
  --max-keys 5 \
  --threads 5 --concurrency 10 \
  --debug-s3 2>&1 | grep '"addressing_style"' | head -3

# Expected: "addressing_style":"path" in trace events
```

**Expect**: Both path and virtual styles work with AWS. Trace reflects the requested style.

### B.6 Prefix / delimiter / max_keys (AWS)

Run the same validation as MinIO A.3.2–A.3.5 against AWS:

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "alpha/" \
  --output-parquet-file "$VALDIR/artifacts/aws-prefix-alpha.parquet"

python3 -c "
import pyarrow.parquet as pq
t = pq.read_table('$VALDIR/artifacts/aws-prefix-alpha.parquet')
keys = t.column('Key').to_pylist()
assert all(k.startswith('alpha/') for k in keys)
print(f'AWS prefix filter OK: {t.num_rows} rows, all start with alpha/')
"
```

### B.7 continuation_token with explicit token

```bash
# List with max_keys=1 to get a continuation token
LIST_OUT=$($BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "/" \
  --max-keys 1 \
  --debug-s3 2>&1 | grep '"next_continuation_token"' | head -1)

echo "Continuation token: $LIST_OUT"
```

This verifies that AWS properly returns `NextContinuationToken` and the trace captures it.

### B.8 encoding-type=url (AWS)

Test that URLs with spaces are handled:

```bash
$BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "logs/" \
  --output-parquet-file "$VALDIR/artifacts/aws-encode.parquet"

python3 -c "
import pyarrow.parquet as pq
t = pq.read_table('$VALDIR/artifacts/aws-encode.parquet')
keys = t.column('Key').to_pylist()
print('Keys:', keys)
assert any(' ' in k for k in keys), 'No key with spaces found'
assert any('+' in k for k in keys), 'No key with + found'
print('AWS special chars OK')
"
```

### B.9 Diff mode lifecycle (AWS — two buckets)

```bash
# Create a second bucket for diff
TARGET_BKT="s3tl-target-$(date +%s)"
aws s3 mb "s3://$TARGET_BKT" --region "$REGION"

# Copy some objects, modify some
aws s3 cp "s3://$BUCKET/alpha/" "s3://$TARGET_BKT/alpha/" --recursive --region "$REGION"
aws s3 cp "s3://$BUCKET/beta/" "s3://$TARGET_BKT/beta/" --recursive --region "$REGION"
# gamma/ is deliberately NOT copied (left-only)
# Add a right-only object
echo "right-only" | aws s3 cp - "s3://$TARGET_BKT/right-only.txt" --region "$REGION"

# Run diff with logging
cd "$VALDIR/aws"
$BIN diff \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --target-bucket "$TARGET_BKT" \
  --target-region "$REGION" \
  --prefix "/" \
  --threads 10 --concurrency 50 \
  --output-parquet-file "$VALDIR/artifacts/aws-diff.parquet" \
  --log
```

**Expect**:
- Exit code 0
- Log contains `"diff mode comparison complete — data_map will finalize output"`
- Parquet contains `DiffFlag` column with various values (0, 1, 2, 3)

**Verify diff output**:
```bash
python3 -c "
import pyarrow.parquet as pq
from collections import Counter

t = pq.read_table('$VALDIR/artifacts/aws-diff.parquet')
print(f'Total rows: {t.num_rows}')

flags = t.column('DiffFlag').to_pylist()
counts = Counter(flags)
print(f'DiffFlag distribution: {dict(counts)}')
# 0 = Equal (present in both, matched)
# 1 = Plus (left-only)
# 2 = Minus (right-only)
# 3 = Mismatch (size or etag differs)

# Check gamma/ objects are Plus (left-only)
keys = t.column('Key').to_pylist()
gamma_keys = [(k, f) for k, f in zip(keys, flags) if 'gamma/' in k]
print(f'gamma/ objects: {gamma_keys}')
# Expect all gamma/ keys to have DiffFlag=1 (left-only)

# Check right-only.txt is Minus
right_only = [(k, f) for k, f in zip(keys, flags) if 'right-only.txt' in k]
print(f'right-only: {right_only}')
# Expect DiffFlag=2

print('Diff mode lifecycle verified')
"
```

**Failure interpretation**:
- Only one side listed → target region/bucket misconfigured
- All DiffFlag=0 → keys not matching between left and right; check prefix/bucket names
- No rows for gamma/ → left-side objects not listed; check permissions
- Crash during diff → data_map channel closed prematurely; check logs

### B.10 Checkpoint/resume (AWS)

```bash
cd "$VALDIR/aws"

# Run with resume and small concurrency (to slow down, ensuring checkpoint saves)
$BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "/" \
  --max-keys 1 \
  --threads 2 --concurrency 2 \
  --resume \
  --no-auto-hints \
  --log

sleep 2
CHECKPOINT_FILE="${REGION}_${BUCKET}_checkpoint.toml"
cat "$CHECKPOINT_FILE" 2>/dev/null && echo "=== Checkpoint exists ===" || echo "No checkpoint yet"
```

**Check identity block**:
```bash
python3 -c "
import sys
try:
    import toml
except ImportError:
    print('Install toml: pip install toml')
    sys.exit(1)

with open('$CHECKPOINT_FILE') as f:
    ckpt = toml.load(f)

identity = ckpt.get('identity')
assert identity is not None, 'No [identity] in checkpoint!'

required = ['bucket', 'region', 'prefix', 'delimiter', 'max_keys',
            'profile', 'addressing_style', 'mode']
for field in required:
    assert field in identity, f'Missing identity field: {field}'
    print(f'  identity.{field} = {identity[field]}')

print('Checkpoint identity block verified')
"
```

### B.11 Parquet output validity (AWS)

```bash
python3 -c "
import pyarrow.parquet as pq
import pyarrow as pa
import os

files = ['aws-basic.parquet', 'aws-diff.parquet']
for f in files:
    path = os.path.join('$VALDIR/artifacts', f)
    if not os.path.exists(path):
        print(f'SKIP {f}')
        continue
    t = pq.read_table(path)
    assert 'Key' in t.column_names
    assert 'Size' in t.column_names
    assert 'LastModified' in t.column_names
    assert 'ETag' in t.column_names
    assert 'DiffFlag' in t.column_names
    assert t.num_rows > 0
    print(f'{f}: {t.num_rows} rows, schema OK')
"
```

### B.12 Config profile precedence (AWS)

Test that `--config`, `~/.s3-turbo-list.toml`, and `./s3-turbo-list.toml` are loaded correctly:

```bash
# Create a home-dir config
cat > ~/.s3-turbo-list.toml << 'HOME_EOF'
[runtime]
max_concurrency = 999
HOME_EOF

# Run (should load home config)
$BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "/" \
  --max-keys 2 \
  --threads 2 \
  --log 2>&1 | grep -i "loaded config\|concurrency"

# Expected log: "Loaded config from /home/ubuntu/.s3-turbo-list.toml"
#               "concurrency 999"

# Override with --config
cat > "$VALDIR/aws/custom.toml" << 'EOF'
[runtime]
max_concurrency = 7
EOF

$BIN list \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --prefix "/" \
  --max-keys 2 \
  --threads 2 \
  --config "$VALDIR/aws/custom.toml" \
  --log 2>&1 | grep -i "loaded config\|concurrency"

# Expected log: "Loaded config from .../custom.toml"
#               "concurrency 7"

# Cleanup
rm ~/.s3-turbo-list.toml
```

**Expect**: Local config (`--config`) takes priority over home-dir config. CLI overrides take priority over both.

---

## Part C — Baidu BOS (S3-Compatible)

**Prerequisites**: BOS credentials in `~/.aws/credentials` under `[bos]` profile.

### C.1 Verify BOS credentials

```bash
grep -A2 '\[bos\]' ~/.aws/credentials
# Expected: aws_access_key_id and aws_secret_access_key

# Test connectivity using s3-turbo-list compat-probe
$BIN compat-probe \
  --endpoint https://s3.bj.bcebos.com \
  --region bj \
  --bucket "s3tl-validation" \
  --addressing-style path \
  --output "$VALDIR/artifacts/bos-compat-probe.json"

cat "$VALDIR/artifacts/bos-compat-probe.json" | python3 -m json.tool
```

**Expect** (if bucket exists and creds are valid):
- `overall_status: "compatible"` (possibly `"partial"` if some tests fail)
- `HeadBucket` response includes `request_id` (BOS returns `x-bce-request-id`)

**Failure interpretation**:
- `SignatureDoesNotMatch` or `AccessDenied` → BOS credentials invalid. Check SecretAccessKey (BOS keys differ from AWS keys)
- `NoSuchBucket` / 404 → bucket doesn't exist in `bj` region. Create one via BOS console.
- `Connection refused` / timeout → endpoint URL wrong; verify `--endpoint https://s3.bj.bcebos.com`
- If compat-probe succeeds but HeadBucket fails → bucket permissions issue

### C.2 Create BOS test bucket + objects

```bash
# This must be done via BOS console or BOS CLI — AWS CLI may not work with BOS signing
# Once bucket exists:

export BOS_BUCKET="s3tl-validation"  # Use an existing bucket
```

**If you have `bcecmd` (BOS CLI)**:
```bash
# bcecmd bos ls bos:/$BOS_BUCKET/ --region bj
```

Otherwise, use the bucket you already have. Populate it manually or skip data-dependent tests.

### C.3 Compat-probe (BOS)

```bash
cd "$VALDIR/bos"

$BIN compat-probe \
  --endpoint https://s3.bj.bcebos.com \
  --region bj \
  --bucket "$BOS_BUCKET" \
  --addressing-style path \
  --output "$VALDIR/artifacts/bos-compat-probe.json"

python3 -m json.tool "$VALDIR/artifacts/bos-compat-probe.json"
```

**Key checks**:
- `ListObjectsV2 (max-keys=1)` → `"ok"`
- `ListObjectsV2 with prefix` → `"ok"`
- `ListObjectsV2 with delimiter` → `"ok"`
- `ListObjectsV2 (encoding-type=url)` → `"ok"` or `"error"` (BOS encoding-type support varies)
- `ListObjectsV2 pagination check` → `"ok"` or `"skipped"`

**Expect**:
- BOS typically supports all ListObjectsV2 parameters
- `addressing_style` field reports the requested style. For normal BOS usage,
  prefer `"virtual"`; path-style is legacy/diagnostic only.

**Failure interpretation**:
- `encoding-type=url` error → known BOS limitation; document in findings
- `delimiter` error → BOS version too old; upgrade or workaround needed

### C.4 BOS listing with --profile bos

```bash
$BIN list \
  --bucket "$BOS_BUCKET" \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --region bj \
  --prefix "/" \
  --max-keys 1000 \
  --threads 6 --concurrency 20 \
  --output-parquet-file "$VALDIR/artifacts/bos-basic.parquet" \
  --output-ks-file "$VALDIR/artifacts/bos-basic.ks" \
  --log
```

**Expect**:
- Exit code 0
- Log shows `"Applied BOS vendor profile: virtual-hosted addressing, bj endpoint"`
- Parquet rows match bucket contents

### C.5 BOS with --debug-s3 and --trace-compat

```bash
$BIN list \
  --bucket "$BOS_BUCKET" \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --region bj \
  --addressing-style path \
  --prefix "/" \
  --max-keys 5 \
  --threads 4 --concurrency 10 \
  --debug-s3 \
  --trace-compat "$VALDIR/artifacts/bos-trace.jsonl" \
  2>"$VALDIR/artifacts/bos-trace-stderr.jsonl" \
  --output-parquet-file "$VALDIR/artifacts/scratch-bos-trace.parquet"
```

**Verify trace fields**:
```bash
python3 -c "
import json

with open('$VALDIR/artifacts/bos-trace.jsonl') as f:
    lines = [json.loads(l.strip()) for l in f if l.strip()]

print(f'Total trace events: {len(lines)}')

# All events should have profile=bos
profiles = {e.get('profile') for e in lines}
print(f'Profiles: {profiles}')
assert 'bos' in profiles, 'bos profile not in trace events!'

# Addressing style should be path
styles = {e['addressing_style'] for e in lines}
print(f'Addressing styles: {styles}')
assert 'path' in styles, 'path style not in trace events!'

# Check if BOS returns request_id
ids = [e.get('request_id') for e in lines if e.get('request_id')]
print(f'Events with request_id: {len(ids)}/{len(lines)}')

# Check endpoint_url
endpoints = {e['endpoint_url'] for e in lines}
print(f'Endpoint URLs: {endpoints}')
assert 'https://s3.bj.bcebos.com' in endpoints

print('BOS trace validation complete')
"
```

### C.6 truncated_raw_body on error paths (BOS)

Deliberately trigger an error to verify `truncated_raw_body` is populated:

```bash
# List a non-existent bucket
$BIN list \
  --bucket "nonexistent-bucket-$(date +%s)" \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --region bj \
  --addressing-style path \
  --prefix "/" \
  --max-keys 1 \
  --threads 1 --concurrency 1 \
  --no-auto-hints \
  --trace-compat "$VALDIR/artifacts/bos-error-trace.jsonl" \
  --output-parquet-file "$VALDIR/artifacts/scratch-bos-err.parquet" 2>&1 || true

# Check for truncated_raw_body in error events
python3 -c "
import json

with open('$VALDIR/artifacts/bos-error-trace.jsonl') as f:
    for line in f:
        obj = json.loads(line.strip())
        if obj.get('http_status', 0) >= 400:
            print(f'Error event: http={obj[\"http_status\"]}, '
                  f's3_error_code={obj.get(\"s3_error_code\")}, '
                  f'has_raw_body={\"truncated_raw_body\" in obj}')
            if 'truncated_raw_body' in obj:
                body = obj['truncated_raw_body']
                print(f'  truncated_raw_body ({len(body)} chars): {body[:200]}')
            break
"
```

**Expect**:
- Error event has `http_status` 404
- `s3_error_code` is `"NoSuchBucket"` (or BOS equivalent)
- `truncated_raw_body` is present and contains XML (first 512 bytes of error body)
- `fatal: true`
- `retryable: false`

**Also test with invalid credentials**:
```bash
AWS_ACCESS_KEY_ID=invalid AWS_SECRET_ACCESS_KEY=invalid \
$BIN list \
  --bucket "$BOS_BUCKET" \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --region bj \
  --addressing-style path \
  --prefix "/" \
  --max-keys 1 \
  --threads 1 --concurrency 1 \
  --no-auto-hints \
  --trace-compat "$VALDIR/artifacts/bos-auth-error-trace.jsonl" \
  --output-parquet-file "$VALDIR/artifacts/scratch-bos-auth.parquet" 2>&1 || true

python3 -c "
import json
with open('$VALDIR/artifacts/bos-auth-error-trace.jsonl') as f:
    for line in f:
        obj = json.loads(line.strip())
        if obj.get('http_status', 0) >= 400:
            code = obj.get('s3_error_code', 'NONE')
            has_body = 'truncated_raw_body' in obj
            print(f'Auth error: code={code}, has_raw_body={has_body}')
            if has_body:
                print(f'  Body: {obj[\"truncated_raw_body\"][:200]}')
            break
"
```

**Expect**:
- HTTP status 403 (BOS) or similar
- `s3_error_code` is `"SignatureDoesNotMatch"` or `"AccessDenied"`
- `truncated_raw_body` present

### C.7 Profile auto-detection (--profile bos)

```bash
# Test that --profile bos auto-detects the BOS config
$BIN list \
  --bucket "$BOS_BUCKET" \
  --profile bos \
  --region bj \
  --prefix "/" \
  --max-keys 5 \
  --threads 4 --concurrency 10 \
  --log 2>&1 | grep -i "bos\|endpoint\|addressing"

# Expected:
# "Applied BOS vendor profile: virtual-hosted addressing, bj endpoint"
```

**Verify the endpoint_url override from profile preset**:
```bash
# Without explicit --endpoint-url, the preset fills it in
$BIN list \
  --bucket "$BOS_BUCKET" \
  --profile bos \
  --region bj \
  --prefix "/" \
  --max-keys 2 \
  --threads 2 --concurrency 2 \
  --debug-s3 2>&1 | grep '"endpoint_url"' | head -1

# Expected: "endpoint_url":"https://s3.bj.bcebos.com"
```

**Test explicit override of preset**:
```bash
# Explicit --endpoint-url should win over profile preset
$BIN list \
  --bucket "$BOS_BUCKET" \
  --profile bos \
  --region bj \
  --endpoint-url https://custom.example.com \
  --prefix "/" \
  --max-keys 2 \
  --threads 2 --concurrency 2 \
  --debug-s3 2>&1 | grep '"endpoint_url"' | head -1

# Expected: "endpoint_url":"https://custom.example.com"
# This will likely fail (connection refused) — that's expected. We're testing config precedence.
```

### C.8 Checkpoint identity (BOS)

```bash
cd "$VALDIR/bos"

$BIN list \
  --bucket "$BOS_BUCKET" \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --region bj \
  --addressing-style path \
  --prefix "/" \
  --max-keys 5 \
  --threads 4 --concurrency 10 \
  --resume \
  --log

CHECKPOINT_FILE="bj_${BOS_BUCKET}_checkpoint.toml"
cat "$CHECKPOINT_FILE" 2>/dev/null

# Verify identity shows bos profile
python3 -c "
import toml
with open('$CHECKPOINT_FILE') as f:
    ckpt = toml.load(f)
identity = ckpt.get('identity', {})
print(f'profile: {identity.get(\"profile\")}')
print(f'addressing_style: {identity.get(\"addressing_style\")}')
print(f'bucket: {identity.get(\"bucket\")}')
print(f'region: {identity.get(\"region\")}')
# Expect: profile=bos, addressing_style=virtual, bucket=$BOS_BUCKET, region=bj
"
```

### C.9 BOS path-style diagnostic mode

BOS supports virtual-hosted addressing for normal usage. Path-style remains
available for legacy or diagnostic checks and must be requested explicitly:

```bash
# Explicit path-style diagnostic run
$BIN list \
  --bucket "$BOS_BUCKET" \
  --profile bos \
  --endpoint-url https://s3.bj.bcebos.com \
  --region bj \
  --addressing-style path \
  --prefix "/" \
  --max-keys 2 \
  --threads 2 --concurrency 2 \
  --trace-compat "$VALDIR/artifacts/bos-virtual-fail.jsonl" \
  --output-parquet-file "$VALDIR/artifacts/scratch-bos-virt.parquet" 2>&1 || true

python3 -c "
import json
with open('$VALDIR/artifacts/bos-virtual-fail.jsonl') as f:
    for line in f:
        obj = json.loads(line.strip())
        if obj.get('http_status', 0) >= 400:
            print(f'virtual-hosted failed: http={obj[\"http_status\"]}, '
                  f'code={obj.get(\"s3_error_code\")}, '
                  f'addressing_style={obj[\"addressing_style\"]}')
            break
    else:
        print('WARNING: virtual-hosted style succeeded against BOS (unexpected)')
"
```

**Expect**: Virtual-hosted style should fail against BOS (404, 301, or connection error). This confirms `--addressing-style path` is required.

---

## Part D — Cross-Endpoint Summary

### D.1 Collect all artifacts

```bash
echo "=== Artifacts collected ==="
find "$VALDIR/artifacts" -type f -ls

echo ""
echo "=== Compat-probe summaries ==="
for f in "$VALDIR/artifacts"/*compat-probe.json; do
  echo "--- $(basename $f) ---"
  python3 -c "
import json
with open('$f') as fh:
    r = json.load(fh)
print(f'  Status: {r[\"overall_status\"]}')
for t in r['tests']:
    print(f'  [{t[\"status\"]:8s}] {t[\"test\"]} ({t[\"latency_ms\"]}ms)')
print()
" 2>/dev/null
done
```

### D.2 Trace event coverage matrix

Verify these fields are populated across all three endpoints:

| Field | MinIO | AWS S3 | BOS |
|---|---|---|---|
| `timestamp` | ✓ | ✓ | ✓ |
| `operation` | ✓ | ✓ | ✓ |
| `profile` | ✓ (minio) | ✓ (aws) | ✓ (bos) |
| `endpoint_url` | ✓ | ✓ | ✓ |
| `region` | ✓ | ✓ | ✓ |
| `addressing_style` | ✓ | ✓ | ✓ |
| `bucket` | ✓ | ✓ | ✓ |
| `prefix` | ✓ | ✓ | ✓ |
| `delimiter` | ✓ (when set) | ✓ (when set) | ✓ (when set) |
| `start_after` | ✓ | ✓ | ✓ |
| `max_keys` | ✓ (when set) | ✓ (when set) | ✓ (when set) |
| `http_status` | ✓ | ✓ | ✓ |
| `s3_error_code` | ✓ (errors) | ✓ (errors) | ✓ (errors) |
| `truncated_raw_body` | ✓ (errors) | ✓ (errors) | ✓ (errors) |
| `request_id` | varies | ✓ | varies |
| `retry_attempt` | ✓ | ✓ | ✓ |
| `latency_ms` | ✓ | ✓ | ✓ |
| `is_truncated` | ✓ | ✓ | ✓ |
| `next_continuation_token` | ✓ (paginated) | ✓ (paginated) | ✓ (paginated) |
| `next_continuation_token_present` | ✓ | ✓ | ✓ |
| `key_count` | ✓ | ✓ | ✓ |
| `contents_count` | ✓ | ✓ | ✓ |
| `common_prefixes_count` | ✓ | ✓ | ✓ |

**Quick verification script**:
```bash
python3 -c "
import json, os, glob

for f in sorted(glob.glob('$VALDIR/artifacts/*trace*.jsonl')):
    print(f'--- {os.path.basename(f)} ---')
    with open(f) as fh:
        first = json.loads(fh.readline().strip())
    fields = sorted(first.keys())
    missing = [c for c in ['profile', 'request_id', 'delimiter', 'max_keys',
                             'next_continuation_token_present', 'key_count',
                             'contents_count', 'common_prefixes_count']
               if c not in fields]
    print(f'  Fields: {len(fields)}, Missing expected: {missing or \"none\"}')
    print(f'  addressing_style: {first.get(\"addressing_style\")}')
    print(f'  profile: {first.get(\"profile\")}')
    print()
"
```

### D.3 Log file collection

```bash
# Collect all log files from the validation run
find "$VALDIR" -name "turbo_list_*.log" -exec cp {} "$VALDIR/artifacts/" \;
ls -la "$VALDIR/artifacts/"*.log 2>/dev/null

# Gather key stats from logs
for log in "$VALDIR/artifacts"/*.log; do
  echo "=== $(basename $log) ==="
  grep -E "completed|error|fatal|checkpoint|resum|diff mode" "$log" | head -20
  echo ""
done
```

---

## E — Failure Interpretation Reference

### Exit codes and symptoms

| Symptom | Probable cause | Debug step |
|---|---|---|
| Exit 0 but 0 objects in parquet | Wrong bucket/prefix, filter excludes all | Run with `--prefix "/"` and check log for "Flat List S3 Task ... completed" |
| Exit 0, "all segments completed" but stale data | Checkpoint from different run matched | Wipe checkpoint file and re-run |
| `DispatchFailure: region must be set` | `--region` missing | Always pass `--region` for non-AWS endpoints |
| `Connection refused` on MinIO | MinIO not running | `curl localhost:9000` |
| `SignatureDoesNotMatch` on BOS | BOS uses HMAC-SHA256; verify correct AccessKey/SecretKey | Check `~/.aws/credentials [bos]` section |
| `NoSuchBucket` on any endpoint | Bucket doesn't exist or wrong region | `aws s3 ls s3://bucketname --region X` |
| Trace file empty | `--trace-compat` path not writable | Check permissions; add `/tmp/` prefix |
| `--debug-s3` produces no output | `RUST_LOG` env var suppressing stderr | Set `RUST_LOG=s3_turbo_list=info` |
| Parquet corruption on read | Incomplete write (crash before close) | Check if `data_map task` completed in log |
| Checkpoint identity mismatch silently accepted | `--resume` with same options but different semantic meaning | Inspect checkpoint TOML identity block before re-running |

### Trace field interpretations

| Field | Normal | Anomalous |
|---|---|---|
| `is_truncated` | true on intermediate pages, false on last | Always false → pagination broken, check max_keys vs actual |
| `next_continuation_token_present` | Same as `is_truncated` | true when is_truncated false → provider bug |
| `contents_count` | > 0 for pages with objects | Always 0 → bucket empty or prefix mismatch |
| `common_prefixes_count` | > 0 when delimiter set and hierarchy exists | Always 0 even with hierarchy → delimiter not being sent |
| `truncated_raw_body` | Only on error events (http_status >= 400) | Present on 200 → bug in emit_trace_compat condition |
| `request_id` | Present on AWS; may be absent on MinIO/BOS | Absent on AWS → possible connection issue or SDK version |

---

## F — Cleanup

```bash
# Remove test buckets (optional — only if created for validation)
# aws s3 rm "s3://$BUCKET" --recursive --region "$REGION"
# aws s3 rb "s3://$BUCKET" --region "$REGION"
# aws s3 rm "s3://$TARGET_BKT" --recursive --region "$REGION"
# aws s3 rb "s3://$TARGET_BKT" --region "$REGION"

# Remove home-dir config if created
rm -f ~/.s3-turbo-list.toml

# Clean validation data
# rm -rf "$VALDIR"

# Clean checkpoint files
rm -f ./*_checkpoint.toml

# Remove scratch parquet files (disposable outputs from trace-only tests)
rm -f "$VALDIR/artifacts"/scratch-*.parquet

echo "Validation artifacts preserved at: $VALDIR/artifacts"
```

---

## Validation Sign-Off

- [ ] MinIO compat-probe → `compatible`
- [ ] MinIO basic list → correct row count in Parquet
- [ ] MinIO prefix/delimiter/max_keys/start_after → correct filtering
- [ ] MinIO encoding-type → special chars decoded
- [ ] MinIO --debug-s3 → valid JSONL on stderr
- [ ] MinIO --trace-compat → valid JSONL file
- [ ] MinIO path-style → `"addressing_style":"path"` in trace
- [ ] MinIO continuation token pagination → multiple pages for small max_keys
- [ ] MinIO Parquet output → valid schema, all columns, correct types
- [ ] MinIO checkpoint save/load → identity verified
- [ ] MinIO checkpoint mismatch → correctly rejected with warning
- [ ] AWS compat-probe → `compatible`
- [ ] AWS basic list → correct row count
- [ ] AWS --trace-compat → `"addressing_style":"virtual"`, request_id present
- [ ] AWS prefix/delimiter/max_keys → correct filtering
- [ ] AWS encoding-type → special chars handled
- [ ] AWS diff mode → DiffFlag 0/1/2/3 present appropriately
- [ ] AWS checkpoint/resume → works with identity verification
- [ ] AWS config profile precedence → `--config` wins over home-dir, CLI overrides both
- [ ] BOS compat-probe → `compatible` or `partial`
- [ ] BOS basic list → correct row count
- [ ] BOS --profile bos → auto-detected endpoint/addr-style
- [ ] BOS --trace-compat → `"profile":"bos"`, `"addressing_style":"virtual"`, `"endpoint_url":"https://s3.bj.bcebos.com"`
- [ ] BOS truncated_raw_body on error → present, XML content, ≤ 512 bytes
- [ ] BOS truncated_raw_body on auth error → present, error code matches
- [ ] BOS explicit path-style diagnostic run → `"addressing_style":"path"`
- [ ] BOS checkpoint identity → profile=bos, addressing_style=virtual in identity block
- [ ] All Parquet outputs valid (schema, types, non-zero rows)
- [ ] Log files collected, key events confirmed
