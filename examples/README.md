# s3tl examples

Templates showing one invocation pattern each; adapt them to your
environment. No script deletes anything or embeds credentials; S3 URLs come
from environment variables. Scripts other than the Python readers contact S3.

| File | Purpose |
|---|---|
| `checkpoint-resume.sh` | Interrupt a listing, then `--resume` it into the same dataset |
| `diff-basic.sh` | Diff two prefixes into a Parquet dataset |
| `read-parquet.py` | Summarize a dataset directory with pandas (`pip install pandas pyarrow`) |
| `inspect-trace.py` | Summarize a `--trace` JSONL file |
| `bench_local.rs` | Output-pipeline benchmark, no S3 (`cargo run --release --example bench_local -- parquet`) |

Scripts use `S3TL` for the binary (default `cargo run --release --`) and
`jq` to read the result. A trace for debugging an endpoint:

```bash
s3tl list s3://my-bucket/ -o out/ --trace trace.jsonl -v
python3 examples/inspect-trace.py trace.jsonl
```
