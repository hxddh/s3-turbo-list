# Benchmarking

v0.1.8 adds a local synthetic benchmark harness for the list-mode streaming
output path.  It generates in-memory object batches and writes Parquet plus KS
outputs through the normal data-map streaming code.  It does not contact S3.

```bash
cargo build --release
./target/release/s3-turbo-list benchmark-local \
  --objects 100000 \
  --batch-size 5000 \
  --prefixes 512 \
  --json
```

Or use the wrapper:

```bash
./scripts/benchmark-local.sh
```

Environment overrides:

```bash
OBJECTS=1000000 BATCH_SIZE=10000 PREFIXES=1024 ./scripts/benchmark-local.sh
```

The JSON report includes:

- tool version
- object count, batch size, and prefix count
- elapsed seconds and objects/sec
- Parquet and KS byte sizes
- data-map metrics: received batches, received objects, streamed rows,
  unique prefixes, Parquet rows, and KS entries

Real endpoint benchmarks remain intentionally opt-in.  Do not use benchmark
scripts against AWS, BOS, R2, B2, OSS, or other cloud endpoints unless the run
has been explicitly authorized.

## Compression Notes

The default Parquet compression is gzip because it is broadly supported and
compresses well, but it can cost more CPU on large streaming outputs.  For local
analysis pipelines, consider `zstd` for a better speed/ratio balance or `snappy`
when write speed and downstream compatibility matter more than compression
ratio:

```toml
[output]
compression = "zstd"
compression_level = 3
```

Compression choice affects local CPU time and output size only; it does not
change S3 request behavior.
