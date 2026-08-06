// The diff filter must reach every row, not just the both-sides ones.
//
// One-sided rows (`+`/`-`) used to be pushed straight to the writer without
// consulting `--filter` at all, so a filtered diff still emitted every
// left-only and right-only object regardless of the predicate — and
// `ignored` only ever counted rejected *pairs*, so the counters gave no
// sign that the filter had been applied to a fraction of the output.
//
// `OBJECT_FILTER` is a process-wide `OnceLock`, so this file installs
// exactly one expression and lives in its own test binary. The
// TARGET-referencing half of the contract — a predicate that cannot be
// evaluated for a one-sided row keeps the row — is covered by the unit
// tests in `src/core.rs`, which need no global.

use s3_turbo_list::core::{
    ObjectKey, ObjectProps, RunMode, S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
    S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
};
use s3_turbo_list::data_map::{run_diff_merge, DiffMergeOutcome, DiffStreamSides};
use s3_turbo_list::utils::AsyncParquetOutput;

type Batch = Vec<(ObjectKey, ObjectProps)>;

/// Objects at or below this size are excluded by the installed filter.
const CUTOFF: u64 = 1000;

fn install() {
    // Idempotent across the tests in this binary; the second call is a no-op
    // error because the OnceLock is already set with the same expression.
    let _ = s3_turbo_list::config::install_filter("SOURCE.size > 1000", &RunMode::BiDir);
}

fn left_obj(key: &str, size: u64, etag: [u8; 16]) -> (ObjectKey, ObjectProps) {
    (
        ObjectKey::from(key),
        ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, size, etag),
    )
}

fn right_obj(key: &str, size: u64, etag: [u8; 16]) -> (ObjectKey, ObjectProps) {
    (
        ObjectKey::from(key),
        ObjectProps::new_open(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE, size, etag),
    )
}

async fn merge(
    left_batches: Vec<Batch>,
    right_batches: Vec<Batch>,
) -> Result<(DiffMergeOutcome, Vec<(String, u8)>), String> {
    let (ltx, lrx) = tokio::sync::mpsc::channel(8);
    let (rtx, rrx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        for batch in left_batches {
            if ltx.send(batch).await.is_err() {
                return;
            }
        }
    });
    tokio::spawn(async move {
        for batch in right_batches {
            if rtx.send(batch).await.is_err() {
                return;
            }
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("diff.parquet");
    let file = tokio::fs::File::create(&parquet_path).await.unwrap();
    let writer = tokio::io::BufWriter::new(file);
    let mut parquet = AsyncParquetOutput::new(writer, "unused.ks");
    let outcome = run_diff_merge(
        DiffStreamSides {
            left: vec![lrx],
            right: vec![rrx],
        },
        &mut parquet,
        || false,
    )
    .await?;
    parquet.close().await.unwrap();

    let std_file = std::fs::File::open(&parquet_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std_file)
        .unwrap()
        .build()
        .unwrap();
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let flags = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((keys.value(i).to_string(), flags.value(i)));
        }
    }
    rows.sort();
    Ok((outcome, rows))
}

#[tokio::test]
async fn test_filter_excludes_small_one_sided_rows() {
    install();

    let left = vec![vec![
        left_obj("a/big_left_only.txt", CUTOFF + 1, [1; 16]),
        left_obj("b/small_left_only.txt", CUTOFF - 1, [2; 16]),
    ]];
    let right = vec![vec![
        right_obj("c/big_right_only.txt", CUTOFF + 1, [3; 16]),
        right_obj("d/small_right_only.txt", CUTOFF - 1, [4; 16]),
    ]];

    let (outcome, rows) = merge(left, right).await.unwrap();

    // Both small one-sided objects are excluded, on both sides.
    assert_eq!(outcome.plus, 1, "one left-only row survives the filter");
    assert_eq!(outcome.minus, 1, "one right-only row survives the filter");
    assert_eq!(outcome.rows, 2);
    assert_eq!(
        outcome.ignored, 2,
        "excluded one-sided rows are counted, not silently dropped"
    );
    assert_eq!(
        rows,
        vec![
            ("a/big_left_only.txt".to_string(), 1),
            ("c/big_right_only.txt".to_string(), 2),
        ]
    );
}

#[tokio::test]
async fn test_filter_applies_consistently_to_one_sided_and_paired_rows() {
    install();

    // Same size on every object, all below the cutoff: nothing at all should
    // reach the output, whether it is one-sided or present on both sides.
    let left = vec![vec![
        left_obj("a/pair.txt", CUTOFF - 1, [1; 16]),
        left_obj("b/left_only.txt", CUTOFF - 1, [2; 16]),
    ]];
    let right = vec![vec![
        right_obj("a/pair.txt", CUTOFF - 1, [9; 16]),
        right_obj("c/right_only.txt", CUTOFF - 1, [3; 16]),
    ]];

    let (outcome, rows) = merge(left, right).await.unwrap();

    assert_eq!(outcome.rows, 0, "the filter excludes every row");
    assert_eq!(outcome.plus, 0);
    assert_eq!(outcome.minus, 0);
    assert_eq!(outcome.astrisk, 0);
    assert_eq!(outcome.ignored, 3, "one pair plus two one-sided rows");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_filter_keeps_large_rows_of_every_flag() {
    install();

    let left = vec![vec![
        left_obj("a/equal.txt", CUTOFF + 1, [1; 16]),
        left_obj("b/changed.txt", CUTOFF + 1, [2; 16]),
        left_obj("c/left_only.txt", CUTOFF + 1, [3; 16]),
    ]];
    let right = vec![vec![
        right_obj("a/equal.txt", CUTOFF + 1, [1; 16]),
        right_obj("b/changed.txt", CUTOFF + 1, [9; 16]),
        right_obj("d/right_only.txt", CUTOFF + 1, [4; 16]),
    ]];

    let (outcome, rows) = merge(left, right).await.unwrap();

    assert_eq!(outcome.equal, 1);
    assert_eq!(outcome.astrisk, 1);
    assert_eq!(outcome.plus, 1);
    assert_eq!(outcome.minus, 1);
    assert_eq!(outcome.ignored, 0);
    assert_eq!(
        rows,
        vec![
            ("a/equal.txt".to_string(), 0),
            ("b/changed.txt".to_string(), 3),
            ("c/left_only.txt".to_string(), 1),
            ("d/right_only.txt".to_string(), 2),
        ]
    );
}
