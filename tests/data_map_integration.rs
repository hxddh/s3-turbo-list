// Integration tests for the streaming diff merge (data_map::run_diff_merge):
// classification, ordering guard, KS output, and Parquet DiffFlag content.

use s3_turbo_list::core::{
    ObjectKey, ObjectProps, S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
};
use s3_turbo_list::data_map::{run_diff_merge, DiffMergeOutcome, DiffStreamSides};
use s3_turbo_list::utils::AsyncParquetOutput;

type Batch = Vec<(ObjectKey, ObjectProps)>;

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

/// Run the merge over in-memory batches; returns the outcome plus the
/// (key, DiffFlag) rows read back from the Parquet bytes.
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
async fn test_merge_classifies_all_diff_flags() {
    let left = vec![vec![
        left_obj("a/equal.txt", 100, [1; 16]),
        left_obj("a/left_only.txt", 100, [2; 16]),
        left_obj("b/changed_etag.txt", 100, [3; 16]),
        left_obj("b/changed_size.txt", 100, [4; 16]),
    ]];
    let right = vec![vec![
        right_obj("a/equal.txt", 100, [1; 16]),
        right_obj("b/changed_etag.txt", 100, [9; 16]),
        right_obj("b/changed_size.txt", 222, [4; 16]),
        right_obj("c/right_only.txt", 100, [5; 16]),
    ]];

    let (outcome, rows) = merge(left, right).await.unwrap();
    assert_eq!(outcome.equal, 1);
    assert_eq!(outcome.plus, 1);
    assert_eq!(outcome.minus, 1);
    assert_eq!(outcome.astrisk, 2);
    assert_eq!(outcome.rows, 5);
    assert_eq!(outcome.received_objects, 8);
    assert_eq!(outcome.unique_prefixes(), 3);

    assert_eq!(
        rows,
        vec![
            ("a/equal.txt".to_string(), 0),
            ("a/left_only.txt".to_string(), 1),
            ("b/changed_etag.txt".to_string(), 3),
            ("b/changed_size.txt".to_string(), 3),
            ("c/right_only.txt".to_string(), 2),
        ]
    );
}

#[tokio::test]
async fn test_merge_unavailable_etag_is_astrisk() {
    // All-zero ETags cannot verify equality even when sizes match.
    let left = vec![vec![left_obj("k.txt", 100, [0; 16])]];
    let right = vec![vec![right_obj("k.txt", 100, [0; 16])]];
    let (outcome, rows) = merge(left, right).await.unwrap();
    assert_eq!(outcome.astrisk, 1);
    assert_eq!(rows, vec![("k.txt".to_string(), 3)]);
}

#[tokio::test]
async fn test_merge_handles_uneven_batches_and_one_empty_side() {
    // Keys split across many small batches on one side, none on the other.
    let left = vec![
        vec![left_obj("a", 1, [1; 16])],
        vec![left_obj("b", 1, [1; 16]), left_obj("c", 1, [1; 16])],
        vec![left_obj("d", 1, [1; 16])],
    ];
    let (outcome, rows) = merge(left, vec![]).await.unwrap();
    assert_eq!(outcome.plus, 4);
    assert_eq!(outcome.rows, 4);
    assert!(rows.iter().all(|(_, flag)| *flag == 1));
}

#[tokio::test]
async fn test_merge_empty_both_sides() {
    let (outcome, rows) = merge(vec![], vec![]).await.unwrap();
    assert_eq!(outcome.rows, 0);
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_merge_rejects_out_of_order_side() {
    let left = vec![vec![
        left_obj("b.txt", 1, [1; 16]),
        left_obj("a.txt", 1, [1; 16]),
    ]];
    let err = merge(left, vec![]).await.unwrap_err();
    assert!(err.contains("out of order"), "{}", err);
}

#[tokio::test]
async fn test_merge_same_side_duplicate_keeps_latest() {
    let left = vec![vec![
        left_obj("dup.txt", 1, [1; 16]),
        left_obj("dup.txt", 2, [2; 16]),
    ]];
    let right = vec![vec![right_obj("dup.txt", 2, [2; 16])]];
    let (outcome, rows) = merge(left, right).await.unwrap();
    assert_eq!(outcome.equal, 1, "latest duplicate entry should win");
    assert_eq!(rows, vec![("dup.txt".to_string(), 0)]);
}

#[tokio::test]
async fn test_merge_writes_ks_counts() {
    let dir = tempfile::tempdir().unwrap();
    let ks_path = dir.path().join("diff.ks");

    let left = vec![vec![
        left_obj("a/1", 1, [1; 16]),
        left_obj("a/2", 1, [1; 16]),
        left_obj("b/1", 1, [1; 16]),
    ]];
    let (outcome, _rows) = merge(left, vec![]).await.unwrap();
    let entries = outcome.write_ks(ks_path.to_str().unwrap()).await.unwrap();
    assert_eq!(entries, 2);
    let content = std::fs::read_to_string(&ks_path).unwrap();
    assert_eq!(content, "\"a\",\"2\"\n\"b\",\"1\"\n");
}
