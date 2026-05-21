// Integration tests for data_map aggregation and Parquet output pipeline.
#![allow(clippy::io_other_error)]

use arrow::array::RecordBatchReader;
use parquet::basic::Compression;
use parquet::file::reader::{FileReader, SerializedFileReader};
use s3_turbo_list::agent::{self, OutputPathSummary};
use s3_turbo_list::config::OutputConfig;
use s3_turbo_list::core::{
    DataMapContext, GlobalState, ObjectKey, ObjectProps, S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
    S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
};
use s3_turbo_list::data_map::{ObjectMap, PrefixMap};
use s3_turbo_list::utils::AsyncParquetOutput;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

const EXPECTED_SCHEMA: [&str; 5] = ["Key", "Size", "LastModified", "ETag", "DiffFlag"];

struct FailingAsyncWrite;

impl AsyncWrite for FailingAsyncWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "intentional write failure",
        )))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "intentional flush failure",
        )))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "intentional shutdown failure",
        )))
    }
}

// ── Single prefix, single object ──────────────────────────

#[test]
fn test_data_map_single_prefix_single_object() {
    let map = PrefixMap::new();
    let key = ObjectKey::from("logs/app.log");
    let (prefix, name) = key.decode();

    let mut props = ObjectProps::default();
    props.size = 42;
    props.last_modified = 1000;

    map.bulk_insert(&prefix, vec![(name, props)]);

    let (prefix_count, obj_count) = map.get_stats();
    assert_eq!(prefix_count, 1);
    assert_eq!(obj_count, 1);
}

// ── Multi-prefix ──────────────────────────────────────────

#[test]
fn test_data_map_multi_prefix() {
    let map = PrefixMap::new();

    let prefixes = ["logs/", "data/", "tmp/"];
    for (i, pfx) in prefixes.iter().enumerate() {
        let key = ObjectKey::from(format!("{}file{}.txt", pfx, i).as_str());
        let (prefix, name) = key.decode();

        let mut props = ObjectProps::default();
        props.size = (i as u64) * 100;

        map.bulk_insert(&prefix, vec![(name, props)]);
    }

    let (prefix_count, obj_count) = map.get_stats();
    assert_eq!(prefix_count, 3);
    assert_eq!(obj_count, 3);

    // Add a second object to one prefix.
    let key2 = ObjectKey::from("logs/app2.log");
    let (prefix2, name2) = key2.decode();
    let props2 = ObjectProps::default();
    map.bulk_insert(&prefix2, vec![(name2, props2)]);

    let (prefix_count2, obj_count2) = map.get_stats();
    assert_eq!(prefix_count2, 3);
    assert_eq!(obj_count2, 4);
}

// ── Diff flags ────────────────────────────────────────────

#[test]
fn test_data_map_diff_flags() {
    let map = PrefixMap::new();

    // Left-only object → should yield MatchResult::Plus → DiffFlag=1
    {
        let key = ObjectKey::from("common/left_only.txt");
        let (prefix, name) = key.decode();
        let mut props = ObjectProps::default();
        props.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        props.size = 100;
        map.bulk_insert(&prefix, vec![(name.clone(), props)]);
    }

    // Right-only object → should yield MatchResult::Minus → DiffFlag=2
    {
        let key = ObjectKey::from("common/right_only.txt");
        let (prefix, name) = key.decode();
        let mut props = ObjectProps::default();
        props.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        props.size = 200;
        map.bulk_insert(&prefix, vec![(name.clone(), props)]);
    }

    // Matching object (left then right) → MatchResult::Equal → DiffFlag=0
    {
        let key = ObjectKey::from("common/same.txt");
        let (prefix, name) = key.decode();
        let mut left = ObjectProps::default();
        left.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        left.size = 300;
        map.bulk_insert(&prefix, vec![(name.clone(), left)]);

        let mut right = ObjectProps::default();
        right.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        right.size = 300;
        map.bulk_insert(&prefix, vec![(name.clone(), right)]);
    }

    // Mismatching object (different sizes) → MatchResult::Astrisk → DiffFlag=3
    {
        let key = ObjectKey::from("common/diff.txt");
        let (prefix, name) = key.decode();
        let mut left = ObjectProps::default();
        left.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        left.size = 400;
        map.bulk_insert(&prefix, vec![(name.clone(), left)]);

        let mut right = ObjectProps::default();
        right.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        right.size = 500;
        map.bulk_insert(&prefix, vec![(name.clone(), right)]);
    }

    // Verify counts.
    let (prefix_count, obj_count) = map.get_stats();
    assert_eq!(prefix_count, 1);
    assert_eq!(obj_count, 4);
}

// ── ObjectMap dedup / matching ────────────────────────────

#[test]
fn test_object_map_dedup_same_side() {
    let obj_map = ObjectMap::new();

    // Insert first version.
    let mut first = ObjectProps::default();
    first.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
    first.size = 100;
    obj_map.bulk_insert("p", vec![("obj.txt".to_string(), first)]);

    assert_eq!(obj_map.get_count(), 1);

    // Insert duplicate from same side — should overwrite, not increase count.
    let mut second = ObjectProps::default();
    second.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
    second.size = 200;
    obj_map.bulk_insert("p", vec![("obj.txt".to_string(), second)]);

    // Count stays at 1 (deduped).
    assert_eq!(obj_map.get_count(), 1);
}

// ── Diff mode output includes Equal rows ──────────────────

#[tokio::test]
async fn test_diff_mode_includes_equal_rows() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("diff_output.parquet");
    let ks_path = dir.path().join("diff_ks.csv");

    let map = PrefixMap::new();

    // Equal object (left then right, same size and etag) → DiffFlag=0
    {
        let key = ObjectKey::from("common/same.txt");
        let (prefix, name) = key.decode();
        let etag = [1u8; 16];
        let left = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, 300, etag);
        map.bulk_insert(&prefix, vec![(name.clone(), left)]);
        let right = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE, 300, etag);
        map.bulk_insert(&prefix, vec![(name.clone(), right)]);
    }

    // Left-only object → DiffFlag=1
    {
        let key = ObjectKey::from("common/left_only.txt");
        let (prefix, name) = key.decode();
        let props = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, 100, [1u8; 16]);
        map.bulk_insert(&prefix, vec![(name, props)]);
    }

    // Right-only object → DiffFlag=2
    {
        let key = ObjectKey::from("common/right_only.txt");
        let (prefix, name) = key.decode();
        let props = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE, 200, [1u8; 16]);
        map.bulk_insert(&prefix, vec![(name, props)]);
    }

    // Dump with include_equal = true (diff mode).
    {
        let file = tokio::fs::File::create(&parquet_path).await.unwrap();
        let buf_writer = tokio::io::BufWriter::new(file);
        let mut writer = AsyncParquetOutput::new(buf_writer, ks_path.to_str().unwrap());
        map.dump(&mut writer, true).await.unwrap();
        writer.close().await.unwrap();
    }

    // Read back and verify DiffFlag distribution.
    let std_file = std::fs::File::open(&parquet_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std_file)
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.collect();
    let total_rows: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "expected 3 rows (equal + left-only + right-only)"
    );

    let mut flags_seen = std::collections::HashSet::new();
    for batch in &batches {
        let batch = batch.as_ref().unwrap();
        let col = batch
            .column(4) // DiffFlag
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        for i in 0..col.len() {
            let flag = col.value(i);
            eprintln!("  row {}: DiffFlag={}", i, flag);
            flags_seen.insert(flag);
        }
    }
    eprintln!("Flags seen: {:?}", flags_seen);
    assert!(
        flags_seen.contains(&0),
        "DiffFlag=0 (Equal) missing from output"
    );
    assert!(
        flags_seen.contains(&1),
        "DiffFlag=1 (Left-only) missing from output"
    );
    assert!(
        flags_seen.contains(&2),
        "DiffFlag=2 (Right-only) missing from output"
    );
}

// ── List streaming output uses DiffFlag=0 and writes KS counts ─────────────

#[tokio::test]
async fn test_list_streaming_writes_equal_diff_flag_and_ks() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("list_streaming.parquet");
    let ks_path = dir.path().join("list_streaming.ks");

    let quit = Arc::new(AtomicBool::new(false));
    let g_state = GlobalState::new(quit, 1);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let ctx = DataMapContext::new(rx, g_state);

    let parquet_path_for_task = parquet_path.to_str().unwrap().to_string();
    let ks_path_for_task = ks_path.to_str().unwrap().to_string();
    let task = tokio::spawn(async move {
        s3_turbo_list::data_map::data_map_task_list_streaming(
            ctx,
            &ks_path_for_task,
            &parquet_path_for_task,
            OutputConfig {
                row_group_size: 1,
                ..OutputConfig::default()
            },
        )
        .await;
    });

    let mut first = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 100, [1u8; 16]);
    first.last_modified = 1;
    let mut second = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 200, [2u8; 16]);
    second.last_modified = 2;
    tx.send(vec![
        (ObjectKey::from("logs/a.txt"), first),
        (ObjectKey::from("logs/b.txt"), second),
    ])
    .await
    .unwrap();
    drop(tx);
    task.await.unwrap();

    let std_file = std::fs::File::open(&parquet_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std_file)
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.collect();
    let total_rows: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
    assert_eq!(total_rows, 2);

    for batch in &batches {
        let batch = batch.as_ref().unwrap();
        let col = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        for i in 0..col.len() {
            assert_eq!(col.value(i), 0, "list streaming DiffFlag must be 0");
        }
    }

    let ks = std::fs::read_to_string(&ks_path).unwrap();
    assert_eq!(ks, "\"logs\",\"2\"\n");
}

#[tokio::test]
async fn test_list_streaming_handles_empty_batch_and_sorts_ks() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("list_streaming_sorted.parquet");
    let ks_path = dir.path().join("list_streaming_sorted.ks");

    let quit = Arc::new(AtomicBool::new(false));
    let g_state = GlobalState::new(quit, 1);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let ctx = DataMapContext::new(rx, g_state);

    let parquet_path_for_task = parquet_path.to_str().unwrap().to_string();
    let ks_path_for_task = ks_path.to_str().unwrap().to_string();
    let task = tokio::spawn(async move {
        s3_turbo_list::data_map::data_map_task_list_streaming(
            ctx,
            &ks_path_for_task,
            &parquet_path_for_task,
            OutputConfig::default(),
        )
        .await;
    });

    tx.send(Vec::new()).await.unwrap();
    tx.send(vec![
        (
            ObjectKey::from("zeta/file.txt"),
            ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 10, [3u8; 16]),
        ),
        (
            ObjectKey::from("alpha/file.txt"),
            ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 20, [4u8; 16]),
        ),
    ])
    .await
    .unwrap();
    drop(tx);
    task.await.unwrap();

    let std_file = std::fs::File::open(&parquet_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std_file)
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.collect();
    let total_rows: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
    assert_eq!(total_rows, 2);

    let ks = std::fs::read_to_string(&ks_path).unwrap();
    assert_eq!(ks, "\"alpha\",\"1\"\n\"zeta\",\"1\"\n");
}

#[tokio::test]
async fn test_list_streaming_synthetic_dataset_consistency_and_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("synthetic.parquet");
    let ks_path = dir.path().join("synthetic.ks");

    let quit = Arc::new(AtomicBool::new(false));
    let g_state = GlobalState::new(quit, 1);
    let (tx, rx) = tokio::sync::mpsc::channel(128);
    let ctx = DataMapContext::new(rx, g_state.clone());

    let prefixes = ["alpha", "beta", "gamma", "omega"];
    let mut expected_prefix_counts = std::collections::BTreeMap::new();
    let total = 20_000usize;
    let batch_size = 257usize;

    let mut batch = Vec::with_capacity(batch_size);
    for i in 0..total {
        let prefix = prefixes[i % prefixes.len()];
        *expected_prefix_counts
            .entry(prefix.to_string())
            .or_insert(0usize) += 1;
        let mut props =
            ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, i as u64, [7u8; 16]);
        props.last_modified = 1_715_700_000 + i as u64;
        batch.push((
            ObjectKey::from(format!("{}/object-{:06}.dat", prefix, i).as_str()),
            props,
        ));
        if batch.len() == batch_size {
            tx.send(std::mem::take(&mut batch)).await.unwrap();
        }
    }
    if !batch.is_empty() {
        tx.send(batch).await.unwrap();
    }
    drop(tx);

    let parquet_path_for_task = parquet_path.to_str().unwrap().to_string();
    let ks_path_for_task = ks_path.to_str().unwrap().to_string();
    let task = tokio::spawn(async move {
        s3_turbo_list::data_map::data_map_task_list_streaming(
            ctx,
            &ks_path_for_task,
            &parquet_path_for_task,
            OutputConfig {
                row_group_size: 512,
                compression: "snappy".to_string(),
                ..OutputConfig::default()
            },
        )
        .await;
    });
    task.await.unwrap();

    assert_list_output_consistency(&parquet_path, &ks_path, total, &expected_prefix_counts);

    let metrics = g_state.metrics_snapshot();
    assert_eq!(metrics.data_received_objects, total);
    assert_eq!(metrics.data_streamed_rows, total);
    assert_eq!(metrics.data_parquet_rows, total);
    assert_eq!(metrics.data_ks_entries, prefixes.len());
    assert_eq!(metrics.data_bytes_total, (0..total as u64).sum::<u64>());
    assert_eq!(metrics.data_top_prefixes.len(), prefixes.len());
    assert_eq!(metrics.data_top_prefixes[0].objects, total / prefixes.len());
    assert!(!metrics.data_summary_only);

    let outputs = OutputPathSummary {
        parquet_file: Some(parquet_path.to_str().unwrap().to_string()),
        ks_file: Some(ks_path.to_str().unwrap().to_string()),
        hints_file: None,
        trace_compat: None,
        log_file: None,
    };
    let artifacts = agent::collect_artifacts(&outputs);
    assert_eq!(artifacts.len(), 2);
    let parquet = artifacts.iter().find(|a| a.kind == "parquet").unwrap();
    assert!(parquet.exists);
    assert!(parquet.size_bytes.unwrap() > 0);
    assert_eq!(parquet.sha256.as_ref().unwrap().len(), 64);
    let parquet_meta = parquet.parquet.as_ref().unwrap();
    assert_eq!(parquet_meta.row_count, total as i64);
    assert_eq!(parquet_meta.schema_fields, EXPECTED_SCHEMA);

    let ks = artifacts.iter().find(|a| a.kind == "ks").unwrap();
    assert!(ks.exists);
    assert_eq!(ks.line_count, Some(prefixes.len()));
    assert_eq!(ks.sha256.as_ref().unwrap().len(), 64);
}

#[tokio::test]
async fn test_list_summary_only_records_metrics_without_artifacts() {
    let quit = Arc::new(AtomicBool::new(false));
    let g_state = GlobalState::new(quit, 1);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let ctx = DataMapContext::new(rx, g_state.clone());

    let task = tokio::spawn(async move {
        s3_turbo_list::data_map::data_map_task_list_summary_only(ctx).await;
    });

    let mut first = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 100, [1u8; 16]);
    first.last_modified = 1;
    let mut second = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 200, [2u8; 16]);
    second.last_modified = 2;
    let mut third = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 300, [3u8; 16]);
    third.last_modified = 3;

    tx.send(vec![
        (ObjectKey::from("logs/a.txt"), first),
        (ObjectKey::from("logs/b.txt"), second),
        (ObjectKey::from("images/c.jpg"), third),
    ])
    .await
    .unwrap();
    drop(tx);
    task.await.unwrap();

    let metrics = g_state.metrics_snapshot();
    assert_eq!(metrics.data_received_batches, 1);
    assert_eq!(metrics.data_received_objects, 3);
    assert_eq!(metrics.data_streamed_rows, 3);
    assert_eq!(metrics.data_unique_prefixes, 2);
    assert_eq!(metrics.data_parquet_rows, 0);
    assert_eq!(metrics.data_ks_entries, 0);
    assert_eq!(metrics.data_bytes_total, 600);
    assert!(metrics.data_summary_only);
    assert_eq!(metrics.data_top_prefixes.len(), 2);
    assert_eq!(metrics.data_top_prefixes[0].prefix, "logs");
    assert_eq!(metrics.data_top_prefixes[0].objects, 2);
    assert_eq!(metrics.data_top_prefixes[0].bytes, 300);
}

#[tokio::test]
async fn test_list_streaming_honors_parquet_compression_config() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("list_streaming_snappy.parquet");
    let ks_path = dir.path().join("list_streaming_snappy.ks");

    let quit = Arc::new(AtomicBool::new(false));
    let g_state = GlobalState::new(quit, 1);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let ctx = DataMapContext::new(rx, g_state);

    let parquet_path_for_task = parquet_path.to_str().unwrap().to_string();
    let ks_path_for_task = ks_path.to_str().unwrap().to_string();
    let task = tokio::spawn(async move {
        s3_turbo_list::data_map::data_map_task_list_streaming(
            ctx,
            &ks_path_for_task,
            &parquet_path_for_task,
            OutputConfig {
                row_group_size: 1,
                compression: "snappy".to_string(),
                ..OutputConfig::default()
            },
        )
        .await;
    });

    tx.send(vec![(
        ObjectKey::from("logs/a.txt"),
        ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 100, [5u8; 16]),
    )])
    .await
    .unwrap();
    drop(tx);
    task.await.unwrap();

    let file = std::fs::File::open(&parquet_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert!(metadata.num_row_groups() >= 1);
    let compression = metadata.row_group(0).column(0).compression();
    assert_eq!(compression, Compression::SNAPPY);
}

// ── Parquet output schema verification ────────────────────

#[tokio::test]
async fn test_parquet_output_schema() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("test_output.parquet");
    let ks_path = dir.path().join("test_ks.csv");

    // Write a small batch.
    {
        let file = tokio::fs::File::create(&parquet_path).await.unwrap();
        let buf_writer = tokio::io::BufWriter::new(file);
        let mut writer = AsyncParquetOutput::new(buf_writer, ks_path.to_str().unwrap());

        let key = ObjectKey::from("test/file.txt");
        let mut props = ObjectProps::default();
        props.size = 1024;
        props.last_modified = 1715700000;

        writer.write_batch(vec![(key, props)], 1).await.unwrap();
        writer.close().await.unwrap();
    }

    // Read back and verify schema using std::fs::File (parquet ChunkReader impl).
    let std_file = std::fs::File::open(&parquet_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std_file)
        .unwrap()
        .build()
        .unwrap();

    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "Key");
    assert_eq!(schema.field(1).name(), "Size");
    assert_eq!(schema.field(2).name(), "LastModified");
    assert_eq!(schema.field(3).name(), "ETag");
    assert_eq!(schema.field(4).name(), "DiffFlag");

    // Verify data.
    let batches: Vec<_> = reader.collect();
    assert!(!batches.is_empty());
    let batch = &batches[0];
    let batch = batch.as_ref().expect("batch should be Ok");
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn test_parquet_output_write_error_is_reported_without_row_inflation() {
    let mut writer = AsyncParquetOutput::new(FailingAsyncWrite, "failed.ks");
    let key = ObjectKey::from("test/file.txt");
    let mut props = ObjectProps::default();
    props.size = 1024;

    writer
        .write_batch(vec![(key, props)], S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE)
        .await
        .unwrap();
    assert_eq!(writer.total_rows(), 1);
    let err = writer
        .close()
        .await
        .expect_err("failing writer must report parquet close error");
    assert!(
        err.contains("Parquet close error") && err.contains("intentional write failure"),
        "{}",
        err
    );
}

fn assert_list_output_consistency(
    parquet_path: &std::path::Path,
    ks_path: &std::path::Path,
    expected_rows: usize,
    expected_prefix_counts: &std::collections::BTreeMap<String, usize>,
) {
    let std_file = std::fs::File::open(parquet_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std_file)
        .unwrap()
        .build()
        .unwrap();
    let schema = reader.schema();
    let actual_schema: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(actual_schema, EXPECTED_SCHEMA);

    let batches: Vec<_> = reader.collect();
    let total_rows: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
    assert_eq!(total_rows, expected_rows);

    for batch in &batches {
        let batch = batch.as_ref().unwrap();
        let diff_flags = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        for i in 0..diff_flags.len() {
            assert_eq!(diff_flags.value(i), 0, "list mode DiffFlag must be 0");
        }
    }

    let ks = std::fs::read_to_string(ks_path).unwrap();
    let mut total_ks_count = 0usize;
    for (prefix, expected_count) in expected_prefix_counts {
        let line = format!("\"{}\",\"{}\"", prefix, expected_count);
        assert!(ks.contains(&line), "missing KS line: {}", line);
        total_ks_count += expected_count;
    }
    assert_eq!(total_ks_count, expected_rows);
}
