// Integration tests for data_map aggregation and Parquet output pipeline.
use arrow::array::RecordBatchReader;
use s3_turbo_list::core::{
    ObjectKey, ObjectProps, S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
};
use s3_turbo_list::data_map::{ObjectMap, PrefixMap};
use s3_turbo_list::utils::AsyncParquetOutput;

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
        map.dump(&mut writer, true).await;
        writer.close().await;
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

        writer.write_batch(vec![(key, props)], 1).await;
        writer.close().await;
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
