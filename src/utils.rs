use arrow_array::RecordBatch;
use arrow_array::array::ArrayRef;
use arrow_array::builder::{
    BooleanBuilder, StringBuilder, TimestampMillisecondBuilder, UInt64Builder,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use log::{info, warn};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use std::str::FromStr;
use std::sync::Arc;

use crate::core::{ObjectKey, ObjectProps};

// ── Output schemas ─────────────────────────────────────────

/// Parquet key-value metadata key naming the schema of a file.
pub const SCHEMA_METADATA_KEY: &str = "s3tl.schema";
pub const LIST_SCHEMA: &str = "s3tl.list/1";
pub const DIFF_SCHEMA: &str = "s3tl.diff/1";
pub const LIST_VERSIONS_SCHEMA: &str = "s3tl.list-versions/1";

/// Which of the two output schemas a writer produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// `key, size, last_modified, etag, storage_class`.
    List,
    /// The list columns plus `version_id, is_latest, is_delete_marker`
    /// (`list --versions`).
    ListVersions,
    /// `key, status, left_size, left_last_modified, left_etag, right_size,
    /// right_last_modified, right_etag`.
    Diff,
}

/// How a key differs between the two sides of a diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffStatus {
    LeftOnly,
    RightOnly,
    Changed,
}

impl DiffStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DiffStatus::LeftOnly => "left_only",
            DiffStatus::RightOnly => "right_only",
            DiffStatus::Changed => "changed",
        }
    }
}

/// One row of a diff output.
#[derive(Debug, Clone)]
pub struct DiffRow {
    pub key: ObjectKey,
    pub status: DiffStatus,
    pub left: Option<ObjectProps>,
    pub right: Option<ObjectProps>,
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

fn schema_for(layout: Layout) -> SchemaRef {
    let fields = match layout {
        Layout::List | Layout::ListVersions => {
            let mut fields = vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("size", DataType::UInt64, false),
                Field::new("last_modified", timestamp_type(), true),
                Field::new("etag", DataType::Utf8, true),
                Field::new("storage_class", DataType::Utf8, true),
            ];
            if layout == Layout::ListVersions {
                fields.push(Field::new("version_id", DataType::Utf8, true));
                fields.push(Field::new("is_latest", DataType::Boolean, false));
                fields.push(Field::new("is_delete_marker", DataType::Boolean, false));
            }
            fields
        }
        Layout::Diff => vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("left_size", DataType::UInt64, true),
            Field::new("left_last_modified", timestamp_type(), true),
            Field::new("left_etag", DataType::Utf8, true),
            Field::new("right_size", DataType::UInt64, true),
            Field::new("right_last_modified", timestamp_type(), true),
            Field::new("right_etag", DataType::Utf8, true),
        ],
    };
    Arc::new(Schema::new(fields))
}

/// `last_modified` of a row: `null` when the endpoint sent none (and for
/// folder rows).
fn last_modified_of(props: &ObjectProps) -> Option<i64> {
    let ms = props.last_modified_millis();
    (ms != 0).then_some(ms as i64)
}

/// The three per-object columns of a row, appended together.
struct ObjectColumns {
    size: UInt64Builder,
    last_modified: TimestampMillisecondBuilder,
    etag: StringBuilder,
}

impl ObjectColumns {
    fn with_capacity(rows: usize) -> Self {
        Self {
            size: UInt64Builder::with_capacity(rows),
            last_modified: TimestampMillisecondBuilder::with_capacity(rows).with_timezone("UTC"),
            etag: StringBuilder::with_capacity(rows, rows.saturating_mul(34)),
        }
    }

    fn append(&mut self, props: Option<&ObjectProps>) {
        let Some(props) = props else {
            self.size.append_null();
            self.last_modified.append_null();
            self.etag.append_null();
            return;
        };
        self.size.append_value(props.size());
        self.last_modified.append_option(last_modified_of(props));
        let mut buf = [0u8; 43];
        self.etag.append_option(props.etag_text(&mut buf));
    }

    fn finish(mut self, columns: &mut Vec<ArrayRef>) {
        columns.push(Arc::new(self.size.finish()));
        columns.push(Arc::new(self.last_modified.finish()));
        columns.push(Arc::new(self.etag.finish()));
    }
}

/// The three version columns of a `list --versions` row.
struct VersionColumns {
    version_id: StringBuilder,
    is_latest: BooleanBuilder,
    is_delete_marker: BooleanBuilder,
}

impl VersionColumns {
    fn with_capacity(rows: usize) -> Self {
        Self {
            version_id: StringBuilder::with_capacity(rows, rows.saturating_mul(32)),
            is_latest: BooleanBuilder::with_capacity(rows),
            is_delete_marker: BooleanBuilder::with_capacity(rows),
        }
    }

    fn append(&mut self, props: &ObjectProps) {
        self.version_id.append_option(props.version_id());
        self.is_latest.append_value(props.is_latest());
        self.is_delete_marker.append_value(props.is_delete_marker());
    }

    fn finish(mut self, columns: &mut Vec<ArrayRef>) {
        columns.push(Arc::new(self.version_id.finish()));
        columns.push(Arc::new(self.is_latest.finish()));
        columns.push(Arc::new(self.is_delete_marker.finish()));
    }
}

/// Column builders for a batch of rows. One lives per writer, so the size
/// difference between the variants does not matter.
#[allow(clippy::large_enum_variant)]
enum RowBatch {
    List {
        key: StringBuilder,
        object: ObjectColumns,
        storage_class: StringBuilder,
        /// `version_id, is_latest, is_delete_marker` (`ListVersions` only).
        versions: Option<VersionColumns>,
        rows: usize,
    },
    Diff {
        key: StringBuilder,
        status: StringBuilder,
        left: ObjectColumns,
        right: ObjectColumns,
        rows: usize,
    },
}

impl RowBatch {
    fn with_capacity(layout: Layout, rows: usize) -> Self {
        // Builders grow as needed: cap the up-front reservation.
        let reserve = rows.min(65_536);
        match layout {
            Layout::List | Layout::ListVersions => RowBatch::List {
                key: StringBuilder::with_capacity(reserve, reserve.saturating_mul(40)),
                object: ObjectColumns::with_capacity(reserve),
                storage_class: StringBuilder::with_capacity(reserve, reserve.saturating_mul(8)),
                versions: (layout == Layout::ListVersions)
                    .then(|| VersionColumns::with_capacity(reserve)),
                rows: 0,
            },
            Layout::Diff => RowBatch::Diff {
                key: StringBuilder::with_capacity(reserve, reserve.saturating_mul(40)),
                status: StringBuilder::with_capacity(reserve, reserve.saturating_mul(9)),
                left: ObjectColumns::with_capacity(reserve),
                right: ObjectColumns::with_capacity(reserve),
                rows: 0,
            },
        }
    }

    fn rows(&self) -> usize {
        match self {
            RowBatch::List { rows, .. } | RowBatch::Diff { rows, .. } => *rows,
        }
    }

    fn append_list(&mut self, k: &ObjectKey, props: &ObjectProps) {
        let RowBatch::List {
            key,
            object,
            storage_class,
            versions,
            rows,
        } = self
        else {
            unreachable!("list row appended to a diff batch");
        };
        key.append_value(k.as_str());
        object.append(Some(props));
        match props.storage_class_name() {
            Some(name) => storage_class.append_value(name),
            None => storage_class.append_null(),
        }
        if let Some(versions) = versions {
            versions.append(props);
        }
        *rows += 1;
    }

    fn append_diff(&mut self, row: &DiffRow) {
        let RowBatch::Diff {
            key,
            status,
            left,
            right,
            rows,
        } = self
        else {
            unreachable!("diff row appended to a list batch");
        };
        key.append_value(row.key.as_str());
        status.append_value(row.status.as_str());
        left.append(row.left.as_ref());
        right.append(row.right.as_ref());
        *rows += 1;
    }

    fn finish(self, schema: SchemaRef) -> Result<RecordBatch, String> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(8);
        match self {
            RowBatch::List {
                mut key,
                object,
                mut storage_class,
                versions,
                ..
            } => {
                columns.push(Arc::new(key.finish()));
                object.finish(&mut columns);
                columns.push(Arc::new(storage_class.finish()));
                if let Some(versions) = versions {
                    versions.finish(&mut columns);
                }
            }
            RowBatch::Diff {
                mut key,
                mut status,
                left,
                right,
                ..
            } => {
                columns.push(Arc::new(key.finish()));
                columns.push(Arc::new(status.finish()));
                left.finish(&mut columns);
                right.finish(&mut columns);
            }
        }
        RecordBatch::try_new(schema, columns).map_err(|e| format!("RecordBatch error: {}", e))
    }
}

// ── AsyncParquetOutput ─────────────────────────────────────

/// A streaming Parquet writer for one output file.
pub struct AsyncParquetOutput<W: AsyncFileWriter> {
    layout: Layout,
    schema_ref: SchemaRef,
    writer: AsyncArrowWriter<W>,
    total_rows: usize,
    row_group_size: usize,
    batch: Option<RowBatch>,
}

impl<W: AsyncFileWriter> AsyncParquetOutput<W> {
    /// A writer with the default settings (tests, benchmarks).
    pub fn new(buf_wr: W, layout: Layout) -> Self {
        let defaults = crate::config::OutputConfig::default();
        Self::with_settings(
            buf_wr,
            layout,
            defaults.row_group_size,
            &defaults.compression,
            defaults.compression_level,
        )
    }

    pub fn with_settings(
        buf_wr: W,
        layout: Layout,
        row_group_size: usize,
        compression_name: &str,
        compression_level: u32,
    ) -> Self {
        let schema_ref = schema_for(layout);
        let compression = parse_compression(compression_name, compression_level);
        let mut props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_encoding(Encoding::PLAIN)
            // Chunk-level statistics instead of the per-page default: readers
            // scan whole files, so page-level min/max on near-unique strings
            // costs encode CPU without buying pruning.
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_compression(compression)
            .set_max_row_group_row_count(Some(row_group_size.max(1)))
            .set_key_value_metadata(Some(vec![KeyValue::new(
                SCHEMA_METADATA_KEY.to_string(),
                match layout {
                    Layout::List => LIST_SCHEMA,
                    Layout::ListVersions => LIST_VERSIONS_SCHEMA,
                    Layout::Diff => DIFF_SCHEMA,
                }
                .to_string(),
            )]));
        // Keys and ETags are near-unique per row: a dictionary never pays for
        // itself, and building one hashes every value until it overflows.
        for column in ["key", "etag", "left_etag", "right_etag", "version_id"] {
            props = props.set_column_dictionary_enabled(ColumnPath::from(column), false);
        }
        let writer =
            AsyncArrowWriter::try_new(buf_wr, Arc::clone(&schema_ref), Some(props.build()))
                .expect("the output schema is valid");
        Self {
            layout,
            schema_ref,
            writer,
            total_rows: 0,
            row_group_size: row_group_size.max(1),
            batch: None,
        }
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows + self.batch.as_ref().map_or(0, RowBatch::rows)
    }

    /// Write the rows of a list batch that `include` accepts.
    pub async fn write_list_batch_filtered<F>(
        &mut self,
        v: Vec<(ObjectKey, ObjectProps)>,
        mut include: F,
    ) -> Result<usize, String>
    where
        F: FnMut(&ObjectKey, &ObjectProps) -> bool,
    {
        let mut count = 0usize;
        for (key, props) in &v {
            if !include(key, props) {
                continue;
            }
            let (layout, cap) = (self.layout, self.row_group_size);
            let batch = self
                .batch
                .get_or_insert_with(|| RowBatch::with_capacity(layout, cap));
            batch.append_list(key, props);
            count += 1;
            if batch.rows() >= self.row_group_size {
                self.flush().await?;
            }
        }
        Ok(count)
    }

    /// Write diff rows (in merged key order) as one record batch.
    pub async fn write_diff_rows(&mut self, rows: Vec<DiffRow>) -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut batch = RowBatch::with_capacity(self.layout, rows.len());
        for row in &rows {
            batch.append_diff(row);
        }
        self.write_batch(batch).await
    }

    async fn write_batch(&mut self, batch: RowBatch) -> Result<(), String> {
        let rows = batch.rows();
        if rows == 0 {
            return Ok(());
        }
        let record_batch = batch.finish(Arc::clone(&self.schema_ref))?;
        self.writer
            .write(&record_batch)
            .await
            .map_err(|e| format!("Parquet write error: {}", e))?;
        self.total_rows += rows;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), String> {
        match self.batch.take() {
            Some(batch) => self.write_batch(batch).await,
            None => Ok(()),
        }
    }

    pub async fn close(mut self) -> Result<(), String> {
        self.flush().await?;
        self.writer
            .close()
            .await
            .map_err(|e| format!("Parquet close error: {}", e))?;
        info!("Parquet file written: {} rows", self.total_rows);
        Ok(())
    }
}

/// Codec names the Parquet writer accepts. An unknown name used to fall back
/// to gzip silently — a different codec from the documented default, chosen
/// without the caller knowing, and reported as the user's own value in the
/// dry-run plan.
pub const SUPPORTED_COMPRESSION: &[&str] = &[
    "uncompressed",
    "snappy",
    "gzip",
    "lz4",
    "lz4_raw",
    "zstd",
    "brotli",
];

pub fn is_supported_compression(name: &str) -> bool {
    let normalized = name.trim().to_lowercase();
    SUPPORTED_COMPRESSION.contains(&normalized.as_str())
}

/// Why this codec/level pair cannot be written as configured, if it cannot.
/// `parse_compression` falls back to gzip on a bad level, so without this
/// check `zstd` at level 99 wrote gzip while every report said zstd.
pub fn compression_setting_error(name: &str, level: u32) -> Option<String> {
    let normalized = name.trim().to_lowercase();
    if !matches!(normalized.as_str(), "gzip" | "zstd" | "brotli") {
        return None;
    }
    Compression::from_str(&format!("{}({})", normalized, level))
        .err()
        .map(|e| e.to_string())
}

fn parse_compression(name: &str, level: u32) -> Compression {
    let normalized = name.trim().to_lowercase();
    let spec = match normalized.as_str() {
        "gzip" | "zstd" | "brotli" => format!("{}({})", normalized, level),
        "uncompressed" | "snappy" | "lz4" | "lz4_raw" => normalized,
        other => {
            warn!(
                "Unsupported Parquet compression '{}'; falling back to gzip({})",
                other, level
            );
            format!("gzip({})", level)
        }
    };

    Compression::from_str(&spec).unwrap_or_else(|e| {
        warn!(
            "Invalid Parquet compression setting '{}': {}; falling back to gzip(6)",
            spec, e
        );
        Compression::from_str("gzip(6)").expect("gzip(6) must be a valid Parquet compression")
    })
}

/// fsync a directory so a rename or removal inside it is durable. A no-op
/// where directories cannot be opened for syncing (Windows).
pub fn sync_dir(dir: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{compression_setting_error, parse_compression};
    use parquet::basic::Compression;

    #[test]
    fn test_parse_compression_gzip_level() {
        assert!(matches!(parse_compression("gzip", 6), Compression::GZIP(_)));
    }

    #[test]
    fn test_parse_compression_snappy_no_level() {
        assert!(matches!(
            parse_compression("snappy", 6),
            Compression::SNAPPY
        ));
    }

    #[test]
    fn test_compression_setting_error_rejects_out_of_range_levels() {
        assert!(compression_setting_error("zstd", 3).is_none());
        assert!(compression_setting_error("zstd", 99).is_some());
        assert!(compression_setting_error("gzip", 11).is_some());
        assert!(compression_setting_error("brotli", 12).is_some());
        // Level is ignored by codecs that take none.
        assert!(compression_setting_error("snappy", 99).is_none());
    }

    #[test]
    fn test_parse_compression_unknown_falls_back() {
        assert!(matches!(
            parse_compression("unknown", 6),
            Compression::GZIP(_)
        ));
    }
}
