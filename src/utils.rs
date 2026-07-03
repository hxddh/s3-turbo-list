use arrow_array::array::ArrayRef;
use arrow_array::builder::{StringBuilder, UInt64Builder, UInt8Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use log::{info, warn};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::AsyncWrite;

use crate::core::{ObjectKey, ObjectProps};

// ── AsyncParquetOutput ─────────────────────────────────────

pub struct AsyncParquetOutput<W: AsyncWrite + Unpin + Send> {
    schema_ref: SchemaRef,
    writer: AsyncArrowWriter<W>,
    ks_path: String,
    total_rows: usize,
    row_group_size: usize,
    list_batch: Option<ListParquetBatch>,
}

struct ListParquetBatch {
    key_builder: StringBuilder,
    size_builder: UInt64Builder,
    last_modified_builder: UInt64Builder,
    etag_builder: StringBuilder,
    diff_flag_builder: UInt8Builder,
    rows: usize,
}

impl ListParquetBatch {
    fn with_capacity(rows: usize) -> Self {
        Self {
            key_builder: StringBuilder::with_capacity(rows, rows.saturating_mul(40)),
            size_builder: UInt64Builder::with_capacity(rows),
            last_modified_builder: UInt64Builder::with_capacity(rows),
            etag_builder: StringBuilder::with_capacity(rows, rows.saturating_mul(36)),
            diff_flag_builder: UInt8Builder::with_capacity(rows),
            rows: 0,
        }
    }

    fn append(&mut self, key: &ObjectKey, props: &ObjectProps, diff_flag: u8) {
        let mut etag_buf = [0u8; 43];
        self.key_builder.append_value(key.as_str());
        self.size_builder.append_value(props.size());
        self.last_modified_builder
            .append_value(props.last_modified());
        let etag = props.write_etag_to_buffer(&mut etag_buf);
        self.etag_builder.append_value(etag);
        self.diff_flag_builder.append_value(diff_flag);
        self.rows += 1;
    }

    fn finish(mut self, schema_ref: SchemaRef) -> Result<RecordBatch, String> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.key_builder.finish()) as ArrayRef,
            Arc::new(self.size_builder.finish()) as ArrayRef,
            Arc::new(self.last_modified_builder.finish()) as ArrayRef,
            Arc::new(self.etag_builder.finish()) as ArrayRef,
            Arc::new(self.diff_flag_builder.finish()) as ArrayRef,
        ];
        RecordBatch::try_new(schema_ref, columns).map_err(|e| format!("RecordBatch error: {}", e))
    }
}

impl<W: AsyncWrite + Unpin + Send> AsyncParquetOutput<W> {
    pub fn new(buf_wr: W, ks_path: &str) -> Self {
        Self::new_with_options(buf_wr, ks_path, 10000, "gzip", 6)
    }

    pub fn new_with_options(
        buf_wr: W,
        ks_path: &str,
        row_group_size: usize,
        compression_name: &str,
        compression_level: u32,
    ) -> Self {
        let field_key = Field::new("Key", DataType::Utf8, false);
        let field_size = Field::new("Size", DataType::UInt64, false);
        let field_last_modified = Field::new("LastModified", DataType::UInt64, false);
        let field_etag = Field::new("ETag", DataType::Utf8, false);
        let field_diff_flag = Field::new("DiffFlag", DataType::UInt8, false);

        let schema_ref = Arc::new(Schema::new(vec![
            field_key,
            field_size,
            field_last_modified,
            field_etag,
            field_diff_flag,
        ]));

        let compression = parse_compression(compression_name, compression_level);
        let writer_props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_encoding(Encoding::PLAIN)
            // Chunk-level statistics instead of the per-page default: readers
            // of these listings scan whole files, so page-level min/max on the
            // near-unique Key/ETag strings costs encode CPU without buying
            // pruning. Chunk keeps coarse row-group pruning intact.
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_compression(compression)
            .set_max_row_group_size(row_group_size.max(1))
            .build();

        let writer =
            AsyncArrowWriter::try_new(buf_wr, Arc::clone(&schema_ref), Some(writer_props)).unwrap();

        Self {
            schema_ref,
            writer,
            ks_path: ks_path.to_string(),
            total_rows: 0,
            row_group_size: row_group_size.max(1),
            list_batch: None,
        }
    }

    pub fn ks_path(&self) -> &str {
        &self.ks_path
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
            + self
                .list_batch
                .as_ref()
                .map(|batch| batch.rows)
                .unwrap_or(0)
    }

    pub async fn write_batch(
        &mut self,
        v: Vec<(ObjectKey, ObjectProps)>,
        diff_flag: u8,
    ) -> Result<(), String> {
        self.write_batch_filtered(v, diff_flag, |_, _| true).await?;
        Ok(())
    }

    pub async fn write_batch_filtered<F>(
        &mut self,
        v: Vec<(ObjectKey, ObjectProps)>,
        diff_flag: u8,
        mut include: F,
    ) -> Result<usize, String>
    where
        F: FnMut(&ObjectKey, &ObjectProps) -> bool,
    {
        if v.is_empty() {
            return Ok(0);
        }

        let mut key_builder = StringBuilder::with_capacity(v.len(), v.len().saturating_mul(40));
        let mut size_builder = UInt64Builder::with_capacity(v.len());
        let mut last_modified_builder = UInt64Builder::with_capacity(v.len());
        let mut etag_builder = StringBuilder::with_capacity(v.len(), v.len().saturating_mul(36));
        let mut diff_flag_builder = UInt8Builder::with_capacity(v.len());
        let mut etag_buf = [0u8; 43];
        let mut count = 0usize;

        for (key, props) in &v {
            if !include(key, props) {
                continue;
            }

            key_builder.append_value(key.as_str());
            size_builder.append_value(props.size());
            last_modified_builder.append_value(props.last_modified());
            let etag = props.write_etag_to_buffer(&mut etag_buf);
            etag_builder.append_value(etag);
            diff_flag_builder.append_value(diff_flag);
            count += 1;
        }

        if count == 0 {
            return Ok(0);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(key_builder.finish()) as ArrayRef,
            Arc::new(size_builder.finish()) as ArrayRef,
            Arc::new(last_modified_builder.finish()) as ArrayRef,
            Arc::new(etag_builder.finish()) as ArrayRef,
            Arc::new(diff_flag_builder.finish()) as ArrayRef,
        ];

        match RecordBatch::try_new(Arc::clone(&self.schema_ref), columns) {
            Ok(batch) => {
                self.writer
                    .write(&batch)
                    .await
                    .map_err(|e| format!("Parquet write error: {}", e))?;
                self.total_rows += count;
            }
            Err(e) => return Err(format!("RecordBatch error: {}", e)),
        }
        Ok(count)
    }

    pub async fn write_list_batch_filtered<F>(
        &mut self,
        v: Vec<(ObjectKey, ObjectProps)>,
        diff_flag: u8,
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

            self.ensure_list_batch();
            let batch = self
                .list_batch
                .as_mut()
                .expect("list batch must exist after ensure");
            batch.append(key, props, diff_flag);
            count += 1;

            if batch.rows >= self.row_group_size {
                self.flush_list_batch().await?;
            }
        }
        Ok(count)
    }

    fn ensure_list_batch(&mut self) {
        if self.list_batch.is_none() {
            self.list_batch = Some(ListParquetBatch::with_capacity(self.row_group_size));
        }
    }

    async fn flush_list_batch(&mut self) -> Result<(), String> {
        let Some(batch) = self.list_batch.take() else {
            return Ok(());
        };
        if batch.rows == 0 {
            return Ok(());
        }
        let rows = batch.rows;
        let record_batch = batch.finish(Arc::clone(&self.schema_ref))?;
        self.writer
            .write(&record_batch)
            .await
            .map_err(|e| format!("Parquet write error: {}", e))?;
        self.total_rows += rows;
        Ok(())
    }

    pub async fn close(mut self) -> Result<(), String> {
        self.flush_list_batch().await?;
        self.writer
            .close()
            .await
            .map_err(|e| format!("Parquet close error: {}", e))?;
        info!("Parquet file written: {} rows", self.total_rows);
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::parse_compression;
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
    fn test_parse_compression_unknown_falls_back() {
        assert!(matches!(
            parse_compression("unknown", 6),
            Compression::GZIP(_)
        ));
    }
}
