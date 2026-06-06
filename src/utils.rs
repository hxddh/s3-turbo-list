use arrow_array::array::ArrayRef;
use arrow_array::builder::{StringBuilder, UInt64Builder, UInt8Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use log::{info, warn};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterVersion};
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
        }
    }

    pub fn ks_path(&self) -> &str {
        &self.ks_path
    }

    #[allow(dead_code)] // Phase 5: used in monitoring and streaming row-group flushes
    pub fn total_rows(&self) -> usize {
        self.total_rows
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
        let mut etag = String::with_capacity(43);
        let mut count = 0usize;

        for (key, props) in &v {
            if !include(key, props) {
                continue;
            }

            key_builder.append_value(key.as_str());
            size_builder.append_value(props.size());
            last_modified_builder.append_value(props.last_modified());
            props.append_etag_string(&mut etag);
            etag_builder.append_value(&etag);
            etag.clear();
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

    /// Flush the in-progress row group (streaming — keeps memory bounded).
    #[allow(dead_code)] // Phase 5: used in streaming row-group flushes
    pub async fn flush_row_group(&mut self) {
        if let Err(e) = self.writer.flush().await {
            warn!("Parquet flush error: {}", e);
        }
    }

    pub async fn close(self) -> Result<(), String> {
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
