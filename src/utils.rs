use arrow_array::array::ArrayRef;
use arrow_array::array::{StringArray, UInt64Array, UInt8Array};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use log::{info, warn};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding, GzipLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
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

        let writer_props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::GZIP(GzipLevel::try_new(6).unwrap()))
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

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    pub async fn write_batch(&mut self, v: Vec<(ObjectKey, ObjectProps)>, diff_flag: u8) {
        if v.is_empty() {
            return;
        }
        let count = v.len();
        let mut vec_key: Vec<&str> = Vec::with_capacity(count);
        let mut vec_size: Vec<u64> = Vec::with_capacity(count);
        let mut vec_last_modified: Vec<u64> = Vec::with_capacity(count);
        let mut vec_etag: Vec<String> = Vec::with_capacity(count);
        let mut vec_diff_flag: Vec<u8> = Vec::with_capacity(count);

        for (key, props) in &v {
            vec_key.push(key.as_str());
            vec_size.push(props.size());
            vec_last_modified.push(props.last_modified());
            vec_etag.push(props.etag_string());
            vec_diff_flag.push(diff_flag);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec_key)) as ArrayRef,
            Arc::new(UInt64Array::from(vec_size)) as ArrayRef,
            Arc::new(UInt64Array::from(vec_last_modified)) as ArrayRef,
            Arc::new(StringArray::from(vec_etag)) as ArrayRef,
            Arc::new(UInt8Array::from(vec_diff_flag)) as ArrayRef,
        ];

        match RecordBatch::try_new(Arc::clone(&self.schema_ref), columns) {
            Ok(batch) => {
                if let Err(e) = self.writer.write(&batch).await {
                    warn!("Parquet write error: {}", e);
                }
                self.total_rows += count;
            }
            Err(e) => warn!("RecordBatch error: {}", e),
        }
    }

    /// Flush the in-progress row group (streaming — keeps memory bounded).
    pub async fn flush_row_group(&mut self) {
        if let Err(e) = self.writer.flush().await {
            warn!("Parquet flush error: {}", e);
        }
    }

    pub async fn close(self) {
        match self.writer.close().await {
            Ok(_) => info!("Parquet file written: {} rows", self.total_rows),
            Err(e) => warn!("Parquet close error: {}", e),
        }
    }
}
