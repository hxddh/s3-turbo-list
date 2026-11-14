//! Where Parquet parts go: a local dataset directory, or an S3 prefix
//! (`-o s3://bucket/prefix/`).
//!
//! An S3 part streams into a multipart upload while it is written, so the
//! run never holds more than a few upload parts per writer in memory and
//! needs no local disk. Nothing becomes visible until the run commits: the
//! commit completes every upload (and takes back what it completed if a
//! later one fails), a failed or interrupted run aborts them all.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::ParquetError;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;

use crate::core::OutputPart;

/// Upload parts in flight per writer.
const UPLOADS_IN_FLIGHT: usize = 4;
/// Per-writer output buffer between the Parquet encoder and the file (local parts only; S3 parts buffer upload parts instead). The
/// `AsyncArrowWriter` already buffers and flushes whole row groups, so this only
/// coalesces those large, sequential writes into fewer syscalls — past a few MB
/// it stops affecting throughput (the page cache absorbs the writes, and on the
/// fast-store path the writers are CPU-bound on encode+compress, not on file
/// I/O). Sized so the pool's *aggregate* buffer stays bounded as the governor
/// scales out: `MAX_LIST_OUTPUT_WORKERS` × this is ~256 MB, sane on any machine
/// with enough cores to reach the cap. (Was a fixed 100 MB per writer, i.e. up
/// to 3.2 GB once the pool fanned out on a many-core host.)
const LOCAL_WRITER_BUF_BYTES: usize = 8 * crate::core::MB;

/// A part writer, local or S3.
pub type PartWriter = Box<dyn AsyncFileWriter>;

/// The destination of a run's Parquet parts.
#[derive(Clone)]
pub enum PartSink {
    /// Hidden temp files in a dataset directory (see `tmp_part_path`).
    Dir(PathBuf),
    /// Multipart uploads under an S3 prefix.
    S3(Arc<S3Output>),
}

impl PartSink {
    /// Start writing part `index`.
    pub async fn open(&self, index: usize) -> Result<PartWriter, String> {
        match self {
            PartSink::Dir(dir) => {
                let path = crate::data_map::tmp_part_path(dir, index);
                let file = tokio::fs::File::create(&path)
                    .await
                    .map_err(|e| format!("cannot create output file {}: {}", path.display(), e))?;
                Ok(Box::new(tokio::io::BufWriter::with_capacity(
                    LOCAL_WRITER_BUF_BYTES,
                    file,
                )))
            }
            PartSink::S3(out) => Ok(Box::new(out.open(index).await?)),
        }
    }

    /// Where part `index` is written, for messages.
    pub fn describe(&self, index: usize) -> String {
        match self {
            PartSink::Dir(dir) => crate::data_map::tmp_part_path(dir, index)
                .display()
                .to_string(),
            PartSink::S3(out) => out.url(&out.part_key(index)),
        }
    }

    /// Discard part `index` of a run that will not commit it.
    pub async fn discard(&self, index: usize) {
        match self {
            PartSink::Dir(dir) => {
                let _ = tokio::fs::remove_file(crate::data_map::tmp_part_path(dir, index)).await;
            }
            PartSink::S3(out) => out.abort(index).await,
        }
    }
}

/// One uploaded (not yet completed) part file.
struct Upload {
    key: String,
    upload_id: String,
    parts: Vec<CompletedPart>,
    bytes: u64,
}

/// A committed part object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedObject {
    pub url: String,
    pub rows: usize,
    pub bytes: u64,
}

/// The S3 prefix a run writes its dataset to.
pub struct S3Output {
    client: aws_sdk_s3::Client,
    bucket: String,
    /// Ends in `/` unless empty.
    prefix: String,
    timeout: Duration,
    /// Size of each upload part but the last (S3 needs at least 5 MiB;
    /// `OutputConfig::upload_part_bytes`).
    part_bytes: usize,
    /// Uploads by part index: opened, then filled in when the part is done.
    uploads: Mutex<BTreeMap<usize, Upload>>,
}

fn sdk_message<E>(e: &aws_sdk_s3::error::SdkError<E>) -> String
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Debug,
{
    match e.as_service_error() {
        Some(service) => format!(
            "{}: {}",
            service.code().unwrap_or("error"),
            service.message().unwrap_or("")
        ),
        None => format!("{:?}", e),
    }
}

impl S3Output {
    pub fn new(
        client: aws_sdk_s3::Client,
        bucket: &str,
        prefix: &str,
        timeout_secs: u64,
        part_bytes: usize,
    ) -> Arc<Self> {
        let prefix = if prefix.is_empty() || prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{}/", prefix)
        };
        Arc::new(Self {
            client,
            bucket: bucket.to_string(),
            prefix,
            timeout: Duration::from_secs(timeout_secs.max(1)),
            part_bytes: part_bytes.max(1),
            uploads: Mutex::new(BTreeMap::new()),
        })
    }

    /// `s3://bucket/prefix/` of the dataset.
    pub fn root_url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    fn url(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, key)
    }

    pub fn part_key(&self, index: usize) -> String {
        format!("{}{}", self.prefix, crate::data_map::part_file_name(index))
    }

    /// Whether the prefix holds no object yet (a dataset is never written
    /// over another one).
    pub async fn is_empty(&self) -> Result<bool, String> {
        let request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .max_keys(1)
            .send();
        let out = tokio::time::timeout(self.timeout, request)
            .await
            .map_err(|_| format!("listing {} timed out", self.root_url()))?
            .map_err(|e| format!("cannot list {}: {}", self.root_url(), sdk_message(&e)))?;
        Ok(out.contents().is_empty())
    }

    async fn open(self: &Arc<Self>, index: usize) -> Result<S3PartWriter, String> {
        let key = self.part_key(index);
        let request = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .content_type("application/vnd.apache.parquet")
            .send();
        let out = tokio::time::timeout(self.timeout, request)
            .await
            .map_err(|_| format!("starting the upload of {} timed out", self.url(&key)))?
            .map_err(|e| format!("cannot upload {}: {}", self.url(&key), sdk_message(&e)))?;
        let upload_id = out
            .upload_id()
            .ok_or_else(|| format!("no upload id for {}", self.url(&key)))?
            .to_string();
        self.uploads.lock().unwrap().insert(
            index,
            Upload {
                key: key.clone(),
                upload_id: upload_id.clone(),
                parts: Vec::new(),
                bytes: 0,
            },
        );
        Ok(S3PartWriter {
            out: Arc::clone(self),
            index,
            key,
            upload_id,
            buf: BytesMut::new(),
            next_part: 1,
            in_flight: JoinSet::new(),
            done: Vec::new(),
            bytes: 0,
        })
    }

    async fn abort(&self, index: usize) {
        let upload = self.uploads.lock().unwrap().remove(&index);
        if let Some(upload) = upload {
            self.abort_upload(&upload).await;
        }
    }

    async fn abort_upload(&self, upload: &Upload) {
        let request = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&upload.key)
            .upload_id(&upload.upload_id)
            .send();
        if !matches!(tokio::time::timeout(self.timeout, request).await, Ok(Ok(_))) {
            log::warn!(
                "could not abort the upload of {}; a bucket lifecycle rule for \
                 incomplete multipart uploads removes it",
                self.url(&upload.key)
            );
        }
    }

    /// Abort every upload of a run that does not commit.
    pub async fn abort_all(&self) {
        let uploads = std::mem::take(&mut *self.uploads.lock().unwrap());
        for upload in uploads.values() {
            self.abort_upload(upload).await;
        }
    }

    async fn delete(&self, key: &str) {
        let request = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send();
        if !matches!(tokio::time::timeout(self.timeout, request).await, Ok(Ok(_))) {
            log::warn!("could not remove {}", self.url(key));
        }
    }

    /// Remove committed objects again (a later commit step failed).
    pub async fn unpublish(&self, objects: &[CommittedObject]) {
        for object in objects {
            let key = object
                .url
                .strip_prefix(&format!("s3://{}/", self.bucket))
                .unwrap_or(&object.url);
            self.delete(key).await;
        }
    }

    /// Complete the uploads of `parts`, all or nothing. Empty parts are
    /// dropped unless the dataset would otherwise have no file.
    pub async fn commit(&self, parts: &[OutputPart]) -> Result<Vec<CommittedObject>, String> {
        let total: usize = parts.iter().map(|p| p.rows).sum();
        let mut uploads = std::mem::take(&mut *self.uploads.lock().unwrap());
        let mut published: Vec<CommittedObject> = Vec::new();
        let mut failed: Option<String> = None;
        for (i, part) in parts.iter().enumerate() {
            let Some(upload) = uploads.remove(&part.index) else {
                failed = Some(format!("part {} has no upload", part.index));
                break;
            };
            if part.rows == 0 && !(total == 0 && i == 0) {
                self.abort_upload(&upload).await;
                continue;
            }
            let request = self
                .client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&upload.key)
                .upload_id(&upload.upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(upload.parts.clone()))
                        .build(),
                )
                .send();
            match tokio::time::timeout(self.timeout, request).await {
                Ok(Ok(_)) => published.push(CommittedObject {
                    url: self.url(&upload.key),
                    rows: part.rows,
                    bytes: upload.bytes,
                }),
                Ok(Err(e)) => {
                    failed = Some(format!(
                        "cannot commit {}: {}",
                        self.url(&upload.key),
                        sdk_message(&e)
                    ));
                    self.abort_upload(&upload).await;
                    break;
                }
                Err(_) => {
                    failed = Some(format!("committing {} timed out", self.url(&upload.key)));
                    self.abort_upload(&upload).await;
                    break;
                }
            }
        }
        if let Some(e) = failed {
            for upload in uploads.values() {
                self.abort_upload(upload).await;
            }
            self.unpublish(&published).await;
            return Err(e);
        }
        Ok(published)
    }

    /// Write `_result.json` (the last object of a committed dataset).
    pub async fn put_result(&self, body: Vec<u8>) -> Result<(), String> {
        let key = format!("{}_result.json", self.prefix);
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .content_type("application/json")
            .body(ByteStream::from(body))
            .send();
        tokio::time::timeout(self.timeout, request)
            .await
            .map_err(|_| format!("writing {} timed out", self.url(&key)))?
            .map_err(|e| format!("cannot write {}: {}", self.url(&key), sdk_message(&e)))?;
        Ok(())
    }
}

/// Streams one part file into its multipart upload.
pub struct S3PartWriter {
    out: Arc<S3Output>,
    index: usize,
    key: String,
    upload_id: String,
    buf: BytesMut,
    next_part: i32,
    in_flight: JoinSet<Result<CompletedPart, String>>,
    done: Vec<CompletedPart>,
    bytes: u64,
}

fn external(message: String) -> ParquetError {
    ParquetError::External(message.into())
}

impl S3PartWriter {
    fn upload(&mut self, body: Bytes) {
        let number = self.next_part;
        self.next_part += 1;
        self.bytes += body.len() as u64;
        let out = Arc::clone(&self.out);
        let (key, upload_id) = (self.key.clone(), self.upload_id.clone());
        self.in_flight.spawn(async move {
            let request = out
                .client
                .upload_part()
                .bucket(&out.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(number)
                .body(ByteStream::from(body))
                .send();
            let res = tokio::time::timeout(out.timeout, request)
                .await
                .map_err(|_| format!("uploading part {} of {} timed out", number, out.url(&key)))?
                .map_err(|e| format!("cannot upload {}: {}", out.url(&key), sdk_message(&e)))?;
            Ok(CompletedPart::builder()
                .part_number(number)
                .set_e_tag(res.e_tag().map(str::to_string))
                .build())
        });
    }

    async fn join_one(&mut self) -> Result<(), String> {
        match self.in_flight.join_next().await {
            Some(Ok(part)) => {
                self.done.push(part?);
                Ok(())
            }
            Some(Err(e)) => Err(format!("upload task failed: {}", e)),
            None => Ok(()),
        }
    }
}

impl AsyncFileWriter for S3PartWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.buf.extend_from_slice(&bs);
            let part_bytes = self.out.part_bytes;
            while self.buf.len() >= part_bytes {
                let body = self.buf.split_to(part_bytes).freeze();
                self.upload(body);
                while self.in_flight.len() >= UPLOADS_IN_FLIGHT {
                    self.join_one().await.map_err(external)?;
                }
            }
            Ok(())
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            // The last part may be short (and is the only one of a small file).
            if !self.buf.is_empty() || self.next_part == 1 {
                let body = std::mem::take(&mut self.buf).freeze();
                self.upload(body);
            }
            while !self.in_flight.is_empty() {
                self.join_one().await.map_err(external)?;
            }
            let mut parts = std::mem::take(&mut self.done);
            parts.sort_by_key(|p| p.part_number());
            if let Some(upload) = self.out.uploads.lock().unwrap().get_mut(&self.index) {
                upload.parts = parts;
                upload.bytes = self.bytes;
            }
            Ok(())
        })
    }
}
