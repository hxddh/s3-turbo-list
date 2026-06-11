use crate::config::S3Config;
use crate::stats::HttpStatusCodeTracker;
use crate::trace::S3TraceWriter;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::sync::mpsc;
use tokio::sync::Barrier;

// ── Constants ──────────────────────────────────────────────

#[allow(dead_code)] // Phase 5: used in size formatting
pub(crate) const KB: usize = 1024;
pub(crate) const MB: usize = 1_048_576;
#[allow(dead_code)] // Phase 5: reserved for future size formatting
pub(crate) const GB: usize = 1_073_741_824;

pub(crate) const DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS: u64 = 5;
pub(crate) const DEFAULT_TASK_COMPLETE_QUIT_WAIT_SECS: u64 = 1;

// ── ObjectProps: bit-flag state machine ────────────────────

const OBJECT_PROPS_FLAG_S3_GP_BUCKET: u8 = 0b1;
#[allow(dead_code)] // Phase 5: reserved for S3 directory bucket support
const OBJECT_PROPS_FLAG_S3_DIR_BUCKET: u8 = 0b10;
const OBJECT_PROPS_FLAG_DIR_LEFT: u8 = 0b1000_0000;
const OBJECT_PROPS_FLAG_DIR_RIGHT: u8 = 0b0100_0000;
const OBJECT_PROPS_FLAG_DIR_BOTH: u8 = 0b1100_0000;
const OBJECT_PROPS_FLAG_DIFF_MODE: u8 = 0b0010_0000;

const OBJECT_PROPS_STATUS_OPEN: u8 = 0xFF;
const OBJECT_PROPS_STATUS_MATCH: u8 = 0x0;
const OBJECT_PROPS_STATUS_SIZE_NOT_MATCH: u8 = 1;
const OBJECT_PROPS_STATUS_ETAG_NOT_AVAIL: u8 = 2;
const OBJECT_PROPS_STATUS_ETAG_NOT_MATCH: u8 = 3;
const OBJECT_PROPS_STATUS_FILTER_OUT: u8 = 4;

pub const S3_TASK_CONTEXT_DIR_LEFT: u8 = OBJECT_PROPS_FLAG_DIR_LEFT;
pub const S3_TASK_CONTEXT_DIR_RIGHT: u8 = OBJECT_PROPS_FLAG_DIR_RIGHT;
pub const S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE: u8 = OBJECT_PROPS_FLAG_DIR_LEFT;
pub const S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE: u8 =
    OBJECT_PROPS_FLAG_DIR_LEFT | OBJECT_PROPS_FLAG_DIFF_MODE;
pub const S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE: u8 =
    OBJECT_PROPS_FLAG_DIR_RIGHT | OBJECT_PROPS_FLAG_DIFF_MODE;

/// Global filter engine (initialised once from config or CLI).
pub(crate) static OBJECT_FILTER: OnceLock<ObjectFilter> = OnceLock::new();

// ── RunMode ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum RunMode {
    List,
    BiDir,
}

// ── MatchResult ────────────────────────────────────────────

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MatchResult {
    Equal = 0,
    Plus = 1,
    Minus = 2,
    Astrisk = 3,
    Dup = 4,
    Ignore = 5,
}

// ── ObjectKey ──────────────────────────────────────────────

pub type ObjectName = String;
pub type ObjectPrefix = String;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Decode `prefix/name` → `(prefix, name)`. Top-level objects use `"/"` as prefix.
    pub fn decode(&self) -> (ObjectPrefix, ObjectName) {
        self.0
            .rsplit_once('/')
            .map_or(("/".to_owned(), self.0.to_owned()), |(p, n)| {
                (p.to_owned(), n.to_owned())
            })
    }

    /// Consuming variant of [`decode`](Self::decode): reuses this key's
    /// allocation for the prefix, so only the name is newly allocated.
    pub fn into_decoded(self) -> (ObjectPrefix, ObjectName) {
        match self.0.rfind('/') {
            Some(pos) => {
                let mut prefix = self.0;
                let name = prefix.split_off(pos + 1);
                prefix.truncate(pos);
                (prefix, name)
            }
            None => ("/".to_owned(), self.0),
        }
    }

    /// Borrow just the `prefix` portion of this key. Top-level objects use `"/"` as prefix.
    pub fn prefix(&self) -> &str {
        self.0.rsplit_once('/').map_or("/", |(p, _)| p)
    }

    pub fn encode(prefix: &ObjectPrefix, name: &ObjectName) -> Self {
        if prefix == "/" {
            return Self(name.to_string());
        }
        Self(format!("{}/{}", prefix, name))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[allow(dead_code)] // Phase 5: used in serialisation/encoding paths
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<&str> for ObjectKey {
    fn from(item: &str) -> Self {
        Self(item.to_string())
    }
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── ObjectProps ────────────────────────────────────────────

/// Compact object metadata.  Layout is carefully aligned for cache-line behaviour.
#[repr(align(8))]
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ObjectProps {
    #[serde(skip)]
    pub(crate) flags: u8,
    #[serde(skip)]
    pub(crate) status: u8,
    #[serde(skip)]
    #[allow(dead_code)] // Phase 5: alignment padding for cache-line behaviour
    pub(crate) pad: u16,
    #[serde(skip)]
    pub(crate) etag_parts: u32,
    pub last_modified: u64,
    pub size: u64,
    #[serde(skip)]
    pub(crate) etag_md5: [u8; 16],
}

impl ObjectProps {
    /// Create a new ObjectProps with OPEN status and the given direction flags.
    /// For use in tests and when constructing objects programmatically.
    pub fn new_open(dir: u8, size: u64, etag: [u8; 16]) -> Self {
        Self {
            flags: dir,
            status: OBJECT_PROPS_STATUS_OPEN,
            pad: 0,
            etag_parts: 0,
            last_modified: 0,
            size,
            etag_md5: etag,
        }
    }

    pub fn set_dir(&mut self, dir: u8) {
        self.flags |= dir;
    }
    pub fn is_diff_mode(&self) -> bool {
        (self.flags & OBJECT_PROPS_FLAG_DIFF_MODE) == OBJECT_PROPS_FLAG_DIFF_MODE
    }
    pub fn is_left(&self) -> bool {
        (self.flags & OBJECT_PROPS_FLAG_DIR_LEFT) == OBJECT_PROPS_FLAG_DIR_LEFT
    }
    pub fn is_right(&self) -> bool {
        (self.flags & OBJECT_PROPS_FLAG_DIR_RIGHT) == OBJECT_PROPS_FLAG_DIR_RIGHT
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn last_modified(&self) -> u64 {
        self.last_modified
    }

    pub fn is_etag_not_avail(&self) -> bool {
        let (prefix, aligned, suffix) = unsafe { self.etag_md5.align_to::<u128>() };
        prefix.iter().all(|&x| x == 0)
            && suffix.iter().all(|&x| x == 0)
            && aligned.iter().all(|&x| x == 0)
            && self.etag_parts == 0
    }

    pub fn etag(&self) -> ([u8; 16], u32) {
        (self.etag_md5, self.etag_parts)
    }

    pub fn etag_string(&self) -> String {
        if self.etag_parts == 0 {
            hex::encode(self.etag_md5)
        } else {
            format!("{}-{}", hex::encode(self.etag_md5), self.etag_parts)
        }
    }

    pub fn append_etag_string(&self, out: &mut String) {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        out.reserve(32 + usize::from(self.etag_parts != 0) * 11);
        for byte in self.etag_md5 {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        if self.etag_parts != 0 {
            let _ = std::fmt::Write::write_fmt(out, format_args!("-{}", self.etag_parts));
        }
    }

    pub(crate) fn write_etag_to_buffer<'a>(&self, buf: &'a mut [u8; 43]) -> &'a str {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let mut pos = 0usize;
        for byte in self.etag_md5 {
            buf[pos] = HEX[(byte >> 4) as usize];
            buf[pos + 1] = HEX[(byte & 0x0f) as usize];
            pos += 2;
        }

        if self.etag_parts != 0 {
            buf[pos] = b'-';
            pos += 1;

            let mut parts = self.etag_parts;
            let mut digits = [0u8; 10];
            let mut digit_count = 0usize;
            while parts >= 10 {
                digits[digit_count] = b'0' + (parts % 10) as u8;
                parts /= 10;
                digit_count += 1;
            }
            digits[digit_count] = b'0' + parts as u8;
            digit_count += 1;

            for digit in digits[..digit_count].iter().rev() {
                buf[pos] = *digit;
                pos += 1;
            }
        }

        std::str::from_utf8(&buf[..pos]).expect("ETag formatter emits ASCII")
    }

    pub(crate) fn include_in_list_output(&self) -> bool {
        if self.is_diff_mode() {
            return false;
        }
        if let Some(filter) = OBJECT_FILTER.get() {
            if filter.evaluate(self, None) == Some(false) {
                return false;
            }
        }
        if self.status != OBJECT_PROPS_STATUS_OPEN {
            return false;
        }

        let dir = self.flags & OBJECT_PROPS_FLAG_DIR_BOTH;
        dir == OBJECT_PROPS_FLAG_DIR_LEFT || dir == OBJECT_PROPS_FLAG_DIR_RIGHT
    }

    /// Final status check — used at dump time.
    pub fn final_status_check(&self) -> MatchResult {
        // In list mode, apply optional filter here.
        if !self.is_diff_mode() {
            if let Some(filter) = OBJECT_FILTER.get() {
                if filter.evaluate(self, None) == Some(false) {
                    return MatchResult::Ignore;
                }
            }
        }

        if self.status == OBJECT_PROPS_STATUS_FILTER_OUT {
            return MatchResult::Ignore;
        }
        if matches!(
            self.status,
            OBJECT_PROPS_STATUS_SIZE_NOT_MATCH
                | OBJECT_PROPS_STATUS_ETAG_NOT_AVAIL
                | OBJECT_PROPS_STATUS_ETAG_NOT_MATCH
        ) {
            return self.both_sides_or_ignore(MatchResult::Astrisk);
        }
        if self.status == OBJECT_PROPS_STATUS_MATCH {
            return self.both_sides_or_ignore(MatchResult::Equal);
        }
        if self.status == OBJECT_PROPS_STATUS_OPEN {
            if (self.flags & OBJECT_PROPS_FLAG_DIR_BOTH) == OBJECT_PROPS_FLAG_DIR_BOTH {
                log::warn!(
                    "ObjectProps: flags {} status OPEN but both direction bits are set; ignoring row",
                    self.flags
                );
                return MatchResult::Ignore;
            }
            return if self.is_left() {
                MatchResult::Plus
            } else if self.is_right() {
                MatchResult::Minus
            } else {
                log::warn!(
                    "ObjectProps: flags {} status OPEN but no direction bit is set; ignoring row",
                    self.flags
                );
                MatchResult::Ignore
            };
        }
        log::warn!(
            "ObjectProps: unhandled flags {} status {}; ignoring row",
            self.flags,
            self.status
        );
        MatchResult::Ignore
    }

    fn both_sides_or_ignore(&self, result: MatchResult) -> MatchResult {
        if (self.flags & OBJECT_PROPS_FLAG_DIR_BOTH) == OBJECT_PROPS_FLAG_DIR_BOTH {
            result
        } else {
            log::warn!(
                "ObjectProps: flags {} status {} requires both direction bits; ignoring row",
                self.flags,
                self.status
            );
            MatchResult::Ignore
        }
    }

    /// Match two ObjectProps (from left and right sides).  Self is the accumulator.
    pub fn r#match(&mut self, other: &ObjectProps) -> MatchResult {
        // Already matched both sides → duplicate.
        if (self.flags & OBJECT_PROPS_FLAG_DIR_BOTH) == OBJECT_PROPS_FLAG_DIR_BOTH {
            return MatchResult::Dup;
        }

        // Same-side duplicate — overwrite with latest.
        if (self.is_right() && other.is_right()) || (self.is_left() && other.is_left()) {
            *self = other.clone();
            return MatchResult::Dup;
        }

        let (left, right): (&ObjectProps, &ObjectProps) = if self.is_left() {
            (self, other)
        } else if self.is_right() {
            (other, self)
        } else {
            log::warn!(
                "ObjectProps: cannot match accumulator with no direction bit set; marking row ignored"
            );
            self.status = OBJECT_PROPS_STATUS_FILTER_OUT;
            return MatchResult::Ignore;
        };

        // Apply optional filter in diff mode.
        if let Some(filter) = OBJECT_FILTER.get() {
            if filter.evaluate(left, Some(right)) == Some(false) {
                *self = left.clone();
                self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
                self.status = OBJECT_PROPS_STATUS_FILTER_OUT;
                return MatchResult::Ignore;
            }
        }

        if left.size != right.size {
            *self = left.clone();
            self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
            self.status = OBJECT_PROPS_STATUS_SIZE_NOT_MATCH;
            return MatchResult::Astrisk;
        }
        if left.is_etag_not_avail() || right.is_etag_not_avail() {
            *self = left.clone();
            self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
            self.status = OBJECT_PROPS_STATUS_ETAG_NOT_AVAIL;
            return MatchResult::Astrisk;
        }
        if left.etag() != right.etag() {
            *self = left.clone();
            self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
            self.status = OBJECT_PROPS_STATUS_ETAG_NOT_MATCH;
            return MatchResult::Astrisk;
        }

        *self = left.clone();
        self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
        self.status = OBJECT_PROPS_STATUS_MATCH;
        MatchResult::Equal
    }
}

impl From<&aws_sdk_s3::types::Object> for ObjectProps {
    fn from(item: &aws_sdk_s3::types::Object) -> Self {
        let mut md5 = [0u8; 16];
        let (etag_md5, etag_parts) = item.e_tag().map_or((md5, 0), |x| {
            if x.len() == 34 {
                if hex::decode_to_slice(&x[1..33], &mut md5).is_ok() {
                    return (md5, 0);
                }
            } else if x.len() >= 36 && x.chars().nth(33) == Some('-') {
                if hex::decode_to_slice(&x[1..33], &mut md5).is_ok() {
                    if let Ok(parts) = &x[34..x.len() - 1].parse::<usize>() {
                        return (md5, *parts as u32);
                    }
                }
            }
            (md5, 0) // fallback: unparseable etag
        });
        Self {
            flags: OBJECT_PROPS_FLAG_S3_GP_BUCKET,
            status: OBJECT_PROPS_STATUS_OPEN,
            pad: 0,
            etag_parts,
            last_modified: item.last_modified().map_or(0, |x| x.secs() as u64),
            size: item.size().map_or(0, |x| x as u64),
            etag_md5,
        }
    }
}

// ── ObjectFilter ───────────────────────────────────────────

pub struct ObjectFilter {
    /// A compiled predicate that returns `Some(bool)` or `None` on error.
    predicate: Box<dyn Fn(&ObjectProps, Option<&ObjectProps>) -> Option<bool> + Send + Sync>,
}

impl std::fmt::Debug for ObjectFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectFilter").finish_non_exhaustive()
    }
}

impl ObjectFilter {
    /// Create a new ObjectFilter from a compiled predicate.
    pub(crate) fn new(
        predicate: Box<dyn Fn(&ObjectProps, Option<&ObjectProps>) -> Option<bool> + Send + Sync>,
    ) -> Self {
        Self { predicate }
    }

    #[allow(dead_code)] // Phase 5: legacy stub; real compile in config::compile_filter_with_mode
    pub fn compile(expr: &str) -> Result<Self, String> {
        // Deferred to Phase 2 (config) — for now a no-op filter.
        // The full Rhai-based filter engine from s3-fast-list will be wired
        // in Phase 2 once config loading is in place.
        let _ = expr;
        Ok(Self::new(Box::new(|_, _| Some(true))))
    }

    pub fn evaluate(&self, source: &ObjectProps, target: Option<&ObjectProps>) -> Option<bool> {
        (self.predicate)(source, target)
    }
}

// ── Task rendezvous ────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct TaskRendezvous {
    barrier: Arc<Barrier>,
}

impl TaskRendezvous {
    fn new(tasks_count: usize) -> Self {
        Self {
            barrier: Arc::new(Barrier::new(tasks_count)),
        }
    }
    async fn wait(&self) {
        let _ = self.barrier.wait().await;
    }
}

// ── GlobalState ────────────────────────────────────────────

const TASK_STATUS_BIT_LEFT: usize = 0x1;
const TASK_STATUS_BIT_RIGHT: usize = 0x2;
const TASK_STATUS_BIT_DATA_MAP: usize = 0x4;
const TASK_STATUS_BIT_MON: usize = 0x8;

#[derive(Clone)]
pub struct GlobalState {
    pub state: Arc<AtomicUsize>,
    pub quit: Arc<AtomicBool>,
    pub tracker: Arc<HttpStatusCodeTracker>,
    pub task_next_stream_timeout_count: Arc<AtomicUsize>,
    pub s3_client_timeout_count: Arc<AtomicUsize>,
    pub s3_client_generic_error_count: Arc<AtomicUsize>,
    pub fatal_error_count: Arc<AtomicUsize>,
    pub output_error_count: Arc<AtomicUsize>,
    pub data_received_batches: Arc<AtomicUsize>,
    pub data_received_objects: Arc<AtomicUsize>,
    pub data_streamed_rows: Arc<AtomicUsize>,
    pub data_unique_prefixes: Arc<AtomicUsize>,
    pub data_parquet_rows: Arc<AtomicUsize>,
    pub data_ks_entries: Arc<AtomicUsize>,
    pub data_bytes_total: Arc<AtomicU64>,
    pub data_top_prefixes: Arc<Mutex<Vec<PrefixMetric>>>,
    pub data_summary_only: Arc<AtomicBool>,
    pub(crate) task_rendez: TaskRendezvous,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PrefixMetric {
    pub prefix: String,
    pub objects: usize,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct RunMetricsSnapshot {
    pub fatal_errors: usize,
    pub output_errors: usize,
    pub stream_timeouts: usize,
    pub s3_client_timeouts: usize,
    pub s3_client_generic_errors: usize,
    pub data_received_batches: usize,
    pub data_received_objects: usize,
    pub data_streamed_rows: usize,
    pub data_unique_prefixes: usize,
    pub data_parquet_rows: usize,
    pub data_ks_entries: usize,
    pub data_bytes_total: u64,
    pub data_top_prefixes: Vec<PrefixMetric>,
    pub data_summary_only: bool,
}

impl GlobalState {
    pub fn new(quit: Arc<AtomicBool>, tasks_count: usize) -> Self {
        Self {
            state: Arc::new(AtomicUsize::new(0)),
            quit,
            tracker: Arc::new(HttpStatusCodeTracker::new()),
            task_next_stream_timeout_count: Arc::new(AtomicUsize::new(0)),
            s3_client_timeout_count: Arc::new(AtomicUsize::new(0)),
            s3_client_generic_error_count: Arc::new(AtomicUsize::new(0)),
            fatal_error_count: Arc::new(AtomicUsize::new(0)),
            output_error_count: Arc::new(AtomicUsize::new(0)),
            data_received_batches: Arc::new(AtomicUsize::new(0)),
            data_received_objects: Arc::new(AtomicUsize::new(0)),
            data_streamed_rows: Arc::new(AtomicUsize::new(0)),
            data_unique_prefixes: Arc::new(AtomicUsize::new(0)),
            data_parquet_rows: Arc::new(AtomicUsize::new(0)),
            data_ks_entries: Arc::new(AtomicUsize::new(0)),
            data_bytes_total: Arc::new(AtomicU64::new(0)),
            data_top_prefixes: Arc::new(Mutex::new(Vec::new())),
            data_summary_only: Arc::new(AtomicBool::new(false)),
            task_rendez: TaskRendezvous::new(tasks_count),
        }
    }

    pub async fn wait_to_start(&self) {
        self.task_rendez.wait().await;
    }

    pub fn inc_task_next_stream_timeout(&self) {
        self.task_next_stream_timeout_count
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn read_task_next_stream_timeout(&self) -> usize {
        self.task_next_stream_timeout_count.load(Ordering::Relaxed)
    }
    pub fn inc_s3_client_timeout(&self) {
        self.s3_client_timeout_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn read_s3_client_timeout(&self) -> usize {
        self.s3_client_timeout_count.load(Ordering::Relaxed)
    }
    pub fn inc_s3_client_generic_error(&self) {
        self.s3_client_generic_error_count
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn read_s3_client_generic_error(&self) -> usize {
        self.s3_client_generic_error_count.load(Ordering::Relaxed)
    }
    pub fn inc_fatal_error(&self) {
        self.fatal_error_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn read_fatal_error(&self) -> usize {
        self.fatal_error_count.load(Ordering::Relaxed)
    }
    pub fn inc_output_error(&self) {
        self.output_error_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn read_output_error(&self) -> usize {
        self.output_error_count.load(Ordering::Relaxed)
    }
    pub fn record_data_metrics(
        &self,
        received_batches: usize,
        received_objects: usize,
        streamed_rows: usize,
        unique_prefixes: usize,
        parquet_rows: usize,
        ks_entries: usize,
        bytes_total: u64,
        top_prefixes: Vec<PrefixMetric>,
        summary_only: bool,
    ) {
        self.data_received_batches
            .store(received_batches, Ordering::Relaxed);
        self.data_received_objects
            .store(received_objects, Ordering::Relaxed);
        self.data_streamed_rows
            .store(streamed_rows, Ordering::Relaxed);
        self.data_unique_prefixes
            .store(unique_prefixes, Ordering::Relaxed);
        self.data_parquet_rows.store(parquet_rows, Ordering::Relaxed);
        self.data_ks_entries.store(ks_entries, Ordering::Relaxed);
        self.data_bytes_total.store(bytes_total, Ordering::Relaxed);
        *self.data_top_prefixes.lock().unwrap() = top_prefixes;
        self.data_summary_only.store(summary_only, Ordering::Relaxed);
    }
    pub fn metrics_snapshot(&self) -> RunMetricsSnapshot {
        RunMetricsSnapshot {
            fatal_errors: self.read_fatal_error(),
            output_errors: self.read_output_error(),
            stream_timeouts: self.read_task_next_stream_timeout(),
            s3_client_timeouts: self.read_s3_client_timeout(),
            s3_client_generic_errors: self.read_s3_client_generic_error(),
            data_received_batches: self.data_received_batches.load(Ordering::Relaxed),
            data_received_objects: self.data_received_objects.load(Ordering::Relaxed),
            data_streamed_rows: self.data_streamed_rows.load(Ordering::Relaxed),
            data_unique_prefixes: self.data_unique_prefixes.load(Ordering::Relaxed),
            data_parquet_rows: self.data_parquet_rows.load(Ordering::Relaxed),
            data_ks_entries: self.data_ks_entries.load(Ordering::Relaxed),
            data_bytes_total: self.data_bytes_total.load(Ordering::Relaxed),
            data_top_prefixes: self.data_top_prefixes.lock().unwrap().clone(),
            data_summary_only: self.data_summary_only.load(Ordering::Relaxed),
        }
    }
    pub fn get_tracker(&self) -> Arc<HttpStatusCodeTracker> {
        Arc::clone(&self.tracker)
    }

    fn start(&self, mask: usize) {
        self.state.fetch_or(mask, Ordering::SeqCst);
    }
    fn complete(&self, mask: usize) {
        self.state.fetch_and(!mask, Ordering::SeqCst);
    }
    pub fn is_running(&self, mask: usize) -> bool {
        self.state.load(Ordering::SeqCst) & mask != 0
    }
    pub fn quit(&self) {
        self.quit.store(true, Ordering::SeqCst);
    }
    pub fn is_quit(&self) -> bool {
        self.quit.load(Ordering::SeqCst)
    }

    pub fn list_task_start(&self, dir: u8) {
        match dir {
            S3_TASK_CONTEXT_DIR_LEFT | S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE => {
                self.start(TASK_STATUS_BIT_LEFT)
            }
            S3_TASK_CONTEXT_DIR_RIGHT | S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE => {
                self.start(TASK_STATUS_BIT_RIGHT)
            }
            _ => log::error!("list_task_start: unknown dir {}", dir),
        }
    }
    pub fn list_task_complete(&self, dir: u8) {
        match dir {
            S3_TASK_CONTEXT_DIR_LEFT | S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE => {
                self.complete(TASK_STATUS_BIT_LEFT)
            }
            S3_TASK_CONTEXT_DIR_RIGHT | S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE => {
                self.complete(TASK_STATUS_BIT_RIGHT)
            }
            _ => log::error!("list_task_complete: unknown dir {}", dir),
        }
    }
    pub fn list_task_is_running(&self, dir: u8) -> bool {
        match dir {
            S3_TASK_CONTEXT_DIR_LEFT | S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE => {
                self.is_running(TASK_STATUS_BIT_LEFT)
            }
            S3_TASK_CONTEXT_DIR_RIGHT | S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE => {
                self.is_running(TASK_STATUS_BIT_RIGHT)
            }
            _ => false,
        }
    }
    pub fn all_list_tasks_is_running(&self) -> bool {
        self.is_running(TASK_STATUS_BIT_LEFT | TASK_STATUS_BIT_RIGHT)
    }
    pub fn data_map_task_start(&self) {
        self.start(TASK_STATUS_BIT_DATA_MAP);
    }
    pub fn data_map_task_complete(&self) {
        self.complete(TASK_STATUS_BIT_DATA_MAP);
    }
    #[allow(dead_code)] // Phase 5: used in task-coordination diagnostics
    pub fn data_map_task_is_running(&self) -> bool {
        self.is_running(TASK_STATUS_BIT_DATA_MAP)
    }
    pub fn mon_task_start(&self) {
        self.start(TASK_STATUS_BIT_MON);
    }
    pub fn mon_task_complete(&self) {
        self.complete(TASK_STATUS_BIT_MON);
    }
}

/// Build an S3 client from a shared SDK config plus per-run overrides.
pub fn build_s3_client(
    sdk_config: &aws_config::SdkConfig,
    region: Option<&str>,
    endpoint: Option<&str>,
    force_path_style: bool,
) -> aws_sdk_s3::Client {
    let mut builder = aws_sdk_s3::config::Builder::from(sdk_config);
    if let Some(region_str) = region {
        builder = builder.region(aws_sdk_s3::config::Region::new(region_str.to_owned()));
    }
    if let Some(endpoint_url) = endpoint {
        builder = builder.endpoint_url(endpoint_url.to_owned());
    }
    if force_path_style {
        builder = builder.force_path_style(true);
    }
    aws_sdk_s3::Client::from_conf(builder.build())
}

// ── S3TaskContext ──────────────────────────────────────────

#[derive(Clone)]
pub struct S3TaskContext {
    pub s3_bucket_name: String,
    pub s3_client: aws_sdk_s3::Client,
    pub data_map_channel: mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>,
    pub dir: u8,
    pub g_state: GlobalState,
    // Trace / S3-compat observability
    pub trace_writer: Option<Arc<dyn S3TraceWriter>>,
    pub endpoint_url: String,
    pub region: Option<String>,
    pub addressing_style: String,
    pub profile: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<i32>,
    pub max_attempts: u32,
    pub operation_timeout_secs: u64,
    /// CLI `--start-after` override — if set, the first segment uses this
    /// instead of the hint-segment start.
    pub start_after: Option<String>,
    /// CLI `--continuation-token` override for a single ListObjectsV2 chain.
    pub continuation_token: Option<String>,
    pub checkpoint_completed: Arc<Mutex<Vec<usize>>>,
}

impl S3TaskContext {
    pub async fn load_sdk_config(s3_config: &S3Config) -> aws_config::SdkConfig {
        aws_config::from_env()
            .retry_config(
                aws_config::retry::RetryConfig::standard()
                    .with_max_attempts(s3_config.max_attempts)
                    .with_initial_backoff(std::time::Duration::from_secs(
                        s3_config.initial_backoff_secs,
                    )),
            )
            .timeout_config(
                aws_config::timeout::TimeoutConfigBuilder::new()
                    .connect_timeout(std::time::Duration::from_secs(
                        s3_config.connect_timeout_secs,
                    ))
                    .operation_timeout(std::time::Duration::from_secs(
                        s3_config.operation_timeout_secs,
                    ))
                    .read_timeout(std::time::Duration::from_secs(
                        s3_config.operation_timeout_secs,
                    ))
                    .operation_attempt_timeout(std::time::Duration::from_secs(
                        s3_config.operation_timeout_secs,
                    ))
                    .build(),
            )
            .load()
            .await
    }

    pub fn new(
        bucket: &str,
        region: Option<&str>,
        endpoint: Option<&str>,
        force_path_style: bool,
        sdk_config: &aws_config::SdkConfig,
        s3_config: &S3Config,
        data_map_channel: mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>,
        dir: u8,
        g_state: GlobalState,
        trace_writer: Option<Arc<dyn S3TraceWriter>>,
        addressing_style: &str,
        profile: Option<&str>,
        delimiter: Option<&str>,
        max_keys: Option<i32>,
        start_after: Option<&str>,
        continuation_token: Option<&str>,
        checkpoint_completed: Arc<Mutex<Vec<usize>>>,
    ) -> Self {
        let s3_client = build_s3_client(sdk_config, region, endpoint, force_path_style);

        Self {
            s3_bucket_name: bucket.to_string(),
            s3_client,
            data_map_channel,
            dir,
            g_state,
            trace_writer,
            endpoint_url: endpoint.unwrap_or("https://s3.amazonaws.com").to_string(),
            region: region.map(|r| r.to_string()),
            addressing_style: addressing_style.to_string(),
            profile: profile.map(|p| p.to_string()),
            delimiter: delimiter.map(|d| d.to_string()),
            max_keys,
            max_attempts: s3_config.max_attempts.max(1),
            operation_timeout_secs: s3_config.operation_timeout_secs.max(1),
            start_after: start_after.map(|s| s.to_string()),
            continuation_token: continuation_token.map(|s| s.to_string()),
            checkpoint_completed,
        }
    }

    pub fn get_tracker(&self) -> Arc<HttpStatusCodeTracker> {
        self.g_state.get_tracker()
    }
    pub fn start(&self) {
        self.g_state.list_task_start(self.dir);
    }
    pub fn complete(&self) {
        self.g_state.list_task_complete(self.dir);
    }
    pub fn is_running(&self) -> bool {
        self.g_state.list_task_is_running(self.dir)
    }
    pub fn is_quit(&self) -> bool {
        self.g_state.is_quit()
    }
}

// ── DataMapContext ─────────────────────────────────────────

pub struct DataMapContext {
    pub data_map_channel: mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
    pub g_state: GlobalState,
}

impl DataMapContext {
    pub fn new(
        data_map_channel: mpsc::Receiver<Vec<(ObjectKey, ObjectProps)>>,
        g_state: GlobalState,
    ) -> Self {
        Self {
            data_map_channel,
            g_state,
        }
    }
    pub fn start(&self) {
        self.g_state.data_map_task_start();
    }
    pub fn complete(&self) {
        self.g_state.data_map_task_complete();
    }
    pub fn quit(&self) {
        self.g_state.quit();
    }
    pub fn is_quit(&self) -> bool {
        self.g_state.is_quit()
    }
    pub fn all_list_tasks_is_running(&self) -> bool {
        self.g_state.all_list_tasks_is_running()
    }
}

// ── MonContext ─────────────────────────────────────────────

pub struct MonContext {
    pub g_state: GlobalState,
}

impl MonContext {
    pub fn new(g_state: GlobalState) -> Self {
        Self { g_state }
    }
    pub fn get_tracker(&self) -> Arc<HttpStatusCodeTracker> {
        self.g_state.get_tracker()
    }
    pub fn start(&self) {
        self.g_state.mon_task_start();
    }
    pub fn complete(&self) {
        self.g_state.mon_task_complete();
    }
    pub fn is_quit(&self) -> bool {
        self.g_state.is_quit()
    }
}

// ── KeySpace hints ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct KeySpacePair {
    pub index: usize,
    pub start: String,
    pub end: Option<String>,
}

impl KeySpacePair {
    pub fn new(index: usize, start: String, end: String) -> Self {
        let e = if end.is_empty() { None } else { Some(end) };
        Self {
            index,
            start,
            end: e,
        }
    }

    pub fn to_task_input(&self) -> (&str, Option<&str>) {
        (&self.start, self.end.as_deref())
    }
}

pub struct KeySpaceHints {
    inner: VecDeque<KeySpacePair>,
    inflight: HashMap<usize, KeySpacePair>,
    done: Vec<KeySpacePair>,
}

/// Defence-in-depth: log a warning for every boundary that looks like leaked
/// TOML syntax.  The hints loader should already reject these, but this guard
/// catches any path that reaches `new_from` with raw TOML still present.
fn warn_on_suspicious_boundaries(hints: &[String]) {
    for (i, b) in hints.iter().enumerate() {
        // Leading whitespace that isn't a legitimate key character.
        if b.starts_with(' ') || b.starts_with('\t') {
            log::warn!(
                "KeySpaceHints boundary {} has leading whitespace: '{}'. \
                 This will be sent verbatim as an S3 start_after value and may \
                 cause request failures.",
                i,
                b
            );
        }
        // Trailing comma — classic TOML array entry leakage.
        if b.ends_with(',') {
            log::warn!(
                "KeySpaceHints boundary {} ends with a comma: '{}'. \
                 This looks like leaked TOML array syntax.",
                i,
                b
            );
        }
        // Surrounded by quotes — another TOML leakage pattern.
        if (b.starts_with('"') && b.ends_with('"')) || (b.starts_with('\'') && b.ends_with('\'')) {
            log::warn!(
                "KeySpaceHints boundary {} is quoted: '{}'. \
                 This looks like leaked TOML string syntax.",
                i,
                b
            );
        }
        // TOML array-open or array-close.
        if b == "[" || b == "]" {
            log::warn!(
                "KeySpaceHints boundary {} is a TOML bracket: '{}'. \
                 This will produce invalid S3 start_after values.",
                i,
                b
            );
        }
        // TOML key = value assignment.
        if b.contains('=') {
            log::warn!(
                "KeySpaceHints boundary {} contains '=': '{}'. \
                 This looks like a TOML assignment line, not an object key.",
                i,
                b
            );
        }
    }
}

impl KeySpaceHints {
    pub fn new_from(hints: &[String]) -> Self {
        // Defensive validation: warn on any boundary that looks like leaked TOML
        // syntax.  The hints loader should already have rejected these, but this
        // guard catches any path that bypasses the loader.
        warn_on_suspicious_boundaries(hints);

        let v = Self::pairs_from_boundaries(hints);
        Self {
            inner: v,
            inflight: HashMap::new(),
            done: Vec::new(),
        }
    }

    pub fn new_uncompleted_from(hints: &[String], completed_indices: &[usize]) -> Self {
        warn_on_suspicious_boundaries(hints);

        let completed: HashSet<usize> = completed_indices.iter().copied().collect();
        let v = Self::pairs_from_boundaries(hints)
            .into_iter()
            .filter(|pair| !completed.contains(&pair.index))
            .collect();
        Self {
            inner: v,
            inflight: HashMap::new(),
            done: Vec::new(),
        }
    }

    fn pairs_from_boundaries(hints: &[String]) -> VecDeque<KeySpacePair> {
        let mut v = VecDeque::new();
        if hints.is_empty() {
            // No hints → single segment covering everything.
            v.push_back(KeySpacePair::new(0, String::new(), String::new()));
            return v;
        }

        let mut start = String::new();
        for (i, key) in hints.iter().enumerate() {
            v.push_back(KeySpacePair::new(i, start.clone(), key.clone()));
            start = key.clone();
        }
        v.push_back(KeySpacePair::new(hints.len(), start, String::new()));
        v
    }

    pub fn next(&mut self) -> Option<KeySpacePair> {
        if let Some(pair) = self.inner.pop_front() {
            self.inflight.insert(pair.index, pair.clone());
            return Some(pair);
        }
        None
    }

    pub fn finish(&mut self, index: usize) {
        if let Some(pair) = self.inflight.remove(&index) {
            self.done.push(pair);
            return;
        }
        log::warn!("KeySpaceHints::finish: index {} not in inflight", index);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
    #[allow(dead_code)] // Phase 5: used in integration tests
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn done_count(&self) -> usize {
        self.done.len()
    }
    pub fn total_count(&self) -> usize {
        self.done.len() + self.inflight.len() + self.inner.len()
    }

    /// Return completed segments for checkpoint persistence.
    #[allow(dead_code)] // Phase 5: used in integration tests
    pub fn completed_indices(&self) -> Vec<usize> {
        self.done.iter().map(|p| p.index).collect()
    }
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_key_top_level() {
        let key = ObjectKey::from("test.jpg");
        let (prefix, name) = key.decode();
        assert_eq!(prefix, "/");
        assert_eq!(key.prefix(), "/");
        assert_eq!(name, "test.jpg");
        let rebuilt = ObjectKey::encode(&prefix, &name);
        assert_eq!(rebuilt.as_str(), "test.jpg");
    }

    #[test]
    fn test_object_key_nested() {
        let key = ObjectKey::from("a/b/c/test.jpg");
        let (prefix, name) = key.decode();
        assert_eq!(prefix, "a/b/c");
        assert_eq!(key.prefix(), "a/b/c");
        assert_eq!(name, "test.jpg");
        let rebuilt = ObjectKey::encode(&prefix, &name);
        assert_eq!(rebuilt.as_str(), "a/b/c/test.jpg");
    }

    #[test]
    fn test_object_key_into_decoded_matches_decode() {
        for raw in ["test.jpg", "a/b/c/test.jpg", "dir/", "a//b", "/leading"] {
            let key = ObjectKey::from(raw);
            assert_eq!(key.decode(), ObjectKey::from(raw).into_decoded(), "{raw}");
        }
    }

    #[test]
    fn test_key_space_hints_empty() {
        let hints = KeySpaceHints::new_from(&[]);
        assert_eq!(hints.total_count(), 1);
    }

    #[test]
    fn test_key_space_hints_with_boundaries() {
        let boundaries = vec!["b/".to_string(), "d/".to_string()];
        let mut hints = KeySpaceHints::new_from(&boundaries);
        assert_eq!(hints.total_count(), 3); // ""→b/, b/→d/, d/→""

        let p0 = hints.next().unwrap();
        assert_eq!(p0.start, "");
        assert_eq!(p0.end.as_deref(), Some("b/"));

        let p1 = hints.next().unwrap();
        assert_eq!(p1.start, "b/");
        assert_eq!(p1.end.as_deref(), Some("d/"));

        let p2 = hints.next().unwrap();
        assert_eq!(p2.start, "d/");
        assert_eq!(p2.end, None);

        assert!(hints.next().is_none());
    }

    #[test]
    fn test_key_space_hints_uncompleted_preserves_original_segment_starts() {
        let boundaries = vec!["m/".to_string()];
        let mut hints = KeySpaceHints::new_uncompleted_from(&boundaries, &[0]);
        assert_eq!(hints.total_count(), 1);

        let remaining = hints.next().unwrap();
        assert_eq!(remaining.index, 1);
        assert_eq!(remaining.start, "m/");
        assert_eq!(remaining.end, None);
        assert!(hints.next().is_none());
    }

    #[test]
    fn test_key_space_hints_start_after_clean() {
        // Verify that generated start_after values are the exact boundary
        // strings — no leading whitespace, no quotes, no trailing commas.
        let boundaries = vec![
            "alpha/".to_string(),
            "beta/file-05.txt".to_string(),
            "logs/file with spaces.log".to_string(),
            "logs/file+plus.log".to_string(),
            "中文/Unicode".to_string(),
        ];
        let mut hints = KeySpaceHints::new_from(&boundaries);

        // Segment 0: "" → "alpha/"
        let p0 = hints.next().unwrap();
        assert_eq!(p0.start, "");
        assert_eq!(p0.end.as_deref(), Some("alpha/"));

        // Segment 1: "alpha/" → "beta/file-05.txt"
        let p1 = hints.next().unwrap();
        assert_eq!(p1.start, "alpha/");
        assert_eq!(p1.end.as_deref(), Some("beta/file-05.txt"));

        // Segment 2: "beta/file-05.txt" → "logs/file with spaces.log"
        let p2 = hints.next().unwrap();
        assert_eq!(p2.start, "beta/file-05.txt");
        assert_eq!(p2.end.as_deref(), Some("logs/file with spaces.log"));

        // Segment 3: "logs/file with spaces.log" → "logs/file+plus.log"
        let p3 = hints.next().unwrap();
        assert_eq!(p3.start, "logs/file with spaces.log");
        assert_eq!(p3.end.as_deref(), Some("logs/file+plus.log"));

        // Segment 4: "logs/file+plus.log" → "中文/Unicode"
        let p4 = hints.next().unwrap();
        assert_eq!(p4.start, "logs/file+plus.log");
        assert_eq!(p4.end.as_deref(), Some("中文/Unicode"));

        // Segment 5: "中文/Unicode" → (none)
        let p5 = hints.next().unwrap();
        assert_eq!(p5.start, "中文/Unicode");
        assert_eq!(p5.end, None);

        assert!(hints.next().is_none());
    }

    #[test]
    fn test_key_space_hints_with_special_chars_preserved() {
        // Verify spaces, +, /, %, Chinese/Unicode are preserved.
        let boundaries = vec![
            "a b/key.log".to_string(),
            "a+b/key.log".to_string(),
            "a%b/key.log".to_string(),
            "中文/key.log".to_string(),
        ];
        let mut hints = KeySpaceHints::new_from(&boundaries);

        let p = hints.next().unwrap();
        assert_eq!(p.start, "");
        assert_eq!(p.end.as_deref(), Some("a b/key.log"));

        let p = hints.next().unwrap();
        assert_eq!(p.start, "a b/key.log");
        assert_eq!(p.end.as_deref(), Some("a+b/key.log"));

        let p = hints.next().unwrap();
        assert_eq!(p.start, "a+b/key.log");
        assert_eq!(p.end.as_deref(), Some("a%b/key.log"));

        let p = hints.next().unwrap();
        assert_eq!(p.start, "a%b/key.log");
        assert_eq!(p.end.as_deref(), Some("中文/key.log"));
    }

    #[test]
    fn test_object_props_match_both_sides() {
        let mut left = ObjectProps::default();
        left.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        left.size = 100;
        left.etag_md5 = [0xabu8; 16];

        let mut right = ObjectProps::default();
        right.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        right.size = 100;
        right.etag_md5 = [0xabu8; 16];

        let result = left.r#match(&right);
        assert_eq!(result, MatchResult::Equal);
    }

    #[test]
    fn test_object_props_final_status_open_without_direction_is_ignored() {
        let mut props = ObjectProps::default();
        props.status = OBJECT_PROPS_STATUS_OPEN;
        assert_eq!(props.final_status_check(), MatchResult::Ignore);
    }

    #[test]
    fn test_object_props_list_output_includes_open_list_object() {
        let props = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 100, [1u8; 16]);
        assert!(props.include_in_list_output());
    }

    #[test]
    fn test_object_props_list_output_excludes_filter_out_status() {
        let mut props = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 100, [1u8; 16]);
        props.status = OBJECT_PROPS_STATUS_FILTER_OUT;
        assert!(!props.include_in_list_output());
    }

    #[test]
    fn test_object_props_list_output_excludes_invalid_direction_state() {
        let mut props = ObjectProps::default();
        props.status = OBJECT_PROPS_STATUS_OPEN;
        assert!(!props.include_in_list_output());

        props.set_dir(S3_TASK_CONTEXT_DIR_LEFT | S3_TASK_CONTEXT_DIR_RIGHT);
        assert!(!props.include_in_list_output());
    }

    #[test]
    fn test_object_props_list_output_excludes_diff_mode() {
        let props = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, 100, [1u8; 16]);
        assert!(!props.include_in_list_output());
    }

    #[test]
    fn test_object_props_fast_etag_matches_plain_etag_string() {
        let props = ObjectProps::new_open(
            S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE,
            100,
            [
                0x00, 0x01, 0x02, 0x03, 0x0a, 0x0b, 0x0c, 0x0d, 0x10, 0x11, 0x12, 0x13, 0xfa, 0xfb,
                0xfc, 0xff,
            ],
        );
        let mut buf = [0u8; 43];
        assert_eq!(props.write_etag_to_buffer(&mut buf), props.etag_string());
    }

    #[test]
    fn test_object_props_fast_etag_matches_multipart_etag_string() {
        let mut props =
            ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 100, [0xabu8; 16]);
        props.etag_parts = 12_345;
        let mut buf = [0u8; 43];
        assert_eq!(props.write_etag_to_buffer(&mut buf), props.etag_string());
    }

    #[test]
    fn test_object_props_fast_etag_matches_max_parts_etag_string() {
        let mut props =
            ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 100, [0xffu8; 16]);
        props.etag_parts = u32::MAX;
        let mut buf = [0u8; 43];
        assert_eq!(props.write_etag_to_buffer(&mut buf), props.etag_string());
    }

    #[test]
    fn test_object_props_fast_etag_matches_zero_etag_string() {
        let props = ObjectProps::new_open(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 100, [0u8; 16]);
        let mut buf = [0u8; 43];
        assert_eq!(props.write_etag_to_buffer(&mut buf), props.etag_string());
    }

    #[test]
    fn test_object_props_final_status_unknown_status_is_ignored() {
        let mut props = ObjectProps::default();
        props.set_dir(S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE);
        props.status = 99;
        assert_eq!(props.final_status_check(), MatchResult::Ignore);
    }

    #[test]
    fn test_object_props_final_status_match_without_both_sides_is_ignored() {
        let props = ObjectProps::default();
        assert_eq!(props.final_status_check(), MatchResult::Ignore);
    }

    #[test]
    fn test_object_props_match_without_accumulator_direction_is_ignored() {
        let mut accumulator = ObjectProps::default();
        accumulator.status = OBJECT_PROPS_STATUS_OPEN;
        let mut right = ObjectProps::default();
        right.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        right.status = OBJECT_PROPS_STATUS_OPEN;

        let result = accumulator.r#match(&right);
        assert_eq!(result, MatchResult::Ignore);
        assert_eq!(accumulator.final_status_check(), MatchResult::Ignore);
    }

    #[test]
    fn test_object_props_match_size_diff() {
        let mut left = ObjectProps::default();
        left.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        left.size = 100;

        let mut right = ObjectProps::default();
        right.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        right.size = 200;

        let result = left.r#match(&right);
        assert_eq!(result, MatchResult::Astrisk);
    }

    #[test]
    fn test_object_props_duplicate_same_side() {
        let mut first = ObjectProps::default();
        first.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        first.size = 100;

        let mut second = ObjectProps::default();
        second.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        second.size = 200;

        let result = first.r#match(&second);
        assert_eq!(result, MatchResult::Dup);
        assert_eq!(first.size, 200); // overwritten
    }

    #[test]
    fn test_object_props_match_etag_not_avail_both_default() {
        // Both sides have default (all-zero) etag → is_etag_not_avail() true.
        let mut left = ObjectProps::default();
        left.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        left.size = 100;

        let mut right = ObjectProps::default();
        right.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        right.size = 100;

        assert!(left.is_etag_not_avail());
        assert!(right.is_etag_not_avail());

        let result = left.r#match(&right);
        assert_eq!(result, MatchResult::Astrisk);
        assert_eq!(left.status, OBJECT_PROPS_STATUS_ETAG_NOT_AVAIL);
    }

    #[test]
    fn test_object_props_match_etag_not_avail_one_side_default() {
        // Left has a real etag, right has default (all-zero) → one side etag not available.
        let mut left = ObjectProps::default();
        left.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        left.size = 100;
        left.etag_md5 = [0xabu8; 16];

        let mut right = ObjectProps::default();
        right.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        right.size = 100;
        // right.etag_md5 remains all-zero

        assert!(!left.is_etag_not_avail());
        assert!(right.is_etag_not_avail());

        let result = left.r#match(&right);
        assert_eq!(result, MatchResult::Astrisk);
        assert_eq!(left.status, OBJECT_PROPS_STATUS_ETAG_NOT_AVAIL);
    }

    #[test]
    fn test_object_props_match_etag_not_match() {
        // Same size, different etags → ETAG_NOT_MATCH.
        let mut left = ObjectProps::default();
        left.set_dir(S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE);
        left.size = 100;
        left.etag_md5 = [0xabu8; 16];

        let mut right = ObjectProps::default();
        right.set_dir(S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE);
        right.size = 100;
        right.etag_md5 = [0x42u8; 16];

        assert!(!left.is_etag_not_avail());
        assert!(!right.is_etag_not_avail());

        let result = left.r#match(&right);
        assert_eq!(result, MatchResult::Astrisk);
        assert_eq!(left.status, OBJECT_PROPS_STATUS_ETAG_NOT_MATCH);
    }
}
