use crate::config::S3Config;
use crate::stats::HttpStatusCodeTracker;
use crate::trace::S3TraceWriter;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::mpsc;
use tokio::sync::Barrier;

// ── Constants ──────────────────────────────────────────────

pub(crate) const KB: usize = 1024;
pub(crate) const MB: usize = 1_048_576;
pub(crate) const GB: usize = 1_073_741_824;

pub(crate) const DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS: u64 = 5;
pub(crate) const DEFAULT_TASK_COMPLETE_QUIT_WAIT_SECS: u64 = 1;

// ── ObjectProps: bit-flag state machine ────────────────────

const OBJECT_PROPS_FLAG_S3_GP_BUCKET: u8 = 0b1;
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

pub(crate) const S3_TASK_CONTEXT_DIR_LEFT: u8 = OBJECT_PROPS_FLAG_DIR_LEFT;
pub(crate) const S3_TASK_CONTEXT_DIR_RIGHT: u8 = OBJECT_PROPS_FLAG_DIR_RIGHT;
pub(crate) const S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE: u8 = OBJECT_PROPS_FLAG_DIR_LEFT;
pub(crate) const S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE: u8 =
    OBJECT_PROPS_FLAG_DIR_LEFT | OBJECT_PROPS_FLAG_DIFF_MODE;
pub(crate) const S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE: u8 =
    OBJECT_PROPS_FLAG_DIR_RIGHT | OBJECT_PROPS_FLAG_DIFF_MODE;

/// Global filter engine (initialised once from config or CLI).
pub(crate) static OBJECT_FILTER: OnceLock<ObjectFilter> = OnceLock::new();

// ── RunMode ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RunMode {
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

    pub fn encode(prefix: &ObjectPrefix, name: &ObjectName) -> Self {
        if prefix == "/" {
            return Self(name.to_string());
        }
        Self(format!("{}/{}", prefix, name))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

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
    pub(crate) pad: u16,
    #[serde(skip)]
    pub(crate) etag_parts: u32,
    pub(crate) last_modified: u64,
    pub(crate) size: u64,
    #[serde(skip)]
    pub(crate) etag_md5: [u8; 16],
}

impl ObjectProps {
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
            return MatchResult::Astrisk;
        }
        if self.status == OBJECT_PROPS_STATUS_MATCH {
            return MatchResult::Equal;
        }
        if self.status == OBJECT_PROPS_STATUS_OPEN {
            assert!((self.flags & OBJECT_PROPS_FLAG_DIR_BOTH) != OBJECT_PROPS_FLAG_DIR_BOTH);
            return if self.is_left() {
                MatchResult::Plus
            } else if self.is_right() {
                MatchResult::Minus
            } else {
                panic!(
                    "ObjectProps: flags {} status OPEN but no dir bit set",
                    self.flags
                )
            };
        }
        panic!(
            "ObjectProps: unhandled flags {} status {}",
            self.flags, self.status
        );
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
        } else {
            assert!(self.is_right());
            (other, self)
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

pub(crate) struct ObjectFilter {
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
struct TaskRendezvous {
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
    pub task_rendez: TaskRendezvous,
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
            task_rendez: TaskRendezvous::new(tasks_count),
        }
    }

    pub async fn wait_to_start(&self) {
        self.task_rendez.wait().await;
    }

    pub fn inc_task_next_stream_timeout(&self) {
        self.task_next_stream_timeout_count
            .fetch_add(1, Ordering::SeqCst);
    }
    pub fn read_task_next_stream_timeout(&self) -> usize {
        self.task_next_stream_timeout_count.load(Ordering::SeqCst)
    }
    pub fn inc_s3_client_timeout(&self) {
        self.s3_client_timeout_count.fetch_add(1, Ordering::SeqCst);
    }
    pub fn read_s3_client_timeout(&self) -> usize {
        self.s3_client_timeout_count.load(Ordering::SeqCst)
    }
    pub fn inc_s3_client_generic_error(&self) {
        self.s3_client_generic_error_count
            .fetch_add(1, Ordering::SeqCst);
    }
    pub fn read_s3_client_generic_error(&self) -> usize {
        self.s3_client_generic_error_count.load(Ordering::SeqCst)
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

// ── S3TaskContext ──────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct S3TaskContext {
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
}

impl S3TaskContext {
    pub fn new(
        bucket: &str,
        region: Option<&str>,
        endpoint: Option<&str>,
        force_path_style: bool,
        s3_config: &S3Config,
        data_map_channel: mpsc::Sender<Vec<(ObjectKey, ObjectProps)>>,
        dir: u8,
        g_state: GlobalState,
        trace_writer: Option<Arc<dyn S3TraceWriter>>,
        addressing_style: &str,
        profile: Option<&str>,
        delimiter: Option<&str>,
        max_keys: Option<i32>,
    ) -> Self {
        let loader = aws_config::from_env()
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
                    .build(),
            );

        let config = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move { loader.load().await })
        });

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&config);
        if let Some(region_str) = region {
            s3_config_builder =
                s3_config_builder.region(aws_sdk_s3::config::Region::new(region_str.to_owned()));
        }
        if let Some(endpoint_url) = endpoint {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint_url.to_owned());
        }
        if force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

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

pub(crate) struct DataMapContext {
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

pub(crate) struct MonContext {
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

impl KeySpaceHints {
    pub fn new_from(hints: &[String]) -> Self {
        let mut v = VecDeque::new();
        if hints.is_empty() {
            // No hints → single segment covering everything.
            v.push_back(KeySpacePair::new(0, String::new(), String::new()));
        } else {
            let mut start = String::new();
            for (i, key) in hints.iter().enumerate() {
                v.push_back(KeySpacePair::new(i, start.clone(), key.clone()));
                start = key.clone();
            }
            v.push_back(KeySpacePair::new(hints.len(), start, String::new()));
        }
        Self {
            inner: v,
            inflight: HashMap::new(),
            done: Vec::new(),
        }
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
        assert_eq!(name, "test.jpg");
        let rebuilt = ObjectKey::encode(&prefix, &name);
        assert_eq!(rebuilt.as_str(), "test.jpg");
    }

    #[test]
    fn test_object_key_nested() {
        let key = ObjectKey::from("a/b/c/test.jpg");
        let (prefix, name) = key.decode();
        assert_eq!(prefix, "a/b/c");
        assert_eq!(name, "test.jpg");
        let rebuilt = ObjectKey::encode(&prefix, &name);
        assert_eq!(rebuilt.as_str(), "a/b/c/test.jpg");
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
