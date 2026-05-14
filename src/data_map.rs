use dashmap::DashMap;
use log::info;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

use crate::core::{self, DataMapContext, MatchResult, ObjectKey, ObjectName, ObjectProps};

// ── Output direction flags ─────────────────────────────────

const OUTPUT_FLAG_EQUAL: u8 = 0;
const OUTPUT_FLAG_PLUS: u8 = 1;
const OUTPUT_FLAG_MINUS: u8 = 2;
const OUTPUT_FLAG_ASTRISK: u8 = 3;

// ── PrefixMap: dashmap-based concurrent object store ───────

pub struct PrefixMap {
    /// dashmap<prefix, ObjectMap> — sharded lock-free.
    inner: DashMap<String, ObjectMap>,
    count: Arc<AtomicUsize>,
}

impl PrefixMap {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn inc_count(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Get or create the ObjectMap for a prefix.
    pub fn get_object_map(&self, prefix: &str) -> ObjectMap {
        if let Some(entry) = self.inner.get(prefix) {
            return entry.clone();
        }
        let new_map = ObjectMap::new();
        match self.inner.entry(prefix.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let map = new_map.clone();
                entry.insert(map.clone());
                self.inc_count();
                map
            }
        }
    }

    /// Bulk insert a batch of objects for a prefix.
    pub fn bulk_insert(&self, prefix: &str, items: Vec<(ObjectName, ObjectProps)>) {
        let obj_map = self.get_object_map(prefix);
        obj_map.bulk_insert(prefix, items);
    }

    pub fn get_stats(&self) -> (usize, usize) {
        let prefix_count = self.get_count();
        let obj_count: usize = self.inner.iter().map(|e| e.value().get_count()).sum();
        (prefix_count, obj_count)
    }

    /// Dump all objects to Parquet. KS file written separately.
    pub async fn dump<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        writer: &mut crate::utils::AsyncParquetOutput<W>,
        include_equal: bool,
    ) {
        let mut ks_entries: Vec<(String, usize)> = Vec::new();

        for entry in self.inner.iter() {
            let prefix = entry.key().clone();
            let obj_map = entry.value();

            let mut plus: Vec<(ObjectKey, ObjectProps)> = Vec::new();
            let mut minus: Vec<(ObjectKey, ObjectProps)> = Vec::new();
            let mut astrisk: Vec<(ObjectKey, ObjectProps)> = Vec::new();
            let mut equal: Vec<(ObjectKey, ObjectProps)> = Vec::new();

            for obj_entry in obj_map.inner.iter() {
                let name = obj_entry.key().clone();
                let props = obj_entry.value().clone();
                let key = ObjectKey::encode(&prefix, &name);

                match props.final_status_check() {
                    MatchResult::Plus => plus.push((key, props)),
                    MatchResult::Minus => minus.push((key, props)),
                    MatchResult::Astrisk => astrisk.push((key, props)),
                    MatchResult::Equal if include_equal => equal.push((key, props)),
                    MatchResult::Ignore => {}
                    _ => {}
                }
            }

            ks_entries.push((prefix, obj_map.get_count()));

            writer.write_batch(plus, OUTPUT_FLAG_PLUS).await;
            writer.write_batch(minus, OUTPUT_FLAG_MINUS).await;
            writer.write_batch(astrisk, OUTPUT_FLAG_ASTRISK).await;
            if include_equal {
                writer.write_batch(equal, OUTPUT_FLAG_EQUAL).await;
            }
        }

        // Write KS file
        ks_entries.sort_by(|a, b| a.0.cmp(&b.0));
        let ks_path = writer.ks_path().to_string();
        if let Ok(ks_file) = tokio::fs::File::create(&ks_path).await {
            let mut buf = tokio::io::BufWriter::new(ks_file);
            for (prefix, count) in &ks_entries {
                let line = format!("\"{}\",\"{}\"\n", prefix, count);
                let _ = buf.write_all(line.as_bytes()).await;
            }
            let _ = buf.flush().await;
        }
    }
}

impl std::fmt::Display for PrefixMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (prefix, object) = self.get_stats();
        write!(f, "prefix count {}, object count {}", prefix, object)
    }
}

// ── ObjectMap: per-prefix object store ─────────────────────

#[derive(Clone)]
pub struct ObjectMap {
    inner: Arc<DashMap<ObjectName, ObjectProps>>,
    count: Arc<AtomicUsize>,
}

impl ObjectMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn inc_count(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Bulk insert with concurrent dedup/matching.
    pub fn bulk_insert(&self, _prefix: &str, items: Vec<(ObjectName, ObjectProps)>) {
        for (name, props) in items {
            if let Some(mut existing) = self.inner.get_mut(&name) {
                existing.r#match(&props);
            } else {
                self.inner.insert(name, props);
                self.inc_count();
            }
        }
    }
}

// ── Data map task (consumer) ───────────────────────────────

pub async fn data_map_task(
    mut ctx: DataMapContext,
    filename_ks: &str,
    filename_output: &str,
    include_equal: bool,
) {
    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task — started");

    let map = PrefixMap::new();
    let mut last_ts = epoch_secs();

    loop {
        let recv_result = ctx.data_map_channel.recv().await;

        match recv_result {
            Some(batch) => {
                for (key, props) in batch {
                    let (prefix, name) = key.decode();
                    map.bulk_insert(&prefix, vec![(name, props)]);
                }
            }
            None => {
                // Channel disconnected — flush accumulated data before exit.
                info!("Data Map Task — channel disconnected, writing output");
                write_output(&map, filename_ks, filename_output, include_equal).await;
                ctx.complete();
                return;
            }
        }

        if ctx.is_quit() {
            info!("Data Map Task — force quit, dumping");
            write_output(&map, filename_ks, filename_output, include_equal).await;
            ctx.complete();
            return;
        } else if !ctx.all_list_tasks_is_running() {
            // All list tasks done — drain any remaining pending data, then write output.
            while let Ok(batch) = ctx.data_map_channel.try_recv() {
                for (key, props) in batch {
                    let (prefix, name) = key.decode();
                    map.bulk_insert(&prefix, vec![(name, props)]);
                }
            }
            info!("Data Map Task — all list tasks done, writing output");
            write_output(&map, filename_ks, filename_output, include_equal).await;
            ctx.complete();
            ctx.quit();
            return;
        }

        let now = epoch_secs();
        if now - last_ts > core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
            info!("Data Map Task — {}", map);
            last_ts = now;
        }
    }
}

async fn write_output(map: &PrefixMap, ks: &str, output: &str, include_equal: bool) {
    let output_file = match tokio::fs::File::create(output).await {
        Ok(f) => f,
        Err(e) => {
            log::error!("Failed to create output file {}: {}", output, e);
            return;
        }
    };
    let buf_writer = tokio::io::BufWriter::with_capacity(100 * core::MB, output_file);
    let mut parquet = crate::utils::AsyncParquetOutput::new(buf_writer, ks);
    map.dump(&mut parquet, include_equal).await;
    parquet.close().await;
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
