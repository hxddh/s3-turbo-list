//! Output-pipeline benchmark: synthetic objects through the production data
//! map (Parquet dataset or NDJSON) or the diff merge, without contacting S3.
//!
//! ```text
//! cargo run --release --example bench_local -- [parquet|ndjson|diff] [OBJECTS] [BATCH]
//! ```
//!
//! Prints one JSON line: scenario, objects, elapsed seconds, objects/sec,
//! output bytes. Compare medians of several runs before and after a change.

use s3tl::config::OutputConfig;
use s3tl::core::{self, ObjectKey, ObjectProps};
use s3tl::data_map;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

type Batch = Vec<(ObjectKey, ObjectProps)>;

fn object(index: usize, dir: u8, variant: u8) -> (ObjectKey, ObjectProps) {
    let key = format!("prefix-{:04}/object-{:012}.dat", index % 512, index);
    let mut etag = [variant; 16];
    etag[..8].copy_from_slice(&(index as u64).to_le_bytes());
    let mut props = ObjectProps::new_open(dir, 1024 + (index % 4096) as u64, etag);
    props.set_last_modified_millis(1_767_225_600_000 + index as u64);
    props.set_storage_class("STANDARD");
    (ObjectKey::from(key), props)
}

fn batches(objects: usize, batch: usize, dir: u8, changed_every: usize) -> Vec<Batch> {
    let mut keys: Vec<usize> = (0..objects).collect();
    keys.sort_by_key(|i| format!("prefix-{:04}/object-{:012}.dat", i % 512, i));
    keys.chunks(batch)
        .map(|chunk| {
            chunk
                .iter()
                .map(|&i| {
                    object(
                        i,
                        dir,
                        u8::from(changed_every > 0 && i % changed_every == 0),
                    )
                })
                .collect()
        })
        .collect()
}

fn dir_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let scenario = args
        .first()
        .map(String::as_str)
        .unwrap_or("parquet")
        .to_string();
    let objects: usize = args
        .get(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let batch: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(1000);
    let out = tempfile::tempdir().expect("temp dir");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // Inputs are generated before the clock starts.
    let (input, left, right) = match scenario.as_str() {
        "diff" => (
            Vec::new(),
            batches(objects, batch, core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, 0),
            batches(
                objects,
                batch,
                core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                100,
            ),
        ),
        _ => (
            batches(objects, batch, core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE, 0),
            Vec::new(),
            Vec::new(),
        ),
    };
    let started = Instant::now();
    let bytes = rt.block_on(async {
        match scenario.as_str() {
            "parquet" | "ndjson" => {
                let g_state = core::GlobalState::new(Arc::new(AtomicBool::new(false)), 2);
                let (tx, rx) = tokio::sync::mpsc::channel::<Batch>(64);
                let ctx = core::DataMapContext::new(rx, g_state.clone());
                let path = out.path().to_path_buf();
                let ndjson = scenario == "ndjson";
                let writer = tokio::spawn(async move {
                    if ndjson {
                        let file = tokio::fs::File::create(path.join("rows.ndjson"))
                            .await
                            .unwrap();
                        data_map::data_map_task_list_text_writer(
                            ctx,
                            tokio::io::BufWriter::new(file),
                        )
                        .await;
                    } else {
                        data_map::data_map_task_list_streaming(
                            ctx,
                            s3tl::s3_output::PartSink::Dir(path),
                            0,
                            OutputConfig::default(),
                        )
                        .await;
                    }
                });
                g_state.list_task_start(core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE);
                g_state.wait_to_start().await;
                for b in input {
                    tx.send(b).await.unwrap();
                }
                drop(tx);
                g_state.list_task_complete(core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE);
                writer.await.unwrap();
                dir_bytes(out.path())
            }
            "diff" => {
                let (ltx, lrx) = tokio::sync::mpsc::channel::<Batch>(32);
                let (rtx, rrx) = tokio::sync::mpsc::channel::<Batch>(32);
                tokio::spawn(async move {
                    for b in left {
                        ltx.send(b).await.unwrap();
                    }
                });
                tokio::spawn(async move {
                    for b in right {
                        rtx.send(b).await.unwrap();
                    }
                });
                let file = tokio::fs::File::create(out.path().join("diff.parquet"))
                    .await
                    .unwrap();
                let mut parquet = s3tl::utils::AsyncParquetOutput::new(
                    tokio::io::BufWriter::new(file),
                    s3tl::utils::Layout::Diff,
                );
                let outcome = data_map::run_diff_merge(
                    data_map::DiffStreamSides::from_receivers(vec![lrx], vec![rrx]),
                    Some(&mut parquet),
                    || false,
                )
                .await
                .unwrap();
                parquet.close().await.unwrap();
                assert_eq!(outcome.astrisk, objects.div_ceil(100));
                dir_bytes(out.path())
            }
            other => panic!("unknown scenario {other}: parquet, ndjson or diff"),
        }
    });
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "{}",
        serde_json::json!({
            "scenario": scenario,
            "objects": objects,
            "elapsed_secs": (elapsed * 1000.0).round() / 1000.0,
            "objects_per_sec": (objects as f64 / elapsed).round(),
            "output_bytes": bytes,
        })
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn synthetic_batches_are_sorted() {
        let all: Vec<String> = super::batches(5000, 700, 0, 0)
            .into_iter()
            .flatten()
            .map(|(k, _)| k.as_str().to_string())
            .collect();
        assert!(all.windows(2).all(|w| w[0] < w[1]));
    }
}
