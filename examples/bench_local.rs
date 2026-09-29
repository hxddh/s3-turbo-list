//! Local synthetic benchmark of the output pipeline (Parquet, KeySpace,
//! TSV/NDJSON, diff merge) without contacting S3. A developer tool: it
//! drives the library's data-map writers with generated batches.
//!
//! ```text
//! cargo run --release --example bench_local -- --objects 1000000 --json
//! ```
//!
//! (It was the hidden `benchmark-local` subcommand before 0.37.)

#![allow(clippy::too_many_arguments)]

use clap::{Parser, ValueEnum};
use s3_turbo_list::config::S3TurboConfig;
use s3_turbo_list::{agent, core, data_map};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

#[derive(Parser)]
#[command(about = "Local synthetic output benchmark (no S3)")]
struct Args {
    /// Local benchmark scenario to run
    #[arg(long, value_enum, default_value_t = LocalBenchmarkKind::ListOutput)]
    benchmark: LocalBenchmarkKind,

    /// Number of synthetic objects to write
    #[arg(long, default_value_t = 10_000)]
    objects: usize,

    /// Objects per synthetic batch
    #[arg(long, default_value_t = 1_000)]
    batch_size: usize,

    /// Number of distinct key prefixes
    #[arg(long, default_value_t = 128)]
    prefixes: usize,

    /// Number of synthetic producer tasks sending into the data-map channel
    #[arg(long, default_value_t = 1)]
    producers: usize,

    /// Synthetic diff data shape for diff-output benchmarks
    #[arg(long, value_enum, default_value_t = LocalDiffShape::Mixed)]
    diff_shape: LocalDiffShape,

    /// Local output path to benchmark
    #[arg(long, value_enum, default_value_t = OutputFormat::Parquet)]
    output_format: OutputFormat,

    /// Parquet compression codec
    #[arg(long)]
    compression: Option<String>,

    /// Parquet compression level
    #[arg(long)]
    compression_level: Option<u32>,

    /// Worker threads (default: CPU count)
    #[arg(short = 'T', long)]
    threads: Option<usize>,

    /// Emit JSON report
    #[arg(long)]
    json: bool,

    /// Write JSON report to this path
    #[arg(short, long)]
    output: Option<String>,

    /// Keep generated local Parquet/KS artifacts
    #[arg(long)]
    keep_artifacts: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Parquet,
    Tsv,
    Ndjson,
}

impl OutputFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Tsv => "tsv",
            Self::Ndjson => "ndjson",
        }
    }
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<OutputFormat> for data_map::ListTextOutputFormat {
    fn from(format: OutputFormat) -> Self {
        match format {
            OutputFormat::Tsv => Self::Tsv,
            OutputFormat::Ndjson => Self::Ndjson,
            OutputFormat::Parquet => unreachable!("parquet does not use the text sink"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LocalBenchmarkKind {
    ListOutput,
    DiffMap,
    DiffOutput,
}

impl LocalBenchmarkKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ListOutput => "list-output",
            Self::DiffMap => "diff-map",
            Self::DiffOutput => "diff-output",
        }
    }
}

impl std::fmt::Display for LocalBenchmarkKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LocalDiffShape {
    Mixed,
    AllEqual,
    AllChanged,
}

impl LocalDiffShape {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mixed => "mixed",
            Self::AllEqual => "all-equal",
            Self::AllChanged => "all-changed",
        }
    }
}

impl std::fmt::Display for LocalDiffShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn exit_before_run(code: agent::ExitCode, message: String) -> ! {
    eprintln!("{}", message);
    std::process::exit(code.code());
}

fn build_runtime_or_exit(worker_threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap_or_else(|e| {
            exit_before_run(
                agent::ExitCode::InternalError,
                format!("Runtime initialization error: {}", e),
            )
        })
}

fn main() {
    let args = Args::parse();
    let mut cfg = S3TurboConfig::default();
    if let Some(codec) = args.compression {
        cfg.output.compression = codec;
    }
    if let Some(level) = args.compression_level {
        cfg.output.compression_level = level;
    }
    if let Some(threads) = args.threads {
        cfg.runtime.worker_threads = threads;
    }
    if !s3_turbo_list::utils::is_supported_compression(&cfg.output.compression) {
        exit_before_run(
            agent::ExitCode::CliConfig,
            format!("compression '{}' is not supported", cfg.output.compression),
        );
    }
    if let Some(reason) = s3_turbo_list::utils::compression_setting_error(
        &cfg.output.compression,
        cfg.output.compression_level,
    ) {
        exit_before_run(agent::ExitCode::CliConfig, reason);
    }
    let report = run_benchmark_local(
        args.benchmark,
        args.objects,
        args.batch_size,
        args.prefixes,
        args.producers,
        args.diff_shape,
        args.output_format,
        args.keep_artifacts,
        &cfg,
    );
    if args.json || args.output.is_some() {
        if let Some(path) = args.output.as_deref()
            && let Err(e) = agent::write_json_file(path, &report)
        {
            exit_before_run(
                agent::ExitCode::OutputWrite,
                format!("Benchmark write error: {}", e),
            );
        }
        if args.json {
            println!("{}", agent::to_pretty_json(&report));
        }
    } else {
        println!(
            "local benchmark: {} {} objects in {:.3}s ({:.0} objects/sec)",
            report.benchmark, report.objects, report.elapsed_secs, report.objects_per_sec
        );
        for (label, path) in [
            ("parquet", &report.parquet_file),
            ("ks:     ", &report.ks_file),
            ("rows:   ", &report.text_file),
        ] {
            if let Some(path) = path {
                println!("  {} {}", label, path);
            }
        }
        if !report.artifacts_kept {
            println!("  artifacts removed");
        }
    }
}

#[derive(Debug, Serialize)]
struct LocalBenchmarkReport {
    schema_version: &'static str,
    tool_version: &'static str,
    status: String,
    benchmark: String,
    network: String,
    compression: String,
    compression_level: u32,
    output_format: String,
    objects: usize,
    batch_size: usize,
    prefixes: usize,
    producers: usize,
    channel_capacity: usize,
    producer_send_wait_secs: f64,
    elapsed_secs: f64,
    objects_per_sec: f64,
    rows_per_sec: f64,
    parquet_bytes_per_object: f64,
    ks_bytes_per_object: f64,
    text_bytes_per_object: f64,
    output_bytes_per_object: f64,
    parquet_mib_per_sec: f64,
    text_mib_per_sec: f64,
    output_mib_per_sec: f64,
    artifact_dir: Option<String>,
    parquet_file: Option<String>,
    parquet_bytes: u64,
    ks_file: Option<String>,
    ks_bytes: u64,
    text_file: Option<String>,
    text_bytes: u64,
    metrics: agent::MetricsSummary,
    artifacts_kept: bool,
}

fn run_benchmark_local(
    benchmark: LocalBenchmarkKind,
    objects: usize,
    batch_size: usize,
    prefixes: usize,
    producers: usize,
    diff_shape: LocalDiffShape,
    output_format: OutputFormat,
    keep_artifacts: bool,
    cfg: &S3TurboConfig,
) -> LocalBenchmarkReport {
    if matches!(
        benchmark,
        LocalBenchmarkKind::DiffMap | LocalBenchmarkKind::DiffOutput
    ) {
        return run_benchmark_local_diff(
            benchmark,
            objects,
            batch_size,
            prefixes,
            diff_shape,
            keep_artifacts,
            cfg,
        );
    }

    let objects = objects.max(1);
    let batch_size = batch_size.max(1);
    let prefixes = prefixes.max(1);
    let producers = producers.max(1);
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let artifact_dir = std::env::temp_dir().join(format!("s3-turbo-list-benchmark-{}", suffix));
    std::fs::create_dir_all(&artifact_dir).unwrap_or_else(|e| {
        exit_before_run(
            agent::ExitCode::OutputWrite,
            format!(
                "Benchmark setup error: failed to create {}: {}",
                artifact_dir.display(),
                e
            ),
        );
    });
    let parquet_file = artifact_dir.join("benchmark.parquet");
    let ks_file = artifact_dir.join("benchmark.ks");
    let text_file = match output_format {
        OutputFormat::Tsv => Some(artifact_dir.join("benchmark.tsv")),
        OutputFormat::Ndjson => Some(artifact_dir.join("benchmark.ndjson")),
        OutputFormat::Parquet => None,
    };

    let quit = Arc::new(AtomicBool::new(false));
    let g_state = core::GlobalState::new(quit, 2);
    let started = Instant::now();
    let output_config = cfg.output.clone();
    let parquet_path = parquet_file.display().to_string();
    let ks_path = ks_file.display().to_string();
    let channel_capacity = cfg.channel.capacity;
    let send_wait_nanos = Arc::new(AtomicU64::new(0));

    let rt = build_runtime_or_exit(cfg.runtime.worker_threads);

    rt.block_on(async {
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(core::ObjectKey, core::ObjectProps)>>(
            channel_capacity,
        );
        let data_map_ctx = core::DataMapContext::new(rx, g_state.clone());
        let data_map = match output_format {
            OutputFormat::Parquet => {
                let data_map_ks = ks_path.clone();
                let data_map_parquet = parquet_path.clone();
                let data_map_output_config = output_config.clone();
                tokio::spawn(async move {
                    data_map::data_map_task_list_streaming(
                        data_map_ctx,
                        &data_map_ks,
                        &data_map_parquet,
                        data_map_output_config,
                    )
                    .await;
                })
            }
            OutputFormat::Tsv | OutputFormat::Ndjson => {
                let text_path = text_file
                    .as_ref()
                    .expect("text benchmark output path")
                    .clone();
                tokio::spawn(async move {
                    let file = match tokio::fs::File::create(&text_path).await {
                        Ok(file) => file,
                        Err(e) => {
                            eprintln!(
                                "Benchmark setup error: failed to create {}: {}",
                                text_path.display(),
                                e
                            );
                            data_map_ctx.g_state.inc_output_error();
                            data_map_ctx.g_state.quit();
                            return;
                        }
                    };
                    let writer = tokio::io::BufWriter::new(file);
                    data_map::data_map_task_list_text_writer(
                        data_map_ctx,
                        data_map::ListTextOutputFormat::from(output_format),
                        writer,
                    )
                    .await;
                })
            }
        };

        let producer_state = g_state.clone();
        let send_wait_nanos = Arc::clone(&send_wait_nanos);
        let producer = tokio::spawn(async move {
            producer_state.list_task_start(core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE);
            producer_state.wait_to_start().await;
            let mut handles = Vec::with_capacity(producers);
            for producer_index in 0..producers {
                let tx = tx.clone();
                let send_wait_nanos = Arc::clone(&send_wait_nanos);
                let start = objects.saturating_mul(producer_index) / producers;
                let end = objects.saturating_mul(producer_index + 1) / producers;
                handles.push(tokio::spawn(async move {
                    let mut sent = start;
                    while sent < end {
                        let take = (end - sent).min(batch_size);
                        let mut batch = Vec::with_capacity(take);
                        for offset in 0..take {
                            let index = sent + offset;
                            let prefix_index = index % prefixes;
                            let key_text =
                                format!("prefix-{}/object-{:012}.dat", prefix_index, index);
                            let key = core::ObjectKey::from(key_text.as_str());
                            let mut etag = [0u8; 16];
                            etag[..8].copy_from_slice(&(index as u64).to_le_bytes());
                            etag[8..].copy_from_slice(&(prefix_index as u64).to_le_bytes());
                            let props = core::ObjectProps::new_open(
                                core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE,
                                1024 + (index % 4096) as u64,
                                etag,
                            );
                            batch.push((key, props));
                        }
                        let send_started = Instant::now();
                        if tx.send(batch).await.is_err() {
                            break;
                        }
                        let waited = send_started.elapsed().as_nanos().min(u128::from(u64::MAX));
                        send_wait_nanos.fetch_add(waited as u64, Ordering::Relaxed);
                        sent += take;
                    }
                }));
            }
            drop(tx);
            for handle in handles {
                if let Err(e) = handle.await {
                    eprintln!("Benchmark producer worker failed: {}", e);
                    producer_state.inc_fatal_error();
                }
            }
            producer_state.list_task_complete(core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE);
        });

        if let Err(e) = producer.await {
            eprintln!("Benchmark producer task failed: {}", e);
            g_state.inc_fatal_error();
        }
        if let Err(e) = data_map.await {
            eprintln!("Benchmark data-map task failed: {}", e);
            g_state.inc_fatal_error();
        }
    });
    rt.shutdown_background();

    let elapsed_secs = started.elapsed().as_secs_f64().max(0.001);
    let producer_send_wait_secs = send_wait_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
    // A pooled run writes `.partN` files beside the base file; the report's
    // size and throughput figures cover all of them.
    let parquet_bytes = std::iter::once(parquet_file.display().to_string())
        .chain(agent::parquet_part_paths(
            &parquet_file.display().to_string(),
            g_state.metrics_snapshot().data_output_files,
        ))
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|m| m.len())
        .sum::<u64>();
    let ks_bytes = std::fs::metadata(&ks_file).map(|m| m.len()).unwrap_or(0);
    let text_bytes = text_file
        .as_ref()
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|m| m.len())
        .unwrap_or(0);
    let output_bytes = parquet_bytes
        .saturating_add(ks_bytes)
        .saturating_add(text_bytes);
    let metrics: agent::MetricsSummary = g_state.metrics_snapshot().into();
    let artifacts_kept = keep_artifacts;
    let artifact_dir_summary = artifacts_kept.then(|| artifact_dir.display().to_string());
    if !artifacts_kept {
        let _ = std::fs::remove_dir_all(&artifact_dir);
    }

    LocalBenchmarkReport {
        schema_version: agent::AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: if g_state.read_fatal_error() == 0 {
            "ok".to_string()
        } else {
            "error".to_string()
        },
        benchmark: LocalBenchmarkKind::ListOutput.to_string(),
        network: "none: synthetic local data only".to_string(),
        compression: output_config.compression.clone(),
        compression_level: output_config.compression_level,
        output_format: output_format.to_string(),
        objects,
        batch_size,
        prefixes,
        producers,
        channel_capacity,
        producer_send_wait_secs,
        elapsed_secs,
        objects_per_sec: objects as f64 / elapsed_secs,
        rows_per_sec: metrics.streamed_rows as f64 / elapsed_secs,
        parquet_bytes_per_object: parquet_bytes as f64 / objects as f64,
        ks_bytes_per_object: ks_bytes as f64 / objects as f64,
        text_bytes_per_object: text_bytes as f64 / objects as f64,
        output_bytes_per_object: output_bytes as f64 / objects as f64,
        parquet_mib_per_sec: parquet_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        text_mib_per_sec: text_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        output_mib_per_sec: output_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        artifact_dir: artifact_dir_summary,
        parquet_file: (output_format == OutputFormat::Parquet).then_some(parquet_path),
        parquet_bytes,
        ks_file: (output_format == OutputFormat::Parquet).then_some(ks_path),
        ks_bytes,
        text_file: text_file.as_ref().map(|path| path.display().to_string()),
        text_bytes,
        metrics,
        artifacts_kept,
    }
}

fn run_benchmark_local_diff(
    benchmark: LocalBenchmarkKind,
    objects: usize,
    batch_size: usize,
    prefixes: usize,
    diff_shape: LocalDiffShape,
    keep_artifacts: bool,
    cfg: &S3TurboConfig,
) -> LocalBenchmarkReport {
    let objects = objects.max(1);
    let batch_size = batch_size.max(1);
    let prefixes = prefixes.max(1);
    let started = Instant::now();

    // DiffOutput writes real Parquet/KS artifacts; DiffMap measures the
    // merge + row encoding against a null writer (no file IO).
    let mut artifact_dir_summary = None;
    let (artifact_dir, parquet_path, ks_path) = if benchmark == LocalBenchmarkKind::DiffOutput {
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let dir = std::env::temp_dir().join(format!("s3-turbo-list-diff-benchmark-{}", suffix));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| {
            exit_before_run(
                agent::ExitCode::OutputWrite,
                format!(
                    "Benchmark setup error: failed to create {}: {}",
                    dir.display(),
                    e
                ),
            );
        });
        let parquet = dir.join("diff.parquet");
        let ks = dir.join("diff.ks");
        (Some(dir), Some(parquet), Some(ks))
    } else {
        (None, None, None)
    };

    let output_config = cfg.output.clone();
    let channel_capacity = cfg.channel.capacity;
    let mut parquet_rows = 0usize;
    let mut ks_entries = 0usize;

    let rt = build_runtime_or_exit(cfg.runtime.worker_threads);
    let outcome = rt.block_on(async {
        let (left_tx, left_rx) = tokio::sync::mpsc::channel::<
            Vec<(core::ObjectKey, core::ObjectProps)>,
        >(channel_capacity);
        let (right_tx, right_rx) = tokio::sync::mpsc::channel::<
            Vec<(core::ObjectKey, core::ObjectProps)>,
        >(channel_capacity);

        let producer = tokio::spawn(async move {
            let mut sent = 0usize;
            while sent < objects {
                let take = (objects - sent).min(batch_size);
                let (left, right) =
                    synthetic_diff_batches(sent, take, prefixes, objects, benchmark, diff_shape);
                if !left.is_empty() && left_tx.send(left).await.is_err() {
                    return;
                }
                if !right.is_empty() && right_tx.send(right).await.is_err() {
                    return;
                }
                sent += take;
            }
        });

        let sides = data_map::DiffStreamSides {
            left: vec![left_rx],
            right: vec![right_rx],
        };
        let merged: Result<data_map::DiffMergeOutcome, String> =
            if let (Some(parquet_path), Some(ks_path)) = (&parquet_path, &ks_path) {
                let output = tokio::fs::File::create(parquet_path)
                    .await
                    .map_err(|e| format!("failed to create {}: {}", parquet_path.display(), e))?;
                let writer = tokio::io::BufWriter::with_capacity(100 * 1_048_576, output);
                let ks = ks_path.display().to_string();
                let mut parquet = s3_turbo_list::utils::AsyncParquetOutput::new_with_options(
                    writer,
                    &ks,
                    output_config.row_group_size,
                    &output_config.compression,
                    output_config.compression_level,
                );
                let outcome = data_map::run_diff_merge(sides, &mut parquet, || false).await?;
                parquet_rows = parquet.total_rows();
                ks_entries = outcome.write_ks(&ks).await?;
                parquet.close().await?;
                Ok(outcome)
            } else {
                let mut parquet = s3_turbo_list::utils::AsyncParquetOutput::new_with_options(
                    tokio::io::sink(),
                    "",
                    output_config.row_group_size,
                    &output_config.compression,
                    output_config.compression_level,
                );
                let outcome = data_map::run_diff_merge(sides, &mut parquet, || false).await?;
                parquet_rows = parquet.total_rows();
                Ok(outcome)
            };
        let _ = producer.await;
        merged
    });
    rt.shutdown_background();

    let outcome = outcome.unwrap_or_else(|e| {
        exit_before_run(
            agent::ExitCode::OutputWrite,
            format!("Benchmark output error: {}", e),
        );
    });

    let mut parquet_bytes = 0u64;
    let mut ks_bytes = 0u64;
    let mut parquet_file = None;
    let mut ks_file = None;
    if let (Some(dir), Some(parquet_path), Some(ks_path)) = (&artifact_dir, &parquet_path, &ks_path)
    {
        parquet_bytes = std::fs::metadata(parquet_path)
            .map(|m| m.len())
            .unwrap_or(0);
        ks_bytes = std::fs::metadata(ks_path).map(|m| m.len()).unwrap_or(0);
        parquet_file = Some(parquet_path.display().to_string());
        ks_file = Some(ks_path.display().to_string());
        if keep_artifacts {
            artifact_dir_summary = Some(dir.display().to_string());
        } else {
            let _ = std::fs::remove_dir_all(dir);
            parquet_file = None;
            ks_file = None;
        }
    }

    let elapsed_secs = started.elapsed().as_secs_f64().max(0.001);
    let output_bytes = parquet_bytes.saturating_add(ks_bytes);
    let metrics = agent::MetricsSummary {
        fatal_errors: 0,
        output_errors: 0,
        stream_timeouts: 0,
        s3_client_timeouts: 0,
        s3_client_generic_errors: 0,
        throttled_responses: 0,
        http_error_statuses: Vec::new(),
        received_batches: outcome.received_batches,
        received_objects: outcome.received_objects,
        streamed_rows: outcome.rows,
        unique_prefixes: outcome.unique_prefixes(),
        parquet_rows,
        ks_entries,
        bytes_total: outcome.bytes_total,
        top_prefixes: Vec::new(),
        summary_only: false,
    };

    LocalBenchmarkReport {
        schema_version: agent::AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: "ok".to_string(),
        benchmark: benchmark.to_string(),
        network: "none: synthetic local data only".to_string(),
        compression: cfg.output.compression.clone(),
        compression_level: cfg.output.compression_level,
        output_format: benchmark.to_string(),
        objects,
        batch_size,
        prefixes,
        producers: 1,
        channel_capacity: cfg.channel.capacity,
        producer_send_wait_secs: 0.0,
        elapsed_secs,
        objects_per_sec: outcome.received_objects as f64 / elapsed_secs,
        rows_per_sec: outcome.rows as f64 / elapsed_secs,
        parquet_bytes_per_object: parquet_bytes as f64 / objects as f64,
        ks_bytes_per_object: ks_bytes as f64 / objects as f64,
        text_bytes_per_object: 0.0,
        output_bytes_per_object: output_bytes as f64 / objects as f64,
        parquet_mib_per_sec: parquet_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        text_mib_per_sec: 0.0,
        output_mib_per_sec: output_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
        artifact_dir: artifact_dir_summary,
        parquet_file,
        parquet_bytes,
        ks_file,
        ks_bytes,
        text_file: None,
        text_bytes: 0,
        metrics,
        artifacts_kept: keep_artifacts,
    }
}

type SyntheticObjectBatch = Vec<(core::ObjectKey, core::ObjectProps)>;
type SyntheticDiffBatches = (SyntheticObjectBatch, SyntheticObjectBatch);

fn synthetic_diff_batches(
    start: usize,
    take: usize,
    prefixes: usize,
    total_objects: usize,
    benchmark: LocalBenchmarkKind,
    diff_shape: LocalDiffShape,
) -> SyntheticDiffBatches {
    let mut left = Vec::with_capacity(take);
    let mut right = Vec::with_capacity(take);
    for offset in 0..take {
        let index = start + offset;
        // Block-partitioned, zero-padded prefixes keep the synthetic key
        // stream in S3 lexicographic order, as the diff merge requires.
        let prefix_index = (index * prefixes) / total_objects.max(1);
        let key_text = format!("prefix-{:06}/object-{:012}.dat", prefix_index, index);
        let key = core::ObjectKey::from(key_text.as_str());
        let mut etag = [0u8; 16];
        etag[..8].copy_from_slice(&(index as u64).to_le_bytes());
        etag[8..].copy_from_slice(&(prefix_index as u64).to_le_bytes());
        etag[15] = etag[15].max(1);
        let size = 1024 + (index % 4096) as u64;
        if benchmark == LocalBenchmarkKind::DiffMap {
            left.push((
                key.clone(),
                core::ObjectProps::new_open(core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE, size, etag),
            ));
            right.push((
                key,
                core::ObjectProps::new_open(core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE, size, etag),
            ));
            continue;
        }

        match diff_shape {
            LocalDiffShape::AllEqual => {
                left.push((
                    key.clone(),
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                        size,
                        etag,
                    ),
                ));
                right.push((
                    key,
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                        size,
                        etag,
                    ),
                ));
            }
            LocalDiffShape::AllChanged => {
                left.push((
                    key.clone(),
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                        size,
                        etag,
                    ),
                ));
                etag[0] = etag[0].wrapping_add(1);
                right.push((
                    key,
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                        size + 1,
                        etag,
                    ),
                ));
            }
            LocalDiffShape::Mixed => match index % 4 {
                0 => {
                    left.push((
                        key.clone(),
                        core::ObjectProps::new_open(
                            core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                            size,
                            etag,
                        ),
                    ));
                    right.push((
                        key,
                        core::ObjectProps::new_open(
                            core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                            size,
                            etag,
                        ),
                    ));
                }
                1 => left.push((
                    key,
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                        size,
                        etag,
                    ),
                )),
                2 => right.push((
                    key,
                    core::ObjectProps::new_open(
                        core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                        size,
                        etag,
                    ),
                )),
                _ => {
                    left.push((
                        key.clone(),
                        core::ObjectProps::new_open(
                            core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE,
                            size,
                            etag,
                        ),
                    ));
                    etag[0] = etag[0].wrapping_add(1);
                    right.push((
                        key,
                        core::ObjectProps::new_open(
                            core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE,
                            size + 1,
                            etag,
                        ),
                    ));
                }
            },
        }
    }
    (left, right)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(
        benchmark: LocalBenchmarkKind,
        shape: LocalDiffShape,
        format: OutputFormat,
        cfg: &S3TurboConfig,
    ) -> serde_json::Value {
        let report = run_benchmark_local(benchmark, 32, 8, 4, 2, shape, format, false, cfg);
        serde_json::to_value(&report).unwrap()
    }

    #[test]
    fn list_output_parquet_and_ndjson() {
        let cfg = S3TurboConfig::default();
        let json = run(
            LocalBenchmarkKind::ListOutput,
            LocalDiffShape::Mixed,
            OutputFormat::Parquet,
            &cfg,
        );
        assert_eq!(json["network"], "none: synthetic local data only");
        assert_eq!(json["compression"], "zstd");
        assert_eq!(json["output_format"], "parquet");
        assert_eq!(json["objects"], 32);
        assert_eq!(json["artifact_dir"], serde_json::Value::Null);
        assert_eq!(json["metrics"]["streamed_rows"], 32);
        assert_eq!(json["metrics"]["parquet_rows"], 32);
        assert_eq!(json["metrics"]["ks_entries"], 4);
        assert!(json["parquet_bytes_per_object"].as_f64().unwrap() > 0.0);

        let json = run(
            LocalBenchmarkKind::ListOutput,
            LocalDiffShape::Mixed,
            OutputFormat::Ndjson,
            &cfg,
        );
        assert_eq!(json["output_format"], "ndjson");
        assert!(json["text_bytes"].as_u64().unwrap() > 0);
        assert_eq!(json["metrics"]["streamed_rows"], 32);
        assert_eq!(json["metrics"]["parquet_rows"], 0);
    }

    #[test]
    fn diff_map_and_output_shapes() {
        let cfg = S3TurboConfig::default();
        let json = run(
            LocalBenchmarkKind::DiffMap,
            LocalDiffShape::Mixed,
            OutputFormat::Parquet,
            &cfg,
        );
        assert_eq!(json["benchmark"], "diff-map");
        assert_eq!(json["metrics"]["received_objects"], 64);
        assert_eq!(json["metrics"]["streamed_rows"], 32);
        assert_eq!(json["metrics"]["parquet_rows"], 32);
        for (shape, received) in [
            (LocalDiffShape::Mixed, 48),
            (LocalDiffShape::AllEqual, 64),
            (LocalDiffShape::AllChanged, 64),
        ] {
            let json = run(
                LocalBenchmarkKind::DiffOutput,
                shape,
                OutputFormat::Parquet,
                &cfg,
            );
            assert_eq!(json["metrics"]["received_objects"], received, "{shape}");
            assert_eq!(json["metrics"]["parquet_rows"], 32, "{shape}");
            assert_eq!(json["metrics"]["ks_entries"], 4, "{shape}");
        }
    }

    #[test]
    fn compression_settings_are_reported() {
        let mut cfg = S3TurboConfig::default();
        cfg.output.compression = "gzip".to_string();
        cfg.output.compression_level = 6;
        let json = run(
            LocalBenchmarkKind::ListOutput,
            LocalDiffShape::Mixed,
            OutputFormat::Parquet,
            &cfg,
        );
        assert_eq!(json["compression"], "gzip");
        assert_eq!(json["compression_level"], 6);
    }
}
