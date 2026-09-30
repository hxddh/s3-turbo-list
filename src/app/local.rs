//! Commands that do not list (completions, man, guide, manifest-summary,
//! doctor output, compat-probe) and the end-of-run summaries.

use super::*;

pub(crate) fn generate_completions(shell: Shell) {
    let mut cmd = without_hidden(&cli_command());
    let name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
}

/// A copy of `cmd` without its hidden arguments and subcommands:
/// completions must not advertise the deprecated spellings and debugging
/// knobs that `--help` hides (clap_complete lists every argument).
fn without_hidden(cmd: &clap::Command) -> clap::Command {
    // Names are `'static` in clap without its `string` feature; this runs
    // once per process, so leaking the few copies is harmless.
    let leak = |text: &str| -> &'static str { Box::leak(text.to_string().into_boxed_str()) };
    let mut copy = clap::Command::new(leak(cmd.get_name()));
    if let Some(about) = cmd.get_about() {
        copy = copy.about(about.clone());
    }
    if let Some(version) = cmd.get_version() {
        copy = copy.version(leak(version));
    }
    copy.args(
        cmd.get_arguments()
            .filter(|arg| !arg.is_hide_set())
            .cloned(),
    )
    .subcommands(
        cmd.get_subcommands()
            .filter(|sub| !sub.is_hide_set())
            .map(without_hidden),
    )
}

pub(crate) fn generate_man_page() {
    let cmd = cli_command();
    let man = clap_mangen::Man::new(cmd);
    let mut buffer: Vec<u8> = Vec::new();
    if let Err(e) = man.render(&mut buffer) {
        exit_before_run(
            agent::ExitCode::InternalError,
            format!("Man page generation error: {}", e),
        );
    }
    if let Err(e) = std::io::stdout().write_all(&buffer) {
        exit_before_run(
            agent::ExitCode::OutputWrite,
            format!("Man page write error: {}", e),
        );
    }
}

pub(crate) fn run_guide(topic: Option<&str>) {
    match local_tools::render_guide(topic) {
        Ok(rendered) => print!("{}", rendered),
        Err(e) => {
            exit_before_run(agent::ExitCode::CliConfig, format!("Guide error: {}", e));
        }
    }
}

pub(crate) fn run_manifest_summary(manifest_file: &str, json: bool, check: bool) {
    match local_tools::manifest_summary(manifest_file, check) {
        Ok(report) => {
            let check_passed = report.check_passed;
            if json {
                println!("{}", agent::to_pretty_json(&report));
            } else {
                print!("{}", local_tools::render_manifest_summary_text(&report));
            }
            if check && !check_passed {
                std::process::exit(agent::ExitCode::DataValidation.code());
            }
        }
        Err(e) => {
            eprintln!("Manifest summary failed: {}", e);
            if json {
                print_json_error(&e);
            }
            std::process::exit(agent::ExitCode::CliConfig.code());
        }
    }
}

pub(crate) fn print_wrote_summary(outputs: &agent::OutputPathSummary, output_files: usize) {
    println!("Wrote:");
    if let Some(path) = &outputs.parquet_file {
        println!("  Parquet: {}", path);
        for part in agent::parquet_part_paths(path, output_files) {
            println!("  Parquet: {}", part);
        }
    }
    if let Some(path) = &outputs.ks_file {
        println!("  KeySpace: {}", path);
    }
    if let Some(path) = &outputs.hints_file {
        println!("  Hints: {}", path);
    }
    if let Some(path) = &outputs.trace_compat {
        println!("  Trace: {}", path);
    }
    if let Some(path) = &outputs.log_file {
        println!("  Log: {}", path);
    }
}

pub(crate) fn print_summary(metrics: &agent::MetricsSummary, delimiter: &str) {
    println!("Summary:");
    if delimiter.is_empty() {
        println!("  objects:  {}", metrics.streamed_rows);
    } else {
        // A --delimiter listing also emits one row per folder (CommonPrefix),
        // which is not an object; bytes and prefixes count objects only.
        println!(
            "  rows:     {} (objects and '{}' folders)",
            metrics.streamed_rows, delimiter
        );
    }
    println!(
        "  bytes:    {} ({})",
        metrics.bytes_total,
        local_tools::human_bytes(metrics.bytes_total)
    );
    println!("  prefixes: {}", metrics.unique_prefixes);
    if !metrics.top_prefixes.is_empty() {
        println!("Top prefixes:");
        for prefix in metrics.top_prefixes.iter().take(10) {
            println!(
                "  {}  objects={} bytes={} ({})",
                if prefix.prefix.is_empty() {
                    "\"\" (root)"
                } else {
                    prefix.prefix.as_str()
                },
                prefix.objects,
                prefix.bytes,
                local_tools::human_bytes(prefix.bytes)
            );
        }
    }
}

/// One compact human format: a line per check, the resolved endpoint
/// settings, and a next step for what is wrong.
pub(crate) fn print_doctor_report(report: &agent::DoctorReport) {
    for check in &report.checks {
        let label = match check.status.as_str() {
            "ok" => "OK   ",
            "warn" => "WARN ",
            "error" => "ERROR",
            "skipped" => "SKIP ",
            _ => "INFO ",
        };
        println!("{} {}: {}", label, check.name, check.message);
    }
    let config = &report.resolved_config;
    println!(
        "Resolved: provider {}, endpoint {}, addressing {}, concurrency {}, threads {}",
        config.s3.provider.as_deref().unwrap_or("-"),
        config.s3.endpoint_url.as_deref().unwrap_or("-"),
        config.s3.addressing_style,
        config.runtime.max_concurrency,
        config.runtime.worker_threads
    );
    if let Some(hints) = &report.hints {
        print_doctor_hints(hints);
    }
    for check in &report.checks {
        if check.name == "endpoint_url" && check.status == "error" {
            println!("NEXT  pass --endpoint-url <url>, or set s3.endpoint_url in the config");
        }
    }
    println!("Doctor status: {}", report.status);
}

pub(crate) fn run_compat_probe(
    endpoint_url: &str,
    region: Option<&str>,
    bucket: &str,
    prefix: &str,
    addressing_style: &str,
    output: Option<&str>,
    cfg: &S3TurboConfig,
    quiet: bool,
    warnings: Vec<String>,
) {
    let rt = build_runtime_or_exit(2);

    let report = rt.block_on(async {
        match compat_probe::run_compat_probe(
            endpoint_url,
            region,
            bucket,
            prefix,
            addressing_style,
            output,
            cfg,
            quiet,
            warnings,
        )
        .await
        {
            Ok(report) => report,
            Err(compat_probe::ProbeFailure::Setup(e)) => {
                exit_before_run(
                    agent::ExitCode::ProviderSetup,
                    format!("Provider setup error: {}", e),
                );
            }
            Err(compat_probe::ProbeFailure::Output(e)) => {
                exit_before_run(
                    agent::ExitCode::OutputWrite,
                    format!("Compat-probe output error: {}", e),
                );
            }
        }
    });
    // `partial` keeps exit 0 (`CompatProbeReport::exit_code_class`); an
    // `incompatible` endpoint exits with the report's `exit_code` and the
    // documented run line.
    if report.exit_code != 0 {
        eprintln!(
            "s3-turbo-list: run failed (exit {}): compat-probe found the endpoint incompatible: \
             every probe operation failed; see the report's tests[] for each error",
            report.exit_code
        );
        std::process::exit(report.exit_code);
    }
}

/// Render the hints-file validation section in human doctor output. Doctor
/// absorbed the former hints-validate command; the formatting matches its old
/// `Hints file:` block.
pub(crate) fn print_doctor_hints(report: &hints::HintsValidationReport) {
    println!("Hints file: {}", report.path);
    println!("  Format:          {:?}", report.format);
    println!("  Boundary count:  {}", report.boundary_count);
    if let Some(metadata) = &report.metadata {
        println!(
            "  Bucket:          {}",
            metadata.bucket.as_deref().unwrap_or("-")
        );
        println!(
            "  Region:          {}",
            metadata.region.as_deref().unwrap_or("-")
        );
        println!(
            "  Generated at:    {}",
            metadata.generated_at.as_deref().unwrap_or("-")
        );
    }
    if !report.first_boundaries.is_empty() {
        println!("  First {} boundaries:", report.first_boundaries.len());
        for boundary in &report.first_boundaries {
            println!("    - {}", boundary);
        }
    }
    if !report.warnings.is_empty() {
        println!("  Warnings:");
        for warning in &report.warnings {
            println!("    - {}", warning);
        }
    }
}
