//! The run manifest's artifact summaries: hashes, counts and Parquet
//! footers of the files a run wrote.

use super::*;

/// The `.partN` paths a pooled list run's extra writers streamed to, given the
/// number of output files the run reported writing (base plus one per extra
/// writer).  Derived from the run's own writer count rather than from what is
/// on disk: an explicit output path reused after a wider run still has that
/// run's higher-numbered parts sitting next to it, and reporting those as this
/// run's output would let verification pass over objects from an earlier
/// listing.
pub fn parquet_part_paths(base: &str, output_files: usize) -> Vec<String> {
    (1..output_files.min(crate::data_map::MAX_LIST_OUTPUT_WORKERS))
        .map(|index| crate::data_map::part_path(base, index))
        .collect()
}

pub fn collect_artifacts(outputs: &OutputPathSummary, output_files: usize) -> Vec<ArtifactSummary> {
    let mut artifacts = Vec::new();
    if let Some(path) = &outputs.parquet_file {
        artifacts.push(summarize_artifact("parquet", path));
        // A pooled list run scales to several writers, each with its own
        // part-file. Recording only the base path left most of the output
        // unlisted and unverifiable, and made the manifest's own
        // artifact row count disagree with metrics.parquet_rows.
        for part in parquet_part_paths(path, output_files) {
            artifacts.push(summarize_artifact("parquet", &part));
        }
    }
    if let Some(path) = &outputs.ks_file {
        artifacts.push(summarize_artifact("ks", path));
    }
    if let Some(path) = &outputs.hints_file {
        artifacts.push(summarize_artifact("hints", path));
    }
    if let Some(path) = &outputs.trace_compat {
        artifacts.push(summarize_artifact("trace", path));
    }
    if let Some(path) = &outputs.log_file {
        artifacts.push(summarize_artifact("log", path));
    }
    artifacts
}

fn summarize_artifact(kind: &str, path: &str) -> ArtifactSummary {
    let exists = Path::new(path).exists();
    if !exists {
        return ArtifactSummary {
            kind: kind.to_string(),
            path: path.to_string(),
            exists,
            size_bytes: None,
            sha256: None,
            line_count: None,
            parquet: None,
        };
    }

    ArtifactSummary {
        kind: kind.to_string(),
        path: path.to_string(),
        exists,
        size_bytes: std::fs::metadata(path).ok().map(|m| m.len()),
        sha256: sha256_file(path).ok(),
        line_count: match kind {
            // KS is CSV: a quoted prefix may itself contain a newline, so
            // count records, not newline bytes — it must equal ks_entries.
            "ks" => csv_record_count(path).ok(),
            "trace" | "log" | "hints" => line_count(path).ok(),
            _ => None,
        },
        parquet: (kind == "parquet")
            .then(|| parquet_summary(path).ok())
            .flatten(),
    }
}

pub fn sha256_file(path: &str) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| format!("failed to open '{}' for hashing: {}", path, e))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Records in an RFC 4180 CSV file: newlines outside double quotes.
fn csv_record_count(path: &str) -> Result<usize, String> {
    let content = std::fs::read(path).map_err(|e| e.to_string())?;
    let mut in_quotes = false;
    let mut records = 0usize;
    for byte in content {
        match byte {
            b'"' => in_quotes = !in_quotes, // "" toggles twice: net no-op
            b'\n' if !in_quotes => records += 1,
            _ => {}
        }
    }
    Ok(records)
}

fn line_count(path: &str) -> Result<usize, String> {
    let content = std::fs::read(path).map_err(|e| e.to_string())?;
    Ok(content.iter().filter(|b| **b == b'\n').count())
}

fn parquet_summary(path: &str) -> Result<ParquetArtifactSummary, String> {
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file).map_err(|e| e.to_string())?;
    let metadata = reader.metadata();
    let schema_fields = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    Ok(ParquetArtifactSummary {
        row_count: metadata.file_metadata().num_rows(),
        row_group_count: metadata.num_row_groups(),
        schema_fields,
    })
}

#[cfg(test)]
mod tests {
    fn parquet_outputs(base: &str) -> super::OutputPathSummary {
        super::OutputPathSummary {
            parquet_file: Some(base.to_string()),
            ks_file: None,
            hints_file: None,
            trace_compat: None,
            log_file: None,
            report_file: None,
        }
    }

    #[test]
    fn manifest_enumerates_every_parquet_part_file() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("out.parquet");
        let base_str = base.to_str().unwrap().to_string();
        for name in ["out.parquet", "out.part1.parquet", "out.part2.parquet"] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }

        // Three writers: the base file plus two parts.
        let parts = super::parquet_part_paths(&base_str, 3);
        assert_eq!(parts.len(), 2, "{:?}", parts);
        assert!(parts[0].ends_with("out.part1.parquet"));
        assert!(parts[1].ends_with("out.part2.parquet"));

        let artifacts = super::collect_artifacts(&parquet_outputs(&base_str), 3);
        assert_eq!(artifacts.len(), 3, "{:?}", artifacts);
        assert!(artifacts.iter().all(|a| a.kind == "parquet" && a.exists));
        assert!(artifacts.iter().all(|a| a.sha256.is_some()));
    }

    #[test]
    fn manifest_ignores_part_files_this_run_did_not_write() {
        // An explicit output path reused after a wider run: the old run's
        // higher-numbered parts are still on disk, holding objects from an
        // earlier listing. Only what this run wrote may be reported.
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("out.parquet");
        let base_str = base.to_str().unwrap().to_string();
        for name in [
            "out.parquet",
            "out.part1.parquet",
            "out.part2.parquet",
            "out.part3.parquet",
        ] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }

        let artifacts = super::collect_artifacts(&parquet_outputs(&base_str), 2);
        assert_eq!(artifacts.len(), 2, "{:?}", artifacts);
        assert!(artifacts[1].path.ends_with("out.part1.parquet"));
    }

    #[test]
    fn manifest_parquet_artifacts_are_just_the_base_file_without_parts() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("out.parquet");
        std::fs::write(&base, b"x").unwrap();
        let base_str = base.to_str().unwrap().to_string();
        // One writer, and the degenerate "nothing recorded" case (a run that
        // failed before the data map reported): base file only either way.
        assert_eq!(
            super::collect_artifacts(&parquet_outputs(&base_str), 1).len(),
            1
        );
        assert_eq!(
            super::collect_artifacts(&parquet_outputs(&base_str), 0).len(),
            1
        );
    }

    #[test]
    fn parquet_part_paths_stay_within_the_writer_cap() {
        // Indices run 0..=cap-1, so the highest part is cap-1 — never `.part32`
        // with a 32-writer cap.
        let parts = super::parquet_part_paths("out.parquet", 10_000);
        assert_eq!(parts.len(), crate::data_map::MAX_LIST_OUTPUT_WORKERS - 1);
        assert!(parts.last().unwrap().ends_with(&format!(
            "out.part{}.parquet",
            crate::data_map::MAX_LIST_OUTPUT_WORKERS - 1
        )));
    }
}
