//! The command line: three commands, a handful of options.

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "s3tl",
    version,
    about = "Fast S3-compatible bucket listing and diffing to Parquet",
    long_about = "Fast S3-compatible bucket listing and diffing to Parquet.\n\n\
        stdout always carries one JSON result document (schema s3tl.result/1), \
        unless `-o -` streams rows there, in which case the result is the last \
        line on stderr. Exit codes: 0 ok, 1 internal, 2 usage, 3 auth/config, \
        4 retryable (network, throttling), 5 output, 130 interrupted (resumable).",
    disable_help_subcommand = true,
    propagate_version = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Command,
    #[command(flatten)]
    pub global: GlobalOpts,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// List objects under s3://bucket[/prefix]
    List {
        /// s3://bucket[/prefix]
        #[arg(value_name = "S3_URL")]
        source: String,
        /// Output: a directory (Parquet dataset) or `-` (NDJSON on stdout).
        /// Without it, only the result's statistics are produced.
        #[arg(short, long, value_name = "DIR|-")]
        output: Option<String>,
        /// List one level: objects plus one row per folder (e.g. '/')
        #[arg(long, value_name = "CHAR")]
        delimiter: Option<String>,
        /// Continue an interrupted run into the same output directory
        #[arg(long)]
        resume: bool,
    },
    /// Compare two prefixes key by key; writes only keys that differ
    Diff {
        /// s3://bucket[/prefix] (left side)
        #[arg(value_name = "S3_URL")]
        source: String,
        /// s3://bucket[/prefix] (right side)
        #[arg(value_name = "S3_URL")]
        target: String,
        /// Output directory (Parquet dataset). Without it, only counts.
        #[arg(short, long, value_name = "DIR")]
        output: Option<String>,
        /// Endpoint of the right side (default: --endpoint-url)
        #[arg(long, value_name = "URL")]
        target_endpoint_url: Option<String>,
    },
    /// Check credentials, endpoint and proxy; with S3_URL, probe the endpoint
    Check {
        /// s3://bucket[/prefix] to probe (a few requests)
        #[arg(value_name = "S3_URL")]
        source: Option<String>,
    },
}

#[derive(Args, Debug)]
pub struct GlobalOpts {
    /// S3-compatible endpoint (default: AWS_ENDPOINT_URL_S3 / AWS_ENDPOINT_URL, else AWS S3)
    #[arg(long, global = true, value_name = "URL")]
    pub endpoint_url: Option<String>,
    /// Path-style addressing (automatic for IP and localhost endpoints)
    #[arg(long, global = true)]
    pub path_style: bool,
    /// Signing region (default: detected from the bucket on AWS, else AWS_REGION, else us-east-1)
    #[arg(long, global = true, value_name = "REGION")]
    pub region: Option<String>,
    /// Maximum concurrent requests
    #[arg(short = 'c', long, global = true, value_name = "N", default_value_t = 100,
          value_parser = clap::value_parser!(u16).range(1..=4096))]
    pub concurrency: u16,
    /// Print the plan as the result and exit without contacting S3
    #[arg(long, global = true)]
    pub dry_run: bool,
    /// Write every ListObjectsV2 request as a JSONL trace event (`-`: stderr)
    #[arg(long, global = true, value_name = "FILE")]
    pub trace: Option<String>,
    /// Progress and diagnostics on stderr (-v info, -vv debug)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,
    /// Page size of each ListObjectsV2 request (testing)
    #[arg(long, global = true, hide = true, value_name = "N",
          value_parser = clap::value_parser!(i32).range(1..=1000))]
    pub max_keys: Option<i32>,
    /// Start from one segment instead of startup discovery (testing)
    #[arg(long, global = true, hide = true)]
    pub no_auto_hints: bool,
}

impl Command {
    pub fn name(&self) -> &'static str {
        match self {
            Command::List { .. } => "list",
            Command::Diff { .. } => "diff",
            Command::Check { .. } => "check",
        }
    }
}

/// A parsed `s3://bucket[/prefix]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Url {
    pub bucket: String,
    pub prefix: String,
}

impl S3Url {
    pub fn parse(value: &str) -> Result<Self, String> {
        let rest = value.strip_prefix("s3://").ok_or_else(|| {
            format!(
                "'{}' is not an S3 URL: expected s3://bucket[/prefix]",
                value
            )
        })?;
        let (bucket, prefix) = match rest.split_once('/') {
            Some((bucket, prefix)) => (bucket, prefix),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            return Err(format!(
                "'{}' has no bucket: expected s3://bucket[/prefix]",
                value
            ));
        }
        Ok(Self {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        })
    }

    pub fn url(&self) -> String {
        if self.prefix.is_empty() {
            format!("s3://{}/", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, self.prefix)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn s3_url_parses_bucket_and_prefix() {
        assert_eq!(
            S3Url::parse("s3://b").unwrap(),
            S3Url {
                bucket: "b".into(),
                prefix: "".into()
            }
        );
        assert_eq!(S3Url::parse("s3://b/").unwrap().prefix, "");
        assert_eq!(
            S3Url::parse("s3://b/logs/2026/").unwrap().prefix,
            "logs/2026/"
        );
        assert_eq!(S3Url::parse("s3://b/a//b").unwrap().prefix, "a//b");
        assert!(S3Url::parse("b/x").is_err());
        assert!(S3Url::parse("s3:///x").is_err());
        assert_eq!(S3Url::parse("s3://b/p").unwrap().url(), "s3://b/p");
        assert_eq!(S3Url::parse("s3://b").unwrap().url(), "s3://b/");
    }

    #[test]
    fn global_options_go_anywhere() {
        let cli = Cli::try_parse_from([
            "s3tl",
            "-c",
            "8",
            "list",
            "s3://b/p",
            "--endpoint-url",
            "http://h",
            "-o",
            "out",
        ])
        .unwrap();
        assert_eq!(cli.global.concurrency, 8);
        assert_eq!(cli.global.endpoint_url.as_deref(), Some("http://h"));
        assert!(matches!(cli.cmd, Command::List { output: Some(ref o), .. } if o == "out"));
    }

    #[test]
    fn diff_takes_no_resume_or_delimiter() {
        assert!(Cli::try_parse_from(["s3tl", "diff", "s3://a", "s3://b", "--resume"]).is_err());
        assert!(
            Cli::try_parse_from(["s3tl", "diff", "s3://a", "s3://b", "--delimiter", "/"]).is_err()
        );
    }
}
