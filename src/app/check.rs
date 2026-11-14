//! `s3tl check [S3_URL]`: what a run would use — credentials, region,
//! endpoint, proxy — and, given an S3 URL, a few probe requests against it.

use super::Outcome;
use super::cli::{Cli, Command, S3Url};
use super::result::{ExitCode, Failure, RunResult};
use aws_credential_types::provider::ProvideCredentials;
use s3tl::config::EngineConfig;
use s3tl::core;
use serde_json::json;

/// The URL a request for `bucket` goes to, as the SDK resolves it.
fn request_url(
    bucket: Option<&str>,
    region: &str,
    endpoint: Option<&str>,
    path_style: bool,
) -> Option<String> {
    use aws_sdk_s3::config::endpoint::{DefaultResolver, Params, ResolveEndpoint};
    let mut params = Params::builder()
        .force_path_style(path_style)
        .region(region);
    if let Some(bucket) = bucket {
        params = params.bucket(bucket);
    }
    if let Some(endpoint) = endpoint {
        params = params.endpoint(endpoint);
    }
    let params = params.build().ok()?;
    // The default resolver answers synchronously; its future is ready at once.
    let endpoint =
        futures::executor::block_on(DefaultResolver::new().resolve_endpoint(&params)).ok()?;
    Some(endpoint.url().to_string())
}

/// The proxy the SDK's HTTP client routes `url` through: it builds its rules
/// from HTTP(S)_PROXY / ALL_PROXY / NO_PROXY with hyper-util's
/// `Matcher::from_env`, so asking that matcher gives the run's answer. Only
/// scheme, host and port are returned, never credentials.
fn proxy_for(url: &str) -> Option<String> {
    let uri: http::Uri = url.parse().ok()?;
    let intercept = hyper_util::client::proxy::matcher::Matcher::from_env().intercept(&uri)?;
    let proxy = intercept.uri();
    let host = proxy.host()?;
    let scheme = proxy.scheme_str().unwrap_or("http");
    Some(match proxy.port_u16() {
        Some(port) => format!("{}://{}:{}", scheme, host, port),
        None => format!("{}://{}", scheme, host),
    })
}

pub fn run(cli: &Cli) -> Outcome {
    let mut result = RunResult::new("check");
    let source = match &cli.cmd {
        Command::Check { source } => source.clone(),
        _ => unreachable!("check::run is called for check"),
    };
    let fail = |mut result: RunResult, failure: Failure| {
        result.fail(failure);
        Outcome::plain(result)
    };
    let url = match source.as_deref().map(S3Url::parse).transpose() {
        Ok(url) => url,
        Err(m) => return fail(result, Failure::usage("usage.invalid_url", m)),
    };
    // Reuse the job rules for endpoint and path style.
    let side = match super::job::side_for_check(url.clone(), cli) {
        Ok(side) => side,
        Err(f) => return fail(result, f),
    };

    let cfg = EngineConfig::from_env();
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(result, Failure::internal(format!("tokio runtime: {}", e))),
    };
    let dry_run = cli.global.dry_run;
    let report = rt.block_on(async {
        let sdk = core::S3TaskContext::load_sdk_config(&cfg.s3).await;
        let credentials = match sdk.credentials_provider() {
            None => json!({"found": false, "error": "no credentials provider"}),
            Some(provider) => match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                provider.provide_credentials(),
            )
            .await
            {
                Ok(Ok(creds)) => json!({
                    "found": true,
                    "expires_at": creds.expiry().map(|t| chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339()),
                }),
                Ok(Err(e)) => json!({"found": false, "error": e.to_string()}),
                Err(_) => json!({"found": false, "error": "timed out resolving credentials"}),
            },
        };
        let found = credentials["found"].as_bool().unwrap_or(false);
        let region = match (&url, dry_run) {
            (Some(_), false) if found => {
                super::run::resolve_region(&sdk, &side, cfg.s3.operation_timeout_secs).await
            }
            _ => Ok(side.region.clone().unwrap_or_else(|| {
                sdk.region()
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "us-east-1".to_string())
            })),
        };
        let region = match region {
            Ok(r) => r,
            Err(f) => return Err((f, credentials)),
        };
        let bucket = url.as_ref().map(|u| u.bucket.as_str());
        let target_url = request_url(bucket, &region, side.endpoint.as_deref(), side.path_style);
        let proxy = target_url.as_deref().and_then(proxy_for);
        let mut report = json!({
            "credentials": credentials,
            "region": region,
            "endpoint_url": side.endpoint.as_deref().map(s3tl::redact::redact_url),
            "path_style": side.path_style,
            "request_url": target_url.as_deref().map(s3tl::redact::redact_url),
            "proxy": proxy,
            "probe": null,
        });
        let mut probe_failure = None;
        if let (Some(u), false) = (&url, dry_run) {
            if !found {
                probe_failure = Some(Failure::from_errno(
                    s3tl::error::ERROR_S3_MISSING_CREDENTIALS,
                    s3tl::error::MISSING_CREDENTIALS_MESSAGE,
                ));
            } else {
                let client = core::build_s3_client(
                    &sdk,
                    Some(&region),
                    side.endpoint.as_deref(),
                    side.path_style,
                );
                let writer: Box<dyn s3tl::trace::S3TraceWriter> =
                    match s3tl::trace::trace_writer_for_target(cli.global.trace.as_deref()) {
                        Ok(Some(w)) => w,
                        Ok(None) => Box::new(NoTrace),
                        Err(e) => return Err((Failure::output(e), report["credentials"].clone())),
                    };
                let endpoint = side
                    .endpoint
                    .clone()
                    .or_else(|| target_url.clone())
                    .unwrap_or_default();
                let probe = s3tl::compat_probe::run_compat_probe(
                    &client,
                    &endpoint,
                    &region,
                    &u.bucket,
                    &u.prefix,
                    side.path_style,
                    writer.as_ref(),
                )
                .await;
                if probe.overall_status == "incompatible" {
                    let message = probe
                        .tests
                        .iter()
                        .find_map(|t| t.error_message.clone())
                        .unwrap_or_else(|| "every probe request failed".to_string());
                    probe_failure = Some(if probe.failures_are_setup_errors() {
                        let errno = probe
                            .tests
                            .iter()
                            .find_map(|t| {
                                t.s3_error_code.as_deref().map(|c| {
                                    s3tl::error::service_error_errno(
                                        Some(c),
                                        t.http_status.unwrap_or(0),
                                    )
                                })
                            })
                            .unwrap_or(s3tl::error::ERROR_ACCESS_DENIED);
                        let f = Failure::from_errno(errno, message.clone());
                        if f.exit == ExitCode::Setup {
                            f
                        } else {
                            Failure::setup("auth.access_denied", message)
                        }
                    } else {
                        Failure::new(ExitCode::Retryable, "net.error", message)
                    });
                }
                report["probe"] = serde_json::to_value(&probe).unwrap_or_default();
            }
        }
        Ok((report, probe_failure))
    });
    match report {
        Ok((report, failure)) => {
            result.check = Some(report);
            if dry_run {
                result.status = super::result::Status::Planned;
            }
            if let Some(f) = failure {
                result.fail(f);
            }
        }
        Err((failure, credentials)) => {
            result.check = Some(json!({"credentials": credentials}));
            result.fail(failure);
        }
    }
    Outcome::plain(result)
}

struct NoTrace;

impl s3tl::trace::S3TraceWriter for NoTrace {
    fn write_event(&self, _event: s3tl::trace::S3CompatEvent) {}
}
