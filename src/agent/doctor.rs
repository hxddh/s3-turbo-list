//! The doctor report, and how a request is routed (URL and proxy).

use super::*;

pub fn doctor_report(
    cfg: &S3TurboConfig,
    config_source: ConfigSourceSummary,
    hints: Option<hints::HintsValidationReport>,
) -> DoctorReport {
    let cwd = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .display()
        .to_string();
    let mut checks = Vec::new();
    checks.push(DoctorCheck {
        name: "binary_version".to_string(),
        status: "ok".to_string(),
        message: env!("CARGO_PKG_VERSION").to_string(),
    });
    checks.push(DoctorCheck {
        name: "working_directory".to_string(),
        status: "ok".to_string(),
        message: cwd.clone(),
    });
    checks.push(DoctorCheck {
        name: "config_parse".to_string(),
        status: "ok".to_string(),
        message: "resolved configuration is valid TOML/CLI state".to_string(),
    });
    // Report the config the command actually loaded (explicit --config,
    // workspace or home), not just whether ./s3-turbo-list.toml exists.
    checks.push(match config_source.loaded_config.as_deref() {
        Some(path) => DoctorCheck {
            name: "config_file".to_string(),
            status: "ok".to_string(),
            message: format!(
                "loaded {} config {}",
                config_source.loaded_config_kind, path
            ),
        },
        None => DoctorCheck {
            name: "config_file".to_string(),
            status: "ok".to_string(),
            message: "no config file (searched ./s3-turbo-list.toml and \
                      ~/.s3-turbo-list.toml); built-in defaults apply"
                .to_string(),
        },
    });
    // Credentials come from the SDK chain whether or not AWS_PROFILE is set:
    // an unset AWS_PROFILE is the normal case, not a warning.
    checks.push(DoctorCheck {
        name: "aws_profile".to_string(),
        status: "ok".to_string(),
        message: std::env::var("AWS_PROFILE")
            .ok()
            .filter(|v| !v.is_empty())
            .map(|v| format!("AWS_PROFILE={}", v))
            .unwrap_or_else(|| {
                "AWS_PROFILE is not set; the AWS SDK uses its default credential chain".to_string()
            }),
    });
    checks.push(DoctorCheck {
        name: "provider".to_string(),
        status: "ok".to_string(),
        message: match cfg.s3.provider.as_deref().and_then(profiles::get_profile) {
            Some(profile) => format!("provider preset '{}' ({})", profile.name, profile.provider),
            None => "no provider preset; plain S3 settings apply".to_string(),
        },
    });
    checks.push(endpoint_url_check(cfg));
    checks.push(proxy_check(cfg));
    checks.push(DoctorCheck {
        name: "network".to_string(),
        status: "skipped".to_string(),
        message: "doctor is a local preflight and does not contact S3 endpoints; use compat-probe to validate an endpoint".to_string(),
    });

    for (name, path) in [
        ("output_parquet_parent", cfg.output.parquet_file.as_ref()),
        ("output_ks_parent", cfg.output.ks_file.as_ref()),
        ("log_parent", cfg.output.log_file.as_ref()),
        ("trace_parent", cfg.s3.trace_compat.as_ref()),
    ] {
        if let Some(path) = path {
            checks.push(parent_dir_check(name, path));
        }
    }

    let status = if checks.iter().any(|check| check.status == "error") {
        "error"
    } else {
        "ok"
    };

    DoctorReport {
        schema_version: AGENT_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION"),
        status: status.to_string(),
        cwd,
        checks,
        config_source,
        resolved_config: cfg.into(),
        hints,
    }
}

/// The URL the SDK will send a bucket's requests to: the S3 endpoint resolver
/// the client itself uses, given the same bucket, region, endpoint override and
/// addressing style (a virtual-hosted AWS request goes to
/// `<bucket>.s3.<region>.amazonaws.com`, not a fixed global host).
pub fn resolved_request_url(
    bucket: Option<&str>,
    region: Option<&str>,
    endpoint: Option<&str>,
    force_path_style: bool,
) -> Option<String> {
    use aws_sdk_s3::config::endpoint::{DefaultResolver, Params, ResolveEndpoint};
    let mut params = Params::builder().force_path_style(force_path_style);
    if let Some(bucket) = bucket {
        params = params.bucket(bucket);
    }
    if let Some(region) = region {
        params = params.region(region);
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

/// The proxy the SDK's HTTP client will route `url` through, if any.
///
/// The SDK (behavior version 2025-08-07 and later, which this binary uses)
/// builds its connector's proxy rules from `HTTP_PROXY` / `HTTPS_PROXY` /
/// `ALL_PROXY` / `NO_PROXY` with hyper-util's `Matcher::from_env`; asking the
/// same matcher about the same request URL gives the same answer the run
/// gets. Only the proxy's scheme, host and port are returned — never
/// credentials in its URL.
pub fn env_proxy_for_url(url: &str) -> Option<String> {
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

/// Doctor has no bucket, so it can only answer exactly when the request host
/// does not depend on one: an explicit endpoint with path-style addressing.
/// Anywhere else a `NO_PROXY` entry could match the real (bucket-qualified,
/// regional) host and not a stand-in, so the check is skipped rather than
/// guessed; the run log names the decision for each resolved endpoint.
fn proxy_check(cfg: &S3TurboConfig) -> DoctorCheck {
    let skipped = |message: &str| DoctorCheck {
        name: "proxy".to_string(),
        status: "skipped".to_string(),
        message: message.to_string(),
    };
    let endpoint = match cfg.s3.endpoint_url.as_deref() {
        Some(e) if profiles::endpoint_url_has_template_placeholder(e) => {
            return skipped(
                "endpoint_url is still a template; proxy rules are checked once it is a real URL",
            );
        }
        Some(e) if cfg.s3.force_path_style() => e,
        _ => {
            return skipped(
                "the request host depends on the bucket and region (virtual-hosted or AWS \
                 endpoint), which doctor does not take; the run log names the proxy, if \
                 any, for each resolved endpoint",
            );
        }
    };
    let url = resolved_request_url(None, None, Some(endpoint), true)
        .unwrap_or_else(|| endpoint.to_string());
    DoctorCheck {
        name: "proxy".to_string(),
        status: "ok".to_string(),
        message: match env_proxy_for_url(&url) {
            Some(proxy) => format!(
                "requests to {} go through proxy {} (from HTTP(S)_PROXY / ALL_PROXY; \
                 add the host to NO_PROXY to connect directly)",
                url, proxy
            ),
            None => format!("requests to {} connect directly (no proxy applies)", url),
        },
    }
}

fn endpoint_url_check(cfg: &S3TurboConfig) -> DoctorCheck {
    if let Some(endpoint) = cfg.s3.endpoint_url.as_deref() {
        // Both endpoint problems below stop every real list/diff run with
        // exit 3, so doctor reports them as errors, not warnings: a preflight
        // that says "ok" before a guaranteed setup failure is worse than none.
        if profiles::endpoint_url_has_template_placeholder(endpoint) {
            return DoctorCheck {
                name: "endpoint_url".to_string(),
                status: "error".to_string(),
                message: format!(
                    "endpoint_url contains template placeholders and must be edited before a real run: {}",
                    redact_url_userinfo(endpoint)
                ),
            };
        }

        return DoctorCheck {
            name: "endpoint_url".to_string(),
            status: "ok".to_string(),
            message: format!("endpoint_url is configured: {}", endpoint),
        };
    }

    let Some(profile) = cfg.s3.provider.as_deref().and_then(profiles::get_profile) else {
        return DoctorCheck {
            name: "endpoint_url".to_string(),
            status: "ok".to_string(),
            message: "no endpoint URL: requests go to AWS S3 (pass --endpoint-url or \
                      --provider for another service)"
                .to_string(),
        };
    };
    if profile.requires_explicit_endpoint {
        return DoctorCheck {
            name: "endpoint_url".to_string(),
            status: "error".to_string(),
            message: format!(
                "provider '{}' requires --endpoint-url or s3.endpoint_url in config",
                profile.name
            ),
        };
    }
    // doctor takes no --region, so a region-derived endpoint cannot be
    // resolved here; the run supplies it.
    if profile.endpoint_template.is_some() {
        return DoctorCheck {
            name: "endpoint_url".to_string(),
            status: if profile.default_region.is_some() {
                "ok"
            } else {
                "warn"
            }
            .to_string(),
            message: match profile.default_region {
                Some(region) => format!(
                    "provider '{}' derives its endpoint from --region (default {})",
                    profile.name, region
                ),
                None => format!(
                    "provider '{}' derives its endpoint from the region; the run needs \
                     --region or --endpoint-url",
                    profile.name
                ),
            },
        };
    }
    DoctorCheck {
        name: "endpoint_url".to_string(),
        status: "ok".to_string(),
        message: format!("provider '{}' needs no explicit endpoint URL", profile.name),
    }
}

/// The same test the dry run applies (`output_path_problem`): the run
/// creates missing parent directories itself, and fails on a path that is a
/// directory or under an unwritable or non-directory ancestor.
fn parent_dir_check(name: &str, path: &str) -> DoctorCheck {
    match output_path_problem(path) {
        Some(problem) => DoctorCheck {
            name: name.to_string(),
            status: "error".to_string(),
            message: format!("output '{}' cannot be created: {}", path, problem),
        },
        None => DoctorCheck {
            name: name.to_string(),
            status: "ok".to_string(),
            message: format!("output '{}' can be created", path),
        },
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn resolved_request_url_matches_the_sdk_request_host() {
        // Virtual-hosted AWS: bucket-qualified, regional host.
        let url =
            super::resolved_request_url(Some("my-bucket"), Some("us-west-2"), None, false).unwrap();
        assert!(
            url.starts_with("https://my-bucket.s3.us-west-2.amazonaws.com"),
            "{}",
            url
        );
        // Path-style custom endpoint: the endpoint plus the bucket path.
        let url = super::resolved_request_url(
            Some("my-bucket"),
            Some("us-east-1"),
            Some("http://127.0.0.1:9000"),
            true,
        )
        .unwrap();
        assert_eq!(url, "http://127.0.0.1:9000/my-bucket");
    }
}
