use crate::config::S3TurboConfig;
use crate::trace::{S3CompatEvent, S3TraceWriter, StderrTraceWriter};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_smithy_runtime_api::client::result::SdkError;
use serde::Serialize;
use std::time::Instant;

#[derive(Debug, Serialize)]
pub struct CompatProbeReport {
    pub endpoint_url: String,
    pub region: String,
    pub bucket: String,
    pub addressing_style: String,
    pub tests: Vec<ProbeTestResult>,
    pub overall_status: String,
}

impl CompatProbeReport {
    fn overall_status_for(results: &[ProbeTestResult]) -> &'static str {
        let error_count = results.iter().filter(|r| r.status == "error").count();
        if error_count == 0 {
            "compatible"
        } else if error_count < results.len() {
            "partial"
        } else {
            "incompatible"
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProbeTestResult {
    pub test: String,
    pub status: String,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id_2: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_continuation_token_present: Option<bool>,
}

pub async fn run_compat_probe(
    endpoint_url: &str,
    region: &str,
    bucket: &str,
    addressing_style: &str,
    output: Option<&str>,
    cfg: &S3TurboConfig,
) -> Result<(), String> {
    let trace_writer: Box<dyn S3TraceWriter> = Box::new(StderrTraceWriter);

    let loader = aws_config::from_env()
        .retry_config(
            aws_config::retry::RetryConfig::standard()
                .with_max_attempts(cfg.s3.max_attempts)
                .with_initial_backoff(std::time::Duration::from_secs(cfg.s3.initial_backoff_secs)),
        )
        .timeout_config(
            aws_config::timeout::TimeoutConfigBuilder::new()
                .connect_timeout(std::time::Duration::from_secs(cfg.s3.connect_timeout_secs))
                .operation_timeout(std::time::Duration::from_secs(
                    cfg.s3.operation_timeout_secs,
                ))
                .read_timeout(std::time::Duration::from_secs(
                    cfg.s3.operation_timeout_secs,
                ))
                .operation_attempt_timeout(std::time::Duration::from_secs(
                    cfg.s3.operation_timeout_secs,
                ))
                .build(),
        );
    let config = loader.load().await;
    let mut s3_cfg = aws_sdk_s3::config::Builder::from(&config);
    s3_cfg = s3_cfg.region(aws_sdk_s3::config::Region::new(region.to_owned()));
    s3_cfg = s3_cfg.endpoint_url(endpoint_url.to_owned());
    if addressing_style == "path" {
        s3_cfg = s3_cfg.force_path_style(true);
    }
    let client = aws_sdk_s3::Client::from_conf(s3_cfg.build());
    let mut results: Vec<ProbeTestResult> = Vec::new();

    let (res, evt) = timed_s3_call(
        || async { client.head_bucket().bucket(bucket).send().await },
        "HeadBucket",
        endpoint_url,
        region,
        bucket,
        addressing_style,
        trace_writer.as_ref(),
        None,
    )
    .await;
    results.push(probe_result_from("HeadBucket", res, evt));

    let (res, evt) = timed_s3_call(
        || async {
            client
                .list_objects_v2()
                .bucket(bucket)
                .max_keys(1)
                .send()
                .await
        },
        "ListObjectsV2 (max-keys=1)",
        endpoint_url,
        region,
        bucket,
        addressing_style,
        trace_writer.as_ref(),
        None,
    )
    .await;
    results.push(probe_result_from("ListObjectsV2 (max-keys=1)", res, evt));

    let (res, evt) = timed_s3_call(
        || async {
            client
                .list_objects_v2()
                .bucket(bucket)
                .prefix("")
                .max_keys(1)
                .send()
                .await
        },
        "ListObjectsV2 with prefix",
        endpoint_url,
        region,
        bucket,
        addressing_style,
        trace_writer.as_ref(),
        None,
    )
    .await;
    results.push(probe_result_from("ListObjectsV2 with prefix", res, evt));

    let (res, evt) = timed_s3_call(
        || async {
            client
                .list_objects_v2()
                .bucket(bucket)
                .delimiter("/")
                .max_keys(1)
                .send()
                .await
        },
        "ListObjectsV2 with delimiter",
        endpoint_url,
        region,
        bucket,
        addressing_style,
        trace_writer.as_ref(),
        None,
    )
    .await;
    results.push(probe_result_from("ListObjectsV2 with delimiter", res, evt));

    let (res, evt) = timed_s3_call(
        || async {
            client
                .list_objects_v2()
                .bucket(bucket)
                .encoding_type(aws_sdk_s3::types::EncodingType::Url)
                .max_keys(1)
                .send()
                .await
        },
        "ListObjectsV2 (encoding-type=url)",
        endpoint_url,
        region,
        bucket,
        addressing_style,
        trace_writer.as_ref(),
        None,
    )
    .await;
    results.push(probe_result_from(
        "ListObjectsV2 (encoding-type=url)",
        res,
        evt,
    ));

    let (res, mut evt) = timed_s3_call(
        || async {
            let resp = client
                .list_objects_v2()
                .bucket(bucket)
                .max_keys(3)
                .send()
                .await?;
            Ok::<
                _,
                aws_sdk_s3::error::SdkError<
                    aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error,
                >,
            >(resp)
        },
        "ListObjectsV2 pagination check",
        endpoint_url,
        region,
        bucket,
        addressing_style,
        trace_writer.as_ref(),
        None,
    )
    .await;
    results.push(
        pagination_probe_result(
            &client,
            endpoint_url,
            region,
            bucket,
            addressing_style,
            trace_writer.as_ref(),
            res,
            &mut evt,
        )
        .await,
    );

    let overall = CompatProbeReport::overall_status_for(&results);

    let report = CompatProbeReport {
        endpoint_url: endpoint_url.to_string(),
        region: region.to_string(),
        bucket: bucket.to_string(),
        addressing_style: addressing_style.to_string(),
        tests: results,
        overall_status: overall.to_string(),
    };

    let json = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
    if let Some(out_path) = output {
        std::fs::write(out_path, &json).map_err(|e| e.to_string())?;
        println!("Compat-probe report written to {}", out_path);
    } else {
        println!("{}", json);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn pagination_probe_result(
    client: &aws_sdk_s3::Client,
    endpoint_url: &str,
    region: &str,
    bucket: &str,
    addressing_style: &str,
    trace_writer: &dyn S3TraceWriter,
    res: Result<
        aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
    >,
    evt: &mut S3CompatEvent,
) -> ProbeTestResult {
    match &res {
        Ok(resp) => {
            let key_count = resp.key_count().unwrap_or(0);
            let is_truncated = resp.is_truncated().unwrap_or(false);
            let content_count = resp.contents().len() as i32;
            let next_token = resp.next_continuation_token().map(|t| t.to_string());
            evt.key_count = Some(key_count);
            evt.contents_count = Some(content_count);
            evt.is_truncated = is_truncated;
            evt.next_continuation_token = next_token.clone();
            evt.next_continuation_token_present = Some(next_token.is_some());
            if !is_truncated && content_count < 3 {
                ProbeTestResult {
                    test: "ListObjectsV2 pagination check".to_string(),
                    status: "skipped".to_string(),
                    latency_ms: evt.latency_ms,
                    http_status: Some(200),
                    s3_error_code: None,
                    error_message: Some(format!(
                        "insufficient_objects: only {} objects; need >= max_keys (3) to test pagination",
                        content_count
                    )),
                    error_kind: None,
                    diagnostic_code: None,
                    recommendation: None,
                    request_id: evt.request_id.clone(),
                    request_id_2: evt.request_id_2.clone(),
                    is_truncated: Some(is_truncated),
                    key_count: Some(key_count),
                    contents_count: Some(content_count),
                    next_continuation_token_present: Some(false),
                }
            } else if is_truncated {
                pagination_second_page_result(
                    client,
                    endpoint_url,
                    region,
                    bucket,
                    addressing_style,
                    trace_writer,
                    evt,
                    key_count,
                    content_count,
                    next_token,
                )
                .await
            } else {
                let mut result =
                    probe_result_from::<
                        (),
                        aws_sdk_s3::error::SdkError<
                            aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error,
                        >,
                    >("ListObjectsV2 pagination check", Ok(()), evt.clone());
                result.is_truncated = Some(is_truncated);
                result.key_count = Some(key_count);
                result.contents_count = Some(content_count);
                result.next_continuation_token_present = Some(false);
                result
            }
        }
        Err(_) => probe_result_from("ListObjectsV2 pagination check", res, evt.clone()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn pagination_second_page_result(
    client: &aws_sdk_s3::Client,
    endpoint_url: &str,
    region: &str,
    bucket: &str,
    addressing_style: &str,
    trace_writer: &dyn S3TraceWriter,
    evt: &S3CompatEvent,
    key_count: i32,
    content_count: i32,
    next_token: Option<String>,
) -> ProbeTestResult {
    let is_truncated = evt.is_truncated;
    match next_token {
        Some(token) => {
            let (second_res, second_evt) = timed_s3_call(
                || {
                    let token_for_request = token.clone();
                    async move {
                        client
                            .list_objects_v2()
                            .bucket(bucket)
                            .max_keys(3)
                            .continuation_token(token_for_request)
                            .send()
                            .await
                    }
                },
                "ListObjectsV2 pagination check (page 2)",
                endpoint_url,
                region,
                bucket,
                addressing_style,
                trace_writer,
                Some(&token),
            )
            .await;

            match second_res {
                Ok(second_resp) => {
                    let second_key_count = second_resp.key_count().unwrap_or(0);
                    let second_content_count = second_resp.contents().len() as i32;
                    ProbeTestResult {
                        test: "ListObjectsV2 pagination check".to_string(),
                        status: "ok".to_string(),
                        latency_ms: evt.latency_ms + second_evt.latency_ms,
                        http_status: Some(200),
                        s3_error_code: None,
                        error_message: Some(format!(
                            "page_1_keys={}, page_2_keys={}",
                            content_count, second_content_count
                        )),
                        error_kind: None,
                        diagnostic_code: None,
                        recommendation: None,
                        request_id: second_evt.request_id.clone(),
                        request_id_2: second_evt.request_id_2.clone(),
                        is_truncated: Some(is_truncated),
                        key_count: Some(key_count + second_key_count),
                        contents_count: Some(content_count + second_content_count),
                        next_continuation_token_present: Some(true),
                    }
                }
                Err(e) => {
                    let http_status = http_status_option(second_evt.http_status);
                    let error_kind = e.error_kind();
                    let (diagnostic_code, recommendation) =
                        diagnostic_for(error_kind, http_status, second_evt.s3_error_code.as_deref());
                    ProbeTestResult {
                        test: "ListObjectsV2 pagination check".to_string(),
                        status: "error".to_string(),
                        latency_ms: evt.latency_ms + second_evt.latency_ms,
                        http_status,
                        s3_error_code: second_evt.s3_error_code,
                        error_message: Some(format!("page_2_error: {:?}", e)),
                        error_kind: error_kind.map(str::to_string),
                        diagnostic_code: Some(diagnostic_code.to_string()),
                        recommendation: Some(recommendation.to_string()),
                        request_id: second_evt.request_id,
                        request_id_2: second_evt.request_id_2,
                        is_truncated: Some(is_truncated),
                        key_count: Some(key_count),
                        contents_count: Some(content_count),
                        next_continuation_token_present: Some(true),
                    }
                }
            }
        }
        None => ProbeTestResult {
            test: "ListObjectsV2 pagination check".to_string(),
            status: "error".to_string(),
            latency_ms: evt.latency_ms,
            http_status: Some(200),
            s3_error_code: None,
            error_message: Some(
                "is_truncated=true but next_continuation_token is absent".to_string(),
            ),
            error_kind: Some("pagination".to_string()),
            diagnostic_code: Some("pagination_token_missing".to_string()),
            recommendation: Some(
                "Endpoint returned is_truncated=true without a continuation token; pagination is not safe for full listings"
                    .to_string(),
            ),
            request_id: evt.request_id.clone(),
            request_id_2: evt.request_id_2.clone(),
            is_truncated: Some(is_truncated),
            key_count: Some(key_count),
            contents_count: Some(content_count),
            next_continuation_token_present: Some(false),
        },
    }
}

async fn timed_s3_call<F, Fut, T, E>(
    f: F,
    test_name: &str,
    endpoint_url: &str,
    region: &str,
    bucket: &str,
    addressing_style: &str,
    trace_writer: &dyn S3TraceWriter,
    continuation_token: Option<&str>,
) -> (Result<T, E>, S3CompatEvent)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: ProbeErrorMetadata + std::fmt::Debug,
{
    let start = Instant::now();
    let result = f().await;
    let latency_ms = start.elapsed().as_millis() as u64;

    let mut event = S3CompatEvent::new(test_name, endpoint_url, bucket, "");
    event.region = Some(region.to_string());
    event.addressing_style = addressing_style.to_string();
    event.latency_ms = latency_ms;
    event.continuation_token = continuation_token.map(|token| token.to_string());

    match &result {
        Ok(_) => {
            event.http_status = 200;
        }
        Err(e) => {
            event.http_status = 0;
            event.fatal = true;
            e.apply_to_event(&mut event);
        }
    }

    trace_writer.write_event(event.clone());
    (result, event)
}

fn probe_result_from<T, E: ProbeErrorMetadata + std::fmt::Debug>(
    test_name: &str,
    result: Result<T, E>,
    event: S3CompatEvent,
) -> ProbeTestResult {
    match result {
        Ok(_) => ProbeTestResult {
            test: test_name.to_string(),
            status: "ok".to_string(),
            latency_ms: event.latency_ms,
            http_status: if event.http_status != 0 {
                Some(event.http_status)
            } else {
                Some(200)
            },
            s3_error_code: event.s3_error_code,
            error_message: event.s3_error_message,
            error_kind: None,
            diagnostic_code: None,
            recommendation: None,
            request_id: event.request_id,
            request_id_2: event.request_id_2,
            is_truncated: None,
            key_count: None,
            contents_count: None,
            next_continuation_token_present: None,
        },
        Err(e) => {
            let http_status = http_status_option(event.http_status);
            let error_kind = e.error_kind();
            let (diagnostic_code, recommendation) =
                diagnostic_for(error_kind, http_status, event.s3_error_code.as_deref());
            ProbeTestResult {
                test: test_name.to_string(),
                status: "error".to_string(),
                latency_ms: event.latency_ms,
                http_status,
                s3_error_code: event.s3_error_code,
                error_message: Some(format!("{:?}", e)),
                error_kind: error_kind.map(str::to_string),
                diagnostic_code: Some(diagnostic_code.to_string()),
                recommendation: Some(recommendation.to_string()),
                request_id: event.request_id,
                request_id_2: event.request_id_2,
                is_truncated: None,
                key_count: None,
                contents_count: None,
                next_continuation_token_present: None,
            }
        }
    }
}

fn http_status_option(status: u16) -> Option<u16> {
    if status != 0 {
        Some(status)
    } else {
        None
    }
}

fn diagnostic_for(
    error_kind: Option<&str>,
    http_status: Option<u16>,
    s3_error_code: Option<&str>,
) -> (&'static str, &'static str) {
    match s3_error_code {
        Some("SignatureDoesNotMatch") => {
            return (
                "signature_mismatch",
                "Check credentials, region, endpoint URL, clock skew, and addressing style",
            );
        }
        Some("InvalidAccessKeyId") | Some("AccessDenied") => {
            return (
                "access_denied",
                "Check credentials, bucket permissions, and whether the selected profile is valid for this endpoint",
            );
        }
        Some("NoSuchBucket") => {
            return (
                "bucket_not_found",
                "Check bucket name, account/project scope, region, and addressing style",
            );
        }
        Some("NotImplemented") | Some("NotSupported") => {
            return (
                "operation_not_supported",
                "Endpoint does not implement this S3 operation or option; inspect which probe test failed before full listing",
            );
        }
        Some("PermanentRedirect") | Some("AuthorizationHeaderMalformed") => {
            return (
                "region_or_endpoint_mismatch",
                "Check region, endpoint URL, and provider-specific region requirements",
            );
        }
        _ => {}
    }

    match http_status {
        Some(301) | Some(307) | Some(308) => (
            "redirect",
            "Check endpoint URL, region, and whether the provider requires a different host",
        ),
        Some(400) => (
            "bad_request",
            "Check endpoint URL, region, bucket name, and addressing style",
        ),
        Some(401) | Some(403) => (
            "access_denied",
            "Check credentials, permissions, bucket policy, and provider profile selection",
        ),
        Some(404) => (
            "not_found",
            "Check bucket name, endpoint URL, region, and addressing style",
        ),
        Some(405) | Some(501) => (
            "operation_not_supported",
            "Endpoint responded but does not support this S3 operation or option",
        ),
        Some(status) if status >= 500 => (
            "server_error",
            "Endpoint returned a server-side error; retry later or check provider status/logs",
        ),
        _ => match error_kind {
            Some("timeout") => (
                "timeout",
                "Check endpoint reachability and consider increasing connect or operation timeout settings",
            ),
            Some("dispatch") => (
                "transport_failure",
                "Check DNS, TLS certificates, proxy/firewall rules, and endpoint reachability",
            ),
            Some("response") => (
                "invalid_response",
                "Endpoint responded with data the S3 SDK could not parse; check S3 compatibility and error body format",
            ),
            Some("construction") => (
                "request_construction",
                "Check local configuration values used to construct the S3 request",
            ),
            _ => (
                "unknown_error",
                "Inspect error_message and rerun with trace diagnostics if more detail is needed",
            ),
        },
    }
}

trait ProbeErrorMetadata {
    fn apply_to_event(&self, event: &mut S3CompatEvent);
    fn error_kind(&self) -> Option<&'static str>;
}

impl<E> ProbeErrorMetadata
    for SdkError<E, aws_smithy_runtime_api::client::orchestrator::HttpResponse>
where
    E: ProvideErrorMetadata,
{
    fn apply_to_event(&self, event: &mut S3CompatEvent) {
        event.s3_error_code = self.code().map(str::to_string);
        event.s3_error_message = self.message().map(str::to_string);

        match self {
            SdkError::ServiceError(err) => {
                event.http_status = err.raw().status().as_u16();
                apply_response_headers(event, err.raw().headers());
            }
            SdkError::ResponseError(err) => {
                event.http_status = err.raw().status().as_u16();
                apply_response_headers(event, err.raw().headers());
            }
            _ => {}
        }
    }

    fn error_kind(&self) -> Option<&'static str> {
        Some(match self {
            SdkError::ConstructionFailure(_) => "construction",
            SdkError::TimeoutError(_) => "timeout",
            SdkError::DispatchFailure(_) => "dispatch",
            SdkError::ResponseError(_) => "response",
            SdkError::ServiceError(_) => "service",
            _ => "unknown",
        })
    }
}

fn apply_response_headers(
    event: &mut S3CompatEvent,
    headers: &aws_smithy_runtime_api::http::Headers,
) {
    event.request_id = headers
        .get("x-amz-request-id")
        .or_else(|| headers.get("x-amzn-requestid"))
        .map(str::to_string);
    event.request_id_2 = headers.get("x-amz-id-2").map(str::to_string);
}

#[cfg(test)]
mod tests {
    use super::{diagnostic_for, CompatProbeReport, ProbeTestResult};

    fn result(status: &str) -> ProbeTestResult {
        ProbeTestResult {
            test: "probe".to_string(),
            status: status.to_string(),
            latency_ms: 1,
            http_status: Some(200),
            s3_error_code: None,
            error_message: None,
            error_kind: None,
            diagnostic_code: None,
            recommendation: None,
            request_id: None,
            request_id_2: None,
            is_truncated: None,
            key_count: None,
            contents_count: None,
            next_continuation_token_present: None,
        }
    }

    #[test]
    fn overall_status_allows_skipped_probe_without_degrading() {
        let results = vec![result("ok"), result("skipped")];
        assert_eq!(
            CompatProbeReport::overall_status_for(&results),
            "compatible"
        );
    }

    #[test]
    fn overall_status_distinguishes_partial_and_incompatible() {
        let partial = vec![result("ok"), result("error")];
        let incompatible = vec![result("error"), result("error")];

        assert_eq!(CompatProbeReport::overall_status_for(&partial), "partial");
        assert_eq!(
            CompatProbeReport::overall_status_for(&incompatible),
            "incompatible"
        );
    }

    #[test]
    fn diagnostic_for_prefers_s3_error_code() {
        let (code, recommendation) =
            diagnostic_for(Some("service"), Some(403), Some("SignatureDoesNotMatch"));

        assert_eq!(code, "signature_mismatch");
        assert!(recommendation.contains("region"));
    }

    #[test]
    fn diagnostic_for_classifies_transport_failures_without_http_status() {
        let (code, recommendation) = diagnostic_for(Some("dispatch"), None, None);

        assert_eq!(code, "transport_failure");
        assert!(recommendation.contains("DNS"));
    }

    #[test]
    fn diagnostic_for_http_5xx_takes_precedence_over_response_kind() {
        let (code, recommendation) = diagnostic_for(Some("response"), Some(502), None);

        assert_eq!(code, "server_error");
        assert!(recommendation.contains("server-side"));
    }
}
