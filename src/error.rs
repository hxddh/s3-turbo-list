use crate::stats::HttpStatusCodeTracker;
use std::sync::Arc;

// ── Error codes ────────────────────────────────────────────

/// Errors below 0x10 are retryable (timeout, transient).
/// Errors >= 0x10 are fatal (no bucket, access denied, permanent redirect).
pub const ERROR_S3_NEXT_STREAM_TIMEOUT: u8 = 0x1;
pub const ERROR_S3_CLIENT_GENERIC: u8 = 0x2;
pub const ERROR_S3_CLIENT_CONNECTION_TIMEOUT: u8 = 0x3;
pub const ERROR_S3_MISSING_REGION: u8 = 0x4;
pub const ERROR_NO_BUCKET: u8 = 0x10;
pub const ERROR_ACCESS_DENIED: u8 = 0x11;
pub const ERROR_PERMANENT_REDIRECT: u8 = 0x12;
pub const ERROR_SIGNATURE_DOES_NOT_MATCH: u8 = 0x13;
pub const ERROR_AUTH_HEADER_MALFORMED: u8 = 0x14;
pub const ERROR_SLOW_DOWN: u8 = 0x15;
pub const ERROR_TOO_MANY_REQUESTS: u8 = 0x16;
pub const ERROR_INTERNAL_ERROR: u8 = 0x17;
pub const ERROR_SERVICE_UNAVAILABLE: u8 = 0x18;
pub const ERROR_UNKNOWN: u8 = 0xff;

// ── FlatRuntimeError ──────────────────────────────────────

/// Flat runtime error with errno classification, retryable-vs-fatal
/// distinction, HTTP status code, and S3-compatible error details.
#[derive(Debug, Clone)]
pub struct FlatRuntimeError {
    errno: u8,
    errmsg: String,
    next_start: String,
    http_status_code: u16,
    /// S3 error code from response body or headers (e.g. "NoSuchBucket").
    pub s3_error_code: Option<String>,
    /// x-amz-request-id from response headers (or vendor equivalent).
    pub request_id: Option<String>,
    /// First 512 bytes of the error response body, if available.
    pub raw_body_excerpt: Option<String>,
}

impl FlatRuntimeError {
    pub fn new(errno: u8, errmsg: String, next_start: String) -> Self {
        Self {
            errno,
            errmsg,
            next_start,
            http_status_code: 0,
            s3_error_code: None,
            request_id: None,
            raw_body_excerpt: None,
        }
    }

    /// The key to resume from on retry.
    pub fn next_start(&self) -> &str {
        if self.next_start.is_empty() {
            "/"
        } else {
            &self.next_start
        }
    }

    pub fn next_start_owned(&self) -> String {
        if self.next_start.is_empty() {
            "/".to_string()
        } else {
            self.next_start.clone()
        }
    }

    /// Returns `true` if this error is transient and the operation should be retried.
    pub fn continue_on_error(&self) -> bool {
        self.errno < ERROR_NO_BUCKET
    }

    #[allow(dead_code)] // Phase 5: used in log/trace formatting
    pub fn errno(&self) -> u8 {
        self.errno
    }

    /// Returns the HTTP status code, or 0 if not set.
    #[allow(dead_code)] // Phase 5: used in log/trace formatting
    pub fn http_status_code(&self) -> u16 {
        self.http_status_code
    }

    /// Attach an HTTP status code for diagnostics.
    #[allow(dead_code)] // Phase 5: used in log/trace formatting
    pub fn with_http_status_code(mut self, code: u16) -> Self {
        self.http_status_code = code;
        self
    }

    /// Attach an HTTP status code AND record it in the global tracker.
    #[allow(dead_code)] // Phase 5: used in log/trace formatting
    pub fn with_http_status_code_tracker(
        mut self,
        code: u16,
        tracker: Arc<HttpStatusCodeTracker>,
    ) -> Self {
        if code != 0 {
            tokio::spawn(async move { tracker.inc(code).await });
        }
        self.http_status_code = code;
        self
    }

    /// Attach full S3-compatible error details: HTTP status, S3 error code,
    /// request ID, raw body excerpt, and record the status code.
    pub fn with_s3_error_details(
        mut self,
        code: u16,
        s3_err_code: Option<String>,
        req_id: Option<String>,
        body_excerpt: Option<String>,
        tracker: Arc<HttpStatusCodeTracker>,
    ) -> Self {
        if code != 0 {
            // Use blocking inc to avoid requiring a tokio runtime in
            // error-construction contexts.
            tracker.inc_sync(code);
        }
        self.http_status_code = code;
        self.s3_error_code = s3_err_code;
        self.request_id = req_id;
        self.raw_body_excerpt = body_excerpt;
        self
    }

    /// Returns `true` if this error is retryable (errno < 0x10).
    #[allow(dead_code)] // Phase 5: used in log/trace formatting
    pub fn is_retryable(&self) -> bool {
        self.errno < ERROR_NO_BUCKET
    }

    /// Returns `true` if this error is fatal (errno >= 0x10).
    #[allow(dead_code)] // Phase 5: used in log/trace formatting
    pub fn is_fatal(&self) -> bool {
        self.errno >= ERROR_NO_BUCKET
    }
}

impl std::fmt::Display for FlatRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "errno: {}, msg: {}, next_start: {}, http_status: {}",
            self.errno,
            self.errmsg,
            self.next_start(),
            self.http_status_code,
        )?;
        if let Some(ref code) = self.s3_error_code {
            write!(f, ", s3_error_code: {}", code)?;
        }
        if let Some(ref req_id) = self.request_id {
            write!(f, ", request_id: {}", req_id)?;
        }
        Ok(())
    }
}

impl std::error::Error for FlatRuntimeError {}

// ── Error code → name mapping ─────────────────────────────

/// Return a human-readable S3 error classification for logging/trace.
#[allow(dead_code)] // Phase 5: used in log/trace formatting
pub fn errno_to_name(errno: u8) -> &'static str {
    match errno {
        ERROR_S3_NEXT_STREAM_TIMEOUT => "StreamTimeout",
        ERROR_S3_CLIENT_GENERIC => "ClientGeneric",
        ERROR_S3_CLIENT_CONNECTION_TIMEOUT => "ClientConnectionTimeout",
        ERROR_S3_MISSING_REGION => "MissingRegion",
        ERROR_NO_BUCKET => "NoSuchBucket",
        ERROR_ACCESS_DENIED => "AccessDenied",
        ERROR_PERMANENT_REDIRECT => "PermanentRedirect",
        ERROR_SIGNATURE_DOES_NOT_MATCH => "SignatureDoesNotMatch",
        ERROR_AUTH_HEADER_MALFORMED => "AuthorizationHeaderMalformed",
        ERROR_SLOW_DOWN => "SlowDown",
        ERROR_TOO_MANY_REQUESTS => "TooManyRequests",
        ERROR_INTERNAL_ERROR => "InternalError",
        ERROR_SERVICE_UNAVAILABLE => "ServiceUnavailable",
        _ => "Unknown",
    }
}

/// Map an S3 error code string to the closest errno constant.
pub fn s3_error_code_to_errno(code: Option<&str>) -> u8 {
    match code {
        Some("NoSuchBucket") => ERROR_NO_BUCKET,
        Some("AccessDenied") => ERROR_ACCESS_DENIED,
        Some("PermanentRedirect") => ERROR_PERMANENT_REDIRECT,
        Some("SignatureDoesNotMatch") => ERROR_SIGNATURE_DOES_NOT_MATCH,
        Some("AuthorizationHeaderMalformed") => ERROR_AUTH_HEADER_MALFORMED,
        Some("SlowDown") => ERROR_SLOW_DOWN,
        Some("TooManyRequests") | Some("ThrottlingException") => ERROR_TOO_MANY_REQUESTS,
        Some("InternalError") | Some("InternalFailure") => ERROR_INTERNAL_ERROR,
        Some("ServiceUnavailable") | Some("RequestTimeout") => ERROR_SERVICE_UNAVAILABLE,
        _ => ERROR_UNKNOWN,
    }
}

// ── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_flat_error_retryable() {
        let err = FlatRuntimeError::new(
            ERROR_S3_NEXT_STREAM_TIMEOUT,
            "timeout".into(),
            "key-42".into(),
        );
        assert!(err.continue_on_error());
        assert!(err.is_retryable());
        assert!(!err.is_fatal());
    }

    #[test]
    fn test_flat_error_fatal() {
        let err = FlatRuntimeError::new(ERROR_NO_BUCKET, "no bucket".into(), "".into());
        assert!(!err.continue_on_error());
        assert!(err.is_fatal());
    }

    #[test]
    fn test_flat_error_with_s3_details() {
        let tracker = Arc::new(HttpStatusCodeTracker::new());
        let err = FlatRuntimeError::new(ERROR_ACCESS_DENIED, "access denied".into(), "/".into())
            .with_s3_error_details(
                403,
                Some("AccessDenied".into()),
                Some("req-abc-123".into()),
                Some("<Error><Code>AccessDenied</Code></Error>".into()),
                tracker,
            );

        assert_eq!(err.http_status_code(), 403);
        assert_eq!(err.s3_error_code.as_deref(), Some("AccessDenied"));
        assert_eq!(err.request_id.as_deref(), Some("req-abc-123"));
        assert!(err.raw_body_excerpt.is_some());
        assert!(err.is_fatal());
    }

    #[test]
    fn test_errno_to_name() {
        assert_eq!(errno_to_name(ERROR_NO_BUCKET), "NoSuchBucket");
        assert_eq!(errno_to_name(ERROR_SLOW_DOWN), "SlowDown");
        assert_eq!(
            errno_to_name(ERROR_SERVICE_UNAVAILABLE),
            "ServiceUnavailable"
        );
        assert_eq!(errno_to_name(0xfe), "Unknown");
    }

    #[test]
    fn test_s3_error_code_to_errno() {
        assert_eq!(
            s3_error_code_to_errno(Some("NoSuchBucket")),
            ERROR_NO_BUCKET
        );
        assert_eq!(
            s3_error_code_to_errno(Some("AccessDenied")),
            ERROR_ACCESS_DENIED
        );
        assert_eq!(s3_error_code_to_errno(Some("SlowDown")), ERROR_SLOW_DOWN);
        assert_eq!(
            s3_error_code_to_errno(Some("ThrottlingException")),
            ERROR_TOO_MANY_REQUESTS
        );
        assert_eq!(
            s3_error_code_to_errno(Some("InternalError")),
            ERROR_INTERNAL_ERROR
        );
        assert_eq!(
            s3_error_code_to_errno(Some("ServiceUnavailable")),
            ERROR_SERVICE_UNAVAILABLE
        );
        assert_eq!(s3_error_code_to_errno(Some("WeirdError")), ERROR_UNKNOWN);
        assert_eq!(s3_error_code_to_errno(None), ERROR_UNKNOWN);
    }

    #[test]
    fn test_flat_error_next_start_defaults_to_slash() {
        let err = FlatRuntimeError::new(ERROR_UNKNOWN, "msg".into(), "".into());
        assert_eq!(err.next_start(), "/");
    }

    #[test]
    fn test_flat_error_next_start_preserved() {
        let err = FlatRuntimeError::new(ERROR_UNKNOWN, "msg".into(), "prefix/obj".into());
        assert_eq!(err.next_start(), "prefix/obj");
    }

    #[test]
    fn test_flat_error_display() {
        let _err = FlatRuntimeError::new(
            ERROR_NO_BUCKET,
            "The bucket does not exist".into(),
            "/".into(),
        )
        .with_http_status_code(404);
        // s3_error_code defaults to None — make sure that it shows.
        let err = FlatRuntimeError {
            errno: ERROR_NO_BUCKET,
            errmsg: "The bucket does not exist".into(),
            next_start: "/".into(),
            http_status_code: 404,
            s3_error_code: Some("NoSuchBucket".into()),
            request_id: Some("rid-1".into()),
            raw_body_excerpt: None,
        };
        let display = format!("{}", err);
        assert!(display.contains("404"));
        assert!(display.contains("NoSuchBucket"));
        assert!(display.contains("rid-1"));
    }
}
