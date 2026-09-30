//! Redaction of secrets in the recorded command line and URLs.

const REDACTED_ARG_VALUE: &str = "<redacted>";
// `--continuation-token` (removed in 0.38) and `--endpoint` (removed in
// 0.39): a command line that still passes one fails to parse, but its value
// is never echoed.
const SENSITIVE_VALUE_FLAGS: &[&str] = &["--continuation-token", "--endpoint-url", "--endpoint"];

/// `url` with any `user:password@` userinfo replaced: the command line is
/// redacted, and the resolved config beside it must not print the same
/// secret in full.
pub fn redact_url_userinfo(url: &str) -> String {
    let Some(scheme_end) = url.find("://").map(|i| i + 3) else {
        return url.to_string();
    };
    let authority_end = url[scheme_end..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |i| scheme_end + i);
    match url[scheme_end..authority_end].rfind('@') {
        Some(at) => format!(
            "{}{}@{}",
            &url[..scheme_end],
            REDACTED_ARG_VALUE,
            &url[scheme_end + at + 1..]
        ),
        None => url.to_string(),
    }
}

pub fn redacted_command_args() -> Vec<String> {
    redact_command_args(std::env::args())
}

pub fn redact_command_args<I, S>(args: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut redacted = Vec::new();
    let mut redact_next = false;

    for arg in args {
        let arg = arg.into();
        if redact_next {
            redacted.push(REDACTED_ARG_VALUE.to_string());
            redact_next = false;
            continue;
        }

        if let Some((flag, _value)) = arg.split_once('=') {
            if is_sensitive_value_flag(flag) {
                redacted.push(format!("{}={}", flag, REDACTED_ARG_VALUE));
                continue;
            }
        }

        if is_sensitive_value_flag(&arg) {
            redacted.push(arg);
            redact_next = true;
            continue;
        }

        redacted.push(arg);
    }

    redacted
}

fn is_sensitive_value_flag(arg: &str) -> bool {
    SENSITIVE_VALUE_FLAGS.contains(&arg)
}

#[cfg(test)]
mod tests {
    use super::redact_command_args;

    #[test]
    fn redacts_sensitive_command_values() {
        let args = redact_command_args([
            "s3-turbo-list",
            "--endpoint-url",
            "https://account.example.com",
            "--continuation-token=token-123",
            "list",
            "--bucket",
            "bucket",
        ]);

        assert_eq!(
            args,
            vec![
                "s3-turbo-list",
                "--endpoint-url",
                "<redacted>",
                "--continuation-token=<redacted>",
                "list",
                "--bucket",
                "bucket",
            ]
        );
    }

    #[test]
    fn redacts_endpoint_alias_and_preserves_diagnostic_values() {
        let args = redact_command_args([
            "s3-turbo-list",
            "--endpoint=https://account.example.com",
            "--provider",
            "r2",
            "list",
            "--bucket",
            "public-diagnostic-bucket",
            "--region",
            "auto",
        ]);

        assert_eq!(
            args,
            vec![
                "s3-turbo-list",
                "--endpoint=<redacted>",
                "--provider",
                "r2",
                "list",
                "--bucket",
                "public-diagnostic-bucket",
                "--region",
                "auto",
            ]
        );
    }

    #[test]
    fn redacts_compat_probe_endpoint_value() {
        let args = redact_command_args([
            "s3-turbo-list",
            "compat-probe",
            "--endpoint",
            "https://account.example.com",
            "--bucket",
            "diagnostic-bucket",
        ]);

        assert_eq!(
            args,
            vec![
                "s3-turbo-list",
                "compat-probe",
                "--endpoint",
                "<redacted>",
                "--bucket",
                "diagnostic-bucket",
            ]
        );
    }

    #[test]
    fn redacts_value_after_sensitive_flag_at_end_safely() {
        let args = redact_command_args(["s3-turbo-list", "list", "--continuation-token"]);

        assert_eq!(args, vec!["s3-turbo-list", "list", "--continuation-token"]);
    }

    #[test]
    fn redact_url_userinfo_hides_credentials_only() {
        use super::redact_url_userinfo as r;
        assert_eq!(r("http://u:pw@h:9000/x"), "http://<redacted>@h:9000/x");
        assert_eq!(r("https://h.example/a@b"), "https://h.example/a@b");
        assert_eq!(r("https://h.example"), "https://h.example");
        assert_eq!(r("not a url"), "not a url");
    }
}
