//! Redaction of URLs shown in results, logs and traces.

/// `url` without credentials: userinfo (`user:pass@`) is replaced by
/// `<redacted>`, and a query string or fragment (a presigned URL's
/// `X-Amz-Security-Token`, ...) is dropped.
pub fn redact_url(url: &str) -> String {
    let base = url.split(['?', '#']).next().unwrap_or(url);
    let Some(scheme_end) = base.find("://").map(|i| i + 3) else {
        return base.to_string();
    };
    let authority_end = base[scheme_end..]
        .find('/')
        .map_or(base.len(), |i| scheme_end + i);
    match base[scheme_end..authority_end].rfind('@') {
        Some(at) => format!(
            "{}<redacted>@{}",
            &base[..scheme_end],
            &base[scheme_end + at + 1..]
        ),
        None => base.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::redact_url as r;

    #[test]
    fn hides_userinfo_and_query() {
        assert_eq!(r("http://u:pw@h:9000/x"), "http://<redacted>@h:9000/x");
        assert_eq!(r("https://h.example/a@b"), "https://h.example/a@b");
        assert_eq!(r("https://h.example"), "https://h.example");
        assert_eq!(r("https://h/x?X-Amz-Security-Token=tok#f"), "https://h/x");
        assert_eq!(r("http://u@h?t=1"), "http://<redacted>@h");
        assert_eq!(r("not a url"), "not a url");
    }
}
