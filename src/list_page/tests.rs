use super::*;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::http::StatusCode;
use std::collections::VecDeque;

// ── Helpers ────────────────────────────────────────────────

const HEX: &str = "0123456789abcdef0123456789abcdef";

fn page(contents: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><Prefix></Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"#
    )
}

fn object(key: &str) -> String {
    format!(
        "<Contents><Key>{key}</Key><LastModified>2026-05-17T00:00:00.000Z</LastModified><ETag>&quot;{HEX}&quot;</ETag><Size>7</Size><StorageClass>STANDARD</StorageClass></Contents>"
    )
}

/// Everything `ObjectProps` holds, for exact comparison.
type PropsView = (u8, u8, u16, u32, u64, u64, [u8; 16]);

fn view(p: &ObjectProps) -> PropsView {
    (
        p.flags,
        p.status,
        p.pad,
        p.etag_parts,
        p.last_modified,
        p.size,
        p.etag_md5,
    )
}

fn rows_view(rows: &[(ObjectKey, ObjectProps)]) -> Vec<(String, PropsView)> {
    rows.iter()
        .map(|(k, p)| (k.as_str().to_string(), view(p)))
        .collect()
}

fn parse_ok(body: &str) -> (Vec<(String, PropsView)>, String) {
    let (rows, stripped) = parse_list_page(body.as_bytes()).expect("fast path should accept");
    (rows_view(&rows), String::from_utf8(stripped).unwrap())
}

fn keys(body: &str) -> Vec<String> {
    parse_ok(body).0.into_iter().map(|(k, _)| k).collect()
}

fn rejects(contents: &str) {
    let body = page(contents);
    assert!(
        parse_list_page(body.as_bytes()).is_none(),
        "expected fallback for: {contents}"
    );
}

fn md5_of(hex: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    hex::decode_to_slice(hex, &mut out).unwrap();
    out
}

// ── Parser: accepted input ─────────────────────────────────

#[test]
fn parses_rows_and_strips_contents() {
    let body = page(&format!("{}{}", object("a.txt"), object("b/c.txt")));
    let (rows, stripped) = parse_ok(&body);
    assert_eq!(stripped, page(""));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "a.txt");
    assert_eq!(rows[1].0, "b/c.txt");
    let (flags, status, pad, parts, lm, size, md5) = rows[0].1;
    let template = ObjectProps::from(&aws_sdk_s3::types::Object::builder().build());
    assert_eq!((flags, status, pad), (template.flags, template.status, 0));
    assert_eq!(parts, 0);
    assert_eq!(lm, 1_778_976_000); // 2026-05-17T00:00:00Z
    assert_eq!(size, 7);
    assert_eq!(md5, md5_of(HEX));
}

#[test]
fn key_entities_and_character_references() {
    let body = page(&object(
        "a&amp;b&lt;c&gt;d&quot;e&apos;f&#65;&#x42;&#x1F600;&#1;&#x7f;g>h\"i'j",
    ));
    assert_eq!(
        keys(&body),
        vec!["a&b<c>d\"e'fAB\u{1F600}\u{1}\u{7f}g>h\"i'j"]
    );
}

#[test]
fn unicode_and_whitespace_keys_are_verbatim() {
    let body = page(&format!(
        "{}{}",
        object("日本語/ファイル 🍕.txt"),
        object(" spaced\tkey\n")
    ));
    assert_eq!(
        keys(&body),
        vec!["日本語/ファイル 🍕.txt", " spaced\tkey\n"]
    );
}

#[test]
fn multipart_and_odd_etags() {
    let contents = format!(
        "<Contents><Key>m</Key><ETag>&quot;{HEX}-12&quot;</ETag></Contents>\
         <Contents><Key>q</Key><ETag>\"{HEX}\"</ETag></Contents>\
         <Contents><Key>r</Key><ETag>&#34;{HEX}-x&#34;</ETag></Contents>\
         <Contents><Key>s</Key><ETag>abc</ETag></Contents>"
    );
    let (rows, _) = parse_ok(&page(&contents));
    assert_eq!((rows[0].1.3, rows[0].1.6), (12, md5_of(HEX)));
    assert_eq!((rows[1].1.3, rows[1].1.6), (0, md5_of(HEX)));
    // Parts unparseable: the ETag is unavailable (zeros), as on the SDK path —
    // not the single-part reading of the same digest, which diff would then
    // call equal to an unrelated object's ETag.
    assert_eq!((rows[2].1.3, rows[2].1.6), (0, [0; 16]));
    assert_eq!((rows[3].1.3, rows[3].1.6), (0, [0; 16]));
}

#[test]
fn missing_fields_default_to_zero() {
    let (rows, _) = parse_ok(&page("<Contents><Key>k</Key></Contents>"));
    let (_, _, _, parts, lm, size, md5) = rows[0].1;
    assert_eq!((parts, lm, size, md5), (0, 0, 0, [0; 16]));
}

#[test]
fn whitespace_between_elements() {
    let body = concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
        "  <Name>b</Name>\n",
        "  <IsTruncated>false</IsTruncated>\n",
        "  <Contents>\n    <Key>k1</Key>\n    <Size>1</Size>\n  </Contents>\n",
        "  <Contents >\r\n\t<Key >k2</Key >\n    <Size>2</Size>\n  </Contents >\n",
        "</ListBucketResult>\n",
    );
    let (rows, stripped) = parse_ok(body);
    assert_eq!(
        rows.iter()
            .map(|r| (r.0.as_str(), r.1.5))
            .collect::<Vec<_>>(),
        vec![("k1", 1), ("k2", 2)]
    );
    assert!(!stripped.contains("Contents"));
    assert!(stripped.contains("<IsTruncated>false</IsTruncated>"));
}

#[test]
fn self_closing_and_empty_elements() {
    let contents = "<Contents><Key/><ETag></ETag><StorageClass/><Owner/><RestoreStatus/></Contents>\
                    <Contents><Key></Key><Size>0</Size></Contents>";
    let (rows, _) = parse_ok(&page(contents));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "");
    assert_eq!(rows[1].0, "");
}

#[test]
fn extra_children_are_ignored() {
    let contents = format!(
        "<Contents><Key>k</Key><LastModified>2024-01-01T00:00:00Z</LastModified>\
         <ETag>&quot;{HEX}&quot;</ETag><ChecksumAlgorithm>CRC32</ChecksumAlgorithm>\
         <ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType>\
         <Size>5</Size><StorageClass>GLACIER</StorageClass>\
         <Owner><ID>abc</ID><DisplayName>me &amp; you</DisplayName></Owner>\
         <RestoreStatus><IsRestoreInProgress>false</IsRestoreInProgress>\
         <RestoreExpiryDate>2024-02-01T00:00:00.000Z</RestoreExpiryDate></RestoreStatus>\
         <FutureField>anything &lt;here&gt;</FutureField></Contents>"
    );
    let (rows, _) = parse_ok(&page(&contents));
    assert_eq!(rows[0].0, "k");
    assert_eq!(rows[0].1.4, 1_704_067_200);
    assert_eq!(rows[0].1.5, 5);
}

#[test]
fn common_prefixes_and_other_fields_stay_for_the_sdk() {
    let body = page(&format!(
        "{}<CommonPrefixes><Prefix>p/</Prefix></CommonPrefixes>{}<NextContinuationToken>t&amp;1</NextContinuationToken>",
        object("a"),
        object("b")
    ));
    let (_, stripped) = parse_ok(&body);
    assert_eq!(
        stripped,
        page(
            "<CommonPrefixes><Prefix>p/</Prefix></CommonPrefixes><NextContinuationToken>t&amp;1</NextContinuationToken>"
        )
    );
}

#[test]
fn no_declaration_and_single_quoted_attributes() {
    let body = format!(
        "<ListBucketResult xmlns='x>y' a = \"1\"><IsTruncated>false</IsTruncated>{}</ListBucketResult>",
        object("k")
    );
    assert_eq!(keys(&body), vec!["k"]);
}

// ── Parser: fallback triggers ──────────────────────────────

#[test]
fn fallback_triggers() {
    let key = |k: &str| format!("<Contents><Key>{k}</Key></Contents>");
    // Markup the fast subset does not handle.
    rejects(&key("<![CDATA[a]]>"));
    rejects("<Contents><!-- c --><Key>a</Key></Contents>");
    rejects(&format!("<!-- c -->{}", key("a")));
    rejects("<Contents><?pi x?><Key>a</Key></Contents>");
    rejects("<s3:Contents><Key>a</Key></s3:Contents>");
    rejects("<Contents><s3:Key>a</s3:Key></Contents>");
    rejects("<Contents id=\"1\"><Key>a</Key></Contents>");
    rejects("<Contents><Key x=\"1\">a</Key></Contents>");
    rejects("<Contents><Key>a<b/></Key></Contents>");
    rejects("<Contents><Foo><Key>b</Key></Foo><Key>a</Key></Contents>");
    rejects("<Contents><Owner><ID><x/></ID></Owner><Key>a</Key></Contents>");
    rejects("<Contents/>");
    rejects(&format!("<Wrapper>{}</Wrapper>", key("a")));
    rejects(&format!("text{}", key("a")));
    // Entities and character references the SDK rejects.
    rejects(&key("&bogus;"));
    rejects(&key("a&amp"));
    rejects(&key("a & b"));
    rejects(&key("&#xD800;"));
    rejects(&key("&#x110000;"));
    rejects(&key("&#X41;"));
    rejects(&key("&#;"));
    rejects("<Contents><Key>a</Key><StorageClass>&nope;</StorageClass></Contents>");
    rejects("<Contents>&amp;<Key>a</Key></Contents>");
    // Characters XML forbids.
    rejects(&key("a\u{1}b"));
    rejects(&key("a\u{FFFF}b"));
    rejects(&key("a]]>b"));
    // Structure.
    rejects("<Contents><Key>a</Key>");
    rejects("<Contents><Key>a</Size></Contents>");
    rejects("<Contents><Key>a</Key></Content>");
    rejects("<Contents><Size>1</Size></Contents>");
    // Duplicate fields.
    rejects("<Contents><Key>a</Key><Key>b</Key></Contents>");
    rejects("<Contents><Key>a</Key><Size>1</Size><Size>2</Size></Contents>");
    rejects("<Contents><Key>a</Key><ETag>x</ETag><ETag>y</ETag></Contents>");
    rejects(
        "<Contents><Key>a</Key><LastModified>2024-01-01T00:00:00Z</LastModified><LastModified>2024-01-01T00:00:00Z</LastModified></Contents>",
    );
    // Values the SDK fails to parse.
    rejects("<Contents><Key>a</Key><Size>99999999999999999999</Size></Contents>");
    rejects("<Contents><Key>a</Key><Size> 1</Size></Contents>");
    rejects("<Contents><Key>a</Key><Size/></Contents>");
    rejects("<Contents><Key>a</Key><LastModified>yesterday</LastModified></Contents>");
    rejects("<Contents><Key>a</Key><LastModified>2024-02-30T00:00:00Z</LastModified></Contents>");
    rejects(
        "<Contents><Key>a</Key><RestoreStatus><IsRestoreInProgress>yes</IsRestoreInProgress></RestoreStatus></Contents>",
    );
    rejects(
        "<Contents><Key>a</Key><RestoreStatus><RestoreExpiryDate>soon</RestoreExpiryDate></RestoreStatus></Contents>",
    );

    // Document level.
    let err = r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code></Error>"#;
    assert!(parse_list_page(err.as_bytes()).is_none());
    let doctype = format!("<!DOCTYPE x>{}", page(&key("a")));
    assert!(parse_list_page(doctype.as_bytes()).is_none());
    let pi = format!("<?other?>{}", page(&key("a")));
    assert!(parse_list_page(pi.as_bytes()).is_none());
    let trailing = format!("{}<x/>", page(&key("a")));
    assert!(parse_list_page(trailing.as_bytes()).is_none());
    let self_closing_root = "<ListBucketResult/>";
    assert!(parse_list_page(self_closing_root.as_bytes()).is_none());
    let mut non_utf8 = page(&key("a")).into_bytes();
    let at = non_utf8.len() - 40;
    non_utf8[at] = 0xFF;
    assert!(parse_list_page(&non_utf8).is_none());
    let truncated = page(&key("a"));
    assert!(parse_list_page(&truncated.as_bytes()[..truncated.len() - 5]).is_none());
}

// ── Field conversions ──────────────────────────────────────

#[test]
fn timestamps_match_the_sdk_parser() {
    let mut samples: Vec<String> = vec![
        "2026-05-17T00:00:00.000Z".into(),
        "2026-05-17T00:00:00Z".into(),
        "2026-05-17T23:59:59.999999999Z".into(),
        "2024-02-29T12:34:56.5Z".into(),
        "2000-02-29T00:00:00Z".into(),
        "1970-01-01T00:00:00Z".into(),
        "1969-12-31T23:59:59.500Z".into(),
        "1900-01-01T00:00:00Z".into(),
        "0001-01-01T00:00:00Z".into(),
        "9999-12-31T23:59:59.999Z".into(),
        "2026-05-17T00:00:00+02:00".into(),
        "2026-05-17T00:00:00.123-07:30".into(),
        "2026-05-17t00:00:00z".into(),
        "2016-12-31T23:59:60Z".into(),
        "2026-05-17T00:00:00.1234567891Z".into(),
    ];
    let mut rng = Rng(0x5eed);
    for _ in 0..2000 {
        let frac = match rng.below(4) {
            0 => String::new(),
            1 => ".000".into(),
            2 => format!(".{}", rng.below(1_000_000)),
            _ => format!(".{:09}", rng.below(1_000_000_000)),
        };
        samples.push(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{frac}Z",
            1900 + rng.below(250),
            1 + rng.below(12),
            1 + rng.below(28),
            rng.below(24),
            rng.below(60),
            rng.below(60)
        ));
    }
    for s in &samples {
        let sdk = DateTime::from_str(s, DateTimeFormat::DateTimeWithOffset)
            .ok()
            .map(|d| d.secs());
        assert_eq!(parse_timestamp_secs(s), sdk, "{s}");
    }
    // Canonical values really take the fast path.
    assert_eq!(
        parse_canonical_timestamp(b"2026-05-17T00:00:00.000Z"),
        Some(1_778_976_000)
    );
}

#[test]
fn etag_rules_match_object_props_from() {
    let cases = [
        format!("\"{HEX}\""),
        format!("\"{HEX}-3\""),
        format!("\"{HEX}-\""),
        format!("\"{HEX}-x\""),
        format!("\"{}g\"", &HEX[..31]),
        format!("\"{}g-2\"", &HEX[..31]),
        "\"é\"".into(),
        String::new(),
        HEX.to_string(),
        format!("\"{}\"", "é".repeat(16)),
    ];
    for etag in cases {
        let object = aws_sdk_s3::types::Object::builder()
            .key("k")
            .e_tag(&etag)
            .build();
        let expected = ObjectProps::from(&object);
        assert_eq!(
            parse_etag(etag.as_bytes()),
            (expected.etag_md5, expected.etag_parts),
            "{etag}"
        );
    }
}

// ── Differential test against the real SDK client ─────────

/// Tiny deterministic PRNG (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

/// Serves generated pages by continuation token (`?continuation-token=N`),
/// streaming each body in random-sized chunks like a socket would.
#[derive(Debug, Clone)]
struct PageServer {
    pages: Arc<Vec<(u16, String)>>,
}

impl HttpClient for PageServer {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}

impl HttpConnector for PageServer {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let uri = request.uri().to_string();
        let index: usize = uri
            .split(['?', '&'])
            .find_map(|part| part.strip_prefix("continuation-token="))
            .and_then(|n| n.parse().ok())
            .expect("page index in continuation token");
        let (status, body) = &self.pages[index];
        let bytes = Bytes::from(body.clone());
        let mut chunks = VecDeque::new();
        let mut at = 0;
        let mut step = (index % 7) * 97 + 13;
        while at < bytes.len() {
            let end = (at + step).min(bytes.len());
            chunks.push_back(bytes.slice(at..end));
            at = end;
            step = step * 3 % 4093 + 1;
        }
        let body = SdkBody::from_body_1_x(ChunkedBody(chunks));
        let response = HttpResponse::new(StatusCode::try_from(*status).unwrap(), body);
        HttpConnectorFuture::ready(Ok(response))
    }
}

struct ChunkedBody(VecDeque<Bytes>);

impl http_body::Body for ChunkedBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, BoxError>>> {
        Poll::Ready(
            self.get_mut()
                .0
                .pop_front()
                .map(|b| Ok(http_body::Frame::data(b))),
        )
    }
}

fn test_client(pages: Vec<(u16, String)>) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "AKIDTEST", "secret", None, None, "test",
        ))
        .endpoint_url("http://127.0.0.1:9")
        .force_path_style(true)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .http_client(PageServer {
            pages: Arc::new(pages),
        })
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

fn escape_key_char(rng: &mut Rng, c: char, out: &mut String) {
    let numeric = |rng: &mut Rng, out: &mut String| {
        if rng.chance(50) {
            out.push_str(&format!("&#{};", c as u32));
        } else if rng.chance(50) {
            out.push_str(&format!("&#x{:x};", c as u32));
        } else {
            out.push_str(&format!("&#x{:X};", c as u32));
        }
    };
    match c {
        '&' => match rng.below(3) {
            0 => out.push_str("&amp;"),
            _ => numeric(rng, out),
        },
        '<' => match rng.below(3) {
            0 => out.push_str("&lt;"),
            _ => numeric(rng, out),
        },
        '>' if rng.chance(50) => out.push_str("&gt;"),
        '"' if rng.chance(50) => out.push_str("&quot;"),
        '\'' if rng.chance(50) => out.push_str("&apos;"),
        c if (c as u32) < 0x20 && !matches!(c, '\t' | '\n' | '\r') => numeric(rng, out),
        _ if rng.chance(8) => numeric(rng, out),
        c => out.push(c),
    }
}

const KEY_CHARS: &[char] = &[
    'a', 'b', 'z', 'A', 'Z', '0', '9', '/', '/', '-', '_', '.', ' ', '=', '+', '%', '&', '<', '>',
    '"', '\'', ';', '#', 'é', 'ß', '中', '日', '🍕', '\u{1}', '\u{1f}', '\u{7f}', '\t', '\n', '\r',
    '\u{a0}', '\u{fffd}',
];

fn gen_key(rng: &mut Rng) -> String {
    let len = if rng.chance(2) { 0 } else { 1 + rng.below(40) };
    let mut xml = String::new();
    for _ in 0..len {
        let c = *rng.pick(KEY_CHARS);
        escape_key_char(rng, c, &mut xml);
    }
    xml
}

fn gen_hex(rng: &mut Rng, len: usize) -> String {
    let digits = if rng.chance(20) {
        "0123456789ABCDEF"
    } else {
        "0123456789abcdef"
    };
    (0..len)
        .map(|_| digits.as_bytes()[rng.below(16) as usize] as char)
        .collect()
}

fn gen_etag(rng: &mut Rng) -> Option<String> {
    let hex = gen_hex(rng, 32);
    Some(match rng.below(12) {
        0 => return None,
        1 => format!("\"{hex}\""),
        2 => format!("&quot;{hex}-{}&quot;", 1 + rng.below(10_000)),
        3 => format!("&#34;{hex}&#x22;"),
        4 => format!("&quot;{}g&quot;", &hex[..31]),
        5 => format!("&quot;{hex}-x&quot;"),
        6 => format!("&quot;{hex}-&quot;"),
        7 => String::new(),
        8 => format!("\"é{}\"", &hex[..30]),
        _ => format!("&quot;{hex}&quot;"),
    })
}

fn gen_timestamp(rng: &mut Rng) -> Option<String> {
    let frac = match rng.below(5) {
        0 => String::new(),
        1 => ".000".into(),
        2 => format!(".{}", rng.below(10)),
        3 => format!(".{:06}", rng.below(1_000_000)),
        _ => format!(".{:09}", rng.below(1_000_000_000)),
    };
    let base = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{frac}",
        1960 + rng.below(80),
        1 + rng.below(12),
        1 + rng.below(28),
        rng.below(24),
        rng.below(60),
        rng.below(60)
    );
    Some(match rng.below(20) {
        0 => return None,
        1 => format!("{base}+05:30"),
        2 => format!("{base}-08:00"),
        3 => base.replace('T', "t") + "z",
        4 => "2016-12-31T23:59:60Z".into(),
        5 if rng.chance(20) => "2026-02-30T00:00:00Z".into(), // SDK error
        _ => format!("{base}Z"),
    })
}

fn gen_size(rng: &mut Rng) -> Option<String> {
    Some(match rng.below(20) {
        0 => return None,
        1 => format!("+{}", rng.below(1000)),
        2 => format!("-{}", rng.below(1000)),
        3 => format!("{:05}", rng.below(1000)),
        4 => (i64::MAX).to_string(),
        5 if rng.chance(20) => "99999999999999999999".into(), // SDK error
        _ => (rng.next() >> (1 + rng.below(63))).to_string(),
    })
}

/// A random `<Contents>` element and whether it stays in the fast subset.
fn gen_contents(rng: &mut Rng, pretty: bool) -> (String, bool) {
    let mut fast = true;
    let mut children: Vec<String> = Vec::new();
    children.push(format!("<Key>{}</Key>", gen_key(rng)));
    if let Some(ts) = gen_timestamp(rng) {
        children.push(format!("<LastModified>{ts}</LastModified>"));
    }
    if let Some(etag) = gen_etag(rng) {
        children.push(if etag.is_empty() && rng.chance(50) {
            "<ETag/>".into()
        } else {
            format!("<ETag>{etag}</ETag>")
        });
    }
    if let Some(size) = gen_size(rng) {
        children.push(format!("<Size>{size}</Size>"));
    }
    if rng.chance(60) {
        children.push(format!(
            "<StorageClass>{}</StorageClass>",
            rng.pick(&["STANDARD", "GLACIER", "INTELLIGENT_TIERING", "X&amp;Y"])
        ));
    }
    if rng.chance(20) {
        children.push("<StorageClass/>".into());
    }
    for _ in 0..rng.below(3) {
        children.push("<ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm>".into());
    }
    if rng.chance(30) {
        children.push("<ChecksumType>FULL_OBJECT</ChecksumType>".into());
    }
    if rng.chance(20) {
        children.push(format!(
            "<Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>",
            gen_hex(rng, 16),
            gen_key(rng)
        ));
    }
    if rng.chance(10) {
        children.push(format!(
            "<RestoreStatus><IsRestoreInProgress>{}</IsRestoreInProgress><RestoreExpiryDate>2030-01-01T00:00:00.000Z</RestoreExpiryDate></RestoreStatus>",
            rng.pick(&["true", "false"])
        ));
    }
    if rng.chance(15) {
        children.push(format!("<FutureField>{}</FutureField>", gen_key(rng)));
    }
    // Occasionally step outside the fast subset; the page must then list
    // exactly as the SDK path does.
    if rng.chance(1) {
        fast = false;
        let odd = match rng.below(6) {
            0 => "<!-- note -->".to_string(),
            1 => "<Key>duplicate</Key>".to_string(),
            2 => "<Extra a=\"1\">x</Extra>".to_string(),
            3 => "<Wrap><Key>nested</Key></Wrap>".to_string(),
            4 => "<X>&unknown;</X>".to_string(),
            _ => "<s3:Tag>x</s3:Tag>".to_string(),
        };
        children.push(odd);
    }
    // Shuffle.
    for i in (1..children.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        children.swap(i, j);
    }
    let sep = if pretty { "\n    " } else { "" };
    let mut xml = String::from("<Contents>");
    for child in children {
        xml.push_str(sep);
        xml.push_str(&child);
    }
    xml.push_str(if pretty {
        "\n  </Contents>"
    } else {
        "</Contents>"
    });
    (xml, fast)
}

/// A random ListObjectsV2 page and whether it stays in the fast subset.
fn gen_page(rng: &mut Rng) -> (u16, String, bool) {
    if rng.chance(2) {
        return (
            200,
            "<?xml version=\"1.0\"?><Error><Code>InternalError</Code><Message>x</Message></Error>"
                .into(),
            false,
        );
    }
    if rng.chance(2) {
        return (
            503,
            "<Error><Code>SlowDown</Code><Message>x</Message></Error>".into(),
            false,
        );
    }
    let pretty = rng.chance(30);
    let nl = if pretty { "\n  " } else { "" };
    let mut fast = true;
    let mut parts: Vec<String> = Vec::new();
    for _ in 0..rng.below(30) {
        let (xml, ok) = gen_contents(rng, pretty);
        fast &= ok;
        parts.push(xml);
    }
    for _ in 0..rng.below(4) {
        let at = rng.below(parts.len() as u64 + 1) as usize;
        parts.insert(
            at,
            format!(
                "<CommonPrefixes><Prefix>{}/</Prefix></CommonPrefixes>",
                gen_key(rng)
            ),
        );
    }
    let mut body = String::new();
    if rng.chance(80) {
        body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    }
    body.push_str(if rng.chance(80) {
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    } else {
        "<ListBucketResult>"
    });
    body.push_str(&format!("{nl}<Name>bucket</Name>{nl}<Prefix></Prefix>"));
    body.push_str(&format!("{nl}<KeyCount>{}</KeyCount>", parts.len()));
    body.push_str(&format!("{nl}<MaxKeys>1000</MaxKeys>"));
    match rng.below(3) {
        0 => body.push_str(&format!("{nl}<IsTruncated>true</IsTruncated>")),
        1 => body.push_str(&format!("{nl}<IsTruncated>false</IsTruncated>")),
        _ => {}
    }
    if rng.chance(50) {
        body.push_str(&format!(
            "{nl}<NextContinuationToken>{}</NextContinuationToken>",
            gen_key(rng)
        ));
    }
    for part in parts {
        body.push_str(nl);
        body.push_str(&part);
    }
    body.push_str(if pretty {
        "\n</ListBucketResult>\n"
    } else {
        "</ListBucketResult>"
    });
    (200, body, fast)
}

fn sdk_rows(output: &ListObjectsV2Output) -> Vec<(String, PropsView)> {
    output
        .contents()
        .iter()
        .filter_map(|o| Some((o.key()?.to_string(), view(&ObjectProps::from(o)))))
        .collect()
}

type PageSummary = (
    Option<bool>,
    Option<String>,
    Option<i32>,
    Vec<Option<String>>,
);

fn page_summary(output: &ListObjectsV2Output) -> PageSummary {
    (
        output.is_truncated(),
        output.next_continuation_token().map(str::to_string),
        output.key_count(),
        output
            .common_prefixes()
            .iter()
            .map(|cp| cp.prefix().map(str::to_string))
            .collect(),
    )
}

#[tokio::test]
async fn differential_against_sdk_deserializer() {
    let mut rng = Rng(0x0123_4567_89ab_cdef);
    let generated: Vec<(u16, String, bool)> = (0..600).map(|_| gen_page(&mut rng)).collect();
    let client = test_client(
        generated
            .iter()
            .map(|(status, body, _)| (*status, body.clone()))
            .collect(),
    );

    let (mut fast_pages, mut fallback_pages, mut error_pages, mut rows_compared) = (0, 0, 0, 0);
    for (index, (_, body, expect_fast)) in generated.iter().enumerate() {
        let token = index.to_string();
        let sdk = client
            .list_objects_v2()
            .bucket("bucket")
            .continuation_token(&token)
            .send()
            .await;
        let slot = ParsedPageSlot::default();
        let fast = client
            .list_objects_v2()
            .bucket("bucket")
            .continuation_token(&token)
            .customize()
            .interceptor(FastContentsInterceptor::new(slot.clone()))
            .send()
            .await;
        let parsed = slot.take();
        match (sdk, fast) {
            (Ok(sdk), Ok(fast)) => {
                let fast_rows = match &parsed {
                    Some(rows) => {
                        assert!(fast.contents().is_empty(), "page {index}: {body}");
                        fast_pages += 1;
                        rows_view(rows)
                    }
                    None => {
                        fallback_pages += 1;
                        sdk_rows(&fast)
                    }
                };
                let expected = sdk_rows(&sdk);
                rows_compared += expected.len();
                assert_eq!(fast_rows, expected, "page {index}: {body}");
                assert_eq!(page_summary(&fast), page_summary(&sdk), "page {index}");
                if *expect_fast {
                    assert!(parsed.is_some(), "page {index} fell back: {body}");
                }
            }
            (Err(_), Err(_)) => {
                error_pages += 1;
                assert!(parsed.is_none(), "page {index}: {body}");
            }
            (sdk, fast) => panic!(
                "page {index}: SDK ok={} but fast ok={}: {body}",
                sdk.is_ok(),
                fast.is_ok()
            ),
        }
    }
    eprintln!(
        "differential: {fast_pages} fast pages, {fallback_pages} fallback pages, \
         {error_pages} error pages, {rows_compared} rows compared"
    );
    assert!(
        fast_pages > 350,
        "fast {fast_pages}, fallback {fallback_pages}"
    );
    assert!(fallback_pages > 10, "fallback {fallback_pages}");
    assert!(error_pages > 5, "errors {error_pages}");
    assert!(rows_compared > 5_000, "rows {rows_compared}");
}
