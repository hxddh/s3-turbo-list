//! Fast path for the `<Contents>` elements of a ListObjectsV2 page.
//!
//! Profiling a large local listing put ~80% of the listing CPU in the SDK's
//! XML deserialization of ListObjectsV2: every `<Object>` is tokenized, each
//! field is copied into an owned `String`, and the engine then converts the
//! result into its own `(ObjectKey, ObjectProps)` rows.  This module does that
//! conversion straight from the response bytes.
//!
//! The flat-list loop attaches a [`FastContentsInterceptor`] to each
//! ListObjectsV2 request.  For an HTTP 200 response it wraps the body; once
//! the body is fully read, [`parse_list_page`] makes one forward pass over it,
//! builds the rows for every top-level `<Contents>` element, and hands the SDK
//! the same document with those elements cut out.  The SDK therefore still
//! parses everything else — `IsTruncated`, `NextContinuationToken`,
//! `KeyCount`, `CommonPrefixes`, and error documents — exactly as before, but
//! over a few hundred bytes instead of the whole page.  The rows go into the
//! request's own [`ParsedPageSlot`]; nothing is shared between requests.
//!
//! The parser only accepts a strict, well-formed subset of XML.  Anything
//! outside it — CDATA, comments, processing instructions (other than the XML
//! declaration), a DOCTYPE, namespace-prefixed element names, attributes on
//! anything but the root, non-UTF-8 input, characters XML forbids, an unknown
//! entity or an invalid character reference, nested markup in a field,
//! duplicate fields, a `<Contents>` without a `<Key>`, a field value the SDK
//! would reject — makes it give up on that page: the body is passed to the SDK
//! untouched, the slot stays empty, and the page takes the SDK path.  The
//! fallback is per page and automatic, so the fast path can only change how a
//! page is parsed, never what it lists.

use crate::core::{ObjectKey, ObjectProps};
use aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextMut;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat, SdkBody};
use aws_smithy_runtime_api::box_error::BoxError;
use bytes::Bytes;
use memchr::{memchr, memmem};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Rows of one page, in document order.
pub(crate) type PageObjects = Vec<(ObjectKey, ObjectProps)>;

/// Nesting allowed below the root before the parser gives up on a page.
const MAX_DEPTH: usize = 32;

// ── Per-request hand-off ───────────────────────────────────

/// Where one request's interceptor leaves the rows it parsed.  Each request
/// gets a fresh slot, so a page can never be attributed to another request.
#[derive(Debug, Clone, Default)]
pub(crate) struct ParsedPageSlot(Arc<Mutex<Option<PageObjects>>>);

impl ParsedPageSlot {
    /// The rows parsed from the response, or `None` when the page must be read
    /// from the SDK output (fallback, non-200 response, or no response).
    pub(crate) fn take(&self) -> Option<PageObjects> {
        self.lock().take()
    }

    fn put(&self, objects: Option<PageObjects>) {
        *self.lock() = objects;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<PageObjects>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Operation-level interceptor that routes a ListObjectsV2 response body
/// through [`parse_list_page`].  Attach it with
/// `.customize().interceptor(FastContentsInterceptor::new(slot))`.
#[derive(Debug)]
pub(crate) struct FastContentsInterceptor {
    slot: ParsedPageSlot,
}

impl FastContentsInterceptor {
    pub(crate) fn new(slot: ParsedPageSlot) -> Self {
        Self { slot }
    }
}

impl Intercept for FastContentsInterceptor {
    fn name(&self) -> &'static str {
        "S3TurboListFastContents"
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // A new attempt must never see a previous attempt's rows.
        self.slot.put(None);
        let response = context.response_mut();
        if response.status().as_u16() != 200 {
            return Ok(());
        }
        // The body is still streaming here (the SDK reads it after this hook),
        // so wrap it: the rewrite happens when the SDK reads it to the end.
        let inner = std::mem::replace(response.body_mut(), SdkBody::taken());
        *response.body_mut() = SdkBody::from_body_1_x(StripContentsBody {
            inner,
            first: None,
            rest: Vec::new(),
            slot: self.slot.clone(),
            done: false,
        });
        Ok(())
    }
}

/// Response body wrapper: collects the whole body, then yields either the
/// document with its `<Contents>` elements removed (rows go to the slot) or
/// the original bytes unchanged.
struct StripContentsBody {
    inner: SdkBody,
    /// First data chunk, kept as-is so a single-chunk body is never copied.
    first: Option<Bytes>,
    /// Concatenation of all chunks once a second one arrives.
    rest: Vec<u8>,
    slot: ParsedPageSlot,
    done: bool,
}

impl StripContentsBody {
    fn push(&mut self, chunk: Bytes) {
        if chunk.is_empty() {
            return;
        }
        match self.first.take() {
            None if self.rest.is_empty() => self.first = Some(chunk),
            None => self.rest.extend_from_slice(&chunk),
            Some(first) => {
                let hint = http_body::Body::size_hint(&self.inner).lower() as usize;
                self.rest
                    .reserve(first.len() + chunk.len() + hint.saturating_sub(chunk.len()));
                self.rest.extend_from_slice(&first);
                self.rest.extend_from_slice(&chunk);
            }
        }
    }

    fn finish(&mut self) -> Bytes {
        let original = match self.first.take() {
            Some(first) => first,
            None => Bytes::from(std::mem::take(&mut self.rest)),
        };
        match parse_list_page(&original) {
            Some((objects, stripped)) => {
                self.slot.put(Some(objects));
                Bytes::from(stripped)
            }
            None => original,
        }
    }
}

impl http_body::Body for StripContentsBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        loop {
            match http_body::Body::poll_frame(Pin::new(&mut this.inner), cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(Some(Ok(frame))) => {
                    // ListObjectsV2 has no trailers; anything but data is
                    // dropped, as the SDK's own body collection ignores it.
                    if let Ok(data) = frame.into_data() {
                        this.push(data);
                    }
                }
                Poll::Ready(None) => {
                    this.done = true;
                    let body = this.finish();
                    return Poll::Ready(Some(Ok(http_body::Frame::data(body))));
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

// ── Field conversions (mirror `ObjectProps::from(&Object)`) ─

/// Epoch seconds → the `last_modified` column.  The SDK path computes the same
/// value as `DateTime::secs() as u64` in `ObjectProps::from`; both paths must
/// agree, which the differential test below checks.
#[inline]
pub(crate) fn epoch_secs_to_u64(secs: i64) -> u64 {
    secs as u64
}

/// ETag text (entity-unescaped) → `(etag_md5, etag_parts)`, byte-for-byte the
/// rules of `ObjectProps::from(&Object)`, including what an unparseable value
/// leaves in the digest.
fn parse_etag(raw: &[u8]) -> ([u8; 16], u32) {
    let mut md5 = [0u8; 16];
    if raw.len() == 34 {
        if let Some(hex_span) = raw.get(1..33) {
            if hex::decode_to_slice(hex_span, &mut md5).is_ok() {
                return (md5, 0);
            }
        }
    } else if raw.len() >= 36 && raw.get(33) == Some(&b'-') {
        if let Some(hex_span) = raw.get(1..33) {
            if hex::decode_to_slice(hex_span, &mut md5).is_ok() {
                if let Some(parts) = raw
                    .get(34..raw.len() - 1)
                    .and_then(|p| std::str::from_utf8(p).ok())
                    .and_then(|p| p.parse::<u32>().ok())
                {
                    return (md5, parts);
                }
            }
        }
    }
    (md5, 0)
}

/// Whole epoch seconds of an RFC 3339 timestamp, as the SDK parses
/// `LastModified` (`DateTime::from_str(.., DateTimeWithOffset)`, then
/// `secs()`, which floors).  `None` where the SDK would reject the value.
fn parse_timestamp_secs(s: &str) -> Option<i64> {
    if let Some(secs) = parse_canonical_timestamp(s.as_bytes()) {
        return Some(secs);
    }
    DateTime::from_str(s, DateTimeFormat::DateTimeWithOffset)
        .ok()
        .map(|dt| dt.secs())
}

/// The form S3 endpoints actually send — `YYYY-MM-DDTHH:MM:SS[.f{1,9}]Z`,
/// year 1970 or later, every field in range — parsed without the SDK's
/// general RFC 3339 parser.  Anything else returns `None` and is left to
/// `DateTime::from_str`, so unusual input keeps the SDK's exact semantics.
fn parse_canonical_timestamp(b: &[u8]) -> Option<i64> {
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || *b.last()? != b'Z'
    {
        return None;
    }
    let fraction = &b[19..b.len() - 1];
    if !fraction.is_empty() {
        let digits = &fraction[1..];
        if fraction[0] != b'.'
            || digits.is_empty()
            || digits.len() > 9
            || !digits.iter().all(u8::is_ascii_digit)
        {
            return None;
        }
    }
    let num = |range: std::ops::Range<usize>| -> Option<i64> {
        let mut value = 0i64;
        for &c in &b[range] {
            if !c.is_ascii_digit() {
                return None;
            }
            value = value * 10 + i64::from(c - b'0');
        }
        Some(value)
    };
    let year = num(0..4)?;
    let month = num(5..7)?;
    let day = num(8..10)?;
    let hour = num(11..13)?;
    let minute = num(14..16)?;
    let second = num(17..19)?;
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if year < 1970 || day < 1 || day > days_in_month || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    // Days from civil (proleptic Gregorian), Howard Hinnant's algorithm.
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hour * 3_600 + minute * 60 + second)
}

// ── XML text ───────────────────────────────────────────────

/// Append the entity-unescaped form of `s` to `out`, with exactly the rules of
/// the SDK's XML decoder (`aws_smithy_xml`): the five predefined entities,
/// `&#NN;` and `&#xHH;` references to any valid `char` (control characters
/// included), and `None` for anything else.
fn unescape_into(s: &str, out: &mut String) -> Option<()> {
    let bytes = s.as_bytes();
    let Some(first) = memchr(b'&', bytes) else {
        out.push_str(s);
        return Some(());
    };
    out.reserve(s.len());
    out.push_str(&s[..first]);
    let mut amp = first;
    loop {
        let start = amp + 1;
        let next_amp = memchr(b'&', &bytes[start..]).map(|i| start + i);
        let section = &s[start..next_amp.unwrap_or(bytes.len())];
        let semi = memchr(b';', section.as_bytes())?;
        match &section[..semi] {
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "amp" => out.push('&'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            entity => {
                let (digits, radix) = if let Some(hex) = entity.strip_prefix("#x") {
                    (hex, 16)
                } else if let Some(dec) = entity.strip_prefix('#') {
                    (dec, 10)
                } else {
                    return None;
                };
                let code = u32::from_str_radix(digits, radix).ok()?;
                out.push(char::from_u32(code)?);
            }
        }
        out.push_str(&section[semi + 1..]);
        match next_amp {
            Some(next) => amp = next,
            None => return Some(()),
        }
    }
}

/// Owned, unescaped copy of a text node (the key: one allocation).
fn unescape_owned(s: &str) -> Option<String> {
    if memchr(b'&', s.as_bytes()).is_none() {
        return Some(s.to_owned());
    }
    let mut out = String::with_capacity(s.len());
    unescape_into(s, &mut out)?;
    Some(out)
}

/// Unescape into the reusable scratch buffer; returns the text.
fn unescape_scratch<'s>(s: &str, scratch: &'s mut String) -> Option<&'s str> {
    scratch.clear();
    unescape_into(s, scratch)?;
    Some(scratch.as_str())
}

/// Check that a text node the SDK would unescape is valid, without keeping it.
fn validate_text(s: &str, scratch: &mut String) -> Option<()> {
    if memchr(b'&', s.as_bytes()).is_some() {
        unescape_scratch(s, scratch)?;
    }
    Some(())
}

#[inline]
fn is_xml_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r')
}

fn all_xml_space(b: &[u8]) -> bool {
    b.iter().all(|&c| is_xml_space(c))
}

/// Characters the XML tokenizer rejects anywhere in a document: C0 controls
/// other than tab/LF/CR, and U+FFFE / U+FFFF.  (Character *references* to
/// controls are fine and handled by `unescape_into`.)
fn has_forbidden_chars(b: &[u8]) -> bool {
    let controls = b.chunks(64).any(|chunk| {
        chunk.iter().fold(false, |found, &c| {
            found | (c < 0x20 && c != b'\t' && c != b'\n' && c != b'\r')
        })
    });
    controls || memmem::find_iter(b, b"\xEF\xBF").any(|i| matches!(b.get(i + 2), Some(0xBE | 0xBF)))
}

// ── Tags ───────────────────────────────────────────────────

/// End of an element name starting at `start`: ASCII `[A-Za-z_][A-Za-z0-9_.-]*`
/// followed by whitespace, `>` or `/`.  Namespace prefixes (`:`) and
/// non-ASCII names are outside the fast subset.
#[inline]
fn name_end(b: &[u8], start: usize) -> Option<usize> {
    let first = *b.get(start)?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    let mut i = start + 1;
    while let Some(&c) = b.get(i) {
        if c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.') {
            i += 1;
        } else if is_xml_space(c) || c == b'>' || c == b'/' {
            return Some(i);
        } else {
            return None;
        }
    }
    None
}

#[inline]
fn skip_space(b: &[u8], mut i: usize) -> usize {
    while b.get(i).is_some_and(|&c| is_xml_space(c)) {
        i += 1;
    }
    i
}

/// Start tag at `lt` (`b[lt] == '<'`) without attributes: returns
/// `(name, index after '>', self_closing)`.
#[inline]
fn open_tag(b: &[u8], lt: usize) -> Option<(&[u8], usize, bool)> {
    let end = name_end(b, lt + 1)?;
    let name = &b[lt + 1..end];
    let i = skip_space(b, end);
    match b.get(i)? {
        b'>' => Some((name, i + 1, false)),
        b'/' if b.get(i + 1) == Some(&b'>') => Some((name, i + 2, true)),
        _ => None,
    }
}

/// End tag at `lt` (`b[lt..]` starts with `</`): returns `(name, index after '>')`.
#[inline]
fn close_tag(b: &[u8], lt: usize) -> Option<(&[u8], usize)> {
    let end = name_end(b, lt + 2)?;
    let i = skip_space(b, end);
    (b.get(i) == Some(&b'>')).then(|| (&b[lt + 2..end], i + 1))
}

/// Text-only element body starting at `start` (just after the start tag of
/// `name`): returns `(text, index after the matching end tag)`.  Nested markup
/// of any kind is outside the fast subset.
#[inline]
fn leaf_text<'a>(text: &'a str, start: usize, name: &[u8]) -> Option<(&'a str, usize)> {
    let b = text.as_bytes();
    let lt = start + memchr(b'<', &b[start..])?;
    if b.get(lt + 1) != Some(&b'/') {
        return None;
    }
    let (close, after) = close_tag(b, lt)?;
    (close == name).then(|| (&text[start..lt], after))
}

// ── Page parser ────────────────────────────────────────────

/// Parse a ListObjectsV2 response body.  Returns the rows of its top-level
/// `<Contents>` elements and the document with those elements removed, or
/// `None` when the page is outside the fast subset (the caller then passes
/// the body to the SDK unchanged).
pub(crate) fn parse_list_page(body: &[u8]) -> Option<(PageObjects, Vec<u8>)> {
    let text = std::str::from_utf8(body).ok()?;
    if has_forbidden_chars(body) || memmem::find(body, b"]]>").is_some() {
        return None;
    }
    let b = body;

    // Prolog: an optional XML declaration, then whitespace.
    let mut pos = 0;
    if b.starts_with(b"<?xml ") {
        pos = 6 + memmem::find(&b[6..], b"?>")? + 2;
    }
    pos = skip_space(b, pos);

    // Root start tag.  Its attributes (the S3 namespace) pass through to the
    // SDK untouched; only the end of the tag has to be found.
    const ROOT: &[u8] = b"ListBucketResult";
    if !b[pos..].starts_with(b"<") || name_end(b, pos + 1)? != pos + 1 + ROOT.len() {
        return None;
    }
    if &b[pos + 1..pos + 1 + ROOT.len()] != ROOT {
        return None;
    }
    pos = root_tag_end(b, pos + 1 + ROOT.len())?;

    let template = ObjectProps::from(&aws_sdk_s3::types::Object::builder().build());
    let mut objects: PageObjects = Vec::with_capacity((b.len() / 256).min(1024));
    let mut stripped: Vec<u8> = Vec::with_capacity(1024);
    let mut copied = 0;
    let mut scratch = String::new();
    // Open elements below the root.
    let mut stack: Vec<&[u8]> = Vec::with_capacity(8);

    loop {
        let lt = pos + memchr(b'<', &b[pos..])?;
        // Text directly under the root may only be whitespace: cutting an
        // element out would otherwise join two text runs.
        if stack.is_empty() && !all_xml_space(&b[pos..lt]) {
            return None;
        }
        match *b.get(lt + 1)? {
            b'/' => {
                let (name, after) = close_tag(b, lt)?;
                match stack.pop() {
                    Some(open) if open == name => pos = after,
                    Some(_) => return None,
                    None => {
                        if name != ROOT || !all_xml_space(&b[after..]) {
                            return None;
                        }
                        stripped.extend_from_slice(&b[copied..]);
                        return Some((objects, stripped));
                    }
                }
            }
            b'!' | b'?' => return None,
            _ => {
                let (name, after, self_closing) = open_tag(b, lt)?;
                if name == b"Contents" {
                    // The SDK treats the children of elements it does not
                    // know as siblings, so a nested `<Contents>` could still
                    // be read as an object.  Only top-level ones are cut out.
                    if !stack.is_empty() || self_closing {
                        return None;
                    }
                    let (row, end) = parse_contents(text, after, &template, &mut scratch)?;
                    objects.push(row);
                    stripped.extend_from_slice(&b[copied..lt]);
                    copied = end;
                    pos = end;
                } else {
                    if !self_closing {
                        if stack.len() >= MAX_DEPTH {
                            return None;
                        }
                        stack.push(name);
                    }
                    pos = after;
                }
            }
        }
    }
}

/// Skip the root's attributes (`S Name S? = S? "value"`), returning the index
/// after its `>`.  A self-closing root is outside the fast subset.
fn root_tag_end(b: &[u8], mut i: usize) -> Option<usize> {
    loop {
        let spaced = b.get(i).is_some_and(|&c| is_xml_space(c));
        i = skip_space(b, i);
        match *b.get(i)? {
            b'>' => return Some(i + 1),
            b'/' | b'<' => return None,
            _ if !spaced => return None,
            _ => {}
        }
        while b
            .get(i)
            .is_some_and(|&c| !is_xml_space(c) && !matches!(c, b'=' | b'>' | b'/' | b'<'))
        {
            i += 1;
        }
        i = skip_space(b, i);
        if b.get(i) != Some(&b'=') {
            return None;
        }
        i = skip_space(b, i + 1);
        let quote = *b.get(i)?;
        if quote != b'"' && quote != b'\'' {
            return None;
        }
        let close = i + 1 + memchr(quote, &b[i + 1..])?;
        if memchr(b'<', &b[i + 1..close]).is_some() {
            return None;
        }
        i = close + 1;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Field {
    Key,
    LastModified,
    ETag,
    Size,
    /// A child the SDK reads as text but that does not affect a row
    /// (`StorageClass`, `ChecksumAlgorithm`, ... and unknown elements).
    Other,
    Owner,
    RestoreStatus,
}

/// One `<Contents>` element, starting just after its start tag.  Returns the
/// row and the index after `</Contents>`.
fn parse_contents(
    text: &str,
    mut pos: usize,
    template: &ObjectProps,
    scratch: &mut String,
) -> Option<((ObjectKey, ObjectProps), usize)> {
    let b = text.as_bytes();
    let mut key: Option<String> = None;
    let mut last_modified: Option<i64> = None;
    let mut etag: Option<([u8; 16], u32)> = None;
    let mut size: Option<i64> = None;

    loop {
        let lt = pos + memchr(b'<', &b[pos..])?;
        if memchr(b'&', &b[pos..lt]).is_some() {
            return None;
        }
        match *b.get(lt + 1)? {
            b'/' => {
                let (name, after) = close_tag(b, lt)?;
                if name != b"Contents" {
                    return None;
                }
                pos = after;
                break;
            }
            b'!' | b'?' => return None,
            _ => {}
        }
        let (name, after, self_closing) = open_tag(b, lt)?;
        let field = match name {
            b"Key" => Field::Key,
            b"LastModified" => Field::LastModified,
            b"ETag" => Field::ETag,
            b"Size" => Field::Size,
            b"Owner" => Field::Owner,
            b"RestoreStatus" => Field::RestoreStatus,
            _ => Field::Other,
        };
        if matches!(field, Field::Owner | Field::RestoreStatus) {
            pos = if self_closing {
                after
            } else {
                parse_struct(text, after, name, field, scratch)?
            };
            continue;
        }
        // A self-closing field reads as "" in the SDK.
        let (raw, end) = if self_closing {
            ("", after)
        } else {
            leaf_text(text, after, name)?
        };
        pos = end;
        match field {
            Field::Key => {
                if key.is_some() {
                    return None;
                }
                key = Some(unescape_owned(raw)?);
            }
            Field::LastModified => {
                if last_modified.is_some() {
                    return None;
                }
                last_modified = Some(parse_timestamp_secs(unescape_scratch(raw, scratch)?)?);
            }
            Field::ETag => {
                if etag.is_some() {
                    return None;
                }
                etag = Some(parse_etag(unescape_scratch(raw, scratch)?.as_bytes()));
            }
            Field::Size => {
                if size.is_some() {
                    return None;
                }
                size = Some(unescape_scratch(raw, scratch)?.parse::<i64>().ok()?);
            }
            _ => validate_text(raw, scratch)?,
        }
    }

    let mut props = template.clone();
    if let Some((md5, parts)) = etag {
        props.etag_md5 = md5;
        props.etag_parts = parts;
    }
    props.last_modified = last_modified.map_or(0, epoch_secs_to_u64);
    props.size = size.map_or(0, |s| s as u64);
    Some(((ObjectKey::from(key?), props), pos))
}

/// `<Owner>` / `<RestoreStatus>`: children must be text-only; the values the
/// SDK parses into typed fields must be ones it accepts.
fn parse_struct(
    text: &str,
    mut pos: usize,
    struct_name: &[u8],
    field: Field,
    scratch: &mut String,
) -> Option<usize> {
    let b = text.as_bytes();
    loop {
        let lt = pos + memchr(b'<', &b[pos..])?;
        if memchr(b'&', &b[pos..lt]).is_some() {
            return None;
        }
        match *b.get(lt + 1)? {
            b'/' => {
                let (name, after) = close_tag(b, lt)?;
                return (name == struct_name).then_some(after);
            }
            b'!' | b'?' => return None,
            _ => {}
        }
        let (name, after, self_closing) = open_tag(b, lt)?;
        let (raw, end) = if self_closing {
            ("", after)
        } else {
            leaf_text(text, after, name)?
        };
        pos = end;
        let value = unescape_scratch(raw, scratch)?;
        if field == Field::RestoreStatus {
            match name {
                b"IsRestoreInProgress" => {
                    value.parse::<bool>().ok()?;
                }
                b"RestoreExpiryDate" => {
                    parse_timestamp_secs(value)?;
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests;
