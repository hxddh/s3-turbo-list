// ── Flat key-range cut search ──────────────────────────────
//
// A flat namespace (no CommonPrefix structure) is partitioned with single-key
// probes: `probe(start_after)` issues a max-keys=1 ListObjectsV2 under the
// listing prefix and returns the first real key strictly after `start_after`.
// A cut is always the key such a probe *returns*, so every boundary is an
// observed key; the synthetic `start_after` candidate only steers where it
// lands.
//
// Candidates come from the first position where the range's low key and its
// high end differ — the exclusive end bound, or, for an open-ended range, a
// near-maximal key discovered with probes.  A maximal digit run at that
// position is treated as a number (so `obj-000000001.snappy.parquet` ..
// `obj-000199000.snappy.parquet` is cut at `obj-000099500`), other
// alphanumerics by a midpoint over the alphabet the keys use (hex keys split
// within `0-9a-f`), anything else by code point, and the candidate is
// truncated right after the differing position: a long constant suffix plays
// no part in where the cut lands.
//
// Used by startup flat bisection (auto_hints) and runtime flat splitting
// (tasks_s3).  Nothing here runs on the listing hot path.

use std::future::Future;

/// Probes one search round issues concurrently.
const PROBE_FANOUT: usize = 8;
/// Rounds per k-ary search.  With `PROBE_FANOUT` points per round, four rounds
/// resolve a position among ~2400 key characters, and an ASCII character exactly.
const SEARCH_MAX_ROUNDS: usize = 4;
/// Character positions refined, from the first differing one, when estimating
/// an open range's high key.  Three decimal digits put a numeric cut within
/// ~1% of the true middle.
const HIGH_REFINE_POSITIONS: usize = 3;
/// Longest digit run treated as one number (fits in u128).
const MAX_DIGITS: usize = 38;
/// Stand-in for "the key ended here" when a range's low key is a prefix of
/// its high key: just below the printable characters.
const FLOOR_CODE_POINT: u32 = 0x1F;
/// Highest printable ASCII character; the search ceiling for ASCII positions.
const ASCII_CEILING: u32 = 0x7E;

/// Result of one cut search.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct FlatCut {
    /// A real key strictly inside the range, or `None` when the range has no
    /// key to cut at.
    pub cut: Option<String>,
    /// A near-maximal real key of the range discovered along the way, if the
    /// search had to estimate the range's high end.  Callers keep it so later
    /// open-ended ranges skip the estimate.
    pub high: Option<String>,
}

// ── Candidate generation (pure) ────────────────────────────

/// The character after `c` in code-point order, skipping surrogates.
fn next_char(c: char) -> Option<char> {
    match c as u32 {
        0xD7FF => Some('\u{E000}'),
        n => char::from_u32(n + 1),
    }
}

/// `key[..pos]` followed by the character after `key`'s character at `pos`:
/// the smallest string above every key that shares `key[..=pos]`.
fn bump_at(key: &str, pos: usize) -> Option<String> {
    let c = key.get(pos..)?.chars().next()?;
    let mut bumped = String::with_capacity(pos + 4);
    bumped.push_str(&key[..pos]);
    bumped.push(next_char(c)?);
    Some(bumped)
}

/// Longest alphanumeric run read from each key when inferring an alphabet.
const ALPHABET_SAMPLE: usize = 32;
/// Most positions a radix midpoint spans (62^6 fits comfortably in u64).
const RADIX_MAX_POSITIONS: usize = 6;

/// Leading ASCII-alphanumeric run of `s`, capped at `ALPHABET_SAMPLE` chars.
fn alnum_run(s: &str) -> &str {
    let len = s
        .bytes()
        .take(ALPHABET_SAMPLE)
        .take_while(u8::is_ascii_alphanumeric)
        .count();
    &s[..len]
}

/// The alphabet two alphanumeric tails are written in, inferred from the
/// tails themselves: every digit if either uses a digit, and for each letter
/// case `a`/`A` up to the largest letter either uses.  Hex keys therefore get
/// `0-9a-f`, not the 62 alphanumerics, and a midpoint over it lands where
/// such keys actually are (`0…`/`f…` splits at `7`, not at `K`).
fn infer_alphabet(lo_run: &str, hi_run: &str) -> Vec<u8> {
    let used = lo_run.bytes().chain(hi_run.bytes());
    let (mut digits, mut max_upper, mut max_lower) = (false, None::<u8>, None::<u8>);
    for b in used {
        match b {
            b'0'..=b'9' => digits = true,
            b'A'..=b'Z' => max_upper = max_upper.max(Some(b)),
            _ => max_lower = max_lower.max(Some(b)),
        }
    }
    let mut alphabet: Vec<u8> = Vec::new();
    if digits {
        alphabet.extend(b'0'..=b'9');
    }
    if let Some(top) = max_upper {
        alphabet.extend(b'A'..=top);
    }
    if let Some(top) = max_lower {
        alphabet.extend(b'a'..=top);
    }
    alphabet
}

/// Midpoint of two alphanumeric tails read as base-R numbers over their
/// inferred alphabet, using as few leading positions as leave a value
/// strictly between them.  Adjacent leading characters (`9…`/`a…` in hex)
/// simply take one more position: `9c…`/`a0…` gives `9e`.
fn radix_midpoint(lo_tail: &str, hi_tail: &str) -> Option<String> {
    let (lo_run, hi_run) = (alnum_run(lo_tail), alnum_run(hi_tail));
    if hi_run.is_empty() {
        return None;
    }
    let alphabet = infer_alphabet(lo_run, hi_run);
    let radix = alphabet.len() as u64;
    let index = |b: u8| alphabet.binary_search(&b).map_or(0, |i| i as u64);
    let value = |run: &str, positions: usize| -> u64 {
        let mut digits = run.bytes().map(index).chain(std::iter::repeat(0));
        (0..positions).fold(0u64, |acc, _| acc * radix + digits.next().unwrap_or(0))
    };
    for positions in 1..=RADIX_MAX_POSITIONS {
        let (a, b) = (value(lo_run, positions), value(hi_run, positions));
        if b < a {
            return None;
        }
        if b > a + 1 {
            let mut mid = a + (b - a) / 2;
            let mut encoded = vec![0u8; positions];
            for slot in encoded.iter_mut().rev() {
                *slot = alphabet[(mid % radix) as usize];
                mid /= radix;
            }
            return String::from_utf8(encoded).ok();
        }
    }
    None
}

/// A character strictly between `lo` (`None`: the low key ended) and `hi`,
/// by code point, skipping surrogates.
fn code_point_mid(lo: Option<char>, hi: char) -> Option<char> {
    let a = lo.map_or(FLOOR_CODE_POINT, |c| c as u32);
    let b = hi as u32;
    if b <= a + 1 {
        return None;
    }
    let m = a + (b - a) / 2;
    let m = if (0xD800..=0xDFFF).contains(&m) {
        if a < 0xD7FF { 0xD7FF } else { 0xE000 }
    } else {
        m
    };
    char::from_u32(m).filter(|&c| lo.is_none_or(|l| c > l) && c < hi)
}

/// Digits of `key` starting at `start`.
fn digit_run(key: &str, start: usize) -> &str {
    let len = key.as_bytes()[start..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    &key[start..start + len]
}

/// Midpoint of two digit runs read as equal-width numbers (the shorter one
/// right-padded with zeros, which preserves their lexicographic order).
/// `None` when no number lies strictly between them.
fn digit_midpoint(lo: &str, hi: &str) -> Option<String> {
    let width = lo.len().max(hi.len()).min(MAX_DIGITS);
    let value = |run: &str| -> u128 {
        run.bytes()
            .chain(std::iter::repeat(b'0'))
            .take(width)
            .fold(0u128, |acc, b| acc * 10 + u128::from(b - b'0'))
    };
    let (a, b) = (value(lo), value(hi));
    if b <= a.saturating_add(1) {
        return None;
    }
    Some(format!("{:0width$}", a + (b - a) / 2, width = width))
}

/// A `start_after` candidate near the middle of `(lo, hi)`, strictly inside
/// it and truncated right after the first position where `lo` and `hi`
/// differ.  `None` when no string can be placed between them at that depth
/// (adjacent keys).
pub(crate) fn flat_cut_candidate(lo: &str, listing_prefix: &str, hi: &str) -> Option<String> {
    midpoint(lo, hi).filter(|c| c.starts_with(listing_prefix))
}

fn midpoint(lo: &str, hi: &str) -> Option<String> {
    if hi <= lo {
        return None;
    }
    // First differing position, on a character boundary (the shared bytes
    // are identical in both keys, so a boundary in `lo` is one in `hi`).
    let mut d = lo
        .bytes()
        .zip(hi.bytes())
        .take_while(|(a, b)| a == b)
        .count();
    while !lo.is_char_boundary(d) {
        d -= 1;
    }
    let lo_c = lo[d..].chars().next();
    let hi_c = hi[d..].chars().next()?;
    let inside = |c: &String| c.as_str() > lo && c.as_str() < hi;

    // Maximal digit run through the differing position: cut numerically.
    if lo_c.is_some_and(|c| c.is_ascii_digit()) && hi_c.is_ascii_digit() {
        let start = d - lo.as_bytes()[..d]
            .iter()
            .rev()
            .take_while(|b| b.is_ascii_digit())
            .count();
        if let Some(mid) = digit_midpoint(digit_run(lo, start), digit_run(hi, start)) {
            let candidate = format!("{}{}", &lo[..start], mid);
            if inside(&candidate) {
                return Some(candidate);
            }
        }
    }

    // Alphanumeric at the differing position: midpoint over the keys'
    // inferred alphabet, spanning a few positions when needed.
    if hi_c.is_ascii_alphanumeric() && lo_c.is_none_or(|c| c.is_ascii_alphanumeric()) {
        if let Some(mid) = radix_midpoint(&lo[d..], &hi[d..]) {
            let candidate = format!("{}{}", &lo[..d], mid);
            if inside(&candidate) {
                return Some(candidate);
            }
        }
    }

    if let Some(mid) = code_point_mid(lo_c, hi_c) {
        let mut candidate = String::with_capacity(d + 4);
        candidate.push_str(&lo[..d]);
        candidate.push(mid);
        if inside(&candidate) {
            return Some(candidate);
        }
    }

    // Adjacent characters: the keys between lie under `lo`'s next character
    // (`lo[..d]` + `lo_c` + …), so cut between `lo`'s remainder and the top
    // of that subtree.
    let lc = lo_c?;
    let p = d + lc.len_utf8();
    let ceiling = match lo[p..].chars().next() {
        Some(c) if !c.is_ascii() => char::MAX,
        _ => '\u{7f}',
    };
    let mut subtree_top = String::with_capacity(p + 4);
    subtree_top.push_str(&lo[..p]);
    subtree_top.push(ceiling);
    midpoint(lo, &subtree_top).filter(|c| c.as_str() < hi)
}

// ── Probe-driven search ────────────────────────────────────

/// `count` distinct ascending points spread over `a..=b`, both ends included.
fn spread(a: u64, b: u64, count: usize) -> Vec<u64> {
    let span = b - a;
    if span < count as u64 {
        return (a..=b).collect();
    }
    let steps = count as u64 - 1;
    let mut points: Vec<u64> = (0..=steps).map(|j| a + span * j / steps).collect();
    points.dedup();
    points
}

/// One probe, keeping its key only when it lies inside `(lo, end)` and under
/// `scope`.  An absent candidate (no representable string) probes nothing.
async fn probe_scoped<F, Fut>(
    probe: &F,
    start_after: Option<String>,
    lo: &str,
    end: Option<&str>,
    scope: &str,
) -> Result<Option<String>, String>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Option<String>, String>>,
{
    let Some(start_after) = start_after else {
        return Ok(None);
    };
    Ok(probe(start_after)
        .await?
        .filter(|k| k.as_str() > lo && k.starts_with(scope) && end.is_none_or(|e| k.as_str() < e)))
}

/// Smallest index in `0..n` whose probe finds a key, for a predicate that is
/// monotone over the indices (absent … absent, present … present).  Each
/// round probes up to `PROBE_FANOUT` points concurrently.  Returns the index
/// and the key its probe found.
async fn search_first_present<E, EFut>(n: usize, eval: E) -> Result<Option<(usize, String)>, String>
where
    E: Fn(usize) -> EFut,
    EFut: Future<Output = Result<Option<String>, String>>,
{
    if n == 0 {
        return Ok(None);
    }
    let (mut a, mut b) = (0usize, n - 1);
    let mut best: Option<(usize, String)> = None;
    for _ in 0..SEARCH_MAX_ROUNDS {
        // `b` is already known present once `best` is set.
        let top = if best.is_some() { b - 1 } else { b };
        if best.is_some() && a > top {
            break;
        }
        let points = spread(a as u64, top as u64, PROBE_FANOUT);
        let results = futures::future::join_all(points.iter().map(|&i| eval(i as usize))).await;
        let mut last_absent: Option<usize> = None;
        let mut first_present: Option<(usize, String)> = None;
        for (&i, result) in points.iter().zip(results) {
            match result? {
                Some(key) if first_present.is_none() => first_present = Some((i as usize, key)),
                None if first_present.is_none() => last_absent = Some(i as usize),
                _ => {}
            }
        }
        match first_present {
            Some((i, key)) => {
                b = i;
                best = Some((i, key));
            }
            // Nothing present up to `top`: the answer is `b` if it was
            // already known, otherwise there is none.
            None => break,
        }
        if let Some(f) = last_absent {
            a = f + 1;
        }
        if a >= b {
            break;
        }
    }
    Ok(best)
}

/// Largest value in `(known, ceiling]` whose probe finds a key, for a
/// predicate that is monotone (present … present, absent … absent).  Returns
/// the key found for it, or `None` when no value above `known` is present.
/// `first_round`, when non-empty, replaces the first round's evenly spread
/// points (values outside `(known, ceiling]` are ignored).
async fn search_last_present<E, EFut>(
    known: u32,
    ceiling: u32,
    first_round: Vec<u64>,
    eval: E,
) -> Result<Option<String>, String>
where
    E: Fn(u32) -> EFut,
    EFut: Future<Output = Result<Option<String>, String>>,
{
    let (mut a, mut b) = (known, ceiling);
    let mut best: Option<String> = None;
    let mut seed = Some(first_round).filter(|points| !points.is_empty());
    for _ in 0..SEARCH_MAX_ROUNDS {
        if a >= b {
            break;
        }
        let points: Vec<u64> = match seed.take() {
            Some(points) => points
                .into_iter()
                .filter(|&v| v > u64::from(a) && v <= u64::from(b))
                .collect(),
            None => spread(u64::from(a) + 1, u64::from(b), PROBE_FANOUT),
        };
        let results = futures::future::join_all(points.iter().map(|&v| eval(v as u32))).await;
        let mut first_absent: Option<u32> = None;
        for (&v, result) in points.iter().zip(results) {
            match result? {
                Some(key) if first_absent.is_none() => {
                    a = v as u32;
                    best = Some(key);
                }
                None if first_absent.is_none() => first_absent = Some(v as u32),
                _ => {}
            }
        }
        if let Some(f) = first_absent {
            b = f - 1;
        }
    }
    Ok(best)
}

/// Estimate the high end of `(lo, end)` — `end` exclusive, `None` open to the
/// end of the listing prefix — as a real, near-maximal key inside it.
///
/// First finds the shallowest position where the range's keys stop sharing
/// `lo`'s characters (monotone, so a k-ary search over positions), then climbs
/// to the largest character present at that position and the next few,
/// each a k-ary search over characters.  Every round's probes run
/// concurrently; the rounds themselves are sequential.
pub(crate) async fn discover_high_key<F, Fut>(
    listing_prefix: &str,
    lo: &str,
    end: Option<&str>,
    probe: &F,
) -> Result<Option<String>, String>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Option<String>, String>>,
{
    if !lo.starts_with(listing_prefix) {
        return Ok(None);
    }
    // Candidate positions: every character of `lo` after the listing prefix,
    // then `lo`'s end (any key after `lo` at all).  Present at position i
    // means a key in range lies above everything sharing `lo[..=i]`.
    let mut positions: Vec<usize> = lo
        .char_indices()
        .map(|(i, _)| i)
        .filter(|&i| i >= listing_prefix.len())
        .collect();
    positions.push(lo.len());
    let found = search_first_present(positions.len(), |idx| {
        let pos = positions[idx];
        let start_after = if pos == lo.len() {
            Some(lo.to_string())
        } else {
            bump_at(lo, pos)
        };
        probe_scoped(probe, start_after, lo, end, listing_prefix)
    })
    .await?;
    let Some((idx, mut high)) = found else {
        return Ok(None);
    };

    // Climb: at each position take the largest character any in-range key
    // has there (given the characters already fixed before it).
    let mut pos = positions[idx];
    for _ in 0..HIGH_REFINE_POSITIONS {
        let Some(current) = high.get(pos..).and_then(|s| s.chars().next()) else {
            break;
        };
        let ceiling = if current.is_ascii() {
            ASCII_CEILING
        } else {
            char::MAX as u32
        };
        if (current as u32) < ceiling {
            let scope = high[..pos].to_string();
            // A digit is usually followed by more digits: probe every larger
            // digit plus a few points past '9' at once, so the common numeric
            // position resolves in a single round.
            // Past ASCII the domain spans a million code points while a
            // script's keys use a narrow block: step geometrically first.
            let first_round: Vec<u64> = if current.is_ascii_digit() {
                (u64::from(current) + 1..=u64::from('9'))
                    .chain(spread(u64::from(':'), u64::from(ceiling), 4))
                    .collect()
            } else if !current.is_ascii() {
                (0..PROBE_FANOUT as u32)
                    .map(|j| u64::from(current) + 4u64.pow(j))
                    .chain(std::iter::once(u64::from(ceiling)))
                    .collect()
            } else {
                Vec::new()
            };
            let found = search_last_present(current as u32, ceiling, first_round, |v| {
                let c = char::from_u32(v).unwrap_or('\u{E000}');
                probe_scoped(probe, Some(format!("{}{}", scope, c)), lo, end, &scope)
            })
            .await?;
            if let Some(key) = found.filter(|k| *k > high) {
                high = key;
            }
        }
        pos += high[pos..].chars().next().map_or(0, char::len_utf8);
    }
    Ok(Some(high))
}

/// Find one real key strictly inside `(lo, end)` near the middle of the
/// range's remaining keys.
///
/// With a known upper end — `end`, or `known_high` (a real key from an earlier
/// estimate) for an open range — this is a single probe at the midpoint
/// candidate.  Otherwise, or when that probe finds nothing inside the range
/// (the keys cluster below the midpoint), the range's high key is estimated
/// first and the midpoint probe runs against it.  A probe error aborts the
/// search.
pub(crate) async fn find_flat_cut<F, Fut>(
    listing_prefix: &str,
    lo: &str,
    end: Option<&str>,
    known_high: Option<&str>,
    probe: &F,
) -> Result<FlatCut, String>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Option<String>, String>>,
{
    let in_range = |k: &str| k > lo && k.starts_with(listing_prefix) && end.is_none_or(|e| k < e);
    let upper = match end {
        Some(e) if e.starts_with(listing_prefix) => Some(e),
        _ => known_high.filter(|h| in_range(h)),
    };
    if let Some(candidate) = upper.and_then(|u| flat_cut_candidate(lo, listing_prefix, u)) {
        if let Some(key) = probe(candidate).await?.filter(|k| in_range(k)) {
            return Ok(FlatCut {
                cut: Some(key),
                high: None,
            });
        }
    }

    let Some(high) = discover_high_key(listing_prefix, lo, end, probe).await? else {
        return Ok(FlatCut::default());
    };
    // `high` is a real key inside the range, so the first key after any
    // candidate below it is inside the range too.
    let cut = match flat_cut_candidate(lo, listing_prefix, &high) {
        Some(candidate) => probe(candidate).await?.filter(|k| in_range(k)),
        None => None,
    };
    // No midpoint (adjacent keys): `high` itself is a real in-range key.
    Ok(FlatCut {
        cut: Some(cut.unwrap_or_else(|| high.clone())),
        high: Some(high),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn cand(lo: &str, hi: &str) -> Option<String> {
        flat_cut_candidate(lo, "", hi)
    }

    fn assert_inside(lo: &str, hi: &str) -> String {
        let c = cand(lo, hi).unwrap_or_else(|| panic!("no candidate for ({lo}, {hi})"));
        assert!(
            c.as_str() > lo && c.as_str() < hi,
            "{c} not in ({lo}, {hi})"
        );
        c
    }

    #[test]
    fn test_digit_run_midpoint_ignores_constant_suffix() {
        let c = assert_inside(
            "obj-000000001.snappy.parquet",
            "obj-000199000.snappy.parquet",
        );
        // Truncated right after the numeric run: the suffix plays no part.
        assert_eq!(c, "obj-000099500");
        let c = assert_inside(
            "data/part-000000000-c000.snappy.parquet",
            "data/part-000200000-c000.snappy.parquet",
        );
        assert_eq!(c, "data/part-000100000");
    }

    #[test]
    fn test_digit_run_reaches_back_over_shared_digits() {
        // The keys first differ at the 4th digit; the whole run is one number.
        let c = assert_inside("k-0001234", "k-0009876");
        assert_eq!(c, "k-0005555");
    }

    #[test]
    fn test_digit_runs_of_unequal_width_keep_lexicographic_order() {
        // "10" < "9" lexicographically; padded they are 10 and 90.
        assert_eq!(assert_inside("v-10", "v-9"), "v-50");
        // Run ends in `lo` before `hi`'s: "12" vs "3456" → 1200 and 3456.
        assert_eq!(assert_inside("v-12.x", "v-3456.x"), "v-2328");
    }

    #[test]
    fn test_hex_keys_split_in_the_middle_of_the_alphabet() {
        // Digits then lower-case letters, skipping the punctuation and
        // upper-case letters between them.
        assert_eq!(assert_inside("0a3f", "f91c"), "7");
        assert_eq!(assert_inside("0000", "FFFF"), "7");
        assert_eq!(assert_inside("a000", "f000"), "c");
    }

    #[test]
    fn test_adjacent_hex_characters_take_one_more_position() {
        // '9' and 'a' are neighbours in hex: the cut goes one position deeper
        // instead of into punctuation no key uses.
        assert_eq!(assert_inside("9c3f.bin", "a07b.bin"), "9e");
        assert_eq!(assert_inside("3a7f", "4c21"), "43");
        // Upper-case and mixed alphanumerics stay inside their alphabet.
        let c = assert_inside("Kx9", "Lb2");
        assert!(c.bytes().all(|b| b.is_ascii_alphanumeric()), "{c}");
    }

    #[test]
    fn test_non_alphanumeric_uses_code_point_midpoint() {
        assert_eq!(assert_inside("a!", "a/"), "a(");
    }

    #[test]
    fn test_unicode_keys_stay_valid_utf8() {
        for (a, b) in [
            ("中文键", "中文钥"),
            ("日本/ア", "日本/ン"),
            ("a中", "a文"),
            ("é1", "ê1"),
            ("中文一", "中文丁"),
        ] {
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            let c = assert_inside(lo, hi);
            assert!(std::str::from_utf8(c.as_bytes()).is_ok());
        }
        // Midpoint that would land on a surrogate code point skips them.
        let c = assert_inside("\u{D000}", "\u{E800}");
        assert!(
            c.chars()
                .all(|ch| !(0xD800..=0xDFFF).contains(&(ch as u32)))
        );
    }

    #[test]
    fn test_adjacent_characters_descend_into_the_low_subtree() {
        // No character between '4' and '5': cut inside lo's subtree.
        let c = assert_inside("obj-4.snappy", "obj-5.snappy");
        assert!(c.starts_with("obj-4"), "{c}");
        let c = assert_inside("ab", "ac");
        assert!(c.starts_with("ab"), "{c}");
    }

    #[test]
    fn test_no_candidate_between_adjacent_keys() {
        // `lo` is a prefix of `hi` and nothing sorts between their tails.
        assert_eq!(cand("a", "a "), None);
        assert_eq!(cand("a", "a"), None);
        assert_eq!(cand("b", "a"), None);
        // Adjacent characters with `lo` at the ASCII ceiling still descend.
        assert_inside("a~", "b");
    }

    #[test]
    fn test_candidate_respects_listing_prefix() {
        let c = flat_cut_candidate("logs/0001", "logs/", "logs/9999").unwrap();
        assert_eq!(c, "logs/5000");
        let c = flat_cut_candidate("logs0", "logs0", "logs1").unwrap();
        assert!(c.starts_with("logs0"), "{c}");
    }

    /// Max-keys=1 probe over a sorted key set, counting requests.
    fn probe_over(
        keys: Arc<Vec<String>>,
        count: Arc<AtomicUsize>,
    ) -> impl Fn(String) -> std::future::Ready<Result<Option<String>, String>> {
        move |sa: String| {
            count.fetch_add(1, Ordering::Relaxed);
            let i = keys.partition_point(|k| k.as_str() <= sa.as_str());
            std::future::ready(Ok(keys.get(i).cloned()))
        }
    }

    fn position(keys: &[String], key: &str) -> usize {
        keys.partition_point(|k| k.as_str() < key)
    }

    #[tokio::test]
    async fn test_open_range_cut_lands_near_the_middle() {
        for shape in [
            "obj-{:09}.snappy.parquet",
            "data/part-{:09}-c000.snappy.parquet",
        ] {
            let keys: Vec<String> = (0..200_000)
                .map(|i| shape.replace("{:09}", &format!("{:09}", i)))
                .collect();
            let keys = Arc::new(keys);
            let count = Arc::new(AtomicUsize::new(0));
            let probe = probe_over(Arc::clone(&keys), Arc::clone(&count));
            let found = find_flat_cut("", &keys[0], None, None, &probe)
                .await
                .unwrap();
            let cut = found.cut.unwrap();
            let at = position(&keys, &cut);
            assert!((90_000..=110_000).contains(&at), "cut {cut} at {at}");
            let high = found.high.unwrap();
            assert!(position(&keys, &high) >= 198_000, "high {high}");
            let probes = count.load(Ordering::Relaxed);
            assert!(probes <= 120, "{probes} probes");

            // With the high key known, a later cut is a single probe.
            count.store(0, Ordering::Relaxed);
            let found = find_flat_cut("", &cut, None, Some(&high), &probe)
                .await
                .unwrap();
            let at2 = position(&keys, &found.cut.unwrap());
            assert!((140_000..=160_000).contains(&at2), "second cut at {at2}");
            assert_eq!(count.load(Ordering::Relaxed), 1);
        }
    }

    #[tokio::test]
    async fn test_bounded_range_cut_is_one_probe() {
        let keys: Arc<Vec<String>> = Arc::new(
            (0..10_000)
                .map(|i| format!("obj-{:09}.snappy.parquet", i))
                .collect(),
        );
        let count = Arc::new(AtomicUsize::new(0));
        let probe = probe_over(Arc::clone(&keys), Arc::clone(&count));
        let found = find_flat_cut("", &keys[1000], Some(&keys[3000]), None, &probe)
            .await
            .unwrap();
        assert_eq!(found.cut.as_deref(), Some(keys[2000].as_str()));
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_skewed_range_falls_back_to_estimating_the_high_key() {
        // Keys cluster far below the exclusive end bound.
        let mut keys: Vec<String> = (0..1000).map(|i| format!("a-{:04}", i)).collect();
        keys.push("z".to_string());
        let keys = Arc::new(keys);
        let count = Arc::new(AtomicUsize::new(0));
        let probe = probe_over(Arc::clone(&keys), Arc::clone(&count));
        let found = find_flat_cut("", "a-0000", Some("z"), None, &probe)
            .await
            .unwrap();
        let at = position(&keys, &found.cut.unwrap());
        assert!((400..=600).contains(&at), "cut at {at}");
    }

    #[tokio::test]
    async fn test_hex_and_unicode_ranges_cut_inside() {
        let hex: Vec<String> = {
            let mut v: Vec<String> = (0u64..5000)
                .map(|i| format!("{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
                .collect();
            v.sort();
            v
        };
        let uni: Vec<String> = {
            let mut v: Vec<String> = (0u32..3000)
                .map(|i| format!("文件/{}-{:04}", char::from_u32(0x4E00 + i % 50).unwrap(), i))
                .collect();
            v.sort();
            v
        };
        for (keys, lo_frac, hi_frac) in [(hex, 25, 75), (uni, 10, 90)] {
            let n = keys.len();
            let keys = Arc::new(keys);
            let probe = probe_over(Arc::clone(&keys), Arc::new(AtomicUsize::new(0)));
            let found = find_flat_cut("", &keys[0], None, None, &probe)
                .await
                .unwrap();
            let at = position(&keys, &found.cut.unwrap());
            assert!(
                at > n * lo_frac / 100 && at < n * hi_frac / 100,
                "cut at {at} of {n}"
            );
        }
    }

    #[tokio::test]
    async fn test_adjacent_real_keys_have_no_cut() {
        let keys = Arc::new(vec!["k1".to_string(), "k2".to_string()]);
        let probe = probe_over(Arc::clone(&keys), Arc::new(AtomicUsize::new(0)));
        let found = find_flat_cut("", "k1", Some("k2"), None, &probe)
            .await
            .unwrap();
        assert_eq!(found.cut, None);
        // Open range with a single key after lo: that key is the only cut.
        let found = find_flat_cut("", "k1", None, None, &probe).await.unwrap();
        assert_eq!(found.cut.as_deref(), Some("k2"));
        let found = find_flat_cut("", "k2", None, None, &probe).await.unwrap();
        assert_eq!(found, FlatCut::default());
    }

    #[tokio::test]
    async fn test_probe_error_aborts_the_search() {
        let probe = |_sa: String| std::future::ready(Err::<Option<String>, _>("boom".to_string()));
        assert!(find_flat_cut("", "a", None, None, &probe).await.is_err());
    }

    #[tokio::test]
    async fn test_listing_prefix_scopes_the_search() {
        let keys: Vec<String> = (0..500)
            .map(|i| format!("logs/{:05}", i))
            .chain((0..500).map(|i| format!("other/{:05}", i)))
            .collect();
        let keys = Arc::new(keys);
        let base = probe_over(Arc::clone(&keys), Arc::new(AtomicUsize::new(0)));
        // A probe under a listing prefix only sees that prefix's keys.
        let probe = |sa: String| {
            let r = base(sa);
            async move { Ok(r.await?.filter(|k| k.starts_with("logs/"))) }
        };
        let found = find_flat_cut("logs/", "logs/00000", None, None, &probe)
            .await
            .unwrap();
        let cut = found.cut.unwrap();
        assert!(cut.starts_with("logs/"), "{cut}");
        let at = position(&keys, &cut);
        assert!((200..=300).contains(&at), "cut at {at}");
    }
}
