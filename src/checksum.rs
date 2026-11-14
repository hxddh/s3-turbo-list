//! `diff --compare checksum`: settle a changed pair whose sizes match by the
//! checksums the store keeps for each object (HeadObject with checksum mode
//! on). Multipart uploads and SSE-KMS/SSE-C give the same bytes different
//! ETags; their stored checksums still agree.

use crate::data_map::ChecksumVerifier;
use std::sync::Arc;
use std::time::Duration;

/// The checksums HeadObject reported for one object.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Checksums {
    /// `(algorithm, value)`, strongest first.
    pub values: Vec<(&'static str, String)>,
    /// `FULL_OBJECT` or `COMPOSITE` (`None`: the store did not say).
    pub kind: Option<String>,
}

impl Checksums {
    fn is_full_object(&self, value: &str) -> bool {
        // A composite (per-part) checksum ends in `-<parts>`.
        self.kind.as_deref() != Some("COMPOSITE") && !value.contains('-')
    }
}

/// Whether two objects' checksums prove them equal (`Some(true)`), prove
/// them different (`Some(false)`), or cannot tell (`None`: no algorithm in
/// common, or two composite checksums, which differ with the part size even
/// for the same bytes).
pub fn settle(left: &Checksums, right: &Checksums) -> Option<bool> {
    for (algorithm, value) in &left.values {
        let Some((_, other)) = right.values.iter().find(|(a, _)| a == algorithm) else {
            continue;
        };
        if value == other {
            return Some(true);
        }
        if left.is_full_object(value) && right.is_full_object(other) {
            return Some(false);
        }
        return None;
    }
    None
}

async fn head_checksums(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    timeout: Duration,
) -> Option<Checksums> {
    let request = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send();
    let out = tokio::time::timeout(timeout, request).await.ok()?.ok()?;
    let mut values = Vec::new();
    for (algorithm, value) in [
        ("CRC64NVME", out.checksum_crc64_nvme()),
        ("SHA256", out.checksum_sha256()),
        ("SHA1", out.checksum_sha1()),
        ("CRC32C", out.checksum_crc32_c()),
        ("CRC32", out.checksum_crc32()),
    ] {
        if let Some(value) = value.filter(|v| !v.is_empty()) {
            values.push((algorithm, value.to_string()));
        }
    }
    Some(Checksums {
        values,
        kind: out.checksum_type().map(|t| t.as_str().to_string()),
    })
}

/// One diff side, for HeadObject requests.
#[derive(Clone)]
pub struct Side {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
    pub prefix: String,
}

/// A verifier that HEADs `prefix + key` on both sides and settles the pair.
pub fn verifier(left: Side, right: Side, timeout_secs: u64) -> ChecksumVerifier {
    let timeout = Duration::from_secs(timeout_secs.max(1));
    Arc::new(move |key: String| {
        let (left, right) = (left.clone(), right.clone());
        Box::pin(async move {
            let left_key = format!("{}{}", left.prefix, key);
            let right_key = format!("{}{}", right.prefix, key);
            let (l, r) = tokio::join!(
                head_checksums(&left.client, &left.bucket, &left_key, timeout),
                head_checksums(&right.client, &right.bucket, &right_key, timeout)
            );
            settle(&l?, &r?)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sums(values: &[(&'static str, &str)], kind: Option<&str>) -> Checksums {
        Checksums {
            values: values.iter().map(|(a, v)| (*a, v.to_string())).collect(),
            kind: kind.map(str::to_string),
        }
    }

    #[test]
    fn equal_checksums_prove_equal() {
        let a = sums(&[("CRC64NVME", "AAA=")], Some("FULL_OBJECT"));
        assert_eq!(settle(&a, &a.clone()), Some(true));
    }

    #[test]
    fn different_full_object_checksums_prove_different() {
        let a = sums(&[("CRC32", "AAA=")], Some("FULL_OBJECT"));
        let b = sums(&[("CRC32", "BBB=")], None);
        assert_eq!(settle(&a, &b), Some(false));
    }

    #[test]
    fn composite_or_disjoint_checksums_cannot_tell() {
        let a = sums(&[("CRC32C", "AAA=-3")], Some("COMPOSITE"));
        let b = sums(&[("CRC32C", "BBB=-4")], Some("COMPOSITE"));
        assert_eq!(settle(&a, &b), None);
        let c = sums(&[("SHA256", "x")], None);
        let d = sums(&[("CRC32", "y")], None);
        assert_eq!(settle(&c, &d), None);
        assert_eq!(settle(&Checksums::default(), &Checksums::default()), None);
    }

    #[test]
    fn the_first_common_algorithm_decides() {
        let a = sums(&[("SHA256", "s"), ("CRC32", "c")], None);
        let b = sums(&[("CRC32", "c")], None);
        assert_eq!(settle(&a, &b), Some(true));
    }
}
