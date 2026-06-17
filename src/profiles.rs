use crate::config::{AddressingStyle, S3TurboConfig};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct EndpointProfile {
    pub name: &'static str,
    pub provider: &'static str,
    pub status: &'static str,
    pub default_region: Option<&'static str>,
    pub default_endpoint_url: Option<&'static str>,
    /// Region-derived endpoint pattern; `{region}` is substituted with the
    /// run's region (or `default_region`) when no explicit endpoint is set.
    pub endpoint_template: Option<&'static str>,
    pub recommended_addressing_style: AddressingStyle,
    pub requires_explicit_endpoint: bool,
    pub tested_by_project: bool,
    pub notes: &'static [&'static str],
    pub limitations: &'static [&'static str],
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileApplication {
    pub name: String,
    pub known: bool,
    pub endpoint_url_applied: bool,
    pub addressing_style_applied: bool,
    pub warnings: Vec<String>,
}

pub fn all_profiles() -> &'static [EndpointProfile] {
    &[
        EndpointProfile {
            name: "aws",
            provider: "AWS S3",
            status: "stable",
            default_region: None,
            endpoint_template: None,
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Virtual,
            requires_explicit_endpoint: false,
            tested_by_project: true,
            notes: &["baseline S3-compatible behavior"],
            limitations: &["region remains user supplied or SDK/default environment supplied"],
        },
        EndpointProfile {
            name: "minio",
            provider: "MinIO",
            status: "stable",
            default_region: None,
            endpoint_template: None,
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Path,
            requires_explicit_endpoint: true,
            tested_by_project: true,
            notes: &["path-style addressing is the safest default for local MinIO deployments"],
            limitations: &["endpoint URL is deployment-specific and must be supplied by the user"],
        },
        EndpointProfile {
            name: "bos",
            provider: "Baidu BOS S3-compatible API",
            status: "stable",
            default_region: Some("bj"),
            endpoint_template: Some("https://s3.{region}.bcebos.com"),
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Virtual,
            requires_explicit_endpoint: false,
            tested_by_project: true,
            notes: &[
                "uses the standard S3-compatible ListObjectsV2 path",
                "fully compatible with hinted multi-segment listing and startup discovery",
            ],
            limitations: &["virtual-hosted addressing is the recommended default"],
        },
        EndpointProfile {
            name: "r2",
            provider: "Cloudflare R2",
            status: "documented",
            default_region: Some("auto"),
            endpoint_template: None,
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Path,
            requires_explicit_endpoint: true,
            tested_by_project: false,
            notes: &["R2 commonly uses region 'auto' with an account-specific endpoint"],
            limitations: &[
                "not claimed as project-validated until compat-probe or endpoint validation is run",
            ],
        },
        EndpointProfile {
            name: "b2",
            provider: "Backblaze B2 S3-compatible API",
            status: "documented",
            default_region: None,
            endpoint_template: Some("https://s3.{region}.backblazeb2.com"),
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Path,
            requires_explicit_endpoint: false,
            tested_by_project: false,
            notes: &["the endpoint derives from the bucket's region (e.g. us-west-004)"],
            limitations: &[
                "not claimed as project-validated until compat-probe or endpoint validation is run",
            ],
        },
        EndpointProfile {
            name: "oss",
            provider: "Alibaba Cloud OSS S3-compatible API",
            status: "documented",
            default_region: None,
            endpoint_template: Some("https://{region}.aliyuncs.com"),
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Virtual,
            requires_explicit_endpoint: false,
            tested_by_project: false,
            notes: &["the endpoint derives from the region (e.g. oss-cn-beijing)"],
            limitations: &[
                "not claimed as project-validated until compat-probe or endpoint validation is run",
            ],
        },
    ]
}

pub fn get_profile(name: &str) -> Option<&'static EndpointProfile> {
    all_profiles()
        .iter()
        .find(|profile| profile.name.eq_ignore_ascii_case(name))
}

pub fn is_endpoint_preset_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "bos" | "minio" | "r2" | "b2" | "oss"
    )
}

pub fn endpoint_profile_guardrail_warnings(cfg: &S3TurboConfig) -> Vec<String> {
    let mut warnings = Vec::new();

    if let Some(profile_name) = cfg.s3.profile.as_deref() {
        if let Some(profile) = get_profile(profile_name) {
            let endpoint = cfg.s3.endpoint_url.as_deref().map(str::trim);
            if endpoint.filter(|value| !value.is_empty()).is_none() {
                if profile.requires_explicit_endpoint {
                    warnings.push(format!(
                        "endpoint compatibility profile '{}' requires an explicit endpoint URL; pass --endpoint-url or set s3.endpoint_url in config",
                        profile.name
                    ));
                } else if profile.endpoint_template.is_some() && profile.default_region.is_none() {
                    warnings.push(format!(
                        "endpoint compatibility profile '{}' derives its endpoint from the region; pass --region or --endpoint-url",
                        profile.name
                    ));
                }
            }
        }
    }

    if let Some(endpoint) = cfg.s3.endpoint_url.as_deref() {
        if endpoint_url_has_template_placeholder(endpoint) {
            warnings.push(format!(
                "endpoint URL '{}' still contains template placeholders; replace values such as <account-id> or <region> before a real run",
                endpoint
            ));
        }
    }

    warnings
}

pub fn endpoint_url_has_template_placeholder(endpoint: &str) -> bool {
    endpoint.contains('<') || endpoint.contains('>')
}

pub fn apply_profile_preset(
    cfg: &mut S3TurboConfig,
    region: Option<&str>,
) -> Option<ProfileApplication> {
    let name = cfg.s3.profile.clone()?;
    let Some(profile) = get_profile(&name) else {
        return Some(ProfileApplication {
            name,
            known: false,
            endpoint_url_applied: false,
            addressing_style_applied: false,
            warnings: vec!["unknown endpoint profile; no preset applied".to_string()],
        });
    };

    let mut endpoint_url_applied = false;
    if cfg.s3.endpoint_url.is_none() {
        if let Some(endpoint) = profile.default_endpoint_url {
            cfg.s3.endpoint_url = Some(endpoint.to_string());
            endpoint_url_applied = true;
        } else if let (Some(template), Some(region)) = (
            profile.endpoint_template,
            region.filter(|r| !r.is_empty()).or(profile.default_region),
        ) {
            cfg.s3.endpoint_url = Some(template.replace("{region}", region));
            endpoint_url_applied = true;
        }
    }

    let mut addressing_style_applied = false;
    if cfg.s3.addressing_style == AddressingStyle::Auto
        && profile.recommended_addressing_style != AddressingStyle::Auto
    {
        cfg.s3.addressing_style = profile.recommended_addressing_style.clone();
        addressing_style_applied = true;
    }

    let warnings = profile
        .limitations
        .iter()
        .map(|item| (*item).to_string())
        .collect();

    Some(ProfileApplication {
        name: profile.name.to_string(),
        known: true,
        endpoint_url_applied,
        addressing_style_applied,
        warnings,
    })
}

#[cfg(test)]
mod profile_metadata_tests {
    use super::*;

    /// BOS has fixed its ListObjectsV2 compatibility (start_after +
    /// continuation-token). The profile must not carry the old
    /// "non-authoritative" / "pagination workaround" caveats, so that hinted
    /// multi-segment listing is presented as fully supported. Guards against
    /// reintroducing the retired BOS-specific limitation.
    #[test]
    fn bos_profile_has_no_stale_compatibility_caveat() {
        let bos = get_profile("bos").expect("bos profile must exist");
        for text in bos.notes.iter().chain(bos.limitations.iter()) {
            let lower = text.to_lowercase();
            assert!(
                !lower.contains("non-authoritative")
                    && !lower.contains("pagination workaround")
                    && !lower.contains("continuation-token"),
                "stale BOS compatibility caveat resurfaced: {text:?}"
            );
        }
    }
}
