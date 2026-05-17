use crate::config::{AddressingStyle, S3TurboConfig};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct EndpointProfile {
    pub name: &'static str,
    pub provider: &'static str,
    pub status: &'static str,
    pub default_region: Option<&'static str>,
    pub default_endpoint_url: Option<&'static str>,
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
            status: "documented",
            default_region: Some("bj"),
            default_endpoint_url: Some("https://s3.bj.bcebos.com"),
            recommended_addressing_style: AddressingStyle::Virtual,
            requires_explicit_endpoint: false,
            tested_by_project: true,
            notes: &["uses the standard S3-compatible ListObjectsV2 path"],
            limitations: &[
                "no default BOS pagination workaround is enabled",
                "hinted multi-segment BOS scans remain non-authoritative until provider compatibility is resolved",
            ],
        },
        EndpointProfile {
            name: "r2",
            provider: "Cloudflare R2",
            status: "documented",
            default_region: Some("auto"),
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Path,
            requires_explicit_endpoint: true,
            tested_by_project: false,
            notes: &["R2 commonly uses region 'auto' with an account-specific endpoint"],
            limitations: &["not claimed as project-validated until compat-probe or endpoint validation is run"],
        },
        EndpointProfile {
            name: "b2",
            provider: "Backblaze B2 S3-compatible API",
            status: "documented",
            default_region: None,
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Path,
            requires_explicit_endpoint: true,
            tested_by_project: false,
            notes: &["B2 endpoints and regions are bucket/account specific"],
            limitations: &["not claimed as project-validated until compat-probe or endpoint validation is run"],
        },
        EndpointProfile {
            name: "oss",
            provider: "Alibaba Cloud OSS S3-compatible API",
            status: "documented",
            default_region: None,
            default_endpoint_url: None,
            recommended_addressing_style: AddressingStyle::Virtual,
            requires_explicit_endpoint: true,
            tested_by_project: false,
            notes: &["OSS endpoint and region are deployment specific"],
            limitations: &["not claimed as project-validated until compat-probe or endpoint validation is run"],
        },
    ]
}

pub fn get_profile(name: &str) -> Option<&'static EndpointProfile> {
    all_profiles()
        .iter()
        .find(|profile| profile.name.eq_ignore_ascii_case(name))
}

pub fn apply_profile_preset(cfg: &mut S3TurboConfig) -> Option<ProfileApplication> {
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
