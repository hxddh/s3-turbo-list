//! Shared by the integration tests that run the built `s3tl` binary.
//!
//! Every spawned binary must behave the same on any machine: a developer's
//! AWS profile, endpoint variables, proxy settings, log filter or `S3TL_*`
//! test tunables must not change what a test sees.

#![allow(dead_code)] // each test crate uses a different subset

use std::path::Path;
use std::process::{Command, Output};

/// Proxy variables the SDK honours: an inherited proxy would reroute
/// requests meant for the local mock, and change `check`'s proxy report.
pub const PROXY_ENV_VARS: &[&str] = &[
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
];

/// AWS variables that point the SDK at the developer's own profile or
/// shared files (`AWS_ENDPOINT_URL*` are removed by prefix), plus the log
/// filter, which would put extra lines on stderr.
const INHERITED_ENV_VARS: &[&str] = &[
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "RUST_LOG",
];

/// Prefixes of inherited variables that are removed: the SDK's endpoint
/// overrides and the binary's own `S3TL_*` test tunables.
const INHERITED_ENV_PREFIXES: &[&str] = &["AWS_ENDPOINT_URL", "S3TL_"];

/// A `Command` for the built `s3tl` binary that cannot see the developer's
/// machine: `HOME` is `home` (so no `~/.aws/config` or `~/.aws/credentials`
/// is read), and the AWS profile, shared-file and endpoint variables, the
/// proxy variables, `RUST_LOG` and `S3TL_*` are removed. Tests add what they
/// need (credentials, region, tunables, cwd) on top.
pub fn hermetic_command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_s3tl"));
    command.env("HOME", home);
    for var in INHERITED_ENV_VARS.iter().chain(PROXY_ENV_VARS) {
        command.env_remove(var);
    }
    for (name, _) in std::env::vars_os() {
        if name.to_str().is_some_and(|name| {
            INHERITED_ENV_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
        }) {
            command.env_remove(name);
        }
    }
    command
}

/// `(exit code, stdout, stderr)` of a finished run (-1 when killed by a
/// signal).
pub fn exit_and_output(output: Output) -> (i32, String, String) {
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}
