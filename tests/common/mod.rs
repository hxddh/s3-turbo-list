//! Shared by the integration tests that run the built binary.
//!
//! Every spawned binary must behave the same on any machine: a developer's
//! `~/.s3-turbo-list.toml`, AWS profile or proxy settings must not change
//! what a test sees (a local config naming another provider or endpoint used
//! to fail a handful of tests).

#![allow(dead_code)] // each test crate uses a different subset

use std::path::Path;
use std::process::{Command, Output};

/// Proxy variables the SDK honours: an inherited proxy would reroute
/// requests meant for the local mock, and change doctor's proxy check.
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

/// AWS variables that point the SDK at the developer's own profile, shared
/// files or endpoint (`AWS_ENDPOINT_URL*` are removed by prefix).
const AWS_LOCATION_ENV_VARS: &[&str] = &[
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
];

/// A `Command` for the built `s3-turbo-list` binary that cannot see the
/// developer's machine: `HOME` is `home` (so no `~/.s3-turbo-list.toml`,
/// `~/.aws/config` or `~/.aws/credentials` is read), and the AWS profile,
/// shared-file and endpoint variables and the proxy variables are removed.
/// Tests add what they need (credentials, region, cwd) on top.
pub fn hermetic_command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_s3-turbo-list"));
    command.env("HOME", home);
    for var in AWS_LOCATION_ENV_VARS.iter().chain(PROXY_ENV_VARS) {
        command.env_remove(var);
    }
    for (name, _) in std::env::vars_os() {
        if name
            .to_str()
            .is_some_and(|name| name.starts_with("AWS_ENDPOINT_URL"))
        {
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
