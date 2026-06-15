# Security Policy

## Reporting a vulnerability

**Do not post security-sensitive reports publicly.**  Until a private
security contact is established, please open a GitHub issue with the
title `[SECURITY]` and minimal details — do not include credentials,
keys, tokens, or unsanitised traces.  A maintainer will follow up to
establish a private channel.

## Credential and data hygiene

s3-turbo-list is a client-side tool.  It interacts with S3-compatible
object stores using credentials supplied by the user.  To keep the
project safe for contributors and users:

- **Never submit credentials, access keys, secret keys, session tokens,
  signed URLs, private keys, or unsanitised traces** to issues, pull
  requests, discussions, commit messages, or documentation.
- **Sanitise bucket names and account identifiers** in any shared output
  (trace excerpts, debug logs, CLI output).
- If you discover that credentials have been accidentally committed or
  posted, contact a maintainer immediately and rotate the keys.

## Supported versions

Only the latest published [release](https://github.com/hxddh/s3-turbo-list/releases)
is supported. Older versions do not receive backported fixes — upgrade to the
latest release.

## Scope

The security policy covers the s3-turbo-list codebase and published
artifacts.  It does not cover third-party S3-compatible services or
the AWS SDK crates that s3-turbo-list depends on — report issues with
those to their respective maintainers.
