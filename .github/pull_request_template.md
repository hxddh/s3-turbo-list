## Checklist

- [ ] `cargo fmt --check` passes.
- [ ] `cargo check` passes.
- [ ] `cargo test` passes (all tests).
- [ ] `cargo build` succeeds.
- [ ] Documentation updated if behaviour changed (`README.md`, `CHANGELOG.md`, `docs/`).
- [ ] No credentials, access keys, secret keys, session tokens, or private keys in the diff.
- [ ] No real production bucket names in code, tests, examples, or comments.
- [ ] Cloud resources created during validation have been cleaned up.
- [ ] Endpoint compatibility impact has been considered (compat-probe or trace where appropriate).
- [ ] Known limitations in `README.md` updated if this PR introduces or resolves one.
