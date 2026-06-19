# Changelog

All notable changes to `hpc-compose` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- macOS authoring guidance surfaced in `doctor`, `preflight`, and `up` so authors
  understand which commands are runtime-supported versus authoring-only.
- A minimum supported Rust version (`rust-version = "1.85"`) is declared so older
  toolchains fail with a clear error instead of an obscure edition-2024 build error.
- A CI `msrv` job that builds against the pinned `1.85` toolchain.

### Changed

- The install one-liner now leads with a zero-edit "latest release" command; the
  pinned-tag form is presented as the reproducible / cluster-recommended variant.
- Governance and supply-chain documentation: added `CHANGELOG.md`, `CODEOWNERS`,
  and `GOVERNANCE.md`, and clarified the `SECURITY.md` reporting channel to prefer
  GitHub private vulnerability reporting.
- Documentation now lists the full case-insensitive substring set that triggers
  name-based redaction, and documents the `test` command's canonical
  `--wait-timeout` flag (alias `--timeout`).
- Release automation pushes the Homebrew formula refresh branch with
  `--force-with-lease` instead of `--force`.

### Fixed

- Install one-liner no longer requires manual version substitution to work on a
  literal copy/paste.
- Corrected `lint` documentation: warnings fail by default and `--allow-warnings`
  downgrades them to advisory; the non-existent `lint --strict` flag was removed.
- CI-integration snippets use `vX.Y.Z` / `${HPC_COMPOSE_VERSION}` placeholders so
  they no longer silently rot against a fixed release tag.

### Security

- Rendered scripts and persisted job state containing resolved secrets are now
  written owner-only (mode `0600`); documentation advises keeping secret-bearing
  specs and state in non-group-readable directories.
- Removed a stale advisory ignore (`RUSTSEC-2025-0119`) from `deny.toml` that no
  longer matched any crate in the dependency tree.
- Release binaries are stripped of symbols (`strip = "symbols"`) in the release
  profile.
