# Changelog

All notable changes to `hpc-compose` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `x-slurm.runtime_root`: optional override for the per-job runtime-state
  directory (`<runtime_root>/<job-id>/{logs,metrics,state.json,artifacts}`),
  defaulting to `<submit_dir>/.hpc-compose`. Resolved to an absolute path at
  submit time and rejected by `preflight` when it points at node-local storage.
- `x-slurm.cleanup.runtime_cache` (`never` | `on_success` | `always`, default
  `never`): controls whether the batch teardown trap removes the per-job enroot
  runtime cache.
- New "Files and Directories" documentation page describing the metadata,
  runtime, and cache directory layouts, the path-affecting environment
  variables, and the cleanup scope of each command.

### Changed

- The rendered `JOB_ROOT` is now baked as a resolved absolute path at submit
  time instead of relying on `${SLURM_SUBMIT_DIR:-$PWD}` at job runtime, so a
  job's runtime state no longer depends on `$SLURM_SUBMIT_DIR` being set and
  shared-visible. Dry-run previews keep the portable form.
- The default batch-log location moved from `slurm-%j.out` in the submit
  directory to `<runtime_root>/logs/hpc-compose-%j.out` (created host-side
  before submission). Set `x-slurm.output` to override.
- `x-slurm.output` / `x-slurm.error` now reject parent-directory (`..`)
  traversal and whitespace-only patterns in addition to the existing
  line-break / null-byte checks.
- `jobs clean` and `down` now also reap the hpc-compose-managed default batch
  log, the per-job enroot runtime cache (`<cache_dir>/runtime/<job-id>`), and
  the rendezvous records the job owns; `cache prune` removes now-empty parent
  directories (never the cache root).
- Concurrent cache-manifest updates are serialized with a best-effort advisory
  lock, closing a lost-update window on shared filesystems.

### Fixed

- `jobs` latest-pointer repair now also repairs the tracked notebook pointer.
- The prepare-time enroot environment now exports `ENROOT_TEMP_PATH` so scratch
  lands on the cache filesystem instead of the node's default `/tmp`.
- GPU metric samples reuse a stable per-node directory instead of accumulating
  one directory per sampling interval.

## [0.1.46] - 2026-06-19

### Added

- macOS authoring guidance surfaced in `doctor`, `preflight`, and `up` so authors
  understand which commands are runtime-supported versus authoring-only.
- A minimum supported Rust version (`rust-version = "1.88"`) is declared so older
  toolchains fail with a clear error instead of an obscure edition-2024 build error.
- A CI `msrv` job that builds against the pinned `1.88` toolchain.

### Changed

- The install one-liner now leads with a zero-edit "latest release" command; the
  pinned-tag form is presented as the reproducible / cluster-recommended variant.
- Governance and supply-chain documentation: added `CHANGELOG.md`, `CODEOWNERS`,
  and `GOVERNANCE.md`, and clarified the `SECURITY.md` reporting channel to prefer
  GitHub private vulnerability reporting.
- Documentation now lists the full case-insensitive substring set that triggers
  name-based redaction, and documents the `test` command's canonical
  `--wait-timeout` flag (alias `--timeout`).
- Breaking CLI history is now explicit: the former top-level submit workflow is
  documented as the canonical `up` command (`submit` -> `up`) so release notes do
  not hide the rename.
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
