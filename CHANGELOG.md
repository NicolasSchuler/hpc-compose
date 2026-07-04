# Changelog

All notable changes to `hpc-compose` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Reject per-service `x-slurm.partition`, `.qos`, and `.account` with a teaching
  error. A single hpc-compose allocation runs in one partition/account/qos, so
  these cannot be routed per service. Instead of serde's opaque "unknown field",
  the validator now points at the top-level `x-slurm` fields and the roadmap's new
  [Heterogeneous Jobs](docs/src/roadmap.md) section, which describes the planned
  hetjob mechanism for routing components to different partitions.
- Added first-class `x-slurm.requeue` and `x-slurm.signal`. `requeue: true`/`false`
  renders `#SBATCH --requeue`/`--no-requeue` to control whether Slurm re-queues the
  whole job after node failure or preemption. `signal: { name, at_seconds, shell? }`
  renders `#SBATCH --signal=[B:]<name>@<sec>`, delivering an early-warning signal
  before the time limit so a job can checkpoint. `name` accepts a name (`USR1`) or
  numeric alias (`10`) from the `HUP`/`INT`/`QUIT`/`USR1`/`USR2`/`TERM` whitelist;
  `at_seconds` must be `1..=65535`. The default `shell: step` delivers straight to
  each service's job step (no trap needed); `shell: batch` (`B:`) delivers only to
  the batch shell and installs a non-exiting forwarding trap that relays the signal
  to the running services. Both fields flow through validation and resume-diff, are
  rejected alongside conflicting raw `--requeue`/`--no-requeue`/`--signal` in
  `x-slurm.submit_args`, and compose with `x-slurm.resume` through
  `SLURM_RESTART_COUNT` with no extra config. Ships the `preemptible-checkpoint`
  example.
- Added per-service `env_file:` (docker-compose compatibility). A string or list
  of dotenv-style files is read on the submit host, relative to the compose
  file's directory, and folded into the service `environment` at spec-load time.
  Merge precedence is lowest-to-highest: `env_file` entries in list order, then
  inline `environment:`. File contents are literal (no `${...}` expansion) while
  the paths are interpolated. A missing file or malformed line surfaces as a
  `SpecError::EnvFileNotFound` / `SpecError::EnvFileMalformedLine` miette
  diagnostic. env_file entries redact the same as inline `environment:` (by
  sensitive key name and declared `secrets:` values). Ships the `env-file`
  example with `.env` fixtures.
- Added `${VAR:?message}` / `${VAR?message}` required-variable interpolation.
  `${VAR:?message}` fails at spec-load time when `VAR` is unset or empty, and
  `${VAR?message}` fails only when `VAR` is unset; the message is interpolated
  and an empty message falls back to a generic diagnostic. Unsatisfied required
  variables surface as a `SpecError::RequiredVariableUnset` miette diagnostic
  with `export`/`.env` help. The `secrets-hf-token` example now guards its token
  with this form.
- Added first-class `x-slurm.reservation` and `x-slurm.licenses` fields that
  render `#SBATCH --reservation` / `--licenses` directives, feed the interactive
  `alloc`/`shell` option builder, and keep validation, interpolation, redaction,
  and resume-diff on the values. Setting either field alongside a conflicting raw
  `--reservation`/`--licenses` (`-L`) entry in `x-slurm.submit_args` is rejected.
- Extended `x-slurm.notify.email.on` with the richer Slurm mail-type events
  `time_limit`, `time_limit_90`, `time_limit_80`, `time_limit_50`, `requeue`,
  `invalid_depend`, `stage_out`, and `array_tasks`. Events now render in a stable
  canonical (`man sbatch`) order, `all` still collapses to `ALL` while keeping an
  explicit `array_tasks` modifier, and using `array_tasks` without `x-slurm.array`
  is rejected.

### Changed

- **BREAKING:** every per-command "give up after this long" flag is now spelled
  `--timeout` and takes a DURATION string (`30s`, `5m`, `1h`, or a bare number
  of seconds). The old spellings are removed outright (no hidden aliases), and
  each command's default is unchanged:

  | Command | Before | After | Default |
  | --- | --- | --- | --- |
  | `test` | `--wait-timeout <DURATION>` (alias `--timeout`) | `--timeout <DURATION>` | `180s` |
  | `notebook` | `--ready-timeout <DURATION>` | `--timeout <DURATION>` | `10m` |
  | `germinate` | `--pending-timeout <DURATION>` | `--timeout <DURATION>` | `30m` |
  | `when` | `--timeout <DURATION>` | unchanged | none |
  | `sweep observe` | `--timeout <DURATION>` | unchanged | none |
  | `doctor mpi-smoke` / `fabric-smoke` / `readiness` | `--timeout-seconds <SECONDS>` | `--timeout <DURATION>` | `5m` (was `300` seconds; same duration) |

  `doctor --timeout` still accepts a bare number of seconds, so
  `--timeout-seconds 300` migrates to `--timeout 300` (or `--timeout 5m`).

### Removed

- **BREAKING:** the hidden deprecated `prepare --force` alias for
  `prepare --force-rebuild` is gone; `prepare --force` is now an unknown-flag
  error. `--force` now means only "overwrite the output file" (`new`/`evolve`)
  everywhere in the CLI. Use `prepare --force-rebuild`.

### Fixed

- `when --after-job` now treats `LAUNCH_FAILED` and `RECONFIG_FAIL` as terminal
  states, matching the job tracker's `JobState::is_terminal`. Previously the
  dependency resolver used a narrower terminal-state list that omitted these two,
  so a dependency job that ended in either state was classified as still pending
  and the `when` monitor polled forever (or exited with a timeout error) instead
  of resolving the condition. Now `afterany`/`afternotok` are satisfied and
  `afterok` fails fast with a "can never satisfy" error, as they already did for
  every other failed terminal state.

## [0.1.52] - 2026-06-30

### Fixed

- Preserved existing user-managed file permission semantics for atomic rewrites
  by rejecting read-only regular destinations before replacing them.

## [0.1.51] - 2026-06-29

### Added

- Added a `plan_render` benchmark for tracking plan construction and render
  throughput against the development Python example.

### Changed

- Scheduler-backed probes now run through a shared timeout-aware command runner
  so unavailable or hanging Slurm commands surface quickly in stats, accounting,
  and queue diagnostics.
- `stats`, `logs`, and `watch` now read large JSONL metrics and log files from
  the needed suffix instead of loading full files for each refresh.

## [0.1.50] - 2026-06-28

### Changed

- Refined setup, runtime, cache, cleanup, schema, and observability docs so the
  examples match the current CLI behavior and generated command surfaces.
- Aligned HAICORE full-GPU examples and the bundled agent skill with the
  `normal` partition GRES syntax (`gpu:N`, for example `gpu:1`).

### Fixed

- Corrected the shipped pre-commit hook metadata and documentation so the
  default hooks only claim `compose.yaml`; `compose.yml` users now get an
  explicit override snippet that points both hooks at the staged filename.

## [0.1.49] - 2026-06-27

### Added

- Dev-cluster safe dry-runs are now a first-class, asserted capability: the
  in-container UC1 harness proves `up --dry-run` renders a valid sbatch while
  submitting nothing (queue and accounting unchanged, text and `--format json`),
  and the UC2 remote harness proves `up --remote --dry-run` stages-but-doesn't-
  submit on the login node.
- Real-scheduler e2e coverage for read-side affordances that were previously only
  fake-tool tested: `weather` (live node/queue signals, text + JSON), N-way `diff`
  of two real runs, `when` (evaluates live conditions and declines to submit when
  unmet), and the interactive `watch` TUI driven under a pseudo-terminal
  (`dev-cluster/pty-run.py`) — asserting it enters **and** restores the alternate
  screen so it never leaves the terminal in a bad state.
- New `dev-cluster-otp-e2e` harness (`scripts/devcluster_otp_e2e.sh`,
  `just dev-cluster-otp-e2e`) and an `otp-sim` toggle baked into the dev-cluster
  image (`dev-cluster/otp-sim.sh`): the SSH login-node stand-in can now require an
  OTP/2FA-style second factor, and the harness proves a multi-command laptop
  session (`up --remote`, `up --remote --dry-run`, a `pull`-style transfer)
  authenticates **exactly once** via SSH ControlMaster multiplexing. Wired into
  the CI `dev-cluster-e2e` job as UC3.

### Changed

- `notebook`'s Jupyter SSH tunnel hint now carries the same connection-
  multiplexing options (and OTP note) as `reach`/`pull`/`experiment`, so a login
  node that requires an OTP/2FA prompts only once per session instead of charging
  a fresh prompt for the notebook tunnel.

## [0.1.48] - 2026-06-22

### Added

- Local Slurm dev-cluster support, including single-node and multi-service
  examples plus an end-to-end dev-cluster CI path.
- New workflow inspection and retrieval commands: `experiment show`,
  `checkpoints`, N-way `diff`, `pull`, and `reach`.
- Sweep workflow upgrades: `sweep results`, sweep-aware `score`/`stats`,
  replicate rollups, and `objective.scaling_axis`.
- `hf://` model and dataset `stage_in` support with content-addressed cache
  reuse and immutable revision validation.
- Submit-time provenance now records tool, git, and image details, with
  `.hpcignore` support for content-addressed source snapshots.
- `run --dataset` / `--output`, notebook `login_host` and JSON output, and
  `x-slurm.parallelism` GPU cross-check metadata.

### Changed

- CLI help, validation, preflight, and prepare paths now share generated
  command-group metadata and provide more direct next-step hints.
- Runtime jobs expose `HPC_COMPOSE_JOB_DIR` for portable host-backend scripts and
  surface readiness-derived endpoints/next commands after startup.
- Manpages and CLI reference docs were regenerated for the new commands and
  flags.

### Fixed

- Hardened `hf://` staging against shell injection and partial downloads.
- Fixed sweep scoring/JSON edge cases, zero-baseline scaling, and
  `--huggingface-cli-bin` propagation.
- Stopped requesting unsupported `AllocTRES` fields from `sstat`.
- Repaired stale `up` lock reclamation and dev-cluster shellcheck
  compatibility.
- Fixed notebook JSON output, sweep trial scoring, and CLI reference markdown
  lint.

## [0.1.47] - 2026-06-20

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
