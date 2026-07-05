# Changelog

All notable changes to `hpc-compose` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added `sweep submit --resume [--sweep-id <ID>]` to re-drive a partial sweep
  manifest after a failed submission. Resume submits only the trials that never
  got a job (those with a `submit_error` or a missing `job_id`) and leaves
  already-submitted trials untouched, keeping the sweep's id and `submitted_at`.
  Before resubmitting, it re-expands the current compose file with the stored
  sweep id (so `matrix: random` samples and per-replicate seeds reproduce) and
  refuses to continue if the sweep block drifted since the original submission.
  Spec edits outside the sweep block (a changed service `command:` or `image:`)
  are not covered by that hard guard, so resume also records the compose file's
  content hash at submit time and warns on stderr when the file changed since,
  since the resumed trials render from the current file and may diverge from
  already-submitted siblings.
  `--resume` composes with `--dry-run` (preview the resume set without
  submitting), `--max-trials`, `--skip-prepare`, `--force-rebuild`,
  `--no-preflight`, and `--format`. The `sweep-submit` JSON output gains
  additive `resumed`, `resubmitted`, and `skipped_already_submitted` fields (no
  `schema_version` bump).
- Added run tagging and notes on tracked job records: `experiment tag` attaches
  short set-semantic labels ("baseline", "lr-bug") and `experiment note` appends
  timestamped observations, both defaulting to the latest tracked run and
  targetable with `--job-id`. Tags and notes surface in `experiment show` and in
  `jobs list` (`tags`/`note_count`), and a repeatable `jobs list --tag <TAG>`
  filter keeps only jobs carrying every given tag. Tagging an older run never
  repoints the tracked "latest" record. Both commands support `--format json`
  with published schemas (`experiment-tag`, `experiment-note`); the record
  fields are additive, so existing records and schemas stay compatible.
- Added `experiment bundle [JOB_ID]`, which emits a citeable, paper-ready
  reproducibility archive for one tracked run: the compose spec, the resolved
  config snapshot (secret-redacted), the rendered `sbatch` (re-rendered from the
  plan and marked "reconstructed" when the on-disk script is gone), the full
  submission record, provenance (git SHA + dirty flag + per-service image
  references), the sweep manifest with seeds (for sweep trials), metrics
  (`stats.csv` plus the raw `*.jsonl` samples), the checkpoint attempt history,
  and a generated methods appendix (`README.md`) alongside a `MANIFEST.json`
  with per-file sha256 and a `missing[]` ledger. A `spec/spec-drift.diff` is
  included only when the current spec differs from the snapshot. Image entries
  are references as recorded at submit time, not content digests, and are never
  resolved against a registry. Output defaults to `experiment-bundle-<job_id>.tar.gz`;
  `--dir` writes an unpacked directory instead, `--strict` fails (after
  reporting) when any ingredient is missing, and `--format json` is pinned by
  the new `experiment-bundle` output schema. Defaults to the latest tracked run
  and contacts the scheduler only as much as `stats` does.
- Added `render --annotate` and `plan --show-script --annotate`: the rendered
  preview script interleaves provenance comments (`# <- x-slurm.mem` field
  markers and `# --- artifact helpers (x-slurm.artifacts) ---` section banners)
  mapping generated lines back to the compose spec fields that produced them.
  Annotations are preview-only: submission paths never enable them, and without
  the flag the rendered script is byte-identical to previous releases.
- Added the static-safe `explain` command mapping spec fields to generated
  script lines and back: `explain --field x-slurm.time` lists the script lines
  a field produced (prefix matching allowed), `explain --line N` names the
  field behind one script line, and bare `explain` prints the full provenance
  map. `--format json` is a registered output schema (`schema --output
  explain`). Line numbers match the `render` / `plan --show-script` preview;
  echoed script fragments are secret-redacted.
- Added `diff --against-spec`: a pre-submit "what changed since job X" check
  that compares the current compose file's effective config against the config
  snapshot recorded on a tracked run (`--job-id <ID>`, default: the latest
  tracked run). Both sides are effective configs — interpolated and
  profile-merged — so an environment-variable change shows up even when the
  file is untouched; secret values are redacted on both sides, so a changed
  secret does not appear as a change. `--fail-on-change` exits non-zero when
  any change is found, for scripted pre-submit gates
  (`hpc-compose diff --against-spec --fail-on-change && hpc-compose up`).
  Sweep-trial records have their swept variables re-applied to the current
  side, so the sweep overlay itself does not read as drift. `--format json`
  output is pinned by the new `diff-spec` output schema.
- Added a `workspace` command group (`status`, `allocate`, `extend`,
  `release`) that drives the site's hpc-workspace tools (`ws_find`,
  `ws_allocate`, `ws_extend`, `ws_release`, `ws_list`) for the workspace named
  in the new settings `workspace` block (`[defaults.workspace]` /
  `[profiles.<name>.workspace]`). Allocation is idempotent and guarded by
  `ws_find`; `status` computes expiry from `ws_list`'s remaining time with a
  version-tolerant parser; `release` prompts for confirmation and refuses
  while tracked jobs keep cache or runtime state under the workspace. Resolved
  facts persist per profile in `.hpc-compose/workspace-state.toml`, and all
  four commands support `--format json` with pinned output schemas. Phase 1
  runs the tools locally (on the login node); submit-time integration
  (auto-allocate/auto-extend, expiry warnings) is planned, and its settings
  (`auto_allocate`, `auto_extend`, `warn_days_left`, `queue_buffer_days`) are
  already part of the settings surface.

## [0.2.0] - 2026-07-04

### Added

- Added a versioned JSON output-schema contract for `--format json`. Every JSON
  command output is now backed by a `schemars`-pinned schema, and a new `schema`
  subcommand emits the JSON Schemas (`schema --output <dir>`) so downstream
  tooling can validate hpc-compose output against a stable, checked-in contract.
- Added sampled CPU utilization to the metrics pipeline. `stats`, `watch`, and
  the metrics JSONL now carry per-sample CPU usage alongside the existing GPU and
  memory samples.
- Added per-service GPU attribution to the metrics pipeline. The in-job sampler
  records raw attribution facts (per-PID cgroup and Slurm rank environment, plus
  a live step-id to step-name map in `steps.jsonl`), and `stats` resolves them
  cgroup -> Slurm step -> service, attributing a GPU device to a service only
  when every process on that GPU resolves unanimously to one service. `stats`
  text output shows `service=` on GPU device and process lines; JSON output
  fills the already-nullable `service`/`rank`/`local_rank` fields (strictly
  additive, no `schema_version` bump). Shared GPUs, MIG, unrecognized cgroup
  layouts, and dead PIDs stay `null` rather than guessing, and all sampler
  probes are best-effort and can never affect the job.
- Honor the CLICOLORS convention (`CLICOLOR` / `CLICOLOR_FORCE`) alongside the
  existing `NO_COLOR` / `--color` / `TERM=dumb` logic under the `--color auto`
  policy. Precedence, highest first: `NO_COLOR` > `CLICOLOR_FORCE` > `TERM=dumb`
  > not-a-tty > `CLICOLOR=0`. Documented in the CLI reference.
- Added an opt-in real-GPU end-to-end recipe (`just remote-gpu-e2e`, backed by
  `scripts/remote_gpu_e2e.sh`) that exercises the remote-submit path against a
  real GPU cluster from a laptop, documented in `dev-cluster/README.md`.
- Added examples for arrays, mail notifications, healthcheck sugar, and secrets.
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

- **Spec validation is now enforced at the planner chokepoint and covers more
  cases.** The full validator runs before any plan/render/submit, and it now
  guards Slurm time formats, `volumes:` short syntax (`host:container[:ro|rw]`),
  conflicting `gpus:`/`x-slurm.gres` GPU requests, memory-unit strings, and
  overlaps between first-class fields and raw `x-slurm.submit_args`. **Behavior
  change:** specs that earlier slipped through (invalid durations, malformed
  volume/memory strings, or a GPU count declared both ways) are now rejected with
  a miette diagnostic instead of being accepted and rendered incorrectly. Review
  specs
  that previously validated only by luck.
- `sweep stop` now routes through the shared destructive-action confirmation
  prompt, matching `down`/`cancel`, so stopping a running sweep asks before
  cancelling its trials (bypass with the standard non-interactive/force path).
- Scheduler probes for `sweep` and `diff` are batched and redundant `sacct`
  calls are gated, reducing the number and latency of scheduler queries on those
  paths.
- The `watch` TUI moves its scheduler probes onto a background worker thread, so
  the UI keeps repainting and stays responsive while probes are in flight.
- Documented and pinned the precedence of global versus per-service `x-slurm`
  settings so the resolution order is unambiguous and regression-tested.

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
- Restore the terminal on `SIGTERM`/`SIGHUP` while the `watch` TUI is in the
  alternate screen, so an interrupted watch no longer leaves the shell in a
  broken state.
- Warn instead of silently ignoring corrupt state files and truncated scheduler
  output, so partial/damaged runtime state surfaces a diagnostic rather than
  being dropped.
- Flush a final metrics sample at job end and degrade GPU fanout gracefully when
  a device query fails, so end-of-run metrics are complete and a single failing
  GPU probe no longer aborts collection.
- Populate `gpu_count` from device samples and aggregate the `watch` GPU line so
  multi-GPU utilization is reported correctly.

### Security

- Redact secret values in `plan` output paths. Secret values (by sensitive key
  name and declared `secrets:` values) are now masked in `plan` text and
  `--format json` output the same way they are elsewhere, closing a path that
  could echo secrets into logs or captured command output.

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
