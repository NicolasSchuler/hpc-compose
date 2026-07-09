# Operate a Real Cluster Run

This runbook is the normal real-cluster flow for adapting a `hpc-compose` spec on a supported Linux Slurm submission host.

If you are new to Slurm, read [Slurm And Container Basics](slurm-container-basics.md) first. If you are adapting to HAICORE@KIT, read [HAICORE Guide](haicore-guide.md) alongside this runbook.

Commands below assume `hpc-compose` is on your `PATH`. If you are running from a local checkout, replace `hpc-compose` with `target/release/hpc-compose`.

Compose-aware commands accept `-f` / `--file`. When omitted, `hpc-compose` uses the active context compose file from `.hpc-compose/settings.toml`, then falls back to `compose.yaml` in the current directory. Global context flags are available everywhere:

- `--profile <NAME>` selects a profile from `.hpc-compose/settings.toml`.
- `--settings-file <PATH>` uses an explicit settings file instead of upward auto-discovery.

Read [Slurm And Container Basics](slurm-container-basics.md), [Execution Model](execution-model.md), [Runtime Backends](runtime-backends.md), and [Support Matrix](support-matrix.md) before adapting a workflow to a new cluster.

## Before You Start

Make sure you have:

- a Linux submission host with `srun` and `sbatch`,
- the runtime backend selected by `runtime.backend`,
- `scontrol` when `x-slurm.nodes > 1`,
- Pyxis support in `srun` when `runtime.backend: pyxis` (`srun --help` should mention `--container-image`),
- shared storage for the resolved cache directory,
- local source trees or local `.sqsh` / `.sif` images in place,
- registry credentials when your cluster or registry requires them.

Backend-specific requirements are listed in [Runtime Backends](runtime-backends.md). Cluster profile generation and MPI smoke probes are covered in [Cluster Profiles](cluster-profiles.md).

## The Operational Spine

For a new spec on a real cluster, work the numbered steps below in order:

1. Choose a starter from [Examples](examples.md), or run `hpc-compose new --template <name> --name my-app --output compose.yaml`. See [Choose A Starting Example](#choose-a-starting-example).
2. Run `hpc-compose setup` once and verify resolved values with `hpc-compose context --format json`. See [Project-Local Settings](#project-local-settings).
3. Choose the cache directory early. See [Choose A Cache Directory Early](#1-choose-a-cache-directory-early).
4. Adapt the example and adjust cluster-specific resource settings. See [Adapt The Example](#2-adapt-the-example).
5. Validate the spec. See [Validate The Spec](#3-validate-the-spec).
6. Plan the run. See [Plan The Run](#4-plan-the-run).
7. Launch with `up`. See [Normal Run: Use `up`](#5-normal-run-use-up).
8. When debugging cluster readiness, prepare, or rendering, break out `preflight`, `prepare`, and `render` separately. See steps [6](#6-run-preflight-when-debugging-cluster-readiness)–[8](#8-render-the-batch-script).
9. Inspect the tracked run. See [Inspect A Tracked Run](#9-inspect-a-tracked-run).
10. Manage cache and old state. See [Manage Cache And Old State](#10-manage-cache-and-old-state).

If a run fails, start with `hpc-compose debug -f compose.yaml --preflight`, then follow the First Triage flow in [Troubleshooting](troubleshooting.md).

For a minimal cluster smoke test from a checkout, set `CACHE_DIR` to shared storage and run `scripts/cluster_smoke.sh`. It validates, preflights, and renders by default; set `HPC_COMPOSE_SMOKE_SUBMIT=1` only when you intentionally want it to launch the smoke job.

## Project-Local Settings

`hpc-compose` can discover `.hpc-compose/settings.toml` by walking upward from the current directory. You can also pin a file with `--settings-file`.

Typical setup flow:

```bash
hpc-compose setup
hpc-compose context
hpc-compose --profile dev context --format json
```

Non-interactive setup is available for scripting:

```bash
hpc-compose setup --profile-name dev --compose-file compose.yaml --env-file .env --env-file .env.dev --cache-dir '<shared-cache-dir>' --default-profile dev --non-interactive
```

Settings file shape:

```toml
version = 1
default_profile = "dev"

[defaults]
compose_file = "compose.yaml"
env_files = [".env"]
login_host = "login01.hpc.example.edu"
login_user = "<username>"

[defaults.env]
CACHE_DIR = "/cluster/shared/hpc-compose-cache"

[defaults.cache]
dir = "/cluster/shared/hpc-compose-cache"

[profiles.dev]
compose_file = "compose.yaml"
env_files = [".env", ".env.dev"]

[profiles.dev.env]
RESUME_DIR = "/shared/$USER/runs/my-run"
MODEL_DIR = "$HOME/models"

[profiles.dev.cache]
dir = "/cluster/shared/dev-hpc-compose-cache"

[resource_profiles.cpu-small]
time = "00:30:00"
cpus_per_task = 4
mem = "16G"

[resource_profiles.gpu-small]
partition = "gpu"
time = "01:00:00"
gpus = 1
cpus_per_task = 8
mem = "32G"
```

Resolution precedence is fixed:

1. CLI flags
2. selected profile values
3. shared settings defaults
4. built-in CLI defaults

Use `context` whenever you want to inspect effective compose path, binaries, interpolation variables, runtime paths, and per-field sources.

Resource profiles are referenced from YAML with `x-slurm.resources: gpu-small`. They are Slurm resource defaults, not the same thing as the global `--profile` setting selector, and explicit `x-slurm` values in the spec override profile defaults.

`login_host` is the SSH login host. It is the default SSH destination for `hpc-compose up --remote` when you do not pass `--remote=<host>`, and it also names the host shown in `notebook`, `reach`, and `pull` connection/tunnel hints and in the machine-readable `hpc-compose notebook --format json` output. A profile's `login_host` overrides the shared default.

`login_user` is the SSH username applied to a bare login host, so the resolved destination becomes `user@host`. A profile's `login_user` overrides the shared default. The login user for `up --remote` is resolved with this precedence: an explicit `user@` already present in `--remote=<dest>` or `login_host` wins; then the `HPC_COMPOSE_REMOTE_USER` environment variable; then settings `login_user` (profile over defaults); then the `User` from your `~/.ssh/config`. Persist both values with `hpc-compose setup --profile-name <name> --login-host <host> --login-user <user>` (written into `[profiles.<name>]`) or edit `settings.toml` directly.

If the login node requires an OTP/2FA on every SSH session, use SSH connection multiplexing (`ControlMaster`/`ControlPersist`) so you authenticate once and reused tunnels skip the prompt — see [Run a Notebook or IDE Session](notebook.md).

An editor schema for `settings.toml` is available:

```bash
hpc-compose schema --kind settings
```

For TOML editor integration, write that schema to a file (`hpc-compose schema --kind settings > hpc-compose-settings.schema.json`) and point your TOML language server at the local path.

## Choose A Starting Example

The maintained selection guide is [Examples](examples.md). It includes:

- four promoted beginner paths,
- a novice ladder from authoring to distributed workloads,
- the full repository example matrix,
- companion notes for LLM worker examples,
- an adaptation checklist.

Keep `docs/src/examples.md` as the single source of example selection truth. The embedded YAML source appendix is [Example Source](example-source.md).

## 1. Choose A Cache Directory Early

Set the cache default to a path visible from both the login node and compute nodes:

```toml
[profiles.dev.cache]
dir = "/cluster/shared/hpc-compose-cache"
```

Or set `x-slurm.cache_dir` directly in the spec when the cache path should travel with that file:

```yaml
x-slurm:
  cache_dir: /cluster/shared/hpc-compose-cache
```

Quick recipe:

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
```

Rules:

- Do not use `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm`.
- If `cache_dir` is unset in the spec, resolution checks profile cache settings, then defaults cache settings, then `$HOME/.cache/hpc-compose`.
- The default may work on some clusters, but a shared project/work/scratch path is safer.
- Validation can accept unsafe local paths; `preflight` reports them as policy errors.

More cache details are in [Cache Management](cache-management.md).

## 2. Adapt The Example

Start with the nearest example and then change:

- `image`
- `command` / `entrypoint`
- `volumes`
- `environment`
- `x-slurm` resource settings
- `x-runtime.prepare` commands for dependencies or tooling

Recommended pattern:

- Put fast-changing application code in `volumes`.
- Put slower-changing dependency installation in `x-runtime.prepare.commands`.
- Add `readiness` only to services that other services truly depend on.

## 3. Validate The Spec

```bash
hpc-compose validate -f compose.yaml
hpc-compose validate -f compose.yaml --strict-env
```

Use `validate` first when changing field names, dependency shape, command/entrypoint form, paths, `x-slurm`, `x-runtime`, or compatibility `x-enroot` blocks.

If `validate` fails, fix that before doing anything more expensive. Use `--strict-env` when missing interpolation variables should fail instead of consuming `${VAR:-default}` or `${VAR-default}` fallbacks.

## 4. Plan The Run

```bash
hpc-compose plan -f compose.yaml
hpc-compose plan --verbose -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
```

Check:

- service order,
- allocation geometry and service step geometry,
- normalized image references,
- host-to-container mount mappings,
- resolved environment values,
- runtime artifact paths,
- cache hit/miss expectations.

`plan` is purely static: it parses, validates, builds the normalized runtime plan, and can print the generated script to stdout, but it does not run preflight, prepare images, call `sbatch`, or write `hpc-compose.sbatch`. Add `--explain` for planner hints about cache paths, missing artifacts, resume/artifact settings, and the next command. `plan --verbose` can print secrets from resolved environment values.

## 5. Normal Run: Use `up`

```bash
hpc-compose up -f compose.yaml
```

`up` is the preferred end-to-end cluster flow. It runs preflight unless disabled, prepares images unless skipped, renders the script, calls `sbatch`, records tracked job metadata, polls scheduler state, and streams logs.
It also uses a spec-scoped lock under `.hpc-compose/locks/` so two concurrent `up` invocations against the same compose file do not race through prepare/render/submit.

Useful options:

- `--script-out path/to/job.sbatch` keeps a copy of the rendered script.
- `--force-rebuild` refreshes imported and prepared artifacts.
- `--skip-prepare` reuses existing prepared artifacts.
- `--no-preflight` skips the preflight phase.
- `--detach` submits or launches, records tracking metadata, and returns without watching.
- `--format text|json` is accepted with `--detach` or `--dry-run`.
- `--watch-queue` waits in line-oriented queue output until the Slurm job reaches `RUNNING`, then opens the normal watch view.
- `--queue-warn-after <DURATION>` warns once when `--watch-queue` stays `PENDING` longer than the threshold; the default is `10m`, and `0` disables the warning.
- `--watch-mode auto|tui|line` selects the live output mode.
- `--hold-on-exit never|failure|always` controls whether the TUI stays open after the job reaches a terminal scheduler state.
- `--resume-diff-only` prints resume-sensitive config diffs without launching.
- `--allow-resume-changes` confirms intentional resume-coupled config drift.

`up --local` is Linux-only, single-host, and limited to Pyxis/Enroot or Apptainer. See [Runtime Backends](runtime-backends.md#local-mode).

Array jobs should be submitted with `up --detach`; use `SLURM_ARRAY_TASK_ID` in the service command and output patterns such as `%A_%a` for task-specific logs. Scheduler dependencies declared with `x-slurm.after_job` or `x-slurm.dependency` are passed to `sbatch --dependency=...` at submit time. Arrays and scheduler dependencies are not supported by `up --local`.

For conditional submission on a busy partition, use `when`:

```bash
hpc-compose when -f compose.yaml --partition gpu8 --free-nodes 4 --poll-interval 120s
hpc-compose when -f compose.yaml --after-job 12345
hpc-compose when -f compose.yaml --between 22:00-06:00
```

`when` is a foreground monitor. Interrupt it with Ctrl-C to stop waiting before the job is submitted. It runs preflight, image preparation, and script rendering before the wait begins, so submission is immediate once the conditions match; use `--skip-prepare` only when the required runtime artifacts already exist. `--detach` applies after submission: it still waits in the foreground for conditions, then returns after tracking metadata is written instead of opening the watch view.

Idle-node checks are advisory, not reservations. Another user can still submit first, and Slurm may queue the job after `when` calls `sbatch`. Keep polling gentle on shared login nodes: the default `--poll-interval` is `60s` (minimum `5s`); reserve very short intervals for brief, intentional watches.

For interactive development inside one allocation, use `alloc`:

```bash
hpc-compose alloc -f compose.yaml
hpc-compose run app -- python -m pytest
```

Inside the allocation shell, `run SERVICE -- CMD` reuses the active allocation with `srun` instead of submitting a new `sbatch` job. `alloc` exports `HPC_COMPOSE_*` metadata for the compose file, cache directory, runtime backend, and allocated nodes. For interactive notebook sessions inside an allocation, see [Notebook](notebook.md).

## 5b. Submit From Your Laptop With `up --remote`

`hpc-compose up` runs on a Linux Slurm login node. macOS (and any host without Slurm) is authoring-only, so to submit from a laptop, delegate the run to a login node over SSH:

```bash
# Uses the configured login_host (with login_user, if set, as user@host):
hpc-compose up --remote -f compose.yaml
# Or target a specific host or ~/.ssh/config alias:
hpc-compose up --remote=login01 -f compose.yaml
# Or pass the SSH user inline:
hpc-compose up --remote=alice@login01 -f compose.yaml
```

The SSH destination comes from `--remote=<dest>` when given, otherwise from `login_host`. The login user follows the precedence documented in [Project-Local Settings](#project-local-settings): an inline `user@` wins, then `HPC_COMPOSE_REMOTE_USER`, then settings `login_user` (profile over defaults), then your `~/.ssh/config` `User`.

### What `--remote` stages

`--remote` rsyncs the compose project to a per-project staging directory on the login node (`~/.hpc-compose-remote/<project>`), including project settings such as `.hpc-compose/settings.toml` and `.hpc-compose/cluster.toml` while excluding tracked job/runtime state. It then runs `hpc-compose up` there over SSH, streaming the output back and propagating the remote exit code. Behavioral `up` flags such as `--detach`, `--dry-run`, `--no-preflight`, `--skip-prepare`, `--force-rebuild`, `--allow-resume-changes`, `--resume-diff-only`, `--format`, `--print-endpoints`, `--watch-mode line`, and `--hold-on-exit` are forwarded; without `--detach` the default remote run streams in line mode.

The staged root is the **settings base**: the directory that contains `.hpc-compose/settings.toml`. Place that file (or run `hpc-compose setup`) at the **repo root** so your whole source tree is staged. If your compose file lives in a subdirectory (for example `hpc/haicore/compose.yaml`) and there is no repo-root settings file, only that subdirectory is staged and the rest of your source tree is hidden from the job; `hpc-compose` prints a warning when it stages only a subdir.

`--remote` stages your repo only. It does not allocate cluster workspaces (for example `ws_allocate`) or create site storage directories — provision those yourself first, or a missing host bind-mount path blocks preflight. See [Repo staging vs cluster workspace provisioning](files-and-directories.md#repo-staging-vs-cluster-workspace-provisioning).

### Auto-installing `hpc-compose` on the login node

Before the (potentially expensive) rsync, `up --remote` probes the login node for `hpc-compose` — on `PATH` or in `~/.local/bin` — and reads its version. If the remote binary is missing or older than your local version, `up --remote` downloads and installs the newest release into `~/.local/bin` with the official installer (`curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh`), reusing the same multiplexed SSH connection so an OTP login node prompts only once. No root is needed, and the release tarball is checksum-verified. The delegated command runs the resolved absolute binary path, so an install in `~/.local/bin` that is not on the non-interactive SSH `PATH` still works.

Control this with `--remote-install <auto|never|force>` (default `auto`) or the `HPC_COMPOSE_REMOTE_INSTALL` environment variable:

- `auto` (default): install only when the remote binary is missing or older than your local version.
- `force`: always reinstall the newest release before delegating.
- `never`: only probe. If the remote binary is missing or old, fail with an actionable error that prints the manual install command. Use this on locked-down or air-gapped login nodes.

If the install fails (for example, the login node has no outbound network), `hpc-compose` prints the manual install one-liner and a clear error. Set `HPC_COMPOSE_REMOTE_INSTALL_URL` to point the installer at a mirror.

### Connection details and first run

Connection details belong in your `~/.ssh/config` (port, identity, jump host), so `--remote=<host>` stays a bare host or alias. For an ad-hoc host not in your config, set `HPC_COMPOSE_REMOTE_SSH_OPTS` (whitespace-split ssh flags, e.g. `-p 2222 -i ~/.ssh/cluster`). Every connection reuses one SSH ControlMaster, so a login node that requires an OTP/2FA prompts only once within `ControlPersist`.

On the first remote run (or after cache eviction) the login-node `prepare` step imports your image with enroot — a multi-GB download plus extract and squashfs build — which can take several minutes; later runs reuse the cache. See [Prepare Images Separately When Needed](#7-prepare-images-separately-when-needed).

This is a thin delegation: it re-stages the project on each run and does not maintain a persistent login session. It is not `up --local` (that launches on the current host); `--remote` and `--local` cannot be combined.

### Inspect a remote run from your laptop

The follow-up commands take the same `--remote` flag, so the metrics/logs workflow stays laptop-native — you don't have to SSH into the staged checkout or know its internal paths. After a successful `up --remote`, `hpc-compose` prints the exact commands to run (fill in the Slurm job id it reported):

```bash
hpc-compose stats --remote=alice@login01 -f compose.yaml --job-id <job-id>   # GPU util / memory / power
hpc-compose logs  --remote=alice@login01 -f compose.yaml --job-id <job-id>
hpc-compose score --remote=alice@login01 -f compose.yaml --job-id <job-id>
hpc-compose pull  --remote=alice@login01 -f compose.yaml --job-id <job-id>
```

These reuse the same host/login-user/staging context as `up --remote`: they SSH into the existing remote stage (no re-sync) and stream the output back, reusing the same SSH ControlMaster so an OTP node still prompts only once. They require the project to have been staged by a prior `up --remote`. `pull --remote` prints the same rsync command from the login-node context; run that printed command from your laptop to copy the artifact bundle locally.

## 6. Run Preflight When Debugging Cluster Readiness

```bash
hpc-compose preflight -f compose.yaml
hpc-compose preflight --verbose -f compose.yaml
hpc-compose preflight -f compose.yaml --strict
```

`preflight` checks selected-backend tools, Slurm tools, cache path policy, local mounts/images, registry credentials, cluster profile compatibility, distributed-readiness hazards, metrics collector tools, and resume path safety.

Generate a cluster capability profile on the target login node when you want validation and preflight to catch partition/backend/QOS/GPU/MPI mismatches earlier:

```bash
hpc-compose doctor cluster-report
```

See [Cluster Profiles](cluster-profiles.md) for generated profile details, site policy packs, and MPI smoke probes.

## 7. Prepare Images Separately When Needed

```bash
hpc-compose prepare -f compose.yaml
hpc-compose prepare -f compose.yaml --force-rebuild
```

Use this when you want to build or refresh prepared images before submission, confirm cache reuse behavior, or debug preparation separately from job submission.

`prepare` needs the selected runtime backend tools, but it does not call `sbatch`.

## 8. Render The Batch Script

```bash
hpc-compose render -f compose.yaml --output /tmp/job.sbatch
```

This is useful when debugging generated `srun` arguments, mounts, environment passing, launch order, and readiness waits.

## 9. Inspect A Tracked Run

```bash
hpc-compose jobs list
hpc-compose status -f compose.yaml
hpc-compose status -f compose.yaml --array
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
hpc-compose replay -f compose.yaml --speed 10
hpc-compose logs -f compose.yaml --service app --follow
hpc-compose stats -f compose.yaml --format jsonl
```

Use [Runtime Observability](runtime-observability.md) for tracked state, replay, logs, metrics, and machine-readable output. For a failed run, start with the First Triage flow in [Troubleshooting](troubleshooting.md#first-triage). Use [Artifacts and Resume](artifacts-and-resume.md) for artifact bundles and resume-aware attempts.

## 10. Manage Cache And Old State

[Cache Management](cache-management.md) owns cache inspection, pruning, and cleanup of old tracked runs (`cache prune`, `jobs list --disk-usage`, `clean --age`). For first triage of a failed run, see [Troubleshooting](troubleshooting.md#first-triage).

## What Changed And What Should I Run?

| If you changed... | Typical next step |
| --- | --- |
| YAML planning/runtime settings only | `plan --verbose`, then `up` |
| Base image, `x-runtime.prepare.commands`, or prepare env | `up --force-rebuild`, or `prepare --force-rebuild` when debugging separately |
| Mounted runtime source under `volumes` | Usually just `up` |
| Cache entries this plan no longer references | `cache prune --all-unused -f compose.yaml` |
| `hpc-compose` itself | Expect cache misses on the next `prepare` or `up`, then optionally prune old entries |

## Related Docs

- [Monitor a Run](runtime-observability.md)
- [Manage the Cache and Clean Up](cache-management.md)
- [Troubleshoot a Failed Run](troubleshooting.md)
- [Develop and Smoke-Test Locally](development-workflow.md)
- [Onboard a Cluster Site](cluster-profiles.md)
- [Notebook](notebook.md)
