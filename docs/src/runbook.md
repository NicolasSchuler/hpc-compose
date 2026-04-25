# Runbook

This runbook is the normal real-cluster flow for adapting a `hpc-compose` spec on a supported Linux Slurm submission host.

Commands below assume `hpc-compose` is on your `PATH`. If you are running from a local checkout, replace `hpc-compose` with `target/release/hpc-compose`.

Compose-aware commands accept `-f` / `--file`. When omitted, `hpc-compose` uses the active context compose file from `.hpc-compose/settings.toml`, then falls back to `compose.yaml` in the current directory. Global context flags are available everywhere:

- `--profile <NAME>` selects a profile from `.hpc-compose/settings.toml`.
- `--settings-file <PATH>` uses an explicit settings file instead of upward auto-discovery.

Read [Execution Model](execution-model.md), [Runtime Backends](runtime-backends.md), and [Support Matrix](support-matrix.md) before adapting a workflow to a new cluster.

## Before You Start

Make sure you have:

- a Linux submission host with `srun` and `sbatch`,
- the runtime backend selected by `runtime.backend`,
- `scontrol` when `x-slurm.nodes > 1`,
- Pyxis support in `srun` when `runtime.backend: pyxis` (`srun --help` should mention `--container-image`),
- shared storage for `x-slurm.cache_dir`,
- local source trees or local `.sqsh` / `.sif` images in place,
- registry credentials when your cluster or registry requires them.

Backend-specific requirements are listed in [Runtime Backends](runtime-backends.md). Cluster profile generation and MPI smoke probes are covered in [Cluster Profiles](cluster-profiles.md).

## Normal Progression

For a new spec on a real cluster:

1. Choose a starter from [Examples](examples.md), or run `hpc-compose new --template <name> --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml`.
2. Run `hpc-compose setup` once if you want compose path, env files, env vars, and binary overrides stored in a project-local settings file.
3. Run `hpc-compose context --format json` to verify resolved values and sources.
4. Set or confirm `x-slurm.cache_dir`, then adjust cluster-specific resource settings.
5. Run `hpc-compose plan -f compose.yaml` and `hpc-compose plan --verbose -f compose.yaml` while adapting the file.
6. Run `hpc-compose up -f compose.yaml` for the normal cluster run.
7. If it fails, start with `hpc-compose debug -f compose.yaml --preflight`, then use [Troubleshooting](troubleshooting.md) and break out `preflight`, `prepare`, `render`, `status`, `ps`, `watch`, `stats`, or `logs` separately.

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
hpc-compose setup --profile-name dev --compose-file compose.yaml --env-file .env --env-file .env.dev --env 'CACHE_DIR=<shared-cache-dir>' --default-profile dev --non-interactive
```

Settings file shape:

```toml
version = 1
default_profile = "dev"

[defaults]
compose_file = "compose.yaml"
env_files = [".env"]

[defaults.env]
CACHE_DIR = "/cluster/shared/hpc-compose-cache"

[profiles.dev]
compose_file = "compose.yaml"
env_files = [".env", ".env.dev"]

[profiles.dev.env]
RESUME_DIR = "/shared/$USER/runs/my-run"
MODEL_DIR = "$HOME/models"
```

Resolution precedence is fixed:

1. CLI flags
2. selected profile values
3. shared settings defaults
4. built-in CLI defaults

Use `context` whenever you want to inspect effective compose path, binaries, interpolation variables, runtime paths, and per-field sources.

## Choose A Starting Example

The maintained selection guide is [Examples](examples.md). It includes:

- four promoted beginner paths,
- a novice ladder from authoring to distributed workloads,
- the full repository example matrix,
- companion notes for LLM worker examples,
- an adaptation checklist.

Keep `docs/src/examples.md` as the single source of example selection truth. The embedded YAML source appendix is [Example Source](example-source.md).

## 1. Choose `x-slurm.cache_dir` Early

Set `x-slurm.cache_dir` to a path visible from both the login node and compute nodes:

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
- If `cache_dir` is unset, the default is `$HOME/.cache/hpc-compose`.
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

`plan` is purely static: it parses, validates, builds the normalized runtime plan, and can print the generated script to stdout, but it does not run preflight, prepare images, call `sbatch`, or write `hpc-compose.sbatch`. `plan --verbose` can print secrets from resolved environment values.

## 5. Normal Run: Use `up`

```bash
hpc-compose up -f compose.yaml
```

`up` is the preferred end-to-end cluster flow. It runs preflight unless disabled, prepares images unless skipped, renders the script, calls `sbatch`, records tracked job metadata, polls scheduler state, and streams logs.

Useful options:

- `--script-out path/to/job.sbatch` keeps a copy of the rendered script.
- `--force-rebuild` refreshes imported and prepared artifacts.
- `--skip-prepare` reuses existing prepared artifacts.
- `--no-preflight` skips the preflight phase.
- `--detach` submits or launches, records tracking metadata, and returns without watching.
- `--format text|json` is accepted with `--detach` or `--dry-run`.
- `--watch-mode auto|tui|line` selects the live output mode; `--no-tui` is a line-mode alias.
- `--resume-diff-only` prints resume-sensitive config diffs without launching.
- `--allow-resume-changes` confirms intentional resume-coupled config drift.

`up --local` is Linux + Pyxis-only and single-host. See [Runtime Backends](runtime-backends.md#local-mode).

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
hpc-compose prepare -f compose.yaml --force
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
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
hpc-compose logs -f compose.yaml --service app --follow
hpc-compose stats -f compose.yaml --format jsonl
```

Use [Runtime Observability](runtime-observability.md) for tracked state, logs, metrics, and machine-readable output. Use [Artifacts and Resume](artifacts-and-resume.md) for artifact bundles and resume-aware attempts.

## 10. Manage Cache And Old State

```bash
hpc-compose cache list
hpc-compose cache inspect -f compose.yaml
hpc-compose cache prune --all-unused -f compose.yaml
hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>'
hpc-compose clean -f compose.yaml --age 7 --dry-run
```

Use [Cache Management](cache-management.md) for cache reuse and pruning. Use [Troubleshooting](troubleshooting.md#clean-old-tracked-runs) before deleting tracked job directories.

## What Changed And What Should I Run?

| If you changed... | Typical next step |
| --- | --- |
| YAML planning/runtime settings only | `plan --verbose`, then `up` |
| Base image, `x-runtime.prepare.commands`, or prepare env | `up --force-rebuild`, or `prepare --force` when debugging separately |
| Mounted runtime source under `volumes` | Usually just `up` |
| Cache entries this plan no longer references | `cache prune --all-unused -f compose.yaml` |
| `hpc-compose` itself | Expect cache misses on the next `prepare` or `up`, then optionally prune old entries |

## Related Docs

- [Quickstart](quickstart.md)
- [Examples](examples.md)
- [Runtime Backends](runtime-backends.md)
- [Troubleshooting](troubleshooting.md)
- [Cluster Profiles](cluster-profiles.md)
- [Runtime Observability](runtime-observability.md)
- [Cache Management](cache-management.md)
- [Artifacts and Resume](artifacts-and-resume.md)
- [Spec Reference](spec-reference.md)
