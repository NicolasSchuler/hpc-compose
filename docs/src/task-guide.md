# Task Guide

Use this page when you know what you want to do, but not yet which command or example should be your starting point.

## First run

- Read [Quickstart](quickstart.md).
- Run `hpc-compose evolve --output compose.yaml` if you want a guided progression from `minimal` through `multi-node-placement`.
- Run `hpc-compose new --list-templates` if you want to inspect the built-in starter templates before choosing one.
- Run `hpc-compose examples recommend` for a static, no-Slurm starting-point recommendation with match reasons and safe next commands. Add a workflow description, such as `hpc-compose examples recommend 'vllm worker'`, when you want registry-backed recommendations for a narrower shape.
- Run `hpc-compose examples list` or `hpc-compose examples search 'vllm worker'` when you want to browse the broader example coverage map by workflow or tag.
- Start from `minimal-batch` with `hpc-compose new --template minimal-batch --name my-app --output compose.yaml`.
- Before running on a cluster, configure a shared cache with `hpc-compose setup --cache-dir '<shared-cache-dir>'` or explicit `x-slurm.cache_dir`. If you copy a repository example that uses `CACHE_DIR`, override it for your cluster before running.
- Run `hpc-compose plan -f compose.yaml` before the first real run. Add `--show-script` when you want to inspect the generated launcher without writing a file.
- Run `hpc-compose up -f compose.yaml` only from a supported Linux Slurm submission host.

## Remember directory/data/env settings once

- Run `hpc-compose setup` to create or update the project-local settings file (`.hpc-compose/settings.toml`).
- Use `hpc-compose --profile dev up` so compose path, env files, env vars, and binary paths come from the selected profile.
- Run `hpc-compose context --format json` to inspect resolved paths plus value sources. Interpolation variables are scoped to names referenced by the compose file and sensitive-looking values are redacted unless you add `--show-values`.
- Use `--settings-file <PATH>` when you need an explicit settings file instead of upward discovery.

## Migrate from Docker Compose

- Read [Docker Compose Migration](docker-compose-migration.md).
- Replace `build:` with `image:` plus `x-runtime.prepare.commands`.
- Replace service-name networking with `127.0.0.1` or explicit allocation metadata where appropriate.

## Pick a starting example

- Browse the annotated catalog and chooser in [Examples](examples.md); it owns the per-example filename, tag, and prerequisite map.
- Run `hpc-compose examples recommend '<workflow description>'` for a registry-backed starting point, e.g. `'multi-service app'`, `'multi-node training'`, `'checkpoint resume training'`, or `'vllm worker'`.

## Single-node multi-service app

- Use [Execution Model](execution-model.md) to confirm which services can rely on localhost.
- Add `depends_on` and `readiness` only where ordering really matters.

## Multi-node distributed training

- Use generated distributed metadata such as `HPC_COMPOSE_DIST_RDZV_ENDPOINT`, `HPC_COMPOSE_DIST_NODE_RANK`, and `HPC_COMPOSE_DIST_NPROC_PER_NODE` instead of Docker-style service discovery.
- Put cluster-specific NCCL/UCX/OFI fabric variables in `.hpc-compose/cluster.toml` under `[distributed.env]` so specs stay portable.

## Checkpoint and resume workflows

- See [Artifacts and Resume](artifacts-and-resume.md) for the export-vs-resume split.
- Keep the canonical resume source in `x-slurm.resume.path`, not in exported artifact bundles.

## LLM serving workflows

- Use `volumes` for model directories and fast-changing code.
- Use `x-runtime.prepare.commands` for slower-changing dependencies.

## Debug cluster readiness

- Run `hpc-compose validate -f compose.yaml`.
- Run `hpc-compose validate -f compose.yaml --strict-env` when default interpolation fallbacks should be treated as failures.
- Run `hpc-compose plan --verbose -f compose.yaml`.
- Run `hpc-compose preflight -f compose.yaml`.
- Run `hpc-compose debug -f compose.yaml --preflight` after a failed tracked run.
- Run `hpc-compose doctor readiness -f compose.yaml --service <name>` to inspect the normalized readiness probe, or add `--run` when the target service, tunnel, or log file is already reachable from the current host.
- Read [Troubleshooting](troubleshooting.md).

## Cache and artifact management

- Use `hpc-compose cache list` to inspect imported/prepared artifacts.
- Use `hpc-compose cache inspect -f compose.yaml` to see per-service reuse expectations.
- Use `hpc-compose --profile dev cache prune --age 14` when you want age-based cleanup to follow the active context cache dir.
- Use `hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>'` when you want a direct cache cleanup that does not depend on compose resolution.
- Use `hpc-compose artifacts -f compose.yaml` after a run to export tracked payloads.

## Find and clean tracked runs

- Use `hpc-compose jobs list` to scan the current repo tree for tracked runs.
- Use `hpc-compose ps -f compose.yaml` when you want a one-shot per-service runtime table.
- Use `hpc-compose watch -f compose.yaml` to reconnect to the live watch UI for the latest tracked job.
- Use `hpc-compose jobs list --disk-usage` when you need a quick size estimate before deleting old state.
- Use `hpc-compose clean -f compose.yaml --dry-run --age 7` to preview what a cleanup would remove.
- Use `hpc-compose clean -f compose.yaml --all --format json` when automation needs a stable cleanup report for one compose context, including effective latest IDs plus stale-pointer diagnostics.

## Automation and scripting with JSON output

- Prefer `--format json` for machine-readable output on non-streaming commands such as `new`, `plan`, `validate`, `render`, `prepare`, `preflight`, `config`, `inspect`, `debug`, `status`, `ps`, `stats`, `score`, `artifacts`, `down`, `cancel`, `setup`, `cache list`/`cache inspect`/`cache prune`, `clean`, and `context`. For `up`, `--format json` requires `--detach` or `--dry-run`.
- Include `context --format json` when automation needs resolved compose path, binaries, referenced interpolation vars, and runtime path roots.
- Use `hpc-compose stats --format jsonl` or `--format csv` when downstream tooling wants row-oriented metrics.
- Use `--format json` for machine-readable output on non-streaming commands. Streaming commands such as `logs --follow`, `watch`, and `completions` keep their native text or script output.

## Related Docs

- [Examples](examples.md)
- [Guided Authoring Tutorial](evolve.md)
- [Migrate a docker-compose.yaml](docker-compose-migration.md)
- [CLI Reference](cli-reference.md)
- [Runbook](runbook.md)
