# Task Guide

Use this page when you know what you want to do, but not yet which command or example should be your starting point.

## First run

- Read [Quickstart](quickstart.md).
- Run `hpc-compose new --list-templates` if you want to inspect the built-in starter templates before choosing one.
- Start from `minimal-batch` with `hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml`.
- If you copy a repository example directly, override `CACHE_DIR` for your cluster before submitting it; the shipped YAML files default to `/cluster/shared/hpc-compose-cache`.
- Run `hpc-compose up -f compose.yaml`.

## Remember directory/data/env settings once

- Run `hpc-compose setup` to create or update the project-local settings file (`.hpc-compose/settings.toml`).
- Use `hpc-compose --profile dev up` so compose path, env files, env vars, and binary paths come from the selected profile.
- Run `hpc-compose context --format json` to inspect all resolved values plus value sources.
- Use `--settings-file <PATH>` when you need an explicit settings file instead of upward discovery.

## Migrate from Docker Compose

- Read [Docker Compose Migration](docker-compose-migration.md).
- Replace `build:` with `image:` plus `x-enroot.prepare.commands`.
- Replace service-name networking with `127.0.0.1` or explicit allocation metadata where appropriate.

## Single-node multi-service app

- Start from [app-redis-worker.yaml](example-source.md#app-redis-worker).
- Add `depends_on` and `readiness` only where ordering really matters.
- Use [Execution Model](execution-model.md) to confirm which services can rely on localhost.

## Multi-node distributed training

- Start from [multi-node-torchrun.yaml](example-source.md#multi-node-torchrun) or [multi-node-mpi.yaml](example-source.md#multi-node-mpi).
- Start from [multi-node-partitioned.yaml](example-source.md#multi-node-partitioned) when independent distributed roles need disjoint node ranges or explicit co-location.
- Use allocation metadata such as `HPC_COMPOSE_PRIMARY_NODE` instead of Docker-style service discovery.
- Use service metadata such as `HPC_COMPOSE_SERVICE_NODELIST` when a service uses `x-slurm.placement`.

## Checkpoint and resume workflows

- Start from [training-checkpoints.yaml](example-source.md#training-checkpoints) when you only need artifact output.
- Start from [training-resume.yaml](example-source.md#training-resume) when the run should resume from shared storage across retries or later submissions.
- Keep the canonical resume source in `x-slurm.resume.path`, not in exported artifact bundles.

## LLM serving workflows

- Start from [llm-curl-workflow.yaml](example-source.md#llm-curl-workflow), [llm-curl-workflow-workdir.yaml](example-source.md#llm-curl-workflow-workdir), [llama-uv-worker.yaml](example-source.md#llama-uv-worker), or [vllm-uv-worker.yaml](example-source.md#vllm-uv-worker).
- Use `volumes` for model directories and fast-changing code.
- Use `x-enroot.prepare.commands` for slower-changing dependencies.

## Debug cluster readiness

- Run `hpc-compose validate -f compose.yaml`.
- Run `hpc-compose validate -f compose.yaml --strict-env` when default interpolation fallbacks should be treated as failures.
- Run `hpc-compose inspect --verbose -f compose.yaml`.
- Run `hpc-compose preflight -f compose.yaml`.
- Read the troubleshooting sections in [Runbook](runbook.md).

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

- Prefer `--format json` for machine-readable output on non-streaming commands such as `new`, `validate`, `render`, `prepare`, `preflight`, `config`, `inspect`, `submit`, `status`, `ps`, `stats`, `artifacts`, `down`, `cancel`, `setup`, `cache`, `clean`, and `context`.
- Include `context --format json` when automation needs resolved compose path, binaries, interpolation vars, and runtime path roots.
- Use `hpc-compose stats --format jsonl` or `--format csv` when downstream tooling wants row-oriented metrics.
- Treat `--json` as a compatibility alias on older machine-readable commands; new automation should prefer `--format json`. Streaming commands such as `logs --follow`, `watch`, and `completions` keep their native text or script output.

## Related Docs

- [Support Matrix](support-matrix.md)
- [CLI Reference](cli-reference.md)
- [Execution Model](execution-model.md)
- [Runbook](runbook.md)
- [Examples](examples.md)
- [Spec Reference](spec-reference.md)
