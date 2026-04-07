# Task Guide

Use this page when you know what you want to do, but not yet which command or example should be your starting point.

## First run

- Read [Quickstart](quickstart.md).
- Start from `minimal-batch` with `hpc-compose init --template minimal-batch --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml`.
- Run `hpc-compose submit --watch -f compose.yaml`.

## Migrate from Docker Compose

- Read [Docker Compose Migration](docker-compose-migration.md).
- Replace `build:` with `image:` plus `x-enroot.prepare.commands`.
- Replace service-name networking with `127.0.0.1` or explicit allocation metadata where appropriate.

## Single-node multi-service app

- Start from [app-redis-worker.yaml](examples.md).
- Add `depends_on` and `readiness` only where ordering really matters.
- Use [Execution model](execution-model.md) to confirm which services can rely on localhost.

## Multi-node distributed training

- Start from [multi-node-torchrun.yaml](examples.md) or [multi-node-mpi.yaml](examples.md).
- Treat helper services as primary-node-only and the distributed job as the single allocation-wide step.
- Use allocation metadata such as `HPC_COMPOSE_PRIMARY_NODE` instead of Docker-style service discovery.

## Checkpoint and resume workflows

- Start from [training-checkpoints.yaml](examples.md) when you only need artifact output.
- Start from [training-resume.yaml](examples.md) when the run should resume from shared storage across retries or later submissions.
- Keep the canonical resume source in `x-slurm.resume.path`, not in exported artifact bundles.

## LLM serving workflows

- Start from [llm-curl-workflow.yaml](examples.md), [llm-curl-workflow-workdir.yaml](examples.md), [llama-uv-worker.yaml](examples.md), or [vllm-uv-worker.yaml](examples.md).
- Use `volumes` for model directories and fast-changing code.
- Use `x-enroot.prepare.commands` for slower-changing dependencies.

## Debug cluster readiness

- Run `hpc-compose validate -f compose.yaml`.
- Run `hpc-compose inspect --verbose -f compose.yaml`.
- Run `hpc-compose preflight -f compose.yaml`.
- Read the troubleshooting sections in [Runbook](runbook.md).

## Cache and artifact management

- Use `hpc-compose cache list` to inspect imported/prepared artifacts.
- Use `hpc-compose cache inspect -f compose.yaml` to see per-service reuse expectations.
- Use `hpc-compose artifacts -f compose.yaml` after a run to export tracked payloads.

## Automation and scripting with JSON output

- Prefer `--format json` for machine-readable output on `validate`, `render`, `prepare`, `preflight`, `inspect`, `status`, `stats`, `artifacts`, and `cache` subcommands.
- Use `hpc-compose stats --format jsonl` or `--format csv` when downstream tooling wants row-oriented metrics.
- Treat `--json` as a compatibility alias on older machine-readable commands; new automation should prefer `--format json`.

## Related docs

- [Support Matrix](support-matrix.md)
- [Execution model](execution-model.md)
- [Runbook](runbook.md)
- [Examples](examples.md)
- [Spec reference](spec-reference.md)
