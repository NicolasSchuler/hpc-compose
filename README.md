# hpc-compose

[![CI](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml/badge.svg)](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/NicolasSchuler/hpc-compose)](https://github.com/NicolasSchuler/hpc-compose/releases/latest)

`hpc-compose` is a single-binary launcher that turns a Compose-like spec into a single Slurm job running one or more services through Enroot and Pyxis.

It is intentionally **not** a full Docker Compose implementation. It focuses on the subset that maps cleanly to `sbatch` + `srun` + Enroot on a single node.

## At a glance

```yaml
# compose.yaml
name: hello

x-slurm:
  time: "00:10:00"
  mem: 4G

services:
  app:
    image: python:3.11-slim
    command: python -c "print('Hello from Slurm!')"
```

```bash
hpc-compose submit --watch -f compose.yaml
```

That is all it takes: a familiar Compose file with Slurm resource settings, and one command to validate, prepare, and submit.

## What it is for

- One Slurm allocation per application.
- One node per allocation in v1.
- Multiple services started inside that allocation.
- Remote images such as `redis:7` or existing local `.sqsh` images.
- Optional image customization on the login node through `x-enroot.prepare`.
- Shared cache management for imported and prepared images.
- Readiness-gated startup across dependent services.

## What it does not support

- Compose `build:`
- `ports`
- custom Docker networks / `network_mode`
- `restart` policies
- `deploy`
- multi-node service placement
- mixed string/array `entrypoint` + `command` combinations in ambiguous cases

If you need to customize an image, use `image:` plus `x-enroot.prepare`, not `build:`.

## Start here

- **Runbook:** [`docs/runbook.md`](docs/runbook.md) for the end-to-end workflow from choosing a cache directory to reading logs and pruning cache artifacts.
- **Settings reference:** [`docs/spec-reference.md`](docs/spec-reference.md) for the supported Compose subset, `x-slurm`, and `x-enroot` settings.
- **Migrating from Docker Compose:** [`docs/docker-compose-migration.md`](docs/docker-compose-migration.md) for a side-by-side comparison and step-by-step migration checklist.
- **Examples:** [`examples/README.md`](examples/README.md) for choosing and adapting the shipped example specs.

## Quickstart

Build the binary:

```bash
cargo build --release
```

Then try one of the examples:

```bash
target/release/hpc-compose init --template dev-python-app --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output /tmp/compose.yaml
target/release/hpc-compose validate -f /tmp/compose.yaml
target/release/hpc-compose inspect --verbose -f /tmp/compose.yaml
target/release/hpc-compose submit --watch -f /tmp/compose.yaml
```

In normal cluster use, `submit --watch` is the fastest end-to-end path. It runs preflight, prepares missing images, renders the batch script, submits it through `sbatch`, then follows scheduler state and service logs for the tracked job. Use `status`, `stats`, and `logs` later to revisit a tracked submission, or use `validate`, `inspect`, `preflight`, `prepare`, or `render` separately when adapting a spec or debugging a failure.

For the full workflow, including example selection, cache setup, and log handling, use the [runbook](docs/runbook.md).

## Releases

Push a version tag such as `v0.1.0` to publish downloadable binaries through GitHub Actions:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow runs `cargo test --locked`, builds release archives for Linux, macOS, and Windows, and attaches those archives plus SHA256 checksum files to the GitHub Release for that tag. If you already have a tag and need to backfill assets, you can also run the `Release` workflow manually from the Actions tab and provide that tag.

Linux release notes:

- Linux x86_64 is built for `x86_64-unknown-linux-musl` to avoid host glibc version mismatches on older clusters.
- Linux arm64 is built for `aarch64-unknown-linux-musl` on a native GitHub-hosted ARM runner.
- If your environment blocks downloaded binaries, build locally with `cargo build --release` on the login node or another Linux machine with a compatible toolchain.

## Command flow

- `init` writes a starter compose file from one of the shipped templates.
- `validate` checks that the spec parses and normalizes successfully.
- `inspect` prints the normalized runtime plan and expected cache behavior, with `--verbose` and `--json` output modes for deeper inspection.
- `preflight` checks the login node environment before submission, with grouped diagnostics by default plus `--verbose` and `--json`.
- `prepare` imports or rebuilds missing runtime artifacts on the login node.
- `render` writes the generated `sbatch` script without submitting it.
- `submit` runs preflight, optional prepare, render, and `sbatch`; `submit --watch` also follows scheduler state and service logs. Use `--dry-run` to preview the rendered script without actually calling `sbatch`.
- `status` shows the tracked scheduler/log state for the latest or selected job id for a compose file, including the top-level batch log path.
- `stats` prefers job-local sampler data when `x-slurm.metrics` is enabled, and otherwise falls back to live Slurm `sstat` job-step CPU/memory metrics plus GPU accounting fields when the cluster exposes them.
- `logs` tails tracked service logs, optionally filtered to one service and followed live.
- `cancel` cancels a tracked job via `scancel`.
- `cache list|inspect|prune` inspects and manages cached artifacts.
- `clean` removes old tracked job directories. Use `--age DAYS` or `--all` to select which jobs to prune.
- `completions` generates shell completions for bash, zsh, fish, or PowerShell.

## Examples

- [`examples/app-redis-worker.yaml`](examples/app-redis-worker.yaml): multi-service launch ordering and readiness checks.
- [`examples/dev-python-app.yaml`](examples/dev-python-app.yaml): mounted-code development workflow.
- [`examples/llm-curl-workflow.yaml`](examples/llm-curl-workflow.yaml): end-to-end LLM request with a `curl` client.
- [`examples/llm-curl-workflow-workdir.yaml`](examples/llm-curl-workflow-workdir.yaml): the same LLM flow simplified for direct use from `$HOME/models` on a login node.
- [`examples/llama-app.yaml`](examples/llama-app.yaml): GPU-backed service with a dependent application.
- [`examples/minimal-batch.yaml`](examples/minimal-batch.yaml): simplest single-service batch job.
- [`examples/training-checkpoints.yaml`](examples/training-checkpoints.yaml): GPU training with checkpoints written to shared storage.
- [`examples/postgres-etl.yaml`](examples/postgres-etl.yaml): PostgreSQL plus a Python data processing job.
- [`examples/vllm-openai.yaml`](examples/vllm-openai.yaml): vLLM serving with an in-job Python client.
- [`examples/mpi-hello.yaml`](examples/mpi-hello.yaml): MPI hello world with Open MPI.
- [`examples/multi-stage-pipeline.yaml`](examples/multi-stage-pipeline.yaml): two-stage pipeline coordinating through the shared job mount.
- [`examples/fairseq-preprocess.yaml`](examples/fairseq-preprocess.yaml): CPU-heavy NLP data preprocessing pipeline.

## Build and test

```bash
cargo build --release
cargo test
cargo test --test cli
```
