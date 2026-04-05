<p align="center">
  <img src="docs/logo.png" width="360" alt="hpc-compose logo">
</p>

# hpc-compose

[![CI](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml/badge.svg)](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/NicolasSchuler/hpc-compose)](https://github.com/NicolasSchuler/hpc-compose/releases/latest)

`hpc-compose` is a single-binary launcher that turns a Compose-like spec into a single Slurm job running one or more services through Enroot and Pyxis.

It is intentionally **not** a full Docker Compose implementation. It focuses on the subset that maps cleanly to one Slurm allocation, one node in v1, and multiple containerized services inside that allocation.

## What It Is For

- one Slurm allocation per application
- one node per allocation in v1
- multiple services started inside that allocation
- remote images such as `redis:7` or existing local `.sqsh` images
- login-node image preparation through `x-enroot.prepare`
- readiness-gated startup across dependent services

## What It Does Not Support

- Compose `build:`
- `ports`
- `networks` / `network_mode`
- `restart`
- `deploy`
- multi-node service placement in v1

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

The installer selects the newest GitHub release for the current Linux or macOS machine and installs `hpc-compose` into `~/.local/bin` by default. Manual release downloads remain documented in [docs/src/installation.md](docs/src/installation.md).

## Minimal example

```yaml
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

`submit --watch` is the normal run. Use `validate`, `inspect`, `preflight`, or `prepare` as the debugging flow when you are adapting a new spec or isolating a failure.

## Documentation

- Published docs: [nicolasschuler.github.io/hpc-compose](https://nicolasschuler.github.io/hpc-compose/)
- Installation: [docs/src/installation.md](docs/src/installation.md)
- Quickstart: [docs/src/quickstart.md](docs/src/quickstart.md)
- Execution model: [docs/src/execution-model.md](docs/src/execution-model.md)
- Runbook: [docs/src/runbook.md](docs/src/runbook.md)
- Examples: [docs/src/examples.md](docs/src/examples.md)
- Spec reference: [docs/src/spec-reference.md](docs/src/spec-reference.md)
- Docker Compose migration: [docs/src/docker-compose-migration.md](docs/src/docker-compose-migration.md)
- Contributor architecture notes: [docs/src/architecture.md](docs/src/architecture.md)
