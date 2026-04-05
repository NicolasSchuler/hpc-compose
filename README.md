<p align="center">
  <img src="docs/logo.png" width="360" alt="hpc-compose logo">
</p>

# hpc-compose

[![CI](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml/badge.svg)](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/NicolasSchuler/hpc-compose)](https://github.com/NicolasSchuler/hpc-compose/releases/latest)

`hpc-compose` is a single-binary launcher that turns a Compose-like spec into a single Slurm job running one or more services through Enroot and Pyxis.

It is intentionally **not** a full Docker Compose implementation. It focuses on the subset that maps cleanly to `sbatch` + `srun` + Enroot on a single node.

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

## Quickstart

```bash
cargo build --release
target/release/hpc-compose init --template dev-python-app --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output /tmp/compose.yaml
target/release/hpc-compose submit --watch -f /tmp/compose.yaml
```

`submit --watch` is the usual run. Use `validate`, `inspect`, `preflight`, or `prepare` mainly the first time you adapt a spec or when troubleshooting.

## Documentation

- Published docs: [nicolasschuler.github.io/hpc-compose](https://nicolasschuler.github.io/hpc-compose/)
- Overview: [docs/src/README.md](docs/src/README.md)
- Installation: [docs/src/installation.md](docs/src/installation.md)
- Quickstart: [docs/src/quickstart.md](docs/src/quickstart.md)
- Runbook: [docs/src/runbook.md](docs/src/runbook.md)
- Spec reference: [docs/src/spec-reference.md](docs/src/spec-reference.md)
- Examples: [docs/src/examples.md](docs/src/examples.md)
- Docker Compose migration: [docs/src/docker-compose-migration.md](docs/src/docker-compose-migration.md)

## Build and test

```bash
cargo build --release
cargo test
cargo doc --no-deps
mdbook build docs
```

## Releases

Prebuilt archives are published on [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases). Push a version tag such as `v0.1.0` to publish downloadable binaries through GitHub Actions.
