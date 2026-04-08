<p align="center">
  <img src="docs/logo.png" width="360" alt="hpc-compose logo">
</p>

# hpc-compose

[![CI](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml/badge.svg)](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/NicolasSchuler/hpc-compose)](https://github.com/NicolasSchuler/hpc-compose/releases/latest)

`hpc-compose` is a single-binary launcher that turns a Compose-like spec into one Slurm job running one or more services through Enroot and Pyxis.

It is intentionally **not** a full Docker Compose implementation. It focuses on the subset that maps cleanly to one Slurm allocation, plus either single-node services or one allocation-wide distributed service without adding a separate orchestration layer.

## What It Is For

- one Slurm allocation per application
- single-node jobs and constrained multi-node distributed runs
- optional helper services pinned to the allocation's primary node
- remote images such as `redis:7` or existing local `.sqsh` images
- login-node image preparation through `x-enroot.prepare`
- readiness-gated startup across dependent services
- per-service `restart_on_failure` with bounded retries and rolling-window crash-loop protection

## What It Does Not Support

- Compose `build:`
- `ports`
- `networks` / `network_mode`
- Compose `restart` (use `services.<name>.x-slurm.failure_policy` instead)
- `deploy`
- arbitrary multi-node orchestration or partial-node service placement

For the exact first-class vs raw pass-through vs out-of-scope Slurm boundary, see [Supported Slurm model](docs/src/supported-slurm-model.md).

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

The installer selects the newest GitHub release for the current Linux or macOS machine and installs `hpc-compose` into `~/.local/bin` by default. Installer availability is not the same thing as full runtime support; see the [Support Matrix](docs/src/support-matrix.md) before assuming a platform/cluster combination is supported end to end. Manual release downloads remain documented in [docs/src/installation.md](docs/src/installation.md).

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

## Restart policy example

When you want per-service retries on transient non-zero exits, use `services.<name>.x-slurm.failure_policy` instead of Compose `restart:`.

```yaml
services:
  worker:
    image: python:3.11-slim
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 5
        backoff_seconds: 5
        window_seconds: 60
        max_restarts_in_window: 3
```

`restart_on_failure` only reacts to non-zero process exits. It enforces both a lifetime cap (`max_restarts`) and a rolling-window cap (`max_restarts_in_window` within `window_seconds`) during one live batch-script execution. `hpc-compose status -f compose.yaml` reports the current restart budget as `window=<current>/<max>@<seconds>s`. See the runnable [`examples/restart-policy.yaml`](examples/restart-policy.yaml), the [Spec Reference](docs/src/spec-reference.md), and the [Runbook](docs/src/runbook.md) for details.

## Documentation

- Published docs: [nicolasschuler.github.io/hpc-compose](https://nicolasschuler.github.io/hpc-compose/)
- Installation: [docs/src/installation.md](docs/src/installation.md)
- Quickstart: [docs/src/quickstart.md](docs/src/quickstart.md)
- Support matrix: [docs/src/support-matrix.md](docs/src/support-matrix.md)
- Task guide: [docs/src/task-guide.md](docs/src/task-guide.md)
- Execution model: [docs/src/execution-model.md](docs/src/execution-model.md)
- Runbook: [docs/src/runbook.md](docs/src/runbook.md)
- Examples: [docs/src/examples.md](docs/src/examples.md)
- Spec reference: [docs/src/spec-reference.md](docs/src/spec-reference.md)
- Supported Slurm model: [docs/src/supported-slurm-model.md](docs/src/supported-slurm-model.md)
- Docker Compose migration: [docs/src/docker-compose-migration.md](docs/src/docker-compose-migration.md)
- Contributor architecture notes: [docs/src/architecture.md](docs/src/architecture.md)

## Project policies

- License: [LICENSE](LICENSE)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- Security: [SECURITY.md](SECURITY.md)
- Code of Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## Citation

If you use `hpc-compose` in research, please cite the software. GitHub also exposes the same metadata through the repository citation UI via [`CITATION.cff`](CITATION.cff).

```bibtex
@software{schuler_hpc_compose_2026,
  author = {Schuler, Nicolas},
  title = {hpc-compose},
  version = {0.1.16},
  year = {2026},
  publisher = {Karlsruhe Institute of Technology (KIT)},
  url = {https://github.com/NicolasSchuler/hpc-compose}
}
```
