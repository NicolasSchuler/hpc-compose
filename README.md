<p align="center">
  <img src="docs/logo.png" width="360" alt="hpc-compose logo">
</p>

# hpc-compose

[![CI](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml/badge.svg)](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/NicolasSchuler/hpc-compose)](https://github.com/NicolasSchuler/hpc-compose/releases/latest)

`hpc-compose` turns a small Compose-like YAML file into one inspectable Slurm job for multi-service HPC and research ML workflows.

Use it when you want Docker Compose-style authoring on Slurm without adding Kubernetes, a long-running control plane, or a pile of hand-written `sbatch` glue.

## Safe First Path

These commands work from a laptop, workstation, or login node because `plan` is purely static:

```bash
hpc-compose plan -f examples/minimal-batch.yaml
hpc-compose plan --show-script -f examples/minimal-batch.yaml
```

Expected signals:

```text
spec is valid
service order: app
Rendered script:
```

Run `hpc-compose up -f compose.yaml` only after moving to a supported Linux Slurm submission host with the runtime backend your spec selects. If a run fails, start triage with `hpc-compose debug -f compose.yaml --preflight`.

## Scope

`hpc-compose` is intentionally narrow:

- one Slurm allocation per application
- one generated batch script you can inspect
- service startup ordering and readiness gates inside that allocation
- Pyxis/Enroot, Apptainer, Singularity, or host runtime backends
- tracked logs, state, metrics, artifacts, cache entries, and follow-up commands

It does not aim to be a full Docker Compose runtime. Unsupported Compose features include `build:`, `ports`, custom Docker networks, `deploy`, and dynamic scheduler-style placement across arbitrary nodes.

## Install

For normal use, install from a published GitHub Release and pin the release tag:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

Replace `vX.Y.Z` with the release tag shown on the [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases) page. The installer downloads the matching release asset and installs `hpc-compose` into `~/.local/bin` by default.

Other install paths:

- Linux `.deb` or `.rpm` assets from the release page
- macOS Homebrew tap: `brew install NicolasSchuler/hpc-compose/hpc-compose`
- source checkout for development: `cargo build --release`

Installer availability is not the same as full runtime support. Check the [Support Matrix](docs/src/support-matrix.md) before assuming a platform or cluster can run submission workflows end to end.

## Start From Docs

- [Published manual](https://nicolasschuler.github.io/hpc-compose/)
- [Support Matrix](docs/src/support-matrix.md)
- [Installation](docs/src/installation.md)
- [Quickstart](docs/src/quickstart.md)
- [Examples](docs/src/examples.md)
- [Task Guide](docs/src/task-guide.md)
- [Runtime Backends](docs/src/runtime-backends.md)
- [Runbook](docs/src/runbook.md)
- [Troubleshooting](docs/src/troubleshooting.md)
- [CLI Reference](docs/src/cli-reference.md)
- [Spec Reference](docs/src/spec-reference.md)

## Feedback

If you try `hpc-compose`, open an [adoption feedback issue](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=adoption-feedback.yml) with:

- cluster type
- workload type
- the main failure or friction point

## Project Policies

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
  version = {0.1.34},
  year = {2026},
  publisher = {Karlsruhe Institute of Technology (KIT)},
  url = {https://github.com/NicolasSchuler/hpc-compose}
}
```
