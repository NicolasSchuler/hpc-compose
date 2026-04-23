<p align="center">
  <img src="docs/logo.png" width="360" alt="hpc-compose logo">
</p>

# hpc-compose

[![CI](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml/badge.svg)](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/NicolasSchuler/hpc-compose)](https://github.com/NicolasSchuler/hpc-compose/releases/latest)

`hpc-compose` turns a Compose-like spec into one Slurm job for multi-service HPC and research ML workflows.

It is for teams who want Docker-Compose-like ergonomics on Slurm without adding Kubernetes or a custom control plane.

Compatibility starts with the [Support Matrix](docs/src/support-matrix.md): Linux is the maintained runtime target, while macOS is intended for authoring and inspection workflows.

## Why This Exists

- Multi-service Slurm jobs are awkward to author, inspect, and repeat with plain `sbatch` scripts alone.
- Docker Compose is familiar, but its runtime assumptions do not map cleanly to one Slurm allocation.
- `hpc-compose` keeps the scope narrow so you can validate, inspect, render, and submit a single generated job without introducing a cluster-side control plane.

## Who It Is For

- research engineers and ML practitioners running jobs on Slurm clusters
- HPC platform or tooling owners who support those users
- teams that want one inspectable batch job instead of a long-running orchestrator

## Used For

- model serving plus helper services inside one allocation
- data and ETL pipelines that need startup ordering, successful-completion DAG stages, scratch staging, and shared job-local state
- training runs with checkpoint export, artifact collection, and resume-aware workflows
- clusters that standardize on Pyxis/Enroot, Apptainer, Singularity, or host module runtimes

## Start Here

These are the four promoted examples to start from.

- [`examples/minimal-batch.yaml`](examples/minimal-batch.yaml): first run.
  Run `hpc-compose up -f examples/minimal-batch.yaml`.
  Success looks like the batch log printing `Hello from Slurm!`.

- [`examples/app-redis-worker.yaml`](examples/app-redis-worker.yaml): multi-service single-node workflow.
  Run `hpc-compose up -f examples/app-redis-worker.yaml`.
  Success looks like the worker log showing `PONG` and repeated `INCR jobs` calls after Redis becomes ready.

- [`examples/llm-curl-workflow-workdir.yaml`](examples/llm-curl-workflow-workdir.yaml): inference and service workflow.
  Prerequisites: one GPU-capable Slurm path plus a GGUF model at `$HOME/models/model.gguf`.
  Run `hpc-compose up -f examples/llm-curl-workflow-workdir.yaml`.
  Success looks like `curl_client.log` containing a JSON response from `/v1/chat/completions`.

- [`examples/training-resume.yaml`](examples/training-resume.yaml): training durability with artifact export and resume.
  Prerequisites: shared storage for `x-slurm.resume.path` and `CACHE_DIR`.
  Run `hpc-compose up -f examples/training-resume.yaml`.
  Success looks like `results/<job-id>/` containing exported checkpoints and later attempts resuming from the saved epoch.

The full example funnel lives in [docs/src/examples.md](docs/src/examples.md).

## Golden Path

If you are evaluating `hpc-compose` from a laptop or workstation first, use the authoring path:

```bash
hpc-compose validate -f examples/minimal-batch.yaml
hpc-compose inspect -f examples/minimal-batch.yaml
hpc-compose up --dry-run --skip-prepare --no-preflight \
  --script-out /tmp/hpc-compose-demo.sbatch \
  -f examples/minimal-batch.yaml
```

Success looks like:

- `validate` prints `spec is valid`
- `inspect` shows `service order: app`
- `up --dry-run` writes a script path and skips `sbatch`

See the [asciinema-style golden-path demo cast](docs/src/quickstart-demo.cast) and the full [Quickstart](docs/src/quickstart.md).

## What It Does Not Support

- Compose `build:`
- `ports`
- `networks` / `network_mode`
- Compose `restart` (use `services.<name>.x-slurm.failure_policy` instead)
- `deploy`
- dynamic multi-node scheduling or automatic node bin packing

For the exact first-class vs raw pass-through vs out-of-scope Slurm boundary, see [Supported Slurm Model](docs/src/supported-slurm-model.md).

## Comparison

| Approach | Best at | Weakness for this problem |
| --- | --- | --- |
| Plain `sbatch` scripts | total control and cluster-specific tuning | multi-service coordination, validation, and repeatability stay ad hoc |
| Docker Compose | familiar service authoring on one machine | networking, restart, and orchestration assumptions do not map cleanly to one Slurm allocation |
| `hpc-compose` | Compose-like authoring for one inspectable Slurm job | intentionally narrow scope; not a general orchestrator or full Compose runtime |

## When Not To Use `hpc-compose`

- You need custom container networking or `ports`.
- You need broad Docker Compose compatibility.
- You want a long-running orchestration control plane.
- You need dynamic cross-node scheduling instead of explicit `x-slurm.placement` node selectors.

## Install

If the repository's [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases) page is still empty, build from source for now:

```bash
cargo build --release
./target/release/hpc-compose --help
```

Once a release tag is published on the Releases page, use a version-pinned installer so the installer script and the downloaded assets come from the same tag:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

Replace `vX.Y.Z` with a tag that exists on the Releases page. The installer selects the matching release asset for the current Linux or macOS machine and installs `hpc-compose` into `~/.local/bin` by default. Installer availability is not the same thing as full runtime support; see the [Support Matrix](docs/src/support-matrix.md) before assuming a platform/cluster combination is supported end to end. Manual release downloads, release verification, and internal mirror notes live in [docs/src/installation.md](docs/src/installation.md).

Additional install paths:

- Linux `.deb`: `apt install ./hpc-compose-vX.Y.Z-x86_64-unknown-linux-musl.deb` or `dpkg -i ./...`
- Linux `.rpm`: `dnf install ./hpc-compose-vX.Y.Z-x86_64-unknown-linux-musl.rpm` or `rpm -i ./...`
- macOS Homebrew tap: `brew install NicolasSchuler/hpc-compose/hpc-compose`

For unreleased testing only, you can still run the installer script from `main`:

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

Treat that path as a moving target rather than a pinned release install.

Unix installs also ship section-1 manpages, and the binary can generate Bash, Zsh, and Fish completions with `hpc-compose completions <shell>`.

When you scaffold or adapt a real cluster spec, choose `x-slurm.cache_dir` explicitly. It must be visible from both the submission host and the compute nodes. The shipped repository examples default to `x-slurm.cache_dir: ${CACHE_DIR:-/cluster/shared/hpc-compose-cache}` so they validate out of the box, and you can override that shared path through `.env`, shell environment variables, or `hpc-compose setup`.

On a new cluster, run `hpc-compose doctor --cluster-report` from the login node to generate `.hpc-compose/cluster.toml`. `validate` and `preflight` use that profile to warn about incompatible partitions, QOS, GPU/MPI requests, runtime backend availability, and shared cache or scratch paths before submission.

For MPI services, `hpc-compose doctor --mpi-smoke -f compose.yaml --service <name>` renders a small rank-count probe against the service's actual runtime path and reports requested/advertised MPI types plus host MPI binds. Add `--submit` only when you want to consume a Slurm allocation and run the smoke job.

## Roadmap

The near-term roadmap is intentionally short:

- [Authoring ergonomics](docs/src/roadmap.md#authoring-ergonomics)
- [Runtime visibility](docs/src/roadmap.md#runtime-visibility)
- [Cluster compatibility](docs/src/roadmap.md#cluster-compatibility)

## Documentation

- Published docs: [nicolasschuler.github.io/hpc-compose](https://nicolasschuler.github.io/hpc-compose/)
- Installation: [docs/src/installation.md](docs/src/installation.md)
- Quickstart: [docs/src/quickstart.md](docs/src/quickstart.md)
- Examples: [docs/src/examples.md](docs/src/examples.md)
- Support Matrix: [docs/src/support-matrix.md](docs/src/support-matrix.md)
- Task Guide: [docs/src/task-guide.md](docs/src/task-guide.md)
- CLI Reference: [docs/src/cli-reference.md](docs/src/cli-reference.md)
- Execution Model: [docs/src/execution-model.md](docs/src/execution-model.md)
- Runbook: [docs/src/runbook.md](docs/src/runbook.md)
- Spec Reference: [docs/src/spec-reference.md](docs/src/spec-reference.md)
- Supported Slurm Model: [docs/src/supported-slurm-model.md](docs/src/supported-slurm-model.md)
- Docker Compose Migration: [docs/src/docker-compose-migration.md](docs/src/docker-compose-migration.md)
- Canonical explainer: [docs/src/running-compose-style-workflows-on-slurm.md](docs/src/running-compose-style-workflows-on-slurm.md)
- Release verification and internal mirrors: [docs/src/installation.md#verify-a-release](docs/src/installation.md#verify-a-release)

## Feedback

If you try `hpc-compose`, open an [adoption feedback issue](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=adoption-feedback.yml) and include:

- cluster type
- workload type
- the main failure or friction point

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
  version = {0.1.26},
  year = {2026},
  publisher = {Karlsruhe Institute of Technology (KIT)},
  url = {https://github.com/NicolasSchuler/hpc-compose}
}
```
