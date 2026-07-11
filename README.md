<p align="center">
  <img src="docs/logo.png" width="420" alt="hpc-compose logo">
  <br>
  <strong>Compose-style multi-service workflows, compiled into one inspectable Slurm job.</strong>
  <br>
  One allocation &middot; one script &middot; Slurm-native runtime
</p>

# hpc-compose

[![CI](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml/badge.svg)](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/ci.yml)
[![Docs](https://github.com/NicolasSchuler/hpc-compose/actions/workflows/docs-pages.yml/badge.svg)](https://nicolasschuler.github.io/hpc-compose/)
[![Release](https://img.shields.io/github/v/release/NicolasSchuler/hpc-compose)](https://github.com/NicolasSchuler/hpc-compose/releases/latest)
[![License](https://img.shields.io/github/license/NicolasSchuler/hpc-compose)](LICENSE)

`hpc-compose` turns a small Compose-like YAML file into one inspectable Slurm job for multi-service HPC and research ML workflows.

Use it when you want Docker Compose-style authoring on Slurm without adding Kubernetes, a long-running control plane, or a pile of hand-written `sbatch` glue.

## Start Here

1. [Choose Your Workflow](docs/src/task-guide.md) to confirm the runtime
   backend, topology, execution style, and submission context.
2. [Run the Quickstart](docs/src/quickstart.md), which deliberately uses the
   smallest `minimal-batch` spec to prove the cluster path before your real
   workload consumes resources.
3. [Choose and adapt an example](docs/src/examples.md), then use the
   [Runbook](docs/src/runbook.md) for repeat operations.

The [published manual](https://nicolasschuler.github.io/hpc-compose/) opens with
three workload paths for batch jobs, multi-service applications, and
distributed training. Static authoring does not submit; runtime commands do.
Check the [Support Matrix](docs/src/support-matrix.md) before a real cluster run.

## Scope

`hpc-compose` is intentionally narrow:

- one Slurm allocation per application
- one generated batch script you can inspect
- service startup ordering and readiness gates inside that allocation
- Slurm-native arrays, submit-time dependencies, and reusable resource profiles
- Pyxis/Enroot, Apptainer, Singularity, or host runtime backends
- finite spec smoke tests plus local `dev` and `tmux` workflows for single-host authoring
- one-off `run --image ... -- <cmd>` jobs and direct `shell --image ...` sessions
- tracked `notebook` sessions launching JupyterLab or VS Code on a compute node
- tracked logs, state, metrics, artifacts, cache entries, and follow-up commands

It does not aim to be a full Docker Compose runtime. Unsupported Compose features include `build:`, `ports`, custom Docker networks, `deploy`, and dynamic scheduler-style placement across arbitrary nodes.

## Install

The fastest path installs the most recent published release with no edits. The
script resolves the latest GitHub Release tag for you and downloads the matching
asset into `~/.local/bin` by default:

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

For reproducible installs (recommended for shared clusters), pin a specific
release tag so every run resolves the exact same asset:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

Replace `vX.Y.Z` with the release tag shown on the [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases) page. Fetching `install.sh` from `main` runs the moving script, but it still installs from a published `releases/download/<tag>/...` asset, not unreleased `main`; pin `HPC_COMPOSE_VERSION` when you need every machine to land on the same build.

Other install paths:

- Linux `.deb` or `.rpm` assets from the release page
- macOS Homebrew tap: `brew install NicolasSchuler/hpc-compose/hpc-compose`
- source checkout for development: `cargo build --release`

Installer availability is not the same as full runtime support. Check the [Support Matrix](docs/src/support-matrix.md) before assuming a platform or cluster can run submission workflows end to end.

## Documentation

- [Published manual](https://nicolasschuler.github.io/hpc-compose/) — complete,
  grouped navigation and built-in search.
- [Choose Your Workflow](docs/src/task-guide.md) and
  [Quickstart](docs/src/quickstart.md) — decisions and the minimal cluster smoke.
- [Examples](docs/src/examples.md) — select and adapt a shipped workload shape.
- [Runbook](docs/src/runbook.md) and
  [Troubleshooting](docs/src/troubleshooting.md) — operate and recover real runs.
- [CLI Reference](docs/src/cli-reference.md) and
  [Spec Reference](docs/src/spec-reference.md) — exact command and YAML contracts.

## Set Up With an AI Agent

You can ask any LLM agent (Claude, Codex, Copilot, Cursor) to set up hpc-compose on your cluster. Point it at the published machine-readable map first, which carries a curated doc index, a safety contract (which commands are static-safe vs. which submit Slurm jobs), and the canonical spec conventions:

- Agent entry map: [`llms.txt`](llms.txt), copied to the Pages root at `https://nicolasschuler.github.io/hpc-compose/llms.txt`
- Walkthrough and copy-paste prompt: [Set Up With an AI Agent](docs/src/ai-agent-setup.md)
- Drop-in skill bundle: [`skills/hpc-compose/SKILL.md`](skills/hpc-compose/SKILL.md)

Agents author and statically verify a spec with redacted `validate`, `lint`,
`plan --format json`, and `inspect --format json` output before any real run,
then apply the command policy before submitting jobs.

## Feedback

If you try `hpc-compose`, start with the [FAQ](docs/src/faq.md) when you are not sure whether a behavior is expected. Otherwise, choose the issue form that matches what you learned:

- [Bug report](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=bug_report.yml) for a reproducible CLI, docs, packaging, or runtime defect.
- [Feature request](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=feature_request.yml) for a proposed workflow, Compose subset, backend, docs, or ergonomics change.
- [Adoption feedback](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=adoption-feedback.yml) for cluster fit, workload fit, and the main failure or friction point.

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
  version = {0.2.1},
  year = {2026},
  publisher = {Karlsruhe Institute of Technology (KIT)},
  url = {https://github.com/NicolasSchuler/hpc-compose}
}
```
