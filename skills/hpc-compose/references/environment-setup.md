# Environment Setup Assistant Reference

Use this reference when the user wants an agent to set up hpc-compose for their own HPC cluster, install or verify the CLI, configure project-local settings, choose a runtime backend, select a shared cache, or guide a first safe cluster smoke run.

Prefer current primary docs when available:

- Published docs: https://nicolasschuler.github.io/hpc-compose/
- Installation: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/installation.md
- Quickstart: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/quickstart.md
- Runbook: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/runbook.md
- Task guide: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/task-guide.md
- Cluster profiles: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/cluster-profiles.md
- Cache management: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/cache-management.md
- Runtime backends: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/runtime-backends.md
- Troubleshooting: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/troubleshooting.md
- HAICORE guide: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/haicore-guide.md

## Setup Goal

Guide the user from "I want to try hpc-compose on my cluster" to:

- an installed or verified `hpc-compose` binary,
- a known target machine for authoring versus submission,
- a selected runtime backend,
- a shared cache path visible to login and compute nodes,
- project-local settings or explicit environment variables,
- a first valid spec or smoke template,
- static checks that pass,
- exact next commands for login-node preflight and optional submission.

Do not claim the setup is cluster-ready until the cluster-specific facts have been checked on the target login/submission host.

## Discovery Checklist

Ask or inspect the minimum facts needed for the next step:

| Fact | How to discover | Why it matters |
| --- | --- | --- |
| Cluster/site name and docs | User, site docs, support pages | Source of partition, runtime, filesystem, and policy facts. |
| Access path | `ssh` target, login node policy | Separates local authoring from cluster submission. |
| OS/architecture | `uname -sm` | Selects release asset or source build path. |
| Slurm tools | `command -v sbatch srun sinfo scontrol` | Confirms submission host readiness. |
| Runtime backend | `srun --help | grep container-image`, `command -v enroot apptainer singularity` | Chooses `pyxis`, `apptainer`, `singularity`, or `host`. |
| Shared cache path | Site filesystem docs, project/work/scratch path, `mkdir -p`, `test -w` | `x-slurm.cache_dir` must be shared and writable. |
| Account/partition/QOS | User, site docs, `sinfo`, cluster policy | Maps to `x-slurm.account`, `partition`, `qos`, and limits. |
| Workload shape | Repo probe, run command, image, CPU/GPU/MPI needs | Chooses template/spec and resource requests. |
| Submission approval | Explicit user confirmation | Avoids spending allocation or GPU hours unexpectedly. |

For HAICORE, read `haicore-kit.md` and the live HAICORE docs before relying on partition, workspace, GPU, or container assumptions.

## Safe Command Ladder

Use commands in this order, stopping when a fact is missing.

1. Verify or install the CLI.

```bash
command -v hpc-compose
hpc-compose --version
```

When installing from a release, use the Installation doc and pin a release tag:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
export PATH="$HOME/.local/bin:$PATH"
hpc-compose --version
```

Use a source checkout only for development, unreleased testing, or when release binaries are not suitable:

```bash
cargo build --release
target/release/hpc-compose --version
```

2. Create or inspect a first spec without touching Slurm.

```bash
hpc-compose new --template minimal-batch --name my-app --output compose.yaml
hpc-compose plan -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
```

For an existing project, run the repo probe first when available, then adapt a separate `compose.hpc.yaml`.

3. Choose and persist a shared cache.

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
hpc-compose setup --profile-name dev --cache-dir "$CACHE_DIR" --default-profile dev --non-interactive
hpc-compose --profile dev context --format json
```

Keep user-private absolute paths, account strings, and secrets out of committed files unless the user explicitly wants a local-only settings file. Prefer `.env.example` or placeholders for shareable examples.

4. Generate a cluster profile on the login/submission host.

```bash
hpc-compose doctor cluster-report
hpc-compose doctor cluster-report --out .hpc-compose/cluster.toml
hpc-compose context --format json
```

Cluster profiles advise and warn; they should not be treated as complete site policy.

5. Run pre-submission checks.

```bash
hpc-compose validate -f compose.yaml
hpc-compose validate -f compose.yaml --strict-env
hpc-compose plan --verbose -f compose.yaml
hpc-compose debug -f compose.yaml --preflight
```

Use `plan --show-script` to inspect generated `#SBATCH` and `srun` lines before asking for submission approval.

6. Ask before real submission.

```bash
hpc-compose up -f compose.yaml
```

After submission, use:

```bash
hpc-compose jobs list
hpc-compose status -f compose.yaml
hpc-compose ps -f compose.yaml
hpc-compose logs -f compose.yaml --follow
hpc-compose debug -f compose.yaml
```

## Backend Selection Rules

- Prefer `pyxis` when `srun --help` advertises `--container-image` and Enroot is available.
- Use `apptainer` when the site standardizes on Apptainer/SIF images.
- Use `singularity` for older Singularity sites.
- Use `host` for module-based jobs or workflows that should not run in containers.
- Do not assume Docker daemon access on an HPC login node.

Run `hpc-compose preflight -f <file>` or `hpc-compose debug -f <file> --preflight` to check selected backend tools in the actual submission context.

## Handoff Format

End environment-setup work with:

- Observation: what was inspected or changed, including docs and commands used.
- Hypothesis: cluster facts that still rely on user reports or dated docs.
- Recommendation: exact next safe command on the target machine.
- Open question: missing account, partition, QOS, shared path, module, runtime, or submission approval.

If the agent changed files, list them. If no real cluster command was run, say that clearly.
