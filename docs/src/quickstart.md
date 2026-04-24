# Quickstart

This is the shortest safe path from an empty shell to a validated spec, an inspectable batch script, and a first real Slurm submission.

## 1. Install The CLI

For normal use, install from the latest published [GitHub Release](https://github.com/NicolasSchuler/hpc-compose/releases) and pin the tag you selected:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

Replace `vX.Y.Z` with the published release tag shown on the release page.

The installer places `hpc-compose` in `~/.local/bin` by default and verifies the release checksum sidecar before installing. Release verification, manual downloads, package-manager installs, and source-checkout builds are covered in [Installation](installation.md).

## 2. Learn The Safe Authoring Path First

These commands do not call `sbatch`, do not import images, and do not require Slurm:

```bash
hpc-compose validate -f examples/minimal-batch.yaml
hpc-compose inspect -f examples/minimal-batch.yaml
hpc-compose up --dry-run --skip-prepare --no-preflight \
  --script-out /tmp/hpc-compose-demo.sbatch \
  -f examples/minimal-batch.yaml
```

Expected output includes:

```text
spec is valid
```

```text
service order: app
```

```text
dry run: skipping sbatch submission
```

This is the right first path on macOS, a laptop, or any machine where you want to evaluate the authoring model before touching a real cluster. The same flow is also available as an [asciinema-style demo cast](quickstart-demo.cast), but the snippets above are the accessible reference output.

## 3. Choose A Starting Spec

Use the built-in starter template when you want a fresh `compose.yaml` with your application name and shared cache directory filled in:

```bash
hpc-compose new \
  --template minimal-batch \
  --name my-app \
  --cache-dir '<shared-cache-dir>' \
  --output compose.yaml
```

Replace `<shared-cache-dir>` with a path visible from both the submission host and the compute nodes.

If you want a known-good repository example instead, start with [Examples](examples.md). The examples page is the single selection guide for beginner, LLM, training, distributed, and pipeline workflows.

## 4. Pick And Test `CACHE_DIR`

Repository examples default to `/cluster/shared/hpc-compose-cache` so they validate out of the box, but real clusters usually need a site-specific shared path.

Ask your cluster documentation or support team for a project scratch, work, or shared filesystem path, then test it:

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
```

Persist it next to your copied spec when you want the same value every time:

```bash
printf 'CACHE_DIR=%s\n' "$CACHE_DIR" > .env
```

Do not use `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm` for `x-slurm.cache_dir`. Validation may accept those strings, but `preflight` reports them as unsafe because prepare happens before runtime and compute nodes must later see the cached artifacts.

## 5. Before Your First Cluster Run

| Command category | Where to run it | Required tools | Notes |
| --- | --- | --- | --- |
| Authoring: `new`, `validate`, `inspect`, `render`, `config`, `schema` | laptop, workstation, or login node | `hpc-compose` | Safe before you have Slurm access. |
| Prepare: `prepare` | Linux host with selected runtime backend | Pyxis needs Enroot; Apptainer needs `apptainer`; Singularity needs `singularity`; host backend needs no container runtime | Does not call `sbatch`, but needs runtime tools for image work. |
| Cluster checks: `preflight`, `doctor --cluster-report` | Linux Slurm login node | Slurm client tools plus selected backend tools | Use `preflight --strict` when warnings should block submission. |
| Submission: `up`, `submit`, `run` | Linux Slurm login node | `sbatch`, `srun`, scheduler tools, selected backend tools | Normal cluster execution path. |
| Local launch: `up --local`, `submit --local` | Linux host only | Enroot and `runtime.backend: pyxis` | Single-host only; not a distributed Slurm substitute. |

For Pyxis, `srun --help` should mention `--container-image`.

## 6. Submit On A Real Cluster

When you move to a supported Linux submission host, the normal run is:

```bash
hpc-compose up -f compose.yaml
```

`up` runs preflight, prepares missing artifacts, renders the batch script, submits it through `sbatch`, then follows scheduler state and tracked logs. On an interactive TTY it opens the full-screen watch UI; otherwise it falls back to line-oriented output.

Success looks like:

- the job is submitted or launched
- a tracked job id is recorded
- the watch UI or text follower shows scheduler progress
- `status`, `ps`, and `logs` can reconnect to the tracked run later

## 7. If The First Cluster Run Fails

| Symptom | Best next command | Why |
| --- | --- | --- |
| Missing `sbatch`, `srun`, `enroot`, `apptainer`, or `singularity` | `hpc-compose preflight -f compose.yaml` | Confirms which selected-backend tool is missing. |
| `srun` does not advertise `--container-image` | `hpc-compose doctor --cluster-report` | Pyxis support is unavailable or not loaded on that node. |
| Job submitted but no service log appeared | `hpc-compose status -f compose.yaml` | Shows the tracked top-level batch log path. |
| Cache path warning or error | `hpc-compose preflight --strict -f compose.yaml` | Confirms whether `x-slurm.cache_dir` is shared and writable. |
| Services start in the wrong order | `hpc-compose inspect --verbose -f compose.yaml` | Shows normalized dependencies and readiness gates. |

The longer symptom guide is [Troubleshooting](troubleshooting.md).

## 8. Revisit A Tracked Run Later

```bash
hpc-compose jobs list
hpc-compose status -f compose.yaml
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
hpc-compose stats -f compose.yaml
hpc-compose logs -f compose.yaml --follow
```

Use `jobs list` first when you need to rediscover tracked runs under the current repo tree. Use `ps` for a stable per-service snapshot, `watch` to reconnect to the live UI, and `logs --follow` for a text-only follower.

## From A Source Checkout

If you are developing from a local checkout instead of an installed binary:

```bash
cargo build --release
target/release/hpc-compose validate -f examples/minimal-batch.yaml
target/release/hpc-compose inspect -f examples/minimal-batch.yaml
target/release/hpc-compose up --dry-run --skip-prepare --no-preflight \
  --script-out /tmp/hpc-compose-demo.sbatch \
  -f examples/minimal-batch.yaml
```

## Read Next

- [Support Matrix](support-matrix.md)
- [Examples](examples.md)
- [Runtime Backends](runtime-backends.md)
- [Runbook](runbook.md)
- [Troubleshooting](troubleshooting.md)
