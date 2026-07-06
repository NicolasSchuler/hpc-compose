# Quickstart

This is the shortest safe path from an empty shell to a static plan, a first real Slurm run, and one-command failure triage.

If Slurm terms such as `sbatch`, `srun`, allocation, job step, Pyxis, or Enroot are unfamiliar, read [Slurm And Container Basics](slurm-container-basics.md) before the first real cluster run.

## 1. Install The CLI

[Installation](installation.md) is the single owner of install, verify, mirror, and source-build commands. Install the CLI from there, confirm `hpc-compose --version` works, then return here.

## 2. Learn The Safe Authoring Path First

The safe authoring path runs entirely on a laptop, workstation, or login node — `new` writes a local starter spec and `plan` is purely static (no `sbatch`, no image import):

```bash
hpc-compose new --template minimal-batch --name my-app --output compose.yaml
hpc-compose plan -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
```

`plan` validates the spec and resolves service order; `plan --show-script` adds the rendered batch script. Run that block first on macOS, a laptop, or any machine where you want to evaluate the authoring model before touching a real cluster. The Overview page covers the same walkthrough with full expected output.

If you want a guided learning path instead of a single starter template, run the Spec Metamorphosis tutorial:

```bash
hpc-compose evolve --output compose.yaml
```

The normal workflow to remember is:

```bash
hpc-compose plan -f compose.yaml
hpc-compose up -f compose.yaml
hpc-compose debug -f compose.yaml --preflight
```

## 3. Choose A Starting Spec

Use the built-in starter templates when you want a fresh `compose.yaml` with your application name filled in:

```bash
hpc-compose new \
  --template minimal-batch \
  --name my-app \
  --output compose.yaml
```

Add `--cache-dir '<shared-cache-dir>'` when you want the generated file to include an explicit `x-slurm.cache_dir`. Otherwise the plan uses the active settings cache default or `$HOME/.cache/hpc-compose`.

From a source checkout, you can also inspect a known-good repository example:

```bash
hpc-compose plan -f examples/minimal-batch.yaml
```

The [Examples](examples.md) page is the single selection guide for beginner, LLM, training, distributed, and pipeline workflows.

Use [Spec Metamorphosis](evolve.md) when you want to learn those concepts progressively in one evolving valid spec.

## 4. Pick And Test A Cache Directory

`cache_dir` is optional in the spec, but real clusters usually need a site-specific shared path because image preparation happens before the job starts and compute nodes must later see those artifacts.

Ask your cluster documentation or support team for a project scratch, work, or shared filesystem path, then test it:

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
```

Persist it in project settings when you want the same value every time:

```bash
hpc-compose setup --profile-name dev --cache-dir "$CACHE_DIR" --default-profile dev --non-interactive
```

Or keep using an environment-backed explicit spec value and persist it next to your copied spec:

```bash
printf 'CACHE_DIR=%s\n' "$CACHE_DIR" > .env
```

Do not use `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm` for `x-slurm.cache_dir`. Validation may accept those strings, but `preflight` reports them as unsafe because prepare happens before runtime and compute nodes must later see the cached artifacts.

## 5. Before Your First Cluster Run

| Command category | Where to run it | Required tools | Notes |
| --- | --- | --- | --- |
| Authoring: `new`, `plan`, `validate`, `inspect`, `render`, `config`, `schema` | laptop, workstation, or login node | `hpc-compose` | `plan` is the recommended static pre-run check. |
| Local real-scheduler smoke test | source checkout on a machine with Docker/Podman | `docker compose` or `podman compose` | The [Local Slurm Dev Cluster](local-slurm-dev-cluster.md) runs real local `sbatch`; use `runtime.backend: host`. |
| Prepare: `prepare` | Linux host with selected runtime backend | Pyxis needs Enroot; Apptainer needs `apptainer`; Singularity needs `singularity`; host backend needs no container runtime | Does not call `sbatch`, but needs runtime tools for image work. |
| Cluster checks: `preflight`, `doctor cluster-report` | Linux Slurm login node | Slurm client tools plus selected backend tools | Use `preflight --strict` when warnings should block launch. |
| Run: `up`, `run` | Linux Slurm login node | `sbatch`, `srun`, scheduler tools, selected backend tools | `up` is the normal cluster execution path. |
| Local launch: `up --local` | Linux host only | Enroot and `runtime.backend: pyxis` | Single-host only; not a distributed Slurm substitute. |

For Pyxis, `srun --help` should mention `--container-image`.

---

> **Everything above is safe on any machine. Everything below requires a real Slurm submission host.**

The steps up to here only author specs, prepare a cache path, and read static plans. From this point the commands call `sbatch`, `srun`, and the runtime backend, so run them only on a supported Linux Slurm submission host.

## 6. Submit On A Real Cluster

When you move to a supported Linux submission host, the normal run is:

```bash
hpc-compose up -f compose.yaml
```

`up` runs preflight, prepares missing artifacts, renders the batch script, submits it through `sbatch`, then follows scheduler state and tracked logs. On the first run (or after cache eviction) the prepare step imports your container image with enroot — a multi-GB download, then extract and squashfs build — which can take several minutes; later runs reuse the cache, and an interactive terminal streams live import sub-progress. On an interactive TTY it opens the full-screen watch UI; otherwise it falls back to line-oriented output. Add `--watch-queue` when you want line-oriented queue polling until the Slurm job reaches `RUNNING` before the normal watch view opens; `--queue-warn-after <DURATION>` controls the one-time long-pending warning. The watch UI holds the final screen on failures by default; use `--hold-on-exit never|failure|always` to tune that behavior. Use `hpc-compose up --detach -f compose.yaml` when you want submit-and-return behavior.

Success looks like:

- the job is submitted or launched
- a tracked job id is recorded
- the watch UI or text follower shows scheduler progress
- `status`, `ps`, and `logs` can reconnect to the tracked run later

## 7. If The First Cluster Run Fails

| Symptom | Best next command | Why |
| --- | --- | --- |
| Missing `sbatch`, `srun`, `enroot`, `apptainer`, or `singularity` | `hpc-compose debug -f compose.yaml --preflight` | Reruns prerequisite checks and keeps the latest tracked context in one report. |
| `srun` does not advertise `--container-image` | `hpc-compose doctor cluster-report` | Pyxis support is unavailable or not loaded on that node. |
| Job submitted but no service log appeared | `hpc-compose debug -f compose.yaml` | Shows scheduler state, batch log tail, service log hints, and the next command. |
| Cache path warning or error | `hpc-compose debug -f compose.yaml --preflight` | Confirms whether `x-slurm.cache_dir` looks shared and is writable from the login node. On a login node, run `hpc-compose preflight -f compose.yaml --fs-probes` to submit a tiny compute-node visibility and rename probe. |
| Services start in the wrong order | `hpc-compose plan --explain --verbose -f compose.yaml` | Shows normalized dependencies, readiness gates, and planner hints before running. |

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
target/release/hpc-compose plan -f examples/minimal-batch.yaml
target/release/hpc-compose plan --show-script -f examples/minimal-batch.yaml
```

## Read Next

- [Installation](installation.md)
- [Support Matrix](support-matrix.md)
- [Why hpc-compose](why-hpc-compose.md)
- [Slurm And Container Basics](slurm-container-basics.md)
- [Examples](examples.md)
- [Runtime Backends](runtime-backends.md)
- [Runbook](runbook.md)
- [Troubleshooting](troubleshooting.md)
