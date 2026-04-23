# Runbook

This runbook is for adapting `hpc-compose` to a real workload on a Slurm cluster with Enroot and Pyxis.

Commands below assume `hpc-compose` is on your `PATH`. If you are running from a local checkout, replace `hpc-compose` with `target/release/hpc-compose`.

Compose-aware commands accept `-f` / `--file` to specify the compose spec path. When omitted, `hpc-compose` first uses the active context compose file from the project-local settings file, then falls back to `compose.yaml` in the current directory. Commands such as `new`, `setup`, `context`, and `completions` do not take `-f`. (`cache prune --all-unused` requires `-f` explicitly. `cache prune --age` can use the active context/profile unless you pass `--cache-dir`.)

Global context flags are also available everywhere:

- `--profile <NAME>` selects a profile from `.hpc-compose/settings.toml`.
- `--settings-file <PATH>` uses an explicit settings file instead of upward auto-discovery.

Read the [Execution model](execution-model.md) page first if you are still orienting on login-node prepare, compute-node runtime, shared cache paths, or localhost networking.

## Before you start

Make sure you have:

- a login node with `srun` and `sbatch` available,
- the runtime backend you selected in `runtime.backend` (`enroot` for Pyxis, `apptainer`, `singularity`, or host modules),
- `scontrol` available when you request `x-slurm.nodes > 1`,
- Pyxis support in `srun` when `runtime.backend: pyxis` (`srun --help` should mention `--container-image`),
- a shared filesystem path for `x-slurm.cache_dir`,
- any required local source trees or local `.sqsh` / `.sif` images in place,
- registry credentials available if your cluster or registry requires them,
- `curl` on the compute node when using `readiness.type: http`,
- `nvidia-smi` on the compute node when the `gpu` metrics collector is enabled,
- `sstat` on the compute node when the `slurm` metrics collector is enabled.

`preflight` validates all of these automatically. See the full preflight checklist below.

## Command cadence

| Command or step | When to use it |
| --- | --- |
| install or build `hpc-compose` | once per checkout or upgrade |
| `new` or copy a shipped example | once per new spec |
| `setup` and `context` | once per repo or when directory/data/env defaults change |
| `validate` and `inspect` | early while adapting a spec |
| `up` | normal run |
| `watch`, `ps`, `status`, `logs`, `stats` | revisit or inspect a tracked run later |
| `preflight`, `prepare`, `render` | first-time cluster setup checks or the debugging flow |

## Normal progression

For a new spec on a real cluster:

1. Run `hpc-compose new --template <name> --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml`, or copy the closest shipped example.
2. Run `hpc-compose setup` once so compose path, env files, env vars, and binary overrides live in the project-local settings file.
3. Run `hpc-compose context --format json` to verify resolved values and sources.
4. Set or confirm `x-slurm.cache_dir`, and adjust any cluster-specific resource settings.
5. Run `hpc-compose validate -f compose.yaml` and `hpc-compose inspect --verbose -f compose.yaml` while you are still adapting the file.
6. Run `hpc-compose --profile <name> up` for the normal run.
7. If that fails, or if you need more visibility later, break out `preflight`, `prepare`, `render`, `status`, `ps`, `watch`, `stats`, or `logs` separately.

For a minimal cluster smoke test from a checkout, set `CACHE_DIR` to shared storage and run `scripts/cluster_smoke.sh`. It validates, preflights, and renders by default; set `HPC_COMPOSE_SMOKE_SUBMIT=1` only when you want it to submit the smoke job.

## Profiled Context (Project-Local Settings File)

`hpc-compose` can discover `.hpc-compose/settings.toml` by walking upward from the current directory. You can also pin a file with `--settings-file`.

Typical setup flow:

```bash
hpc-compose setup
hpc-compose context
hpc-compose --profile dev context --format json
```

Non-interactive setup is available for scripting:

```bash
hpc-compose setup --profile-name dev --compose-file compose.yaml --env-file .env --env-file .env.dev --env 'CACHE_DIR=<shared-cache-dir>' --default-profile dev --non-interactive
```

Settings file shape (`.hpc-compose/settings.toml`):

```toml
version = 1
default_profile = "dev"

[defaults]
compose_file = "compose.yaml"
env_files = [".env"]

[defaults.env]
CACHE_DIR = "/cluster/shared/hpc-compose-cache"

[profiles.dev]
compose_file = "compose.yaml"
env_files = [".env", ".env.dev"]

[profiles.dev.env]
RESUME_DIR = "/shared/$USER/runs/my-run"
MODEL_DIR = "$HOME/models"
```

Resolution precedence is fixed and explicit:

1. CLI flags
2. selected profile values
3. shared settings defaults
4. built-in CLI defaults

Use `context` whenever you want to inspect effective compose path, binaries, interpolation variables, runtime paths, and per-field sources (`cli`, `profile`, `defaults`, `compose`, `builtin`, `process_env`).

## Pick a starting example

| Example | Use it when you need | File |
| --- | --- | --- |
| Dev app | mounted source tree plus a small prepare step | [`examples/dev-python-app.yaml`](example-source.md#dev-python-app) |
| Redis worker stack | multi-service launch ordering and readiness checks | [`examples/app-redis-worker.yaml`](example-source.md#app-redis-worker) |
| LLM curl workflow | one GPU-backed LLM plus a one-shot `curl` request from a second service | [`examples/llm-curl-workflow.yaml`](example-source.md#llm-curl-workflow) |
| LLM curl workflow (home) | the same request flow, but anchored under `$HOME/models` for direct use on a login node | [`examples/llm-curl-workflow-workdir.yaml`](example-source.md#llm-curl-workflow-workdir) |
| GPU-backed app | one GPU service plus a dependent application | [`examples/llama-app.yaml`](example-source.md#llama-app) |
| llama.cpp + uv worker | llama.cpp serving plus a source-mounted Python worker run through `uv` | [`examples/llama-uv-worker.yaml`](example-source.md#llama-uv-worker) |
| Minimal batch | simplest single-service batch job | [`examples/minimal-batch.yaml`](example-source.md#minimal-batch) |
| Multi-node MPI | first-class MPI launch, generated MPI hostfile, and one primary-node helper | [`examples/multi-node-mpi.yaml`](example-source.md#multi-node-mpi) |
| Multi-node partitioned | disjoint node ranges and explicit co-location within one allocation | [`examples/multi-node-partitioned.yaml`](example-source.md#multi-node-partitioned) |
| Multi-node torchrun | allocation-wide GPU training with the primary node as rendezvous | [`examples/multi-node-torchrun.yaml`](example-source.md#multi-node-torchrun) |
| Training checkpoints | GPU training with checkpoints to shared storage | [`examples/training-checkpoints.yaml`](example-source.md#training-checkpoints) |
| Training resume | GPU training with a shared resume directory and attempt-aware checkpoints | [`examples/training-resume.yaml`](example-source.md#training-resume) |
| Postgres ETL | PostgreSQL plus a Python data processing job | [`examples/postgres-etl.yaml`](example-source.md#postgres-etl) |
| vLLM serving | vLLM with an in-job Python client | [`examples/vllm-openai.yaml`](example-source.md#vllm-openai) |
| vLLM + uv worker | vLLM serving with a source-mounted Python worker run through `uv` | [`examples/vllm-uv-worker.yaml`](example-source.md#vllm-uv-worker) |
| MPI hello | small MPI workload using service-level `x-slurm.mpi` | [`examples/mpi-hello.yaml`](example-source.md#mpi-hello) |
| MPI PMIx v4 + host MPI | versioned PMIx launch plus host MPI bind/env configuration | [`examples/mpi-pmix-v4-host-mpi.yaml`](example-source.md#mpi-pmix-v4-host-mpi) |
| Multi-stage pipeline | two-stage pipeline with file-based handoff | [`examples/multi-stage-pipeline.yaml`](example-source.md#multi-stage-pipeline) |
| Pipeline DAG | one-shot preprocess -> train -> postprocess completion dependencies | [`examples/pipeline-dag.yaml`](example-source.md#pipeline-dag) |
| Data preprocessing | CPU-heavy NLP preprocessing pipeline | [`examples/fairseq-preprocess.yaml`](example-source.md#fairseq-preprocess) |

The fastest path is usually to copy the closest example and adapt it instead of starting from scratch.

You can also let `hpc-compose` scaffold one of these examples directly:

```bash
hpc-compose new --template dev-python-app --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml
```

## 1. Choose `x-slurm.cache_dir` early

Set `x-slurm.cache_dir` to a path that is visible from both the login node and the compute nodes.

```yaml
x-slurm:
  cache_dir: /cluster/shared/hpc-compose-cache
```

Rules:

- Do **not** use `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm`.
- If you leave `cache_dir` unset, the default is `$HOME/.cache/hpc-compose`.
- The default is convenient for small or home-directory workflows, but a shared project or workspace path is usually safer on real clusters.
- The important constraint is visibility: `prepare` runs on the login node, but the batch job later reuses those cached artifacts from compute nodes.

The shipped repository examples default `x-slurm.cache_dir` to `/cluster/shared/hpc-compose-cache` and still honor `CACHE_DIR`, so you can set the shared path once in `.env`, the shell environment, or `hpc-compose setup`.

## 2. Adapt the example to your workload

Start with the nearest example and then change:

- `image`
- `command` / `entrypoint`
- `volumes`
- `environment`
- `x-slurm` resource settings
- `x-runtime.prepare` commands for dependencies or tooling

Recommended pattern:

- Put fast-changing application code in `volumes`.
- Put slower-changing dependency installation in `x-runtime.prepare.commands`.
- Add `readiness` to any service that other services truly depend on.

## 3. Validate the spec

```bash
hpc-compose validate -f compose.yaml
hpc-compose validate -f compose.yaml --strict-env
```

Use `validate` first when you are changing:

- field names,
- `depends_on` shape,
- `command` / `entrypoint` form,
- path values,
- `x-slurm` / `x-enroot` blocks.

If `validate` fails, fix that before doing anything more expensive.
Use `--strict-env` when you want missing interpolation variables to fail instead of silently consuming `${VAR:-default}` or `${VAR-default}` fallbacks.

## 4. Inspect the normalized plan

```bash
hpc-compose inspect -f compose.yaml
hpc-compose inspect --verbose -f compose.yaml
```

Check:

- service order,
- allocation geometry and each service's step geometry,
- how images were normalized,
- final host-to-container mount mappings,
- resolved environment values,
- where runtime artifacts will live,
- whether the planner expects a cache hit or miss,
- whether a prepared image will rebuild on every submit because `prepare.mounts` are present.

`inspect` is the quickest way to confirm that the planner understood your spec the way you intended.
`inspect --verbose` is a debugging-oriented view and can print secrets from resolved environment values.

## 5. Normal run: use up

```bash
hpc-compose up -f compose.yaml
```

`up` is the preferred end-to-end flow. It:

1. run preflight unless `--no-preflight` is set,
2. prepare images unless `--skip-prepare` is set,
3. render the script,
4. call `sbatch`,
5. records the tracked job metadata under `.hpc-compose/`,
6. polls scheduler state with `squeue` / `sacct` when available,
7. streams tracked service logs as they appear.

On an interactive TTY, `up` launches the full-screen watch UI with a per-service table on the left and the selected service log on the right. In non-interactive contexts it keeps the line-oriented follower so scripts and tests still get stable plain text. `submit --watch` remains available as a compatibility path to the same watch behavior.

<div class="callout note">
  <p><strong>Note</strong></p>
  <p><code>up</code> and <code>submit</code> treat preflight warnings as non-fatal. If you want warnings to block submission, run <code>preflight --strict</code> separately first.</p>
</div>

Useful options:

- `--local` runs the plan on the current Linux host through Enroot instead of calling `sbatch`.
- `--resume-diff-only` prints the resume-sensitive config diff without submitting.
- `--allow-resume-changes` confirms that you intend to change resume-coupled config between tracked runs.
- `--script-out path/to/job.sbatch` keeps a copy of the rendered script.
- When `--script-out` is omitted, the script is written to `<compose-file-dir>/hpc-compose.sbatch`.
- `--force-rebuild` refreshes imported and prepared artifacts during submission.
- `--skip-prepare` reuses existing prepared artifacts.
- `--keep-failed-prep` keeps the Enroot rootfs around when a prepare step fails.

For the shipped examples, `up` is usually the only command you need in the normal run. Use the other commands when you need more visibility into planning, environment checks, image preparation, tracked job state, or the generated script.

## 6. Run preflight checks when you need to debug cluster readiness

```bash
hpc-compose preflight -f compose.yaml
hpc-compose preflight --verbose -f compose.yaml
```

`preflight` checks:

- required binaries for the selected backend plus `srun` and `sbatch`,
- `scontrol` when `x-slurm.nodes > 1`,
- Pyxis container support in `srun` when `runtime.backend: pyxis`,
- generated cluster profile compatibility when `.hpc-compose/cluster.toml` is present,
- cache directory policy and writability,
- cache directory under `$HOME` warning (shared storage is safer on real clusters),
- local mount and image paths (`.sqsh` for Pyxis, `.sif` for Apptainer/Singularity),
- registry credentials,
- skip-prepare reuse safety when relevant,
- `nvidia-smi` availability when the `gpu` metrics collector is enabled,
- `sstat` availability when the `slurm` metrics collector is enabled,
- `curl` availability when any service uses `readiness.type: http`,
- multi-node service readiness does not rely on localhost,
- resume path does not use a node-local temporary directory (`/tmp`, `/var/tmp`),
- HAICORE/Pyxis helper mount paths (task prolog and shared libraries) when present.

Generate a cluster capability profile on the target login node when you want preflight and validate to catch partition/backend/QOS/GPU/MPI mismatches before submission:

```bash
hpc-compose doctor --cluster-report
```

This writes `.hpc-compose/cluster.toml` by default using `sinfo`, `scontrol`, `srun --mpi=list`, runtime binary probes, and shared-path environment hints.

If your cluster installs these tools in non-standard locations, pass explicit paths:

```bash
hpc-compose preflight -f compose.yaml \
  --enroot-bin /opt/enroot/bin/enroot \
  --apptainer-bin /opt/apptainer/bin/apptainer \
  --srun-bin /usr/local/bin/srun \
  --sbatch-bin /usr/local/bin/sbatch
```

The same runtime and Slurm override flags are available on `prepare`, `up`, `submit`, and `run` where relevant.

Use strict mode if you want warnings to fail the command:

```bash
hpc-compose preflight -f compose.yaml --strict
```

## 7. Prepare images on the login node when needed

```bash
hpc-compose prepare -f compose.yaml
```

Use this when you want to:

- build or refresh prepared images before submission,
- confirm cache reuse behavior,
- debug preparation separately from job submission.

Force a refresh of imported and prepared artifacts:

```bash
hpc-compose prepare -f compose.yaml --force
```

## 8. Render the batch script when you need to inspect it

```bash
hpc-compose render -f compose.yaml --output /tmp/job.sbatch
```

This is useful when:

- debugging generated `srun` arguments,
- checking mounts and environment passing,
- reviewing the launch order and readiness waits.

## 9. Read logs and submission output

After a successful submit, `hpc-compose` prints:

- the rendered script path,
- the cache directory,
- one log path per service,
- the tracked metadata location when a numeric Slurm job id was returned.

### Tracked state and logs

Use the tracked helpers for later inspection:

```bash
hpc-compose status -f compose.yaml
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
hpc-compose watch -f compose.yaml --service app
hpc-compose stats -f compose.yaml
hpc-compose stats -f compose.yaml --format csv
hpc-compose stats -f compose.yaml --format jsonl
hpc-compose artifacts -f compose.yaml
hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
hpc-compose cancel -f compose.yaml
hpc-compose logs -f compose.yaml
hpc-compose logs -f compose.yaml --service app --follow
```

`status` also reports the tracked top-level batch log path so early job failures are visible even when a service log was never created. When `services.<name>.x-slurm.failure_policy` is used, `status` includes per-service policy state (`failure_policy`, restart counters, rolling-window budget as `window=<current>/<max>@<seconds>s`, last exit code, and completed-successfully state) from tracked runtime state.

`ps` is the stable per-service snapshot view. It reports the tracked step name, launcher PID, readiness state, derived service status (`starting`, `ready`, `running`, `exited`, `failed`, or `unknown`), restart counters, last exit code, and tracked log path. Use `ps --format json` when tooling needs those fields directly.

`watch` reconnects to the latest tracked run without resubmitting. The live TUI refreshes scheduler state plus tracked logs and supports `q` to quit, `j` / `k` or the arrow keys to change services, `g` / `G` to jump to the first or last service, and `Tab` to cycle focus if needed.

Example:

```text
state service 'worker': failure_policy=restart_on_failure restarts=2/5 window=2/3@60s last_exit=42
```

Read that line as:

- the service has already been relaunched twice
- it can still use three more lifetime restarts before hitting `max_restarts: 5`
- two restart-triggering failures are still inside the current 60-second rolling window
- one more restart-triggering failure inside that same window would exhaust `max_restarts_in_window: 3`

If you need the machine-readable form, `status --format json` exposes the same policy state per service through `failure_policy_mode`, `restart_count`, `max_restarts`, `window_seconds`, `max_restarts_in_window`, `restart_failures_in_window`, `last_exit_code`, and `completed_successfully`.

For multi-node jobs, `status` also reports tracked placement geometry (`placement_mode`, nodes, task counts, and expanded nodelist) for each service.

### Metrics output

`stats` now prefers sampler data from `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/metrics` when `x-slurm.metrics` is enabled. In v1 that sampler can collect:

- GPU snapshots and compute-process rows through `nvidia-smi`
- job-step CPU and memory snapshots through `sstat`

If the sampler is absent, disabled, or only partially available, `stats` falls back to live `sstat`. It works best for running jobs, requires the cluster's `jobacct_gather` plugin to be enabled for Slurm-side step metrics, and only shows GPU accounting fields from Slurm when the cluster exposes GPU TRES accounting.

In multi-node v1, GPU sampler collection remains primary-node-only. Slurm step metrics still cover the whole step through `sstat`, but `nvidia-smi` fan-in across nodes is intentionally out of scope.

Use `--format json`, `--format csv`, or `--format jsonl` when you want machine-friendly output for dashboards, plotting, or experiment tracking. `--format json` is the preferred interface for `validate`, `render`, `prepare`, `preflight`, `inspect`, `status`, `stats`, `artifacts`, `cache`, and `context`. `--json` remains supported only as a compatibility alias on older machine-readable commands.

Runtime logs live under:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/logs/<service>.log
```

That same per-job directory is also mounted inside every container at `/hpc-compose/job`. Use it for small cross-service coordination files when a workflow needs shared ephemeral state.

When metrics sampling is enabled, the job also writes:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/metrics/
  meta.json
  gpu.jsonl
  gpu_processes.jsonl
  slurm.jsonl
```

Collector failures are best-effort: missing `nvidia-smi`, missing `sstat`, or unsupported queries do not fail the batch job itself.

### Artifact export

When `x-slurm.artifacts` is enabled, teardown collection writes:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/artifacts/
  manifest.json
  payload/...
```

Use `hpc-compose artifacts -f compose.yaml` after the job finishes to copy the collected payload into the configured `x-slurm.artifacts.export_dir`. The export path is resolved relative to the compose file and expands `${SLURM_JOB_ID}` from tracked metadata.

If the compose file defines named bundles under `x-slurm.artifacts.bundles`, `hpc-compose artifacts --bundle <name>` exports only the selected bundle(s). Named bundles are written under `<export_dir>/bundles/<bundle>/`, and every export writes provenance JSON under `<export_dir>/_hpc-compose/bundles/<bundle>.json`. Add `--tarball` to also create `<bundle>.tar.gz` archives during export. The bundle name `default` is reserved for top-level `x-slurm.artifacts.paths`.

Slurm may also write a top-level batch log such as `slurm-<jobid>.out`, or to the path configured with `x-slurm.output`. Check that file first when the job fails before any service log appears.

Service names containing non-alphanumeric characters are encoded in the log filename. For example, a service named `my.app` produces `my_x2e_app.log`. Prefer `[a-zA-Z0-9_-]` in service names for readability.

If you used `--script-out`, keep that script with the job logs when debugging cluster behavior.

### Resume-aware runs

When `x-slurm.resume` is enabled, `hpc-compose` also:

- mounts the shared resume path into every service at `/hpc-compose/resume`,
- injects `HPC_COMPOSE_RESUME_DIR`, `HPC_COMPOSE_ATTEMPT`, and `HPC_COMPOSE_IS_RESUME`,
- writes attempt-specific runtime outputs under `.hpc-compose/<jobid>/attempts/<attempt>/`,
- keeps `.hpc-compose/<jobid>/{logs,metrics,artifacts,state.json}` pointed at the latest attempt for compatibility.

Use the shared resume directory for the canonical checkpoint a restarted run should load next. Treat exported artifacts as retrieval and provenance output after the attempt finishes, not as the primary live resume source.

## 10. Inspect and prune cache artifacts

List cached artifacts:

```bash
hpc-compose cache list
```

Inspect cache state for the current plan:

```bash
hpc-compose cache inspect -f compose.yaml
```

Inspect a single service:

```bash
hpc-compose cache inspect -f compose.yaml --service app
```

Prune old entries by age (in days):

```bash
hpc-compose --profile dev cache prune --age 14
```

Prune artifacts not referenced by the current plan:

```bash
hpc-compose cache prune --all-unused -f compose.yaml
```

Prune one cache directory directly without loading a compose plan:

```bash
hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>'
```

The two strategies (`--age` and `--all-unused`) are mutually exclusive — pick one per invocation.

When `--age` runs without `--cache-dir`, `hpc-compose` resolves the cache directory from the active context first and only falls back to the default cache path when no compose file is available. Passing `--cache-dir` makes age-based pruning context-free.

Use `cache inspect` when you need to answer questions such as:

- which artifact is being reused,
- whether a prepared image came from a cached manifest,
- whether a service rebuilds on every submit because of prepare mounts.

### After upgrading hpc-compose

Cache keys include the tool version, so upgrading `hpc-compose` invalidates all existing cached artifacts. You will see a full rebuild on the next `prepare`, `up`, or `submit`. To clean up orphaned artifacts after an upgrade:

```bash
hpc-compose cache prune --age 0
```

## What changed and what should I run?

| If you changed... | Typical next step |
| --- | --- |
| YAML planning/runtime settings only | `hpc-compose validate -f compose.yaml`, `hpc-compose inspect --verbose -f compose.yaml`, then `hpc-compose up -f compose.yaml` |
| The base image, `x-enroot.prepare.commands`, or prepare env | `hpc-compose up --force-rebuild -f compose.yaml` for the normal run, or `hpc-compose prepare --force -f compose.yaml` when debugging prepare separately |
| Only mounted runtime source such as app code under `volumes` | Usually just `hpc-compose up -f compose.yaml` |
| Cache entries you no longer want and this plan does not reference | `hpc-compose cache prune --all-unused -f compose.yaml` |
| `hpc-compose` itself | Expect cache misses on the next `prepare`, `up`, or `submit`, then optionally prune old entries |

## Decision guide

### When should I use `volumes`?

Use `volumes` for source code or other files you edit frequently.

### When should I use `x-enroot.prepare.commands`?

Use prepare commands for slower-changing dependencies, tools, or image customization that you want baked into a cached runtime image.

### When should I use `--skip-prepare`?

Only when the prepared artifact already exists and you want to reuse it. `preflight` can warn or fail if reuse is unsafe.

### When should I use `--force-rebuild` or `prepare --force`?

Use them after changing:

- the base image,
- prepare commands,
- prepare environment,
- tooling or dependencies that should invalidate the cached runtime image.

### When should I manually run `enroot remove`?

Treat manual `enroot remove` as a rare last resort.

Use it only when Enroot state is clearly broken or inconsistent and `hpc-compose prepare --force` plus cache pruning did not fix the problem. In the normal rebuild or refresh path, prefer `up --force-rebuild`, `prepare --force`, and `cache prune` so `hpc-compose` stays in charge of artifact state.

### Why does my service rebuild every time?

If `x-enroot.prepare.mounts` is non-empty, that service intentionally rebuilds on every `prepare` / `submit`.

## Troubleshooting

### `required binary '...' was not found`

Run on a node with the Slurm client tools and Enroot available, or pass the explicit binary path with `--enroot-bin`, `--srun-bin`, or `--sbatch-bin`.

### `srun does not advertise --container-image`

Pyxis support appears unavailable on that node. Move to a supported login node or cluster environment.

### Cache directory errors or warnings

- Errors usually mean the path is not shared or not writable.
- A warning under `$HOME` means the path may work on some clusters, but a shared workspace or project path is safer because prepare happens on the login node and runtime happens on compute nodes.

### Missing local mount or image paths

Remember that relative paths resolve from the compose file directory, not from the shell's current working directory.

### A mounted file exists on the host but not inside the container

This is often a symlink issue. If you mount a directory such as `$HOME/models:/models` and `model.gguf` is a symlink whose target lives outside `$HOME/models`, the target may not be visible inside the container. Copy the real file into the mounted directory or mount the directory that contains the symlink target.

<div class="callout warning">
  <p><strong>Warning</strong></p>
  <p>The mount itself can succeed while the symlink target is still invisible inside the container. Check the target path, not just the link path.</p>
</div>

### Anonymous pull or registry credential warnings

Add the required credentials before relying on private registries or heavily rate-limited public registries.

### Services start in the wrong order

Use `depends_on` with `condition: service_healthy` when a dependent must wait for a dependency's readiness probe. Plain list form still means `service_started`.

Use `condition: service_completed_successfully` for one-shot DAG stages where the next service should start only after the previous stage exits with status `0`, such as preprocess -> train -> postprocess.

When a TCP port opens before the service is fully usable, prefer HTTP or log-based readiness over TCP readiness.

### Preview a submission without running sbatch

Use `submit --dry-run` to run the full pipeline (preflight, prepare, render) without actually calling `sbatch`. The rendered script is written to disk so you can inspect it:

```bash
hpc-compose submit --dry-run -f compose.yaml
```

Combine with `--skip-prepare` for a pure validation-and-render dry run.

### Inspect tracked jobs across the repo tree

Use `jobs list` when you want to rediscover tracked runs before jumping into one compose context:

```bash
hpc-compose jobs list
hpc-compose jobs list --disk-usage
hpc-compose jobs list --format json
```

`jobs list` scans from the nearest git root, or the current directory when no git root exists. It reports tracked submissions even when a runtime directory has already been removed or `latest.json` is stale.

### Clean up old job directories

Tracked job metadata and logs accumulate in `.hpc-compose/`. Use `clean` to remove old entries:

```bash
# Remove jobs older than 7 days
hpc-compose clean -f compose.yaml --age 7

# Remove all except the latest tracked job
hpc-compose clean -f compose.yaml --all

# Preview cleanup without deleting files
hpc-compose clean -f compose.yaml --age 7 --dry-run

# Produce machine-readable cleanup output
hpc-compose clean -f compose.yaml --all --format json
```

`clean` stays compose-scoped even though `jobs list` scans the repo tree. Use `context` when you need to confirm the difference between the compose directory, which resolves spec-relative paths, and the current submit directory, which anchors new runtime job state under `.hpc-compose/<job-id>`. JSON cleanup output reports effective latest job IDs for automation and also includes `latest_pointer_job_id_before` when a parseable `latest.json` pointer existed before repair.

### Shell completions

Generate completions for your shell and source them:

```bash
# bash
hpc-compose completions bash > ~/.local/share/bash-completion/completions/hpc-compose

# zsh
hpc-compose completions zsh > ~/.zfunc/_hpc-compose

# fish
hpc-compose completions fish > ~/.config/fish/completions/hpc-compose.fish
```

## Related Docs

- [CLI Reference](cli-reference.md)
- [Spec Reference](spec-reference.md)
- [Docker Compose Migration](docker-compose-migration.md)
- [Examples](examples.md)
