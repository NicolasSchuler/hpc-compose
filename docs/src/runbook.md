# Runbook

This runbook is for adapting `hpc-compose` to a real workload on a Slurm cluster with Enroot and Pyxis.

Commands below assume `hpc-compose` is on your `PATH`. If you are running from a local checkout, replace `hpc-compose` with `target/release/hpc-compose`.

All commands accept `-f` / `--file` to specify the compose spec path. When omitted, it defaults to `compose.yaml` in the current directory. (The `cache prune --all-unused` subcommand requires `-f` explicitly.)

Read the [Execution model](execution-model.md) page first if you are still orienting on login-node prepare, compute-node runtime, shared cache paths, or localhost networking.

## Before you start

Make sure you have:

- a login node with `enroot`, `srun`, and `sbatch` available,
- `scontrol` available when you request `x-slurm.nodes > 1`,
- Pyxis support in `srun` (`srun --help` should mention `--container-image`),
- a shared filesystem path for `x-slurm.cache_dir`,
- any required local source trees or local `.sqsh` images in place,
- registry credentials available if your cluster or registry requires them.

## Command cadence

| Command or step | When to use it |
| --- | --- |
| install or build `hpc-compose` | once per checkout or upgrade |
| `init` or copy a shipped example | once per new spec |
| `validate` and `inspect` | early while adapting a spec |
| `submit --watch` | normal run |
| `preflight`, `prepare`, `render` | first-time cluster setup checks or the debugging flow |

## Normal progression

For a new spec on a real cluster:

1. Run `hpc-compose init --template <name> --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml`, or copy the closest shipped example.
2. Set `x-slurm.cache_dir` if you need an explicit shared cache path, and adjust any cluster-specific resource settings.
3. Run `hpc-compose validate -f compose.yaml` and `hpc-compose inspect --verbose -f compose.yaml` while you are still adapting the file.
4. Run `hpc-compose submit --watch -f compose.yaml` for the normal run.
5. If that fails, or if you need more visibility, break out `preflight`, `prepare`, `render`, `status`, `stats`, or `logs` separately.

## Pick a starting example

| Example | Use it when you need | File |
| --- | --- | --- |
| Dev app | mounted source tree plus a small prepare step | [`examples/dev-python-app.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/dev-python-app.yaml) |
| Redis worker stack | multi-service launch ordering and readiness checks | [`examples/app-redis-worker.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/app-redis-worker.yaml) |
| LLM curl workflow | one GPU-backed LLM plus a one-shot `curl` request from a second service | [`examples/llm-curl-workflow.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llm-curl-workflow.yaml) |
| LLM curl workflow (home) | the same request flow, but anchored under `$HOME/models` for direct use on a login node | [`examples/llm-curl-workflow-workdir.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llm-curl-workflow-workdir.yaml) |
| GPU-backed app | one GPU service plus a dependent application | [`examples/llama-app.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llama-app.yaml) |
| llama.cpp + uv worker | llama.cpp serving plus a source-mounted Python worker run through `uv` | [`examples/llama-uv-worker.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llama-uv-worker.yaml) |
| Minimal batch | simplest single-service batch job | [`examples/minimal-batch.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/minimal-batch.yaml) |
| Multi-node MPI | one helper on the primary node plus one allocation-wide distributed step | [`examples/multi-node-mpi.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-node-mpi.yaml) |
| Multi-node torchrun | allocation-wide GPU training with the primary node as rendezvous | [`examples/multi-node-torchrun.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-node-torchrun.yaml) |
| Training checkpoints | GPU training with checkpoints to shared storage | [`examples/training-checkpoints.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/training-checkpoints.yaml) |
| Training resume | GPU training with a shared resume directory and attempt-aware checkpoints | [`examples/training-resume.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/training-resume.yaml) |
| Postgres ETL | PostgreSQL plus a Python data processing job | [`examples/postgres-etl.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/postgres-etl.yaml) |
| vLLM serving | vLLM with an in-job Python client | [`examples/vllm-openai.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/vllm-openai.yaml) |
| vLLM + uv worker | vLLM serving with a source-mounted Python worker run through `uv` | [`examples/vllm-uv-worker.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/vllm-uv-worker.yaml) |
| MPI hello | MPI hello world with Open MPI | [`examples/mpi-hello.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/mpi-hello.yaml) |
| Multi-stage pipeline | two-stage pipeline with file-based handoff | [`examples/multi-stage-pipeline.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-stage-pipeline.yaml) |
| Data preprocessing | CPU-heavy NLP preprocessing pipeline | [`examples/fairseq-preprocess.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/fairseq-preprocess.yaml) |

The fastest path is usually to copy the closest example and adapt it instead of starting from scratch.

You can also let `hpc-compose` scaffold one of these examples directly:

```bash
hpc-compose init --template dev-python-app --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml
```

## 1. Choose `x-slurm.cache_dir` early

Set `x-slurm.cache_dir` to a path that is visible from both the login node and the compute nodes.

```yaml
x-slurm:
  cache_dir: /shared/$USER/hpc-compose-cache
```

Rules:

- Do **not** use `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm`.
- If you leave `cache_dir` unset, the default is `$HOME/.cache/hpc-compose`.
- The default is convenient for small or home-directory workflows, but a shared project or workspace path is usually safer on real clusters.
- The important constraint is visibility: `prepare` runs on the login node, but the batch job later reuses those cached artifacts from compute nodes.

## 2. Adapt the example to your workload

Start with the nearest example and then change:

- `image`
- `command` / `entrypoint`
- `volumes`
- `environment`
- `x-slurm` resource settings
- `x-enroot.prepare` commands for dependencies or tooling

Recommended pattern:

- Put fast-changing application code in `volumes`.
- Put slower-changing dependency installation in `x-enroot.prepare.commands`.
- Add `readiness` to any service that other services truly depend on.

## 3. Validate the spec

```bash
hpc-compose validate -f compose.yaml
```

Use `validate` first when you are changing:

- field names,
- `depends_on` shape,
- `command` / `entrypoint` form,
- path values,
- `x-slurm` / `x-enroot` blocks.

If `validate` fails, fix that before doing anything more expensive.

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

## 5. Normal run: submit the job and watch it

```bash
hpc-compose submit --watch -f compose.yaml
```

`submit` does the normal end-to-end flow:

1. run preflight unless `--no-preflight` is set,
2. prepare images unless `--skip-prepare` is set,
3. render the script,
4. call `sbatch`.

With `--watch`, `submit` also:

5. records the tracked job metadata under `.hpc-compose/`,
6. polls scheduler state with `squeue` / `sacct` when available,
7. streams tracked service logs as they appear.

<div class="callout note">
  <p><strong>Note</strong></p>
  <p><code>submit</code> treats preflight warnings as non-fatal. If you want warnings to block submission, run <code>preflight --strict</code> separately before <code>submit</code>.</p>
</div>

Useful options:

- `--script-out path/to/job.sbatch` keeps a copy of the rendered script.
- When `--script-out` is omitted, the script is written to `<compose-file-dir>/hpc-compose.sbatch`.
- `--force-rebuild` refreshes imported and prepared artifacts during submit.
- `--skip-prepare` reuses existing prepared artifacts.
- `--keep-failed-prep` keeps the Enroot rootfs around when a prepare step fails.

For the shipped examples, `submit --watch` is usually the only command you need in the normal run. Use the other commands when you need more visibility into planning, environment checks, image preparation, tracked job state, or the generated script.

## 6. Run preflight checks when you need to debug cluster readiness

```bash
hpc-compose preflight -f compose.yaml
hpc-compose preflight --verbose -f compose.yaml
```

`preflight` checks:

- required binaries (`enroot`, `srun`, `sbatch`),
- `scontrol` when `x-slurm.nodes > 1`,
- Pyxis container support in `srun`,
- cache directory policy and writability,
- local mount and image paths,
- registry credentials,
- skip-prepare reuse safety when relevant.

If your cluster installs these tools in non-standard locations, pass explicit paths:

```bash
hpc-compose preflight -f compose.yaml --enroot-bin /opt/enroot/bin/enroot --srun-bin /usr/local/bin/srun --sbatch-bin /usr/local/bin/sbatch
```

The same override flags (`--enroot-bin`, `--srun-bin`, `--sbatch-bin`) are available on `prepare` and `submit`.

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
- one log path per service.
- the tracked metadata location when a numeric Slurm job id was returned.

Use the tracked helpers for later inspection:

```bash
hpc-compose status -f compose.yaml
hpc-compose stats -f compose.yaml
hpc-compose stats -f compose.yaml --format csv
hpc-compose stats -f compose.yaml --format jsonl
hpc-compose artifacts -f compose.yaml
hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
hpc-compose cancel -f compose.yaml
hpc-compose logs -f compose.yaml
hpc-compose logs -f compose.yaml --service app --follow
```

`status` also reports the tracked top-level batch log path so early job failures are visible even when a service log was never created. When `services.<name>.x-slurm.failure_policy` is used, `status` includes per-service policy state (`failure_policy`, restart counters, and last exit code) from tracked runtime state.

For multi-node jobs, `status` also reports tracked placement geometry (`placement_mode`, nodes, task counts, and expanded nodelist) for each service.

`stats` now prefers sampler data from `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/metrics` when `x-slurm.metrics` is enabled. In v1 that sampler can collect:

- GPU snapshots and compute-process rows through `nvidia-smi`
- job-step CPU and memory snapshots through `sstat`

If the sampler is absent, disabled, or only partially available, `stats` falls back to live `sstat`. It works best for running jobs, requires the cluster's `jobacct_gather` plugin to be enabled for Slurm-side step metrics, and only shows GPU accounting fields from Slurm when the cluster exposes GPU TRES accounting.

In multi-node v1, GPU sampler collection remains primary-node-only. Slurm step metrics still cover the whole step through `sstat`, but `nvidia-smi` fan-in across nodes is intentionally out of scope.

Use `--format json`, `--format csv`, or `--format jsonl` when you want machine-friendly output for dashboards, plotting, or experiment tracking. `--format json` is the preferred interface for `validate`, `render`, `prepare`, `preflight`, `inspect`, `status`, `stats`, `artifacts`, and `cache` subcommands. `--json` remains supported as a compatibility alias on older machine-readable commands.

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
hpc-compose cache prune --age 14
```

Prune artifacts not referenced by the current plan:

```bash
hpc-compose cache prune --all-unused -f compose.yaml
```

The two strategies (`--age` and `--all-unused`) are mutually exclusive — pick one per invocation.

Use `cache inspect` when you need to answer questions such as:

- which artifact is being reused,
- whether a prepared image came from a cached manifest,
- whether a service rebuilds on every submit because of prepare mounts.

### After upgrading hpc-compose

Cache keys include the tool version, so upgrading `hpc-compose` invalidates all existing cached artifacts. You will see a full rebuild on the next `prepare` or `submit`. To clean up orphaned artifacts after an upgrade:

```bash
hpc-compose cache prune --age 0
```

## What changed and what should I run?

| If you changed... | Typical next step |
| --- | --- |
| YAML planning/runtime settings only | `hpc-compose validate -f compose.yaml`, `hpc-compose inspect --verbose -f compose.yaml`, then `hpc-compose submit --watch -f compose.yaml` |
| The base image, `x-enroot.prepare.commands`, or prepare env | `hpc-compose submit --watch --force-rebuild -f compose.yaml` for the normal run, or `hpc-compose prepare --force -f compose.yaml` when debugging prepare separately |
| Only mounted runtime source such as app code under `volumes` | Usually just `hpc-compose submit --watch -f compose.yaml` |
| Cache entries you no longer want and this plan does not reference | `hpc-compose cache prune --all-unused -f compose.yaml` |
| `hpc-compose` itself | Expect cache misses on the next `prepare` or `submit`, then optionally prune old entries |

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

Use it only when Enroot state is clearly broken or inconsistent and `hpc-compose prepare --force` plus cache pruning did not fix the problem. In the normal rebuild or refresh path, prefer `submit --force-rebuild`, `prepare --force`, and `cache prune` so `hpc-compose` stays in charge of artifact state.

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

When a TCP port opens before the service is fully usable, prefer HTTP or log-based readiness over TCP readiness.

### Preview a submission without running sbatch

Use `submit --dry-run` to run the full pipeline (preflight, prepare, render) without actually calling `sbatch`. The rendered script is written to disk so you can inspect it:

```bash
hpc-compose submit --dry-run -f compose.yaml
```

Combine with `--skip-prepare` for a pure validation-and-render dry run.

### Clean up old job directories

Tracked job metadata and logs accumulate in `.hpc-compose/`. Use `clean` to remove old entries:

```bash
# Remove jobs older than 7 days
hpc-compose clean -f compose.yaml --age 7

# Remove all except the latest tracked job
hpc-compose clean -f compose.yaml --all
```

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

## Related docs

- [Spec reference](spec-reference.md)
- [Docker Compose migration](docker-compose-migration.md)
- [Examples](examples.md)
