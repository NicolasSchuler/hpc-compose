# Spec reference

This page describes the Compose subset that `hpc-compose` accepts today. Unknown or unsupported fields are rejected unless this page explicitly says otherwise.

## How To Use This Reference

This page is intentionally complete. If you are new, start with [Quickstart](quickstart.md), [Examples](examples.md), and [Runtime Backends](runtime-backends.md), then use the table below to jump into the field group you need.

| Need | Section |
| --- | --- |
| Overall YAML shape | [Top-level shape](#top-level-shape) and [Top-level fields](#top-level-fields) |
| Runtime backend choice | [`runtime`](#runtime) and [Runtime Backends](runtime-backends.md) |
| Slurm allocation settings | [`x-slurm`](#x-slurm) |
| Service command, image, env, and mounts | [Service fields](#service-fields), [Image rules](#image-rules), [`command` and `entrypoint`](#command-and-entrypoint), [`environment`](#environment), [`volumes`](#volumes) |
| Startup ordering | [`depends_on`](#depends_on), [`readiness`](#readiness), and [`healthcheck`](#healthcheck) |
| Multi-node placement and MPI | [Multi-node placement rules](#multi-node-placement-rules), [`services.<name>.x-slurm.placement`](#servicesnamex-slurmplacement), and [`services.<name>.x-slurm.mpi`](#servicesnamex-slurmmpi) |
| Prepared images | [`x-runtime.prepare` and `x-enroot.prepare`](#x-runtimeprepare-and-x-enrootprepare) |
| Metrics, artifacts, and resume | [`x-slurm.metrics`](#x-slurmmetrics), [`x-slurm.artifacts`](#x-slurmartifacts), and [`x-slurm.resume`](#x-slurmresume) |
| Unsupported Compose features | [Unsupported Compose keys](#unsupported-compose-keys) |

## Top-level shape

```yaml
name: demo
version: "3.9"

runtime:
  backend: pyxis

x-slurm:
  time: "00:30:00"
  cache_dir: /cluster/shared/hpc-compose-cache

services:
  app:
    image: python:3.11-slim
    command: python -m main
```

## Top-level fields

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `name` | string | omitted | Used as the Slurm job name when `x-slurm.job_name` is not set. |
| `version` | string | omitted | Accepted for Compose compatibility. Ignored by the planner. |
| `runtime` | mapping | `backend: pyxis` | Selects the service runtime backend and GPU passthrough policy. |
| `services` | mapping | required | Must contain at least one service. |
| `x-env` | mapping | omitted | Structured host-side module, Spack view, and environment setup shared by all services. |
| `x-slurm` | mapping | omitted | Top-level Slurm settings and shared runtime defaults. |

## `x-env`

`x-env` is structured host-side software setup. It is available at the top level and under `services.<name>`.

```yaml
x-env:
  modules:
    - cuda/12.4
    - openmpi/5
  spack:
    view: /shared/spack/views/ml
  env:
    HDF5_USE_FILE_LOCKING: "FALSE"

services:
  app:
    image: python:3.11-slim
    x-env:
      modules:
        purge: false
        load:
          - netcdf/4.9
      env:
        OMP_NUM_THREADS: "8"
```

Supported forms:

- `modules: [name, ...]`
- `modules: { purge: bool, load: [name, ...] }`
- `spack: { view: /path/to/view }`
- `env: { KEY: VALUE }`

Rules:

- Top-level `x-env` renders before `x-slurm.setup`.
- Service-level `x-env` renders immediately before that service's `srun`.
- `env` entries are exported on the host and forwarded into Pyxis containers.
- Service-level `x-env.env` overrides top-level `x-env.env` when the same variable is set.
- `spack.view` prepends `bin`, `lib`, `lib64`, and Python site-package paths only when those directories exist.
- Modules and Spack views are host-side setup. Container filesystem visibility still requires explicit `volumes`, `x-slurm.mpi.host_mpi.bind_paths`, or other site-specific binds.

## Settings-aware command table

Use these commands and global flags when you want the project-local settings file (`.hpc-compose/settings.toml`) to remember compose path, env files, env vars, and binary overrides.

| Command or flag | Purpose | Notes |
| --- | --- | --- |
| `--profile <NAME>` | Select the profile from settings | Global flag; applies to every subcommand. |
| `--settings-file <PATH>` | Use an explicit settings file | Global flag; bypasses upward auto-discovery of `.hpc-compose/settings.toml`. |
| `hpc-compose setup` | Create or update the project-local settings file | Interactive by default; supports `--non-interactive` with `--profile-name`, `--compose-file`, `--env-file`, `--env`, `--binary`, and `--default-profile`. |
| `hpc-compose context` | Print fully resolved execution context | Shows selected settings/profile, compose path, binaries, interpolation vars, runtime paths, and value sources; supports `--format json`. |
| `hpc-compose validate --strict-env` | Fail when interpolation fell back to defaults | Detects when `${VAR:-...}` or `${VAR-...}` consumed fallback values because `VAR` was missing. |
| `hpc-compose schema` | Print the checked-in JSON Schema | Useful for editor integration and authoring tools. Rust validation remains the semantic source of truth. |

## `x-slurm`

These fields live under the top-level `x-slurm` block.

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `job_name` | string | `name` when present | Rendered as `#SBATCH --job-name`. |
| `partition` | string | omitted | Passed through to `#SBATCH --partition`. |
| `account` | string | omitted | Passed through to `#SBATCH --account`. |
| `qos` | string | omitted | Passed through to `#SBATCH --qos`. |
| `time` | string | omitted | Passed through to `#SBATCH --time`. |
| `nodes` | integer | omitted | Slurm allocation node count. Defaults to `1` when omitted. |
| `ntasks` | integer | omitted | Passed through to `#SBATCH --ntasks`. |
| `ntasks_per_node` | integer | omitted | Passed through to `#SBATCH --ntasks-per-node`. |
| `cpus_per_task` | integer | omitted | Top-level Slurm CPU request. |
| `mem` | string | omitted | Passed through to `#SBATCH --mem`. |
| `gres` | string | omitted | Passed through to `#SBATCH --gres`. |
| `gpus` | integer | omitted | Used only when `gres` is not set. |
| `gpus_per_node` | integer | omitted | Passed through to `#SBATCH --gpus-per-node`. |
| `gpus_per_task` | integer | omitted | Passed through to `#SBATCH --gpus-per-task`. |
| `cpus_per_gpu` | integer | omitted | Passed through to `#SBATCH --cpus-per-gpu`. |
| `mem_per_gpu` | string | omitted | Passed through to `#SBATCH --mem-per-gpu`. |
| `gpu_bind` | string | omitted | Passed through to `#SBATCH --gpu-bind`. |
| `cpu_bind` | string | omitted | Passed through to `#SBATCH --cpu-bind`. |
| `mem_bind` | string | omitted | Passed through to `#SBATCH --mem-bind`. |
| `distribution` | string | omitted | Passed through to `#SBATCH --distribution`. |
| `hint` | string | omitted | Passed through to `#SBATCH --hint`. |
| `constraint` | string | omitted | Passed through to `#SBATCH --constraint`. |
| `output` | string | omitted | Passed through to `#SBATCH --output`. |
| `error` | string | omitted | Passed through to `#SBATCH --error`. |
| `chdir` | string | omitted | Passed through to `#SBATCH --chdir`. |
| `cache_dir` | string | `$HOME/.cache/hpc-compose` | Must resolve to shared storage visible from the login node and the compute nodes. |
| `scratch` | mapping | omitted | Optional scratch path mounted into services and exposed as `HPC_COMPOSE_SCRATCH_DIR`. |
| `stage_in` | list of mappings | omitted | Copy or rsync host paths before services launch. |
| `stage_out` | list of mappings | omitted | Copy or rsync paths during teardown, optionally by outcome. |
| `burst_buffer` | mapping | omitted | Raw `#BB` / `#DW` directives for site-specific burst-buffer systems. |
| `metrics` | mapping | omitted | Enables runtime metrics sampling. |
| `artifacts` | mapping | omitted | Enables tracked artifact collection and export metadata. |
| `resume` | mapping | omitted | Enables checkpoint-aware resume semantics with a shared host path mounted into every service. |
| `notify` | mapping | omitted | First-class Slurm email notification settings. |
| `setup` | list of strings | omitted | Raw shell lines inserted into the generated batch script before service launches. |
| `submit_args` | list of strings | omitted | Extra raw Slurm arguments appended as `#SBATCH ...` lines. |

### `x-slurm.setup`

```yaml
x-slurm:
  setup:
    - module load enroot
    - source /shared/env.sh
```

- Shape: list of strings
- Default: omitted
- Notes:
  - Each line is emitted verbatim into the generated bash script.
  - The script runs under `set -euo pipefail`.
  - Shell quoting and escaping are the user's responsibility.

### `x-slurm.submit_args`

```yaml
x-slurm:
  submit_args:
    - "--mail-type=END"
    - "--mail-user=user@example.com"
    - "--reservation=gpu-reservation"
```

- Shape: list of strings
- Default: omitted
- Notes:
  - Each entry is emitted as `#SBATCH {arg}`.
  - Entries are rejected if they contain line breaks or null bytes.
  - Entries are not validated against Slurm option syntax.

### `x-slurm.notify`

```yaml
x-slurm:
  notify:
    email:
      to: user@example.com
      on: [end, fail]
```

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `notify.email` | mapping | omitted | Required when `notify` is present. |
| `notify.email.to` | string | required | Rendered as `#SBATCH --mail-user`. |
| `notify.email.on` | list of events | `[end, fail]` | Rendered as `#SBATCH --mail-type`. |

Supported events:

| Event | Slurm mail type |
| --- | --- |
| `start` | `BEGIN` |
| `end` | `END` |
| `fail` | `FAIL` |
| `all` | `ALL` |

Rules:

- When `on` is omitted or empty, defaults to `[end, fail]`.
- If `all` is present, it replaces all other events.
- Cannot be combined with raw `--mail-type` or `--mail-user` in `x-slurm.submit_args`.

### `x-slurm.cache_dir`

- Shape: string
- Default: `$HOME/.cache/hpc-compose`
- Notes:
  - Relative paths and environment variables are resolved against the compose file directory.
  - Paths under `/tmp`, `/var/tmp`, `/private/tmp`, and `/dev/shm` are accepted by parsing and planning, but `preflight` reports them as unsafe because they are not valid shared-cache locations for login-node prepare plus compute-node reuse.
  - The path must be visible from both the login node and the compute nodes.

## `runtime`

```yaml
runtime:
  backend: apptainer
  gpu: auto
```

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `backend` | `pyxis`, `apptainer`, `singularity`, or `host` | `pyxis` | Selects the runtime used inside Slurm steps. |
| `gpu` | `auto`, `none`, or `nvidia` | `auto` | For Apptainer/Singularity, controls `--nv`; `auto` enables it when Slurm GPU resources are requested. |

Backend notes:

- `pyxis` uses `srun --container-*` flags and Enroot `.sqsh` artifacts.
- `apptainer` and `singularity` build or reuse `.sif` artifacts and launch them through `apptainer exec/run` or `singularity exec/run` inside `srun`.
- `host` runs commands directly under `srun`; services must set `command` or `entrypoint`, and image prepare blocks, service `volumes`, and `x-slurm.mpi.host_mpi.bind_paths` are not allowed because no container bind mount is applied.
- `x-enroot.prepare` is a Pyxis/Enroot compatibility spelling. Prefer `x-runtime.prepare` for new specs, especially with Apptainer/Singularity.

### `x-slurm.scratch`, `stage_in`, `stage_out`, and `burst_buffer`

```yaml
x-slurm:
  scratch:
    scope: shared
    base: /scratch/$USER/jobs
    mount: /scratch
    cleanup: on_success
  stage_in:
    - from: /shared/input
      to: /scratch/input
      mode: rsync
  stage_out:
    - from: /scratch/output
      to: /shared/results/${SLURM_JOB_ID}
      when: always
      mode: copy
  burst_buffer:
    directives:
      - "#BB create_persistent name=data capacity=100G"
```

- `scratch.base` is a host path. `scratch.mount` is the container-visible mount point.
- `scratch.scope` is `node_local` or `shared`; cluster profiles can warn when a shared scratch path does not look shared.
- `scratch.cleanup` is `always`, `on_success`, or `never`.
- `stage_in` runs before services launch; `stage_out` runs during teardown.
- `mode` is `rsync` or `copy`; `rsync` falls back to `cp -R` when `rsync` is unavailable.
- `stage_out.when` is `always`, `on_success`, or `on_failure`.
- `${SLURM_JOB_ID}` is preserved in scratch and staging paths for runtime expansion.
- `burst_buffer.directives` entries are emitted as raw batch-script directives and must start with `#BB ` or `#DW `.

### Multi-node placement rules

- `x-slurm.nodes > 1` reserves a multi-node allocation.
- Helper services remain single-node steps and are pinned to the allocation's primary node.
- When a multi-node job has exactly one service, that service defaults to the distributed full-allocation step.
- Services may use `services.<name>.x-slurm.placement` to select explicit allocation node indices.
- Overlapping explicit placements are rejected unless one side sets `allow_overlap: true` or uses `share_with`.
- Any service spanning more than one node may use `readiness.type: sleep` or `readiness.type: log`, or TCP/HTTP readiness only with an explicit non-local host or URL.

### `x-slurm.metrics`

```yaml
x-slurm:
  metrics:
    interval_seconds: 5
    collectors: [gpu, slurm]
```

- Shape: mapping
- Default: omitted
- Notes:
  - Omitting the block disables runtime metrics sampling.
  - If the block is present and `enabled` is omitted, metrics sampling is enabled.
  - `interval_seconds` defaults to `5` and must be at least `1`.
  - `collectors` defaults to `[gpu, slurm]`.
  - Supported collectors:
    - `gpu` samples device and process telemetry through `nvidia-smi`
    - `slurm` samples job-step CPU and memory data through `sstat`
  - In multi-node jobs, `gpu` sampling launches one best-effort sampler task per allocated node and writes node metadata into GPU rows; legacy rows without `node` remain readable as primary-node samples.
  - Sampler files are written under `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/metrics` on the host and are also visible inside containers at `/hpc-compose/job/metrics`.
  - Diagnostics are written under `metrics/diagnostics/` when available, including `nvidia-smi topo -m`, `nvidia-smi -q`, selected fabric/GPU environment variables, and best-effort `ibstat`, `ibv_devinfo`, `ucx_info -v`, and `fi_info` output.
  - Collector failures are best-effort and do not fail the batch job.

### `x-slurm.artifacts`

```yaml
x-slurm:
  artifacts:
    collect: always
    export_dir: ./results/${SLURM_JOB_ID}
    paths:
      - /hpc-compose/job/metrics/**
    bundles:
      checkpoints:
        paths:
          - /hpc-compose/job/checkpoints/*.pt
```

- Shape: mapping
- Default: omitted
- Notes:
  - Omitting the block disables tracked artifact collection.
  - `collect` defaults to `always`. Supported values are `always`, `on_success`, and `on_failure`.
  - `export_dir` is required and is resolved relative to the compose file directory when `hpc-compose artifacts` runs.
  - `${SLURM_JOB_ID}` is preserved in `export_dir` until `hpc-compose artifacts` expands it from tracked metadata.
  - `paths` remains supported as the implicit `default` bundle.
  - `bundles` is optional. Bundle names must match `[A-Za-z0-9_-]+`, and `default` is reserved for top-level `paths`.
  - At least one source path must be present in `paths` or `bundles`.
  - Every source path must be an absolute container-visible path rooted at `/hpc-compose/job`.
  - Paths under `/hpc-compose/job/artifacts` are rejected.
  - Collection happens during batch teardown and is best-effort.
  - Collected payloads and `manifest.json` are written under `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/artifacts/`.
  - `hpc-compose artifacts --bundle <name>` exports only the selected bundle or bundles.
  - `hpc-compose artifacts --tarball` also writes one `<bundle>.tar.gz` archive per exported bundle.
  - Export writes per-bundle provenance metadata under `<export_dir>/_hpc-compose/bundles/<bundle>.json`.

### `x-slurm.resume`

```yaml
x-slurm:
  resume:
    path: /shared/$USER/runs/my-run
```

- Shape: mapping
- Default: omitted
- Notes:
  - Omitting the block disables resume semantics.
  - `path` is required and must be an absolute host path.
  - `/hpc-compose/...` paths are rejected because `path` must point at shared host storage, not a container-visible path.
  - `/tmp` and `/var/tmp` technically validate, but `preflight` warns because those paths are not reliable resume storage.
  - When enabled, `hpc-compose` mounts `path` into every service at `/hpc-compose/resume`.
  - Services also receive `HPC_COMPOSE_RESUME_DIR`, `HPC_COMPOSE_ATTEMPT`, and `HPC_COMPOSE_IS_RESUME`.
  - The canonical resume source is the shared `path`, not exported artifact bundles.
  - Attempt-specific runtime state moves under `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/attempts/<attempt>/`, and the top-level `logs`, `metrics`, `artifacts`, and `state.json` paths continue to point at the latest attempt for compatibility.

### Allocation metadata inside services

Every service receives:

- `HPC_COMPOSE_PRIMARY_NODE`
- `HPC_COMPOSE_NODE_COUNT`
- `HPC_COMPOSE_NODELIST`
- `HPC_COMPOSE_NODELIST_FILE`
- `HPC_COMPOSE_SERVICE_PRIMARY_NODE`
- `HPC_COMPOSE_SERVICE_NODE_COUNT`
- `HPC_COMPOSE_SERVICE_NODELIST`
- `HPC_COMPOSE_SERVICE_NODELIST_FILE`

The allocation-wide data is also written under `/hpc-compose/job/allocation/primary_node` and `/hpc-compose/job/allocation/nodes.txt`. Service-scoped node lists are written under `/hpc-compose/job/allocation/service-nodelists/`.

Multi-node services also receive distributed launch helpers:

- `HPC_COMPOSE_DIST_MASTER_ADDR`
- `HPC_COMPOSE_DIST_MASTER_PORT`
- `HPC_COMPOSE_DIST_RDZV_ENDPOINT`
- `HPC_COMPOSE_DIST_NNODES`
- `HPC_COMPOSE_DIST_NODE_RANK`
- `HPC_COMPOSE_DIST_LOCAL_RANK`
- `HPC_COMPOSE_DIST_GLOBAL_RANK`
- `HPC_COMPOSE_DIST_NPROC_PER_NODE`
- `HPC_COMPOSE_DIST_WORLD_SIZE`
- `HPC_COMPOSE_DIST_HOSTFILE`

`HPC_COMPOSE_DIST_NPROC_PER_NODE` is derived from a service environment override, GPU requests, `ntasks_per_node`, then `1`. The distributed hostfile is written under `/hpc-compose/job/allocation/distributed-hostfiles/`. When a discovered `.hpc-compose/cluster.toml` contains `[distributed.env]`, those profile variables are injected only for multi-node services; explicit service `environment` values win on name conflicts and are still the durable config source.

Services that configure `services.<name>.x-slurm.mpi` also receive:

- `HPC_COMPOSE_MPI_TYPE`
- `HPC_COMPOSE_MPI_PROFILE` when `x-slurm.mpi.profile` is set
- `HPC_COMPOSE_MPI_IMPLEMENTATION` when `x-slurm.mpi.implementation` is set or implied by `x-slurm.mpi.profile`
- `HPC_COMPOSE_MPI_HOSTFILE`

The MPI hostfile is written under `/hpc-compose/job/allocation/mpi-hostfiles/` and contains the service's effective node list. When `ntasks_per_node` is known, each host line includes `slots=<ntasks_per_node>`. For a single-node service with `ntasks` but no `ntasks_per_node`, the hostfile uses `slots=<ntasks>`. Otherwise it emits one node per line without slots.

MPI services also forward common PMI, PMIx, and Slurm rank variables into the container through Pyxis `--container-env`, including `PMI_RANK`, `PMI_SIZE`, `PMIX_RANK`, `PMIX_NAMESPACE`, `SLURM_PROCID`, `SLURM_LOCALID`, `SLURM_NODEID`, `SLURM_NTASKS`, and `SLURM_TASKS_PER_NODE`.

### `gres` and `gpus`

When both `gres` and `gpus` are set at the same level, `gres` takes priority and `gpus` is ignored.

## Service fields

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `image` | string | required unless `runtime.backend: host` | Can be a remote image reference, a local `.sqsh` / `.squashfs` path for Pyxis, or a local `.sif` path for Apptainer/Singularity. |
| `command` | string or list of strings | omitted | Shell form or exec form. |
| `entrypoint` | string or list of strings | omitted | Must use the same form as `command` when both are present. |
| `environment` | mapping or list of `KEY=VALUE` strings | omitted | Both forms normalize to key/value pairs. |
| `volumes` | list of `host_path:container_path` strings | omitted | Runtime bind mounts. Host paths resolve against the compose file directory. |
| `working_dir` | string | omitted | Valid only when the service also has an explicit `command` or `entrypoint`. |
| `depends_on` | list or mapping | omitted | Dependency list with `service_started` or `service_healthy` conditions. |
| `readiness` | mapping | omitted | Post-launch readiness gate. |
| `healthcheck` | mapping | omitted | Compose-compatible sugar for a subset of `readiness`. Mutually exclusive with `readiness`. |
| `x-env` | mapping | omitted | Structured host-side module, Spack view, and environment setup for this service. |
| `x-slurm` | mapping | omitted | Per-service Slurm overrides. |
| `x-runtime` | mapping | omitted | Backend-neutral image preparation rules. |
| `x-enroot` | mapping | omitted | Pyxis/Enroot preparation compatibility alias. |

## Image rules

### Remote images

- Any image reference without an explicit `://` scheme is prefixed with `docker://`.
- Explicit schemes are allowed only for `docker://`, `dockerd://`, and `podman://`.
- Other schemes are rejected.
- Shell variables in the image string are expanded at plan time.
- Unset variables expand to empty strings.

### Local images

- Pyxis local image paths must point to `.sqsh` or `.squashfs` files.
- Apptainer/Singularity local image paths must point to `.sif` files.
- Relative paths are resolved against the compose file directory.
- Paths that look like build contexts are rejected.

## `command` and `entrypoint`

Both fields accept either:

- a string, interpreted as shell form
- a list of strings, interpreted as exec form

Rules:

- If both fields are present, they must use the same form.
- Mixed string/array combinations are rejected.
- If neither field is present, the image default entrypoint and command are used.
- If `working_dir` is set, at least one of `command` or `entrypoint` must also be set.

## `environment`

Accepted forms:

```yaml
environment:
  APP_ENV: prod
  LOG_LEVEL: info
```

```yaml
environment:
  - APP_ENV=prod
  - LOG_LEVEL=info
```

Rules:

- List items must use `KEY=VALUE` syntax.
- `.env` from the compose file directory is loaded automatically when present.
- Shell environment variables override `.env`; `.env` fills only missing variables.
- `environment`, `x-runtime.prepare.env`, and compatibility `x-enroot.prepare.env` values support `$VAR`, `${VAR}`, `${VAR:-default}`, and `${VAR-default}` interpolation.
- Missing variables without defaults are errors.
- Use `$$` for a literal dollar sign in interpolated fields.
- String-form shell snippets are still literal. For example, `$PATH` inside a string-form `command` is not expanded at plan time.

## `volumes`

Accepted form:

```yaml
volumes:
  - ./app:/workspace
  - /shared/data:/data
  - /shared/reference:/reference:ro
```

Rules:

- Host paths are resolved against the compose file directory.
- Runtime mounts accept `host_path:container_path` and `host_path:container_path:ro|rw`.
- Pyxis mounts are passed through `srun --container-mounts=...`; Apptainer/Singularity mounts are passed as `--bind`.
- Every service also gets an automatic shared mount at `/hpc-compose/job`, backed by `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}` on the host.
- `/hpc-compose/job` is reserved and cannot be used as an explicit volume destination.

<div class="callout warning">
  <p><strong>Warning</strong></p>
  <p>If a mounted file is a symlink, the symlink target must also be visible from inside the mounted directory. Otherwise the path can exist on the host but fail inside the container.</p>
</div>

## `depends_on`

Accepted forms:

```yaml
depends_on:
  - redis
```

```yaml
depends_on:
  redis:
    condition: service_started
```

```yaml
depends_on:
  redis:
    condition: service_healthy
```

Rules:

- List form means `condition: service_started`.
- Map form accepts `condition: service_started`, `condition: service_healthy`, and `condition: service_completed_successfully`.
- `service_healthy` requires the dependency service to define `readiness`.
- `service_started` waits only for the dependency process to be launched and still alive.
- `service_healthy` waits for the dependency readiness check to succeed.
- `service_completed_successfully` waits for the dependency to exit with status `0` before launching the dependent service, which is useful for one-shot DAG stages such as preprocess -> train -> postprocess.

## `readiness`

Supported types:

### Sleep

```yaml
readiness:
  type: sleep
  seconds: 5
```

- `seconds` is required.

### TCP

```yaml
readiness:
  type: tcp
  host: 127.0.0.1
  port: 6379
  timeout_seconds: 30
```

- `host` defaults to `127.0.0.1`.
- `timeout_seconds` defaults to `60`.

### Log

```yaml
readiness:
  type: log
  pattern: "Server started"
  timeout_seconds: 60
```

- `timeout_seconds` defaults to `60`.

### HTTP

```yaml
readiness:
  type: http
  url: http://127.0.0.1:8080/health
  status_code: 200
  timeout_seconds: 30
```

- `status_code` defaults to `200`.
- `timeout_seconds` defaults to `60`.
- The readiness check polls the URL through `curl`.

## `healthcheck`

`healthcheck` is accepted as migration sugar and is normalized into the readiness model.

```yaml
services:
  redis:
    image: redis:7
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "6379"]
      timeout: 30s
```

Rules:

- `healthcheck` and `readiness` are mutually exclusive.
- Supported probe forms are a constrained subset:
  - `["CMD", "nc", "-z", HOST, PORT]`
  - `["CMD-SHELL", "nc -z HOST PORT"]`
  - recognized `curl` probes against `http://` or `https://` URLs
  - recognized `wget --spider` probes against `http://` or `https://` URLs
- `timeout` maps to `timeout_seconds`.
- `disable: true` disables readiness for that service.
- `interval`, `retries`, and `start_period` are parsed but rejected in v1.
- HTTP-style healthchecks normalize to `readiness.type: http` with `status_code: 200`.

## Service-level `x-slurm`

These fields live under `services.<name>.x-slurm`.

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `nodes` | integer | omitted | Legacy shorthand: `1` for a helper step, or the full top-level allocation node count for a full-allocation distributed service. Partial multi-node counts require `placement.node_count`. |
| `placement` | mapping | omitted | Explicit node-index placement inside the allocation. |
| `ntasks` | integer | omitted | Adds `--ntasks` to that service's `srun`. |
| `ntasks_per_node` | integer | omitted | Adds `--ntasks-per-node` to that service's `srun`. |
| `cpus_per_task` | integer | omitted | Adds `--cpus-per-task` to that service's `srun`. |
| `gpus` | integer | omitted | Adds `--gpus` when `gres` is not set. |
| `gres` | string | omitted | Adds `--gres` to that service's `srun`. Takes priority over `gpus`. |
| `gpus_per_node` | integer | omitted | Adds `--gpus-per-node` to that service's `srun`. |
| `gpus_per_task` | integer | omitted | Adds `--gpus-per-task` to that service's `srun`. |
| `cpus_per_gpu` | integer | omitted | Adds `--cpus-per-gpu` to that service's `srun`. |
| `mem_per_gpu` | string | omitted | Adds `--mem-per-gpu` to that service's `srun`. |
| `gpu_bind` | string | omitted | Adds `--gpu-bind` to that service's `srun`. |
| `cpu_bind` | string | omitted | Adds `--cpu-bind` to that service's `srun`. |
| `mem_bind` | string | omitted | Adds `--mem-bind` to that service's `srun`. |
| `distribution` | string | omitted | Adds `--distribution` to that service's `srun`. |
| `hint` | string | omitted | Adds `--hint` to that service's `srun`. |
| `time_limit` | string | omitted | Advisory per-service time limit. Validated against Slurm time formats but not passed to `srun`. `inspect` surfaces warnings when the limit exceeds allocation time or conflicts with dependencies. Accepted formats: `MM`, `MM:SS`, `HH:MM:SS`, `D-HH`, `D-HH:MM`, `D-HH:MM:SS`. |
| `extra_srun_args` | list of strings | omitted | Appended directly to the service's `srun` command. |
| `mpi` | mapping | omitted | Adds first-class MPI launch metadata and `srun --mpi=<type>`. |
| `failure_policy` | mapping | omitted | Per-service failure handling (`fail_job`, `ignore`, `restart_on_failure`). |
| `prologue` | string or mapping | omitted | Per-service shell hook run before each launch attempt. String shorthand runs on the host. |
| `epilogue` | string or mapping | omitted | Per-service shell hook run after each service exit attempt. String shorthand runs on the host. |

### `services.<name>.x-slurm.prologue` / `epilogue`

```yaml
services:
  trainer:
    image: trainer:latest
    command: python train.py
    x-slurm:
      prologue: |
        module load cuda/12.1
        nvidia-smi
      epilogue:
        context: container
        script: |
          tar czf /shared/logs-${SLURM_JOB_ID}.tar.gz /hpc-compose/job/logs
```

- Shape: either a block string, or a mapping with `script` and optional `context`.
- `context`: `host` (default) or `container`.
- Hook scripts are emitted as trusted shell and are not Compose-interpolated, so runtime variables such as `${SLURM_JOB_ID}` are preserved.
- Hooks run once per service launch attempt, including `restart_on_failure` retries.
- Host hooks run in the generated batch supervisor on the allocation's primary execution context. Container hooks wrap the service command inside the container and can use `/hpc-compose/job`.
- Hook stdout/stderr is written to the service log.
- Container hooks require an explicit `command` or `entrypoint`; image-default services cannot be wrapped.

### `services.<name>.x-slurm.placement`

```yaml
services:
  a:
    image: app:a
    x-slurm:
      placement: { node_range: "0-3" }
  b:
    image: app:b
    x-slurm:
      placement: { node_range: "4-7" }
  ps:
    image: app:b
    x-slurm:
      placement: { share_with: b }
```

Exactly one selector is required:

| Field | Shape | Notes |
| --- | --- | --- |
| `node_range` | string | Zero-based inclusive allocation indices, for example `"0-3"` or `"0-3,6"`. |
| `node_count` | integer | Selects this many eligible nodes starting at `start_index`, default `0`. |
| `node_percent` | integer `1..100` | Selects `ceil(percent * eligible_nodes / 100)`, minimum one node. |
| `share_with` | string | Reuses another service's resolved node set for explicit co-location. |

Optional fields:

- `start_index`: applies to `node_count` and `node_percent`.
- `exclude`: zero-based allocation indices removed from the eligible set and passed to `srun --exclude`.
- `allow_overlap`: permits intentional overlap with another explicit placement.

Node indices are resolved against the Slurm allocation order from `scontrol show hostnames "$SLURM_JOB_NODELIST"`. At runtime, containers receive both allocation-wide metadata (`HPC_COMPOSE_NODELIST`) and service-scoped metadata (`HPC_COMPOSE_SERVICE_NODELIST`, `HPC_COMPOSE_SERVICE_NODELIST_FILE`, `HPC_COMPOSE_SERVICE_PRIMARY_NODE`, `HPC_COMPOSE_SERVICE_NODE_COUNT`).

### `services.<name>.x-slurm.mpi`

```yaml
services:
  trainer:
    image: mpi-image:latest
    command: /usr/local/bin/train
    x-slurm:
      nodes: 2
      ntasks_per_node: 4
      mpi:
        type: pmix_v4
        profile: openmpi
        implementation: openmpi
        launcher: srun
        expected_ranks: 8
        host_mpi:
          bind_paths:
            - /opt/site/openmpi:/opt/site/openmpi:ro
          env:
            MPI_DIR: /opt/site/openmpi
```

- Shape: mapping
- Default: omitted
- `type` is an exact `srun --mpi=<type>` plugin token. Common values include `pmix`, `pmix_v4`, `pmi2`, `pmi1`, and `openmpi`; use `srun --mpi=list` or `hpc-compose doctor --cluster-report` on the target cluster to discover site-specific values.
- Notes:
  - Rendered as `--mpi=<type>` on the service's `srun` command.
  - `profile` is optional compatibility metadata used for validation, cluster-profile diagnostics, and `doctor --mpi-smoke` output. Supported values are `openmpi`, `mpich`, and `intel_mpi`.
  - `profile` does not auto-select or rewrite `type`; use the exact token that your cluster reports through `srun --mpi=list`.
  - `launcher` defaults to `srun`; v1 rejects other launchers.
  - `implementation` is optional metadata for diagnostics. Supported values are `openmpi`, `mpich`, `intel_mpi`, `mvapich2`, `cray_mpi`, `hpe_mpi`, and `unknown`.
  - When both `profile` and `implementation` are set, they must describe the same MPI family.
  - `expected_ranks`, when set, must match the resolved Slurm task geometry.
  - `host_mpi.bind_paths` uses `host_path:container_path[:ro|rw]` syntax, is validated like service volumes, and is automatically mounted into the service.
  - `host_mpi.env` is injected into the service environment after normal service environment entries.
  - Cannot be combined with raw `--mpi...` entries in `extra_srun_args`.
  - MPI services receive `HPC_COMPOSE_MPI_TYPE` and `HPC_COMPOSE_MPI_HOSTFILE`.
  - MPI services also receive `HPC_COMPOSE_MPI_PROFILE` when `profile` is set and `HPC_COMPOSE_MPI_IMPLEMENTATION` when `implementation` is set or implied by `profile`.
  - `hpc-compose doctor --mpi-smoke -f compose.yaml --service trainer` renders a smoke probe for the service; add `--submit` to run it through Slurm. The smoke plan keeps allocation and MPI launch settings, but strips application workflow blocks such as setup, scratch staging, resume metadata, artifacts, and burst-buffer directives.

Profile-specific compatibility checks are intentionally conservative:

- `profile: openmpi` expects a PMIx-capable `type` such as `pmix` or `pmix_v*`, with `pmi2` accepted as a fallback.
- `profile: mpich` expects `pmi2` or a PMIx-capable setup.
- `profile: intel_mpi` expects `pmi2`; preflight and doctor warn when no `I_MPI_PMI_LIBRARY` or cluster-profile PMI2 library is visible.

### `services.<name>.x-slurm.failure_policy`

```yaml
services:
  worker:
    image: python:3.11-slim
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 3
        backoff_seconds: 5
        window_seconds: 60
        max_restarts_in_window: 3
```

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `mode` | `fail_job` \| `ignore` \| `restart_on_failure` | `fail_job` | `fail_job` keeps fail-fast behavior. `ignore` keeps the job running after non-zero exits. `restart_on_failure` restarts on non-zero exits only. |
| `max_restarts` | integer | `3` when `mode=restart_on_failure` | Required to be at least `1` after defaults are applied. Valid only for `restart_on_failure`. |
| `backoff_seconds` | integer | `5` when `mode=restart_on_failure` | Fixed delay between restart attempts. Required to be at least `1` after defaults are applied. Valid only for `restart_on_failure`. |
| `window_seconds` | integer | `60` when `mode=restart_on_failure` | Rolling window for counting restart-triggering exits. Required to be at least `1` after defaults are applied. Valid only for `restart_on_failure`. |
| `max_restarts_in_window` | integer | resolved `max_restarts` when `mode=restart_on_failure` | Maximum restart-triggering exits allowed within `window_seconds`. Required to be at least `1` after defaults are applied. Valid only for `restart_on_failure`. |

Rules:

- In a multi-node allocation, implicit helper services are pinned to `HPC_COMPOSE_PRIMARY_NODE`.
- Explicit service placements may not overlap unless one side sets `placement.allow_overlap: true` or uses `placement.share_with`.
- `max_restarts`, `backoff_seconds`, `window_seconds`, and `max_restarts_in_window` are rejected unless `mode: restart_on_failure`.
- Restart attempts count relaunches after the initial launch.
- Restarts trigger only for non-zero exits.
- `restart_on_failure` enforces both a lifetime cap (`max_restarts`) and a rolling-window cap (`max_restarts_in_window` within `window_seconds`) during one live batch-script execution.
- If you omit the rolling-window fields, `restart_on_failure` still enables default crash-loop protection with `window_seconds: 60` and `max_restarts_in_window: <resolved max_restarts>`.
- Services configured with `mode: ignore` cannot be used as dependencies in `depends_on`.

Examples:

Use the defaults when you only need bounded retries:

```yaml
services:
  worker:
    image: python:3.11-slim
    x-slurm:
      failure_policy:
        mode: restart_on_failure
```

That resolves to:

- `max_restarts: 3`
- `backoff_seconds: 5`
- `window_seconds: 60`
- `max_restarts_in_window: 3`

Use explicit fields when you need a larger lifetime budget but still want a tighter crash-loop guard:

```yaml
services:
  worker:
    image: python:3.11-slim
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 8
        backoff_seconds: 10
        window_seconds: 60
        max_restarts_in_window: 3
```

Semantics:

- The initial launch does not count as a restart.
- `restart_count` counts granted relaunches after the initial launch.
- `max_restarts_in_window` counts restart-triggering non-zero exits whose timestamps still satisfy `now - event < window_seconds`.
- If a non-zero exit would exceed the rolling-window cap, the job fails immediately and that blocked exit is not recorded as a consumed restart.
- Successful exits do not trigger restarts and do not add entries to the rolling window.
- The rolling window is attempt-local to one live batch-script execution. It is not hydrated from prior `state.json`, resume metadata, or Slurm requeue history.

Tracked state:

- `status --format json` includes `failure_policy_mode`, `restart_count`, `max_restarts`, `window_seconds`, `max_restarts_in_window`, `restart_failures_in_window`, and `last_exit_code` for each tracked service.
- Text `status` renders the live rolling-window budget as `window=<current>/<max>@<seconds>s`.

Unknown keys under top-level `x-slurm` or per-service `x-slurm` cause hard errors.

## `x-runtime.prepare` and `x-enroot.prepare`

`x-runtime.prepare` lets a service build a prepared runtime image from its base image before submission. `x-enroot.prepare` remains accepted as a Pyxis-only compatibility spelling.

```yaml
services:
  app:
    image: python:3.11-slim
    x-runtime:
      prepare:
        commands:
          - pip install --no-cache-dir numpy pandas
        mounts:
          - ./requirements.txt:/tmp/requirements.txt
        env:
          PIP_CACHE_DIR: /tmp/pip-cache
        root: true
```

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `commands` | list of strings | required when `prepare` is present | Each command runs through the selected backend's writable prepare flow. |
| `mounts` | list of `host_path:container_path` strings | omitted | Visible only during prepare. Relative host paths resolve against the compose file directory. |
| `env` | mapping or list of `KEY=VALUE` strings | omitted | Passed only during prepare. Values support the same interpolation rules as `environment`. |
| `root` | boolean | `true` | Controls whether prepare commands request root/fakeroot behavior where the backend supports it. |

Rules:

- If `x-runtime.prepare` or `x-enroot.prepare` is present, `commands` cannot be empty.
- A service may not set both spellings.
- `x-enroot.prepare` is rejected when `runtime.backend` is not `pyxis`.
- If `prepare.mounts` is non-empty, the service rebuilds on every `prepare` or `submit`.
- Remote base images are imported under `cache_dir/base`.
- Prepared images are exported under `cache_dir/prepared`.
- Unknown keys under `x-runtime`, `x-enroot`, or `prepare` cause hard errors.

## Unsupported Compose keys

These keys are rejected with explicit messages:

- `build`
- `ports`
- `networks`
- `network_mode`
- Compose `restart` (use `services.<name>.x-slurm.failure_policy`)
- `deploy`

Any other unknown key at the service level is also rejected.
