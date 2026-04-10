# Spec reference

This page describes the Compose subset that `hpc-compose` accepts today. Unknown or unsupported fields are rejected unless this page explicitly says otherwise.

## Top-level shape

```yaml
name: demo
version: "3.9"

x-slurm:
  time: "00:30:00"
  cache_dir: /shared/$USER/hpc-compose-cache

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
| `services` | mapping | required | Must contain at least one service. |
| `x-slurm` | mapping | omitted | Top-level Slurm settings and shared runtime defaults. |

## Settings-aware command table

Use these commands and global flags when you want repo-adjacent profile memory for compose path, env files, env vars, and binary overrides.

| Command or flag | Purpose | Notes |
| --- | --- | --- |
| `--profile <NAME>` | Select the profile from settings | Global flag; applies to every subcommand. |
| `--settings-file <PATH>` | Use an explicit settings file | Global flag; bypasses upward auto-discovery of `.hpc-compose/settings.toml`. |
| `hpc-compose setup` | Create or update repo-adjacent settings | Interactive by default; supports `--non-interactive` with `--profile-name`, `--compose-file`, `--env-file`, `--env`, `--binary`, and `--default-profile`. |
| `hpc-compose context` | Print fully resolved execution context | Shows selected settings/profile, compose path, binaries, interpolation vars, runtime paths, and value sources; supports `--format json`. |
| `hpc-compose validate --strict-env` | Fail when interpolation fell back to defaults | Detects when `${VAR:-...}` or `${VAR-...}` consumed fallback values because `VAR` was missing. |

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
| `constraint` | string | omitted | Passed through to `#SBATCH --constraint`. |
| `output` | string | omitted | Passed through to `#SBATCH --output`. |
| `error` | string | omitted | Passed through to `#SBATCH --error`. |
| `chdir` | string | omitted | Passed through to `#SBATCH --chdir`. |
| `cache_dir` | string | `$HOME/.cache/hpc-compose` | Must resolve to shared storage visible from the login node and the compute nodes. |
| `metrics` | mapping | omitted | Enables runtime metrics sampling. |
| `artifacts` | mapping | omitted | Enables tracked artifact collection and export metadata. |
| `resume` | mapping | omitted | Enables checkpoint-aware resume semantics with a shared host path mounted into every service. |
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
  - Entries are not validated against Slurm option syntax.

### `x-slurm.cache_dir`

- Shape: string
- Default: `$HOME/.cache/hpc-compose`
- Notes:
  - Relative paths and environment variables are resolved against the compose file directory.
  - Paths under `/tmp`, `/var/tmp`, `/private/tmp`, and `/dev/shm` are rejected.
  - The path must be visible from both the login node and the compute nodes.

### Multi-node placement rules

- `x-slurm.nodes > 1` reserves a multi-node allocation.
- Multi-node v1 supports at most one distributed service spanning the full allocation.
- Helper services remain single-node steps and are pinned to the allocation's primary node.
- When a multi-node job has exactly one service, that service defaults to the distributed full-allocation step.
- Distributed services may use `readiness.type: sleep` or `readiness.type: log`, or TCP/HTTP readiness only with an explicit non-local host or URL.

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
  - In multi-node v1, `gpu` sampling remains primary-node-only; `slurm` sampling still observes the full distributed step through `sstat`.
  - Sampler files are written under `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/metrics` on the host and are also visible inside containers at `/hpc-compose/job/metrics`.
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

The same data is also written under `/hpc-compose/job/allocation/primary_node` and `/hpc-compose/job/allocation/nodes.txt`.

### `gres` and `gpus`

When both `gres` and `gpus` are set at the same level, `gres` takes priority and `gpus` is ignored.

## Service fields

| Field | Shape | Default | Notes |
| --- | --- | --- | --- |
| `image` | string | required | Can be a remote image reference or a local `.sqsh` / `.squashfs` path. |
| `command` | string or list of strings | omitted | Shell form or exec form. |
| `entrypoint` | string or list of strings | omitted | Must use the same form as `command` when both are present. |
| `environment` | mapping or list of `KEY=VALUE` strings | omitted | Both forms normalize to key/value pairs. |
| `volumes` | list of `host_path:container_path` strings | omitted | Runtime bind mounts. Host paths resolve against the compose file directory. |
| `working_dir` | string | omitted | Valid only when the service also has an explicit `command` or `entrypoint`. |
| `depends_on` | list or mapping | omitted | Dependency list with `service_started` or `service_healthy` conditions. |
| `readiness` | mapping | omitted | Post-launch readiness gate. |
| `healthcheck` | mapping | omitted | Compose-compatible sugar for a subset of `readiness`. Mutually exclusive with `readiness`. |
| `x-slurm` | mapping | omitted | Per-service Slurm overrides. |
| `x-enroot` | mapping | omitted | Per-service Enroot preparation rules. |

## Image rules

### Remote images

- Any image reference without an explicit `://` scheme is prefixed with `docker://`.
- Explicit schemes are allowed only for `docker://`, `dockerd://`, and `podman://`.
- Other schemes are rejected.
- Shell variables in the image string are expanded at plan time.
- Unset variables expand to empty strings.

### Local images

- Local image paths must point to `.sqsh` or `.squashfs` files.
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
- `environment` and `x-enroot.prepare.env` values support `$VAR`, `${VAR}`, `${VAR:-default}`, and `${VAR-default}` interpolation.
- Missing variables without defaults are errors.
- Use `$$` for a literal dollar sign in interpolated fields.
- String-form shell snippets are still literal. For example, `$PATH` inside a string-form `command` is not expanded at plan time.

## `volumes`

Accepted form:

```yaml
volumes:
  - ./app:/workspace
  - /shared/data:/data
```

Rules:

- Host paths are resolved against the compose file directory.
- Runtime mounts are passed through `srun --container-mounts=...`.
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
- Map form accepts `condition: service_started` and `condition: service_healthy`.
- `service_healthy` requires the dependency service to define `readiness`.
- `service_started` waits only for the dependency process to be launched and still alive.
- `service_healthy` waits for the dependency readiness check to succeed.

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
| `nodes` | integer | omitted | `1` for a helper step, or the full top-level allocation node count for the one distributed service. |
| `ntasks` | integer | omitted | Adds `--ntasks` to that service's `srun`. |
| `ntasks_per_node` | integer | omitted | Adds `--ntasks-per-node` to that service's `srun`. |
| `cpus_per_task` | integer | omitted | Adds `--cpus-per-task` to that service's `srun`. |
| `gpus` | integer | omitted | Adds `--gpus` when `gres` is not set. |
| `gres` | string | omitted | Adds `--gres` to that service's `srun`. Takes priority over `gpus`. |
| `extra_srun_args` | list of strings | omitted | Appended directly to the service's `srun` command. |
| `failure_policy` | mapping | omitted | Per-service failure handling (`fail_job`, `ignore`, `restart_on_failure`). |

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

- In a multi-node allocation, at most one service may resolve to distributed placement.
- Distributed placement requires `services.<name>.x-slurm.nodes` to equal the top-level allocation node count when it is set explicitly.
- Helper services in multi-node jobs are pinned to `HPC_COMPOSE_PRIMARY_NODE`.
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

- `status --json` includes `failure_policy_mode`, `restart_count`, `max_restarts`, `window_seconds`, `max_restarts_in_window`, `restart_failures_in_window`, and `last_exit_code` for each tracked service.
- Text `status` renders the live rolling-window budget as `window=<current>/<max>@<seconds>s`.

Unknown keys under top-level `x-slurm` or per-service `x-slurm` cause hard errors.

## `x-enroot.prepare`

`x-enroot.prepare` lets a service build a prepared runtime image from its base image before submission.

```yaml
services:
  app:
    image: python:3.11-slim
    x-enroot:
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
| `commands` | list of strings | required when `prepare` is present | Each command runs via `enroot start ... /bin/sh -lc ...`. |
| `mounts` | list of `host_path:container_path` strings | omitted | Visible only during prepare. Relative host paths resolve against the compose file directory. |
| `env` | mapping or list of `KEY=VALUE` strings | omitted | Passed only during prepare. Values support the same interpolation rules as `environment`. |
| `root` | boolean | `true` | Controls whether prepare commands run with `--root`. |

Rules:

- If `x-enroot.prepare` is present, `commands` cannot be empty.
- If `prepare.mounts` is non-empty, the service rebuilds on every `prepare` or `submit`.
- Remote base images are imported under `cache_dir/base`.
- Prepared images are exported under `cache_dir/prepared`.
- Unknown keys under `x-enroot` or `x-enroot.prepare` cause hard errors.

## Unsupported Compose keys

These keys are rejected with explicit messages:

- `build`
- `ports`
- `networks`
- `network_mode`
- Compose `restart` (use `services.<name>.x-slurm.failure_policy`)
- `deploy`

Any other unknown key at the service level is also rejected.
