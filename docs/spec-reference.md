# Spec reference

This document covers the Compose subset that `hpc-compose` actually accepts today. If a field is not listed here, it is either unsupported or intentionally ignored.

`hpc-compose` validates the spec in `src/spec.rs` and then normalizes it in `src/planner.rs`. The rules below match that behavior.

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

| Field | Shape | Notes |
| --- | --- | --- |
| `name` | string | Optional application name. Used as the job name if `x-slurm.job_name` is not set. |
| `version` | string | Accepted for Compose compatibility, but not used by the planner. |
| `services` | mapping | Required. Must contain at least one service. |
| `x-slurm` | mapping | Optional top-level Slurm settings and shared runtime defaults. |

## `x-slurm` fields

These fields live under the top-level `x-slurm` block.

| Field | Shape | Notes |
| --- | --- | --- |
| `job_name` | string | Overrides `name` for the rendered `#SBATCH --job-name`. |
| `partition` | string | Passed through to `#SBATCH --partition`. |
| `account` | string | Passed through to `#SBATCH --account`. |
| `qos` | string | Passed through to `#SBATCH --qos`. |
| `time` | string | Passed through to `#SBATCH --time`. |
| `nodes` | integer | Must be `1` or omitted in v1. |
| `cpus_per_task` | integer | Top-level Slurm CPU request. |
| `mem` | string | Passed through to `#SBATCH --mem`. |
| `gres` | string | Passed through to `#SBATCH --gres`. |
| `gpus` | integer | Used only when `gres` is not set. |
| `constraint` | string | Passed through to `#SBATCH --constraint`. |
| `output` | string | Passed through to `#SBATCH --output`. |
| `error` | string | Passed through to `#SBATCH --error`. |
| `chdir` | string | Passed through to `#SBATCH --chdir`. |
| `cache_dir` | string | Shared cache root. Relative paths and env vars are resolved against the compose file directory. |
| `setup` | list of strings | Raw shell lines inserted into the generated script before any service launches. |
| `submit_args` | list of strings | Extra raw `#SBATCH ...` lines appended to the script header. |

### `x-slurm.setup`

```yaml
x-slurm:
  setup:
    - module load enroot
    - source /shared/env.sh
```

Rules:

- Each line is emitted verbatim into the generated bash script, which runs under `set -euo pipefail`.
- A syntax error in a setup line will abort the entire job at startup.
- Shell quoting and escaping are the user's responsibility.
- Prefer leaving `setup` empty unless your cluster actually requires module loads or shell initialization.

### `x-slurm.submit_args`

```yaml
x-slurm:
  submit_args:
    - "--mail-type=END"
    - "--mail-user=user@example.com"
    - "--reservation=gpu-reservation"
```

Rules:

- Each entry is emitted as `#SBATCH {arg}` (the `#SBATCH` prefix is added automatically).
- Entries are not validated against Slurm's option syntax.

### `x-slurm.cache_dir`

- Defaults to `$HOME/.cache/hpc-compose`.
- Must resolve to shared storage visible from both login and compute nodes.
- Paths under `/tmp`, `/var/tmp`, `/private/tmp`, and `/dev/shm` are rejected.
- The default is convenient for simple or home-directory workflows, but shared project or workspace storage is usually a better long-term choice on real clusters.

### `gres` vs `gpus`

When both `gres` and `gpus` are set at the same level (top-level or per-service), `gres` takes priority and `gpus` is ignored. Use `gres` when you need the full Slurm syntax (e.g. `gres: gpu:a100:2`), or `gpus` for the simpler integer form.

## Service fields

| Field | Shape | Notes |
| --- | --- | --- |
| `image` | string | Required. Can be a remote image reference or a local `.sqsh` / `.squashfs` path. |
| `command` | string or list of strings | Optional. Shell form or exec form. |
| `entrypoint` | string or list of strings | Optional. Same form rules as `command`. |
| `environment` | mapping or list of `KEY=VALUE` strings | Optional. Both forms normalize to key/value pairs. |
| `volumes` | list of `host_path:container_path` strings | Optional runtime bind mounts. Host paths resolve against the compose file directory. |
| `working_dir` | string | Optional. Only valid when the service also has an explicit `command` or `entrypoint`. |
| `depends_on` | list or mapping | Optional launch-order dependency list. Only `condition: service_started` is supported in map form. |
| `readiness` | mapping | Optional readiness gate run after the service launches. |
| `x-slurm` | mapping | Optional per-service Slurm overrides. |
| `x-enroot` | mapping | Optional per-service Enroot preparation rules. |

## Image rules

### Remote images

- Any image reference without an explicit `://` scheme is auto-prefixed with `docker://`. Bare names like `redis:7` become `docker://redis:7`, and registry-prefixed references like `ghcr.io/org/app:tag` become `docker://ghcr.io/org/app:tag`.
- If you need a non-Docker transport (for example Enroot's `podman://` pull path), write the scheme explicitly.
- Explicit schemes are allowed only for:
  - `docker://`
  - `dockerd://`
  - `podman://`
- Other schemes (e.g. `oci://`) are rejected.
- Shell variables in the image string are expanded at plan time (e.g. `$MY_REGISTRY/app:v1` resolves `$MY_REGISTRY` from the environment). Unset variables silently expand to empty strings.

### Local images

- Local image paths must point to `.sqsh` or `.squashfs` files.
- Relative paths are resolved against the compose file directory.
- Paths that look like build contexts (e.g. `./Dockerfile`, `../my-build/`) are rejected with a message to use `image:` plus `x-enroot.prepare` instead.

## `command` and `entrypoint`

Both fields accept either:

- a string, interpreted as shell form, or
- a list of strings, interpreted as exec form.

Rules:

- If both `entrypoint` and `command` are present, they must use the same form.
- Mixed string/array combinations are rejected.
- If neither field is present, the image default entrypoint/command is used.
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
- The normalized values are passed through to the runtime environment.
- Values are **not** shell-expanded. `$PATH` in a value is the literal string `$PATH`, not the host's `$PATH`. Only image strings and volume host-paths receive `$VAR` expansion at plan time.

## `volumes`

Accepted form:

```yaml
volumes:
  - ./app:/workspace
  - /shared/data:/data
```

Rules:

- Host paths are resolved against the compose file directory, not the shell's current working directory.
- The planner treats them as runtime mounts for `srun --container-mounts=...`.
- Every service also gets an automatic shared mount at `/hpc-compose/job`, backed by `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}` on the host.
- `/hpc-compose/job` is reserved for that automatic shared mount and cannot be used as an explicit volume destination.
- Avoid mounting over `/hpc-compose/job`; that path is reserved for the built-in per-job shared directory.
- If a mounted file is a symlink, the symlink target must still be visible from inside the container. Otherwise the file may appear to exist on the host but fail inside the container.

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

Rules:

- `depends_on` controls launch order only.
- Readiness gating is separate and configured through `readiness`.
- In map form, only `condition: service_started` is accepted.

## `readiness`

Supported types:

### Sleep

```yaml
readiness:
  type: sleep
  seconds: 5
```

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

## Service-level `x-slurm`

These fields live under `services.<name>.x-slurm`.

| Field | Shape | Notes |
| --- | --- | --- |
| `cpus_per_task` | integer | Adds `--cpus-per-task` to that service's `srun`. |
| `gpus` | integer | Adds `--gpus` when `gres` is not set. |
| `gres` | string | Adds `--gres` to that service's `srun`. Takes priority over `gpus`. |
| `extra_srun_args` | list of strings | Appended directly to the service's `srun` command. |

Unknown keys under `x-slurm` or per-service `x-slurm` cause hard errors rather than being silently ignored.

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

| Field | Shape | Notes |
| --- | --- | --- |
| `commands` | list of strings | Required when `prepare` is present. Each command runs via `enroot start ... /bin/sh -lc ...`. |
| `mounts` | list of `host_path:container_path` strings | Optional mounts visible only during prepare. Relative host paths resolve against the compose file directory. |
| `env` | mapping or list of `KEY=VALUE` strings | Optional environment variables passed only during prepare. Values are not shell-expanded. |
| `root` | boolean | Optional. Defaults to `true`. Controls whether prepare commands run with `--root`. |

Rules:

- If `x-enroot.prepare` is present, `commands` cannot be empty.
- If `prepare.mounts` is non-empty, the service rebuilds on every `prepare` / `submit` because mounted host content is not cached safely.
- Remote base images are imported under `cache_dir/base`.
- Prepared images are exported under `cache_dir/prepared`.
- Unknown keys under `x-enroot` or `x-enroot.prepare` cause hard errors.
- Prefer `x-enroot.prepare` for slower-changing dependencies or tooling, not for fast-changing application source code.

## Unsupported Compose keys

These are rejected with explicit messages:

- `build`
- `ports`
- `networks`
- `network_mode`
- `restart`
- `deploy`

Any other unknown key at the service level is also rejected.

## Practical constraints to remember

- v1 supports one Slurm allocation on one node.
- `depends_on` orders startup, but readiness determines when dependents are allowed to continue.
- The dev workflow is `volumes` for active source code and `x-enroot.prepare.commands` for slower-changing dependencies or tools.
- Upgrading `hpc-compose` invalidates all cached artifacts because cache keys include the tool version. After an upgrade, expect a full rebuild on the next `prepare` or `submit`. Use `cache prune --age 0` or `cache prune --all-unused -f compose.yaml` to clean up orphaned artifacts.
- `gres` takes priority over `gpus` at both the top level and per-service level. If both are set, only `gres` is emitted.
- Environment values are literal strings — `$VAR` is not expanded in `environment` or `x-enroot.prepare.env` values.
- `setup` lines are raw bash. They must be syntactically correct because the generated script runs under `set -euo pipefail`.
- Service names with non-alphanumeric characters are encoded in log filenames (e.g. `my.app` produces `my_x2e_app.log`). Prefer `[a-zA-Z0-9_-]` in service names for readability.
