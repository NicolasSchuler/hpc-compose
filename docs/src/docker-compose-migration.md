# Migrating from Docker Compose

This guide helps you convert an existing `docker-compose.yaml` into an `hpc-compose` spec for Slurm clusters with Enroot and Pyxis.

## At a glance

| Docker Compose feature | hpc-compose equivalent |
| --- | --- |
| `image` | `image` (same syntax, auto-prefixed with `docker://`) |
| `command` | `command` (string or list, same syntax) |
| `entrypoint` | `entrypoint` (string or list, same syntax) |
| `environment` | `environment` (map or list, same syntax) |
| `volumes` | `volumes` (host:container bind mounts, same syntax) |
| `depends_on` | `depends_on` (list or map with `condition: service_started` / `service_healthy`) |
| `working_dir` | `working_dir` (requires explicit `command` or `entrypoint`) |
| `build` | **Not supported.** Use `image` + `x-runtime.prepare.commands` instead. |
| `ports` | **Not supported.** Use host networking semantics instead. `127.0.0.1` works only when both sides run on the same node. |
| `networks` / `network_mode` | **Not supported.** There is no Docker-style overlay network or service-name DNS layer. |
| `restart` | **Not supported as a Compose key.** Use `services.<name>.x-slurm.failure_policy`. |
| `deploy` | **Not supported.** Use `x-slurm` for resource allocation. |
| `healthcheck` | Supported for a constrained TCP/HTTP subset and normalized into `readiness`; use explicit `readiness` for anything more complex. |
| Resource limits (`cpus`, `mem_limit`) | Use `x-slurm.cpus_per_task`, `x-slurm.mem`, `x-slurm.gpus` |

## Side-by-side: web app + Redis

### Docker Compose

```yaml
version: "3.9"
services:
  redis:
    image: redis:7
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  app:
    build: .
    ports:
      - "8000:8000"
    depends_on:
      redis:
        condition: service_healthy
    environment:
      REDIS_HOST: redis
    volumes:
      - ./app:/workspace
    working_dir: /workspace
    command: python -m main
```

### hpc-compose

```yaml
name: my-app

x-slurm:
  job_name: my-app
  time: "01:00:00"
  mem: 8G
  cpus_per_task: 4
  cache_dir: /cluster/shared/hpc-compose-cache

services:
  redis:
    image: redis:7
    command: redis-server --save "" --appendonly no
    readiness:
      type: tcp
      host: 127.0.0.1
      port: 6379
      timeout_seconds: 30

  app:
    image: python:3.11-slim
    depends_on:
      redis:
        condition: service_healthy
    environment:
      REDIS_HOST: 127.0.0.1
    volumes:
      - ./app:/workspace
    working_dir: /workspace
    command: python -m main
    x-runtime:
      prepare:
        commands:
          - pip install --no-cache-dir redis fastapi uvicorn
```

### Key changes

1. **`build: .`** → `image: python:3.11-slim` + `x-runtime.prepare.commands` for dependencies.
2. **`ports`** → Removed. Services communicate via `127.0.0.1` because they run on the same node.
3. **`REDIS_HOST: redis`** → `REDIS_HOST: 127.0.0.1`. No DNS service names; use localhost.
4. **`healthcheck`** → `readiness` with `type: tcp`.
5. **Added `x-slurm`** block for Slurm resource allocation (time, memory, CPUs).
6. **Added `x-slurm.cache_dir`** for shared image storage.

## Key differences

### Networking

Docker Compose creates isolated networks where services find each other by name. In `hpc-compose`, helper services on the same node share the host network directly, and multi-node distributed steps must use explicit rendezvous addresses. Replace service hostnames with `127.0.0.1` only when both sides intentionally stay on one node. For multi-node runs, derive the rendezvous host from `/hpc-compose/job/allocation/primary_node` or `HPC_COMPOSE_PRIMARY_NODE`.

### Building images

Docker Compose uses `build:` to run a `Dockerfile`. `hpc-compose` uses `x-runtime.prepare.commands` instead:

```yaml
# Docker Compose
app:
  build:
    context: .
    dockerfile: Dockerfile

# hpc-compose
app:
  image: python:3.11-slim
  x-runtime:
    prepare:
      commands:
        - pip install --no-cache-dir -r /tmp/requirements.txt
      mounts:
        - ./requirements.txt:/tmp/requirements.txt
```

Prefer `volumes` for fast-changing source code and `x-runtime.prepare.commands` for slower-changing dependencies. `x-enroot.prepare` remains accepted as a Pyxis/Enroot compatibility spelling, but new specs should use `x-runtime.prepare`.

### Health checks vs readiness

Docker Compose uses `healthcheck` with a test command, interval, timeout, and retries. `hpc-compose` now accepts a constrained `healthcheck` subset and normalizes it into `readiness`:

```yaml
# TCP: wait for a port to accept connections
readiness:
  type: tcp
  host: 127.0.0.1
  port: 6379
  timeout_seconds: 30

# Log: wait for a pattern in service output
readiness:
  type: log
  pattern: "Server started"
  timeout_seconds: 60

# Sleep: fixed delay
readiness:
  type: sleep
  seconds: 5
```

Supported `healthcheck` migration patterns:

- `["CMD", "nc", "-z", HOST, PORT]`
- `["CMD-SHELL", "nc -z HOST PORT"]`
- recognized `curl` probes against `http://` or `https://` URLs
- recognized `wget --spider` probes against `http://` or `https://` URLs

Still unsupported in v1:

- arbitrary custom command probes
- `interval`
- `retries`
- `start_period`

### Resource allocation

Docker Compose uses `deploy.resources` or top-level `cpus`/`mem_limit`. `hpc-compose` uses Slurm-native resource settings:

```yaml
x-slurm:
  time: "02:00:00"
  mem: 32G
  cpus_per_task: 8
  gpus: 1

services:
  app:
    x-slurm:
      cpus_per_task: 4
      gpus: 1
```

### Restart policies

Docker Compose supports `restart: always`, `on-failure`, etc. `hpc-compose` does not accept the Compose `restart:` key, but it does support per-service restart behavior through `services.<name>.x-slurm.failure_policy`.

```yaml
services:
  app:
    image: python:3.11-slim
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 3
        backoff_seconds: 5
        window_seconds: 60
        max_restarts_in_window: 3
```

`restart_on_failure` retries only on non-zero exits. It enforces both a lifetime restart cap and a rolling-window crash-loop cap during one live batch-script execution. If you omit the rolling-window fields, `hpc-compose` defaults to `window_seconds: 60` and `max_restarts_in_window: <resolved max_restarts>`. Use `mode: fail_job` (default) for fail-fast behavior, or `mode: ignore` for non-critical sidecars.

Practical mapping:

- Compose `restart: "no"` -> omit `failure_policy` or use `mode: fail_job`
- Compose `restart: on-failure[:N]` -> use `mode: restart_on_failure` with `max_restarts: N` when you want a similar lifetime retry budget
- Compose `restart: always` / `unless-stopped` -> no direct equivalent; `hpc-compose` intentionally keeps restart handling bounded within one batch job

The rolling-window fields have no direct Docker Compose equivalent. They exist to stop fast crash loops inside one Slurm allocation without giving up a larger lifetime retry budget for transient failures.

## What to do about unsupported features

| Feature | Alternative |
| --- | --- |
| `build` | Use `image` + `x-runtime.prepare.commands`. Mount build context files with `x-runtime.prepare.mounts` if needed. |
| `ports` | Not needed. Services share `127.0.0.1` on one node. |
| `networks` / `network_mode` | Not needed. All services are on the same host network. |
| `restart` | Use `services.<name>.x-slurm.failure_policy` (`fail_job`, `ignore`, `restart_on_failure`). |
| `deploy` | Use `x-slurm` for resources. |
| Service DNS names | Use `127.0.0.1` for same-node helpers, or explicit host metadata such as `HPC_COMPOSE_PRIMARY_NODE` for distributed runs. |
| Named volumes | Use host-path bind mounts in `volumes`. |
| `.env` file | Supported. `.env` in the compose file directory is loaded automatically. |

## Migration checklist

1. **Remove `build:`** — Replace with `image:` pointing to a base image. Move dependency installation to `x-runtime.prepare.commands`.
2. **Remove `ports:`** — Use host-network semantics instead of container port publishing.
3. **Remove `networks:` / `network_mode:`** — There is no Docker-style overlay network or service-name DNS layer.
4. **Remove Compose `restart:`** — use `services.<name>.x-slurm.failure_policy` when you need per-service restart behavior.
5. **Remove `deploy:`** — Use `x-slurm` for resource allocation.
6. **Replace service hostnames** — Change any service-name references (e.g. `redis`, `postgres`) to `127.0.0.1` for same-node helpers, or to explicit allocation metadata for distributed runs.
7. **Replace `healthcheck:`** — Convert to `readiness:` with `type: tcp`, `type: log`, or `type: sleep`.
8. **Add `x-slurm:`** — Set `time`, `mem`, `cpus_per_task`, and optionally `gpus`, `partition`, `account`.
9. **Set `cache_dir`** — Point `x-slurm.cache_dir` to shared storage visible from login and compute nodes.
10. **Validate** — Run `hpc-compose validate -f compose.yaml` to check the converted spec.
11. **Inspect** — Run `hpc-compose inspect --verbose -f compose.yaml` to confirm the planner understood your intent.

## Related docs

- [Execution model](execution-model.md)
- [Spec reference](spec-reference.md)
- [Runbook](runbook.md)
- [Examples](examples.md)
