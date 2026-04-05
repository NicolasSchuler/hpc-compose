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
| `build` | **Not supported.** Use `image` + `x-enroot.prepare.commands` instead. |
| `ports` | **Not supported.** Services communicate via `127.0.0.1` on a single node. |
| `networks` / `network_mode` | **Not supported.** All services share the host network on one node. |
| `restart` | **Not supported.** Slurm handles job lifecycle. |
| `deploy` | **Not supported.** Use `x-slurm` for resource allocation. |
| `healthcheck` | Use `readiness` (TCP, log, or sleep) instead. |
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
  cache_dir: /shared/$USER/hpc-compose-cache

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
    x-enroot:
      prepare:
        commands:
          - pip install --no-cache-dir redis fastapi uvicorn
```

### Key changes

1. **`build: .`** → `image: python:3.11-slim` + `x-enroot.prepare.commands` for dependencies.
2. **`ports`** → Removed. Services communicate via `127.0.0.1` because they run on the same node.
3. **`REDIS_HOST: redis`** → `REDIS_HOST: 127.0.0.1`. No DNS service names; use localhost.
4. **`healthcheck`** → `readiness` with `type: tcp`.
5. **Added `x-slurm`** block for Slurm resource allocation (time, memory, CPUs).
6. **Added `x-slurm.cache_dir`** for shared image storage.

## Key differences

### Networking

Docker Compose creates isolated networks where services find each other by name. In `hpc-compose`, all services run on the same node and share the host network. Replace service hostnames with `127.0.0.1`.

### Building images

Docker Compose uses `build:` to run a `Dockerfile`. `hpc-compose` uses `x-enroot.prepare.commands` instead:

```yaml
# Docker Compose
app:
  build:
    context: .
    dockerfile: Dockerfile

# hpc-compose
app:
  image: python:3.11-slim
  x-enroot:
    prepare:
      commands:
        - pip install --no-cache-dir -r /tmp/requirements.txt
      mounts:
        - ./requirements.txt:/tmp/requirements.txt
```

Prefer `volumes` for fast-changing source code and `x-enroot.prepare.commands` for slower-changing dependencies.

### Health checks vs readiness

Docker Compose uses `healthcheck` with a test command, interval, timeout, and retries. `hpc-compose` uses `readiness` with three types:

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

Docker Compose supports `restart: always`, `on-failure`, etc. `hpc-compose` does not have restart policies. Slurm manages the job lifecycle: if a service fails, the job fails. Design your workflows to be robust or use Slurm's own retry mechanisms at the job level.

## What to do about unsupported features

| Feature | Alternative |
| --- | --- |
| `build` | Use `image` + `x-enroot.prepare.commands`. Mount build context files with `x-enroot.prepare.mounts` if needed. |
| `ports` | Not needed. Services share `127.0.0.1` on one node. |
| `networks` / `network_mode` | Not needed. All services are on the same host network. |
| `restart` | Slurm handles job lifecycle. No restart policies. |
| `deploy` | Use `x-slurm` for resources. |
| Service DNS names | Use `127.0.0.1` instead of service names. |
| Named volumes | Use host-path bind mounts in `volumes`. |
| `.env` file | Supported. `.env` in the compose file directory is loaded automatically. |

## Migration checklist

1. **Remove `build:`** — Replace with `image:` pointing to a base image. Move dependency installation to `x-enroot.prepare.commands`.
2. **Remove `ports:`** — Services communicate via localhost on the same node.
3. **Remove `networks:` / `network_mode:`** — Not applicable on a single Slurm node.
4. **Remove `restart:`** — Slurm manages job lifecycle.
5. **Remove `deploy:`** — Use `x-slurm` for resource allocation.
6. **Replace service hostnames** — Change any service-name references (e.g. `redis`, `postgres`) to `127.0.0.1`.
7. **Replace `healthcheck:`** — Convert to `readiness:` with `type: tcp`, `type: log`, or `type: sleep`.
8. **Add `x-slurm:`** — Set `time`, `mem`, `cpus_per_task`, and optionally `gpus`, `partition`, `account`.
9. **Set `cache_dir`** — Point `x-slurm.cache_dir` to shared storage visible from login and compute nodes.
10. **Validate** — Run `hpc-compose validate -f compose.yaml` to check the converted spec.
11. **Inspect** — Run `hpc-compose inspect --verbose -f compose.yaml` to confirm the planner understood your intent.

## Related docs

- [Spec reference](spec-reference.md)
- [Runbook](runbook.md)
- [Examples](../examples/README.md)
