# Examples

These examples are the fastest way to understand the intended `hpc-compose` workflows and adapt them to a real application.

## Example matrix

| Example | What it demonstrates | When to start from it |
| --- | --- | --- |
| [`app-redis-worker.yaml`](app-redis-worker.yaml) | Multiple services, `depends_on`, and TCP readiness checks | You need service startup ordering or a small multi-service stack |
| [`dev-python-app.yaml`](dev-python-app.yaml) | Mounted source code plus `x-enroot.prepare.commands` for dependencies | You want an iterative development workflow |
| [`llama-app.yaml`](llama-app.yaml) | GPU-backed service, mounted model files, dependent app service | You need accelerator resources or a model-serving pattern |

## How to adapt an example

1. Copy the closest example to your own `compose.yaml`.
2. Set `x-slurm.cache_dir` to a shared filesystem path.
3. Replace the example `image`, `command`, `environment`, and `volumes` with your workload.
4. Keep active source trees in `volumes` and reserve `x-enroot.prepare.commands` for slower-changing dependencies or tools.
5. Add `readiness` to services that must be actually reachable before dependents continue.
6. Adjust `x-slurm` resource settings at the top level or per service as needed.

## Notes per example

### `dev-python-app.yaml`

- Best reference for the preferred dev workflow.
- Mounts `./app` into the container at runtime.
- Uses `x-enroot.prepare.commands` only for Python dependencies.

### `app-redis-worker.yaml`

- Best reference for `depends_on` plus readiness.
- Shows one service waiting for another service's TCP port.

### `llama-app.yaml`

- Best reference for GPU-backed services and dependent apps.
- Expects a model file at `models/model.gguf`; see [`models/README.md`](models/README.md).

## Related docs

- [`../docs/runbook.md`](../docs/runbook.md)
- [`../docs/spec-reference.md`](../docs/spec-reference.md)
