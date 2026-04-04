# Examples

These examples are the fastest way to understand the intended `hpc-compose` workflows and adapt them to a real application.

## Example matrix

| Example | What it demonstrates | When to start from it |
| --- | --- | --- |
| [`app-redis-worker.yaml`](app-redis-worker.yaml) | Multiple services, `depends_on`, and TCP readiness checks | You need service startup ordering or a small multi-service stack |
| [`dev-python-app.yaml`](dev-python-app.yaml) | Mounted source code plus `x-enroot.prepare.commands` for dependencies | You want an iterative development workflow |
| [`llm-curl-workflow.yaml`](llm-curl-workflow.yaml) | End-to-end LLM request flow with a login-node prepare step and a `curl` client | You want the smallest concrete inference workflow |
| [`llm-curl-workflow-workdir.yaml`](llm-curl-workflow-workdir.yaml) | Same LLM workflow, but parameterized around one work directory via `HPC_COMPOSE_HOME` | You want to run the example from a login-node home or project directory |
| [`llama-app.yaml`](llama-app.yaml) | GPU-backed service, mounted model files, dependent app service | You need accelerator resources or a model-serving pattern |

## How to adapt an example

1. Copy the closest example to your own `compose.yaml`.
2. Set `x-slurm.cache_dir` to a shared filesystem path.
3. Replace the example `image`, `command`, `environment`, and `volumes` with your workload.
4. Keep active source trees in `volumes` and reserve `x-enroot.prepare.commands` for slower-changing dependencies or tools.
5. Add `readiness` to services that must be actually reachable before dependents continue.
6. Adjust `x-slurm` resource settings at the top level or per service as needed.
7. Add `x-slurm.setup` only when your cluster actually needs module loads or shell initialization.

## Notes per example

### `dev-python-app.yaml`

- Best reference for the preferred dev workflow.
- Mounts `./app` into the container at runtime.
- Uses `x-enroot.prepare.commands` only for Python dependencies.

### `app-redis-worker.yaml`

- Best reference for `depends_on` plus readiness.
- Shows one service waiting for another service's TCP port.

### `llm-curl-workflow.yaml`

- Best reference for a complete request/response path.
- Uses `x-enroot.prepare.commands` on the login node to build a Debian-based client image with `bash` and `curl`.
- Uses a fixed sleep readiness gate so the client does not race model loading.
- Shares a tiny workflow directory so the LLM server can shut down after the request completes.

### `llm-curl-workflow-workdir.yaml`

- Best reference when you already have a working directory on the login node.
- Resolves cache and mount paths through `HPC_COMPOSE_HOME`.
- Expects `models/` and `llm-curl/` under that work directory.

### `llama-app.yaml`

- Best reference for GPU-backed services and dependent apps.
- Expects a model file at `models/model.gguf`; see [`models/README.md`](models/README.md).

## Related docs

- [`../docs/runbook.md`](../docs/runbook.md)
- [`../docs/spec-reference.md`](../docs/spec-reference.md)
