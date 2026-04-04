# Examples

These examples are the fastest way to understand the intended `hpc-compose` workflows and adapt them to a real application.

If you want one of these files written straight to your working directory, use:

```bash
hpc-compose init --template dev-python-app --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml
```

## Example matrix

| Example | What it demonstrates | When to start from it |
| --- | --- | --- |
| [`app-redis-worker.yaml`](app-redis-worker.yaml) | Multiple services, `depends_on`, and TCP readiness checks | You need service startup ordering or a small multi-service stack |
| [`dev-python-app.yaml`](dev-python-app.yaml) | Mounted source code plus `x-enroot.prepare.commands` for dependencies | You want an iterative development workflow |
| [`llm-curl-workflow.yaml`](llm-curl-workflow.yaml) | End-to-end LLM request flow with a login-node prepare step and a `curl` client | You want the smallest concrete inference workflow |
| [`llm-curl-workflow-workdir.yaml`](llm-curl-workflow-workdir.yaml) | Same LLM workflow, but anchored under `$HOME/models` for direct use on a login node | You want the lowest-overhead path from a login-node home directory |
| [`llama-app.yaml`](llama-app.yaml) | GPU-backed service, mounted model files, dependent app service | You need accelerator resources or a model-serving pattern |

If you want the fastest end-to-end cluster example, start with [`llm-curl-workflow-workdir.yaml`](llm-curl-workflow-workdir.yaml). If you want a source-mounted development workflow, start with [`dev-python-app.yaml`](dev-python-app.yaml).

## How to adapt an example

1. Copy the closest example to your own `compose.yaml`.
   Or run `hpc-compose init --template <name> --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml`.
2. Set `x-slurm.cache_dir` to a shared filesystem path when your cluster needs one. The home-directory examples can also rely on the default `$HOME/.cache/hpc-compose`.
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
- Waits for `llama.cpp` to print `main: model loaded` before launching the client.
- Uses the built-in `/hpc-compose/job` mount to shut the server down after the request completes.

### `llm-curl-workflow-workdir.yaml`

- Best reference when you want a direct login-node example without copying helper files.
- Expects the model at `$HOME/models/model.gguf`.
- Uses the default cache directory under `$HOME/.cache/hpc-compose` unless you set `x-slurm.cache_dir`.
- Best first example for a real cluster if you already have a GGUF model.

### `llama-app.yaml`

- Best reference for GPU-backed services and dependent apps.
- Expects a model file at `models/model.gguf`; see [`models/README.md`](models/README.md).

## Related docs

- [`../docs/runbook.md`](../docs/runbook.md)
- [`../docs/spec-reference.md`](../docs/spec-reference.md)
