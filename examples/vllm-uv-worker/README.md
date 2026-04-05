# vLLM + uv Worker

This example shows a common Slurm pattern:

- one `vllm` service serves an OpenAI-compatible API on the compute node,
- one Python worker mounts its source tree at runtime,
- `x-enroot.prepare.commands` installs `uv` once into the worker image,
- and `uv run worker.py` executes the mounted worker code on each submit.

## Normal run

```bash
hpc-compose submit --watch -f examples/vllm-uv-worker.yaml
```

That command already runs preflight, prepares the cached worker image when needed, renders the batch script, submits the job, and follows the logs.

## One-time vs per-run steps

- One-time or occasional: edit `examples/vllm-uv-worker.yaml`, adjust `MODEL_NAME`, and change the mounted code under [`worker.py`](./worker.py).
- Cached image setup: `pip install --no-cache-dir uv` runs through `x-enroot.prepare.commands` and is reused until the worker image changes.
- Per run: `uv run worker.py` executes the mounted source tree against the in-job vLLM API.

## Debugging flow

```bash
hpc-compose validate -f examples/vllm-uv-worker.yaml
hpc-compose inspect --verbose -f examples/vllm-uv-worker.yaml
hpc-compose preflight -f examples/vllm-uv-worker.yaml
hpc-compose prepare -f examples/vllm-uv-worker.yaml
```

Use the separate commands when you want to inspect planning, cluster prerequisites, or cache behavior before the first submit.
