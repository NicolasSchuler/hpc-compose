# llama.cpp + uv Worker

This example shows the GGUF-serving pattern that commonly comes up on Slurm:

- one `llama.cpp` service serves an OpenAI-compatible API from a mounted GGUF file,
- one Python worker mounts its source tree at runtime and runs through `uv`,
- readiness waits for `llama.cpp` to finish loading the model,
- and both services coordinate shutdown through `/hpc-compose/job/request.done`.

## Normal run

```bash
hpc-compose up -f examples/llama-uv-worker.yaml
```

## Debugging flow

```bash
hpc-compose validate -f examples/llama-uv-worker.yaml
hpc-compose inspect --verbose -f examples/llama-uv-worker.yaml
hpc-compose preflight -f examples/llama-uv-worker.yaml
hpc-compose prepare -f examples/llama-uv-worker.yaml
```

## Expected files and paths

- Put the GGUF file at `./models/model.gguf`.
- The server resolves `GGUF_MODEL_PATH=/models/model.gguf`.
- The worker code lives under [`worker.py`](./worker.py) and is mounted from `./llama-uv-worker`.
- The per-job handoff file lives at `/hpc-compose/job/request.done`.

## Rebuild behavior

- Updating `worker.py` or other mounted source usually only needs another `up`.
- Changing the worker base image or `x-enroot.prepare.commands` is when `up --force-rebuild` or `prepare --force` helps.
- `UV_CACHE_DIR` is pointed at `/hpc-compose/job/.uv-cache` so each job keeps its runtime cache inside the shared job mount.
