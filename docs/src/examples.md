# Examples

These examples are the fastest way to understand the intended `hpc-compose` workflows and adapt them to a real application.

For almost every example, the normal path is:

```bash
hpc-compose submit --watch -f examples/<example>.yaml
```

Use `validate`, `inspect`, `preflight`, or `prepare` separately when you are wiring up the example for the first time or troubleshooting a failure.

If you want one of these files written straight to your working directory, use:

```bash
hpc-compose init --template dev-python-app --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml
```

## Example matrix

| Example | What it demonstrates | When to start from it |
| --- | --- | --- |
| [`app-redis-worker.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/app-redis-worker.yaml) | Multiple services, `depends_on`, and TCP readiness checks | You need service startup ordering or a small multi-service stack |
| [`dev-python-app.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/dev-python-app.yaml) | Mounted source code plus `x-enroot.prepare.commands` for dependencies | You want an iterative development workflow |
| [`llm-curl-workflow.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llm-curl-workflow.yaml) | End-to-end LLM request flow with a login-node prepare step and a `curl` client | You want the smallest concrete inference workflow |
| [`llm-curl-workflow-workdir.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llm-curl-workflow-workdir.yaml) | Same LLM workflow, but anchored under `$HOME/models` for direct use on a login node | You want the lowest-overhead path from a login-node home directory |
| [`llama-app.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llama-app.yaml) | GPU-backed service, mounted model files, dependent app service | You need accelerator resources or a model-serving pattern |
| [`minimal-batch.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/minimal-batch.yaml) | Single service, no dependencies, no GPU, no prepare | You want the simplest possible starting point |
| [`training-checkpoints.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/training-checkpoints.yaml) | GPU training with checkpoints written to shared storage | You need a batch training workflow with artifact collection |
| [`postgres-etl.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/postgres-etl.yaml) | PostgreSQL plus a Python data processing job | You need a database-backed batch pipeline |
| [`vllm-openai.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/vllm-openai.yaml) | vLLM serving with an in-job Python client | You want vLLM-based inference instead of llama.cpp |
| [`vllm-uv-worker.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/vllm-uv-worker.yaml) | vLLM serving plus a source-mounted Python worker executed through `uv` | You want a common LLM stack with mounted app code |
| [`mpi-hello.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/mpi-hello.yaml) | MPI hello world compiled and run with Open MPI | You need an MPI workload |
| [`multi-stage-pipeline.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-stage-pipeline.yaml) | Two-stage pipeline coordinating through the shared job mount | You need file-based stage-to-stage handoff |
| [`fairseq-preprocess.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/fairseq-preprocess.yaml) | CPU-heavy NLP data preprocessing with parallel workers | You need a CPU-bound data preprocessing pipeline |

If you want the fastest end-to-end cluster example, start with [`llm-curl-workflow-workdir.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llm-curl-workflow-workdir.yaml). If you want a source-mounted development workflow, start with [`dev-python-app.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/dev-python-app.yaml). If you are new to `hpc-compose` and want the absolute simplest file, start with [`minimal-batch.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/minimal-batch.yaml).

## Choose an example

```
Do you need GPUs?
├─ No
│  ├─ Just trying hpc-compose?       → minimal-batch.yaml
│  ├─ MPI workload?                   → mpi-hello.yaml
│  ├─ CPU-heavy data preprocessing?   → fairseq-preprocess.yaml
│  ├─ Multi-service with ordering?    → app-redis-worker.yaml
│  ├─ Database-backed pipeline?       → postgres-etl.yaml
│  └─ Multi-stage file pipeline?      → multi-stage-pipeline.yaml
└─ Yes
   ├─ LLM inference (llama.cpp)?      → llm-curl-workflow.yaml
   ├─ LLM inference (vLLM only)?      → vllm-openai.yaml
   ├─ LLM inference (vLLM + uv app)?  → vllm-uv-worker.yaml
   ├─ GPU training with artifacts?    → training-checkpoints.yaml
   └─ GPU app + dependent service?    → llama-app.yaml

Iterative development with mounted source? → dev-python-app.yaml
```

## One-time setup vs normal runs

| Cadence | Typical commands |
| --- | --- |
| once per new spec | copy or `init` the example, then adjust paths, models, and resource settings |
| early validation while adapting | `hpc-compose validate -f ...` and `hpc-compose inspect --verbose -f ...` |
| normal run | `hpc-compose submit --watch -f ...` |
| troubleshooting | `hpc-compose preflight -f ...`, `hpc-compose prepare -f ...`, `hpc-compose render -f ... --output ...` |

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
- Uses `x-enroot.prepare.commands` only for slower-changing Python dependencies.
- The mounted source tree is the per-run part; the prepared dependency layer is the cached part.

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
- Expects a model file at `models/model.gguf`; see [`examples/models/README.md`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/models/README.md).

### `minimal-batch.yaml`

- Best reference when you want the absolute simplest single-service batch job.
- No dependencies, no GPU, no prepare step.
- Good starting point for understanding the basic file format.

### `training-checkpoints.yaml`

- Best reference for GPU training workflows that produce offline artifacts.
- Writes checkpoints to a shared storage volume.
- Pairs well with `x-slurm.artifacts` when you also want tracked result export under `.hpc-compose/<job-id>/artifacts/`.
- Uses `x-enroot.prepare` implicitly through the PyTorch base image.

### `postgres-etl.yaml`

- Best reference for database-backed batch processing.
- PostgreSQL with TCP readiness on port 5432.
- ETL service waits for the database with `depends_on: service_healthy`.
- Uses `x-enroot.prepare.commands` to install `psycopg2`.

### `vllm-openai.yaml`

- Best reference for vLLM-based model serving as an alternative to llama.cpp.
- Serves an OpenAI-compatible API on localhost.
- In-job client sends a test request and signals completion via `/hpc-compose/job`.

### `vllm-uv-worker.yaml`

- Best reference for a common LLM app stack: one vLLM server plus a source-mounted Python worker.
- Caches `uv` into the worker image through `x-enroot.prepare.commands`.
- Runs the mounted worker code on each submit with `uv run worker.py`.
- Good starting point when your Python app code changes faster than the base worker image.

### `mpi-hello.yaml`

- Best reference for MPI workloads on a single node.
- Compiles a C hello-world program with Open MPI during prepare.
- Runs with `mpirun -np 4` inside the container.

### `multi-stage-pipeline.yaml`

- Best reference for file-based stage-to-stage coordination.
- Producer writes a CSV to `/hpc-compose/job/output.csv`.
- Consumer uses `depends_on: service_healthy` with log-based readiness to wait for data.
- Shows the recommended pattern for multi-step batch workflows.

### `fairseq-preprocess.yaml`

- Best reference for CPU-heavy data preprocessing pipelines.
- Reads raw `.txt` files from shared storage and writes JSONL output.
- Uses Python's `ProcessPoolExecutor` for parallel processing.
- No GPU, no dependencies, no prepare step — pure CPU batch work.

## Related docs

- [Runbook](runbook.md)
- [Spec reference](spec-reference.md)
- [Docker Compose migration](docker-compose-migration.md)
