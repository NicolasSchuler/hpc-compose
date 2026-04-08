# Examples

These examples are the fastest way to understand the intended `hpc-compose` workflows and adapt them to a real application.

For almost every example, the normal run is:

```bash
hpc-compose submit --watch -f examples/<example>.yaml
```

Use the debugging flow (`validate`, `inspect`, `preflight`, `prepare`) when you are wiring up the example for the first time or isolating a failure.

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
| [`llama-uv-worker.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llama-uv-worker.yaml) | llama.cpp serving plus a source-mounted Python worker executed through `uv` | You want the GGUF server + mounted worker pattern |
| [`minimal-batch.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/minimal-batch.yaml) | Single service, no dependencies, no GPU, no prepare | You want the simplest possible starting point |
| [`multi-node-mpi.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-node-mpi.yaml) | One primary-node helper plus one allocation-wide distributed CPU step | You want a minimal multi-node pattern without adding orchestration |
| [`multi-node-torchrun.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-node-torchrun.yaml) | Allocation-wide torchrun launch using the primary node as rendezvous | You want a multi-node GPU training starting point |
| [`training-checkpoints.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/training-checkpoints.yaml) | GPU training with checkpoints written to shared storage | You need a batch training workflow with artifact collection |
| [`training-resume.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/training-resume.yaml) | GPU training with a shared resume directory and attempt-aware checkpoints | You need restart-safe checkpoint semantics across requeues or repeated submissions |
| [`postgres-etl.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/postgres-etl.yaml) | PostgreSQL plus a Python data processing job | You need a database-backed batch pipeline |
| [`restart-policy.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/restart-policy.yaml) | Per-service `restart_on_failure` with bounded retries and a rolling-window crash-loop guard | You need transient-failure retries without letting one service spin forever |
| [`vllm-openai.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/vllm-openai.yaml) | vLLM serving with an in-job Python client | You want vLLM-based inference instead of llama.cpp |
| [`vllm-uv-worker.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/vllm-uv-worker.yaml) | vLLM serving plus a source-mounted Python worker executed through `uv` | You want a common LLM stack with mounted app code |
| [`mpi-hello.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/mpi-hello.yaml) | MPI hello world compiled and run with Open MPI | You need an MPI workload |
| [`multi-stage-pipeline.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-stage-pipeline.yaml) | Two-stage pipeline coordinating through the shared job mount | You need file-based stage-to-stage handoff |
| [`fairseq-preprocess.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/fairseq-preprocess.yaml) | CPU-heavy NLP data preprocessing with parallel workers | You need a CPU-bound data preprocessing pipeline |

## Which example should I start from?

- Start with [`minimal-batch.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/minimal-batch.yaml) if you are new to `hpc-compose` and want the smallest possible file.
- Start with [`multi-node-mpi.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-node-mpi.yaml) if you need one distributed step plus small helper services on the primary node.
- Start with [`multi-node-torchrun.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/multi-node-torchrun.yaml) if you need a torchrun-style rendezvous pattern across multiple nodes.
- Start with [`dev-python-app.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/dev-python-app.yaml) if you want a source-mounted development loop.
- Start with [`llm-curl-workflow-workdir.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llm-curl-workflow-workdir.yaml) if you want the fastest real-cluster GPU inference example.
- Start with [`training-checkpoints.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/training-checkpoints.yaml) if you need a GPU training job with checkpoint output.
- Start with [`training-resume.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/training-resume.yaml) if you need resume-aware checkpoints on shared storage.
- Start with [`restart-policy.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/restart-policy.yaml) if you need a clear starting point for `restart_on_failure` tuning and `status`-visible retry budgets.
- Start with [`app-redis-worker.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/app-redis-worker.yaml) or [`postgres-etl.yaml`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/postgres-etl.yaml) if your workload depends on multi-service startup ordering.

Companion notes for the more involved examples live alongside the example assets:

- [`examples/llm-curl/README.md`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llm-curl/README.md)
- [`examples/llama-uv-worker/README.md`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/llama-uv-worker/README.md)
- [`examples/vllm-uv-worker/README.md`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/vllm-uv-worker/README.md)
- [`examples/models/README.md`](https://github.com/NicolasSchuler/hpc-compose/blob/main/examples/models/README.md)

## Adaptation checklist

1. Copy the closest example to your own `compose.yaml`, or run `hpc-compose init --template <name> --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml`.
2. Set `x-slurm.cache_dir` to a path visible from both the login node and the compute nodes.
3. Replace the example `image`, `command`, `environment`, and `volumes` with your workload.
4. Keep active source in `volumes` and keep slower-changing dependency installation in `x-enroot.prepare.commands`.
5. Add `readiness` to services that must be reachable before dependents continue.
6. Adjust top-level or per-service `x-slurm` settings for your cluster.
7. Run the debugging flow before the first submit when you need to confirm planning, prerequisites, or cache behavior.

## Related docs

- [Execution model](execution-model.md)
- [Runbook](runbook.md)
- [Spec reference](spec-reference.md)
- [Docker Compose migration](docker-compose-migration.md)
