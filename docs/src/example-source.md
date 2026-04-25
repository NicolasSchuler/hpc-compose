# Example Source

This appendix embeds the runnable repository example YAML files directly from `examples/`.

The repository examples default `x-slurm.cache_dir` to `/cluster/shared/hpc-compose-cache` so they validate directly. Before running one on a real cluster, set `CACHE_DIR` to a shared path visible from both the submission host and the compute nodes, or place the same assignment in `.env` next to the copied spec:

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
```

## App Redis Worker

Source: `examples/app-redis-worker.yaml`

```yaml
{{#include ../../examples/app-redis-worker.yaml}}
```

## Dev Python App

Source: `examples/dev-python-app.yaml`

```yaml
{{#include ../../examples/dev-python-app.yaml}}
```

## Fairseq Preprocess

Source: `examples/fairseq-preprocess.yaml`

```yaml
{{#include ../../examples/fairseq-preprocess.yaml}}
```

## Llama App

Source: `examples/llama-app.yaml`

```yaml
{{#include ../../examples/llama-app.yaml}}
```

## Llama UV Worker

Source: `examples/llama-uv-worker.yaml`

```yaml
{{#include ../../examples/llama-uv-worker.yaml}}
```

## LLM Curl Workflow

Source: `examples/llm-curl-workflow.yaml`

```yaml
{{#include ../../examples/llm-curl-workflow.yaml}}
```

## LLM Curl Workflow Workdir

Source: `examples/llm-curl-workflow-workdir.yaml`

```yaml
{{#include ../../examples/llm-curl-workflow-workdir.yaml}}
```

## Minimal Batch

Source: `examples/minimal-batch.yaml`

```yaml
{{#include ../../examples/minimal-batch.yaml}}
```

## MPI Hello

Source: `examples/mpi-hello.yaml`

```yaml
{{#include ../../examples/mpi-hello.yaml}}
```

## MPI PMIx v4 Host MPI

Source: `examples/mpi-pmix-v4-host-mpi.yaml`

```yaml
{{#include ../../examples/mpi-pmix-v4-host-mpi.yaml}}
```

## Multi Node MPI

Source: `examples/multi-node-mpi.yaml`

```yaml
{{#include ../../examples/multi-node-mpi.yaml}}
```

## Multi Node Partitioned

Source: `examples/multi-node-partitioned.yaml`

```yaml
{{#include ../../examples/multi-node-partitioned.yaml}}
```

## Multi Node Torchrun

Source: `examples/multi-node-torchrun.yaml`

```yaml
{{#include ../../examples/multi-node-torchrun.yaml}}
```

## Multi Node Deepspeed

Source: `examples/multi-node-deepspeed.yaml`

```yaml
{{#include ../../examples/multi-node-deepspeed.yaml}}
```

## Multi Node Accelerate

Source: `examples/multi-node-accelerate.yaml`

```yaml
{{#include ../../examples/multi-node-accelerate.yaml}}
```

## Multi Node Horovod

Source: `examples/multi-node-horovod.yaml`

```yaml
{{#include ../../examples/multi-node-horovod.yaml}}
```

## Multi Node Jax

Source: `examples/multi-node-jax.yaml`

```yaml
{{#include ../../examples/multi-node-jax.yaml}}
```

## Nccl Tests

Source: `examples/nccl-tests.yaml`

```yaml
{{#include ../../examples/nccl-tests.yaml}}
```

## Ray Symmetric

Source: `examples/ray-symmetric.yaml`

```yaml
{{#include ../../examples/ray-symmetric.yaml}}
```

## Ray Head Workers

Source: `examples/ray-head-workers.yaml`

```yaml
{{#include ../../examples/ray-head-workers.yaml}}
```

## Dask Scheduler Workers

Source: `examples/dask-scheduler-workers.yaml`

```yaml
{{#include ../../examples/dask-scheduler-workers.yaml}}
```

## Spark Standalone

Source: `examples/spark-standalone.yaml`

```yaml
{{#include ../../examples/spark-standalone.yaml}}
```

## Flux Nested

Source: `examples/flux-nested.yaml`

```yaml
{{#include ../../examples/flux-nested.yaml}}
```

## Nextflow Bridge

Source: `examples/nextflow-bridge.yaml`

```yaml
{{#include ../../examples/nextflow-bridge.yaml}}
```

## Snakemake Bridge

Source: `examples/snakemake-bridge.yaml`

```yaml
{{#include ../../examples/snakemake-bridge.yaml}}
```

## Multi Stage Pipeline

Source: `examples/multi-stage-pipeline.yaml`

```yaml
{{#include ../../examples/multi-stage-pipeline.yaml}}
```

## Pipeline DAG

Source: `examples/pipeline-dag.yaml`

```yaml
{{#include ../../examples/pipeline-dag.yaml}}
```

## Postgres ETL

Source: `examples/postgres-etl.yaml`

```yaml
{{#include ../../examples/postgres-etl.yaml}}
```

## Restart Policy

Source: `examples/restart-policy.yaml`

```yaml
{{#include ../../examples/restart-policy.yaml}}
```

## Training Checkpoints

Source: `examples/training-checkpoints.yaml`

```yaml
{{#include ../../examples/training-checkpoints.yaml}}
```

## Training Resume

Source: `examples/training-resume.yaml`

```yaml
{{#include ../../examples/training-resume.yaml}}
```

## vLLM OpenAI

Source: `examples/vllm-openai.yaml`

```yaml
{{#include ../../examples/vllm-openai.yaml}}
```

## vLLM UV Worker

Source: `examples/vllm-uv-worker.yaml`

```yaml
{{#include ../../examples/vllm-uv-worker.yaml}}
```
