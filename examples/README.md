# Examples Directory

Runnable hpc-compose example specs. Each is a starting point you can copy and adapt — inspect any of them statically with `hpc-compose plan -f examples/<file>`.

For the annotated catalog (selection guidance, prerequisites, success signals, the example matrix, and adaptation tips), use the book page:

- [Published examples guide](https://nicolasschuler.github.io/hpc-compose/examples.html)
- [Book source in this repo](../docs/src/examples.md)

Promoted starting points: [`minimal-batch.yaml`](minimal-batch.yaml), [`app-redis-worker.yaml`](app-redis-worker.yaml), [`llm-curl-workflow-workdir.yaml`](llm-curl-workflow-workdir.yaml), [`training-resume.yaml`](training-resume.yaml).

## Starters and single-host development

- [`minimal-batch.yaml`](minimal-batch.yaml)
- [`app-redis-worker.yaml`](app-redis-worker.yaml)
- [`dev-python-app.yaml`](dev-python-app.yaml)
- [`dev-python-smoke.yaml`](dev-python-smoke.yaml)
- [`cuda-probe.yaml`](cuda-probe.yaml)
- [`restart-policy.yaml`](restart-policy.yaml)

## Model serving and LLM workflows

- [`llama-app.yaml`](llama-app.yaml)
- [`llama-uv-worker.yaml`](llama-uv-worker.yaml)
- [`vllm-openai.yaml`](vllm-openai.yaml)
- [`vllm-uv-worker.yaml`](vllm-uv-worker.yaml)
- [`llm-curl-workflow.yaml`](llm-curl-workflow.yaml)
- [`llm-curl-workflow-workdir.yaml`](llm-curl-workflow-workdir.yaml)
- [`jupyter.yaml`](jupyter.yaml)

## Multi-node training

- [`multi-node-accelerate.yaml`](multi-node-accelerate.yaml)
- [`multi-node-deepspeed.yaml`](multi-node-deepspeed.yaml)
- [`multi-node-horovod.yaml`](multi-node-horovod.yaml)
- [`multi-node-jax.yaml`](multi-node-jax.yaml)
- [`multi-node-mpi.yaml`](multi-node-mpi.yaml)
- [`multi-node-partitioned.yaml`](multi-node-partitioned.yaml)
- [`multi-node-torchrun.yaml`](multi-node-torchrun.yaml)
- [`nccl-tests.yaml`](nccl-tests.yaml)

## MPI and HPC fabrics

- [`mpi-hello.yaml`](mpi-hello.yaml)
- [`mpi-pmix-v4-host-mpi.yaml`](mpi-pmix-v4-host-mpi.yaml)
- [`flux-nested.yaml`](flux-nested.yaml)

## Distributed frameworks

- [`ray-head-workers.yaml`](ray-head-workers.yaml)
- [`ray-symmetric.yaml`](ray-symmetric.yaml)
- [`dask-scheduler-workers.yaml`](dask-scheduler-workers.yaml)
- [`spark-standalone.yaml`](spark-standalone.yaml)

## Pipelines, ETL, and workflow-engine bridges

- [`multi-stage-pipeline.yaml`](multi-stage-pipeline.yaml)
- [`pipeline-dag.yaml`](pipeline-dag.yaml)
- [`postgres-etl.yaml`](postgres-etl.yaml)
- [`fairseq-preprocess.yaml`](fairseq-preprocess.yaml)
- [`nextflow-bridge.yaml`](nextflow-bridge.yaml)
- [`snakemake-bridge.yaml`](snakemake-bridge.yaml)

## Training lifecycle: checkpoints, resume, sweeps

- [`training-checkpoints.yaml`](training-checkpoints.yaml)
- [`training-resume.yaml`](training-resume.yaml)
- [`training-sweep.yaml`](training-sweep.yaml)
- [`training-tensorboard.yaml`](training-tensorboard.yaml)
- [`canary-right-size.yaml`](canary-right-size.yaml)

## Cross-job rendezvous

- [`rendezvous-model-server.yaml`](rendezvous-model-server.yaml)
- [`rendezvous-client.yaml`](rendezvous-client.yaml)
