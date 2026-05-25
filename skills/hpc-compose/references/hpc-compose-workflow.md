# hpc-compose Workflow Reference

Use this reference for hpc-compose command sequencing, spec migration, verification gates, and troubleshooting. Prefer current primary docs when available:

- Repository: https://github.com/NicolasSchuler/hpc-compose
- Published docs: https://nicolasschuler.github.io/hpc-compose/
- Quickstart: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/quickstart.md
- Task guide: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/task-guide.md
- Examples: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/examples.md
- Spec reference: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/spec-reference.md
- Runtime backends: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/runtime-backends.md
- Supported Slurm model: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/supported-slurm-model.md
- Docker Compose migration: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/docker-compose-migration.md
- Troubleshooting: https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/troubleshooting.md

## Contents

- [Safe Authoring Path](#safe-authoring-path)
- [First Cluster Path](#first-cluster-path)
- [Cache Directory](#cache-directory)
- [Runtime Backends](#runtime-backends)
- [Choosing A Template Or Example](#choosing-a-template-or-example)
- [Docker Compose Migration](#docker-compose-migration)
- [Slurm Model Boundary](#slurm-model-boundary)
- [Readiness And Dependencies](#readiness-and-dependencies)
- [Troubleshooting Ladder](#troubleshooting-ladder)

## Safe Authoring Path

Use this path before touching Slurm:

```bash
hpc-compose new --template minimal-batch --name my-app --output compose.yaml
hpc-compose plan -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
```

For an existing or adapted spec:

```bash
hpc-compose validate -f compose.hpc.yaml
hpc-compose validate -f compose.hpc.yaml --strict-env
hpc-compose plan -f compose.hpc.yaml
hpc-compose plan --show-script -f compose.hpc.yaml
hpc-compose inspect --verbose -f compose.hpc.yaml
```

`plan` is the safest static command: it validates and renders intent without importing images, preparing artifacts, writing a batch script, or calling `sbatch`.

## First Cluster Path

Run these only on a supported Linux Slurm submission host:

```bash
hpc-compose preflight -f compose.hpc.yaml
hpc-compose debug -f compose.hpc.yaml --preflight
hpc-compose doctor cluster-report
```

Submit only with user approval:

```bash
hpc-compose up -f compose.hpc.yaml
```

After a tracked run:

```bash
hpc-compose jobs list
hpc-compose status -f compose.hpc.yaml
hpc-compose ps -f compose.hpc.yaml
hpc-compose logs -f compose.hpc.yaml --follow
hpc-compose debug -f compose.hpc.yaml
```

## Cache Directory

Real clusters need a cache visible to both the submission host and compute nodes because image preparation happens before the job starts and compute nodes reuse prepared artifacts later.

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
hpc-compose setup --profile-name dev --cache-dir "$CACHE_DIR" --default-profile dev --non-interactive
```

Do not use local temporary paths for `x-slurm.cache_dir`: `/tmp`, `/var/tmp`, `/private/tmp`, and `/dev/shm` are unsafe for shared prepare/reuse.

## Runtime Backends

Default to:

```yaml
runtime:
  backend: pyxis
```

Backend selection:

| Backend | Use when | Check |
| --- | --- | --- |
| `pyxis` | Slurm has Pyxis and Enroot; best HAICORE default. | `srun --help | grep container-image` |
| `apptainer` | Site standardizes on Apptainer/SIF. | `apptainer --version` |
| `singularity` | Older site has Singularity only. | `singularity --version` |
| `host` | No container image; module/host software workflow. | Commands and modules exist on compute nodes |

Run `hpc-compose preflight -f <file>` to check selected backend tools.

## Choosing A Template Or Example

Use `hpc-compose new --list-templates` and `hpc-compose new --describe-template <name>` when the CLI is available.

Fast starts:

| Workload | Starting point |
| --- | --- |
| Small first run | `minimal-batch` |
| Single-node app plus helper | `app-redis-worker.yaml` |
| Python source-mounted development | `dev-python-app.yaml` |
| Finite Python smoke | `dev-python-smoke.yaml` |
| LLM service plus client | `llm-curl-workflow-workdir.yaml`, `llama-app.yaml`, `vllm-openai.yaml` |
| Checkpointed training | `training-checkpoints.yaml`, `training-resume.yaml` |
| Distributed PyTorch | `multi-node-torchrun.yaml` |
| DeepSpeed | `multi-node-deepspeed.yaml` |
| Hugging Face Accelerate | `multi-node-accelerate.yaml` |
| JAX distributed | `multi-node-jax.yaml` |
| MPI | `multi-node-mpi.yaml`, `mpi-hello.yaml` |
| Fabric/NCCL probe | `nccl-tests.yaml` |
| Ray, Dask, Spark, Flux | matching framework examples |
| Nextflow or Snakemake wrapper | `nextflow-bridge.yaml`, `snakemake-bridge.yaml` |
| Hyperparameter trials | `training-sweep.yaml` |

## Docker Compose Migration

Allowed or mostly compatible:

| Docker Compose | hpc-compose |
| --- | --- |
| `image` | `image` |
| `command` | `command` |
| `entrypoint` | `entrypoint` |
| `environment` | `environment` |
| `volumes` | host path bind mounts |
| `depends_on` | list or map with `service_started`, `service_healthy`, or `service_completed_successfully` |
| `working_dir` | `working_dir` with explicit command/entrypoint |

Replace:

| Docker Compose feature | hpc-compose approach |
| --- | --- |
| `build:` | `image:` plus `x-runtime.prepare.commands` and optional `x-runtime.prepare.mounts` |
| `ports:` | Remove; use host networking inside the allocation |
| service DNS names | `127.0.0.1` for same-node helpers, allocation metadata for distributed |
| `networks` / `network_mode` | Remove |
| `restart:` | `services.<name>.x-slurm.failure_policy` |
| `deploy:` or resource limits | top-level or service-level `x-slurm` |
| `healthcheck:` | explicit `readiness` unless the constrained healthcheck subset fits |

Minimal migrated shape:

```yaml
version: "1"
name: my-app

runtime:
  backend: pyxis

x-slurm:
  job_name: my-app
  partition: normal
  time: "01:00:00"
  cpus_per_task: 4
  mem: 8G
  cache_dir: ${CACHE_DIR:-/path/to/shared/hpc-compose-cache}

services:
  app:
    image: python:3.11-slim
    volumes:
      - ./:/workspace
    working_dir: /workspace
    command: python -m my_app
    x-runtime:
      prepare:
        commands:
          - pip install --no-cache-dir -r /workspace/requirements.txt
```

## Slurm Model Boundary

hpc-compose intentionally models one application as one Slurm allocation with one or more service steps.

Use first-class fields for supported Slurm settings:

```yaml
x-slurm:
  partition: normal
  account: my-account
  time: "02:00:00"
  nodes: 1
  ntasks: 1
  cpus_per_task: 8
  mem: 32G
  gres: gpu:full:1
```

Use raw pass-through only for site-specific options:

```yaml
x-slurm:
  submit_args:
    - "--reservation=my-reservation"
```

Unsupported or out-of-scope includes admin-plane Slurm management, reservations lifecycle, federation/multi-cluster control, generic `scontrol` mutation, broad Slurm reporting, Docker Compose `build`, `ports`, custom networks, `restart`, and `deploy`.

## Readiness And Dependencies

Use `depends_on` with `condition: service_healthy` when a dependent must wait for a real readiness probe.

```yaml
services:
  redis:
    image: redis:7
    command: redis-server --save "" --appendonly no
    readiness:
      type: tcp
      host: 127.0.0.1
      port: 6379
      timeout_seconds: 30

  worker:
    image: python:3.11-slim
    depends_on:
      redis:
        condition: service_healthy
```

Use `service_completed_successfully` for one-shot DAG stages such as preprocess -> train -> postprocess.

## Troubleshooting Ladder

Start with:

```bash
hpc-compose validate -f compose.hpc.yaml
hpc-compose validate -f compose.hpc.yaml --strict-env
hpc-compose plan --verbose -f compose.hpc.yaml
hpc-compose debug -f compose.hpc.yaml --preflight
```

Common mappings:

| Symptom | Likely next step |
| --- | --- |
| Missing `sbatch`, `srun`, `enroot`, `apptainer`, or `singularity` | `hpc-compose debug -f <file> --preflight` |
| `srun` lacks `--container-image` | `hpc-compose doctor cluster-report`; confirm Pyxis or switch backend |
| Cache warning/error | choose shared cache and rerun preflight |
| Service order wrong | `hpc-compose plan --explain --verbose -f <file>` |
| No service logs | `hpc-compose debug -f <file>` |
| Readiness never passes | inspect service logs, use tighter readiness, make smoke finite |

When output contains secrets, avoid pasting full verbose plan/debug logs back to the user. Summarize relevant lines.
