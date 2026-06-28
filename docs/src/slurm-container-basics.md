# Slurm and Container Basics

This page is for users who know shell scripts, Python jobs, or Docker images, but are new to Slurm and HPC container runtimes.

It is not a Slurm administration guide. The goal is to explain the vocabulary you will see in generated `hpc-compose` scripts and in cluster error messages.

## The Short Mental Model

The important point is that `hpc-compose` does not replace Slurm. It writes one inspectable Slurm batch script and uses Slurm to run the planned services inside one allocation. For the full spec->sbatch->srun pipeline, see [Execution Model](execution-model.md).

## Slurm Terms In Plain Language

| Term | Meaning for `hpc-compose` users |
| --- | --- |
| Login node | The machine where you edit files, run `plan`, run `preflight`, and submit jobs. Do not run long compute work here. |
| Compute node | A worker machine where Slurm runs your job after it starts. |
| Partition | A named queue or resource pool. Sites often use partitions to separate CPU, GPU, debug, and large jobs. |
| Job | A submitted unit of work managed by Slurm. `hpc-compose up` submits one job. |
| Allocation | The nodes, CPUs, memory, GPUs, and wall time reserved for a job. |
| Batch script | A shell script submitted with `sbatch`. It contains `#SBATCH` directives and normal shell commands. |
| Job step | A launched process group inside the allocation. `hpc-compose` launches services as `srun` steps. |
| Task | Usually one process or rank. More `ntasks` means more processes, not more CPU threads per process. |
| `cpus_per_task` | CPU threads requested for each task. This is common for threaded Python, OpenMP, or data-loader-heavy jobs. |
| `gres` | Slurm's generic resource request field, commonly used for GPUs. |

If you only remember one distinction: `sbatch` gets the allocation; `srun` starts work inside it.

## A Minimal `sbatch` Script

A traditional Slurm script often looks like this:

```bash
#!/usr/bin/env bash
#SBATCH --job-name=hello-slurm
#SBATCH --partition=<partition>
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G

set -euo pipefail

hostname
python -c 'print("hello from a Slurm job")'
```

Submit it from a Slurm login node:

```bash
sbatch hello.sbatch
```

`sbatch` returns a job id. The job may wait in the queue before it starts, and Slurm normally writes batch output to a file such as `slurm-<job-id>.out` unless the script or site policy sets another output path.

## Where `hpc-compose` Fits

The equivalent `hpc-compose` starting point is a spec:

```yaml
name: hello-slurm

x-slurm:
  job_name: hello-slurm
  partition: <partition>
  time: "00:10:00"
  cpus_per_task: 2
  mem: 4G

services:
  app:
    image: python:3.11-slim
    command: python -c "import socket; print('hello from', socket.gethostname())"
```

Preview the generated Slurm script before submitting:

```bash
hpc-compose plan -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
```

Run it on a supported Slurm login node:

```bash
hpc-compose up -f compose.yaml
```

`up` runs preflight checks, prepares missing runtime artifacts, renders the batch script, calls `sbatch`, records tracked job metadata, and follows scheduler/log output.

## How YAML Maps To Slurm

`hpc-compose` translates top-level and service `x-slurm` fields into `#SBATCH` directives and `srun` arguments. For the exact field-by-field mapping and the full command surface (`sbatch`, `srun`, `render`, `up`, tracked follow-ups), see [Spec Reference](spec-reference.md) and [CLI Reference](cli-reference.md). Prefer first-class fields when they exist; use raw `submit_args` or `extra_srun_args` only for site-specific options that `hpc-compose` does not model directly.

When debugging, inspect the generated script:

```bash
hpc-compose plan --show-script -f compose.yaml
```

If a job was submitted but failed before service logs appeared, inspect Slurm state and batch output through:

```bash
hpc-compose debug -f compose.yaml
```

## Pyxis And Enroot Basics

Slurm itself is the scheduler. Container support depends on what the cluster installed. The default `runtime.backend: pyxis` path uses the Pyxis Slurm plugin plus the Enroot unprivileged runtime, and `hpc-compose` maps each service into a generated `srun --container-*` launch.

For the Pyxis support check, the Enroot/Apptainer/Singularity/host tooling differences, and how to choose a backend, see [Runtime Backends](runtime-backends.md).

## Why Shared Storage Matters

`hpc-compose prepare` can run before the Slurm job starts, but services run later on compute nodes, so the resolved runtime cache must be visible from both places. For why the cache must live on shared storage and the operational cache configuration, see [Execution Model](execution-model.md) and [Cache Management](cache-management.md).

The same rule applies to host paths mounted through `volumes`: the compute node must be able to read the path when the service starts.

## Small Checks That Explain A Lot

These commands are useful in tiny smoke tests:

```bash
hostname
env | grep '^SLURM_' | sort
python -c 'import socket; print(socket.gethostname())'
cat /etc/os-release
```

Inside a container, `cat /etc/os-release` should describe the container image. Outside the container, it describes the host. That simple distinction helps diagnose whether a command is running where you expect.

## Common Beginner Mistakes

| Symptom | Likely misunderstanding | Next step |
| --- | --- | --- |
| `plan` looks fine but `up` fails immediately | Static validation is not the same as cluster readiness. | Run `hpc-compose debug -f compose.yaml --preflight` on the login node. |
| `srun` does not accept `--container-image` | Pyxis is not available or not loaded in Slurm. | Read [Runtime Backends](runtime-backends.md) and use the site-supported backend. |
| Cache warnings mention local paths | The cache path is not shared between login and compute nodes. | Configure `x-slurm.cache_dir` or `setup --cache-dir` with shared storage. |
| A GPU job waits longer than expected | The request may be larger than available idle resources. | Check site queue policy and start with the smallest useful request. |
| More CPUs were requested but only one process appears | `cpus_per_task` adds threads per task; it does not create more tasks. | Use `ntasks` for more processes/ranks, and make the application use them. |
| Docker Compose `ports` or service DNS do not work | This is one Slurm allocation, not a Docker Compose network. | See the networking stance in [Execution Model](execution-model.md). |

## Further Reading

- [Slurm Quick Start User Guide](https://slurm.schedmd.com/quickstart.html)
- [Slurm `sbatch` reference](https://slurm.schedmd.com/sbatch.html)
- [Slurm job launch design notes](https://slurm.schedmd.com/job_launch.html)
- [Slurm containers guide](https://slurm.schedmd.com/containers.html)
- [NVIDIA Pyxis](https://github.com/NVIDIA/pyxis)
- [NVIDIA Enroot](https://github.com/NVIDIA/enroot)

## Read Next

- [Why hpc-compose](why-hpc-compose.md)
- [Runtime Backends](runtime-backends.md)
- [Execution Model](execution-model.md)
- [Slurm Capability Scope](slurm-capability-scope.md)
