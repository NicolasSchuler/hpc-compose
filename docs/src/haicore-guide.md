# HAICORE@KIT Guide

This page collects `hpc-compose` configuration notes for HAICORE@KIT. It is a practical starting point, not a replacement for the official [NHR@KIT HAICORE documentation](https://www.nhr.kit.edu/userdocs/haicore/).

Before long or expensive runs, re-check current HAICORE policy pages for partitions, quotas, GPU limits, container requirements, and filesystem lifetime rules.

## Where Commands Run

HAICORE is accessed through the login host documented by NHR@KIT:

```bash
ssh <username>@haicore.scc.kit.edu
```

Use the login node for editing, Git operations, `hpc-compose plan`, `hpc-compose preflight`, image preparation, and Slurm job management. Run compute work through Slurm with `hpc-compose up`, `sbatch`, or site-approved interactive Slurm commands.

Do not treat the login node as a place for long Python training, GPU work, data conversion, or large preprocessing jobs. Those belong inside a Slurm allocation.

## HAICORE Slurm Settings To Know

The current HAICORE batch-system documentation describes Slurm partitions named `normal` and `advanced`. The `normal` partition is the general starting point; `advanced` requires special permission and allows larger jobs.

Common settings you will map into `hpc-compose`:

| HAICORE / Slurm setting | `hpc-compose` field | Notes |
| --- | --- | --- |
| Partition | `x-slurm.partition` | Usually start with the site-documented general partition. |
| Account/project | `x-slurm.account` | Use the account string assigned by the site or project. |
| Wall time | `x-slurm.time` | Keep smoke tests short; request only what the run needs. |
| Nodes | `x-slurm.nodes` | `normal` is documented for single-node jobs; confirm before multi-node runs. |
| Tasks | `x-slurm.ntasks`, service `x-slurm.ntasks` | Process/rank count. |
| CPUs per task | `x-slurm.cpus_per_task`, service `x-slurm.cpus_per_task` | CPU threads per process/rank. |
| Memory | `x-slurm.mem` | Scheduler/runtime memory request, not storage. |
| Full GPUs | `x-slurm.gres` or service `x-slurm.gres` | HAICORE examples use `gpu:full:N` style requests. |
| MIG GPUs | `x-slurm.gres` or service `x-slurm.gres` | HAICORE documents MIG profiles such as `gpu:1g.5gb:1`; confirm current names. |
| Constraints | `x-slurm.constraint` or `x-slurm.submit_args` | HAICORE documents constraints such as `LSDF` and `BEEOND`. |

Example single-node GPU starting point:

```yaml
name: haicore-smoke

x-slurm:
  job_name: haicore-smoke
  partition: normal
  account: <account>
  time: "00:10:00"
  nodes: 1
  cpus_per_task: 4
  mem: 16G
  gres: gpu:full:1
  cache_dir: <workspace-path>/hpc-compose-cache

services:
  app:
    image: python:3.11-slim
    command: python -c "import os, socket; print(socket.gethostname()); print(os.environ.get('SLURM_JOB_ID'))"
```

Preview before submitting:

```bash
hpc-compose plan -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
hpc-compose preflight -f compose.yaml
```

## Workspaces And Storage

HAICORE documents several storage types. For `hpc-compose`, the most important distinction is shared persistent-enough storage versus job-local temporary storage.

| Storage | Use with `hpc-compose` | Avoid using it for |
| --- | --- | --- |
| `$HOME` | Small configuration, source code, shell setup, credentials handled under site policy. | Large image caches, datasets, checkpoints, or logs from many jobs. |
| Workspace | `x-slurm.cache_dir`, Enroot data/cache, datasets, model files, run logs, artifacts, checkpoints. | Data that must be backed up elsewhere; workspaces are documented as not backed up and time-limited. |
| `$TMPDIR` | Fast node-local temporary files created and consumed within one job. | `x-slurm.cache_dir` or anything needed by login-node prepare and later compute-node runtime. |
| BeeOND | Job-local shared scratch across nodes when explicitly requested. | Long-term cache, persistent checkpoints, or files needed after the job unless copied out. |

Create and locate a workspace with HAICORE's workspace tools:

```bash
ws_allocate <workspace-name> <duration>
ws_find <workspace-name>
ws_list
ws_extend <workspace-name> <duration>
```

Use the path from `ws_find` for the cache:

```bash
export CACHE_DIR=<workspace-path>/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
```

Then set it in your spec:

```yaml
x-slurm:
  cache_dir: ${CACHE_DIR}
```

The official HAICORE filesystem page documents workspace lifetime, extension limits, quotas, and backup policy. Treat workspace expiration as operational risk: long-running projects should have a habit of checking `ws_list` and copying durable results to the correct long-term location.

## Containers On HAICORE

The official HAICORE container documentation says native Docker and rootless Docker are not supported on the HPC systems. The relevant paths are site-supported HPC runtimes, including Enroot/Pyxis and Apptainer.

For the default `hpc-compose` backend:

```yaml
runtime:
  backend: pyxis
```

Validate Pyxis support on the login node:

```bash
srun --help | grep container-image
hpc-compose preflight -f compose.yaml
```

HAICORE documents Pyxis as the Slurm integration for Enroot and lists container options such as `--container-image`, `--container-name`, `--container-mounts`, `--container-mount-home`, `--container-writable`, and `--container-remap-root`.

The HAICORE docs also list site-required Pyxis mounts for Slurm integration. Because mount paths are site policy and can change, inspect the current HAICORE container page before copying them into a spec. When needed, pass site-specific Pyxis flags through service-level `extra_srun_args`:

```yaml
services:
  app:
    image: python:3.11-slim
    command: python -c "print('hello from HAICORE')"
    x-slurm:
      extra_srun_args:
        - "--container-mounts=<site-required-mounts>"
```

If the cluster recommends Apptainer for your workflow or Pyxis is not available in `srun`, choose the corresponding backend:

```yaml
runtime:
  backend: apptainer
```

See [Runtime Backends](runtime-backends.md) for the backend behavior and required tools.

## Enroot Cache Placement

HAICORE documents Enroot as available by default, with default data paths under the user's home directory. For repeated container jobs, large images, or quota-sensitive projects, place runtime cache/data under a workspace-backed `x-slurm.cache_dir`.

`hpc-compose` sets per-job Enroot runtime paths below the configured cache directory. That keeps image runtime state close to the job and avoids filling `$HOME` accidentally.

## BeeOND And Job-Local Scratch

HAICORE documents BeeOND as a job-local filesystem requested through a Slurm constraint:

```yaml
x-slurm:
  constraint: BEEOND
```

Use BeeOND for temporary high-throughput working data inside a job, then copy durable results back to a workspace or other approved persistent location. Do not put `x-slurm.cache_dir` on BeeOND because the cache must exist before the job and be reusable by later jobs.

## Software Modules

HAICORE software is exposed through Lmod environment modules. For host-runtime or MPI workflows, keep module setup explicit in `x-slurm.setup`:

```yaml
x-slurm:
  setup:
    - module purge
    - module avail
    - module load <module-name>
```

Do not leave `module avail` in production scripts if it produces too much output; it is useful while discovering the environment. Use `module list` in smoke tests when you need the batch log to record the active software stack.

## Suggested First HAICORE Checklist

Run these on the HAICORE login node before the first real job:

```bash
ws_find <workspace-name>
sinfo
srun --help | grep container-image
hpc-compose plan --show-script -f compose.yaml
hpc-compose preflight -f compose.yaml
hpc-compose doctor cluster-report --out .hpc-compose/haicore-cluster.toml
```

Check the rendered script for:

- the intended `#SBATCH --partition`,
- the intended account/project,
- a short wall time for smoke tests,
- a workspace-backed `cache_dir`,
- expected GPU or MIG request,
- expected `srun --container-*` options when using Pyxis.

Submit only after the static plan and preflight output are understandable:

```bash
hpc-compose up --detach -f compose.yaml
hpc-compose status -f compose.yaml
hpc-compose logs -f compose.yaml --follow
```

## Common HAICORE Failure Modes

| Symptom | Likely cause | What to check |
| --- | --- | --- |
| Workspace path is missing | Workspace expired or wrong name/path was used. | `ws_list` and `ws_find <workspace-name>`. |
| Cache path fails preflight | Path is not shared, writable, or policy-safe. | Move `x-slurm.cache_dir` to a workspace path. |
| `--container-image` is unknown | Pyxis is not active in the current Slurm environment. | `srun --help | grep container-image`; HAICORE container docs; selected backend. |
| Job is rejected for partition/account | Site policy or project/account mismatch. | HAICORE batch docs, `sacctmgr`/support guidance, rendered `#SBATCH` lines. |
| GPU request is rejected | Wrong `gres` name, too many GPUs, or partition limit. | HAICORE batch docs and a tiny smoke job. |
| Job starts but cannot see data | Data is on node-local storage or an unmounted path. | Use workspace paths or explicit `volumes`. |
| Workspace fills or expires | Container cache, datasets, checkpoints, or logs accumulated. | `ws_list`, quota tools, cache cleanup, artifact retention policy. |

## Official HAICORE References

- [HAICORE overview](https://www.nhr.kit.edu/userdocs/haicore/)
- [Interactive login](https://www.nhr.kit.edu/userdocs/haicore/login/)
- [Hardware overview](https://www.nhr.kit.edu/userdocs/haicore/hardware/)
- [Batch system](https://www.nhr.kit.edu/userdocs/haicore/batch/)
- [File systems and workspaces](https://www.nhr.kit.edu/userdocs/haicore/filesystems/)
- [Software modules](https://www.nhr.kit.edu/userdocs/haicore/software/)
- [Containers](https://www.nhr.kit.edu/userdocs/haicore/containers/)
- [BeeOND](https://www.nhr.kit.edu/userdocs/haicore/batch_slurm_beeond/)

## Read Next

- [Onboard a Cluster Site](cluster-profiles.md)
- [Operate a Real Cluster Run](runbook.md)
- [Troubleshoot a Failed Run](troubleshooting.md)
- [Slurm And Container Basics](slurm-container-basics.md)
- [Runtime Backends](runtime-backends.md)
