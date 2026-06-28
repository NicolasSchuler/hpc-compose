# HAICORE / NHR@KIT Reference

Use this reference when adapting hpc-compose specs for HAICORE at NHR@KIT. Treat it as a dated guide, not as a substitute for current live docs.

Primary docs:

- HAICORE overview: https://www.nhr.kit.edu/userdocs/haicore/
- Batch system: https://www.nhr.kit.edu/userdocs/haicore/batch/
- Containers: https://www.nhr.kit.edu/userdocs/haicore/containers/
- File systems: https://www.nhr.kit.edu/userdocs/haicore/filesystems/
- Hardware overview: https://www.nhr.kit.edu/userdocs/haicore/hardware/
- Support: https://www.nhr.kit.edu/userdocs/haicore/support/

## Contents

- [What To Verify Live](#what-to-verify-live)
- [Slurm And Partitions](#slurm-and-partitions)
- [Resource Discipline](#resource-discipline)
- [Filesystems And Cache](#filesystems-and-cache)
- [Containers On HAICORE](#containers-on-haicore)
- [hpc-compose HAICORE Sketch](#hpc-compose-haicore-sketch)
- [Verification On HAICORE](#verification-on-haicore)
- [Reporting Back](#reporting-back)

## What To Verify Live

Before a real submission, verify:

- Current partition names, limits, and access policy.
- Whether the user has `advanced` access or only `normal`.
- Whether Pyxis is available on the selected login/submission node.
- Which shared path should hold hpc-compose cache and Enroot/Apptainer artifacts.
- Whether account, project, reservation, or mail flags are required.
- Whether site-specific module loads are needed for Slurm, CUDA, Pyxis, Enroot, Apptainer, MPI, NCCL, UCX, or OFI.

## Slurm And Partitions

HAICORE uses Slurm for compute access. Users work from login nodes; compute nodes are accessed through Slurm jobs.

Documented HAICORE queues include:

| Partition | Typical use | Notes |
| --- | --- | --- |
| `normal` | broadly available HAICORE access | up to 1 node per job in the docs snapshot; max runtime 3 days |
| `advanced` | privileged access | can use more nodes including DGX A100 nodes; requires special privilege |

Common hpc-compose fields:

```yaml
x-slurm:
  partition: normal
  time: "01:00:00"
  nodes: 1
  ntasks: 1
  gres: gpu:1
```

Full A100 GPU requests on the `normal` partition use Slurm GRES:

```yaml
x-slurm:
  gres: gpu:1
```

MIG profiles use profile-specific GRES:

```yaml
x-slurm:
  gres: gpu:1g.5gb:1
```

Other documented profiles include `2g.10gb` and `4g.20gb`. Confirm availability with current docs or cluster commands before relying on a profile.

## Resource Discipline

HAICORE is documented as an ad-hoc AI/data resource with per-user limits. Keep first specs small:

- Use short walltimes for first tests.
- Request one GPU or a small MIG profile unless the workload clearly needs more.
- Prefer finite smoke commands before launching long-running services.
- Use `gpu_avail` on HAICORE when you need a quick view of currently idle normal-queue GPUs.

## Filesystems And Cache

Use globally visible storage for hpc-compose cache:

- Workspaces are a strong default for cache, prepared images, checkpoints that can be recreated, and intermediate artifacts.
- `$HOME` is globally visible but quota-limited and better for durable source/config, not large container artifacts.
- `$TMPDIR` is node-local and removed after the job. It is good runtime scratch, but not a valid hpc-compose cache path.
- BeeOND is job-local and removed after the job. It can help multi-node runtime scratch, but not persistent cache reuse unless staged carefully.

Create and locate a workspace:

```bash
ws_allocate hpc-compose-cache 60
ws_find hpc-compose-cache
```

Use the returned path as cache after verifying it is writable:

```bash
export CACHE_DIR="$(ws_find hpc-compose-cache)/cache"
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
hpc-compose setup --profile-name haicore --cache-dir "$CACHE_DIR" --default-profile haicore --non-interactive
```

In a portable spec, prefer:

```yaml
x-slurm:
  cache_dir: ${CACHE_DIR}
```

and place the actual HAICORE path in `.env` or project settings.

## Containers On HAICORE

The docs describe Apptainer plus Enroot/Pyxis support. For hpc-compose:

- Start with `runtime.backend: pyxis` when Pyxis/Enroot is available.
- Check Pyxis support on the target node with:

```bash
srun --help | grep container-image
hpc-compose doctor cluster-report
```

- Enroot data defaults to `$HOME/.local/share/enroot`; move it to a workspace if quota or performance requires it:

```bash
export ENROOT_DATA_PATH="$(ws_find hpc-compose-cache)/enroot-data"
mkdir -p "$ENROOT_DATA_PATH"
```

- The HAICORE docs mention required Pyxis container mounts for some direct `srun` usage. Let hpc-compose render its own launcher first, then use `doctor cluster-report`, `preflight`, and site docs to decide whether extra mounts belong in `volumes`, `x-env`, or raw `extra_srun_args`.

## hpc-compose HAICORE Sketch

Use this only as a starting point. Fill account/resource details from current docs and user context.

```yaml
version: "1"
name: my-app

runtime:
  backend: pyxis

x-slurm:
  job_name: my-app
  partition: normal
  time: "00:30:00"
  nodes: 1
  ntasks: 1
  cpus_per_task: 8
  gres: gpu:1
  cache_dir: ${CACHE_DIR}

services:
  app:
    image: python:3.11-slim
    volumes:
      - ./:/workspace
    working_dir: /workspace
    command: python -m my_app
```

## Verification On HAICORE

Run on a login/submission node:

```bash
hpc-compose context --format json
hpc-compose validate -f compose.hpc.yaml --strict-env
hpc-compose plan --show-script -f compose.hpc.yaml
hpc-compose debug -f compose.hpc.yaml --preflight
hpc-compose doctor cluster-report
```

Ask before:

```bash
hpc-compose up -f compose.hpc.yaml
```

If a cluster run fails, use:

```bash
hpc-compose debug -f compose.hpc.yaml
hpc-compose logs -f compose.hpc.yaml --follow
hpc-compose status -f compose.hpc.yaml
```

## Reporting Back

For HAICORE work, report:

- Observation: current files and docs used.
- Hypothesis: likely runtime/backend/resource assumptions.
- Recommendation: exact next command on the login node.
- Open question: access, account, workspace path, module loads, and whether a real submission is approved.
