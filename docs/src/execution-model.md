# Execution model

This page explains the few runtime rules that matter most when a Compose mental model meets Slurm, Enroot, and Pyxis.

## What runs where

| Stage | Where it runs | What happens |
| --- | --- | --- |
| `validate`, `inspect`, `preflight` | login node or local shell | Parse the spec, resolve paths, and check prerequisites |
| `prepare` | login node or local shell with Enroot access | Import base images and build prepared runtime artifacts |
| `submit` | login node or local shell with Slurm access | Run preflight, prepare missing artifacts, render the batch script, and call `sbatch` |
| Batch script and services | compute-node allocation | Launch the planned services through `srun` and Pyxis |
| `status`, `stats`, `logs`, `artifacts` | login node or local shell | Read tracked metadata and job outputs after submission |

The main consequence is simple: image preparation and validation happen before the job starts, but the containers themselves run later inside the Slurm allocation.

## Which paths must be shared

- `x-slurm.cache_dir` must be visible from both the login node and the compute nodes.
- Relative host paths in `volumes`, local image paths, and `x-enroot.prepare.mounts` resolve against the compose file directory.
- Each submitted job writes tracked state under `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}` on the host.
- That per-job directory is mounted into every container at `/hpc-compose/job`.
- Multi-node jobs also populate `/hpc-compose/job/allocation/{primary_node,nodes.txt}` and export `HPC_COMPOSE_PRIMARY_NODE`, `HPC_COMPOSE_NODE_COUNT`, `HPC_COMPOSE_NODELIST`, and `HPC_COMPOSE_NODELIST_FILE`.

Use `/hpc-compose/job` for small shared state inside the allocation, such as ready files, request payloads, logs, metrics, or teardown signals.

<div class="callout warning">
  <p><strong>Warning</strong></p>
  <p>Do not put <code>x-slurm.cache_dir</code> under <code>/tmp</code>, <code>/var/tmp</code>, <code>/private/tmp</code>, or <code>/dev/shm</code>. Those paths are not safe for login-node prepare plus compute-node reuse.</p>
</div>

## Networking inside the allocation

- Single-node services share the host network on one node.
- In a multi-node job, helper services stay on the allocation's primary node by default.
- The one distributed service spans the full allocation and must use explicit non-localhost coordination.
- `ports`, custom Docker networks, and service-name DNS are not part of the model.
- Use `depends_on` plus `readiness` when a dependent service must wait for real availability rather than process start.

Use `127.0.0.1` only when both sides are intentionally on the same node. For multi-node distributed runs, derive rendezvous addresses from the allocation metadata files or environment variables instead of relying on localhost.

If a service binds its TCP port before it is actually ready, prefer HTTP or log-based readiness over plain TCP readiness.

## `volumes` vs `x-enroot.prepare`

| Mechanism | Use it for | When it is applied | Reuse behavior |
| --- | --- | --- | --- |
| `volumes` | fast-changing source code, model directories, input data, checkpoint paths | at runtime inside the allocation | reads live host content every normal run |
| `x-enroot.prepare.commands` | slower-changing dependencies, tools, and image customization | before submission on the login node | cached until the prepared artifact changes |

Recommended default:

- keep active source trees in `volumes`
- keep slower-changing dependency installation in `x-enroot.prepare.commands`
- use `prepare.mounts` only when the prepare step truly needs host files

<div class="callout warning">
  <p><strong>Warning</strong></p>
  <p>If a mounted file is a symlink, the symlink target must also be visible from inside the mounted directory. Otherwise the path can exist on the host but fail inside the container.</p>
</div>

## Command vocabulary

- The <strong>normal run</strong> is <code>hpc-compose submit --watch -f compose.yaml</code>.
- The <strong>debugging flow</strong> is <code>validate</code>, <code>inspect</code>, <code>preflight</code>, and <code>prepare</code> run separately when you need more visibility.

Read [Runbook](runbook.md) for the operational workflow, [Examples](examples.md) for starting points, and [Spec reference](spec-reference.md) for exact field behavior.
