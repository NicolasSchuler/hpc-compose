# Execution model

This page explains the few runtime rules that matter most when a Compose mental model meets Slurm and HPC runtime backends.

## What runs where

| Stage | Where it runs | What happens |
| --- | --- | --- |
| `plan`, `validate`, `inspect`, `preflight` | login node or local shell | Parse the spec, resolve paths, preview the runtime plan, and check prerequisites |
| `prepare` | login node or local shell with the selected runtime backend | Import base images and build prepared runtime artifacts |
| `up` | login node or local shell with Slurm access | Run preflight, prepare missing artifacts, render the batch script, call `sbatch`, and watch by default |
| Batch script and services | compute-node allocation | Launch the planned services through `srun` and the selected runtime backend |
| `status`, `ps`, `watch`, `stats`, `logs`, `artifacts` | login node or local shell | Read tracked metadata and job outputs after submission |

The main consequence is simple: image preparation and validation happen before the job starts, but the containers themselves run later inside the Slurm allocation.

## Service failure policies inside one job

`hpc-compose` does not provide a separate long-running orchestrator. Service failure handling happens inside the rendered batch script for the current allocation.

- `mode: fail_job` keeps fail-fast behavior and stops the job on the first non-zero service exit.
- `mode: ignore` records the failure but allows the rest of the job to continue.
- `mode: restart_on_failure` only reacts to non-zero process exits. It does not restart on successful exits, and it does not use cross-attempt or cross-requeue history.

For `restart_on_failure`, the batch script enforces two limits during one live execution:

- a lifetime cap through `max_restarts`
- a rolling-window cap through `max_restarts_in_window` within `window_seconds`

If a service omits the rolling-window fields, `hpc-compose` still enables crash-loop protection with `window_seconds: 60` and `max_restarts_in_window: <resolved max_restarts>`.

Use `status` to inspect the tracked policy state after submission. The text view reports:

```text
state service 'worker': failure_policy=restart_on_failure restarts=1/5 window=1/3@60s last_exit=42
```

Use `logs` to inspect the corresponding restart messages from the batch script when you need to distinguish lifetime-cap exhaustion from rolling-window exhaustion.

## Which paths must be shared

- `x-slurm.cache_dir` must be visible from both the login node and the compute nodes.
- Relative host paths in `volumes`, local image paths, and `x-runtime.prepare.mounts` resolve against the compose file directory.
- Each submitted job writes tracked state under `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}` on the host.
- That per-job directory is mounted into every container at `/hpc-compose/job`.
- Multi-node jobs also populate `/hpc-compose/job/allocation/{primary_node,nodes.txt}` and export allocation-wide `HPC_COMPOSE_NODE...` variables plus service-scoped `HPC_COMPOSE_SERVICE_NODE...` variables.

Use `/hpc-compose/job` for small shared state inside the allocation, such as ready files, request payloads, logs, metrics, or teardown signals.

### Enroot runtime paths

The generated batch script sets three Enroot runtime paths scoped per job under the configured cache directory:

| Variable | Value | Purpose |
| --- | --- | --- |
| `ENROOT_CACHE_PATH` | `$CACHE_ROOT/runtime/$SLURM_JOB_ID/cache` | Enroot image cache for the current job |
| `ENROOT_DATA_PATH` | `$CACHE_ROOT/runtime/$SLURM_JOB_ID/data` | Enroot data directory for the current job |
| `ENROOT_TEMP_PATH` | `$CACHE_ROOT/runtime/$SLURM_JOB_ID/tmp` | Enroot temp directory for the current job |

These paths are created at batch startup and are available inside the batch script and to tooling that reads Enroot environment variables. They are not injected into service containers.

<div class="callout warning">
  <p><strong>Warning</strong></p>
  <p>Do not put <code>x-slurm.cache_dir</code> under <code>/tmp</code>, <code>/var/tmp</code>, <code>/private/tmp</code>, or <code>/dev/shm</code>. Those paths are not safe for login-node prepare plus compute-node reuse.</p>
</div>

## Networking inside the allocation

- Single-node services share the host network on one node.
- In a multi-node job, helper services stay on the allocation's primary node by default.
- A distributed service may span the full allocation, or services may use `x-slurm.placement` to select explicit allocation node subsets.
- Partitioned services should use service-scoped metadata such as `HPC_COMPOSE_SERVICE_PRIMARY_NODE`, `HPC_COMPOSE_SERVICE_NODE_COUNT`, `HPC_COMPOSE_SERVICE_NODELIST`, and `HPC_COMPOSE_SERVICE_NODELIST_FILE`.
- `ports`, custom Docker networks, and service-name DNS are not part of the model.
- Use `depends_on` plus `readiness` when a dependent service must wait for real availability rather than process start.
- Use `depends_on` with `condition: service_completed_successfully` when a dependent service should wait for a one-shot stage to exit successfully.

Use `127.0.0.1` only when both sides are intentionally on the same node. For multi-node distributed or partitioned runs, derive rendezvous addresses from allocation or service metadata files and environment variables instead of relying on localhost.

If a service binds its TCP port before it is actually ready, prefer HTTP or log-based readiness over plain TCP readiness.

## `volumes` vs `x-runtime.prepare`

| Mechanism | Use it for | When it is applied | Reuse behavior |
| --- | --- | --- | --- |
| `volumes` | fast-changing source code, model directories, input data, checkpoint paths | at runtime inside the allocation | reads live host content every normal run |
| `x-runtime.prepare.commands` | slower-changing dependencies, tools, and image customization | before submission on the login node | cached until the prepared artifact changes |

Recommended default:

- keep active source trees in `volumes`
- keep slower-changing dependency installation in `x-runtime.prepare.commands`
- use `prepare.mounts` only when the prepare step truly needs host files

<div class="callout warning">
  <p><strong>Warning</strong></p>
  <p>If a mounted file is a symlink, the symlink target must also be visible from inside the mounted directory. Otherwise the path can exist on the host but fail inside the container.</p>
</div>

## Command vocabulary

- The **normal run** is <code>hpc-compose up -f compose.yaml</code>. See [Quickstart](quickstart.md) for the full end-to-end description.
- The <strong>tracked follow-up tools</strong> are <code>status</code> for scheduler/log summaries, <code>ps</code> for a stable per-service snapshot, and <code>watch</code> when you want to reconnect to the live TUI later.
- The <strong>debugging flow</strong> is <code>validate</code>, <code>inspect</code>, <code>preflight</code>, and <code>prepare</code> run separately when you need more visibility.

Read [Runtime Backends](runtime-backends.md) before changing `runtime.backend`, [Runbook](runbook.md) for the operational workflow, [Examples](examples.md) for starting points, and [Spec reference](spec-reference.md) for exact field behavior.
