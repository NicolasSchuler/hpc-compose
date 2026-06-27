# Slurm Capability Scope

This page makes the `hpc-compose` Slurm boundary explicit. It is a tool for compiling one Compose-like application into one Slurm allocation with one or more `srun` steps. Those steps can use Pyxis/Enroot, Apptainer, Singularity, or host runtime software. It is not a general frontend for the full Slurm command surface.

## First-class support

These capabilities are modeled, validated, and intentionally supported by the planner, renderer, and tracked-job workflow.

| Area | Support |
| --- | --- |
| Allocation model | One Slurm allocation per application |
| Submission flow | `new`, `plan`, `validate`, `config`, `inspect`, `preflight`, `prepare`, `render`, `up`, `when`, `alloc`, `run`, `debug` |
| Tracked job workflow | `status`, `ps`, `watch`, `stats`, `score`, `logs`, `down`, `cancel`, `artifacts`, `clean`, cache inspection/pruning |
| Top-level Slurm fields | `job_name`, `partition`, `account`, `qos`, `time`, `nodes`, `ntasks`, `ntasks_per_node`, `cpus_per_task`, `mem`, `gres`, `gpus`, GPU/CPU binding fields, `constraint`, `output`, `error`, `chdir` |
| Service step fields | `nodes`, `placement`, `ntasks`, `ntasks_per_node`, `cpus_per_task`, `gres`, `gpus`, GPU/CPU binding fields, `mpi` |
| Multi-node model | Single-node jobs, full-allocation distributed steps, and explicit node-index partitioning within one allocation |
| Runtime orchestration | `depends_on`, readiness checks, one-shot completion dependencies, service failure policies, primary-node helper placement, explicit co-location through `placement.share_with` |
| Service hooks | Per-service `prologue` and `epilogue` lifecycle hooks, plus host-side `restart` and `window_exhausted` event hooks |
| Runtime workflow | Pyxis/Enroot `.sqsh`, Apptainer/Singularity `.sif`, host runtime commands, `x-runtime.prepare`, shared cache handling |
| Scratch and staging | `x-slurm.scratch`, `stage_in`, `stage_out`, per-service scratch opt-out, raw `#BB`/`#DW` burst-buffer directives |
| Job tracking | Scheduler state via `squeue`/`sacct`, step stats via `sstat`, tracked logs, runtime state, metrics, artifacts, resume metadata |
| Advisory cluster weather | `weather` summarizes current node and queue conditions from read-only Slurm probes without reserving resources or changing submission behavior |
| Conditional submission | `when` actively monitors typed conditions, then submits one normal `hpc-compose` allocation |
| Canary right-sizing | `germinate` submits one short canary, writes `latest-canary.json`, and recommends resource settings without rewriting the spec |
| Hyperparameter sweeps | `sweep submit` expands one embedded sweep into many independent single-allocation jobs, then `sweep status` aggregates their tracked state |
| Cross-job rendezvous | Provider/client discovery through shared-cache JSON records under one cluster-visible cache directory |

## Raw pass-through

These capabilities are usable, but `hpc-compose` does not model or validate their semantics beyond passing them through to Slurm.

| Mechanism | What it allows |
| --- | --- |
| `x-slurm.submit_args` | Raw `#SBATCH ...` lines for site-specific flags such as mail settings, reservations, or other submit-time options |
| `services.<name>.x-slurm.extra_srun_args` | Raw `srun` arguments for site-specific launch flags such as exclusivity settings |
| Existing reservations | Joining an already-created reservation through raw submit args is supported as pass-through |

Pass-through is appropriate when a site-specific flag is useful but does not justify a first-class schema field. `hpc-compose` rejects line breaks and null bytes in raw `#SBATCH` entries so one list entry cannot emit multiple directives, but it does not validate the Slurm semantics of those flags.

## Unsupported or out of scope

These capabilities are intentionally outside the product seam.

| Area | Status |
| --- | --- |
| Admin-plane Slurm management | Out of scope |
| `sacctmgr` account administration | Out of scope |
| Reservation creation or lifecycle management | Out of scope |
| Federation / multi-cluster control | Out of scope |
| Cross-cluster service discovery | Out of scope; rendezvous is same-cluster shared-storage coordination only |
| Generic `scontrol` mutation | Out of scope |
| Broad cluster inspection tools such as a full `sinfo` / `sprio` / `sreport` frontend | Out of scope; `weather` is limited to a compact advisory snapshot |
| Background submit daemons or reservations | Out of scope; `when` is a foreground advisory monitor and does not reserve resources |
| Dynamic scheduling or bin packing across nodes | Not supported; use explicit `x-slurm.placement` selectors |
| Heterogeneous jobs | Not supported |
| Slurm arrays | Supported only through `x-slurm.array` for detached Slurm submissions. Local mode and live watch do not fan out array tasks; sweeps deliberately submit many normal allocations instead of Slurm arrays. |
| Compose `build`, `ports`, custom networks, `restart`, `deploy` | Not supported |

## Non-goals

`hpc-compose` should not grow into a generic Slurm administration layer. In particular, it will not broaden into `sacctmgr`, reservation management, federation control, or generic `scontrol` mutation. Those are real Slurm features, but they do not fit the “one application, one allocation, tracked runtime workflow” seam this tool is built around.

## Related Docs

- [Why hpc-compose](why-hpc-compose.md)
- [Execution Model](execution-model.md)
- [Runtime Backends](runtime-backends.md)
- [Spec Reference](spec-reference.md)
