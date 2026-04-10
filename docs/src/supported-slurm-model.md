# Supported Slurm Model

This page makes the `hpc-compose` Slurm boundary explicit. It is a tool for compiling one Compose-like application into one Slurm allocation with one or more containerized `srun` steps. It is not a general frontend for the full Slurm command surface.

## First-class support

These capabilities are modeled, validated, and intentionally supported by the planner, renderer, and tracked-job workflow.

| Area | Support |
| --- | --- |
| Allocation model | One Slurm allocation per application |
| Submission flow | `new`, `validate`, `config`, `inspect`, `preflight`, `prepare`, `render`, `up`, `submit`, `run` |
| Tracked job workflow | `status`, `ps`, `watch`, `stats`, `logs`, `down`, `cancel`, `artifacts`, `clean`, cache inspection/pruning |
| Top-level Slurm fields | `job_name`, `partition`, `account`, `qos`, `time`, `nodes`, `ntasks`, `ntasks_per_node`, `cpus_per_task`, `mem`, `gres`, `gpus`, `constraint`, `output`, `error`, `chdir` |
| Service step fields | `nodes`, `ntasks`, `ntasks_per_node`, `cpus_per_task`, `gres`, `gpus` |
| Multi-node model | Single-node jobs and constrained multi-node runs with at most one distributed service spanning the allocation |
| Runtime orchestration | `depends_on`, readiness checks, service failure policies, primary-node helper placement |
| Container workflow | Remote images, local `.sqsh` images, `x-enroot.prepare`, shared cache handling |
| Job tracking | Scheduler state via `squeue`/`sacct`, step stats via `sstat`, tracked logs, runtime state, metrics, artifacts, resume metadata |

## Raw pass-through

These capabilities are usable, but `hpc-compose` does not model or validate their semantics beyond passing them through to Slurm.

| Mechanism | What it allows |
| --- | --- |
| `x-slurm.submit_args` | Raw `#SBATCH ...` lines for site-specific flags such as mail settings, reservations, or other submit-time options |
| `services.<name>.x-slurm.extra_srun_args` | Raw `srun` arguments for site-specific launch flags such as MPI or exclusivity settings |
| Existing reservations | Joining an already-created reservation through raw submit args is supported as pass-through |

Pass-through is appropriate when a site-specific flag is useful but does not justify a first-class schema field. It is not a guarantee that `hpc-compose` understands the operational consequences of that flag.

## Unsupported or out of scope

These capabilities are intentionally outside the product seam.

| Area | Status |
| --- | --- |
| Admin-plane Slurm management | Out of scope |
| `sacctmgr` account administration | Out of scope |
| Reservation creation or lifecycle management | Out of scope |
| Federation / multi-cluster control | Out of scope |
| Generic `scontrol` mutation | Out of scope |
| Broad cluster inspection tools such as a full `sinfo` / `sprio` / `sreport` frontend | Out of scope |
| Arbitrary multi-node orchestration or partial-node service placement | Not supported in v1 |
| Heterogeneous jobs and job arrays as first-class workflow concepts | Not supported in v1 |
| Compose `build`, `ports`, custom networks, `restart`, `deploy` | Not supported |

## Non-goals

`hpc-compose` should not grow into a generic Slurm administration layer. In particular, it will not broaden into `sacctmgr`, reservation management, federation control, or generic `scontrol` mutation. Those are real Slurm features, but they do not fit the “one application, one allocation, tracked runtime workflow” seam this tool is built around.
