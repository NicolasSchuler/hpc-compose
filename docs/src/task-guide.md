# Choose Your Workflow

Use this page to choose a workflow shape. It deliberately stops before the
first-run commands; [Quickstart](quickstart.md) is the sole checklist for the
first successful cluster run.

Work through these decisions in order. Later choices depend on the earlier
ones.

## 1. Choose the Runtime Backend

| What exists on the target? | Choose | Verify before runtime | Good starting point |
| --- | --- | --- | --- |
| Pyxis options in `srun --help` plus Enroot on the submission host | `runtime.backend: pyxis` | `srun --help` lists `--container-image`; shared cache is visible to compute nodes | [`minimal-batch`](example-source.md#minimal-batch) |
| `apptainer` on login and compute nodes | `runtime.backend: apptainer` | A finite allocation can execute the chosen `.sif` or OCI source | [`minimal-batch`](example-source.md#minimal-batch) after changing the backend |
| `singularity` on login and compute nodes | `runtime.backend: singularity` | The site's installed version and bind behavior match the spec | [`minimal-batch`](example-source.md#minimal-batch) after changing the backend |
| Site modules or vendor software, no container required | `runtime.backend: host` | Required `module load` commands work inside an allocation | [`host-modules`](example-source.md#host-modules) |

If more than one path is available, prefer the backend the site supports for
your workload and interconnect. Do not infer Pyxis from Enroot alone. See
[Runtime Backends](runtime-backends.md).

## 2. Choose the Topology

```text
one command or co-located services?
├─ one node ───────────────► single-node plan
└─ more than one node
   ├─ one distributed service spans the allocation ─► supported pattern
   └─ independent services dynamically placed ──────► outside current scope
```

| Need | Start from | Key decision |
| --- | --- | --- |
| One finite service | [`minimal-batch`](example-source.md#minimal-batch) | Request only the CPU, memory, and accelerator resources it needs. |
| Co-located service plus worker/client | [`app-redis-worker`](example-source.md#app-redis-worker) | Use readiness and dependency conditions only where a consumer truly waits. |
| MPI across the allocation | [`multi-node-mpi`](example-source.md#multi-node-mpi) | Verify the site's MPI/PMIx path before the application. |
| PyTorch distributed training | [`multi-node-torchrun`](example-source.md#multi-node-torchrun) | Use generated rendezvous and rank metadata; do not build SSH fanout. |
| DeepSpeed, Accelerate, Horovod, JAX, Ray, Dask, or Spark | [distributed examples](examples.md) | Choose the framework-native launcher that fits the one-allocation model. |

For unfamiliar fabric or MPI setups, render `doctor mpi-smoke` or `doctor
fabric-smoke` first. Adding `--submit` consumes an allocation and belongs after
authorization.

## 3. Choose Batch, Interactive, or Notebook Execution

| Working style | Command family | Boundary |
| --- | --- | --- |
| Finite unattended run | `up` or `test --submit` | Normal production path; submission consumes quota. |
| Iterate inside one allocation | `alloc`, then `run SERVICE -- ...` | The held allocation continues consuming resources while idle. |
| JupyterLab or VS Code | `notebook` | Tracked interactive job; requires an explicit stop/cancel plan. |
| Local hot reload before Slurm | `dev` / `tmux` | Single-host development, not evidence of cluster compatibility. |

Promote a successful notebook into a reproducible batch spec with `notebook
promote`; promotion itself is static authoring. See [Notebook](notebook.md) and
[Development Workflow](development-workflow.md).

## 4. Choose One Run, an Array, or a Sweep

| Multiplicity | Choose | Use when |
| --- | --- | --- |
| One tracked allocation | ordinary `up` | One configuration or one distributed run. |
| Slurm array | top-level `x-slurm.array` | Tasks share one script shape and differ mainly by `SLURM_ARRAY_TASK_ID`. |
| hpc-compose sweep | top-level `sweep` plus `sweep submit` | Named parameters, replicates, objectives, per-trial records, or resume of partial fanout matter. |

Arrays and sweeps can consume many allocations. Dry-run and inspect the trial
count first; authorization for one job does not imply authorization for a
fanout. See [Sweeps](sweeps.md).

## 5. Choose Where the Command Runs

| Context | Use it for | Important boundary |
| --- | --- | --- |
| Login node | static checks, preflight, prepare, submission, and tracked operations | Do not run sustained application compute directly on the login node. |
| Laptop to remote login node | `up --remote` after settings identify the login host | Stages the repository and delegates; it does not allocate site storage or accounts. |
| Local runtime | `up --local`, `test --local`, `dev`, or `tmux` on a supported Linux host | Single-host evidence only; not a distributed Slurm substitute. |
| Local Slurm dev cluster | `test --submit --dev-cluster` from a source checkout | Real local `sbatch`, but fake/local hardware and host backend do not prove a production site. |

Cluster profiles are advisory policy. Site workspace allocation, storage
directories, and account access must exist before `up` or `up --remote` can use
them. Read [Onboard a Cluster Site](cluster-profiles.md) and the applicable
generated site guide.

## Your Next Page

- Ready for the first cluster run: [Quickstart](quickstart.md).
- Migrating an existing stack: [Migrate a docker-compose.yaml](docker-compose-migration.md).
- Choosing among concrete specs: [Examples](examples.md).
- Operating a workflow that already ran once: [Runbook](runbook.md).
- Preparing for costly or long runs: [Production Readiness](production-readiness.md).

## Related Docs

- [Quickstart](quickstart.md)
- [Examples](examples.md)
- [Runtime Backends](runtime-backends.md)
- [Execution Model](execution-model.md)
- [Slurm Capability Scope](slurm-capability-scope.md)
