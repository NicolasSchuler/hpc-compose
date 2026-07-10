# Cluster setup

Read this reference for installation, execution contexts, site facts, profiles, storage, runtime selection, workspaces, or remote submission.

## Establish the boundary

Record which machine is in use:

| Context | Appropriate work |
| --- | --- |
| Authoring workstation | Repository inspection, spec edits, offline docs/schemas, static JSON checks. |
| Slurm login/submission host | Site discovery, strict preflight, preparation, and user-approved submission. |
| Active allocation | Workload execution and `x-slurm.setup`; do not treat it as durable provisioning. |
| Local Linux runtime host | Explicitly approved local supervisor/testing commands. |
| Checked-in dev cluster | Explicitly approved source-checkout Slurm integration tests. |

Check the binary version and use `hpc-compose --offline docs` so command guidance matches it. Published raw Markdown describes the latest published manual and can differ from an older installed binary.

## Required site facts

Verify from primary site docs, the user's account, or read-only tools:

- login/submission boundary and login-node policy;
- account/project, partitions, QOS, walltime, node/GPU shapes, and GRES syntax;
- Pyxis/Enroot, Apptainer, Singularity, or host modules;
- shared home/project/work/scratch roles, quotas, purge, and backup policy;
- node-local temporary storage and job-local burst storage;
- MPI/PMIx/fabric requirements and container/host MPI compatibility;
- support escalation and the verification date.

Retrieve a named site guide with:

```bash
hpc-compose --offline docs "<site> accounts partitions qos gres storage runtime" --format json
```

If no verified guide exists, keep settings as labelled hypotheses and do not invent account, partition, QOS, GRES, module, or filesystem values.

## Profiles are advisory, not provisioning

`doctor cluster-report` and cluster profiles describe observed/advisory policy. They do not allocate workspaces, create accounts, grant QOS, load permanent modules, install a runtime, or prove compute-node behavior.

Workspace lifecycle is explicit external mutation:

- `workspace status` reads site workspace state but may execute site tools.
- `workspace allocate` and `workspace extend` require the runtime/external-mutation tier.
- `workspace release` is destructive.

`x-slurm.setup` runs inside an allocation. It is not a login-node provisioning mechanism. `up` and `up --remote` deliberately do not provision workspaces, accounts, partitions/QOS, container runtimes, modules outside the job, or durable project storage.

## Shared storage contract

Image preparation occurs before the job and compute nodes reuse its output. Therefore `x-slurm.cache_dir`, prepared images, resume checkpoints, and required recovery artifacts need storage visible in every relevant context.

Never assume `/tmp`, `/var/tmp`, `/private/tmp`, `/dev/shm`, `$TMPDIR`, or job-local burst storage is a persistent shared cache. Treat workspace/project paths as candidates until policy, write access, and compute-node visibility are verified. `preflight --fs-probes` consumes a tiny allocation and requires explicit quota authorization.

## Runtime selection

- Use Pyxis only when Slurm advertises the container integration and Enroot is available in the actual submission context.
- Use Apptainer/Singularity when site policy and image format require it.
- Use host execution only when modules and binaries are guaranteed inside the job.
- Do not assume Docker daemon access on a cluster.

Use the backend-neutral `x-runtime.prepare.commands`; `x-enroot.prepare` is a Pyxis compatibility alias, not the portable default. Preparation can execute commands and pull content, so it is not a static-safe check.

## Remote submission

`up --remote` stages the project and delegates over SSH. It does not make an unverified local spec cluster-ready. Confirm the remote binary version, staged settings behavior, shared paths, and login-node policy first. A dry-run must be resolved through the command policy; never infer that `--remote` authorizes SSH or submission.

## Handoff

Separate confirmed observations from dated docs or hypotheses. Name the machine for every next command, its policy tier, whether it contacts Slurm or consumes quota, and the stable success signal. Do not claim cluster readiness from authoring checks alone.
