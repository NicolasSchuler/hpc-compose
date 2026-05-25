# General Cluster Adaptation Reference

Use this reference when the target cluster is not HAICORE or when the user only says "make my repository work with hpc-compose" without giving a site.

## Documentation Reconnaissance

Find current primary sources before writing cluster-specific settings:

- Cluster user guide for Slurm partitions/queues.
- Container runtime docs: Pyxis/Enroot, Apptainer, Singularity, or host modules.
- Filesystem docs: shared project/work/scratch paths, node-local scratch, quotas, purge policy.
- GPU docs: GRES syntax, MIG profiles, GPU memory, CPU/memory coupling.
- MPI/fabric docs: PMIx/PMI2 token, host MPI bind requirements, NCCL/UCX/OFI variables.
- Account/project policy: `--account`, `--qos`, reservations, job limits, mail policy.
- Login-node policy: what may run on login nodes versus compute nodes.

If live docs are unavailable, clearly label cluster settings as hypotheses and leave placeholders.

## Cluster Fact Sheet

Capture these facts before the first real `up`:

| Fact | Why it matters |
| --- | --- |
| Slurm partition | maps to `x-slurm.partition` |
| Account/QOS | maps to `x-slurm.account` and `x-slurm.qos` |
| Walltime limit | maps to `x-slurm.time` |
| Node/GPU shape | maps to `nodes`, `gres`, `gpus`, CPU/memory fields |
| Runtime backend | maps to `runtime.backend` |
| Shared cache path | maps to `x-slurm.cache_dir` or settings |
| Host modules/env | maps to `x-env` or `.hpc-compose/cluster.toml` |
| MPI token | maps to `services.<name>.x-slurm.mpi.type` |
| Fabric env | belongs in cluster-level env or profile, not app logic |

## Backend Selection

1. Prefer `pyxis` when Slurm advertises `--container-image` and Enroot is available.
2. Use `apptainer` when site docs standardize on Apptainer/SIF and Pyxis is unavailable.
3. Use `singularity` for older Singularity sites.
4. Use `host` for module-based jobs or when containers are not allowed.

Do not assume Docker daemon access on HPC systems.

## Repository Adaptation Pattern

1. Keep original app run instructions intact.
2. Add a separate hpc-compose spec such as `compose.hpc.yaml`.
3. Add `.env.example` or document required environment variables if needed.
4. Add `.hpc-compose/settings.toml` only when repository-local settings are useful and not user-private.
5. Keep user-private values, absolute personal workspace paths, secrets, and tokens out of committed files.
6. Add a finite smoke variant when the main service is long-running.

## Portable Spec Guidelines

- Put app intent in `compose.hpc.yaml`.
- Put cluster policy in settings, profiles, `.env`, or cluster docs.
- Use interpolation for paths and resource variants:

```yaml
x-slurm:
  partition: ${HPC_PARTITION:-normal}
  time: ${HPC_TIME:-00:30:00}
  cache_dir: ${CACHE_DIR}
```

- Use `validate --strict-env` when fallback defaults should not silently hide missing cluster values.
- Prefer first-class hpc-compose fields over raw pass-through.

## Approval Boundary

Ask before commands that:

- Submit or cancel jobs.
- Consume GPU hours or allocation quotas.
- Delete caches, artifacts, or tracked run state.
- Upload images, credentials, or artifacts to external registries.

Static commands such as `validate`, `plan`, `inspect`, `render`, and `context` are generally safe.

## Final Handoff Shape

End with:

- Observation: repository evidence and docs used.
- Hypothesis: cluster/runtime assumptions not yet verified.
- Recommendation: exact next safe command.
- Open question: any required account, workspace path, module, or approval.
