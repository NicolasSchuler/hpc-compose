# Cluster Profiles

Cluster profiles let `validate` and `preflight` compare a spec against site-specific Slurm, runtime, MPI, storage, and policy hints.

Generate a best-effort profile on the target login node:

```bash
hpc-compose doctor --cluster-report
```

This writes `.hpc-compose/cluster.toml` by default. Use `--cluster-report-out -` to print TOML instead.

## What Gets Discovered

The profile generator uses available local tools and environment hints:

- `sinfo`, `scontrol`, and `srun --mpi=list`
- selected runtime binaries
- shared-path environment hints
- loaded MPI stack hints from `PATH`, `MPI_HOME`, `MPI_DIR`, `I_MPI_ROOT`, `EBROOTOPENMPI`, and `EBROOTMPICH`
- editable distributed defaults such as rendezvous port and `[distributed.env]`

It does not run `module avail`. Module-only MPI installations can be added manually to the generated `mpi_installations` list.

## Site Policy Packs

Support teams can edit optional sections such as:

- `[site]`
- `[[software.modules]]`
- `[[filesystems]]`
- `[gpu]`
- `[network]`
- `[containers]`
- `[slurm.defaults]`
- `[slurm.required]`

Policy sections warn and suggest snippets. They do not silently add modules, bind mounts, environment variables, or SBATCH directives to user specs.

## MPI Smoke Probe

For MPI services, render a small rank-count probe against the service's real runtime path:

```bash
hpc-compose doctor --mpi-smoke -f compose.yaml --service trainer --script-out mpi-smoke.sbatch
```

Submit it only when you intentionally want to consume a Slurm allocation:

```bash
hpc-compose doctor --mpi-smoke -f compose.yaml --service trainer --submit
```

The smoke plan keeps allocation and MPI launch settings but strips application workflow blocks such as setup, scratch staging, resume metadata, artifacts, and burst-buffer directives.

## Fabric Smoke Probe

For distributed GPU or fabric-sensitive services, render a broader smoke probe:

```bash
hpc-compose doctor --fabric-smoke -f compose.yaml --service trainer --checks auto --script-out fabric-smoke.sbatch
```

`--checks auto` always includes the MPI rank probe, adds NCCL when the selected service requests GPU resources, and collects UCX, OFI, and InfiniBand diagnostics when the corresponding tools are available. Use an explicit list such as `--checks mpi,nccl` when a missing tool should fail the probe instead of being reported as skipped.

## Related Docs

- [Runbook](runbook.md)
- [Runtime Backends](runtime-backends.md)
- [Troubleshooting](troubleshooting.md)
- [Spec Reference](spec-reference.md#servicesnamex-slurmmpi)
