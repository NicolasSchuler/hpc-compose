# Runtime Backends

`runtime.backend` selects how each service is launched inside the Slurm step. The default is `pyxis`.

```yaml
runtime:
  backend: pyxis
```

## Backend Summary

| Backend | Launch shape | Required tools | Image/artifact shape | Notes |
| --- | --- | --- | --- | --- |
| `pyxis` | `srun --container-*` | Slurm with Pyxis support plus Enroot on the submission host | remote images or local `.sqsh` / `.squashfs` | Default path and the only backend supported by `up --local`. |
| `apptainer` | `srun` plus `apptainer exec/run` | `apptainer` on submission and compute nodes | remote images prepared or reused as `.sif`; local `.sif` accepted | Use when the site standardizes on Apptainer instead of Pyxis. |
| `singularity` | `srun` plus `singularity exec/run` | `singularity` on submission and compute nodes | remote images prepared or reused as `.sif`; local `.sif` accepted | Similar to Apptainer for sites that still use Singularity. |
| `host` | direct `srun` command | Slurm client tools and host software/modules | no container image | Services must set `command` or `entrypoint`; image prepare and container bind mounts are not applied. |

For Pyxis, check support with:

```bash
srun --help | grep container-image
```

For all backends, `preflight` checks the selected backend tools:

```bash
hpc-compose preflight -f compose.yaml
```

## Local Mode

`up --local` is intentionally narrow:

- Linux only
- `runtime.backend: pyxis` only
- single-host specs only
- no distributed or partitioned placement
- no service-level MPI

Use local mode to inspect and debug a Pyxis/Enroot single-host launch path. It is not a replacement for Slurm distributed execution.

## Host Runtime Notes

`runtime.backend: host` runs service commands directly under `srun`. It is useful for module-based workflows or nested schedulers that already manage their own software environment.

Because there is no container:

- `image` is optional
- service `volumes` are rejected
- `x-runtime.prepare` and `x-enroot.prepare` are rejected
- `x-slurm.mpi.host_mpi.bind_paths` is not meaningful

Use top-level or service-level `x-env` for host modules, Spack views, and environment variables.

## Related Docs

- [Support Matrix](support-matrix.md)
- [Execution Model](execution-model.md)
- [Spec Reference](spec-reference.md#runtime)
- [CLI Reference](cli-reference.md#up-options)
