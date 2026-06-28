# Runtime Backends

`runtime.backend` selects how each service is launched inside the Slurm step. The default is `pyxis`.

For a beginner explanation of Slurm steps, Pyxis, Enroot, and shared runtime caches, start with [Slurm And Container Basics](slurm-container-basics.md).

```yaml
runtime:
  backend: pyxis
```

## Backend Summary

| Backend | Launch shape | Required tools | Image/artifact shape | Notes |
| --- | --- | --- | --- | --- |
| `pyxis` | `srun --container-*` | Slurm with Pyxis support plus Enroot on the submission host | remote images or local `.sqsh` / `.squashfs` | Default path and the only backend supported by local development workflows. |
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

On the first `pyxis`/Enroot run, `prepare` imports the image with enroot — download, extract, then squashfs build — which can take several minutes for a multi-GB image; later runs reuse the cached `.sqsh`. The extraction scratch defaults to the shared cache (`<cache_dir>/enroot/tmp`); on shared NFS/Lustre/GPFS storage you can redirect it to node-local storage with `x-slurm.enroot_temp_dir` (or `cache.enroot_temp_dir`) to avoid slow imports and `Stale file handle` errors, while the layer cache and final `.sqsh` stay on the shared cache. See [Files and Directories](files-and-directories.md#cache-directory).

When the prepare scratch is node-local, also watch prepare-time bind mounts: `x-runtime.prepare.mounts` (and enroot prepare-hook mounts) run on the login node, so a mount whose **source** is on a network/shared filesystem can become a new failure point during prepare. Prefer a **dependency-only prepare** — install dependencies into the image during prepare (`pip install -r requirements.txt`, `uv pip install`, …) and mount your source tree as a **runtime** volume (`services.<name>.volumes`) rather than a `prepare.mounts` entry — so prepare stays independent of network-FS mounts. `examples/dev-python-app.yaml` shows source-mounted-at-runtime with deps baked in during prepare. `preflight` checks prepare mount sources (an absolute source is hinted as a possible cluster-workspace/site-storage path needing provisioning), and a prepare command that fails with bind mounts active lists the active mounts and suggests this pattern.

### Installing Python packages (PEP 668 / externally-managed images)

How you install dependencies in `prepare` depends on the base image's Python:

- **`pip install` works** on the official `python:*`/`python:*-slim` images (Python from python.org, installed under `/usr/local`) and on Conda-based images such as `pytorch/pytorch:*`. The shipped Python examples use these, so a plain `pip install --no-cache-dir <pkgs>` is fine.
- **`pip install` is blocked** on images whose Python comes from the distribution package manager — e.g. `apt install python3` on an `ubuntu`/`debian` or `nvidia/cuda:*-ubuntu*` base. These ship an `EXTERNALLY-MANAGED` marker (PEP 668), so `python -m pip install …` fails with *"externally managed environment"*.

For an externally-managed image, do **not** reach for `pip install --break-system-packages`. Use one of:

```yaml
x-runtime:
  prepare:
    commands:
      # Option A — a dedicated venv that can still see the image's system packages
      # (e.g. a CUDA build of torch baked into the base image):
      - python3 -m venv --system-site-packages /opt/venv
      - /opt/venv/bin/pip install --no-cache-dir <your-extra-deps>
      # Option B — uv, installed without pip via its standalone installer, then
      # installing into the system environment (uv does not honor PEP 668):
      - curl -LsSf https://astral.sh/uv/install.sh | sh
      - $HOME/.local/bin/uv pip install --system --no-cache <your-extra-deps>
services:
  trainer:
    # With Option A, run the venv's interpreter so the extra deps are importable:
    command: ["/opt/venv/bin/python", "train.py"]
```

`--system-site-packages` keeps framework packages that are baked into the base image (such as a CUDA-matched PyTorch) visible inside the venv, so you only install the extras on top.

## Local Mode

`up --local`, `test --local`, `dev`, and `tmux` are intentionally narrow:

- Linux only
- `runtime.backend: pyxis` only
- Pyxis-compatible Enroot tooling on the host
- single-host specs only
- no distributed or partitioned placement
- no service-level MPI
- no Slurm arrays or scheduler dependencies

Use local mode to inspect and debug a Pyxis/Enroot single-host launch path. `dev` adds file-change restart requests to the local supervisor, and `tmux` tails tracked local service logs in panes. Neither command changes the process-supervision model, and local mode is not a replacement for Slurm distributed execution.

## Host Runtime Notes

`runtime.backend: host` runs service commands directly under `srun`. It is useful for module-based workflows or nested schedulers that already manage their own software environment.

Because there is no container:

- `image` is optional
- service `volumes` are rejected
- `x-runtime.prepare` and `x-enroot.prepare` are rejected
- `x-slurm.mpi.host_mpi.bind_paths` is rejected

Use top-level or service-level `x-env` for host modules, Spack views, and environment variables.

## Related Docs

- [Support Matrix](support-matrix.md)
- [Slurm And Container Basics](slurm-container-basics.md)
- [Execution Model](execution-model.md)
- [Spec Reference](spec-reference.md#runtime)
- [CLI Reference](cli-reference.md#up-options)
