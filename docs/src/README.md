# hpc-compose

<div class="hpc-compose-hero">
  <img src="logo.png" alt="hpc-compose logo">
  <p><code>hpc-compose</code> turns a small Compose-like YAML file into one inspectable Slurm job for multi-service HPC and research ML workflows.</p>
  <div class="hpc-compose-links">
    <a href="support-matrix.html">Support Matrix</a>
    <a href="installation.html">Installation</a>
    <a href="quickstart.html">Quickstart</a>
    <a href="examples.html">Examples</a>
    <a href="task-guide.html">Task Guide</a>
    <a href="runtime-backends.html">Runtime Backends</a>
    <a href="runbook.html">Runbook</a>
    <a href="troubleshooting.html">Troubleshooting</a>
    <a href="cli-reference.html">CLI Reference</a>
    <a href="spec-reference.html">Spec Reference</a>
  </div>
</div>

Use `hpc-compose` when you want Docker Compose-style authoring on Slurm without adding Kubernetes, a long-running scheduler, or custom cluster-side services.

Start with the [Support Matrix](support-matrix.md) before planning a real runtime workflow. Linux is the maintained runtime target; macOS is intended for authoring, validation, rendering, and inspection.

## Safe First Path

These commands are safe from a laptop, workstation, or login node because they do not submit a job:

```bash
hpc-compose validate -f examples/minimal-batch.yaml
hpc-compose inspect -f examples/minimal-batch.yaml
hpc-compose up --dry-run --skip-prepare --no-preflight \
  --script-out /tmp/hpc-compose-demo.sbatch \
  -f examples/minimal-batch.yaml
```

Expected output includes:

```text
spec is valid
service order: app
dry run: skipping sbatch submission
```

Run `hpc-compose up -f compose.yaml` only on a supported Linux Slurm submission host with the runtime backend your spec selects.

Download the [asciinema-style quickstart demo cast](quickstart-demo.cast) if you want the same flow as a terminal recording.

## Terms To Know

| Term | Meaning |
| --- | --- |
| spec | The YAML file that describes services, runtime backend, and Slurm settings. |
| allocation | The Slurm job allocation where all planned services run. |
| runtime backend | The mechanism used to launch services: Pyxis/Enroot, Apptainer, Singularity, or host. |
| preflight | Checks that inspect local tools, paths, backend support, and optional cluster profiles before submit. |
| prepare | The login-node image import/customization phase used before compute-node runtime. |
| tracked job | Metadata under `.hpc-compose/<job-id>/` that lets `status`, `ps`, `watch`, `logs`, `stats`, and `artifacts` reconnect later. |
| `x-slurm` | The spec section for Slurm settings and hpc-compose runtime extensions. |

## What It Is For

- model serving plus helper services inside one Slurm allocation
- data and ETL pipelines with startup ordering or stage-completion dependencies
- training jobs with checkpoint export, artifact tracking, and resume-aware reruns
- explicit multi-node launch patterns that still fit inside one allocation

## What It Is Not

`hpc-compose` is not a full Docker Compose runtime and is not a general cluster orchestrator.

Unsupported Compose features include:

- `build:`
- `ports`
- `networks` / `network_mode`
- Compose `restart` as a Docker key
- `deploy`
- dynamic node bin packing

For exact boundaries, read [Execution Model](execution-model.md), [Supported Slurm Model](supported-slurm-model.md), and [Spec Reference](spec-reference.md).

## Read Next

1. [Quickstart](quickstart.md) for the shortest safe path.
2. [Examples](examples.md) to choose a starting spec.
3. [Runtime Backends](runtime-backends.md) before changing `runtime.backend`.
4. [Runbook](runbook.md) when adapting a real workload on a cluster.
5. [Troubleshooting](troubleshooting.md) when the first cluster run fails.
