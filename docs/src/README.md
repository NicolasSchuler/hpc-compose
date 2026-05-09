# hpc-compose

<div class="hpc-compose-hero">
  <div class="hpc-compose-hero-copy">
    <img class="hpc-compose-hero-mark" src="logo.png" alt="" aria-hidden="true">
    <p class="hpc-compose-tagline">Compose-style multi-service workflows, compiled into one inspectable Slurm job.</p>
    <p><code>hpc-compose</code> gives research and HPC teams a small YAML authoring model for services, startup order, readiness checks, runtime backends, logs, artifacts, and follow-up commands.</p>
    <nav class="hpc-compose-actions" aria-label="Start using hpc-compose">
      <a class="primary" href="quickstart.html">Quickstart</a>
      <a href="examples.html">Examples</a>
      <a href="support-matrix.html">Support Matrix</a>
    </nav>
  </div>
  <div class="hpc-compose-proof" aria-label="Static plan preview">
    <pre><code>services:&#10;  app:&#10;    image: python:3.12-slim&#10;    command: python train.py&#10;&#10;$ hpc-compose plan --show-script -f compose.yaml&#10;spec is valid&#10;service order: app&#10;&#35;SBATCH --job-name=my-app</code></pre>
  </div>
</div>

Use `hpc-compose` when you want Docker Compose-style authoring on Slurm without adding Kubernetes, a long-running control plane, or custom cluster-side services.

Start with the [Support Matrix](support-matrix.md) before planning a real runtime workflow. Linux is the maintained runtime target; macOS is intended for authoring, validation, rendering, and inspection.

## Safe First Path

These commands are safe from a laptop, workstation, or login node because `new` writes a local starter spec and `plan` is purely static:

```bash
hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml
hpc-compose plan -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
```

Replace `<shared-cache-dir>` with a path visible from both the Slurm submission host and compute nodes. From a source checkout, you can also inspect the checked-in examples with `hpc-compose plan -f examples/minimal-batch.yaml`.

Expected output includes:

```text
spec is valid
service order: app
Rendered script:
```

Run `hpc-compose up -f compose.yaml` only on a supported Linux Slurm submission host with the runtime backend your spec selects. If it fails, start with `hpc-compose debug -f compose.yaml --preflight`.

Download the [asciinema-style quickstart demo cast](quickstart-demo.cast) if you want the same flow as a terminal recording.

## Terms To Know

| Term | Meaning |
| --- | --- |
| spec | The YAML file that describes services, runtime backend, and Slurm settings. |
| allocation | The Slurm job allocation where all planned services run. |
| runtime backend | The mechanism used to launch services: Pyxis/Enroot, Apptainer, Singularity, or host. |
| preflight | Checks that inspect local tools, paths, backend support, and optional cluster profiles before a run. |
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

## Reference

- [Installation](installation.md)
- [Task Guide](task-guide.md)
- [CLI Reference](cli-reference.md)
- [Spec Reference](spec-reference.md)
