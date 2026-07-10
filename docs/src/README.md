# hpc-compose

<div class="hpc-compose-hero">
  <div class="hpc-compose-hero-copy">
    <img class="hpc-compose-hero-mark" src="favicon.png" alt="" aria-hidden="true">
    <p class="hpc-compose-tagline">Compose-style workflows, compiled into one inspectable Slurm job.</p>
    <p class="hpc-compose-trust">One allocation &middot; one script &middot; Slurm-native runtime.</p>
    <p>Choose the shape closest to your workload. Each path joins the same canonical first-run checklist before it can submit.</p>
  </div>
  <div class="hpc-compose-proof" aria-label="hpc-compose execution model">
    <pre><code>compose.yaml&#10;    │ validate · lint · plan&#10;    ▼&#10;one generated batch script&#10;    │ explicit submission&#10;    ▼&#10;one tracked Slurm allocation</code></pre>
  </div>
</div>

## Choose Your Path

<section class="journey-grid" aria-label="Choose a workload journey">
  <article class="journey-card">
    <p class="journey-kicker">One finite command</p>
    <h3>Single batch job</h3>
    <p>Start with <code>minimal-batch</code>, then follow the only first-cluster-run checklist.</p>
    <a href="quickstart.html">Run the Quickstart <span aria-hidden="true">→</span></a>
  </article>
  <article class="journey-card">
    <p class="journey-kicker">Services that coordinate</p>
    <h3>Multi-service application</h3>
    <p>Learn dependency conditions and readiness from <code>app-redis-worker</code> before adapting your own stack.</p>
    <a href="example-source.html#app-redis-worker">Open the worked example <span aria-hidden="true">→</span></a>
  </article>
  <article class="journey-card">
    <p class="journey-kicker">One service across nodes</p>
    <h3>Distributed training</h3>
    <p>Choose the framework and topology first, then begin with <code>multi-node-torchrun</code>.</p>
    <a href="task-guide.html#2-choose-the-topology">Choose a topology <span aria-hidden="true">→</span></a>
  </article>
</section>

All three paths use the same safety boundary:

1. [Choose Your Workflow](task-guide.md) selects the backend, topology,
   execution style, run multiplicity, and submission context.
2. [Quickstart](quickstart.md) owns the first successful cluster run from
   version check through logs.
3. [Operate a Real Cluster Run](runbook.md) owns repeat operations after that
   first success.

## The Mental Model

`hpc-compose` is a compiler and run tracker, not a long-running orchestrator.
It validates a small Compose-like YAML model, produces a normalized plan and
one generated Slurm script, submits only after an explicit runtime command,
then records enough state for status, logs, metrics, artifacts, and recovery.

Use [Command Families](command-families.md) when the next question is “which
command answers this?” Use the [Spec Reference](spec-reference.md) when the
question is “which YAML fields are legal?”

## Scope at a Glance

- One Slurm allocation per application.
- Single-node services or one explicit distributed service spanning the allocation.
- Pyxis/Enroot, Apptainer, Singularity, or host runtime backends.
- Service ordering, readiness, finite tests, metrics, artifacts, and resume-aware reruns.
- No Docker daemon, Kubernetes control plane, custom Docker networks, dynamic node bin-packing, or per-service partitions in one allocation.

Check the [Support Matrix](support-matrix.md) before assuming a platform or
cluster can run the workflow. For exact boundaries, read the [Execution
Model](execution-model.md) and [Slurm Capability Scope](slurm-capability-scope.md).

## Read Next

- [Installation](installation.md)
- [Choose Your Workflow](task-guide.md)
- [Quickstart](quickstart.md)
- [Examples](examples.md)
- [Production Readiness](production-readiness.md)
