# hpc-compose

<div class="hpc-compose-hero">
  <img src="logo.png" alt="hpc-compose logo">
  <p><code>hpc-compose</code> turns a Compose-like spec into a single Slurm job that runs one or more services through Enroot and Pyxis.</p>
  <div class="hpc-compose-links">
    <a href="quickstart.html">Quickstart</a>
    <a href="support-matrix.html">Support matrix</a>
    <a href="task-guide.html">Task guide</a>
    <a href="execution-model.html">Execution model</a>
    <a href="runbook.html">Runbook</a>
    <a href="spec-reference.html">Spec reference</a>
    <a href="supported-slurm-model.html">Supported Slurm model</a>
    <a href="examples.html">Examples</a>
  </div>
</div>

`hpc-compose` is intentionally **not** a full Docker Compose implementation. It focuses on the subset that maps cleanly to one Slurm allocation, plus either single-node services or one allocation-wide distributed service without a separate orchestration layer.

## Start Here

1. Read [Quickstart](quickstart.md) for the shortest install-and-run path.
2. Read [Support Matrix](support-matrix.md) to confirm what is officially supported, CI-tested, or only release-built.
3. Use [Task Guide](task-guide.md) when you want the shortest path for a specific workflow.
4. Read [Execution model](execution-model.md) to understand what runs on the login node, what runs on the compute node, and which paths must be shared.
5. Use [Runbook](runbook.md) when adapting a real workload to a real cluster.
6. Use [Examples](examples.md) when you want the closest known-good starting point.
7. Use [Spec reference](spec-reference.md) when you need exact field behavior or validation rules.
8. Use [Supported Slurm model](supported-slurm-model.md) when you need the product boundary spelled out clearly.

## What it is for

- One Slurm allocation per application
- Single-node jobs and constrained multi-node distributed runs
- Optional helper services pinned to the allocation's primary node
- Remote images such as `redis:7` or existing local `.sqsh` images
- Optional image customization on the login node through `x-enroot.prepare`
- Shared cache management for imported and prepared images
- Readiness-gated startup across dependent services
- Per-service `restart_on_failure` with bounded retries and rolling-window crash-loop protection

## What it does not support

- Compose `build:`
- `ports`
- custom Docker networks / `network_mode`
- Compose `restart:` as a Docker key (use `services.<name>.x-slurm.failure_policy` instead)
- `deploy`
- arbitrary multi-node orchestration or partial-node service placement
- mixed string/array `entrypoint` + `command` combinations in ambiguous cases

If you need image customization, use `image:` plus `x-enroot.prepare`, not `build:`.

## Fast path

```yaml
name: hello

x-slurm:
  time: "00:10:00"
  mem: 4G

services:
  app:
    image: python:3.11-slim
    command: python -c "print('Hello from Slurm!')"
```

```bash
hpc-compose submit --watch -f compose.yaml
```

`submit --watch` is the normal run. On an interactive TTY it opens the live watch UI; later you can reconnect with `hpc-compose watch -f compose.yaml` or inspect a one-shot per-service snapshot with `hpc-compose ps -f compose.yaml`. Break out `inspect`, `preflight`, or `prepare` as the debugging flow when you are validating a new spec for the first time or isolating a failure.

## Read next

- [Installation](installation.md) for release and source install paths
- [Quickstart](quickstart.md) for the shortest working flow
- [Support Matrix](support-matrix.md) for platform and runtime support expectations
- [Task Guide](task-guide.md) for goal-oriented workflow entry points
- [Execution model](execution-model.md) for the login-node / compute-node split
- [Runbook](runbook.md) for real-cluster setup and debugging
- [Examples](examples.md) for example selection and adaptation
- [Spec Reference](spec-reference.md) for the supported Compose subset
- [Supported Slurm model](supported-slurm-model.md) for the first-class / pass-through / out-of-scope boundary
- [Docker Compose Migration](docker-compose-migration.md) for feature mapping and conversion guidance
