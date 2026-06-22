# Local Slurm dev cluster

Run hpc-compose specs against a **real single-node Slurm scheduler** on your
laptop — no cluster login required, and no fake-Slurm simulation. The cluster is
one container running `slurmctld` + `slurmd`, with `hpc-compose` preinstalled.

This is the laptop on-ramp for the Mac↔login-node split: you exercise the *real*
`up` → `sbatch` → `slurmd` → `squeue`/`scontrol` path locally, then run the same
spec unchanged on the cluster.

## Status

> **Boot-verified** on macOS (Apple Silicon) with Podman 5.7 (libkrun): the node
> registers `idle`, and `hpc-compose up` submits via real `sbatch`, runs the
> service, and tracks the job to `COMPLETED` via `sacct` (exit 0).

## Requirements

- `docker compose` **or** `podman compose`, with the engine running.
  - macOS Podman: `podman machine init && podman machine start` first.
- A **privileged** container (set in `compose.yaml`). Rootless engines mount
  `/sys/fs/cgroup` read-only, so slurmd can't create its cgroup/v2 scope without
  it. This is a throwaway local dev node, not a shared host.
- The first `up` compiles `hpc-compose` for the container (a few minutes) and
  installs Slurm + MariaDB; later starts are fast.

## Quickstart

```bash
# Build the image and start the cluster (mounts the repo at /workspace).
scripts/devcluster.sh up

# Confirm the node registered and is idle.
scripts/devcluster.sh sinfo

# Submit the smallest end-to-end spec against the real scheduler.
scripts/devcluster.sh run dev-cluster/specs/hello.yaml

# Prove multi-service ordering: a `client` waits on a `server` readiness gate
# (depends_on) before hitting it, then the allocation drains to COMPLETED.
scripts/devcluster.sh run dev-cluster/specs/multi-service.yaml

# Tear down when done.
scripts/devcluster.sh down
```

To work on your own project instead of this repo:

```bash
scripts/devcluster.sh up --project /path/to/your/project
scripts/devcluster.sh run compose.yaml
```

Everything the wrapper does is a thin shell over `docker/podman compose` and
`exec`; run those directly if you prefer (see `compose.yaml`).

## What this validates — and what it doesn't

Use `runtime.backend: host` for dev-cluster specs (see `specs/hello.yaml`). That
runs each service as a plain process on the node, which is what makes the local
loop tractable.

**Validated locally** (the bulk of hpc-compose's value, and the easy-to-break
part):

- spec rendering and `sbatch` submission against a real controller
- service startup ordering (`depends_on`) and readiness gates
- multi-service composition inside one allocation
- scheduler-facing observability: `up`, `status`, `watch`, `logs`, `ps`
- `sacct`-backed commands — `up` tracks to a terminal state, `score` reports
  efficiency — thanks to the in-container `slurmdbd` + MariaDB

**Not validated locally** (revalidate on the cluster):

- the container-runtime layer (`pyxis`/`enroot`, `apptainer`). Enroot needs
  unprivileged user namespaces that don't nest cleanly inside Docker/Podman
  (`enroot-nsenter: failed to create user namespace`), so the dev cluster uses
  `host` instead. Containerized services are validated on the real cluster.
- GPU execution (no NVIDIA on a Mac).
- `stats` live step sampling (`sstat`) — it applies only to a running job, and
  on this Slurm build emits an `Invalid field requested: AllocTRES` notice for a
  completed one. Post-run accounting via `sacct` (used by `score`) works.

## Automated end-to-end check

`scripts/devcluster_e2e.sh` (also `just dev-cluster-e2e`) boots the cluster and
runs every spec under `specs/` through the real path, asserting for each one:

- the job submits via real `sbatch`,
- it drains to `COMPLETED` via `sacct` with exit code `0:0`,
- the expected log output is present, and
- `status` and `score` render the terminal/efficiency data.

CI runs this as a **separate** `dev-cluster-e2e` job (privileged container on a
Linux runner) that runs in parallel with — and never gates — the fast
lint/unit lanes. It prebuilds the image with a cached cargo build layer
(`docker/build-push-action` + `type=gha`), then boots with
`DEVCLUSTER_SKIP_BUILD=1` to reuse it. This is the harness that closes the
unit-suite gap: it exercises the scheduler/cluster code paths the unit tests
mock out. The `host`-backend scope above still applies — the e2e check does
**not** cover the `pyxis`/`enroot` runtime layer or GPU execution.

## Files

| File | Purpose |
| --- | --- |
| `Dockerfile` | Multi-stage build: compile `hpc-compose`, then a Slurm + MariaDB node image |
| `slurm.conf.tmpl` | Single-node, container-safe Slurm config (CPUs/RAM filled in at boot) |
| `cgroup.conf` | `IgnoreSystemd=yes` so slurmd skips the absent dbus/systemd scope |
| `slurmdbd.conf` | Accounting daemon config (installed 0600 at boot) for `sacct` |
| `entrypoint.sh` | munge → MariaDB + slurmdbd → `slurmctld`/`slurmd`; surfaces failures |
| `compose.yaml` | One-service, privileged compose for `docker compose`/`podman compose` |
| `specs/hello.yaml` | Smallest `host`-backend spec to prove the loop |
| `specs/multi-service.yaml` | Two `host`-backend services proving `depends_on` + a readiness gate (server/client) against the real scheduler |
| `../scripts/devcluster.sh` | `up` / `run` / `exec` / `sinfo` / `logs` / `down` wrapper |
| `../scripts/devcluster_e2e.sh` | End-to-end assertion harness (boots, runs every spec, checks `sacct`/`status`/`score`) |
