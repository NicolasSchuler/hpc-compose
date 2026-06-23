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
- expected failure propagation from a nonzero service exit through `up`,
  `status`, `ps`, `logs`, and accounting
- real `sbatch --array` fan-out: per-task `sacct` accounting rows and the
  merged `status --array` view (`specs/_extra/array.yaml`)
- the `restart_on_failure` batch-supervisor loop — a service that fails twice
  then succeeds drains to COMPLETED only after real srun re-invocations
  (`specs/restart-policy.yaml`)
- `cancel` against a live RUNNING job: a real `scancel` drives `sacct` to the
  CANCELLED terminal state (`specs/_extra/long-running.yaml`)
- artifact teardown collection into the tracked payload dir, then `artifacts`
  export and `pull` resolution against a **real** manifest (`specs/artifacts.yaml`)
- scheduler-level inter-job dependencies — `x-slurm.after_job` (afterok) holds a
  consumer PENDING until the producer terminates, enforced ordering verified from
  accounting (`specs/_extra/dep-producer.yaml` + `dep-consumer.yaml`)
- `failure_policy: ignore` — a nonzero service exit does NOT fail the job
  (`specs/ignore-policy.yaml`); and `depends_on: service_completed_successfully`
  one-shot DAG ordering across three stages (`specs/pipeline-dag.yaml`)
- the tracked-state readers over a real run — `experiment`, `replay`, `debug`,
  `checkpoints`, `jobs list`, `clean --dry-run`
- the host-backend resume dir — `$HPC_COMPOSE_RESUME_DIR` resolves to the real
  on-node path, not the unmounted container mount (`specs/_extra/resume.yaml`)
- `alloc` opens a real `salloc` and `run` reuses that allocation via `srun`
  instead of a fresh `sbatch`

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
runs every spec under `specs/` through the real path. Each checked-in spec has
an explicit expected outcome, and the harness asserts:

- the job submits via real `sbatch`,
- it drains to the expected terminal state via `sacct` with the expected exit
  code shape,
- the expected log output is present, and
- `status` and `ps` render tracked runtime data.

For successful specs, the harness also checks `score` against `sacct`-backed
efficiency data. For all specs, it checks `stats` does not regress into the
known `sstat`/`AllocTRES` field mismatch.

Specs that self-terminate to a terminal state ride this generic
`up --watch-mode line` loop. Specs that need a different flow — `--detach` plus
polling, multi-job orchestration, or a `scancel` — live under `specs/_extra/`
(which the generic loop does **not** glob) and are driven by dedicated blocks
that submit detached, poll `sacct`/`squeue`, and cover: array fan-out, the
cancel→CANCELLED path, and scheduler inter-job dependencies. A post-loop
deep-check resolves the `artifacts.yaml` manifest through `pull` and `artifacts`.
A leaked detached job can't strand the single node: every `--detach` submission
is registered and `scancel`ed in the EXIT trap. Adding an `_extra/` spec without
a dedicated block fails the harness loudly, mirroring the generic registry.

The same image is also an SSH-reachable login-node stand-in (`sshd` + `rsync`,
port `2222`), which `scripts/devcluster_remote_e2e.sh` uses to exercise the thin
remote-submit path (`up --remote`) from the host: it rsyncs the project to the
node and submits over SSH, asserting a real remote `sbatch` tracked to
COMPLETED. That harness injects a throwaway per-run key (no credentials are
baked into the image).

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
| `specs/failing-service.yaml` | Negative `host`-backend spec proving nonzero service exits propagate through real scheduler state |
| `specs/restart-policy.yaml` | `restart_on_failure` supervisor: fails then succeeds, draining to COMPLETED via real srun re-invocations |
| `specs/artifacts.yaml` | Real artifact teardown collection + manifest, exercised end to end by the `pull`/`artifacts` deep-check |
| `specs/ignore-policy.yaml` | `failure_policy: ignore` — a nonzero service exit does not fail the job |
| `specs/pipeline-dag.yaml` | `depends_on: service_completed_successfully` one-shot DAG ordering across three stages |
| `specs/_extra/array.yaml` | `sbatch --array` fan-out (driven detached; the generic loop doesn't glob `_extra/`) |
| `specs/_extra/long-running.yaml` | A long sleep used by the cancel→CANCELLED block |
| `specs/_extra/dep-producer.yaml` | Producer half of the scheduler inter-job dependency block |
| `specs/_extra/dep-consumer.yaml` | Consumer half: `after_job` (afterok) held PENDING until the producer terminates |
| `specs/_extra/resume.yaml` | Host-backend resume dir: `$HPC_COMPOSE_RESUME_DIR` is a real on-node path, not the container mount |
| `../scripts/devcluster.sh` | `up` / `run` / `exec` / `sinfo` / `logs` / `down` wrapper |
| `../scripts/devcluster_e2e.sh` | UC1 end-to-end harness (generic loop + `_extra/` dedicated blocks; checks `sacct`/`status`/`ps`/`score`/`pull`) |
| `../scripts/devcluster_remote_e2e.sh` | UC2 end-to-end harness: drives `up --remote` from the host against this node as an SSH login-node stand-in (`sshd` + `rsync` in the image; port `2222`) |
