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

### Configuration knobs

- `DEVCLUSTER_SSH_PORT` (default `2222`) sets the host port mapped to the
  container's sshd (the login-node stand-in used by the remote/OTP harnesses).
  Export it to avoid a clash with something already bound to `2222`, e.g.
  `DEVCLUSTER_SSH_PORT=2223 scripts/devcluster.sh up`. Both e2e harnesses honour
  the same variable, so set it once in your shell before running either.
- `SLURM_VERSION` (build ARG, default pins the Slurm packages to a known-good
  release) makes image builds reproducible. Bump it when the base image
  supersedes that version — see the comment in `Dockerfile`; build with
  `--build-arg SLURM_VERSION=` (empty) to unpin.

## Quickstart

```bash
# Build the image and start the cluster (mounts the repo at /workspace).
scripts/devcluster.sh up

# Confirm the node registered and is idle.
scripts/devcluster.sh sinfo

# Submit the smallest end-to-end spec against the real scheduler.
scripts/devcluster.sh run dev-cluster/specs/hello.yaml

# Or drive the checked-in test --submit smoke path through the dev cluster.
hpc-compose test --submit --dev-cluster -f dev-cluster/specs/_extra/test-pass.yaml

# Prove multi-service ordering: a `client` waits on a `server` readiness gate
# (depends_on) before hitting it, then the allocation drains to COMPLETED.
scripts/devcluster.sh run dev-cluster/specs/multi-service.yaml

# Preview a run WITHOUT submitting: render the exact sbatch and stop. `run`
# forwards extra args to `hpc-compose up`, so `--dry-run` works against the
# dev cluster too — nothing reaches the scheduler (see "Safe dry-runs" below).
scripts/devcluster.sh run dev-cluster/specs/hello.yaml --dry-run

# Tear down when done.
scripts/devcluster.sh down
```

### Safe dry-runs against the dev cluster

The dev cluster is the safest place to preview a real run: `up --dry-run` renders
the exact sbatch it would submit and stops — no job is created, so the queue and
accounting database stay untouched. It works both ways:

```bash
# In-container (running ON the login node): renders .../hpc-compose.sbatch, exit 0,
# and submits nothing. Add --format json for {submitted:false, job_id:null, dry_run:true}.
scripts/devcluster.sh run dev-cluster/specs/hello.yaml --dry-run

# Host preview with a remote target configured: still renders locally and exits
# before SSH, rsync, preparation, or submission. The target may be unreachable.
HPC_COMPOSE_REMOTE_SSH_OPTS="-p 2222 -i <your-key> -o StrictHostKeyChecking=no" \
  hpc-compose up --remote=root@localhost -f dev-cluster/specs/hello.yaml --dry-run
```

Both paths are asserted end to end: the rendered script exists and is valid,
the remote preview creates no staged project, and `squeue`/`sacct` show no new
job. See "Automated end-to-end check".

To work on your own project instead of this repo:

```bash
scripts/devcluster.sh up --project /path/to/your/project
scripts/devcluster.sh run compose.yaml
hpc-compose test --submit --dev-cluster -f compose.smoke.yaml
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
- safe dry-runs: `up --dry-run` (in-container) and `up --remote --dry-run`
  (host-side) render the real sbatch locally but submit nothing; the remote form
  also proves that no SSH session or remote stage is created
- the **one-OTP-per-session** property of the laptop thin client: the login-node
  stand-in is flipped into an OTP/2FA-requiring sshd, and a multi-command session
  (`up --remote`, a remote `status`, and a `pull`-style transfer)
  is shown to authenticate **exactly once** via SSH ControlMaster multiplexing
- read-side affordances over the live scheduler: `weather` (live node/queue
  signals), `diff` (pairwise + N-way comparison of two real runs), `when`
  (evaluates live conditions and declines to submit when they are unmet), and the
  interactive `watch` TUI driven under a pseudo-terminal (it enters **and**
  restores the alternate screen, so it never leaves the terminal in a bad state)

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

### Selectable local cases (not CI)

Three higher-impact scheduler contracts are available as opt-in local cases.
They are deliberately **not** added to the CI E2E matrix:

```bash
just dev-cluster-cases
just dev-cluster-case preemption
just dev-cluster-case fs-probes
just dev-cluster-case remote-reads
```

`preemption` proves a real `USR1` checkpoint, `scontrol requeue`, attempt-2
resume, and checkpoint history. `fs-probes` runs the active `sbatch --wait`
visibility/rename/headroom probe. `remote-reads` submits an artifact-producing
job through the SSH stand-in, then reads that same job through remote `status`,
`stats`, `logs`, `score`, and `pull` without creating another allocation.

The cluster stays up between cases for fast local iteration. Set
`DEVCLUSTER_E2E_DOWN=1` on a case invocation to tear it down afterward. A failed
case preserves scheduler and container diagnostics under
`.tmp/devcluster-cases/`; a successful case removes its temporary state. The
runner serializes local cases, snapshots the pre-existing queue, and avoids
blanket cleanup for these selectable cases.

Specs that self-terminate to a terminal state ride this generic
`up --watch-mode line` loop. Specs that need a different flow — `--detach` plus
polling, multi-job orchestration, or a `scancel` — live under `specs/_extra/`
(which the generic loop does **not** glob) and are driven by dedicated blocks
that submit detached, poll `sacct`/`squeue`, and cover: array fan-out, the
cancel→CANCELLED path, and scheduler inter-job dependencies. A post-loop
deep-check resolves the `artifacts.yaml` manifest through `pull` and `artifacts`.
A leaked detached job can't strand the single node: every `--detach` submission
is registered and `scancel`ed in the EXIT trap. Adding an `_extra/` spec without
a dedicated block fails the harness loudly, mirroring the generic registry. A
post-loop block also proves the **dry-run-submits-nothing** property: `up
--dry-run` renders a valid sbatch while the queue and accounting db stay
unchanged (text and `--format json` forms). Further blocks drive the read-side
affordances against the live scheduler — `weather`, an N-way `diff` of two real
runs, a `when` that evaluates conditions and declines to submit, and the
interactive `watch` TUI under a pseudo-terminal (asserting it enters and restores
the alternate screen).

The same image is also an SSH-reachable login-node stand-in (`sshd` + `rsync`,
port `2222`), which `scripts/devcluster_remote_e2e.sh` uses to exercise the thin
remote-submit path (`up --remote`) from the host: it rsyncs the project to the
node and submits over SSH, asserting a real remote `sbatch` tracked to
COMPLETED, then that `up --remote --dry-run` remains a local static preview and
does not recreate the remote stage. That harness injects a throwaway per-run key
(no credentials are baked into the image).

`scripts/devcluster_otp_e2e.sh` (also `just dev-cluster-otp-e2e`) closes the
last laptop-thin-client gap: real login nodes demand an OTP/2FA per SSH session,
and hpc-compose copes via SSH ControlMaster multiplexing so a whole session
authenticates **once**. The harness flips the stand-in into an OTP-requiring
sshd (publickey **plus** an interactive second factor counted by a `pam_exec`
hook — see `otp-sim.sh`), verifies a key-only login is now *rejected*, then
drives a multi-command laptop session (`up --remote`, a remote `status`, and a
`pull`-style `rsync`) and asserts **exactly one**
authentication occurred — corroborated by the live ControlMaster socket and `ssh
-O check`. It restores the key-only sshd and removes the control socket on exit.

CI runs this as a **separate** `dev-cluster-e2e` job (privileged container on a
Linux runner) that runs in parallel with — and never gates — the fast
lint/unit lanes. It prebuilds the image with a cached cargo build layer
(`docker/build-push-action` + `type=gha`), then boots with
`DEVCLUSTER_SKIP_BUILD=1` to reuse it. This is the harness that closes the
unit-suite gap: it exercises the scheduler/cluster code paths the unit tests
mock out. The `host`-backend scope above still applies — the e2e check does
**not** cover the `pyxis`/`enroot` runtime layer or GPU execution.

## Manual real-GPU check (metrics pipeline)

The dev cluster is GPU-less by design (no NVIDIA on a Mac), so the one gap the
automated harnesses cannot close is the **real-GPU metrics pipeline**:
`gpu.jsonl`, the sampler's `gpu` node in `stats --format json`, and a populated
`gpu_count`. `scripts/remote_gpu_e2e.sh` (also `just remote-gpu-e2e`) closes it
against a real cluster (HAICORE by default). It is **opt-in and manual**: it
needs a live login node, a real GPU allocation, and **one** interactive OTP, so
it is deliberately **not** part of `just ci` and never runs in CI.

It drives the thin laptop client end to end over a **single OTP session**: it
opens one SSH ControlMaster up front (the only prompt), then `up --remote`s a
tiny 1-GPU `cuda-probe`-style job (`examples/cuda-probe.yaml`), watches it to
COMPLETED, reads `stats --remote --format json` (asserting `sampler.cpu`, gpu
`nodes`, and `gpu_count >= 1`), and `rsync`s the job-local `gpu.jsonl` /
`cpu.jsonl` over the shared master to assert non-null GPU
utilization/memory and non-null CPU utilization rows. Every later
`ssh`/`rsync`/`--remote` reuses the one master, so the whole run costs one OTP.

Run it by hand (defaults `HPC_REMOTE_HOST=haicore`, `HPC_SLURM_ACCOUNT=kastel`,
`HPC_SLURM_PARTITION=normal`, `HPC_SLURM_GRES=gpu:1` — HAICORE has `gpu:1`, not
`gpu:full`):

```sh
# Uses your ~/.ssh config alias for the login host (hostname/user/OTP).
HPC_REMOTE_HOST=haicore just remote-gpu-e2e
```

Safety and cleanup (EXIT trap, safe to re-run): it cancels **only** the job id
this run submitted — never a blanket `scancel`, since a real cluster may hold
unrelated production jobs — removes this run's remote stage dir
(`~/.hpc-compose-remote/remote-gpu-e2e`), and closes the ControlMaster.

## Files

| File | Purpose |
| --- | --- |
| `Dockerfile` | Multi-stage build: compile `hpc-compose`, then a Slurm + MariaDB node image |
| `slurm.conf.tmpl` | Single-node, container-safe Slurm config (CPUs/RAM filled in at boot) |
| `cgroup.conf` | `IgnoreSystemd=yes` so slurmd skips the absent dbus/systemd scope |
| `slurmdbd.conf` | Accounting daemon config (installed 0600 at boot) for `sacct` |
| `entrypoint.sh` | munge → MariaDB + slurmdbd → `slurmctld`/`slurmd` → `sshd`; surfaces failures |
| `otp-sim.sh` | `otp-sim {enable\|disable\|reset\|count}`: toggles the sshd login-node stand-in into an OTP/2FA-requiring mode and counts authentications (used by the one-OTP e2e) |
| `pty-run.py` | Runs a command under a fresh pseudo-terminal (sized 40×120) and captures its output, so the e2e can drive the crossterm `watch` TUI non-interactively |
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
| `specs/_extra/when.yaml` | Pins `x-slurm.partition` so the `when` block can evaluate `--free-nodes` against the live scheduler and decline to submit |
| `specs/_extra/watch-tui.yaml` | A ~20s job that stays RUNNING long enough for the pty-driven `watch` TUI block to attach, render, and auto-exit on success |
| `specs/_extra/sweep.yaml` | Embedded `sweep` block: `sweep submit` fans two trials into independent tracked sbatch jobs, then `sweep status`/`results` and sacct agree per trial |
| `specs/_extra/test-pass.yaml` | Passing half of the `test --submit` smoke block (service completes → "smoke test passed", exit 0) |
| `specs/_extra/test-fail.yaml` | Failing half of the `test --submit` smoke block (service exits nonzero → "smoke test failed", nonzero exit) |
| `specs/_extra/germinate.yaml` | `germinate` renders + submits a minimized canary, waits for terminal, and rightsizes from sacct accounting |
| `specs/_extra/down.yaml` | A long sleep used by the `down --job-id --yes` block: real scancel → CANCELLED plus tracked-state reaping |
| `specs/_local/preemption.yaml` | Opt-in local-only USR1 checkpoint → requeue → resumed-attempt contract |
| `specs/_local/fs-probes.yaml` | Opt-in local-only input for the active shared-filesystem probe |
| `../scripts/devcluster.sh` | `up` / `run` / `exec` / `sinfo` / `logs` / `down` wrapper |
| `../scripts/devcluster_case.sh` | Selectable local-only case dispatcher (`preemption`, `fs-probes`, `remote-reads`); not run in CI |
| `../scripts/devcluster_local_case.sh` | Shared lifecycle, scoped cleanup, and failure diagnostics for in-container local cases |
| `../scripts/devcluster_case_assert.py` | Stable JSON contract assertions used by the selectable local cases |
| `../scripts/devcluster_e2e.sh` | UC1 end-to-end harness (generic loop + `_extra/` dedicated blocks; checks `sacct`/`status`/`ps`/`score`/`pull`) |
| `../scripts/devcluster_remote_e2e.sh` | UC2 end-to-end harness: drives `up --remote` from the host against this node as an SSH login-node stand-in (`sshd` + `rsync` in the image; port `2222`); also asserts remote `--dry-run` is a local static preview with no remote stage or submission |
| `../scripts/devcluster_otp_e2e.sh` | UC3 end-to-end harness: flips the stand-in into an OTP/2FA-requiring sshd and proves a multi-command laptop session authenticates exactly once via SSH ControlMaster multiplexing |
| `../scripts/remote_gpu_e2e.sh` | Opt-in **real-GPU** manual check (`just remote-gpu-e2e`, HAICORE by default; **not** in CI): one-OTP session that `up --remote`s a 1-GPU job and asserts the metrics pipeline (`gpu.jsonl`, `cpu.jsonl`, `stats --format json` sampler nodes + `gpu_count`) against real hardware |
