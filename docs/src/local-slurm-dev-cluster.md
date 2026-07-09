# Local Slurm Dev Cluster

The local Slurm dev cluster is source-checkout tooling for running hpc-compose
against a real throwaway scheduler on a laptop. It starts one privileged
Docker/Podman container with `slurmctld`, `slurmd`, `slurmdbd`, MariaDB, and the
current checkout's `hpc-compose` binary.

Use it when you want a real scheduler smoke test before moving to a shared
cluster. It is not a dry-run: `scripts/devcluster.sh run ...` calls real
`sbatch` inside the local container. The job consumes only the local throwaway
Slurm node.

## Preview Levels

| Goal | Command | Scheduler contact | Writes runtime state |
| --- | --- | --- | --- |
| Static authoring preview | `hpc-compose plan --show-script -f compose.yaml` | No | No |
| Preflight, prepare, and render without submission | `hpc-compose up --dry-run -f compose.yaml` | No `sbatch` | Writes the rendered script |
| Real local scheduler smoke test | `scripts/devcluster.sh run compose.yaml` or `hpc-compose test --submit --dev-cluster -f compose.yaml` | Local dev-cluster `sbatch` | Yes, inside the mounted project |

Use `plan` first for fast static feedback. Use `up --dry-run` when you want the
same preflight and preparation path as submission but no `sbatch`. Use the dev
cluster when you specifically want to exercise hpc-compose's real
`up -> sbatch -> slurmd -> sacct` path without a cluster login.

## Requirements

- A source checkout of this repository. Release archives install the CLI and
  manpages, not the dev-cluster wrapper and Dockerfile.
- `docker compose` or `podman compose`, with the engine running.
- Support for privileged containers. The local node needs cgroup access for
  `slurmd`; treat it as a disposable developer machine workflow.

## Quickstart

From the repository root:

```bash
scripts/devcluster.sh up
scripts/devcluster.sh sinfo
scripts/devcluster.sh run dev-cluster/specs/hello.yaml
hpc-compose test --submit --dev-cluster -f dev-cluster/specs/_extra/test-pass.yaml
scripts/devcluster.sh down
```

To smoke-test another project tree with the same local Slurm node:

```bash
scripts/devcluster.sh up --project /path/to/project
scripts/devcluster.sh run compose.yaml
hpc-compose test --submit --dev-cluster -f compose.smoke.yaml
scripts/devcluster.sh down
```

`test --submit --dev-cluster` is a source-checkout convenience wrapper: it starts
the checked-in dev cluster with the current project mounted at `/workspace`, then
runs `hpc-compose test --submit` inside the container. It is useful on macOS for
a real local `sbatch` smoke test, but it does not make macOS a real runtime
execution host.

Specs run in the dev cluster should use `runtime.backend: host`. That keeps the
local loop tractable and avoids nesting Pyxis/Enroot or Apptainer inside
Docker/Podman. If your production spec uses a container backend, keep a small
host-backend smoke variant for local scheduler validation and revalidate the
container runtime on the real cluster.

## Automated Check

Maintainers can run the checked-in real-scheduler suite with:

```bash
DEVCLUSTER_E2E_DOWN=1 scripts/devcluster_e2e.sh
```

The script boots the cluster, runs every spec under `dev-cluster/specs`, asserts
that each spec has an explicit expected outcome, and verifies scheduler-backed
commands such as `status`, `ps`, `logs`, and `score` where applicable. CI runs
the same harness as a separate `Dev Cluster E2E` job with a cached image build.

### Remote and OTP Harnesses

The same image doubles as an SSH-reachable login-node stand-in (`sshd` + `rsync`,
host port `2222`), so two further harnesses exercise the laptop thin client:

```bash
just dev-cluster-remote-e2e   # scripts/devcluster_remote_e2e.sh
just dev-cluster-otp-e2e      # scripts/devcluster_otp_e2e.sh
```

- **Remote submit** (`devcluster_remote_e2e.sh`) drives `hpc-compose up --remote`
  from the host and proves the thin remote-submit path: the project is staged
  over `rsync`, a real `sbatch` runs on the node and tracks to `COMPLETED`, and
  `up --remote --dry-run` stages-but-doesn't-submit. It injects a throwaway
  per-run SSH key, so no credentials are baked into the image.
- **One OTP per session** (`devcluster_otp_e2e.sh`) flips the stand-in into an
  OTP/2FA-requiring sshd, then runs a multi-command laptop session
  (`up --remote`, a second `up --remote --dry-run`, and a `pull`-style transfer)
  and asserts it authenticates **exactly once** — the SSH ControlMaster
  multiplexing hpc-compose relies on so a real login node prompts only on the
  first connection.

Both harnesses require the same privileged container as `dev-cluster-e2e` and
route the ControlMaster socket through a per-run temp dir (nothing is written to
your `~/.ssh`). The host SSH port is configurable with `DEVCLUSTER_SSH_PORT`
(default `2222`) if `2222` is already in use.

## Scope

Validated locally:

- `sbatch` submission against a real controller
- service ordering and readiness gates
- multi-service composition inside one allocation
- terminal accounting through `sacct`
- scheduler-facing observability for tracked runs
- expected failure propagation for negative smoke specs
- `sbatch --array` fan-out with per-task accounting and `status --array`
- the `restart_on_failure` supervisor draining to COMPLETED through real restarts
- `cancel` driving a running job to the CANCELLED terminal state, with tracked-state teardown
- artifact teardown collection resolved by `pull`/`artifacts` against a real manifest
- scheduler inter-job dependencies (`after_job` holds a consumer until the producer ends)
- `failure_policy: ignore` and `depends_on: service_completed_successfully` ordering
- tracked-state readers over a real run (`experiment`, `replay`, `debug`, `checkpoints`, `jobs`, `clean`)
- the host-backend resume dir resolving to a real on-node path
- `alloc` + `run` reusing one allocation via `srun`

Still validate on the real cluster:

- Pyxis/Enroot, Apptainer, or Singularity runtime behavior
- GPU execution
- site-specific modules, filesystems, partitions, and accounting policy
- multi-node network and placement behavior

## Related Docs

- [Quickstart](quickstart.md)
- [Develop and Smoke-Test Locally](development-workflow.md)
- [Operate a Real Cluster Run](runbook.md)
- [Troubleshoot a Failed Run](troubleshooting.md)
