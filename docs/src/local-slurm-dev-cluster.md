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
| Real local scheduler smoke test | `scripts/devcluster.sh run compose.yaml` | Local dev-cluster `sbatch` | Yes, inside the mounted project |

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
scripts/devcluster.sh down
```

To smoke-test another project tree with the same local Slurm node:

```bash
scripts/devcluster.sh up --project /path/to/project
scripts/devcluster.sh run compose.yaml
scripts/devcluster.sh down
```

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
