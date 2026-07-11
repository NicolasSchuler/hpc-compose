# Quickstart

This is the canonical `minimal-batch` cluster smoke: verify the binary, create
and check the smallest spec, configure shared storage, run strict preflight,
submit once, then reconnect through status and logs. It deliberately proves the
site and runtime path before you submit the multi-service, distributed, or
production workload you ultimately selected.

This page is not a generic execution recipe for every example: its commands and
expected signals intentionally name the single `app` service created below.
After it succeeds, return to [Examples](examples.md) and adapt the selected
workload shape.

If `sbatch`, `srun`, allocation, Pyxis, or Enroot are unfamiliar, read [Slurm
and Container Basics](slurm-container-basics.md) first. Check the [Support
Matrix](support-matrix.md) before expecting a runtime workflow to work on the
current machine.

## How to Read the Checklist

- **Authoring host** means a laptop, workstation, or login node with the CLI.
- **Submission host** means a supported Linux login node with Slurm and the
  selected backend. Commands after the submission boundary run there.
- **Compute quota** means CPU/GPU allocation time. Storage writes may still
  count against a filesystem quota.
- Expected signals are deliberately short and stable. Full transcripts vary by
  backend, site, cache state, and scheduler.

Before starting, use [Choose Your Workflow](task-guide.md#1-choose-the-runtime-backend)
to confirm which runtime backend the site supports. The `minimal-batch` template
created in step 2 omits `runtime.backend`, so it selects the default, `pyxis`.
After creating the file, set `runtime.backend` explicitly when the site uses
Apptainer or Singularity. For `host`, also remove the image and choose a finite
site-provided command as described in [Host Runtime Notes](runtime-backends.md#host-runtime-notes).
Do not submit until the static plan reports a backend the target site supports.

## 1. Verify the Installed Version

| Property | Value |
| --- | --- |
| Run on | Any authoring host |
| Slurm contact | None |
| Compute quota | None |

```bash
hpc-compose --version
```

Expected signal: one line beginning with `hpc-compose` and a semantic version.
Record it when asking for help; embedded docs and schemas match this binary.

Failure fork: if the command is missing or the version is not the one your
project expects, stop and follow [Installation](installation.md). Do not debug a
cluster with an unidentified binary.

## 2. Create the Smallest Batch Spec

| Property | Value |
| --- | --- |
| Run on | Any authoring host, in the project directory |
| Slurm contact | None |
| Compute quota | None; writes only `compose.yaml` |

```bash
hpc-compose new \
  --template minimal-batch \
  --name my-app \
  --output compose.yaml
```

Expected signal: `compose.yaml` exists and names one `app` service.

Failure fork: if the file already exists or a template name is rejected, do not
force an overwrite. Run `hpc-compose new --list-templates`, choose the intended
output path, then repeat this step.

## 3. Validate, Then Lint

| Property | Value |
| --- | --- |
| Run on | Any authoring host |
| Slurm contact | Forbidden by global `--offline` |
| Compute quota | None |

```bash
hpc-compose --offline validate --strict-env -f compose.yaml
hpc-compose --offline lint --allow-warnings --format json -f compose.yaml
```

Expected signals:

- validation exits successfully without unsupported-field errors;
- lint emits a JSON object, and every finding has a stable `HPC...` code (the
  pristine template lints clean, so expect an empty `findings` array here);
- `--allow-warnings` keeps advisory findings visible without turning this first
  pass into an unexplained non-zero exit once your spec grows real content.

Failure fork:

- Invalid YAML or field: fix the named field, then rerun `validate`; do not move
  on to planning.
- Missing interpolation variable: set it explicitly or intentionally encode a
  default; rerun `validate --strict-env`.
- Lint finding: run `hpc-compose --offline lint -f compose.yaml` for the human
  explanation. Preview auto-fixes with `lint --fix --dry-run`; do not apply a
  rewrite you have not reviewed.

## 4. Inspect the Static Execution Plan

| Property | Value |
| --- | --- |
| Run on | Any authoring host |
| Slurm contact | Forbidden by global `--offline` |
| Compute quota | None |

```bash
hpc-compose --offline plan --format json -f compose.yaml
hpc-compose --offline inspect --format json -f compose.yaml
```

Expected signals: one service named `app`, a deterministic service order, the
selected runtime backend, one allocation geometry, and normalized mount/image
information. Static planning does not import images, call `sbatch`, or write a
batch script.

Failure fork: run `hpc-compose --offline plan --explain -f compose.yaml` and
follow the first concrete hint. If the problem is a runtime/backend assumption,
return to [Choose Your Workflow](task-guide.md) instead of adding arbitrary
Slurm flags.

> **No cluster yet? Preview the full submission locally.** You can see exactly
> what `up` would submit without any Slurm access:
>
> ```bash
> hpc-compose up --dry-run -f compose.yaml   # any host, incl. macOS
> ```
>
> This renders the complete submission script to `./hpc-compose.sbatch` (use
> `--script-out` to choose the path) and reports `"dry_run": true` — no
> preflight, SSH, or Slurm contact. On a Linux machine without Slurm you can go
> one step further and execute the job with `hpc-compose up --local`. Steps 5-9
> below need a real submission host.

## 5. Configure Shared Cache and Project Context

Move to the real submission host for this step. Ask the site's documentation or
support channel for an approved project/work/scratch path visible from login
and compute nodes.

| Property | Value |
| --- | --- |
| Run on | Slurm submission host |
| Slurm contact | None |
| Compute quota | None; directory creation may consume storage quota |

```bash
export CACHE_DIR=<shared-cache-dir>
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
hpc-compose setup \
  --profile-name cluster \
  --cache-dir "$CACHE_DIR" \
  --default-profile cluster \
  --non-interactive
hpc-compose context --format json
```

Expected signals: the write test succeeds, and context JSON reports the
`cluster` profile plus the intended cache path and source.

Failure fork:

- Missing or expired site workspace: use [cluster onboarding](cluster-profiles.md),
  the applicable generated site guide, or `hpc-compose workspace status`;
  provision it before continuing.
- Context shows a fallback under `$HOME/.cache`: correct the selected profile
  or explicit `x-slurm.cache_dir`.
- Never substitute `/tmp`, `/var/tmp`, `/private/tmp`, `/dev/shm`, `$TMPDIR`, or
  job-local burst storage for a shared cache.

## 6. Run Strict Preflight

| Property | Value |
| --- | --- |
| Run on | Slurm submission host |
| Slurm contact | May read scheduler/capability state; does not submit without `--fs-probes` |
| Compute quota | None without `--fs-probes` |

```bash
hpc-compose preflight --strict --format json -f compose.yaml
```

Expected signal: JSON reports no blocking failure for Slurm tools, selected
backend, cache policy, mounts, image inputs, cluster profile, or distributed
configuration.

Failure fork: fix the first failed prerequisite and rerun the same command. Use
`hpc-compose debug --preflight -f compose.yaml` only when tracked context also
matters. Do not bypass a failure with `up --no-preflight` on a first run.

For production-bound shared paths, add the active compute-node probe only with
explicit quota authorization:

| Property | Value |
| --- | --- |
| Run on | Slurm submission host |
| Slurm contact | **Submits a tiny job** |
| Compute quota | **Yes** |

```bash
hpc-compose preflight --strict --fs-probes -f compose.yaml
```

Expected signal: compute-node visibility, rename behavior, and headroom probes
pass for the configured shared paths. Failure fork: move the path to approved
shared storage or contact site support; do not treat the login-node write test
as equivalent evidence.

---

> **Submission boundary:** the next command prepares external image content,
> calls Slurm, and can consume allocation quota. Confirm the plan, account,
> partition, walltime, and accelerator request before continuing.

## 7. Submit Once and Detach

| Property | Value |
| --- | --- |
| Run on | Slurm submission host |
| Slurm contact | **Calls `sbatch` and scheduler tools** |
| Compute quota | **Yes, once the allocation starts** |

```bash
hpc-compose up --detach --format json -f compose.yaml
```

Expected signals: one JSON object with a tracked Slurm job id and next-command
context. First use of an image may spend several minutes importing and preparing
it before submission; later runs can reuse the cache.

Failure fork:

- Preparation/backend error before a job id: rerun strict preflight, then use
  `hpc-compose prepare -f compose.yaml` only to isolate image preparation.
- Submission rejected: read the exact account/partition/GRES message and compare
  the site guide; do not immediately resubmit the same request.
- Tracked failure after a job id exists: run
  `hpc-compose debug --preflight -f compose.yaml`.

## 8. Read Scheduler and Service Status

| Property | Value |
| --- | --- |
| Run on | Slurm submission host |
| Slurm contact | Scheduler reads (`squeue` / `sacct` as available) |
| Compute quota | No new allocation; a running tracked job continues consuming its request |

```bash
hpc-compose status --format json -f compose.yaml
```

Expected signal: the same job id plus an explicit scheduler state such as
`PENDING`, `RUNNING`, or `COMPLETED`, with tracked service information when
available.

Failure fork:

- No tracked job: run `hpc-compose jobs list --format json` and select the
  intended id explicitly.
- `PENDING`: inspect the scheduler reason; this is queue evidence, not a reason
  to submit a duplicate.
- Terminal failure or contradictory evidence: run
  `hpc-compose status --verify --format json -f compose.yaml`, then `debug`.

## 9. Read the First Log

| Property | Value |
| --- | --- |
| Run on | Slurm submission host |
| Slurm contact | None for local tracked logs |
| Compute quota | None; a running job continues independently |

```bash
hpc-compose logs --service app --lines 100 -f compose.yaml
```

Expected signal: output from the `app` service, or an explicit “not available
yet”/path hint while the job is pending or starting. Use `--follow` only when a
stream is useful; the bounded command above is deterministic for scripts and
support captures.

Failure fork: run `hpc-compose debug -f compose.yaml`. If the batch job ran but
the service log did not appear, the debug report distinguishes scheduler,
launcher, readiness, and service-exit evidence.

The smoke is successful when the job reaches `COMPLETED`, the service exits
successfully, and its expected log is readable. Return to [Examples](examples.md)
to adapt the selected workload, continue with the [Runbook](runbook.md) for
repeat operations, or use [Worked Failure Recovery](failure-recovery.md) when a
stage does not reach its expected signal.

## Read Next

- [Command Families](command-families.md)
- [Operate a Real Cluster Run](runbook.md)
- [Production Readiness](production-readiness.md)
- [Worked Failure Recovery](failure-recovery.md)
- [Troubleshooting](troubleshooting.md)
