# Operate a Real Cluster Run

This runbook starts **after** the canonical [Quickstart](quickstart.md) has
succeeded on the target cluster. It owns repeat launches, remote delegation,
monitoring, change handling, and recovery. It does not repeat the first-run
checklist or the example catalog.

Commands assume `hpc-compose` is on `PATH`. Global `--profile <NAME>` selects a
settings profile; `--settings-file <PATH>` bypasses upward settings discovery.

## Before Each Meaningful Run

Use the cheapest evidence that matches what changed:

| Change since the last successful run | Check before launch |
| --- | --- |
| YAML or interpolation values | `validate --strict-env`, `lint`, then `plan --format json` |
| Service topology, placement, commands, or mounts | `inspect --format json` and `explain` |
| Base image or prepare commands | `prepare` separately when diagnosis is useful; otherwise normal `up` |
| Backend, modules, account, partition, GRES, or shared path | `preflight --strict`; use an explicitly authorized active probe when needed |
| Resume-coupled values | `up --resume-diff-only`, then decide whether changes are intentional |
| Long/costly production configuration | [Production Readiness](production-readiness.md) |

`plan --verbose`, `plan --show-script`, rendered scripts, and `--show-values` can
contain sensitive values. Prefer redacted JSON and `explain`; keep an explicitly
requested script owner-only.

## Resolve the Active Context

Project settings live in `.hpc-compose/settings.toml` and are discovered by
walking upward. Check the effective context before operating the wrong spec,
profile, cache, or remote host:

```bash
hpc-compose context --format json
hpc-compose --profile production context --format json
```

Resolution remains CLI flags, selected profile, shared defaults, then built-in
defaults. Resource profiles referenced by `x-slurm.resources` are Slurm request
defaults; they are not the same as global `--profile`.

Cluster capability profiles in `.hpc-compose/cluster.toml` are advisory. They
do not allocate accounts or workspaces, create directories, load modules, or
silently alter the spec. Use [Cluster Profiles](cluster-profiles.md) and the
applicable generated site guide when policy changes.

## Normal Run: Use `up`

```bash
hpc-compose up -f compose.yaml
```

`up` runs preflight, prepares missing runtime artifacts, renders one script,
submits through `sbatch`, writes tracked metadata, and watches state/logs. It
uses a spec-scoped lock so concurrent launches of the same file do not race
through prepare and submission.

Operational options:

| Need | Option | Boundary |
| --- | --- | --- |
| Submit and return | `--detach` | Still prepares and submits; use `--format json` for one stable result object. |
| Preview the full runtime path | `--dry-run` | No real submission; does not authorize a later real run. |
| Keep a script file | `--script-out <PATH>` | File may contain secrets; choose an owner-only location. |
| Refresh imported/prepared artifacts | `--force-rebuild` | May download and rebuild large images before submission. |
| Reuse existing prepared artifacts | `--skip-prepare` | Only safe when the required cache entries already exist. |
| Skip readiness checks on the host | `--no-preflight` | Avoid for production; record why the check is intentionally bypassed. |
| Wait visibly in the queue | `--watch-queue` and `--queue-warn-after <DURATION>` | Polls scheduler state; does not improve priority. |
| Force stable text output | `--watch-mode line` | Useful for CI, remote streams, and assistive tools. |
| Keep or close final TUI | `--hold-on-exit never\|failure\|always` | Presentation only. |
| Review resume-sensitive drift | `--resume-diff-only` | Static comparison; does not launch. |
| Accept reviewed resume drift | `--allow-resume-changes` | Real launch still requires normal quota authorization. |

Array jobs should normally use `up --detach`; task-specific outputs need `%A_%a`
or equivalent identifiers. Scheduler dependencies in `x-slurm.after_job` or
`x-slurm.dependency` remain submit-time Slurm dependencies. Arrays and
scheduler dependencies are not supported by `up --local`.

## 5b. Submit From Your Laptop With `up --remote`

Remote mode lets the laptop stage a project and delegate the same Linux/Slurm
operation to a login node:

```bash
# Uses login_host/login_user from the selected settings profile.
hpc-compose up --remote -f compose.yaml

# Explicit SSH alias, host, or user@host.
hpc-compose up --remote=login01 -f compose.yaml
hpc-compose up --remote=alice@login01 -f compose.yaml
```

### Remote setup and staging boundary

Keep `.hpc-compose/settings.toml` at the repository root. That directory becomes
the staging base; placing settings only beside a nested compose file can stage
only that subtree and hide the rest of the source tree. Remote staging includes
project settings and the cluster profile while excluding tracked job/runtime
state.

`up --remote` stages the repository. It deliberately does **not**:

- provision a site account, partition, reservation, or QOS;
- allocate/extend a workspace or create cache, dataset, resume, and artifact directories;
- copy durable results out of expiring storage;
- turn laptop environment values into remote secrets unless they are part of an explicitly configured source.

Use the shipped `workspace` commands or site tools before delegation, then
create the host directories referenced by the staged spec. Commands in
`x-slurm.setup` run later, inside the allocation; they cannot repair missing
submission-host paths.

### Remote binary and connection

Remote mode probes the login node for `hpc-compose`. `--remote-install
auto|never|force` controls whether a missing/older binary is installed into
`~/.local/bin`; choose `never` on locked-down or air-gapped hosts. Auto/force
installation contacts the release source before staging and should match site
policy.

Put ports, identities, and jump hosts in `~/.ssh/config`. Remote commands reuse
an SSH ControlMaster so an OTP/2FA login can be authenticated once during the
session. `HPC_COMPOSE_REMOTE_SSH_OPTS` exists for explicit ad-hoc SSH flags, but
stable host configuration belongs in SSH config.

Without `--detach`, remote output uses the line-oriented watch path. The exact
supported/forwarded flags and incompatible TUI combinations are specified in
the [CLI Reference](cli-reference.md#up-options).

### Reconnect from the laptop

After a prior remote stage exists, follow-up commands delegate without
re-staging the project:

```bash
hpc-compose status --remote=alice@login01 --format json -f compose.yaml
hpc-compose logs --remote=alice@login01 --lines 100 -f compose.yaml
hpc-compose stats --remote=alice@login01 --format json -f compose.yaml
hpc-compose score --remote=alice@login01 --format json -f compose.yaml
hpc-compose pull --remote=alice@login01 --format json -f compose.yaml
```

Use the same destination/settings context as the original remote launch. A
missing remote stage is not fixed by guessing its internal path; run a reviewed
remote dry-run/stage flow first.

## Monitor and Reconnect

Prefer bounded, machine-readable snapshots before interactive views:

```bash
hpc-compose jobs list --format json
hpc-compose status --format json -f compose.yaml
hpc-compose ps -f compose.yaml
hpc-compose logs --lines 100 -f compose.yaml
hpc-compose stats --format json -f compose.yaml
```

Use `watch --watch-mode line` for an ongoing text stream or ordinary `watch` for
the TUI. A pending reason is scheduler evidence; it is not proof that the spec
is invalid. `weather` describes observed cluster conditions, and `when` can
gate one deliberate submission, but neither reserves capacity. See [Right-Size
With Canary Runs](canary-runs.md) for the full advisory loop.

Telemetry warnings are part of the result. A batch-node-only or unknown
multi-node collector may display partial measurements, but it cannot support an
allocation-wide idle conclusion or GPU-reduction recommendation.

## Isolate Preparation or Rendering

Break the normal `up` spine apart only when the boundary itself needs evidence:

```bash
hpc-compose prepare -f compose.yaml
hpc-compose prepare --force-rebuild -f compose.yaml
hpc-compose render --annotate --output job.preview.sbatch -f compose.yaml
hpc-compose explain --field x-slurm.time -f compose.yaml
```

`prepare` can access registries and writes cache artifacts but does not call
`sbatch`. `render` writes a potentially sensitive script. An annotated preview
is never the submitted script; annotations exist to explain provenance.

## Diagnose a Failed Tracked Run

Work from evidence already associated with the job:

```bash
hpc-compose status --verify --format json -f compose.yaml
hpc-compose debug --preflight -f compose.yaml
hpc-compose logs --lines 200 -f compose.yaml
hpc-compose checkpoints --format json -f compose.yaml
```

Interpret scheduler state, service state, readiness, logs, checkpoints,
artifacts, and telemetry as separate evidence sources. The worked sequence in
[Failure Recovery](failure-recovery.md) demonstrates the handoff between them;
[Troubleshooting](troubleshooting.md) remains the symptom index.

## Resume and Recover Artifacts

Keep the canonical resume directory in `x-slurm.resume.path`. Artifact bundles
are exported copies and must not become the implicit source of the next attempt.

```bash
hpc-compose up --resume-diff-only -f compose.yaml
hpc-compose checkpoints --format json -f compose.yaml
hpc-compose artifacts --tarball -f compose.yaml
```

Review resume-sensitive changes before using `--allow-resume-changes`. For
preemptible jobs, a separately authorized `test --preemption` drill is the
evidence that signal, checkpoint, requeue, and attempt-two assertions work
together.

## Clean Up Deliberately

Preview before deleting:

```bash
hpc-compose cache prune --age 14 --cache-dir <shared-cache-dir>
hpc-compose clean --age 14 --deep --dry-run --disk-usage -f compose.yaml
```

Actual cache pruning, `clean`, `down`, rendezvous pruning, cancellation, and
`workspace release` are destructive or externally mutating operations. Confirm
the exact scope separately, preserve canonical checkpoints and evidence, and do
not infer cleanup authorization from authorization to run a job.

## Related Docs

- [Quickstart](quickstart.md)
- [Command Families](command-families.md)
- [Production Readiness](production-readiness.md)
- [Runtime Observability](runtime-observability.md)
- [Troubleshooting](troubleshooting.md)
- [Artifacts and Resume](artifacts-and-resume.md)
