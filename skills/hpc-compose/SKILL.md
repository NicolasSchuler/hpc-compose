---
name: hpc-compose
description: Set up, author, migrate, validate, operate, and recover hpc-compose workflows for Slurm with Pyxis/Enroot, Apptainer, Singularity, or host runtimes. Use when an agent needs to inspect a repository, choose a shipped example, create or repair an hpc-compose spec, configure a cluster or shared cache, migrate Docker Compose or sbatch workflows, diagnose a tracked run, or prepare a safe first cluster submission without crossing authorization boundaries.
---

# hpc-compose

Build an evidence-backed, version-matched workflow and stop at the applicable authorization boundary.

## 1. Establish version and execution context

Before relying on command names or fields:

```bash
command -v hpc-compose
hpc-compose --version
uname -sm
pwd
```

Read `VERSION` and compare it with the installed CLI version. If they differ, treat the installed binary's help, embedded docs, and schemas as authoritative; do not apply a command or field only because this skill mentions it. Offer the matching release skill archive or a CLI upgrade, but do not download or install without authorization.

Classify the current machine as one of: authoring workstation, Slurm login/submission host, active allocation, local Linux runtime host, or checked-in dev cluster. Then inspect the redacted context:

```bash
hpc-compose --offline context --format json
```

## 2. Apply the safety gate

Read [command-safety.md](references/command-safety.md) before invoking hpc-compose commands. It is generated from the versioned command policy.

- Tier 1 may run automatically only when the independent sensitive-output guard is satisfied.
- Tier 2 requires an explicitly scoped local write, unless the user's request already names that write.
- Tiers 3–5 always require explicit authorization for runtime/external mutation, quota, or destruction.
- Never treat `--yes`, a prior submission, or access to credentials as authorization.
- Never echo or ingest unredacted scripts, values, logs, or debug output.

## 3. Load only the applicable reference

- Read [authoring-migration.md](references/authoring-migration.md) for new specs, repository adaptation, Docker Compose/sbatch migration, topology, readiness, and the static repair loop.
- Read [cluster-setup.md](references/cluster-setup.md) for installation, execution context, site facts, profiles, shared storage, workspace provisioning, runtime backends, or remote submission.
- Read [operations-recovery.md](references/operations-recovery.md) for a tracked failure, observation, artifacts, resume, preemption, sweeps, right-sizing, or cleanup.

For site-specific facts, retrieve the installed version's embedded guide instead of relying on copied site prose:

```bash
hpc-compose --offline docs "<site and question>" --format json
```

Use published `/raw/*.md` only when no suitable installed binary is available, and label it as latest-release guidance rather than version-matched guidance.

## 4. Gather repository evidence

Run the bounded evidence-only probe bundled with this skill when adapting an existing repository. Resolve `<skill-dir>` to this installed skill directory; do not expect the target repository to contain the probe:

```bash
python3 <skill-dir>/scripts/hpc_compose_repo_probe.py /path/to/repository
```

The JSON output contains confidence-labelled signals, evidence paths, scan limits, and derived `workload_phrases`; it contains no snippets or secret-file contents. Confirm material signals against the named files. Pass the derived phrases to the shipped recommender—do not select examples in the probe:

```bash
hpc-compose --offline examples recommend "<derived workload phrases>" --format json
```

Preserve existing Docker Compose, sbatch, CI, and user files unless replacement is explicitly requested. Prefer a separate `compose.hpc.yaml` for migration.

## 5. Use machine-readable, redacted interfaces

Inspect a command's output schema before depending on fields:

```bash
hpc-compose --offline schema --output validate
hpc-compose --offline schema --output lint
hpc-compose --offline schema --output plan
hpc-compose --offline schema --output spec-inspect
```

Iterate on the smallest coherent spec change:

```bash
hpc-compose --offline validate -f compose.hpc.yaml --format json
hpc-compose --offline lint -f compose.hpc.yaml --format json
hpc-compose --offline plan -f compose.hpc.yaml --format json
hpc-compose --offline inspect -f compose.hpc.yaml --format json
hpc-compose --offline explain -f compose.hpc.yaml --format json
```

Do not use `plan --show-script`, `render` to stdout, `--show-values`, verbose planning, logs, or debug output in an agent context. If the user explicitly requests a script file, authorize the scoped write, create it owner-only, and do not read it back.

## 6. Stop and hand off

Before any allocation-consuming preflight probe such as `preflight --fs-probes`, preparation, local runtime, SSH, workspace lifecycle, submission, allocation, cancellation, cleanup, or deletion, resolve the exact flags through the policy and stop if its tier lacks authorization. Ordinary non-submitting `preflight` is a scheduler-read action and follows its base policy classification.

Report:

- observations and evidence paths;
- files changed and static JSON checks run;
- cluster facts still unverified;
- the next command and its authorization tier;
- whether no runtime, SSH, scheduler mutation, or destructive action was performed.
