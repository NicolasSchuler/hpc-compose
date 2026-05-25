---
name: hpc-compose
description: Help adapt repositories to hpc-compose, a Compose-like launcher for one Slurm allocation with services run through Pyxis/Enroot, Apptainer, Singularity, or host runtimes. Use when Codex is asked to make a repository work with hpc-compose, migrate Docker Compose or Slurm scripts to hpc-compose, author or validate hpc-compose specs, choose HAICORE/NHR@KIT or other cluster settings, configure shared cache/storage, debug hpc-compose preflight/run failures, or prepare a user for a real HPC submission.
---

# hpc-compose

Use this skill to turn a repository into an inspectable hpc-compose workflow for Slurm without overclaiming cluster readiness. Favor a safe authoring path first, then cluster-specific checks, then explicit user approval before real submissions.

## Core Workflow

1. Inspect the target repository before proposing a spec.
   - Run `scripts/hpc_compose_repo_probe.py <repo>` when useful.
   - Look for `docker-compose*.yml`, `compose*.yaml`, `Dockerfile*`, `*.sbatch`, `requirements.txt`, `pyproject.toml`, `environment.yml`, `package.json`, workflow-engine files, training scripts, model-serving code, and existing README run commands.
2. Read the relevant references only as needed.
   - `references/hpc-compose-workflow.md`: command path, migration rules, templates, verification gates, and troubleshooting.
   - `references/haicore-kit.md`: HAICORE-specific Slurm, GPU, filesystem, Pyxis/Enroot, and cache guidance.
   - `references/cluster-adaptation.md`: general cluster-doc reconnaissance and non-HAICORE adaptation.
3. State findings as observations, hypotheses, recommendations, and open questions when cluster facts or workload intent are uncertain.
4. Create or adapt an hpc-compose spec conservatively.
   - Prefer `compose.hpc.yaml` if the repo already has Docker Compose files.
   - Keep cluster-specific values in `.hpc-compose/settings.toml`, `.hpc-compose/cluster.toml`, `.env`, or documented profile choices when possible.
   - Preserve existing Docker Compose, Slurm, and CI files unless the user explicitly asks to replace them.
5. Verify with static checks before any real cluster action.
   - Use `hpc-compose validate -f <file>`.
   - Use `hpc-compose plan -f <file>` and often `hpc-compose plan --show-script -f <file>`.
   - Use `hpc-compose inspect --verbose -f <file>` when resource mapping or dependencies matter.
6. Treat real Slurm operations as user-approved actions.
   - Ask before `hpc-compose up`, `run`, `test --submit`, `down`, `cancel`, or any command that submits/cancels jobs or consumes allocation quota.
   - On a login node, prefer `hpc-compose debug -f <file> --preflight` and `hpc-compose doctor cluster-report` before first `up`.

## Adaptation Rules

- Use `runtime.backend: pyxis` by default for HAICORE-like Slurm + Enroot/Pyxis sites. Switch to `apptainer`, `singularity`, or `host` only when site docs or observed tools justify it.
- Configure `x-slurm.cache_dir` or `hpc-compose setup --cache-dir` to a shared path visible from both login and compute nodes. Do not use `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm` for the cache.
- For Docker Compose migration:
  - Keep `image`, `command`, `entrypoint`, `environment`, `volumes`, `depends_on`, and `working_dir` when compatible.
  - Replace `build:` with a base `image:` plus `x-runtime.prepare.commands`.
  - Remove `ports`, `networks`, `network_mode`, `restart`, and `deploy`.
  - Replace service-name DNS with `127.0.0.1` only for same-node helper services; use hpc-compose allocation metadata for distributed jobs.
  - Convert simple healthchecks to `readiness` with `tcp`, `http`, `log`, or `sleep`.
- For Slurm resource mapping, use first-class `x-slurm` fields before raw pass-through: `partition`, `account`, `time`, `nodes`, `ntasks`, `cpus_per_task`, `mem`, `gres`, `gpus`, `gpus_per_node`, `mem_per_gpu`, and binding fields.
- Use raw `x-slurm.submit_args` and service `extra_srun_args` only for site-specific flags that hpc-compose does not model.
- Keep source code mounted through `volumes` during iteration; put slower dependency installation in `x-runtime.prepare.commands`.
- Add readiness gates only where startup order matters. Plain `depends_on` means started, not healthy.
- For finite smoke checks, create a short-running companion spec or command instead of testing a long service by hoping it exits.

## HAICORE Default Posture

For NHR@KIT HAICORE, start by reading `references/haicore-kit.md` and verify current live docs when network access is available. Default assumptions to check:

- Slurm is the batch system.
- `normal` is the broadly available queue; `advanced` requires extra privilege.
- Full A100 GPU requests use Slurm GRES such as `gpu:full:<count>`; MIG profiles use profile-specific GRES.
- A workspace path is usually the right starting point for shared hpc-compose cache and prepared images.
- `$TMPDIR` is node-local and job-lifetime only; it is good for runtime scratch, not for hpc-compose cache reuse across prepare and compute nodes.
- Pyxis/Enroot support should be confirmed with `srun --help | grep container-image` or `hpc-compose doctor cluster-report`.

## Output Expectations

When implementing for a repository, leave the user with:

- The created or changed hpc-compose files.
- The exact static checks run and their outcome.
- The cluster assumptions that remain unverified.
- The next safest command for the target environment.
- A short note if a real submission was intentionally not run.
