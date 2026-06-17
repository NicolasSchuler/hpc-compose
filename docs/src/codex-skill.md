# Codex Skill

This repository ships a Codex skill at `skills/hpc-compose/` for agents that need to help users set up, adapt, validate, and troubleshoot hpc-compose workflows.

Use it when a user asks for tasks such as:

- make my repository work with hpc-compose
- set up hpc-compose for my HPC cluster
- install or configure hpc-compose on a login node
- choose a shared cache, runtime backend, profile, or first smoke run
- migrate this Docker Compose or Slurm workflow to hpc-compose
- prepare this project for HAICORE or another Slurm cluster
- debug hpc-compose validation, preflight, or run failures

## What It Contains

The skill keeps the main trigger and workflow in `SKILL.md`, then uses progressively loaded references for details:

| Path | Purpose |
| --- | --- |
| `skills/hpc-compose/SKILL.md` | Trigger description, core workflow, adaptation rules, and output expectations. |
| `skills/hpc-compose/references/environment-setup.md` | User onboarding flow for installation, cluster requirement discovery, shared cache setup, profile/context checks, and first safe cluster handoff. |
| `skills/hpc-compose/references/hpc-compose-workflow.md` | hpc-compose command path, Docker Compose migration, backend selection, verification, and troubleshooting. |
| `skills/hpc-compose/references/haicore-kit.md` | HAICORE/NHR@KIT Slurm, GPU, filesystem, cache, Pyxis/Enroot, and verification guidance. |
| `skills/hpc-compose/references/cluster-adaptation.md` | General Slurm cluster reconnaissance and portable adaptation guidance. |
| `skills/hpc-compose/scripts/hpc_compose_repo_probe.py` | Heuristic repository probe for migration clues. |

## Using The Skill

Install or copy `skills/hpc-compose/` into the Codex skills directory, typically `$CODEX_HOME/skills/hpc-compose` or `~/.codex/skills/hpc-compose`, then start a fresh Codex session so skill discovery can reload.

Example prompt:

```text
Use $hpc-compose to make this repository run with hpc-compose on HAICORE.
```

```text
Use $hpc-compose to set up hpc-compose for my Slurm cluster and guide me through the first safe smoke run.
```

For local reconnaissance, run:

```bash
python3 skills/hpc-compose/scripts/hpc_compose_repo_probe.py .
```

The probe is intentionally heuristic. Treat its output as an inventory and hypothesis generator, then verify with repository files, cluster documentation, and hpc-compose static checks.

## Agent Expectations

Agents using this skill should:

- inspect the target repository before proposing a spec
- check current cluster documentation for site-specific details
- discover the user's environment before writing cluster-specific setup
- prefer hpc-compose static checks before real Slurm submissions
- ask before commands that submit or cancel jobs or consume allocation quota
- report observations, hypotheses, recommendations, and open questions when cluster facts remain uncertain
