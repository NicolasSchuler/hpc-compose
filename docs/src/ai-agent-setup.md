# Set Up With an AI Agent

You can hand hpc-compose setup to an AI agent — Claude, Codex, Copilot, Cursor, or any LLM that can read a repository and run shell commands. This page is the agent-agnostic entry point: a copy-paste prompt, the safety boundary every agent must respect, and how to install the bundled skill for agents that support skills.

The machine-readable entry point is the published map [`llms.txt`](llms.txt), served at `https://nicolasschuler.github.io/hpc-compose/llms.txt`. Point an agent at that URL first; it carries the curated doc map, the safety contract, and the canonical spec conventions in a token-lean form.

## Copy-paste prompt for any agent

```text
Help me set up hpc-compose for my Slurm cluster.

First read https://nicolasschuler.github.io/hpc-compose/llms.txt and honor its
safety contract: never submit, allocate, or cancel a Slurm job without my explicit
approval. Author the spec and verify it with the safe static checks
(validate, plan --show-script, inspect) before proposing any real run.

Then: inspect this repository, ask me what you need about my cluster (account,
partition, runtime backend, and a shared cache path visible from login and compute
nodes), and produce an hpc-compose spec plus the exact login-node commands. Stop and
ask before any command that submits or cancels a job.
```

For a one-line nudge once the agent has context: *"Set up hpc-compose for my cluster, read the published llms.txt first, and don't submit any Slurm job without my approval."*

## The safety boundary (what an agent may run unprompted)

| Safe to run unprompted (never submits, cancels, or allocates; no quota) | Requires your explicit approval (submits/cancels/allocates) |
| --- | --- |
| Static, no scheduler contact: `new`, `validate`, `plan`, `plan --show-script`, `inspect`, `render`, `explain`, `config` | `up`, `run`, `test --submit`, `notebook`, `alloc`, `shell`, `sweep submit`, `down`, `cancel` |
| Read-only scheduler queries (`squeue`/`sacct`, no changes): `status`, `ps`, `stats`, `diff`, `logs` — avoid tight polling on rate-limited login nodes. `artifacts` also writes exported files to the local `export_dir` | — |

A well-behaved agent authors and statically verifies a spec first, and only runs a submitting command after you approve it on a supported Linux Slurm submission host. On a login node it should prefer `hpc-compose debug -f <file> --preflight` and `hpc-compose doctor cluster-report` before a first `up`.

## Install the bundled skill (Claude, Codex, and other skill-aware agents)

This repository ships a drop-in skill bundle at `skills/hpc-compose/` — the source of truth for the setup recipe. Copy it into your agent's skills directory and start a fresh session so skill discovery reloads:

- Claude Code: `~/.claude/skills/hpc-compose` (user scope) or `.claude/skills/hpc-compose` (project scope)
- Codex: `$CODEX_HOME/skills/hpc-compose` or `~/.codex/skills/hpc-compose`
- Other runtimes: the skills location your agent documents

The bundle progressively loads detail as needed:

| Path | Purpose |
| --- | --- |
| `skills/hpc-compose/SKILL.md` | Trigger description, the safe-first core workflow, adaptation rules, and output expectations. |
| `skills/hpc-compose/references/environment-setup.md` | Onboarding: installation, cluster-requirement discovery, shared-cache setup, profile/context checks, and the first safe cluster handoff. |
| `skills/hpc-compose/references/hpc-compose-workflow.md` | Command path, Docker Compose migration, backend selection, verification, and troubleshooting. |
| `skills/hpc-compose/references/haicore-kit.md` | HAICORE / NHR@KIT Slurm, GPU, filesystem, cache, and Pyxis/Enroot guidance. |
| `skills/hpc-compose/references/cluster-adaptation.md` | General Slurm cluster reconnaissance and portable adaptation. |
| `skills/hpc-compose/scripts/hpc_compose_repo_probe.py` | Heuristic repository probe for migration clues. |

For local reconnaissance you (or the agent) can run the probe directly:

```bash
python3 skills/hpc-compose/scripts/hpc_compose_repo_probe.py .
```

The probe is intentionally heuristic — treat its output as an inventory and a set of hypotheses, then confirm against repository files, current cluster documentation, and hpc-compose static checks.

## What to expect from a good agent run

An agent helping with hpc-compose should:

- inspect the target repository before proposing a spec;
- discover your environment (cluster, access method, workload, backend, shared filesystem, account/partition/QOS) before writing cluster-specific files;
- prefer `x-runtime.prepare.commands` and a shared cache path (never `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm`);
- verify with `validate`, `plan --show-script`, and `inspect` before any real submission;
- ask before any command that submits or cancels jobs or consumes allocation quota;
- leave you with the created files, the static checks it ran, the cluster assumptions still unverified, and the next safest command.

## Related Docs

- [Quickstart](quickstart.md)
- [Task Guide](task-guide.md)
- [Installation](installation.md)
- [CLI Reference](cli-reference.md)
