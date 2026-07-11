# Set Up With an AI Agent

An AI agent can inspect a repository, author an hpc-compose spec, run static checks, and prepare a cluster handoff. It must use guidance that matches the installed binary and stop at the authorization tier for the next action.

The root [`llms.txt`](https://nicolasschuler.github.io/hpc-compose/llms.txt) is the concise discovery map. It follows the [llms.txt proposal](https://github.com/AnswerDotAI/llms-txt): one H1, a summary blockquote, brief invariant notes, H2 link lists, and a skippable Optional section. It is not an authorization mechanism. The generated [command-safety page](agent-command-safety.md) and [JSON policy](agent-command-policy.json) are the command contract.

The bundled skill follows the [Agent Skills specification](https://agentskills.io/specification): discovery metadata in `SKILL.md`, a short activated router, and focused references or scripts loaded only when needed.

## Match the binary and guidance

Start by identifying the binary that will actually run:

```bash
command -v hpc-compose
hpc-compose --version
uname -sm
```

The installed binary carries version-matched documentation and schemas:

```bash
hpc-compose --offline docs "first cluster run" --format json
hpc-compose --offline schema
hpc-compose --offline schema --kind settings
```

Published Pages and `/raw/*.md` describe a CI-verified snapshot of the `main`
branch. They can be newer than the latest GitHub release and may mention commands
or fields absent from an installed binary. When they disagree, use the installed
binary's help, embedded docs, and schemas.

## Install a matching release skill when available

The release workflow publishes `hpc-compose-skill-vX.Y.Z.tar.gz` and its checksum
alongside binary archives for releases that contain the agent bundle. On the
[release matching the installed CLI](https://github.com/NicolasSchuler/hpc-compose/releases),
confirm that both skill assets exist before downloading them. Older releases may
predate the bundle; do not substitute the current `main` skill for an older
binary and call it version-matched.

```bash
VERSION="$(hpc-compose --version | awk '{print $2}')"
curl -fLO "https://github.com/NicolasSchuler/hpc-compose/releases/download/v${VERSION}/hpc-compose-skill-v${VERSION}.tar.gz"
curl -fLO "https://github.com/NicolasSchuler/hpc-compose/releases/download/v${VERSION}/hpc-compose-skill-v${VERSION}.tar.gz.sha256"
shasum -a 256 -c "hpc-compose-skill-v${VERSION}.tar.gz.sha256"
tar -xzf "hpc-compose-skill-v${VERSION}.tar.gz"
```

Install the extracted directory in one of these locations, then start a fresh agent session:

- Codex: `$CODEX_HOME/skills/hpc-compose` or `~/.codex/skills/hpc-compose`;
- Claude Code: `~/.claude/skills/hpc-compose` or `.claude/skills/hpc-compose`;
- another skill-aware client: its documented Agent Skills directory.

If the matching release does not contain the archive, keep using the binary's
embedded docs and schemas. Upgrade the CLI only when requested, or use
`skills/hpc-compose/` from an exact matching source checkout. That directory's
`VERSION` file is an explicit mismatch guard.

## Copy-paste prompt

```text
Use $hpc-compose to adapt this repository for my Slurm cluster.

First establish the installed hpc-compose version and whether this machine is an
authoring workstation, login/submission host, active allocation, local Linux
runtime host, or dev cluster. Use the installed binary's `--offline docs`, help,
and schemas as the version-matched source of truth.

Inspect the repository with the skill's evidence-only JSON probe. Pass its
derived workload phrases to `examples recommend --format json`; do not select an
example from keywords alone. Iterate through redacted `validate`, `lint`, `plan`,
`inspect`, and `explain` JSON, inspecting each output schema before depending on
fields.

Apply the generated command policy before every action. Do not submit, allocate,
execute workload code, use SSH, provision external storage, cancel, requeue,
delete, or expose sensitive output without the required explicit authorization.
Stop with the exact next command, its execution location, effects, and tier.
```

## Authorization model

The policy orders actions from automatic read-only through scoped local mutation, explicit runtime or external mutation, explicit quota, and explicit destructive action. Flag overrides matter: for example, `preflight --fs-probes`, smoke-probe `--submit`, preemption drills, sweep cancellation paths, local-runtime modes, and workspace lifecycle commands do not share the base command's effects.

Sensitive output is independent of authorization tier. An agent must not echo or ingest unredacted `plan --show-script`, `render`, `plan --verbose`, `--show-values`, logs, or debug output. Prefer:

```bash
hpc-compose --offline plan -f compose.hpc.yaml --format json
hpc-compose --offline explain -f compose.hpc.yaml --format json
```

If you explicitly request a generated script file, authorize the named local destination. The agent should create it owner-only and should not read it back into the conversation.

See [Command Safety for Agents](agent-command-safety.md) for the complete generated classification. Approval applies to the named invocation; `--yes`, a prior run, or available credentials do not broaden it.

## Expected workflow

A good agent run should leave you with:

- the installed binary version and skill version comparison;
- the execution context and repository evidence paths;
- a spec chosen through the shipped example recommender;
- redacted machine-readable validation, lint, plan, inspect, and provenance checks;
- confirmed cluster facts separated from assumptions;
- the exact next command, where it runs, whether it contacts Slurm or consumes quota, and its authorization tier;
- an explicit statement that no runtime, SSH, scheduler mutation, deletion, or sensitive-output ingestion occurred when the work stopped at authoring.

For a larger preloaded context, use [`llms-ctx.txt`](https://nicolasschuler.github.io/hpc-compose/llms-ctx.txt). Use [`llms-ctx-full.txt`](https://nicolasschuler.github.io/hpc-compose/llms-ctx-full.txt) only when the Optional pages are relevant.

## Related Docs

- [Command Safety for Agents](agent-command-safety.md)
- [Command Families](command-families.md)
- [CLI Reference](cli-reference.md)
- [JSON Output Stability](json-output-stability.md)
