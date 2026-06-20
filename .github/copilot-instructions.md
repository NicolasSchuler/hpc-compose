# Copilot instructions

The canonical guidance for working inside this repository lives in [`AGENTS.md`](../AGENTS.md) — read it for project structure, build/test commands, coding conventions, and scope rules. It is kept current; this file is a thin pointer so it does not drift.

Quick reminders:

- Build with `cargo build --release`; run tests with `cargo test`.
- The CLI entrypoint is `src/main.rs`; core behavior lives in the library modules (`src/spec/`, `src/planner.rs`, `src/prepare.rs`, `src/preflight.rs`, `src/render.rs`, `src/cache.rs`, `src/job/`), with command families under `src/commands/`. See [`docs/src/architecture.md`](../docs/src/architecture.md) for the module map.
- In specs, prefer the backend-neutral `x-runtime.prepare.commands`; `x-enroot.prepare` is a valid Pyxis/Enroot compatibility alias, only accepted when `runtime.backend: pyxis`.
- `x-slurm.cache_dir` must be storage shared between login and compute nodes; `/tmp`, `/var/tmp`, `/private/tmp`, and `/dev/shm` are rejected.

To help a user set up hpc-compose on their own cluster (rather than change this codebase), use the skill bundle at `skills/hpc-compose/` and [`docs/src/ai-agent-setup.md`](../docs/src/ai-agent-setup.md).
