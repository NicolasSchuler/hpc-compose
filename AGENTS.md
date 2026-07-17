# Contributor and In-Repo Agent Guide

This guide is for working **inside this repository** — contributors and coding agents changing hpc-compose's code, tests, and docs. To instead help a user **set up hpc-compose on their own cluster**, use the agent skill at `skills/hpc-compose/` and the [Set Up With an AI Agent](docs/src/ai-agent-setup.md) page.

## Project Structure & Module Organization

`hpc-compose` is a Rust CLI for turning a Compose-like spec into a single Slurm job. [`src/main.rs`](src/main.rs) is the CLI entrypoint; core behavior lives in [`src/spec/`](src/spec/), [`src/planner.rs`](src/planner.rs), [`src/prepare.rs`](src/prepare.rs), [`src/preflight.rs`](src/preflight.rs), [`src/render.rs`](src/render.rs), [`src/cache/`](src/cache/) (`mod.rs`, `dataset.rs`, `source.rs`), and [`src/job/mod.rs`](src/job/mod.rs). For the fuller contributor module map and execution flow, see [Architecture for Contributors](docs/src/architecture.md). Integration-test sources live in `tests/` and are registered through the shared harnesses in `tests/harnesses/`, with isolated targets retained where process boundaries matter. User-facing docs belong in [`docs/`](docs/), and runnable sample specs live in [`examples/`](examples/).

## Build, Test, and Development Commands

- `cargo build --release --locked`: build the production binary with the checked-in dependency graph.
- `cargo test --locked`: run the configured Rust test suite, including explicit integration targets.
- `cargo test --locked --test cli_runtime up_command_runs_end_to_end_with_fake_tools -- --exact`: run one integration test while debugging.
- `cargo run --locked -- inspect --verbose -f examples/dev-python-app.yaml`: inspect a sample spec without producing a release build.
- `just check`: run the fast Rust and workflow gate (workflow lint, formatting, Clippy, and Rust tests).
- `just docs-check`: verify documentation, generated agent assets, manpages, links, spelling, and accessibility.
- `just examples-check`: validate shipped examples and shellcheck rendered scripts.
- `just release-check`: verify release metadata, dependency policy, and coverage thresholds.
- `just ci`: run the full local CI mirror.

## Coding Style & Naming Conventions

Use standard Rust style with `cargo fmt`; keep default 4-space indentation and avoid custom formatting. Follow Rust naming conventions: snake_case for modules, files, and functions; PascalCase for structs and enums; SHOUTY_SNAKE_CASE for constants. Keep `src/main.rs` thin and move reusable behavior into library modules. Prefer explicit validation in `src/spec/validation.rs` over silently accepting unsupported Compose keys.

## Testing Guidelines

Add tests whenever behavior changes in parsing, planning, preparation, rendering, or job tracking. Integration tests in `tests/cli_*.rs` use fake `enroot`, `srun`, `sbatch`, `squeue`, and `sacct` binaries; extend that pattern for end-to-end scenarios. Name tests by observed behavior, for example `up_command_runs_end_to_end_with_fake_tools`.

## Commit & Pull Request Guidelines

Match the existing history: short, imperative subjects such as `Fix SBATCH directive ordering in rendered scripts` or `Cut v0.1.46 release`. Pull requests should describe the user-visible effect, list verification commands run, and note any coordinated updates to docs or examples. Link the relevant issue when one exists.

## Scope & Configuration Notes

Preserve the project’s intended scope: one Slurm allocation per application, supporting single-node jobs, full-allocation distributed steps, and explicit node-index partitioning within the allocation. Do not broaden it into dynamic scheduling or bin packing across nodes, heterogeneous jobs, or cluster administration. Keep README, docs, examples, and parser behavior aligned when spec semantics change. Treat `x-slurm.cache_dir` as shared storage visible to login and compute nodes; do not assume `/tmp`-style local paths are valid. In examples and docs, prefer the backend-neutral `x-runtime.prepare.commands`; `x-enroot.prepare` is a valid Pyxis/Enroot compatibility alias, not the default spelling.
