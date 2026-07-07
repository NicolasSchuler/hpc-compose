# Contributor and In-Repo Agent Guide

This guide is for working **inside this repository** — contributors and coding agents changing hpc-compose's code, tests, and docs. To instead help a user **set up hpc-compose on their own cluster**, use the agent skill at `skills/hpc-compose/` and the [Set Up With an AI Agent](docs/src/ai-agent-setup.md) page.

## Project Structure & Module Organization
`hpc-compose` is a Rust CLI for turning a Compose-like spec into a single Slurm job. [`src/main.rs`](/Users/nicolas/hpc-compose/src/main.rs) is the CLI entrypoint; core behavior lives in [`src/spec/`](/Users/nicolas/hpc-compose/src/spec), [`src/planner.rs`](/Users/nicolas/hpc-compose/src/planner.rs), [`src/prepare.rs`](/Users/nicolas/hpc-compose/src/prepare.rs), [`src/preflight.rs`](/Users/nicolas/hpc-compose/src/preflight.rs), [`src/render.rs`](/Users/nicolas/hpc-compose/src/render.rs), [`src/cache/`](/Users/nicolas/hpc-compose/src/cache) (`mod.rs`, `dataset.rs`, `source.rs`), and [`src/job/mod.rs`](/Users/nicolas/hpc-compose/src/job/mod.rs). For the fuller contributor module map and execution flow, see [Architecture for Contributors](/Users/nicolas/hpc-compose/docs/src/architecture.md). Integration coverage is split across `tests/cli_*.rs` plus focused suites such as `tests/docs_examples.rs`, `tests/install_script.rs`, `tests/public_api.rs`, and `tests/release_metadata.rs`. User-facing docs belong in [`docs/`](/Users/nicolas/hpc-compose/docs), and runnable sample specs live in [`examples/`](/Users/nicolas/hpc-compose/examples).

## Build, Test, and Development Commands
- `cargo build --release`: build the production binary.
- `cargo test`: run the full test suite.
- `cargo test --test cli_spec --test cli_runtime --test cli_cache --test cli_context --test cli_init --test cli_jobs --test cli_canary_rendezvous --test cli_dev_workflow --test cli_doctor_readiness --test cli_sweep --test docs_examples --test install_script --test public_api --test release_metadata`: run the split integration and release/doc guard tests.
- `cargo test --test cli_runtime up_command_runs_end_to_end_with_fake_tools -- --exact`: run one integration test while debugging.
- `cargo run -- inspect --verbose -f examples/dev-python-app.yaml`: inspect a sample spec without producing a release build.

## Coding Style & Naming Conventions
Use standard Rust style with `cargo fmt`; keep default 4-space indentation and avoid custom formatting. Follow Rust naming conventions: snake_case for modules, files, and functions; PascalCase for structs and enums; SHOUTY_SNAKE_CASE for constants. Keep `src/main.rs` thin and move reusable behavior into library modules. Prefer explicit validation in `src/spec/validation.rs` over silently accepting unsupported Compose keys.

## Testing Guidelines
Add tests whenever behavior changes in parsing, planning, preparation, rendering, or job tracking. Integration tests in `tests/cli_*.rs` use fake `enroot`, `srun`, `sbatch`, `squeue`, and `sacct` binaries; extend that pattern for end-to-end scenarios. Name tests by observed behavior, for example `up_command_runs_end_to_end_with_fake_tools`.

## Commit & Pull Request Guidelines
Match the existing history: short, imperative subjects such as `Fix SBATCH directive ordering in rendered scripts` or `Cut v0.1.46 release`. Pull requests should describe the user-visible effect, list verification commands run, and note any coordinated updates to docs or examples. Link the relevant issue when one exists.

## Scope & Configuration Notes
Preserve the project’s intended scope: one Slurm allocation per application, with single-node jobs and constrained multi-node runs where one distributed service spans the allocation. Keep README, docs, examples, and parser behavior aligned when spec semantics change. Treat `x-slurm.cache_dir` as shared storage visible to login and compute nodes; do not assume `/tmp`-style local paths are valid. In examples and docs, prefer the backend-neutral `x-runtime.prepare.commands`; `x-enroot.prepare` is a valid Pyxis/Enroot compatibility alias, not the default spelling.
