# Copilot instructions

## Build and test

- Build the release binary with `cargo build --release`.
- Run the full test suite with `cargo test`.
- Run the split CLI integration tests with `cargo test --test cli_spec --test cli_runtime --test cli_cache --test cli_context --test cli_init --test cli_jobs`.
- Run a single integration test with `cargo test --test cli_runtime submit_command_runs_end_to_end_with_fake_tools -- --exact`.
- Run a single unit test with `cargo test planner::tests::prepare_mounts_force_rebuild -- --exact`.

## High-level architecture

- `src/main.rs` is the only CLI entrypoint. Most commands follow the same pipeline: `ComposeSpec::load` in `src/spec.rs` -> `planner::build_plan` in `src/planner.rs` -> `prepare::build_runtime_plan` in `src/prepare.rs`.
- `validate` stops after plan construction, `inspect` prints the normalized runtime plan, `render` passes the runtime plan to `render::render_script`, `prepare` runs `prepare_runtime_plan`, and `submit` chains preflight, optional prepare, render, and `sbatch`.
- `src/spec.rs` is the strict schema layer for the supported Compose subset plus the `x-slurm` and `x-enroot` extensions. It rejects unsupported Compose keys up front with explicit messages instead of ignoring them.
- `src/planner.rs` is the normalization layer. It resolves env vars and relative paths against the compose file directory, normalizes bare image refs like `redis:7` to `docker://redis:7`, topologically orders services from `depends_on`, and collapses `entrypoint`/`command` into `ExecutionSpec`.
- `src/prepare.rs` and `src/cache.rs` own artifact creation and reuse. Remote images are imported into `cache_dir/base`, prepared images are exported into `cache_dir/prepared`, and adjacent JSON manifests drive reuse, `cache inspect`, and prune behavior.
- `src/preflight.rs` checks cluster assumptions before submit: required binaries, Pyxis `--container-image` support, shared-cache policy, local mount/image paths, registry credentials, and `--skip-prepare` reuse safety.
- `src/render.rs` emits one `sbatch` script that launches all services with `srun --container-image=...`, writes logs under `.hpc-compose/$SLURM_JOB_ID/logs`, and inserts readiness gates after each service launch.
- The examples in `examples/app-redis-worker.yaml`, `examples/dev-python-app.yaml`, and `examples/llama-app.yaml` are the clearest reference specs for the supported multi-service, dev-mount, and GPU-backed workflows.
- `src/lib.rs` re-exports all modules: `cache`, `planner`, `preflight`, `prepare`, `render`, `spec`.

## Key conventions

- Preserve the repo's intentional scope: one Slurm allocation per application, with single-node jobs and constrained multi-node runs where one distributed service spans the allocation.
- Keep the README and parser behavior aligned. The unsupported Compose features called out in `README.md` are also enforced in `src/spec.rs`; adding or changing spec fields usually requires updates in both places.
- Relative paths are anchored to the compose file's parent directory, not the shell's current working directory. That applies to local `.sqsh` images, `volumes`, and `x-slurm.cache_dir`.
- `depends_on` only controls launch order. Actual startup gating is handled separately by `readiness` and rendered into the batch script.
- `depends_on` map syntax supports `condition: service_started` and `condition: service_healthy`. Other Compose conditions are rejected.
- `working_dir` is only valid when the service also has an explicit `command` or `entrypoint`.
- Mixed string/array `entrypoint` and `command` combinations are rejected in v1. Keep both sides in the same form.
- `x-enroot.prepare.commands` is required when `x-enroot.prepare` is present. If `prepare.mounts` is non-empty, the service intentionally rebuilds on every prepare/submit instead of reusing a cached prepared image.
- Prefer the dev workflow from the README and `examples/dev-python-app.yaml`: use `volumes` for active source trees and reserve `x-enroot.prepare.commands` for slower-changing dependencies or tools.
- `x-slurm.cache_dir` is expected to be shared storage visible from login and compute nodes. Paths under `/tmp`, `/var/tmp`, `/private/tmp`, and `/dev/shm` are treated as invalid by planning/preflight.
- Cache manifests are part of the product behavior, not disposable metadata. If you change cache keys or artifact naming in `prepare.rs`, update `cache.rs`, `inspect` output in `main.rs`, and prune behavior together.
- If you change cache keys, artifact naming, or prepared-image semantics in code, the docs (`docs/src/spec-reference.md`, `docs/src/runbook.md`) and cache behavior should be updated together.
- `render.rs` encodes service names into bash-safe tokens via `service_token()`, replacing non-alphanumeric bytes with `_x{hex}_`. This encoding is used for both bash function names (`launch_<token>`) and log filenames. Tests in `render.rs` verify that names differing only in punctuation (e.g. `api.v1` vs `api_v1`) produce distinct tokens.
- New spec features usually need coordinated changes across `src/spec.rs` (schema and validation), `schema/hpc-compose.schema.json` (authoring schema), `src/planner.rs` (normalization), `src/prepare.rs` or `src/render.rs` (runtime behavior), and the relevant split `tests/cli_*.rs` integration tests with fake `enroot`/`srun`/`sbatch` binaries.
