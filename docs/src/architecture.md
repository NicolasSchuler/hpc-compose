# Architecture for Contributors

The library crate owns the core staged pipeline. The binary entrypoint delegates to command-family modules under `src/commands/`, while presentation lives under `src/output/`. Reusable planning, prepare, render, tracking, cache, context, and template logic stay in the library modules.

## Module map

- `spec`: parse, interpolate, and validate the supported Compose subset
- `planner`: normalize the parsed spec into a deterministic plan
- `context`: resolve `.hpc-compose/settings.toml`, profiles, env files, interpolation variables, and binary overrides
- `cluster`: generate and apply best-effort cluster capability profiles from `doctor --cluster-report`
- `preflight`: check login-node prerequisites and cluster policy issues
- `prepare`: import base images and rebuild prepared runtime artifacts
- `render`: generate the final `sbatch` script and service launch commands
- `job`: track submissions, logs, metrics, status, and artifact export
- `tracked_paths`: centralize the `.hpc-compose/` layout used by render and job tracking
- `cache`: persist cache manifests for imported and prepared images
- `init`: expose the shipped example templates for `hpc-compose new` plus the legacy `init` alias
- `schema` and `manpages`: expose the checked-in JSON Schema and generated section-1 manpage flow
- `commands/spec`: binary-only handlers for `validate`, `render`, `prepare`, `preflight`, `config`, and `inspect`
- `commands/runtime`: binary-only handlers for `up`, `submit`, `run`, `status`, `ps`, `watch`, `stats`, `artifacts`, `logs`, `down`, `cancel`, and `clean`
- `commands/cache`: binary-only handlers for cache inspection and pruning
- `commands/init`: binary-only handlers for `new` / `init`, `setup`, `context`, and `completions`
- `watch_ui`: terminal UI controller and renderer for `up`, `submit --watch`, and `watch`
- `output`: binary-only text, JSON, CSV, and JSONL formatting helpers

## Execution flow

1. `ComposeSpec::load` parses YAML, validates supported keys, interpolates variables, and applies semantic validation.
2. `planner::build_plan` resolves paths, command shapes, dependencies, and prepare blocks into a normalized plan.
3. `prepare::build_runtime_plan` computes concrete cache artifact locations.
4. `context` and optional cluster profiles provide resolved paths, binaries, env, and compatibility warnings.
5. `preflight::run` checks cluster prerequisites before submission.
6. `prepare::prepare_runtime_plan` imports or rebuilds artifacts when needed.
7. `render::render_script` emits the batch script consumed by `sbatch`.
8. `job` persists tracked metadata under `.hpc-compose/` and powers `status`, `ps`, `watch`, `stats`, `logs`, `cancel`, and artifact export.
9. `commands/*` turns CLI variants into library calls, and `output` formats the final presentation.

## Tracked Runtime Layout

`tracked_paths` is the single source of truth for the tracked-job layout shared by `render` and `job`.

- Compose-level metadata lives under `.hpc-compose/` next to the compose file.
- Per-job runtime state lives under `${SLURM_SUBMIT_DIR}/.hpc-compose/<job-id>/`.
- Root-level `logs/`, `metrics/`, `artifacts/`, and `state.json` are the latest-view paths used by status and export commands.
- Resume-aware runs still write attempt-specific state under `attempts/<attempt>/...`.
- The batch script updates root-level latest symlinks so contributor-facing tooling can read the most recent attempt without reconstructing shell logic independently.

## Contributor commands

```bash
cargo test
cargo test --test cli_runtime
cargo test --test release_metadata
cargo doc --no-deps
mdbook build docs
cargo run --features manpage-bin --bin gen-manpages -- --check
```

## Documentation split

- Use this mdBook for user-facing workflows, examples, and reference material.
- Use rustdoc for contributor-facing internals and the library module map.
- Keep README short and point readers into the book instead of duplicating long-form guidance.
