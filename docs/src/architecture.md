# Architecture for Contributors

The library crate owns the core staged pipeline. The binary entrypoint delegates to command-family modules under `src/commands/`, while presentation lives under `src/output/`. Reusable planning, runtime-plan derivation, prepare, render, tracking, cache, context, and template logic stay in the library modules.

## Module map

- `spec`: parse, interpolate, and validate the supported Compose subset
- `planner`: normalize the parsed spec into a deterministic plan
- `lint`: run opinionated static checks over validated plans
- `authoring_diagnostics`: diagnose one in-memory YAML document for editor and agent authoring loops
- `lsp`: diagnostics-only stdio Language Server adapter over `authoring_diagnostics`
- `context`: resolve `.hpc-compose/settings.toml`, profiles, env files, interpolation variables, and binary overrides
- `cluster`: generate and apply best-effort cluster capability profiles from `doctor cluster-report`
- `preflight`: check login-node prerequisites and cluster policy issues
- `runtime_plan`: derive the runtime-ready service model and deterministic cache artifact paths without performing I/O
- `prepare`: import base images and rebuild prepared runtime artifacts described by a runtime plan
- `render`: generate the final `sbatch` script and service launch commands
- `job`: track submissions, logs, metrics, replay, status, and artifact export
- `tracked_paths`: centralize the `.hpc-compose/` layout used by render and job tracking
- `cache`: persist cache manifests for imported and prepared images
- `init`: expose the shipped example templates for `hpc-compose new` plus the legacy `init` alias
- `schema` and `manpages`: expose the checked-in JSON Schema and generated section-1 manpage flow
- `commands/spec`: static authoring commands such as `plan`, `validate`, `lint`, `render`, `config`, `inspect`, `prepare`, and `preflight`
- `commands/runtime`: submission, tracked-run, and local-development commands such as `up`, `when`, `run`, `alloc`, `debug`, `status`, `ps`, `watch`, `replay`, `stats`, `logs`, `artifacts`, `down`, `cancel`, `clean`, `dev`, `tmux`, and `test`
- `commands/cache`: cache inspection and pruning
- `commands/doctor`, `commands/evolve`, `commands/examples`, `commands/weather`: the `doctor`, `evolve`, `examples`, and `weather` command families
- `commands/init`: `new` / `init`, `setup`, `context`, and `completions`
- `commands` (`mod.rs`): parses the CLI and routes every command to its handler module
- `watch_ui`: terminal UI controller and renderer for `up`, `watch`, and replay playback
- `output`: binary-only text, JSON, CSV, and JSONL formatting helpers

## Execution flow

1. `ComposeSpec::load` parses YAML, resolves authoring `extends`, validates supported keys, interpolates variables, and applies semantic validation.
2. `planner::build_plan` resolves paths, command shapes, dependencies, and prepare blocks into a normalized plan.
3. `runtime_plan::build_runtime_plan` computes concrete cache artifact locations. The former `prepare::*` paths for this model remain compatibility re-exports.
4. `context` and optional cluster profiles provide resolved paths, binaries, env, and compatibility warnings.
5. `authoring_diagnostics` can stop here for static editor/LSP feedback: it overlays the open root YAML buffer, builds a plan/runtime plan, and reports blocking errors or lint/cluster-profile warnings without prepare, render, Slurm, SSH, or network access.
6. `preflight::run` checks cluster prerequisites before submission.
7. `prepare::prepare_runtime_plan` imports or rebuilds artifacts when needed.
8. `render::render_script` emits the batch script consumed by `sbatch`.
9. `job` persists tracked metadata under `.hpc-compose/` and powers `status`, `ps`, `watch`, `replay`, `stats`, `logs`, `cancel`, and artifact export. `job::replay` reconstructs a best-effort timeline from existing state, service-exit, metrics, and log artifacts while reusing the watch renderer for playback.
10. `commands/*` turns CLI variants into library calls, and `output` formats the final presentation.

## Tracked Runtime Layout

`tracked_paths` is the single source of truth for the tracked-job layout shared by `render` and `job`.

- Compose-level metadata lives under `.hpc-compose/` next to the compose file.
- Per-job runtime state lives under `<runtime-root>/<job-id>/`, where `<runtime-root>` defaults to `<submit-dir>/.hpc-compose` and can be overridden with `x-slurm.runtime_root`. The renderer resolves this to an absolute path at submit time and bakes it into `JOB_ROOT`, so a running job does not depend on `$SLURM_SUBMIT_DIR`. Records persist an explicit override so later lookups address the same directory.
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

## Coverage Notes

- Treat `src/spec/mod.rs` as high risk for broad refactors until parser and semantic-validation behavior has more focused coverage. Prefer adding behavior-first tests in `tests/cli_spec.rs` or spec unit tests before moving large validation blocks.
- Render changes should keep generated-script assertions close to `src/render.rs`. `just examples-check` shellchecks rendered batch scripts, while local launchers are produced through `up/run --local`, so local launcher syntax needs focused render or local dry-run coverage.
- Runtime command refactors should start with pure helpers that have deterministic unit tests and existing CLI integration filters. Submission, tracking, watching, and process orchestration should stay together until a narrower harness makes a larger move low risk.

## Documentation split

- Use this mdBook for user-facing workflows, examples, and reference material.
- Use rustdoc for contributor-facing internals and the library module map.
- Keep README short and point readers into the book instead of duplicating long-form guidance.

## Related Docs

- [Execution Model](execution-model.md)
- [Spec Reference](spec-reference.md)
- [Roadmap](roadmap.md)
