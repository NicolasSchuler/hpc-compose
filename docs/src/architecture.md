# Architecture for Contributors

The CLI is intentionally thin. Most behavior lives in the library crate so the binary, integration tests, and generated rustdoc all describe the same pipeline.

## Module map

- `spec`: parse, interpolate, and validate the supported Compose subset
- `planner`: normalize the parsed spec into a deterministic plan
- `preflight`: check login-node prerequisites and cluster policy issues
- `prepare`: import base images and rebuild prepared runtime artifacts
- `render`: generate the final `sbatch` script and service launch commands
- `job`: track submissions, logs, metrics, status, and artifact export
- `cache`: persist cache manifests for imported and prepared images
- `init`: expose the shipped example templates for `hpc-compose init`

## Execution flow

1. `ComposeSpec::load` parses YAML, validates supported keys, interpolates variables, and applies semantic validation.
2. `planner::build_plan` resolves paths, command shapes, dependencies, and prepare blocks into a normalized plan.
3. `prepare::build_runtime_plan` computes concrete cache artifact locations.
4. `preflight::run` checks cluster prerequisites before submission.
5. `prepare::prepare_runtime_plan` imports or rebuilds artifacts when needed.
6. `render::render_script` emits the batch script consumed by `sbatch`.
7. `job` persists tracked metadata under `.hpc-compose/` and powers `status`, `stats`, `logs`, `cancel`, and artifact export.

## Contributor commands

```bash
cargo test
cargo test --test cli
cargo doc --no-deps
mdbook build docs
```

## Documentation split

- Use this mdBook for user-facing workflows, examples, and reference material.
- Use rustdoc for contributor-facing internals and the library module map.
- Keep README short and point readers into the book instead of duplicating long-form guidance.
