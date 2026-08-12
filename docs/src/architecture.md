# Architecture for Contributors

The library crate owns the core staged pipeline. The binary entrypoint only collects arguments, handles the hidden completion endpoint, invokes the library CLI parser and dispatcher, renders terminal errors, and selects the process exit code. `src/cli/` defines the Clap command tree, `src/commands/` owns dispatch and command orchestration, and crate-private presentation lives under `src/output/`. Reusable parsing, planning, runtime-plan derivation, prepare, render, tracking, cache, context, and template logic stay in library modules.

## Module map

- `cli`: define and parse the Clap command tree and its typed arguments
- `spec`: parse, interpolate, and validate the supported Compose subset
- `planner`: normalize the parsed spec into a deterministic plan
- `lint`: run opinionated static checks over validated plans
- `authoring_diagnostics`: diagnose one in-memory YAML document for editor and agent authoring loops
- `lsp`: diagnostics-only stdio Language Server adapter over `authoring_diagnostics`
- `context`: resolve `.hpc-compose/settings.toml`, profiles, env files, interpolation variables, and binary overrides
- `cluster`: strictly discover/load the nearest upward cluster profile for authoring and runtime callers (a malformed nearest profile blocks ancestor fallback), and generate/apply best-effort capability profiles from `doctor cluster-report`; weather deliberately warns and continues on an invalid discovered profile, while completion lookup fails quietly
- `domain`: privately own small pure shared policies for service identity, mounts, safe ASCII identifiers, artifact filename tokens, registries, rendezvous names, and human scheduler IDs
- `dotenv`: privately own the path-free dotenv grammar shared by context and spec interpolation
- `memory`: privately own shared memory parsing and binary-byte display policies
- `path_util`: privately own path defaults, lexical repository-root lookup, normalization helpers, and shared-storage diagnostics
- `shell_quote`: privately own byte-stable executable and display shell-quoting policies
- `diagnostics`: own flat and grouped diagnostic reports, contextual-warning classification, and compatibility-stable notice types
- `diagnostics::presentation`: privately own report text styling, generic CLI error adaptation, per-thread notice formatting and stderr emission, and tracing setup
- `preflight`: check login-node prerequisites and cluster policy issues; re-export the shared report model for compatibility
- `preflight::shared_fs_probe`: privately own active shared-filesystem target selection, Slurm submission/cancellation, probe scripts, deadlines, and result parsing
- `readiness_analysis`: privately own pure readiness locality classification, effective probe defaults, and static TCP/HTTP endpoint derivation
- `readiness_util`: own host-side readiness probe description and execution
- `runtime_plan`: resolve prepared-local content fingerprints through a narrow filesystem adapter, then derive the runtime-ready service model, per-service configured-scratch eligibility, and deterministic cache artifact paths with pure service logic
- `prepare`: import base images and rebuild prepared runtime artifacts described by a runtime plan
- `prepare::stream`: privately own prepare-specific subprocess draining, bounded lossy output decoding and stderr tails, and streamed failure classification
- `render`: generate the final `sbatch` script and service launch commands; `render::local` adapts the serialized batch output for local execution while preserving its distinct shebang/directive filtering contract
- `job`: track submissions, logs, metrics, replay, status, and artifact export
- `job::config_snapshot`: privately own redacted effective-config YAML serialization persisted for resume and diff comparisons
- `job::analytics`: privately own dependency-leaf TRES parsing and pure primitives shared by accounting, scoring, right-sizing, stats, and watchdog analysis
- `job::annotation_policy`: privately own tag/note limits and validation shared by record and evidence persistence
- `job::batch_log`: privately own submitted batch-log path expansion and discovery used by record persistence
- `job::file_digest`: privately own ordinary SHA-256 reader/file derivation shared by artifact export, experiment bundles, and sweep compose identity
- `job::sampler_protocol`: privately own persisted sampler JSON/JSONL row shapes shared by stats, scoring, right-sizing, and watchdog analysis; its metadata row deliberately embeds the public stats-owned collector status type
- `job::scheduler_command`: privately own bounded scheduler subprocess execution and unavailable/error classification shared by scheduler, accounting, and stats
- `job::logs`: own tracked-log selection, streaming, and the bounded lossy tail reader shared with `watch_ui`
- `job::metadata_io`: privately own ordinary tracked-metadata JSON persistence; run-evidence and experiment-bundle formats remain with their protocol modules
- `job::evidence`: persist the additive immutable manifest/input lock, typed
  append-only annotation events, and rebuildable materialized run view
- `tracked_paths`: centralize the `.hpc-compose/` layout used by render and job tracking
- `time_util`: centralize equivalent crate-internal current Unix seconds, milliseconds, and full-nanoseconds reads, system-time conversion, and shared duration constants; secure-I/O subsecond temp names and the notebook token's distinct error fallback remain protocol-local
- `cache`: persist cache manifests for imported/prepared images and expose the stable staged-input vocabulary, derivation, and rendering facade
- `cache::observation`: privately own read-only artifact-presence, reuse-expectation, and rebuild-reason vocabulary
- `cache::dataset::manifest`: privately own rebuildable staged-input tracking sidecars and their kind/suffix layout policy
- `cache::dataset::store`: privately own mutable login-node staged-input CAS publication, completion records, and legacy migration
- `init`: expose the shipped example templates for `hpc-compose new` plus the legacy `init` alias
- `schema`: own the checked-in Compose/settings JSON Schema flow
- `manpages`: own generated section-1 manpage rendering and checks
- `commands::load`: share compose-to-plan/runtime-plan/effective-config orchestration and compose-parent cluster-profile discovery between command families
- `commands/spec`: own spec-rooted schema, validation, planning, rendering, explanation, inspection, config/context, prepare, and preflight handlers
- `commands/runtime`: submission, tracked-run, and local-development commands such as `up`, `when`, `run`, `alloc`, `debug`, `status`, `ps`, `watch`, `replay`, `stats`, `logs`, `artifacts`, `down`, `cancel`, `clean`, `dev`, `tmux`, `test`, `sweep`, `experiment`, `notebook`, `pull`, `reach`, `germinate`, and rendezvous operations
- `commands/cache`: cache inspection and pruning; private `commands::cache::inspect_report` owns fallible cache-inspect filesystem queries and report assembly
- `commands/doctor`, `commands/evolve`, `commands/examples`, `commands/weather`: the `doctor`, `evolve`, `examples`, and `weather` command families
- `commands/docs`, `commands/feedback`, `commands/workspace`: documentation search, feedback reporting, and workspace lifecycle command families
- `commands/init`: `new` / `init`, `setup`, and completions
- `commands` (`mod.rs`): intentionally centralize global output-mode detection, offline policy, raw explicit-flag handling, context/binary resolution, destructive confirmation, and routing to handler modules
- `watch_ui`: terminal UI controller and renderer for `up`, `watch`, and replay playback
- `runtime_control`: privately own atomic local-supervisor control-message publication shared by dev watch and the watch TUI
- `output`: crate-private text, JSON, CSV, and JSONL presentation; `output::contract` registers schema envelopes, while presentation-only DTO families live in `output::{runtime,spec,evolve,doctor,sweep,examples,feedback,workspace}`
- `output::common`: own shared output-format default and legacy-JSON selection policy
- `output::docs`: own exact offline documentation-search text presentation through an injected writer
- `output::spec`: own spec-family presentation models and exact dry-run diff rendering
- `output::contract`: own versioned command-output envelopes plus checked-in output-schema registration and generation

## Known retained boundaries

- The public observation model in `job::{accounting,scheduler,stats,verify,watchdog}` is a deliberate compatibility SCC. Public snapshots embed module-owned nominal types and their builders preserve scheduler fallback, diagnostic, and probe ordering. Neutral parsing policy may move into leaf modules such as `job::analytics`, but the public types and high-level assembly stay in their current owners.
- `job::record` and `job::deep_clean` deliberately retain a narrow public cleanup SCC. `CleanupReport` embeds deep-clean details, while deep cleanup consumes record-owned mode/report/build/run behavior. Moving either side changes public type names, Schemars schema IDs, or function-item provenance; deep execution also preserves base cleanup before rendezvous pruning before orphan-directory removal.
- The public staged-input surface deliberately retains a `spec`/`cache`/planning cycle. Spec-owned Hugging Face stage-in types expose cache-owned staged-input vocabulary, while cache pruning accepts runtime plans whose services originate in spec/planner models. Moving or duplicating those public nominal types would change Rust API identity; separate this only with an explicit compatibility design, not a facade-only relocation.
- Public planning APIs deliberately retain `context::ResourceProfile` as the nominal type of `PlanOptions::resource_profiles` and the profile argument accepted by planning helpers. Moving or facade-reexporting that definition would change public type provenance and signatures; keep the direction until an explicit compatibility design permits a public API change.
- Seven schema-registered execution/result models remain command-owned: sweep status, observe, results, and score; doctor MPI and fabric smoke; and runtime smoke-test output. Their fields still drive scheduling, rollups, or command failure. Moving only their declarations would put execution policy in presentation; split a neutral result model first if a future behavior-backed refactor justifies it.
- Local and active-allocation launch paths deliberately retain their distinct post-processing of the serialized batch script. Shebang retention, leading whitespace before directives, and trailing newlines are observable; replace this with a render-target/body interface only after both complete byte streams are characterized.
- `output` still contains three compatibility-sensitive seams beyond pure formatting: inspect time-limit advisories, watch outcome-to-result handling, and failed-service state observation. Commands such as examples, weather, workspace, and sweep also retain presentation where exact writer fixtures or neutral result models are absent. Move these one characterized family at a time rather than relocating untyped filesystem reads, error ordering, ranking, or CSV policy mechanically.
- Parent-module compatibility reexports are not treated as implementation-policy ownership. Public `job` reexports preserve the library API, `commands::runtime` remains the crate-private family facade used by the centralized router and sibling handlers, and the CLI command catalog imports its cohesive help-text catalog. Dependency reviews should distinguish these facade edges from nominal-type and call edges before proposing a cycle break.
- The root command router retains workflows whose validation, context resolution, environment reads, raw-argument checks, and confirmation order are observable. Family routers move only when the root can pass resolved values without handing down global options or resolver callbacks.

## Execution flow

A typical spec-rooted submission follows this order; commands that do not need a Compose file short-circuit after routing or context resolution.

1. `main` handles the hidden completion endpoint, then `cli` parses a typed command and the centralized `commands` router applies global output, offline, confirmation, and raw-flag policy.
2. The router resolves the command's `context`: settings/profile selection, Compose and project paths, environment inputs, secrets, and binary overrides. Family handlers discover an optional cluster profile from the resolved Compose parent when their workflow needs one.
3. `ComposeSpec::load` parses YAML, resolves authoring `extends`, validates supported keys, interpolates variables, and applies semantic validation.
4. `planner::build_plan` resolves paths, command shapes, dependencies, and prepare blocks into a normalized plan.
5. `runtime_plan::build_runtime_plan` fingerprints each prepared local image immediately before pure service and cache-path derivation, then computes concrete cache artifact locations. The former `prepare::*` paths for this model remain compatibility re-exports.
6. Submission-oriented handlers call `preflight::run` to check login-node prerequisites and cluster policy before mutation.
7. `prepare::prepare_runtime_plan` imports or rebuilds artifacts when needed.
8. `render::render_script` emits the batch script consumed by `sbatch`; runtime handlers submit it or adapt the rendered body for an already active or local execution environment.
9. `job` persists tracked metadata under `.hpc-compose/` and powers `status`, `ps`, `watch`, `replay`, `stats`, `logs`, `cancel`, and artifact export. New submissions also initialize additive run evidence under `.hpc-compose/evidence/<job-id>/`; explicit note and tag mutations append evidence events, while scheduler observation commands remain read-only. During the additive migration, the legacy record remains the commit boundary: evidence failures emit `run_evidence_degraded` and retry on a later write instead of misreporting an already-tracked operation as failed. `job::replay` reconstructs a best-effort timeline from existing state, service-exit, metrics, and log artifacts while reusing the watch renderer for playback.
10. Crate-private `output` renders the selected text, JSON, CSV, or JSONL presentation; `main` is the only process-exit site and maps typed failures to the stable exit-code catalog.

`authoring_diagnostics` is an alternate static path rather than a submission step: it overlays the open root YAML buffer, resolves the same spec/plan/runtime-plan core, and reports blocking errors or lint/cluster-profile warnings without prepare, render, Slurm, SSH, or network access.

## Tracked Runtime Layout

`tracked_paths` is the single source of truth for the tracked-job layout shared by `render` and `job`.

- Compose-level metadata lives under `.hpc-compose/` next to the compose file.
- Additive run evidence lives under `.hpc-compose/evidence/<job-id>/`. Its
  immutable manifest and input lock are authoritative protocol inputs;
  `events.jsonl` is append-only under `events.lock`, and `view.json` is an
  atomically published, rebuildable projection. See
  [Run Evidence Architecture](run-evidence.md) for the identity, migration,
  concurrency, and privacy contracts.
- Per-job runtime state lives under `<runtime-root>/<job-id>/`, where `<runtime-root>` defaults to `<submit-dir>/.hpc-compose` and can be overridden with `x-slurm.runtime_root`. The renderer resolves this to an absolute path at submit time and bakes it into `JOB_ROOT`, so a running job does not depend on `$SLURM_SUBMIT_DIR`. Records persist an explicit override so later lookups address the same directory.
- Root-level `logs/`, `metrics/`, `artifacts/`, and `state.json` are the latest-view paths used by status and export commands.
- Resume-aware runs still write attempt-specific state under `attempts/<attempt>/...`.
- The batch script updates root-level latest symlinks so contributor-facing tooling can read the most recent attempt without reconstructing shell logic independently.

## Contributor commands

```bash
CARGO_INCREMENTAL=0 cargo test --workspace --locked
cargo test --test cli_runtime
cargo test --test release_metadata
cargo test --test cli_spec
cargo doc --no-deps
mdbook build docs
cargo run --features manpage-bin --bin gen-manpages -- --check
```

The full suite uses nine explicit integration-test binaries. Source suites are grouped by the harnesses in `tests/harnesses/`; `cli_spec` stays separate because its timeout probes contend with other CLI suites, and `public_api` remains isolated because it temporarily changes the process working directory.

## Coverage Notes

- Treat `src/spec/mod.rs` as high risk for broad refactors until parser and semantic-validation behavior has more focused coverage. Prefer adding behavior-first tests in `tests/cli_spec.rs` or spec unit tests before moving large validation blocks.
- Render changes should keep generated-script assertions close to `src/render.rs`. `just examples-check` shellchecks rendered batch scripts, while local launchers are produced through `up/run --local`, so local launcher syntax needs focused render or local dry-run coverage.
- Runtime command refactors should start with pure helpers that have deterministic unit tests and existing CLI integration filters. Submission, tracking, watching, and process orchestration should stay together until a narrower harness makes a larger move low risk.
- Router or retained-SCC changes require characterization of validation, diagnostic, fallback, confirmation, and destructive short-circuit order before moving a boundary; a compile-only dependency cleanup is not evidence that these runtime contracts are interchangeable. Breaking the observation SCC additionally requires scheduler/status/error-order evidence; relaxing cleanup provenance requires exact public identities, JSON, and base-cleanup → rendezvous → orphan-removal ordering.
- Run-evidence changes must preserve immutable-document publication,
  monotonically sequenced locked appends, deterministic view rebuilding, and
  legacy-record fallback. Add filesystem-level regression coverage; an
  in-memory fold alone does not verify the crash and concurrency contract.

## Documentation split

- Use this mdBook for user-facing workflows, examples, and reference material.
- Use rustdoc for contributor-facing internals and the library module map.
- Keep README short and point readers into the book instead of duplicating long-form guidance.

## Related Docs

- [Execution Model](execution-model.md)
- [Run Evidence Architecture](run-evidence.md)
- [Spec Reference](spec-reference.md)
- [Roadmap](roadmap.md)
