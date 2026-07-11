# Product Backlog

- **Last reviewed:** 2026-07-09
- **Current release baseline:** hpc-compose v0.2.0
- **Source of truth:** this file; the manual publishes it through an include
- **Scope:** product candidates and explicit decisions beyond the short strategic roadmap

This is a living, evidence-linked backlog, not a commitment schedule. Every idea
has one stable ID and one of exactly four statuses:

| Status | Meaning |
| --- | --- |
| `shipped` | The public CLI path exists and is backed by user docs, generated manpages, and focused tests at this baseline. |
| `candidate` | Worth retaining, but not committed; the next condition must be met before implementation. |
| `rejected` | Deliberately outside the current product direction; reconsider only if the stated constraint changes. |
| `superseded` | The original shape was replaced by a narrower or safer shipped direction; do not implement under this ID. |

When a release changes a row, update its evidence and next condition in the same
change. A claim is not `shipped` merely because code exists on a branch: the
Clap path, tests, manpage, and user docs must agree.

## Researcher Workflow

| ID | Proposal | Status | Evidence or decision | Next condition |
| --- | --- | --- | --- | --- |
| RW-01 | Resume a partial sweep by submitting only failed, missing, or never-submitted trials. | `shipped` | [`sweep submit --resume` docs](https://nicolasschuler.github.io/hpc-compose/sweeps.html#resume-a-partial-sweep), [CLI tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_sweep.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-sweep-submit.1) | Keep manifest drift guards and dry-run output backward-compatible. |
| RW-02 | Compare results, objective values, efficiency, and provenance across arbitrary tracked runs. | `candidate` | [`diff` and `experiment show` building blocks](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#tracked-runtime) | Define result-source precedence and a stable comparison schema before adding a new command. |
| RW-03 | Produce a paper-ready reproducibility bundle for one tracked run. | `shipped` | [`experiment bundle` docs](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html#experiment-bundles), [runtime tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_runtime.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-experiment-bundle.1) | Extend only when new provenance fields have durable recorded sources. |
| RW-04 | Attach tags and timestamped notes to tracked runs and filter by tag. | `shipped` | [`experiment tag` / `note` docs](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html#tag-and-annotate-runs), [runtime tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_runtime.rs), [`tag` manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-experiment-tag.1), [`note` manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-experiment-note.1) | Preserve append-only notes and sorted, bounded tags. |
| RW-05 | Record and render parent-child lineage across resume, sweep, and rerun edges. | `candidate` | [`checkpoints` and provenance building blocks](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html#checkpoints) | Specify identity, cross-directory discovery, and legacy-record degradation before storing edges. |
| RW-06 | Add an offline W&B/MLflow bridge with explicit post-run sync hints. | `candidate` | [`artifacts` export boundary](https://nicolasschuler.github.io/hpc-compose/artifacts-and-resume.html) | Demonstrate a tracker-neutral contract that never assumes compute-node egress or leaks credentials. |
| RW-07 | Promote a tracked notebook and notebook file into a Papermill batch spec. | `shipped` | [`notebook promote` docs](https://nicolasschuler.github.io/hpc-compose/notebook.html#promote-a-notebook-to-batch), [CLI tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_runtime.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-notebook-promote.1) | Keep promotion static and require explicit overrides when old records lack provenance. |
| RW-08 | Emit a self-contained HTML sweep report. | `candidate` | [`sweep results` and scaling reports](https://nicolasschuler.github.io/hpc-compose/sweeps.html#scaling-reports) | Choose a dependency and accessibility strategy plus a deterministic fixture-based renderer. |
| RW-09 | Record content-addressed provenance for staged datasets and models. | `candidate` | [`stage_in` and cache inventory](https://nicolasschuler.github.io/hpc-compose/spec-reference.html) | Define digest acquisition for local and remote sources without forcing network access during static planning. |
| RW-10 | Generalize sweep early termination to ASHA-style rung pruning. | `candidate` | [`sweep observe --stop-when`](https://nicolasschuler.github.io/hpc-compose/sweeps.html#objectives-and-early-termination) | Specify intermediate-objective durability, cancellation authorization, and reproducible pruning semantics. |

## Cluster Citizenship

| ID | Proposal | Status | Evidence or decision | Next condition |
| --- | --- | --- | --- | --- |
| CC-01 | Estimate queue wait from the user's own accounting history (`plan --eta`). | `candidate` | [`weather` and accounting surfaces](https://nicolasschuler.github.io/hpc-compose/canary-runs.html) | Validate useful, honest quantiles across at least two sites and define sparse-history behavior. |
| CC-02 | Advise on walltimes likely to fit visible backfill windows. | `candidate` | [`weather` / `when` advisory boundary](https://nicolasschuler.github.io/hpc-compose/canary-runs.html) | Prove a useful result from user-visible `squeue` data without implying reservation or start-time certainty. |
| CC-03 | Declare and report a run-level energy or carbon budget. | `candidate` | [`score` energy accounting](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html) | Define estimation error, site power-data precedence, and non-enforcing default behavior. |
| CC-04 | Add carbon-intensity or time-of-use conditions to `when`. | `candidate` | [`when` conditions](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#plan-and-run) | Choose a pluggable source with offline fallback, provenance, and failure behavior that cannot silently block forever. |
| CC-05 | Translate fair-share and priority data into plain-language guidance. | `candidate` | [`weather` command](https://nicolasschuler.github.io/hpc-compose/canary-runs.html) | Validate interpretations with administrators because site weighting and visibility differ. |
| CC-06 | Lint current requests against historical utilization for the same workload. | `candidate` | [`inspect --rightsize`](https://nicolasschuler.github.io/hpc-compose/canary-runs.html) | Define workload identity, minimum evidence, coverage requirements, and a no-auto-fix first release. |
| CC-07 | Warn on sustained idle CPU/GPU resources without cancelling jobs. | `shipped` | [watchdog docs](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html#idle-resource-watchdog), [runtime tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_runtime.rs), [spec schema guard](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/release_metadata.rs), [`watch` manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-watch.1) | Keep `action: cancel` rejected until a separately authorized, runtime-enforced contract exists. |
| CC-08 | Aggregate an account-period citizenship report from tracked runs. | `candidate` | [`score` reports](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#tracked-runtime) | Define discovery scope, missing-run bias, group privacy, and export format. |
| CC-09 | Recommend a preemptible checkpoint/requeue patch when site policy supports it. | `candidate` | [preemption contract](https://nicolasschuler.github.io/hpc-compose/artifacts-and-resume.html#requeue-and-the-resume-attempt-counter) | Require profile-backed policy evidence and a passing `test --preemption` drill before recommending conversion. |
| CC-10 | Remember user-observed flaky nodes and suggest exclusions. | `candidate` | [`status --verify` evidence model](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html#status-verification) | Quantify false-positive risk and keep the heuristic personal, explainable, and opt-in. |

## Robustness and Operations

| ID | Proposal | Status | Evidence or decision | Next condition |
| --- | --- | --- | --- | --- |
| OP-01 | Exercise signal, requeue, attempt-two resume, and assertions through `test --preemption`. | `shipped` | [`test --preemption` CLI docs](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#plan-and-run), [focused tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_dev_workflow.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-test.1) | Add real-cluster evidence per site without weakening the explicit quota boundary. |
| OP-02 | Freshly resubmit terminal failures through `x-slurm.retry`. | `rejected` | [one-allocation execution model](https://nicolasschuler.github.io/hpc-compose/execution-model.html) | Reconsider only if a safe, site-approved controller exists outside login-node babysitting; use Slurm requeue/resume meanwhile. |
| OP-03 | Diff a newly probed cluster report against a checked-in profile. | `candidate` | [cluster profiles](https://nicolasschuler.github.io/hpc-compose/cluster-profiles.html) | Define stable versus volatile fields and maintenance-window acceptance workflow. |
| OP-04 | Classify arbitrary application failures from logs (`debug --classify`). | `rejected` | [evidence-oriented troubleshooting model](https://nicolasschuler.github.io/hpc-compose/troubleshooting.html) | Reconsider only for explicit typed checks; application-specific classifiers remain outside core. |
| OP-05 | Add one-keystroke self-healing actions inferred by `watch`. | `superseded` | [`status --verify` and watchdog diagnostics](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html) | Propose each future action separately against a typed finding and the command authorization policy; do not infer mutation from arbitrary logs. |
| OP-06 | Reconcile tracked records, scheduler state, runtime files, checkpoints, and artifacts. | `shipped` | [`status --verify` docs](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html#status-verification), [runtime tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_runtime.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-status.1) | Keep findings diagnostic-only and degrade clearly when evidence is unavailable. |
| OP-07 | Select among multiple remote cluster profiles and list cross-cluster jobs. | `candidate` | [`up --remote` and profiles](https://nicolasschuler.github.io/hpc-compose/runbook.html#5b-submit-from-your-laptop-with-up---remote) | Define cluster identity, record discovery, version skew, and credential boundaries. |
| OP-08 | Probe shared-filesystem visibility and rename behavior from a compute node. | `shipped` | [`preflight --fs-probes` docs](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#plan-and-run), [focused tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_spec.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-preflight.1) | Add site evidence without making an allocation-consuming probe implicit. |
| OP-09 | Reap tracked-state, rendezvous, and orphaned runtime residue in one dry-run-first flow. | `shipped` | [`clean --deep` docs](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#tracked-runtime), [focused tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_runtime.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-clean.1) | Preserve dry-run parity and explicit confirmation for deletion. |
| OP-10 | Warn when requested walltime is close to historical runtime. | `candidate` | [`stats --accounting`](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html) | Define workload identity, censored-run handling, minimum sample size, and conservative thresholds. |

## Developer Experience

| ID | Proposal | Status | Evidence or decision | Next condition |
| --- | --- | --- | --- | --- |
| DX-01 | Annotate rendered scripts and map spec fields to generated lines with `explain`. | `shipped` | [`render --annotate` / `explain` docs](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#plan-and-run), [CLI tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_spec.rs), [`render` manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-render.1), [`explain` manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-explain.1) | Preserve redaction and line-number provenance as renderer behavior evolves. |
| DX-02 | Complete services, profiles, partitions, job IDs, sweep IDs, tags, and bundles dynamically. | `shipped` | [`completions` docs](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#authoring-and-setup), [completion tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_init.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-completions.1) | Keep dynamic lookup local and fast; shells without dynamic support retain static completion. |
| DX-03 | Serve validator/planner/lint diagnostics through `hpc-compose lsp`. | `shipped` | [LSP docs](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#lsp-agent-usage), [focused tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_lsp.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-lsp.1) | Add capabilities only when they reuse the Rust semantic source of truth. |
| DX-04 | Build a full-screen interactive spec wizard. | `superseded` | [`evolve` and `examples recommend`](https://nicolasschuler.github.io/hpc-compose/evolve.html) | Reopen only with user evidence that the progressive tutorial and recommender cannot address without a TUI. |
| DX-05 | Merge Compose-style base and site overlay files. | `candidate` | [settings/profile boundary](https://nicolasschuler.github.io/hpc-compose/files-and-directories.html) | Write and test merge, deletion, interpolation, provenance, and resume-diff semantics before accepting multiple `-f` inputs. |
| DX-06 | Guarantee static-safe `--offline` behavior and mutation-free dry-run overrides. | `shipped` | [`--offline` contract](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#common-flags), [offline/dry-run tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_spec.rs), [root manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose.1) | Keep every new external path covered by the command-action policy and dry-run regressions. |
| DX-07 | Extend local execution to Apptainer and expose a real local Slurm smoke path. | `shipped` | [local mode docs](https://nicolasschuler.github.io/hpc-compose/runtime-backends.html#local-mode), [dev-cluster docs](https://nicolasschuler.github.io/hpc-compose/local-slurm-dev-cluster.html), [runtime suites](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_runtime.rs), [`up` manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-up.1), [`test` manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-test.1) | Keep local mode single-host and keep dev-cluster submission explicit. |
| DX-08 | Migrate older spec versions with a diff-previewing `lint --migrate`. | `candidate` | [`lint --fix` foundation](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#lint-rules) | Introduce a spec-version contract and at least one real migration before adding the command. |
| DX-09 | Search version-matched embedded documentation offline. | `shipped` | [`hpc-compose docs` reference](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#authoring-and-setup), [CLI tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_spec.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-docs.1) | Keep indexing deterministic and version-matched to the binary. |
| DX-10 | Compare the current effective spec against a tracked run before submission. | `shipped` | [`diff --against-spec` docs](https://nicolasschuler.github.io/hpc-compose/cli-reference.html#tracked-runtime), [runtime tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_runtime.rs), [manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-diff.1) | Preserve secret redaction and sweep-variable replay. |

## Cross-Cutting and Long-Horizon Items

| ID | Proposal | Status | Evidence or decision | Next condition |
| --- | --- | --- | --- | --- |
| SITE-01 | Manage expiring `ws_*` workspaces through hpc-compose. | `shipped` | [workspace lifecycle docs](https://github.com/NicolasSchuler/hpc-compose/blob/main/docs/src/workspaces.md), [CLI tests](https://github.com/NicolasSchuler/hpc-compose/blob/main/tests/cli_workspace.rs), [`workspace` manpage](https://github.com/NicolasSchuler/hpc-compose/blob/main/man/man1/hpc-compose-workspace.1) | Add adapters only for named sites with primary-source verification; never make release implicit. |
| META-01 | Rebuild LLM discovery, version matching, safety policy, and the progressive skill. | `candidate` | [AI setup boundary](https://nicolasschuler.github.io/hpc-compose/ai-agent-setup.html) | Mark shipped only when generated policy/context/skill artifacts and unauthorized-action forward tests all pass. |
| MOON-01 | Support Slurm heterogeneous-job components per service. | `candidate` | [declared roadmap epic](https://nicolasschuler.github.io/hpc-compose/roadmap.html#heterogeneous-jobs) | Complete capability probing and a planner/render/runtime design that preserves one inspectable submission. |
| MOON-02 | Run elastic sweep workers from a shared trial queue. | `candidate` | [current one-trial-one-allocation model](https://nicolasschuler.github.io/hpc-compose/sweeps.html) | Demonstrate crash-safe locking and preserve per-trial provenance before trading inspectability for throughput. |
| MOON-03 | Race equivalent submissions across clusters and cancel the loser. | `candidate` | [remote execution boundary](https://nicolasschuler.github.io/hpc-compose/runbook.html#5b-submit-from-your-laptop-with-up---remote) | First ship multi-cluster identity and an external atomic claim mechanism with explicit duplicate-quota authorization. |
| MOON-04 | Hot-swap laptop edits inside a held allocation. | `candidate` | [`alloc` and development modes](https://nicolasschuler.github.io/hpc-compose/development-workflow.html) | Prove robust one-OTP synchronization and an idle-allocation guard before combining the workflows. |
| MOON-05 | Add CPU/GPU power postures and power-capped runs. | `candidate` | [`score` energy accounting](https://nicolasschuler.github.io/hpc-compose/runtime-observability.html) | Separate unprivileged Slurm CPU controls from site-admin GPU controls and require capability evidence. |
| MOON-06 | Estimate queue what-if outcomes across resource geometries. | `candidate` | [`weather` advisory model](https://nicolasschuler.github.io/hpc-compose/canary-runs.html) | Start with measured personal-history quantiles; never label an estimate a scheduler simulation. |

## Review Rules

- Reconcile `shipped` rows against Clap, focused tests, generated manpages, and
  user docs at every release baseline change.
- Prefer a new stable ID when a proposal's authorization boundary or core
  semantics materially change; use `superseded` to close the old shape.
- Keep rejected ideas visible so the same unsafe or out-of-scope design is not
  repeatedly rediscovered.
- The [short roadmap](https://nicolasschuler.github.io/hpc-compose/roadmap.html)
  owns strategy. This backlog owns item-level state and evidence.
