# Implementation Plan — Experiment-Workflow Features

> **Status: exploratory engineering plan, not a public commitment.**
> Generated 2026-06-21 from a systematic exploration of the codebase. This is an
> internal planning artifact for contributors — it is **not** part of the published
> manual and does **not** supersede the intentionally minimal public
> [`docs/src/roadmap.md`](src/roadmap.md). Every plan below was verified
> (high confidence) against the code as it stood when written; cited line numbers are
> hints — verify by symbol name, as lines drift.

## Goal and guardrails

The north star: **absorb multi-step *remote* friction into single, inspectable,
agent-parseable commands**, drivable from a laptop with **no agent installed on the
cluster**. Every feature here must preserve hpc-compose's narrow scope:

- one Slurm allocation per application; one inspectable `sbatch` script;
- **no** long-running control plane/daemon, **no** proxy, **no** dynamic cross-node
  scheduling beyond explicit `x-slurm.placement`;
- commands stay one-shot; state lives in tracked files under `.hpc-compose/`;
- the static-safe vs. approval-gated safety contract is preserved;
- out of scope (unchanged): Bayesian/adaptive sweep optimizers, Docker Compose
  `build:`/`ports`/`networks`/`deploy`.

## The throughline

hpc-compose already nails **authoring** and **submission**. It abandons a
laptop-driven user at the two hardest *remote* moments: **reaching what you launched**
(a served endpoint or dashboard) and **pulling results back** off shared storage. All
three target personas (LLM experiments, SE/scalability studies, accelerated ML) stall
at the same place, so a handful of cross-cutting connectivity + result-collection
features unblock them at once.

## Master plan

| # | Feature | Surface | Effort | Risk | Wave | Depends on | Files | Conf. |
|---|---------|---------|:--:|:--:|:--:|:--:|:--:|:--:|
| 1 | `login_host` setting + `notebook --format json` | settings+output | S | low | 1 | — | 10 | high |
| 4 | `up` surfaces `endpoints` + `next_commands` | output | S | low | 1 | — | 5 | high |
| 6 | `new --template eval-harness` | template | S | low | 1 | — | 5 | high |
| 9 | Auto-pin provenance into `SubmissionRecord` | tracked-record | M | med | 1 | — | 18 | high |
| 10 | `sweep results --format csv\|json` + `score/stats --sweep` | subcmd+flag | M | low | 1 | 9¹ | 5 | high |
| 15 | `examples/training-tensorboard.yaml` | example | S | low | 1 | — | 5 | high |
| 2 | `reach <service>` (print/`--open` SSH tunnel) | new-command | M | low | 2 | 1¹ | 9 | high |
| 3 | `pull <job>` (resolve bundle, print rsync) | new-command | M | low | 2 | — | 7 | high |
| 5 | `experiment`/`show <job>` (one JSON per run) | new-command | M | low | 2 | **9** | 9 | high |
| 7 | `x-slurm.parallelism {tensor,pipeline}` + GPU check | spec-field | M | low | 2 | — | 8 | high |
| 8 | `run --image --dataset --output` | flag | M | med | 2 | — | 7 | high |
| 12 | `replicates: N` + mean/std rollup | spec+output | M | med | 2 | — | 9 | high |
| 13 | `diff --across <sweep>` / `--jobs a,b,c` (N-way) | flag | M | med | 2 | — | 7 | high |
| 16 | `checkpoints <job>` (attempt/requeue history) | new-command | M | low | 2 | — | 7 | high |
| 17 | Content-addressed dataset/model cache | cache-infra | L | med | 3 | — | 6 | high |
| 11 | URI `stage_in: hf://org/model@rev` | spec-field | M | med | 3 | **17** | 18 | high |
| 14 | `sweep.objective.scaling_axis` + `observe --scaling` | spec+flag | M | med | 3 | 12¹ | 8 | high |

¹ *soft* edge (build-first preferred, not a compile dependency). **Bold** deps are hard.

**Topological build order:** `1 → 4 → 6 → 15 → 9 → 10 → 2 → 3 → 5 → 7 → 8 → 12 → 13 → 16 → 17 → 11 → 14`

## Build once, reuse everywhere (shared components)

| Component | Introduced in | Reused by |
|---|:--:|---|
| `current_hostname()` promoted private→`pub(crate)` (exec.rs) — host-resolution primitive | 1 | 2, 3, 5 |
| `ResolvedContext.login_host` + Defaults/Profile parity + schema catalog | 1 | 2, 5 |
| `tunnel_hint(port,compute,login)` renderer (extract bare `ssh -L` from notebook hint) | 1 | 2, 5 |
| `next_commands: Vec<String>` serde pattern (`skip_serializing_if`) | 4 | 5, 2, 3 |
| `JobProvenance`/`GitProvenance` + `collect_provenance` (new `src/job/provenance.rs`) | 9 | 5, 10 |
| `build_status_snapshot` + `SchedulerOptions` read-only discovery path | 2 | 3, 5, 13, 16 |
| Dedicated per-command CSV format enum (sibling of `StatsOutputFormat`, **not** on `OutputFormat`) + `csv_field` | 10 | 13 |
| `CacheEntryKind`/manifest extension + `scan_cache` discovery triple + `ensure_staged_dataset()` | 17 | 11 |
| Reserved `HPC_COMPOSE_SWEEP_*` interpolation-var convention | 12 | 14, 8 |
| `ExperimentShowReport` read-only aggregator | 5 | — |

## Risk register

| # | Risk | Mitigation |
|---|---|---|
| 9 | Touches ~18 files incl. every hand-built `SubmissionRecord` literal; image refs must derive from `ImageSource` (no `image` field); fabricated-SHA hazard | Additive `Option` field w/ `serde(default, skip_serializing_if)` → no schema bump; reuse cache.rs 4-arm `ImageSource` stringify; `read_git_provenance` returns `None` on **any** error (hermetic tests) |
| 17 | `scan_cache` is a 3-fn chain — an in-dir `manifest.json` re-derives a bogus path; manifest has no `schema_version` | **Sidecar** files (`<key>.dataset.json`), patch **all three** discovery fns; add `#[serde(other)] Unknown` + back-compat deser test; stage under `cache_dir/{datasets,models}/` |
| 11 | Real login-node network egress at prepare; `HF_TOKEN`; 12 `PrepareOptions` literals need new bin; weights could be pruned | Hard-error non-`hf://` + require immutable `@rev` at validate; add `huggingface_cli_bin` to struct **and** `Default`; new `referenced_artifacts` loop over `stage_in`; `HF_TOKEN` import-only |
| 12 | `SWEEP_MANIFEST_SCHEMA_VERSION` 2→3; best-trial semantics change to group-mean; bare reserved vars could clash | `serde(default)` new fields so v2 loads; rank on winning **group mean**; collision-protected `HPC_COMPOSE_SWEEP_` prefix |
| 8 | `run` has **no `--dry-run`** (only `--script-out`); volumes validated in planner, not `validate_mount_syntax` | Test w/ fake `sbatch/squeue/sacct` + `--no-preflight --skip-prepare`; thread params as explicit `run_ephemeral` args; compute env var via same `resolve_path` |
| 14 | Draft assumed `scheduler.elapsed_seconds` — **does not exist** | Source runtime from `max(PsServiceRow.duration_seconds)`; one snapshot/trial; live-only, skip trials missing runtime |
| 2 | `--open` must not daemonize; Sleep/Log readiness has no port | Foreground `.status()` only; bail with `--port` message for non-Tcp/Http |
| 5 | Could over-reach into scheduler probes | Confine to `build_status_snapshot` + terminal-only sacct; non-mutating manifest read |
| 13 | Format enum must live in `cli/mod.rs`; N-way types need re-export via `job/mod.rs` | Declare enum beside other format enums; add re-export; reuse single-record `parse_config_snapshot` |

## Wave 1 — detailed walkthrough

Wave 1 is the set of low-risk wins that also lay the shared foundations (host
resolution, the `next_commands` convention, provenance) that Wave 2 consumes.

### #1 · `login_host` setting + `notebook --format json` · S

- **Problem.** When `notebook` launches, hpc-compose prints a human-styled connection
  blurb. An agent on your laptop must regex-scrape that styled text for the URL, and it
  *guesses* the SSH jump host from the compute node's hostname — wrong behind a bastion.
  There is also nowhere to record what your cluster's login host actually is.
- **Mechanics.** Add optional `login_host` under `[defaults]` and `[profiles.<name>]`,
  resolved (profile > defaults > none) into `ResolvedContext`. Add `--format json` to
  `notebook` emitting one object `{url, tunnel_hint, compute_node, login_host, job_id,
  next_commands}`. The tunnel hint prefers the configured `login_host` over the guess.
- **Why Wave 1.** Small, and it introduces three primitives the connectivity features
  reuse: `login_host`, the promoted `current_hostname()`, and the reusable
  `tunnel_hint()` renderer — so #2 and #5 don't re-invent host resolution.
- **Guard.** `login_host` is descriptive only; the tool never opens a connection itself.
- **Before → after.** 5 fragile steps (scrape URL, find node, guess host, assemble
  `ssh -L`, often wrong) → set `login_host` once, get correct structured JSON.

### #4 · `up` surfaces `endpoints` + `next_commands` · S

- **Problem.** `up --detach --format json` returns only `job_id`/`script_path`. If the
  spec serves something (vLLM on 8000, a dashboard on 6006), the agent must separately
  poll status, find the node, and hand-build a tunnel — and guess what to do next.
- **Mechanics.** After readiness, `SubmitOutput` gains `endpoints:[{service,host,port,
  url}]` (derived from each service's `readiness: tcp/http` port) and a `next_commands`
  list of valid follow-ups. `--print-endpoints` for humans. Both fields
  `skip_serializing_if` empty, so existing JSON consumers are unaffected.
- **Why Wave 1.** S effort; hardens the `next_commands` serde pattern #5 reuses verbatim.
  Pure derivation from the plan + readiness — no new scheduler calls.
- **Guard.** Reported **exactly once at readiness**; no watcher loop, proxy, or daemon —
  afterwards identical to today's `--detach`/`--follow`.
- **Before → after.** Poll + manual tunnel assembly → endpoints + next commands handed
  to you in the submit output.

### #6 · `new --template eval-harness` · S

- **Problem.** A serve-and-evaluate spec is among the hardest to author: a vLLM server,
  a client gated on the server's health (`depends_on: service_healthy` + HTTP readiness),
  the loopback `OPENAI_BASE_URL`, and artifacts capturing `results.json`. Readiness and
  ordering are easy to get wrong.
- **Mechanics.** A new built-in template (registered in the template + examples
  registries + docs) scaffolding exactly that, with a model/tasks `sweep` stub using
  `${...:-default}` fallbacks so the base spec still validates/plans/renders.
- **Why Wave 1.** S, pure composition of existing primitives — no Rust types, no schema
  change, no new runtime. Independent; immediate LLM-persona value.
- **Guard.** Composition only.
- **Before → after.** Hand-author the trickiest multi-service spec →
  `hpc-compose new my-eval --template eval-harness`.

### #9 · Auto-pin provenance into `SubmissionRecord` · M (med risk — front-loaded)

- **Problem.** Reproducibility needs to know exactly *what* produced a result: code
  version, tool version, image. Today that lives in a lab notebook by hand.
- **Mechanics.** New `src/job/provenance.rs` with `JobProvenance{tool_version,
  git:Option<GitProvenance{sha,dirty,branch}>, image_refs}`, populated at every submit
  path (up/run/exec/notebook/germinate/sweep-trial) and stored as an optional field on
  `SubmissionRecord` (additive, no schema-version bump). `diff` surfaces deltas.
- **Why Wave 1.** Hard dependency for #5's provenance block and feeds #10's per-trial
  columns. Medium-risk (≈18 files — every record literal), so front-loading de-risks the
  Wave 2 features that consume it and lets them use *real* provenance, not stubs.
- **Guard.** Never fabricate a SHA — `read_git_provenance` returns `None` on any error;
  a real registry digest is captured only during the approval-gated image import.
- **Before → after.** Manual notebook bookkeeping → every run self-describes; `diff`
  shows what changed between two runs for free.

### #10 · `sweep results --format csv|json` + `score/stats --sweep` · M (flags alone: S)

- **Problem.** Turning a sweep into a paper table is ~4–6 steps *per trial* (load
  manifest, `sweep status --format json`, `score`/`stats`/`sacct` per job, manual join).
- **Mechanics.** New `sweep results` emits one tidy row per trial: `trial_id`, each sweep
  variable as its own column, status, parsed objective(s), elapsed, optional
  `--include score,energy`. Uses a dedicated per-command CSV format enum (sibling of
  `StatsOutputFormat`, **not** added to the shared `OutputFormat`). Plus `--sweep <id>`
  on `score`/`stats` for the per-trial efficiency/energy table.
- **Why Wave 1.** The core (objective parsing, manifest iteration) exists today and is
  read-only/low-risk; only the optional provenance columns need #9 (soft 9→10 edge). It
  establishes the dedicated CSV-enum pattern #13 reuses.
- **Guard.** Ranking stays single-objective; extra metrics are export-time *columns
  only*; `sweep results` leaves the manifest byte-identical (unlike `sweep observe`).
- **Before → after.** Per-trial manual joins → `hpc-compose sweep results --format csv > runs.csv`.

### #15 · `examples/training-tensorboard.yaml` · S

- **Problem.** Reaching a live TensorBoard from your laptop while a GPU job trains is
  common, but the readiness probe + sidecar + shared-logdir wiring is fiddly — and the
  allocation must still terminate despite a long-lived sidecar.
- **Mechanics.** A shipped example: trainer (gpus:1 → shared logdir) + TensorBoard
  sidecar on 6006 with HTTP readiness + shared-logdir volume in both + artifacts + the
  `request.done` sentinel (from `vllm-openai.yaml`) so the allocation ends after
  training. `next_commands` point at the tunnel hint (or `reach tensorboard` once #2 ships).
- **Why Wave 1.** S, self-contained with today's primitives (no code), independent.
- **Guard.** Composition only.
- **Before → after.** Hand-author a sidecar + guess the readiness probe + derive the
  tunnel → start from a validated example.

## Per-feature plans (all waves)

### Wave 2 — the loop closes

**#2 · `reach <service>`** — new `src/commands/runtime/reach.rs` (modeled on
`inspect.rs::status`); new `reach` command + dispatch + help + completions; logic:
`build_status_snapshot` → service row → derive node + port (`Tcp.port`/`Http` url) →
`tunnel_hint`; `--open` = foreground `ssh -N -L` via `.status()`. Tests (6): port
derivation, JSON, `--open` rejects `--format`, Sleep/Log → `--port` required.
**Guard:** foreground only, never daemonize.

**#3 · `pull <job>`** — new `pull` command; `--into <dir>` (default `.`), `--job-id`,
`--format json`; runs `export_artifacts` then **prints** `rsync -avz
<host>:<cluster_dir>/ <dest>/`; JSON `{bundles,cluster_path,suggested_command,files,
bytes}`. Reuses `current_hostname` from #1. **Guard:** never opens a network connection.

**#5 · `experiment`/`show <job>`** (hard-dep #9) — `ExperimentShowReport` aggregating
`StatusSnapshot` + `SubmissionRecord`(+provenance) + `ArtifactManifest` +
`EfficiencyScoreReport`; new `show` command (text + `--format json`); per-service
nodelist/status/tunnel_hint; efficiency only for terminal Slurm jobs; `next_commands`
shipped-only. **Guard:** read-only; scheduler contact == `status` + terminal-only sacct.

**#7 · `x-slurm.parallelism {tensor,pipeline}`** — `Parallelism{tensor,pipeline}` on
service `x-slurm`; validation rejects `tensor*pipeline != nodes*gpus_per_node` (when set)
and non-positive values; render exports `$HPC_COMPOSE_TP_SIZE`/`$HPC_COMPOSE_PP_SIZE`;
schema + spec-reference + 3 `cli_spec` parity tests. **Guard:** no `#SBATCH`/srun flag
emitted; no dynamic scheduling.

**#8 · `run --image --dataset --output`** — `--dataset <path>`/`--output <dir>` on `run`
(explicit `run_ephemeral` params, not shared `ResourceCliOptions`); dataset → RO mount +
`HPC_COMPOSE_DATASET_DIR`; output → `artifacts.export_dir` (collect=Always) +
`HPC_COMPOSE_OUTPUT_DIR`, via the same `resolve_path`; both flags without `--image` →
clear error. Tests with fake bins + `--no-preflight --skip-prepare`. **Guard:**
path-based only; no `hf://`; no build system.

**#12 · `replicates: N`** — `replicates` on `SweepConfig`; `config_key` + replicate axis
on `SweepManifestTrial`; `SWEEP_MANIFEST_SCHEMA_VERSION` 2→3 (`serde(default)` so v2
loads); expand N seeded trials (`t000r0…`); group `observe`/`status` by config →
`mean±std(n)`; `best_trial_id` ranks **group mean**; reserved
`HPC_COMPOSE_SWEEP_REPLICATE/SEED`. **Guard:** no new command; ranks on group mean.

**#13 · `diff --across` / `--jobs`** — `NwayJobDiffReport` + `build_nway_job_diff_report`
(re-export via `job/mod.rs`); `DiffOutputFormat` in `cli/mod.rs`; `--across <sweep-id>`,
`--jobs a,b,c`, `--format csv`; column per run, rows only for fields differing anywhere;
reuse single-record `parse_config_snapshot`. **Guard:** pairwise behavior preserved; pure
projection.

**#16 · `checkpoints <job>`** — `CheckpointsReport{current_attempt,attempts,requeues,
is_resume,resume_dir,attempt_history,notes}`; new `checkpoints` command; `--all-attempts`,
`--format json`; projects local `state.json` + `attempts/<n>/`; missing/corrupt → notes +
`readable=false`. **Guard:** local state only, no scheduler/cluster-FS reads by default.

### Wave 3 — staging + dedup

**#17 · Content-addressed dataset/model cache** (build before #11) —
`CacheEntryKind::{Dataset,Model}` + `#[serde(other)] Unknown`; manifest gains
`uri/revision/content_digest`; new `src/cache/dataset.rs` (`dataset_cache_key`,
`ensure_staged_dataset`); **sidecar** manifests (`<key>.dataset.json`); patch all 3
discovery fns; stage under `cache_dir/{datasets,models}/`; write-through reuse (`Reused`
on 2nd call). **Guard:** no compose-schema change.

**#11 · `stage_in: hf://org/model@rev`** (hard-dep #17) — `hf://` only (hard-error other
schemes); require immutable `@rev`; login-node `huggingface-cli download --local-dir`
through #17's cache; `HF_HOME` only if unset; `HF_TOKEN` import-only/never persisted;
`referenced_artifacts` loop so weights aren't pruned; `--huggingface-cli-bin` on
prepare/up/run/sweep/doctor (struct **and** `Default`). **Guard:** import-only; `hf://`
only; rendered script has no `hf://` entry.

**#14 · `sweep.objective.scaling_axis`** (soft-after #12) — `scaling_axis` on
`sweep.objective`; `scaling` block on `SweepObserveOutput`; axis must name a real sweep
parameter; runtime from `max(PsServiceRow.duration_seconds)` (live-only); strong →
speedup+efficiency, weak → efficiency; skip trials missing runtime; `sweep observe
--scaling`. **Guard:** post-hoc only; no submission change, no manifest bump.

## How this was produced

A multi-agent exploration mapped the relevant subsystems and prior art, proposed
features per persona, adversarially critiqued each against the narrow-scope guardrails,
and produced per-feature implementation plans that were each verified by opening the
cited files. The verification corrected real first-pass errors (e.g. a non-existent
`scheduler.elapsed_seconds`; provenance targeting `SubmissionRecord`, not `JobRecord`;
the need for a dedicated CSV format enum). Treat line numbers as hints and re-confirm
symbols before editing.
