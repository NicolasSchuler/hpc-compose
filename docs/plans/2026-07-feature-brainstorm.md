# Feature Brainstorm — Ideas & Further Improvements

- **Date:** 2026-07-04
- **Status:** brainstorm / candidate backlog (nothing here is committed work)
- **Baseline:** hpc-compose v0.2.0
- **Scope:** product ideas spanning quick wins to ambitious bets, in four lanes:
  researcher workflow, cluster citizenship, robustness & operations, developer
  experience. Each idea lists a one-sentence pitch, who benefits, rough effort
  (S/M/L), and the existing subsystem it builds on.

## Grounding

This brainstorm is grounded in the README, the docs (roadmap, execution model,
sweeps, canary runs, artifacts/resume, runtime observability, full CLI
reference), the ~60-command CLI tree in `src/cli/commands.rs`, and the
`examples/` directory. Existing surface deliberately built around rather than
duplicated:

- Sweeps already have objectives, replicates, `--stop-when` early termination,
  and scaling reports; Bayesian/adaptive *selection* is declared out of scope,
  but the docs note the objective/stop machinery "is the foundation any future
  optimizer would build on."
- `score` already models energy (`--pue`, `--gpu-tdp-w`,
  `--cpu-watts-per-core`).
- `weather`, `when`, `germinate`, `inspect --rightsize`, `experiment show`,
  `replay`, `checkpoints`, `reach`, `rendezvous`, and cluster profiles
  (`doctor cluster-report` → `.hpc-compose/cluster.toml`) all exist.
- Per-service cgroup GPU attribution landed in v0.2.0.
- Heterogeneous jobs are already a declared planned epic in the roadmap.
- Remote mode (`up --remote`, `--remote` on follow-up commands) operates over
  a one-OTP-per-session SSH ControlMaster contract.

---

## Lane 1 — Researcher workflow

What does a PhD student running ML experiments still do manually around
hpc-compose?

### 1.1 `sweep submit --resume` (delta submission)

Re-submit only the failed/missing/never-submitted trials of an existing
manifest instead of starting over — today submission is sequential and a
single `sbatch` failure strands a partial manifest that must be repaired by
hand (documented as a limitation in `sweeps.md`).

- **Who:** anyone running sweeps > 10 trials on a flaky login node.
- **Effort:** S
- **Builds on:** sweep manifest + per-trial status.

### 1.2 `experiment compare` (cross-run results table)

Aggregate N tracked runs — not just sweep siblings — into one table combining
parsed objective, efficiency score, key spec deltas, and provenance.
`diff --jobs` compares specs and `sweep results` compares trials, but nothing
compares *results* of ad-hoc run sets ("Tuesday's baseline vs. today's fix").

- **Who:** PhD students keeping results in a spreadsheet today.
- **Effort:** M
- **Builds on:** `experiment show`, `diff --jobs` matrix, sweep objective
  parsing.

### 1.3 `experiment bundle` (paper-ready reproducibility archive)

One command emits a citeable tarball: compose spec, fully resolved config,
rendered sbatch, image digests, git SHA + dirty flag, sweep seeds, metrics
CSV, `checkpoints` attempt history, and a generated "methods appendix"
markdown.

- **Who:** researchers writing the reproducibility section; reviewers.
- **Effort:** S–M
- **Builds on:** submit-time provenance already captured by `experiment show`,
  artifacts bundles, `config --format json`.

### 1.4 Run tagging and notes (`experiment tag/note`)

Attach tags ("baseline", "lr-bug") and freeform notes to tracked job records,
filterable in `jobs list` and shown in `experiment show` — the sticky-note
layer every researcher currently keeps in a text file.

- **Who:** anyone with more than ~20 tracked runs.
- **Effort:** S
- **Builds on:** tracked job JSON records.

### 1.5 Run lineage graph

Record and render parent→child edges: resumed-from-checkpoint,
sweep-spawned-from-base, re-run-after-diff — `experiment lineage` prints the
ancestry of any run (text/DOT), answering "which run produced the checkpoint
this run loaded?"

- **Who:** long training campaigns with resume chains.
- **Effort:** M
- **Builds on:** resume attempts, sweep trial records (`kind: sweep_trial`),
  `inspect --dependencies` DOT rendering.

### 1.6 W&B/MLflow offline bridge

A `x-slurm.trackers:` block that injects the right env/mounts for
offline-mode tracking inside airgapped compute nodes, then surfaces
`wandb sync` / `mlflow` upload as a post-run "Next:" hint (or runs it during
`artifacts` export from the login node, where there *is* network).

- **Who:** labs standardized on W&B whose compute nodes have no egress.
- **Effort:** M
- **Builds on:** artifacts export stage, existing "Next:" hint machinery,
  hooks.

### 1.7 `notebook promote`

Convert a tracked notebook session plus an `.ipynb` into a batch compose spec
(papermill execution, same image, same resources, parameters exposed as
interpolation variables) — the interactive→batch cliff is where
reproducibility currently dies.

- **Who:** every researcher who prototypes in Jupyter.
- **Effort:** M
- **Builds on:** `notebook` tracked sessions, `new` template machinery, sweep
  interpolation.

### 1.8 `sweep report --html`

Emit a single self-contained HTML report per sweep: ranked table, per-config
mean±std, scaling plot, objective-vs-trial chart, links to logs — dropped into
`export_dir` beside the bundles.

- **Who:** advisors/PIs who will never SSH in.
- **Effort:** M
- **Builds on:** `sweep observe/results/--scaling` JSON, artifacts export.

### 1.9 First-class staged-data provenance

Promote the `hf-stage-model` pattern into a spec-level `x-data.stage:` with
content hashes recorded into run provenance, so `experiment show` answers
"exactly which dataset/model bytes did this run see."

- **Who:** anyone whose results changed because the dataset silently did.
- **Effort:** M
- **Builds on:** cache staged entries, provenance capture.

### 1.10 ASHA-style rung pruning for sweeps

Not Bayesian *selection* (explicitly out of scope), but generalized early
*termination*: parse intermediate objectives at rung checkpoints and `scancel`
the bottom fraction — the docs themselves position the objective/stop
machinery as the foundation any future optimizer would build on.

- **Who:** GPU-hour-constrained sweep users.
- **Effort:** M–L
- **Builds on:** `sweep observe --watch --stop-when`, `sweep stop`'s scancel
  path.

---

## Lane 2 — Cluster citizenship

Features that make jobs cheaper, greener, and friendlier to co-users of the
cluster.

### 2.1 Queue-wait estimates from your own history (`plan --eta`)

Compute Submit→Start quantiles from the user's own `sacct` history bucketed by
geometry (partition, GPUs, time limit) and show "requests like this waited
p50 40m / p90 6h" at plan time — honest quantiles, not prediction theater.

- **Who:** everyone deciding between 4 GPUs now vs. 8 GPUs someday.
- **Effort:** M–L
- **Builds on:** `stats --accounting` sacct plumbing, `weather`.

### 2.2 Backfill-fit advisor

Suggest the largest walltime that likely fits the current backfill hole, using
`squeue --start` estimates and running-job end times — "cap at 3h50m and
you'll likely start now." Must degrade gracefully: some sites (HAICORE among
them) deny `sinfo` to regular users, so build on `squeue`-visible data first.

- **Who:** short-job users stuck behind whales.
- **Effort:** M–L
- **Builds on:** `weather`, `when --free-nodes` polling.

### 2.3 `x-slurm.energy_budget`

Declare a kWh (or CO₂) budget per run; `plan` estimates
geometry × TDP × walltime against it, `score` reports actual vs. budget, and a
hook fires on breach — turns the existing energy math from retrospective into
a contract.

- **Who:** green-computing-mandated labs (increasingly all of them, especially
  in Germany).
- **Effort:** S–M
- **Builds on:** `score --pue/--gpu-tdp-w/--cpu-watts-per-core`, hooks.

### 2.4 Carbon/time-of-use windows for `when`

New conditions — `--carbon-below <gCO2/kWh>` (pluggable source:
electricityMaps API or a static tariff table in the cluster profile) — so
deferrable jobs land in green/cheap windows; `--between` already proves the
pattern.

- **Who:** sweep and batch users with flexible deadlines.
- **Effort:** M
- **Builds on:** `when` condition machinery, cluster profiles.

### 2.5 Fair-share interpreter (`weather --me`)

`weather` already pulls `sshare`/`sprio`; translate them into plain language
and action: "your account consumed 140% of its share; expected priority
penalty ~X; smaller/shorter requests will backfill" — raw fair-share numbers
are famously unreadable.

- **Who:** every user who has ever asked the admin "why is my job pending."
- **Effort:** S–M
- **Builds on:** `weather`.

### 2.6 Historical right-size lint

A lint rule that checks the spec against past runs of the same spec name:
"last 5 runs peaked at 38% of requested mem — HPC9xx, suggested patch:
`mem: 24G`," with `lint --fix` applying it — closes the loop from
`inspect --rightsize` back into authoring, where it actually changes behavior.

- **Who:** chronic over-requesters (i.e., everyone).
- **Effort:** M
- **Builds on:** `inspect --rightsize`, tracked history, lint's patch
  machinery.

### 2.7 Idle-GPU watchdog

An in-job policy: if attributed GPU utilization stays ~0% for N minutes across
all services, fire a hook / warn in `watch` / optionally self-cancel — the
"allocation left running overnight" tax, now detectable per service thanks to
the just-landed cgroup attribution.

- **Who:** users and the admins who email them.
- **Effort:** M
- **Builds on:** metrics sampler + per-service GPU attribution, failure-policy
  hooks.

### 2.8 Lab citizenship report (`score --account --period`)

Aggregate efficiency and energy across a group's tracked runs for a month:
total GPU-hours, mean utilization, energy, worst offenders — a PDF-able report
card a PI can circulate.

- **Who:** PIs, allocation-renewal proposals.
- **Effort:** M
- **Builds on:** `score`, sacct accounting.

### 2.9 Preemptible conversion advisor

Detect scavenger/preemptible QOS in the cluster profile and offer the exact
YAML patch (requeue + signal + resume, per the `preemptible-checkpoint`
pattern) with an estimate of queue-time savings — most users don't use
preemptible tiers because the checkpoint wiring feels risky.

- **Who:** users with resumable workloads sitting in the priority queue.
- **Effort:** M
- **Builds on:** `doctor cluster-report`, lint patch output, the existing
  requeue/resume contract.

### 2.10 Personal flaky-node memory

Record nodelists of failed runs; when the same node correlates with repeated
NCCL/ESTALE/exit-code failures in *your* history, suggest an `--exclude` list
at submit — honest about being a personal heuristic, not cluster telemetry.

- **Who:** multi-node training users on aging clusters.
- **Effort:** M
- **Builds on:** tracked state nodelists, failure records.

---

## Lane 3 — Robustness & operations

### 3.1 Preemption-contract verification (`test --preemption`)

Extend `test` to run the spec briefly, deliver the configured signal,
kill/requeue, restart as attempt 2, and assert the workload actually resumed
(attempt counter advanced, user-provided resume assertion passed) — today the
whole requeue+signal+resume contract is unverifiable until real preemption at
hour 40.

- **Who:** everyone using `x-slurm.resume`; CI via the local Slurm dev
  cluster.
- **Effort:** M
- **Builds on:** `test --local/--submit`, signal/resume/requeue, dev cluster.

### 3.2 Resubmission policy (`x-slurm.retry`)

Requeue covers in-place scheduler requeues; add a declarative policy for
*fresh resubmission* after terminal failures (exit-code allowlist,
node-failure classes, max attempts, backoff), executed by a foreground
babysitter in the `when`/`watch` mold — no daemon, staying true to
no-control-plane.

- **Who:** long campaigns hit by transient NCCL/fabric flakes.
- **Effort:** L
- **Builds on:** `when` monitoring, tracked state, resume attempts.

### 3.3 Cluster drift detection (`doctor cluster-report --diff`)

Re-probe capabilities and diff against the checked-in `cluster.toml` —
partitions renamed, GRES changed, Pyxis/Enroot version bumped, default account
gone — and surface drift as a preflight warning instead of a mid-run mystery.

- **Who:** everyone after a cluster maintenance window.
- **Effort:** S–M
- **Builds on:** `doctor cluster-report`, `preflight`.

### 3.4 Failure classifier (`debug --classify`)

Encode the troubleshooting guide as a signature library over batch+service
logs (CUDA OOM vs. host OOM, NCCL timeout, ESTALE, time-limit,
readiness-never-met) emitting a typed diagnosis + next command, JSON for CI —
`debug` already recommends next commands; this makes the mapping systematic
and testable.

- **Who:** new users; CI pipelines triaging red runs.
- **Effort:** M
- **Builds on:** `debug`, `logs --grep`, troubleshooting doc content.

### 3.5 Self-healing watch (`watch` action hints)

When the classifier (3.4) fires during a live run, the TUI offers
one-keystroke actions: "OOM detected — apply suggested mem patch and resubmit?
(y/n)" — triage becomes interactive instead of archaeology.

- **Who:** anyone babysitting a run.
- **Effort:** M (after 3.4)
- **Builds on:** watch TUI, lint patch application.

### 3.6 State reconciliation (`status --verify`)

Cross-check tracked state vs. `sacct` vs. on-disk runtime files and flag
contradictions ("tracked says running; sacct says NODE_FAIL 2h ago") with
repair suggestions — tracked metadata can silently diverge when sessions die
mid-watch.

- **Who:** remote/laptop-driven users reconnecting after the fact.
- **Effort:** S–M
- **Builds on:** `status`, `checkpoints` degraded-notes pattern.

### 3.7 Multi-cluster profiles (`up --cluster <profile>`)

Same spec, N clusters: profiles already carry settings; add per-profile
login_host/partition/account mapping and `jobs list --all-clusters`, so moving
a workload from HAICORE to a second site is a flag, not a fork of the YAML.

- **Who:** users with allocations on 2+ machines (common near paper
  deadlines).
- **Effort:** L
- **Builds on:** profiles, `--remote` mode, cluster profiles.

### 3.8 Shared-FS behavior probes in preflight

Probe rename atomicity, close-to-open consistency, and quota headroom on the
cache and runtime paths, recording results into the cluster profile — ESTALE
on HAICORE was reproduced in the field; make its preconditions detectable
before submission.

- **Who:** every Lustre/NFS-backed site.
- **Effort:** M
- **Builds on:** `preflight`, `doctor cluster-report`, the enroot
  node-local-temp mitigation already shipped.

### 3.9 Unified residue reaper

One `clean --deep` that handles today's three separate leftovers — stale
tracked jobs, expired rendezvous records, orphaned per-job enroot runtime dirs
from crashed allocations — with a dry-run report first.

- **Who:** long-lived project directories; admins auditing shared caches.
- **Effort:** S–M
- **Builds on:** `clean`, `cache prune`, `rendezvous prune`.

### 3.10 Walltime-risk warning

At plan time, compare the requested time limit against historical runtimes of
the same spec name: "p90 runtime 3h50m vs. limit 4h — enable signal+resume or
raise time" — the cheapest possible insurance against the most common
late-stage failure.

- **Who:** iterating researchers who never update `time:`.
- **Effort:** S
- **Builds on:** tracked runtime history, plan hints.

---

## Lane 4 — Developer experience

### 4.1 `render --annotate` / `hpc-compose explain`

Render the batch script with interleaved provenance comments (this `#SBATCH`
line ← `x-slurm.mem`; this block ← `readiness.http`), and an
`explain <line|field>` query in both directions — makes the core brand promise
("one inspectable script") tangible and teachable.

- **Who:** new users, reviewers, and AI agents consuming llms.txt.
- **Effort:** S (annotate) → M (query form)
- **Builds on:** renderer, `plan --explain`.

### 4.2 Live-value shell completions

Make completions dynamic: `--partition <TAB>` from the cluster profile,
`--job-id <TAB>` from tracked runs, `--sweep-id <TAB>` from manifests,
`--service <TAB>` from the compose file.

- **Who:** daily CLI users.
- **Effort:** S–M
- **Builds on:** `completions`, `cluster.toml`, tracked state.

### 4.3 `hpc-compose lsp`

A language server wrapping the *real* Rust validator (not just the published
JSON Schema): live semantic diagnostics with HPC lint codes, hover docs
sourced from spec-reference, quick-fixes from `lint --fix`.

- **Who:** VS Code/Neovim spec authors.
- **Effort:** L
- **Builds on:** `schema`, `validate`, `lint --fix`.

### 4.4 Interactive spec builder (`new --interactive`)

A TUI wizard: choose template → answer topology/GPU/readiness questions →
watch the rendered script preview update live → write spec; `evolve` is
currently a doc-guided tutorial, this is its executable form.

- **Who:** first-hour users.
- **Effort:** M–L
- **Builds on:** `new` templates, `examples recommend`, ratatui infra from
  `watch`.

### 4.5 Compose overlays (`-f base.yaml -f site.yaml`)

Docker-compose-style multi-file merge so labs can separate the experiment
(shared, in git) from site bindings (partition, cache_dir, account) — profiles
cover settings but not service-level overrides.

- **Who:** multi-site labs, shared example repos.
- **Effort:** M–L (merge semantics become a contract — design carefully)
- **Builds on:** config resolution, profiles.

### 4.6 Strict `--dry-run`/`--offline` contract

Audit and guarantee that every command's dry-run touches nothing (the HAICORE
e2e found `up --remote --dry-run` still runs prepare), plus a global
`--offline` that forbids SSH/scheduler calls — then document it in the
llms.txt safety contract.

- **Who:** AI-agent setups, cautious first-time cluster users.
- **Effort:** S–M
- **Builds on:** existing static-safety classification in llms.txt.

### 4.7 Local-mode backend parity

Extend `up --local` beyond pyxis/Linux: apptainer backend locally, and a
`test --submit --dev-cluster` path that transparently targets the checked-in
local Slurm container so macOS users get a real sbatch smoke test without
leaving the laptop.

- **Who:** Mac-based developers (including the maintainer).
- **Effort:** L
- **Builds on:** `up --local`, local Slurm dev cluster.

### 4.8 Spec migration assistant (`lint --migrate`)

With breaking changes accepted pre-1.0, ship the counterweight: version-aware
rewriting of old specs to current semantics, with a diff preview — turns
"breaking changes OK" into "breaking changes painless."

- **Who:** early adopters across upgrades.
- **Effort:** M
- **Builds on:** lint --fix rewriting, schema versioning.

### 4.9 Offline doc search (`hpc-compose docs <query>`)

Embed the mdBook content and fuzzy-search it from the CLI — login nodes often
have no browser, and the manual is 20+ substantial pages.

- **Who:** SSH-bound users.
- **Effort:** S–M
- **Builds on:** docs build, llms.txt curation.

### 4.10 `diff --against-spec` (pre-submit what-changed)

Diff the *current* compose file against the spec snapshot of the last tracked
run before submitting — "you changed lr and the image tag since job 12345" —
catching the accidental-variable problem before it costs GPU-hours.

- **Who:** iterating experimenters.
- **Effort:** S
- **Builds on:** `diff`, submit-time spec snapshots.

---

## Top 5 by value ÷ effort

### 1. `sweep submit --resume` (1.1, S)

This is the highest ratio on the board: the failure mode is documented in the
project's own docs as a known limitation ("if a submission fails, later trials
are not submitted and the partial manifest is kept"), the manifest already
contains everything needed to compute the delta, and sweeps are a flagship
feature that this footgun directly undermines. A login-node hiccup at trial 40
of 100 currently means manual manifest surgery or a duplicate sweep; after
this, it means re-running the same command. Why now: sweeps just gained
replicates, objectives, and scaling reports — the feature is attracting
exactly the heavy users who will hit sequential-submit failure first.

### 2. Preemption-contract verification, `test --preemption` (3.1, M)

hpc-compose has quietly built the best preemption story in the Slurm-tooling
space — requeue + signal + resume + attempt tracking unified through
`SLURM_RESTART_COUNT` — but no user can find out whether *their* spec honors
the contract until a real preemption 40 hours into a run. A test mode that
signals, kills, restarts, and asserts resume converts the whole contract from
"trust me" to "verified in 90 seconds," and the local Slurm dev cluster
(already in-tree) makes it CI-able. Why now: v0.2.0 just completed the
resume/requeue unification; verification is the natural capstone, and it
hard-differentiates against every hand-rolled sbatch setup.

### 3. `render --annotate` + `explain` (4.1, S→M)

The project's core pitch is "one generated batch script you can inspect" —
annotation makes inspection self-explanatory, and it compounds everywhere:
onboarding (new users learn Slurm *through* the annotations), debugging (which
spec field caused this flag), docs (examples become self-documenting), and the
AI-agent story (agents can ground answers in provenance rather than guessing).
The annotate form is days of work because the renderer already knows the
mapping; it just doesn't say it. Why now: cheap, on-brand, and it amplifies
the llms.txt/agent-setup investment already made.

### 4. Cluster drift detection, `doctor cluster-report --diff` (3.3, S–M)

Cluster profiles are static snapshots of a moving target — the HAICORE e2e
alone surfaced multiple reality-vs-assumption gaps (denied `sinfo`, missing
`gpu:full` GRES, partition quirks). Every maintenance window silently
invalidates some of the profile, and today users discover it as a confusing
mid-submit failure. Re-probing and diffing is mostly recombining existing
probe code, and wiring a one-line warning into `preflight` puts it exactly
where users already look. Why now: the project is onboarding its second
cluster site; drift detection is what makes the cluster-profile concept
trustworthy at N > 1.

### 5. Fair-share interpreter + walltime-risk warning (2.5 + 3.10, S each)

Bundled because they share a theme — turning data hpc-compose already has into
judgment. `weather` already fetches `sshare`/`sprio` but presents raw numbers
no grad student can act on; tracked history already knows the p90 runtime
that's about to blow through the requested time limit. Both are pure
interpretation layers, both prevent the two most common queue-related miseries
(mysterious pending, death at the time limit), and both make the tool feel
like it has an experienced HPC admin inside it. Why now: they're weekend-sized
wins that convert existing plumbing into daily-felt value, and they warm up
the historical-sacct machinery that queue-ETA (2.1) would later need.

### Runners-up

Just missed the cut: idle-GPU watchdog (2.7) — very timely after the
cgroup-attribution merge but needs careful policy design before it touches
`scancel`; reproducibility bundle (1.3) — high academic value, held back only
because `experiment show` + `pull` already cover 60% of it.

---

## Moonshots (honest about Slurm)

### M1. Heterogeneous jobs — the declared epic, taken seriously

Per-service partition/account/QOS via `#SBATCH hetjob` components and
`srun --het-group`, letting a CPU data-loader service live in the cheap
partition beside a GPU trainer. Slurm genuinely permits this, but honesty: it
reshapes the planner's single-allocation assumption end to end, MPI across het
groups is fragile, and some sites disable hetjobs outright — so it needs a
capability probe in `cluster-report` before anything else.

### M2. Elastic trial-queue sweeps on plain arrays

Instead of one allocation per trial, submit one Slurm *array* of N worker
tasks that pull trial configs from a shared-filesystem queue — a poor-man's
Ray Tune with zero daemons, which also makes ASHA pruning nearly free (a
worker just doesn't pick up a pruned config). Slurm permits everything
required (arrays + shared FS + file locking); the honest cost is trading the
current one-trial-one-job inspectability for throughput, so it should be an
opt-in sweep mode, not a replacement.

### M3. First-to-start multi-cluster racing

Submit the same spec to two clusters, let whichever starts first claim the
run, cancel the loser. Nothing in Slurm forbids it — but the clusters share no
filesystem, so the claim step needs an external rendezvous (an object-store
lock or even a git ref), and the race window means occasionally burning a few
duplicate node-minutes. Builds directly on multi-cluster profiles (3.7);
honest prerequisite: that lands first.

### M4. Hot-swap dev inside a held allocation

Marry `alloc` (live allocation, `run` reuses it) with `dev`'s file-watching
hot-reload and the dual-mode Mac↔login-node source sync: edit on the laptop,
service restarts *inside the standing compute allocation* in seconds. Slurm
permits it cleanly — new `srun` steps into a live allocation is exactly the
mechanism — the hard parts are the sync contract over one-OTP SSH and not
letting the held allocation become the idle-GPU waste that lane 2 fights (the
watchdog, 2.7, is the natural guardrail).

### M5. Power-capped runs (`x-slurm.power`)

Declare a power posture: CPU side via Slurm's real, existing `--cpu-freq` flag
(works today, unprivileged), GPU side via clock capping for the
well-documented ~10% throughput / ~30% energy trade. Honesty:
`nvidia-smi -lgc` generally needs root or site prolog support, so GPU capping
ships as "generate the request for your admin" plus detection of sites that
allow it — the CPU half and the score-integrated energy accounting are real on
day one.

### M6. Queue digital twin

"When would this start if I asked for 2 GPUs instead of 4?" answered by
simulating the scheduler against current queue state. Honest: full Slurm
simulators are research-grade artifacts nobody maintains; the shippable core
is quantile regression over your own sacct history (2.1) with per-geometry
what-if deltas — call it estimation, not simulation, and it's actually
buildable.

## Agreed roadmap (updated 2026-07-05)

Sequence agreed with Nicolas. Each item gets an architecture sketch (grounded
in subsystem recon) before implementation; implementation runs via isolated
worktree agents with adversarial review before PR.

**Done / in flight:**

- 1.1 `sweep submit --resume` — PR #68 open
- 1.4 `experiment tag/note` — in implementation
- 4.1 `render --annotate` + `explain` — in implementation

**Queued, in this order:**

1. 4.10 `diff --against-spec` (pre-submit what-changed)
2. 1.3 `experiment bundle` (paper-ready reproducibility archive)
3. 3.6 State reconciliation (`status --verify`)
4. 3.9 Unified residue reaper
5. 4.2 Live-value shell completions
6. 4.6 Strict `--dry-run`/`--offline` contract
7. 4.9 Offline doc search (`hpc-compose docs <query>`)
8. 1.7 `notebook promote`
9. 2.7 Idle-GPU watchdog
10. 3.1 Preemption-contract verification (`test --preemption`)
11. 3.4 Failure classifier (`debug --classify`)
12. 3.8 Shared-FS behavior probes in preflight

**Second wave (after the 12 above):**

13. 3.2 Resubmission policy (`x-slurm.retry`)
14. 4.3 `hpc-compose lsp`
15. 4.7 Local-mode backend parity

**Additional items:**

- **Workspace lifecycle automation** — strengthen support for HPC workspace
  creation and management (the `ws_allocate`/`ws_find`/`ws_extend`/`ws_release`
  family on KIT-style clusters) so users never interact with those tools
  directly: auto-allocate/resolve a workspace for cache and job data, track
  expiry, extend or warn before data loss, and release on teardown. New idea
  from Nicolas 2026-07-05; interpretation confirmed. Sketch to be produced
  alongside 4.10; Nicolas slots it afterwards (touches cache_dir resolution,
  setup, preflight, and remote mode).
- **LLM usability overhaul (closes the roadmap)** — after everything above,
  re-investigate, redesign, and sketch the agent-facing surface: the
  `skills/hpc-compose/SKILL.md` bundle, `docs/src/llms.txt` entry map, and
  the AI-agent-setup walkthrough. Target outcome: a user can task any LLM
  agent with "prepare my repo for this cluster" and the agent reliably knows
  where to look things up, what is static-safe vs. what submits jobs, and how
  to author + verify a spec end to end. By then the CLI surface will include
  everything above, so this is deliberately last — the redesign documents the
  final shape rather than chasing a moving target.
