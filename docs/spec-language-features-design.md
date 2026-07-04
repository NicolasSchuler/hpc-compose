# Spec-language feature menu — prioritized design (planning pass, F9)

Status: **design only, nothing implemented.** Scopes six additive spec-language
features against the code as it stands at `main` (v0.1.45-era). Every proposal is
additive: no existing spec changes meaning, and no first-class field is removed.

Anchors are `file:line` at the time of writing; treat them as "the site that does
X", not as frozen line numbers.

---

## 0. TL;DR — ranking and verdicts

Ranked by **user value ÷ effort**, best first:

| Rank | Feature | Effort | Value | One-line verdict |
|---|---|---|---|---|
| 1 | **#2 reservation + licenses** | S | Med | Pure job-level directives; clones the exact `partition`/`qos` pattern. Zero-risk win. |
| 2 | **#4 richer mail events** | S | Med | Extend one enum + one match; the only subtlety is normalizer ordering. |
| 3 | **#6 `${VAR:?err}`** | S–M | Med | Additive (today it's a hard error). Cost = **three** grammar walkers must stay in sync. |
| 4 | **#5 `env_file:`** | M | High | Headline compose-compat gap. Real decisions: interpolation, redaction, submit-host file I/O. |
| 5 | **#3 requeue + `--signal`** | M | Med–High | Genuine integration with the supervisor trap, `resume`, and `failure_policy`. |
| 6 | **#1 per-service partition/qos/account** | — | — | **Not expressible** under one-allocation model. See §Feature 1: reject+document now, hetjob is the real (L/XL) answer. |

**Recommended build order:** a small additive wave **#2 → #4 → #6** (each S, each
lands independently), then **#5** (high value, absorb the design cost), then **#3**
(touches the runtime trap), and finally **#1** as an S "reject-and-point-to-hetjob"
guard while hetjob itself is deferred to its own epic.

---

## 1. Shared implementation surface (read this once)

Adding a first-class **top-level `x-slurm` scalar** is not a one-file change. The
codebase deliberately spreads each field across ~11 sites, several of which are
**test-gated** so you cannot forget them. This checklist is the backbone of every
"effort" estimate below.

| # | Site | Anchor | Why |
|---|---|---|---|
| 1 | `SlurmConfig` struct field | `src/spec/mod.rs:512` | The raw parsed field (`#[serde(deny_unknown_fields)]`). |
| 2 | `EffectiveSlurmConfig` mirror field | `src/spec/mod.rs:1646` | Stable, interpolated `config` surface persisted for resume-diff. |
| 3 | Effective-config builder | `src/spec/mod.rs:~2425` | Copies raw → effective (`submit_args: self.slurm.submit_args.clone()`). |
| 4 | `SlurmConfig::validate()` | `src/spec/mod.rs:2817` | Line-safety / positivity / semantic guards. |
| 5 | `SlurmConfig::interpolate()` | `src/spec/mod.rs:2942` | `${VAR}` expansion for string fields. |
| 6 | Renderer directive emission | `src/render.rs:370–465` | Emits `#SBATCH --flag=value` **in order**; snapshot-tested. |
| 7 | Submit-arg conflict guard | `src/spec/mod.rs:4262` (`FIRST_CLASS_TOP_LEVEL_SLURM_FLAGS`) + `4362` (`top_level_slurm_field_is_set`) | Stops setting a field *and* passing the raw flag in `submit_args`. |
| 8 | JSON schema | `schema/hpc-compose.schema.json:369` (`/definitions/slurm/properties`) | **Gated** by `schema_nested_definitions_match_spec_struct_fields` (`tests/release_metadata.rs:337`). |
| 9 | Resume-diff field list | `src/job/diff.rs:95` | So a changed value is surfaced on `resume`, not silently ignored. |
| 10 | Spec-reference docs | `docs/src/spec-reference.md` `## x-slurm` (line 224) | User-facing reference. |
| 11 | Gated example + tests | `examples/*.yaml`, `src/render/tests.rs`, `src/spec/tests.rs` | At least one gated example render + validate/interpolate unit tests. |

**The schema-parity gate is your friend and your tax.** `tests/release_metadata.rs`
recovers each struct's serde field set at runtime (via a `deny_unknown_fields`
sentinel probe) and asserts it equals the schema definition's `properties`. So:

- Add a field to `SlurmConfig` and forget the schema → **red test**, with the exact
  missing key. You physically cannot merge schema drift for the gated structs
  (`slurm`, `serviceSlurm`, `notify`, `emailNotify`, `failurePolicy`, …).
- New **structs** you introduce (e.g. a `signal` block) should be added to that
  test's `assert_definition_matches_struct!` list so they inherit the same gate.

Per-**service** fields have a parallel surface: `ServiceSlurmConfig`
(`src/spec/mod.rs:1225`), the srun builder `build_srun_command_for_backend_*`
(`src/render/command.rs:52`), and `FIRST_CLASS_SERVICE_SLURM_FLAGS` /
`service_slurm_field_is_set` (`src/spec/mod.rs:4310`, `4396`).

---

## Feature 2 — `x-slurm.reservation` and `x-slurm.licenses` (RANK 1)

### User story
> "Ops handed me a reservation `maint_2026` for tonight's deadline run, and my job
> needs two `ansys` licenses. Today I have to hand-write `submit_args: ["--reservation=maint_2026", "--licenses=ansys:2"]`
> and lose validation, redaction, and resume-diff on those values."

### Proposed YAML
```yaml
x-slurm:
  reservation: maint_2026
  licenses: "ansys:2,comsol:1"
```
Both are plain strings (Slurm's own `--licenses` grammar is `name[:count][,name[:count]…]`;
we pass it through rather than model it).

### Validation rules
- `validate_sbatch_safe_string` on both (no newlines/NULs) — identical to `partition`.
- No emptiness or semantic parsing beyond line-safety (mirrors `submit_args`
  philosophy: we validate transport safety, Slurm validates semantics).
- Add `("reservation", "--reservation")`, `("licenses", "--licenses")`, `("licenses", "-L")`
  to `FIRST_CLASS_TOP_LEVEL_SLURM_FLAGS` so `submit_args` can't double-set them.

### Render / sbatch mapping
Two lines in the header builder next to `partition` (`src/render.rs:378`):
```rust
if let Some(reservation) = &plan.slurm.reservation {
    sbatch::push_directive(&mut out, "reservation", reservation);
}
if let Some(licenses) = &plan.slurm.licenses {
    sbatch::push_directive(&mut out, "licenses", licenses);
}
```
→ `#SBATCH --reservation=maint_2026` / `#SBATCH --licenses=ansys:2,comsol:1`.
No `srun` interaction (both are allocation-level).

### Surface: schema + docs + example + tests
- Schema: two `lineSafeString` props under `/definitions/slurm/properties`.
- Docs: a short subsection under `### x-slurm.submit_args` cross-linking "prefer
  first-class fields over raw `submit_args`".
- Example: extend an existing example (e.g. `examples/minimal-batch.yaml`) or add
  `examples/reservation-licenses.yaml`.
- Tests: render snapshot with both set; `validate` rejects a newline; conflict test
  for `reservation` + raw `--reservation`. The schema-parity test auto-covers the
  new struct fields.

### Effort: **S.** Risks: **near-zero** — mechanical clone of a proven pattern.

---

## Feature 4 — richer mail events (RANK 2)

### User story
> "My 48-hour training run should email me at `TIME_LIMIT_90` so I can extend or
> checkpoint before it's killed, and once per array task on a sweep."

### Current state
`NotifyEvent` (`src/spec/mod.rs:1045`) = `{ start, end, fail, all }` →
`notify_event_mail_type` (`4230`) maps to `BEGIN/END/FAIL/ALL`;
`normalize_notify_events` (`4213`) dedups to `[End, Fail]` by default and collapses
to `[All]` when `all` is present. Rendered as one `#SBATCH --mail-type=…`
(`src/render.rs:390`).

### Proposed YAML
```yaml
x-slurm:
  notify:
    email:
      to: me@example.org
      on: [end, fail, time_limit_90, requeue, array_tasks]
```
New enum variants (Slurm's documented `--mail-type` tokens):
`time_limit`, `time_limit_90`, `time_limit_80`, `time_limit_50`, `requeue`,
`invalid_depend`, `stage_out`, `array_tasks`.

### Validation rules
- Serde `rename_all = "snake_case"` handles the spelling; unknown tokens already
  fail with the schema enum + serde error.
- **`array_tasks` is a modifier, not a standalone trigger** — Slurm applies it per
  array task and it's only meaningful with `x-slurm.array`. Add a soft validation:
  warn (or hard-error, pick one — recommend **hard error via `SpecError`**) if
  `array_tasks` is set without `array`. This is the one real design call here.
- `all` still short-circuits: keep the "if `All` present → `[All]`" collapse.

### Render / sbatch mapping
- Extend `notify_event_mail_type` with the new `SCREAMING_SNAKE` tokens
  (`TIME_LIMIT_90`, `ARRAY_TASKS`, `REQUEUE`, …).
- **Fix `normalize_notify_events` ordering:** today it hard-codes iteration over
  `[Start, End, Fail]`. Rewrite it to preserve a *stable canonical order over the
  full variant list* while de-duping, so `--mail-type` output stays deterministic
  (snapshot tests depend on order). This is the only non-trivial code.

### Surface
- Schema: extend the `on` enum in **both** `/definitions/emailNotify` (line 674)
  — note the per-service copy at schema line ~902 is a *different* `on` (hook events);
  don't cross-wire them.
- Docs: table of events under `### x-slurm.notify` (line 388).
- Example: extend `examples/notify-mail.yaml`.
- Tests: `notify_mail_type_value` for a multi-event list (order!); the
  `array_tasks`-without-`array` guard; schema enum round-trip.

### Effort: **S.** Risks: **low** — normalizer ordering is the only trap; the
`array_tasks` semantics decision is a 10-minute call.

---

## Feature 6 — `${VAR:?error-message}` required-variable interpolation (RANK 3)

### User story
> "If `HF_TOKEN` isn't set, I want the run to fail at `validate` time with a message
> I wrote — not to submit a job that dies 20 minutes in with an opaque 401."

### Current state (verified)
`interpolate.rs` supports `${VAR}`, `${VAR:-d}`, `${VAR-d}`, `$$`. `${FOO?…}` is an
**explicit error today** — `resolve_braced_variable` falls through to
`_ => bail!("invalid variable expression …")` (`src/spec/interpolate.rs:452`), and
the existing test `interpolate_string_covers_required_defaults_escapes_and_errors`
**asserts `${FOO?bad}` is rejected** (`interpolate.rs:545`). Because no valid spec
can use `?` today, adding it is **purely additive** (that one test assertion flips).

### Proposed semantics (compose/bash parity)
- `${VAR:?msg}` → error if `VAR` is unset **or empty**; message is `msg`.
- `${VAR?msg}` → error if `VAR` is unset (empty is allowed through).
- Empty message allowed (`${VAR:?}` → generic "is required").
- `msg` itself is interpolated (matches how `${VAR:-$OTHER}` recurses).

### The catch: **three** grammar walkers must agree
The `:?`/`?` suffix has to be handled identically in all three, or scanners and the
real interpolator diverge:
1. `resolve_braced_variable` (`interpolate.rs:422`) — the actual substitution.
2. `collect_referenced_from_braced_expr` (`interpolate.rs:124`) — feeds
   `referenced_variables` (used for env/`.env` diagnostics). `${VAR:?}` **references**
   `VAR`.
3. `collect_missing_from_braced_expr` (`interpolate.rs:216`) — feeds
   `missing_defaulted_variables`. A required var is **not** "defaulted"; decide
   whether it appears here (recommend: **no** — it's a hard error, not a silent
   default, so it shouldn't be reported as "consumed a default").

### Validation / error surface (the miette ask)
Interpolation errors are currently plain `anyhow::bail!` strings, **not** `SpecError`
miette diagnostics. To satisfy "validation with miette help", add a
`SpecError::RequiredVariableUnset { name, message }` variant with a `#[help]` that
echoes the user's message and suggests `export VAR=…` or a `.env` entry. This is the
only reason this is S–**M** rather than pure S.

### Surface
- Docs: extend `## environment` interpolation grammar (`spec-reference.md:778`) and
  the `${VAR:-default}` table.
- Example: a gated example that references `${DEPLOY_ENV:?set DEPLOY_ENV to staging|prod}`.
- Tests: unit tests for set/unset/empty × `:?`/`?`; **flip** the existing
  "should be rejected" assertion; a scanner test proving `referenced_variables`
  still sees `VAR`.

### Effort: **S–M.** Risks: **medium-low** — the failure mode is a scanner/
interpolator divergence, caught by targeted tests. Keep the three walkers edited in
one commit.

---

## Feature 5 — `env_file:` per service (RANK 4)

### User story
> "My `docker-compose.yml` has `env_file: .env.prod` on every service. Porting to
> hpc-compose I have to inline all 30 vars into `environment:`."

### Proposed YAML
```yaml
services:
  trainer:
    image: myrepo/trainer:1.2
    env_file: config/train.env          # string form
    # or:
    env_file: [config/base.env, config/train.env]   # list form, later wins
    environment:
      RUN_ID: ${RUN_ID}                  # inline `environment:` wins over env_file
```

### Semantics & precedence (compose-compatible)
- Files are read **relative to the compose file's directory** (same base as the
  existing `.env` loader, `interpolate.rs:11`), on the **submitting host**.
- Merge order (lowest→highest): `env_file` (in list order) **<** inline
  `environment:`. Matches docker-compose.
- **Reuse `load_dotenv_vars`** (`interpolate.rs:259`) as the parser — it already
  handles `export `, quotes, comments, and `KEY=VALUE` errors. Refactor it to accept
  an explicit path instead of only `<dir>/.env`.

### Three real design decisions (call these out in review)
1. **Interpolation of env_file values.** docker-compose treats env_file contents as
   **literal** (no `${…}` expansion), and hpc-compose's own `.env` loader is literal
   too. **Recommend: literal.** It's compose-compatible and avoids surprising
   secret-expansion inside a file the user thinks of as inert. (Inline `environment:`
   stays interpolated, unchanged.)
2. **Redaction.** env_file entries become service `environment` pairs, so the
   **name heuristic already redacts** `*_TOKEN`/`*_KEY`/… keys in `config`/`context`
   (`src/redaction.rs:35`). The prompt's requirement — "env_file values must be
   redaction-eligible" — is only *fully* met if a benign-keyed value that equals a
   sensitive env_file value is also hidden. That needs the env_file values fed into
   the value-equality set (`secret_value_set`, `redaction.rs:238`), guarded by the
   existing `MIN_SUBSTRING_REDACTION_LEN` (`redaction.rs:377`) to avoid over-redacting
   short values. **Recommend: yes, register env_file values for value-equality
   redaction** (they're config a user chose to externalize, i.e. secret-ish), but do
   **not** treat them as `ValueSource::Secret` for the name-independent path — keep
   `secrets:` as the only structural-secret source.
3. **File I/O in `validate`/`config`.** These commands already read the compose file
   and `.env`, so reading `env_file` is consistent — but it adds a new failure mode:
   **missing file** and **malformed line**. Surface both as `SpecError` (miette),
   e.g. `SpecError::EnvFileNotFound { service, path }`. **Dual-mode note:** the file
   must exist on the *submit host* (laptop), like `.env`; document that it is not
   staged to the compute node.

### Render / plan mapping
No sbatch/srun flags. env_file resolves entirely into `ServiceSpec.environment`
before the planner runs `service.environment.to_pairs()` (`src/planner.rs:225`).
Cleanest insertion: resolve+merge during spec load/interpolate so the rest of the
pipeline (planner, redaction, `config`) sees a single merged `environment`.

### Surface
- New `ServiceSpec.env_file: EnvFileSpec` (untagged `String | Vec<String>`, like
  `CommandSpec`); add `"env_file"` to `SERVICE_ALLOWED_KEYS` (`validation.rs:17`) —
  **gated** by `schema_allowed_keys_match_spec_validation_allowlists`
  (`tests/release_metadata.rs:228`), so schema + allowlist must move together.
- Schema: `env_file` on `/definitions/service` as `string | array<string>`.
- Docs: `docs/src/docker-compose-migration.md` (compat win) + `## environment`.
- Example: `examples/env-file.yaml` + a committed `*.env` fixture.
- Tests: merge precedence; missing-file error; malformed-line error; redaction of a
  sensitive-keyed env_file var; literal-not-interpolated assertion.

### Effort: **M.** Risks: **medium** — the redaction and interpolation decisions have
a security dimension; get them reviewed explicitly rather than defaulted.

---

## Feature 3 — requeue / preemption policy + `--signal` (RANK 5)

### User story
> "My run is on a preemptible partition. I want Slurm to requeue it on preemption,
> and to send `USR1` 60s before `SIGTERM` so my trainer checkpoints and the
> hpc-compose `resume` path picks up cleanly."

### Proposed YAML
```yaml
x-slurm:
  requeue: true          # → #SBATCH --requeue   (false → --no-requeue)
  signal:                # structured; renders --signal=[R:]<sig>[@<sec>]
    name: USR1
    at_seconds: 60
    shell: batch         # batch => B: prefix (signal the batch shell, not the step)
    require_prolog: false # R: prefix
```

### Validation rules
- `requeue: bool`. Emit `--requeue` for `true`, `--no-requeue` for `false`; omit the
  directive when unset (inherit site default).
- `signal.name`: accept `USR1`/`USR2`/`TERM`/`INT`/… or a positive int; validate
  against a known set (Slurm accepts names or numbers).
- `signal.at_seconds`: `u64`, positivity via `validate_positive_*`. Slurm caps the
  lead time at 65535s — validate the ceiling and give a helpful message.
- Conflict-table: `("requeue", "--requeue")`, `("requeue", "--no-requeue")`,
  `("signal", "--signal")` into `FIRST_CLASS_TOP_LEVEL_SLURM_FLAGS`.

### Render / sbatch mapping
- `--requeue` / `--no-requeue` (bool → directive).
- `--signal=[R:][B:]<name>@<seconds>` assembled from the block.

### The integration surface (why this is M, not S)
- **Supervisor trap.** The generated batch script already tracks
  `RECEIVED_SIGNAL=""` and installs signal handling (`src/render.rs:477`). A
  `--signal=B:USR1@60` delivers `USR1` to the **batch shell**, so the trap must
  actually catch `USR1` and drive graceful teardown / stage-out. Design the trap and
  the `signal.shell: batch` default together, or the flag is cosmetic.
- **`resume` interplay.** Requeue restarts the *same job id*; the existing
  `x-slurm.resume` path and `HPC_COMPOSE_IS_RESUME`/`HPC_COMPOSE_ATTEMPT` env
  (`src/render/command.rs:87–89`) must behave sanely across a requeue, not just
  across hpc-compose's own restart loop.
- **`failure_policy` is orthogonal — document it loudly.** `failure_policy`
  (`src/spec/mod.rs:1578`) does *in-job, in-allocation* service restarts. `requeue`
  is *scheduler-level, whole-job* re-dispatch after preemption/node-failure. They
  compose (a requeued job re-runs its failure_policy from scratch). The docs must
  draw this two-layer picture or users will set both and be confused.

### Surface
- New `SignalConfig` struct → add to the schema-parity test list; `requeue: Option<bool>`
  on `SlurmConfig`/`EffectiveSlurmConfig`.
- Docs: new `### x-slurm.requeue` + `### x-slurm.signal` under `## x-slurm`, with the
  requeue-vs-failure_policy comparison box.
- Example: `examples/preemptible-checkpoint.yaml` pairing `requeue` + `signal` +
  `resume`.
- Tests: render snapshots for `--requeue`/`--no-requeue`/`--signal`; the trap catches
  `USR1` (batch-script assertion); ceiling validation.

### Effort: **M.** Risks: **medium** — the trap/resume wiring is where correctness
lives; the flags alone are easy but hollow without it.

---

## Feature 1 — per-service `partition` / `qos` / `account` (RANK 6 — reframed)

### The blunt finding: **Slurm forbids the naive version.**
hpc-compose's execution model is **one `sbatch` allocation; each service is an
`srun` step inside it** (`build_srun_command_for_backend_*`, `src/render/command.rs:52`).
Under that model:

- **Partition:** a job is allocated from **one** partition; its nodes belong to that
  partition. `srun --partition` for a step can only ever select a **subset of the
  allocation's** nodes — you cannot make service A land in partition `gpu` and
  service B in partition `cpu` within a single allocation. Confirmed by the renderer:
  `command.rs` emits **no** `--partition` today, and adding one would be a footgun
  that silently does nothing useful (or errors at `srun`).
- **account / qos:** these are **job-level accounting attributes** fixed at
  submission. Steps inherit them; there is no meaningful per-step reassignment. A
  per-service override would be a field that looks like it works and doesn't.

So a per-service `partition`/`qos`/`account` **override is not implementable
honestly** in the current model. Shipping it would violate the "no silently
misleading fields" bar.

### What to actually do

**Now (S, ship it):** add a **validation guard** that produces a good miette error if
someone puts `partition`/`qos`/`account` under a service's `x-slurm`, pointing at the
real answer:
> `service 'worker' sets x-slurm.partition, but a single hpc-compose allocation runs
> in one partition. Set x-slurm.partition at the top level, or use a heterogeneous
> job (see Roadmap: hetjob) to route components to different partitions.`

This is pure value: it turns a confusing "unknown field" (or worse, a silent no-op)
into a teaching error.

**Later (L/XL epic — the real feature): heterogeneous jobs (hetjob).** Slurm's
**only** mechanism for "different partitions/accounts/resources per component in one
submission" is a hetjob: `#SBATCH hetjob`-separated components, launched with
`srun --het-group=…`. Each component gets its own partition/account/qos/nodes/gres.

Why it's a separate epic, not a field:
- The whole planner assumes a **single allocation** (`plan.slurm.allocation_nodes()`,
  single node set, single placement space). Hetjob means N allocations with N node
  sets and cross-component rendezvous.
- The renderer's single `#SBATCH` header (`src/render.rs:370`) becomes N stanzas
  separated by `#SBATCH hetjob`.
- Placement (`ServicePlacementSpec`), MPI hostfiles, metrics, and rendezvous all
  currently reason over one allocation and would need het-group awareness.

**Proposed YAML for the eventual epic** (so the door stays open):
```yaml
x-slurm:
  components:                     # NEW top-level: opt-in hetjob mode
    - name: gpu
      partition: gpu
      account: proj-gpu
      gpus_per_node: 8
    - name: cpu
      partition: cpu
      account: proj-cpu
services:
  trainer:  { x-slurm: { component: gpu } }   # bind service → het component
  loader:   { x-slurm: { component: cpu } }
```
Backward-compat: absent `components` ⇒ today's single-allocation behavior, byte-identical.

### Effort: guard = **S**; hetjob = **L/XL** (own design doc + phased rollout).
### Risks: shipping the naive override would be a **correctness/trust regression** —
don't. The guard is safe; the epic needs its own planning pass.

---

## Appendix — sequencing rationale

- **#2, #4, #6 first (all S):** each is self-contained, each rides an existing gate,
  and together they close three compose-parity / ergonomics gaps for ~one feature's
  worth of effort. Ship them in that order (least to most subtle).
- **#5 next:** highest raw user value (docker-compose migration), but wants the three
  design decisions settled in review — do it when there's bandwidth for that
  conversation, not interleaved with mechanical work.
- **#3 after #5:** it's the first feature that reaches into the *runtime* batch script
  (the trap), so land it when you can test the supervisor path end-to-end (dev-cluster
  or HAICORE), not just render snapshots.
- **#1 guard anytime; hetjob never as part of this menu** — spin it out.

### Cross-cutting reminders
- Every new struct → add to `schema_nested_definitions_match_spec_struct_fields`.
- Every new persisted `x-slurm` value → add to `src/job/diff.rs` field list or
  `resume` won't notice it changed.
- Every new first-class flag → add its spellings to the relevant `*_SLURM_FLAGS`
  conflict table, or `submit_args` can double-set it.
- Prefer `SpecError` miette variants over bare `bail!` for anything user-facing
  (Features 5, 6, and the #1 guard specifically ask for help text).
