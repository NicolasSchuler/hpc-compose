# Design — Dual-Mode Operation + Content-Addressed Source Sync

> **Status: exploratory engineering design, not a public commitment.**
> Drafted 2026-06-21. Internal planning artifact for contributors — it is **not**
> part of the published manual. This design **extends**
> [`docs/implementation-plan.md`](implementation-plan.md) and inherits its
> guardrails: no long-running control-plane/daemon/proxy; commands stay one-shot;
> state lives in tracked files under `.hpc-compose/`; the static-safe vs.
> approval-gated safety contract is preserved. Cited symbols are hints — verify by
> name, as lines drift.

## The problem

hpc-compose is a **two-machine tool**: authoring on the Mac (`platform.rs` is
authoring-only) and submission on the Linux login node (preflight-enforced). Today
the **human is the integration layer** between them — SSHing in, getting edits
across, remembering which of `status`/`logs`/`ps`/`watch`/`stats`/`show` to run,
and hand-building `pull`/`reach`. The two recurring pains:

1. *"remember a lot of commands"*, and
2. *"sync code manually"*.

Both are symptoms of that single split.

## Decisions (agreed with the maintainer)

- **Support both operation modes.**
  - **Mode L — run on the login node** (today's model): submit + inspect locally.
  - **Mode M — laptop thin client**: the Mac is the single touchpoint; it ships
    source and delegates the actual submit to the login-node binary, one-shot,
    over a shared SSH connection.
- **Source sync via content-addressed snapshots** (extend the d8a9e9c CAS).

## The factoring that makes "both modes" cheap

There is **one canonical implementation — the login-node binary** — and exactly
**one mode-dependent step** in the submit path:

> `stage_source(working_tree) → content_hash`

Everything downstream of "here is the source hash" (submit, provenance, state,
logs, the inspect surface) is identical across modes. So the laptop client is
never a second implementation; it is **pre-stage + delegate** to the same
login-node `up`.

That collapses the work to two small seams:

- **stage_source** — Mode L materializes the working tree into the *local* CAS;
  Mode M delta-transfers the same snapshot to the *cluster* CAS over the shared
  SSH master. Both return a content hash naming an immutable snapshot.
- **executor** — Mode L runs `sbatch`/`squeue` directly (today); Mode M runs
  `ssh login 'hpc-compose up --source-hash <h> --no-restage'` and streams back.

Mode is **auto-detected** (a configured remote context + "I am not that host" →
Mode M); there is no user-facing mode flag. (No runtime host detection exists
today — `platform.rs` is compile-time `is_macos()`; a hostname match against the
resolved `login_host` is the small addition.)

### Why content-addressing (not the dataset CAS's spec key)

The dataset/model store keys an entry by an immutable upstream `(uri, revision)`
to avoid re-reading large trees. Source has **no upstream pin**: a *dirty* tree at
a given git SHA differs from a clean one, and two dirty states at the same SHA
differ from each other. So a source snapshot is keyed by the **content hash** of
the enumerated file set. Identical content (anywhere on disk) dedups to one entry,
and any change to a path, bytes, or exec bit yields a new hash — which also makes a
laptop and a login node dedup to the same key for free.

### Tension with "no agent on the cluster"

`implementation-plan.md`'s north star is *"drivable from a laptop with no agent
installed on the cluster."* Mode M's thin client delegates one-shot to the
login-node binary, so it **does** require the binary there — as the product already
does for submit today. It preserves the deeper guardrail (*no long-running
control-plane/daemon/proxy*): delegation is a one-shot `ssh login 'hpc-compose …'`
over an SSH master, not a daemon. The "no agent" framing continues to hold for the
connectivity helpers (`reach`/`pull` emit or run commands and install nothing).

## OTP / SSH contract (Mode M only)

In Mode L nothing authenticates over SSH. OTP only bites in Mode M + the
`pull`/`reach` bridges, so the entire strategy concentrates in the connection
layer.

- **One `hpc-compose login` per session** is the single human-in-the-loop auth: it
  prompts the one OTP and opens the ControlMaster. After that, *every* command —
  including an agent driving `up` repeatedly — rides the warm master
  non-interactively.
- **Never hide an OTP prompt inside a command.** Gate every Mode-M command on
  `ssh -O check`; a cold master with no TTY → **fail fast** with "run
  `hpc-compose login`", never fall through to a prompt (a hidden prompt makes an
  agent hang forever).
- **Pre-establish the master serially** (`ensure_master()`) before any SSH
  fan-out, or `ControlMaster=auto` races and each parallel `ssh` prompts its own
  OTP.
- **One canonical `ControlPath`**, used by every path (sync rsync, exec ssh,
  `pull`, `reach`). A different host spelling/port = a second master = a second
  OTP — the subtle review bug. One place builds all ssh args.
- `reach` becomes `ssh -O forward` on the master (no OTP, no blocking `-N`
  process; drop with `ssh -O cancel`); `pull` rsyncs over the master; `logout` =
  `ssh -O exit`.
- **`ControlPersist` tension:** long enough for a work session vs. not undermining
  the mandated 2FA → configurable (session-length default), explicit `logout`,
  optional heartbeat while actively watching. Today
  `CONTROL_MASTER_SSH_OPTS` (`commands/runtime/ssh_hint.rs`) is
  `ControlPersist=10m`.

## Phasing (Mode L never breaks)

0. **Source CAS snapshot + `source_content_hash`** — pure login-node, no SSH.
   Buys reproducibility, concurrency-safety, recoverable dirty trees. Foundation
   both modes share.
1. **Command surface** — parameterized next-step hints (real job ids) + a `top`
   dashboard folding `status`/`logs`/`ps`/`stats`/`watch`. Independent, low-risk.
2. **Remote context + shared connection** = the OTP phase: canonical identity,
   `ensure_master()`, `login`/`logout`, `ssh -O check` gating, fail-fast-when-cold,
   configurable `ControlPersist`.
3. **Laptop thin client** — `RemoteSourceProvider` + delegating executor + auto
   mode-detect + a version handshake (`--protocol-version`; the wire contract is
   the `--source-hash` flags).
4. **`pull`/`reach` auto-offered + executed** over the master.

## Out of scope (state in docs)

Continuous two-way file sync (mutagen-style); driving Slurm with **no**
hpc-compose on the login node.

## What's landed so far (Phase 0a)

A behavior-neutral, fully-tested foundation — the load-bearing primitive both
modes reuse — without yet changing how a job runs:

- **`src/cache/source.rs`** — `stage_source(root, cache_dir) → SourceSnapshot`:
  enumerates the working tree (`git ls-files -z --cached --others
  --exclude-standard`, falling back to a `.git`-skipping walk), computes a
  deterministic content hash (path + bytes + exec-bit + symlink-aware), and stages
  it into `cache_dir/source/<key>` by reusing `ensure_staged_input`. Identical
  content reuses; dirty/clean states are captured distinctly and remain
  retrievable.
- **`StagedInputKind::Source` + `CacheEntryKind::Source`** — wired through the CAS
  discovery/label paths so `cache list`/`prune` track source snapshots.
- **`JobProvenance::source_content_hash: Option<String>`** + a
  `with_source_content_hash` builder; additive (`serde(default,
  skip_serializing_if)`), so existing records round-trip byte-identically. `diff`
  surfaces the delta. `collect_provenance` stays static-safe and sets `None`.

### Deliberately deferred (need maintainer review)

- **`.hpcignore` — LANDED.** A gitignore/dockerignore-style matcher (`*`/`**`/`?`
  globs, `!` negation, dir-only, root/basename anchoring) at the snapshot root
  filters `enumerate_source` on top of `.gitignore`; an absent file is a no-op.
- **Wiring `stage_source` into the gated submit path — LANDED (record-only).**
  `collect_submit_provenance` now stages via `attach_submit_source_snapshot`, gated
  on `provenance.git.is_some()` (so it is dormant in non-git test tempdirs — zero
  churn — and active in real git repos), best-effort (a staging failure never
  blocks a submit). This pins `source_content_hash` and stores the snapshot, giving
  reproducibility + recoverable dirty trees. **Remaining (Phase 0c):** redirect the
  job's bind mounts at the snapshot so it *runs* against it (concurrency safety),
  plus the `source: live` opt-out + schema — the job-execution change, needs review.

## Phase 1a — next-step hints in human output (landed)

Wave-1 feature #4 already builds `next_commands`/`endpoints`, but only into JSON
behind `--print-endpoints`. This surfaces them for humans:

- `output::print_next_steps` renders a `Next:` block; `submit_next_commands`
  (simplified — dropped the unused backend arg) now also suggests `pull` before
  the destructive `down`, so results are collected before teardown.
- `up`'s summary box (`print_submit_summary_box`, covering all submit paths) and
  `status`'s text output now print parameterized hints (`--job-id` filled in).
- `inspect_next_commands` powers `status` (omits `status` itself). Unit-tested,
  plus an integration test asserts the rendered `up` hint carries the job id.

**Deferred:** hints on `logs`/`ps`/`stats` (targeted data views run in polling
loops — a footer reads as noise; a maintainer UX call), and the `top` single-shot
dashboard (new command → manpage/completions/cli-reference surface; overlaps
`experiment show`).
