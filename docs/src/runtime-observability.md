# Monitor a Run

Status, watch, logs.

After a real submission, `hpc-compose` writes per-job runtime artifacts under:

```text
<runtime-root>/<job-id>/
```

`<runtime-root>` defaults to `<submit-dir>/.hpc-compose` and can be overridden with `x-slurm.runtime_root`. The tracked submission record lives next to the compose file under `.hpc-compose/jobs/<job-id>.json`, and together those paths let follow-up commands reconnect without resubmitting.

## Common Commands

```bash
hpc-compose status -f compose.yaml
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
hpc-compose watch -f compose.yaml --hold-on-exit always
hpc-compose replay -f compose.yaml --speed 10
hpc-compose watch -f compose.yaml --watch-mode line
hpc-compose logs -f compose.yaml --follow
hpc-compose logs -f compose.yaml --grep 'error|oom' --since 30m
hpc-compose stats -f compose.yaml
hpc-compose stats -f compose.yaml --accounting
hpc-compose inspect -f compose.yaml --rightsize
hpc-compose score 12345
hpc-compose germinate -f compose.yaml
hpc-compose sweep status -f compose.yaml
hpc-compose sweep list -f compose.yaml
hpc-compose diff 12345 12346 -f compose.yaml
```

| Command | Use it for |
| --- | --- |
| `status` | Scheduler state, batch log path, runtime paths, and failure-policy state. |
| `ps` | Stable per-service snapshot with readiness, status, restart counters, and log path. |
| `watch` | Live terminal UI; falls back to line-oriented output on non-interactive terminals. |
| `replay` | Best-effort DVR for a tracked run, reconstructed from existing runtime artifacts. |
| `logs` | Text log output, optionally focused, searched, or coarsely time-filtered. |
| `stats` | Tracked metrics, Slurm step statistics, and optional accounting rollups. |
| `inspect --rightsize` | Post-run request-versus-usage recommendations for memory, CPUs, GPUs, and walltime. |
| `score` | 0-100 post-run efficiency score with GPU, memory, compute-time, and kWh components. |
| `germinate` | Short canary submission; see [Right-Size With Canary Runs](canary-runs.md). |
| `sweep status` / `sweep list` | Inspect sweep trials and manifests; see [Hyperparameter Sweeps](sweeps.md). |
| `diff` | Compact comparison between two tracked submissions. |

Use `--format json` on non-streaming commands when automation needs stable fields. `stats` also supports `--format csv` and `--format jsonl`.

## Watch UI

On an interactive terminal, `watch` and the default `up` follow mode open a live view with service state on the left and log output on the right. The UI automatically switches to a compact single-column view on narrow or short terminals. It keeps a detailed status view while the job runs and, by default, holds the final screen on failures so the failing service, final scheduler state, and next diagnostic commands stay visible.

Keybindings:

| Key | Action |
| --- | --- |
| `j`, `Down`, `Tab` | Move to the next service. |
| `k`, `Up` | Move to the previous service. |
| `g` / `G` | Jump to the first or last service. |
| `/` | Filter services by name; press `Enter` to apply or `Esc` to cancel. |
| `f` | Find within log content; matches are highlighted and counted in the log header. |
| `Space` | Pause or resume log following. |
| `PgUp` / `PgDn` | Scroll the visible log pane while paused. |
| `End` | Return to live-follow mode at the newest log lines. |
| `a` | Toggle between the selected service log and all tracked service logs. |
| `w` | Toggle wrapping of long log lines (otherwise they are truncated). |
| `o` | Cycle service ordering between spec order and triage (failed, then unhealthy, first). |
| `r` | Request a restart of the selected service (local supervised jobs; see note below). |
| `Enter` | Open a detail panel for the selected service (placement, ntasks, nodelist, restart policy, timings, assertions); `Esc`/`Enter` closes and `j`/`k` switches service. |
| `y` | Copy a ready-to-run `logs` command for the selected service to the system clipboard (OSC 52; works over SSH). |
| `?` | Toggle in-UI help. |
| `q` / `Ctrl-C` | Leave the watch view without cancelling the job. |

Log lines are colored by inferred severity: lines mentioning `error`/`fatal`/`panic` show in red and `warn`/`warning` in yellow (subject to the active color policy).

Use `--hold-on-exit never|failure|always` on `up` or `watch` to control whether the final TUI stays open after a terminal scheduler state. When the view is held, press `d`, `l`, or `s` to print the exact `debug`, `logs`, or `stats` command after leaving the alternate screen.

The `r` restart action writes a request consumed by the local Pyxis/Enroot supervisor, the same mechanism `hpc-compose dev` uses for file-watch reloads; it applies to local supervised jobs and is reported as unavailable for Slurm batch jobs. Run `hpc-compose dev --tui` to get this live view during a dev session: file-watching keeps reloading changed services in the background while the watch UI (including `r` for an on-demand restart) runs in the foreground. Without `--tui`, `dev` keeps its line-oriented output, which is friendlier for CI and logs.

The watch and replay views repaint only the rows that change between refreshes, which keeps the display flicker-free and minimizes bytes sent over SSH. Two environment variables tune the live view:

| Variable | Effect |
| --- | --- |
| `HPC_COMPOSE_WATCH_REFRESH_MS` | Scheduler/log refresh cadence in milliseconds (default 1000, clamped to 100–60000). |
| `HPC_COMPOSE_WATCH_METRICS_REFRESH_MS` | Metrics refresh cadence in milliseconds (default 5000, clamped to 500–600000). |
| `HPC_COMPOSE_WATCH_MOUSE` | Set to a non-zero value to enable mouse capture; the scroll wheel then drives the log pane. Off by default so native terminal text selection keeps working. |

These display preferences can also be set per-project in `.hpc-compose/settings.toml` under a `[watch]` section; environment variables take precedence over the file:

```toml
[watch]
sort = "triage"          # spec | triage
wrap = true
refresh_ms = 500         # 100–60000
metrics_refresh_ms = 2000 # 500–600000
mouse = false
```

Use `hpc-compose up --watch-queue` when you want explicit queue polling before the watch view opens. It prints queue state changes, pending reason, and expected start time when Slurm exposes them; `--queue-warn-after <DURATION>` controls the one-time long-pending warning.

Use `--watch-mode line` when you are recording output, using a screen reader, running in CI, or working in a terminal where alternate-screen UIs are inconvenient. Line mode preserves detailed scheduler and log updates without alternate-screen control codes.

## Replay

`hpc-compose replay` reconstructs a best-effort execution timeline after the run. It reuses the watch-style view, but reads only artifacts that already exist under the tracked job directory. This makes it useful for rewinding to the time a service failed, comparing the nearest prior metrics sample, or sharing a deterministic text/JSON summary without querying Slurm again.

```bash
hpc-compose replay -f compose.yaml
hpc-compose replay -f compose.yaml --speed 10
hpc-compose replay -f compose.yaml --job-id 12345 --service trainer
hpc-compose replay -f compose.yaml --format json
```

Replay controls:

| Key | Action |
| --- | --- |
| `Space` | Pause or play the replay. |
| `+` / `-` | Move between speed presets such as `1x`, `10x`, and `100x`. |
| `Left` / `Right` | Seek backward or forward by five seconds. |
| `[` / `]` | Jump to the previous or next reconstructed event. |
| `Home` / `End` | Jump to the first or final replay frame. |
| `/`, `f`, `a`, `w`, `o`, `PgUp`, `PgDn`, `q` | Same filter, find, log-pane, wrap, sort, scroll, and quit behavior as `watch`. |

A timeline scrubber under the header shows the playback cursor and reconstructed event ticks between the start and end of the run.

Replay data sources:

| Source | What replay uses | Fidelity notes |
| --- | --- | --- |
| `state.json` | Final per-service state, start/finish times, exit code fallback, placement metadata | This file is overwritten during the run, so intermediate readiness and scheduler transitions are not exact. |
| `service-exits/*.jsonl` | Append-only service exit markers and restart evidence | Multiple exits reconstruct failure/restart sequences, but accepted restart relaunch time is inferred. |
| `metrics/*.jsonl` | Historical GPU and Slurm sampler rows | Replay shows the latest metrics sample at or before the cursor and never displays future metrics as current. |
| `logs/*.log` | Service log tails in the replay UI | Service logs do not include guaranteed per-line timestamps, so log panes are contextual tails, not exact log-time scrubbing. |
| Scheduler commands | Not queried during replay | Historical queue state, pending reason changes, and accounting gaps are not reconstructed. |

Use `--format json` when notebooks, dashboards, or experiment records need the reconstructed events, frame summaries, artifact paths, and fidelity notes.

## Checkpoints

`hpc-compose checkpoints` reports the attempt and requeue history of a tracked job from LOCAL tracked state only. It contacts no scheduler and reads nothing from the cluster filesystem, so it is safe to run from a laptop against a synced tracked directory.

```bash
hpc-compose checkpoints -f compose.yaml
hpc-compose checkpoints --job-id 12345
hpc-compose checkpoints --format json
```

The history derives from the per-attempt `state.json` files written under `.hpc-compose/<job>/attempts/<n>/`. These per-attempt directories are produced only when `x-slurm.resume` is configured and the job is requeued: each requeue records a new 0-based attempt index (`attempts = highest index + 1`, `requeues = attempts - 1`). A non-resume job has no `attempts/` directory and writes a single top-level `state.json`, which `checkpoints` reports as one attempt with zero requeues and no per-attempt index.

For each attempt, the command reports the earliest service start, the latest service finish, the derived duration, the job status, and the job exit code. A missing or unreadable per-attempt `state.json` is skipped and surfaced under `degraded[]` rather than failing the command, and a gap in the 0-based attempt indices (for example, an early attempt reaped by retention) is flagged as a truncated history so requeue counts are not silently miscounted.

`--format json` emits one object: `{job_id, compose_file, submitted_at, resume_configured, attempts, requeues, current_attempt, is_resume, resume_dir, entries[], degraded[]}`. This is distinct from the `artifacts --bundle checkpoints` export, which copies model checkpoint files rather than describing attempt history. See [Artifacts and Resume](artifacts-and-resume.md) for the attempt directory layout.

## Logs

Runtime logs live under:

```text
<runtime-root>/<job-id>/logs/<service>.log
```

Unless `x-slurm.output` is set, real submissions also write the top-level batch log under `<runtime-root>/logs/hpc-compose-<job-id>.out`. Check the batch log first when a job fails before any service log appears.

Service names containing non-alphanumeric characters are encoded in log filenames. Prefer `[a-zA-Z0-9_-]` in service names for readability.

Each service log is bracketed by timestamped lifecycle markers so a run does not look stuck before it produces output. A `[hpc-compose] <ts> service <name>: container starting via srun …` line is written just before the container launch (which is where srun scheduling and the first-use image extract happen), and a `[hpc-compose] <ts> service <name>: command exited rc=<code>` line is written when the command finishes. The gap between the start marker and the command's own first line is the container-launch time, not a hang.

Use `--grep <pattern>` to print only matching raw log lines across selected service logs. Use `--since <duration>` for coarse time-bounded initial output, for example `30s`, `15m`, `2h`, `1d`, or `1h30m`. Because service logs do not include line timestamps, `--since` filters by each log file's modification time rather than by individual line time. Follow mode still starts from the current end of each selected log and applies `--grep` to appended lines.

## Event Hooks

Per-service `x-slurm.hooks` can run host-side observability scripts when `restart_on_failure` accepts a restart or when the rolling restart window blocks a crash loop. Hook stdout/stderr is appended to that service's log, and non-zero hook exits are logged without changing the restart or failure outcome.

Use `on: restart` for retry notifications and `on: window_exhausted` for crash-loop alerts. Event hooks receive service identity, exit code, Slurm attempt, and restart-window counters through `HPC_COMPOSE_*` environment variables; see [Spec reference](spec-reference.md#servicesnamex-slurmhooks) for the full list.

## Metrics

When `x-slurm.metrics` is enabled, sampler files are written under:

```text
<runtime-root>/<job-id>/metrics/
  meta.json
  gpu.jsonl
  gpu_processes.jsonl
  slurm.jsonl
  cpu.jsonl
  diagnostics/
```

The sampler can collect GPU snapshots through `nvidia-smi`, job-step CPU/memory snapshots through `sstat`, and sampled host CPU utilization from `/proc/stat`. Collector failures are best-effort: missing `nvidia-smi`, missing `sstat`, an unreadable `/proc/stat`, or unsupported queries do not fail the batch job itself.

### Sampled CPU utilization

The `cpu` collector writes one `cpu.jsonl` row per node per interval with:

- `cpu_util_pct` — busy percentage (0–100, one decimal). It is computed as the non-idle over total delta of the aggregate `cpu` line in `/proc/stat` between the current and previous sample. The collector keeps the previous tick's counters in a per-node state file, so no extra sleep is spent inside a sample.
- `core_count` — the number of logical cores (per-core `cpuN` lines in `/proc/stat`).
- `loadavg_1m` — the 1-minute load average from `/proc/loadavg`.
- `node` — the sampling node, populated the same way GPU rows are, so multi-node allocations carry one row per node each tick.

On a multi-node allocation the collector fans out through `srun` (the same mechanism the GPU collector uses) and, if that `srun` fails for a tick, degrades to sampling the batch node's own `/proc/stat` rather than dropping the tick. `/proc/stat` is Linux-only; a node without it marks the CPU collector unavailable through the warn-once diagnostics instead of failing the job.

**First-sample caveat:** the very first sample for a given node has no previous counters to diff against, so its `cpu_util_pct` is `null`. Utilization appears from the second sample onward. `hpc-compose stats` surfaces the latest per-node `util`/`cores`/`load1` (plus a cross-node mean/max summary on multi-node jobs); the `--format json` snapshot exposes `sampler.cpu.nodes` and `sampler.cpu.summary`; the watch metrics line appends a compact `cpu: <mean>%` segment once utilization is available.

### Known limitations

- **Per-service and per-rank GPU attribution is not available.** GPU samples carry `service`, `rank`, and `local_rank` fields, but the sampler reads `nvidia-smi` at the node level and cannot map a device back to the service or distributed rank that is using it, so those fields are always `null`. Correlate GPU rows with a service through the `gpu_processes.jsonl` PID rows and your own launch layout rather than these fields.
- **`sstat`-derived GPU utilization and memory require cluster `acct_gather` NVML.** The `slurm.jsonl` collector exposes whatever `sstat` reports, but live per-step GPU accounting depends on the cluster having the NVML `acct_gather` plugin enabled, which is rarely present. The GPU numbers you can rely on come from the `nvidia-smi` collector (`gpu.jsonl`), not from `sstat`.
- **A final sample is flushed at job end.** When the sampler stops, it takes one extra synchronous sample before tearing down the periodic loop, so the window between the last interval tick and job teardown is captured. The final sample is time-bounded so a hung `nvidia-smi` or `sstat` cannot delay cleanup.
- **Multi-node GPU fanout degrades to batch-node sampling.** On a multi-node allocation the sampler fans out to every node through `srun`. If that `srun` fails for a tick, the sampler falls back to sampling the batch node's own GPUs and records a degraded note on the GPU collector rather than dropping the whole tick.

Add `--accounting` to `stats` when you need post-run `sacct` rollups for reporting. The accounting summary includes allocated CPU-hours, total CPU-hours when available, allocated GPU-hours, allocation-based memory byte-seconds, and observed maximum RSS. Memory byte-seconds are labeled as allocation-based because Slurm's standard accounting fields do not reliably provide true per-line memory-seconds across all clusters.

Use `hpc-compose inspect --rightsize -f compose.yaml` after a tracked Slurm run to convert those observations into conservative resource suggestions. The assistant requires tracked submission metadata and compares explicit requests such as `x-slurm.mem`, `x-slurm.time`, `x-slurm.gpus`, and service `x-slurm.cpus_per_task` against `sacct`, `sstat`, and `nvidia-smi` sampler evidence. It only reports suggestions; it does not rewrite the compose file.

Use `hpc-compose score <job-id>` after a tracked Slurm run when you want a compact efficiency grade. The score reuses sampler history, `sacct`, `sstat`, and right-sizing recommendations, then reports GPU utilization, memory utilization, active compute-time versus requested walltime, and a best-effort kWh estimate. Energy uses sampled GPU power when available, otherwise falls back to power limits or configured TDP assumptions through `--gpu-tdp-w`, `--cpu-watts-per-core`, and `--pue`; it does not claim carbon intensity or emissions.

Use `hpc-compose experiment show <job-id>` when you want all of that in one read-only object. A single call aggregates scheduler status, the post-run efficiency score, the artifact manifest, and submit-time provenance, so a notebook or experiment tracker can capture one run with one command (`hpc-compose experiment show <job-id> --format json`). It is static-safe: it contacts the scheduler only as much as `status` and `score` already do, writes nothing, and opens no connection. For each service with TCP or HTTP readiness it emits a per-service `ssh -L` tunnel hint, and `next_commands` carries SSH `ControlMaster`/`ControlPath`/`ControlPersist` multiplexing guidance so an OTP/2FA login node prompts you only once. Legacy records without provenance, non-terminal jobs without a complete efficiency report, and runs without an artifact manifest still produce a valid object with those fields omitted.

For a short canary run before a full run, use `hpc-compose germinate`; see [Right-Size With Canary Runs](canary-runs.md).

## Sweep Manifests

Sweep submission and monitoring (`sweep submit`, `sweep status`, `sweep list`) are covered in [Hyperparameter Sweeps](sweeps.md). Sweep-trial records do not replace normal `latest.json` or `latest-run.json`, so `hpc-compose status`, `watch`, and `logs` continue to target ordinary runs unless you pass an explicit job id.

## Diffing Runs

Use `hpc-compose diff <job-id-1> <job-id-2>` to compare two tracked submissions. The compact text view highlights outcome, resource, and config changes; `--format json` returns the full uncapped diff for notebooks or experiment records. Older tracked jobs without config snapshots still compare outcome metadata and report a note that config comparison is unavailable.

### N-Way Comparison Matrix

To compare more than two runs at once, drop the positional job ids and pass either `--jobs a,b,c` (an explicit comma-separated list of tracked job ids) or `--across <sweep-id>` (every *submitted* trial of a sweep; unsubmitted trials are skipped with a note). The result is a matrix with one column per run and one row per field that differs in at least one run — fields identical across every run are collapsed and omitted, so the output stays focused on what actually changed. The same outcome, provenance, resource, and config sections as the pairwise diff are projected across all runs.

Choose the output with `--matrix-format text|csv|json` (default `text`). `--matrix-format csv` emits a `section,field,<job_id>...` table for spreadsheets, while `--matrix-format json` serializes the full uncapped matrix (the text view caps the `config` section at 25 rows). This is a pure read-only projection over already-persisted records; like pairwise `diff`, it opens no connection and only probes the scheduler as much as `status` does.

```bash
hpc-compose diff --jobs 12345,12346,12347 --matrix-format json
hpc-compose diff --across sweep-1700000000-1234 --matrix-format csv
```

## Related Docs

- [Operate a Real Cluster Run](runbook.md)
- [Troubleshoot a Failed Run](troubleshooting.md)
- [Manage the Cache and Clean Up](cache-management.md)
- [Artifacts and Resume](artifacts-and-resume.md)
- [Hyperparameter Sweeps](sweeps.md)
- [Right-Size With Canary Runs](canary-runs.md)
