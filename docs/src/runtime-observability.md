# Runtime Observability

After a submission, `hpc-compose` records tracked metadata under:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/
```

That directory lets follow-up commands reconnect without resubmitting.

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
| `germinate` | Short (default one-minute) canary submission that writes `latest-canary.json` and recommends resource settings from fresh metrics. |
| `sweep status` | Aggregate persisted sweep trials into completed, failed, running, pending, unknown, missing-tracking, and submit-failed counts. |
| `sweep list` | List prior sweep manifests without querying the scheduler. |
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
| `Space` | Pause or resume log following. |
| `PgUp` / `PgDn` | Scroll the visible log pane while paused. |
| `End` | Return to live-follow mode at the newest log lines. |
| `a` | Toggle between the selected service log and all tracked service logs. |
| `?` | Toggle in-UI help. |
| `q` / `Ctrl-C` | Leave the watch view without cancelling the job. |

Use `--hold-on-exit never|failure|always` on `up` or `watch` to control whether the final TUI stays open after a terminal scheduler state. When the view is held, press `d`, `l`, or `s` to print the exact `debug`, `logs`, or `stats` command after leaving the alternate screen.

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
| `/`, `a`, `PgUp`, `PgDn`, `q` | Same filter, log-pane, scroll, and quit behavior as `watch`. |

Replay data sources:

| Source | What replay uses | Fidelity notes |
| --- | --- | --- |
| `state.json` | Final per-service state, start/finish times, exit code fallback, placement metadata | This file is overwritten during the run, so intermediate readiness and scheduler transitions are not exact. |
| `service-exits/*.jsonl` | Append-only service exit markers and restart evidence | Multiple exits reconstruct failure/restart sequences, but accepted restart relaunch time is inferred. |
| `metrics/*.jsonl` | Historical GPU and Slurm sampler rows | Replay shows the latest metrics sample at or before the cursor and never displays future metrics as current. |
| `logs/*.log` | Service log tails in the replay UI | Service logs do not include guaranteed per-line timestamps, so log panes are contextual tails, not exact log-time scrubbing. |
| Scheduler commands | Not queried during replay | Historical queue state, pending reason changes, and accounting gaps are not reconstructed. |

Use `--format json` when notebooks, dashboards, or experiment records need the reconstructed events, frame summaries, artifact paths, and fidelity notes.

## Logs

Runtime logs live under:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/logs/<service>.log
```

Slurm may also write a top-level batch log such as `slurm-<jobid>.out`, or to the path configured with `x-slurm.output`. Check the batch log first when a job fails before any service log appears.

Service names containing non-alphanumeric characters are encoded in log filenames. Prefer `[a-zA-Z0-9_-]` in service names for readability.

Use `--grep <pattern>` to print only matching raw log lines across selected service logs. Use `--since <duration>` for coarse time-bounded initial output, for example `30s`, `15m`, `2h`, `1d`, or `1h30m`. Because service logs do not include line timestamps, `--since` filters by each log file's modification time rather than by individual line time. Follow mode still starts from the current end of each selected log and applies `--grep` to appended lines.

## Event Hooks

Per-service `x-slurm.hooks` can run host-side observability scripts when `restart_on_failure` accepts a restart or when the rolling restart window blocks a crash loop. Hook stdout/stderr is appended to that service's log, and non-zero hook exits are logged without changing the restart or failure outcome.

Use `on: restart` for retry notifications and `on: window_exhausted` for crash-loop alerts. Event hooks receive service identity, exit code, Slurm attempt, and restart-window counters through `HPC_COMPOSE_*` environment variables; see [Spec reference](spec-reference.md#servicesnamex-slurmhooks) for the full list.

## Metrics

When `x-slurm.metrics` is enabled, sampler files are written under:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/metrics/
  meta.json
  gpu.jsonl
  gpu_processes.jsonl
  slurm.jsonl
  diagnostics/
```

The sampler can collect GPU snapshots through `nvidia-smi` and job-step CPU/memory snapshots through `sstat`. Collector failures are best-effort: missing `nvidia-smi`, missing `sstat`, or unsupported queries do not fail the batch job itself.

Add `--accounting` to `stats` when you need post-run `sacct` rollups for reporting. The accounting summary includes allocated CPU-hours, total CPU-hours when available, allocated GPU-hours, allocation-based memory byte-seconds, and observed maximum RSS. Memory byte-seconds are labeled as allocation-based because Slurm's standard accounting fields do not reliably provide true per-line memory-seconds across all clusters.

Use `hpc-compose inspect --rightsize -f compose.yaml` after a tracked Slurm run to convert those observations into conservative resource suggestions. The assistant requires tracked submission metadata and compares explicit requests such as `x-slurm.mem`, `x-slurm.time`, `x-slurm.gpus`, and service `x-slurm.cpus_per_task` against `sacct`, `sstat`, and `nvidia-smi` sampler evidence. It only reports suggestions; it does not rewrite the compose file.

Use `hpc-compose score <job-id>` after a tracked Slurm run when you want a compact efficiency grade. The score reuses sampler history, `sacct`, `sstat`, and right-sizing recommendations, then reports GPU utilization, memory utilization, active compute-time versus requested walltime, and a best-effort kWh estimate. Energy uses sampled GPU power when available, otherwise falls back to power limits or configured TDP assumptions through `--gpu-tdp-w`, `--cpu-watts-per-core`, and `--pue`; it does not claim carbon intensity or emissions.

Use `hpc-compose germinate -f compose.yaml` before a full run when you want a short canary to gather fresh evidence. Canary runs write `.hpc-compose/latest-canary.json` so normal `up` metadata remains the latest production submission.

## Sweep Manifests

`hpc-compose sweep submit` stores sweep state under `.hpc-compose/sweeps/<sweep-id>/sweep.json` and refreshes `.hpc-compose/sweeps/latest.json`. The manifest records the matrix mode, persisted random seed, trial ids, trial variables, rendered script paths, job ids, per-trial job record paths, submit times, and any submit error.

Each submitted trial also writes a normal job record under `.hpc-compose/jobs/<job-id>.json` with `kind: sweep_trial` and a `sweep` metadata block. Sweep-trial records deliberately do not replace normal `latest.json` or `latest-run.json`, so `hpc-compose status`, `watch`, and `logs` continue to target ordinary runs unless you pass an explicit job id.

`hpc-compose sweep status -f compose.yaml --format json` loads the manifest and queries the same scheduler/tracking snapshot code used for ordinary jobs. It reports per-trial state plus aggregate counts for `completed`, `failed`, `running`, `pending`, `unknown`, `missing_tracking`, and `submit_failed`. hpc-compose does not parse metric files or infer the best trial; keep metric summaries in your training output or external experiment tracker.

## Diffing Runs

Use `hpc-compose diff <job-id-1> <job-id-2>` to compare two tracked submissions. The compact text view highlights outcome, resource, and config changes; `--format json` returns the full uncapped diff for notebooks or experiment records. Older tracked jobs without config snapshots still compare outcome metadata and report a note that config comparison is unavailable.

## Related Docs

- [Runbook](runbook.md)
- [Troubleshooting](troubleshooting.md)
- [Artifacts and Resume](artifacts-and-resume.md)
- [Hyperparameter Sweeps](sweeps.md)
- [Right-Sizing With Canary Runs](canary-runs.md)
