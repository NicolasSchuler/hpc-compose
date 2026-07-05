# Run Hyperparameter Sweeps

`hpc-compose sweep` turns one compose file with an embedded `sweep` block into many independent tracked Slurm jobs. Each trial is a normal `sbatch` submission with its own allocation, rendered script, job record, and scheduler state. The sweep manifest ties those jobs together for listing and aggregate status.

## Quickstart

Start from a spec that can run with ordinary defaults, then add a top-level `sweep` block:

```yaml
name: training-sweep

x-slurm:
  time: "00:20:00"
  cache_dir: ${CACHE_DIR:-/cluster/shared/hpc-compose-cache}

sweep:
  parameters:
    lr: [0.001, 0.01, 0.1]
    batch_size: [32, 64]
  matrix: full

services:
  trainer:
    image: python:3.11-slim
    environment:
      LR: "${lr:-0.001}"
      BATCH_SIZE: "${batch_size:-32}"
    command: ["python", "train.py"]
```

Preview the expansion first:

```bash
hpc-compose sweep submit -f examples/training-sweep.yaml --dry-run
```

Then submit the trials:

```bash
hpc-compose sweep submit -f examples/training-sweep.yaml
hpc-compose sweep status -f examples/training-sweep.yaml
hpc-compose sweep list -f examples/training-sweep.yaml
```

## Matrix Modes

`matrix: full` expands the full Cartesian product over sorted parameter names, so the example above produces six trials in stable `t000`, `t001`, ... order.

Random sampling selects without replacement:

```yaml
sweep:
  parameters:
    lr: [0.001, 0.01, 0.1]
    batch_size: [32, 64]
  matrix:
    random: 5
    seed: "paper-table-2"
```

With a seed, the selected trials are stable across machines. Without a seed, `sweep submit` derives one from the new sweep id and persists it in the manifest.

## Interpolation Rules

Sweep parameter names are interpolation variable names. Values may be scalar strings, numbers, or booleans. For each trial, those variables override values from the environment and settings before planning, preparing, and rendering.

Reserved variables are also available:

| Variable | Value |
| --- | --- |
| `HPC_COMPOSE_SWEEP_ID` | The persisted sweep id. |
| `HPC_COMPOSE_SWEEP_TRIAL` | The stable trial label such as `t000` (or `t000r0` when `replicates` > 1). |
| `HPC_COMPOSE_SWEEP_TRIAL_INDEX` | Zero-based trial index. |
| `HPC_COMPOSE_SWEEP_REPLICATE` | Zero-based replicate index within the config (`0` when `replicates: 1`). |
| `HPC_COMPOSE_SWEEP_SEED` | Deterministic per-replicate seed; present only when `replicates` > 1. |

Normal commands still treat `sweep` as metadata. If `plan`, `up`, or `render` encounters `${lr}` without a default, it fails unless `lr` is provided in the environment or settings. Use defaults such as `${lr:-0.001}` when the base spec should remain runnable, and use `sweep submit --dry-run` as the validation path for missing sweep-only variables.

## Replicates

Set `replicates: N` to submit `N` seeded trials per parameter config. This is sweep sugar for repeating each combination so noise can be averaged out:

```yaml
sweep:
  parameters:
    lr: [0.001, 0.01]
  matrix: full
  replicates: 3
  objective:
    direction: minimize
    log_pattern: 'final loss=([0-9.]+)'
```

With `replicates: 1` (the default) the expansion is byte-identical to a non-replicated sweep: trial ids stay `t000`, `t001`, … and no replicate seed is injected. With `replicates` > 1 each config `c` fans out into `t{c:03}r0` … `t{c:03}r{N-1}` (for example `t000r0`, `t000r1`, `t000r2`), each its own Slurm allocation. The example above submits `2 configs × 3 replicates = 6` trials.

Each replicate gets a deterministic seed exposed as `HPC_COMPOSE_SWEEP_SEED`, derived as the hex SHA-256 digest of `<sweep_id>:<config_key>:<replicate>` (where `config_key` is the `name=value;…` join of the config's sorted variables). Re-expanding the same `sweep` block with the same sweep id always reproduces the same seed, so a training script can feed `HPC_COMPOSE_SWEEP_SEED` to its RNG and recover the same run. `HPC_COMPOSE_SWEEP_REPLICATE` carries the zero-based replicate index.

`sweep status`, `sweep observe`, and `sweep results` group the trials of each config and report a mean±std(n) rollup (population standard deviation, so `n=1` reports `std=0`). Crucially, `best_trial` ranks on the per-config **group mean**, not the single luckiest replicate, and `sweep observe` reports the winning config's mean objective:

```text
replicate rollup (mean+/-std over n replicates per config):
  lr=0.001: mean=0.034000 std=0.002160 n=3 (3 replicate(s))
  lr=0.01:  mean=0.041000 std=0.001414 n=3 (3 replicate(s))
best config: t000r0 (mean objective=0.034)
```

The fanout guard below counts materialized runs (`combinations × replicates`), so a 40-config matrix with `replicates: 3` is 120 runs and is rejected without `--max-trials`.

## Fanout Guard

By default, submitted sweeps are capped at 100 trials. Larger matrices fail before calling `sbatch`:

```bash
hpc-compose sweep submit -f examples/training-sweep.yaml
```

Raise the explicit ceiling when the fanout is intentional:

```bash
hpc-compose sweep submit -f examples/training-sweep.yaml --max-trials 500
```

The guard applies to real submissions. Dry runs can inspect any matrix size.

## Status Output

`sweep status` loads the manifest, queries the tracked state for submitted jobs, and aggregates:

- `completed`
- `failed`
- `running`
- `pending`
- `unknown`
- `missing_tracking`
- `submit_failed`

Use JSON for notebooks, dashboards, or CI automation:

```bash
hpc-compose sweep submit -f examples/training-sweep.yaml --format json
hpc-compose sweep status -f examples/training-sweep.yaml --format json
hpc-compose sweep status -f examples/training-sweep.yaml --sweep-id sweep-123 --format json
hpc-compose sweep list -f examples/training-sweep.yaml --format json
```

The JSON includes the sweep id, manifest path, matrix mode, persisted seed, trial variables, job ids, record paths, and per-trial status. When the sweep used `replicates`, it also carries a `groups` array with the per-config mean±std(n) rollup.

## Manifest Layout

Sweep state is stored beside normal tracked jobs:

```text
.hpc-compose/
  sweeps/
    latest.json
    <sweep-id>/
      sweep.json
      t000.sbatch
      t001.sbatch
  jobs/
    <job-id>.json
```

With `replicates` > 1 the per-trial scripts are named `t000r0.sbatch`, `t000r1.sbatch`, … (config index plus replicate index) instead of the flat `t000.sbatch`.

Sweep-trial records have `kind: sweep_trial` and include sweep metadata. They do not update the normal `latest.json` or `latest-run.json` pointers, so `status`, `watch`, and `logs` for ordinary runs keep their existing meaning.

## Objectives and Early Termination

Declare an `objective` block to have `sweep observe` parse a metric from each terminal trial, rank trials, and record the best on the manifest:

```yaml
sweep:
  parameters: { lr: [0.001, 0.01, 0.1] }
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'final loss=([0-9.]+)'
```

The trial workload prints the metric to its service log (e.g. `final loss=0.034`). Two parse sources are supported (set exactly one):

- `log_pattern`: a regex against the trial's primary service log; capture group `group` (default 1) is parsed as a float.
- `json_path` + `json_field`: read a JSON field from the trial's artifact-collected tree.

```bash
hpc-compose sweep observe -f train.yaml             # parse + rank + print best
hpc-compose sweep observe -f train.yaml --format json
```

Early termination stops the sweep once a threshold is met. Use `--watch --stop-when` to poll and auto-stop:

```bash
hpc-compose sweep observe -f train.yaml --watch --stop-when 'objective < 0.05' --poll-interval 30s
```

Or stop manually after inspecting `sweep observe` output:

```bash
hpc-compose sweep stop -f train.yaml --yes --reason 'objective threshold met'
```

`sweep stop` cancels every non-terminal trial via `scancel` and records the stop on the manifest. `--stop-when` uses a tiny grammar: `objective < N`, `objective <= N`, `objective > N`, or `objective >= N`, evaluated against the best observed value.

> Bayesian/adaptive trial selection is intentionally out of scope. The objective writeback, ranking, and stop machinery here are the foundation any future optimizer would build on.

## Scaling Reports

Set `objective.scaling_axis` to the name of a numeric sweep parameter (for example `nodes` or `model_size`) to enable a post-hoc scaling report:

```yaml
sweep:
  parameters:
    nodes: [1, 2, 4, 8]
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'final loss=([0-9.]+)'
    scaling_axis: nodes
```

`scaling_axis` must name a key under `sweep.parameters`, and every value of that parameter must parse as a number. Both are checked at validate time (including `sweep submit --dry-run`), so a typo or a non-numeric axis is rejected with a clear message before anything is submitted.

Run `sweep observe --scaling` to print the report alongside the usual ranked table:

```bash
hpc-compose sweep observe -f train.yaml --scaling
hpc-compose sweep observe -f train.yaml --scaling --format json
```

The report pairs each config group's mean objective with its axis value, reports a log-log least-squares slope (`ln(objective)` vs `ln(axis)`), and computes speedup/efficiency relative to a baseline group:

```text
scaling (minimize objective vs nodes):
  baseline nodes=1
  nodes=1 mean=0.800000 runtime=100s speedup=1.000x efficiency=100.0% (n=1)
  nodes=2 mean=0.400000 runtime=50s speedup=2.000x efficiency=100.0% (n=1)
  nodes=4 mean=0.200000 runtime=25s speedup=4.000x efficiency=100.0% (n=1)
  log-log slope (objective vs nodes): -1.0000
```

The report is purely read-only, post-hoc analysis over the persisted manifest and tracked local state: it reuses the same terminal-only scheduler/runtime probe as `sweep observe` and never opens a new connection. Runtime is taken from the maximum observed service duration of each terminal trial; trials that are non-terminal or report no runtime are skipped rather than zero-filled. The baseline is the smallest-axis group that has runtime data. The report is print/JSON-only and is never written back to the manifest, so omitting `--scaling` leaves observe output byte-identical.

## Resume a Partial Sweep

Trials submit sequentially, and if one submission fails, later trials are not submitted (see [Limitations](#limitations)). The partial manifest is kept, so a failed submission can be resumed with `sweep submit --resume`:

```bash
hpc-compose sweep submit -f train.yaml --resume
hpc-compose sweep submit -f train.yaml --resume --sweep-id sweep-1700000000-1234
```

Resume re-drives the existing manifest and submits only the trials that never got a job: those that recorded a submit error and those that were never attempted. Already-submitted trials keep their job id untouched, and no new sweep id is minted, so the sweep keeps its identity and the `sweep status`/`observe`/`results` history stays continuous. Without `--sweep-id`, resume targets the latest sweep for the compose file.

Before resubmitting anything, resume re-expands the current compose file's sweep block using the stored sweep id, so `matrix: random` samples and per-replicate seeds reproduce exactly. It then compares the re-expansion against the manifest. If the sweep block changed since the original submission (matrix mode, parameter combinations, trial count, per-trial variables, or seeds), resume refuses with a clear error rather than submit trials that no longer match the recorded plan; submit a new sweep instead. Other spec edits that the sweep block does not describe -- a changed service `command:` or `image:`, for example -- cannot be caught by this guard: resume records the compose file's content hash at the original submit time and, if the file changed, prints a warning to stderr and continues. The resumed trials render from the *current* file and may diverge from already-submitted siblings, so this is a warning (benign edits like comments must not block recovery) rather than a hard error. The same fanout guard as the original submit applies, so a sweep that expands to more than 100 trials still needs `--max-trials` on resume.

Preview the resume set without submitting anything with `--dry-run`:

```bash
hpc-compose sweep submit -f train.yaml --resume --dry-run
```

This validates each pending trial's plan and reports how many trials would be resubmitted versus left in place, without writing scripts, submitting jobs, or touching the manifest. If every trial already has a job, resume exits successfully with a "nothing to resume" message. A resume run's JSON output adds `resumed`, `resubmitted`, and `skipped_already_submitted` fields alongside the usual manifest.

A `--resume` run is itself re-runnable: if a resubmission fails again, the manifest records the new error and later trials are left for the next resume.

## Limitations

- Sweeps must be embedded in the same compose file. `sweep.spec` is not supported.
- Each trial is a separate Slurm allocation. Sweeps are not Slurm arrays.
- `x-slurm.array` is rejected during `sweep submit`.
- Trials submit sequentially. If a submission fails, later trials are not submitted and the partial manifest is kept; resume it with `sweep submit --resume` (see [Resume a Partial Sweep](#resume-a-partial-sweep)).
- `sweep status` summarizes scheduler/tracking state; use `sweep observe` to parse and rank objectives.

## Related Docs

- [Right-Size With Canary Runs](canary-runs.md)
- [Runtime Observability](runtime-observability.md)
- [CLI Reference](cli-reference.md)
- [Spec Reference](spec-reference.md)
