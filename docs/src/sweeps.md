# Hyperparameter Sweeps

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
| `HPC_COMPOSE_SWEEP_TRIAL` | The stable trial label such as `t000`. |
| `HPC_COMPOSE_SWEEP_TRIAL_INDEX` | Zero-based trial index. |

Normal commands still treat `sweep` as metadata. If `plan`, `up`, or `render` encounters `${lr}` without a default, it fails unless `lr` is provided in the environment or settings. Use defaults such as `${lr:-0.001}` when the base spec should remain runnable, and use `sweep submit --dry-run` as the validation path for missing sweep-only variables.

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

The JSON includes the sweep id, manifest path, matrix mode, persisted seed, trial variables, job ids, record paths, and per-trial status.

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

> Bayesian/adaptive trial selection is intentionally out of scope for v1. The objective writeback, ranking, and stop machinery here are the foundation any future optimizer would build on.

## Limitations

- Sweeps must be embedded in the same compose file. `sweep.spec` is not supported.
- Each trial is a separate Slurm allocation. Sweeps are not Slurm arrays.
- `x-slurm.array` is rejected during `sweep submit`.
- Trials submit sequentially. If a submission fails, later trials are not submitted and the partial manifest is kept.
- `sweep status` summarizes scheduler/tracking state; use `sweep observe` to parse and rank objectives.

## Related Docs

- [Runtime Observability](runtime-observability.md)
- [Right-Sizing With Canary Runs](canary-runs.md)
- [CLI Reference](cli-reference.md)
- [Spec Reference](spec-reference.md)
