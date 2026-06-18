# Right-Sizing With Canary Runs

`hpc-compose germinate` submits a short Slurm canary for an existing compose spec, forces runtime metrics on, waits for the canary to finish, and prints conservative resource recommendations for the original spec.

Canaries are short probes, not benchmark truth. They are useful for catching obvious over-requests such as asking for many GPUs when only one device is touched, or requesting far more memory than the process ever approaches during startup. They are not a substitute for full-run profiling when a workload has long warmup, data-dependent memory, lazy model loading, or late training phases.

## Basic Workflow

```bash
hpc-compose germinate -f compose.yaml
hpc-compose germinate -f compose.yaml --format json
hpc-compose germinate -f compose.yaml --canary-time 00:01:00 --metrics-interval 5
```

The canary keeps partition, account, QoS, constraints, cache, runtime backend, and service topology from the original plan. It minimizes CPU, memory, and GPU requests in memory only, writes `latest-canary.json`, and leaves normal `latest.json` untouched.

Dry-run the canary script without submitting:

```bash
hpc-compose germinate -f compose.yaml --dry-run --script-out canary.sbatch
```

## Output

Text output includes the canary job id, the standard right-sizing observations, and a YAML patch you can apply manually:

```yaml
x-slurm:
  mem: 16G
services:
  trainer:
    x-slurm:
      cpus_per_task: 4
```

JSON output includes the same patch plus the full right-sizing report:

```bash
hpc-compose germinate -f compose.yaml --format json
```

## Recommendation Rules

- CPU recommendations use observed CPU demand with conservative headroom and round up.
- Memory recommendations use the strongest available evidence from sampler rows, `sstat`, and `sacct`, then round to Slurm-friendly units.
- GPU recommendations shrink only when GPU sampler evidence shows fewer active devices.
- Walltime is observed but not down-sized from a short canary run.

## Caveats

- Warmup-heavy jobs can look smaller than steady-state jobs.
- Data-dependent memory may peak after the canary exits.
- Lazy model loading can under-report memory and GPU use if no real request hits the model.
- Distributed training may need full topology even when a canary only exercises startup.
- Failed, OOM-like, time-limit, malformed-metrics, and missing-metrics cases are reported as diagnostics rather than YAML rewrites.

Start from [`examples/canary-right-size.yaml`](example-source.md#canary-right-size) when you want a small, explicit spec to practice the workflow.

## Related Docs

- [Runtime Observability](runtime-observability.md)
- [Runbook](runbook.md)
- [Spec Reference](spec-reference.md)
