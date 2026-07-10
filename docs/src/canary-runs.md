# Right-Size With Canary Runs

Resource advice is a loop, not one prediction: observe current conditions, gate
one intentional submission, probe a short representative run, monitor it,
evaluate the completed evidence, then change requests manually.

```text
weather ─► when ─► germinate ─► stats/watchdog ─► score ─► inspect --rightsize
 observe     gate      probe          monitor        evaluate       adjust
```

Each stage has a different evidence limit. None reserves capacity or proves
that a short canary represents a long production phase.

## Observe Capacity With `weather` {#observe-capacity-with-weather}

```bash
hpc-compose weather
hpc-compose weather --format json
```

`weather` reads one live snapshot from available Slurm tools and may include
node, queue, fair-share, and priority signals. It does not submit, reserve, or
change a spec.

Limits:

- Sites expose different subsets of `sinfo`, `squeue`, `sshare`, and `sprio`.
- “Free” at observation time is not “reserved for this user.”
- Fair-share and priority values are inputs to site policy, not portable start
  time predictions.
- A denied or missing probe reduces evidence; it is not equivalent to zero load.

## Gate Submission With `when`

```bash
hpc-compose when -f compose.yaml --partition gpu --free-nodes 1 --poll-interval 120s
hpc-compose when -f compose.yaml --after-job <job-id>
hpc-compose when -f compose.yaml --between 22:00-06:00
```

`when` prepares and renders, polls typed conditions in the foreground, then
calls `sbatch` once they match. Interrupt before the match to prevent that
submission. `--detach` applies after submission; it does not detach the wait.

This is an **allocation-submitting command**. A matching observation does not
reserve the nodes, so another request can win the race and the job may still
enter `PENDING`. Keep polling gentle on login nodes.

## Estimate With `germinate`

`germinate` submits a short Slurm canary for an existing spec, enables metrics,
waits for completion, and emits conservative request observations plus a manual
YAML patch.

```bash
hpc-compose germinate -f compose.yaml
hpc-compose germinate --canary-time 00:01:00 --metrics-interval 5 --format json -f compose.yaml
```

This consumes allocation quota. Preview the generated canary without submitting:

```bash
hpc-compose germinate --dry-run --script-out canary.sbatch -f compose.yaml
```

The canary keeps site-binding settings and service topology from the source
plan, minimizes selected resources in memory, writes `latest-canary.json`, and
leaves ordinary `latest.json` untouched.

Recommendation rules are deliberately conservative:

- CPU uses observed demand with headroom and rounds up.
- Memory uses the strongest available sampler, `sstat`, or `sacct` evidence and
  rounds to scheduler-friendly units.
- GPU count can shrink only when GPU sampler evidence shows fewer active devices
  **and collector coverage supports that scope**.
- A short canary never reduces walltime from its own short elapsed time.

Start with [`canary-right-size.yaml`](example-source.md#canary-right-size) when
you want an explicit practice workload.

## Monitor With `stats` and the Watchdog

During or after the canary:

```bash
hpc-compose stats --format json -f compose.yaml
hpc-compose watch --watch-mode line -f compose.yaml
```

`x-slurm.watchdog` turns sustained sampler history into advisory idle-resource
warnings after its configured grace period. It separates GPU compute from GPU
memory residency and can use CPU utilization where available. The current
watchdog does not cancel jobs.

Read collector coverage before interpreting a metric:

- `allocation` with all expected nodes observed supports allocation-wide views;
- `batch_node` is a one-node fallback and must be shown as partial;
- `unknown` preserves legacy or unavailable evidence without pretending it is complete;
- `degraded: true` plus a reason explains why the intended scope was not observed.

A warning such as `TELEMETRY DEGRADED: GPU covers batch node only (1/4)` is a
semantic warning, not cosmetic color. Partial measurements may be displayed,
but they cannot justify allocation-wide idle conclusions.

## Evaluate With `score`

```bash
hpc-compose score -f compose.yaml
hpc-compose score --format json -f compose.yaml
```

`score` evaluates post-run efficiency and optional energy estimates. Treat it as
a summary of recorded evidence, not a universal benchmark grade. Missing or
partial collectors, accounting delays, short warmup, and workload phase all
affect confidence; machine-readable reports carry the available coverage and
confidence notes.

## Adjust Manually With `inspect --rightsize` {#adjust-manually-with-inspect-rightsize}

```bash
hpc-compose inspect --rightsize -f compose.yaml
hpc-compose inspect --rightsize --format json -f compose.yaml
```

This is the post-run counterpart to `germinate`. Ordinary `inspect` is static
and explains the current plan; `inspect --rightsize` reads a completed tracked
run and suggests conservative replacements. Review and edit the YAML manually,
then validate, lint, and plan again.

Right-sizing must suppress allocation-wide GPU reductions and idle conclusions
when multi-node coverage is degraded or unknown. Absence of a recommendation is
the correct result when the evidence cannot support one.

## What Canaries Cannot Establish

- Warmup-heavy jobs can look smaller than steady state.
- Lazy model loading can miss later GPU or memory demand.
- Data-dependent peaks can occur after the canary exits.
- Distributed startup can exercise only one phase of a multi-node workload.
- Failed, OOM-like, time-limited, malformed, or missing metrics are diagnostics,
  not permission to reduce requests.
- Queue observations and canaries do not predict an exact start time or reserve capacity.
- Partial telemetry must never be presented as complete allocation coverage.

The next discriminating step is usually a representative finite smoke or a
longer canary that reaches the missing workload phase—not a more aggressive
automatic patch.

## Related Docs

- [Production Readiness](production-readiness.md)
- [Runtime Observability](runtime-observability.md)
- [Run Hyperparameter Sweeps](sweeps.md)
- [Operate a Real Cluster Run](runbook.md)
- [Spec Reference](spec-reference.md)
