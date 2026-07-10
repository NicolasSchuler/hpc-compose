# Command Families

Start with the question you need answered. Commands in adjacent families often
look similar because they examine the same spec, but they produce different
evidence.

## Which Command Answers My Question?

| Your question | Command | Evidence produced | What it does not prove |
| --- | --- | --- | --- |
| Is this legal hpc-compose YAML? | `validate` | Parser and semantic validation result | That the request is wise for this site |
| Is it valid but operationally risky? | `lint` | Stable `HPC...` findings and optional reviewed fixes | That login/compute nodes provide the environment |
| What execution would be planned? | `plan` | Service order, allocation shape, runtime plan, optional safe hints | That runtime tools or paths exist |
| What are the normalized topology and mounts? | `inspect` | Normalized services, placement, mounts, argv, and dependency views | Post-run utilization unless `--rightsize` is requested |
| Where did this generated script line come from? | `render --annotate` / `explain` | Script preview and field-to-line provenance | That the script was submitted or succeeded |
| Is the environment ready? | `preflight` / `doctor` | Backend, Slurm, storage, profile, readiness, MPI, or fabric evidence | Application correctness; active probes may need an allocation |
| Does the finite workflow work end to end? | `test --local` / `test --submit` | Service start, readiness, completion, and assertion result | Long-run performance or convergence |
| Why did the tracked run fail? | `debug` | Scheduler, batch log, service state, log tails, and next-command evidence | A generic classification of arbitrary application logs |

## Static Authoring

`validate`, `lint`, `plan`, ordinary `inspect`, `config`, `schema`, `render`,
and `explain` can answer authoring questions before submission. Add global
`--offline` when an automation or agent must fail rather than contact SSH or
Slurm.

The progression is intentional:

```text
YAML shape ─► policy risk ─► execution plan ─► normalized details ─► script provenance
 validate       lint            plan              inspect            render/explain
```

Use JSON output for automation and inspect the corresponding output schema
before depending on fields. Avoid unredacted `--show-values`, `plan --verbose`,
or full scripts in shared logs and conversations.

## Environment Readiness

`preflight` checks the selected spec against the current submission host and an
optional advisory cluster profile. Ordinary strict preflight does not allocate
compute resources. `preflight --fs-probes`, `doctor mpi-smoke --submit`, and
`doctor fabric-smoke --submit` do submit work and consume quota; the render-only
doctor forms do not.

`doctor cluster-report` records a best-effort capability snapshot. It does not
grant access, provision storage, load modules, or guarantee that a volatile
partition remains unchanged.

## Execution and Finite Tests

- `up` is the normal prepare, render, submit, track, and watch path.
- `when` waits for advisory conditions, then submits; it does not reserve the
  observed capacity.
- `test --submit` spends a finite Slurm allocation to check behavior.
- `test --preemption` additionally signals and requeues a deliberately prepared
  resume contract.
- `alloc`, `run`, `shell`, and `notebook` are interactive or one-off runtime
  paths with their own quota lifetime.

Dry-run and offline behavior do not authorize the corresponding real command.
Read the [CLI Reference](cli-reference.md) for exact flags and the generated
agent command-safety page for authorization tiers.

## Tracked Operations and Recovery

After submission, use the least expensive evidence first:

1. `status --format json` for scheduler and service summary.
2. `logs --lines <N>` for a bounded tail.
3. `status --verify` for contradictions across tracked evidence.
4. `debug` for a joined failure report and next command.
5. `stats`, `score`, and artifacts only when those records are relevant.

`cancel`, `down`, `clean`, cache pruning, rendezvous pruning, and workspace
release mutate or delete state. Their read-only previews or list commands are
separate decisions from executing the mutation.

## Two Meanings of `inspect`

| Form | Evidence source | When to use it |
| --- | --- | --- |
| `inspect -f compose.yaml` | Current static spec and normalized plan | Before a run, to understand topology, placement, mounts, commands, and dependencies |
| `inspect --rightsize -f compose.yaml` | A completed tracked run's accounting and sampler evidence | After a run, to consider conservative manual request changes |

Ordinary `inspect` is static. `inspect --rightsize` is post-run analysis and can
only be as complete as its telemetry coverage. Unknown or partial multi-node
evidence must not be interpreted as allocation-wide idleness.

## Related Docs

- [CLI Reference](cli-reference.md)
- [Quickstart](quickstart.md)
- [Execution Model](execution-model.md)
- [Production Readiness](production-readiness.md)
- [Runtime Observability](runtime-observability.md)
