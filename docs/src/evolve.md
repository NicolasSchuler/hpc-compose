# Spec Metamorphosis

`hpc-compose evolve` is an interactive authoring tutorial. It starts from a minimal valid spec and progressively rewrites the same output file through increasingly realistic HPC workflow features.

The command is safe to run on a laptop or login node:

- it validates and plans candidate specs,
- it writes only the selected compose file,
- it does not prepare images,
- it does not call `sbatch`,
- it does not run `preflight`.

## Canonical Lesson

V1 ships one lesson:

```bash
hpc-compose evolve --describe-lesson progressive-complexity
```

The `progressive-complexity` path contains five valid snapshots:

| Step id | What it teaches | Safe follow-up |
| --- | --- | --- |
| `minimal` | One service and one single-node Slurm allocation | `hpc-compose plan -f compose.yaml` |
| `second-service` | A dependent service and startup ordering | `hpc-compose plan -f compose.yaml` |
| `readiness` | `readiness` plus `depends_on.condition: service_healthy` | `hpc-compose plan --show-script -f compose.yaml` |
| `failure-policy` | `restart_on_failure` with bounded retries and a rolling crash-loop window | `hpc-compose inspect -f compose.yaml` |
| `multi-node-placement` | A two-node allocation with explicit non-overlapping service placement | `hpc-compose plan -f compose.yaml` |

The final step can validate anywhere, but running it requires a Slurm target that can grant a two-node allocation and a runtime backend available on that cluster.

## Interactive Flow

Start the tutorial:

```bash
hpc-compose evolve --output compose.yaml
```

At each step, the command prints:

- a short explanation,
- the concepts being introduced,
- a compact diff from the last accepted spec,
- and the validation summary for the candidate.

Controls:

- `Enter`, `y`, or `a` accepts the step and writes `compose.yaml`.
- `s` skips the current step.
- `q` quits after the last accepted valid spec.
- `?` prints prompt help.

## Transcript Example

```text
$ hpc-compose evolve --output compose.yaml
Step 1/5: Minimal batch spec
Accept this step? [Y/a/s/q/?]
wrote /path/to/compose.yaml

Step 2/5: Add a dependent service
Accept this step? [Y/a/s/q/?]
wrote /path/to/compose.yaml

Step 3/5: Gate on readiness
Accept this step? [Y/a/s/q/?]
wrote /path/to/compose.yaml
```

Inspect the accepted readiness-gated spec:

```bash
hpc-compose plan -f compose.yaml
```

Then continue the tutorial to failure policies and multi-node placement:

```text
Accept this step? [Y/a/s/q/?]
```

For automation or docs examples, accept through a specific step noninteractively:

```bash
hpc-compose evolve --yes --until readiness --format json --output compose.yaml
```

## Non-Goals

- V1 does not mutate arbitrary existing specs.
- V1 is not a full-screen TUI.
- V1 does not submit jobs.

For a fresh single-template scaffold, use [`hpc-compose new`](cli-reference.md#authoring-and-setup). For choosing among the broader runnable examples, use [Examples](examples.md).
