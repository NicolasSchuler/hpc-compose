# Running Compose-Style Multi-Service Workflows on Slurm

This is the canonical explainer for `hpc-compose`.

`hpc-compose` exists because two common approaches leave a gap:

- plain `sbatch` scripts give you control, but multi-service coordination, startup ordering, and repeatability stay ad hoc
- Docker Compose is familiar, but its networking and orchestration assumptions do not map cleanly to one Slurm allocation

`hpc-compose` takes the narrow path between them: a Compose-like authoring model that still produces one inspectable Slurm job.

## The Pain in Current Slurm Workflows

Once a job stops being a single process, the friction climbs quickly:

- helper services need explicit startup ordering
- cluster-specific environment setup gets mixed into hand-written shell
- debugging starts from generated state you never inspected beforehand
- repeated workflows drift because the real behavior lives across scripts, notes, and local conventions

This is especially common in research ML and HPC-adjacent work where one job may need:

- a serving process plus a client
- a database plus a worker
- a training step plus checkpoint export and resume handling

## Why Docker Compose Does Not Fit Slurm Directly

Docker Compose is good at expressing a small multi-service application on one machine. Slurm solves a different problem: scheduling one batch allocation onto shared cluster resources.

That mismatch shows up in exactly the features `hpc-compose` leaves out:

- `ports`
- custom `networks`
- Compose `restart`
- `deploy`
- broad runtime compatibility with arbitrary Compose features

Those omissions are deliberate. The point is not to emulate all of Compose on a cluster. The point is to keep a familiar authoring shape for the subset that maps cleanly to one Slurm job.

## The Narrow Execution Model

`hpc-compose` keeps the execution model explicit:

```text
compose-like spec
      |
      +--> validate / inspect / render on the submission host
      |
      +--> one generated batch script
                |
                v
          one Slurm allocation
                |
                +--> primary-node helper services
                +--> optional allocation-wide distributed service
                +--> shared /hpc-compose/job scratch for coordination
```

This gives you a few important properties:

- one inspectable unit of submission
- one obvious place to look when the job fails
- one explicit product boundary instead of hidden orchestration behavior

## One Real Example

[`app-redis-worker.yaml`](example-source.md#app-redis-worker) is a good example of the intended shape:

- one Redis service
- one dependent worker service
- TCP readiness gating before the worker starts
- both services living inside the same allocation

That is awkward to hand-roll repeatedly with cluster scripts alone, but it does not justify a full orchestrator. This is the exact middle ground `hpc-compose` targets.

If you want the smallest possible first run, start with [`minimal-batch.yaml`](example-source.md#minimal-batch). If you want the smallest concrete inference flow, start with [`llm-curl-workflow-workdir.yaml`](example-source.md#llm-curl-workflow-workdir).

## Why the Inspectable Path Matters

The authoring flow is designed to answer the practical questions before you submit:

```bash
hpc-compose validate -f compose.yaml
hpc-compose inspect -f compose.yaml
hpc-compose render --output job.sbatch -f compose.yaml
```

That lets you confirm:

- whether the spec is valid
- what service order will run
- what image and cache behavior the planner inferred
- what batch script you are actually handing to Slurm

For a Slurm-first tool, that inspectability matters more than feature breadth.

## When Not To Use `hpc-compose`

Do not use `hpc-compose` when you need:

- custom container networking
- broad Docker Compose compatibility
- a long-running orchestration control plane
- arbitrary cross-node service placement beyond one distributed service plus primary-node helpers

If that list rules out your workload, that is not a failure of the tool. It is the intended product boundary.

## Read Next

- [Quickstart](quickstart.md)
- [Examples](examples.md)
- [Execution Model](execution-model.md)
- [Supported Slurm Model](supported-slurm-model.md)
