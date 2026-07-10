# Worked Failure Recovery

This narrative follows one spec through five distinct failures. Each stage keeps
the previous fix, so the evidence advances from authoring to environment,
scheduler, runtime, and recovery. Use [Troubleshooting](troubleshooting.md) as
the symptom lookup reference after you understand the flow.

## Starting Point

The workload should start a small HTTP service and retain diagnostics if the
run fails:

```yaml
name: recovery-demo

runtime:
  backend: pyxis

x-slurm:
  time: "00:05:00"
  cpus_per_task: 2
  mem: 2G
  cache_dir: ${CACHE_DIR}
  artifacts:
    collect: on_failure
    export_dir: ./recovered/${SLURM_JOB_ID}
    paths:
      - /hpc-compose/job/diagnostics/**

services:
  api:
    image: python:3.12-slim
    command:
      - /bin/sh
      - -lc
      - |
        mkdir -p /hpc-compose/job/diagnostics
        python --version > /hpc-compose/job/diagnostics/python.txt 2>&1
        exec python -m http.server 8000
    readiness:
      tcp:
        port: 8000
      timeout_seconds: 30
```

The snippets below show only the field that changes.

## Stage 1: Invalid Field

An author adds a familiar Docker Compose key:

```yaml
services:
  api:
    ports:
      - "8000:8000"
```

| Step | Result |
| --- | --- |
| Symptom | `validate` exits before planning and names unsupported `services.api.ports`. |
| Evidence | `hpc-compose --offline validate -f compose.yaml` |
| Interpretation | This is not legal hpc-compose YAML. One allocation uses explicit host-network semantics; Docker port publishing is outside scope. |
| Fix | Remove `ports`; keep the explicit readiness port and use `reach`/SSH tunneling when external access is needed. |
| Expected next signal | `validate` succeeds and `plan --format json` contains service `api`. |

No Slurm contact or quota is involved. Do not move to preflight until the static
contract is valid.

## Stage 2: Preflight Finds Node-Local Storage

To get past a missing path quickly, the cache was changed to:

```yaml
x-slurm:
  cache_dir: /tmp/hpc-compose-cache
```

| Step | Result |
| --- | --- |
| Symptom | Strict preflight rejects or warns about an unsafe node-local cache. |
| Evidence | `hpc-compose preflight --strict --format json -f compose.yaml` |
| Interpretation | Image preparation occurs before the job, while compute-node runtime must see the result later. `/tmp` does not establish that shared contract. |
| Fix | Allocate or locate approved shared storage, create the directory, persist it through the selected profile, and restore `cache_dir: ${CACHE_DIR}`. |
| Expected next signal | Strict preflight reports no cache policy failure. With explicit quota approval, `preflight --fs-probes` confirms compute-node visibility. |

The filesystem probe submits a tiny job. The ordinary strict check does not.

## Stage 3: The Job Is Pending

Submission now succeeds:

```bash
hpc-compose up --detach --format json -f compose.yaml
hpc-compose status --format json -f compose.yaml
```

| Step | Result |
| --- | --- |
| Symptom | The tracked job remains `PENDING`; no service log exists yet. |
| Evidence | Status JSON includes the scheduler reason. `hpc-compose weather --format json` provides a separate advisory queue snapshot. |
| Interpretation | A reason such as `Priority` or `Resources` means the scheduler accepted the job but has not started it. Queue observations are not reservations or start-time guarantees. |
| Fix | Usually wait. If policy permits, deliberately reduce geometry/walltime or use `when`; do not submit duplicate copies to “try again.” |
| Expected next signal | The same job id moves to `RUNNING`, then service state and logs appear. |

`status` and `weather` read scheduler state but do not create another allocation.
The pending job begins consuming compute quota only when Slurm starts it.

## Stage 4: Runtime and Readiness Disagree

Suppose the command accidentally serves on `8001` while readiness still checks
`8000`:

```yaml
command: /bin/sh -lc 'mkdir -p /hpc-compose/job/diagnostics; python --version > /hpc-compose/job/diagnostics/python.txt 2>&1; exec python -m http.server 8001'
readiness:
  tcp:
    port: 8000
  timeout_seconds: 30
```

| Step | Result |
| --- | --- |
| Symptom | Slurm runs the batch job, the process starts, but readiness times out and the tracked run fails. |
| Evidence | `hpc-compose debug -f compose.yaml` joins scheduler state, launcher/service state, and bounded log tails. `doctor readiness -f compose.yaml --service api` explains the normalized probe. |
| Interpretation | Backend and scheduling succeeded. The application contract is inconsistent: producer port `8001`, consumer probe `8000`. |
| Fix | Make command and readiness use the same port, then run a finite `test --submit` before another production launch. |
| Expected next signal | Readiness becomes ready, the finite test completes, and a later normal run reaches `COMPLETED`. |

The failed run consumed quota while it waited for readiness. A short explicit
timeout bounded the cost.

## Stage 5: Recover On-Failure Artifacts

The failed attempt already used `collect: on_failure`, so teardown should have
copied `/hpc-compose/job/diagnostics/**` into tracked artifact state.

```bash
hpc-compose status --verify --format json -f compose.yaml
hpc-compose artifacts --tarball -f compose.yaml
```

| Step | Result |
| --- | --- |
| Symptom | The application failed, but the diagnostic payload is needed after the allocation exited. |
| Evidence | `status --verify` checks tracked artifact metadata; `artifacts` prints the export manifest and destination. |
| Interpretation | Collection during teardown and later export are separate. A missing payload is recovery evidence, not proof that the application never wrote it. |
| Fix | Inspect the tracked manifest, source path, collect policy, and export destination. Keep canonical checkpoints outside the exported bundle. |
| Expected next signal | `./recovered/<job-id>/` contains bundle metadata and the collected diagnostic file; the tarball can be moved to durable storage. |

Artifact export is a local/shared-storage write and does not submit compute. Do
not delete tracked runtime state or release the workspace until the recovered
payload and canonical resume data are verified.

## What the Sequence Established

```text
validate ─► preflight ─► status/weather ─► debug/readiness ─► verify/artifacts
 syntax       environment       queue          runtime          recovery
```

Each command narrows the failure boundary. Repeating submission before that
boundary is understood adds cost without adding discriminating evidence.

## Related Docs

- [Quickstart](quickstart.md)
- [Troubleshooting](troubleshooting.md)
- [Runtime Observability](runtime-observability.md)
- [Artifacts and Resume](artifacts-and-resume.md)
- [Production Readiness](production-readiness.md)
