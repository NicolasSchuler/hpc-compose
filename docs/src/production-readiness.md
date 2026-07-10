# Production Readiness

Use this checklist before a long, costly, preemptible, secret-bearing, or
publication-relevant run. It assumes the [Quickstart](quickstart.md) has already
succeeded on the target site.

Labels state the operational boundary in text as well as color:

- <span class="risk-label safe">NO ALLOCATION</span> does not spend compute quota.
- <span class="risk-label quota">ALLOCATION-CONSUMING</span> submits or uses Slurm resources.
- <span class="risk-label destructive">DESTRUCTIVE</span> removes or releases state and requires a separate decision.

## Images and Provenance

- [ ] <span class="risk-label safe">NO ALLOCATION</span> Pin immutable image
  digests or release tags. Run `hpc-compose lint -f compose.yaml` and resolve
  `HPC007` rather than treating a mutable tag as reproducible provenance.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Record the exact
  `hpc-compose --version`, effective config, source revision, and dirty-tree
  state needed to explain the run. Prefer tracked submit-time snapshots over a
  later copy of the working file.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Review generated
  script provenance through `explain` or a trusted local owner-only script file;
  do not paste unredacted scripts or verbose plans into tickets or chats.

## Shared Storage

- [ ] <span class="risk-label safe">NO ALLOCATION</span> Put cache, runtime
  state, canonical resume data, and artifact destinations on storage whose role
  and lifetime match the workflow. Node-local temporary storage is only for
  data created and consumed within the allocation.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Create every host bind
  and destination directory before submission, confirm login-node permissions,
  and check workspace expiry and quota.
- [ ] <span class="risk-label quota">ALLOCATION-CONSUMING</span> Run
  `hpc-compose preflight --strict --fs-probes -f compose.yaml` to verify
  compute-node visibility, rename behavior, and headroom. A login-node write
  test is not equivalent.

## Secrets and Generated Artifacts

- [ ] <span class="risk-label safe">NO ALLOCATION</span> Source secrets from
  approved environment or owner-readable files. Keep values out of YAML,
  commands, image references, job names, and artifact paths.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Confirm `context`,
  `config`, JSON planning, logs, and debug output stay redacted. Never use
  `--show-values` in shared captures.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> When a script or
  evidence bundle must be written, use an owner-only destination and inspect it
  locally. Do not read secret-bearing output back into an agent conversation.

## Timeouts, Readiness, and Finite Behavior

- [ ] <span class="risk-label safe">NO ALLOCATION</span> Set realistic Slurm
  walltime, service readiness timeouts, and finite-test timeouts. A missing
  timeout can turn a readiness bug into an expensive idle allocation.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Make readiness test the
  dependency's actual contract (port, HTTP response, log signal, or command),
  not merely process existence.
- [ ] <span class="risk-label quota">ALLOCATION-CONSUMING</span> Run a short
  `hpc-compose test --submit --time 00:02:00 -f compose.smoke.yaml` with
  production-equivalent backend, mounts, and readiness before scaling walltime
  or geometry.

## Artifacts and Recovery

- [ ] <span class="risk-label safe">NO ALLOCATION</span> Define artifact paths
  under `/hpc-compose/job`, an approved export directory, and a `collect` policy
  that covers the failure modes you need to diagnose.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Keep a canonical
  checkpoint/resume path separate from exported evidence bundles. Export is a
  copy-out workflow; it is not the resume source.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Rehearse recovery from
  a prior small failed run with `hpc-compose artifacts` and verify the resulting
  manifest and payload before trusting the production failure path.

## Resume and Preemption

- [ ] <span class="risk-label safe">NO ALLOCATION</span> Run
  `hpc-compose up --resume-diff-only -f compose.yaml`; review every
  resume-sensitive difference. Use `--allow-resume-changes` only for an
  intentional, understood change.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Verify the canonical
  checkpoint path is shared, attempt-aware, writable, and not an expiring
  temporary directory.
- [ ] <span class="risk-label quota">ALLOCATION-CONSUMING</span> For a
  requeue/preemption contract, run `hpc-compose test --preemption -f
  compose.yaml`. It submits, signals, and requeues a job; normal submission
  approval is not enough unless it explicitly covers this drill.

## Telemetry Evidence

- [ ] <span class="risk-label safe">NO ALLOCATION</span> Confirm metrics are
  enabled at a useful interval and the required CPU/GPU tools exist before the
  long run.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> After the smoke run,
  read collector coverage: `allocation` scope with all expected nodes observed
  is different from `batch_node`, `unknown`, or degraded coverage.
- [ ] <span class="risk-label safe">NO ALLOCATION</span> Treat partial metrics
  as partial. Do not accept allocation-wide idle conclusions, watchdog action,
  or GPU-reduction advice from degraded or unknown multi-node evidence.

## Cleanup Plan

- [ ] <span class="risk-label safe">NO ALLOCATION</span> Preview cache and state
  cleanup with `cache prune`/`clean --dry-run`; record what must be retained for
  reproducibility and recovery.
- [ ] <span class="risk-label destructive">DESTRUCTIVE</span> Run cache pruning,
  `clean`, `down`, rendezvous pruning, or `workspace release` only as a separate
  confirmed operation after artifacts and canonical checkpoints are safe.

## Go / No-Go

Proceed only when the static plan is understood, strict preflight passes, the
finite smoke test behaves as expected, recovery was exercised, and every
allocation-consuming check above was explicitly authorized. A warning you
cannot interpret is a no-go signal, not documentation noise.

## Related Docs

- [Quickstart](quickstart.md)
- [Artifacts and Resume](artifacts-and-resume.md)
- [Use Secrets](secrets.md)
- [Right-Size With Canary Runs](canary-runs.md)
- [Runtime Observability](runtime-observability.md)
