# Operations and recovery

Read this reference for tracked failures, observation, artifacts, resume, preemption, sweeps, scoring, right-sizing, or cleanup.

## Observe without leaking data

Inspect output schemas before consuming JSON. Prefer bounded, non-following machine-readable commands where available:

- `status` for scheduler plus tracked state;
- `ps` for per-service state;
- `stats` for metrics and accounting;
- `checkpoints` for local attempt/requeue history;
- `experiment show` for aggregate run evidence.

Logs, debug reports, generated scripts, verbose diagnostics, connection hints, tokens, paths, and provenance bundles can be sensitive. Do not paste or ingest them wholesale. Ask for the smallest redacted evidence needed, such as a bounded error class or a user-provided sanitized excerpt.

Polling remains an effect even when it is read-only. Avoid tight scheduler loops and use bounded timeouts.

## Evidence ladder

At each stage record symptom, evidence, interpretation, fix, and expected next signal:

1. Invalid spec: use `validate` JSON and fix the exact field or invariant.
2. Host/site mismatch: use ordinary preflight evidence; filesystem probes require quota.
3. Pending job: use status and queue evidence; observations are advisory, not capacity guarantees.
4. Runtime/readiness failure: use service state and the smallest sanitized log evidence; inspect readiness dependencies and timeouts.
5. Recovery: export configured artifacts to an authorized local destination before destructive cleanup.

Do not skip directly from a symptom to a speculative refactor. Preserve the failed run until the evidence and recovery artifacts are sufficient.

## Artifacts and resume

`artifacts` and `experiment bundle` write local output and require a scoped destination. Bundles may contain submitted scripts, effective config, paths, and provenance; do not ingest them unredacted.

Before resuming, compare the current effective config with the tracked snapshot. Treat checkpoint path, image, entrypoint, world size/topology, model/optimizer layout, and resume hooks as sensitive invariants. Use one canonical checkpoint path shared by attempts and export it before cleanup.

`test --preemption` is destructive: it submits, signals, and requeues a job. It is a production-readiness drill only after explicit authorization and with bounded walltime, checkpoint grace, and artifact verification.

## Sweeps

`sweep submit --resume` can submit trials that never received a job; resume is not read-only. `sweep submit --resume --dry-run` must retain no submit/cancel/delete effects. `sweep observe` writes parsed objectives to the manifest; with `--watch --stop-when` it can cancel remaining trials. `sweep stop` is destructive.

## Telemetry and recommendations

Display partial observations as partial. Before accepting score, watchdog, idle, or right-sizing conclusions, check collector coverage and confidence. Unknown or degraded multi-node evidence must not justify allocation-wide idle conclusions or GPU-reduction recommendations.

Queue weather and canaries are observations, not reservations or performance guarantees. Prefer a discriminating short run and preserve the original request until evidence is complete.

## Cleanup boundary

`down`, `cancel`, `clean`, `cache prune`, `rendezvous prune`, `sweep stop`, and `workspace release` are never implied by a request to inspect, debug, recover, or finish documentation. `--yes` only suppresses a prompt; it does not grant authorization.

Before any destructive action, name the exact job/cache/workspace/path scope, state what will be retained or exported, and obtain explicit authorization for that invocation.
