# Artifacts And Resume

Artifacts are collected after a run for export and provenance. Resume state is the canonical live state a later attempt should load. Keep those roles separate.

## Artifact Export

When `x-slurm.artifacts` is enabled, teardown collection writes:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/artifacts/
  manifest.json
  payload/...
```

Export collected payloads after the job finishes:

```bash
hpc-compose artifacts -f compose.yaml
hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
```

`export_dir` is resolved relative to the compose file and expands `${SLURM_JOB_ID}` from tracked metadata. Named bundles are written under `<export_dir>/bundles/<bundle>/`, and provenance JSON is written under `<export_dir>/_hpc-compose/bundles/<bundle>.json`.

The bundle name `default` is reserved for top-level `x-slurm.artifacts.paths`.

## Resume-Aware Runs

When `x-slurm.resume` is enabled, `hpc-compose`:

- mounts the shared resume path into every service at `/hpc-compose/resume`
- injects `HPC_COMPOSE_RESUME_DIR`, `HPC_COMPOSE_ATTEMPT`, and `HPC_COMPOSE_IS_RESUME`
- writes attempt-specific runtime outputs under `.hpc-compose/<jobid>/attempts/<attempt>/`
- keeps `.hpc-compose/<jobid>/{logs,metrics,artifacts,state.json}` pointed at the latest attempt for compatibility

Use the shared resume directory for the canonical checkpoint a restarted run should load next. Treat exported artifacts as retrieval and provenance output after the attempt finishes, not as the primary live resume source.

## Useful Commands

```bash
hpc-compose up --resume-diff-only -f compose.yaml
hpc-compose up --allow-resume-changes -f compose.yaml
hpc-compose artifacts -f compose.yaml
```

## Related Docs

- [Examples](examples.md)
- [Spec Reference](spec-reference.md#x-slurmartifacts)
- [Runtime Observability](runtime-observability.md)
