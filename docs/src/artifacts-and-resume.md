# Artifacts and Resume

Artifacts are collected after a run for export and provenance. Resume state is the canonical live checkpoint a later attempt loads on restart. Keep those roles separate: exported checkpoints are retrieval output, while the shared resume path is what a restarted run reads first.

## Artifacts: Collection vs. Export

Artifact handling has **two stages**, and only the first is automatic.

### 1. Collection — automatic, at teardown (compute node)

When `x-slurm.artifacts` is enabled, the in-job teardown collects the declared paths into the tracked runtime directory:

```text
<runtime-root>/<job-id>/artifacts/
  manifest.json
  payload/...
```

For resume-aware runs, the active attempt writes first under `<runtime-root>/<job-id>/attempts/<attempt>/artifacts/`; the top-level `artifacts` path is kept as the latest view.

This stage **only fills the runtime payload — it never writes to `export_dir`.**

### 2. Export — manual, on demand (login node)

Copying the collected payload into the configured `export_dir` is a separate, explicit step. Run it after the job finishes:

```bash
hpc-compose artifacts -f compose.yaml
hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
```

> **`export_dir` is populated only by `hpc-compose artifacts`.** Nothing runs it for you: `down` tears the job down without exporting, and `pull` only prints an `rsync` line that copies the runtime payload to your laptop (it does not touch `export_dir`). If downstream jobs read `<export_dir>/<job-id>`, run `hpc-compose artifacts` before `down`. When an `export_dir` is configured, hpc-compose surfaces this step in the "Next:" hints after `up`, `status`, and `experiment`.

`export_dir` is resolved relative to the compose file and expands `${SLURM_JOB_ID}` from tracked metadata. Named bundles are written under `<export_dir>/bundles/<bundle>/`, and provenance JSON is written under `<export_dir>/_hpc-compose/bundles/<bundle>.json`.

The bundle name `default` is reserved for top-level `x-slurm.artifacts.paths`.

## Resume-Aware Runs

When `x-slurm.resume` is enabled, `hpc-compose`:

- mounts the shared resume path into every service at `/hpc-compose/resume`
- injects `HPC_COMPOSE_RESUME_DIR`, `HPC_COMPOSE_ATTEMPT`, and `HPC_COMPOSE_IS_RESUME`
- writes attempt-specific runtime outputs under `<runtime-root>/<job-id>/attempts/<attempt>/`
- keeps `<runtime-root>/<job-id>/{logs,metrics,artifacts,state.json}` pointed at the latest attempt for compatibility

Use the shared resume directory for the canonical checkpoint a restarted run should load next. Treat exported artifacts as retrieval and provenance output after the attempt finishes, not as the primary live resume source.

## Useful Commands

```bash
hpc-compose up --resume-diff-only -f compose.yaml
hpc-compose up --allow-resume-changes -f compose.yaml
hpc-compose artifacts -f compose.yaml
```

## Related Docs

- [Connect Jobs Across Allocations](cross-job-rendezvous.md)
- [Runtime Observability](runtime-observability.md)
- [Spec Reference](spec-reference.md#x-slurmartifacts)
- [Examples](examples.md)
