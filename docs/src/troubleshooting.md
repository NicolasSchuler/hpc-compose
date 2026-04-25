# Troubleshooting

Use this page when the safe authoring path worked but the first real cluster run failed.

## First Triage

```bash
hpc-compose validate -f compose.yaml
hpc-compose validate -f compose.yaml --strict-env
hpc-compose plan --verbose -f compose.yaml
hpc-compose debug -f compose.yaml --preflight
```

`plan --verbose` can print resolved environment values and final mount mappings. Treat its output as sensitive when the spec contains secrets. `debug` is read-only unless `--preflight` is passed; with `--preflight`, it reruns prerequisite checks and includes those findings in the triage report.

## Common Symptoms

| Symptom | Likely cause | Next step |
| --- | --- | --- |
| `required binary '...' was not found` | Selected backend or Slurm client tool is not on `PATH`. | Run `debug --preflight`; pass `--enroot-bin`, `--apptainer-bin`, `--singularity-bin`, `--srun-bin`, or `--sbatch-bin` as needed. |
| `srun does not advertise --container-image` | Pyxis support is unavailable or not loaded. | Move to a supported login node, load the site module, or choose another backend. |
| Cache directory warning/error | `x-slurm.cache_dir` is not shared, writable, or policy-safe. | Choose a shared project/work/scratch path and rerun `debug --preflight`. |
| Missing local mount or image path | Relative paths are resolved from the compose file directory. | Check paths relative to the copied `compose.yaml`. |
| Mounted symlink exists on the host but fails in the container | The symlink target is outside the mounted directory. | Copy the real file into the mounted directory or mount the target directory. |
| Anonymous pull or registry warning | Registry credentials are missing or rate limits apply. | Configure credentials before relying on private or rate-limited images. |
| Services start in the wrong order | Dependency condition or readiness is too weak. | Use `service_healthy` with `readiness`, or `service_completed_successfully` for DAG stages. |
| No service logs exist | The batch script failed before launching a service. | Use `debug` to see scheduler state, the tracked top-level batch log tail, and missing-log hints. |

## Readiness Issues

Use `depends_on` with `condition: service_healthy` when a dependent must wait for a dependency's readiness probe. Plain list form means `service_started`.

Use `condition: service_completed_successfully` for one-shot DAG stages where the next service should start only after the previous stage exits with status `0`, such as preprocess -> train -> postprocess.

When a TCP port opens before the service is fully usable, prefer HTTP or log-based readiness over TCP readiness.

## Preview A Run

Use `plan` for the static preview. It never prepares images, runs preflight, calls `sbatch`, or writes `hpc-compose.sbatch`:

```bash
hpc-compose plan --show-script -f compose.yaml
```

Use `up --dry-run` only when you intentionally want to exercise preflight, prepare, and render without calling `sbatch`:

```bash
hpc-compose up --dry-run -f compose.yaml
```

## Clean Old Tracked Runs

Tracked job metadata and logs accumulate in `.hpc-compose/`. Preview cleanup before deleting:

```bash
hpc-compose jobs list --disk-usage
hpc-compose clean -f compose.yaml --age 7 --dry-run
hpc-compose clean -f compose.yaml --age 7
```

## Related Docs

- [Quickstart](quickstart.md#7-if-the-first-cluster-run-fails)
- [Runbook](runbook.md)
- [Cluster Profiles](cluster-profiles.md)
- [Runtime Observability](runtime-observability.md)
