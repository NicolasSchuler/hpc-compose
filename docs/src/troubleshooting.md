# Troubleshooting

Use this page when the safe authoring path worked but the first real cluster run failed.

For background on Slurm allocations, `sbatch`, `srun`, Pyxis, and Enroot, see [Slurm And Container Basics](slurm-container-basics.md). For HAICORE-specific storage and runtime checks, see [HAICORE Guide](haicore-guide.md).

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
| Cache directory warning/error | The resolved cache directory is not shared, writable, or policy-safe. | Choose a shared project/work/scratch path through `x-slurm.cache_dir` or `setup --cache-dir`, then rerun `debug --preflight`. |
| Missing local mount or image path | Relative paths are resolved from the compose file directory. | Check paths relative to the copied `compose.yaml`. |
| Mounted symlink exists on the host but fails in the container | The symlink target is outside the mounted directory. | Copy the real file into the mounted directory or mount the target directory. |
| Anonymous pull or registry warning | Registry credentials are missing or rate limits apply. | Configure credentials before relying on private or rate-limited images. |
| Services start in the wrong order | Dependency condition or readiness is too weak. | Use `service_healthy` with `readiness`, or `service_completed_successfully` for DAG stages. |
| No service logs exist | The batch script failed before launching a service. | Use `debug` to see scheduler state, the tracked top-level batch log tail, and missing-log hints. |
| `dev` reports no watchable source directories | Services only mount files, missing paths, cache paths, or container-only paths. | Mount the source as a host directory or pass `hpc-compose dev --watch-path ./src -f compose.yaml`. |
| Readiness never passes | Probe target, pattern, host, or dependency timing does not match the real service. | Inspect the service log with `logs --service <name>` and try a finite `hpc-compose test --local` or short `test --submit` spec. |
| Smoke test times out | The spec is long-running, readiness blocks forever, or the scheduler job never reaches terminal state. | Make the smoke spec finite, lower service readiness timeouts, and use `--format json` to inspect the failed phase and service reason. |
| `tmux` is unavailable or attach fails | `tmux` is not installed or the shell is non-interactive. | Install `tmux`, pass `--tmux-bin <PATH>`, or create the dashboard with `--no-attach`. |
| Local mode is unsupported | Local workflows require a Linux host with Pyxis-compatible Enroot behavior. | Use authoring commands on non-Linux hosts, then run `test --submit` or `up` on a supported Slurm login node. |

## Readiness Issues

Use `depends_on` with `condition: service_healthy` when a dependent must wait for a dependency's readiness probe. Plain list form means `service_started`.

Use `condition: service_completed_successfully` for one-shot DAG stages where the next service should start only after the previous stage exits with status `0`, such as preprocess -> train -> postprocess.

When a TCP port opens before the service is fully usable, prefer HTTP or log-based readiness over TCP readiness.

Inspect the normalized readiness probe without starting or submitting anything:

```bash
hpc-compose doctor readiness -f compose.yaml --service api
```

If the service is already running, tunneled, or otherwise reachable from the current host, run the same probe host-side:

```bash
hpc-compose doctor readiness -f compose.yaml --service api --run
hpc-compose doctor readiness -f compose.yaml --service api --run --log-file .hpc-compose/12345/logs/api.log
```

`doctor readiness --run` does not launch services, prepare images, or call Slurm. It only checks the selected readiness target from the current host, which makes it useful before testing a dependent service or while debugging an already tracked run.

For `hpc-compose test`, readiness failures are terminal smoke-test failures. A service with configured readiness must become healthy and then complete successfully; ignored sidecars are still expected to pass in a smoke spec.

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
- [Slurm And Container Basics](slurm-container-basics.md)
- [HAICORE Guide](haicore-guide.md)
- [Runbook](runbook.md)
- [Cluster Profiles](cluster-profiles.md)
- [Runtime Observability](runtime-observability.md)
