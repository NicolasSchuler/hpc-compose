# Troubleshoot a Failed Run

Use this page when the safe authoring path worked but the first real cluster run failed.

For background on Slurm allocations, `sbatch`, `srun`, Pyxis, and Enroot, see [Slurm And Container Basics](slurm-container-basics.md). For HAICORE-specific storage and runtime checks, see [HAICORE Guide](haicore-guide.md).

## First Triage

```bash
hpc-compose validate -f compose.yaml
hpc-compose validate -f compose.yaml --strict-env
hpc-compose plan --verbose -f compose.yaml
hpc-compose lint --fix --dry-run -f compose.yaml
hpc-compose debug -f compose.yaml --preflight
```

`plan --verbose` can print resolved environment values and final mount mappings. Treat its output as sensitive when the spec contains secrets. `validate` and `lint` emit "Did you mean ..." suggestions for misspelled service keys and dependency conditions. `lint --fix --dry-run` previews auto-fixes (for example, making an implicit `depends_on` condition explicit) without writing. `debug` is read-only unless `--preflight` is passed; with `--preflight`, it reruns prerequisite checks and includes those findings in the triage report.

For opaque CLI failures, add global `-v` or `--debug`; set `RUST_LOG` when you need an explicit tracing filter such as `RUST_LOG=hpc_compose=debug`. JSON-output commands keep stdout machine-readable and emit warning notices as one JSON object per line on stderr.

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
| `dev` reports no watchable source directories | Services only mount files, missing paths, cache paths, or container-only paths. | Mount the source as a host directory or pass `hpc-compose dev --watch-paths ./src -f compose.yaml`. |
| Readiness never passes | Probe target, pattern, host, or dependency timing does not match the real service. | Inspect the service log with `logs --service <name>` and try a finite `hpc-compose test --local` or short `test --submit` spec. |
| Smoke test times out | The spec is long-running, readiness blocks forever, or the scheduler job never reaches terminal state. | Make the smoke spec finite, lower service readiness timeouts, and use `--format json` to inspect the failed phase and service reason. |
| `tmux` is unavailable or attach fails | `tmux` is not installed or the shell is non-interactive. | Install `tmux`, pass `--tmux-bin <PATH>`, or create the dashboard with `--no-attach`. |
| Local mode is unsupported | Local workflows require a Linux host with Pyxis-compatible Enroot behavior. | Use authoring commands on non-Linux hosts, then run `test --submit` or `up` on a supported Slurm login node. |
| `up --remote` reports the remote `hpc-compose` is missing or older | The login node has no `hpc-compose` on `PATH` or `~/.local/bin`, or has an older version than your local one. | Default `--remote-install auto` downloads and installs the newest release into `~/.local/bin` over the same SSH connection. On a locked-down/air-gapped node, use `--remote-install never` and install manually with the printed one-liner. |
| `up --remote` job cannot see part of your source tree | The compose file lives in a subdirectory with no repo-root settings, so only that subdir was staged (watch for the "staged only a subdir" warning). | Put `.hpc-compose/settings.toml` at the repo root (or run `hpc-compose setup` there) so the whole source tree is staged. |
| `--skip-prepare` reports the runtime image is not prepared | `--skip-prepare` reuses an existing image cache and builds nothing; on a first run (or after cache eviction) the image does not exist yet. | Run `hpc-compose up` or `hpc-compose prepare` once without `--skip-prepare`, then reuse the cache with `--skip-prepare`. |
| enroot import fails at `Creating squashfs filesystem...` with `Stale file handle` | The default extraction scratch (`<cache_dir>/enroot/tmp`) is on a shared NFS/Lustre/GPFS filesystem, where the extract-then-`mksquashfs` import triggers ESTALE. | Point the prepare scratch at node-local storage (opt-in): set `x-slurm.enroot_temp_dir` in the spec (e.g. `/tmp/${USER}-hpc-compose-enroot`), `cache.enroot_temp_dir` in `.hpc-compose/settings.toml`, or `HPC_COMPOSE_ENROOT_TEMP_DIR`. `hpc-compose` retries once on a clean temp dir before failing. |
| prepare command fails when a `prepare.mounts` source is on a network filesystem | The prepare step binds that source on the login node, where a network/shared-FS mount can fail. | Use a dependency-only prepare (install deps into the image, mount the source as a runtime `volumes` entry), or ensure the mount source is stable on the login node. `examples/dev-python-app.yaml` shows the pattern. |
| enroot import fails with `manifest unknown` / `manifest not found` / `401 Unauthorized` | The image tag does not exist on the registry (often a typo, or a tag that was never published), or the pull needs credentials. | Verify the reference exists before submitting: `skopeo inspect docker://<image>` or `docker manifest inspect <image>`. `hpc-compose lint` (HPC007) warns about mutable/`latest` tags but cannot confirm a tag exists on the registry; for private images configure registry credentials. |

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
hpc-compose doctor readiness -f compose.yaml --service api --run --log-file .hpc-compose/<job-id>/logs/api.log
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

Cleaning up accumulated tracked job metadata and logs is covered in [Manage the Cache and Clean Up](cache-management.md#clean-up-old-tracked-runs).

## Related Docs

- [Operate a Real Cluster Run](runbook.md)
- [Monitor a Run](runtime-observability.md)
- [Manage the Cache and Clean Up](cache-management.md)
- [Develop and Smoke-Test Locally](development-workflow.md)
- [Slurm And Container Basics](slurm-container-basics.md)
- [HAICORE@KIT Guide](haicore-guide.md)
