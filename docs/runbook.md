# Runbook

This runbook is for adapting `hpc-compose` to a real workload on a Slurm cluster with Enroot and Pyxis.

Commands below assume `hpc-compose` is on your `PATH`. If you are running from a local checkout, replace `hpc-compose` with `target/release/hpc-compose`.

All commands accept `-f` / `--file` to specify the compose spec path. When omitted, it defaults to `compose.yaml` in the current directory. (The `cache prune --all-unused` subcommand requires `-f` explicitly.)

## Before you start

Make sure you have:

- a login node with `enroot`, `srun`, and `sbatch` available,
- Pyxis support in `srun` (`srun --help` should mention `--container-image`),
- a shared filesystem path for `x-slurm.cache_dir`,
- any required local source trees or local `.sqsh` images in place,
- registry credentials available if your cluster or registry requires them.

## Typical path

For a first working run:

1. Run `hpc-compose init --template <name> --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml`, or copy the closest shipped example.
2. Set `x-slurm.cache_dir` if you need an explicit shared cache path, and adjust any cluster-specific resource settings.
3. Run `hpc-compose submit --watch -f compose.yaml`.
4. If that fails, or if you are still adapting the spec, use `validate`, `inspect`, `preflight`, `prepare`, `render`, `status`, or `logs` separately.

## Pick a starting example

| Example | Use it when you need | File |
| --- | --- | --- |
| Dev app | mounted source tree plus a small prepare step | [`examples/dev-python-app.yaml`](../examples/dev-python-app.yaml) |
| Redis worker stack | multi-service launch ordering and readiness checks | [`examples/app-redis-worker.yaml`](../examples/app-redis-worker.yaml) |
| LLM curl workflow | one GPU-backed LLM plus a one-shot `curl` request from a second service | [`examples/llm-curl-workflow.yaml`](../examples/llm-curl-workflow.yaml) |
| LLM curl workflow (home) | the same request flow, but anchored under `$HOME/models` for direct use on a login node | [`examples/llm-curl-workflow-workdir.yaml`](../examples/llm-curl-workflow-workdir.yaml) |
| GPU-backed app | one GPU service plus a dependent application | [`examples/llama-app.yaml`](../examples/llama-app.yaml) |

The fastest path is usually to copy the closest example and adapt it instead of starting from scratch.

You can also let `hpc-compose` scaffold one of these examples directly:

```bash
hpc-compose init --template dev-python-app --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml
```

## 1. Choose `x-slurm.cache_dir` early

Set `x-slurm.cache_dir` to a path that is visible from both the login node and the compute nodes.

```yaml
x-slurm:
  cache_dir: /shared/$USER/hpc-compose-cache
```

Rules:

- Do **not** use `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm`.
- If you leave `cache_dir` unset, the default is `$HOME/.cache/hpc-compose`.
- The default is convenient for small or home-directory workflows, but a shared project or workspace path is usually safer on real clusters.

## 2. Adapt the example to your workload

Start with the nearest example and then change:

- `image`
- `command` / `entrypoint`
- `volumes`
- `environment`
- `x-slurm` resource settings
- `x-enroot.prepare` commands for dependencies or tooling

Recommended pattern:

- Put fast-changing application code in `volumes`.
- Put slower-changing dependency installation in `x-enroot.prepare.commands`.
- Add `readiness` to any service that other services truly depend on.

## 3. Validate the spec

```bash
hpc-compose validate -f compose.yaml
```

Use `validate` first when you are changing:

- field names,
- `depends_on` shape,
- `command` / `entrypoint` form,
- path values,
- `x-slurm` / `x-enroot` blocks.

If `validate` fails, fix that before doing anything more expensive.

## 4. Inspect the normalized plan

```bash
hpc-compose inspect -f compose.yaml
hpc-compose inspect --verbose -f compose.yaml
```

Check:

- service order,
- how images were normalized,
- where runtime artifacts will live,
- whether the planner expects a cache hit or miss,
- whether a prepared image will rebuild on every submit because `prepare.mounts` are present.

`inspect` is the quickest way to confirm that the planner understood your spec the way you intended.

## 5. Run preflight checks

```bash
hpc-compose preflight -f compose.yaml
hpc-compose preflight --verbose -f compose.yaml
```

`preflight` checks:

- required binaries (`enroot`, `srun`, `sbatch`),
- Pyxis container support in `srun`,
- cache directory policy and writability,
- local mount and image paths,
- registry credentials,
- skip-prepare reuse safety when relevant.

If your cluster installs these tools in non-standard locations, pass explicit paths:

```bash
hpc-compose preflight -f compose.yaml --enroot-bin /opt/enroot/bin/enroot --srun-bin /usr/local/bin/srun --sbatch-bin /usr/local/bin/sbatch
```

The same override flags (`--enroot-bin`, `--srun-bin`, `--sbatch-bin`) are available on `prepare` and `submit`.

Use strict mode if you want warnings to fail the command:

```bash
hpc-compose preflight -f compose.yaml --strict
```

## 6. Prepare images on the login node when needed

```bash
hpc-compose prepare -f compose.yaml
```

Use this when you want to:

- build or refresh prepared images before submission,
- confirm cache reuse behavior,
- debug preparation separately from job submission.

Force a refresh of imported and prepared artifacts:

```bash
hpc-compose prepare -f compose.yaml --force
```

## 7. Render the batch script when you need to inspect it

```bash
hpc-compose render -f compose.yaml --output /tmp/job.sbatch
```

This is useful when:

- debugging generated `srun` arguments,
- checking mounts and environment passing,
- reviewing the launch order and readiness waits.

## 8. Submit the job

```bash
hpc-compose submit --watch -f compose.yaml
```

`submit` does the normal end-to-end flow:

1. run preflight unless `--no-preflight` is set,
2. prepare images unless `--skip-prepare` is set,
3. render the script,
4. call `sbatch`.

With `--watch`, `submit` also:

5. records the tracked job metadata under `.hpc-compose/`,
6. polls scheduler state with `squeue` / `sacct` when available,
7. streams tracked service logs as they appear.

Note: `submit` treats preflight **warnings** as non-fatal. If you want warnings to block submission, run `preflight --strict` separately before `submit`.

Useful options:

- `--script-out path/to/job.sbatch` keeps a copy of the rendered script.
- When `--script-out` is omitted, the script is written to `<compose-file-dir>/hpc-compose.sbatch`.
- `--force-rebuild` refreshes imported and prepared artifacts during submit.
- `--skip-prepare` reuses existing prepared artifacts.
- `--keep-failed-prep` keeps the Enroot rootfs around when a prepare step fails.

For the shipped examples, `submit --watch` is usually the only command you need in the normal path. Use the other commands when you need more visibility into planning, environment checks, image preparation, tracked job state, or the generated script.

## 9. Read logs and submission output

After a successful submit, `hpc-compose` prints:

- the rendered script path,
- the cache directory,
- one log path per service.
- the tracked metadata location when a numeric Slurm job id was returned.

Use the tracked helpers for later inspection:

```bash
hpc-compose status -f compose.yaml
hpc-compose logs -f compose.yaml
hpc-compose logs -f compose.yaml --service app --follow
```

`status` also reports the tracked top-level batch log path so early job failures are visible even when a service log was never created.

Runtime logs live under:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/logs/<service>.log
```

That same per-job directory is also mounted inside every container at `/hpc-compose/job`. Use it for small cross-service coordination files when a workflow needs shared ephemeral state.

Slurm may also write a top-level batch log such as `slurm-<jobid>.out`, or to the path configured with `x-slurm.output`. Check that file first when the job fails before any service log appears.

Service names containing non-alphanumeric characters are encoded in the log filename. For example, a service named `my.app` produces `my_x2e_app.log`. Prefer `[a-zA-Z0-9_-]` in service names for readability.

If you used `--script-out`, keep that script with the job logs when debugging cluster behavior.

## 10. Inspect and prune cache artifacts

List cached artifacts:

```bash
hpc-compose cache list
```

Inspect cache state for the current plan:

```bash
hpc-compose cache inspect -f compose.yaml
```

Inspect a single service:

```bash
hpc-compose cache inspect -f compose.yaml --service app
```

Prune old entries by age (in days):

```bash
hpc-compose cache prune --age 14
```

Prune artifacts not referenced by the current plan:

```bash
hpc-compose cache prune --all-unused -f compose.yaml
```

The two strategies (`--age` and `--all-unused`) are mutually exclusive — pick one per invocation.

Use `cache inspect` when you need to answer questions such as:

- which artifact is being reused,
- whether a prepared image came from a cached manifest,
- whether a service rebuilds on every submit because of prepare mounts.

### After upgrading hpc-compose

Cache keys include the tool version, so upgrading `hpc-compose` invalidates all existing cached artifacts. You will see a full rebuild on the next `prepare` or `submit`. To clean up orphaned artifacts after an upgrade:

```bash
hpc-compose cache prune --age 0
```

## Decision guide

### When should I use `volumes`?

Use `volumes` for source code or other files you edit frequently.

### When should I use `x-enroot.prepare.commands`?

Use prepare commands for slower-changing dependencies, tools, or image customization that you want baked into a cached runtime image.

### When should I use `--skip-prepare`?

Only when the prepared artifact already exists and you want to reuse it. `preflight` can warn or fail if reuse is unsafe.

### When should I use `--force-rebuild` or `prepare --force`?

Use them after changing:

- the base image,
- prepare commands,
- prepare environment,
- tooling or dependencies that should invalidate the cached runtime image.

### Why does my service rebuild every time?

If `x-enroot.prepare.mounts` is non-empty, that service intentionally rebuilds on every `prepare` / `submit`.

## Troubleshooting

### `required binary '...' was not found`

Run on a node with the Slurm client tools and Enroot available, or pass the explicit binary path with `--enroot-bin`, `--srun-bin`, or `--sbatch-bin`.

### `srun does not advertise --container-image`

Pyxis support appears unavailable on that node. Move to a supported login node or cluster environment.

### Cache directory errors or warnings

- Errors usually mean the path is not shared or not writable.
- A warning under `$HOME` means the path may work, but a shared workspace path is preferred.

### Missing local mount or image paths

Remember that relative paths resolve from the compose file directory, not from the shell's current working directory.

### A mounted file exists on the host but not inside the container

This is often a symlink issue. If you mount a directory such as `$HOME/models:/models` and `model.gguf` is a symlink whose target lives outside `$HOME/models`, the target may not be visible inside the container. Copy the real file into the mounted directory or mount the directory that contains the symlink target.

### Anonymous pull or registry credential warnings

Add the required credentials before relying on private registries or heavily rate-limited public registries.

### Services start in the wrong order

Use `depends_on` for launch order and `readiness` for actual startup gating. `depends_on` alone does not wait for ports or logs.

When a TCP port opens before the service is fully usable, prefer log-based readiness over TCP readiness.

## Related docs

- [`docs/spec-reference.md`](spec-reference.md)
- [`examples/README.md`](../examples/README.md)
