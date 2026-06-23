# Develop and Smoke-Test Locally

`test`, `dev`, and `tmux` are the local-development command layer. They reuse the same prepare, render, local supervisor, runtime state, and tracking paths as `up`, so a run started by one command remains visible to `status`, `ps`, `logs`, `stats`, `watch`, and `debug`.

## Smoke-Test Specs

Use `test` for finite specs that prove a workflow starts, satisfies readiness gates, and exits cleanly:

```bash
hpc-compose test --local -f examples/dev-python-smoke.yaml
hpc-compose test --submit --time 00:01:00 --timeout 180s -f compose.smoke.yaml
hpc-compose test --submit --format json -f compose.smoke.yaml
```

`test` requires exactly one execution mode:

- `--local` runs the rendered local supervisor on the current host.
- `--submit` calls `sbatch`; it defaults to `--time 00:01:00` and `--timeout 180s`. This is a real-scheduler operation that consumes an allocation, so it needs explicit user approval before running.

A smoke test passes only when every service:

- appears in tracked runtime state,
- launched at least once,
- passed readiness when `readiness` is configured,
- completed successfully.

Services with `failure_policy.mode: ignore` still have to complete successfully for `test` to pass. That makes smoke tests stricter than production runs by design: ignored sidecars are useful operationally, but they should not silently hide a broken spec test.

## Making Long-Running Specs Finite

Production services often run forever. For smoke tests, create a finite variant of the spec or override the service command in a copied file:

```yaml
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -c
      - "import main; print('smoke ok', flush=True)"
```

Keep the same image, mounts, environment, dependencies, and readiness where possible. Change only the command or entrypoint needed to prove startup and exit. If a dependent service uses `condition: service_healthy`, keep the upstream readiness probe real enough to catch wiring mistakes.

## Hot Reload

`dev` is local-only:

```bash
hpc-compose dev -f examples/dev-python-app.yaml
hpc-compose dev -f compose.yaml --watch-paths ./src --debounce-ms 500
```

It infers watch roots from host directories mounted through service `volumes`. File mounts, container-only paths, cache paths, missing paths, and non-directory paths are ignored. `--watch-paths` adds an explicit directory and restarts every service when it changes.

File changes write restart requests into the tracked run's dev control directory. The local supervisor handles those requests as development restarts, so readiness and completion state reset for the affected service without consuming `failure_policy.restart_on_failure` counters.

By default, Ctrl-C stops the local supervisor. Add `--keep-running` when you want to leave the tracked local run alive after exiting the watch loop.

## Tmux Dashboard

`tmux` is a log dashboard, not a process supervisor:

```bash
hpc-compose tmux -f compose.yaml
hpc-compose tmux -f compose.yaml --job-id local-123
hpc-compose tmux -f compose.yaml --session demo --no-attach
```

Without `--job-id`, it launches a new local run. With `--job-id`, it attaches to an existing tracked local run. Each pane tails one service log with `tail -F`, and pane titles use service names. Use `--no-attach` when running from a non-interactive terminal or CI smoke check.

## Shared Local Constraints

`up --local`, `test --local`, `dev`, and `tmux` share the same current constraints:

- Linux hosts only
- `runtime.backend: pyxis` only
- Pyxis-compatible Enroot tooling on the host
- single-host specs only
- no distributed or partitioned placement
- no service-level MPI
- no Slurm arrays or scheduler dependencies

Use these commands to author and debug single-host launch behavior. Use `test --submit` or `up` on a Slurm login node for real scheduler behavior, or use the [Local Slurm Dev Cluster](local-slurm-dev-cluster.md) from a source checkout when you want a throwaway real `sbatch` smoke test without a cluster login.

## Example Recipe

The source-mounted app in `examples/dev-python-app.yaml` is intentionally long-running, so it is a good `dev` target:

```bash
hpc-compose dev -f examples/dev-python-app.yaml
hpc-compose tmux -f examples/dev-python-app.yaml --no-attach
```

The companion `examples/dev-python-smoke.yaml` keeps the same mounted source pattern but uses a finite command:

```bash
hpc-compose test --local -f examples/dev-python-smoke.yaml
hpc-compose test --submit --time 00:01:00 -f examples/dev-python-smoke.yaml
```

## Related Docs

- [Operate a Real Cluster Run](runbook.md)
- [Local Slurm Dev Cluster](local-slurm-dev-cluster.md)
- [Monitor a Run](runtime-observability.md)
- [Troubleshoot a Failed Run](troubleshooting.md)
- [Manage the Cache and Clean Up](cache-management.md)
- [CLI Reference](cli-reference.md)
