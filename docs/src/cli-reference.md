# CLI Reference

This page maps the public `hpc-compose` CLI by workflow. Use [Quickstart](quickstart.md) for the shortest install-and-run path, [Runbook](runbook.md) for real-cluster operations, and [Spec Reference](spec-reference.md) for YAML field behavior.

## Common Flags

| Flag | Use it for | Notes |
| --- | --- | --- |
| `--profile <NAME>` | Select a profile from the project-local settings file | Applies to every command. |
| `--settings-file <PATH>` | Use an explicit settings file | Bypasses upward discovery of `.hpc-compose/settings.toml`. |
| `-f`, `--file <FILE>` | Select the compose file on compose-aware commands | When omitted, `hpc-compose` uses the active context compose file or falls back to `compose.yaml`. |
| `--format json` | Machine-readable output | Preferred on non-streaming commands. `--json` remains available only as a compatibility alias on older machine-readable commands. |

## Authoring and Setup

| Command | Use it for | Notes |
| --- | --- | --- |
| `new` (alias: `init`) | Generate a starter compose file from a built-in template | Use `--list-templates` and `--describe-template <name>` to inspect templates before writing a file. Writing a template requires `--cache-dir`. |
| `setup` | Create or update the project-local settings file | Records compose path, env files, env vars, and binary overrides. |
| `context` | Print the resolved execution context | Shows the selected profile, binaries, interpolation vars, runtime paths, and value sources. |
| `completions` | Generate shell completion scripts | Supports Bash, Zsh, Fish, PowerShell, and Elvish through Clap's completion generator. |

```bash
hpc-compose new --list-templates
hpc-compose new --describe-template minimal-batch
hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml
hpc-compose setup
hpc-compose context --format json
hpc-compose completions zsh
```

## Plan and Run

| Command | Use it for | Notes |
| --- | --- | --- |
| `plan` | Validate and preview the static runtime plan | Recommended before every first run. `--show-script` prints the generated launcher to stdout without writing a file. |
| `validate` | Check YAML shape and field validation | Add `--strict-env` when interpolation fallbacks should fail. |
| `config` | Show the fully interpolated effective config | Use `--format json` when you need stable machine-readable snapshots or resume diffs. |
| `schema` | Print the checked-in JSON Schema | Use it for editor integration and authoring tools. Rust validation remains the semantic source of truth. |
| `inspect` | View the normalized runtime plan | `--verbose` can reveal resolved secrets and final mount mappings. |
| `preflight` | Check host and cluster prerequisites | Use `--strict` when warnings should block a later run. |
| `doctor cluster-report` | Generate a best-effort cluster capability profile | Writes `.hpc-compose/cluster.toml` by default; use `--out -` to print the TOML profile. |
| `doctor mpi-smoke` | Render or run a small MPI probe for one service | Reports requested/advertised MPI types, MPI profile metadata, discovered MPI installs, host MPI binds/env, and rendered `srun`; add `--submit` to consume a Slurm allocation. |
| `doctor fabric-smoke` | Render or run MPI/NCCL/UCX/OFI smoke probes for one MPI service | Use `--checks auto` or a comma-separated list such as `mpi,nccl`; render-only by default, `--submit` consumes a Slurm allocation. |
| `prepare` | Import images and build prepared runtime artifacts | Use `--force` when the base image or prepare inputs changed. |
| `render` | Write the generated launcher script without submitting | Good for reviewing the final batch script. |
| `up` | Run the one-command launch/watch/logs workflow | Preferred normal run on a real cluster. |
| `run` | Launch one service in a fresh one-off allocation | Ignores `depends_on` and follows logs until the one-off command finishes. |

```bash
hpc-compose plan -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
hpc-compose validate -f compose.yaml
hpc-compose config -f compose.yaml
hpc-compose schema > hpc-compose.schema.json
hpc-compose inspect --verbose -f compose.yaml
hpc-compose preflight -f compose.yaml
hpc-compose doctor cluster-report
hpc-compose doctor mpi-smoke -f compose.yaml --service trainer --script-out mpi-smoke.sbatch
hpc-compose doctor mpi-smoke -f compose.yaml --service trainer --submit
hpc-compose doctor fabric-smoke -f compose.yaml --service trainer --checks auto --script-out fabric-smoke.sbatch
hpc-compose doctor fabric-smoke -f compose.yaml --service trainer --checks mpi,nccl --submit
hpc-compose prepare -f compose.yaml
hpc-compose render -f compose.yaml --output job.sbatch
hpc-compose up -f compose.yaml
hpc-compose up --detach --format json -f compose.yaml
hpc-compose run app -- python -m smoke_test
```

### `up` Options

Useful workflow flags:

- `--local` runs a Pyxis/Enroot plan on the current Linux host instead of calling `sbatch`.
- `--detach` submits or launches and returns after tracking metadata is written.
- `--format text|json` is accepted with `--detach` or `--dry-run`.
- `--watch-mode auto|tui|line` selects the live output mode; `--no-tui` is a line-mode alias.
- `--allow-resume-changes` acknowledges an intentional change to resume-coupled config between tracked runs.
- `--resume-diff-only` prints the resume-sensitive config diff without submitting.
- `--script-out <PATH>` keeps a copy of the rendered batch script.
- `--force-rebuild` refreshes imported and prepared artifacts before launch.
- `--skip-prepare` skips image import and prepare reuse checks.
- `--keep-failed-prep` leaves the failed Enroot rootfs behind for inspection.

### `up --local`

`up --local` launches a Pyxis/Enroot plan on the current host instead of calling `sbatch`. It is useful for local authoring and script inspection, not for distributed Slurm execution.

```bash
hpc-compose up --local --dry-run -f compose.yaml
```

Current constraints:

- Linux hosts only
- `runtime.backend: pyxis` only
- single-host specs only
- no distributed or partitioned placement
- no `services.<name>.x-slurm.extra_srun_args`
- no `services.<name>.x-slurm.mpi`
- reservation-related `x-slurm.submit_args` are ignored
- `x-slurm.error` is ignored, and local batch stderr is written into the tracked local batch log

`up --local` follows the tracked local launch immediately, just like `up` does for a submitted job. Add `--detach` when you want to launch and return.

In local mode the batch script also exports `HPC_COMPOSE_BACKEND_OVERRIDE=local`, `HPC_COMPOSE_LOCAL_ENROOT_BIN` pointing to the resolved `enroot` binary, and `HPC_COMPOSE_LOCAL_BIN_DIR` containing a generated `srun` shim. These variables are internal to `hpc-compose` and not intended for direct use in compose specs.

## Tracked Runtime

| Command | Use it for | Notes |
| --- | --- | --- |
| `debug` | Diagnose the latest tracked run | Shows scheduler state, per-service state, batch and service log tails, missing-log hints, and a recommended next command. Add `--preflight` to rerun prerequisite checks. |
| `status` | Summarize scheduler state, the top-level batch log, and failure-policy state | Prefer `--format json` for automation. |
| `ps` | Show a stable per-service runtime snapshot | Useful when you want a point-in-time view instead of the live TUI. |
| `watch` | Reconnect to the live watch UI | Falls back to line-oriented output on non-interactive terminals. |
| `logs` | Print tracked service logs | Add `--follow` for the simplest text-only follower. |
| `stats` | Report tracked runtime metrics and step stats | Supports `--format json`, `--format jsonl`, and `--format csv`. |
| `artifacts` | Export tracked artifact bundles after a run | Use `--bundle <name>` and `--tarball` when needed. |
| `cancel` | Cancel the latest tracked job or an explicit job id | Uses tracked metadata instead of making you retype paths. |
| `down` | Cancel a tracked job and clean tracked state | Supports `--purge-cache` when the tracked snapshot names concrete cache artifacts. |
| `jobs list` | Scan the current repo tree for tracked runs | Start here when you need to rediscover an older run. |
| `clean` | Remove old tracked job directories for one compose context | Use `--dry-run` first when you are unsure. |

```bash
hpc-compose debug -f compose.yaml
hpc-compose debug -f compose.yaml --preflight
hpc-compose jobs list
hpc-compose status -f compose.yaml --format json
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml --watch-mode line
hpc-compose logs -f compose.yaml --service app --follow
hpc-compose stats -f compose.yaml --format jsonl
hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
hpc-compose down -f compose.yaml
hpc-compose cancel -f compose.yaml
hpc-compose clean -f compose.yaml --age 7 --dry-run
```

## Cache Maintenance

| Command | Use it for | Notes |
| --- | --- | --- |
| `cache list` | Inspect cached imported and prepared image artifacts | Works without a compose file. |
| `cache inspect` | Show cache reuse expectations for the current plan | Supports `--service <name>` for one service. |
| `cache prune` | Remove old or unused cache entries | `--age` and `--all-unused` are mutually exclusive. |

```bash
hpc-compose cache list
hpc-compose cache inspect -f compose.yaml --service app
hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>'
hpc-compose cache prune --all-unused -f compose.yaml
```

## Related Docs

- [Examples](examples.md)
- [Execution Model](execution-model.md)
- [Runbook](runbook.md)
- [Spec Reference](spec-reference.md)
