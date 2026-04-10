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
| `new` | Generate a starter compose file from a built-in template | Use `--list-templates` and `--describe-template <name>` to inspect templates before writing a file. |
| `setup` | Create or update the project-local settings file | Records compose path, env files, env vars, and binary overrides. |
| `context` | Print the resolved execution context | Shows the selected profile, binaries, interpolation vars, runtime paths, and value sources. |

```bash
hpc-compose new --list-templates
hpc-compose new --describe-template minimal-batch
hpc-compose new --template minimal-batch --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml
hpc-compose setup
hpc-compose context --format json
```

## Submission and Planning

| Command | Use it for | Notes |
| --- | --- | --- |
| `validate` | Check YAML shape and field validation | Add `--strict-env` when interpolation fallbacks should fail. |
| `config` | Show the fully interpolated effective config | Use `--format json` when you need stable machine-readable snapshots or resume diffs. |
| `inspect` | View the normalized runtime plan | `--verbose` can reveal resolved secrets and final mount mappings. |
| `preflight` | Check host and cluster prerequisites | Use `--strict` when warnings should block a later submit. |
| `prepare` | Import images and build prepared runtime artifacts | Use `--force` when the base image or prepare inputs changed. |
| `render` | Write the generated launcher script without submitting | Good for reviewing the final batch script. |
| `up` | Run the one-command submit/watch/logs workflow | Preferred normal run on a real cluster. |
| `submit` | Run the end-to-end flow | Kept as a compatibility path and for lower-level flag combinations such as `--local`. |
| `run` | Launch one service in a fresh one-off allocation | Ignores `depends_on` and follows logs until the one-off command finishes. |

```bash
hpc-compose validate -f compose.yaml
hpc-compose config -f compose.yaml
hpc-compose inspect --verbose -f compose.yaml
hpc-compose preflight -f compose.yaml
hpc-compose prepare -f compose.yaml
hpc-compose render -f compose.yaml --output job.sbatch
hpc-compose up -f compose.yaml
hpc-compose run app -- python -m smoke_test
hpc-compose submit --dry-run -f compose.yaml
```

### `submit --local`

`submit --local` launches the planned services through Enroot on the current host instead of calling `sbatch`. It is useful for local authoring and script inspection, not for distributed Slurm execution.

```bash
hpc-compose submit --local --dry-run -f compose.yaml
```

Current constraints:

- Linux hosts only
- single-host specs only
- no distributed placement
- no `x-slurm.extra_srun_args`
- reservation-related `x-slurm.submit_args` are ignored
- `x-slurm.error` is ignored, and local batch stderr is written into the tracked local batch log

Use `--watch` to follow the tracked local launch the same way you would follow a submitted job.

## Tracked Runtime

| Command | Use it for | Notes |
| --- | --- | --- |
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
hpc-compose jobs list
hpc-compose status -f compose.yaml --format json
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
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
hpc-compose cache prune --age 7 --cache-dir /shared/$USER/hpc-compose-cache
hpc-compose cache prune --all-unused -f compose.yaml
```

## Related Docs

- [Examples](examples.md)
- [Execution Model](execution-model.md)
- [Runbook](runbook.md)
- [Spec Reference](spec-reference.md)
