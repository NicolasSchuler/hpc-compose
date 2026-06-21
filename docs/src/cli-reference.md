# CLI Reference

This page maps the public `hpc-compose` CLI by workflow. Use [Quickstart](quickstart.md) for the shortest install-and-run path, [Runbook](runbook.md) for real-cluster operations, and [Spec Reference](spec-reference.md) for YAML field behavior.

## Command Index

Jump to the section that documents each command group:

| Commands | Section |
| --- | --- |
| `new` / `init`, `examples`, `evolve`, `setup`, `context`, `completions` | [Authoring and Setup](#authoring-and-setup) |
| `--profile`, `--settings-file`, `setup`, `context`, `validate --strict-env`, `lint`, `schema` | [Settings-aware commands](#settings-aware-commands) |
| `plan`, `validate`, `lint`, `config`, `schema`, `inspect`, `preflight`, `doctor`, `weather`, `prepare`, `render`, `up`, `test`, `dev`, `tmux`, `germinate`, `sweep`, `when`, `alloc`, `run`, `shell`, `notebook` | [Plan and Run](#plan-and-run) |
| `lint` finding codes (`HPC001`-`HPC900`) | [Lint rules](#lint-rules) |
| `debug`, `status`, `ps`, `watch`, `replay`, `logs`, `inspect --rightsize`, `stats`, `score`, `diff`, `artifacts`, `cancel`, `down`, `jobs`, `clean`, `rendezvous` | [Tracked Runtime](#tracked-runtime) |
| `cache list`, `cache inspect`, `cache prune` | [Cache Maintenance](#cache-maintenance) |
| `--<tool>-bin` overrides | [Tool overrides](#tool-overrides) |

## Manual Pages

Every command also ships a Unix man page, generated from the same definitions as this reference:

```bash
man hpc-compose
man hpc-compose-up
man hpc-compose-sweep-submit
```

Release archives install them under `share/man/man1/`. From a source checkout, regenerate them with `cargo run --features manpage-bin --bin gen-manpages`.

## Common Flags

| Flag | Use it for | Notes |
| --- | --- | --- |
| `--profile <NAME>` | Select a profile from the project-local settings file | Applies to every command. |
| `--settings-file <PATH>` | Use an explicit settings file | Bypasses upward discovery of `.hpc-compose/settings.toml`. |
| `-f`, `--file <FILE>` | Select the compose file on compose-aware commands | When omitted, `hpc-compose` uses the active context compose file or falls back to `compose.yaml`. |
| `--color auto|always|never` | Control ANSI color output | Use `--color never` for logs, CI captures, or assistive tooling that should receive plain text. |
| `--quiet` | Suppress non-essential progress labels | Useful when a wrapper only needs command output and errors. |
| `--format json` | Machine-readable output | Preferred on non-streaming commands. |

## Authoring and Setup

| Command | Use it for | Notes |
| --- | --- | --- |
| `new` (alias: `init`) | Generate a starter compose file from a built-in template | Use `--list-templates` and `--describe-template <name>` to inspect templates before writing a file. `--cache-dir` is optional and writes an explicit `x-slurm.cache_dir`. |
| `examples` | Search and recommend shipped examples and starter templates | Use `examples recommend` for a no-Slurm starting-point chooser, `examples list` or `examples search` to browse, and `examples coverage` to generate the docs coverage table. |
| `evolve` | Learn spec features through a progressive valid-spec tutorial | Use `--list-lessons`, `--describe-lesson <id>`, and `--until <step>` to inspect or stop at a lesson step. `--format json` requires `--yes`. |
| `setup` | Create or update the project-local settings file | Records compose path, env files, env vars, binary overrides, and an optional profile cache default. |
| `context` | Print the resolved execution context | Shows the selected profile, binaries, interpolation vars, runtime paths, and value sources. |
| `completions` | Generate shell completion scripts | Supports Bash, Zsh, Fish, PowerShell, and Elvish through Clap's completion generator. |

```bash
hpc-compose new --list-templates
hpc-compose new --describe-template minimal-batch
hpc-compose new --template minimal-batch --name my-app --output compose.yaml
hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml
hpc-compose examples list
hpc-compose examples list --tag mpi --format json
hpc-compose examples search 'vllm worker'
hpc-compose examples recommend 'multi-node training' --tag gpu
hpc-compose examples recommend --format json
hpc-compose examples coverage --format markdown
hpc-compose evolve --list-lessons
hpc-compose evolve --describe-lesson progressive-complexity
hpc-compose evolve --output compose.yaml --name my-app
hpc-compose evolve --yes --until readiness --format json
hpc-compose setup
hpc-compose setup --profile-name dev --cache-dir '<shared-cache-dir>' --default-profile dev --non-interactive
hpc-compose context --format json
hpc-compose context --show-values --format json
hpc-compose completions zsh
```

### `evolve` Options

`evolve` is authoring-only: it validates and writes candidate specs but does not prepare images, run preflight, or submit jobs. The default lesson is `progressive-complexity`, with steps `minimal`, `second-service`, `readiness`, `failure-policy`, and `multi-node-placement`.

- `--list-lessons` prints shipped lessons.
- `--describe-lesson <LESSON>` prints lesson steps and concepts.
- `--lesson <LESSON>` selects the lesson to run.
- `--until <STEP>` stops after a step id such as `readiness`.
- `--yes` accepts steps noninteractively.
- `--format json` is available for list/describe and for `--yes` runs.
- `--force` allows overwriting the output file.

## Settings-aware commands

Use these commands and global flags when you want the project-local settings file (`.hpc-compose/settings.toml`) to remember compose path, env files, env vars, and binary overrides. The YAML these commands operate on is documented in [Spec Reference](spec-reference.md).

| Command or flag | Purpose | Notes |
| --- | --- | --- |
| `--profile <NAME>` | Select the profile from settings | Global flag; applies to every subcommand. |
| `--settings-file <PATH>` | Use an explicit settings file | Global flag; bypasses upward auto-discovery of `.hpc-compose/settings.toml`. |
| `hpc-compose setup` | Create or update the project-local settings file | Interactive by default; supports `--non-interactive` with `--profile-name`, `--compose-file`, `--env-file`, `--env`, `--binary`, `--cache-dir`, and `--default-profile`. |
| `hpc-compose context` | Print fully resolved execution context | Shows selected settings/profile, compose path, binaries, referenced interpolation vars, runtime paths, and value sources; supports `--format json`. Sensitive-looking interpolation values are redacted unless `--show-values` is passed. |
| `hpc-compose validate --strict-env` | Fail when interpolation fell back to defaults | Detects when `${VAR:-...}` or `${VAR-...}` consumed fallback values because `VAR` was missing. |
| `hpc-compose lint` | Run opinionated authoring checks | Builds on validation and planning, then reports stable finding codes for risky dependency, memory, and shared-write patterns. Auto-fixable findings can be applied with `--fix` (preview with `--fix --dry-run`). See [Lint rules](#lint-rules). |
| `hpc-compose schema` | Print the checked-in JSON Schema | Useful for editor integration and authoring tools. Rust validation remains the semantic source of truth. |

## Plan and Run

| Command | Use it for | Notes |
| --- | --- | --- |
| `plan` | Validate and preview the static runtime plan | Recommended before every first run. `--show-script` prints the generated launcher to stdout without writing a file; `--explain` adds actionable cache, resume, preflight, and next-command hints. |
| `validate` | Check YAML shape and field validation | Add `--strict-env` when interpolation fallbacks should fail. |
| `lint` | Run stricter opinionated static checks | Flags risky-but-valid specs such as weak dependency readiness, unusual memory/CPU ratios, ignored services that can write shared paths, node-local cache or volume paths, and implicit `depends_on` conditions. Warnings fail by default; add `--allow-warnings` to make warning-only results successful. Pass `--fix` to apply auto-fixable findings in place (preview with `--fix --dry-run`). |
| `config` | Show the fully interpolated effective config | Use `--format json` when you need stable machine-readable snapshots or resume diffs. `config --variables` reports only interpolation variables referenced by the compose file and redacts sensitive-looking names unless `--show-values` is passed. |
| `schema` | Print the checked-in JSON Schema | Use it for editor integration and authoring tools. The same schema is published with the docs site for YAML Language Server and SchemaStore consumption. Rust validation remains the semantic source of truth. |
| `inspect` | View the normalized runtime plan | `--verbose` shows resolved argv and final mount mappings with secret values redacted. Add `--dependencies` for a service DAG in text, DOT, or JSON form. |
| `preflight` | Check host and cluster prerequisites | Use `--strict` when warnings should block a later run. |
| `doctor cluster-report` | Generate a best-effort cluster capability profile | Writes `.hpc-compose/cluster.toml` by default; use `--out -` to print the TOML profile. |
| `doctor readiness` | Explain or run one service readiness probe from the current host | Does not start services or submit jobs. Use `--run` only against an already reachable endpoint, tracked log, tunnel, or login-node-visible service. |
| `doctor mpi-smoke` | Render or run a small MPI probe for one service | Reports requested/advertised MPI types, MPI profile metadata, discovered MPI installs, host MPI binds/env, and rendered `srun`; add `--submit` to consume a Slurm allocation. |
| `doctor fabric-smoke` | Render or run MPI/NCCL/UCX/OFI smoke probes for one MPI service | Use `--checks auto` or a comma-separated list such as `mpi,nccl`; render-only by default, `--submit` consumes a Slurm allocation. |
| `weather` | Show advisory live cluster conditions | One-shot dashboard from `sinfo`, `squeue`, optional `sshare`, and optional `sprio`; does not reserve resources or change submission behavior. |
| `prepare` | Import images and build prepared runtime artifacts | Use `--force-rebuild` when the base image or prepare inputs changed. |
| `render` | Write the generated launcher script without submitting | Good for reviewing the final batch script. |
| `up` | Run the one-command launch/watch/logs workflow | Preferred normal run on a real cluster. Uses a spec-scoped `.hpc-compose/locks/*.up.lock` to prevent concurrent `up` races. |
| `test` | Smoke-test a finite spec end to end | Requires explicit `--local` or `--submit`; every service must start, pass configured readiness, and complete successfully. |
| `dev` | Run local hot-reload mode | Watches bind-mounted source directories and restarts affected services through the local supervisor. |
| `tmux` | Open a multi-pane local service log dashboard | Tails one tracked local service log per pane; tmux does not own service processes. |
| `germinate` | Submit a short canary (default one minute) and recommend resource settings | Writes `latest-canary.json`, keeps normal `latest.json` untouched, and prints a manual YAML patch. |
| `sweep submit` | Submit many independent trials from a top-level `sweep` block | Each trial is a tracked Slurm allocation. Use `--dry-run` first and `--max-trials` for intentional fanout above 100. |
| `when` | Submit after cluster conditions are met | Prepares and renders now, then monitors typed conditions such as idle nodes, prior job completion, or a local time window before calling `sbatch`. |
| `alloc` | Open an interactive Slurm allocation for iterative service runs | Uses top-level `x-slurm` allocation settings, exports `HPC_COMPOSE_*`, and lets `run SERVICE -- CMD` reuse the active allocation. |
| `run` | Launch a one-off command | Service mode uses an existing compose service. Image mode uses `--image IMAGE -- CMD` and builds an ephemeral one-service plan. |
| `shell` | Open an interactive Pyxis shell | Thin wrapper around `srun --pty --container-image=<image> bash -l`. |
| `notebook` | Launch a tracked JupyterLab or VS Code notebook server | Submits a single-service Slurm job (or `--local`), waits for readiness, and prints the connection URL plus an SSH tunnel hint for Jupyter. `--format json` emits `{url, tunnel_hint, compute_node, login_host, job_id, next_commands}` as a single object. Set `login_host` in settings so the tunnel names your real SSH login host. Stop with `hpc-compose cancel`. |

```bash
hpc-compose plan -f compose.yaml
hpc-compose plan --explain -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
hpc-compose validate -f compose.yaml
hpc-compose lint -f compose.yaml
hpc-compose lint -f compose.yaml --allow-warnings
hpc-compose lint -f compose.yaml --fix
hpc-compose lint -f compose.yaml --fix --dry-run
hpc-compose lint -f compose.yaml --format json
hpc-compose config -f compose.yaml
hpc-compose config -f compose.yaml --variables
hpc-compose schema > hpc-compose.schema.json
hpc-compose inspect --verbose -f compose.yaml
hpc-compose inspect --dependencies -f compose.yaml
hpc-compose inspect --dependencies --dependencies-format dot -f compose.yaml
hpc-compose preflight -f compose.yaml
hpc-compose doctor cluster-report
hpc-compose doctor readiness -f compose.yaml --service api
hpc-compose doctor readiness -f compose.yaml --service api --run
hpc-compose doctor readiness -f compose.yaml --service api --run --log-file .hpc-compose/<job-id>/logs/api.log
hpc-compose doctor mpi-smoke -f compose.yaml --service trainer --script-out mpi-smoke.sbatch
hpc-compose doctor mpi-smoke -f compose.yaml --service trainer --submit
hpc-compose doctor fabric-smoke -f compose.yaml --service trainer --checks auto --script-out fabric-smoke.sbatch
hpc-compose doctor fabric-smoke -f compose.yaml --service trainer --checks mpi,nccl --submit
hpc-compose weather
hpc-compose weather --format json
hpc-compose prepare -f compose.yaml
hpc-compose render -f compose.yaml --output job.sbatch
hpc-compose up -f compose.yaml
hpc-compose up --hold-on-exit always -f compose.yaml
hpc-compose up --watch-queue --queue-warn-after 15m -f compose.yaml
hpc-compose up --detach --format json -f compose.yaml
hpc-compose up --detach --format json --print-endpoints -f compose.yaml
hpc-compose test --local -f compose.yaml
hpc-compose test --submit --time 00:01:00 -f compose.yaml
hpc-compose dev -f examples/dev-python-app.yaml
hpc-compose tmux -f examples/dev-python-app.yaml --no-attach
hpc-compose germinate -f compose.yaml
hpc-compose germinate -f compose.yaml --format json
hpc-compose germinate -f compose.yaml --dry-run --script-out canary.sbatch
hpc-compose sweep submit -f compose.yaml --dry-run
hpc-compose sweep submit -f compose.yaml --max-trials 200
hpc-compose sweep results -f compose.yaml --format csv > runs.csv
hpc-compose sweep results -f compose.yaml --include score,energy --format json
hpc-compose score --sweep latest -f compose.yaml --format json
hpc-compose stats --sweep latest -f compose.yaml --format json
hpc-compose sweep status -f compose.yaml --format json
hpc-compose sweep list -f compose.yaml
hpc-compose when -f compose.yaml --partition gpu8 --free-nodes 4
hpc-compose when -f compose.yaml --after-job 12345
hpc-compose when -f compose.yaml --between 22:00-06:00
hpc-compose when --detach --format json -f compose.yaml --partition gpu8 --free-nodes 4
hpc-compose alloc -f compose.yaml
hpc-compose run app -- python -m smoke_test
hpc-compose run --image docker://python:3.12 --resources cpu-small -- python -V
hpc-compose shell --image docker://ubuntu:24.04
hpc-compose notebook --kind jupyter --gpus 1 --volume ./project:/workspace
hpc-compose notebook --kind vscode --image ghcr.io/example/code:1 --gpus 1
hpc-compose notebook --local --kind jupyter
hpc-compose notebook --kind jupyter --format json
```

### Lint rules

`hpc-compose lint` emits stable finding codes after validation and planning succeed. Warning-level findings fail the command by default; pass `--allow-warnings` to downgrade them to advisory so a warning-only run still succeeds.

| Code | Severity | Trigger | Recommendation |
| --- | --- | --- | --- |
| `HPC001` | warning | A service uses `depends_on` with `condition: service_started` against an upstream service that has no `readiness` probe. The dependency may fire before the upstream is actually ready. | Add a `readiness` block to the upstream service, or switch to `service_completed_successfully` for one-shot dependencies. |
| `HPC002` | warning | `x-slurm.mem` gives fewer than 512 MiB or more than 512 GiB per requested CPU. Very low ratios may OOM; very high ratios may queue poorly or violate site policy. | Adjust `x-slurm.mem` or CPU/task counts to land in the expected band. |
| `HPC003` | warning | A service with `failure_policy.mode: ignore` has a writable mount from a shared cache path. Ignored failures can leave corrupt state for subsequent jobs. | Use a read-only mount, write to job-local scratch, or avoid `mode: ignore` for services that mutate shared state. |
| `HPC004` | warning | `x-slurm.cache_dir` resolves under a node-local root (`/tmp`, `/var/tmp`, `/private/tmp`, `/dev/shm`). Compute nodes typically cannot see these paths, so the cache is rebuilt every job. | Point `x-slurm.cache_dir` at shared storage visible from both login and compute nodes. Advisory only; `--fix` will not rewrite paths. |
| `HPC005` | warning | A service `volumes` host path lives under a node-local root. The mount will be missing or empty on compute nodes. | Move the host path under shared storage, or use job-local scratch. Advisory only; `--fix` will not rewrite paths. |
| `HPC006` | warning (fixable) | A `depends_on` edge has no explicit `condition:` (list-form `depends_on: [name]`, or mapping form with the `condition:` key omitted). The implicit `service_started` default is easy to misread. | Make the condition explicit. `hpc-compose lint --fix` writes the current default for you, preserving comments and formatting everywhere else. |
| `HPC900` | warning | Cluster profile advisory: the site cluster profile (`doctor cluster-report`) detected a runtime-plan mismatch such as a shared-cache path, port-range overlap, or MPI configuration concern. | Inspect the finding message for the specific cluster-level concern and adjust the spec or cluster profile accordingly. |

#### Auto-fixable findings

`hpc-compose lint --fix` applies every fixable finding directly to the compose file. Today only `HPC006` (implicit `depends_on` condition) is auto-fixable, because the rewrite is deterministic and semantics-preserving: the implicit `service_started` default is written out verbatim, so the rendered Slurm script is byte-identical.

```bash
hpc-compose lint -f compose.yaml --fix --dry-run   # preview the diff
hpc-compose lint -f compose.yaml --fix              # apply in place
```

The rewriter edits only the located `depends_on` block; comments, blank lines, and author formatting elsewhere are preserved byte-for-byte. A safety gate re-parses and re-plans the file after each run; if anything fails to reload the original file is restored. Path findings (`HPC004`, `HPC005`) are intentionally not auto-fixed because the correct replacement is cluster-specific.

### Editor Schema

The checked-in schema is draft-07 JSON Schema and is published with the docs site at `/schema/hpc-compose.schema.json`. SchemaStore should associate it only with hpc-compose-specific filenames: `hpc-compose.yaml`, `hpc-compose.yml`, `*.hpc-compose.yaml`, and `*.hpc-compose.yml`. Generic `compose.yaml` remains a supported input file, but it is intentionally not claimed for zero-config editor association.

### `up` Options

Useful workflow flags:

- `--local` runs a Pyxis/Enroot plan on the current Linux host instead of calling `sbatch`.
- `--detach` submits or launches and returns after tracking metadata is written.
- `--format text|json` is accepted with `--detach` or `--dry-run`.
- `--watch-queue` waits in line-oriented queue output until the Slurm job reaches `RUNNING`, then opens the normal watch view.
- `--queue-warn-after <DURATION>` warns once when `--watch-queue` stays `PENDING` longer than the threshold; the default is `10m`, and `0` disables the warning.
- `--watch-mode auto|tui|line` selects the live output mode.
- `--hold-on-exit never|failure|always` controls whether the TUI stays open after the job reaches a terminal scheduler state.
- `--allow-resume-changes` acknowledges an intentional change to resume-coupled config between tracked runs.
- `--resume-diff-only` prints the resume-sensitive config diff without submitting.
- `--script-out <PATH>` keeps a copy of the rendered batch script.
- `--force-rebuild` refreshes imported and prepared artifacts before launch.
- `--skip-prepare` skips image import and prepare reuse checks.
- `--keep-failed-prep` leaves the failed Enroot rootfs behind for inspection.
- Array jobs (`x-slurm.array`) require `--detach` because live watch/log fan-out is not array-aware yet.
- Scheduler dependencies from `x-slurm.after_job` and `x-slurm.dependency` are passed as `sbatch --dependency=...`.

#### Tool overrides

Commands that interact with Slurm or container runtimes accept `--<tool>-bin <PATH>` flags to point at non-default executables. This is useful when tools live outside `PATH` or when testing against fake binaries.

| Flag | Default | Accepted by |
| --- | --- | --- |
| `--sbatch-bin` | `sbatch` | `up`, `when`, `germinate`, `test`, `run`, `notebook`, `sweep submit`, `preflight`, `debug`, `doctor` |
| `--srun-bin` | `srun` | `up`, `when`, `alloc`, `germinate`, `test`, `run`, `notebook`, `shell`, `sweep submit`, `preflight`, `debug`, `doctor` |
| `--squeue-bin` | `squeue` | `up`, `when`, `germinate`, `test`, `run`, `notebook`, `watch`, `status`, `stats`, `ps`, `inspect`, `score`, `diff`, `reach`, `sweep status`, `sweep observe`, `sweep stop`, `sweep results`, `debug`, `weather` |
| `--sacct-bin` | `sacct` | `up`, `when`, `germinate`, `test`, `run`, `notebook`, `watch`, `status`, `stats`, `ps`, `inspect`, `score`, `diff`, `reach`, `sweep status`, `sweep observe`, `sweep stop`, `sweep results`, `debug` |
| `--salloc-bin` | `salloc` | `alloc` |
| `--scontrol-bin` | `scontrol` | `alloc`, `sweep submit`, `preflight`, `debug`, `doctor` |
| `--sinfo-bin` | `sinfo` | `when`, `weather` |
| `--scancel-bin` | `scancel` | `test`, `cancel`, `down`, `sweep observe`, `sweep stop` |
| `--sstat-bin` | `sstat` | `germinate`, `stats`, `inspect`, `score`, `sweep results` |
| `--sshare-bin` | `sshare` | `weather` |
| `--sprio-bin` | `sprio` | `weather` |
| `--enroot-bin` | `enroot` | `up`, `when`, `alloc`, `germinate`, `test`, `dev`, `tmux`, `run`, `notebook`, `sweep submit`, `prepare`, `preflight`, `debug`, `doctor` |
| `--apptainer-bin` | `apptainer` | `up`, `when`, `alloc`, `germinate`, `test`, `dev`, `tmux`, `run`, `notebook`, `sweep submit`, `prepare`, `preflight`, `debug`, `doctor` |
| `--singularity-bin` | `singularity` | `up`, `when`, `alloc`, `germinate`, `test`, `dev`, `tmux`, `run`, `notebook`, `sweep submit`, `prepare`, `preflight`, `debug`, `doctor` |
| `--tmux-bin` | `tmux` | `tmux` |

Settings profiles can also configure these via `[defaults.binaries]` or `[profiles.<name>.binaries]` (see [Runbook](runbook.md)).

### `germinate` Canary Runs

`germinate` is the conservative right-sizing workflow:

```bash
hpc-compose germinate -f compose.yaml
hpc-compose germinate -f compose.yaml --canary-time 00:01:00 --metrics-interval 5
hpc-compose germinate -f compose.yaml --pending-timeout 30m --format json
```

Useful options:

- `--canary-time <TIME>` defaults to `00:01:00`.
- `--metrics-interval <SECONDS>` defaults to `5` and is forced on in the canary plan.
- `--pending-timeout <DURATION>` defaults to `30m`.
- `--min-cpus <N>`, `--min-mem <MEM>`, and `--min-gpus <N>` set canary floors.
- `--dry-run` renders the canary script without calling `sbatch`.
- `--skip-prepare`, `--force-rebuild`, `--keep-failed-prep`, `--no-preflight`, and `--script-out` match the normal preparation flags.

The command rejects `x-slurm.array` and never rewrites your compose file automatically. See [Right-Sizing With Canary Runs](canary-runs.md).

### `sweep` Hyperparameter Sweeps

`sweep` expands the top-level `sweep` block in a compose file. Each generated trial is rendered and submitted as an independent tracked Slurm job; `sweep status` and `sweep list` read the persisted manifest under `.hpc-compose/sweeps/`.

```bash
hpc-compose sweep submit -f train.yaml --dry-run
hpc-compose sweep submit -f train.yaml --max-trials 200
hpc-compose sweep submit -f train.yaml --format json
hpc-compose sweep status -f train.yaml
hpc-compose sweep status -f train.yaml --sweep-id sweep-123 --format json
hpc-compose sweep list -f train.yaml --format json
```

`sweep submit` options:

| Option | Use it for |
| --- | --- |
| `-f`, `--file <FILE>` | Select the compose file containing the embedded `sweep` block. |
| `--dry-run` | Expand and validate all trials without writing manifests, scripts, or job records. |
| `--max-trials <N>` | Permit real submissions above the default 100-trial fanout guard. |
| `--skip-prepare` | Reuse existing prepared artifacts and skip image preparation. |
| `--force-rebuild` | Refresh imported/prepared artifacts for each submitted trial. |
| `--no-preflight` | Skip preflight checks before trial submission. |
| `--format text|json` | Print human-readable or machine-readable trial output. |

`sweep status` options:

| Option | Use it for |
| --- | --- |
| `-f`, `--file <FILE>` | Select the compose file whose sweep manifests should be read. |
| `--sweep-id <ID>` | Inspect a specific sweep instead of `.hpc-compose/sweeps/latest.json`. |
| `--format text|json` | Print aggregate counts and per-trial state for automation. |

`sweep list` options:

| Option | Use it for |
| --- | --- |
| `-f`, `--file <FILE>` | Select the compose file whose sweep directory should be scanned. |
| `--format text|json` | Print persisted sweep manifests without querying Slurm. |

See [Hyperparameter Sweeps](sweeps.md) for the `sweep` spec shape, interpolation rules, status categories, and current limitations.

### `when` Conditional Submission

`when` is a foreground monitor for constrained partitions and off-hour workflows. It runs the normal pre-submit work first, then polls until every supplied condition is true:

```bash
hpc-compose when -f compose.yaml --partition gpu8 --free-nodes 4
hpc-compose when -f compose.yaml --after-job 12345 --after-job-condition afterok
hpc-compose when -f compose.yaml --between 22:00-06:00
```

All conditions must hold (logical AND). `--free-nodes` counts only `idle` rows from `sinfo -h -p <partition> -o "%T|%D"` and requires `--partition` to match `x-slurm.partition`. `--after-job` polls `squeue` first and then `sacct`; `afterok` and `afternotok` fail immediately when the prior job reaches a terminal state that can never satisfy the requested condition. `--between` uses local login-node wall-clock time and supports wraparound windows such as `22:00-06:00`.

Useful options:

- `--poll-interval <DURATION>` defaults to `60s`; the minimum is `5s`.
- `--timeout <DURATION>` gives up if conditions are not met; `0s` performs one check.
- `--detach` returns after submission and tracking metadata are written.
- `--format json` is accepted with `--detach` and returns the condition summaries plus normal submission metadata.
- `--skip-prepare`, `--force-rebuild`, `--keep-failed-prep`, `--no-preflight`, and `--script-out` match the corresponding `up` preparation flags.

Example JSON automation:

```bash
hpc-compose when --detach --format json -f compose.yaml --partition gpu8 --free-nodes 4
```

There is no `x-when` YAML field. Conditional submission is intentionally a CLI workflow layered over the normal compose spec.

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
- no `x-slurm.array`
- no scheduler dependencies from `x-slurm.after_job` or `x-slurm.dependency`
- reservation-related `x-slurm.submit_args` are ignored
- `x-slurm.error` is ignored, and local batch stderr is written into the tracked local batch log

`up --local` follows the tracked local launch immediately, just like `up` does for a submitted job. Add `--detach` when you want to launch and return.

In local mode the batch script also exports `HPC_COMPOSE_BACKEND_OVERRIDE=local`, `HPC_COMPOSE_LOCAL_ENROOT_BIN` pointing to the resolved `enroot` binary, and `HPC_COMPOSE_LOCAL_BIN_DIR` containing a generated `srun` shim. These variables are internal to `hpc-compose` and not intended for direct use in compose specs.

### Development Workflow

`test`, `dev`, and `tmux` are intentionally small workflows layered over the same render/prepare/tracking machinery as `up`. See [Development Workflow](development-workflow.md) for the smoke-test guide, hot-reload behavior, and local-mode constraints.

`test` is for finite smoke specs:

```bash
hpc-compose test --local -f compose.yaml
hpc-compose test --submit --time 00:01:00 --timeout 180s -f compose.yaml
hpc-compose test --submit --format json -f compose.yaml
```

Success means all tracked services appear in runtime state, launched at least once, passed readiness when `readiness` is configured, and completed successfully. Long-running application specs should use a smoke-test variant of the command or service entrypoint that exits after proving the workflow.

Useful `test` options:

| Option | Use it for |
| --- | --- |
| `--local` | Run the finite smoke spec through the local supervisor. |
| `--submit` | Submit the finite smoke spec to Slurm; required before any scheduler submission happens. |
| `--time <TIME>` | Override Slurm wall time for `--submit`; defaults to `00:01:00`. |
| `--wait-timeout <DURATION>` (alias `--timeout`) | Stop waiting and best-effort cancel/cleanup after the timeout; defaults to `180s`. |
| `--format json` | Emit phase status, job id, script path, per-service results, and failure reason for automation. |

`dev` is local-only and watches host directories from service `volumes`:

```bash
hpc-compose dev -f examples/dev-python-app.yaml
hpc-compose dev -f compose.yaml --watch-paths ./src --debounce-ms 500
```

Directory bind mounts are mapped back to affected services. File mounts, missing paths, container-only paths, cache paths, and non-directory paths are ignored. `--watch-paths` adds an explicit directory and restarts all services when it changes. By default, leaving `dev` stops the local supervisor; use `--keep-running` when you want the tracked local job to continue.

Useful `dev` options:

| Option | Use it for |
| --- | --- |
| `--watch-paths <PATH>` | Add an explicit watch root when mounted source directories cannot be inferred. |
| `--debounce-ms <N>` | Coalesce rapid file changes before requesting a restart. |
| `--keep-running` | Leave the local supervisor alive when the watch loop exits. |

`tmux` opens a log dashboard for local runs:

```bash
hpc-compose tmux -f compose.yaml
hpc-compose tmux -f compose.yaml --job-id local-123
hpc-compose tmux -f compose.yaml --session demo --no-attach
```

When `--job-id` is omitted, `tmux` launches a new local run first. Each pane runs `tail -F` against one tracked service log and uses the service name as the pane title.

Useful `tmux` options:

| Option | Use it for |
| --- | --- |
| `--job-id <ID>` | Attach the dashboard to an existing tracked local run. |
| `--session <NAME>` | Choose the tmux session name instead of `hpc-compose-<job-id>`. |
| `--no-attach` | Create/update the dashboard without requiring an interactive terminal. |
| `--lines <N>` | Set the initial `tail -n` history for each pane. |

### `run` and `shell`

`run` has two forms:

```bash
hpc-compose run [-f compose.yaml] SERVICE -- CMD [ARGS...]
hpc-compose run --image IMAGE [--resources NAME] [--time T] [--mem M] [--cpus-per-task N] [--gpus N] [--partition P] [--env K=V] [--dataset PATH] [--output DIR] [--local] -- CMD [ARGS...]
```

Service mode reuses the named service's image, environment, mounts, working directory, and prepare rules, clears `depends_on`, and submits a fresh tracked run job. When launched inside `hpc-compose alloc`, service mode detects `HPC_COMPOSE_ALLOCATION=1` and `SLURM_JOB_ID`, prints the active allocation id, runs the one-service launcher inside the allocation with `srun`, and records the latest run metadata against the allocation job id. Image mode creates an ephemeral one-service plan from CLI flags, then follows the normal render/prepare/submit path. `--resources` refers to `[resource_profiles.<name>]` in settings; it is not the global `--profile` selector.

Image mode also accepts two batch-inference flags (both image-mode-only; using either without `--image` is an error):

- `--dataset <PATH>` binds an existing shared-filesystem path read-only into the container and exposes its in-container location as `HPC_COMPOSE_DATASET_DIR`. The path is filesystem-based only; remote or registry schemes such as `hf://` are rejected, and a non-existent path fails before any submission. Copy datasets onto the shared filesystem first.
- `--output <DIR>` turns on artifact export: the in-container path exposed as `HPC_COMPOSE_OUTPUT_DIR` is collected after the job and exported into `<DIR>` (recorded as the run's `artifact_export_dir`). Have the in-job command write its results under `$HPC_COMPOSE_OUTPUT_DIR`.

```bash
hpc-compose run --image docker://python:3.12 --dataset /scratch/data --output ./results -- python infer.py
```

`alloc` requests an interactive allocation through `salloc`:

```bash
hpc-compose alloc -f compose.yaml
hpc-compose alloc -f compose.yaml -- bash -lc 'hpc-compose run app -- python -m pytest'
```

It runs preflight and image preparation by default, accepts the matching `up` preparation flags (`--no-preflight`, `--skip-prepare`, `--force-rebuild`, and `--keep-failed-prep`), rejects `x-slurm.array`, and exports allocation metadata such as `HPC_COMPOSE_COMPOSE_FILE`, `HPC_COMPOSE_CACHE_DIR`, `HPC_COMPOSE_NODELIST_FILE`, and `HPC_COMPOSE_PRIMARY_NODE`.

`shell` is intentionally thinner:

```bash
hpc-compose shell --image IMAGE [--resources NAME] [--time T] [--mem M] [--cpus-per-task N] [--gpus N] [--partition P] [--env K=V]
```

It calls `srun --pty` directly with Pyxis `--container-image` and defaults to `bash -l`. It does not render an sbatch script or create tracked job metadata.

`notebook` launches a tracked interactive server:

```bash
hpc-compose notebook [--kind jupyter|vscode] [--image IMAGE] [--port N] [--token TOKEN]
                     [--volume HOST:CONTAINER]... [--working-dir PATH] [--tunnel-name NAME]
                     [--ready-timeout DURATION] [--follow] [--dry-run] [--local] [-- ARGS...]
                     [--resources NAME] [--time T] [--mem M] [--cpus-per-task N] [--gpus N]
                     [--partition P] [--env K=V]
```

It synthesizes a one-service compose job from the preset, runs the normal preflight/prepare/render path, submits (or launches locally with `--local`), waits for a log readiness signal, then prints the connection URL — a localhost Jupyter URL plus an SSH tunnel hint for Jupyter on Slurm, or the scraped `vscode.dev` link for VS Code. The session is a tracked job of kind `notebook` (see [Notebook Sessions](notebook.md)); stop it with `hpc-compose cancel`. `--kind vscode` requires `--image` because no universal default `code` image is shipped.

## Accessible and Automation-Friendly Output

Use plain or structured output when terminal styling, progress labels, or alternate-screen interfaces make automation or assistive tooling harder:

```bash
hpc-compose --color never plan -f compose.yaml
hpc-compose --quiet validate -f compose.yaml
hpc-compose watch -f compose.yaml --watch-mode line
hpc-compose logs -f compose.yaml --service app --follow
hpc-compose logs -f compose.yaml --grep 'error|oom' --since 30m
hpc-compose status -f compose.yaml --format json
```

`context` and `config --variables` intentionally scope interpolation variables to names referenced by the compose file. Values whose names look secret-bearing are shown as `<redacted>` by default; add `--show-values` only in trusted local diagnostics. A name triggers redaction when, after upper-casing, it **contains** any of these case-insensitive substrings: `SECRET`, `TOKEN`, `PASSWORD`, `PASSWD`, `API_KEY`, `ACCESS_KEY`, `PRIVATE_KEY`, `CREDENTIAL`, `AUTH`, `COOKIE`, `SESSION`, `BEARER`. Because the test is a substring match, names such as `SESSION_DIR` or `AUTH_MODE` also match.

## Tracked Runtime

| Command | Use it for | Notes |
| --- | --- | --- |
| `debug` | Diagnose the latest tracked run | Shows scheduler state, per-service state, batch and service log tails, missing-log hints, and a recommended next command. Add `--preflight` to rerun prerequisite checks. |
| `status` | Summarize scheduler state, the top-level batch log, per-service outcomes, and failure-policy state | Prefer `--format json` for automation. Add `--array` to include merged `squeue --array` and `sacct --array` task rows. |
| `ps` | Show a stable per-service runtime snapshot | Useful when you want a point-in-time view instead of the live TUI. |
| `watch` | Reconnect to the live watch UI | Falls back to line-oriented output on non-interactive terminals. |
| `reach` | Print the SSH tunnel to reach a tracked service from a laptop | Resolves the compute node from tracked status and the port from the service's TCP/HTTP readiness, then prints an `ssh -L` command (with `ControlMaster` multiplexing so an OTP login node prompts once) or runs it in the foreground with `--open`. Pass `--port` for services without TCP/HTTP readiness; `--format json` emits `{service, job_id, compute_node, login_host, local_port, remote_port, url, ssh_command}`. |
| `replay` | Reanimate a tracked job timeline from existing artifacts | Best-effort DVR view built from final state, service-exit markers, metrics JSONL, and logs. Use `--speed` or `--format json` as needed. |
| `logs` | Print tracked service logs | Add `--follow`, `--grep <pattern>`, or coarse `--since <duration>` as needed. |
| `inspect --rightsize` | Suggest conservative resource request reductions after a tracked run | Uses tracked `sacct`, `sstat`, and sampler evidence; supports `--job-id` and `--format json`. |
| `stats` | Report tracked runtime metrics, step stats, and optional accounting | Supports `--accounting`, `--format json`, `--format jsonl`, and `--format csv`. |
| `score` | Score post-run resource efficiency | Supports positional job ids, `--format json`, `--pue`, `--gpu-tdp-w`, and `--cpu-watts-per-core`. |
| `diff` | Compare two tracked job submissions | Compact text by default; use `--format json` for full detail. |
| `artifacts` | Export tracked artifact bundles after a run | Use `--bundle <name>` and `--tarball` when needed. |
| `pull` | Print the `rsync` command to copy a tracked job's artifacts to a laptop | Resolves the artifact payload directory from tracked state and prints an `rsync` line (with `ControlMaster` multiplexing so an OTP login node prompts once); `--into <DIR>` sets the local destination, `--format json` emits `{bundles, cluster_path, suggested_command, files, bytes}`. Read-only: copies nothing and opens no connection. |
| `cancel` | Cancel the latest tracked job or an explicit job id | Uses tracked metadata instead of making you retype paths. |
| `down` | Cancel a tracked job and clean tracked state | Supports `--purge-cache` when the tracked snapshot names concrete cache artifacts. |
| `jobs list` | Scan the current repo tree for tracked runs | Start here when you need to rediscover an older run. |
| `clean` | Remove old tracked job directories for one compose context | Use `--dry-run` first when you are unsure. |
| `rendezvous list` | List live shared-cache service records | Defaults to the resolved cache dir; `--cache-dir` inspects a specific cache. |
| `rendezvous resolve NAME` | Resolve one provider record | Prints endpoint fields or JSON for automation. |
| `rendezvous register NAME` | Manually register a provider record | Intended for debugging and custom workflows; declarative specs usually register providers. |
| `rendezvous prune` | Remove expired provider records | Cleans stale latest and historical rendezvous JSON files. |

```bash
hpc-compose debug -f compose.yaml
hpc-compose debug -f compose.yaml --preflight
hpc-compose jobs list
hpc-compose status -f compose.yaml --format json
hpc-compose status -f compose.yaml --array
hpc-compose status -f compose.yaml --job-id 12345_7 --array
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml --watch-mode line
hpc-compose watch -f compose.yaml --hold-on-exit always
hpc-compose replay -f compose.yaml
hpc-compose replay -f compose.yaml --speed 10
hpc-compose replay -f compose.yaml --job-id 12345 --service app
hpc-compose replay -f compose.yaml --format json
hpc-compose logs -f compose.yaml --service app --follow
hpc-compose logs -f compose.yaml --grep 'error|oom' --since 30m
hpc-compose inspect -f compose.yaml --rightsize
hpc-compose stats -f compose.yaml --format jsonl
hpc-compose stats -f compose.yaml --accounting --format csv
hpc-compose score 12345
hpc-compose diff 12345 12346 -f compose.yaml
hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
hpc-compose down -f compose.yaml --yes
hpc-compose cancel -f compose.yaml --yes
hpc-compose clean -f compose.yaml --age 7 --dry-run
hpc-compose rendezvous list
hpc-compose rendezvous resolve model-server
hpc-compose rendezvous register model-server --host node01 --port 8000 --job-id 12345
hpc-compose rendezvous prune
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
hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>' --yes
hpc-compose cache prune --all-unused -f compose.yaml --yes
```

## Related Docs

- [Spec Reference](spec-reference.md)
- [Glossary](glossary.md)
- [Full Example Specs](example-source.md)
- [Roadmap and Non-Goals](roadmap.md)
- [Runbook](runbook.md)
