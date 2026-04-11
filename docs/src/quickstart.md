# Quickstart

This is the shortest install-and-run path from an empty shell to a submitted job.

## 1. Install a release binary

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

The installer selects the newest published release for the current Linux or macOS machine and installs `hpc-compose` into `~/.local/bin` by default. Check the [Support Matrix](support-matrix.md) before assuming that a platform can run full cluster workflows.

If you prefer native packages, published Linux releases also ship `.deb` and `.rpm` assets, and macOS users can install with `brew install NicolasSchuler/hpc-compose/hpc-compose`.

The installed CLI also ships Unix manpages. Use `man hpc-compose`, `man hpc-compose-up`, or `man hpc-compose-submit` as the concise command reference, and keep the longer mdBook docs for workflow guidance. If you want shell integration right away, generate completions with `hpc-compose completions bash|zsh|fish`.

## 2. Write a starter spec

```bash
hpc-compose new \
  --template minimal-batch \
  --name my-app \
  --cache-dir '<shared-cache-dir>' \
  --output compose.yaml
```

Replace `<shared-cache-dir>` with a path visible from both the submission host and the compute nodes.

If you already know the closest shipped example, copy it directly instead. The [Examples](examples.md) page is the fastest way to choose one. The repository examples default `x-slurm.cache_dir` to `/cluster/shared/hpc-compose-cache` and honor `CACHE_DIR`, so set `CACHE_DIR` through `.env`, your shell environment, or `hpc-compose setup` before submitting them on a real cluster.

## 3. Optional: create a project-local settings file once

If you want to stop repeating compose paths, env files, and binary overrides, create the project-local settings file (`.hpc-compose/settings.toml`) once in the current repo tree:

```bash
hpc-compose setup
hpc-compose context
```

Use `context` whenever you want to verify the fully resolved values and their sources before running cluster commands.

<div class="callout warning">
  <p><strong>Runtime commands require a Slurm cluster</strong></p>
  <p>Commands like <code>up</code>, <code>submit</code>, <code>prepare</code>, and <code>preflight</code> need Slurm and Enroot on the submission host. On macOS or a workstation without Slurm, use <code>new</code>, <code>validate</code>, <code>inspect</code>, <code>render</code>, and <code>config</code> for authoring, then run the full workflow from a login node. See the <a href="support-matrix.md">Support Matrix</a> for the platform-by-platform breakdown.</p>
</div>

## 4. Normal run

```bash
hpc-compose up -f compose.yaml
hpc-compose --profile dev up
```

`up` is the preferred normal run. It runs preflight, prepares missing artifacts, renders the batch script, submits it through `sbatch`, then follows scheduler state and tracked logs. On an interactive TTY it opens the full-screen watch UI; otherwise it falls back to the line-oriented follower used in scripts and tests. `submit --watch` remains available when you want the older spelling.

Use `up --local` for authoring on a Linux host without `sbatch`, `up --resume-diff-only` when you want to inspect resume-related config deltas without submitting, and `up --allow-resume-changes` when you intentionally changed the resume-coupled config between runs.

## 5. Debugging flow and first-job failures

```bash
hpc-compose validate -f compose.yaml
hpc-compose validate -f compose.yaml --strict-env
hpc-compose config -f compose.yaml
hpc-compose inspect --verbose -f compose.yaml
hpc-compose preflight -f compose.yaml
hpc-compose prepare -f compose.yaml
```

Use the debugging flow when you want to confirm:

- service order
- normalized image references
- cache artifact paths
- whether prepare steps will rebuild every submit

Use `config` when you want the fully interpolated effective config (what `submit` actually receives), and `inspect` when you want the normalized runtime plan (service order, placement, cache behavior).

<div class="callout warning">
  <p><strong>Warning</strong></p>
  <p><code>inspect --verbose</code> prints resolved environment values and final mount mappings. Treat its output as sensitive when the spec contains secrets.</p>
</div>

When your first job fails, follow this sequence:

1. `validate` to catch spec errors.
2. `inspect --verbose` to confirm the planner understood your intent.
3. `preflight` to check cluster prerequisites.
4. `render --output job.sbatch` to inspect the generated batch script.
5. Check the top-level batch log (`slurm-<jobid>.out` or the path in `x-slurm.output`) for early failures before any service log appears.
6. Use `logs --service <name>` for per-service output.

## 6. Revisit a tracked run later

```bash
hpc-compose jobs list
hpc-compose status -f compose.yaml
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
hpc-compose stats -f compose.yaml
hpc-compose logs -f compose.yaml --follow
```

Use `jobs list` first when you need to rediscover tracked runs under the current repo tree. Use `ps` for a stable per-service runtime snapshot, `watch` to reconnect to the live TUI, and `logs --follow` when you want the simplest text-only follower. If a service uses `x-slurm.failure_policy.mode: restart_on_failure`, `status` also shows the current retry state and rolling-window budget for that service.

## From a source checkout

If you are running from a local checkout instead of an installed binary:

```bash
cargo build --release
target/release/hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml
target/release/hpc-compose up -f compose.yaml
```

## Read next

- Use the [Execution Model](execution-model.md) page to understand what runs where and which paths must be shared.
- Use the [CLI Reference](cli-reference.md) page for the current command surface grouped by workflow.
- Use the [Support Matrix](support-matrix.md) page to confirm what is officially supported versus only release-built.
- Use the [Task Guide](task-guide.md) page when you want a goal-oriented starting point.
- Use the [Runbook](runbook.md) when adapting a real workload to a real cluster.
- Use the [Examples](examples.md) page when you want the closest known-good template.
- Use the [Spec Reference](spec-reference.md) when changing fields or validation-sensitive values.
