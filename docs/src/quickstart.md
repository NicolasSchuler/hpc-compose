# Quickstart

This is the shortest path from an empty shell to a validated spec, an inspectable batch script, and a real cluster submission.

## 1. Install the CLI

If the repository's [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases) page is still empty, build from source first:

```bash
git clone https://github.com/NicolasSchuler/hpc-compose.git
cd hpc-compose
cargo build --release
./target/release/hpc-compose --help
```

Once a release tag is published, use the matching installer script from that tag:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

The pinned installer selects the matching published release asset for the current Linux or macOS machine and installs `hpc-compose` into `~/.local/bin` by default. Check the [Support Matrix](support-matrix.md) before assuming that a platform can run full cluster workflows.

If you prefer native packages, published Linux releases also ship `.deb` and `.rpm` assets, and macOS users can install with `brew install NicolasSchuler/hpc-compose/hpc-compose`.

The installed CLI also ships Unix manpages. Use `man hpc-compose`, `man hpc-compose-up`, or `man hpc-compose-submit` as the concise command reference, and keep the longer docs for workflow guidance.

When you install from a published release, verify it with the [release verification steps](installation.md#verify-a-release) before using it on a cluster or internal mirror.

## 2. Choose the smallest starting point

Use the built-in starter template when you want the shortest path to your own `compose.yaml`:

```bash
hpc-compose new \
  --template minimal-batch \
  --name my-app \
  --cache-dir '<shared-cache-dir>' \
  --output compose.yaml
```

Replace `<shared-cache-dir>` with a path visible from both the submission host and the compute nodes.

If you want a known-good repository example instead, start with one of the four promoted examples on the [Examples](examples.md) page. The repository examples default `x-slurm.cache_dir` to `/cluster/shared/hpc-compose-cache` and honor `CACHE_DIR`, so set `CACHE_DIR` through `.env`, your shell environment, or `hpc-compose setup` before submitting them on a real cluster.

## 3. Run the authoring golden path

These three commands are the fastest proof that the tool understood your intent:

```bash
hpc-compose validate -f examples/minimal-batch.yaml
hpc-compose inspect -f examples/minimal-batch.yaml
hpc-compose up --dry-run --skip-prepare --no-preflight \
  --script-out /tmp/hpc-compose-demo.sbatch \
  -f examples/minimal-batch.yaml
```

Success looks like:

- `validate` prints `spec is valid`
- `inspect` shows `service order: app`
- `up --dry-run` writes a launcher script and skips `sbatch`

Download the [asciinema-style demo cast](quickstart-demo.cast) for the same flow.

This is the right path on macOS or on any machine where you want to evaluate the authoring model before touching a real cluster.

## 4. Optional: create a project-local settings file once

If you want to stop repeating compose paths, env files, and binary overrides, create the project-local settings file (`.hpc-compose/settings.toml`) once in the current repo tree:

```bash
hpc-compose setup
hpc-compose context
```

Use `context` whenever you want to verify the fully resolved values and their sources before running cluster commands.

<div class="callout warning">
  <p><strong>Runtime commands require a Linux submission host</strong></p>
  <p>Commands like <code>up</code>, <code>submit</code>, <code>prepare</code>, and <code>preflight</code> need Slurm and Enroot on the submission host. On macOS or a workstation without Slurm, stay on the authoring path until you move to a login node. See the <a href="support-matrix.md">Support Matrix</a> for the platform-by-platform breakdown.</p>
</div>

## 5. Submit on a real cluster

When you move to a supported Linux submission host, the normal run is:

```bash
hpc-compose up -f compose.yaml
```

`up` is the preferred normal run. It runs preflight, prepares missing artifacts, renders the batch script, submits it through `sbatch`, then follows scheduler state and tracked logs. On an interactive TTY it opens the full-screen watch UI; otherwise it falls back to the line-oriented follower used in scripts and tests.

Success looks like:

- the job is submitted or launched
- a tracked job id is recorded
- the watch UI or text follower shows scheduler progress
- `status`, `ps`, and `logs` can reconnect to the tracked run later

Use `up --resume-diff-only` when you want to inspect resume-related config deltas without submitting, and `up --allow-resume-changes` when you intentionally changed resume-coupled config between runs.

## 6. First-job debugging flow

Use this sequence when the first real submission fails:

```bash
hpc-compose validate -f compose.yaml
hpc-compose validate -f compose.yaml --strict-env
hpc-compose inspect --verbose -f compose.yaml
hpc-compose render --output job.sbatch -f compose.yaml
hpc-compose preflight -f compose.yaml
hpc-compose prepare -f compose.yaml
```

Use the debugging flow when you want to confirm:

- service order
- normalized image references
- cache artifact paths
- whether prepare steps will rebuild every submit
- whether the generated job script matches what you expect to hand to Slurm

<div class="callout warning">
  <p><strong>Warning</strong></p>
  <p><code>inspect --verbose</code> prints resolved environment values and final mount mappings. Treat its output as sensitive when the spec contains secrets.</p>
</div>

## 7. Revisit a tracked run later

```bash
hpc-compose jobs list
hpc-compose status -f compose.yaml
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
hpc-compose stats -f compose.yaml
hpc-compose logs -f compose.yaml --follow
```

Use `jobs list` first when you need to rediscover tracked runs under the current repo tree. Use `ps` for a stable per-service runtime snapshot, `watch` to reconnect to the live TUI, and `logs --follow` when you want the simplest text-only follower.

## From a source checkout

If you are running from a local checkout instead of an installed binary:

```bash
cargo build --release
target/release/hpc-compose validate -f examples/minimal-batch.yaml
target/release/hpc-compose inspect -f examples/minimal-batch.yaml
target/release/hpc-compose up --dry-run --skip-prepare --no-preflight \
  --script-out /tmp/hpc-compose-demo.sbatch \
  -f examples/minimal-batch.yaml
```

## Read Next

- Use [Examples](examples.md) when you want the closest known-good starting point.
- Use [Execution Model](execution-model.md) to understand what runs where and which paths must be shared.
- Use [Support Matrix](support-matrix.md) before adapting a real workflow to a new machine or cluster.
- Use [Task Guide](task-guide.md) when you want a goal-oriented entry point instead of the full reference.
- Use [Runbook](runbook.md) when adapting a real workload to a real cluster.
