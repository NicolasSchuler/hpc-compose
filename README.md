# hpc-compose

`hpc-compose` is a single-binary launcher that turns a Compose-like spec into one Slurm job running multiple services through Enroot and Pyxis.

It is intentionally **not** a full Docker Compose implementation. It targets the subset that maps cleanly to `sbatch` + `srun` + Enroot on a single node.

## What it is for

- One Slurm allocation per application.
- One node per allocation in v1.
- Multiple services started inside that allocation.
- Remote images such as `redis:7` or existing local `.sqsh` images.
- Optional image customization on the login node through `x-enroot.prepare`.
- Shared cache management for imported and prepared images.
- Readiness-gated startup across dependent services.

## What it does not support

- Compose `build:`
- `ports`
- custom Docker networks / `network_mode`
- `restart` policies
- `deploy`
- multi-node service placement
- mixed string/array `entrypoint` + `command` combinations in ambiguous cases

If you need to customize an image, use `image:` plus `x-enroot.prepare`, not `build:`.

## Start here

- **Runbook:** [`docs/runbook.md`](docs/runbook.md) for the end-to-end workflow from choosing a cache directory to reading logs and pruning cache artifacts.
- **Settings reference:** [`docs/spec-reference.md`](docs/spec-reference.md) for the supported Compose subset, `x-slurm`, and `x-enroot` settings.
- **Examples:** [`examples/README.md`](examples/README.md) for choosing and adapting the shipped example specs.

## Quickstart

Build the binary:

```bash
cargo build --release
```

Then try one of the examples:

```bash
target/release/hpc-compose validate -f examples/dev-python-app.yaml
target/release/hpc-compose inspect -f examples/dev-python-app.yaml
target/release/hpc-compose preflight -f examples/dev-python-app.yaml
target/release/hpc-compose render -f examples/dev-python-app.yaml --output /tmp/job.sbatch
```

For the full submit workflow, including prepare and log handling, use the [runbook](docs/runbook.md).

## Releases

Push a version tag such as `v0.1.0` to publish downloadable binaries through GitHub Actions:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow runs `cargo test --locked`, builds release archives for Linux, macOS, and Windows, and attaches those archives plus SHA256 checksum files to the GitHub Release for that tag. If you already have a tag and need to backfill assets, you can also run the `Release` workflow manually from the Actions tab and provide that tag.

## Command flow

- `validate` checks that the spec parses and normalizes successfully.
- `inspect` prints the normalized runtime plan and expected cache behavior.
- `preflight` checks the login node environment before submission.
- `prepare` imports or rebuilds missing runtime artifacts on the login node.
- `render` writes the generated `sbatch` script without submitting it.
- `submit` runs preflight, optional prepare, render, and `sbatch`.
- `cache list|inspect|prune` inspects and manages cached artifacts.

## Examples

- [`examples/app-redis-worker.yaml`](examples/app-redis-worker.yaml): multi-service launch ordering and readiness checks.
- [`examples/dev-python-app.yaml`](examples/dev-python-app.yaml): mounted-code development workflow.
- [`examples/llama-app.yaml`](examples/llama-app.yaml): GPU-backed service with a dependent application.

## Build and test

```bash
cargo build --release
cargo test
cargo test --test cli
```
