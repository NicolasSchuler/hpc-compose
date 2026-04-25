# Installation

For normal use, install from a published GitHub Release. Build from source when you are developing the project or need to inspect a local checkout before using it on a cluster.

## Install From A Published Release

Pick the release tag you want from the [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases) page and pin it:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

The installer downloads the matching archive for the current Linux or macOS machine, verifies the published `.sha256` sidecar, installs `hpc-compose` into `~/.local/bin` by default, and installs shipped Unix manpages when present.

Useful overrides:

```bash
RELEASE_TAG=vX.Y.Z

curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_INSTALL_DIR=/usr/local/bin HPC_COMPOSE_VERSION="$RELEASE_TAG" sh
```

Installer availability does not imply full runtime support. Check the [Support Matrix](support-matrix.md) before assuming a platform can run submission, prepare, or watch workflows end to end.

## About The `main` Installer Script

Fetching `install.sh` from `main` without `HPC_COMPOSE_VERSION` does **not** install unreleased `main`:

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

That command runs the moving script from `main`, but the script resolves the latest published GitHub Release and downloads from `releases/download/<tag>/...`. Use the version-pinned command above for reproducible installs. Use a source checkout when you want unreleased code.

## Manual Release Download

Prebuilt archives are published on the release page. Pick the archive that matches your platform.

Example for Linux `x86_64`:

```bash
RELEASE_TAG=vX.Y.Z

curl -L "https://github.com/NicolasSchuler/hpc-compose/releases/download/${RELEASE_TAG}/hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.tar.gz" -o hpc-compose.tar.gz
tar -xzf hpc-compose.tar.gz
./hpc-compose --help
```

Linux `x86_64` releases use a musl target to avoid common cluster glibc mismatches. Unix release archives also contain `share/man/man1/`.

## Native Packages

Published Linux releases may include `.deb` and `.rpm` assets:

```bash
RELEASE_TAG=vX.Y.Z

sudo apt install "./hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.deb"
sudo dnf install "./hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.rpm"
```

Package availability does not change runtime support policy. Linux cluster workflows still need Slurm client tools, the selected runtime backend, and shared storage for `x-slurm.cache_dir`.

## Homebrew On macOS

The repository exposes a same-repo Homebrew tap:

```bash
brew install NicolasSchuler/hpc-compose/hpc-compose
```

The formula is refreshed by release automation when a Homebrew-published release is cut. Check `brew info NicolasSchuler/hpc-compose/hpc-compose` when you need to confirm the formula version before installing.

macOS support is for authoring and local non-runtime commands such as `new`, `plan`, `validate`, `inspect`, `render`, and `completions`; it is not a supported Slurm runtime target.

## Verify A Release

Use GitHub-native verification as the primary trust path for published binaries.

1. Verify the release:

```bash
RELEASE_TAG=vX.Y.Z
gh release verify "$RELEASE_TAG" -R NicolasSchuler/hpc-compose
```

2. Verify a downloaded asset:

```bash
RELEASE_TAG=vX.Y.Z
ASSET="hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.tar.gz"

gh release download "$RELEASE_TAG" -R NicolasSchuler/hpc-compose -p "$ASSET"
gh release verify-asset "$RELEASE_TAG" "./$ASSET" -R NicolasSchuler/hpc-compose
```

3. Verify the artifact attestation directly:

```bash
gh attestation verify "./$ASSET" \
  --repo NicolasSchuler/hpc-compose \
  --signer-workflow NicolasSchuler/hpc-compose/.github/workflows/release.yml
```

Published releases also ship `SHA256SUMS` and per-asset `.sha256` files. Those checksums are primarily for installer compatibility, mirroring, and corruption checks; attestations are the stronger authenticity signal.

## Internal Mirrors And Cluster-Admin Installs

For internal mirrors, preserve release filenames exactly, including:

- platform archives or native packages
- `SHA256SUMS`
- each per-asset `.sha256` sidecar

Then point the installer at the mirrored base URL and pin the matching version:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_BASE_URL="https://mirror.example.org/hpc-compose/${RELEASE_TAG}" \
        HPC_COMPOSE_VERSION="$RELEASE_TAG" sh
```

`HPC_COMPOSE_VERSION` is required when `HPC_COMPOSE_BASE_URL` is set so the installer, mirrored assets, and checksum files stay aligned.

## Build From Source

Use this path for development, unreleased testing, or local inspection:

```bash
git clone https://github.com/NicolasSchuler/hpc-compose.git
cd hpc-compose
cargo build --release
./target/release/hpc-compose --help
```

Before using a local build on a cluster workflow, validate the binary and one example spec:

```bash
env CACHE_DIR=/cluster/shared/hpc-compose-cache \
  target/release/hpc-compose validate -f examples/minimal-batch.yaml
env CACHE_DIR=/cluster/shared/hpc-compose-cache \
  target/release/hpc-compose plan --verbose -f examples/minimal-batch.yaml
```

## Local Docs Commands

The repo ships two documentation layers:

- `mdbook` for the user manual
- `cargo doc` for contributor-facing crate internals

Useful commands:

```bash
mdbook build docs
mdbook serve docs
cargo doc --no-deps
```

Regenerate checked-in manpages from a checkout with:

```bash
cargo run --features manpage-bin --bin gen-manpages
cargo test --locked --test release_metadata
man -l man/man1/hpc-compose.1
```
