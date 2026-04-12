# Installation

## Build from source today

If the repository's [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases) page is still empty, build from source until the first public release is published:

```bash
git clone https://github.com/NicolasSchuler/hpc-compose.git
cd hpc-compose
cargo build --release
./target/release/hpc-compose --help
```

This path is also the safest choice when you want to inspect or modify the checkout before using the CLI on a cluster.

## Install from a published release

For supported Linux and macOS targets, use a version-pinned installer so the installer script and the downloaded archive come from the same tag:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

Replace `vX.Y.Z` with a tag that exists on the GitHub Releases page.

By default this installs `hpc-compose` into `~/.local/bin` and verifies the published SHA-256 checksum before placing the binary. The checksum sidecars protect against download corruption or mismatched assets; use GitHub release verification and artifact attestations as the primary authenticity check.

On Unix installs, the release archive also ships section-1 manpages. After installation you can use:

```bash
man hpc-compose
man hpc-compose-submit
```

If you want shell integration immediately after install, generate completions with:

```bash
hpc-compose completions bash > ~/.local/share/bash-completion/completions/hpc-compose
hpc-compose completions zsh > ~/.zfunc/_hpc-compose
hpc-compose completions fish > ~/.config/fish/completions/hpc-compose.fish
```

Installer availability does not imply full runtime support. Check the [Support Matrix](support-matrix.md) before assuming that a platform can run submission, prepare, or watch workflows end to end.

Useful overrides:

```bash
RELEASE_TAG=vX.Y.Z

curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_INSTALL_DIR=/usr/local/bin HPC_COMPOSE_VERSION="$RELEASE_TAG" sh
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="$RELEASE_TAG" sh
```

Replace `vX.Y.Z` with the release tag you want from the GitHub Releases page.

For unreleased testing only, you can still fetch the installer script from `main`:

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

Treat that path as a moving target rather than a pinned release install.

Supported targets match the release workflow:

- Linux x86_64
- Linux arm64
- macOS x86_64
- macOS arm64

Windows release archives are also published, but Windows is not part of the installer path and is not an officially supported runtime target.

## Download a release build manually

Prebuilt archives are published on the project's [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases).

Typical flow on Linux or macOS:

```bash
RELEASE_TAG=vX.Y.Z

curl -L "https://github.com/NicolasSchuler/hpc-compose/releases/download/${RELEASE_TAG}/hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.tar.gz" -o hpc-compose.tar.gz
tar -xzf hpc-compose.tar.gz
./hpc-compose --help
```

Pick the archive that matches your platform from the release page. Linux x86_64 releases use a musl target to avoid common cluster glibc mismatches.

Unix release archives also contain `share/man/man1/` so the shipped manpages can be installed alongside the binary.

The repository keeps generated manpages under `man/man1`. Regenerate them from a checkout with:

```bash
cargo run --features manpage-bin --bin gen-manpages
cargo test --locked --test release_metadata
man -l man/man1/hpc-compose.1
```

## Verify a release

Use GitHub-native verification as the primary trust path for published binaries.

1. Verify that the release itself has a valid GitHub attestation:

```bash
RELEASE_TAG=vX.Y.Z
gh release verify "$RELEASE_TAG" -R NicolasSchuler/hpc-compose
```

2. Verify that a downloaded asset matches the attested release:

```bash
RELEASE_TAG=vX.Y.Z
ASSET="hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.tar.gz"

gh release download "$RELEASE_TAG" -R NicolasSchuler/hpc-compose -p "$ASSET"
gh release verify-asset "$RELEASE_TAG" "./$ASSET" -R NicolasSchuler/hpc-compose
```

3. Verify the artifact attestation directly and pin it to the release workflow identity:

```bash
gh attestation verify "./$ASSET" \
  --repo NicolasSchuler/hpc-compose \
  --signer-workflow NicolasSchuler/hpc-compose/.github/workflows/release.yml
```

Published releases also ship `SHA256SUMS` and per-asset `.sha256` files. Those checksums are primarily for installer compatibility, mirroring, and corruption checks. They are not the primary authenticity mechanism.

## Install through a native package manager

GitHub Releases also attach Linux-native packages for the published Linux targets:

```bash
RELEASE_TAG=vX.Y.Z

sudo apt install "./hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.deb"
sudo dpkg -i "./hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.deb"

sudo dnf install "./hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.rpm"
sudo rpm -i "./hpc-compose-${RELEASE_TAG}-x86_64-unknown-linux-musl.rpm"
```

Pick the package that matches your machine from the release page. Package availability does not change the runtime support policy; Linux cluster workflows still require the same Slurm, Pyxis, Enroot, and shared-storage assumptions described in the [Support Matrix](support-matrix.md).

## Install with Homebrew on macOS

The repository also exposes a same-repo Homebrew tap:

```bash
brew install NicolasSchuler/hpc-compose/hpc-compose
```

The formula tracks the latest published release on `main`. It installs the same prebuilt macOS tarballs and their shipped manpages.

## Internal mirrors and cluster-admin installs

For internal mirrors, preserve the release filenames exactly, including:

- the platform archives or native packages
- `SHA256SUMS`
- each per-asset `.sha256` sidecar

Then point the installer at the mirrored base URL and pin the matching version:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_BASE_URL="https://mirror.example.org/hpc-compose/${RELEASE_TAG}" \
        HPC_COMPOSE_VERSION="$RELEASE_TAG" sh
```

This keeps the installer version, mirrored assets, and checksum files aligned.

## Local docs commands

The repo ships two documentation layers:

- `mdbook` for the user manual
- `cargo doc` for contributor-facing crate internals

Useful commands:

```bash
mdbook build docs
mdbook serve docs
cargo doc --no-deps
```

## Verification

Before using a local build on a cluster workflow, validate the binary and one example spec:

```bash
env CACHE_DIR=/cluster/shared/hpc-compose-cache \
  target/release/hpc-compose validate -f examples/minimal-batch.yaml
env CACHE_DIR=/cluster/shared/hpc-compose-cache \
  target/release/hpc-compose inspect --verbose -f examples/minimal-batch.yaml
```
