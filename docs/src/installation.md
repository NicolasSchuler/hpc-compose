# Installation

## One-line installer

For supported Linux and macOS targets, the repo now ships a small installer script that picks the newest release and the matching archive for your machine:

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

By default this installs `hpc-compose` into `~/.local/bin` and verifies the published SHA-256 checksum before placing the binary.

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

curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | env HPC_COMPOSE_INSTALL_DIR=/usr/local/bin sh
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | env HPC_COMPOSE_VERSION="$RELEASE_TAG" sh
```

Replace `vX.Y.Z` with the release tag you want from the GitHub Releases page.

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

## Build from source

Requirements:

- Rust stable toolchain
- A normal local build machine for the CLI itself
- Slurm/Enroot tools only when you actually run `preflight`, `prepare`, or `submit`

```bash
git clone https://github.com/NicolasSchuler/hpc-compose.git
cd hpc-compose
cargo build --release
./target/release/hpc-compose --help
```

The repository keeps generated manpages under `man/man1`. Regenerate them from a checkout with:

```bash
cargo run --features manpage-bin --bin gen-manpages
cargo test --locked --test release_metadata
man -l man/man1/hpc-compose.1
```

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
