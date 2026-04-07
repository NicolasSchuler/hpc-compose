# Installation

## One-line installer

For supported Linux and macOS targets, the repo now ships a small installer script that picks the newest release and the matching archive for your machine:

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | sh
```

By default this installs `hpc-compose` into `~/.local/bin` and verifies the published SHA-256 checksum before placing the binary.

Installer availability does not imply full runtime support. Check the [Support Matrix](support-matrix.md) before assuming that a platform can run submission, prepare, or watch workflows end to end.

Useful overrides:

```bash
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | env HPC_COMPOSE_INSTALL_DIR=/usr/local/bin sh
curl -fsSL https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh | env HPC_COMPOSE_VERSION=v0.1.12 sh
```

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
curl -L https://github.com/NicolasSchuler/hpc-compose/releases/latest/download/hpc-compose-v0.1.12-x86_64-unknown-linux-musl.tar.gz -o hpc-compose.tar.gz
tar -xzf hpc-compose.tar.gz
./hpc-compose --help
```

Pick the archive that matches your platform from the release page. Linux x86_64 releases use a musl target to avoid common cluster glibc mismatches.

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
target/release/hpc-compose validate -f examples/minimal-batch.yaml
target/release/hpc-compose inspect --verbose -f examples/minimal-batch.yaml
```
