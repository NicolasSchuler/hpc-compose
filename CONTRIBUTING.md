# Contributing

Thanks for contributing to `hpc-compose`.

## Local setup

```bash
git clone https://github.com/NicolasSchuler/hpc-compose.git
cd hpc-compose
cargo build
```

Useful local commands:

```bash
cargo test --locked
cargo test --locked --test cli
cargo test --locked --test release_metadata
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
mdbook build docs
cargo run --locked --features manpage-bin --bin gen-manpages -- --check
```

Release/distribution helpers:

```bash
python3 scripts/update_homebrew_formula.py \
  --version 0.1.23 \
  --arm64-sha256 <aarch64-apple-darwin tarball sha256> \
  --x86-64-sha256 <x86_64-apple-darwin tarball sha256>
```

## Expectations for changes

- Keep the project scope aligned with one Slurm allocation per application.
- Prefer small, coherent changes over broad refactors.
- Add or update tests when parser, planner, prepare, render, cache, or tracked-job behavior changes.
- If a user-facing workflow changes, update the relevant docs in `README.md`, `docs/src/`, and `examples/` together.
- When release-facing docs or CLI help change, regenerate checked-in manpages with `cargo run --features manpage-bin --bin gen-manpages` and keep `tests/release_metadata.rs` passing.

## Examples

- Validate every shipped example after changes that affect parsing or planning:

```bash
cargo build --locked
for f in examples/*.yaml; do
  cargo run -- validate -f "$f"
done
```

- When adding a new example, document when to use it in `docs/src/examples.md`.
- If the example should be available through `hpc-compose init`, add it to `src/init.rs` with a concise description.

## Pull requests

- Use a short, imperative commit subject.
- Describe the user-visible change in the PR body.
- List the verification commands you ran.
- Call out any coordinated documentation or example updates.

## Releases

- Tagging `vX.Y.Z` publishes GitHub release tarballs plus Linux `.deb` and `.rpm` assets.
- After the release assets are live, update `Formula/hpc-compose.rb` with `scripts/update_homebrew_formula.py` and commit that formula refresh on `main`.
- Treat package availability as a distribution convenience only. Do not widen the supported runtime matrix unless `docs/src/support-matrix.md` changes too.

## Reporting bugs

- Open a GitHub issue with the compose file shape, command used, observed output, and cluster-specific constraints when relevant.
- For sensitive security issues, use the process in `SECURITY.md` instead of a public issue.
