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
just bootstrap-docs-tools
just check
just docs-check
just examples-check
just release-check
just ci
```

The `just` recipes mirror the main CI gates. They expect the same external QA tools used in CI (`actionlint`, `mdbook`, `lychee`, `pa11y-ci`, `shellcheck`, `cargo-deny`, and `cargo-llvm-cov`) to be installed locally. `just bootstrap-docs-tools` installs the pinned docs tools (`mdbook`, `lychee`, and `pa11y-ci`); install `actionlint`, `shellcheck`, `cargo-deny`, and `cargo-llvm-cov` through your platform package manager or Cargo as appropriate.

Quality gates:

| Gate | Command | Use it for |
| --- | --- | --- |
| Fast Rust and workflow check | `just check` | GitHub workflow linting, formatting, Clippy, and Rust tests. |
| Documentation | `just docs-check` | mdBook, rustdoc warnings, manpage drift, links, and accessibility. |
| Examples and shell output | `just examples-check` | Shipped spec validation and shellcheck for rendered batch scripts. |
| Release metadata and coverage | `just release-check` | Release metadata, dependency policy, and coverage thresholds. |
| Full local CI mirror | `just ci` | All local gates above. |

Equivalent raw commands:

```bash
actionlint -color
cargo test --locked
cargo test --locked --test cli_spec --test cli_runtime --test cli_cache --test cli_context --test cli_init --test cli_jobs
cargo test --locked --test release_metadata
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
mdbook build docs
cargo run --locked --features manpage-bin --bin gen-manpages -- --check
```

Release/distribution helpers:

```bash
python3 scripts/update_homebrew_formula.py \
  --version X.Y.Z \
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
- If the example should be available through `hpc-compose new` (and the legacy `init` alias), add it to `src/init.rs` with a concise description.

## Pull requests

- Use a short, imperative commit subject.
- Describe the user-visible change in the PR body.
- List the verification commands you ran.
- Call out any coordinated documentation or example updates.

## Releases

- Create an annotated `vX.Y.Z` tag. The tag body becomes the curated summary block in the published release notes.
- Tagging `vX.Y.Z` publishes GitHub release tarballs plus Linux `.deb` and `.rpm` assets.
- Published releases now include `SHA256SUMS`, per-asset `.sha256` sidecars, and GitHub artifact attestations for the release assets.
- After the release assets are live, verify the published release and at least one downloaded asset:

```bash
gh release verify vX.Y.Z -R NicolasSchuler/hpc-compose
gh release verify-asset vX.Y.Z ./hpc-compose-vX.Y.Z-x86_64-unknown-linux-musl.tar.gz -R NicolasSchuler/hpc-compose
gh attestation verify ./hpc-compose-vX.Y.Z-x86_64-unknown-linux-musl.tar.gz \
  --repo NicolasSchuler/hpc-compose \
  --signer-workflow NicolasSchuler/hpc-compose/.github/workflows/release.yml
```

- The release workflow opens a follow-up PR to refresh `Formula/hpc-compose.rb` on `main`; merge that PR after verifying the generated checksums.
- Keep the version-pinned installer docs aligned with the published tag and keep the release notes links accurate.
- Treat package availability as a distribution convenience only. Do not widen the supported runtime matrix unless `docs/src/support-matrix.md` changes too.

## Reporting bugs

- Open a GitHub issue with the compose file shape, command used, observed output, and cluster-specific constraints when relevant.
- For sensitive security issues, use the process in `SECURITY.md` instead of a public issue.
