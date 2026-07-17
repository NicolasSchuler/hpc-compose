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
just bootstrap
just check
just docs-check
just examples-check
just release-check
just ci
```

The `just` recipes mirror the main CI gates. They expect the same external QA tools used in CI (`actionlint`, `mdbook`, `lychee`, `pa11y-ci`, `typos`, `markdownlint-cli2`, `shellcheck`, `cargo-deny`, `cargo-llvm-cov`, and `cargo-sweep`) to be installed locally. `just bootstrap` installs everything cargo/npm-installable — the pinned docs tools (via `just bootstrap-docs-tools`: `mdbook`, `lychee`, `typos`, `pa11y-ci`, `markdownlint-cli2`) plus the pinned Cargo subcommands — and prints package-manager hints for `actionlint` and `shellcheck`, the only tools you install through your platform package manager.

Quality gates:

| Gate | Command | Use it for |
| --- | --- | --- |
| Fast Rust and workflow check | `just check` | GitHub workflow linting, formatting, Clippy, and Rust tests. |
| Documentation | `just docs-check` | mdBook, rustdoc warnings, manpage drift, spell check, markdown lint, links, and accessibility. |
| Examples and shell output | `just examples-check` | Shipped spec validation and shellcheck for rendered batch scripts. |
| Release metadata and coverage | `just release-check` | Release metadata, dependency policy, and coverage thresholds. |
| Full local CI mirror | `just ci` | All local gates above. |

Equivalent raw commands:

```bash
actionlint -color
cargo fmt --all -- --check
CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets --locked -- -D warnings
CARGO_INCREMENTAL=0 cargo test --workspace --locked
mdbook build docs
cargo run --locked --features manpage-bin --bin gen-manpages -- --check
```

Focused commands such as `cargo check`, a single filtered test, or a normal development build keep Cargo's default incremental compilation. The comprehensive `just` and CI gates set `CARGO_INCREMENTAL=0` because their artifacts are unlikely to be reused interactively.

## Build artifact budget

The development profile keeps line tables for useful panic source locations without forcing a cross-platform split-debug format. To inspect the main artifact categories before changing the policy:

```bash
du -sh target/debug/deps target/debug/incremental target/debug/build
cargo tree -d
```

After CI or another comprehensive local gate, preview and apply the default 8 GB cache budget with:

```bash
just cache-sweep-preview
just cache-sweep
```

`just ci` applies the sweep only after all gates pass. Before adopting a different budget, preview it explicitly, for example `just cache-sweep-preview 10GB`; then apply it with `just cache-sweep 3 10GB`. Do not sweep after focused builds where the artifacts are likely to be reused.

Integration-test source files under `tests/` are registered through the explicit targets in `Cargo.toml` and the shared harnesses under `tests/harnesses/`. Add each new source to exactly one harness unless it needs process isolation; `release_metadata` enforces both that rule and the nine-binary budget. Run a grouped source with its module filter, for example:

```bash
cargo test --locked --test cli_spec
cargo test --locked --test project_contracts docs_examples::
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
- In docs, describe deliberate limits as present-tense design choices, not version-coupled "v1" limitations. Give every `docs/src/` page a `## Related Docs` (or `## Read Next`) footer, and use `<job-id>` for tracked-job placeholders. `cargo test --test project_contracts docs_examples::` enforces these conventions.
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
