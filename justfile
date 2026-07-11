set shell := ["bash", "-euo", "pipefail", "-c"]

MDBOOK_VERSION := "0.5.2"
LYCHEE_VERSION := "0.23.0"
PA11Y_CI_VERSION := "4.0.1"
ACTIONLINT_VERSION := "1.7.12"
TYPOS_VERSION := "1.28.4"
MARKDOWNLINT_CLI2_VERSION := "0.14.0"
CARGO_DENY_VERSION := "0.19.9"
CARGO_LLVM_COV_VERSION := "0.8.7"

_require-tools *tools:
    @missing=0; for tool in {{tools}}; do if ! command -v "$tool" >/dev/null 2>&1; then echo "missing required tool: $tool" >&2; missing=1; fi; done; exit "$missing"

_require-cargo-subcommands:
    @cargo deny --version >/dev/null 2>&1 || { echo "missing required cargo subcommand: cargo-deny" >&2; exit 1; }
    @cargo llvm-cov --version >/dev/null 2>&1 || { echo "missing required cargo subcommand: cargo-llvm-cov" >&2; exit 1; }

bootstrap-docs-tools: (_require-tools "cargo" "npm")
    cargo install mdbook --locked --version "{{MDBOOK_VERSION}}"
    cargo install lychee --locked --version "{{LYCHEE_VERSION}}"
    cargo install typos-cli --locked --version "{{TYPOS_VERSION}}"
    npm install --global "pa11y-ci@{{PA11Y_CI_VERSION}}" "markdownlint-cli2@{{MARKDOWNLINT_CLI2_VERSION}}"

# Install every cargo/npm-installable tool `just ci` needs: the docs tools plus
# the merge-gating cargo subcommands (cargo-deny, cargo-llvm-cov). Prints hints
# for the two system tools you install via your package manager.
bootstrap: bootstrap-docs-tools
    cargo install cargo-deny --locked --version "{{CARGO_DENY_VERSION}}"
    cargo install cargo-llvm-cov --locked --version "{{CARGO_LLVM_COV_VERSION}}"
    @echo "Next, install actionlint and shellcheck via your package manager:"
    @echo "  macOS:  brew install actionlint shellcheck"
    @echo "  Linux:  see https://github.com/rhysd/actionlint and https://www.shellcheck.net"

# Bump the project version in Cargo.toml and CITATION.cff and regenerate man
# pages. Finish by hand (release-metadata guards check these too): update the
# README citation version field and add a CHANGELOG section for the new version.
# Usage: just bump-version 0.1.51
bump-version VERSION:
    sed -i.bak -E 's/^version = "[0-9]+\.[0-9]+\.[0-9]+"$/version = "{{VERSION}}"/' Cargo.toml && rm -f Cargo.toml.bak
    sed -i.bak -E 's/^version: "[0-9]+\.[0-9]+\.[0-9]+"$/version: "{{VERSION}}"/' CITATION.cff && rm -f CITATION.cff.bak
    cargo run --locked --features manpage-bin --bin gen-manpages
    @echo "Bumped Cargo.toml + CITATION.cff to {{VERSION}} and regenerated man pages."
    @echo "Now update the README citation version field and add a CHANGELOG entry for {{VERSION}}."

clean:
    rm -rf target .tmp coverage htmlcov tarpaulin-report.html lcov.info *.profraw *.profdata

# Trim stale build artifacts without a full `cargo clean` (keeps the warm debug cache).
# Drops the duplicate coverage/mutants target trees unconditionally (a `cargo llvm-cov`
# run rebuilds the whole workspace into target/llvm-cov-target), then sweeps main-tree
# artifacts untouched for >N days via cargo-sweep when it is installed.
cache-sweep days="3":
    rm -rf target/llvm-cov-target target/mutants
    @if command -v cargo-sweep >/dev/null 2>&1; then \
        cargo sweep --time {{days}}; \
    else \
        echo "cargo-sweep not installed; removed duplicate trees only."; \
        echo "Install with: cargo install cargo-sweep"; \
    fi

# Drop artifacts not built by a currently-installed toolchain. Run after `rustup update`,
# which orphans the entire previous toolchain's artifacts that `--time` alone won't catch.
cache-sweep-installed:
    @if command -v cargo-sweep >/dev/null 2>&1; then \
        cargo sweep --installed; \
    else \
        echo "cargo-sweep not installed. Install with: cargo install cargo-sweep"; \
    fi

workflow-check: (_require-tools "actionlint")
    actionlint -color

check: workflow-check
    cargo fmt --all -- --check
    cargo clippy --all-targets --locked -- -D warnings
    cargo test --locked

docs-check: (_require-tools "mdbook" "lychee" "pa11y-ci" "typos" "markdownlint-cli2" "curl" "python3")
    python3 scripts/generate_site_guides.py --check
    python3 scripts/generate_agent_assets.py --check
    python3 scripts/package_skill.py --check
    python3 -m unittest discover -s scripts/tests -v
    python3 -m unittest discover -s skills/hpc-compose/tests -v
    mdbook build docs
    python3 scripts/generate_agent_assets.py --site-dir target/mdbook
    python3 scripts/generate_agent_assets.py --check --site-dir target/mdbook
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
    cargo run --locked --features manpage-bin --bin gen-manpages -- --check
    typos docs/src docs/brand docs/plans/2026-07-feature-brainstorm.md examples dev-cluster/README.md skills/hpc-compose llms.txt README.md CHANGELOG.md CONTRIBUTING.md GOVERNANCE.md SECURITY.md CODE_OF_CONDUCT.md
    markdownlint-cli2
    shopt -s globstar nullglob; lychee --no-progress --include-fragments=anchor-only --fallback-extensions md --exclude '^https://github\.com/NicolasSchuler/hpc-compose/edit/main/' --exclude '^https://nicolasschuler\.github\.io/hpc-compose/(raw/|llms-ctx(-full)?\.txt$|agent-command-policy(-v[0-9.]+)?\.json$|schema/)' --exclude '^https://nicolasschuler\.github\.io/hpc-compose/[^#]+\.html#' --exclude-path 'target/mdbook/404\.html$' README.md CHANGELOG.md CONTRIBUTING.md GOVERNANCE.md SECURITY.md CODE_OF_CONDUCT.md llms.txt dev-cluster/README.md docs/brand/README.md docs/plans/2026-07-feature-brainstorm.md docs/src/**/*.md examples/**/*.md skills/hpc-compose/**/*.md target/mdbook/**/*.html
    python3 scripts/gen_pa11y_urls.py --output target/pa11y-ci.json
    python3 -m http.server 3000 --directory target/mdbook >/tmp/hpc-compose-docs-http.log 2>&1 & server_pid=$!; trap 'kill "$server_pid"' EXIT; for _ in $(seq 1 30); do if curl -fsS http://127.0.0.1:3000/ >/dev/null; then break; fi; sleep 1; done; python3 scripts/generate_agent_assets.py --check --site-dir target/mdbook --base-url http://127.0.0.1:3000; pa11y-ci --config target/pa11y-ci.json

examples-check: (_require-tools "shellcheck")
    cargo build --locked
    for f in examples/*.yaml; do echo "Validating $f"; env -u CACHE_DIR cargo run --locked -- validate -f "$f"; done
    shellcheck install.sh scripts/cluster_smoke.sh scripts/devcluster.sh scripts/devcluster_collect_evidence.sh scripts/devcluster_case.sh scripts/devcluster_local_case.sh scripts/devcluster_e2e.sh scripts/devcluster_remote_e2e.sh scripts/devcluster_otp_e2e.sh scripts/remote_gpu_e2e.sh dev-cluster/otp-sim.sh
    tmpdir="$(mktemp -d)"; trap 'rm -rf "$tmpdir"' EXIT; for f in examples/*.yaml; do echo "Shellchecking rendered $f"; out="$tmpdir/$(basename "$f" .yaml).sbatch"; env -u CACHE_DIR cargo run --locked -- render -f "$f" --output "$out"; shellcheck -e SC2034 -x -s bash "$out"; done

# Boot the local single-node Slurm dev cluster and run the real
# up -> sbatch -> slurmd -> sacct path end to end against every spec under
# dev-cluster/specs (see dev-cluster/README.md). Needs docker/podman compose and
# a privileged container, so it is NOT part of `ci`; CI runs it as the separate
# `dev-cluster-e2e` job with a cached image build.
dev-cluster-e2e:
    scripts/devcluster_e2e.sh

# List the opt-in local-only real-scheduler cases. These are intentionally not
# wired into CI; use them while changing preemption, active probes, or remote
# follow-up behavior.
dev-cluster-cases:
    @scripts/devcluster_case.sh --list

# Run one opt-in local-only dev-cluster case while reusing the running cluster.
# Example: `just dev-cluster-case preemption`.
dev-cluster-case case:
    scripts/devcluster_case.sh "{{case}}"

# Boot the dev cluster as an SSH login-node stand-in and exercise the thin
# remote-submit path (`up --remote`) from this host: rsync the project over,
# submit on the node via real sbatch, and track to COMPLETED. Same privileged
# container requirements as dev-cluster-e2e; CI runs it in the same job.
dev-cluster-remote-e2e:
    scripts/devcluster_remote_e2e.sh

# Opt-in REAL-GPU end-to-end check for the metrics pipeline against a real
# cluster (HAICORE by default). Drives the thin laptop client (`up --remote`) to
# submit a tiny 1-GPU cuda-probe job, watches it to COMPLETED, then asserts the
# collected GPU/CPU sampler output (gpu.jsonl, cpu.jsonl, `stats --format json`).
# Needs a live login node, a real GPU allocation, and ONE interactive OTP, so it
# is deliberately NOT part of `ci` and never runs in CI — run it by hand.
# Override the host/account/partition via env (see the script header):
#   HPC_REMOTE_HOST=haicore HPC_SLURM_ACCOUNT=kastel just remote-gpu-e2e
remote-gpu-e2e:
    scripts/remote_gpu_e2e.sh

# Flip the login-node stand-in into an OTP/2FA-requiring mode and prove the
# laptop thin client's SSH ControlMaster multiplexing authenticates a whole
# multi-command session exactly ONCE (the one-OTP-per-session property). Same
# privileged container requirements as dev-cluster-e2e; CI runs it in the same job.
dev-cluster-otp-e2e:
    scripts/devcluster_otp_e2e.sh

fuzz-check:
    cargo fuzz check spec_parser

release-check: _require-cargo-subcommands
    cargo test --locked --test release_metadata
    cargo deny check
    cargo llvm-cov --workspace --locked --no-report
    # Broad core-logic gate (mirrors the CI Coverage job): excludes presentation
    # surfaces so a regression in core planner/runtime/job logic trips here first.
    cargo llvm-cov report --json --summary-only --locked --ignore-filename-regex 'commands/|output/mod\.rs|watch_ui\.rs|term\.rs|progress\.rs|manpages\.rs|main\.rs|cli/|job/model\.rs' --fail-under-lines 92 --fail-under-regions 91 --fail-under-functions 88
    # Strict whole-crate floor (only the four thin declarative shells excluded).
    cargo llvm-cov report --json --summary-only --locked --ignore-filename-regex '(^|/)commands/mod\.rs$|(^|/)cli/commands\.rs$|(^|/)main\.rs$|(^|/)job/model\.rs$' --fail-under-lines 87 --fail-under-regions 87 --fail-under-functions 85

# `release-check` runs `cargo llvm-cov`, which leaves a full duplicate workspace build in
# target/llvm-cov-target; sweep it (and prune >3d main-tree artifacts) once the suite passes.
ci: check docs-check examples-check release-check cache-sweep
